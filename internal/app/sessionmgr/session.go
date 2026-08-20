package sessionmgr

import (
	"context"
	"errors"
	"sync"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/despachador"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// ErrNoLiveSender: el multiplex CloudLink intentó enviar por el cliente vivo de la sesión pero no hay
// ciclo de escucha activo (entre reconexiones o antes del primer Connect del listener). El Adapter lo
// traduce a Ack{ok=false} y NO tumba nada: es un envío que llegó con la sesión sin cliente vivo.
var ErrNoLiveSender = errors.New("sessionmgr: sin cliente vivo para enviar en esta sesión")

// ErrInyectorNoCableado: la sesión ESTÁ VIVA pero no hay camino entrante por el que inyectar AHORA MISMO
// (MP-10 Parte A). Es el gemelo exacto de ErrNoLiveSender para el camino ENTRANTE, y se mantiene DISTINTO
// del error de «no hay sesión viva con ese id» (ErrSesionNoViva, inyector.go) porque los dos se arreglan de
// forma opuesta: este se arregla ESPERANDO (el ciclo de escucha está subiendo), el otro emparejando o
// restaurando la sesión. Colapsarlos mandaría al operador a reparar lo que no está roto.
//
// ⚠️ CUBRE DOS ESTADOS, no uno, y el segundo es el que de verdad se pisa en campo — por eso el texto ya no
// dice solo «aún no cableó»:
//
//  1. AÚN NO SE CABLEÓ: la sesión se acaba de registrar y su factory todavía no publicó el cable
//     (`liveInyectar` nil). Es el arranque, y dura lo que tarda el primer ciclo.
//  2. SE DESCABLEÓ POR RECONEXIÓN: el cable está puesto pero apunta a un gateway cuyo `serve()` ya salió y
//     limpió su Listener. `runListener` está esperando el backoff exponencial (hasta 60 s) para reconstruir
//     el gateway, y nadie pone el cable a nil mientras tanto. Lo detecta la traducción de
//     whatsmeow.ErrSinEscuchaViva en Manager.InyectarEntrante (inyector.go), no la guarda `fn == nil`.
//
// Los dos piden lo mismo a quien llama —esperar y repetir— y por eso comparten centinela y comparten 409.
var ErrInyectorNoCableado = errors.New("sessionmgr: la sesión está viva pero no hay escucha a la que inyectar " +
	"(su listener aún no cableó el inyector, o se descableó al caer y espera el backoff de reconexión)")

// SessionHealth es la salud de RUNTIME del listener de una sesión (design §10.H): un estado vivo,
// distinto del estado de NEGOCIO persistido (domain.SessionState: pairing/active/loggedout). El
// plano de control (GET /v1/sessions, T6) lo expone para que el operador vea una sesión 'degraded'
// (su socket cayó y está reintentando) sin que eso tumbe el proceso ni las otras sesiones.
type SessionHealth int

const (
	// HealthStarting: la goroutine listener se arrancó pero aún no reporta socket vivo (estado inicial).
	HealthStarting SessionHealth = iota
	// HealthListening: el listener está escuchando (su runner corre sin haber caído).
	HealthListening
	// HealthDegraded: el listener cayó (error o pánico) y está reintentando con backoff (§10.H). Aislado:
	// no afecta a las demás sesiones.
	HealthDegraded
	// HealthStopped: el listener terminó por apagado ordenado (Stop canceló su context, §10.I).
	HealthStopped
)

// String da una etiqueta legible de la salud (logs / plano de control).
func (h SessionHealth) String() string {
	switch h {
	case HealthStarting:
		return "starting"
	case HealthListening:
		return "listening"
	case HealthDegraded:
		return "degraded"
	case HealthStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// liveSession es el estado VIVO de una sesión que el Manager posee (design §1). Reúne el metadato de
// negocio, la custodia DEK resuelta para ESA sesión, su logger etiquetado y, a partir de T4, el cancel
// de su goroutine listener y su salud de runtime.
//
// La conexión (store/cliente whatsmeow) NO se materializa como campo: el listener la abre y la cierra
// DENTRO de su goroutine (design §6/§10.I), de modo que cada intento de reconexión obtiene un handle
// fresco y el apagado ordenado cierra el *sql.DB vía defer al cancelarse el context. Así el Manager no
// arrastra un puntero compartido a recursos de red que habría que proteger entre goroutines.
type liveSession struct {
	// meta es el metadato de negocio persistido (session_id, jid, estado, store_dir, timestamps). Es
	// inmutable tras el registro: el listener NO lo muta (la salud de runtime va en `health`), así que
	// List() puede leerlo bajo el lock del Manager sin tocar mu.
	meta domain.Session
	// custody es la custodia DEK de ESTA sesión (NewFileCustody(layout.DEKPath(id))); inyectada, no global.
	custody app.KeyCustody
	// log arrastra session_id/jid en cada línea (design §10.J); hijo del logger del Manager.
	log sharedlogger.Logger

	// mu protege cancel/done/health/lastErr: los escribe la goroutine listener y los leen Stop()/Health().
	mu sync.Mutex
	// cancel detiene la goroutine listener de la sesión (apagado ordenado, design §10.I). nil hasta que
	// startListener arranca un listener real.
	cancel context.CancelFunc
	// done se CIERRA cuando la goroutine listener de la sesión retorna (tras cancel, ya cerró su *sql.DB
	// vía defer). Permite a Unlink esperar SOLO a ESTA goroutine —el borrado quirúrgico de una sesión
	// sin tocar a las demás (design §7)— sin usar el WaitGroup GLOBAL del Manager (que une a todas). nil
	// si no se arrancó listener (sesión registrada sin escucha): waitDone es entonces un no-op.
	done chan struct{}
	// despachadores cuenta la(s) goroutine(s) del DESPACHADOR de la cola de esta sesión (Plan 051 Ola 3,
	// T3.3). Es el gemelo POR SESIÓN de `done`, para el segundo hilo que la sesión estrena en esa ola: el
	// que drena su cola durable y entrega al cable.
	//
	// POR QUÉ UN WaitGroup Y NO OTRO `chan struct{}`: `done` es de un solo cierre porque la goroutine del
	// listener es exactamente una y vive todo el ciclo; aquí el WaitGroup no obliga a decidir de antemano
	// si el despachador arranca (con la cola no cableada NO arranca, y entonces esperarlo es un no-op
	// inmediato en vez de un canal nil que hay que comprobar). Lo usa stopLive para no borrar la DEK de una
	// sesión cuyo despachador aún esté abriendo filas. No lo protege `mu`: un WaitGroup ya es seguro entre
	// goroutines, y su Add ocurre ANTES del `go` (happens-before), igual que el de `m.wg`.
	despachadores sync.WaitGroup

	// despachador es EL DESPACHADOR DE ESTA SESIÓN, retenido (Plan 051 Ola 4 · T4.0). Hasta esta ola la `d`
	// que devolvía `despachador.New` era una VARIABLE LOCAL de `startDespachador` que sólo veía su propia
	// goroutine: sus contadores —los ocho motivos de omisión, las cabezas atascadas, los dos sellos— existían
	// y no había NINGUNA forma de leerlos desde fuera. No faltaba un tubo, faltaba esta referencia.
	//
	// SE PROTEGE CON `mu`, EL CANDADO QUE YA TIENE LA SESIÓN, y no con uno nuevo: lo escribe la goroutine que
	// arranca el despachador y lo lee el colector de salud desde el hilo del heartbeat o del plano de control.
	// Es el mismo par escritor/lector que `cancel` y `health`, así que comparte su candado (un tercer mutex
	// sobre el mismo struct sólo añadiría un orden de bloqueo que alguien puede invertir).
	//
	// nil mientras no haya despachador arrancado: sin cola cableada, sin mux, sin sink, o si `New` falló. Ese
	// nil es un HECHO («esta sesión escucha pero no drena»), y por eso `despachoStats` lo distingue de un
	// despachador con todos los contadores a cero en vez de devolver un struct vacío.
	//
	// NO SE PONE A nil CUANDO EL BUCLE TERMINA, y es deliberado: una sesión viva cuyo despachador murió por
	// pánico es exactamente el caso en el que hay que poder leer sus contadores. La referencia no queda
	// colgando: muere con la propia liveSession cuando el Manager la saca de `live` (Unlink/cleanupPairing).
	despachador *despachador.Despachador

	// health es la salud de runtime observada por la goroutine listener (starting→listening→degraded→stopped).
	health SessionHealth
	// lastErr es la última causa de caída del listener (para diagnóstico/plano de control); nil si sano.
	lastErr error
	// liveSend es el emisor por CLIENTE VIVO de esta sesión, ROTADO por el factory del listener en cada
	// ciclo de (re)conexión (el gateway whatsmeow se recrea por ciclo, lección Plan 006: nada efímero).
	// El multiplex CloudLink registra sendVia (indirección ESTABLE) UNA sola vez al arrancar; así un
	// comando SendText siempre llega al cliente vivo ACTUAL de la sesión, no a uno muerto de un ciclo
	// previo. nil SOLO antes del primer Connect —nadie lo limpia nunca: el factory lo re-apunta, no lo
	// borra—, y en ese estado sendVia devuelve ErrNoLiveSender. Recibe el
	// command_id del envío (Plan 013 §10.E) para alimentar la correlación command_id ↔ MessageID.
	liveSend func(ctx context.Context, commandID, to, text string) error
	// liveMediaSend es el emisor de ARCHIVOS por cliente vivo (Plan 017 §7), hermano de liveSend: lo rota
	// el factory del listener en cada ciclo apuntando al gateway recién creado. El multiplex registra
	// sendViaMedia (indirección estable). nil SOLO antes del primer Connect (mismo motivo que liveSend:
	// el cable se re-apunta, nunca se limpia); en ese estado sendViaMedia devuelve ErrNoLiveSender.
	liveMediaSend func(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) error
	// liveInyectar es el INYECTOR DE ENTRANTES SINTÉTICOS de esta sesión (MP-10 Parte A): mete un mensaje
	// fabricado por el camino REAL del handler entrante del gateway, para poder medir el p99 de INV-051.2 sin
	// mandar cien mensajes de verdad contra el número de producción. Es el TERCER campo función de este
	// bloque y se rota igual que sus dos hermanos —el factory del listener lo publica en cada ciclo de
	// (re)conexión apuntando al gateway recién creado (lección Plan 006: nada efímero)—, con una asimetría que
	// conviene tener presente: los otros dos son SALIDA (el mux empuja hacia WhatsApp) y este es ENTRADA (el
	// plano de control empuja hacia el handler), así que el disparador no es el stream del cloud sino una
	// petición local.
	//
	// 🔴 CUÁNDO ES NIL, DE VERDAD: SOLO antes del primer ciclo. NADIE lo limpia nunca — el factory es el
	// único que lo escribe (listen.go:169) y siempre con una función, así que entre ciclos el cable NO
	// vuelve a nil: se queda apuntando al gateway del ciclo ANTERIOR durante todo el backoff de
	// reconexión (hasta 60 s). La guarda `fn == nil` de inyectarVia cubre por tanto el ARRANQUE, no la
	// reconexión, y quien cubre la reconexión es otra cosa: el centinela ErrSinEscuchaViva que devuelve
	// el gateway sin Listener publicado, traducido a ErrInyectorNoCableado en Manager.InyectarEntrante
	// (inyector.go). Creerse lo contrario es exactamente el agujero por el que una tanda lanzada en
	// backoff contestaba 200 con `inyectados: 0` en vez de 409.
	//
	// El bool que devuelve es el ACUSE del camino entrante (si la inyección fue efectivamente admitida y
	// anotada, o si el propio handler la descartó por sus filtros normales), no un «hubo error»: el error va
	// aparte. Un false SIN error es un resultado legítimo y significativo —el camino real dijo que no—, y
	// aplastarlo contra el error perdería exactamente la señal que la medición busca.
	liveInyectar func(ctx context.Context, p app.InyeccionEntrante) (bool, error)
}

// setLiveSender publica el emisor por cliente vivo de ESTE ciclo de escucha (acepta nil, pero ningún
// camino de producción lo llama así: el cable se re-apunta, no se limpia). Lo
// invoca el factory del listener en cada (re)conexión, apuntando al gateway recién creado.
func (s *liveSession) setLiveSender(fn func(ctx context.Context, commandID, to, text string) error) {
	s.mu.Lock()
	s.liveSend = fn
	s.mu.Unlock()
}

// sendVia despacha por el cliente vivo ACTUAL de la sesión (indirección estable que el multiplex
// registra una vez), propagando el command_id para la correlación del acuse (Plan 013 §10.E). Si no hay
// ciclo de escucha activo (liveSend nil), devuelve ErrNoLiveSender.
func (s *liveSession) sendVia(ctx context.Context, commandID, to, text string) error {
	s.mu.Lock()
	fn := s.liveSend
	s.mu.Unlock()
	if fn == nil {
		return ErrNoLiveSender
	}
	return fn(ctx, commandID, to, text)
}

// setLiveMediaSender publica el emisor de ARCHIVOS por cliente vivo de ESTE ciclo de
// escucha (Plan 017 §7). Hermano de setLiveSender; lo invoca el factory del listener en cada (re)conexión.
func (s *liveSession) setLiveMediaSender(fn func(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) error) {
	s.mu.Lock()
	s.liveMediaSend = fn
	s.mu.Unlock()
}

// sendViaMedia despacha un ARCHIVO por el cliente vivo ACTUAL de la sesión (indirección estable que el
// multiplex registra una vez), propagando el command_id para la correlación del acuse (Plan 013 §10.E). Si
// no hay ciclo de escucha activo (liveMediaSend nil), devuelve ErrNoLiveSender.
func (s *liveSession) sendViaMedia(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) error {
	s.mu.Lock()
	fn := s.liveMediaSend
	s.mu.Unlock()
	if fn == nil {
		return ErrNoLiveSender
	}
	return fn(ctx, commandID, to, presignedURL, filename, mime, kind, caption)
}

// setLiveInyector publica el inyector de entrantes sintéticos de ESTE ciclo de escucha (MP-10 Parte A).
// Hermano de setLiveSender/setLiveMediaSender; lo invoca el factory del listener en cada (re)conexión,
// apuntando al gateway recién creado.
//
// Acepta nil por simetría con sus hermanos, pero NINGÚN camino de producción lo llama así: el cable se
// re-apunta, no se limpia. Ver el doc del campo liveInyectar para lo que eso implica.
func (s *liveSession) setLiveInyector(fn func(ctx context.Context, p app.InyeccionEntrante) (bool, error)) {
	s.mu.Lock()
	s.liveInyectar = fn
	s.mu.Unlock()
}

// inyectarVia mete un entrante SINTÉTICO por el camino real del handler del cliente vivo ACTUAL de la
// sesión (MP-10 Parte A). Calcado de sendVia, y con su misma propiedad crítica: la función se LEE bajo el
// candado y se LLAMA fuera de él. No es un detalle de estilo — la llamada recorre el handler entrante
// entero, que serializa metadatos y ESCRIBE una fila cifrada en SQLite; sostener `s.mu` durante ese I/O
// congelaría a todo el que lee la salud o el despachador de esta sesión.
//
// Si no hay ciclo de escucha activo (liveInyectar nil), devuelve ErrInyectorNoCableado, que es un error
// DISTINTO del de sus hermanos (ErrNoLiveSender) a propósito: aquí no hay ningún envío que reintentar, hay
// una medición que aún no puede empezar.
func (s *liveSession) inyectarVia(ctx context.Context, p app.InyeccionEntrante) (bool, error) {
	s.mu.Lock()
	fn := s.liveInyectar
	s.mu.Unlock()
	if fn == nil {
		return false, ErrInyectorNoCableado
	}
	return fn(ctx, p)
}

// arm prepara la sesión para su goroutine listener bajo lock: guarda su cancel (apagado ordenado /
// borrado quirúrgico) y abre el canal done que esa goroutine cerrará al retornar. Lo invoca
// startListener justo antes de lanzar la goroutine; Stop usa cancel y Unlink espera done.
func (s *liveSession) arm(cancel context.CancelFunc) {
	s.mu.Lock()
	s.cancel = cancel
	s.done = make(chan struct{})
	s.mu.Unlock()
}

// signalDone cierra el canal done (la goroutine listener ya retornó). Idempotente solo dentro de una
// goroutine (cada listener lo cierra una vez al salir, vía defer). Seguro sin lock: la referencia a
// done quedó publicada por arm antes del `go` (happens-before), y el cierre no compite con escrituras.
func (s *liveSession) signalDone() {
	if s.done != nil {
		close(s.done)
	}
}

// waitDone bloquea hasta que la goroutine listener de ESTA sesión haya retornado (done cerrado). Si la
// sesión se registró SIN escucha (done nil), retorna de inmediato. Lee la referencia bajo lock para no
// competir con arm. Es la pieza que permite a Unlink unir SOLO esta goroutine, no el WaitGroup global.
func (s *liveSession) waitDone() {
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done != nil {
		<-done
	}
}

// stop cancela la goroutine listener si está arrancada (idempotente). No espera: el WaitGroup del
// Manager hace el join. Marca la sesión como deteniéndose para reflejarlo en Health() de inmediato.
func (s *liveSession) stop() {
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// mark fija la salud de runtime (y opcionalmente la causa de caída) bajo lock.
func (s *liveSession) mark(h SessionHealth, cause error) {
	s.mu.Lock()
	s.health = h
	if h == HealthDegraded {
		s.lastErr = cause
	}
	if h == HealthListening {
		s.lastErr = nil
	}
	s.mu.Unlock()
}

// snapshot devuelve la salud y la última causa de caída bajo lock (lectura para Health()/tests).
func (s *liveSession) snapshot() (SessionHealth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.health, s.lastErr
}

// setDespachador publica el despachador de esta sesión bajo lock (Plan 051 Ola 4 · T4.0). Lo invoca
// startDespachador ANTES de lanzar la goroutine del bucle: así los contadores son legibles desde el primer
// instante, incluso si el bucle muere en su primera vuelta.
func (s *liveSession) setDespachador(d *despachador.Despachador) {
	s.mu.Lock()
	s.despachador = d
	s.mu.Unlock()
}

// getDespachador lee el despachador de esta sesión bajo lock. nil si la sesión no llegó a arrancar uno.
func (s *liveSession) getDespachador() *despachador.Despachador {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.despachador
}
