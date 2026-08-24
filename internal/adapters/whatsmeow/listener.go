// Package whatsmeow — listener de RECEPCIÓN 24/7 (RF-5/RF-6, design §5). Código NUEVO (no existe en
// EduGo, que deshabilitó la escucha): se construye desde cero sobre client.AddEventHandler.
//
// El Listener registra UN handler en el cliente y ENRUTA por tipo de evento:
//   - *events.Message      -> ANOTA el entrante en la COLA DURABLE y retorna. NO entrega a nadie: quien
//     entrega al cable es el DESPACHADOR de la sesión (internal/app/despachador), que corre en su propia
//     goroutine (Plan 051 Ola 3 · T3.0, INV-051.2). El hilo de whatsmeow queda libre en cuanto el INSERT
//     cifrado termina. Y devuelve el PERMISO DE ACUSE (ver abajo).
//   - *events.Connected    -> marca estado conectado y RESETEA el backoff.
//   - *events.Disconnected -> marca estado desconectado y AVANZA el backoff (whatsmeow auto-reconecta).
//   - *events.LoggedOut    -> marca la sesión CAÍDA (no se re-empareja automáticamente).
//   - *events.OfflineSyncPreview/*events.OfflineSyncCompleted -> abren y cierran el corchete de la ráfaga
//     offline SOLO para contar y reconciliar (ADR-0037 §4). NO deciden descartes: quien decide qué entra
//     es la VENTANA TEMPORAL de onMessage, per-evento (ver inbound_window.go).
//
// La lógica de enrutado/mapeo vive en handleEvent(ctx, evt any), TESTEABLE con eventos sintéticos sin
// un *whatsmeow.Client real. Register() solo cablea handleEvent al AddEventHandler real (no se cubre
// en tests: requiere socket/red, por diseño; lo que sí se custodia es la FORMA de ese cableado, ver
// listener_acuse_test.go).
//
// 🔴 EL VALOR QUE DEVUELVE EL HANDLER ES EL PERMISO DE ACUSE (Plan 051 · T1.13). El listener se registra
// con client.AddEventHandlerWithSuccessStatus, NO con AddEventHandler, y eso no es un detalle de firma:
// whatsmeow acusa el mensaje a WhatsApp DESPUÉS de correr los handlers (message.go:431 los invoca, :467
// acusa) y se salta el acuse si alguno devuelve false (message.go:437, sobre el dispatchEvent síncrono de
// client.go:918-933). Con el AddEventHandler pelado el envoltorio devuelve true SIEMPRE (client.go:763-768),
// así que hasta el 2026-08-18 un INSERT fallido se acusaba igual: WhatsApp daba el mensaje por entregado,
// no lo reenviaba nunca y se perdía EN SILENCIO, en un plan cuya promesa entera es «ni un mensaje perdido».
//
// Devolver false es, literalmente, PEDIR A WHATSAPP QUE REENVÍE. Por eso lo devuelve UNA sola situación —el
// entrante que no dejó fila por un fallo de escritura— y todo lo demás devuelve true, incluidos los
// descartes deliberados: reclamar el reenvío de algo que hemos decidido no procesar es un bucle, porque lo
// reenviado vuelve idéntico y se vuelve a descartar. El veredicto camino a camino está en onMessage.
package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/latencia"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-shared/logger"
)

// ConnState es el estado de conexión observado por el Listener a partir de los eventos de whatsmeow.
type ConnState int

const (
	// StateDisconnected: el socket no está conectado (estado inicial y tras *events.Disconnected).
	StateDisconnected ConnState = iota
	// StateConnected: socket conectado y autenticado (tras *events.Connected).
	StateConnected
	// StateLoggedOut: la sesión fue cerrada por WhatsApp (tras *events.LoggedOut); requiere re-pairing.
	StateLoggedOut
)

// Listener enruta los eventos de whatsmeow hacia la COLA DURABLE (entrantes) y hacia los hooks de
// conexión/acuse, y lleva el estado de conexión y la política de backoff. Es seguro para uso concurrente
// (whatsmeow invoca el handler desde sus goroutines): el estado se protege con mu.
//
// 🔴 EL LISTENER YA NO TIENE SINK (Plan 051 Ola 3 · T3.0). Hasta esta ola guardaba un app.InboundSink y
// entregaba cada entrante en línea, en el hilo de whatsmeow. Ese campo se retiró ENTERO —no se dejó nil—
// porque un puntero al cable colgando de esta estructura es una invitación a volver a entregar desde
// aquí, y eso es exactamente lo que INV-051.2 prohíbe: la entrega es del despachador.
type Listener struct {
	log     logger.Logger
	backoff *Backoff

	mu    sync.Mutex
	state ConnState

	// onDisconnect, si está definido, se invoca tras avanzar el backoff en cada *events.Disconnected
	// con el delay calculado. En el spike es nil (whatsmeow auto-reconecta); se inyecta en tests para
	// verificar el disparo de la política de reconexión, y queda como costura para una reconexión
	// manual en Fase 1.
	onDisconnect func(attempt int, delay time.Duration)

	// onConnect, si está definido, se invoca tras marcar el estado conectado en cada *events.Connected.
	// La escucha lo usa para anunciar presencia (SendPresence available, §10.D) sobre el cliente vivo,
	// de modo que WhatsApp propague los acuses de entrega/lectura. nil = no hace nada (tests que no lo
	// ejercitan). Se re-dispara en cada reconexión (re-anunciar presencia es lo correcto).
	onConnect func()

	// onReceipt, si está definido, recibe cada ACUSE (delivered/read) ya mapeado a domain.ReceiptEvent.
	// Es la costura de SALIDA del acuse (el análogo del InboundSink de los entrantes): el CloudLink la
	// cablea en T2 para subir el estado a la nube. nil = se ignora sin romper la escucha. No lleva PII.
	onReceipt func(domain.ReceiptEvent)

	// onLoggedOut, si está definido, se invoca al recibir *events.LoggedOut (WhatsApp cerró el device),
	// TRAS marcar el estado y loguear (Plan 020 T3). El CloudLink lo cablea para propagar el estado ZOMBIE
	// a la nube mientras el stream aún vive. nil = solo se loguea (comportamiento previo). Corre FUERA del
	// lock. No lleva PII. (Sufijo Hook para no colisionar con el método onLoggedOut.)
	onLoggedOutHook func()

	// reporter registra la prueba de vida del socket y la edad del último entrante en el registro de salud
	// por sesión (Plan 031 T6): connected en *events.Connected, connecting en *events.Disconnected (whatsmeow
	// reintenta), dead en *events.LoggedOut, y el instante de cada *events.Message. nil ⇒ no se reporta (el
	// registro es opcional; el factory del sessionmgr lo liga a la sesión). No lleva PII: solo estados/tiempos.
	reporter health.SessionReporter

	// brackets OBSERVA los corchetes de la ráfaga de esta sesión y lleva los contadores de la puerta.
	// Solo observabilidad y reconciliación (ADR-0037 §4): NO decide ni un descarte. Nunca nil.
	brackets *bracketObserver

	// connectSeal devuelve el INICIO DE LA CONEXIÓN vigente, leído FRESCO en cada llamada y jamás cacheado.
	// Lo cablea Register a partir del cliente vivo (Client.LastSuccessfulConnect). nil ⇒ no hay sello y
	// resolveThreshold cae a `now`, que es el respaldo estricto. En tests se inyecta un sello controlado.
	connectSeal func() time.Time

	// margin es el margen de la ventana temporal (ADR-0037): se descarta lo anterior a `sello − margin`.
	// Default defaultConnectMargin; configurable por Edge (WAPP_AGENT_INBOUND_MARGIN_SECONDS).
	margin time.Duration

	// cola es la COLA DURABLE de entrantes (Plan 051 Ola 1) y, desde T3.0, el ÚNICO destino del entrante:
	// el mesonero anota el mensaje en disco (cifrado con la DEK de la sesión) y retorna; el despachador de
	// la sesión lo lee y lo entrega al cable.
	//
	// 🔴 nil ⇒ LOS ENTRANTES DE ESTA SESIÓN SE PIERDEN. Hasta la Ola 3 el nil era una degradación benigna
	// (quedaba el `sink.Deliver` inline); retirado ese camino, un nil aquí es un agujero negro. Por eso
	// NewListener lo GRITA al construir en vez de arrancar callado.
	//
	// EN PRODUCCIÓN NO PUEDE SER nil desde el 2026-08-17: el daemon no arranca si `cola_entrantes.db` no se
	// puede abrir, migrar o construir (internal/infra/daemon). Lo que queda aquí es la red para los
	// cableados que no vienen del daemon. Ver el bloque de «SIN COLA» de NewListener.
	cola app.ColaEntrantes

	// sessionID etiqueta las filas de la cola. Va EN CLARO en disco a propósito (app/cola.go): es la
	// clave de enrutado y la que ELIGE la DEK con que el adaptador sella Texto y Meta. El Listener NO lo
	// conoce por sí mismo (el patrón vigente es que el sink por-sesión etiquete el evento aguas abajo,
	// sessionmgr/listen.go), así que se INYECTA con WithSessionID desde quien sí lo sabe. Vacío + cola
	// cableada = fila sin sesión: el adaptador no podría elegir DEK, por eso van juntas en el cableado y
	// por eso NewListener desactiva la cola (ruidosamente) si falta esta.
	sessionID string

	// latencia es el CRONÓMETRO del handler de entrantes (Plan 051 Ola 3 · T3.13). Es lo único que hace
	// MEDIBLE el criterio de cierre de la ola —«handler < 50 ms p99», INV-051.2—, que hasta esta tarea
	// existía sobre el papel y no tenía instrumento.
	//
	// nil ⇒ NO SE MIDE Y NADA MÁS CAMBIA. Todos los métodos del histograma son nil-safe a propósito: sin
	// WithLatencia el comportamiento del listener es idéntico byte a byte al de antes de T3.13, porque un
	// instrumento de medida no puede alterar lo que mide ni, mucho menos, tumbar la escucha.
	//
	// 🔴 ES UN PUNTERO A TIPO CONCRETO, NO UNA INTERFAZ, Y ESO ES DELIBERADO. Una interfaz colgando aquí
	// sería la puerta por la que mañana alguien engancha un exportador REMOTO en el camino caliente —
	// exactamente lo que INV-051.2 prohíbe y lo que vigila el barrido de listener_camino_caliente_test.go
	// (que este campo pasa por construcción: no cumple app.InboundSink, no expone Classify y no es un func
	// que reciba domain.InboundEvent). Un *latencia.Histograma no puede hablar por la red.
	//
	// 🔴 INV-051.1: aquí no entra nada del contenido. El histograma solo ve duraciones.
	latencia *latencia.Histograma

	// sesionPasiva dice si la sesión que se le pase está marcada como PASIVA en la config de perfiles que
	// la nube empuja (kind:"filters", Plan 046 · Ola 2 · T2.2 / ADR-0027). Con `true`, el entrante se
	// descarta EN LA PUERTA: no se encola, no se persiste y no se entrega (REQ-07, «nada local»).
	//
	// Se pide un predicado y NO un bool porque el perfil cambia EN CALIENTE: la consola de la nube marca
	// una sesión como pasiva —o la devuelve a activa— y el efecto tiene que verse en el mensaje siguiente,
	// sin reiniciar el Edge. Un bool capturado al construir el Listener congelaría la foto del arranque.
	//
	// RECIBE EL session_id EN VEZ DE CERRARSE SOBRE ÉL porque el consultor es COMPARTIDO por todas las
	// sesiones del Edge (un solo mapa por tenant, igual que el cronómetro): una closure por sesión
	// obligaría a construir N predicados y a que el cableado no se equivocara de sesión en ninguno. El
	// Listener ya sabe cuál es la suya (l.sessionID).
	//
	// 🔴 nil ⇒ NINGUNA SESIÓN ES PASIVA (fail-open, D-046.2), y la asimetría es deliberada: de los dos
	// fallos posibles, un cableado incompleto que cayera hacia «pasiva»
	// dejaría al Edge SORDO —el cliente escribe y no pasa nada, sin un solo error en el log— mientras que
	// caer hacia «activa» solo sube tráfico que la nube ya sabe ignorar (`reactiveBlocked`, D-046.7). Se
	// falla hacia el lado que no pierde mensajes.
	sesionPasiva func(sessionID string) bool
}

// ListenerOption configura el Listener sin romper la firma de NewListener (variádica), igual que
// app.ListenOption hace con el caso de uso Listen. Sin opciones, los defaults son los de siempre.
type ListenerOption func(*Listener)

// WithCola cablea la cola durable de entrantes (Plan 051 Ola 1). nil se IGNORA, pero eso NO es un
// fallback: sin cola el listener no tiene dónde dejar el entrante y la sesión queda sorda (NewListener lo
// grita, y desde el Plan 051 O3 el daemon ni siquiera arranca en esa situación). La opción viaja SIEMPRE
// con WithSessionID.
func WithCola(c app.ColaEntrantes) ListenerOption {
	return func(l *Listener) {
		if c != nil {
			l.cola = c
		}
	}
}

// WithSessionID inyecta el identificador de sesión con que se etiquetan las filas de la cola (y con el
// que el adaptador elige la DEK). Vacío se ignora. Va SIEMPRE junto a WithCola.
func WithSessionID(id string) ListenerOption {
	return func(l *Listener) {
		if id != "" {
			l.sessionID = id
		}
	}
}

// WithSesionPasiva inyecta el CONSULTOR DE PERFILES de sesión (Plan 046 · Ola 2 · T2.2): un predicado que
// se consulta por mensaje —pasándole el session_id de ESTA sesión— para decidir si el entrante se corta en
// la puerta por pertenecer a una sesión PASIVA (REQ-07).
//
// Se pide un `func(string) bool` y NO un bool, y el porqué está en el campo sesionPasiva: el perfil cambia
// en caliente. nil se ignora y el Listener queda en su default FAIL-OPEN (nadie es pasiva), que es el
// comportamiento anterior a esta tarea byte a byte.
func WithSesionPasiva(fn func(sessionID string) bool) ListenerOption {
	return func(l *Listener) {
		if fn != nil {
			l.sesionPasiva = fn
		}
	}
}

// WithLatencia cablea el CRONÓMETRO del handler de entrantes (Plan 051 Ola 3 · T3.13): el histograma
// COMPARTIDO por todas las sesiones del Edge donde se acumula cuánto tarda onMessage, que es lo que hace
// medible «handler < 50 ms p99» (INV-051.2).
//
// Es compartido —no uno por sesión— porque el criterio es del EDGE: un p99 por sesión con 3 mensajes cada
// una no responde la pregunta, y sumarlos después exigiría fusionar histogramas para nada.
//
// nil se IGNORA y el listener queda sin medir, que es el comportamiento anterior a T3.13 exactamente.
func WithLatencia(h *latencia.Histograma) ListenerOption {
	return func(l *Listener) {
		if h != nil {
			l.latencia = h
			// 🔴 Y CON ÉL VIAJAN LOS CONTADORES DE LA PUERTA (T1.13). El mismo instrumento compartido lleva
			// las dos cosas —la rejilla de duraciones y las dos cardinalidades de degradación— para que
			// publicar T1.13 no necesitara ni un puerto, ni una opción, ni un setter, ni una línea del
			// cableado del daemon. La consecuencia buena: esto HEREDA la custodia del cronómetro, que ya
			// tiene test de que llega hasta el Listener que nace de listenerOpts().
			//
			// Se cablea sobre el observador de corchetes y no sobre el Listener porque es él quien cuenta,
			// y así el acumulado por sesión y el del Edge se escriben desde el mismo sitio.
			l.brackets.puerta = h.Puerta()
		}
	}
}

// WithConnectMargin cablea el MARGEN de la ventana temporal de ingesta (ADR-0037) que el Edge trae
// configurado (WAPP_AGENT_INBOUND_MARGIN_SECONDS) para ESTA sesión. <=0 se ignora y manda el default
// (defaultConnectMargin): ver el porqué de esa asimetría en SetConnectMargin, a quien delega — el guardián
// del valor no positivo vive AHÍ y solo ahí, para que un solo test lo pueda custodiar.
//
// 🔴 EXISTE COMO OPCIÓN, Y NO SOLO COMO SETTER, POR UN MOTIVO DE PRUEBA. Hasta el 2026-08-18 el margen se
// aplicaba en serve() —que exige un device pareado y un socket vivo—, así que el cable entre
// SetInboundMargin y el Listener no lo ejercitaba ningún test: borrarlo dejaba los cuatro gates en VERDE y
// en campo devolvía la ventana a su default sin que nadie lo notara hasta la siguiente microcaída. Pasando
// por listenerOpts() el cable queda interrogable, igual que los de la cola, el clasificador y el cronómetro.
func WithConnectMargin(d time.Duration) ListenerOption {
	return func(l *Listener) { l.SetConnectMargin(d) }
}

// SetHealthReporter liga el listener al registro de salud de SU sesión (Plan 031 T6). Se llama ANTES de
// Register (al construir el ciclo de escucha). nil ⇒ no se reporta salud. No es secreto ni PII.
//
// 🔴 CABLEA DOS CAMPOS CON LA MISMA INSTANCIA (Plan 046 · T2.3), Y ESA ES LA ÚNICA FORMA DE QUE NO DIVERJAN.
// El listener lo usa para sellar la prueba de vida (`MarkInbound`, en onMessage); el observador de corchetes
// lo usa para publicar el contador de descartes por perfil pasivo desde el mismo sitio donde lo incrementa
// (ver countPassiveDrop). Dos setters separados —o un segundo Set que alguien olvidara llamar— dejarían el
// contador de una sesión reportándose en el registro de OTRA, o en ninguno. Es una línea; que sean dos
// asignaciones aquí es preferible a que sean dos llamadas en el cableado.
// ⚠️ LA SEGUNDA ASIGNACIÓN VA POR `setSalud` Y NO A PELO: el observador lee ese campo desde el hilo de
// whatsmeow (countPassiveDrop) y aquí se escribe desde la goroutine que monta el ciclo de escucha. Hoy hay
// happens-before —esto corre antes de Register— pero es justo el patrón que `-race` delata bajo el daemon
// real; el candado del observador ya se paga en esa ruta, así que cerrarlo no cuesta nada. `l.reporter` no
// lo necesita: lo lee el mismo hilo de whatsmeow que ya está ordenado por el registro del handler.
func (l *Listener) SetHealthReporter(r health.SessionReporter) {
	l.reporter = r
	l.brackets.setSalud(r)
}

// NewListener construye el listener con el logger dado, el backoff por defecto del spike y el observador
// de corchetes. El margen de la ventana arranca en su default; SetConnectMargin lo ajusta.
//
// YA NO RECIBE SINK (Plan 051 Ola 3 · T3.0): el listener no entrega. Sin WithCola no tiene a dónde dejar
// el entrante, y eso ES un fallo de cableado, no un modo de operación — ver el grito de abajo.
func NewListener(log logger.Logger, opts ...ListenerOption) *Listener {
	l := &Listener{
		log:      log,
		backoff:  DefaultBackoff(),
		state:    StateDisconnected,
		brackets: newBracketObserver(log),
		margin:   defaultConnectMargin,
	}
	for _, o := range opts {
		o(l)
	}
	// CABLEADO INCOMPLETO: WithCola SIN WithSessionID. Sin sesión, cada fila se escribiría con
	// session_id="" y el adaptador pediría la DEK de la cadena vacía: o falla en cada mensaje, o —peor—
	// mezcla el material de todas las sesiones bajo una llave que no es de nadie. Ninguna de las dos se
	// puede descubrir en producción por casualidad, así que NO se falla en silencio: se GRITA en el log y
	// se desactiva la cola. Tumbar el proceso tampoco es opción: un error de cableado no puede dejar sin
	// escucha a las sesiones que sí están bien.
	if l.cola != nil && l.sessionID == "" {
		l.log.Error("listener: cableado incompleto — WithCola sin WithSessionID; la cola durable queda DESACTIVADA para esta sesión. Corrige el arranque: ambas opciones van juntas")
		l.cola = nil
	}
	// SIN COLA ⇒ SESIÓN SORDA, Y HAY QUE DECIRLO. Un listener sin cola no anota, no entrega y no falla:
	// cada mensaje que llegue se evapora con el socket conectado y sin un solo error. El síntoma en campo
	// —«el cliente escribe y no pasa nada»— no apunta a su causa por ninguna parte; esta línea es lo único
	// que la delata.
	//
	// 🔴 YA NO ES ALCANZABLE DESDE PRODUCCIÓN (Plan 051 O3, 2026-08-17). Esta comprobación nació como
	// PALIATIVO de un agujero real: T3.0 retiró el `sink.Deliver` inline y la apertura de
	// `cola_entrantes.db` en el daemon seguía sin ser fatal, así que un fichero que no abría dejaba
	// listeners sordos de verdad. Ese agujero se cerró en el sitio correcto: el daemon NO ARRANCA si la
	// cola no se puede abrir, migrar o construir. Un daemon vivo tiene cola por construcción.
	//
	// SE CONSERVA IGUAL, y no por inercia: `NewListener` es público y lo llaman los tests y cualquier
	// cableado que no venga del daemon. Si esta línea aparece en un log de campo, el diagnóstico ya no es
	// «revisa el fichero de la cola» sino «alguien construyó un listener por fuera del arranque»: es un bug
	// de cableado, no un estado degradado. Se grita y se SIGUE, porque tumbar el proceso desde un
	// constructor dejaría sin escucha a las sesiones que sí están bien.
	if l.cola == nil {
		l.log.Error("listener: sesión SIN cola durable de entrantes; sus mensajes NO se anotan ni se despachan. El daemon no arranca sin cola desde el Plan 051 O3, así que esto es un cableado hecho por fuera de `agent serve`")
	}
	return l
}

// SetConnectMargin fija el margen de la ventana temporal (ADR-0037). Se llama ANTES de Register (al
// construir el ciclo de escucha). Un valor <=0 se ignora: el margen es lo que absorbe el desfase de reloj
// que no podemos medir, así que dejarlo en cero descartaría tráfico vivo.
func (l *Listener) SetConnectMargin(d time.Duration) {
	if d > 0 {
		l.margin = d
	}
}

// SetConnectSeal inyecta la fuente del inicio de conexión. En producción la cablea Register desde el
// cliente vivo; existe como costura para probar el criterio con un sello controlado.
func (l *Listener) SetConnectSeal(fn func() time.Time) { l.connectSeal = fn }

// InboundStats devuelve el acumulado de la puerta de entrada de esta sesión (ADR-0037 §4): corchetes
// reconciliados, descartados por la ventana, ecos propios, admitidos sin hora, descartados por PERFIL
// PASIVO (Plan 046 · T2.2) y las dos degradaciones del encolado durable (Plan 051 · INV-051.3). Son
// cardinalidades, sin PII.
//
// ⚠️ SIGUE SIN TENER LLAMANTES DE PRODUCCIÓN, Y YA NO IMPORTA. Este método fue el caso de estudio de T1.13
// («once llamantes y los once eran tests»); lo que se arregló no fue darle un llamante, sino que los
// contadores se publicaran desde donde se incrementan. Hoy los descartes por perfil pasivo salen en el
// bloque del latido (`descartes_perfil_pasivo`) y en `GET /v1/health` (`dropped_passive`) sin pasar por
// aquí. Esta función es la vista POR SESIÓN que usan los tests del corte; añadirle un llamante de
// producción sería reabrir la discusión, no cerrarla.
func (l *Listener) InboundStats() InboundStats { return l.brackets.snapshot() }

// State devuelve el estado de conexión observado (para observabilidad/tests).
func (l *Listener) State() ConnState {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

// Register cablea handleEvent al despachador de eventos REAL del cliente whatsmeow. El ctx (vida de la
// sesión Listen) se propaga a cada anotación en la cola. NO se cubre en tests (requiere un client real);
// su FORMA sí se custodia, porque es donde se decide si el acuse depende de nosotros: ver
// TestRegister_SeRegistraConElHandlerQueGobiernaElAcuse.
//
// 🔴 SE REGISTRA CON AddEventHandlerWithSuccessStatus, Y NO ES INTERCAMBIABLE CON AddEventHandler
// (T1.13). El de siempre envuelve el handler en uno que devuelve true SIEMPRE (client.go:763-768), y ese
// true es lo que autoriza a whatsmeow a acusar el mensaje a WhatsApp. Con él, un entrante que NO se pudo
// escribir en la cola se acusaba igual: WhatsApp lo daba por entregado, dejaba de reenviarlo y el mensaje
// desaparecía sin un solo síntoma para el cliente final. Con esta variante, el false que devuelve
// handleEvent llega hasta message.go:437 y whatsmeow retorna ANTES del acuse ⇒ WhatsApp reenvía.
//
// Volver al AddEventHandler pelado deja los cuatro gates en VERDE y reabre el agujero entero: la firma
// compila, el handler sigue corriendo y lo único que se pierde es el valor de retorno.
func (l *Listener) Register(ctx context.Context, client *wm.Client) uint32 {
	// Sello de la ventana temporal (ADR-0037): Client.LastSuccessfulConnect leído FRESCO en cada
	// evaluación, nunca cacheado. Se lee desde el handler de mensaje, que es exactamente donde la cadena
	// happens-before del handlerQueue ya lo ordena (ver la cabecera de inbound_window.go, y ahí también el
	// residual F2/F3 que esta lectura NO cubre y por el que resolveThreshold desconfía del valor).
	l.connectSeal = func() time.Time { return client.LastSuccessfulConnect }
	return client.AddEventHandlerWithSuccessStatus(func(evt any) bool {
		return l.handleEvent(ctx, evt)
	})
}

// handleEvent es el ENRUTADOR PURO (testeable): recibe un evento de whatsmeow y reacciona según su
// tipo. No abre sockets ni depende de un client; en tests se le pasan eventos sintéticos.
//
// DEVUELVE EL PERMISO DE ACUSE (T1.13): true = whatsmeow puede acusar. Solo el camino del ENTRANTE puede
// negarlo; el resto de los eventos devuelve true incondicionalmente, y no por simetría estética: un false
// CORTA la cadena de handlers en seco (dispatchEvent retorna en el primero que falla, client.go:929), así
// que negarlo en un *events.Receipt o un *events.Connected dejaría ciegos a los demás handlers del cliente
// sin ganar nada a cambio — esos eventos no llevan acuse por esta vía.
//
// 🔴 EL recover DE AQUÍ ES LO QUE HACE QUE LA GARANTÍA NO DEPENDA DE QUE NADIE SE DESPISTE. Sin él, la
// promesa «no se acusa lo que no se escribió» se sostenía sobre que el recover de enqueueCola siguiera
// cubriendo TODO el camino de escritura: un pánico un centímetro más arriba —el reporter de salud, la
// lectura del sello, cualquier hook de sesión— se escapaba del handler y lo recogía whatsmeow, y ahí pasan
// dos cosas malas a la vez:
//
//  1. SE ACUSA EL MENSAJE PERDIDO. dispatchEvent recupera el pánico en un defer que NO toca su resultado
//     con nombre (client.go:918-933), así que `handlerFailed` se queda en su valor CERO —false— y
//     whatsmeow sigue hasta el acuse. El modo de fallo exacto que T1.13 vino a cerrar, por la puerta de
//     atrás.
//  2. SE FILTRA CONTENIDO AL LOG. Antes de eso, whatsmeow imprime el valor recuperado con %v y la pila
//     entera (`Event handler panicked while handling a %T: %v`), y ese valor puede arrastrar el argumento
//     que provocó el pánico — es decir, el TEXTO del mensaje. Por el puente waLog→slog eso aterriza en
//     NUESTRO log, que es justo lo que prohíbe INV-051.1 / ADR-0034 nivel 3.
//
// NO ES EL MISMO GUARDIÁN QUE EL DE enqueueCola, y la diferencia es la razón de que convivan: aquél SABE
// dónde ocurrió el pánico y por eso CUENTA (ColaEnqueuePanics, el desglose de INV-051.3) y anota el
// message_id; éste no sabe nada, no cuenta nada y solo garantiza. Quitar el de abajo deja el contador a
// cero con la garantía intacta; quitar éste deja el contador intacto y la garantía rota en todo lo que no
// sea el INSERT. Cada uno tiene su propia mutación en rojo, que es como debe ser.
//
// El false NO se asigna a mano, por lo mismo que en enqueueCola: al recuperar, el resultado con nombre
// queda en su valor cero, que ya es la respuesta. Una asignación explícita sería una línea borrable sin
// efecto observable.
func (l *Listener) handleEvent(ctx context.Context, evt any) (acusar bool) {
	defer func() {
		if r := recover(); r != nil {
			// INV-051.1: NI el valor recuperado NI el evento se imprimen — los dos pueden arrastrar el
			// contenido del mensaje. Solo el TIPO del evento, que es lo que permite orientar el diagnóstico
			// sin transcribir nada.
			l.log.Error("listener: panic recuperado en el handler de eventos; la escucha SIGUE y el entrante NO se acusa (WhatsApp lo reenviará)",
				"evento", fmt.Sprintf("%T", evt))
		}
	}()
	switch e := evt.(type) {
	case *events.Message:
		return l.onMessage(ctx, e)
	case *events.Connected:
		l.onConnected()
	case *events.Disconnected:
		l.onDisconnected()
	case *events.LoggedOut:
		l.onLoggedOut(e)
	case *events.Receipt:
		l.onReceiptEvent(e)
	case *events.OfflineSyncPreview:
		// SOLO OBSERVABILIDAD (ADR-0037 §4): abre el corchete para poder reconciliar «el servidor anunció
		// N, la ventana descartó M». NO decide descartes — eso es cosa de la ventana temporal, per-evento.
		l.brackets.arm(e)
	case *events.OfflineSyncCompleted:
		l.brackets.close(closeCompleted)
	default:
		// Otros eventos (presencia, history sync, …) no son del alcance actual.
	}
	return true
}

// onMessage decide si el entrante se ADMITE y, si lo hace, lo ANOTA EN LA COLA DURABLE y RETORNA. No
// entrega a nadie y no espera a nadie: quien entrega al cable es el despachador de la sesión.
//
// 🔴 INV-051.2 — ESTA FUNCIÓN CORRE EN EL HILO DE WHATSMEOW Y DEBE SALIR RÁPIDO. Lo único que hace con el
// mensaje es un INSERT cifrado local. NO hay aquí ninguna llamada al LLM, ninguna espera sobre el cajero y
// ninguna entrega al cable: cada una de esas tres cosas ataría el bucle de eventos de whatsmeow a la
// latencia de un tercero (Ollama, el worker, la nube), que es justo lo que la Ola 3 vino a romper.
//
// Los filtros de la puerta, en este orden:
//  1. ECO PROPIO (IsFromMe): lo mandamos nosotros; no es una conversación entrante y no tiene por qué subir.
//  2. PERFIL PASIVO (Plan 046 · T2.2, REQ-07): la sesión está marcada como PASIVA en la config empujada
//     por la nube ⇒ se descarta SIN dejar nada local. Va ANTES de «SIN HORA UTILIZABLE» —y no en el
//     «3.5» que el plan proponía— porque ese paso también encola: ver el bloque del filtro.
//  3. SIN HORA UTILIZABLE: se ADMITE explícitamente (ver abajo).
//  4. VENTANA TEMPORAL (ADR-0037, criterio único): anterior a `inicioDeConexión − margen` ⇒ se descarta.
//  5. GRUPO (Plan 044 · Ola 1.5 · T1.5-3, REQ-36/D-044.30): el entrante viene de un grupo o lista de
//     difusión ⇒ se descarta SIN dejar fila, SIN entregar nada y CON acuse. Hasta esta tarea vivía en la
//     puerta de ELEGIBILIDAD (el switch de enqueueCola, el «paso 6») y allí dejaba fila con la marca
//     `no_elegible`; esa conducta queda DEROGADA. Es el ÚNICO filtro que domina los DOS caminos que
//     encolan —el normal y el del entrante sin hora—, y por eso está donde está: ver su bloque.
//
// ⚠️ EL ORDEN ES POR COSTE CRECIENTE (D-044.30, detalle 2) Y EL PASO 5 ES LA EXCEPCIÓN CONSCIENTE. Leer
// `e.Info.IsGroup` es un campo booleano ya materializado: cuesta MENOS que el paso 4, que llama a
// `connectSeal()`, pide la hora y compara. Por coste puro iría justo detrás del eco propio. Se deja en el 5
// porque lo fija el Plan 044 · T1.5-3, y porque el precio de la excepción es acotado y conocido: los
// entrantes de grupo de una ráfaga offline se cuentan como descarte de VENTANA y no como descarte de GRUPO
// (los dos son gratis y los dos acusan, así que el precio es de telemetría, no de conducta).
//
// Lo ADMITIDO sigue el orden del Plan 051: ventana → GRUPO → PUERTA DE ELEGIBILIDAD (sin texto / feature
// apagada / fastlane, ver el switch de enqueueCola) → INSERT cifrado en la cola durable → fin. El INSERT es
// lo que hace durable el mensaje ANTES de que WhatsApp reciba el acuse. La puerta de elegibilidad NO
// descarta nada: decide si la fila nace reclamable por el cajero o ya resuelta con su marca de omisión.
//
// El descarte es SILENCIOSO hacia el cliente final y CONTADO hacia nosotros: un descarte invisible es
// indistinguible de un bug. Se cuenta, no se transcribe (ADR-0034 nivel 3): ni texto ni teléfono al log.
//
// 🔴 EL VEREDICTO DEL ACUSE, CAMINO A CAMINO (T1.13). Lo que esta función devuelve NO es «hubo fila»: es
// «whatsmeow puede acusar este mensaje a WhatsApp». Los dos conceptos coinciden en el caso que importa y
// se separan en los descartes, y confundirlos tiene un precio caro en cada dirección — un true de más
// pierde el mensaje para siempre; un false de más le pide a WhatsApp que reenvíe algo que volveremos a
// descartar, es decir un bucle a ritmo de reconexión. El criterio es UNO: solo el FALLO DE ESCRITURA
// niega el acuse, porque solo él es un fallo NUESTRO y transitorio, y por tanto lo único que un reenvío
// puede arreglar.
//
//	ACUSA (true)  · ECO PROPIO (IsFromMe)         — es nuestro; no hay nada que persistir ni que reenviar.
//	ACUSA (true)  · PERFIL PASIVO (Plan 046)      — decisión deliberada y DETERMINISTA sobre config estable:
//	                                                el reenvío llegaría idéntico y se volvería a descartar.
//	                                                Negarlo convertiría el tráfico de la pasiva en una
//	                                                ráfaga perpetua de reofrecimientos.
//	ACUSA (true)  · FUERA DE VENTANA (ADR-0037)   — decisión deliberada y DETERMINISTA: el reenvío traería
//	                                                el MISMO Info.Timestamp, volvería a caer fuera y
//	                                                pediríamos otro reenvío. Bucle sin salida (REQ-051.5).
//	ACUSA (true)  · GRUPO (Plan 044 · T1.5-3)     — decisión deliberada y DETERMINISTA sobre el JID: el
//	                                                reenvío llegaría con el mismo `IsGroup`, se volvería a
//	                                                descartar y pediríamos otro reenvío. Negarlo convertiría
//	                                                el tráfico de cada grupo en una ráfaga perpetua de
//	                                                reofrecimientos (D-044.30, detalle 1).
//	ACUSA (true)  · SIN HORA UTILIZABLE           — encola: el acuse lo decide su INSERT, como el normal.
//	ACUSA (true)  · SIN TEXTO                     — ⚠️ NO ES UN DESCARTE: una imagen, un audio o un sticker
//	                                                se encolan y se entregan EXACTAMENTE IGUAL que un
//	                                                texto. Acusa porque el mensaje está en disco, no por
//	                                                indulgencia.
//	                                                🔴 ESTA LÍNEA ERAN TRES CASOS (SIN TEXTO / APAGADO /
//	                                                FASTLANE) hasta el Plan 044 · T1.6-5 (ADR-0045), y
//	                                                cuatro hasta T1.5-3 (GRUPO se fue arriba). Los tres
//	                                                dejaban fila naciendo `clasificado` con su marca de
//	                                                omisión; hoy nacen `nuevo` y sin sobre, como todo lo
//	                                                demás, así que ya no son casos aparte — sólo queda
//	                                                nombrado el que sigue sorprendiendo a quien lo lee.
//	NO ACUSA (false) · el INSERT falló, entró en
//	                   pánico, o no hay cola       — el mensaje NO está en ningún sitio. Acusarlo es
//	                                                perderlo en silencio; no acusarlo lo deja en manos de
//	                                                WhatsApp, que lo reenviará, y el segundo intento es
//	                                                IDEMPOTENTE por (session_id, wa_message_id).
func (l *Listener) onMessage(ctx context.Context, e *events.Message) bool {
	// ⏱️ CRONÓMETRO (T3.13). Envuelve la función ENTERA —no solo el Enqueue— porque lo que INV-051.2 acota
	// es cuánto tiempo del hilo de whatsmeow consume este handler, y eso incluye los filtros de la puerta.
	//
	// `camino` decide en qué SERIE cae la medida y se reasigna en cada salida temprana; el `defer` es lo que
	// garantiza que se anota pase lo que pase, incluido el recover de enqueueCola. Sale por `Encolado` por
	// defecto porque los caminos que encolan son dos (el normal y el del entrante sin hora — eran tres con
	// el del fastlane, que desde T1.6-5 ya no se distingue del normal) y los
	// que descartan son CUATRO (eco propio, perfil pasivo, fuera de ventana y —desde el Plan 044 · T1.5-3—
	// grupo): el default cubre el caso mayoritario —el mensaje que entra— y cada descarte lo dice
	// explícitamente.
	//
	// COSTE: una sola llamada extra al reloj (MarkInbound reusa `inicio`), más una búsqueda de 16
	// comparaciones y un atomic.Add. Ver el análisis en la cabecera de internal/app/latencia.
	inicio := time.Now()
	camino := latencia.Encolado
	defer func() { l.latencia.Observar(camino, time.Since(inicio)) }()

	// Prueba de vida (Plan 031 T6): sella el instante del último entrante. Se marca ANTES de filtrar y para
	// TODO mensaje —incluidos los descartados— porque es prueba de vida del SOCKET, no señal de negocio:
	// una ráfaga descartada sigue demostrando que el socket recibe. Comportamiento idéntico al previo.
	//
	// Reusa `inicio` en vez de pedir la hora otra vez: es el mismo instante con una resolución que a esta
	// escala no distingue nada, y ahorra la única llamada al reloj que el cronómetro habría añadido.
	if l.reporter != nil {
		l.reporter.MarkInbound(inicio)
	}

	// 1 · Eco propio. Antes de la Ola 1 este dato solo servía para no gastar el clasificador (lo miraba
	// `intent.Decorator.eligible`, hoy retirado); el evento subía igual a la nube. Descartarlo aquí es un
	// CAMBIO DE COMPORTAMIENTO deliberado y aprobado.
	if e.Info.IsFromMe {
		camino = latencia.Descartado // (T3.13) sale sin fila: su tiempo no es tiempo de encolar
		l.brackets.countSelfDrop()
		// SE ACUSA (T1.13): el mensaje lo mandamos nosotros. No hay nada que persistir, y pedir su reenvío
		// sería pedir que nos devuelvan nuestro propio eco una y otra vez.
		return true
	}

	// 1.5 · PERFIL DE SESIÓN — LA SESIÓN PASIVA NO RECIBE (Plan 046 · Ola 2 · T2.2, REQ-07/ADR-0027).
	//
	// 🔴 EL PLAN DECÍA «PASO 3.5» (entre la ventana y el enqueueCola) Y ESO NO BASTA — DESVIACIÓN DELIBERADA.
	// `enqueueCola` se alcanza desde DOS sitios, no uno: el camino normal de abajo Y el del entrante SIN HORA
	// UTILIZABLE (`Info.Timestamp.IsZero()`, el `t="0"` del paso 2), que ADMITE por precaución y encola
	// ANTES de que la ventana llegue a evaluarse. Un filtro colocado tras la ventana dejaría escribirse en
	// disco —cifrado, pero escrito— los entrantes de una sesión pasiva que llegaran con `t="0"`. Son raros;
	// REQ-07 no dice «casi nada local», dice NADA. Puesto aquí, el corte domina los dos caminos.
	//
	// POR QUÉ ESTE SITIO Y NO OTRO:
	//   - DESPUÉS de `MarkInbound` (arriba): eso es prueba de vida del SOCKET, no señal de negocio, y se
	//     marca para TODO mensaje incluidos los descartados. La salud de una sesión pasiva tiene que seguir
	//     latiendo (D-046.3: Heartbeat/SessionHealth SIEMPRE suben).
	//   - DESPUÉS del eco propio: así `DroppedSelf` conserva su significado exacto —«ecos nuestros»— en vez
	//     de perder los de las sesiones pasivas en otro contador.
	//   - ANTES de la rama del timestamp: el veredicto es DETERMINISTA y no depende de la hora, así que no
	//     tiene ninguna razón para ir detrás de algo que sí depende de ella.
	//
	// 🔴 SE ACUSA (true), por el MISMO razonamiento que la ventana del ADR-0037 (ver el paso 3, más abajo): el
	// descarte es deliberado y determinista sobre config ESTABLE, así que el reenvío llegaría idéntico y se
	// volvería a descartar. Negar el acuse convertiría el tráfico de una sesión pasiva —que puede ser todo
	// el de un número muy activo— en una ráfaga PERPETUA de reofrecimientos de WhatsApp.
	//
	// 🔴 Y SE MARCA `Descartado`: sin esta línea, el p99 del handler contaría estos descartes de microsegundos
	// como encolados y MEJORARÍA justo cuando el Edge más filtra (INV-051.2 se juzga contra la serie
	// `Encolado`).
	//
	// 🔴 EL PERFIL SE JUZGA UNA VEZ, AQUÍ. No se re-evalúa en el despachador ni en el cajero: una fila que ya
	// está en la cola entró cuando la sesión era ACTIVA y ya fue acusada a WhatsApp; filtrarla al salir sería
	// una pérdida silenciosa de un mensaje que el cliente ve con doble check.
	if l.sesionEsPasiva() {
		camino = latencia.Descartado // sale sin fila: su tiempo no es tiempo de encolar
		l.brackets.countPassiveDrop()
		// Nivel DEBUG y no Warn a propósito: esto no es una anomalía, es la configuración funcionando, y una
		// sesión pasiva con tráfico escribiría una línea por mensaje a ritmo de socket. ADR-0034 nivel 3: ni
		// texto ni teléfono; solo el identificador del mensaje, que es lo que permite correlacionar.
		//
		// 📌 EN CAMPO ESTA LÍNEA NO SE VE (el Edge corre en Info), y por eso NO es la línea del filtro: la que
		// se lee la emite `countPassiveDrop` (T2.3), en Info, THROTTLED a una por sesión y por ventana de
		// enfriamiento, con el ACUMULADO y sin un solo identificador de mensaje. Esta de aquí es la de
		// diagnóstico fino, para cuando alguien sube el nivel a Debug a propósito.
		l.log.Debug("listener: entrante descartado en la puerta — la sesión tiene perfil PASIVO (Plan 046, REQ-07): no se encola, no se persiste y no se entrega",
			"message_id", e.Info.ID)
		return true
	}

	// 2 · Sin hora utilizable ⇒ SE ADMITE, explícitamente y contado. No es teórico: GetUnixTime devuelve
	// un time.Time CERO y ok=true cuando el atributo `t` vale "0" (binary/attrs.go:116-123), sin registrar
	// error, así que el mensaje llega hasta aquí con Info.Timestamp en cero. (Si `t` falta del todo,
	// whatsmeow lo rechaza antes: UnixTime exige el atributo y parseMessageInfo corta en ag.OK().) Un cero
	// es anterior a CUALQUIER umbral, así que dejarlo caer por la comparación lo descartaría en silencio —
	// justo lo contrario de la asimetría del ADR: ante la duda, DEJAR PASAR.
	//
	// 🔴 EL SELLO DE LA FILA SE CALCULA AQUÍ Y SE USA ABAJO, y esa variable es lo que permite que el
	// filtro de GRUPO (paso 5) sea UNO SOLO. Hasta el Plan 044 · T1.5-3 los dos caminos que ADMITEN —el del
	// entrante SIN HORA UTILIZABLE y el normal— terminaban cada uno en su propio `return l.enqueueCola(...)`,
	// así que un filtro colocado entre la ventana y el encolado NO habría dominado al primero: un entrante de
	// grupo con `t="0"` se habría colado hasta la cola y, retirado ya el caso `grupo` de la puerta de
	// elegibilidad, habría nacido `nuevo` — o sea tráfico de grupo mandado al cajero y a la nube, justo lo
	// contrario de REQ-36. Con el sello en una variable los dos caminos convergen en UN encolado y el filtro
	// de grupo los cubre a los dos con una sola rama.
	//
	// ⚠️ CONSECUENCIA PARA LOS TESTS: la mutación M12 que cita listener_acuse_test.go —tirar el retorno del
	// INSERT en UNO de los dos `return l.enqueueCola`— deja de ser aplicable, porque solo queda uno. Lo que
	// aquella mutación custodiaba no se pierde: se custodia ahora con el caso `t="0"` del mismo test, que
	// sigue recorriendo un camino distinto hasta el encolado común.
	var tsCola int64

	if e.Info.Timestamp.IsZero() {
		l.brackets.countNoTimestamp()
		l.log.Warn("listener: entrante SIN hora utilizable; se admite por precaución (la ventana no puede juzgarlo)")
		// Este camino ENCOLA Y RETORNA, exactamente igual que el normal de abajo. Antes de T3.0 tenía además
		// su propio `sink.Deliver`; se retiró con el otro (decisión del 2026-08-17): los dos caminos encolan
		// desde T1.7, y el despachador drena la cola entera sin distinguir por qué entró cada fila. Dejar la
		// entrega SOLO aquí habría sido peor que dejarla en los dos: un duplicado intermitente, disparado por
		// una condición rarísima (`t="0"`), imposible de reproducir a voluntad.
		//
		// SELLO: aquí Info.Timestamp es CERO y NO se puede persistir tal cual — un 0 en la columna es epoch
		// 1970, y la poda por TTL de la cola se llevaría la fila en el primer barrido, o sea perder justo el
		// mensaje que acabamos de admitir por precaución. Se sella con la hora LOCAL de recepción, que es lo
		// único cierto que tenemos de él: no es su hora real, pero sitúa la fila en la ventana de retención.
		//
		// ACUSE (T1.13): lo decide su INSERT, exactamente igual que el camino normal. Este mensaje se ADMITE,
		// así que su durabilidad se exige como la de cualquier otro admitido.
		// 🔴 YA NO RETORNA AQUÍ (Plan 044 · T1.5-3): fija el sello y sigue hasta el filtro de GRUPO y el
		// encolado común de abajo. Lo de arriba —«este camino ENCOLA Y RETORNA»— sigue siendo cierto en lo que
		// importa: encola, y lo que se retorna sigue siendo el veredicto de SU INSERT.
		tsCola = time.Now().Unix()
	} else {
		// 3 · Ventana temporal (ADR-0037): el criterio ÚNICO. Info.Timestamp es el reloj del SERVIDOR del
		// mensaje original (message.go:216); el umbral sale del inicio de la conexión, NO de time.Now(), para
		// que nuestro propio atasco no cuente como antigüedad. Solo se loguean edad y umbral, nunca el mensaje.
		var seal time.Time
		if l.connectSeal != nil {
			seal = l.connectSeal() // lectura FRESCA, sin cachear (ver Register y la cabecera de inbound_window.go)
		}
		now := time.Now()
		threshold := resolveThreshold(seal, l.margin, now)
		if e.Info.Timestamp.Before(threshold) {
			// (T3.13) La ráfaga del ADR-0037 son miles de eventos que SÍ atan el hilo del socket, así que se
			// miden; pero en su propia serie, porque cuestan microsegundos y mezclarlos con los encolados
			// diluiría el p99 justo cuando hay ráfaga — es decir, mejoraría el número cuando el Edge va peor.
			camino = latencia.Descartado
			age := now.Sub(e.Info.Timestamp)
			if age < 0 {
				age = 0
			}
			l.brackets.countWindowDrop(age)
			l.log.Warn("listener: entrante descartado por caer fuera de la ventana temporal (ADR-0037)",
				"edad", age.Round(time.Second).String(), "margen", l.margin.String())
			// SE ACUSA (T1.13), y es la decisión más delicada de esta función. El descarte es DELIBERADO y
			// DETERMINISTA sobre un dato inmutable: Info.Timestamp es el reloj del servidor del mensaje ORIGINAL,
			// así que un reenvío llegaría con el mismo sello, volvería a caer fuera de la ventana y volveríamos a
			// pedir otro reenvío. Negar el acuse aquí no rescata ni un mensaje: convierte la ráfaga de miles de
			// eventos del ADR-0037 en una ráfaga PERPETUA que se repite en cada reconexión.
			return true
		}
		tsCola = e.Info.Timestamp.Unix()
	}

	// 3.5 · GRUPO — EL EDGE NO ATIENDE GRUPOS, Y SE CORTA AQUÍ (Plan 044 · Ola 1.5 · T1.5-3, REQ-36/D-044.30).
	//
	// Es el QUINTO filtro de la lista canónica del docstring; lleva el marcador «3.5» por el SITIO del fichero
	// donde se inserta, exactamente igual que el «1.5» del perfil pasivo es el SEGUNDO de aquella lista. La
	// lista manda; los marcadores solo dicen dónde está cada cosa.
	//
	// 🔴 QUÉ CONDUCTA DEROGA. Hasta esta tarea el grupo se juzgaba en la PUERTA DE ELEGIBILIDAD (el switch
	// de enqueueCola, el paso 6): dejaba fila, la fila nacía `clasificado` con la marca `no_elegible` y el
	// despachador la subía a la nube sin intención. Se pagaba el INSERT cifrado, el disco local, la entrega
	// por el cable y el almacenamiento en la nube por un mensaje que el Edge nunca iba a atender. Desde
	// REQ-36 no queda NADA: ni fila, ni entrega, ni un byte local.
	//
	// 🔴 SE ACUSA (true), y es el detalle 1 de D-044.30: negar el acuse sería un REENVÍO ETERNO. Venir de un
	// grupo es una decisión DETERMINISTA sobre el JID del entrante — el reenvío llegaría con el mismo
	// `IsGroup`, se volvería a descartar y volveríamos a pedir otro, en bucle y a ritmo de reconexión. Misma
	// familia que ECO PROPIO, PERFIL PASIVO y FUERA DE VENTANA, y por la misma razón exacta.
	//
	// 🔴 FAIL-OPEN ante cableado incompleto (detalle 3 de D-044.30). Aquí no hay predicado que cablear ni
	// opción que leer: `IsGroup` lo pone whatsmeow al parsear el JID del chat, así que el criterio no puede
	// quedarse «a medio cablear» como sí podía el del perfil pasivo. Y si algún día el campo no viniera, el
	// cero de un bool es `false` y el mensaje seguiría el camino NORMAL: se encola, se entrega y como mucho
	// gastamos de más. Por omisión nunca se corta de más, que es la dirección cara.
	//
	// 🔴 Y SE MARCA `Descartado`: sin esta línea el p99 del handler contaría estos descartes de microsegundos
	// como encolados y MEJORARÍA justo cuando el Edge más filtra (INV-051.2 se juzga contra la serie
	// `Encolado`). Mismo razonamiento que el eco propio y el perfil pasivo.
	if e.Info.IsGroup {
		camino = latencia.Descartado // sale sin fila: su tiempo no es tiempo de encolar
		l.brackets.countGroupDrop()
		// Nivel DEBUG y no Warn a propósito, igual que el filtro pasivo: esto no es una anomalía, es el alcance
		// del producto funcionando, y una sesión metida en grupos activos escribiría una línea por mensaje a
		// ritmo de socket. La señal que se lee EN CAMPO es el contador (`descartes_grupo`, en el bloque del
		// latido), no esta línea. ADR-0034 nivel 3: ni texto ni teléfono; solo el identificador del mensaje,
		// que es lo único que permite correlacionar.
		l.log.Debug("listener: entrante descartado en la puerta — viene de un GRUPO (Plan 044, REQ-36): no se encola, no se persiste y no se entrega",
			"message_id", e.Info.ID)
		return true
	}

	// 4 · COLA DURABLE (Plan 051 Ola 1) — y AQUÍ SE ACABA EL TRABAJO DEL LISTENER (Ola 3 · T3.0).
	//
	// 🔴 AQUÍ NO SE ENTREGA, Y REINTRODUCIR UNA ENTREGA VIOLA INV-051.2. Quien entrega este mensaje al cable
	// es el DESPACHADOR de esta sesión (internal/app/despachador, cableado en sessionmgr/despacho.go): lee la
	// cola en orden de `seq` y hace el `Deliver` desde SU goroutine, al instante. (Hasta T1.6-5 esperaba
	// además, con presupuesto acotado, a que el cajero le pusiera una intención; el ADR-0045 disolvió esa
	// espera.) Hasta el 2026-08-17 aquí abajo había un `l.sink.Deliver` en escritura
	// doble con este mismo INSERT; era el camino INLINE, y era transitorio por diseño.
	//
	// Por qué no puede volver, aunque parezca un atajo inofensivo:
	//   - ATA EL HILO DE WHATSMEOW A UN TERCERO. Este código corre en el bucle de eventos del socket. Cada
	//     milisegundo que se pasa aquí es un milisegundo que la sesión no procesa el siguiente evento.
	//   - DUPLICA. El despachador entrega igual, y la deduplicación por `wa_message_id` del cable es una red
	//     de seguridad, no una licencia para mandar dos veces.
	//   - ROMPE EL ORDEN. La cola entrega FIFO por `seq`; una entrega paralela desde aquí adelanta mensajes.
	//   - Y NO AÑADE DURABILIDAD: la promesa «mensaje durable antes del acuse» la cumple el INSERT, no la
	//     entrega.
	//
	// Un descartado por la ventana (paso 3) ya ha vuelto: no genera fila (REQ-051.5). Y desde el Plan 044 ·
	// T1.5-3, tampoco lo hace un entrante de GRUPO (paso 3.5): aquí abajo ya no llega ninguno, que es la
	// razón por la que el `case e.Info.IsGroup` desapareció del switch de enqueueCola.
	//
	// Y AQUÍ SE DECIDE EL ACUSE (T1.13): lo que devuelva el INSERT es lo que whatsmeow usa para acusar —o no—
	// a WhatsApp. Es lo que convierte «durable antes del acuse» de un orden de instrucciones en una GARANTÍA:
	// sin fila no hay acuse, y sin acuse WhatsApp reenvía.
	return l.enqueueCola(ctx, e, tsCola)
}

// enqueueCola anota el entrante en la cola durable. tsWhatsApp va en epoch-SEGUNDOS (no milis): es el
// sello con el que la cola ordena y poda.
//
// REQ-051.8 — un fallo del Enqueue se REGISTRA y ya: no tumba el socket y no aborta el handler. INV-051.1 —
// el texto del mensaje NUNCA sale por el log, ni entero ni truncado: solo identificadores y el error.
//
// ⚠️ LO QUE SIGNIFICA UN FALLO HA CAMBIADO DOS VECES, y conviene leer las dos. En T3.0, retirado el camino
// inline (`sink.Deliver`), un Enqueue fallido pasó de costar solo la intención a ser un MENSAJE PERDIDO. En
// T1.13 deja de serlo: el fallo ya no se acusa, así que WhatsApp REENVÍA y el mensaje sigue vivo del otro
// lado. La política de no tumbar la escucha no cambia en ninguna de las dos (un fallo de disco no puede
// dejar sordas también a las sesiones sanas); lo que cambia es qué mide `cola_enqueue_error` en InboundStats:
// de métrica de calidad (T1.7) a métrica de PÉRDIDA (T3.0) a métrica de REINTENTO (T1.13). Quien la publique
// en el latido debe leerla como «mensajes que WhatsApp tendrá que reofrecer», no como bajas.
//
// cola == nil ⇒ no-op (NewListener ya gritó al construir; ver allí).
//
// 🔴 DEVUELVE SI LA FILA QUEDÓ EN DISCO (T1.13), y ese bool es lo que gobierna el acuse a WhatsApp: false
// ⇒ whatsmeow NO acusa ⇒ WhatsApp REENVÍA. Es lo que convierte el contador de arriba de «métrica de
// pérdida» en «métrica de reintento»: desde el 2026-08-18 un Enqueue fallido ya no pierde el mensaje, lo
// aplaza. El reenvío es seguro por construcción: el segundo intento choca contra el índice único
// (session_id, wa_message_id) y el store lo trata como duplicado devolviendo nil, así que un mensaje que
// SÍ se escribió y falló después no se duplica — se acusa a la segunda.
func (l *Listener) enqueueCola(ctx context.Context, e *events.Message, tsWhatsApp int64) (dejoFila bool) {
	if l.cola == nil {
		// SIN COLA NO SE ACUSA, y es deliberado pese a que aquí no ha fallado ninguna escritura: es que no
		// hay dónde escribir. Un listener sin cola es un fallo de CABLEADO (imposible desde `agent serve`
		// desde el Plan 051 O3, ver NewListener), y acusar sería tragarse en silencio TODOS los mensajes de
		// esa sesión — el modo de fallo que este plan existe para eliminar.
		//
		// El precio conocido: mientras dure el cableado roto, WhatsApp reofrece los mismos mensajes en cada
		// reconexión. Se prefiere a la alternativa por dos razones. Una, los mensajes SOBREVIVEN: en cuanto
		// el proceso arranque bien cableado —y el daemon no arranca de otra forma— entran todos. Dos, el
		// fallo se hace VISIBLE donde nadie lo puede ignorar: quien escribe al negocio ve su mensaje sin
		// confirmar en vez de verlo confirmado y sin respuesta.
		return false
	}
	// REQ-051.8 / T1.10 — RED DE SEGURIDAD. Un panic aquí dentro (driver de la BD, un crypterFor ajeno,
	// un nil inesperado) subiría al bucle de handlers de whatsmeow y TUMBARÍA LA SESIÓN entera: la cola
	// es una mejora de durabilidad, jamás puede ser peor que no tenerla.
	//
	// 🔴 INV-051.1: el valor recuperado NO se loguea. Un panic puede arrastrar en su mensaje el
	// argumento que lo provocó —y aquí ese argumento puede ser el TEXTO del mensaje o su meta—, así que
	// imprimirlo con %v filtraría contenido de negocio al log. Se anota solo el identificador del
	// mensaje, que es lo que permite correlacionar sin transcribir nada.
	//
	// 🔴 UN PÁNICO RECUPERADO CUENTA COMO «NO DEJÓ FILA» (T1.13), y por tanto NO SE ACUSA. Un pánico a mitad
	// del camino de escritura no deja ninguna prueba de que la fila llegara al disco, y ante la duda la
	// asimetría es la del plan: reenviar de más es barato (el índice único absorbe el duplicado), acusar de
	// más es perder el mensaje para siempre.
	//
	// El false NO se asigna aquí a mano: al recuperar un pánico, el resultado de la función queda en su
	// valor cero, que es exactamente la respuesta correcta. Escribir `dejoFila = false` sería una segunda
	// defensa que dice lo mismo que la primera — y una línea que se puede borrar sin que nada cambie es una
	// línea que ningún test puede custodiar.
	defer func() {
		if r := recover(); r != nil {
			// INV-051.3: la degradación se CUENTA además de loguearse.
			l.brackets.countColaEnqueuePanic()
			l.log.Error("listener: panic al encolar el entrante; la escucha sigue (REQ-051.8) y el mensaje NO se acusa: WhatsApp lo reenviará",
				"message_id", e.Info.ID)
		}
	}()
	text := messageText(e)
	item := app.ColaItem{
		SessionID:   l.sessionID,
		ChatJID:     e.Info.Chat.String(),
		WAMessageID: e.Info.ID,
		TSWhatsApp:  tsWhatsApp,
		Texto:       text,
		Meta:        l.colaMeta(e),
		Estado:      app.EstadoNuevo,
	}
	// 🔴 AQUÍ VIVÍAN LAS TRES MARCAS DE OMISIÓN QUE ESTE LISTENER ESCRIBÍA AL NACER LA FILA
	// (`sin_texto`, `apagado`, `fastlane`), RETIRADAS EL 2026-08-24 — Plan 044 · Ola 1.6 · T1.6-5 ·
	// ADR-0045 · D-044.31 · REQ-35. Con ellas se va el último `switch` de esta función: hoy TODA fila
	// nace `nuevo`, sin sobre, y se entrega.
	//
	// QUÉ DECIDÍAN DE VERDAD, porque es fácil recordarlo mal: NO decidían si el mensaje se encolaba —los
	// tres casos se encolaban y se entregaban igual, y lo siguen haciendo—, sino en qué ESTADO NACÍA la
	// fila. Naciendo `clasificado` el worker-cajero no la reclamaba, y así no gastaba en ella una plaza de
	// su semáforo de un hueco. Era una optimización del PUSH, y el orden entre las tres ramas existía sólo
	// para que la telemetría dijera la verdad sobre cuál de los tres motivos había ganado.
	//
	// POR QUÉ NINGUNA SOBREVIVE. El ADR-0045 pasó la clasificación a PULL: este Edge ya no clasifica por
	// iniciativa propia, así que no hay ningún cajero al que ahorrarle una plaza ni ninguna omisión que
	// justificar. Un sobre escrito aquí hoy sería un dato que nadie volvería a mirar.
	//
	// DÓNDE FUE A PARAR CADA UNA:
	//   - `sin_texto`  — a ninguna parte, y no hace falta: un entrante sin cuerpo textual (imagen, audio,
	//                    sticker, ubicación) se encola y se entrega exactamente igual que uno con texto.
	//                    Quien no le pide inferencia es el Cloud, que ya sabe qué tipo de mensaje es.
	//   - `apagado`    — al Cloud, como decisión, y al Edge, como respuesta: el entitlement `llm_intent`
	//                    sigue mandando, pero se ejerce al SERVIR inferencia (T1.6-2), no al encolar.
	//   - `fastlane`   — al motor de flujos del Cloud (ADR-0044 §1.B): el carril rápido vive donde vive la
	//                    pregunta, y bajo pull la pregunta «¿hace falta el LLM?» se la hace quien va a
	//                    llamarlo. Ver `MotivoFastlane`.
	//
	// 🔴 LOS VALORES DEL ENUM SIGUEN VIVOS (`internal/app/cola.go`) para poder decodificar las filas que ya
	// están escritas en los discos de los clientes. Lo que desaparece es el escritor, no el vocabulario —
	// mismo trato que recibió `no_elegible` en T1.5-3.
	//
	// ✅ SE MANTIENE INTACTA LA PUERTA QUE SÍ ES UNA PUERTA: el descarte de GRUPOS del paso 5 de
	// `onMessage` (T1.5-3) y el filtro de SESIÓN PASIVA (Plan 046). Aquéllos deciden si hay fila; éstas
	// decidían quién la tocaba después.
	if err := l.cola.Enqueue(ctx, item); err != nil {
		// INV-051.3: el fallo se CUENTA siempre, se grite o no (un log se pierde; el acumulado no).
		l.brackets.countColaEnqueueError()
		// EL THROTTLE DEL LOG NO VIVE AQUÍ: vive en el adaptador de la cola, que es quien conoce la sesión,
		// la causa y su ventana de enfriamiento — aquí solo se ve "un error por mensaje" y no hay forma de
		// saber si es el mismo fallo repetido. El adaptador marca con app.ErrColaFalloRepetido lo que YA
		// gritó, y aquí únicamente se baja el nivel. Sin esto, una sesión con la DEK ausente escribía un
		// Error por mensaje entrante, a ritmo de socket.
		if errors.Is(err, app.ErrColaFalloRepetido) {
			l.log.Debug("listener: la cola durable sigue rechazando los entrantes de esta sesión (fallo ya reportado); este mensaje NO se acusa y WhatsApp lo reenviará",
				"message_id", e.Info.ID)
			return false
		}
		l.log.Error("listener: no se pudo anotar el entrante en la cola durable; el mensaje NO SE ACUSA (T1.13: WhatsApp lo reenviará en vez de darlo por entregado) y la escucha sigue (REQ-051.8)",
			"error", err, "message_id", e.Info.ID)
		return false
	}
	return true
}

// sesionEsPasiva consulta el perfil de ESTA sesión con el DEFAULT FAIL-OPEN aplicado (Plan 046 · T2.2):
// sin predicado cableado —o sin session_id conocido— la respuesta es NO PASIVA, de modo que un arranque al
// que le falte la opción se comporte exactamente como antes de esta tarea en vez de dejar sorda a media
// flota en silencio (D-046.2).
//
// El session_id vacío merece su propia rama y no es paranoia: `NewListener` DESACTIVA la cola cuando llega
// sin él (listener.go, «cableado incompleto»), así que es un estado alcanzable de verdad. Consultar el mapa
// con la cadena vacía no encontraría nada y daría el mismo resultado, pero por casualidad; aquí queda dicho.
//
// Se llama UNA VEZ por entrante y no se cachea: el perfil cambia en caliente (ver el campo sesionPasiva).
func (l *Listener) sesionEsPasiva() bool {
	if l.sesionPasiva == nil || l.sessionID == "" {
		return false
	}
	return l.sesionPasiva(l.sessionID)
}

// colaMetaPayload son los metadatos de negocio que acompañan a la fila. Es lo que el despachador (Ola 3)
// necesitará para reconstruir el domain.InboundEvent sin volver a ver el *events.Message, menos lo que ya
// son columnas propias (chat, id de mensaje, sello). Se persiste CIFRADO con la DEK de la sesión
// (INV-051.1): por eso puede llevar identidad del remitente, que en un log estaría prohibida.
type colaMetaPayload struct {
	Sender         string `json:"sender,omitempty"`
	SenderAlt      string `json:"sender_alt,omitempty"`
	AddressingMode string `json:"addressing_mode,omitempty"`
	PushName       string `json:"push_name,omitempty"`
	Type           string `json:"type,omitempty"`
	IsGroup        bool   `json:"is_group,omitempty"`
	// Sintetico marca la fila que NO vino de WhatsApp sino del inyector de entrantes sintéticos
	// (MP-10 Parte A, internal/adapters/whatsmeow/inyector.go).
	//
	// 🔴 ES LA MARCA **LOCAL**, Y NO ES LA QUE SOSTIENE EL GUARDARRAÍL. Estos bytes se persisten CIFRADOS
	// con la DEK de la sesión (INV-051.1) y no salen de la máquina: la nube no los ve nunca. La marca
	// PORTANTE —la que sí viaja aguas abajo hasta el proto de CloudLink— es el prefijo
	// `PrefijoSintetico` del `wa_message_id`, que es columna propia de la cola y que el despachador copia
	// al `domain.InboundEvent`. Ésta es comodidad de auditoría local; aquélla es el contrato.
	//
	// Se DERIVA del prefijo, no se inyecta: así las dos marcas no pueden discrepar. Un true aquí con un
	// wa_message_id sin prefijo sería una fila que el Edge cree sintética y la nube cree real.
	Sintetico bool `json:"sintetico,omitempty"`
}

// colaMeta serializa los metadatos a JSON. Un fallo de serialización NO impide anotar la fila: se anota
// con Meta nil (columna NULL) y se registra, porque el texto durable vale más que su metadato.
func (l *Listener) colaMeta(e *events.Message) []byte {
	b, err := json.Marshal(colaMetaPayload{
		Sender:         e.Info.Sender.String(),
		SenderAlt:      e.Info.SenderAlt.String(),
		AddressingMode: string(e.Info.AddressingMode),
		PushName:       e.Info.PushName,
		Type:           e.Info.Type,
		// 🔴 CONSTANTEMENTE `false` EN LAS FILAS NUEVAS desde el Plan 044 · Ola 1.5 · T1.5-3: el filtro de
		// GRUPO corta en la puerta (paso 5 de onMessage) y aquí abajo no llega ni un entrante de grupo. NO
		// es señal viva: para «cuánto grupo estamos filtrando» la serie es `descartes_grupo`, no este campo.
		// Se sigue rellenando —y por eso el campo no se retira— porque las filas ANTIGUAS, las anotadas
		// antes de T1.5-3, lo llevan a `true` y tienen que seguir decodificándose igual.
		IsGroup: e.Info.IsGroup,
		// MARCA LOCAL del inyector (MP-10 Parte A). Se DERIVA del wa_message_id en vez de venir por un
		// campo aparte: la marca portante y la local salen así del MISMO dato y no pueden divergir.
		// Coste en el camino caliente: un HasPrefix sobre una cadena corta (nanosegundos), y solo para los
		// mensajes que llegan a encolarse.
		Sintetico: strings.HasPrefix(e.Info.ID, PrefijoSintetico),
	})
	if err != nil {
		l.log.Error("listener: no se pudo serializar el metadato de la cola; la fila se anota sin meta",
			"error", err, "message_id", e.Info.ID)
		return nil
	}
	return b
}

// onConnected marca el estado conectado, resetea el backoff (reconexión exitosa) y dispara el hook
// onConnect si está cableado (anuncio de presencia, §10.D). El hook corre FUERA del lock.
func (l *Listener) onConnected() {
	l.mu.Lock()
	l.state = StateConnected
	l.backoff.Reset()
	hook := l.onConnect
	l.mu.Unlock()
	l.log.Info("listener: socket conectado (escucha 24/7 activa)")
	// Prueba de vida (Plan 031 T6): el socket de WhatsApp está VIVO (no solo "el cliente existe").
	if l.reporter != nil {
		l.reporter.SetSocketState(health.SocketConnected, "")
	}
	if hook != nil {
		hook()
	}
}

// onReceiptEvent mapea un *events.Receipt a domain.ReceiptEvent (acuse de entrega/lectura de un
// SALIENTE) y lo despacha por el hook onReceipt si está cableado. Los tipos de acuse fuera del ciclo
// {delivered, read} se IGNORAN sin romper (§10.A). El SessionID lo etiqueta el sink por-sesión aguas
// abajo (T2), como el InboundSink de entrantes (mux.SinkFor); aquí va vacío. No emite PII a los logs.
func (l *Listener) onReceiptEvent(e *events.Receipt) {
	status, ok := mapReceiptStatus(e.Type)
	if !ok {
		return // sender/retry/inactive/hist_sync/… no son del ciclo de vida de un saliente.
	}
	if l.onReceipt == nil {
		return
	}
	l.onReceipt(domain.ReceiptEvent{
		// Copia defensiva del slice de whatsmeow (types.MessageID es alias de string).
		MessageIDs: append([]string(nil), e.MessageIDs...),
		Status:     status,
		Timestamp:  e.Timestamp,
		// SessionID: lo rellena el sink por-sesión en T2 (patrón mux.SinkFor de los entrantes).
	})
}

// mapReceiptStatus traduce el types.ReceiptType de whatsmeow al estado de dominio (Plan 013 §10.A):
// Delivered→delivered (✓✓); Read/ReadSelf/Played→read (✓✓ azul); cualquier otro tipo se ignora
// (ok=false). NOTA DE CAMPO: las constantes reales llevan el infijo "Type"
// (types.ReceiptTypeDelivered/Read/ReadSelf/Played); el diseño §10.A las citaba sin él.
func mapReceiptStatus(t types.ReceiptType) (domain.ReceiptStatus, bool) {
	switch t {
	case types.ReceiptTypeDelivered:
		return domain.ReceiptDelivered, true
	case types.ReceiptTypeRead, types.ReceiptTypeReadSelf, types.ReceiptTypePlayed:
		return domain.ReceiptRead, true
	default:
		return "", false
	}
}

// onDisconnected marca el estado desconectado y AVANZA el backoff. whatsmeow auto-reconecta; aquí
// solo trazamos la cadencia y, si hay hook inyectado, lo disparamos con el delay calculado.
func (l *Listener) onDisconnected() {
	l.mu.Lock()
	l.state = StateDisconnected
	delay := l.backoff.Next()
	attempt := l.backoff.Attempt()
	hook := l.onDisconnect
	l.mu.Unlock()

	// Cierra el corchete abierto, si lo hay, para no perder su reconciliación. Es BEST-EFFORT y solo
	// observabilidad: este evento no es fiable (sale con `go` en client.go:512 y :580, y ni siquiera se
	// emite en una desconexión ESPERADA, client.go:576-580). Da igual: no decide ningún descarte.
	l.brackets.close(closeReconnect)

	l.log.Warn("listener: socket desconectado; whatsmeow reintentará (política de backoff)",
		"intento", attempt, "siguiente_delay", delay.String())
	// Prueba de vida (Plan 031 T6): corte transitorio — whatsmeow reintenta el dial (auto-reconnect); el
	// socket no está vivo pero tampoco caído para siempre ⇒ connecting, no degraded.
	if l.reporter != nil {
		l.reporter.SetSocketState(health.SocketConnecting, health.ReasonReconnecting)
	}
	if hook != nil {
		hook(attempt, delay)
	}
}

// onLoggedOut marca la sesión CAÍDA. NO se re-empareja automáticamente (requiere acción humana:
// escanear un QR nuevo). Se reporta para que el control/cloud lo sepa.
func (l *Listener) onLoggedOut(e *events.LoggedOut) {
	l.mu.Lock()
	l.state = StateLoggedOut
	hook := l.onLoggedOutHook
	l.mu.Unlock()
	// Mismo cierre best-effort del corchete que en la desconexión (ver onDisconnected).
	l.brackets.close(closeReconnect)

	l.log.Error("listener: sesión cerrada por WhatsApp (LoggedOut); requiere re-emparejar",
		"on_connect", e.OnConnect, "reason", e.Reason.String())
	// Prueba de vida (Plan 031 T6): WhatsApp cerró la sesión — dead, no se recupera solo (requiere re-QR).
	if l.reporter != nil {
		l.reporter.SetSocketState(health.SocketDead, health.ReasonLoggedOut)
	}
	// Plan 020 T3: propaga el cierre al cloud (estado ZOMBIE) mientras el stream aún vive, ANTES del
	// teardown local. El comportamiento local previo (logging/no re-pairing) no cambia. Corre fuera del lock.
	if hook != nil {
		hook()
	}
}

// toInboundEvent extrae de un *events.Message los campos útiles de dominio. El cuerpo de texto sale
// de Conversation o, si viene envuelto, de ExtendedTextMessage. No toca material cifrado.
//
// ⚠️ YA NO LA LLAMA EL CAMINO CALIENTE (T3.0): el listener no construye el evento de dominio, porque no
// entrega. SE CONSERVA —y no se borra— porque sigue siendo la ESPECIFICACIÓN EJECUTABLE del mapeo
// *events.Message → domain.InboundEvent: el despachador reconstruye ese mismo evento desde las columnas y
// el `meta` de la cola (despachador.go, «la traducción inversa exacta de toInboundEvent + colaMeta»), y los
// tests de esta capa la usan como referencia de qué campos hay que preservar. Borrarla dejaría al
// despachador sin contra-parte contra la que contrastarse. Si algún día el despachador deja de imitarla,
// muere con ella.
func toInboundEvent(e *events.Message) domain.InboundEvent {
	return domain.InboundEvent{
		MessageID: e.Info.ID,
		Chat:      e.Info.Chat.String(),
		Sender:    e.Info.Sender.String(),
		// SenderAlt: la dirección alterna (número<->LID) que resuelve whatsmeow. Si el mapeo aún no se
		// conoce (JID vacío, "No LID found" en el primer contacto), .String() devuelve "" y aguas
		// abajo se sube solo lo conocido (tolerancia Plan 010 §10.H, sin llamar a GetPNForLID).
		SenderAlt:      e.Info.SenderAlt.String(),
		AddressingMode: string(e.Info.AddressingMode),
		PushName:       e.Info.PushName,
		Timestamp:      e.Info.Timestamp,
		Type:           e.Info.Type,
		Text:           messageText(e),
		IsFromMe:       e.Info.IsFromMe,
		// 🔴 CONSTANTEMENTE `false` en lo que el Edge produce hoy (Plan 044 · Ola 1.5 · T1.5-3): el filtro
		// de GRUPO corta en la puerta y ningún entrante de grupo llega a mapearse. Se mantiene en el mapeo
		// porque esta función es la ESPECIFICACIÓN del sobre —su contra-parte en el despachador lee este
		// mismo campo del `meta`— y el `meta` de las filas ANTIGUAS sí trae `true`.
		IsGroup: e.Info.IsGroup,
	}
}

// messageText devuelve el texto del mensaje: Conversation (mensaje simple) o el Text del
// ExtendedTextMessage (mensaje con contexto/enlace). Vacío si no es de texto.
func messageText(e *events.Message) string {
	if e.Message == nil {
		return ""
	}
	if c := e.Message.GetConversation(); c != "" {
		return c
	}
	return e.Message.GetExtendedTextMessage().GetText()
}
