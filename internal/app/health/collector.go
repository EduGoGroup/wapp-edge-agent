// collector.go — COLECTOR DE SALUD del Edge (Plan 031 T7). Arma un Report por sesión combinando el
// snapshot de runtime del Registry (T6: prueba de vida del socket, motivo de degradación, duración de la
// última carga de la DEK, edad del último entrante) con las señales de ALCANCE DAEMON que T6 no puebla:
// profundidad del outbox (ADR-0003), versión del binario (ldflags) y uptime del proceso. El Report es
// NEUTRAL (sin proto): el adapter CloudLink lo mapea a SessionHealth para el heartbeat y el plano de
// control lo serializa a JSON en GET /v1/health.
//
// PLAN 051 OLA 4 · EL COLECTOR PASA A LEER DOS FUENTES MÁS, y las dos existen porque la Ola 3 partió el
// Edge en DOS PROCESOS y la telemetría se quedó a un lado de la frontera:
//
//	T4.3 · el PARTE DEL WORKER-CAJERO (app.ParteWorkerLector) — circuito del clasificador, taskset y p50
//	       de inferencia. Ocurren en el otro proceso; llegan por la BD de la cola, con REGLA DE RANCIDEZ
//	       (ver parteDelWorker): un parte viejo se descarta ENTERO, jamás se hereda un "closed".
//	T4.0 · los CONTADORES DEL DESPACHADOR por sesión (DespachoReader, ver despacho.go) — los ocho motivos
//	       de omisión distinguibles (INV-051.3) y los cuatro contadores de atasco/sello.
//
// Frontera zero-knowledge (ADR-0007): el Report SOLO lleva METADATOS operativos (estados, edades,
// duraciones, profundidades, versiones). JAMÁS la DEK, credenciales ni contenido de mensajes.
package health

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Report es la foto de salud DERIVADA de una sesión, lista para el wire (edades/duraciones ya calculadas).
// Un único origen de los campos que exige el contrato SessionHealth (Plan 031 T1); dos serializadores
// (proto en el heartbeat, JSON en /v1/health) lo consumen.
type Report struct {
	// SocketState es la etiqueta de prueba de vida del socket ("connected"/"connecting"/"degraded"/"dead"/"").
	SocketState string
	// DegradedReason es el motivo corto cuando degradado/muerto; "" si sano.
	DegradedReason string
	// LastInboundAgeS es la edad en segundos del último evento entrante (0 si aún ninguno).
	LastInboundAgeS int64
	// DEKLoadDurationMs es la duración en ms de la última carga de la DEK (0 si ninguna completó).
	DEKLoadDurationMs int64
	// IntentCircuit es el estado del circuito del clasificador ("closed"/"open"/"half_open"); "" si 029 off.
	IntentCircuit string
	// OutboxDepth es la profundidad del outbox de la sesión (eventos pendientes de drenar).
	OutboxDepth int64
	// BinaryVersion es la build del Edge (ldflags); traza de flota y base del auto-update (Plan 032).
	BinaryVersion string
	// DaemonUptimeS es el uptime del daemon en segundos (mismo valor para todas las sesiones del proceso).
	DaemonUptimeS int64

	// ─── Plan 046 · Ola 2 · T2.3 · el contador del filtro de PRIVACIDAD ───

	// DroppedByPassiveProfile son los entrantes que la puerta de ESTA sesión descartó por tener PERFIL
	// PASIVO (REQ-07: «se descartan en el Edge, nada local»). Sale a JSON como `dropped_passive` y SIN
	// omitempty: un cero es un DATO («esta sesión no está descartando nada») y un hueco obligaría al que lee
	// a preguntarse si el mecanismo existe siquiera.
	//
	// Es `uint64` y no `int64` como sus vecinos A PROPÓSITO: nace uint64 en el listener y en el Registry, y
	// convertirlo aquí solo añadiría una conversión que un día alguien tendrá que justificar ante el linter
	// a cambio de nada — el JSON de un entero sin signo es idéntico.
	//
	// 🔴 CAMBIA EL SIGNIFICADO DE `DroppedByWindow`, Y HAY QUE LEER LAS DOS SERIES JUNTAS. El filtro pasivo
	// corta ANTES de la ventana temporal del ADR-0037 (listener.go, paso 1.5), así que un entrante de una
	// sesión pasiva que ADEMÁS venía fuera de ventana se cuenta aquí y DEJA de contarse allí. Para la sesión
	// pasiva da igual —se descarta todo—, pero para quien mira las métricas no: una CAÍDA de
	// `DroppedByWindow` puede no ser «el Edge está ingiriendo mejor» sino «hay más sesiones pasivas». Ver el
	// mismo aviso en whatsmeow.InboundStats.DroppedByWindow y en el campo `descartes_perfil_pasivo` del
	// bloque del latido.
	//
	// ⚠️ VIVE EN MEMORIA Y SE REINICIA CON EL PROCESO (y desde T5.4 del Plan 051 el núcleo se relanza solo):
	// un `dropped_passive: 0` tras un reinicio significa «este proceso acaba de nacer», no «no descartó nada».
	//
	// ⚠️ NO VIAJA EN EL HEARTBEAT. INV-5 de esta ola prohíbe tocar el proto, así que `SessionHealth` no lo
	// lleva: sale por GET /v1/health, por el bundle de diagnóstico y por el bloque del latido. El día que el
	// proto se abra, este es el campo que se mapea.
	DroppedByPassiveProfile uint64

	// FiltersVersion es la VERSIÓN DEL MAPA DE PERFILES con la que este Edge está filtrando ahora mismo —el
	// `version` del último ConfigUpdate kind:"filters" aplicado (D-046.2)—. Sale a JSON como
	// `filters_version`, y ese nombre es CONTRATO con `wapp-ctl` y con el runbook.
	//
	// 🔴 ES DEL EDGE, NO DE LA SESIÓN, y viaja en la vista por sesión por el mismo motivo que `binary_version`
	// y `daemon_uptime_s`: quien lee `dropped_passive` de una sesión necesita, EN LA MISMA FOTO, con qué mapa
	// se tomó esa decisión. Separarlo en otro bloque obligaría a correlacionar dos lecturas a mano.
	//
	// 🔴 SIN ÉL, EL PEOR MODO DE FALLO DE ESTA OLA ES INDIAGNOSTICABLE. `Store.Put` sobrescribe sin condición y
	// los ConfigUpdate llegan por workers en paralelo: un push viejo que persistiera después de uno nuevo deja
	// al Edge, tras el siguiente reinicio, filtrando con un mapa RETRASADO —sesiones reactivadas que siguen
	// mudas— sin error, sin log y sin métrica. Este número es lo único que delata ese estado: se compara con
	// el que la consola dice haber empujado y, si no cuadran, ahí está el diagnóstico.
	//
	// 0 significa «aún no hay config de filtros aplicada» ⇒ NADIE es pasiva (fail-open de D-046.2), que es
	// exactamente el comportamiento anterior al Plan 046. Ojo: es también el valor legítimo del tenant sin
	// ninguna fila de sesión (regla 2 de T2.1). `dropped_passive` al lado distingue los dos casos.
	FiltersVersion int64

	// ─── Plan 051 Ola 4 · T4.3 · lo que el WORKER-CAJERO sabe y este proceso no ───
	//
	// Los TRES campos de abajo (IntentCircuit incluido) salen del MISMO parte, y por eso caen JUNTOS: si el
	// parte está rancio o no se pudo leer, los tres quedan a su cero. Ver `parteDelWorker`.

	// WorkerTaskset es cómo está confinado el worker-cajero ("disjunta"/"solapada"/"cajero_sin_confinar");
	// "" si no se sabe (parte ausente, rancio o ilegible).
	WorkerTaskset string
	// IntentP50Ms es el p50 de la INFERENCIA del clasificador en ms, medido por el cajero. 0 = sin muestras
	// (o parte ausente/rancio): NO significa «inferencia instantánea».
	IntentP50Ms int64

	// ─── Plan 051 Ola 4 · T4.0 · los OCHO motivos, distinguibles (INV-051.3) ───

	// IntentOmittedByReason es el desglose de despachos SIN intención de ESTA sesión, motivo a motivo, con
	// las OCHO claves de app.MotivosOmitido() SIEMPRE presentes (un motivo a 0 es un dato, no un hueco).
	//
	// 🔴 NUNCA AGREGADO ENTRE MOTIVOS (INV-051.3): `fastlane` es el camino SANO y `presupuesto`/`breaker`
	// son fallos; sumarlos borra la única pregunta que este desglose responde («¿hay que mirar Ollama?»).
	// Sumar el MISMO motivo entre sesiones sí es legítimo (es lo que hace el agregado del daemon).
	IntentOmittedByReason map[string]int64
	// StuckHeads es cuántas filas quedaron de cabeza en un estado que esta versión no conoce. 🔴 CERO ES EL
	// ÚNICO VALOR SANO: cada una es una sesión que dejó de drenar y no vuelve sola.
	StuckHeads int64
	// StuckHeadPolls dice si el atasco es HISTORIA o es AHORA: sigue creciendo mientras la sesión siga
	// bloqueada.
	StuckHeadPolls int64
	// FailedSealDispatch: falló `MarcarDespachada` tras una entrega CON ÉXITO. 🔴 Cada uno es un DUPLICADO
	// en la nube. Es el número que se mira.
	FailedSealDispatch int64
	// FailedSealBudget: falló `DespacharSinIntent`. La fila se reintenta y nada sale mal aguas arriba.
	//
	// 🔴 SEPARADO DE FailedSealDispatch A PROPÓSITO (T3.12): sólo UNO de los dos implica mensajes duplicados.
	// Sumarlos deshace esa distinción y convierte ruido operativo en un incidente (o al revés).
	FailedSealBudget int64
}

// OutboxDepther es el puerto MÍNIMO que el colector necesita del outbox: la profundidad por sesión. Se
// declara aquí (no se importa app.Outbox) para que el paquete health no dependa de la capa de aplicación
// ni del adapter. Lo satisface *outbox.Store. nil ⇒ el colector reporta profundidad 0.
type OutboxDepther interface {
	Depth(ctx context.Context, sessionID string) (int64, error)
}

// Collector arma Reports de salud. Uno por daemon, compartido: lee el Registry (T6) y las señales daemon.
// Todas sus dependencias externas son tolerantes a nil (outbox/parte/despacho ausentes ⇒ campos en su cero).
type Collector struct {
	reg    *Registry
	outbox OutboxDepther
	// parte es el LECTOR DEL PARTE DEL WORKER-CAJERO (Plan 051 Ola 4 · T4.3): el canal por el que este
	// proceso se entera del circuito, el taskset y el p50 de inferencia, que ocurren en OTRO proceso. nil
	// ⇒ los tres campos quedan a su cero, que es EXACTAMENTE el comportamiento previo a T4.3 (y el que
	// siguen ejercitando los tests y los wirings que no lo cablean).
	parte     app.ParteWorkerLector
	version   string
	startedAt time.Time
	now       func() time.Time
	log       sharedlogger.Logger

	// filtersVersion LEE la versión del mapa de perfiles vigente (Plan 046 · Ola 2). Es un `func` y no un
	// entero copiado al construir porque el mapa cambia EN CALIENTE: la nube empuja una versión nueva y el
	// siguiente latido tiene que publicarla, no la del arranque. nil ⇒ 0, que es lo mismo que responde la
	// vista de perfiles cuando aún no hay config: los cableados que no lo pasan (tests, arranques que no
	// vienen de `agent serve`) se comportan igual que antes de esta línea.
	filtersVersion func() int64

	// muDespacho protege `despacho`, que se cablea TARDE (SetDespachoReader) porque en el wiring del daemon
	// el colector nace ANTES que el session manager que lo implementa. Sin el candado, ese Set competiría
	// con el primer heartbeat que ya puede estar corriendo (el multiplexor CloudLink se construye en medio).
	muDespacho sync.RWMutex
	despacho   DespachoReader
}

// CollectorOption ajusta el colector (reloj de test, logger, lector de contadores del despachador).
type CollectorOption func(*Collector)

// WithClock inyecta el reloj (tests deterministas de edades/uptime Y de la RANCIDEZ del parte del worker).
func WithClock(now func() time.Time) CollectorOption {
	return func(c *Collector) {
		if now != nil {
			c.now = now
		}
	}
}

// WithLogger inyecta el logger del colector. Sólo se usa para AVISAR (warn) de un parte del worker que no
// se pudo leer: la telemetría nunca tumba un heartbeat, pero tampoco debe fallar en silencio. nil ⇒
// sharedlogger.Default().
func WithLogger(log sharedlogger.Logger) CollectorOption {
	return func(c *Collector) {
		if log != nil {
			c.log = log
		}
	}
}

// WithDespachoReader cablea, YA EN LA CONSTRUCCIÓN, el lector de los contadores del despachador por sesión
// (T4.0). En producción NO se usa —el daemon lo cablea después, con SetDespachoReader, porque el Manager
// nace más tarde—; existe para los tests, que sí pueden construirlo todo de una vez.
func WithDespachoReader(r DespachoReader) CollectorOption {
	return func(c *Collector) {
		if r != nil {
			c.despacho = r
		}
	}
}

// WithFiltersVersion cablea el LECTOR de la versión del mapa de perfiles de sesión (Plan 046 · Ola 2). En
// producción lo pasa el daemon con `perfiles.Version` —el método, no su resultado—, de modo que cada Report
// publique la versión VIGENTE en ese momento y no la del arranque.
//
// 🔴 SIN ESTA OPCIÓN EL CAMPO SALE A 0 SIEMPRE, y un 0 se lee como «este Edge no tiene mapa» (fail-open). Es
// decir: olvidar esta línea en el cableado no rompe nada visible, publica una MENTIRA plausible. Por eso el
// cable tiene su propio test (internal/infra/daemon), igual que el del cronómetro.
//
// nil se IGNORA (no desconecta un lector ya cableado), como el resto de opciones del colector.
func WithFiltersVersion(fn func() int64) CollectorOption {
	return func(c *Collector) {
		if fn != nil {
			c.filtersVersion = fn
		}
	}
}

// NewCollector construye el colector. reg puede ser nil (Collect devolverá ok=false para toda sesión);
// outbox nil ⇒ profundidad 0; parte nil ⇒ IntentCircuit/WorkerTaskset/IntentP50Ms en su cero. version es
// la build del binario (ldflags) y startedAt marca el arranque del proceso (base del uptime).
//
// 🔴 EL TERCER PARÁMETRO CAMBIÓ EN T4.3 y no es un renombrado cosmético. Hasta la Ola 3 era un
// `circuit func() string` que leía el breaker del decorador INLINE; T3.0 retiró ese decorador y el
// parámetro quedó recibiendo un `nil` explícito desde el daemon, con `intent_circuit` viajando vacío para
// siempre. El circuito REAL vive en el worker-cajero, en otro proceso, y sólo se puede leer por el canal
// que ambos comparten: la BD de la cola. Por eso ahora se pide el LECTOR DEL PARTE y no un callback: un
// callback local no puede volver a decir la verdad sobre un proceso que no es este.
func NewCollector(reg *Registry, outbox OutboxDepther, parte app.ParteWorkerLector, version string, startedAt time.Time, opts ...CollectorOption) *Collector {
	c := &Collector{
		reg:       reg,
		outbox:    outbox,
		parte:     parte,
		version:   version,
		startedAt: startedAt,
		now:       time.Now,
		log:       sharedlogger.Default(),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// SetDespachoReader cablea (o recablea) el lector de los contadores del despachador por sesión (T4.0).
//
// POR QUÉ UN SETTER Y NO UNA OPCIÓN DEL CONSTRUCTOR: en el wiring del daemon el colector se construye
// ANTES que el session manager —el multiplexor CloudLink necesita el colector, y el Manager necesita el
// multiplexor—, así que en el momento de `NewCollector` el implementador todavía no existe. Es la misma
// costura tardía que `srv.SetHealthProvider`. nil se IGNORA (no desconecta un lector ya cableado).
func (c *Collector) SetDespachoReader(r DespachoReader) {
	if c == nil || r == nil {
		return
	}
	c.muDespacho.Lock()
	c.despacho = r
	c.muDespacho.Unlock()
}

// despachoReader lee el lector bajo el candado (ver muDespacho).
func (c *Collector) despachoReader() DespachoReader {
	c.muDespacho.RLock()
	defer c.muDespacho.RUnlock()
	return c.despacho
}

// Collect arma el Report de la sesión sessionID. ok=false si la sesión no tiene entrada de salud en el
// Registry (sin prueba de vida real ⇒ no se reporta salud, por diseño T6/T7). Deriva la edad del último
// entrante y la duración de la DEK del snapshot; puebla las señales daemon (outbox/circuito/versión/uptime)
// SIEMPRE que la sesión exista.
func (c *Collector) Collect(ctx context.Context, sessionID string) (Report, bool) {
	if c == nil {
		return Report{}, false
	}
	snap, ok := c.reg.Snapshot(sessionID)
	if !ok {
		return Report{}, false
	}
	now := c.now()
	circuito, taskset, p50 := c.parteDelWorker(ctx, now)
	r := Report{
		SocketState:       string(snap.SocketState),
		DegradedReason:    snap.DegradedReason,
		DEKLoadDurationMs: snap.DEKLoadDuration.Milliseconds(),
		IntentCircuit:     circuito,
		WorkerTaskset:     taskset,
		IntentP50Ms:       p50,
		BinaryVersion:     c.version,
		DaemonUptimeS:     c.uptimeS(now),
		// Plan 046 · T2.3: sale del MISMO Snapshot que el resto de la salud de runtime, tal cual. No se
		// deriva, no se agrega y no se condiciona a nada: es el acumulado del proceso para esta sesión.
		DroppedByPassiveProfile: snap.DroppedByPassiveProfile,
		// Plan 046 · Ola 2: la versión del mapa con que se tomó esa decisión. Se lee FRESCA en cada Report
		// (ver el campo filtersVersion): el mapa cambia en caliente y una copia del arranque mentiría.
		FiltersVersion: c.versionDeFiltros(),
	}
	if !snap.LastInboundAt.IsZero() {
		if age := now.Sub(snap.LastInboundAt); age > 0 {
			r.LastInboundAgeS = int64(age.Seconds())
		}
	}
	if c.outbox != nil {
		if depth, err := c.outbox.Depth(ctx, sessionID); err == nil {
			r.OutboxDepth = depth
		}
	}
	c.poblarDespacho(&r, sessionID)
	return r, true
}

// poblarDespacho vuelca en el Report los contadores del despachador de ESTA sesión (T4.0). El desglose por
// motivo se rellena SIEMPRE con las ocho claves canónicas —haya o no lector, esté o no viva la sesión—,
// porque «este motivo no se ha dado» y «no sé nada de esta sesión» se distinguen mirando el resto del
// Report, no dejando huecos en el mapa que el consumidor tenga que adivinar.
func (c *Collector) poblarDespacho(r *Report, sessionID string) {
	r.IntentOmittedByReason = omitidosEnCero()
	lector := c.despachoReader()
	if lector == nil {
		return
	}
	st, ok := lector.DespachoStats(sessionID)
	if !ok {
		return
	}
	// Se COPIA clave a clave (no se sustituye el mapa) por dos motivos: el lector podría devolver un mapa
	// que él siga escribiendo, y así una clave que él no traiga —un rollback a un binario con menos
	// motivos— sigue apareciendo a 0 en vez de desaparecer de la serie.
	for motivo, n := range st.OmitidosPorMotivo {
		r.IntentOmittedByReason[motivo] = n
	}
	r.StuckHeads = st.CabezasAtascadas
	r.StuckHeadPolls = st.PollsCabezaAtascada
	// 🔴 LOS DOS SELLOS VIAJAN SEPARADOS Y NO HAY NINGÚN CAMPO QUE LOS SUME (T3.12). `FailedSealDispatch`
	// es un DUPLICADO ya publicado en la nube; `FailedSealBudget` es una fila que se reintenta sola en el
	// poll siguiente. Un `+` en esta línea —o un tercer campo «total»— borraría esa diferencia justo en el
	// sitio donde el operador la lee, y convertiría ruido operativo en un incidente (o al revés).
	r.FailedSealDispatch = st.FallosSelloDespacho
	r.FailedSealBudget = st.FallosSelloPresupuesto
}

// DespachoVivas devuelve el AGREGADO de los contadores del despachador sobre las sesiones VIVAS (T4.0),
// para el bloque de daemon del plano de control local. Sin lector cableado devuelve el cero con las ocho
// claves presentes. Ver el doc de sessionmgr.Manager.DespachoStatsVivas para la semántica de «vivas».
func (c *Collector) DespachoVivas() DespachoStats {
	if c == nil {
		return DespachoStatsCero()
	}
	lector := c.despachoReader()
	if lector == nil {
		return DespachoStatsCero()
	}
	st := lector.DespachoStatsVivas()
	if st.OmitidosPorMotivo == nil {
		st.OmitidosPorMotivo = omitidosEnCero()
	}
	return st
}

// Reports arma el Report de TODAS las sesiones vivas (las que tienen entrada en el Registry). Lo consume
// GET /v1/health y el snapshot de subsistemas del bundle de diagnóstico (T8). Nunca es nil (mapa vacío si
// no hay sesiones).
func (c *Collector) Reports(ctx context.Context) map[string]Report {
	out := make(map[string]Report)
	if c == nil {
		return out
	}
	for _, id := range c.reg.SessionIDs() {
		if r, ok := c.Collect(ctx, id); ok {
			out[id] = r
		}
	}
	return out
}

// DaemonUptimeS devuelve el uptime del daemon en segundos (para el bloque daemon de /v1/health).
func (c *Collector) DaemonUptimeS() int64 {
	if c == nil {
		return 0
	}
	return c.uptimeS(c.now())
}

// Version devuelve la build del binario (para el bloque daemon de /v1/health).
func (c *Collector) Version() string {
	if c == nil {
		return ""
	}
	return c.version
}

// parteDelWorker lee el parte que el worker-cajero deja en la BD de la cola y devuelve los TRES campos que
// de él dependen: circuito, taskset y p50 de inferencia. Es la única puerta por la que este proceso se
// entera de lo que pasa en el otro (Plan 051 Ola 4 · T4.3).
//
// 🔴 LA REGLA DE RANCIDEZ, QUE ES EL CORAZÓN DE ESTA FUNCIÓN. Si el parte se aparta del ahora más de
// `app.ParteRancio` —viejo por detrás O adelantado por delante, la ventana es SIMÉTRICA (ver abajo)—,
// SE DESCARTA ENTERO y los tres campos salen a su cero. No se publica el `"closed"` del último parte de un
// cajero que lleva media hora muerto: eso sería una señal de SALUD INVENTADA viajando a la nube en cada
// latido, y una salud inventada es peor que la ausencia del dato — el operador la cree, y `intent_circuit`
// vacío al menos le dice la verdad («este Edge no sabe»). El umbral se usa POR NOMBRE, nunca un literal.
//
// LOS TRES CAEN JUNTOS, y también es deliberado: vienen del mismo instante del mismo proceso. Conservar el
// p50 y tirar el circuito daría una foto mitad viva mitad muerta que nadie sabría interpretar.
//
// ⚠️ UN ERROR DE LECTURA (BD bloqueada por el cajero, fichero movido) TAMPOCO TUMBA NADA: se avisa a warn
// y los tres quedan a cero. Este código corre en el camino del HEARTBEAT; que la telemetría de un
// subsistema impida decir «sigo vivo» sería el peor cambio posible.
//
// La ausencia de parte (el cajero nunca escribió: ok=false) NO es un error y NO se loguea: es el estado
// normal de un Edge recién arrancado, y avisar por cada sesión en cada latido sería ruido perpetuo.
func (c *Collector) parteDelWorker(ctx context.Context, now time.Time) (circuito, taskset string, p50 int64) {
	if c.parte == nil {
		return "", "", 0
	}
	p, ok, err := c.parte.LeerParte(ctx)
	if err != nil {
		c.log.Warn("health: no se pudo leer el parte del worker-cajero; intent_circuit/worker_taskset/"+
			"intent_p50_ms viajan VACÍOS en este latido (nunca un valor heredado)", "error", err)
		return "", "", 0
	}
	if !ok {
		return "", "", 0
	}
	// 🔴 LA VENTANA ES SIMÉTRICA, Y EL LADO DEL FUTURO NO ES PARANOIA DE LIBRO. Con la comparación en un
	// solo sentido (`now.Sub(p.TS) > ParteRancio`), un parte con TS EN EL FUTURO da una edad NEGATIVA, que
	// nunca supera el umbral: sería fresco PARA SIEMPRE. El escenario concreto es el de la plataforma
	// objetivo, un portátil que se suspende: el cajero escribe su parte, el reloj salta HACIA ATRÁS (NTP al
	// despertar, cambio manual de hora), el cajero muere — y a partir de ahí el daemon publica en cada
	// latido el `"closed"` de un clasificador APAGADO, sin caducar jamás. Es exactamente la salud inventada
	// que esta regla existe para impedir, y encima con la peor cara posible: verde permanente.
	//
	// La migración `0002_parte_worker.sql` YA PROMETÍA este comportamiento («un salto de NTP hacia atrás …
	// produce un parte que parece del futuro o rancio de más — degrada a `intent_circuit` vacío, que es el
	// fallo seguro»). Hasta esta línea, la promesa era falsa por el lado del futuro.
	//
	// La tolerancia hacia el futuro es de un ParteRancio entero, y es GENEROSA a propósito: los dos procesos
	// leen el reloj de LA MISMA MÁQUINA, así que el desfase legítimo es cero (sólo cabe la fracción de
	// segundo que se pierde al truncar `ts_unix` a segundos). Un parte que se adelanta más de 90 s no es un
	// desfase: es un reloj que saltó, y de un reloj que saltó no se puede deducir salud.
	edad := now.Sub(p.TS)
	if edad > app.ParteRancio || edad < -app.ParteRancio {
		// Sin log: un cajero parado a propósito (o aún arrancando) haría de esto una línea por sesión y por
		// latido. El hecho ya viaja —y de forma más útil— como `intent_circuit` vacío en el propio Report.
		return "", "", 0
	}
	// Normaliza al contrato del wire ("half-open" → "half_open"): el endpoint /v1/intent/status conserva la
	// forma con guion, el heartbeat y el bundle usan la del contrato SessionHealth (ADR-0023).
	return strings.ReplaceAll(p.Circuito, "-", "_"), p.Taskset, p.P50ms
}

// versionDeFiltros lee la versión del mapa de perfiles vigente, con el 0 como respuesta cuando no hay lector
// cableado. Existe como método —en vez de un `if` en Collect— para que el nil se juzgue en UN solo sitio: un
// pánico aquí ocurriría en el camino del HEARTBEAT, y que la telemetría de un subsistema impida decir «sigo
// vivo» es el peor cambio posible (mismo criterio que parteDelWorker).
func (c *Collector) versionDeFiltros() int64 {
	if c.filtersVersion == nil {
		return 0
	}
	return c.filtersVersion()
}

// uptimeS calcula el uptime en segundos (0 si startedAt es cero o el reloj retrocedió).
func (c *Collector) uptimeS(now time.Time) int64 {
	if c.startedAt.IsZero() {
		return 0
	}
	if d := now.Sub(c.startedAt); d > 0 {
		return int64(d.Seconds())
	}
	return 0
}
