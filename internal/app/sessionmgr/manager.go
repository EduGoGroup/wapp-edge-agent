package sessionmgr

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/latencia"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Manager es el registro vivo de N sesiones del Edge (design §1). Posee el ciclo de vida de cada
// sesión y, dado un session_id, resuelve su custodia/store/cliente/listener. Concurrencia sin broker
// (ADR-0003): un map protegido por sync.Mutex y una goroutine listener por sesión gobernada por
// context.Context con su cancel + un WaitGroup para el apagado ordenado (design §10.I).
//
// BD ÚNICA (Plan 022 T3): el Pair/Restore/Unlink operan sobre una sola *sql.DB COMPARTIDA (campo db,
// inyectada por WithSharedDB) — Container whatsmeow compartido + msg_enc_* per-device + metadatos — en
// vez del modelo `.db`-por-sesión retirado. El borrado quirúrgico purga metadatos, DEK y material cifrado
// sin dejar huérfanos (decisión §10.I).
type Manager struct {
	// mu protege live (y, en T4+, los arranques/paradas de listeners).
	mu sync.Mutex
	// live indexa las sesiones VIVAS por session_id. Vacío hasta que Pair/Restore (T3/T4) lo pueblen.
	live map[string]*liveSession
	// wg espera a que todas las goroutines listener terminen en el apagado (Stop).
	wg sync.WaitGroup

	// layout resuelve la ruta de la DEK por sesión (keys/<id>.key). Desde Plan 022 T3 (BD única) ya NO
	// hay directorio ni store.db por sesión: el store vive en la BD compartida `db`; la custodia de la DEK
	// sigue DESACOPLADA en disco (Plan 022 §3/§10.C), y es lo único que el Layout resuelve en runtime.
	layout Layout
	// sessions persiste los metadatos de negocio (tablas accounts/devices de la BD única); fuente para
	// Persisted().
	sessions app.SessionStore

	// cascade y account son las CAPACIDADES OPCIONALES del store para el borrado quirúrgico sin huérfanos
	// (Plan 027 T4, cierra H4): borrado por-dispositivo en cascada y operaciones por-cuenta (número). Se
	// resuelven UNA vez en NewManager por interface-upgrade sobre `sessions` (puertos EXPLÍCITOS de app,
	// no interfaces ad-hoc acopladas al store concreto), en vez de re-asertar en cada borrado. nil si el
	// store no las implementa (fakes en memoria de los tests): entonces deleteDeviceMeta cae a
	// sessions.Delete y UnlinkAccount devuelve error claro de "no soportado".
	cascade app.DeviceCascadeStore
	account app.AccountStore

	// db es la BD ÚNICA COMPARTIDA del Edge (Plan 022 T3, decisión §10.A): una sola *sql.DB de la que
	// cuelgan los metadatos (accounts/devices), el Container whatsmeow compartido (whatsmeow_*) y el store
	// cifrado per-device (msg_enc_*). El Manager NO la posee (la abre/cierra el daemon, apagado ordenado);
	// aquí solo se referencia para el pairing (Container compartido), el listener per-device y el borrado
	// quirúrgico del material cifrado (cryptostore.DeleteDevice). nil en los tests con factories fake (sin
	// BD real): el borrado cifrado se omite de forma segura. Lo inyecta WithSharedDB.
	db *sql.DB
	// dbDialect es el motor con el que se abrió `db` (DialectSQLite|DialectPostgres): se pasa tal cual a
	// cryptostore/whatsmeow para que emitan el SQL correcto. Lo inyecta WithSharedDB junto a db.
	dbDialect string
	// max es el límite suave de sesiones simultáneas (WAPP_AGENT_MAX_SESSIONS, design §10.G).
	max int
	// multiDevicePerAccount es el número de DISPOSITIVOS VIVOS permitidos por CUENTA (número), base del
	// failover multi-dispositivo por número (Plan 022 T5, design §6/§10.F). Default 1 (un device operativo
	// por número; comportamiento actual). Con >1 (tope 4) el Manager admite N devices vivos del mismo
	// número: 1 primary + standbys, y promueve un standby si el primary cae/expira (LoggedOut). Lo gobierna
	// Pair (assignRoleLocked): rechaza el pairing que excedería el cupo del número. CAVEAT (requisito del
	// plan §10.F): multi-device es RESILIENCIA, NO SIGILO — más companions = más huella, no menos baneo;
	// por eso va OFF por defecto. Lo inyecta WithMultiDevicePerAccount (clamp [1,4]).
	multiDevicePerAccount int
	// log es el logger raíz; cada liveSession derivará un hijo con session_id/jid (design §10.J).
	log sharedlogger.Logger

	// newPairer construye, para UNA sesión, el caso de uso de pairing sobre su custodia y su store
	// (design §5). Se inyecta por opción (WithWhatsmeowPairing en producción; un fake en los tests)
	// para que Manager.Pair sea testeable sin WhatsApp. nil hasta que se configure: Pair lo exige.
	newPairer pairFactory

	// newListener construye, para UNA sesión, el runner de escucha always-on sobre su custodia y su
	// store (design §6). Se inyecta por opción (WithWhatsmeowListen en producción; un fake en los
	// tests) para que Restore/runListener sean testeables sin WhatsApp. nil hasta que se configure:
	// Restore lo exige y startListener registra la sesión SIN escucha (warn) si falta.
	newListener listenFactory

	// newCustody construye la custodia DEK de UNA sesión dado el path de su DEK (Layout.DEKPath(id)).
	// Default = keycustody.NewFileCustody, que resuelve el backend POR PLATAFORMA en compilación (Plan
	// 023 T2): Keychain en darwin, archivo plano en el resto. Se inyecta como factory —en vez de llamar
	// al constructor concreto en custodyFor— para que los tests cablen un DOBLE en memoria y no toquen el
	// Keychain REAL de la máquina (headless, determinista, sin efectos globales). Producción usa el
	// default; NUNCA es nil (NewManager lo fija). Mismo patrón que newPairer/newListener.
	newCustody func(path string) app.KeyCustody

	// cloudMux es el multiplexor CloudLink del Edge (UN stream, N sesiones, ADR-0008): el Manager
	// registra cada sesión al arrancar su listener (Restore/Pair) y la quita al desvincularla (Unlink).
	// Lo inyecta WithWhatsmeowListen junto al factory de escucha. nil en los tests que cablean newListener
	// directamente (sin mux): startListener/Unlink omiten el registro de forma segura.
	cloudMux CloudLinkMux

	// (aquí vivió `inboundDecorator`, el envoltorio del sink de cada listener que anotaba la intención LLM
	// en línea, Plan 029 · T11. Se retiró en el Plan 051 Ola 3 · T3.0 junto con su opción
	// WithInboundDecorator: el listener ya no entrega, así que no hay entrega que decorar. La intención la
	// resuelve el worker-cajero y la escribe en la cola; el despachador la lee de ahí.)

	// backoffBase/backoffMax acotan la política de reintento de un listener caído (aislamiento §10.H):
	// retroceso exponencial Base·2^n saturado en Max. Defaults 1s/60s; los tests inyectan valores
	// minúsculos (WithListenerBackoff) para no depender de esperas reales.
	backoffBase time.Duration
	backoffMax  time.Duration

	// health es el registro de salud de runtime por sesión (Plan 031 T6): el factory liga cada sesión a su
	// SessionReporter (socket state + duración de carga de DEK + edad del último entrante) y runListener lo
	// actualiza al arrancar/caer. T7 lo consume para el heartbeat y GET /v1/health. nil ⇒ no se reporta (el
	// reporter ligado es no-op, ver health.Registry.For). Lo inyecta WithHealthRegistry.
	health *health.Registry

	// cola es la COLA DURABLE DE ENTRANTES del Edge (Plan 051 Ola 1): el factory la pasa al gateway de cada
	// sesión JUNTO CON su session_id, para que el listener anote ahí el mensaje (sellado con la DEK de ESA
	// sesión). Es COMPARTIDA por todas las sesiones (una sola BD, un `seq` global). Lo inyecta
	// WithColaEntrantes.
	//
	// 🔴 nil ⇒ LA SESIÓN QUEDA SORDA (cambió en T3.0). Mientras existió el camino inline, un nil aquí era un
	// fallback benigno: el listener seguía entregando al sink. Retirado el inline, un nil significa que el
	// entrante no se anota en ningún sitio y nadie lo despacha.
	//
	// EN PRODUCCIÓN YA NO ES ALCANZABLE (2026-08-17): el daemon no arranca sin cola. Este campo sigue
	// pudiendo ser nil para los tests, y el Manager sigue sin impedir ARRANCAR la sesión por ello —el
	// socket, los acuses y el estado de conexión valen por sí solos, y una sesión rota no puede tumbar a
	// las sanas—, pero ya no describe ningún modo de operación real: el listener lo grita al construirse.
	cola app.ColaEntrantes

	// colaDespachador es el lado DESPACHADOR de esa MISMA cola (Plan 051 Ola 3, T3.3): con él, cada sesión
	// arranca junto a su listener el bucle que DRENA su cola —en orden de `seq`— y entrega al cable. Es el
	// mismo adaptador que `cola` visto por el otro extremo del puerto (leer la cabeza y sellarla), no una
	// segunda cola. nil (opción no inyectada, es decir: tests) ⇒ la sesión escucha pero no drena, y desde
	// T3.0 eso significa que no entrega NADA. En producción no puede ser nil: el daemon falla antes de
	// llegar a cablear el Manager. Lo inyecta WithColaDespachador.
	colaDespachador app.ColaDespachador

	// presupuestoIntent es cuánto retiene el despachador la cabeza de una sesión esperando a que el cajero
	// la clasifique, antes de entregarla sin intención (WAPP_AGENT_INTENT_WAIT_MS). 0 (opción no inyectada)
	// ⇒ manda el default del propio despachador. Lo inyecta WithPresupuestoIntent.
	presupuestoIntent time.Duration

	// clasificadorActivo es el LECTOR del interruptor del clasificador de intenciones (Plan 051 Ola 2,
	// T2.12) que el factory pasa al gateway de cada sesión: con él, un entrante que llega con la feature
	// apagada nace en la cola YA resuelto (marca `apagado`) en vez de ocupar una plaza del semáforo del
	// cajero para acabar descartado. Es COMPARTIDO (un solo clasificador por Edge), a diferencia de la cola,
	// que se etiqueta por sesión. nil (opción no inyectada) ⇒ NO se toca el gateway
	// y manda el default SEGURO del Listener (activo). Lo inyecta WithClasificadorActivo.
	clasificadorActivo func() bool

	// latencia es el CRONÓMETRO COMPARTIDO del handler de entrantes (Plan 051 Ola 3 · T3.13): el factory lo
	// pasa al gateway de cada sesión y de ahí al Listener, que anota en él cuánto tardó cada onMessage. Es
	// uno para todo el Edge porque INV-051.2 se juzga sobre el Edge, no sesión a sesión. nil ⇒ no se mide y
	// nada más cambia. Lo inyecta WithLatencia.
	latencia *latencia.Histograma

	// inboundMargin es el margen de la ventana temporal de ingesta (ADR-0037) que el factory pasa al
	// gateway de cada sesión. 0 (opción no inyectada) ⇒ NO se toca el gateway y manda el default del propio
	// Listener; así los tests del Manager que no cablean la ventana siguen intactos.
	inboundMargin time.Duration
}

// Option configura un Manager en su construcción (inyección de dependencias opcionales como el
// factory de pairing). Mantiene NewManager retrocompatible: sin opciones, el esqueleto T1 intacto.
type Option func(*Manager)

// NewManager construye el Manager con el Layout, el puerto de persistencia de metadatos, el límite
// suave de sesiones (max) y el logger. No abre sockets ni restaura sesiones: eso lo hacen
// Pair/Restore en tramos posteriores. Las opciones inyectan dependencias del ciclo de vida (p.ej.
// WithWhatsmeowPairing para habilitar Manager.Pair).
func NewManager(layout Layout, sessions app.SessionStore, max int, log sharedlogger.Logger, opts ...Option) *Manager {
	m := &Manager{
		live:                  make(map[string]*liveSession),
		layout:                layout,
		sessions:              sessions,
		max:                   max,
		multiDevicePerAccount: 1, // off por defecto: un device vivo por número (design §10.F). WithMultiDevicePerAccount lo sube.
		log:                   log,
		backoffBase:           1 * time.Second,
		backoffMax:            60 * time.Second,
	}
	// Resuelve UNA vez las capacidades opcionales del store (Plan 027 T4, H4): interface-upgrade en el
	// wiring, no type-assert repetido en cada borrado del runtime. nil si el store no las implementa.
	m.cascade, _ = sessions.(app.DeviceCascadeStore)
	m.account, _ = sessions.(app.AccountStore)
	for _, o := range opts {
		o(m)
	}
	return m
}

// WithKeyCustodyFactory inyecta el constructor de custodia de DEK por sesión (DIP, Plan 035).
func WithKeyCustodyFactory(fn func(path string) app.KeyCustody) Option {
	return func(m *Manager) {
		if fn != nil {
			m.newCustody = fn
		}
	}
}

// WithListenerBackoff ajusta la política de reintento de los listeners caídos (aislamiento §10.H). En
// producción se usan los defaults (1s/60s); los tests inyectan valores minúsculos para ejercitar el
// reintento de forma determinista y rápida (sin esperas reales). base y max deben ser > 0.
func WithListenerBackoff(base, max time.Duration) Option {
	return func(m *Manager) {
		if base > 0 {
			m.backoffBase = base
		}
		if max > 0 {
			m.backoffMax = max
		}
	}
}

// (aquí vivió `WithInboundDecorator`. Retirada en el Plan 051 Ola 3 · T3.0: era la costura por la que el
// decorador de intenciones se metía en el camino del listener, y ese camino ya no entrega nada. NO la
// resucites para "enriquecer" entregas: lo que haya que añadir a un entrante se añade en la cola, que es
// donde el cajero y el despachador se encuentran, no en el hilo de whatsmeow — INV-051.2.)

// WithInboundMargin fija el MARGEN de la ventana temporal de ingesta (ADR-0037) para todas las sesiones:
// se descarta en la puerta lo anterior a `inicioDeConexión − margen`, y eso no sube a la nube. En
// producción lo cablea el daemon desde WAPP_AGENT_INBOUND_MARGIN_SECONDS. Un valor <=0 se IGNORA (no
// desactiva nada): manda el default del Listener, porque el margen es lo que absorbe el desfase de reloj.
func WithInboundMargin(d time.Duration) Option {
	return func(m *Manager) {
		if d > 0 {
			m.inboundMargin = d
		}
	}
}

// WithColaEntrantes inyecta la COLA DURABLE DE ENTRANTES compartida (Plan 051 Ola 1): cada listener anota
// el entrante en disco —cifrado con la DEK de SU sesión— ANTES de entregarlo al sink, de modo que el acuse
// de WhatsApp salga con el mensaje ya durable. En producción la cablea el daemon desde wiring.BuildCola, y
// desde el 2026-08-17 ese daemon NO ARRANCA si no puede construirla, así que en campo esta opción llega
// siempre con valor. nil se IGNORA (los tests cablean Managers sin cola): el Manager nunca impide arrancar
// una sesión por esto, porque la política de arranque del proceso no es suya.
func WithColaEntrantes(c app.ColaEntrantes) Option {
	return func(m *Manager) {
		if c != nil {
			m.cola = c
		}
	}
}

// WithClasificadorActivo inyecta el LECTOR del interruptor del clasificador de intenciones (Plan 051 Ola 2,
// T2.12): cada listener lo consulta al encolar para que un entrante que llega con la feature apagada nazca
// ya resuelto (`apagado`) en vez de gastar una plaza del semáforo del cajero y acabar descartado igual. En
// producción lo cablea el daemon desde el stack de intent (wiring.IntentStack); los tests que no lo pasan
// quedan con el default del Listener. Se pide un `func() bool` y no un bool para que la feature pueda
// apagarse en caliente sin que el Manager quede con la foto del arranque. nil se IGNORA (default ACTIVO:
// clasificar de más es barato y visible; dejar de clasificar en silencio, no).
func WithClasificadorActivo(fn func() bool) Option {
	return func(m *Manager) {
		if fn != nil {
			m.clasificadorActivo = fn
		}
	}
}

// WithLatencia inyecta el CRONÓMETRO del handler de entrantes (Plan 051 Ola 3 · T3.13): el histograma
// COMPARTIDO donde cada listener acumula cuánto tarda su onMessage. Es lo que hace medible el criterio de
// cierre de la ola —«handler < 50 ms p99», INV-051.2—, que hasta esta tarea no tenía instrumento.
//
// Uno solo para todo el Edge, no uno por sesión: el criterio es del Edge. Un p99 por sesión con 3 mensajes
// cada una no responde la pregunta, y fusionar histogramas después sería trabajo para nada.
//
// nil se IGNORA (los tests cablean Managers sin cronómetro) y los listeners quedan sin medir, que es
// exactamente el comportamiento anterior a T3.13: la observabilidad nunca impide arrancar una sesión.
func WithLatencia(h *latencia.Histograma) Option {
	return func(m *Manager) {
		if h != nil {
			m.latencia = h
		}
	}
}

// WithMultiDevicePerAccount fija el cupo de DISPOSITIVOS VIVOS por CUENTA (número) del failover multi-
// dispositivo (Plan 022 T5, design §6/§10.F). En producción lo cablea cmd/agent desde
// WAPP_AGENT_MULTIDEVICE_PER_ACCOUNT; los tests lo suben para ejercitar 2 devices vivos + promoción. Se
// CLAMP a [1,4] (1 = off, comportamiento actual; 4 = tope de WhatsApp). CAVEAT (§10.F): multi-device es
// RESILIENCIA, NO SIGILO — más companions = más huella; por eso el default (NewManager) es 1.
func WithMultiDevicePerAccount(n int) Option {
	return func(m *Manager) {
		if n < 1 {
			n = 1
		}
		if n > 4 {
			n = 4
		}
		m.multiDevicePerAccount = n
	}
}

// WithHealthRegistry inyecta el registro de salud de runtime por sesión (Plan 031 T6): el Manager liga
// cada listener a su SessionReporter (prueba de vida del socket, duración de carga de DEK, edad del último
// entrante) para que T7 lo consuma en el heartbeat y el plano de control lo exponga en GET /v1/health. Sin
// esta opción (tests) el registro es nil y el reporte es no-op (health.Registry.For es nil-safe). nil se ignora.
func WithHealthRegistry(reg *health.Registry) Option {
	return func(m *Manager) {
		if reg != nil {
			m.health = reg
		}
	}
}

// WithSharedDB inyecta la BD ÚNICA COMPARTIDA del Edge y su dialecto (Plan 022 T3, decisión §10.A). Es
// la costura que "enciende" el modelo BD única en runtime: el pairing construye su Container per-device
// sobre ESTA db, el listener carga cada device por SU JID sobre ELLA, y el borrado quirúrgico purga el
// material cifrado (msg_enc_*/whatsmeow_*) aquí. En producción la abre/cierra el daemon (apagado ordenado);
// el Manager solo la referencia. Sin esta opción (tests con factories fake) el Manager opera sin BD real:
// el borrado del material cifrado se OMITE de forma segura (la limpieza de DEK/metadatos sigue vigente).
func WithSharedDB(db *sql.DB, dialect string) Option {
	return func(m *Manager) {
		m.db = db
		m.dbDialect = dialect
	}
}

// deleteDeviceMeta borra la fila de metadatos del dispositivo id purgando la cuenta si queda vacía
// (DeleteDeviceCascade) cuando el store soporta esa capacidad (app.DeviceCascadeStore, resuelta en
// NewManager); si no (fakes en memoria), cae al Delete simple del puerto app.SessionStore. Compartido por
// Unlink y cleanupPairing (cero huérfanos de metadatos).
func (m *Manager) deleteDeviceMeta(ctx context.Context, id string) error {
	if m.cascade != nil {
		return m.cascade.DeleteDeviceCascade(ctx, id)
	}
	return m.sessions.Delete(ctx, id)
}

// custodyFor resuelve la custodia DEK de UNA sesión: NewFileCustody apuntando a Layout.DEKPath(id)
// (R1, design §3). Cada sesión obtiene SU custodia (multi-entrada), no una global compartida. Devuelve
// error si id no es un session_id válido (el Layout lo valida para no escapar de data_dir).
func (m *Manager) custodyFor(id string) (app.KeyCustody, error) {
	path, err := m.layout.DEKPath(id)
	if err != nil {
		return nil, err
	}
	if m.newCustody == nil {
		return nil, fmt.Errorf("sessionmgr: falta inyectar KeyCustodyFactory (WithKeyCustodyFactory)")
	}
	return m.newCustody(path), nil
}

// List devuelve los metadatos de las sesiones VIVAS (registro en RAM). Vacío al arranque, antes de
// restaurar/parear. Para el inventario persistido completo (incluye 'pairing' aún no viva) usar
// Persisted.
func (m *Manager) List() []domain.Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]domain.Session, 0, len(m.live))
	for _, s := range m.live {
		out = append(out, s.meta)
	}
	return out
}

// Persisted devuelve TODAS las sesiones registradas en el store de metadatos (no solo las vivas). Es
// la fuente del inventario para el plano de control (GET /v1/sessions, T6). Solo lectura.
func (m *Manager) Persisted(ctx context.Context) ([]domain.Session, error) {
	return m.sessions.List(ctx)
}

// Capacity devuelve el límite suave de sesiones simultáneas (design §10.G). El plano de control lo
// usa para reportar/gatear POST /pair por encima del límite (T3/T6).
func (m *Manager) Capacity() int { return m.max }

// Health devuelve la salud de runtime del listener de una sesión viva (design §10.H): listening si
// escucha, degraded si cayó y reintenta, stopped tras el apagado. ok=false si el session_id no está
// vivo. El plano de control (T6) lo usa para reportar el estado real por sesión.
func (m *Manager) Health(id string) (SessionHealth, bool) {
	m.mu.Lock()
	s, ok := m.live[id]
	m.mu.Unlock()
	if !ok {
		return HealthStopped, false
	}
	h, _ := s.snapshot()
	return h, true
}

// Stop realiza el apagado ORDENADO (design §10.I): cancela el context de cada listener vivo y espera a
// que TODAS las goroutines terminen (WaitGroup), momento en que cada una ya cerró su *sql.DB vía defer.
// Snapshotea las sesiones bajo el lock y cancela FUERA de él para no sostener m.mu mientras corren los
// cancels (y para que cancel se lea bajo el lock de la propia liveSession, sin carrera con startListener).
// Sin listeners arrancados es un no-op seguro (WaitGroup vacío).
func (m *Manager) Stop() {
	m.mu.Lock()
	m.log.Info("deteniendo session manager", "sesiones_vivas", len(m.live))
	sessions := make([]*liveSession, 0, len(m.live))
	for _, s := range m.live {
		sessions = append(sessions, s)
	}
	m.mu.Unlock()

	for _, s := range sessions {
		s.log.Info("deteniendo listener de sesión")
		s.stop()
	}
	m.wg.Wait()
}
