package sessionmgr

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
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
	// Persisted(). El runtime T3 aprovecha además sus métodos por-dispositivo/cuenta vía type-assert
	// (cascadeStore/accountStore) para el borrado quirúrgico sin huérfanos.
	sessions app.SessionStore

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

	// backoffBase/backoffMax acotan la política de reintento de un listener caído (aislamiento §10.H):
	// retroceso exponencial Base·2^n saturado en Max. Defaults 1s/60s; los tests inyectan valores
	// minúsculos (WithListenerBackoff) para no depender de esperas reales.
	backoffBase time.Duration
	backoffMax  time.Duration
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
		// Default de producción: el backend real de custodia (Keychain en darwin, archivo en el resto),
		// seleccionado por build-tag en keycustody. Un wrapper porque NewFileCustody devuelve un tipo
		// concreto y el campo es del puerto app.KeyCustody. Los tests lo sustituyen por un doble en memoria.
		newCustody: func(path string) app.KeyCustody { return keycustody.NewFileCustody(path) },
	}
	for _, o := range opts {
		o(m)
	}
	return m
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

// cascadeStore es el subconjunto del sessionstore concreto para el borrado transaccional POR DISPOSITIVO
// (device + cuenta vacía en una tx, sin huérfanos). El puerto app.SessionStore solo expone Delete; el
// runtime T3 prefiere DeleteDeviceCascade cuando el store lo implementa (type-assert), y cae a Delete con
// los fakes en memoria de los tests (que no purgan la cuenta).
type cascadeStore interface {
	DeleteDeviceCascade(ctx context.Context, sessionID string) error
}

// accountStore es el subconjunto del sessionstore concreto para el borrado POR CUENTA (número): listar los
// dispositivos de la cuenta y borrarlos junto a la cuenta en una tx. Lo usa Manager.UnlinkAccount vía
// type-assert; los fakes en memoria no lo implementan (UnlinkAccount devuelve error claro con ellos).
type accountStore interface {
	GetByAccount(ctx context.Context, accountID string) ([]domain.Session, error)
	DeleteByAccount(ctx context.Context, accountID string) error
}

// deleteDeviceMeta borra la fila de metadatos del dispositivo id purgando la cuenta si queda vacía
// (DeleteDeviceCascade) cuando el store lo soporta; si no (fakes en memoria), cae al Delete simple del
// puerto app.SessionStore. Compartido por Unlink y cleanupPairing (cero huérfanos de metadatos).
func (m *Manager) deleteDeviceMeta(ctx context.Context, id string) error {
	if cs, ok := m.sessions.(cascadeStore); ok {
		return cs.DeleteDeviceCascade(ctx, id)
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
