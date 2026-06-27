package sessionmgr

import (
	"context"
	"sync"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Manager es el registro vivo de N sesiones del Edge (design §1). Posee el ciclo de vida de cada
// sesión y, dado un session_id, resuelve su custodia/store/cliente/listener. Concurrencia sin broker
// (ADR-0003): un map protegido por sync.Mutex y, a partir de T4, una goroutine listener por sesión
// gobernada por context.Context con su cancel + un WaitGroup para el apagado ordenado (design §10.I).
//
// ESQUELETO T1: aquí va el armazón (map + mutex + custodia por sesión + apagado). La lógica de
// Pair/Restore/Unlink llega en T3/T4/T5; este tipo NO la implementa todavía.
type Manager struct {
	// mu protege live (y, en T4+, los arranques/paradas de listeners).
	mu sync.Mutex
	// live indexa las sesiones VIVAS por session_id. Vacío hasta que Pair/Restore (T3/T4) lo pueblen.
	live map[string]*liveSession
	// wg espera a que todas las goroutines listener terminen en el apagado (Stop).
	wg sync.WaitGroup

	// layout es la única fuente de rutas por sesión (sessions/<id>/{store.db,dek.key}).
	layout Layout
	// sessions persiste los metadatos de negocio (tabla sessions_v2); fuente para Persisted().
	sessions app.SessionStore
	// max es el límite suave de sesiones simultáneas (WAPP_AGENT_MAX_SESSIONS, design §10.G).
	max int
	// log es el logger raíz; cada liveSession derivará un hijo con session_id/jid (design §10.J).
	log sharedlogger.Logger

	// newPairer construye, para UNA sesión, el caso de uso de pairing sobre su custodia y su store
	// (design §5). Se inyecta por opción (WithWhatsmeowPairing en producción; un fake en los tests)
	// para que Manager.Pair sea testeable sin WhatsApp. nil hasta que se configure: Pair lo exige.
	newPairer pairFactory
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
		live:     make(map[string]*liveSession),
		layout:   layout,
		sessions: sessions,
		max:      max,
		log:      log,
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

// custodyFor resuelve la custodia DEK de UNA sesión: NewFileCustody apuntando a Layout.DEKPath(id)
// (R1, design §3). Cada sesión obtiene SU custodia (multi-entrada), no una global compartida. Devuelve
// error si id no es un session_id válido (el Layout lo valida para no escapar de data_dir).
func (m *Manager) custodyFor(id string) (app.KeyCustody, error) {
	path, err := m.layout.DEKPath(id)
	if err != nil {
		return nil, err
	}
	return keycustody.NewFileCustody(path), nil
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

// Stop realiza el apagado ORDENADO (design §10.I): cancela el listener de cada sesión viva y espera a
// que todas las goroutines terminen (WaitGroup). En el esqueleto T1 no hay listeners arrancados, así
// que es un no-op seguro; la estructura queda lista para cuando T4 arranque goroutines por sesión.
func (m *Manager) Stop() {
	m.mu.Lock()
	m.log.Info("deteniendo session manager", "sesiones_vivas", len(m.live))
	for _, s := range m.live {
		if s.log != nil {
			s.log.Info("deteniendo listener de sesión")
		}
		if s.cancel != nil {
			s.cancel()
		}
	}
	m.mu.Unlock()
	m.wg.Wait()
}
