package sessionmgr

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// --- fakes (sin red, sin WhatsApp, sin BD): controlan la escucha por channels para tests deterministas ---

// fakeStore implementa app.SessionStore en memoria; ListActive devuelve lo cargado en `active`.
type fakeStore struct {
	mu      sync.Mutex
	active  []domain.Session
	listErr error
}

func (f *fakeStore) Upsert(context.Context, domain.Session) error { return nil }
func (f *fakeStore) List(context.Context) ([]domain.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active, nil
}
func (f *fakeStore) ListActive(context.Context) ([]domain.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.active, nil
}
func (f *fakeStore) Get(context.Context, string) (domain.Session, error) {
	return domain.Session{}, errors.New("no usado")
}
func (f *fakeStore) Delete(context.Context, string) error { return nil }

// fakeCloser cuenta los Close() para verificar que el apagado ordenado cierra el store de cada sesión.
type fakeCloser struct{ fab *fakeFabric }

func (c *fakeCloser) Close() error { c.fab.closes.Add(1); return nil }

// blockRunner simula la escucha sana: BLOQUEA hasta que el ctx se cancele (apagado), devolviendo nil
// (como app.Listen → ListenGateway al cancelarse limpio).
type blockRunner struct{}

func (blockRunner) Run(ctx context.Context) error { <-ctx.Done(); return nil }

// panicRunner simula un listener que REVIENTA (pánico): debe ser recuperado por runListenOnce sin
// derribar el proceso ni a las otras sesiones (aislamiento §10.H).
type panicRunner struct{}

func (panicRunner) Run(context.Context) error { panic("listener boom (simulado)") }

// errRunner simula una caída por error de conexión: Run devuelve el error de inmediato.
type errRunner struct{ err error }

func (e errRunner) Run(context.Context) error { return e.err }

var errListenerDown = errors.New("socket caído (simulado)")

// fakeFabric es el factory inyectado en el Manager (en vez de WithWhatsmeowListen). Decide, por sesión
// y por número de llamada (1-based), qué runner devolver, y opcionalmente BLOQUEA el arranque del
// reintento (call>=2) en un gate hasta que el test lo libere — eso permite observar la sesión en
// 'degraded' de forma determinista (sin depender de ventanas de tiempo).
type fakeFabric struct {
	mu        sync.Mutex
	calls     map[string]int
	panicCall map[string]int           // sesión -> índice de llamada que paniquea (0 = ninguna)
	failCall  map[string]int           // sesión -> índice de llamada que devuelve error (0 = ninguna)
	retryGate map[string]chan struct{} // sesión -> gate que bloquea el reintento (call>=2) hasta cerrarse
	closes    atomic.Int32
}

func newFakeFabric() *fakeFabric {
	return &fakeFabric{
		calls:     map[string]int{},
		panicCall: map[string]int{},
		failCall:  map[string]int{},
		retryGate: map[string]chan struct{}{},
	}
}

func (f *fakeFabric) factory(ctx context.Context, s *liveSession) (listenRunner, io.Closer, error) {
	id := s.meta.SessionID
	f.mu.Lock()
	f.calls[id]++
	n := f.calls[id]
	gate := f.retryGate[id]
	pc := f.panicCall[id]
	fc := f.failCall[id]
	f.mu.Unlock()

	// Gate del reintento: mantiene la sesión en 'degraded' hasta que el test lo libere (o se cancele).
	if gate != nil && n >= 2 {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}

	closer := &fakeCloser{fab: f}
	switch {
	case pc == n:
		return panicRunner{}, closer, nil
	case fc == n:
		return errRunner{err: errListenerDown}, closer, nil
	default:
		return blockRunner{}, closer, nil
	}
}

func (f *fakeFabric) callCount(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[id]
}

// newListenManager arma un Manager con un fakeStore y el fakeFabric inyectado como factory de escucha,
// con backoff minúsculo para que los reintentos sean rápidos y deterministas.
func newListenManager(t *testing.T, active []domain.Session) (*Manager, *fakeFabric) {
	t.Helper()
	store := &fakeStore{active: active}
	m := NewManager(NewLayout(t.TempDir()), store, 5, testLogger(),
		WithListenerBackoff(1*time.Millisecond, 5*time.Millisecond))
	fab := newFakeFabric()
	m.newListener = fab.factory
	return m, fab
}

// activeSession construye una fila 'active' con un session_id UUID válido (custodyFor lo exige).
func activeSession(id, jid string) domain.Session {
	return domain.Session{SessionID: id, JID: jid, State: domain.SessionStateActive}
}

// waitForHealth espera (con timeout) a que la salud de la sesión id sea want; falla si no llega.
func waitForHealth(t *testing.T, m *Manager, id string, want SessionHealth) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h, ok := m.Health(id); ok && h == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	got, _ := m.Health(id)
	t.Fatalf("salud de %s = %v, esperaba %v (timeout)", id, got, want)
}

// --- tests ---

// TestManager_Restore_StartsListenerPerSession (DoD T4: ≥2 sesiones activas → 2 listeners, ambos
// escuchando, una goroutine por sesión). Verifica además el apagado ordenado: Stop cierra cada store.
func TestManager_Restore_StartsListenerPerSession(t *testing.T) {
	m, fab := newListenManager(t, []domain.Session{
		activeSession(uuidA, "a@s.whatsapp.net"),
		activeSession(uuidB, "b@s.whatsapp.net"),
	})

	if err := m.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Ambas sesiones escuchando (un listener por sesión).
	waitForHealth(t, m, uuidA, HealthListening)
	waitForHealth(t, m, uuidB, HealthListening)

	if got := len(m.List()); got != 2 {
		t.Fatalf("List() debería tener 2 sesiones vivas, got %d", got)
	}
	if c := fab.callCount(uuidA); c != 1 {
		t.Fatalf("la sesión A debería haber arrancado 1 listener, got %d", c)
	}

	// Apagado ordenado: Stop cancela ambos, espera el WaitGroup y cierra los 2 stores.
	m.Stop()
	if got := fab.closes.Load(); got != 2 {
		t.Fatalf("Stop debería cerrar 2 stores, cerró %d", got)
	}
	if h, _ := m.Health(uuidA); h != HealthStopped {
		t.Fatalf("tras Stop, A debería estar stopped, got %v", h)
	}
}

// TestManager_Restore_PanicIsolation (DoD T4: un listener panica → los otros siguen vivos; el caído se
// marca degradado y reintenta con backoff). El gate de reintento mantiene A en 'degraded' de forma
// determinista para poder afirmarlo, sin tumbar el proceso ni a B.
func TestManager_Restore_PanicIsolation(t *testing.T) {
	m, fab := newListenManager(t, []domain.Session{
		activeSession(uuidA, "a@s.whatsapp.net"),
		activeSession(uuidB, "b@s.whatsapp.net"),
	})

	// A revienta en su 1er arranque; su reintento (call#2) queda retenido en el gate hasta liberarlo.
	gate := make(chan struct{})
	fab.mu.Lock()
	fab.panicCall[uuidA] = 1
	fab.retryGate[uuidA] = gate
	fab.mu.Unlock()

	if err := m.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// B escucha sin verse afectada por el pánico de A (aislamiento).
	waitForHealth(t, m, uuidB, HealthListening)
	// A cayó (pánico recuperado) y está reintentando: marcada degradada (retenida en el gate).
	waitForHealth(t, m, uuidA, HealthDegraded)
	// B sigue intacta tras el fallo de A.
	if h, _ := m.Health(uuidB); h != HealthListening {
		t.Fatalf("el pánico de A afectó a B: B=%v", h)
	}

	// Liberamos el reintento de A: vuelve a escuchar (recuperación con backoff).
	close(gate)
	waitForHealth(t, m, uuidA, HealthListening)
	if c := fab.callCount(uuidA); c < 2 {
		t.Fatalf("A debería haber reintentado (≥2 arranques), got %d", c)
	}

	m.Stop()
}

// TestManager_Restore_ErrorDegradesAndRecovers: una caída por ERROR (no pánico) marca la sesión
// degradada y reintenta; al recuperarse vuelve a 'listening' y limpia la causa. Cubre el camino de
// error de conexión del aislamiento §10.H sin involucrar pánico.
func TestManager_Restore_ErrorDegradesAndRecovers(t *testing.T) {
	m, fab := newListenManager(t, []domain.Session{activeSession(uuidA, "a@s.whatsapp.net")})

	gate := make(chan struct{})
	fab.mu.Lock()
	fab.failCall[uuidA] = 1 // 1er arranque devuelve error
	fab.retryGate[uuidA] = gate
	fab.mu.Unlock()

	if err := m.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	waitForHealth(t, m, uuidA, HealthDegraded)
	if _, lastErr := mustLive(t, m, uuidA).snapshot(); !errors.Is(lastErr, errListenerDown) {
		t.Fatalf("la sesión degradada debería registrar la causa, got %v", lastErr)
	}

	close(gate)
	waitForHealth(t, m, uuidA, HealthListening)
	if _, lastErr := mustLive(t, m, uuidA).snapshot(); lastErr != nil {
		t.Fatalf("al recuperarse, la causa debería limpiarse, got %v", lastErr)
	}

	m.Stop()
}

// TestManager_Restore_NotConfigured: Restore sin factory de escucha es un error de cableado claro.
func TestManager_Restore_NotConfigured(t *testing.T) {
	m := NewManager(NewLayout(t.TempDir()), &fakeStore{}, 5, testLogger())
	if err := m.Restore(context.Background()); !errors.Is(err, ErrListenNotConfigured) {
		t.Fatalf("Restore sin escucha debería dar ErrListenNotConfigured, got %v", err)
	}
}

// TestManager_Restore_ListError: un fallo al listar las activas se propaga (sin arrancar listeners).
func TestManager_Restore_ListError(t *testing.T) {
	sentinel := errors.New("disco caído")
	store := &fakeStore{listErr: sentinel}
	m := NewManager(NewLayout(t.TempDir()), store, 5, testLogger())
	fab := newFakeFabric()
	m.newListener = fab.factory

	if err := m.Restore(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("Restore debería propagar el error de ListActive, got %v", err)
	}
	if len(m.List()) != 0 {
		t.Fatal("no debería quedar ninguna sesión viva si falla ListActive")
	}
}

// mustLive devuelve la liveSession de id o falla (helper para inspeccionar lastErr).
func mustLive(t *testing.T, m *Manager, id string) *liveSession {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.live[id]
	if !ok {
		t.Fatalf("la sesión %s debería estar viva", id)
	}
	return s
}
