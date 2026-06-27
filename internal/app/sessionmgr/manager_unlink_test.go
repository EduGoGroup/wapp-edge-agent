package sessionmgr

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// uuidC es un tercer session_id válido (UUID) para el caso "inexistente"; uuidA/uuidB viven en
// layout_test.go.
const uuidC = "33333333-3333-4333-8333-333333333333"

// unlinkStore es un app.SessionStore en memoria con Get/Delete FUNCIONALES (a diferencia del fakeStore
// de manager_listen_test.go, cuyo Delete es no-op): lo necesita el borrado quirúrgico para afirmar que
// la fila de A desaparece y la de B permanece, y que un id ausente da app.ErrSessionNotFound.
type unlinkStore struct {
	mu   sync.Mutex
	rows map[string]domain.Session
}

func newUnlinkStore(sessions ...domain.Session) *unlinkStore {
	s := &unlinkStore{rows: map[string]domain.Session{}}
	for _, sess := range sessions {
		s.rows[sess.SessionID] = sess
	}
	return s
}

func (s *unlinkStore) Upsert(_ context.Context, sess domain.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[sess.SessionID] = sess
	return nil
}

func (s *unlinkStore) List(context.Context) ([]domain.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Session, 0, len(s.rows))
	for _, sess := range s.rows {
		out = append(out, sess)
	}
	return out, nil
}

func (s *unlinkStore) ListActive(context.Context) ([]domain.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.Session
	for _, sess := range s.rows {
		if sess.State == domain.SessionStateActive {
			out = append(out, sess)
		}
	}
	return out, nil
}

func (s *unlinkStore) Get(_ context.Context, id string) (domain.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.rows[id]
	if !ok {
		return domain.Session{}, app.ErrSessionNotFound
	}
	return sess, nil
}

func (s *unlinkStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rows, id)
	return nil
}

func (s *unlinkStore) has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.rows[id]
	return ok
}

// seedOnDisk materializa en disco lo que el borrado quirúrgico debe limpiar: el directorio de la
// sesión, su DEK (vía la custodia real) y un store.db de relleno. Devuelve el dir de la sesión.
func seedOnDisk(t *testing.T, m *Manager, id string) string {
	t.Helper()
	dir, err := m.layout.SessionDir(id)
	if err != nil {
		t.Fatalf("SessionDir(%s): %v", id, err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	custody, err := m.custodyFor(id)
	if err != nil {
		t.Fatalf("custodyFor(%s): %v", id, err)
	}
	if err := custody.Store(bytes.Repeat([]byte{0x11}, 32)); err != nil {
		t.Fatalf("Store DEK %s: %v", id, err)
	}
	storeDB, err := m.layout.StoreDB(id)
	if err != nil {
		t.Fatalf("StoreDB(%s): %v", id, err)
	}
	if err := os.WriteFile(storeDB, []byte("store-db-relleno"), 0o600); err != nil {
		t.Fatalf("escribir store.db %s: %v", id, err)
	}
	return dir
}

// TestManager_Unlink_SurgicalIsolation (DoD T5, design §7): con 2 sesiones VIVAS (A y B), Unlink(A)
// borra TODO lo de A (listener cancelado, fila, dir, DEK) y deja a B OPERATIVA e intacta (listener
// vivo, fila/dir/DEK de B presentes). Sin daño colateral.
func TestManager_Unlink_SurgicalIsolation(t *testing.T) {
	base := t.TempDir()
	store := newUnlinkStore(
		activeSession(uuidA, "a@s.whatsapp.net"),
		activeSession(uuidB, "b@s.whatsapp.net"),
	)
	m := NewManager(NewLayout(base), store, 5, testLogger(),
		WithListenerBackoff(1*time.Millisecond, 5*time.Millisecond))
	fab := newFakeFabric()
	m.newListener = fab.factory

	dirA := seedOnDisk(t, m, uuidA)
	dirB := seedOnDisk(t, m, uuidB)

	if err := m.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	waitForHealth(t, m, uuidA, HealthListening)
	waitForHealth(t, m, uuidB, HealthListening)

	// Act: borrado quirúrgico de A.
	if err := m.Unlink(context.Background(), uuidA); err != nil {
		t.Fatalf("Unlink(A): %v", err)
	}

	// --- A desaparece por completo ---
	if _, ok := m.Health(uuidA); ok {
		t.Fatal("A no debería seguir viva tras Unlink")
	}
	if store.has(uuidA) {
		t.Fatal("la fila de metadatos de A debería estar borrada")
	}
	if _, err := os.Stat(dirA); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("el directorio de A debería estar borrado, stat err=%v", err)
	}
	custA, _ := m.custodyFor(uuidA)
	if custA.Exists() {
		t.Fatal("la DEK de A debería estar borrada")
	}
	// El listener de A fue cancelado y unido: exactamente UN store cerrado (el de A). El de B sigue vivo.
	if got := fab.closes.Load(); got != 1 {
		t.Fatalf("Unlink(A) debería cerrar SOLO el store de A, cerró %d", got)
	}

	// --- B intacta y operativa ---
	if h, ok := m.Health(uuidB); !ok || h != HealthListening {
		t.Fatalf("B debería seguir escuchando, ok=%v salud=%v", ok, h)
	}
	if !store.has(uuidB) {
		t.Fatal("la fila de B no debería haberse tocado")
	}
	if _, err := os.Stat(dirB); err != nil {
		t.Fatalf("el directorio de B debería seguir existiendo, stat err=%v", err)
	}
	custB, _ := m.custodyFor(uuidB)
	if !custB.Exists() {
		t.Fatal("la DEK de B no debería haberse tocado")
	}

	// Apagado: solo queda B; Stop la cierra (segundo close).
	m.Stop()
	if got := fab.closes.Load(); got != 2 {
		t.Fatalf("tras Stop deberían haberse cerrado 2 stores en total (A en Unlink + B en Stop), got %d", got)
	}
}

// TestManager_Unlink_NotFound (DoD T5): Unlink de un session_id inexistente (ni vivo ni en metadatos)
// devuelve ErrSessionNotFound (→ 404) y NO crea ni borra nada.
func TestManager_Unlink_NotFound(t *testing.T) {
	base := t.TempDir()
	store := newUnlinkStore(activeSession(uuidA, "a@s.whatsapp.net"))
	m := NewManager(NewLayout(base), store, 5, testLogger())

	err := m.Unlink(context.Background(), uuidC)
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("Unlink de inexistente debería dar ErrSessionNotFound, got %v", err)
	}

	// Sin efectos colaterales: ni se creó el dir del inexistente, ni se tocó la fila de A.
	dirC := filepath.Join(base, "sessions", uuidC)
	if _, err := os.Stat(dirC); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Unlink not-found no debería crear el dir %s, stat err=%v", dirC, err)
	}
	if !store.has(uuidA) {
		t.Fatal("Unlink de un id ajeno no debería tocar la fila de A")
	}
}

// TestManager_Unlink_PersistedNotLive (design §7): una sesión persistida pero SIN listener vivo (p.ej.
// en 'pairing', o tras reinicio antes del restore) también se borra quirúrgicamente (fila + dir + DEK),
// sin necesidad de goroutine que unir.
func TestManager_Unlink_PersistedNotLive(t *testing.T) {
	base := t.TempDir()
	store := newUnlinkStore(domain.Session{
		SessionID: uuidA, State: domain.SessionStatePairing, StoreDir: "sessions/" + uuidA,
	})
	m := NewManager(NewLayout(base), store, 5, testLogger())

	dirA := seedOnDisk(t, m, uuidA)

	if err := m.Unlink(context.Background(), uuidA); err != nil {
		t.Fatalf("Unlink(A persistida no-viva): %v", err)
	}
	if store.has(uuidA) {
		t.Fatal("la fila de A debería estar borrada")
	}
	if _, err := os.Stat(dirA); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("el directorio de A debería estar borrado, stat err=%v", err)
	}
	custA, _ := m.custodyFor(uuidA)
	if custA.Exists() {
		t.Fatal("la DEK de A debería estar borrada")
	}
}
