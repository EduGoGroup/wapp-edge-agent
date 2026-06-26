package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// --- fakes (sin red, sin BD) ---

// fakeSessionStore simula la tabla `sessions` en memoria.
type fakeSessionStore struct {
	mu        sync.Mutex
	list      []domain.Session
	listErr   error
	upsertErr error
	upserts   []domain.Session
}

func (f *fakeSessionStore) Upsert(_ context.Context, s domain.Session) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.upsertErr != nil {
		return f.upsertErr
	}
	f.upserts = append(f.upserts, s)
	return nil
}

func (f *fakeSessionStore) List(_ context.Context) ([]domain.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.list, nil
}

func (f *fakeSessionStore) Get(_ context.Context, jid string) (domain.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.list {
		if s.JID == jid {
			return s, nil
		}
	}
	return domain.Session{}, ErrSessionNotFound
}

func (f *fakeSessionStore) lastUpsert() (domain.Session, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.upserts) == 0 {
		return domain.Session{}, false
	}
	return f.upserts[len(f.upserts)-1], true
}

// fakeLocator simula el resolver del device pareado del store cifrado.
type fakeLocator struct {
	jid string
	ok  bool
	err error
}

func (f *fakeLocator) PairedJID(_ context.Context) (string, bool, error) {
	return f.jid, f.ok, f.err
}

// fakeRunner simula app.Listen: registra si se invocó y respeta la cancelación del ctx.
type fakeRunner struct {
	called          atomic.Bool
	runErr          error
	blockTillCancel bool
	gotCancel       error
}

func (f *fakeRunner) Run(ctx context.Context) error {
	f.called.Store(true)
	if f.runErr != nil {
		return f.runErr
	}
	if f.blockTillCancel {
		<-ctx.Done()
		f.gotCancel = ctx.Err()
	}
	return nil
}

// fixedClock devuelve siempre el mismo instante (timestamps deterministas).
func fixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

// --- tests ---

// TestRestore_FromRegistry_DelegatesAndMarksActive: con una sesión en el registro, RestoreSessions la
// marca activa (updated_at = ahora) y delega la escucha al runner, respetando la cancelación.
func TestRestore_FromRegistry_DelegatesAndMarksActive(t *testing.T) {
	now := time.Unix(1_700_000_123, 0).UTC()
	paired := time.Unix(1_600_000_000, 0).UTC()
	store := &fakeSessionStore{list: []domain.Session{
		{JID: "j@x", State: domain.SessionStateActive, PairedAt: paired, UpdatedAt: paired},
	}}
	loc := &fakeLocator{}
	run := &fakeRunner{blockTillCancel: true}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- NewRestoreSessions(store, loc, run, withClock(fixedClock(now))).Run(ctx)
	}()

	// Espera a que el runner haya sido invocado, luego cancela.
	waitFor(t, func() bool { return run.called.Load() })
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run debía retornar nil al cancelar: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run no retornó tras cancelar")
	}

	up, ok := store.lastUpsert()
	if !ok {
		t.Fatal("RestoreSessions no refrescó el metadato (sin Upsert)")
	}
	if up.JID != "j@x" || up.State != domain.SessionStateActive {
		t.Fatalf("upsert = %+v, esperaba j@x/active", up)
	}
	if !up.UpdatedAt.Equal(now) {
		t.Fatalf("UpdatedAt = %v, esperaba el reloj inyectado %v", up.UpdatedAt, now)
	}
	if up.PairedAt.Equal(now) {
		t.Fatal("PairedAt no debía cambiar al restaurar (se conserva el original)")
	}
	if !errors.Is(run.gotCancel, context.Canceled) {
		t.Fatalf("el runner no respetó la cancelación: %v", run.gotCancel)
	}
}

// TestRestore_BackfillsFromLocator: registro vacío pero device pareado en el store cifrado:
// RestoreSessions backfillea la sesión (Upsert active con el JID del locator) y delega.
func TestRestore_BackfillsFromLocator(t *testing.T) {
	now := time.Unix(1_700_000_123, 0).UTC()
	store := &fakeSessionStore{} // registro vacío
	loc := &fakeLocator{jid: "back@x", ok: true}
	run := &fakeRunner{}

	err := NewRestoreSessions(store, loc, run, withClock(fixedClock(now))).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !run.called.Load() {
		t.Fatal("el runner debía invocarse tras el backfill")
	}
	up, ok := store.lastUpsert()
	if !ok || up.JID != "back@x" || up.State != domain.SessionStateActive {
		t.Fatalf("backfill upsert = %+v (ok=%v), esperaba back@x/active", up, ok)
	}
	if !up.PairedAt.Equal(now) || !up.UpdatedAt.Equal(now) {
		t.Fatalf("timestamps de backfill = %v/%v, esperaba %v", up.PairedAt, up.UpdatedAt, now)
	}
}

// TestRestore_NoSessions: ni registro ni device pareado -> ErrNoSessions, sin invocar al runner.
func TestRestore_NoSessions(t *testing.T) {
	store := &fakeSessionStore{}
	loc := &fakeLocator{ok: false}
	run := &fakeRunner{}
	err := NewRestoreSessions(store, loc, run).Run(context.Background())
	if !errors.Is(err, ErrNoSessions) {
		t.Fatalf("error = %v, esperaba ErrNoSessions", err)
	}
	if run.called.Load() {
		t.Fatal("el runner NO debía invocarse sin sesión")
	}
}

// TestRestore_LoggedOut: una sesión marcada loggedout no se restaura (ErrSessionLoggedOut), sin runner.
func TestRestore_LoggedOut(t *testing.T) {
	store := &fakeSessionStore{list: []domain.Session{
		{JID: "j@x", State: domain.SessionStateLoggedOut},
	}}
	run := &fakeRunner{}
	err := NewRestoreSessions(store, &fakeLocator{}, run).Run(context.Background())
	if !errors.Is(err, ErrSessionLoggedOut) {
		t.Fatalf("error = %v, esperaba ErrSessionLoggedOut", err)
	}
	if run.called.Load() {
		t.Fatal("el runner NO debía invocarse con la sesión cerrada")
	}
}

// TestRestore_ListError: un fallo al listar el registro se propaga, sin invocar al runner.
func TestRestore_ListError(t *testing.T) {
	sentinel := errors.New("disco caído")
	store := &fakeSessionStore{listErr: sentinel}
	run := &fakeRunner{}
	err := NewRestoreSessions(store, &fakeLocator{}, run).Run(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, esperaba envolver %v", err, sentinel)
	}
	if run.called.Load() {
		t.Fatal("el runner NO debía invocarse si falla List")
	}
}

// TestRestore_LocatorError: un fallo del locator (al backfillear) se propaga, sin invocar al runner.
func TestRestore_LocatorError(t *testing.T) {
	sentinel := errors.New("store ilegible")
	store := &fakeSessionStore{} // vacío -> intenta backfill
	loc := &fakeLocator{err: sentinel}
	run := &fakeRunner{}
	err := NewRestoreSessions(store, loc, run).Run(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, esperaba envolver %v", err, sentinel)
	}
	if run.called.Load() {
		t.Fatal("el runner NO debía invocarse si falla el locator")
	}
}

// TestRestore_UpsertError: un fallo al refrescar el metadato se propaga ANTES de delegar al runner.
func TestRestore_UpsertError(t *testing.T) {
	sentinel := errors.New("no se pudo escribir")
	store := &fakeSessionStore{
		list:      []domain.Session{{JID: "j@x", State: domain.SessionStateActive}},
		upsertErr: sentinel,
	}
	run := &fakeRunner{}
	err := NewRestoreSessions(store, &fakeLocator{}, run).Run(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, esperaba envolver %v", err, sentinel)
	}
	if run.called.Load() {
		t.Fatal("el runner NO debía invocarse si falla el Upsert")
	}
}

// TestRestore_RunnerError: un fallo del runner (p.ej. custody sin DEK o error de conexión, que
// app.Listen propaga) se propaga envuelto.
func TestRestore_RunnerError(t *testing.T) {
	sentinel := errors.New("sin DEK custodiada")
	store := &fakeSessionStore{list: []domain.Session{
		{JID: "j@x", State: domain.SessionStateActive},
	}}
	run := &fakeRunner{runErr: sentinel}
	err := NewRestoreSessions(store, &fakeLocator{}, run).Run(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, esperaba envolver %v", err, sentinel)
	}
	if !run.called.Load() {
		t.Fatal("el runner debía haberse invocado")
	}
}

// TestListenSatisfiesSessionRunner: *Listen cumple el contrato SessionRunner (sin duplicar conexión).
func TestListenSatisfiesSessionRunner(t *testing.T) {
	var _ SessionRunner = (*Listen)(nil)
}
