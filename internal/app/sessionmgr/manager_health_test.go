package sessionmgr

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// waitForSocketState espera (con timeout) a que el registro de salud reporte el estado/motivo esperados
// para la sesión id; falla si no llegan. Evita depender de ventanas de tiempo fijas.
func waitForSocketState(t *testing.T, reg *health.Registry, id string, wantState health.SocketState, wantReason string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if snap, ok := reg.Snapshot(id); ok && snap.SocketState == wantState && snap.DegradedReason == wantReason {
			return
		}
		time.Sleep(time.Millisecond)
	}
	snap, _ := reg.Snapshot(id)
	t.Fatalf("salud de socket de %s = %s/%q, esperaba %s/%q (timeout)", id, snap.SocketState, snap.DegradedReason, wantState, wantReason)
}

// TestManager_Health_DEKTimeoutDegradesWithReason: cuando el listener cae con app.ErrDEKLoadTimeout, el
// registro de salud marca el socket DEGRADED con el motivo dek_load_timeout (regla T6: ningún "sano" sin
// prueba de vida, y toda degradación con motivo clasificado y observable).
func TestManager_Health_DEKTimeoutDegradesWithReason(t *testing.T) {
	reg := health.NewRegistry()
	store := &fakeStore{active: []domain.Session{activeSession(uuidA, "a@s.whatsapp.net")}}
	m := NewManager(NewLayout(t.TempDir()), store, 5, testLogger(),
		WithListenerBackoff(1*time.Millisecond, 5*time.Millisecond),
		WithHealthRegistry(reg))
	m.newCustody = newMemCustodyFactory()

	// Factory: el 1er arranque cae con ErrDEKLoadTimeout; los reintentos quedan retenidos en el gate para
	// poder observar el estado degradado de forma determinista.
	gate := make(chan struct{})
	var calls int
	m.newListener = func(ctx context.Context, s *liveSession) (listenRunner, io.Closer, error) {
		calls++
		if calls == 1 {
			return errRunner{err: app.ErrDEKLoadTimeout}, nil, nil
		}
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
		return blockRunner{}, nil, nil
	}

	if err := m.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// El registro debe reflejar el socket degradado con el motivo dek_load_timeout.
	waitForSocketState(t, reg, uuidA, health.SocketDegraded, health.ReasonDEKLoadTimeout)

	// Al liberar el reintento, la sesión vuelve a escuchar (la salud del socket la pondrá 'connected' el
	// listener whatsmeow real vía eventos; aquí basta con no seguir degradado por el timeout de DEK).
	close(gate)
	waitForHealth(t, m, uuidA, HealthListening)

	m.Stop()

	// Al no desvincular, la entrada sigue; Unlink la quitaría (Remove). Verificamos que el snapshot existe.
	if _, ok := reg.Snapshot(uuidA); !ok {
		t.Fatal("la sesión debía tener entrada de salud tras el ciclo")
	}
}

// TestManager_Health_StartMarksConnecting: al arrancar un listener (aún sin socket vivo) el registro marca
// connecting; y Unlink borra la entrada de salud (Remove).
func TestManager_Health_StartMarksConnecting(t *testing.T) {
	reg := health.NewRegistry()
	store := &fakeStore{active: []domain.Session{activeSession(uuidA, "a@s.whatsapp.net")}}
	m := NewManager(NewLayout(t.TempDir()), store, 5, testLogger(),
		WithListenerBackoff(1*time.Millisecond, 5*time.Millisecond),
		WithHealthRegistry(reg))
	m.newCustody = newMemCustodyFactory()
	fab := newFakeFabric()
	m.newListener = fab.factory

	if err := m.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	// El runner sano (blockRunner) no emite eventos: el estado se queda en connecting (arrancando, sin
	// prueba de vida del socket todavía).
	waitForSocketState(t, reg, uuidA, health.SocketConnecting, "")

	if err := m.Unlink(context.Background(), uuidA); err != nil {
		t.Fatalf("Unlink: %v", err)
	}
	if _, ok := reg.Snapshot(uuidA); ok {
		t.Fatal("Unlink debía borrar la entrada de salud (Remove)")
	}

	m.Stop()
}
