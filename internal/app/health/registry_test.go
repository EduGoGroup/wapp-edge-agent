package health

import (
	"sync"
	"testing"
	"time"
)

// TestRegistry_SetAndSnapshot: los setters pueblan el snapshot por sesión; los estados sanos LIMPIAN el
// motivo (no arrastran uno viejo).
func TestRegistry_SetAndSnapshot(t *testing.T) {
	r := NewRegistry()
	now := time.Now()

	r.SetSocketState("s1", SocketDegraded, ReasonDEKLoadTimeout)
	r.SetDEKLoadDuration("s1", 250*time.Millisecond)
	r.MarkInbound("s1", now)

	snap, ok := r.Snapshot("s1")
	if !ok {
		t.Fatal("s1 debía existir")
	}
	if snap.SocketState != SocketDegraded || snap.DegradedReason != ReasonDEKLoadTimeout {
		t.Fatalf("estado/motivo inesperados: %s / %s", snap.SocketState, snap.DegradedReason)
	}
	if snap.DEKLoadDuration != 250*time.Millisecond {
		t.Fatalf("dek duration = %v", snap.DEKLoadDuration)
	}
	if !snap.LastInboundAt.Equal(now) {
		t.Fatalf("last inbound = %v, quería %v", snap.LastInboundAt, now)
	}

	// Volver a un estado sano limpia el motivo aunque pasemos uno.
	r.SetSocketState("s1", SocketConnected, "ignorado")
	snap, _ = r.Snapshot("s1")
	if snap.SocketState != SocketConnected || snap.DegradedReason != "" {
		t.Fatalf("el estado sano no limpió el motivo: %s / %q", snap.SocketState, snap.DegradedReason)
	}
}

// TestRegistry_UnknownAndRemove: una sesión desconocida no existe; Remove la borra y es idempotente.
func TestRegistry_UnknownAndRemove(t *testing.T) {
	r := NewRegistry()
	if _, ok := r.Snapshot("nope"); ok {
		t.Fatal("una sesión desconocida no debía existir")
	}
	r.SetSocketState("s1", SocketConnected, "")
	if _, ok := r.Snapshot("s1"); !ok {
		t.Fatal("s1 debía existir")
	}
	r.Remove("s1")
	if _, ok := r.Snapshot("s1"); ok {
		t.Fatal("Remove debía borrar s1")
	}
	r.Remove("s1") // idempotente: no debe entrar en pánico
}

// TestRegistry_NilSafe: un *Registry nil hace no-op en setters y devuelve (zero,false) en Snapshot, para
// que los caminos/tests sin registro cableado operen sin ramificar.
func TestRegistry_NilSafe(t *testing.T) {
	var r *Registry
	r.SetSocketState("s1", SocketConnected, "")
	r.SetDEKLoadDuration("s1", time.Second)
	r.MarkInbound("s1", time.Now())
	r.Remove("s1")
	if _, ok := r.Snapshot("s1"); ok {
		t.Fatal("un Registry nil no debe tener entradas")
	}
	// El reporter ligado a un Registry nil también es no-op.
	rep := r.For("s1")
	rep.SetSocketState(SocketDead, ReasonLoggedOut)
	rep.SetDEKLoadDuration(time.Second)
	rep.MarkInbound(time.Now())
}

// TestRegistry_ForReporter: el reporter ligado escribe en la sesión correcta del registro.
func TestRegistry_ForReporter(t *testing.T) {
	r := NewRegistry()
	rep := r.For("s9")
	rep.SetSocketState(SocketDead, ReasonLoggedOut)
	rep.SetDEKLoadDuration(3 * time.Millisecond)

	snap, ok := r.Snapshot("s9")
	if !ok {
		t.Fatal("s9 debía existir tras el reporter")
	}
	if snap.SocketState != SocketDead || snap.DegradedReason != ReasonLoggedOut {
		t.Fatalf("estado/motivo del reporter inesperados: %s / %s", snap.SocketState, snap.DegradedReason)
	}
	if snap.DEKLoadDuration != 3*time.Millisecond {
		t.Fatalf("dek duration del reporter = %v", snap.DEKLoadDuration)
	}
}

// TestRegistry_Concurrent ejercita escrituras/lecturas concurrentes (correr con -race): sin data races.
func TestRegistry_Concurrent(t *testing.T) {
	r := NewRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rep := r.For("s")
			rep.SetSocketState(SocketConnecting, ReasonReconnecting)
			rep.MarkInbound(time.Now())
			rep.SetDEKLoadDuration(time.Millisecond)
			_, _ = r.Snapshot("s")
		}()
	}
	wg.Wait()
}
