package whatsmeow

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/store"
)

// TestStartConnection_BadDEK_Error: una DEK de tamaño inválido falla al construir el container
// cifrado (envelope rechaza != 32 B), y StartConnection devuelve error SIN abrir canal ni conectar
// (no se filtra nada, no se toca la BD).
func TestStartConnection_BadDEK_Error(t *testing.T) {
	// db/dialecto no se llegan a tocar: la DEK inválida (!= 32 bytes) falla al construir el envelope antes.
	c := NewConnector(nil, "sqlite")
	_, err := c.StartConnection(context.Background(), []byte("corta"))
	if err == nil {
		t.Fatal("StartConnection con DEK inválida debía fallar")
	}
}

// TestConnector_FactoryWiring: el Connector con una fábrica de container inyectada construye el
// container con la DEK dada. Verifica el cableado (DEK -> factory) sin red ni BD reales; el
// container fake NO es un *cryptoContainer, así que NewDeviceForPairing entra en panic y paramos ahí.
func TestConnector_FactoryWiring(t *testing.T) {
	var gotDEK []byte
	c := newConnectorWithFactory(func(_ context.Context, dek []byte) (store.DeviceContainer, error) {
		gotDEK = append([]byte(nil), dek...)
		return fakeContainer{}, nil
	})

	dek := make([]byte, 32)
	for i := range dek {
		dek[i] = byte(i + 1)
	}

	func() {
		defer func() { _ = recover() }() // NewDeviceForPairing entra en panic con un container ajeno.
		_, _ = c.StartConnection(context.Background(), dek)
	}()

	if len(gotDEK) != 32 {
		t.Fatalf("la fábrica recibió una DEK de %d bytes, esperaba 32", len(gotDEK))
	}
	for i := range gotDEK {
		if gotDEK[i] != byte(i+1) {
			t.Fatalf("la DEK llegó alterada a la fábrica en el byte %d", i)
		}
	}
}

// TestWaitActivation_ConnectedBeforeDisconnect: tras un "success", run() NO debe desconectar hasta
// que llegue el *events.Connected de la reconexión post-pairing. Con connectedCh cerrado (Connected
// llegó) waitActivation retorna PRONTO, sin agotar el activationTimeout.
func TestWaitActivation_ConnectedBeforeDisconnect(t *testing.T) {
	c := &Connector{activationTimeout: 2 * time.Second}
	connectedCh := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(connectedCh)
	}()

	start := time.Now()
	c.waitActivation(context.Background(), connectedCh)
	if elapsed := time.Since(start); elapsed >= c.activationTimeout {
		t.Fatalf("waitActivation esperó hasta el timeout (%v) en vez de retornar al llegar Connected", elapsed)
	}
}

// TestWaitActivation_TimeoutWhenNoConnected: si la reconexión nunca emite Connected, waitActivation
// no cuelga: retorna al vencer activationTimeout.
func TestWaitActivation_TimeoutWhenNoConnected(t *testing.T) {
	c := &Connector{activationTimeout: 30 * time.Millisecond}
	connectedCh := make(chan struct{}) // nunca se cierra

	start := time.Now()
	c.waitActivation(context.Background(), connectedCh)
	if elapsed := time.Since(start); elapsed < c.activationTimeout {
		t.Fatalf("waitActivation retornó antes del timeout (%v < %v) sin Connected", elapsed, c.activationTimeout)
	}
}

// TestWaitActivation_CtxCancelled: un timeout/aborto del caso de uso (ctx cancelado) corta la espera
// de inmediato, sin agotar el activationTimeout.
func TestWaitActivation_CtxCancelled(t *testing.T) {
	c := &Connector{activationTimeout: 5 * time.Second}
	connectedCh := make(chan struct{}) // nunca se cierra

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	c.waitActivation(ctx, connectedCh)
	if elapsed := time.Since(start); elapsed >= c.activationTimeout {
		t.Fatalf("waitActivation no respetó ctx.Done(): esperó %v (timeout %v)", elapsed, c.activationTimeout)
	}
}

// fakeContainer satisface store.DeviceContainer pero NO es el decorator de cryptostore.
type fakeContainer struct{}

func (fakeContainer) PutDevice(context.Context, *store.Device) error    { return nil }
func (fakeContainer) DeleteDevice(context.Context, *store.Device) error { return nil }
