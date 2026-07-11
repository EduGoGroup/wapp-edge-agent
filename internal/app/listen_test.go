package app

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// --- fakes (sin red ni teléfono) ---

// spySink registra los eventos entregados (seguro para concurrencia).
type spySink struct {
	mu       sync.Mutex
	received []domain.InboundEvent
	err      error
}

func (s *spySink) Deliver(_ context.Context, evt domain.InboundEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.received = append(s.received, evt)
	return s.err
}

func (s *spySink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.received)
}

// fakeListenGateway emite N eventos sintéticos al sink y luego BLOQUEA hasta que el ctx se cancele,
// imitando el socket always-on real. Registra la DEK recibida y si respetó la cancelación.
type fakeListenGateway struct {
	emit       int
	gotDEK     []byte
	connectErr error
	returned   error
}

func (g *fakeListenGateway) Listen(ctx context.Context, dek []byte, sink InboundSink) error {
	// Copia defensiva: el caso de uso borra (zero) la DEK al salir.
	g.gotDEK = append([]byte(nil), dek...)
	if g.connectErr != nil {
		return g.connectErr
	}
	for i := 0; i < g.emit; i++ {
		_ = sink.Deliver(ctx, domain.InboundEvent{MessageID: "m"})
	}
	<-ctx.Done() // socket vivo hasta la cancelación.
	g.returned = ctx.Err()
	return nil
}

// --- tests ---

// TestListen_DeliversAndStopsOnCancel: carga la DEK, los eventos llegan al sink y Run retorna limpio
// (nil) al cancelar el ctx, respetando la cancelación (always-on).
func TestListen_DeliversAndStopsOnCancel(t *testing.T) {
	dek := bytes.Repeat([]byte{0xCD}, DEKSize)
	cust := custodyWith(dek)
	sink := &spySink{}
	gw := &fakeListenGateway{emit: 3}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- NewListen(cust, gw, sink, nil).Run(ctx) }()

	// Espera a que lleguen los 3 eventos antes de cancelar.
	waitFor(t, func() bool { return sink.count() == 3 })
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run debía retornar nil al cancelar: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run no retornó tras cancelar el ctx")
	}

	if !bytes.Equal(gw.gotDEK, dek) {
		t.Fatalf("la DEK no llegó intacta al gateway: %v", gw.gotDEK)
	}
	if !errors.Is(gw.returned, context.Canceled) {
		t.Fatalf("el gateway no respetó la cancelación: %v", gw.returned)
	}
}

// TestListen_NoDEK: sin DEK custodiada, Run falla y NO invoca al gateway.
func TestListen_NoDEK(t *testing.T) {
	sink := &spySink{}
	gw := &fakeListenGateway{emit: 1}
	err := NewListen(&fakeCustody{}, gw, sink, nil).Run(context.Background())
	if err == nil {
		t.Fatal("se esperaba error al no haber DEK custodiada")
	}
	if gw.gotDEK != nil {
		t.Fatal("el gateway NO debía invocarse sin DEK")
	}
}

// TestListen_GatewayError: un fallo de conexión del gateway se propaga envuelto.
func TestListen_GatewayError(t *testing.T) {
	sentinel := errors.New("socket caído")
	cust := custodyWith(bytes.Repeat([]byte{1}, DEKSize))
	gw := &fakeListenGateway{connectErr: sentinel}
	err := NewListen(cust, gw, &spySink{}, nil).Run(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, quería envolver %v", err, sentinel)
	}
}

// waitFor sondea cond hasta que sea true o falla por timeout (evita sleeps fijos en tests con
// goroutines).
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condición no satisfecha dentro del timeout")
}
