package app

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// blockingCustody es una custodia cuya Load() BLOQUEA hasta que se libera `release`, imitando el cuelgue
// cgo del Keychain (SecItemCopyMatching esperando el diálogo de permiso). Registra si Load fue invocada.
type blockingCustody struct {
	dek     []byte
	release chan struct{}
	loaded  chan struct{} // se cierra al ENTRAR en Load (para sincronizar el test)
	once    sync.Once
}

func newBlockingCustody(dek []byte) *blockingCustody {
	return &blockingCustody{dek: dek, release: make(chan struct{}), loaded: make(chan struct{})}
}

func (c *blockingCustody) Store([]byte) error { return nil }
func (c *blockingCustody) Exists() bool       { return true }
func (c *blockingCustody) Load() ([]byte, error) {
	c.once.Do(func() { close(c.loaded) })
	<-c.release
	return append([]byte(nil), c.dek...), nil
}

// TestListen_DEKLoadTimeout: si la carga de la DEK excede el plazo, Run devuelve ErrDEKLoadTimeout (no se
// cuelga), NO invoca al gateway, y la duración se reporta TARDE cuando la carga abandonada por fin retorna.
func TestListen_DEKLoadTimeout(t *testing.T) {
	dek := bytes.Repeat([]byte{0xAB}, DEKSize)
	cust := newBlockingCustody(dek)
	gw := newFakeListenGateway()

	var mu sync.Mutex
	var reported []time.Duration
	report := func(d time.Duration) {
		mu.Lock()
		reported = append(reported, d)
		mu.Unlock()
	}

	l := NewListen(cust, gw, nil,
		WithDEKLoadTimeout(20*time.Millisecond),
		WithDEKDurationReporter(report))

	done := make(chan error, 1)
	go func() { done <- l.Run(context.Background()) }()

	<-cust.loaded // asegura que entramos en Load antes de esperar el timeout

	select {
	case err := <-done:
		if !errors.Is(err, ErrDEKLoadTimeout) {
			t.Fatalf("Run debía devolver ErrDEKLoadTimeout, dio: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run se colgó: el watchdog no abandonó la carga de DEK")
	}

	// El gateway NO debió invocarse (no hay DEK utilizable todavía).
	if gw.gotDEK != nil {
		t.Fatal("el gateway NO debía invocarse tras un timeout de carga de DEK")
	}
	// Aún sin duración reportada (la carga sigue bloqueada).
	mu.Lock()
	n := len(reported)
	mu.Unlock()
	if n != 0 {
		t.Fatalf("no debía haber duración reportada aún, hubo %d", n)
	}

	// Libera la carga abandonada: onLate debe reportar la duración real.
	close(cust.release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n = len(reported)
		mu.Unlock()
		if n == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n != 1 {
		t.Fatalf("la carga tardía debía reportar su duración una vez, hubo %d", n)
	}
}

// TestListen_DEKLoadReportsDurationOnSuccess: en una carga normal (dentro del plazo) se reporta la duración
// y el gateway recibe la DEK.
func TestListen_DEKLoadReportsDurationOnSuccess(t *testing.T) {
	dek := bytes.Repeat([]byte{0x11}, DEKSize)
	cust := custodyWith(dek)
	// El gateway avisa al ENTRAR en Listen: sincroniza sin leer gw.gotDEK en carrera. Hasta T3.8 la señal
	// era el conteo de un spySink que el propio fake alimentaba; ese sink murió con el parámetro del puerto.
	gw := newFakeListenGateway()

	var mu sync.Mutex
	var got time.Duration
	var called bool
	l := NewListen(cust, gw, nil,
		WithDEKDurationReporter(func(d time.Duration) {
			mu.Lock()
			got, called = d, true
			mu.Unlock()
		}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	gw.esperaEntrada(t) // la DEK se cargó y llegó al gateway
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if !called || got < 0 {
		t.Fatalf("la duración de carga de DEK debía reportarse en éxito (called=%v, got=%v)", called, got)
	}
}
