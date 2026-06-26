package whatsmeow

import (
	"testing"
	"time"
)

// TestBackoff_Sequence verifica que la secuencia crece exponencial (1,2,4,8,…) y satura en el tope.
func TestBackoff_Sequence(t *testing.T) {
	b := &Backoff{Base: 1 * time.Second, Max: 60 * time.Second}
	want := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
		60 * time.Second, // 64s saturado al tope.
		60 * time.Second, // se mantiene en el tope.
		60 * time.Second,
	}
	for i, w := range want {
		if got := b.Next(); got != w {
			t.Fatalf("Next()#%d = %s, quería %s", i, got, w)
		}
		if b.Attempt() != i+1 {
			t.Fatalf("Attempt tras Next#%d = %d, quería %d", i, b.Attempt(), i+1)
		}
	}
}

// TestBackoff_Reset comprueba que Reset vuelve la secuencia al inicio.
func TestBackoff_Reset(t *testing.T) {
	b := DefaultBackoff()
	_ = b.Next() // 1s
	_ = b.Next() // 2s
	_ = b.Next() // 4s
	if b.Attempt() != 3 {
		t.Fatalf("Attempt = %d, quería 3", b.Attempt())
	}
	b.Reset()
	if b.Attempt() != 0 {
		t.Fatalf("Attempt tras Reset = %d, quería 0", b.Attempt())
	}
	if got := b.Next(); got != 1*time.Second {
		t.Fatalf("primer Next tras Reset = %s, quería 1s", got)
	}
}

// TestBackoff_RespectsMax: con un tope pequeño, ningún delay lo supera.
func TestBackoff_RespectsMax(t *testing.T) {
	b := &Backoff{Base: 500 * time.Millisecond, Max: 2 * time.Second}
	for i := 0; i < 10; i++ {
		if got := b.Next(); got > b.Max {
			t.Fatalf("Next()#%d = %s superó el tope %s", i, got, b.Max)
		}
	}
}

// TestBackoff_DefaultValues confirma los parámetros del spike (1s base, 60s tope).
func TestBackoff_DefaultValues(t *testing.T) {
	b := DefaultBackoff()
	if b.Base != 1*time.Second || b.Max != 60*time.Second {
		t.Fatalf("DefaultBackoff = {Base:%s Max:%s}, quería {1s 60s}", b.Base, b.Max)
	}
}
