package whatsmeow

import (
	"testing"
	"time"
)

// TestCorrelator_RememberLookup: un envío recordado (command_id ↔ MessageID) se resuelve por Lookup con
// el MessageID que traería el events.Receipt.
func TestCorrelator_RememberLookup(t *testing.T) {
	c := NewCorrelator(0, 0)
	c.Remember("cmd-1", "MSG-A", time.Unix(100, 0))

	cmd, ok := c.Lookup([]string{"MSG-A"})
	if !ok || cmd != "cmd-1" {
		t.Fatalf("Lookup([MSG-A]) = (%q, %v), quería (cmd-1, true)", cmd, ok)
	}
}

// TestCorrelator_LookupMultiIDs: un receipt puede acusar VARIOS IDs; basta con que uno matchee.
func TestCorrelator_LookupMultiIDs(t *testing.T) {
	c := NewCorrelator(0, 0)
	c.Remember("cmd-2", "MSG-B", time.Unix(1, 0))

	cmd, ok := c.Lookup([]string{"MSG-X", "MSG-B", "MSG-Y"})
	if !ok || cmd != "cmd-2" {
		t.Fatalf("Lookup con varios IDs = (%q, %v), quería (cmd-2, true)", cmd, ok)
	}
}

// TestCorrelator_LookupMiss: un MessageID desconocido no correlaciona (ok=false): el acuse subirá como
// estado crudo (§10.E).
func TestCorrelator_LookupMiss(t *testing.T) {
	c := NewCorrelator(0, 0)
	c.Remember("cmd-3", "MSG-C", time.Unix(1, 0))

	if cmd, ok := c.Lookup([]string{"DESCONOCIDO"}); ok {
		t.Fatalf("Lookup de ID desconocido debía fallar, dio %q", cmd)
	}
}

// TestCorrelator_IgnoraVacios: command_id o msgID vacíos no se registran (el camino de envío sin
// command_id no correlaciona).
func TestCorrelator_IgnoraVacios(t *testing.T) {
	c := NewCorrelator(0, 0)
	c.Remember("", "MSG-D", time.Unix(1, 0))
	c.Remember("cmd-4", "", time.Unix(1, 0))

	if n := c.Len(); n != 0 {
		t.Fatalf("no debía registrarse nada con campos vacíos, Len=%d", n)
	}
}

// TestCorrelator_TTL: una entrada más vieja que el TTL vence y deja de resolverse (reloj falso).
func TestCorrelator_TTL(t *testing.T) {
	now := time.Unix(1000, 0)
	c := NewCorrelator(0, 10*time.Minute)
	c.now = func() time.Time { return now }

	c.Remember("cmd-5", "MSG-E", now)

	// Aún vigente: 5 min < TTL.
	now = now.Add(5 * time.Minute)
	if _, ok := c.Lookup([]string{"MSG-E"}); !ok {
		t.Fatal("la entrada debía seguir vigente a los 5 min")
	}

	// Vencida: 20 min > TTL.
	now = now.Add(15 * time.Minute)
	if cmd, ok := c.Lookup([]string{"MSG-E"}); ok {
		t.Fatalf("la entrada debía haber vencido, dio %q", cmd)
	}
	if n := c.Len(); n != 0 {
		t.Fatalf("la entrada vencida debía podarse, Len=%d", n)
	}
}

// TestCorrelator_TopeEvictaMasAntiguo: superado el tope, se evicta el más antiguo (FIFO); los recientes
// sobreviven.
func TestCorrelator_TopeEvictaMasAntiguo(t *testing.T) {
	c := NewCorrelator(2, 0) // tope 2.
	c.Remember("cmd-A", "MSG-1", time.Unix(1, 0))
	c.Remember("cmd-B", "MSG-2", time.Unix(2, 0))
	c.Remember("cmd-C", "MSG-3", time.Unix(3, 0)) // desborda: evicta cmd-A.

	if _, ok := c.Lookup([]string{"MSG-1"}); ok {
		t.Fatal("cmd-A (más antiguo) debía haberse evictado por tope")
	}
	if _, ok := c.Lookup([]string{"MSG-2"}); !ok {
		t.Fatal("cmd-B debía sobrevivir")
	}
	if _, ok := c.Lookup([]string{"MSG-3"}); !ok {
		t.Fatal("cmd-C debía sobrevivir")
	}
	if n := c.Len(); n != 2 {
		t.Fatalf("Len=%d, quería 2 (tope)", n)
	}
}

// TestCorrelator_ReRegistro: re-registrar el mismo command_id actualiza su MessageID y limpia el índice
// inverso previo (el ID viejo deja de resolver).
func TestCorrelator_ReRegistro(t *testing.T) {
	c := NewCorrelator(0, 0)
	c.Remember("cmd-6", "MSG-VIEJO", time.Unix(1, 0))
	c.Remember("cmd-6", "MSG-NUEVO", time.Unix(2, 0))

	if _, ok := c.Lookup([]string{"MSG-VIEJO"}); ok {
		t.Fatal("el MessageID viejo no debía resolver tras el re-registro")
	}
	cmd, ok := c.Lookup([]string{"MSG-NUEVO"})
	if !ok || cmd != "cmd-6" {
		t.Fatalf("el MessageID nuevo debía resolver a cmd-6, dio (%q,%v)", cmd, ok)
	}
	if n := c.Len(); n != 1 {
		t.Fatalf("Len=%d, quería 1 (mismo command_id)", n)
	}
}
