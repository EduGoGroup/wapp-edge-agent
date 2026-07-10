package outbox

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

func testLogger() sharedlogger.Logger {
	return sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}), sharedlogger.WithJSON(true))
}

// newOutbox abre una BD única temporal YA migrada (incluye 0005_outbox) y devuelve el Store.
func newOutbox(t *testing.T, maxEvents, ttlHours int, opts ...Option) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "edge.db")
	database, err := db.OpenAndMigrate(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	s, err := New(context.Background(), database, maxEvents, ttlHours, testLogger(), opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, path
}

func item(dedupe, session, kind, payload string) app.OutboxItem {
	return app.OutboxItem{DedupeKey: dedupe, SessionID: session, Kind: kind, Payload: []byte(payload)}
}

// TestEnqueueDrainPreservesOrder: el drenaje devuelve los eventos en el ORDEN de encolado (FIFO por seq),
// incluso mezclando sesiones — el orden relativo POR SESIÓN se preserva.
func TestEnqueueDrainPreservesOrder(t *testing.T) {
	ctx := context.Background()
	s, _ := newOutbox(t, 100, 0)

	// Intercala dos sesiones: A1, B1, A2, B2, A3.
	seq := []app.OutboxItem{
		item("a1", "A", app.OutboxKindIncoming, "A1"),
		item("b1", "B", app.OutboxKindIncoming, "B1"),
		item("a2", "A", app.OutboxKindReceipt, "A2"),
		item("b2", "B", app.OutboxKindIncoming, "B2"),
		item("a3", "A", app.OutboxKindIncoming, "A3"),
	}
	for _, it := range seq {
		if err := s.Enqueue(ctx, it); err != nil {
			t.Fatalf("Enqueue %s: %v", it.DedupeKey, err)
		}
	}

	got, err := s.Drain(ctx, 100)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(got) != len(seq) {
		t.Fatalf("Drain devolvió %d, esperaba %d", len(got), len(seq))
	}
	// Orden global de encolado.
	for i, it := range seq {
		if got[i].DedupeKey != it.DedupeKey {
			t.Fatalf("orden global roto en %d: got %q want %q", i, got[i].DedupeKey, it.DedupeKey)
		}
	}
	// Orden relativo por sesión A: a1<a2<a3.
	var aOrder []string
	for _, g := range got {
		if g.SessionID == "A" {
			aOrder = append(aOrder, g.DedupeKey)
		}
	}
	if len(aOrder) != 3 || aOrder[0] != "a1" || aOrder[1] != "a2" || aOrder[2] != "a3" {
		t.Fatalf("orden por sesión A roto: %v", aOrder)
	}
}

// TestEnqueueIdempotent: encolar dos veces el MISMO dedupe_key no duplica (idempotencia local).
func TestEnqueueIdempotent(t *testing.T) {
	ctx := context.Background()
	s, _ := newOutbox(t, 100, 0)

	it := item("cmd-1", "A", app.OutboxKindIncoming, "hola")
	if err := s.Enqueue(ctx, it); err != nil {
		t.Fatalf("Enqueue 1: %v", err)
	}
	if err := s.Enqueue(ctx, it); err != nil {
		t.Fatalf("Enqueue 2 (dup): %v", err)
	}
	got, err := s.Drain(ctx, 100)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("esperaba 1 evento tras encolar el mismo command_id dos veces, hay %d", len(got))
	}
}

// TestDeleteAfterResend: Delete quita el evento reenviado; el resto sigue drenable.
func TestDeleteAfterResend(t *testing.T) {
	ctx := context.Background()
	s, _ := newOutbox(t, 100, 0)
	_ = s.Enqueue(ctx, item("a1", "A", app.OutboxKindIncoming, "1"))
	_ = s.Enqueue(ctx, item("a2", "A", app.OutboxKindIncoming, "2"))

	if err := s.Delete(ctx, "a1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, _ := s.Drain(ctx, 100)
	if len(got) != 1 || got[0].DedupeKey != "a2" {
		t.Fatalf("tras Delete(a1) esperaba [a2], got %+v", got)
	}
	// Delete idempotente: borrar de nuevo no falla.
	if err := s.Delete(ctx, "a1"); err != nil {
		t.Fatalf("Delete idempotente: %v", err)
	}
}

// TestDropOldestAtCapacity: al alcanzar el tope, Enqueue descarta el más viejo (drop-oldest) conservando
// los más nuevos en orden.
func TestDropOldestAtCapacity(t *testing.T) {
	ctx := context.Background()
	s, _ := newOutbox(t, 3, 0) // tope 3

	for _, d := range []string{"e1", "e2", "e3", "e4", "e5"} {
		if err := s.Enqueue(ctx, item(d, "A", app.OutboxKindIncoming, d)); err != nil {
			t.Fatalf("Enqueue %s: %v", d, err)
		}
	}
	got, _ := s.Drain(ctx, 100)
	if len(got) != 3 {
		t.Fatalf("tope 3: esperaba 3 eventos, hay %d", len(got))
	}
	if got[0].DedupeKey != "e3" || got[1].DedupeKey != "e4" || got[2].DedupeKey != "e5" {
		t.Fatalf("drop-oldest: esperaba [e3,e4,e5], got %s,%s,%s", got[0].DedupeKey, got[1].DedupeKey, got[2].DedupeKey)
	}
}

// TestTTLPrune: con TTL de 1h, un evento más viejo que el TTL se poda al encolar uno nuevo.
func TestTTLPrune(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s, _ := newOutbox(t, 100, 1, WithClock(clock.Now)) // ttl 1h

	if err := s.Enqueue(ctx, item("viejo", "A", app.OutboxKindIncoming, "v")); err != nil {
		t.Fatalf("Enqueue viejo: %v", err)
	}
	clock.t = clock.t.Add(2 * time.Hour) // supera el TTL
	if err := s.Enqueue(ctx, item("nuevo", "A", app.OutboxKindIncoming, "n")); err != nil {
		t.Fatalf("Enqueue nuevo: %v", err)
	}
	got, _ := s.Drain(ctx, 100)
	if len(got) != 1 || got[0].DedupeKey != "nuevo" {
		t.Fatalf("TTL: esperaba solo [nuevo], got %+v", got)
	}
}

// TestSeqSurvivesReopen: al reabrir el Store sobre la misma BD, la secuencia continúa (el evento nuevo va
// DESPUÉS del backlog persistido) — el orden sobrevive a un reinicio.
func TestSeqSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "edge.db")
	database, err := db.OpenAndMigrate(ctx, path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	s1, err := New(ctx, database, 100, 0, testLogger())
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	_ = s1.Enqueue(ctx, item("a1", "A", app.OutboxKindIncoming, "1"))
	_ = s1.Enqueue(ctx, item("a2", "A", app.OutboxKindIncoming, "2"))

	// "Reinicio": nuevo Store sobre la MISMA BD (siembra seq de MAX(seq)).
	s2, err := New(ctx, database, 100, 0, testLogger())
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	_ = s2.Enqueue(ctx, item("a3", "A", app.OutboxKindIncoming, "3"))

	got, _ := s2.Drain(ctx, 100)
	if len(got) != 3 || got[0].DedupeKey != "a1" || got[1].DedupeKey != "a2" || got[2].DedupeKey != "a3" {
		t.Fatalf("seq tras reopen: esperaba [a1,a2,a3], got %+v", dedupeKeys(got))
	}

	// PendingSessions refleja la sesión con backlog.
	sessions, err := s2.PendingSessions(ctx)
	if err != nil {
		t.Fatalf("PendingSessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0] != "A" {
		t.Fatalf("PendingSessions: esperaba [A], got %v", sessions)
	}
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time { return c.t }

func dedupeKeys(items []app.OutboxItem) []string {
	ks := make([]string, len(items))
	for i, it := range items {
		ks[i] = it.DedupeKey
	}
	return ks
}
