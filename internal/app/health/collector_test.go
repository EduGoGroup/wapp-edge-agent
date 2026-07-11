package health

// collector_test.go — El colector deriva bien el Report (Plan 031 T7): estado del socket, edad del último
// entrante, duración de la DEK, profundidad del outbox, circuito normalizado, versión y uptime. Y respeta
// la prueba de vida: una sesión SIN entrada en el Registry no reporta salud (ok=false).

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeOutbox satisface OutboxDepther con una profundidad fija por sesión (o error).
type fakeOutbox struct {
	depth map[string]int64
	err   error
}

func (f fakeOutbox) Depth(_ context.Context, sessionID string) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.depth[sessionID], nil
}

func TestCollector_DerivesReport(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	start := now.Add(-90 * time.Second)

	reg := NewRegistry()
	reg.SetSocketState("S", SocketDegraded, ReasonDEKLoadTimeout)
	reg.SetDEKLoadDuration("S", 1500*time.Millisecond)
	reg.MarkInbound("S", now.Add(-30*time.Second))

	c := NewCollector(reg, fakeOutbox{depth: map[string]int64{"S": 4}}, func() string { return "half-open" },
		"9.9.9", start, WithClock(func() time.Time { return now }))

	r, ok := c.Collect(context.Background(), "S")
	if !ok {
		t.Fatal("Collect devolvió ok=false para una sesión con salud")
	}
	if r.SocketState != string(SocketDegraded) || r.DegradedReason != ReasonDEKLoadTimeout {
		t.Errorf("estado/motivo = %q/%q", r.SocketState, r.DegradedReason)
	}
	if r.LastInboundAgeS != 30 {
		t.Errorf("edad último entrante = %d, want 30", r.LastInboundAgeS)
	}
	if r.DEKLoadDurationMs != 1500 {
		t.Errorf("dek_load_ms = %d, want 1500", r.DEKLoadDurationMs)
	}
	if r.OutboxDepth != 4 {
		t.Errorf("outbox_depth = %d, want 4", r.OutboxDepth)
	}
	if r.IntentCircuit != "half_open" { // normaliza guion → guion bajo (contrato del wire)
		t.Errorf("intent_circuit = %q, want half_open", r.IntentCircuit)
	}
	if r.BinaryVersion != "9.9.9" || r.DaemonUptimeS != 90 {
		t.Errorf("version/uptime = %q/%d, want 9.9.9/90", r.BinaryVersion, r.DaemonUptimeS)
	}
}

// TestCollector_UnknownSessionNoHealth: sin entrada en el Registry (sin prueba de vida) ⇒ ok=false.
func TestCollector_UnknownSessionNoHealth(t *testing.T) {
	c := NewCollector(NewRegistry(), nil, nil, "v", time.Now())
	if _, ok := c.Collect(context.Background(), "desconocida"); ok {
		t.Error("Collect devolvió ok=true para una sesión sin salud")
	}
}

// TestCollector_OutboxErrorTolerated: un error del outbox no rompe el Report (depth queda en 0).
func TestCollector_OutboxErrorTolerated(t *testing.T) {
	reg := NewRegistry()
	reg.SetSocketState("S", SocketConnected, "")
	c := NewCollector(reg, fakeOutbox{err: errors.New("db down")}, nil, "v", time.Now())
	r, ok := c.Collect(context.Background(), "S")
	if !ok || r.OutboxDepth != 0 {
		t.Errorf("ok=%v depth=%d, want ok=true depth=0 (error de outbox tolerado)", ok, r.OutboxDepth)
	}
}

// TestCollector_ReportsAllLiveSessions: Reports enumera todas las sesiones con salud.
func TestCollector_ReportsAllLiveSessions(t *testing.T) {
	reg := NewRegistry()
	reg.SetSocketState("A", SocketConnected, "")
	reg.SetSocketState("B", SocketDead, ReasonLoggedOut)
	c := NewCollector(reg, nil, nil, "v", time.Now())
	reports := c.Reports(context.Background())
	if len(reports) != 2 {
		t.Fatalf("Reports = %d sesiones, want 2", len(reports))
	}
	if reports["B"].DegradedReason != ReasonLoggedOut {
		t.Errorf("B.reason = %q, want %q", reports["B"].DegradedReason, ReasonLoggedOut)
	}
}
