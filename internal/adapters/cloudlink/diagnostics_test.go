package cloudlink

// diagnostics_test.go — Tests del enriquecimiento de salud del heartbeat (Plan 031 T7) y del handler de
// DiagnosticsRequest (T8) sobre el mismo arnés rawHarness (Adapter real contra el server-double bufconn).
//
// Cubre: (1) el Heartbeat lleva adjunto el SessionHealth que arma el colector; (2) sin colector el
// heartbeat viaja sin salud (retrocompatible); (3) un DiagnosticsRequest produce un DiagnosticsBundle
// correlacionado por command_id + Ack; (4) idempotencia por command_id (un request repetido no rearma ni
// reenvía el bundle).

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/diagnostics"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
)

// fakeCollector satisface cloudlink.HealthCollector: devuelve un Report fijo para la sesión conocida.
type fakeCollector struct {
	sessionID string
	report    health.Report
}

func (f fakeCollector) Collect(_ context.Context, sessionID string) (health.Report, bool) {
	if sessionID != f.sessionID {
		return health.Report{}, false
	}
	return f.report, true
}

// fakeBuilder satisface cloudlink.DiagnosticsBuilder: devuelve un bundle fijo y CUENTA las invocaciones
// (para afirmar la idempotencia: un request duplicado no debe volver a construir).
type fakeBuilder struct {
	bundle diagnostics.Bundle
	calls  atomic.Int64
}

func (f *fakeBuilder) Build(_ context.Context, _ string) diagnostics.Bundle {
	f.calls.Add(1)
	return f.bundle
}

// pushDiag empuja un DiagnosticsRequest cloud->edge.
func pushDiag(t *testing.T, h *rawHarness, cmdID, sessionID, scope string) {
	pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
		CommandId: cmdID,
		SessionId: sessionID,
		Payload:   &cloudlinkv1.CloudToEdge_DiagnosticsRequest{DiagnosticsRequest: &cloudlinkv1.DiagnosticsRequest{CommandId: cmdID, SessionId: sessionID, Scope: scope}},
	})
}

// TestHeartbeat_CarriesSessionHealth: con un colector cableado, el Heartbeat de la sesión lleva el
// SessionHealth mapeado desde el Report (estado del socket, motivo, edades, circuito, outbox, versión).
func TestHeartbeat_CarriesSessionHealth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	report := health.Report{
		SocketState:       string(health.SocketDegraded),
		DegradedReason:    health.ReasonDEKLoadTimeout,
		LastInboundAgeS:   42,
		DEKLoadDurationMs: 1234,
		IntentCircuit:     "half_open",
		OutboxDepth:       7,
		BinaryVersion:     "1.2.3-test",
		DaemonUptimeS:     99,
	}
	h := newRawHarness(t, ctx,
		WithHealthCollector(fakeCollector{sessionID: "S", report: report}),
		WithHeartbeatInterval(30*time.Millisecond),
	)
	h.adapter.Register("S", "", func(context.Context, string, string, string) error { return nil }, nil, func() bool { return true })
	h.run(t, ctx)

	msg := recvKind(t, ctx, h.srv, "Heartbeat con salud", func(m *cloudlinkv1.EdgeToCloud) bool {
		return m.GetHeartbeat() != nil && m.GetHeartbeat().GetSessionHealth() != nil
	})
	sh := msg.GetHeartbeat().GetSessionHealth()
	if sh.GetWhatsappSocketState() != cloudlinkv1.WhatsappSocketState_WHATSAPP_SOCKET_STATE_DEGRADED {
		t.Errorf("socket_state = %v, want DEGRADED", sh.GetWhatsappSocketState())
	}
	if sh.GetDegradedReason() != health.ReasonDEKLoadTimeout {
		t.Errorf("degraded_reason = %q, want %q", sh.GetDegradedReason(), health.ReasonDEKLoadTimeout)
	}
	if sh.GetLastInboundEventAgeS() != 42 || sh.GetDekLoadDurationMs() != 1234 {
		t.Errorf("age=%d dek_ms=%d, want 42/1234", sh.GetLastInboundEventAgeS(), sh.GetDekLoadDurationMs())
	}
	if sh.GetIntentCircuit() != "half_open" || sh.GetOutboxDepth() != 7 {
		t.Errorf("circuit=%q outbox=%d, want half_open/7", sh.GetIntentCircuit(), sh.GetOutboxDepth())
	}
	if sh.GetBinaryVersion() != "1.2.3-test" || sh.GetDaemonUptimeS() != 99 {
		t.Errorf("version=%q uptime=%d, want 1.2.3-test/99", sh.GetBinaryVersion(), sh.GetDaemonUptimeS())
	}
}

// TestHeartbeat_NoCollectorNoHealth: sin colector, el Heartbeat viaja SIN SessionHealth (retrocompatible).
func TestHeartbeat_NoCollectorNoHealth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h := newRawHarness(t, ctx, WithHeartbeatInterval(30*time.Millisecond))
	h.adapter.Register("S", "", func(context.Context, string, string, string) error { return nil }, nil, func() bool { return true })
	h.run(t, ctx)

	msg := recvKind(t, ctx, h.srv, "Heartbeat", func(m *cloudlinkv1.EdgeToCloud) bool { return m.GetHeartbeat() != nil })
	if sh := msg.GetHeartbeat().GetSessionHealth(); sh != nil {
		t.Errorf("SessionHealth = %v, want nil sin colector", sh)
	}
}

// TestDiagnostics_RequestProducesBundle: un DiagnosticsRequest produce un DiagnosticsBundle correlacionado
// por command_id (log_tail + goroutine_dump + subsystems_json) y un Ack ok.
func TestDiagnostics_RequestProducesBundle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	builder := &fakeBuilder{bundle: diagnostics.Bundle{
		LogTail:        "line-a\nline-b",
		GoroutineDump:  "goroutine 1 [running]",
		SubsystemsJSON: `{"daemon":{"version":"v"}}`,
	}}
	h := newRawHarness(t, ctx, WithDiagnosticsBuilder(builder))
	h.run(t, ctx)

	pushDiag(t, h, "diag-1", "S", "full")

	msg := recvKind(t, ctx, h.srv, "DiagnosticsBundle", func(m *cloudlinkv1.EdgeToCloud) bool {
		return m.GetDiagnosticsBundle() != nil
	})
	b := msg.GetDiagnosticsBundle()
	if b.GetCommandId() != "diag-1" {
		t.Errorf("bundle command_id = %q, want diag-1", b.GetCommandId())
	}
	if b.GetLogTail() != "line-a\nline-b" || b.GetGoroutineDump() != "goroutine 1 [running]" {
		t.Errorf("bundle log/goroutine no coinciden: %q / %q", b.GetLogTail(), b.GetGoroutineDump())
	}
	if b.GetSubsystemsJson() != `{"daemon":{"version":"v"}}` {
		t.Errorf("bundle subsystems = %q", b.GetSubsystemsJson())
	}
	if ack := recvAck(t, ctx, h.srv, "diag-1"); !ack.GetOk() {
		t.Errorf("Ack de diag-1: ok=false inesperado (%q)", ack.GetError())
	}
}

// TestDiagnostics_IdempotentByCommandID: un DiagnosticsRequest repetido (mismo command_id) NO vuelve a
// construir ni reenviar el bundle. Se comprueba encolando el duplicado y, tras un request DISTINTO que sí
// se atiende, afirmando que el builder se invocó exactamente 2 veces (el primero y el tercero, no el
// duplicado).
func TestDiagnostics_IdempotentByCommandID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	builder := &fakeBuilder{bundle: diagnostics.Bundle{LogTail: "x"}}
	h := newRawHarness(t, ctx, WithDiagnosticsBuilder(builder))
	h.run(t, ctx)

	// Mismo session_id ⇒ mismo worker FIFO del dispatcher: los tres requests se procesan en orden.
	pushDiag(t, h, "diag-1", "S", "full") // 1ª construcción
	recvKind(t, ctx, h.srv, "bundle diag-1", func(m *cloudlinkv1.EdgeToCloud) bool {
		return m.GetDiagnosticsBundle().GetCommandId() == "diag-1"
	})
	pushDiag(t, h, "diag-1", "S", "full") // DUPLICADO: debe ignorarse
	pushDiag(t, h, "diag-2", "S", "full") // 2ª construcción (sirve de barrera de sincronía)
	recvKind(t, ctx, h.srv, "bundle diag-2", func(m *cloudlinkv1.EdgeToCloud) bool {
		return m.GetDiagnosticsBundle().GetCommandId() == "diag-2"
	})

	if got := builder.calls.Load(); got != 2 {
		t.Errorf("builder invocado %d veces, want 2 (el duplicado diag-1 NO debe reconstruir)", got)
	}
}

// compile-time: los fakes satisfacen los puertos del adapter.
var (
	_ HealthCollector    = fakeCollector{}
	_ DiagnosticsBuilder = (*fakeBuilder)(nil)
)
