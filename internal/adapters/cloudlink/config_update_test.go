package cloudlink

// intent_transport_test.go — Plan 029 · T12/T10: mapeo de la intención LLM al proto y demux del ConfigUpdate.
//
//   - T12 Deliver: la intención de dominio (evt.Intent) se mapea a ClassifiedIntent. CON sellado activo va
//     DENTRO del SensitivePayload (el espejo claro in.Intent queda nil); SIN sellado va en el espejo claro
//     IncomingMessage.Intent. Sin intención, el campo queda nil en ambos caminos (mismo criterio que text).
//   - T10 handleConfigUpdate: un CloudToEdge{ConfigUpdate} se atiende ANTES de resolver la sesión, delega en
//     el ConfigApplier y responde Ack{ok} según el resultado. Sin applier ⇒ Ack tolerante.

import (
	"context"
	"sync"
	"testing"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-shared/envelope"
	"google.golang.org/protobuf/proto"
)

// intentEvent es el evento de referencia con una intención accionable anotada.
func intentEvent() domain.InboundEvent {
	return domain.InboundEvent{
		MessageID: "WAMID-INTENT",
		Chat:      "593999@s.whatsapp.net",
		Sender:    "593999@s.whatsapp.net",
		Timestamp: time.Unix(1_700_000_000, 0),
		Text:      "quiero 2 hamburguesas",
		Intent: &domain.ClassifiedIntent{
			Name:          "crear_pedido",
			Params:        map[string]string{"producto": "hamburguesas", "cantidad": "2"},
			Confidence:    0.92,
			ConfigVersion: "v-abc123",
		},
	}
}

func assertProtoIntent(t *testing.T, ci *cloudlinkv1.ClassifiedIntent) {
	t.Helper()
	if ci == nil {
		t.Fatalf("ClassifiedIntent nil: se esperaba la intención mapeada")
	}
	if ci.GetIntent() != "crear_pedido" {
		t.Errorf("intent: got %q want crear_pedido", ci.GetIntent())
	}
	if ci.GetParams()["producto"] != "hamburguesas" || ci.GetParams()["cantidad"] != "2" {
		t.Errorf("params mal mapeados: %v", ci.GetParams())
	}
	if ci.GetConfigVersion() != "v-abc123" {
		t.Errorf("config_version: got %q want v-abc123", ci.GetConfigVersion())
	}
	if d := ci.GetConfidence() - 0.92; d > 1e-6 || d < -1e-6 {
		t.Errorf("confidence: got %v want ~0.92", ci.GetConfidence())
	}
}

// SIN sellado: la intención viaja en el espejo claro IncomingMessage.Intent.
func TestDeliver_Intent_ClearMirror_WhenNoSeal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h := newSealHarness(t, ctx) // sin WithCloudEncPubKey => fallback claro
	if err := h.adapter.SinkFor(sealSessionID).Deliver(ctx, intentEvent()); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	msg := recvKind(t, ctx, h.srv, "IncomingMessage", func(m *cloudlinkv1.EdgeToCloud) bool {
		return m.GetIncoming() != nil
	})
	in := msg.GetIncoming()
	if len(in.GetEncPayload()) != 0 {
		t.Fatalf("EncPayload no vacío en fallback claro")
	}
	assertProtoIntent(t, in.GetIntent())
}

// CON sellado: la intención viaja DENTRO del SensitivePayload; el espejo claro queda nil.
func TestDeliver_Intent_Sealed_WhenCloudPubPresent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub, priv, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	h := newSealHarness(t, ctx, WithCloudEncPubKey(pub))
	if err := h.adapter.SinkFor(sealSessionID).Deliver(ctx, intentEvent()); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	msg := recvKind(t, ctx, h.srv, "IncomingMessage", func(m *cloudlinkv1.EdgeToCloud) bool {
		return m.GetIncoming() != nil
	})
	in := msg.GetIncoming()
	if in.GetIntent() != nil {
		t.Errorf("intent NO debe viajar en el espejo claro cuando hay sellado")
	}
	if len(in.GetEncPayload()) == 0 {
		t.Fatalf("EncPayload vacío: se esperaba el SensitivePayload sellado")
	}
	raw, err := envelope.OpenWith(priv, in.GetEncPayload())
	if err != nil {
		t.Fatalf("OpenWith: %v", err)
	}
	var sp cloudlinkv1.SensitivePayload
	if err := proto.Unmarshal(raw, &sp); err != nil {
		t.Fatalf("Unmarshal SensitivePayload: %v", err)
	}
	assertProtoIntent(t, sp.GetIntent())
}

// Sin intención anotada, el campo queda nil (no se inventa) — camino claro.
func TestDeliver_NoIntent_LeavesFieldNil(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h := newSealHarness(t, ctx)
	evt := intentEvent()
	evt.Intent = nil
	if err := h.adapter.SinkFor(sealSessionID).Deliver(ctx, evt); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	msg := recvKind(t, ctx, h.srv, "IncomingMessage", func(m *cloudlinkv1.EdgeToCloud) bool {
		return m.GetIncoming() != nil
	})
	if msg.GetIncoming().GetIntent() != nil {
		t.Errorf("intent debe ser nil cuando el evento no trae intención")
	}
}

// fakeApplier registra las llamadas Apply y devuelve un error configurable.
type fakeApplier struct {
	mu     sync.Mutex
	calls  []applyCall
	retErr error
}

type applyCall struct {
	kind, version string
	payload       []byte
}

func (f *fakeApplier) Apply(_ context.Context, kind, version string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, applyCall{kind: kind, version: version, payload: payload})
	return f.retErr
}

func (f *fakeApplier) last() (applyCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return applyCall{}, false
	}
	return f.calls[len(f.calls)-1], true
}

// T10: ConfigUpdate válido ⇒ applier invocado + Ack{ok=true}.
func TestHandleConfigUpdate_DelegaYAckOK(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fa := &fakeApplier{}
	h := newSealHarness(t, ctx, WithConfigApplier(fa))

	cmdID := "cmd-cfg-1"
	pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
		CommandId: cmdID,
		SessionId: sealSessionID,
		Payload: &cloudlinkv1.CloudToEdge_ConfigUpdate{ConfigUpdate: &cloudlinkv1.ConfigUpdate{
			CommandId: cmdID,
			SessionId: sealSessionID,
			Kind:      "intents",
			Version:   "v1",
			Payload:   []byte(`{"version":"v1"}`),
		}},
	})
	ack := recvAck(t, ctx, h.srv, cmdID)
	if !ack.GetOk() {
		t.Errorf("Ack ok=false inesperado: %q", ack.GetError())
	}
	call, ok := fa.last()
	if !ok {
		t.Fatalf("el applier no fue invocado")
	}
	if call.kind != "intents" || call.version != "v1" {
		t.Errorf("Apply recibió kind=%q version=%q", call.kind, call.version)
	}
}

// T10: ConfigUpdate para un session_id NO registrado se atiende igual (config global) ⇒ Ack.
func TestHandleConfigUpdate_SessionDesconocida_SigueAckeando(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fa := &fakeApplier{}
	h := newSealHarness(t, ctx, WithConfigApplier(fa))

	cmdID := "cmd-cfg-unknown"
	pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
		CommandId: cmdID,
		SessionId: "sesion-no-registrada",
		Payload: &cloudlinkv1.CloudToEdge_ConfigUpdate{ConfigUpdate: &cloudlinkv1.ConfigUpdate{
			CommandId: cmdID,
			SessionId: "sesion-no-registrada",
			Kind:      "intents",
			Version:   "v9",
			Payload:   []byte(`{"version":"v9"}`),
		}},
	})
	ack := recvAck(t, ctx, h.srv, cmdID)
	if !ack.GetOk() {
		t.Errorf("Ack ok=false inesperado para config global: %q", ack.GetError())
	}
	if _, ok := fa.last(); !ok {
		t.Errorf("el applier debe atender config aunque el session_id no esté registrado")
	}
}
