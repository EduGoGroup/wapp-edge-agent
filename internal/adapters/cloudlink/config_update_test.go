package cloudlink

// config_update_test.go — Plan 029 · T10: demux del ConfigUpdate.
//
// handleConfigUpdate: un CloudToEdge{ConfigUpdate} se atiende ANTES de resolver la sesión, delega en el
// ConfigApplier y responde Ack{ok} según el resultado. Sin applier ⇒ Ack tolerante.
//
// 🔴 ESTE FICHERO SE LLAMABA `intent_transport_test.go` Y TENÍA OTRA MITAD (Plan 029 · T12): tres tests
// que comprobaban que `evt.Intent` se mapeaba a `ClassifiedIntent`, sellado dentro del SensitivePayload
// o en el espejo claro según hubiera pública de cifrado. Se BORRARON el 2026-08-24 con el push (Plan 044
// · Ola 1.6 · T1.6-5 · ADR-0045): no había que ajustarlos, porque el hecho que aseveraban dejó de existir
// —el proto no tiene campo `ClassifiedIntent` y `domain.InboundEvent` no tiene campo `Intent`—.
//
// Lo que aquellos tests cuidaban de verdad —que el SELLADO no se rompa— sigue cubierto, y por quien
// corresponde: `sealing_test.go` asevera los cuatro campos sensibles (text/push_name/from_pn/from_lid)
// en sus dos caminos. Y que el push no vuelva por la puerta de atrás lo custodia
// `internal/domain/inbound_sin_intent_test.go`, que mira el TIPO en vez de un camino concreto.

import (
	"context"
	"sync"
	"testing"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
)

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
