package cloudlink

import (
	"testing"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// TestReceiptStatusToProto ancla el mapeo del estado de dominio del acuse al enum del contrato (Plan 013
// §10.A): delivered→DELIVERED, read→READ, y cualquier estado fuera del enum cerrado (defensivo) →
// UNSPECIFIED. Es el corazón de SendReceipt (ReceiptEvent→MessageReceipt), aislado del transporte.
func TestReceiptStatusToProto(t *testing.T) {
	cases := []struct {
		name string
		in   domain.ReceiptStatus
		want cloudlinkv1.ReceiptStatus
	}{
		{"delivered", domain.ReceiptDelivered, cloudlinkv1.ReceiptStatus_RECEIPT_STATUS_DELIVERED},
		{"read", domain.ReceiptRead, cloudlinkv1.ReceiptStatus_RECEIPT_STATUS_READ},
		{"desconocido_cae_a_unspecified", domain.ReceiptStatus("otro"), cloudlinkv1.ReceiptStatus_RECEIPT_STATUS_UNSPECIFIED},
		{"vacio_cae_a_unspecified", domain.ReceiptStatus(""), cloudlinkv1.ReceiptStatus_RECEIPT_STATUS_UNSPECIFIED},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := receiptStatusToProto(tc.in); got != tc.want {
				t.Errorf("receiptStatusToProto(%q): got %v want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestSendReceipt_SinStreamNoPanic verifica que subir un acuse SIN stream vivo (Adapter recién creado,
// cl==nil) NO paniquea ni bloquea: se descarta con warn (follow-up outbox), como sessionSink.Deliver.
func TestSendReceipt_SinStreamNoPanic(t *testing.T) {
	a := NewAdapter(nil, nil, nil)
	// No hay stream (Run nunca corrió): currentClient()==nil. Debe retornar limpio.
	a.SendReceipt("cmd-x", domain.ReceiptEvent{
		SessionID:  "sess-1",
		Status:     domain.ReceiptDelivered,
		MessageIDs: []string{"WAMID-1"},
	})
}
