package whatsmeow

import (
	"context"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// *ListenGateway debe satisfacer el puerto app.LiveSender (envío por cliente vivo).
var _ app.LiveSender = (*ListenGateway)(nil)

// TestSendViaLiveClient_SinClienteVivo verifica que enviar SIN una sesión de escucha activa (cliente
// nil) falla con error claro en lugar de hacer panic por desreferencia. No abre socket ni red.
func TestSendViaLiveClient_SinClienteVivo(t *testing.T) {
	g := &ListenGateway{} // client == nil: ninguna escucha en curso.

	err := g.SendViaLiveClient(context.Background(), "5215555555555", "hola")
	if err == nil {
		t.Fatal("se esperaba error al enviar sin cliente vivo, se obtuvo nil")
	}
}
