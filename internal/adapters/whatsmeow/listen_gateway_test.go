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

// TestResolvePushName cubre la decisión del nombre visible a fijar antes de anunciar presencia (§10.D,
// hallazgo e2e Plan 013): el nombre REAL ya conocido prevalece; el fallback configurado solo entra si el
// store aún no lo trae; sin fallback no se fuerza nada (SendPresence queda best-effort).
func TestResolvePushName(t *testing.T) {
	cases := []struct {
		name     string
		current  string
		fallback string
		wantName string
		wantSet  bool
	}{
		{name: "nombre real prevalece sobre fallback", current: "Cuenta Real", fallback: "wApp", wantName: "Cuenta Real", wantSet: false},
		{name: "store vacío usa el fallback", current: "", fallback: "wApp", wantName: "wApp", wantSet: true},
		{name: "sin real ni fallback no fuerza", current: "", fallback: "", wantName: "", wantSet: false},
		{name: "nombre real sin fallback se respeta", current: "Cuenta Real", fallback: "", wantName: "Cuenta Real", wantSet: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			name, set := resolvePushName(tc.current, tc.fallback)
			if name != tc.wantName || set != tc.wantSet {
				t.Fatalf("resolvePushName(%q,%q) = (%q,%v); se esperaba (%q,%v)",
					tc.current, tc.fallback, name, set, tc.wantName, tc.wantSet)
			}
		})
	}
}
