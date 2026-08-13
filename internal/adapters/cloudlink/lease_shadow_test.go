package cloudlink

// lease_shadow_test.go verifica (T4.4, Plan 055, D-055.4) el modo SOMBRA del gate de lease: con un
// Validator revocado, SOMBRA deja pasar el envío (el sendFunc real se invoca, Ack{ok=true}) y loguea el
// WARN de "habría sido bloqueado"; ENFORCE (el default, ya cubierto por
// TestE2E_CloudLinkAdapter_Flow/lease_revoked_blocks_sendtext) sigue bloqueando exactamente igual que
// antes de este campo — no se toca ese test, es la prueba de regresión de que el modo sombra no se
// filtra al camino por defecto. Reusa los helpers de e2e_test.go (newServerDouble, pushCloud, recvAck,
// sendCall) para no duplicar el bufconn/cliente real.
//
// Se pone rojo si: WithLeaseShadowMode deja de despachar el sendFunc con un lease revocado, si dejara de
// loguear el WARN de sombra, o si leaseShadowMode dejara de tener zero-value false (filtrado al default).

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"strings"
	"testing"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-cloudlink/lease"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const leaseShadowSessionID = "sess-shadow"

// TestLeaseShadowMode_RevokedLease_DispatchesAndWarns verifica que, en modo SOMBRA, un SendText con
// lease REVOCADO se despacha igual (el sendFunc real se invoca, Ack{ok=true}) y el WARN de "habría sido
// bloqueado" queda en el log — en vez de Ack{ok=false} sin invocar al Sender (comportamiento enforce).
func TestLeaseShadowMode_RevokedLease_DispatchesAndWarns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Lease controlado por el test: Issuer (priv) en "la nube", Validator (pub) en el Edge — mismo patrón
	// que TestE2E_CloudLinkAdapter_Flow.
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	issuer, err := lease.NewIssuer(priv)
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	newValidator := func() *lease.Validator { return lease.NewValidator(pub) }

	srv := newServerDouble()
	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	cloudlinkv1.RegisterCloudLinkServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	cc, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	// Logger a buffer inspeccionable: verificamos que el WARN de sombra quede escrito.
	var logBuf bytes.Buffer
	log := sharedlogger.New(sharedlogger.WithWriter(&logBuf), sharedlogger.WithJSON(true))

	adapter := NewAdapter(cc, log, newValidator, WithHeartbeatInterval(time.Hour), WithLeaseShadowMode(true))

	sent := make(chan sendCall, 8)
	adapter.Register(leaseShadowSessionID, "", func(_ context.Context, commandID, to, text string) error {
		sent <- sendCall{commandID: commandID, to: to, text: text}
		return nil
	}, nil, func() bool { return true })

	go func() { _ = adapter.Run(ctx) }()

	var stream cloudlinkv1.CloudLink_ConnectServer
	select {
	case stream = <-srv.streamCh:
	case <-ctx.Done():
		t.Fatalf("timeout esperando que el Adapter abra el stream: %v", ctx.Err())
	}

	// Revocar de entrada (sin aplicar nunca un lease vigente, CanOperate ya es false: "nunca applied").
	luRevoke, err := issuer.Revoke("edge-shadow", "tenant-1")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	pushCloud(t, stream, &cloudlinkv1.CloudToEdge{
		CommandId: "cmd-lease-revoke",
		SessionId: leaseShadowSessionID,
		Payload:   &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luRevoke},
	})

	const cmdID = "cmd-send-shadow"
	pushCloud(t, stream, &cloudlinkv1.CloudToEdge{
		CommandId: cmdID,
		SessionId: leaseShadowSessionID,
		Payload: &cloudlinkv1.CloudToEdge_SendText{
			SendText: &cloudlinkv1.SendText{To: "5491100000000", Text: "esto SI debe salir en modo sombra"},
		},
	})

	select {
	case sc := <-sent:
		if sc.to != "5491100000000" || sc.text != "esto SI debe salir en modo sombra" {
			t.Errorf("Sender invocado con (%q,%q), inesperado", sc.to, sc.text)
		}
	case <-ctx.Done():
		t.Fatalf("modo sombra debió despachar el envío pese al lease revocado: %v", ctx.Err())
	}

	ack := recvAck(t, ctx, srv, cmdID)
	if !ack.GetOk() {
		t.Errorf("Ack.ok: got false (err=%q) want true (modo sombra no bloquea)", ack.GetError())
	}

	if !strings.Contains(logBuf.String(), "HABRÍA sido bloqueado") {
		t.Errorf("el log no contiene el WARN de modo sombra; log=%s", logBuf.String())
	}
}
