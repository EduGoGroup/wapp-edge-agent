package cloudlink

// lease_log_test.go ancla que el LOG del gate de lease diga la VERDAD (Plan 056 · T6.2). En campo
// (2026-08-16) el Edge escribió "lease renovado/aplicado" en cada latido posterior a la restauración de
// una empresa mientras seguía respondiendo "SendText BLOQUEADO por lease no vigente": handleLeaseUpdate
// decidía el mensaje mirando el campo top-level lu.Revoked (que ni va firmado ni es fuente de verdad) en
// vez del estado del Validator, y la revocación es PEGAJOSA a propósito (anti-clon, ADR-0007) — Apply
// devuelve nil y descarta la renovación en silencio. Un operador leía un servicio sano donde había un
// servicio cortado.
//
// Se pone rojo si la renovación descartada vuelve a logearse como una renovación normal (o si deja de
// avisar de que los envíos siguen bloqueados y de que hay que re-registrar la sesión). El
// comportamiento NO se toca: la pegajosidad es la garantía anti-clon, y el test la re-verifica pidiendo
// un envío al final —que debe seguir bloqueado— para que el mensaje se contraste contra la conducta real.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-cloudlink/lease"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const leaseLogSessionID = "sess-lease-log"

// syncBuf es un bytes.Buffer con lock: el logger escribe desde las goroutines del Adapter (worker de la
// sesión, latido, drenaje) mientras el test lee, y sin el mutex -race canta.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestLeaseUpdateLog_RenovacionTrasRevocacion_NoSeLogeaComoRenovacion recorre los TRES casos por el cable
// real (revocar → renovar → intentar enviar) y verifica que cada uno deja su propio rastro:
// revocación = WARN de kill-switch, renovación descartada = WARN que dice que sigue bloqueado, y una sola
// línea "lease renovado/aplicado" en todo el test (la del lease vigente inicial, la única renovación que
// de verdad se aplicó).
func TestLeaseUpdateLog_RenovacionTrasRevocacion_NoSeLogeaComoRenovacion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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

	logBuf := &syncBuf{}
	log := sharedlogger.New(sharedlogger.WithWriter(logBuf), sharedlogger.WithJSON(true))

	// Modo ENFORCE (el default): el gate bloquea de verdad, que es la conducta contra la que se contrasta
	// el mensaje.
	adapter := NewAdapter(cc, log, newValidator, WithHeartbeatInterval(time.Hour))
	adapter.Register(leaseLogSessionID, "", func(_ context.Context, _, _, _ string) error {
		return nil
	}, nil, func() bool { return true })

	go func() { _ = adapter.Run(ctx) }()

	var stream cloudlinkv1.CloudLink_ConnectServer
	select {
	case stream = <-srv.streamCh:
	case <-ctx.Done():
		t.Fatalf("timeout esperando que el Adapter abra el stream: %v", ctx.Err())
	}

	// (1) Lease vigente: la ÚNICA renovación que de verdad se aplica en todo el test.
	luOK, err := issuer.Issue("edge-lease-log", "tenant-1", time.Hour, 1)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	pushCloud(t, stream, &cloudlinkv1.CloudToEdge{
		CommandId: "cmd-lease-ok",
		SessionId: leaseLogSessionID,
		Payload:   &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luOK},
	})

	// (2) Kill-switch.
	luRevoke, err := issuer.Revoke("edge-lease-log", "tenant-1")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	pushCloud(t, stream, &cloudlinkv1.CloudToEdge{
		CommandId: "cmd-lease-revoke",
		SessionId: leaseLogSessionID,
		Payload:   &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luRevoke},
	})

	// (3) La restauración vista en campo: la nube vuelve a emitir un lease vigente (counter mayor) y el
	// Validator lo DESCARTA en silencio por pegajosidad.
	luRenew, err := issuer.Issue("edge-lease-log", "tenant-1", time.Hour, 2)
	if err != nil {
		t.Fatalf("Issue (renovación): %v", err)
	}
	pushCloud(t, stream, &cloudlinkv1.CloudToEdge{
		CommandId: "cmd-lease-renew",
		SessionId: leaseLogSessionID,
		Payload:   &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luRenew},
	})

	// El envío final cumple dos papeles: prueba que la conducta NO cambió (sigue bloqueado) y ordena el
	// test — los comandos de una misma sesión los procesa un solo worker en orden, así que al llegar su
	// Ack los tres LeaseUpdate ya están logueados.
	const cmdID = "cmd-send-tras-renovacion"
	pushCloud(t, stream, &cloudlinkv1.CloudToEdge{
		CommandId: cmdID,
		SessionId: leaseLogSessionID,
		Payload: &cloudlinkv1.CloudToEdge_SendText{
			SendText: &cloudlinkv1.SendText{To: "5491100000000", Text: "esto NO debe salir: la revocación es pegajosa"},
		},
	})
	if ack := recvAck(t, ctx, srv, cmdID); ack.GetOk() {
		t.Fatalf("Ack.ok: got true want false — la renovación tras revocar NO debe des-revocar (pegajosidad, ADR-0007)")
	}

	out := logBuf.String()

	if n := strings.Count(out, "lease renovado/aplicado"); n != 1 {
		t.Errorf("líneas 'lease renovado/aplicado': got %d want 1 — la renovación descartada por la revocación "+
			"pegajosa NO puede logearse como una renovación normal; log=%s", n, out)
	}
	if !strings.Contains(out, "lease REVOCADO (kill-switch activo)") {
		t.Errorf("falta el WARN del kill-switch al revocar; log=%s", out)
	}
	for _, frag := range []string{"IGNORADO tras la revocación", "SIGUEN BLOQUEADOS", "RE-REGISTRAR"} {
		if !strings.Contains(out, frag) {
			t.Errorf("el WARN de la renovación descartada no dice %q; log=%s", frag, out)
		}
	}
	if !strings.Contains(out, `"level":"WARN"`) || !strings.Contains(out, `"envios_bloqueados":true`) {
		t.Errorf("el aviso de la renovación descartada debe ser WARN y greppable por envios_bloqueados; log=%s", out)
	}
}
