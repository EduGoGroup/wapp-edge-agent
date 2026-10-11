package cloudlink

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"testing"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-cloudlink/lease"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsappfalso"
)

const leaseFalsoSessionID = "sess-whatsapp-falso"

// TestWhatsAppFalso_ConElLeaseRevocadoNoSeCapturaNingunEnvio es la prueba de que la doble llave NO tiene
// excepción de pruebas (ADR-0007, ADR-0049 pieza 9; RW.3.c). El emisor de la sesión es el transporte FALSO
// de verdad, detrás del adaptador de CloudLink de verdad y de su gate 2-de-2:
//
//   - CONTROL POSITIVO: con un lease válido emitido por el servidor, un SendText llega al transporte falso
//     y queda capturado. Sin este paso el test pasaría también con un cableado roto que no captura nunca.
//   - LA PRUEBA: el servidor REVOCA el lease; el siguiente SendText se rechaza (Ack.ok=false) y el
//     transporte falso NO captura nada más.
//
// El transporte falso no sabe nada del lease, y así debe ser: quien decide es el gate, que está ANTES.
func TestWhatsAppFalso_ConElLeaseRevocadoNoSeCapturaNingunEnvio(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	log := sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}))

	// El transporte falso, escuchando.
	registro := whatsappfalso.NuevoRegistro(0)
	escucha := whatsappfalso.NuevaEscucha(registro, log)
	escucha.SetCola(nil, leaseFalsoSessionID)
	escuchaFin := make(chan struct{})
	go func() { _ = escucha.Listen(ctx, nil); close(escuchaFin) }()
	t.Cleanup(func() { cancel(); <-escuchaFin })

	// El adaptador de CloudLink SIN modo sombra: el gate bloquea de verdad.
	adapter := NewAdapter(cc, log, newValidator, WithHeartbeatInterval(time.Hour))
	adapter.Register(leaseFalsoSessionID, "",
		func(ctx context.Context, commandID, to, text string) error {
			_, err := escucha.SendViaLiveClientTracked(ctx, commandID, to, text)
			return err
		},
		func(ctx context.Context, commandID, to, url, filename, mime, kind, caption string) error {
			_, err := escucha.SendMediaViaLiveClientTracked(ctx, commandID, to, url, filename, mime, kind, caption)
			return err
		},
		func() bool { return true })
	go func() { _ = adapter.Run(ctx) }()

	var stream cloudlinkv1.CloudLink_ConnectServer
	select {
	case stream = <-srv.streamCh:
	case <-ctx.Done():
		t.Fatalf("timeout esperando que el Adapter abra el stream: %v", ctx.Err())
	}

	enviar := func(cmdID, texto string) *cloudlinkv1.Ack {
		t.Helper()
		pushCloud(t, stream, &cloudlinkv1.CloudToEdge{
			CommandId: cmdID,
			SessionId: leaseFalsoSessionID,
			Payload:   &cloudlinkv1.CloudToEdge_SendText{SendText: &cloudlinkv1.SendText{To: "5491100000000", Text: texto}},
		})
		return recvAck(t, ctx, srv, cmdID)
	}
	capturados := func() int {
		envios, _ := registro.De(leaseFalsoSessionID)
		return len(envios)
	}

	// CONTROL POSITIVO: lease válido ⇒ el envío se captura. El primer Ack puede llegar antes de que la
	// escucha falsa publique su puerta, así que se reintenta hasta que uno entra.
	luOK, err := issuer.Issue("edge-falso", "tenant-1", time.Hour, 1)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	pushCloud(t, stream, &cloudlinkv1.CloudToEdge{
		CommandId: "cmd-lease-ok", SessionId: leaseFalsoSessionID,
		Payload: &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luOK},
	})
	entro := false
	for i := 0; i < 200 && !entro; i++ {
		ack := enviar("cmd-ok-"+time.Now().Format("150405.000000000"), "con lease valido")
		entro = ack.GetOk()
		if !entro {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !entro {
		t.Fatal("con un lease VÁLIDO ningún SendText entró: el control positivo falla y el resto no probaría nada")
	}
	if n := capturados(); n != 1 {
		t.Fatalf("con lease válido y un envío aceptado, el transporte falso capturó %d", n)
	}

	// LA PRUEBA: el servidor revoca.
	luRevoke, err := issuer.Revoke("edge-falso", "tenant-1")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	pushCloud(t, stream, &cloudlinkv1.CloudToEdge{
		CommandId: "cmd-lease-revoke", SessionId: leaseFalsoSessionID,
		Payload: &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luRevoke},
	})
	ack := enviar("cmd-revocado", "esto NO debe capturarse")
	if ack.GetOk() {
		t.Error("con el lease REVOCADO el SendText se aceptó (Ack.ok=true): el WhatsApp falso se salta el kill-switch")
	}
	if n := capturados(); n != 1 {
		t.Errorf("tras revocar el lease el transporte falso tiene %d envíos capturados y debe seguir en 1: "+
			"capturó un envío que el servidor ya no autoriza", n)
	}
}
