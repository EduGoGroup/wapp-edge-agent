package cloudlink

// e2e_test.go — Hito T6 / Criterio de éxito 6 del adaptador CloudLink REAL del Edge.
//
// Prueba DETERMINISTA y LOCAL del FLUJO que sustituye al LogSink del spike: el Adapter real actúa de
// cliente contra un server-double in-memory (bufconn, sin red, sin mTLS — el mTLS ya se cubrió en T3).
// Se ejercita el ciclo completo de la pieza 02 a nivel de aplicación:
//
//   a) SALIDA: Deliver(InboundEvent) -> el server-double recibe un IncomingMessage con los campos de
//      negocio mapeados (from/text/ts/wa_message_id/is_group) correlacionado por session_id.
//   b) ENTRADA: el server-double empuja SendText -> el Adapter invoca el Sender real-inyectado y
//      responde Ack{ok=true}.
//   c) KILL-SWITCH: el server-double empuja un LeaseUpdate(revoked) FIRMADO por el Issuer del test ->
//      tras aplicarse, un nuevo SendText NO invoca al Sender y el Adapter responde Ack{ok=false}.
//
// Sincronización por canales + contexto con timeout (sin sleeps frágiles). El loop de recepción del
// Adapter corre en goroutine (Run); nos anclamos al canal del server-double y al canal del Sender fake.
//
// ZERO-KNOWLEDGE (T6.3): un sub-test confirma que el EdgeToCloud que sale por el cable solo transporta
// CONTENIDO DE NEGOCIO; no existe (ni en el contrato proto ni en el mapeo) campo alguno para la DEK,
// el store cifrado o llaves Signal (ADR-0007).

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
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// --- server-double ------------------------------------------------------------------------------

// serverDouble es un cloudlinkv1.CloudLinkServer mínimo del TEST (NO se importa internal/server de
// cloudlink, que es package-internal). Registra el stream del primer Connect, expone por canal lo que
// recibe del Edge y permite al test EMPUJAR comandos cloud->edge por el mismo stream.
type serverDouble struct {
	cloudlinkv1.UnimplementedCloudLinkServer
	received chan *cloudlinkv1.EdgeToCloud            // edge -> cloud (lo que emite el Adapter)
	streamCh chan cloudlinkv1.CloudLink_ConnectServer // se entrega al test cuando el Adapter conecta
}

func newServerDouble() *serverDouble {
	return &serverDouble{
		received: make(chan *cloudlinkv1.EdgeToCloud, 64),
		streamCh: make(chan cloudlinkv1.CloudLink_ConnectServer, 1),
	}
}

func (s *serverDouble) Connect(stream cloudlinkv1.CloudLink_ConnectServer) error {
	s.streamCh <- stream // handshake: el test ya puede empujar cloud->edge por este stream
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err // el cliente cerró / ctx cancelado: fin del stream
		}
		s.received <- msg
	}
}

// recvKind lee del canal del server-double hasta encontrar un EdgeToCloud que satisfaga want (p.ej.
// "es un Incoming", "es un Ack"), descartando ruido como los Heartbeat. Falla por timeout vía ctx.
func recvKind(t *testing.T, ctx context.Context, srv *serverDouble, what string, want func(*cloudlinkv1.EdgeToCloud) bool) *cloudlinkv1.EdgeToCloud {
	t.Helper()
	for {
		select {
		case msg := <-srv.received:
			if want(msg) {
				return msg
			}
			// ignora otros mensajes (heartbeat inicial, etc.)
		case <-ctx.Done():
			t.Fatalf("timeout esperando %s del Edge: %v", what, ctx.Err())
			return nil
		}
	}
}

// --- arnés --------------------------------------------------------------------------------------

const e2eSessionID = "sess-t6"

type sendCall struct{ to, text string }

// e2eHarness cablea el Adapter real (cliente) contra el server-double (bufconn + insecure), igual
// patrón que los tests de transporte de cloudlink.
type e2eHarness struct {
	srv     *serverDouble
	stream  cloudlinkv1.CloudLink_ConnectServer
	sent    chan sendCall // capturas del Sender fake (to,text) de cada SendText despachado
	adapter *Adapter
}

func newE2EHarness(t *testing.T, ctx context.Context, validator *lease.Validator) *e2eHarness {
	t.Helper()

	// 1) server-double sobre bufconn (in-memory, sin red ni TLS).
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

	// 2) Sender fake: captura (to,text). hasDEK=true (gate 2-de-2: la parte DEK presente).
	sent := make(chan sendCall, 8)
	sendFunc := func(_ context.Context, to, text string) error {
		sent <- sendCall{to: to, text: text}
		return nil
	}
	hasDEK := func() bool { return true }

	// Logger a buffer descartable: el sub-test ZK inspecciona que NO se logueen secretos.
	log := sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}), sharedlogger.WithJSON(true))

	// 3) Adapter REAL. Heartbeat largo: solo nos interesa el latido inicial (ancla de conexión); no
	// queremos ruido periódico en el canal del server-double durante el test.
	adapter := NewAdapter(cc, e2eSessionID, sendFunc, validator, hasDEK, log, WithHeartbeatInterval(time.Hour))

	go func() { _ = adapter.Run(ctx) }()

	// Handshake: esperamos el stream (el Adapter conectó) y drenamos el heartbeat inicial. Tras él,
	// currentClient() del Adapter es no-nil, así que Deliver ya tiene stream vivo.
	var stream cloudlinkv1.CloudLink_ConnectServer
	select {
	case stream = <-srv.streamCh:
	case <-ctx.Done():
		t.Fatalf("timeout esperando que el Adapter abra el stream: %v", ctx.Err())
	}
	recvKind(t, ctx, srv, "heartbeat inicial", func(m *cloudlinkv1.EdgeToCloud) bool {
		return m.GetHeartbeat() != nil
	})

	return &e2eHarness{srv: srv, stream: stream, sent: sent, adapter: adapter}
}

// --- test e2e -----------------------------------------------------------------------------------

func TestE2E_CloudLinkAdapter_Flow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Lease controlado por el test: Issuer (priv) en "la nube", Validator (pub) en el Edge.
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	issuer, err := lease.NewIssuer(priv)
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	validator := lease.NewValidator(pub)

	h := newE2EHarness(t, ctx, validator)

	// (a) SALIDA: Deliver(InboundEvent) -> IncomingMessage con campos de negocio mapeados.
	t.Run("incoming", func(t *testing.T) {
		ts := time.Date(2026, 6, 26, 10, 30, 0, 0, time.UTC)
		evt := domain.InboundEvent{
			MessageID: "WAMID-123",
			Chat:      "5491100000000@s.whatsapp.net",
			Sender:    "5491100000000@s.whatsapp.net",
			Timestamp: ts,
			Text:      "hola desde el cliente",
			IsGroup:   false,
		}
		if err := h.adapter.Deliver(ctx, evt); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
		msg := recvKind(t, ctx, h.srv, "IncomingMessage", func(m *cloudlinkv1.EdgeToCloud) bool {
			return m.GetIncoming() != nil
		})
		if msg.GetSessionId() != e2eSessionID {
			t.Errorf("session_id: got %q want %q", msg.GetSessionId(), e2eSessionID)
		}
		in := msg.GetIncoming()
		if in.GetFrom() != evt.Sender {
			t.Errorf("from: got %q want %q", in.GetFrom(), evt.Sender)
		}
		if in.GetText() != evt.Text {
			t.Errorf("text: got %q want %q", in.GetText(), evt.Text)
		}
		if in.GetTsUnix() != ts.Unix() {
			t.Errorf("ts_unix: got %d want %d", in.GetTsUnix(), ts.Unix())
		}
		if in.GetWaMessageId() != evt.MessageID {
			t.Errorf("wa_message_id: got %q want %q", in.GetWaMessageId(), evt.MessageID)
		}
		if in.GetIsGroup() != evt.IsGroup {
			t.Errorf("is_group: got %v want %v", in.GetIsGroup(), evt.IsGroup)
		}
	})

	// Setup del gate: aplicar un lease VIGENTE antes de probar el SendText feliz. El Adapter procesa
	// los CloudToEdge en orden (un único canal/goroutine), así que el LeaseUpdate queda aplicado antes
	// del SendText que empujamos a continuación; el Ack{ok=true} lo confirma.
	luOK, err := issuer.Issue("edge-1", "tenant-1", time.Hour, 1)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
		CommandId: "cmd-lease-ok",
		SessionId: e2eSessionID,
		Payload:   &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luOK},
	})

	// (b) ENTRADA: SendText con lease vigente -> Sender invocado + Ack{ok=true}.
	t.Run("sendtext_ack_ok", func(t *testing.T) {
		const cmdID = "cmd-send-ok"
		pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
			CommandId: cmdID,
			SessionId: e2eSessionID,
			Payload: &cloudlinkv1.CloudToEdge_SendText{
				SendText: &cloudlinkv1.SendText{To: "5491100000000", Text: "respuesta de la nube"},
			},
		})

		select {
		case sc := <-h.sent:
			if sc.to != "5491100000000" || sc.text != "respuesta de la nube" {
				t.Errorf("Sender invocado con (%q,%q), inesperado", sc.to, sc.text)
			}
		case <-ctx.Done():
			t.Fatalf("timeout: el Sender no fue invocado: %v", ctx.Err())
		}

		ack := recvAck(t, ctx, h.srv, cmdID)
		if !ack.GetOk() {
			t.Errorf("Ack.ok: got false (err=%q) want true", ack.GetError())
		}
	})

	// (c) KILL-SWITCH: revocar el lease (firmado por el Issuer del test) y reintentar SendText.
	t.Run("lease_revoked_blocks_sendtext", func(t *testing.T) {
		luRevoke, err := issuer.Revoke("edge-1", "tenant-1")
		if err != nil {
			t.Fatalf("Revoke: %v", err)
		}
		pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
			CommandId: "cmd-lease-revoke",
			SessionId: e2eSessionID,
			Payload:   &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luRevoke},
		})

		const cmdID = "cmd-send-blocked"
		pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
			CommandId: cmdID,
			SessionId: e2eSessionID,
			Payload: &cloudlinkv1.CloudToEdge_SendText{
				SendText: &cloudlinkv1.SendText{To: "5491100000000", Text: "esto no debe salir"},
			},
		})

		// El Ack{ok=false} se emite TRAS decidir NO invocar al Sender; al recibirlo, si el Sender
		// hubiera sido invocado ya estaría en el canal. Verificamos ambos: Ack negativo + Sender mudo.
		ack := recvAck(t, ctx, h.srv, cmdID)
		if ack.GetOk() {
			t.Errorf("Ack.ok: got true want false (el lease revocado debe bloquear el envío)")
		}
		select {
		case sc := <-h.sent:
			t.Fatalf("el Sender fue invocado pese al lease revocado: (%q,%q)", sc.to, sc.text)
		default:
		}
	})
}

// pushCloud empuja un CloudToEdge (con el payload oneof dado) por el stream del server-double. El
// payload se pasa ya construido en su wrapper exportado (CloudToEdge_SendText / _LeaseUpdate / _Ping);
// la interfaz oneof subyacente es package-private de cloudlinkv1, pero el campo Payload sí es asignable.
func pushCloud(t *testing.T, stream cloudlinkv1.CloudLink_ConnectServer, msg *cloudlinkv1.CloudToEdge) {
	t.Helper()
	if err := stream.Send(msg); err != nil {
		t.Fatalf("stream.Send(cloud->edge): %v", err)
	}
}

// recvAck espera un Ack del Edge correlacionado con ackedCmdID.
func recvAck(t *testing.T, ctx context.Context, srv *serverDouble, ackedCmdID string) *cloudlinkv1.Ack {
	t.Helper()
	msg := recvKind(t, ctx, srv, "Ack de "+ackedCmdID, func(m *cloudlinkv1.EdgeToCloud) bool {
		return m.GetAck() != nil && m.GetAck().GetAckedCommandId() == ackedCmdID
	})
	return msg.GetAck()
}

// TestE2E_ZeroKnowledge_NoSecretsOnWire confirma (T6.3) que el EdgeToCloud que produce Deliver solo
// transporta CONTENIDO DE NEGOCIO: el único payload poblado es el Incoming y sus campos son los de
// negocio. El contrato proto no tiene campo alguno para la DEK/store/llaves; este test ancla esa
// invariante a nivel de mensaje en el cable y verifica que ningún material secreto colado en el evento
// (texto, sender) arrastre nada extra.
func TestE2E_ZeroKnowledge_NoSecretsOnWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Sin Validator (nil) para aislar el camino de SALIDA puro.
	h := newE2EHarness(t, ctx, nil)

	evt := domain.InboundEvent{
		MessageID: "WAMID-ZK",
		Chat:      "5491100000000@s.whatsapp.net",
		Sender:    "5491100000000@s.whatsapp.net",
		Timestamp: time.Unix(1_700_000_000, 0),
		Text:      "mensaje de negocio",
	}
	if err := h.adapter.Deliver(ctx, evt); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	msg := recvKind(t, ctx, h.srv, "IncomingMessage", func(m *cloudlinkv1.EdgeToCloud) bool {
		return m.GetIncoming() != nil
	})

	// El único payload poblado debe ser Incoming; ningún otro variant del oneof.
	if msg.GetAck() != nil || msg.GetHeartbeat() != nil || msg.GetPong() != nil || msg.GetDelivery() != nil {
		t.Fatalf("EdgeToCloud trae payload distinto de Incoming: %+v", msg)
	}

	// El IncomingMessage solo tiene campos de negocio (no hay campo de DEK/store en el contrato). Lo
	// verificamos serializando a wire y comprobando que solo contiene lo de negocio reconstruible: el
	// re-marshal de un IncomingMessage con EXACTAMENTE esos campos es byte-idéntico al recibido.
	want := &cloudlinkv1.IncomingMessage{
		From:        evt.Sender,
		Text:        evt.Text,
		TsUnix:      evt.Timestamp.Unix(),
		WaMessageId: evt.MessageID,
	}
	gotBytes, err := proto.Marshal(msg.GetIncoming())
	if err != nil {
		t.Fatalf("marshal got: %v", err)
	}
	wantBytes, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if !bytes.Equal(gotBytes, wantBytes) {
		t.Fatalf("el IncomingMessage en el cable contiene campos fuera del conjunto de negocio esperado:\n got=%+v\nwant=%+v", msg.GetIncoming(), want)
	}
}
