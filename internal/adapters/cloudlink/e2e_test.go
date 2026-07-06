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
	"sync"
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

type sendCall struct{ commandID, to, text string }

// mediaCall captura lo que el sendMediaFunc fake recibió (Plan 017): metadatos del archivo + presigned URL
// (que en producción el despachador DESCARGA; el fake solo verifica el cableado del gate y del demux).
type mediaCall struct{ commandID, to, url, filename, mime, kind, caption string }

// e2eHarness cablea el Adapter MULTIPLEXOR real (cliente) contra el server-double (bufconn + insecure),
// igual patrón que los tests de transporte de cloudlink. Registra una o varias sesiones y captura por
// session_id lo que cada sendFunc fake despacha, para verificar el demux de entrada por sesión.
type e2eHarness struct {
	srv     *serverDouble
	stream  cloudlinkv1.CloudLink_ConnectServer
	adapter *Adapter

	mu        sync.Mutex
	sent      map[string]chan sendCall  // session_id -> capturas (to,text) del sendFunc fake de esa sesión
	sentMedia map[string]chan mediaCall // session_id -> capturas del sendMediaFunc fake de esa sesión
}

// newE2EHarness arma el multiplexor con el factory de Validator dado (nil => sin gate de lease) y
// registra las sesiones indicadas (cada una con su captura de envíos y hasDEK=true). Heartbeat largo:
// solo nos interesa el latido inicial por sesión (ancla de conexión), sin ruido periódico.
func newE2EHarness(t *testing.T, ctx context.Context, newValidator ValidatorFactory, sessionIDs ...string) *e2eHarness {
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

	// Logger a buffer descartable: el sub-test ZK inspecciona que NO se logueen secretos.
	log := sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}), sharedlogger.WithJSON(true))

	// 2) Adapter MULTIPLEXOR real (un stream, N sesiones). Las sesiones se registran ANTES de Run: al
	// conectar, el adapter emite el heartbeat inicial de cada una.
	adapter := NewAdapter(cc, log, newValidator, WithHeartbeatInterval(time.Hour))

	h := &e2eHarness{srv: srv, adapter: adapter, sent: make(map[string]chan sendCall), sentMedia: make(map[string]chan mediaCall)}
	for _, id := range sessionIDs {
		h.register(id)
	}

	go func() { _ = adapter.Run(ctx) }()

	// Handshake: esperamos el stream (el Adapter conectó). Los heartbeats iniciales por sesión los filtra
	// recvKind (busca por tipo), así que no hace falta drenarlos explícitamente.
	select {
	case h.stream = <-srv.streamCh:
	case <-ctx.Done():
		t.Fatalf("timeout esperando que el Adapter abra el stream: %v", ctx.Err())
	}
	return h
}

// register da de alta una sesión en el multiplex con un sendFunc fake que captura (to,text) en su propio
// canal y hasDEK=true (gate 2-de-2: la parte DEK presente).
func (h *e2eHarness) register(sessionID string) {
	ch := make(chan sendCall, 8)
	mch := make(chan mediaCall, 8)
	h.mu.Lock()
	h.sent[sessionID] = ch
	h.sentMedia[sessionID] = mch
	h.mu.Unlock()
	h.adapter.Register(sessionID, "", func(_ context.Context, commandID, to, text string) error {
		ch <- sendCall{commandID: commandID, to: to, text: text}
		return nil
	}, func(_ context.Context, commandID, to, url, filename, mime, kind, caption string) error {
		mch <- mediaCall{commandID: commandID, to: to, url: url, filename: filename, mime: mime, kind: kind, caption: caption}
		return nil
	}, func() bool { return true })
}

// sentCh devuelve el canal de capturas de envío de la sesión.
func (h *e2eHarness) sentCh(sessionID string) chan sendCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sent[sessionID]
}

// sentMediaCh devuelve el canal de capturas de envío de MEDIA de la sesión.
func (h *e2eHarness) sentMediaCh(sessionID string) chan mediaCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sentMedia[sessionID]
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
	newValidator := func() *lease.Validator { return lease.NewValidator(pub) }

	h := newE2EHarness(t, ctx, newValidator, e2eSessionID)

	// (a) SALIDA: SinkFor(session).Deliver(InboundEvent) -> IncomingMessage con campos de negocio mapeados.
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
		if err := h.adapter.SinkFor(e2eSessionID).Deliver(ctx, evt); err != nil {
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
		case sc := <-h.sentCh(e2eSessionID):
			if sc.to != "5491100000000" || sc.text != "respuesta de la nube" {
				t.Errorf("Sender invocado con (%q,%q), inesperado", sc.to, sc.text)
			}
			// El command_id del SendText DEBE propagarse al sendFunc (Plan 013 §10.E: alimenta el Correlator).
			if sc.commandID != cmdID {
				t.Errorf("command_id propagado: got %q want %q", sc.commandID, cmdID)
			}
		case <-ctx.Done():
			t.Fatalf("timeout: el Sender no fue invocado: %v", ctx.Err())
		}

		ack := recvAck(t, ctx, h.srv, cmdID)
		if !ack.GetOk() {
			t.Errorf("Ack.ok: got false (err=%q) want true", ack.GetError())
		}
	})

	// (b.2) ACUSE: SendReceipt(command_id, ReceiptEvent) -> EdgeToCloud{MessageReceipt} por el MISMO
	// stream, con el mapeo de estado y el command_id de correlación (Plan 013 T2a §10.A/§10.E/§10.G).
	t.Run("receipt_upload", func(t *testing.T) {
		ts := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
		h.adapter.SendReceipt("cmd-send-ok", domain.ReceiptEvent{
			MessageIDs: []string{"WAMID-AAA", "WAMID-BBB"},
			Status:     domain.ReceiptRead,
			Timestamp:  ts,
			SessionID:  e2eSessionID,
		})
		msg := recvKind(t, ctx, h.srv, "MessageReceipt", func(m *cloudlinkv1.EdgeToCloud) bool {
			return m.GetReceipt() != nil
		})
		if msg.GetSessionId() != e2eSessionID {
			t.Errorf("session_id: got %q want %q", msg.GetSessionId(), e2eSessionID)
		}
		r := msg.GetReceipt()
		if r.GetSessionId() != e2eSessionID {
			t.Errorf("receipt.session_id: got %q want %q", r.GetSessionId(), e2eSessionID)
		}
		if r.GetCommandId() != "cmd-send-ok" {
			t.Errorf("receipt.command_id: got %q want %q", r.GetCommandId(), "cmd-send-ok")
		}
		if r.GetStatus() != cloudlinkv1.ReceiptStatus_RECEIPT_STATUS_READ {
			t.Errorf("receipt.status: got %v want READ", r.GetStatus())
		}
		if r.GetTimestamp() != ts.Unix() {
			t.Errorf("receipt.timestamp: got %d want %d", r.GetTimestamp(), ts.Unix())
		}
		if got := r.GetMessageIds(); len(got) != 2 || got[0] != "WAMID-AAA" || got[1] != "WAMID-BBB" {
			t.Errorf("receipt.message_ids: got %v want [WAMID-AAA WAMID-BBB]", got)
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
		case sc := <-h.sentCh(e2eSessionID):
			t.Fatalf("el Sender fue invocado pese al lease revocado: (%q,%q)", sc.to, sc.text)
		default:
		}
	})
}

// TestE2E_CloudLinkAdapter_SendMedia ancla el cableado del comando SendMedia (Plan 017 §7) en el Adapter
// real: (a) con lease VIGENTE, un CloudToEdge{SendMedia} invoca al sendMediaFunc de la sesión con los
// metadatos correctos (to/filename/mime/caption + kind mapeado del MediaKind) y responde Ack{ok=true}; la
// presigned URL se propaga tal cual (el despachador la DESCARGA, aquí solo se verifica el demux); (b) con
// lease REVOCADO el emisor de media NO se invoca (kill-switch) y el Ack es {ok=false}.
func TestE2E_CloudLinkAdapter_SendMedia(t *testing.T) {
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

	h := newE2EHarness(t, ctx, newValidator, e2eSessionID)

	// Lease vigente para habilitar el envío de media.
	luOK, err := issuer.Issue("edge-1", "tenant-1", time.Hour, 1)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
		CommandId: "cmd-lease-ok",
		SessionId: e2eSessionID,
		Payload:   &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luOK},
	})

	t.Run("sendmedia_document_ack_ok", func(t *testing.T) {
		const cmdID = "cmd-media-ok"
		pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
			CommandId: cmdID,
			SessionId: e2eSessionID,
			Payload: &cloudlinkv1.CloudToEdge_SendMedia{SendMedia: &cloudlinkv1.SendMedia{
				To:       "5491100000000",
				Caption:  "acá va la lista de precios",
				Mime:     "application/pdf",
				Filename: "Lista de precios.pdf",
				Kind:     cloudlinkv1.MediaKind_MEDIA_KIND_DOCUMENT,
				Src:      &cloudlinkv1.SendMedia_PresignedUrl{PresignedUrl: "https://r2.example/wapp/media/lista.pdf?sig=abc"},
			}},
		})

		select {
		case mc := <-h.sentMediaCh(e2eSessionID):
			if mc.to != "5491100000000" || mc.filename != "Lista de precios.pdf" || mc.mime != "application/pdf" {
				t.Errorf("media invocada con metadatos inesperados: %+v", mc)
			}
			if mc.kind != "document" {
				t.Errorf("kind: got %q want document", mc.kind)
			}
			if mc.caption != "acá va la lista de precios" {
				t.Errorf("caption: got %q", mc.caption)
			}
			if mc.url != "https://r2.example/wapp/media/lista.pdf?sig=abc" {
				t.Errorf("presigned URL no propagada tal cual: %q", mc.url)
			}
			if mc.commandID != cmdID {
				t.Errorf("command_id propagado: got %q want %q", mc.commandID, cmdID)
			}
		case <-ctx.Done():
			t.Fatalf("timeout: el emisor de media no fue invocado: %v", ctx.Err())
		}

		ack := recvAck(t, ctx, h.srv, cmdID)
		if !ack.GetOk() {
			t.Errorf("Ack.ok: got false (err=%q) want true", ack.GetError())
		}
	})

	t.Run("lease_revoked_blocks_sendmedia", func(t *testing.T) {
		luRevoke, err := issuer.Revoke("edge-1", "tenant-1")
		if err != nil {
			t.Fatalf("Revoke: %v", err)
		}
		pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
			CommandId: "cmd-lease-revoke",
			SessionId: e2eSessionID,
			Payload:   &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luRevoke},
		})

		const cmdID = "cmd-media-blocked"
		pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
			CommandId: cmdID,
			SessionId: e2eSessionID,
			Payload: &cloudlinkv1.CloudToEdge_SendMedia{SendMedia: &cloudlinkv1.SendMedia{
				To:       "5491100000000",
				Mime:     "image/png",
				Filename: "orden.png",
				Kind:     cloudlinkv1.MediaKind_MEDIA_KIND_IMAGE,
				Src:      &cloudlinkv1.SendMedia_PresignedUrl{PresignedUrl: "https://r2.example/x"},
			}},
		})

		ack := recvAck(t, ctx, h.srv, cmdID)
		if ack.GetOk() {
			t.Errorf("Ack.ok: got true want false (lease revocado debe bloquear el media)")
		}
		select {
		case mc := <-h.sentMediaCh(e2eSessionID):
			t.Fatalf("el emisor de media fue invocado pese al lease revocado: %+v", mc)
		default:
		}
	})
}

// TestE2E_CloudLinkMux_TwoSessions ancla el MULTIPLEX de T7 (ADR-0008 §"un stream", ADR-0016 §5 "lease
// por sesión"): DOS sesiones (A y B) sobre UN ÚNICO stream Connect. Verifica:
//
//	(1) SALIDA etiquetada: un evento por SinkFor(A) sale con session_id=A y uno por SinkFor(B) con B,
//	    POR EL MISMO stream.
//	(2) DEMUX de entrada: un CloudToEdge{session_id=B, SendText} se enruta al sender de B, no al de A.
//	(3) LEASE POR SESIÓN: revocar el lease de A bloquea los envíos de A pero NO los de B.
//	(4) LIFECYCLE: Unregister(A) y, tras él, los comandos para A se ignoran limpio (sin Ack ni envío),
//	    mientras B sigue respondiendo por el mismo stream.
func TestE2E_CloudLinkMux_TwoSessions(t *testing.T) {
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
	// Factory: cada sesión obtiene un Validator FRESCO (estado de lease independiente) sobre la misma
	// clave del Edge — justo lo que prueba el "lease por sesión".
	newValidator := func() *lease.Validator { return lease.NewValidator(pub) }

	const sessA, sessB = "sess-A", "sess-B"
	h := newE2EHarness(t, ctx, newValidator, sessA, sessB)

	// (1) SALIDA etiquetada por sesión, mismo stream.
	t.Run("salida_etiquetada_por_sesion", func(t *testing.T) {
		evtA := domain.InboundEvent{MessageID: "A1", Sender: "111@s.whatsapp.net", Text: "hola A", Timestamp: time.Unix(1_700_000_000, 0)}
		evtB := domain.InboundEvent{MessageID: "B1", Sender: "222@s.whatsapp.net", Text: "hola B", Timestamp: time.Unix(1_700_000_001, 0)}
		if err := h.adapter.SinkFor(sessA).Deliver(ctx, evtA); err != nil {
			t.Fatalf("Deliver A: %v", err)
		}
		if err := h.adapter.SinkFor(sessB).Deliver(ctx, evtB); err != nil {
			t.Fatalf("Deliver B: %v", err)
		}
		mA := recvKind(t, ctx, h.srv, "Incoming de A", func(m *cloudlinkv1.EdgeToCloud) bool {
			return m.GetIncoming() != nil && m.GetSessionId() == sessA
		})
		if mA.GetIncoming().GetWaMessageId() != "A1" {
			t.Errorf("Incoming de A: wa_message_id=%q want A1", mA.GetIncoming().GetWaMessageId())
		}
		mB := recvKind(t, ctx, h.srv, "Incoming de B", func(m *cloudlinkv1.EdgeToCloud) bool {
			return m.GetIncoming() != nil && m.GetSessionId() == sessB
		})
		if mB.GetIncoming().GetWaMessageId() != "B1" {
			t.Errorf("Incoming de B: wa_message_id=%q want B1", mB.GetIncoming().GetWaMessageId())
		}
	})

	// Aplica un lease VIGENTE a A y a B (counter=1 cada uno: estado independiente) para habilitar envíos.
	for _, id := range []string{sessA, sessB} {
		lu, err := issuer.Issue("edge-1", "tenant-1", time.Hour, 1)
		if err != nil {
			t.Fatalf("Issue(%s): %v", id, err)
		}
		pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
			CommandId: "lease-" + id,
			SessionId: id,
			Payload:   &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: lu},
		})
	}

	// (2) DEMUX de entrada: SendText a B se enruta a B, no a A.
	t.Run("demux_entrada_a_B", func(t *testing.T) {
		const cmdID = "send-B"
		pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
			CommandId: cmdID,
			SessionId: sessB,
			Payload:   &cloudlinkv1.CloudToEdge_SendText{SendText: &cloudlinkv1.SendText{To: "222", Text: "para B"}},
		})
		select {
		case sc := <-h.sentCh(sessB):
			if sc.to != "222" || sc.text != "para B" {
				t.Errorf("B recibió (%q,%q), inesperado", sc.to, sc.text)
			}
		case <-ctx.Done():
			t.Fatalf("timeout: B no recibió el SendText: %v", ctx.Err())
		}
		ack := recvAck(t, ctx, h.srv, cmdID)
		if !ack.GetOk() {
			t.Errorf("Ack de B: ok=false (err=%q) want true", ack.GetError())
		}
		// A no debe haber recibido nada (el Ack de B sirve de barrera: el comando ya se procesó).
		select {
		case sc := <-h.sentCh(sessA):
			t.Fatalf("A recibió un envío que era para B: (%q,%q)", sc.to, sc.text)
		default:
		}
	})

	// (3) LEASE POR SESIÓN: revocar A bloquea A, B sigue operando.
	t.Run("lease_por_sesion", func(t *testing.T) {
		luRev, err := issuer.Revoke("edge-1", "tenant-1")
		if err != nil {
			t.Fatalf("Revoke: %v", err)
		}
		pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
			CommandId: "lease-revoke-A",
			SessionId: sessA,
			Payload:   &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luRev},
		})

		// SendText a A -> bloqueado (Ack ok=false, sender mudo).
		const cmdBlocked = "send-A-blocked"
		pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
			CommandId: cmdBlocked,
			SessionId: sessA,
			Payload:   &cloudlinkv1.CloudToEdge_SendText{SendText: &cloudlinkv1.SendText{To: "111", Text: "no debe salir"}},
		})
		ackA := recvAck(t, ctx, h.srv, cmdBlocked)
		if ackA.GetOk() {
			t.Errorf("Ack de A: ok=true want false (lease de A revocado debe bloquear)")
		}
		select {
		case sc := <-h.sentCh(sessA):
			t.Fatalf("A envió pese al lease revocado: (%q,%q)", sc.to, sc.text)
		default:
		}

		// SendText a B -> sigue funcionando (su lease no fue tocado).
		const cmdBOK = "send-B-ok"
		pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
			CommandId: cmdBOK,
			SessionId: sessB,
			Payload:   &cloudlinkv1.CloudToEdge_SendText{SendText: &cloudlinkv1.SendText{To: "222", Text: "B sigue viva"}},
		})
		select {
		case sc := <-h.sentCh(sessB):
			if sc.text != "B sigue viva" {
				t.Errorf("B recibió texto inesperado: %q", sc.text)
			}
		case <-ctx.Done():
			t.Fatalf("timeout: B bloqueada indebidamente tras revocar A: %v", ctx.Err())
		}
		ackB := recvAck(t, ctx, h.srv, cmdBOK)
		if !ackB.GetOk() {
			t.Errorf("Ack de B: ok=false (err=%q) want true (revocar A no debe afectar a B)", ackB.GetError())
		}
	})

	// (4) LIFECYCLE: Unregister(A); comandos para A se ignoran limpio; B sigue respondiendo.
	t.Run("unregister_A_ignora_comandos", func(t *testing.T) {
		h.adapter.Unregister(sessA)

		// Comando para A tras el unregister: debe ignorarse (sin Ack ni envío, sin panic).
		pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
			CommandId: "send-A-post",
			SessionId: sessA,
			Payload:   &cloudlinkv1.CloudToEdge_SendText{SendText: &cloudlinkv1.SendText{To: "111", Text: "post unlink"}},
		})

		// Barrera determinista: un Ping a B debe responder Pong. El loop de recepción procesa en orden
		// sobre una sola goroutine, así que al llegar el Pong el comando para A ya fue procesado (ignorado).
		pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
			CommandId: "ping-B",
			SessionId: sessB,
			Payload:   &cloudlinkv1.CloudToEdge_Ping{Ping: &cloudlinkv1.Ping{Nonce: 7}},
		})
		pong := recvKind(t, ctx, h.srv, "Pong de B", func(m *cloudlinkv1.EdgeToCloud) bool {
			return m.GetPong() != nil && m.GetSessionId() == sessB
		})
		if pong.GetPong().GetNonce() != 7 {
			t.Errorf("Pong de B: nonce=%d want 7", pong.GetPong().GetNonce())
		}

		// A no envió nada y no hay Ack para "send-A-post" (el comando se ignoró por sesión desconocida).
		select {
		case sc := <-h.sentCh(sessA):
			t.Fatalf("A envió tras Unregister: (%q,%q)", sc.to, sc.text)
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

	// Sin factory de Validator (nil) para aislar el camino de SALIDA puro.
	h := newE2EHarness(t, ctx, nil, e2eSessionID)

	evt := domain.InboundEvent{
		MessageID: "WAMID-ZK",
		Chat:      "5491100000000@s.whatsapp.net",
		Sender:    "5491100000000@s.whatsapp.net",
		Timestamp: time.Unix(1_700_000_000, 0),
		Text:      "mensaje de negocio",
	}
	if err := h.adapter.SinkFor(e2eSessionID).Deliver(ctx, evt); err != nil {
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
	// Nota (Plan 010): from_pn es CONTENIDO DE NEGOCIO derivado del Sender (número E.164 sin '+'),
	// no material secreto; entra en el conjunto esperado. Con SenderAlt/PushName/AddressingMode
	// vacíos en este evento, esos campos no se pueblan.
	want := &cloudlinkv1.IncomingMessage{
		From:        evt.Sender,
		Text:        evt.Text,
		TsUnix:      evt.Timestamp.Unix(),
		WaMessageId: evt.MessageID,
		FromPn:      "5491100000000",
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
