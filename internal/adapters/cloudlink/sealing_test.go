package cloudlink

// sealing_test.go — Plan 011 T4: sellado en tránsito del SensitivePayload (§6.3/§10.E/§10.H).
//
// Verifica el comportamiento de Deliver frente a la pública de cifrado de la nube:
//
//   - CON pública: el IncomingMessage en el cable trae EncPayload no vacío y los campos SENSIBLES
//     (text/push_name/from_pn/from_lid) VACÍOS; los NO sensibles (from/ts_unix/wa_message_id/is_group/
//     addressing_mode) siguen en claro. El EncPayload abre con OpenWith(priv) y reconstruye el
//     SensitivePayload original (round-trip SealFor↔OpenWith).
//   - SIN pública (nil): fallback claro (§10.H) — los sensibles viajan planos y EncPayload va vacío.
//
// Reusa el server-double y helpers de e2e_test.go (mismo paquete), con un builder local que permite
// inyectar la Option WithCloudEncPubKey (el newE2EHarness estándar no la expone).

import (
	"context"
	"net"
	"testing"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-shared/envelope"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

const sealSessionID = "sess-seal"

// newSealHarness arma el Adapter real contra el server-double (bufconn) con las Options dadas, registra
// UNA sesión y devuelve el arnés con su stream ya establecido. Espejo de newE2EHarness, pero con Options
// arbitrarias para inyectar WithCloudEncPubKey.
func newSealHarness(t *testing.T, ctx context.Context, opts ...Option) *e2eHarness {
	t.Helper()
	srv := newServerDouble()
	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	cloudlinkv1.RegisterCloudLinkServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	log := sharedlogger.New(sharedlogger.WithWriter(discardWriter{}), sharedlogger.WithJSON(true))
	adapter := NewAdapter(cc, log, nil, append([]Option{WithHeartbeatInterval(time.Hour)}, opts...)...)

	h := &e2eHarness{srv: srv, adapter: adapter, sent: make(map[string]chan sendCall), sentMedia: make(map[string]chan mediaCall)}
	h.register(sealSessionID)
	go func() { _ = adapter.Run(ctx) }()
	select {
	case h.stream = <-srv.streamCh:
	case <-ctx.Done():
		t.Fatalf("timeout esperando el stream del Adapter: %v", ctx.Err())
	}
	return h
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// sealEvent es el evento entrante de referencia: campos sensibles + no sensibles poblados.
func sealEvent() domain.InboundEvent {
	return domain.InboundEvent{
		MessageID: "WAMID-SEAL",
		Chat:      "593999@s.whatsapp.net",
		Sender:    "593999@s.whatsapp.net",
		SenderAlt: "10001@lid",
		Timestamp: time.Unix(1_700_000_000, 0),
		Text:      "texto sensible PII",
		PushName:  "Juan Cliente",
		IsGroup:   false,
	}
}

func TestDeliver_SealsSensitivePayload_WhenCloudPubPresent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub, priv, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}

	h := newSealHarness(t, ctx, WithCloudEncPubKey(pub))
	evt := sealEvent()
	if err := h.adapter.SinkFor(sealSessionID).Deliver(ctx, evt); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	msg := recvKind(t, ctx, h.srv, "IncomingMessage", func(m *cloudlinkv1.EdgeToCloud) bool {
		return m.GetIncoming() != nil
	})
	in := msg.GetIncoming()

	// EncPayload presente; sensibles VACÍOS en el cable.
	if len(in.GetEncPayload()) == 0 {
		t.Fatalf("EncPayload vacío: se esperaba el SensitivePayload sellado")
	}
	if in.GetText() != "" || in.GetPushName() != "" || in.GetFromPn() != "" || in.GetFromLid() != "" {
		t.Errorf("campos sensibles NO vacíos en el cable: text=%q push=%q pn=%q lid=%q",
			in.GetText(), in.GetPushName(), in.GetFromPn(), in.GetFromLid())
	}
	// NO sensibles en claro siempre.
	if in.GetFrom() != evt.Sender || in.GetWaMessageId() != evt.MessageID || in.GetTsUnix() != evt.Timestamp.Unix() {
		t.Errorf("campos no sensibles alterados: from=%q wa=%q ts=%d", in.GetFrom(), in.GetWaMessageId(), in.GetTsUnix())
	}

	// Round-trip: abrir con la privada y reconstruir el SensitivePayload original.
	raw, err := envelope.OpenWith(priv, in.GetEncPayload())
	if err != nil {
		t.Fatalf("OpenWith: %v", err)
	}
	var sp cloudlinkv1.SensitivePayload
	if err := proto.Unmarshal(raw, &sp); err != nil {
		t.Fatalf("Unmarshal SensitivePayload: %v", err)
	}
	if sp.GetText() != evt.Text {
		t.Errorf("text: got %q want %q", sp.GetText(), evt.Text)
	}
	if sp.GetPushName() != evt.PushName {
		t.Errorf("push_name: got %q want %q", sp.GetPushName(), evt.PushName)
	}
	if sp.GetFromPn() != "593999" {
		t.Errorf("from_pn: got %q want %q", sp.GetFromPn(), "593999")
	}
	if sp.GetFromLid() != "10001" {
		t.Errorf("from_lid: got %q want %q", sp.GetFromLid(), "10001")
	}
}

func TestDeliver_FallbackClaro_WhenNoCloudPub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Sin WithCloudEncPubKey => cloudEncPub nil => fallback claro (§10.H).
	h := newSealHarness(t, ctx)
	evt := sealEvent()
	if err := h.adapter.SinkFor(sealSessionID).Deliver(ctx, evt); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	msg := recvKind(t, ctx, h.srv, "IncomingMessage", func(m *cloudlinkv1.EdgeToCloud) bool {
		return m.GetIncoming() != nil
	})
	in := msg.GetIncoming()

	if len(in.GetEncPayload()) != 0 {
		t.Errorf("EncPayload NO vacío en fallback claro: %d bytes", len(in.GetEncPayload()))
	}
	if in.GetText() != evt.Text || in.GetPushName() != evt.PushName {
		t.Errorf("sensibles planos ausentes en fallback: text=%q push=%q", in.GetText(), in.GetPushName())
	}
	if in.GetFromPn() != "593999" || in.GetFromLid() != "10001" {
		t.Errorf("identidad plana ausente en fallback: pn=%q lid=%q", in.GetFromPn(), in.GetFromLid())
	}
}
