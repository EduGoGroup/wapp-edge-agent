package cloudlink

// outbox_integration_test.go — Integración del outbox durable con el Adapter CloudLink (Plan 027 Ola 3 · T2,
// H2). Usa un outbox REAL (internal/adapters/outbox sobre SQLite migrada) contra el server-double bufconn.
// Cubre: encolar con stream caído, drenar EN ORDEN al conectar, orden por sesión preservado, envío en vivo
// sin encolar cuando hay stream, e idempotencia (el outbox queda vacío tras drenar: sin reenvío duplicado).

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/outbox"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// newOutbox abre una BD única temporal migrada y devuelve un outbox real.
func newTestOutbox(t *testing.T) *outbox.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "edge.db")
	database, err := db.OpenAndMigrate(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	ob, err := outbox.New(context.Background(), database, 1000, 0,
		sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}), sharedlogger.WithJSON(true)))
	if err != nil {
		t.Fatalf("outbox.New: %v", err)
	}
	return ob
}

// newOutboxAdapter construye un Adapter con outbox pero SIN arrancarlo (sin stream): currentClient()==nil.
func newOutboxAdapter(t *testing.T, ob *outbox.Store) *Adapter {
	t.Helper()
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return nil, net.ErrClosed }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	log := sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}), sharedlogger.WithJSON(true))
	a := NewAdapter(cc, log, nil, WithHeartbeatInterval(time.Hour), WithOutbox(ob))
	a.Register("A", "", func(context.Context, string, string, string) error { return nil }, nil, func() bool { return true })
	return a
}

func incomingEvt(msgID, text string) domain.InboundEvent {
	return domain.InboundEvent{
		MessageID: msgID,
		Chat:      "5490000000000@s.whatsapp.net",
		Sender:    "5490000000000@s.whatsapp.net",
		Timestamp: time.Unix(1_700_000_000, 0),
		Text:      text,
	}
}

// TestOutbox_EnqueueWhenStreamDown: sin stream, Deliver ENCOLA en vez de descartar (H2).
func TestOutbox_EnqueueWhenStreamDown(t *testing.T) {
	ctx := context.Background()
	ob := newTestOutbox(t)
	a := newOutboxAdapter(t, ob) // sin Run: no hay stream

	if err := a.SinkFor("A").Deliver(ctx, incomingEvt("m1", "hola")); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	items, err := ob.Drain(ctx, 10)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(items) != 1 || items[0].SessionID != "A" || items[0].Kind != "incoming" {
		t.Fatalf("esperaba 1 entrante encolado para A, got %+v", items)
	}
	if !a.isPending("A") {
		t.Fatalf("la sesión A debería quedar marcada como pendiente tras encolar")
	}
}

// TestOutbox_DrainInOrderOnReconnect: se encolan 3 entrantes con el stream caído (orden A) y, al conectar,
// se drenan EN ORDEN al server-double; el outbox queda vacío (idempotencia: no se reenvían de nuevo).
func TestOutbox_DrainInOrderOnReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ob := newTestOutbox(t)

	// 1) Adapter SIN stream: encola 3 entrantes en orden (m1, m2, m3). El primero por no-stream, los
	//    siguientes por el guard de orden (sesión ya pendiente).
	down := newOutboxAdapter(t, ob)
	for _, m := range []struct{ id, txt string }{{"m1", "1"}, {"m2", "2"}, {"m3", "3"}} {
		if err := down.SinkFor("A").Deliver(ctx, incomingEvt(m.id, m.txt)); err != nil {
			t.Fatalf("Deliver %s: %v", m.id, err)
		}
	}
	if items, _ := ob.Drain(ctx, 10); len(items) != 3 {
		t.Fatalf("esperaba 3 encolados antes de conectar, hay %d", len(items))
	}

	// 2) Levanta el server-double y un Adapter VIVO con el MISMO outbox: al conectar drena el backlog.
	srv := newServerDouble()
	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	cloudlinkv1.RegisterCloudLinkServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	log := sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}), sharedlogger.WithJSON(true))
	live := NewAdapter(cc, log, nil, WithHeartbeatInterval(time.Hour), WithOutbox(ob), WithOutboxDrainInterval(50*time.Millisecond))
	live.Register("A", "", func(context.Context, string, string, string) error { return nil }, nil, func() bool { return true })
	go func() { _ = live.Run(ctx) }()

	// 3) El server-double recibe los 3 entrantes EN ORDEN (m1, m2, m3), ignorando heartbeats.
	for _, want := range []string{"m1", "m2", "m3"} {
		msg := recvKind(t, ctx, srv, "IncomingMessage", func(m *cloudlinkv1.EdgeToCloud) bool {
			return m.GetIncoming() != nil
		})
		if got := msg.GetIncoming().GetWaMessageId(); got != want {
			t.Fatalf("orden de drenaje: got %q want %q", got, want)
		}
	}

	// 4) Idempotencia: el outbox queda vacío (drenados y borrados), sin reenvío en el siguiente tick.
	deadline := time.Now().Add(3 * time.Second)
	for {
		items, _ := ob.Drain(ctx, 10)
		if len(items) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("el outbox no se vació tras drenar: quedan %d", len(items))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestOutbox_LiveSendDoesNotEnqueue: con stream vivo y sin backlog, Deliver envía en vivo y NO toca el outbox.
func TestOutbox_LiveSendDoesNotEnqueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ob := newTestOutbox(t)
	// Adapter VIVO con outbox contra el server-double.
	srv := newServerDouble()
	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	cloudlinkv1.RegisterCloudLinkServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	log := sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}), sharedlogger.WithJSON(true))
	a := NewAdapter(cc, log, nil, WithHeartbeatInterval(time.Hour), WithOutbox(ob))
	a.Register("A", "", func(context.Context, string, string, string) error { return nil }, nil, func() bool { return true })
	go func() { _ = a.Run(ctx) }()

	// Espera el handshake (stream vivo).
	select {
	case <-srv.streamCh:
	case <-ctx.Done():
		t.Fatalf("timeout esperando el stream: %v", ctx.Err())
	}

	if err := a.SinkFor("A").Deliver(ctx, incomingEvt("m1", "en vivo")); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	// Llega por el stream…
	msg := recvKind(t, ctx, srv, "IncomingMessage", func(m *cloudlinkv1.EdgeToCloud) bool { return m.GetIncoming() != nil })
	if msg.GetIncoming().GetWaMessageId() != "m1" {
		t.Fatalf("entrante en vivo inesperado: %q", msg.GetIncoming().GetWaMessageId())
	}
	// …y el outbox sigue vacío (no se encoló).
	if items, _ := ob.Drain(ctx, 10); len(items) != 0 {
		t.Fatalf("envío en vivo NO debería encolar; hay %d en el outbox", len(items))
	}
}
