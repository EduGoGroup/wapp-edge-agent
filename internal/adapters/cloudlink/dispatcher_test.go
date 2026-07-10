package cloudlink

// dispatcher_test.go — Tests -race del despacho CONCURRENTE del demux (Plan 027 Ola 1 · T1, H1/H7).
//
// Cubren las dos garantías del despacho concurrente por session_id:
//   (a) NO head-of-line blocking: una operación lenta de la sesión A NO frena a la sesión B.
//   (b) Orden por sesión PRESERVADO: los comandos de UNA sesión se procesan FIFO aunque el despacho
//       entre sesiones sea concurrente.
// Y el cierre limpio: al cancelar el ctx, Run (con su dispatcher y workers) retorna sin colgarse.
//
// Mismo arnés que e2e_test.go (Adapter real cliente contra un server-double bufconn, sin red/TLS), pero
// con sendFunc a medida por sesión para orquestar el bloqueo/orden. Sin gate de lease (newValidator nil)
// para aislar el camino del demux.

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// rawHarness cablea el Adapter real (cliente) contra el server-double, dejando el registro de sesiones al
// test (a diferencia de e2eHarness, que registra con capturas fijas). Expone runDone para poder afirmar
// el cierre limpio del loop de Run.
type rawHarness struct {
	srv     *serverDouble
	stream  cloudlinkv1.CloudLink_ConnectServer
	adapter *Adapter
	runDone chan struct{} // se cierra cuando adapter.Run retorna
}

func newRawHarness(t *testing.T, ctx context.Context, opts ...Option) *rawHarness {
	t.Helper()

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

	log := sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}), sharedlogger.WithJSON(true))
	// Heartbeat largo: no queremos ruido periódico; solo el latido inicial por sesión.
	adapter := NewAdapter(cc, log, nil, append([]Option{WithHeartbeatInterval(time.Hour)}, opts...)...)

	return &rawHarness{srv: srv, adapter: adapter, runDone: make(chan struct{})}
}

// run arranca el loop del Adapter (tras registrar las sesiones) y espera el handshake del stream.
func (h *rawHarness) run(t *testing.T, ctx context.Context) {
	t.Helper()
	go func() {
		_ = h.adapter.Run(ctx)
		close(h.runDone)
	}()
	select {
	case h.stream = <-h.srv.streamCh:
	case <-ctx.Done():
		t.Fatalf("timeout esperando que el Adapter abra el stream: %v", ctx.Err())
	}
}

// sendText empuja un SendText cloud->edge para una sesión.
func sendText(t *testing.T, h *rawHarness, sessionID, cmdID, to, text string) {
	pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
		CommandId: cmdID,
		SessionId: sessionID,
		Payload:   &cloudlinkv1.CloudToEdge_SendText{SendText: &cloudlinkv1.SendText{To: to, Text: text}},
	})
}

// TestDispatcher_SlowSessionDoesNotBlockOthers ancla H1: un SendText de la sesión A que se queda colgado
// en su sendFunc NO impide que un SendText de la sesión B se procese. Antes del despacho concurrente el
// handleCommand síncrono en el loop del stream congelaba a todas las sesiones.
func TestDispatcher_SlowSessionDoesNotBlockOthers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h := newRawHarness(t, ctx)

	startedA := make(chan struct{}) // A entró en su sendFunc
	releaseA := make(chan struct{}) // el test libera a A
	doneB := make(chan struct{})    // B terminó su sendFunc

	// Sesión A: sendFunc que se BLOQUEA hasta que el test lo libere (simula una operación lenta —p.ej.
	// una descarga de media). Respeta el ctx para no colgar el cierre.
	h.adapter.Register("A", "", func(opCtx context.Context, _, _, _ string) error {
		close(startedA)
		select {
		case <-releaseA:
		case <-opCtx.Done():
		}
		return nil
	}, nil, func() bool { return true })

	// Sesión B: sendFunc que señala que corrió.
	h.adapter.Register("B", "", func(_ context.Context, _, _, _ string) error {
		close(doneB)
		return nil
	}, nil, func() bool { return true })

	h.run(t, ctx)

	// 1) Cuelga a A y espera a que su worker esté DENTRO del sendFunc (no solo encolado).
	sendText(t, h, "A", "cmd-a", "5490000000000", "lento")
	select {
	case <-startedA:
	case <-ctx.Done():
		t.Fatalf("timeout: la sesión A nunca entró en su sendFunc: %v", ctx.Err())
	}

	// 2) Con A colgado, B debe procesarse igualmente. Este es el corazón de H1.
	sendText(t, h, "B", "cmd-b", "5491111111111", "rapido")
	select {
	case <-doneB:
	case <-time.After(3 * time.Second):
		t.Fatalf("HOL blocking: la sesión B quedó bloqueada por la sesión A lenta")
	}

	// 3) Libera a A y confirma su Ack (cierre del ciclo sin fugas).
	close(releaseA)
	if ack := recvAck(t, ctx, h.srv, "cmd-a"); !ack.GetOk() {
		t.Errorf("Ack de A: ok=false inesperado (%q)", ack.GetError())
	}
}

// TestDispatcher_PreservesPerSessionOrder ancla la invariante del demux: dentro de UNA sesión los
// comandos se procesan en el ORDEN en que llegaron, aunque el despacho entre sesiones sea concurrente
// (un único worker FIFO por session_id).
func TestDispatcher_PreservesPerSessionOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h := newRawHarness(t, ctx)

	const n = 20
	got := make(chan string, n)
	h.adapter.Register("A", "", func(_ context.Context, _, _, text string) error {
		got <- text
		return nil
	}, nil, func() bool { return true })

	h.run(t, ctx)

	want := make([]string, n)
	for i := 0; i < n; i++ {
		txt := string(rune('a' + i))
		want[i] = txt
		sendText(t, h, "A", "cmd", "5490000000000", txt)
	}

	for i := 0; i < n; i++ {
		select {
		case g := <-got:
			if g != want[i] {
				t.Fatalf("orden por sesión roto en la posición %d: got %q want %q", i, g, want[i])
			}
		case <-ctx.Done():
			t.Fatalf("timeout esperando el comando %d: %v", i, ctx.Err())
		}
	}
}

// TestDispatcher_CleanShutdown confirma el cierre limpio (sin goroutines fugadas): con una operación en
// vuelo que respeta el ctx, cancelar el ctx del Adapter hace que Run (con su dispatcher y sus workers)
// retorne sin colgarse.
func TestDispatcher_CleanShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	runCtx, runCancel := context.WithCancel(ctx)
	h := newRawHarness(t, runCtx)

	started := make(chan struct{})
	var once sync.Once
	h.adapter.Register("A", "", func(opCtx context.Context, _, _, _ string) error {
		once.Do(func() { close(started) })
		<-opCtx.Done() // operación que respeta la cancelación
		return opCtx.Err()
	}, nil, func() bool { return true })

	h.run(t, runCtx)

	sendText(t, h, "A", "cmd-a", "5490000000000", "en-vuelo")
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatalf("timeout: la operación en vuelo nunca arrancó: %v", ctx.Err())
	}

	// Apagado del agente: el dispatcher debe cancelar la operación en vuelo y drenar sus workers.
	runCancel()
	select {
	case <-h.runDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("cierre no limpio: Run no retornó tras cancelar el ctx (posible goroutine fugada)")
	}
}
