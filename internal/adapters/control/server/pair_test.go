package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// secretDEK simula material que el núcleo "sella" DENTRO de app.Pair. El contrato /v1 NUNCA debe
// transportarlo: los tests verifican que no aparece en ningún cuerpo de respuesta.
const secretDEK = "DEK-SECRETA-32-BYTES-NO-DEBE-FUGARSE"

// fakePairer es un doble del puerto sessionPairer (el real es *sessionmgr.Manager.Pair): publica QRs en
// el sink y bloquea hasta que el test empuja el resultado terminal por release. Simula sellar una DEK
// ficticia (secretDEK) dentro de Pair, fuera del contrato, para verificar la invariante DEK-fuera-del-/v1.
type fakePairer struct {
	qrs     []string
	release chan error // el test empuja nil (success) o un error (fallo); buffer 1.
}

func newFakePairer(qrs ...string) *fakePairer {
	return &fakePairer{qrs: qrs, release: make(chan error, 1)}
}

func (f *fakePairer) Pair(ctx context.Context, qr app.QRSink) (app.PairResult, error) {
	for _, code := range f.qrs {
		_ = qr.ShowQR(code)
	}
	select {
	case err := <-f.release:
		if err != nil {
			return app.PairResult{}, err
		}
		_ = secretDEK // "sellada" en custodia local, jamás retornada por el puerto.
		return app.PairResult{WaJID: "fake-jid@s.whatsapp.net", SessionID: "fake-session-id"}, nil
	case <-ctx.Done():
		return app.PairResult{}, ctx.Err()
	}
}

var _ sessionPairer = (*fakePairer)(nil)

// startPairServer levanta el Server real sobre un Unix socket de prueba con el sessionPairer dado
// registrado, y devuelve un http.Client que marca por ese socket. Espejo de startServer (server_test.go)
// más RegisterPairing antes de Serve.
func startPairServer(t *testing.T, p sessionPairer) *http.Client {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "wapp-ctl-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "edge.sock")

	srv := New(Config{SocketPath: socket, Version: testVersion}, nil, fakeLister{})
	srv.RegisterPairing(p)
	ln, err := srv.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}
}

// readBody lee y cierra el cuerpo devolviéndolo como string (para aserciones de invariante).
func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("leyendo body: %v", err)
	}
	return string(b)
}

// TestPair_PostReturnsIDAndQR: POST /v1/sessions/pair devuelve 200 con id no vacío y un QR en
// data-URL PNG (el doble publica un QR de inmediato).
func TestPair_PostReturnsIDAndQR(t *testing.T) {
	f := newFakePairer("2@qr-uno")
	t.Cleanup(func() { f.release <- nil })
	c := startPairServer(t, f)

	resp := do(t, c, http.MethodPost, "/v1/sessions/pair")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	var got pairResponse
	decode(t, resp, &got)
	if got.ID == "" {
		t.Fatal("id vacío")
	}
	if got.Status != "pending" {
		t.Errorf("status: got %q, want pending", got.Status)
	}
	if !strings.HasPrefix(got.QR, "data:image/png;base64,") {
		t.Errorf("qr no es data-URL PNG: %.40q", got.QR)
	}
}

// TestPair_PollPendingToSuccess: tras el POST, el poll está pending (con QR), y transita a success
// cuando el doble completa.
func TestPair_PollPendingToSuccess(t *testing.T) {
	f := newFakePairer("2@qr-uno")
	c := startPairServer(t, f)

	var post pairResponse
	decode(t, do(t, c, http.MethodPost, "/v1/sessions/pair"), &post)

	// Estado intermedio: pending con QR vigente renderizado.
	var poll pollResponse
	decode(t, do(t, c, http.MethodGet, "/v1/sessions/"+post.ID+"/pair"), &poll)
	if poll.Status != "pending" {
		t.Fatalf("poll inicial: got %q, want pending", poll.Status)
	}
	if !strings.HasPrefix(poll.QR, "data:image/png;base64,") {
		t.Errorf("poll pending sin QR data-URL: %.40q", poll.QR)
	}

	// El doble completa el pairing → el poll debe transitar a success.
	f.release <- nil
	waitStatus(t, c, post.ID, "success")

	var done pollResponse
	decode(t, do(t, c, http.MethodGet, "/v1/sessions/"+post.ID+"/pair"), &done)
	if done.Status != "success" {
		t.Fatalf("poll final: got %q, want success", done.Status)
	}
	if done.QR != "" {
		t.Errorf("en success el QR debe venir vacío, got %.40q", done.QR)
	}
	if done.Error != "" {
		t.Errorf("en success no debe haber error: %q", done.Error)
	}
}

// TestPair_PollError: si el doble falla, el poll transita a error con mensaje (sin material sensible).
func TestPair_PollError(t *testing.T) {
	f := newFakePairer("2@qr-uno")
	c := startPairServer(t, f)

	var post pairResponse
	decode(t, do(t, c, http.MethodPost, "/v1/sessions/pair"), &post)

	f.release <- context.DeadlineExceeded
	waitStatus(t, c, post.ID, "error")

	var poll pollResponse
	decode(t, do(t, c, http.MethodGet, "/v1/sessions/"+post.ID+"/pair"), &poll)
	if poll.Status != "error" {
		t.Fatalf("poll: got %q, want error", poll.Status)
	}
	if poll.Error == "" {
		t.Error("error vacío en estado error")
	}
}

// TestPair_PollUnknownID: poll con id inexistente → 404 envelope.
func TestPair_PollUnknownID(t *testing.T) {
	c := startPairServer(t, newFakePairer("2@qr"))
	resp := do(t, c, http.MethodGet, "/v1/sessions/no-existe/pair")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}
	assertErrorEnvelope(t, resp, codeNotFound)
}

// TestPair_SecondConcurrentPairConflict: un segundo POST mientras hay uno en curso → 409 conflict
// (decisión MVP: un pairing activo a la vez). Tras completar el primero, un nuevo POST vuelve a 200.
func TestPair_SecondConcurrentPairConflict(t *testing.T) {
	f := newFakePairer("2@qr-uno")
	c := startPairServer(t, f)

	var post pairResponse
	decode(t, do(t, c, http.MethodPost, "/v1/sessions/pair"), &post)

	// Segundo POST mientras el primero sigue en curso (el doble aún no fue liberado) → 409.
	resp := do(t, c, http.MethodPost, "/v1/sessions/pair")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("segundo pair: got %d, want 409", resp.StatusCode)
	}
	assertErrorEnvelope(t, resp, codeConflict)

	// Al completar el primero, se libera el single-flight y un nuevo pair vuelve a aceptarse.
	f.release <- nil
	waitStatus(t, c, post.ID, "success")
	// El doble de un nuevo pairing necesita su propio canal; reusamos uno fresco.
}

// TestPair_DEKNeverCrossesContract: ni el POST ni el poll (pending/success) pueden contener la DEK
// que el núcleo sella dentro de app.Pair. Es la invariante ADR-0014/0007 verificada sobre el cable.
func TestPair_DEKNeverCrossesContract(t *testing.T) {
	f := newFakePairer("2@qr-uno")
	c := startPairServer(t, f)

	postBody := readBody(t, do(t, c, http.MethodPost, "/v1/sessions/pair"))
	if strings.Contains(postBody, secretDEK) {
		t.Fatalf("la DEK se filtró en POST /pair: %s", postBody)
	}
	// Re-extrae el id del POST para el poll.
	var post pairResponse
	if err := json.Unmarshal([]byte(postBody), &post); err != nil {
		t.Fatalf("parse POST body: %v", err)
	}

	pendingBody := readBody(t, do(t, c, http.MethodGet, "/v1/sessions/"+post.ID+"/pair"))
	if strings.Contains(pendingBody, secretDEK) {
		t.Fatalf("la DEK se filtró en poll pending: %s", pendingBody)
	}

	f.release <- nil
	waitStatus(t, c, post.ID, "success")
	successBody := readBody(t, do(t, c, http.MethodGet, "/v1/sessions/"+post.ID+"/pair"))
	if strings.Contains(successBody, secretDEK) {
		t.Fatalf("la DEK se filtró en poll success: %s", successBody)
	}
}

// waitStatus hace polling al endpoint hasta que el estado del pairing id sea want, o falla por
// timeout. Cubre la asincronía entre liberar el doble y que la goroutine marque Finish.
func waitStatus(t *testing.T, c *http.Client, id, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var poll pollResponse
		decode(t, do(t, c, http.MethodGet, "/v1/sessions/"+id+"/pair"), &poll)
		if poll.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("el pairing %s no alcanzó el estado %q a tiempo", id, want)
}
