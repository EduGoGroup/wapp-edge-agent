package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
)

// fakeUnlinker es un doble de sessionUnlinker (el real es *sessionmgr.Manager.Unlink): permite testear
// el handler DELETE sin BD, cryptostore ni WhatsApp. Captura el session_id recibido.
type fakeUnlinker struct {
	err      error
	calledID string
}

func (f *fakeUnlinker) Unlink(_ context.Context, id string) error {
	f.calledID = id
	return f.err
}

// startServerWithUnlink levanta el Server real con un sessionUnlinker doble cableado por RegisterUnlink,
// sobre un Unix socket de prueba (mismo criterio de ruta corta bajo /tmp que startServer, por sun_path).
func startServerWithUnlink(t *testing.T, u sessionUnlinker) *http.Client {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "wapp-ctl-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "edge.sock")

	srv := New(Config{SocketPath: socket, Version: testVersion}, nil, fakeLister{})
	srv.RegisterUnlink(u)
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

// TestUnlink_OK: el handler devuelve 200 con el cuerpo de confirmación e invoca el Manager con el
// session_id de la ruta (re-llaveado: {id} = session_id, integración Plan 008).
func TestUnlink_OK(t *testing.T) {
	const sid = "11111111-1111-4111-8111-111111111111"
	f := &fakeUnlinker{}
	c := startServerWithUnlink(t, f)

	resp := do(t, c, http.MethodDelete, "/v1/sessions/"+sid)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	var got unlinkResponse
	decode(t, resp, &got)
	if !got.Unlinked || got.SessionID != sid {
		t.Errorf("cuerpo inesperado: %+v", got)
	}
	if f.calledID != sid {
		t.Errorf("session_id pasado al Manager: got %q", f.calledID)
	}
}

// TestUnlink_NotFound: si el Manager devuelve sessionmgr.ErrSessionNotFound, el handler responde 404
// con envelope.
func TestUnlink_NotFound(t *testing.T) {
	c := startServerWithUnlink(t, &fakeUnlinker{err: sessionmgr.ErrSessionNotFound})

	resp := do(t, c, http.MethodDelete, "/v1/sessions/22222222-2222-4222-8222-222222222222")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}
	assertErrorEnvelope(t, resp, codeNotFound)
}

// TestUnlink_InternalError: un error genérico del Manager (fallo de limpieza) se traduce a 500 con
// envelope.
func TestUnlink_InternalError(t *testing.T) {
	c := startServerWithUnlink(t, &fakeUnlinker{err: errors.New("boom")})

	resp := do(t, c, http.MethodDelete, "/v1/sessions/33333333-3333-4333-8333-333333333333")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500", resp.StatusCode)
	}
	assertErrorEnvelope(t, resp, codeInternal)
}
