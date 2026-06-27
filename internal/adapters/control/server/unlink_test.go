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

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// fakeUnlinker es un doble de Unlinker (el real es *app.UnlinkSession): permite testear el handler DELETE
// sin BD, cryptostore ni WhatsApp.
type fakeUnlinker struct {
	res       app.UnlinkResult
	err       error
	calledJID string
}

func (f *fakeUnlinker) Run(_ context.Context, jid string) (app.UnlinkResult, error) {
	f.calledJID = jid
	if f.err != nil {
		return app.UnlinkResult{}, f.err
	}
	return f.res, nil
}

// startServerWithUnlink levanta el Server real con un Unlinker doble cableado por RegisterUnlink, sobre
// un Unix socket de prueba (mismo criterio de ruta corta bajo /tmp que startServer, por sun_path).
func startServerWithUnlink(t *testing.T, u Unlinker) *http.Client {
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

// TestUnlink_OK: el handler devuelve 200 con el cuerpo de confirmación e invoca el use case con el JID.
func TestUnlink_OK(t *testing.T) {
	f := &fakeUnlinker{res: app.UnlinkResult{
		JID:          "111@s.whatsapp.net",
		Previous:     domain.Session{JID: "111@s.whatsapp.net", State: domain.SessionStateActive},
		RemoteLogout: app.LogoutOK,
	}}
	c := startServerWithUnlink(t, f)

	resp := do(t, c, http.MethodDelete, "/v1/sessions/111@s.whatsapp.net")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	var got unlinkResponse
	decode(t, resp, &got)
	if !got.Unlinked || got.JID != "111@s.whatsapp.net" {
		t.Errorf("cuerpo inesperado: %+v", got)
	}
	if got.RemoteLogout != "ok" || got.PreviousState != "active" {
		t.Errorf("desenlace inesperado: %+v", got)
	}
	if f.calledJID != "111@s.whatsapp.net" {
		t.Errorf("JID pasado al use case: got %q", f.calledJID)
	}
}

// TestUnlink_NotFound: si el use case devuelve ErrSessionNotFound, el handler responde 404 con envelope.
func TestUnlink_NotFound(t *testing.T) {
	c := startServerWithUnlink(t, &fakeUnlinker{err: app.ErrSessionNotFound})

	resp := do(t, c, http.MethodDelete, "/v1/sessions/nope@s.whatsapp.net")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}
	assertErrorEnvelope(t, resp, codeNotFound)
}

// TestUnlink_InternalError: un error genérico del use case se traduce a 500 con envelope.
func TestUnlink_InternalError(t *testing.T) {
	c := startServerWithUnlink(t, &fakeUnlinker{err: errors.New("boom")})

	resp := do(t, c, http.MethodDelete, "/v1/sessions/111@s.whatsapp.net")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500", resp.StatusCode)
	}
	assertErrorEnvelope(t, resp, codeInternal)
}
