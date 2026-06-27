package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/supervisor"
)

// startFakeCore levanta un núcleo falso escuchando en un Unix socket en dir temporal: responde
// /v1/health y /v1/ping. Devuelve la ruta del socket. Sirve para ejercitar el reverse-proxy sin lanzar
// un proceso (no se prueba aquí exec/SIGTERM, eso lo cubre el test del paquete supervisor).
func startFakeCore(t *testing.T) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "edge.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","version":"fake"}`))
	})
	mux.HandleFunc("GET /v1/ping", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return sock
}

// TestProxyRoutesToCore: una request /v1/ping al router se proxya al socket del núcleo falso.
func TestProxyRoutesToCore(t *testing.T) {
	sock := startFakeCore(t)
	sup := supervisor.New(supervisor.Config{SocketPath: sock}, nil)
	router := newRouter(sup, sock, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/ping", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("proxy /v1/ping status = %d; quería 200", rec.Code)
	}
	if rec.Body.String() != "pong" {
		t.Fatalf("proxy /v1/ping body = %q; quería pong", rec.Body.String())
	}
}

// TestProxyDaemonDown: con un socket inexistente (núcleo caído), el proxy traduce el fallo a 503 +
// envelope code "daemon_down" (no un 502 crudo). Contrato para la UI (T5).
func TestProxyDaemonDown(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "missing.sock")
	sup := supervisor.New(supervisor.Config{SocketPath: sock}, nil)
	router := newRouter(sup, sock, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("daemon-down status = %d; quería 503", rec.Code)
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("daemon-down body no es el envelope JSON: %v (%s)", err, rec.Body.String())
	}
	if body.Error.Code != codeDaemonDown {
		t.Fatalf("daemon-down code = %q; quería %q", body.Error.Code, codeDaemonDown)
	}
}

// TestDaemonStatusStopped: GET /v1/daemon/status sin núcleo corriendo devuelve 200 + state "stopped".
func TestDaemonStatusStopped(t *testing.T) {
	dir := t.TempDir()
	sup := supervisor.New(supervisor.Config{SocketPath: filepath.Join(dir, "edge.sock")}, nil)
	router := newRouter(sup, filepath.Join(dir, "edge.sock"), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/daemon/status", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("daemon/status = %d; quería 200", rec.Code)
	}
	var st daemonStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("daemon/status body inválido: %v", err)
	}
	if st.State != supervisor.StateStopped {
		t.Fatalf("daemon/status state = %q; quería stopped", st.State)
	}
}

// TestDaemonStartWrongMethod: GET a /v1/daemon/start (verbo equivocado) → 405 + envelope, NO se proxya.
func TestDaemonStartWrongMethod(t *testing.T) {
	dir := t.TempDir()
	sup := supervisor.New(supervisor.Config{SocketPath: filepath.Join(dir, "edge.sock")}, nil)
	router := newRouter(sup, filepath.Join(dir, "edge.sock"), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/daemon/start", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("daemon/start GET status = %d; quería 405", rec.Code)
	}
}

// TestWebUIServed: la web UI embebida responde 200 en "/".
func TestWebUIServed(t *testing.T) {
	dir := t.TempDir()
	sup := supervisor.New(supervisor.Config{SocketPath: filepath.Join(dir, "edge.sock")}, nil)
	router := newRouter(sup, filepath.Join(dir, "edge.sock"), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("webui / status = %d; quería 200", rec.Code)
	}
}
