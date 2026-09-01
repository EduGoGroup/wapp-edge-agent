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
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
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
	router := newRouter(sup, sock, "", nil)

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
	router := newRouter(sup, sock, "", nil)

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
	router := newRouter(sup, filepath.Join(dir, "edge.sock"), "", nil)

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
	router := newRouter(sup, filepath.Join(dir, "edge.sock"), "", nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/daemon/start", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("daemon/start GET status = %d; quería 405", rec.Code)
	}
}

// TestRootRedirectsToLoginWithoutSession: tras el Plan 033 · Ola 3, "/" está protegido: sin cookie de
// sesión válida el borde redirige a /login (303), no sirve el dashboard.
func TestRootRedirectsToLoginWithoutSession(t *testing.T) {
	dir := t.TempDir()
	sup := supervisor.New(supervisor.Config{SocketPath: filepath.Join(dir, "edge.sock")}, nil)
	router := newRouter(sup, filepath.Join(dir, "edge.sock"), "", nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("/ sin sesión status = %d; quería 303 (redirect a /login)", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/login" {
		t.Fatalf("/ sin sesión Location = %q; quería /login", loc)
	}
}

// TestRootGateBlocksSessionWithoutTenant: M-01 (code review 056) — una sesión válida pero SIN tenant
// asignado ("en revisión") no puede abrir la SPA aunque pida "/" directamente. La barrera es de SERVIDOR
// (rootGate), no del cliente: antes de esta corrección, escribir "/index.html" a mano bastaba para
// saltársela.
func TestRootGateBlocksSessionWithoutTenant(t *testing.T) {
	fc := newFakeCore(t)
	router := fc.routerFor(t)
	cookies := login(t, router, "pending@edge", "secret") // sin tenant_id

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	withCookies(req, cookies)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("/ con sesión sin tenant status = %d; quería 303 (redirect, NO la SPA)", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/login" {
		t.Fatalf("/ con sesión sin tenant Location = %q; quería /login", loc)
	}
}

// TestRootGateAllowsSessionWithTenantNoRoles: D-056.11 declara legítimo un usuario CON tenant pero SIN
// roles (membresía sin rol asignado todavía). Antes de M-01, la detección contaba roles.length===0 y
// confundía este caso con "en revisión"; ahora decide por el tenant y debe dejarlo entrar.
func TestRootGateAllowsSessionWithTenantNoRoles(t *testing.T) {
	fc := newFakeCore(t)
	router := fc.routerFor(t)
	cookies := login(t, router, "norole@edge", "secret") // tenant_id presente, roles vacíos

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	withCookies(req, cookies)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/ con tenant y sin roles status = %d; quería 200 (la SPA)", rec.Code)
	}
}

// TestRefreshPropagatesTenant: si el tenant llega vacío en el login pero el administrador aprueba al
// operador justo después, el siguiente refresh (proxy.go, vía sess.apply) debe propagar el tenant nuevo:
// la sesión no puede quedar CONGELADA en "en revisión" para siempre (M-01, code review 056).
func TestRefreshPropagatesTenant(t *testing.T) {
	fc := newFakeCore(t)
	router := fc.routerFor(t)
	cookies := login(t, router, "pending@edge", "secret") // sin tenant_id

	// Antes del refresh: "/" rebota a /login (sigue "en revisión").
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	withCookies(req, cookies)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("/ antes del refresh status = %d; quería 303 (aún en revisión)", rec.Code)
	}

	// Fuerza que el access de "pending@edge" ya no valga (simula expiración) y que el refresh
	// devuelva un access nuevo CON tenant (simula la aprobación del administrador mientras tanto).
	fc.mu.Lock()
	fc.validAccess = "access-approved"
	fc.refreshTo = "access-approved"
	fc.refreshTenant = "tenant-later"
	fc.mu.Unlock()

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
	withCookies(req2, cookies)
	router.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("GET /v1/sessions tras refresh status = %d; quería 200 (body=%s)", rec2.Code, rec2.Body.String())
	}

	// Tras el refresh: "/" ya deja pasar (el tenant recién asignado se propagó a la sesión).
	rec3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodGet, "/", nil)
	withCookies(req3, cookies)
	router.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Fatalf("/ tras el refresh status = %d; quería 200 (el tenant recién asignado debe abrir la SPA)", rec3.Code)
	}
}

// TestPlatformAPIBaseURLLeftAtDevDefault: el default de fábrica ("http://localhost:8103") debe marcarse
// para que main() avise en el log de arranque (Trabajo 1, code review 056 · T11); cualquier otro valor —
// típicamente una URL de producción fijada por el operador— no debe disparar el aviso.
func TestPlatformAPIBaseURLLeftAtDevDefault(t *testing.T) {
	if !platformAPIBaseURLLeftAtDevDefault(config.Config{PlatformAPIBaseURL: config.DefaultPlatformAPIBaseURL}) {
		t.Fatalf("el default de desarrollo (%q) no se detectó como tal", config.DefaultPlatformAPIBaseURL)
	}
	if platformAPIBaseURLLeftAtDevDefault(config.Config{PlatformAPIBaseURL: "https://cloud.wapp.example"}) {
		t.Fatalf("una URL de producción no debe marcarse como default de desarrollo")
	}
	if platformAPIBaseURLLeftAtDevDefault(config.Config{PlatformAPIBaseURL: ""}) {
		t.Fatalf("una URL vacía no debe marcarse como default de desarrollo (no es el valor de fábrica)")
	}
}

// TestStaticAssetsServedWithoutSession: los assets estáticos (styles.css) se sirven SIN sesión (los
// necesita también la pantalla de login).
func TestStaticAssetsServedWithoutSession(t *testing.T) {
	dir := t.TempDir()
	sup := supervisor.New(supervisor.Config{SocketPath: filepath.Join(dir, "edge.sock")}, nil)
	router := newRouter(sup, filepath.Join(dir, "edge.sock"), "", nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/styles.css", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/styles.css status = %d; quería 200", rec.Code)
	}
}

// TestSharedUICSSServedWithoutSession comprueba que las tres hojas de wapp-shared/ui que login.html
// enlaza (tokens → componentes → tema, mismo orden y mismo patrón que las otras tres consolas del
// ecosistema) se sirven en texto plano y sin exigir sesión.
func TestSharedUICSSServedWithoutSession(t *testing.T) {
	dir := t.TempDir()
	sup := supervisor.New(supervisor.Config{SocketPath: filepath.Join(dir, "edge.sock")}, nil)
	router := newRouter(sup, filepath.Join(dir, "edge.sock"), "", nil)

	for _, path := range []string{
		"/static/css/wapp-tokens.css",
		"/static/css/wapp-components.css",
		"/static/css/theme-edge.css",
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d; quería 200", path, rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "text/css; charset=utf-8" {
			t.Fatalf("%s Content-Type = %q; quería text/css; charset=utf-8", path, ct)
		}
	}
}
