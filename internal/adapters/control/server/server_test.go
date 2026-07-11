package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// fakeLister es un doble de SessionLister (el real lo provee *sessionmgr.Manager vía un adaptador en
// cmd/agent). Permite testear los handlers sin BD ni WhatsApp: Persisted devuelve el inventario fijo y
// Health la etiqueta de salud por session_id (vacío/no-vivo si no está en el mapa).
type fakeLister struct {
	sessions []domain.Session
	health   map[string]string
	err      error
}

func (f fakeLister) Persisted(context.Context) ([]domain.Session, error) {
	return f.sessions, f.err
}

func (f fakeLister) Health(id string) (string, bool) {
	h, ok := f.health[id]
	return h, ok
}

const testVersion = "0.0.0-test"

// startServer levanta el Server real sobre un Unix socket de prueba y devuelve un http.Client que
// marca por ese socket. Usa un directorio temporal CORTO bajo /tmp (no t.TempDir()) porque las rutas
// de t.TempDir() en macOS (/var/folders/...) suelen exceder el límite de sun_path (~104 bytes) de los
// Unix sockets.
func startServer(t *testing.T, lister SessionLister) *http.Client {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "wapp-ctl-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "edge.sock")

	srv := New(Config{SocketPath: socket, Version: testVersion}, nil, lister)
	ln, err := srv.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	// Verifica los permisos restrictivos del socket (0600).
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatalf("Stat socket: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("permisos del socket: got %o, want 600", perm)
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

// get hace un GET (o el método dado) por el socket. El host de la URL es irrelevante (lo ignora el
// DialContext unix); se usa "unix" por convención.
func do(t *testing.T, c *http.Client, method, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, "http://unix"+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("leyendo body: %v", err)
	}
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("unmarshal %q: %v", string(body), err)
	}
}

func TestHealth(t *testing.T) {
	c := startServer(t, fakeLister{})

	resp := do(t, c, http.MethodGet, "/v1/health")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type: got %q", ct)
	}

	var got healthResponse
	decode(t, resp, &got)
	if got.Status != "ok" {
		t.Errorf("status: got %q, want ok", got.Status)
	}
	if got.Version != testVersion {
		t.Errorf("version: got %q, want %q", got.Version, testVersion)
	}
}

// fakeHealth satisface HealthReporter para GET /v1/health enriquecido (Plan 031 T7).
type fakeHealth struct {
	uptime  int64
	reports map[string]health.Report
}

func (f fakeHealth) DaemonUptimeS() int64                             { return f.uptime }
func (f fakeHealth) Reports(context.Context) map[string]health.Report { return f.reports }

// startServerWithHealth arranca el servidor con un colector de salud cableado (SetHealthProvider).
func startServerWithHealth(t *testing.T, lister SessionLister, h HealthReporter) *http.Client {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "wapp-ctl-h-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "edge.sock")

	srv := New(Config{SocketPath: socket, Version: testVersion}, nil, lister)
	srv.SetHealthProvider(h)
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
	return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}}}
}

// TestHealth_Enriched: con colector cableado, GET /v1/health suma uptime del daemon + salud por sesión
// (Plan 031 T7), conservando status/version (retrocompatible con el supervisor).
func TestHealth_Enriched(t *testing.T) {
	h := fakeHealth{uptime: 77, reports: map[string]health.Report{
		"sess-1": {SocketState: "degraded", DegradedReason: "dek_load_timeout", OutboxDepth: 3, BinaryVersion: testVersion, DaemonUptimeS: 77},
	}}
	c := startServerWithHealth(t, fakeLister{}, h)

	resp := do(t, c, http.MethodGet, "/v1/health")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	var got healthResponse
	decode(t, resp, &got)
	if got.Status != "ok" || got.Version != testVersion {
		t.Errorf("base: status=%q version=%q", got.Status, got.Version)
	}
	if got.UptimeS != 77 {
		t.Errorf("uptime_s = %d, want 77", got.UptimeS)
	}
	sh, ok := got.Sessions["sess-1"]
	if !ok {
		t.Fatalf("falta la sesión sess-1 en /v1/health: %+v", got.Sessions)
	}
	if sh.SocketState != "degraded" || sh.DegradedReason != "dek_load_timeout" || sh.OutboxDepth != 3 {
		t.Errorf("salud sess-1 = %+v", sh)
	}
}

func TestSessions(t *testing.T) {
	now := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	const sid0 = "11111111-1111-4111-8111-111111111111"
	const sid1 = "22222222-2222-4222-8222-222222222222"
	lister := fakeLister{
		sessions: []domain.Session{
			{SessionID: sid0, JID: "111@s.whatsapp.net", State: domain.SessionStateActive, PairedAt: now, UpdatedAt: now},
			{SessionID: sid1, JID: "222@s.whatsapp.net", State: domain.SessionStateLoggedOut},
		},
		// sid0 está VIVA y escuchando; sid1 no está viva (sin entrada → health omitido).
		health: map[string]string{sid0: "listening"},
	}
	c := startServer(t, lister)

	resp := do(t, c, http.MethodGet, "/v1/sessions")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got sessionsResponse
	decode(t, resp, &got)
	if len(got.Sessions) != 2 {
		t.Fatalf("sessions: got %d, want 2", len(got.Sessions))
	}
	if got.Sessions[0].SessionID != sid0 || got.Sessions[0].JID != "111@s.whatsapp.net" || got.Sessions[0].State != "active" {
		t.Errorf("sesión[0]: %+v", got.Sessions[0])
	}
	if got.Sessions[0].Health != "listening" {
		t.Errorf("health[0]: got %q, want listening", got.Sessions[0].Health)
	}
	if got.Sessions[0].PairedAt != now.Format(time.RFC3339) {
		t.Errorf("paired_at: got %q, want %q", got.Sessions[0].PairedAt, now.Format(time.RFC3339))
	}
	// El segundo: session_id presente, timestamps cero omitidos (omitempty) y health omitido (no vivo).
	if got.Sessions[1].SessionID != sid1 {
		t.Errorf("sesión[1] session_id: %+v", got.Sessions[1])
	}
	if got.Sessions[1].PairedAt != "" || got.Sessions[1].UpdatedAt != "" || got.Sessions[1].Health != "" {
		t.Errorf("campos cero deberían omitirse: %+v", got.Sessions[1])
	}
}

func TestSessions_Empty(t *testing.T) {
	c := startServer(t, fakeLister{})

	resp := do(t, c, http.MethodGet, "/v1/sessions")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// El JSON debe traer "sessions": [] (no null), para que el cliente itere sin comprobar null.
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "{\"sessions\":[]}\n" {
		t.Errorf("body lista vacía: got %q", string(body))
	}
}

func TestSessions_ListerError(t *testing.T) {
	c := startServer(t, fakeLister{err: context.DeadlineExceeded})

	resp := do(t, c, http.MethodGet, "/v1/sessions")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500", resp.StatusCode)
	}
	assertErrorEnvelope(t, resp, codeInternal)
}

// TestErrorEnvelope cubre 404 (ruta desconocida) y 405 (método no permitido) con el envelope JSON.
func TestErrorEnvelope(t *testing.T) {
	c := startServer(t, fakeLister{})

	cases := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantCode   string
		wantAllow  string // Allow esperado (solo en 405)
	}{
		{"ruta desconocida", http.MethodGet, "/v1/desconocido", http.StatusNotFound, codeNotFound, ""},
		{"prefijo no v1", http.MethodGet, "/otra/cosa", http.StatusNotFound, codeNotFound, ""},
		{"metodo no permitido health", http.MethodPost, "/v1/health", http.StatusMethodNotAllowed, codeMethodNotAllowed, "GET"},
		{"metodo no permitido sessions", http.MethodDelete, "/v1/sessions", http.StatusMethodNotAllowed, codeMethodNotAllowed, "GET"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := do(t, c, tc.method, tc.path)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status: got %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if tc.wantAllow != "" {
				if allow := resp.Header.Get("Allow"); allow != tc.wantAllow {
					t.Errorf("Allow: got %q, want %q", allow, tc.wantAllow)
				}
			}
			assertErrorEnvelope(t, resp, tc.wantCode)
		})
	}
}

func assertErrorEnvelope(t *testing.T, resp *http.Response, wantCode string) {
	t.Helper()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type: got %q", ct)
	}
	var env errorBody
	decode(t, resp, &env)
	if env.Error.Code != wantCode {
		t.Errorf("error.code: got %q, want %q", env.Error.Code, wantCode)
	}
	if env.Error.Message == "" {
		t.Errorf("error.message vacío")
	}
}

// TestListen_StaleSocket verifica que un socket huérfano de un arranque previo se limpia y se puede
// volver a escuchar en la misma ruta.
func TestListen_StaleSocket(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "wapp-ctl-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "edge.sock")

	// Deja un socket presente en la ruta (Go hace unlink-on-close, así que NO lo cerramos: lo dejamos
	// vivo para que el archivo de socket exista cuando srv.Listen ejecute su limpieza).
	stale, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("socket previo: %v", err)
	}
	t.Cleanup(func() { _ = stale.Close() })
	if info, statErr := os.Stat(socket); statErr != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("precondición: el socket previo no está presente (err=%v)", statErr)
	}

	srv := New(Config{SocketPath: socket, Version: testVersion}, nil, fakeLister{})
	ln, err := srv.Listen()
	if err != nil {
		t.Fatalf("Listen sobre socket huérfano debería limpiar y reusar: %v", err)
	}
	_ = ln.Close()
}

// TestListen_RefusesRegularFile comprueba que Listen NO borra un archivo regular ajeno en la ruta.
func TestListen_RefusesRegularFile(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "wapp-ctl-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	regular := filepath.Join(dir, "edge.sock")
	if err := os.WriteFile(regular, []byte("no soy un socket"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	srv := New(Config{SocketPath: regular, Version: testVersion}, nil, fakeLister{})
	if _, err := srv.Listen(); err == nil {
		t.Fatal("Listen debería negarse a borrar un archivo regular en la ruta del socket")
	}
	if _, err := os.Stat(regular); err != nil {
		t.Errorf("el archivo regular no debería haberse borrado: %v", err)
	}
}
