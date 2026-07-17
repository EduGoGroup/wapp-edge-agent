package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/supervisor"
)

// shortSocketPath crea un socket en un dir temporal de nombre CORTO (os.MkdirTemp con prefijo mínimo): el
// t.TempDir() basado en el nombre del test excede el límite de 104 bytes de sun_path en macOS.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "wc")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

// fakeCore es un núcleo falso sobre Unix socket que implementa el CONTRATO que consume el Paso B:
// /v1/auth/login, /v1/auth/refresh, /v1/auth/logout, /v1/enroll/status y una ruta protegida (/v1/sessions,
// /v1/sessions/pair) que exige "Authorization: Bearer <validAccess>". Permite forzar la expiración del
// access (cambiando validAccess) y contar los refresh (para verificar el single-flight).
type fakeCore struct {
	socket string

	mu           sync.Mutex
	validAccess  string // el único Bearer que la ruta protegida acepta
	refreshCount int    // nº de veces que se llamó /v1/auth/refresh
	refreshFails bool   // si true, /v1/auth/refresh responde 401 (refresh_invalid)
	refreshTo    string // access que devuelve el refresh
}

func newFakeCore(t *testing.T) *fakeCore {
	t.Helper()
	fc := &fakeCore{
		socket:      shortSocketPath(t),
		validAccess: "access-1",
		refreshTo:   "access-2",
	}
	ln, err := net.Listen("unix", fc.socket)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Email, Password string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Email == "op@edge" && req.Password == "secret" {
			writeJSON(w, http.StatusOK, map[string]any{
				"access_token": "access-1",
				"token_type":   "Bearer",
				"expires_at":   time.Now().Add(time.Hour),
				"roles":        []string{"edge.operator"},
			})
			return
		}
		writeError(w, http.StatusUnauthorized, "invalid_credentials", "credenciales inválidas")
	})

	mux.HandleFunc("POST /v1/auth/refresh", func(w http.ResponseWriter, _ *http.Request) {
		fc.mu.Lock()
		fc.refreshCount++
		fails := fc.refreshFails
		to := fc.refreshTo
		if !fails {
			fc.validAccess = to // el refresh rota el access válido
		}
		fc.mu.Unlock()
		time.Sleep(15 * time.Millisecond) // ventana para que se apilen los concurrentes (prueba single-flight)
		if fails {
			writeError(w, http.StatusUnauthorized, "refresh_invalid", "refresh inválido")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token": to,
			"token_type":   "Bearer",
			"expires_at":   time.Now().Add(time.Hour),
			"roles":        []string{"edge.operator"},
		})
	})

	mux.HandleFunc("POST /v1/auth/logout", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})

	mux.HandleFunc("GET /v1/enroll/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"enrolled": true})
	})

	// Rutas protegidas: exigen el Bearer válido vigente.
	protected := func(w http.ResponseWriter, r *http.Request) {
		fc.mu.Lock()
		valid := fc.validAccess
		fc.mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer "+valid {
			writeError(w, http.StatusUnauthorized, "invalid_credentials", "token inválido")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "path": r.URL.Path})
	}
	mux.HandleFunc("GET /v1/sessions", protected)
	mux.HandleFunc("POST /v1/sessions/pair", protected)

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return fc
}

func (fc *fakeCore) refreshes() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.refreshCount
}

func (fc *fakeCore) setValidAccess(v string) {
	fc.mu.Lock()
	fc.validAccess = v
	fc.mu.Unlock()
}

func (fc *fakeCore) routerFor(t *testing.T) *http.ServeMux {
	t.Helper()
	sup := supervisor.New(supervisor.Config{SocketPath: fc.socket}, nil)
	return newRouter(sup, fc.socket, nil)
}

// login realiza POST /login y devuelve las cookies emitidas (sesión + csrf).
func login(t *testing.T, router http.Handler, email, pass string) []*http.Cookie {
	t.Helper()
	rec := httptest.NewRecorder()
	body := `{"email":"` + email + `","password":"` + pass + `"}`
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(body))
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /login status = %d; quería 200 (body=%s)", rec.Code, rec.Body.String())
	}
	return rec.Result().Cookies()
}

func csrfFrom(cookies []*http.Cookie) string {
	for _, c := range cookies {
		if c.Name == cookieCSRF {
			return c.Value
		}
	}
	return ""
}

func withCookies(req *http.Request, cookies []*http.Cookie) {
	for _, c := range cookies {
		req.AddCookie(c)
	}
}

// TestLoginSetsCookie: un login correcto emite la cookie HttpOnly de sesión + la cookie legible de CSRF,
// y /session queda autenticado con el rol.
func TestLoginSetsCookie(t *testing.T) {
	fc := newFakeCore(t)
	router := fc.routerFor(t)

	cookies := login(t, router, "op@edge", "secret")

	var sess, csrf *http.Cookie
	for _, c := range cookies {
		switch c.Name {
		case cookieSession:
			sess = c
		case cookieCSRF:
			csrf = c
		}
	}
	if sess == nil || sess.Value == "" {
		t.Fatal("no se emitió la cookie de sesión")
	}
	if !sess.HttpOnly {
		t.Fatal("la cookie de sesión debe ser HttpOnly")
	}
	if sess.SameSite != http.SameSiteStrictMode {
		t.Fatal("la cookie de sesión debe ser SameSite=Strict")
	}
	if csrf == nil || csrf.Value == "" {
		t.Fatal("no se emitió la cookie CSRF")
	}
	if csrf.HttpOnly {
		t.Fatal("la cookie CSRF debe ser legible por el JS (no HttpOnly)")
	}

	// /session refleja la sesión.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/session", nil)
	withCookies(req, cookies)
	router.ServeHTTP(rec, req)
	var s struct {
		Authenticated bool     `json:"authenticated"`
		Roles         []string `json:"roles"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &s)
	if !s.Authenticated || len(s.Roles) != 1 || s.Roles[0] != "edge.operator" {
		t.Fatalf("/session = %+v; quería autenticado con rol edge.operator", s)
	}
}

// TestLoginBadCredsMapsError: credenciales incorrectas → 401 + envelope invalid_credentials + mensaje
// amigable; NO se emite cookie de sesión.
func TestLoginBadCredsMapsError(t *testing.T) {
	fc := newFakeCore(t)
	router := fc.routerFor(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"email":"op@edge","password":"wrong"}`))
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("login malo status = %d; quería 401", rec.Code)
	}
	var body errorBody
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Error.Code != "invalid_credentials" {
		t.Fatalf("code = %q; quería invalid_credentials", body.Error.Code)
	}
	if !strings.Contains(body.Error.Message, "incorrect") && !strings.Contains(strings.ToLower(body.Error.Message), "incorrect") {
		// mensaje amigable en español ("Email o contraseña incorrectos.")
		if !strings.Contains(strings.ToLower(body.Error.Message), "correo") && !strings.Contains(strings.ToLower(body.Error.Message), "contraseña") {
			t.Fatalf("mensaje no amigable: %q", body.Error.Message)
		}
	}
	for _, c := range rec.Result().Cookies() {
		if c.Name == cookieSession && c.Value != "" {
			t.Fatal("no debe emitirse cookie de sesión en login fallido")
		}
	}
}

// TestProxyInjectsBearer: con sesión válida, el proxy inyecta Authorization: Bearer <access> hacia el
// núcleo (la ruta protegida acepta el token y responde 200).
func TestProxyInjectsBearer(t *testing.T) {
	fc := newFakeCore(t)
	router := fc.routerFor(t)
	cookies := login(t, router, "op@edge", "secret")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
	withCookies(req, cookies)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/sessions status = %d; quería 200 (Bearer inyectado) — body=%s", rec.Code, rec.Body.String())
	}
}

// TestProxyRefreshRetrySingleFlight: si el núcleo responde 401 (access expirado), el proxy refresca y
// reintenta UNA vez con el nuevo access → 200. Bajo concurrencia, el refresh se llama EXACTAMENTE una vez.
func TestProxyRefreshRetrySingleFlight(t *testing.T) {
	fc := newFakeCore(t)
	router := fc.routerFor(t)
	cookies := login(t, router, "op@edge", "secret") // sesión con access-1

	// Simula expiración: el núcleo ya no acepta access-1 (solo aceptará el access-2 que devuelve el refresh).
	fc.setValidAccess("access-2")

	const n = 8
	var wg sync.WaitGroup
	codes := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
			withCookies(req, cookies)
			router.ServeHTTP(rec, req)
			codes[idx] = rec.Code
		}(i)
	}
	wg.Wait()

	for i, c := range codes {
		if c != http.StatusOK {
			t.Fatalf("req %d status = %d; quería 200 tras refresh+retry", i, c)
		}
	}
	if got := fc.refreshes(); got != 1 {
		t.Fatalf("refresh llamado %d veces; el single-flight debe llamarlo exactamente 1", got)
	}
}

// TestProxyRefreshFailClearsSession: si el refresh también da 401, el proxy limpia las cookies y responde
// 401 (la SPA redirige a /login).
func TestProxyRefreshFailClearsSession(t *testing.T) {
	fc := newFakeCore(t)
	router := fc.routerFor(t)
	cookies := login(t, router, "op@edge", "secret")

	fc.setValidAccess("nunca-coincide") // access-1 ya no vale
	fc.mu.Lock()
	fc.refreshFails = true
	fc.mu.Unlock()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
	withCookies(req, cookies)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("refresh fallido status = %d; quería 401", rec.Code)
	}
	cleared := false
	for _, c := range rec.Result().Cookies() {
		if c.Name == cookieSession && c.MaxAge < 0 {
			cleared = true
		}
	}
	if !cleared {
		t.Fatal("el proxy debe caducar la cookie de sesión cuando el refresh falla")
	}
}

// TestCSRFRejectsMutatingWithoutToken: una mutadora con sesión pero SIN X-CSRF-Token se rechaza (403); con
// el token válido pasa al núcleo (200).
func TestCSRFRejectsMutatingWithoutToken(t *testing.T) {
	fc := newFakeCore(t)
	router := fc.routerFor(t)
	cookies := login(t, router, "op@edge", "secret")

	// Sin header CSRF → 403.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/sessions/pair", nil)
	withCookies(req, cookies)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("pair sin CSRF status = %d; quería 403", rec.Code)
	}
	var body errorBody
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Error.Code != "csrf_invalid" {
		t.Fatalf("code = %q; quería csrf_invalid", body.Error.Code)
	}

	// Con header CSRF válido → 200.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/v1/sessions/pair", nil)
	withCookies(req2, cookies)
	req2.Header.Set(headerCSRF, csrfFrom(cookies))
	router.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("pair con CSRF status = %d; quería 200 (body=%s)", rec2.Code, rec2.Body.String())
	}
}

// TestProxyBlocksAuthRoutesFromBrowser: /v1/auth/* no se sirve por el proxy del navegador (404).
func TestProxyBlocksAuthRoutesFromBrowser(t *testing.T) {
	fc := newFakeCore(t)
	router := fc.routerFor(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/login", strings.NewReader(`{}`))
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("/v1/auth/login vía navegador status = %d; quería 404", rec.Code)
	}
}

// TestEnrollBootstrapNoSessionNoCSRF: en primera ejecución (sin sesión) el proxy deja pasar POST /v1/enroll
// sin exigir sesión ni CSRF (bootstrap). Aquí el núcleo falso no implementa /v1/enroll, pero basta con
// verificar que NO se corta con 403 CSRF ni 401 (llega al núcleo, que responde 404/lo que sea, no 403).
func TestEnrollBootstrapNoSessionNoCSRF(t *testing.T) {
	fc := newFakeCore(t)
	router := fc.routerFor(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/enroll", strings.NewReader(`{"activation_code":"x"}`))
	router.ServeHTTP(rec, req)
	if rec.Code == http.StatusForbidden {
		t.Fatalf("POST /v1/enroll sin sesión no debe cortarse por CSRF (status=%d)", rec.Code)
	}
}

// TestLogoutClearsCookies: logout con CSRF válido limpia la cookie de sesión.
func TestLogoutClearsCookies(t *testing.T) {
	fc := newFakeCore(t)
	router := fc.routerFor(t)
	cookies := login(t, router, "op@edge", "secret")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/logout", strings.NewReader(`{}`))
	withCookies(req, cookies)
	req.Header.Set(headerCSRF, csrfFrom(cookies))
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("logout status = %d; quería 200", rec.Code)
	}
	cleared := false
	for _, c := range rec.Result().Cookies() {
		if c.Name == cookieSession && c.MaxAge < 0 {
			cleared = true
		}
	}
	if !cleared {
		t.Fatal("logout debe caducar la cookie de sesión")
	}
}
