package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
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

	mu            sync.Mutex
	validAccess   string // el único Bearer que la ruta protegida acepta
	refreshCount  int    // nº de veces que se llamó /v1/auth/refresh
	refreshFails  bool   // si true, /v1/auth/refresh responde 401 (refresh_invalid)
	refreshTo     string // access que devuelve el refresh
	refreshTenant string // tenant_id que devuelve el refresh (M-01, code review 056)
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
		if req.Password != "secret" {
			writeError(w, http.StatusUnauthorized, "invalid_credentials", "credenciales inválidas")
			return
		}
		switch req.Email {
		case "op@edge":
			writeJSON(w, http.StatusOK, map[string]any{
				"access_token": "access-1",
				"token_type":   "Bearer",
				"expires_at":   time.Now().Add(time.Hour),
				"roles":        []string{"edge.operator"},
				"tenant_id":    "tenant-e2e",
			})
		case "pending@edge":
			// Sin tenant_id: estado "en revisión" (M-01, code review 056).
			writeJSON(w, http.StatusOK, map[string]any{
				"access_token": "access-pending",
				"token_type":   "Bearer",
				"expires_at":   time.Now().Add(time.Hour),
				"roles":        []string{},
			})
		case "norole@edge":
			// Con tenant_id pero SIN roles: estado legítimo (D-056.11), NO es "en revisión".
			writeJSON(w, http.StatusOK, map[string]any{
				"access_token": "access-norole",
				"token_type":   "Bearer",
				"expires_at":   time.Now().Add(time.Hour),
				"roles":        []string{},
				"tenant_id":    "tenant-e2e",
			})
		default:
			writeError(w, http.StatusUnauthorized, "invalid_credentials", "credenciales inválidas")
		}
	})

	mux.HandleFunc("POST /v1/auth/refresh", func(w http.ResponseWriter, _ *http.Request) {
		fc.mu.Lock()
		fc.refreshCount++
		fails := fc.refreshFails
		to := fc.refreshTo
		tenant := fc.refreshTenant
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
			"tenant_id":    tenant,
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

// routerFor monta el router con una URL de plataforma DUMMY (no hay servidor detrás): válido para todos
// los tests que no llaman a /signup (login, proxy, csrf, logout, ...). Los tests de signup necesitan una
// plataforma FALSA de verdad — usan routerForPlatform.
func (fc *fakeCore) routerFor(t *testing.T) *http.ServeMux {
	t.Helper()
	return fc.routerForPlatform(t, "http://127.0.0.1:1")
}

// routerForPlatform monta el router apuntando el signup (C-03/T3.5) a platformBaseURL — normalmente un
// httptest.Server que representa POST /api/v1/signup de wapp-cloud-platform.
func (fc *fakeCore) routerForPlatform(t *testing.T, platformBaseURL string) *http.ServeMux {
	t.Helper()
	sup := supervisor.New(supervisor.Config{SocketPath: fc.socket}, nil)
	return newRouter(sup, fc.socket, platformBaseURL, nil)
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

// fakePlatformSignup es un servidor HTTP REAL (no Unix socket) que representa `POST /api/v1/signup` de
// wapp-cloud-platform (C-03/T3.5): el Edge lo llama por RED, no por el socket local del núcleo. Cuenta
// invocaciones, guarda el último body decodificado, y devuelve el status/cuerpo que el test configure.
type fakePlatformSignup struct {
	mu       sync.Mutex
	calls    int
	lastBody map[string]any
	status   int
	body     string // cuerpo crudo de la respuesta; vacío ⇒ sin cuerpo
}

// newFakePlatformSignup levanta el servidor y lo registra para cerrarse al final del test.
func newFakePlatformSignup(t *testing.T, status int, body string) (*httptest.Server, *fakePlatformSignup) {
	t.Helper()
	fp := &fakePlatformSignup{status: status, body: body}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/signup", func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		fp.mu.Lock()
		fp.calls++
		fp.lastBody = b
		fp.mu.Unlock()
		w.WriteHeader(fp.status)
		if fp.body != "" {
			_, _ = w.Write([]byte(fp.body))
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, fp
}

func (fp *fakePlatformSignup) invocations() int {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return fp.calls
}

func (fp *fakePlatformSignup) lastRequestBody() map[string]any {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return fp.lastBody
}

// TestSignup_Success: un signup válido hace EXACTAMENTE una llamada a POST /api/v1/signup de la
// plataforma, con origin:"edge" en el cuerpo, y el operador ve 202 con el mensaje amigable — solo
// DESPUÉS de que esa llamada haya tenido éxito (fija C-03: antes se fabricaba el mismo 202 sin llamar a
// nadie).
func TestSignup_Success(t *testing.T) {
	fc := newFakeCore(t)
	srv, fp := newFakePlatformSignup(t, http.StatusAccepted, `{"message":"Listo. Entra con tu correo y tu clave."}`)
	router := fc.routerForPlatform(t, srv.URL)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/signup", strings.NewReader(`{
		"first_name": "Carlos",
		"last_name": "Gomez",
		"email": "carlos@edge.local",
		"password": "Password123456!"
	}`))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /signup status = %d, want 202. Body: %s", rec.Code, rec.Body.String())
	}
	var res map[string]string
	_ = json.NewDecoder(rec.Body).Decode(&res)
	if !strings.Contains(res["message"], "Listo. Entra con tu correo y tu clave.") {
		t.Fatalf("mensaje inesperado: %v", res)
	}
	if got := fp.invocations(); got != 1 {
		t.Fatalf("la plataforma se llamó %d veces; quería exactamente 1", got)
	}
	body := fp.lastRequestBody()
	if body["origin"] != "edge" {
		t.Fatalf("origin enviado = %v; quería \"edge\"", body["origin"])
	}
	if body["email"] != "carlos@edge.local" {
		t.Fatalf("email enviado = %v; quería carlos@edge.local", body["email"])
	}
}

// TestSignup_Validations valida campos requeridos y longitud mínima de clave, SIN llamar a la plataforma
// (falla antes, en el borde local).
func TestSignup_Validations(t *testing.T) {
	fc := newFakeCore(t)
	srv, fp := newFakePlatformSignup(t, http.StatusAccepted, `{"message":"ok"}`)
	router := fc.routerForPlatform(t, srv.URL)

	// Clave muy corta
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/signup", strings.NewReader(`{
		"first_name": "Carlos",
		"last_name": "Gomez",
		"email": "carlos@edge.local",
		"password": "corta"
	}`))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST /signup status = %d, want 400", rec.Code)
	}
	if got := fp.invocations(); got != 0 {
		t.Fatalf("la validación local no debe llamar a la plataforma; se llamó %d veces", got)
	}
}

// TestSignup_CoreNoDisponible: si la plataforma no responde (destino caído / conexión rechazada), el
// operador recibe 503 con un envelope de error honesto — NUNCA el 202 verde que fabricaba el fallback
// eliminado por C-03.
func TestSignup_CoreNoDisponible(t *testing.T) {
	fc := newFakeCore(t)
	// Servidor que se cierra ANTES de la llamada: la conexión se rechaza, como un destino no disponible.
	down := httptest.NewServer(http.NewServeMux())
	downURL := down.URL
	down.Close()

	router := fc.routerForPlatform(t, downURL)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/signup", strings.NewReader(`{
		"first_name": "Carlos",
		"last_name": "Gomez",
		"email": "carlos@edge.local",
		"password": "Password123456!"
	}`))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("POST /signup con plataforma caída status = %d; quería 503. Body: %s", rec.Code, rec.Body.String())
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body no es el envelope de error: %v (%s)", err, rec.Body.String())
	}
	if body.Error.Code != codePlatformDown {
		t.Fatalf("code = %q; quería %q", body.Error.Code, codePlatformDown)
	}
}

// TestSignup_InsecurePlatformURL_Rejected: si PlatformAPIBaseURL es "http://" contra un host que NO es
// loopback, el signup se corta ANTES de llamar a la plataforma — mandar la contraseña en claro por la red
// no es un fallo transitorio, así que lleva su propio código (Trabajo 1, code review 056 · T11).
func TestSignup_InsecurePlatformURL_Rejected(t *testing.T) {
	fc := newFakeCore(t)
	srv, fp := newFakePlatformSignup(t, http.StatusAccepted, `{"message":"ok"}`)
	// srv.URL es http://127.0.0.1:PUERTO (loopback). Se reescribe SOLO el host a uno NO loopback,
	// manteniendo el mismo puerto, para simular una plataforma real detrás de un config inseguro sin
	// depender de DNS real (el fakePlatform sigue escuchando en 127.0.0.1; si la llamada de verdad
	// saliera, fallaría por conexión rechazada — la aserción clave es que NO se llega a intentar).
	insecureURL := strings.Replace(srv.URL, "127.0.0.1", "cloud.wapp.example", 1)
	router := fc.routerForPlatform(t, insecureURL)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/signup", strings.NewReader(`{
		"first_name": "Carlos",
		"last_name": "Gomez",
		"email": "carlos@edge.local",
		"password": "Password123456!"
	}`))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("POST /signup con plataforma insegura status = %d; quería 500. Body: %s", rec.Code, rec.Body.String())
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body no es el envelope de error: %v (%s)", err, rec.Body.String())
	}
	if body.Error.Code != codeInsecurePlatformConfig {
		t.Fatalf("code = %q; quería %q", body.Error.Code, codeInsecurePlatformConfig)
	}
	if got := fp.invocations(); got != 0 {
		t.Fatalf("no debe llamar a la plataforma cuando la URL configurada es insegura; se llamó %d veces", got)
	}
}

// TestSignup_PlataformaResponde503_SePropaga: si la plataforma (no la red) responde 503 —p.ej. su
// cliente M2M sin configurar—, el Edge propaga 503 tal cual, no lo convierte en 202.
func TestSignup_PlataformaResponde503_SePropaga(t *testing.T) {
	fc := newFakeCore(t)
	srv, fp := newFakePlatformSignup(t, http.StatusServiceUnavailable, "registro no disponible")
	router := fc.routerForPlatform(t, srv.URL)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/signup", strings.NewReader(`{
		"first_name": "Carlos",
		"last_name": "Gomez",
		"email": "carlos@edge.local",
		"password": "Password123456!"
	}`))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; quería 503 (propagado de la plataforma)", rec.Code)
	}
	if got := fp.invocations(); got != 1 {
		t.Fatalf("invocaciones = %d; quería 1", got)
	}
}

// TestSignup_PlataformaResponde502_SePropaga: un 502 (p.ej. identity caído en registerIdentityUser, o
// ReplaceUserSystems fallando) es, hoy, el caso COMÚN mientras T0.2 (credencial M2M de wApp hacia
// identity) siga sin hacerse — no un caso raro. El Edge debe propagar 502 con el MISMO mensaje "no es
// culpa tuya" que el 503, y NUNCA el texto crudo del upstream (que nombraría la pieza interna que
// falló).
func TestSignup_PlataformaResponde502_SePropaga(t *testing.T) {
	fc := newFakeCore(t)
	srv, fp := newFakePlatformSignup(t, http.StatusBadGateway, "servicio de identidad no disponible")
	router := fc.routerForPlatform(t, srv.URL)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/signup", strings.NewReader(`{
		"first_name": "Carlos",
		"last_name": "Gomez",
		"email": "carlos@edge.local",
		"password": "Password123456!"
	}`))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d; quería 502 (propagado de la plataforma)", rec.Code)
	}
	if got := fp.invocations(); got != 1 {
		t.Fatalf("invocaciones = %d; quería 1", got)
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body no es el envelope de error: %v (%s)", err, rec.Body.String())
	}
	if body.Error.Code != codePlatformDown {
		t.Fatalf("code = %q; quería %q (mismo código que el 503)", body.Error.Code, codePlatformDown)
	}
	if !strings.Contains(body.Error.Message, "El alta no está disponible en este momento") {
		t.Fatalf("mensaje = %q; quería el mismo tono que el 503", body.Error.Message)
	}
	if strings.Contains(body.Error.Message, "servicio de identidad no disponible") {
		t.Fatalf("el texto crudo del upstream se filtró al operador: %q", body.Error.Message)
	}
}

// TestSignup_PlataformaResponde409_CorreoExistente: si la plataforma responde 409 (correo ya registrado
// — contrato en evolución en paralelo a C-03), el Edge lo propaga con un mensaje honesto, no un 202.
func TestSignup_PlataformaResponde409_CorreoExistente(t *testing.T) {
	fc := newFakeCore(t)
	srv, _ := newFakePlatformSignup(t, http.StatusConflict, `{"error":{"code":"email_taken","message":"ya existe una cuenta con ese correo"}}`)
	router := fc.routerForPlatform(t, srv.URL)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/signup", strings.NewReader(`{
		"first_name": "Carlos",
		"last_name": "Gomez",
		"email": "carlos@edge.local",
		"password": "Password123456!"
	}`))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d; quería 409 (propagado de la plataforma)", rec.Code)
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body no es el envelope de error: %v (%s)", err, rec.Body.String())
	}
	if body.Error.Code != "email_taken" {
		t.Fatalf("code = %q; quería email_taken", body.Error.Code)
	}
}

// TestLoginGet_AlphaPasswordFromEnv verifica M-13 (code review 056): el data-password del selector
// Alpha en login.html sale de WAPP_ALPHA_TEST_PASSWORD, NUNCA de un literal fijo en el fichero
// versionado. Sin la env var, el campo sale vacío (el operador la teclea); con ella, sale su valor.
func TestLoginGet_AlphaPasswordFromEnv(t *testing.T) {
	auth := newAuthBorder(newSessionStore(), nil, nil, "", nil)

	t.Run("SinVariable_CampoVacio", func(t *testing.T) {
		t.Setenv("WAPP_ALPHA_TEST_ACCOUNTS", "true")
		t.Setenv("WAPP_ALPHA_TEST_PASSWORD", "")

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/login", nil)
		auth.handleLoginGet(rec, req)

		body := rec.Body.String()
		if strings.Contains(body, "1234567890AB") {
			t.Errorf("credencial fija encontrada en el HTML: no debe salir del fichero versionado")
		}
		if !strings.Contains(body, `data-password=""`) {
			t.Errorf("sin WAPP_ALPHA_TEST_PASSWORD, data-password debía quedar vacío. Body: %s", body)
		}
	})

	t.Run("ConVariable_CampoRelleno", func(t *testing.T) {
		t.Setenv("WAPP_ALPHA_TEST_ACCOUNTS", "true")
		t.Setenv("WAPP_ALPHA_TEST_PASSWORD", "clave-de-prueba-del-operador")

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/login", nil)
		auth.handleLoginGet(rec, req)

		body := rec.Body.String()
		if !strings.Contains(body, `data-password="clave-de-prueba-del-operador"`) {
			t.Errorf("con WAPP_ALPHA_TEST_PASSWORD puesta, data-password debía reflejarla. Body: %s", body)
		}
	})
}

// TestLoginGet_CSPConNonce cierra U-3 (documentations/deuda.md): login.html era la única superficie
// del ecosistema servida sin ninguna cabecera de seguridad. Verifica que la CSP viaje con un nonce
// no vacío y que ESE MISMO nonce sea el que lleva el <script> inline del documento — un nonce en la
// cabecera que no coincide con el del HTML no protege nada.
func TestLoginGet_CSPConNonce(t *testing.T) {
	auth := newAuthBorder(newSessionStore(), nil, nil, "", nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/login", nil)
	auth.handleLoginGet(rec, req)

	csp := rec.Header().Get("Content-Security-Policy")
	if csp == "" {
		t.Fatal("falta la cabecera Content-Security-Policy en /login")
	}

	re := regexp.MustCompile(`'nonce-([^']+)'`)
	m := re.FindStringSubmatch(csp)
	if len(m) != 2 || m[1] == "" {
		t.Fatalf("la CSP no lleva un nonce no vacío: %q", csp)
	}
	nonce := m[1]

	body := rec.Body.String()
	if !strings.Contains(body, `<script nonce="`+nonce+`">`) {
		t.Errorf("el <script> inline no lleva el MISMO nonce que la cabecera CSP (%q). Body: %s", nonce, body)
	}
}
