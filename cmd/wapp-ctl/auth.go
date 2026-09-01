package main

// auth.go implementa el borde de AUTENTICACIÓN de wapp-ctl (Plan 033 · Ola 3 · Paso B): las rutas propias
// del supervisor que la webui usa para iniciar/cerrar sesión, y el cliente hacia el socket /v1/auth/* del
// núcleo. El navegador NUNCA habla con /v1/auth/* directamente (se bloquea en el proxy): pasa por estos
// endpoints, que traducen cookie de sesión ⇄ access token del núcleo.
//
//	GET  /login   → sirve la pantalla de login (si ya hay sesión válida, redirige a /)
//	POST /login   → valida input, llama al socket /v1/auth/login, crea sesión + cookies
//	POST /logout  → llama al socket /v1/auth/logout, limpia cookies, responde ok (la SPA navega a /login)
//	GET  /session → datos NO sensibles de la sesión para pintar la UI (roles, expiración, authenticated)

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"html/template"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/webui"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// loginResult refleja el LoginResult del núcleo (internal/auth): SOLO el access token + metadatos. El
// refresh token NUNCA viaja hasta aquí.
type loginResult struct {
	AccessToken string    `json:"access_token"`
	TokenType   string    `json:"token_type"`
	ExpiresAt   time.Time `json:"expires_at"`
	Roles       []string  `json:"roles"`
	// TenantID decide si la sesión está "en revisión" (vacío) o puede abrir la SPA (M-01, code review
	// 056): lo consumen rootGate (main.go) y handleSession, nunca el navegador contando roles.
	TenantID string `json:"tenant_id"`
}

// newSocketClient construye un http.Client que marca SIEMPRE el Unix socket del núcleo (para las llamadas
// server-side a /v1/auth/{login,refresh,logout}). El host de la URL es un placeholder ("unix").
func newSocketClient(socketPath string) *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socketPath)
			},
		},
	}
}

// newPlatformClient construye el http.Client que habla por RED (no Unix socket) con la API PÚBLICA de la
// plataforma cloud (wapp-cloud-platform, puerto 8103) — lo usa el signup (C-03/T3.5), la única llamada
// del borde de auth que sale de la máquina hacia la nube en vez de quedarse en el socket local del
// núcleo. 15s: mismo timeout que usan los clientes salientes equivalentes del BFF y la consola
// (guardian/wapp-guardian-bff/internal/apiclient/transport.go, guardian/wapp-platform-console/...).
func newPlatformClient() *http.Client {
	return &http.Client{Timeout: 15 * time.Second}
}

// authBorder agrupa las dependencias de los handlers de auth del borde.
type authBorder struct {
	store  *sessionStore
	client *http.Client
	log    sharedlogger.Logger

	// platformClient/platformBaseURL sirven el signup (C-03/T3.5): llamada DIRECTA a
	// POST {platformBaseURL}/api/v1/signup, distinta del socket local del núcleo (que no la relaya).
	platformClient  *http.Client
	platformBaseURL string
}

func newAuthBorder(store *sessionStore, client *http.Client, platformClient *http.Client, platformBaseURL string, log sharedlogger.Logger) *authBorder {
	return &authBorder{
		store:           store,
		client:          client,
		log:             log,
		platformClient:  platformClient,
		platformBaseURL: platformBaseURL,
	}
}

// callAuth hace una llamada POST server-side a un endpoint /v1/auth/* del socket. Devuelve el status HTTP
// y el cuerpo crudo (para que el caller decodifique LoginResult o el envelope de error).
func (a *authBorder) callAuth(ctx context.Context, path string, body any, bearer string) (int, []byte, error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return 0, nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix"+path, &buf)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw := new(bytes.Buffer)
	_, _ = raw.ReadFrom(resp.Body)
	return resp.StatusCode, raw.Bytes(), nil
}

// callPlatformSignup hace la llamada POST REAL hacia el alta pública de la plataforma cloud
// (`POST {platformBaseURL}/api/v1/signup`, C-03/T3.5). A diferencia de callAuth (que SIEMPRE marca el
// Unix socket local del núcleo), esta sale por RED hacia wapp-cloud-platform con su propio http.Client y
// timeout — el mismo camino que ya usa el BFF (guardian/wapp-guardian-bff/internal/apiclient/auth.go), no
// uno inventado. Devuelve el status y el cuerpo crudo tal cual: /api/v1/signup hoy responde texto plano
// en sus errores (no el envelope {"error":{code,message}} del resto de la plataforma), así que el caller
// decodifica con decodePlatformError en vez de decodeAuthError.
func (a *authBorder) callPlatformSignup(ctx context.Context, payload any) (int, []byte, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(payload); err != nil {
		return 0, nil, err
	}
	url := strings.TrimRight(a.platformBaseURL, "/") + "/api/v1/signup"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &buf)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := a.platformClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw := new(bytes.Buffer)
	_, _ = raw.ReadFrom(resp.Body)
	return resp.StatusCode, raw.Bytes(), nil
}

// friendlyAuthMessage mapea el `code` del envelope de error del núcleo a un mensaje amigable para la UI.
func friendlyAuthMessage(code, fallback string) string {
	switch code {
	case "invalid_credentials":
		return "Email o contraseña incorrectos."
	case "user_inactive":
		return "Tu usuario está inactivo. Contacta al administrador."
	case "tenant_mismatch":
		return "Tu cuenta no corresponde a este Edge."
	case "refresh_invalid":
		return "La sesión caducó. Vuelve a iniciar sesión."
	case "invalid_input":
		return "Revisa el email y la contraseña."
	case "relay_offline":
		return "Sin conexión con la nube: no se puede iniciar sesión ahora."
	case "internal":
		return "Error del servidor de autenticación. Inténtalo de nuevo."
	default:
		if fallback != "" {
			return fallback
		}
		return "No se pudo iniciar sesión."
	}
}

// handleLoginPost valida el input, llama al socket /v1/auth/login, y en caso de éxito crea la sesión de
// operador (cookies HttpOnly + CSRF). No requiere CSRF (aún no hay sesión) — SameSite=Strict + loopback.
func (a *authBorder) handleLoginPost(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_input", "Cuerpo de la petición inválido.")
		return
	}
	req.Email = strings.TrimSpace(req.Email)
	if req.Email == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "invalid_input", "Email y contraseña son obligatorios.")
		return
	}

	status, raw, err := a.callAuth(r.Context(), "/v1/auth/login", req, "")
	if err != nil {
		// El socket no respondió (núcleo caído): equivalente a daemon down.
		writeError(w, http.StatusServiceUnavailable, codeDaemonDown,
			"El núcleo no responde: arranca el daemon e inténtalo de nuevo.")
		return
	}
	if status != http.StatusOK {
		code, msg := decodeAuthError(raw)
		writeError(w, status, code, friendlyAuthMessage(code, msg))
		return
	}

	var res loginResult
	if err := json.Unmarshal(raw, &res); err != nil || res.AccessToken == "" {
		writeError(w, http.StatusBadGateway, "internal", "Respuesta de login inválida del núcleo.")
		return
	}
	sess := a.store.create(res.AccessToken, res.Roles, res.TenantID, res.ExpiresAt)
	setSessionCookies(w, sess)
	writeJSON(w, http.StatusOK, map[string]any{
		"authenticated": true,
		"roles":         res.Roles,
		// pending (M-01, code review 056): decidido por el TENANT, no por contar roles — un usuario con
		// membresía y sin rol (D-056.11) tiene tenant y NO está pendiente.
		"pending":    res.TenantID == "",
		"expires_at": res.ExpiresAt,
	})
}

// handleLoginGet sirve la pantalla de login. Si ya hay sesión válida CON tenant, redirige a la webui
// principal. Una sesión válida SIN tenant ("en revisión", M-01 code review 056) NO se redirige a "/"
// —rootGate (main.go) la rebotaría de vuelta aquí igualmente— sino que sirve login.html tal cual: su JS
// consulta GET /session y pinta la sección "pending" en vez del formulario.
func (a *authBorder) handleLoginGet(w http.ResponseWriter, r *http.Request) {
	if sess := a.store.fromRequest(r); sess != nil {
		if _, tenant, _ := sess.meta(); tenant != "" {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
	}
	tmpl, err := template.ParseFS(webui.FS(), "login.html")
	if err != nil {
		http.ServeFileFS(w, r, webui.FS(), "login.html")
		return
	}
	enableAlpha := os.Getenv("WAPP_ALPHA_TEST_ACCOUNTS") == "true" || os.Getenv("WAPP_ENABLE_ALPHA_LOGIN") == "true"
	nonce, err := newNonce()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "No se pudo preparar la pantalla de login.")
		return
	}
	// CSP (deuda U-3, documentations/deuda.md): esta era la única superficie del ecosistema que
	// servía HTML sin ninguna cabecera de seguridad. login.html es un documento autocontenido con UN
	// <script> inline, así que necesita 'nonce-…' en vez de 'unsafe-inline'. Sin librería compartida:
	// wapp-shared/web/gin da nonce+CSP a las otras tres consolas, pero está pensado para gin y
	// wapp-ctl es net/http puro.
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'nonce-"+nonce+"'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = tmpl.Execute(w, map[string]any{
		"EnableAlphaTestAccounts": enableAlpha,
		// AlphaTestPassword (M-13, code review 056): la clave que el selector de "Usuario de prueba
		// (Alpha)" autocompleta sale de WAPP_ALPHA_TEST_PASSWORD, NUNCA del fichero versionado. Vacía
		// por default: el selector sigue autocompletando el correo, y el campo de contraseña queda
		// vacío para que el operador la teclee. Mismo patrón y misma env var que el BFF
		// (guardian/wapp-guardian-bff/internal/config/config.go), para que las dos superficies se
		// configuren igual.
		"AlphaTestPassword": os.Getenv("WAPP_ALPHA_TEST_PASSWORD"),
		"Nonce":             nonce,
	})
}

// newNonce genera el nonce criptográfico de la CSP de login.html (U-3): 16 bytes de crypto/rand,
// uno distinto por petición. Base64 SIN relleno y con el alfabeto de URL (RawURLEncoding), no el
// estándar: el estándar mete `+` y `=`, y html/template los escapa como entidad (`&#43;`, `&#61;`)
// al interpolarlos en el atributo `nonce="…"` — el navegador decodifica la entidad al parsear el
// HTML y el nonce efectivo coincide igual, pero evitarlo de raíz es más simple que fiarse de eso.
func newNonce() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// handleSignupPost procesa la solicitud de registro público para el Edge (C-03/T3.5): tras validar el
// input, llama DIRECTO a POST /api/v1/signup de la plataforma cloud — igual que hace el BFF — con
// origin:"edge". NUNCA responde éxito sin que esa llamada haya tenido éxito: el fallback que fabricaba un
// 202 sin llamar a nadie (el bug de C-03) queda eliminado.
func (a *authBorder) handleSignupPost(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email     string `json:"email"`
		Password  string `json:"password"`
		FirstName string `json:"first_name"`
		LastName  string `json:"last_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_input", "Cuerpo de la petición inválido.")
		return
	}
	req.Email = strings.TrimSpace(req.Email)
	req.FirstName = strings.TrimSpace(req.FirstName)
	req.LastName = strings.TrimSpace(req.LastName)

	if req.Email == "" || req.Password == "" || req.FirstName == "" || req.LastName == "" {
		writeError(w, http.StatusBadRequest, "invalid_input", "Todos los campos son obligatorios.")
		return
	}
	if len(req.Password) < 12 {
		writeError(w, http.StatusBadRequest, "invalid_input", "La contraseña debe tener al menos 12 caracteres.")
		return
	}

	// Trabajo 1 (code review 056 · T11): PlatformAPIBaseURL es config del EDGE, no input del operador — si
	// quedó en "http://" contra un host que no es loopback, seguir mandaría la contraseña en claro por la
	// red. Se corta ANTES de marcar el socket: no es un fallo transitorio (reintentar no lo arregla), así
	// que lleva su propio código para que la UI no lo confunda con "la plataforma no responde".
	if config.PlatformAPIBaseURLInsecure(a.platformBaseURL) {
		if a.log != nil {
			a.log.Error("wapp-ctl: signup rechazado — PlatformAPIBaseURL insegura (http fuera de loopback)",
				"platform_api_base_url", a.platformBaseURL)
		}
		writeError(w, http.StatusInternalServerError, codeInsecurePlatformConfig,
			"No se puede completar el registro: la configuración de este Edge no es segura (mandaría tu contraseña sin cifrar por la red). Avisa al administrador para que revise la URL de la plataforma en este equipo.")
		return
	}

	payload := map[string]any{
		"email":      req.Email,
		"password":   req.Password,
		"first_name": req.FirstName,
		"last_name":  req.LastName,
		"origin":     "edge",
	}

	status, raw, err := a.callPlatformSignup(r.Context(), payload)
	if err != nil {
		// La plataforma no respondió (red caída, DNS, timeout, etc.): error real, NUNCA un 202 fabricado.
		writeError(w, http.StatusServiceUnavailable, codePlatformDown,
			"No se pudo contactar la plataforma para completar el registro: inténtalo de nuevo.")
		return
	}
	if status == http.StatusOK || status == http.StatusAccepted {
		writeJSON(w, http.StatusAccepted, map[string]string{
			"message": "Listo. Entra con tu correo y tu clave. Si ya tenías cuenta en el ecosistema, usa la de siempre.",
		})
		return
	}

	code, msg := friendlySignupMessage(status, raw)
	writeError(w, status, code, msg)
}

// decodePlatformError extrae un mensaje legible del cuerpo de error de POST /api/v1/signup. A diferencia
// del socket del núcleo (envelope estable {"error":{code,message}}, ver decodeAuthError), este endpoint
// HOY devuelve texto plano en sus errores (http.Error, ver wapp-cloud-platform/internal/platformadmin/
// signup.go) y su contrato de error está EN EVOLUCIÓN en paralelo a este cambio (C-03). Por eso se
// prueba, en orden: (1) el envelope JSON estándar por si el endpoint lo adopta, (2) un objeto JSON simple
// {"message":"..."}, (3) el cuerpo crudo como texto (recortado) — nunca se asume un shape concreto.
func decodePlatformError(raw []byte) string {
	var e errorBody
	if err := json.Unmarshal(raw, &e); err == nil && e.Error.Message != "" {
		return e.Error.Message
	}
	var m struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &m); err == nil && m.Message != "" {
		return m.Message
	}
	msg := strings.TrimSpace(string(raw))
	const maxLen = 300
	if len(msg) > maxLen {
		msg = msg[:maxLen]
	}
	return msg
}

// friendlySignupMessage traduce el status HTTP no-2xx de POST /api/v1/signup a (code, mensaje amigable)
// para el operador. Cubre explícitamente 503/502 (la plataforma o una dependencia suya —el cliente M2M,
// identity, ReplaceUserSystems— no está disponible) y 409 (correo ya registrado); el resto cae a un
// mensaje genérico con el detalle del cuerpo si lo hay. El status HTTP real se propaga siempre al
// navegador (nunca se colapsa a 202).
//
// El 502 comparte el mensaje del 503 a propósito, en vez de caer al default (que SÍ filtraría el
// detalle crudo del upstream): mientras T0.2 (credencial M2M de wApp hacia identity) siga sin hacerse,
// el 502/503 es el caso COMÚN de todo despliegue, no el raro — y ninguno de los dos debe revelar qué
// pieza interna falló (identity vs. ReplaceUserSystems).
func friendlySignupMessage(status int, raw []byte) (code, msg string) {
	detail := decodePlatformError(raw)
	switch status {
	case http.StatusServiceUnavailable, http.StatusBadGateway:
		return codePlatformDown, "El alta no está disponible en este momento: inténtalo más tarde."
	case http.StatusConflict:
		return "email_taken", "Ya existe una cuenta con ese correo. Inicia sesión, o contacta al administrador si no la recuerdas."
	case http.StatusTooManyRequests:
		return "rate_limited", "Demasiados intentos. Espera un momento e inténtalo de nuevo."
	case http.StatusBadRequest:
		if detail != "" {
			return "invalid_input", detail
		}
		return "invalid_input", "Revisa los datos del formulario."
	default:
		if detail != "" {
			return "signup_failed", detail
		}
		return "signup_failed", "No se pudo completar el registro. Inténtalo de nuevo."
	}
}

// handleLogout cierra la sesión: llama al socket /v1/auth/logout (best-effort), borra la sesión server-side
// y caduca las cookies. Exige CSRF si hay sesión (evita logout forzado de origen cruzado). La SPA navega a
// /login tras el 200.
func (a *authBorder) handleLogout(w http.ResponseWriter, r *http.Request) {
	sess := a.store.fromRequest(r)
	if sess != nil {
		if !csrfValid(r, sess) {
			writeError(w, http.StatusForbidden, "csrf_invalid", "Token CSRF ausente o inválido.")
			return
		}
		var body struct {
			AllSessions bool `json:"all_sessions"`
		}
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&body)
		}
		access, _ := sess.snapshot()
		// Best-effort: aunque el socket falle, cerramos la sesión local igual.
		_, _, _ = a.callAuth(r.Context(), "/v1/auth/logout", body, access)
		a.store.delete(sess.id)
	}
	clearSessionCookies(w)
	writeJSON(w, http.StatusOK, okResp{OK: true})
}

// handleSession expone los datos NO sensibles de la sesión para que la webui pinte el rol / decida si
// redirigir a login. Nunca devuelve el access token. "pending" (M-01, code review 056) es el estado REAL
// —tenant vacío— y no una cuenta de roles: un usuario con membresía y sin rol (D-056.11) NO está pendiente.
func (a *authBorder) handleSession(w http.ResponseWriter, r *http.Request) {
	sess := a.store.fromRequest(r)
	if sess == nil {
		writeJSON(w, http.StatusOK, map[string]any{"authenticated": false})
		return
	}
	roles, tenant, expires := sess.meta()
	writeJSON(w, http.StatusOK, map[string]any{
		"authenticated": true,
		"roles":         roles,
		"pending":       tenant == "",
		"expires_at":    expires,
	})
}

// decodeAuthError extrae (code, message) del envelope {error:{code,message}} del núcleo. Si no parsea,
// devuelve ("internal", "").
func decodeAuthError(raw []byte) (code, message string) {
	var e errorBody
	if err := json.Unmarshal(raw, &e); err != nil || e.Error.Code == "" {
		return "internal", ""
	}
	return e.Error.Code, e.Error.Message
}

// okResp es el cuerpo {"ok":true} de logout.
type okResp struct {
	OK bool `json:"ok"`
}
