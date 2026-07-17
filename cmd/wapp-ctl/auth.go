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
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"time"

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

// authBorder agrupa las dependencias de los handlers de auth del borde.
type authBorder struct {
	store  *sessionStore
	client *http.Client
	log    sharedlogger.Logger
}

func newAuthBorder(store *sessionStore, client *http.Client, log sharedlogger.Logger) *authBorder {
	return &authBorder{store: store, client: client, log: log}
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
	sess := a.store.create(res.AccessToken, res.Roles, res.ExpiresAt)
	setSessionCookies(w, sess)
	writeJSON(w, http.StatusOK, map[string]any{
		"authenticated": true,
		"roles":         res.Roles,
		"expires_at":    res.ExpiresAt,
	})
}

// handleLoginGet sirve la pantalla de login. Si ya hay sesión válida, redirige a la webui principal.
func (a *authBorder) handleLoginGet(w http.ResponseWriter, r *http.Request) {
	if a.store.fromRequest(r) != nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	http.ServeFileFS(w, r, webui.FS(), "login.html")
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
// redirigir a login. Nunca devuelve el access token.
func (a *authBorder) handleSession(w http.ResponseWriter, r *http.Request) {
	sess := a.store.fromRequest(r)
	if sess == nil {
		writeJSON(w, http.StatusOK, map[string]any{"authenticated": false})
		return
	}
	roles, expires := sess.meta()
	writeJSON(w, http.StatusOK, map[string]any{
		"authenticated": true,
		"roles":         roles,
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
