package main

// proxy.go es el reverse-proxy de /v1/* al Unix socket del núcleo (ADR-0015), ENDURECIDO para el borde
// navegador (Plan 033 · Ola 3 · Paso B, ADR-0025):
//
//   - Inyecta `Authorization: Bearer <access de la sesión>` cuando hay sesión (la cookie HttpOnly ata el
//     navegador a la sesión; el access vive server-side).
//   - CSRF (double-submit): las peticiones mutadoras (POST/DELETE/PUT/PATCH) exigen X-CSRF-Token válido
//     SI hay sesión. Las de bootstrap sin sesión (p.ej. POST /v1/enroll en primera ejecución) pasan.
//   - Retry-on-401 con SINGLE-FLIGHT: si el núcleo responde 401 (access expirado) y hay sesión, wapp-ctl
//     llama /v1/auth/refresh (sin body; el refresh lo custodia el núcleo), rota el access y REINTENTA una
//     vez. Si el refresh también falla → limpia cookies + 401 (la SPA redirige a /login).
//   - Bloquea /v1/auth/* desde el navegador: esas rutas se consumen server-side vía /login,/logout.
//   - /v1/health y /v1/enroll* quedan EXENTOS (bootstrap primera ejecución): se proxyan sin Bearer.
//
// El SSE (/v1/logs) se transmite en directo (sin capturar para retry): si diera 401, la SPA reconecta.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// errRefreshFailed marca un refresh no-200 del socket (dispara la limpieza de la sesión de operador).
var errRefreshFailed = errors.New("wapp-ctl: refresh del operador rechazado por el núcleo")

// coreProxy es el handler de /v1/* endurecido.
type coreProxy struct {
	rp    *httputil.ReverseProxy
	auth  *authBorder
	store *sessionStore
	log   sharedlogger.Logger
}

// newCoreProxy construye el reverse-proxy a /v1/* del núcleo por el Unix socket. Si el socket no responde
// (núcleo caído), el ErrorHandler TRADUCE el fallo a 503 + envelope "daemon_down" (contrato estable para
// la UI), nunca un 502 crudo.
func newCoreProxy(socketPath string, auth *authBorder, store *sessionStore, log sharedlogger.Logger) *coreProxy {
	rp := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			r.URL.Scheme = "http"
			r.URL.Host = "unix" // placeholder; el DialContext ignora el host y marca el socket
		},
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socketPath)
			},
			ResponseHeaderTimeout: 30 * time.Second,
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if log != nil {
				log.Warn("wapp-ctl: núcleo no responde por el socket (daemon down)", "path", r.URL.Path, "error", err)
			}
			writeError(w, http.StatusServiceUnavailable, codeDaemonDown,
				"el núcleo no responde por el socket: arranca el daemon (POST /v1/daemon/start)")
		},
	}
	return &coreProxy{rp: rp, auth: auth, store: store, log: log}
}

// isBootstrapExempt son las rutas EXENTAS de sesión/CSRF (coherente con las exentas del núcleo): la sonda
// de salud y el onboarding de primera ejecución (aún no hay operador).
func isBootstrapExempt(path string) bool {
	return path == "/v1/health" || path == "/v1/enroll" || strings.HasPrefix(path, "/v1/enroll/")
}

// isMutating indica si el método muta estado (exige CSRF cuando hay sesión).
func isMutating(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

// isStream detecta el SSE de /v1/logs (no se captura para retry: se transmite en directo).
func isStream(r *http.Request) bool {
	return r.URL.Path == "/v1/logs" || strings.Contains(r.Header.Get("Accept"), "text/event-stream")
}

func (p *coreProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// El navegador NO habla /v1/auth/* directamente: son server-side (vía /login,/logout).
	if strings.HasPrefix(path, "/v1/auth/") {
		writeError(w, http.StatusNotFound, codeNotFound, "ruta no disponible desde el navegador")
		return
	}

	sess := p.store.fromRequest(r)

	// CSRF: mutadoras con sesión exigen token válido. Bootstrap (enroll) sin sesión pasa.
	if isMutating(r.Method) && sess != nil && !csrfValid(r, sess) {
		writeError(w, http.StatusForbidden, "csrf_invalid", "Token CSRF ausente o inválido.")
		return
	}

	// Bootstrap exento (health/enroll): proxy directo sin Bearer.
	if isBootstrapExempt(path) {
		p.rp.ServeHTTP(w, r)
		return
	}

	// Sin sesión: se proxya tal cual (sin Bearer); el núcleo es la frontera de confianza y decide (401).
	if sess == nil {
		p.rp.ServeHTTP(w, r)
		return
	}

	access, gen := sess.snapshot()

	// SSE: transmisión directa (no se captura para retry). Se inyecta el Bearer vigente.
	if isStream(r) {
		r.Header.Set("Authorization", "Bearer "+access)
		p.rp.ServeHTTP(w, r)
		return
	}

	// Buffer del cuerpo para poder REINTENTAR el request original tras un refresh.
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		_ = r.Body.Close()
	}

	cap1 := newCapture()
	p.attempt(r, body, access, cap1)

	if cap1.status == http.StatusUnauthorized {
		// Access expirado: refresh single-flight + un reintento.
		newAccess, err := sess.refreshIfStale(gen, func() (string, []string, time.Time, error) {
			return p.doRefresh(r.Context())
		})
		if err != nil {
			// Refresh también inválido: la sesión murió. Limpia cookies y responde 401.
			p.store.delete(sess.id)
			clearSessionCookies(w)
			writeError(w, http.StatusUnauthorized, "session_expired", "La sesión caducó. Vuelve a iniciar sesión.")
			return
		}
		cap2 := newCapture()
		p.attempt(r, body, newAccess, cap2)
		cap2.flush(w)
		return
	}

	cap1.flush(w)
}

// attempt ejecuta un intento del proxy con el Bearer indicado sobre una copia del request (cuerpo fresco).
func (p *coreProxy) attempt(orig *http.Request, body []byte, bearer string, cw *captureWriter) {
	r2 := orig.Clone(orig.Context())
	if body != nil {
		r2.Body = io.NopCloser(bytes.NewReader(body))
		r2.ContentLength = int64(len(body))
	}
	r2.Header.Set("Authorization", "Bearer "+bearer)
	p.rp.ServeHTTP(cw, r2)
}

// doRefresh llama al socket /v1/auth/refresh (sin body: el refresh lo custodia el núcleo) y devuelve el
// access rotado + metadatos. Un status != 200 se traduce a error (dispara la limpieza de sesión).
func (p *coreProxy) doRefresh(ctx context.Context) (string, []string, time.Time, error) {
	status, raw, err := p.auth.callAuth(ctx, "/v1/auth/refresh", nil, "")
	if err != nil {
		return "", nil, time.Time{}, err
	}
	if status != http.StatusOK {
		return "", nil, time.Time{}, errRefreshFailed
	}
	var res loginResult
	if err := json.Unmarshal(raw, &res); err != nil || res.AccessToken == "" {
		return "", nil, time.Time{}, errRefreshFailed
	}
	return res.AccessToken, res.Roles, res.ExpiresAt, nil
}
