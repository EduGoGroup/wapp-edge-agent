package server

// auth.go cuelga los endpoints de auth de OPERADOR del contrato /v1 (Plan 033 Ola 3 / ADR-0025):
// POST /v1/auth/{login,refresh,logout}. Estos endpoints están EXENTOS del gate edge.* (son la vía para
// OBTENER el token, o para refrescarlo/cerrarlo). El núcleo relaya al IAM por CloudLink (AuthService =
// *auth.Manager) y custodia el refresh token; SOLO devuelve el ACCESS token a wapp-ctl (borde navegador),
// que lo reenvía como "Authorization: Bearer" en las rutas protegidas. El refresh token NUNCA sale del
// núcleo (zero-knowledge del refresh, ADR-0025).
//
// CONTRATO (para el Paso B, wapp-ctl + UI):
//
//	POST /v1/auth/login   body {"email","password"}      → 200 LoginResult | {error:{code,message}}
//	POST /v1/auth/refresh  (sin cuerpo; usa el refresh custodiado) → 200 LoginResult | error
//	POST /v1/auth/logout  body {"all_sessions":bool?}     → 200 {"ok":true} | error
//
// LoginResult = {"access_token","token_type","expires_at" (RFC3339),"roles":[...]}. NO incluye el refresh.
// Códigos de error (code): invalid_credentials(401) · user_inactive(403) · refresh_invalid(401) ·
// invalid_input(400) · tenant_mismatch(403) · relay_offline(503) · internal(502). El Paso B mapea code→UI.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	edgeauth "github.com/EduGoGroup/wapp-edge-agent/internal/auth"
)

// AuthService es el puerto ESTRECHO que los endpoints /v1/auth necesitan del session manager. Lo cumple
// *auth.Manager. Login/Refresh devuelven el access token (para wapp-ctl); el refresh se custodia dentro.
type AuthService interface {
	Login(ctx context.Context, email, password string) (edgeauth.LoginResult, error)
	Refresh(ctx context.Context) (edgeauth.LoginResult, error)
	Logout(ctx context.Context, allSessions bool) error
}

// authHandler sirve /v1/auth/* sobre el AuthService.
type authHandler struct {
	svc AuthService
	log logger // subconjunto Info/Error; puede ser nil
}

// RegisterAuth cuelga POST /v1/auth/{login,refresh,logout} sobre el AuthService. Se llama ANTES de Serve
// (igual que RegisterPairing/RegisterUnlink). NO se protegen con guard (son la puerta de entrada de la auth).
func (s *Server) RegisterAuth(svc AuthService) {
	h := &authHandler{svc: svc}
	if s.log != nil {
		h.log = s.log
	}
	s.Handle(http.MethodPost, "/v1/auth/login", h.handleLogin)
	s.Handle(http.MethodPost, "/v1/auth/refresh", h.handleRefresh)
	s.Handle(http.MethodPost, "/v1/auth/logout", h.handleLogout)
}

// loginRequest es el cuerpo de POST /v1/auth/login.
type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// logoutRequest es el cuerpo (opcional) de POST /v1/auth/logout.
type logoutRequest struct {
	AllSessions bool `json:"all_sessions"`
}

// okResponse es el cuerpo de un logout exitoso.
type okResponse struct {
	OK bool `json:"ok"`
}

func (h *authHandler) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "cuerpo JSON inválido")
		return
	}
	email := strings.TrimSpace(req.Email)
	if email == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "email y password son requeridos")
		return
	}
	res, err := h.svc.Login(r.Context(), email, req.Password)
	if err != nil {
		h.writeAuthError(w, "login", err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *authHandler) handleRefresh(w http.ResponseWriter, r *http.Request) {
	res, err := h.svc.Refresh(r.Context())
	if err != nil {
		h.writeAuthError(w, "refresh", err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *authHandler) handleLogout(w http.ResponseWriter, r *http.Request) {
	var req logoutRequest
	// Cuerpo opcional: un logout sin cuerpo (all_sessions=false) es válido.
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if err := h.svc.Logout(r.Context(), req.AllSessions); err != nil {
		h.writeAuthError(w, "logout", err)
		return
	}
	writeJSON(w, http.StatusOK, okResponse{OK: true})
}

// writeAuthError mapea los errores tipados del relay/manager al envelope /v1 con su status y code estables.
func (h *authHandler) writeAuthError(w http.ResponseWriter, op string, err error) {
	status, code := authErrorToHTTP(err)
	if h.log != nil {
		h.log.Error("plano de control: auth de operador falló", "op", op, "code", code, "error", err)
	}
	writeError(w, status, code, err.Error())
}

// authErrorToHTTP traduce el error tipado a (statusHTTP, code estable del contrato).
func authErrorToHTTP(err error) (int, string) {
	switch {
	case errors.Is(err, edgeauth.ErrInvalidCredentials):
		return http.StatusUnauthorized, "invalid_credentials"
	case errors.Is(err, edgeauth.ErrRefreshInvalid):
		return http.StatusUnauthorized, "refresh_invalid"
	case errors.Is(err, edgeauth.ErrUserInactive):
		return http.StatusForbidden, "user_inactive"
	case errors.Is(err, edgeauth.ErrTenantMismatch):
		return http.StatusForbidden, "tenant_mismatch"
	case errors.Is(err, edgeauth.ErrInvalidInput):
		return http.StatusBadRequest, "invalid_input"
	case errors.Is(err, edgeauth.ErrRelayOffline):
		return http.StatusServiceUnavailable, "relay_offline"
	default:
		return http.StatusBadGateway, "internal"
	}
}
