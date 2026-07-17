// Package auth implementa la identidad del OPERADOR del plano de control del Edge
// (Plan 033 Ola 3 / ADR-0025): login/refresh/logout relayados al IAM de la nube
// por CloudLink, custodia del refresh token, validación ES256 OFFLINE del access
// token (con la llave pública instalada por ConfigUpdate kind:"jwks") y el gate
// RBAC edge.* (default DENY) que protege las rutas del contrato /v1.
//
// Frontera zero-knowledge (ADR-0007): la auth de usuario protege el PLANO DE
// CONTROL, jamás el plano de datos. Un operador logueado NO obtiene la DEK; el
// daemon 24/7 sigue enviando/recibiendo aunque el login falle.
package auth

import (
	"context"
	"errors"
	"time"
)

// Tokens es el par de tokens que el IAM emite (espejo de UserTokens del proto
// CloudLink). ExpiresAt es el instante de expiración del access token. En un
// logout exitoso el Relay devuelve nil error sin Tokens (no hay credenciales
// nuevas), por eso Logout no devuelve Tokens.
type Tokens struct {
	AccessToken  string
	RefreshToken string
	TokenType    string
	ExpiresAt    time.Time
}

// Relay es el puerto de auth de usuario contra el IAM de la nube. Lo implementa
// el adapter CloudLink (internal/adapters/cloudlink): relaya las credenciales por
// el stream bidi existente y correlaciona la respuesta por command_id. El
// Manager depende SOLO de este puerto (mockeable en tests, sin red).
//
// El tenant NO viaja en estos métodos: es implícito del canal mTLS enrolado del
// Edge (ADR-0025 dec.1). Los errores se devuelven TIPADOS (ver más abajo) para
// que el Manager/endpoint decidan sin parsear texto.
type Relay interface {
	// Login canjea email+password por un par de tokens del IAM.
	Login(ctx context.Context, email, password string) (Tokens, error)
	// Refresh canjea el refresh token vigente por un par nuevo (rotación).
	Refresh(ctx context.Context, refreshToken string) (Tokens, error)
	// Logout revoca el refresh token indicado (o todas las sesiones del usuario
	// si allSessions=true). Éxito ⇒ nil.
	Logout(ctx context.Context, refreshToken string, allSessions bool) error
}

// Errores TIPADOS del relay de auth (mapean UserAuthError.Code del proto y el
// caso "sin stream"). El adapter CloudLink los produce; el Manager y el endpoint
// /v1/auth los discriminan con errors.Is.
var (
	// ErrInvalidCredentials: email/password incorrectos (code "invalid_credentials").
	ErrInvalidCredentials = errors.New("auth: credenciales inválidas")
	// ErrUserInactive: el usuario existe pero está inactivo (code "user_inactive").
	ErrUserInactive = errors.New("auth: usuario inactivo")
	// ErrRefreshInvalid: el refresh token no es válido/está revocado/expiró (code "refresh_invalid").
	ErrRefreshInvalid = errors.New("auth: refresh token inválido")
	// ErrInvalidInput: entrada malformada rechazada por el IAM (code "invalid_input").
	ErrInvalidInput = errors.New("auth: entrada inválida")
	// ErrTenantMismatch: el token es válido en el IAM pero de otro tenant (code "tenant_mismatch").
	ErrTenantMismatch = errors.New("auth: tenant no coincide")
	// ErrRelayInternal: fallo interno del IAM/gateway (code "internal" o desconocido).
	ErrRelayInternal = errors.New("auth: error interno del relay")
	// ErrRelayOffline: no hay stream CloudLink vivo para relayar la petición. No hay
	// login offline de primera vez (ADR-0025 dec.3): sin nube, no hay identidad nueva.
	ErrRelayOffline = errors.New("auth: sin conexión con la nube (relay offline)")
)
