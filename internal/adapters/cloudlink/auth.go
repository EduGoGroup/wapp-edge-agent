package cloudlink

// auth.go implementa el RELAY de auth de usuario del operador (Plan 033 Ola 3 / ADR-0025): el Edge
// ENVÍA EdgeToCloud{UserLogin|UserRefresh|UserLogout} por el único stream y ESPERA el CloudToEdge{
// UserAuthResponse} correlacionado por command_id (mismo patrón request/response que Ping→Pong y
// DiagnosticsRequest→Bundle). El Adapter satisface el puerto internal/auth.Relay; el session manager
// (internal/auth.Manager) depende solo de ese puerto, sin conocer gRPC.
//
// El tenant NO viaja en los frames: es implícito del canal mTLS enrolado del Edge (ADR-0025 dec.1). Las
// credenciales del operador se RELAYAN sin custodiarse (el refresh sí se custodia, pero eso es del Manager).

import (
	"context"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	edgeauth "github.com/EduGoGroup/wapp-edge-agent/internal/auth"
	"github.com/google/uuid"
)

// controlSessionID es el session_id "de control" ESTABLE que el Edge estampa en los frames de auth.
//
// Por qué una constante y no una sesión de WhatsApp: el gateway de la nube (contrato heredado de la Ola 2)
// enruta el UserAuthResponse por registry.Push(session_id) y registra la sesión de forma PEREZOSA al
// primer frame, así que el frame de auth DEBE llevar un session_id no vacío. Pero el operador puede
// loguearse ANTES de emparejar ningún teléfono (primer arranque), cuando no existe ninguna sesión de
// WhatsApp. Un id de control fijo resuelve ambas cosas: es estable, existe siempre, y como la respuesta se
// correlaciona por command_id en el pre-switch del demux (handleUserAuthResponse), el Edge no necesita que
// este id esté registrado como sesión de WhatsApp en su propio Registry.
const controlSessionID = "__wapp_control__"

// authRelayTimeout acota la espera de la respuesta de auth cuando el contexto del llamador no trae deadline
// propio, para que un stream vivo pero mudo no cuelgue el endpoint /v1/auth indefinidamente.
const authRelayTimeout = 20 * time.Second

var _ edgeauth.Relay = (*Adapter)(nil)

// Login relaya email+password al IAM por el stream y espera el par de tokens. ErrRelayOffline si no hay
// stream vivo (no hay login offline de primera vez, ADR-0025 dec.3).
func (a *Adapter) Login(ctx context.Context, email, password string) (edgeauth.Tokens, error) {
	return a.doAuthTokens(ctx, func(cmdID string) *cloudlinkv1.EdgeToCloud {
		return &cloudlinkv1.EdgeToCloud{
			CommandId: cmdID,
			SessionId: controlSessionID,
			Payload: &cloudlinkv1.EdgeToCloud_UserLogin{UserLogin: &cloudlinkv1.UserLoginRequest{
				CommandId: cmdID,
				SessionId: controlSessionID,
				Email:     email,
				Password:  password,
			}},
		}
	})
}

// Refresh relaya el refresh token vigente y espera el par nuevo (rotación).
func (a *Adapter) Refresh(ctx context.Context, refreshToken string) (edgeauth.Tokens, error) {
	return a.doAuthTokens(ctx, func(cmdID string) *cloudlinkv1.EdgeToCloud {
		return &cloudlinkv1.EdgeToCloud{
			CommandId: cmdID,
			SessionId: controlSessionID,
			Payload: &cloudlinkv1.EdgeToCloud_UserRefresh{UserRefresh: &cloudlinkv1.UserRefreshRequest{
				CommandId:    cmdID,
				SessionId:    controlSessionID,
				RefreshToken: refreshToken,
			}},
		}
	})
}

// Logout relaya la revocación del refresh (o de todas las sesiones si allSessions). Éxito ⇒ nil (el
// gateway responde un UserTokens vacío = ok sin credenciales nuevas).
func (a *Adapter) Logout(ctx context.Context, refreshToken string, allSessions bool) error {
	_, err := a.doAuth(ctx, func(cmdID string) *cloudlinkv1.EdgeToCloud {
		return &cloudlinkv1.EdgeToCloud{
			CommandId: cmdID,
			SessionId: controlSessionID,
			Payload: &cloudlinkv1.EdgeToCloud_UserLogout{UserLogout: &cloudlinkv1.UserLogoutRequest{
				CommandId:    cmdID,
				SessionId:    controlSessionID,
				RefreshToken: refreshToken,
				AllSessions:  allSessions,
			}},
		}
	})
	return err
}

// doAuthTokens ejecuta doAuth y proyecta el UserTokens de la respuesta a edgeauth.Tokens.
func (a *Adapter) doAuthTokens(ctx context.Context, build func(cmdID string) *cloudlinkv1.EdgeToCloud) (edgeauth.Tokens, error) {
	resp, err := a.doAuth(ctx, build)
	if err != nil {
		return edgeauth.Tokens{}, err
	}
	tk := resp.GetTokens()
	return edgeauth.Tokens{
		AccessToken:  tk.GetAccessToken(),
		RefreshToken: tk.GetRefreshToken(),
		TokenType:    tk.GetTokenType(),
		ExpiresAt:    time.Unix(tk.GetExpiresAt(), 0),
	}, nil
}

// doAuth arma el frame con un command_id nuevo, registra el canal de correlación, lo envía por el único
// stream y espera el UserAuthResponse (o el timeout/cancelación). Mapea UserAuthError.Code a los errores
// tipados de internal/auth. Sin stream vivo ⇒ ErrRelayOffline.
func (a *Adapter) doAuth(ctx context.Context, build func(cmdID string) *cloudlinkv1.EdgeToCloud) (*cloudlinkv1.UserAuthResponse, error) {
	cl := a.currentClient()
	if cl == nil {
		return nil, edgeauth.ErrRelayOffline
	}

	cmdID := uuid.NewString()
	ch := make(chan *cloudlinkv1.UserAuthResponse, 1)
	a.authMu.Lock()
	a.authPending[cmdID] = ch
	a.authMu.Unlock()
	defer func() {
		a.authMu.Lock()
		delete(a.authPending, cmdID)
		a.authMu.Unlock()
	}()

	a.send(cl, build(cmdID))

	// Deadline efectivo: el del llamador si lo trae, o authRelayTimeout como suelo.
	timer := time.NewTimer(authRelayTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		a.log.Warn("CloudLink: auth de usuario sin respuesta dentro del timeout (relay)", "command_id", cmdID)
		return nil, edgeauth.ErrRelayOffline
	case resp := <-ch:
		return a.mapAuthResponse(resp)
	}
}

// mapAuthResponse traduce la rama del oneof de UserAuthResponse: tokens ⇒ (resp, nil); error ⇒ error
// tipado por code. Una respuesta sin ninguna rama es un fallo interno del gateway.
func (a *Adapter) mapAuthResponse(resp *cloudlinkv1.UserAuthResponse) (*cloudlinkv1.UserAuthResponse, error) {
	if resp.GetTokens() != nil {
		return resp, nil
	}
	if e := resp.GetError(); e != nil {
		return nil, mapAuthErrorCode(e.GetCode())
	}
	// Rama vacía: sin tokens y sin error explícito. Es válido SOLO para el logout (UserTokens vacío = ok);
	// como GetTokens() devuelve nil también en ese caso, distinguimos por ausencia de error ⇒ ok.
	return resp, nil
}

// mapAuthErrorCode mapea el code estable de UserAuthError a los errores tipados de internal/auth.
func mapAuthErrorCode(code string) error {
	switch code {
	case "invalid_credentials":
		return edgeauth.ErrInvalidCredentials
	case "user_inactive":
		return edgeauth.ErrUserInactive
	case "refresh_invalid":
		return edgeauth.ErrRefreshInvalid
	case "invalid_input":
		return edgeauth.ErrInvalidInput
	case "tenant_mismatch":
		return edgeauth.ErrTenantMismatch
	default: // "internal" y cualquier code futuro no reconocido
		return edgeauth.ErrRelayInternal
	}
}

// handleUserAuthResponse entrega la respuesta al canal pendiente correlacionado por command_id (demux,
// pre-switch de handleCommand). Una respuesta sin petición pendiente (timeout previo, duplicado) se
// descarta con log. El envío es no bloqueante (canal con buffer 1).
func (a *Adapter) handleUserAuthResponse(ar *cloudlinkv1.UserAuthResponse) {
	cmdID := ar.GetCommandId()
	a.authMu.Lock()
	ch := a.authPending[cmdID]
	a.authMu.Unlock()
	if ch == nil {
		a.log.Warn("CloudLink: UserAuthResponse sin petición pendiente (timeout/duplicado), ignorado",
			"command_id", cmdID)
		return
	}
	select {
	case ch <- ar:
	default:
		a.log.Warn("CloudLink: UserAuthResponse duplicado para el mismo command_id, ignorado", "command_id", cmdID)
	}
}
