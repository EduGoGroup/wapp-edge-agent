package app

// logout.go define el PUERTO de logout remoto best-effort (soltar el dispositivo vinculado en WhatsApp),
// reubicado aquí tras retirar el caso de uso por-JID app.UnlinkSession (integración Plan 008).
//
// Estado (integración 008): el borrado canónico es sessionmgr.Manager.Unlink (DELETE /v1/sessions/{id}),
// que hace el BORRADO QUIRÚRGICO LOCAL de la sesión (cancela su listener, limpia SU DEK, borra SU fila y
// SU directorio; design §7). El Manager NO ejecuta hoy el logout REMOTO porque no retiene el cliente vivo
// por sesión (cada listener lo posee dentro de su goroutine). Este puerto y su adaptador
// (whatsmeow.ListenGateway.LogoutLiveClient) se CONSERVAN como infraestructura reutilizable para cuando
// Manager.Unlink incorpore el logout remoto por sesión (FOLLOW-UP, no en este corte).

import (
	"context"
	"errors"
)

// ErrNoLiveClient lo devuelve LiveLogout cuando no hay un cliente whatsmeow VIVO para la sesión (la
// escucha no está corriendo o el cliente vivo es de otra sesión). Un consumidor lo trata como "logout
// omitido" (best-effort), no como fallo.
var ErrNoLiveClient = errors.New("logout: no hay cliente vivo para el logout remoto")

// LiveLogout intenta un LOGOUT REMOTO best-effort sobre el cliente vivo de la escucha (que WhatsApp
// suelte el dispositivo). Lo implementa *whatsmeow.ListenGateway (LogoutLiveClient sobre el cliente vivo
// que ya mantiene). Devuelve ErrNoLiveClient si no hay cliente vivo (la escucha no corre / es otra
// sesión); cualquier otro error es un fallo del logout remoto (no debe bloquear la limpieza local).
type LiveLogout interface {
	LogoutLiveClient(ctx context.Context, jid string) error
}
