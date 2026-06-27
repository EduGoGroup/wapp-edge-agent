package app

// UnlinkSession es el caso de uso de DESVINCULACIÓN y LIMPIEZA LOCAL de una sesión (DELETE
// /v1/sessions/{id}, ADR-0015). Materializa lo que el usuario pidió desde la UI: "eliminar/desvincular
// una sesión y limpiar su estado local".
//
// ORDEN SEGURO (idempotente de punta a punta):
//  1. best-effort LOGOUT REMOTO sobre el cliente vivo de la escucha, si lo hay (que WhatsApp suelte el
//     dispositivo vinculado). NO es fatal: si no hay cliente vivo o falla, se continúa con la limpieza
//     local (el mínimo viable es dejar el estado local limpio para un re-emparejamiento sano).
//  2. borrar el DEVICE del store cifrado (material whatsmeow) — cryptostore.DeleteDevice, sin la DEK.
//  3. borrar la fila de NEGOCIO de la sesión (tabla `sessions`).
//  4. limpiar la DEK de CUSTODIA (la que descifraba ese device ya no tiene uso).
//
// ZERO-KNOWLEDGE (ADR-0007): ningún paso DESCIFRA material ni mueve la DEK fuera del equipo; solo borra
// filas/ciphertext locales y el archivo de la DEK. El logout remoto viaja por el socket whatsmeow ya
// autenticado (no transporta la DEK).
//
// SINGLE-SESIÓN (verdad de campo 2026-06-27, dossier MP-01): hoy el Edge custodia una sola sesión (una
// ranura `dek.key`, un device por `.db`). Este caso de uso opera por JID EXPLÍCITO y es forward-compatible
// con el multi-sesión per-JID que resolverá MP-01: no asume "la única sesión", desvincula exactamente la
// del JID dado. NO intenta resolver multi-sesión (eso es MP-01).

import (
	"context"
	"errors"
	"fmt"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// ErrNoLiveClient lo devuelve LiveLogout cuando no hay un cliente whatsmeow VIVO para la sesión (la
// escucha no está corriendo o el cliente vivo es de otra sesión). El caso de uso lo trata como "logout
// omitido" (best-effort), no como fallo.
var ErrNoLiveClient = errors.New("unlink: no hay cliente vivo para el logout remoto")

// SessionRegistry es el puerto de METADATOS de negocio que necesita la desvinculación: consultar la
// sesión (existencia → 404) y borrar su fila. Lo implementa *sessionstore.Store (Get + Delete). Un fake
// lo simula en los tests.
type SessionRegistry interface {
	Get(ctx context.Context, jid string) (domain.Session, error)
	Delete(ctx context.Context, jid string) error
}

// DeviceEraser borra el material cripto local del device de la sesión `jid` del store cifrado, SIN la
// DEK (solo filas/ciphertext). Lo implementa *sessionstore.DeviceEraser (cryptostore.DeleteDevice).
type DeviceEraser interface {
	DeleteDevice(ctx context.Context, jid string) error
}

// CustodyCleaner limpia la DEK custodiada de la sesión. Lo implementa *keycustody.FileCustody (Clear).
// Es un puerto APARTE de KeyCustody a propósito: no se añade Clear a KeyCustody para no forzar a todos
// los custodios (el cambio de custodio de v1, ADR-0007, sigue siendo solo de adaptador).
type CustodyCleaner interface {
	Clear() error
}

// LiveLogout intenta un LOGOUT REMOTO best-effort sobre el cliente vivo de la escucha (que WhatsApp
// suelte el dispositivo). Lo implementa *whatsmeow.ListenGateway (LogoutLiveClient sobre el cliente vivo
// que ya mantiene). Devuelve ErrNoLiveClient si no hay cliente vivo (la escucha no corre / es otra
// sesión); cualquier otro error es un fallo del logout remoto (no bloquea la limpieza local).
type LiveLogout interface {
	LogoutLiveClient(ctx context.Context, jid string) error
}

// LogoutOutcome resume cómo fue el logout REMOTO best-effort, para informarlo en la respuesta /v1 sin
// que el resultado de la desvinculación dependa de él.
type LogoutOutcome string

const (
	// LogoutOK: WhatsApp aceptó el remove-companion-device (el teléfono ve el dispositivo desvinculado).
	LogoutOK LogoutOutcome = "ok"
	// LogoutSkipped: no había cliente vivo (escucha apagada / otra sesión) — solo limpieza local.
	LogoutSkipped LogoutOutcome = "skipped"
	// LogoutFailed: había cliente vivo pero el logout remoto falló — se limpió igual el estado local.
	LogoutFailed LogoutOutcome = "failed"
)

// UnlinkResult es el resultado de una desvinculación: el JID desvinculado, el estado de negocio PREVIO
// (lo que había antes de borrar) y el desenlace del logout remoto best-effort.
type UnlinkResult struct {
	JID          string
	Previous     domain.Session
	RemoteLogout LogoutOutcome
}

// UnlinkSession es el caso de uso. Sus dependencias son puertos (interfaces) para inyectar fakes en
// tests. `logout` y `locator` pueden ser nil (degradan a "sin logout remoto" / "sin fallback de device").
type UnlinkSession struct {
	registry SessionRegistry
	locator  PairedDeviceLocator // existencia por device cuando el registro aún no tiene la fila (ver Run)
	eraser   DeviceEraser
	custody  CustodyCleaner
	logout   LiveLogout // opcional (best-effort)
}

// NewUnlinkSession construye el caso de uso con los puertos dados.
func NewUnlinkSession(registry SessionRegistry, locator PairedDeviceLocator, eraser DeviceEraser, custody CustodyCleaner, logout LiveLogout) *UnlinkSession {
	return &UnlinkSession{registry: registry, locator: locator, eraser: eraser, custody: custody, logout: logout}
}

// Run desvincula y limpia la sesión `jid`. Devuelve ErrSessionNotFound (→ 404) si el JID no existe ni en
// el registro de negocio ni como device pareado en el store. Cualquier error de borrado real (BD/IO) se
// devuelve (→ 500); los borrados de algo ya ausente NO son error (idempotencia).
func (u *UnlinkSession) Run(ctx context.Context, jid string) (UnlinkResult, error) {
	previous, err := u.resolve(ctx, jid)
	if err != nil {
		return UnlinkResult{}, err
	}

	// 1. Logout remoto best-effort (no fatal): clasifica el desenlace para la respuesta.
	outcome := LogoutSkipped
	if u.logout != nil {
		switch err := u.logout.LogoutLiveClient(ctx, jid); {
		case err == nil:
			outcome = LogoutOK
		case errors.Is(err, ErrNoLiveClient):
			outcome = LogoutSkipped
		default:
			outcome = LogoutFailed
		}
	}

	// 2. Borrar el device del store cifrado (material whatsmeow), sin la DEK. Idempotente.
	if err := u.eraser.DeleteDevice(ctx, jid); err != nil {
		return UnlinkResult{}, fmt.Errorf("unlink: borrar device del store cifrado: %w", err)
	}

	// 3. Borrar la fila de negocio de la sesión. Idempotente (no-op si el registro no la tenía aún).
	if err := u.registry.Delete(ctx, jid); err != nil {
		return UnlinkResult{}, fmt.Errorf("unlink: borrar registro de sesión: %w", err)
	}

	// 4. Limpiar la DEK de custodia. Idempotente.
	if err := u.custody.Clear(); err != nil {
		return UnlinkResult{}, fmt.Errorf("unlink: limpiar DEK de custodia: %w", err)
	}

	return UnlinkResult{JID: jid, Previous: previous, RemoteLogout: outcome}, nil
}

// resolve decide si la sesión `jid` EXISTE (para distinguir 404 de borrado real) y devuelve su estado
// previo. Verdad de campo: el emparejamiento por /v1 aún NO registra la sesión en `sessions` (lo hace el
// backfill de RestoreSessions al rearrancar; dossier MP-01 §3.2), así que un device recién pareado puede
// existir en el store cifrado SIN fila de negocio. Por eso la existencia es: hay fila en el registro O
// hay un device pareado con ese JID en el store. Si ninguna → ErrSessionNotFound.
func (u *UnlinkSession) resolve(ctx context.Context, jid string) (domain.Session, error) {
	sess, err := u.registry.Get(ctx, jid)
	switch {
	case err == nil:
		return sess, nil
	case errors.Is(err, ErrSessionNotFound):
		// Sin fila de negocio: ¿hay un device pareado con ese JID en el store cifrado?
		if u.locator != nil {
			pairedJID, ok, lerr := u.locator.PairedJID(ctx)
			if lerr != nil {
				return domain.Session{}, fmt.Errorf("unlink: resolver device pareado: %w", lerr)
			}
			if ok && pairedJID == jid {
				// Device presente sin registro: existe a efectos de limpieza (estado previo desconocido).
				return domain.Session{JID: jid, State: domain.SessionStateActive}, nil
			}
		}
		return domain.Session{}, ErrSessionNotFound
	default:
		return domain.Session{}, fmt.Errorf("unlink: consultar sesión: %w", err)
	}
}
