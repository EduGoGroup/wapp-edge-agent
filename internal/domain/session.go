package domain

import "time"

// SessionState es el estado de NEGOCIO de una sesión vinculada del Edge (no es estado de socket:
// eso lo gestiona el listener en RAM). Persiste en la tabla `sessions` para que el arranque
// (app.RestoreSessions) sepa qué restaurar sin descifrar el store.
type SessionState string

const (
	// SessionStateActive: sesión vinculada y operable (se restaura al arrancar).
	SessionStateActive SessionState = "active"
	// SessionStateLoggedOut: WhatsApp cerró la sesión (events.LoggedOut). NO se re-empareja
	// automáticamente (RF-6); requiere un nuevo pairing manual.
	SessionStateLoggedOut SessionState = "loggedout"
)

// Session es la entidad de dominio con los METADATOS de negocio de una sesión/teléfono vinculado
// (T6.1, RF-7). NO contiene material criptográfico: las claves whatsmeow viven CIFRADAS en
// msg_enc_device; aquí solo se referencia el JID y se anota el ciclo de vida (estado + timestamps).
//
// Una instancia por número (semilla del multi-teléfono, ADR-0008; el spike maneja UNA).
type Session struct {
	// JID es el identificador de la sesión en WhatsApp (coincide con msg_enc_device.jid).
	JID string
	// State es el estado de negocio (active / loggedout).
	State SessionState
	// PairedAt es el instante del emparejamiento original.
	PairedAt time.Time
	// UpdatedAt es el instante de la última actualización de estado (p.ej. al restaurar).
	UpdatedAt time.Time
}
