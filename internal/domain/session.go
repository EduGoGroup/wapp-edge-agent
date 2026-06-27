package domain

import "time"

// SessionState es el estado de NEGOCIO de una sesión vinculada del Edge (no es estado de socket:
// eso lo gestiona el listener en RAM). Persiste en la tabla `sessions_v2` para que el arranque
// (app.RestoreSessions) sepa qué restaurar sin descifrar el store.
type SessionState string

const (
	// SessionStatePairing: sesión recién creada cuyo emparejamiento está EN CURSO (UUID y store/DEK
	// ya provisionados, JID aún desconocido hasta PairSuccess). No se restaura al arrancar; se
	// promueve a active en PairSuccess o se limpia si el pairing falla/expira (ADR-0016 §3, design §5).
	SessionStatePairing SessionState = "pairing"
	// SessionStateActive: sesión vinculada y operable (se restaura al arrancar).
	SessionStateActive SessionState = "active"
	// SessionStateLoggedOut: WhatsApp cerró la sesión (events.LoggedOut). NO se re-empareja
	// automáticamente (RF-6); requiere un nuevo pairing manual.
	SessionStateLoggedOut SessionState = "loggedout"
)

// Session es la entidad de dominio con los METADATOS de negocio de una sesión/teléfono vinculado
// (RF-7). NO contiene material criptográfico: las claves whatsmeow viven CIFRADAS en msg_enc_device
// de un store.db POR SESIÓN (ADR-0016 §2); aquí solo se referencia el JID y se anota el ciclo de vida
// (estado + timestamps) y dónde vive el store de la sesión (StoreDir).
//
// Una instancia por sesión (multi-sesión real, ADR-0008/0016).
type Session struct {
	// SessionID es la IDENTIDAD CANÓNICA de la sesión: un UUIDv4 opaco generado por el Edge al
	// INICIAR el emparejamiento (ADR-0016 §3). Nombra el directorio del store y discrimina la sesión
	// en CloudLink/fleet/Motor de Flujos. Es la clave primaria de `sessions_v2`.
	SessionID string
	// JID es el identificador de la sesión en WhatsApp (coincide con msg_enc_device.jid). OPCIONAL:
	// vacío mientras el estado es 'pairing' (el número se descubre recién en PairSuccess); cuando no
	// está vacío es ÚNICO entre sesiones (índice parcial ux_sessions_jid).
	JID string
	// State es el estado de negocio (pairing / active / loggedout).
	State SessionState
	// StoreDir es la ruta RELATIVA (a data_dir) del directorio de la sesión (sessions/<session_id>),
	// donde viven su store.db cifrado y su dek.key (ADR-0016 §4). El Layout (T1) la materializa; aquí
	// es solo el metadato persistido.
	StoreDir string
	// PairedAt es el instante del emparejamiento original (cero mientras pairing).
	PairedAt time.Time
	// UpdatedAt es el instante de la última actualización de estado (p.ej. al restaurar).
	UpdatedAt time.Time
}
