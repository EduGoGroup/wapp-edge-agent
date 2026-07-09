package domain

import "time"

// SessionState es el estado de NEGOCIO de un dispositivo vinculado del Edge (no es estado de socket:
// eso lo gestiona el listener en RAM). Persiste en `devices.state` (BD única, Plan 022) para que el
// arranque (Manager.Restore) sepa qué restaurar sin descifrar el store.
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

// DeviceRole es el ROL del dispositivo dentro de su cuenta (número), base del failover multi-dispositivo
// por número (ADR-0018 §Decisión.5, Plan 022 T5). Off por defecto: un dispositivo 'primary' por cuenta.
// Multi-device es RESILIENCIA, NO sigilo (más dispositivos NO reduce el riesgo de baneo).
type DeviceRole = string

const (
	// DeviceRolePrimary: el dispositivo operativo por defecto de la cuenta (default en `devices.role`).
	DeviceRolePrimary DeviceRole = "primary"
	// DeviceRoleStandby: dispositivo de reserva del mismo número; se promueve a primary si el primary
	// cae/expira (T5). A nivel de protocolo todos reciben el fan-out; el rol es funcional en la app.
	DeviceRoleStandby DeviceRole = "standby"
)

// Session es la entidad de dominio con los METADATOS de negocio de un DISPOSITIVO vinculado y su CUENTA
// (RF-7). NO contiene material criptográfico: las claves whatsmeow viven CIFRADAS en msg_enc_device de la
// BD única (ADR-0018 §2), cifradas con la DEK POR DISPOSITIVO; aquí solo se referencian JID/cuenta y se
// anota el ciclo de vida (estado + timestamps).
//
// Con la BD única (Plan 022) el registro persiste en `devices` (⨝ `accounts`), no en `sessions_v2`. El
// nombre `Session` se conserva por compatibilidad con los consumidores/puertos existentes: un `Session`
// modela un DISPOSITIVO (`session_id`) y arrastra los datos de su cuenta (`account_id`/`self_pn`).
type Session struct {
	// SessionID es la IDENTIDAD CANÓNICA del dispositivo: un UUIDv4 opaco generado por el Edge al
	// INICIAR el emparejamiento (ADR-0016 §3). Discrimina el dispositivo en CloudLink/fleet/Motor de
	// Flujos. Es la clave primaria de `devices`.
	SessionID string
	// AccountID es la cuenta (número de negocio) a la que pertenece el dispositivo (FK a `accounts`).
	// Vacío en construcción por los consumidores actuales: el adaptador lo resuelve/asigna al persistir
	// (cuenta por `self_pn`, o provisional por `session_id` mientras el número no se conoce). Se puebla
	// al leer de la BD.
	AccountID string
	// SelfPN es el número propio (E.164 sin '+') de la CUENTA del dispositivo. Vacío hasta conocer el
	// JID (PairSuccess); cuando no está vacío es ÚNICO entre cuentas. Al persistir, misma SelfPN ⇒ misma
	// cuenta (un re-escaneo cuelga de la cuenta existente).
	SelfPN string
	// DisplayName es el nombre visible de la cuenta (opcional).
	DisplayName string
	// JID es el identificador del dispositivo en WhatsApp (coincide con msg_enc_device.jid). OPCIONAL:
	// vacío mientras el estado es 'pairing' (el número se descubre recién en PairSuccess); cuando no
	// está vacío es ÚNICO entre dispositivos (índice parcial ux_devices_jid).
	JID string
	// State es el estado de negocio (pairing / active / suspended / loggedout).
	State SessionState
	// Role es el rol del dispositivo dentro de su cuenta (primary/standby, failover T5). Default primary.
	Role DeviceRole
	// StoreDir es la ruta RELATIVA (a data_dir) del directorio histórico de la sesión (sessions/<id>).
	// Con la BD única ya NO existe columna `store_dir`: el adaptador la DERIVA de `sessions/<session_id>`
	// para no romper a los consumidores runtime mientras migran a la BD única (T3). No se persiste.
	StoreDir string
	// PairedAt es el instante del emparejamiento original (cero mientras pairing).
	PairedAt time.Time
	// UpdatedAt es el instante de la última actualización de estado (p.ej. al restaurar).
	UpdatedAt time.Time
}
