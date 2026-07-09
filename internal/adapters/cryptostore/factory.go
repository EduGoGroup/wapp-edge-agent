package cryptostore

// factory expone el constructor PÚBLICO del store cifrado para los consumidores fuera del
// paquete (el adaptador whatsmeow de pairing). El decorator interno (cryptoContainer /
// cryptoStore) sigue siendo privado: lo único que cruza la frontera del paquete es
// store.DeviceContainer (interfaz upstream de whatsmeow) y un *store.Device ya cableado.
//
// Diseño: la DEK (32 bytes, AES-256) se inyecta por construcción y NUNCA se guarda en claro;
// el Envelope la mantiene dentro del AEAD. El llamante genera la DEK con CSPRNG, construye el
// container, ejecuta el pairing y borra la DEK de RAM al sellarla.
//
// Copia-adaptación de edugo-api-messaging/internal/infra/cryptostore (ADR-0004): renombrado al
// namespace wApp, envelope -> wapp-shared/envelope, dialecto SQLite en vez de PostgreSQL.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-shared/envelope"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
)

// Dialectos SQL del store cifrado, re-exportados desde internal/infra/db (Plan 022 T0) para que los
// consumidores del paquete (adaptador whatsmeow) pasen el dialecto sin importar el paquete db solo por
// la constante. Hoy el store del Edge es SQLite embebido (ADR-0002); Postgres queda disponible para la
// BD única dialecto-aware que cablea T1.
const (
	DialectSQLite   = wappdb.DialectSQLite
	DialectPostgres = wappdb.DialectPostgres
)

// NewEncryptedContainer construye un store.DeviceContainer que cifra TODO el material sensible
// de whatsmeow (claves del Device + stores de sesión) con AES-256-GCM bajo la DEK dada, sobre
// una BD YA migrada (las tablas whatsmeow_* las crea sqlstore.Upgrade aquí; las msg_enc_* las
// crea internal/infra/db).
//
// dek DEBE medir exactamente 32 bytes (envelope.DEKSize); en caso contrario devuelve error sin
// tocar la BD. El container resultante se pasa a NewDeviceForPairing (o a whatsmeow vía el
// Device) para que el pairing persista cifrado.
//
// dialect (Plan 022 T0) es el motor con el que se abrió la BD (DialectSQLite|DialectPostgres): se pasa
// tal cual a whatsmeow (sqlstore) para que emita el SQL correcto. Ya NO se asume "sqlite".
func NewEncryptedContainer(ctx context.Context, db *sql.DB, dialect string, dek []byte) (store.DeviceContainer, error) {
	env, err := envelope.NewEnvelope(dek)
	if err != nil {
		return nil, fmt.Errorf("cryptostore: construir envelope con la DEK: %w", err)
	}
	c, err := newCryptoContainer(ctx, db, dialect, env)
	if err != nil {
		return nil, fmt.Errorf("cryptostore: construir container cifrado: %w", err)
	}
	return c, nil
}

// NewDeviceForPairing fabrica un *store.Device NUEVO (claves frescas, sin JID) cuyo Container
// apunta al container cifrado dado. Es el device que se entrega a whatsmeow.NewClient para
// arrancar un pairing por QR.
//
// Sequencing (verificado contra whatsmeow pair.go): el Device arranca con Initialized=false y
// los stores de sesión en nil. whatsmeow, al completar el pairing, fija el JID y llama
// Device.Save -> cryptoContainer.PutDevice, que en la PRIMERA escritura ejecuta wrapStores:
// reinyecta los stores de sesión CIFRADOS y reapunta Device.Container al decorator. Recién
// DESPUÉS whatsmeow toca Identities/PreKeys, ya cifrados. Por eso un Device pre-pairing con
// stores nil es seguro: la primera operación es siempre Save.
func NewDeviceForPairing(container store.DeviceContainer) *store.Device {
	c, ok := container.(*cryptoContainer)
	if !ok {
		// Defensa: este paquete solo produce *cryptoContainer; un container ajeno no podría
		// cablear los stores cifrados. Construir un Device "plano" sería un fallo silencioso de
		// cifrado, así que paniqueamos en vez de devolver algo sin cifrar.
		panic("cryptostore: NewDeviceForPairing requiere un container creado por NewEncryptedContainer")
	}
	return c.newDevice()
}

// LoadDevice carga el *store.Device EXISTENTE de la sesión `jid` desde el container cifrado,
// descifrando su material (NoiseKey/IdentityKey/SignedPreKey/AdvSecretKey/Account) con la DEK del
// container y rehidratando los stores de sesión cifrados (vía wrapStores). Es la contraparte de
// NewDeviceForPairing para reconstruir un device pareado y conectar como esa sesión.
//
// Devuelve (nil, nil) si no hay device para ese jid (sesión no pareada / store vacío). GetDevice
// vive en el *cryptoContainer concreto (no en la interfaz store.DeviceContainer, que solo expone
// PutDevice/DeleteDevice), por eso se type-assertea aquí, igual que NewDeviceForPairing.
func LoadDevice(ctx context.Context, container store.DeviceContainer, jid types.JID) (*store.Device, error) {
	c, ok := container.(*cryptoContainer)
	if !ok {
		panic("cryptostore: LoadDevice requiere un container creado por NewEncryptedContainer")
	}
	return c.GetDevice(ctx, jid)
}

// DeleteDevice borra TODO el material local de la sesión `jid` del store: la fila del device cifrado
// (msg_enc_device) y el material de sesión por JID (identities/sessions/prekeys/sender_keys), MÁS las
// filas whatsmeow_* NO sensibles vía el Container nativo (que conoce el esquema y sus cascadas). Es la
// limpieza local de una desvinculación (DELETE /v1/sessions/{id}, ADR-0015).
//
// NO requiere la DEK: solo borra filas/ciphertext, no DESCIFRA nada. Por eso no toca la custodia ni
// viola zero-knowledge (la DEK la limpia aparte el caso de uso). Idempotente: borrar una sesión ausente
// no es error (los DELETE ... WHERE no afectan filas y no fallan).
//
// SINGLE-SESIÓN (verdad de campo 2026-06-27): hoy el Edge custodia una sola sesión; este borrado por
// JID es correcto y forward-compatible para el multi-sesión per-JID que resolverá MP-01 (no asume que
// haya una sola, borra exactamente la del JID dado).
func DeleteDevice(ctx context.Context, db *sql.DB, dialect string, jid types.JID) error {
	// 1. whatsmeow_* (no sensible): delega al Container nativo. Upgrade es idempotente (crea las tablas
	//    whatsmeow_* si la BD nunca pareó); DeleteDevice solo necesita el JID (DELETE ... WHERE jid=?).
	//    dialect (Plan 022 T0): el mismo motor con el que se abrió la BD; ya NO se asume "sqlite".
	inner := sqlstore.NewWithDB(db, dialect, nil)
	if err := inner.Upgrade(ctx); err != nil {
		return fmt.Errorf("cryptostore: upgrade esquema whatsmeow para borrado: %w", err)
	}
	jidCopy := jid
	if err := inner.DeleteDevice(ctx, &store.Device{ID: &jidCopy}); err != nil {
		return fmt.Errorf("cryptostore: borrar device whatsmeow: %w", err)
	}

	// 2. msg_enc_* (cifrado, esquema propio): el decorator no expone un borrado por JID, así que se
	//    eliminan las filas a mano. Sentencias explícitas (no query dinámica) por claridad y para no
	//    construir SQL por concatenación.
	stmts := []string{
		`DELETE FROM msg_enc_device      WHERE jid=?`,
		`DELETE FROM msg_enc_identities  WHERE our_jid=?`,
		`DELETE FROM msg_enc_sessions    WHERE our_jid=?`,
		`DELETE FROM msg_enc_prekeys     WHERE jid=?`,
		`DELETE FROM msg_enc_sender_keys WHERE our_jid=?`,
	}
	j := jid.String()
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt, j); err != nil {
			return fmt.Errorf("cryptostore: borrar material cifrado de la sesión: %w", err)
		}
	}
	return nil
}

// FirstDeviceJID devuelve el JID del único device pareado en el store cifrado (tabla
// msg_enc_device). El Edge del spike custodia UNA sola sesión; este helper la resuelve sin que el
// llamante (el sender) tenga que conocer el JID de antemano, que tampoco se persiste fuera del
// store. Es la contraparte de LoadDevice para el caso de envío: primero resolver el JID, luego
// cargar el device.
//
// Consulta solo la columna `jid` (NO material sensible: no descifra nada). Devuelve error si no hay
// ninguna sesión pareada (envío imposible) o si el JID persistido no parsea.
func FirstDeviceJID(ctx context.Context, db *sql.DB) (types.JID, error) {
	var raw string
	err := db.QueryRowContext(ctx, `SELECT jid FROM msg_enc_device LIMIT 1`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return types.JID{}, fmt.Errorf("cryptostore: no hay device pareado en el store")
	}
	if err != nil {
		return types.JID{}, fmt.Errorf("cryptostore: leer device pareado: %w", err)
	}
	jid, err := types.ParseJID(raw)
	if err != nil {
		return types.JID{}, fmt.Errorf("cryptostore: JID pareado inválido: %w", err)
	}
	return jid, nil
}
