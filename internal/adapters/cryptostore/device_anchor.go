package cryptostore

// device_anchor.go crea el ANCLA DE INTEGRIDAD REFERENCIAL del device propio en `whatsmeow_device`.
//
// EL BUG QUE ARREGLA (verdad de campo, e2e 2026-08-06): 3.866 errores
// `FOREIGN KEY constraint failed (787)` en 78 minutos —el 89% del log del Edge— repartidos en
// `Failed to save push name` (1.970), `Failed to store message secret key` (1.782),
// `business name` (102) y `app state sync key` (12), sobre 91 contactos distintos.
//
// LA CADENA CAUSAL:
//  1. La BD del Edge abre con `PRAGMA foreign_keys=ON` en cada conexión (internal/infra/db/db.go).
//  2. 12 tablas nativas de whatsmeow declaran `FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid)`
//     (upstream store/sqlstore/upgrades/00-latest-schema.sql).
//  3. El device propio de wApp vive CIFRADO en `msg_enc_device` (esquema propio, BLOB libre) porque las
//     columnas de clave de `whatsmeow_device` llevan CHECK length=32/64 y NO admiten el ciphertext GCM
//     (+28B) — demostrado en schema_reject_test.go. `cryptoContainer.PutDevice` nunca escribió en
//     `whatsmeow_device`, así que esa tabla quedaba VACÍA y TODA FK contra ella fallaba.
//  4. `cryptoStore` solo sobreescribe Identity/Session/PreKey/SenderKey; los otros ~44 métodos se
//     HEREDAN del `*sqlstore.SQLStore` nativo y escriben en tablas `whatsmeow_*` ⇒ 787 sin fin (la caché
//     de contactos de upstream se actualiza DESPUÉS del Exec: al fallar conserva el valor viejo y
//     reintenta con CADA mensaje entrante, para siempre).
//
// LA CONSECUENCIA MAYOR no eran las líneas de log: con la FK rota, el APP-STATE nunca pudo sincronizar
// (whatsmeow_app_state_version / _sync_keys / whatsmeow_chat_settings / _nct_salt / buffers de evento y
// retry se rompían calladas). El push name del device no "llegaba tarde": no se podía escribir.
//
// LA SOLUCIÓN: una fila ANCLA en `whatsmeow_device` con el JID real y TODO lo demás INERTE. Es el único
// requisito que las FK imponen (existencia de la fila padre por `jid`); nada lee esa fila en el Edge
// (GetDevice/LoadDevice leen SIEMPRE de `msg_enc_device`, ver container.go y factory.go).
//
// 🔴 ZERO-KNOWLEDGE (ADR-0007) — INVARIANTE DE ESTE FICHERO: el ancla NO contiene material
// criptográfico. Las columnas de clave `NOT NULL` con CHECK de longitud se rellenan con CEROS del largo
// exigido (32/64 B). No hay noise key, ni identity key, ni signed pre-key, ni firmas ADV: quien lea el
// .db sin la DEK obtiene ceros, exactamente lo mismo que obtenía cuando la tabla estaba vacía. La fila
// es un ANCLA DE INTEGRIDAD REFERENCIAL, no un device. `platform` lleva el marcador anchorPlatform para
// que sea evidente en una inspección forense que esta fila no describe un dispositivo real.

import (
	"context"
	"database/sql"
	"fmt"

	"go.mau.fi/whatsmeow/types"
)

// anchorPlatform marca la fila ancla en la columna libre `platform` (TEXT NOT NULL, default cadena vacía).
// Es un marcador FORENSE, no funcional: el Edge nunca lee `whatsmeow_device` (el device sale de
// `msg_enc_device`), así que este valor jamás llega a un *store.Device vivo.
const anchorPlatform = "wapp-fk-anchor"

// Longitudes que exigen los CHECK de whatsmeow_device (00-latest-schema.sql:10-21):
// noise_key/identity_key/signed_pre_key/adv_account_sig_key = 32 B; signed_pre_key_sig/
// adv_account_sig/adv_device_sig = 64 B. adv_key y adv_details son NOT NULL SIN CHECK de longitud.
const (
	anchorKeyLen = 32
	anchorSigLen = 64
)

// ensureDeviceAnchor garantiza (idempotente) la fila ancla de `jid` en `whatsmeow_device`, para que las
// FK de las 12 tablas nativas resuelvan. `ON CONFLICT (jid) DO NOTHING` —no `INSERT OR IGNORE`— para NO
// enmascarar otros errores de restricción (un CHECK violado debe reventar, no callarse) y para respetar
// una fila preexistente sin pisarla.
//
// Sirve a la vez de MIGRACIÓN para las BD que ya existen en máquinas reales: no puede ser un .sql
// numerado porque (a) el runner de internal/infra/db aplica el set "store" ANTES de que
// sqlstore.Upgrade cree las tablas whatsmeow_* (en una BD nueva ni siquiera existen al migrar) y
// (b) el JID no se conoce en tiempo de migración, solo al cargar/parear el device. Por eso el ancla se
// asegura en wrapStores, el ÚNICO punto por el que pasan los dos caminos que producen un Device usable
// (PutDevice en el pairing nuevo, GetDevice en cada arranque): una BD que lleva meses con
// `whatsmeow_device` vacía se cura sola en el siguiente arranque, sin re-escanear ni re-emparejar.
//
// Dialecto: SQL con placeholders `?`, como el resto del paquete (PutDevice usa INSERT OR REPLACE).
// El store del Edge es SQLite embebido (ADR-0002).
func ensureDeviceAnchor(ctx context.Context, db *sql.DB, jid types.JID) error {
	// Relleno INERTE (ver invariante zero-knowledge arriba): ceros del largo que exige cada CHECK.
	inertKey := make([]byte, anchorKeyLen)
	inertSig := make([]byte, anchorSigLen)
	_, err := db.ExecContext(ctx, `
		INSERT INTO whatsmeow_device
			(jid, registration_id, noise_key, identity_key,
			 signed_pre_key, signed_pre_key_id, signed_pre_key_sig,
			 adv_key, adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig,
			 platform)
		VALUES (?,0,?,?,?,0,?,?,?,?,?,?,?)
		ON CONFLICT (jid) DO NOTHING`,
		jid.String(), inertKey, inertKey,
		inertKey, inertSig,
		inertKey, inertKey, inertSig, inertKey, inertSig,
		anchorPlatform,
	)
	if err != nil {
		return fmt.Errorf("cryptostore: crear ancla de integridad en whatsmeow_device: %w", err)
	}
	return nil
}

// deleteDeviceAnchor borra el ancla de `jid`. Las 12 tablas nativas declaran la FK con
// `ON DELETE CASCADE`, así que este DELETE arrastra consigo contactos, chat settings, app-state,
// message secrets, privacy tokens, nct salt y los buffers de ESE device: la desvinculación deja la BD
// limpia en vez de dejar material del emparejamiento anterior colgando de un JID reutilizado.
func deleteDeviceAnchor(ctx context.Context, db *sql.DB, jid types.JID) error {
	if _, err := db.ExecContext(ctx, `DELETE FROM whatsmeow_device WHERE jid=?`, jid.String()); err != nil {
		return fmt.Errorf("cryptostore: borrar ancla de integridad de whatsmeow_device: %w", err)
	}
	return nil
}
