package cryptostore

// session.go aporta los helpers de apertura del store cifrado. Dos modelos conviven durante Plan 022:
//
//   - OpenDeviceContainer  (Plan 022 §3/§10.B, decisión A): variante sobre la BD ÚNICA COMPARTIDA. N
//     dispositivos comparten una sola *sql.DB; el runtime construye N Containers, UNO por dispositivo,
//     cada uno enlazado en construcción con SU envelope (SU DEK). CERO DEK global. Es el modelo objetivo
//     que T3 cablea (Pair/Restore/sender sobre la BD única).
//   - OpenSessionContainer (legacy, ADR-0016 §2/§4, Plan 008 §4 — TODO(T3)): un .db POR SESIÓN. Abre/migra
//     el archivo y devuelve su Container cifrado. Modelo por-fichero que T3 retira al conmutar a la BD
//     única. Se conserva para sus tests hasta que el runtime deje de usar el layout por-fichero.
//
// En ambos, el cifrado ya es POR DISPOSITIVO (envelope enlazado en construcción; reuso verbatim de
// wrapStores/newCryptoStore): la diferencia es SOLO la topología de la BD (una compartida vs. una por
// fichero), no la capa cripto.

import (
	"context"
	"database/sql"
	"fmt"

	"go.mau.fi/whatsmeow/store"

	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
)

// OpenDeviceContainer construye el Container cifrado de UN dispositivo sobre una BD YA ABIERTA y
// COMPARTIDA (la BD única de T1), con la DEK de ESE dispositivo. Es la variante sobre BD única compartida
// de OpenSessionContainer (que abre y POSEE un .db por-fichero, modelo legacy → TODO(T3)).
//
// Modelo (Plan 022 §3/§10.B, decisión A): N dispositivos comparten UNA sola *sql.DB; el runtime construye
// N de estos Containers, UNO por dispositivo, cada uno enlazado en construcción con SU envelope (SU DEK).
// CERO DEK global: ningún Container cifra material de más de un dispositivo, y la DEK de uno NO puede
// descifrar las filas (msg_enc_*, llaveadas por JID/our_jid) de otro — GCM no autentica (ver
// isolation_test.go). Reusa verbatim wrapStores/newCryptoStore: la topología compartida NO cambia la cripto.
//
// A diferencia de OpenSessionContainer, NO abre ni cierra la db: el Manager (T3) POSEE el ciclo de vida
// de la BD única compartida (design §10.I). dek DEBE medir 32 bytes (envelope.DEKSize); lo valida
// NewEncryptedContainer, sobre el que este helper es una fachada intención-revelante.
func OpenDeviceContainer(ctx context.Context, db *sql.DB, dialect string, dek []byte) (store.DeviceContainer, error) {
	return NewEncryptedContainer(ctx, db, dialect, dek)
}

// OpenSessionContainer abre (creando) el store.db de UNA sesión en storePath, le aplica SOLO las
// migraciones del store cifrado (set "store": tablas msg_enc_*, vía db.OpenSessionStore) y construye
// el Container cifrado con la DEK dada. Las tablas whatsmeow_* no sensibles las crea el propio
// container (sqlstore.Upgrade), no el runner de migración.
//
// LEGACY / TODO(T3): es el modelo por-fichero (un .db por sesión). El corte a BD única compartida (Plan
// 022 §10.A) lo retira: el runtime pasará a OpenDeviceContainer sobre la única db. Se conserva mientras
// sus tests (session_test.go) ejerciten el aislamiento por-fichero; ningún consumidor de runtime lo llama.
//
// Devuelve también la *sql.DB para que el LLAMANTE la cierre en el apagado ordenado (design §10.I): el
// Container la usa por dentro pero NO posee su ciclo de vida (un Manager con N sesiones cierra cada db
// al parar o al borrar la sesión). Si abrir/migrar o construir el container falla, cierra la db antes
// de devolver el error para no fugar el handle.
//
// dek DEBE medir 32 bytes (envelope.DEKSize); NewEncryptedContainer lo valida. No se loguea ni se
// retiene aquí: solo cruza hacia el envelope del container.
func OpenSessionContainer(ctx context.Context, storePath string, dek []byte) (store.DeviceContainer, *sql.DB, error) {
	sdb, err := wappdb.OpenSessionStore(ctx, storePath)
	if err != nil {
		return nil, nil, fmt.Errorf("cryptostore: abrir store de sesión %q: %w", storePath, err)
	}
	// El store por sesión es un fichero SQLite (ADR-0016 §4): OpenSessionStore abre en DialectSQLite, así
	// que el container cifrado usa el mismo motor. La conmutación a la BD única dialecto-aware la cablea T1.
	container, err := NewEncryptedContainer(ctx, sdb, DialectSQLite, dek)
	if err != nil {
		_ = sdb.Close()
		return nil, nil, err
	}
	return container, sdb, nil
}
