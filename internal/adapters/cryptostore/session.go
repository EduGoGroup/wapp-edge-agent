package cryptostore

// session.go aporta el helper de apertura del store cifrado sobre la BD ÚNICA COMPARTIDA
// (OpenDeviceContainer, Plan 022 §3/§10.B, decisión A): N dispositivos comparten una sola
// *sql.DB y el runtime construye N Containers, UNO por dispositivo, cada uno enlazado en
// construcción con SU envelope (SU DEK). CERO DEK global.
//
// El modelo por-fichero que convivía aquí (OpenSessionContainer: un .db POR SESIÓN, legacy de
// ADR-0016 §2/§4 y Plan 008 §4, marcado TODO(T3)) se RETIRÓ el 2026-08-12: T3 conmutó el runtime
// a la BD única y desde entonces ningún camino de producción lo llamaba —solo su propio test—.
// El aislamiento por DEK que aquel test verificaba lo cubre isolation_test.go sobre el modelo
// vigente. Si alguna vez hace falta de vuelta, está en la historia de git (ver
// docs/archive/codigo-extirpado.md).

import (
	"context"
	"database/sql"

	"go.mau.fi/whatsmeow/store"
)

// OpenDeviceContainer construye el Container cifrado de UN dispositivo sobre una BD YA ABIERTA y
// COMPARTIDA (la BD única de T1), con la DEK de ESE dispositivo.
//
// Modelo (Plan 022 §3/§10.B, decisión A): N dispositivos comparten UNA sola *sql.DB; el runtime construye
// N de estos Containers, UNO por dispositivo, cada uno enlazado en construcción con SU envelope (SU DEK).
// CERO DEK global: ningún Container cifra material de más de un dispositivo, y la DEK de uno NO puede
// descifrar las filas (msg_enc_*, llaveadas por JID/our_jid) de otro — GCM no autentica (ver
// isolation_test.go). Reusa verbatim wrapStores/newCryptoStore: la topología compartida NO cambia la cripto.
//
// NO abre ni cierra la db: el Manager (T3) POSEE el ciclo de vida de la BD única compartida
// (design §10.I). dek DEBE medir 32 bytes (envelope.DEKSize); lo valida NewEncryptedContainer,
// sobre el que este helper es una fachada intención-revelante.
func OpenDeviceContainer(ctx context.Context, db *sql.DB, dialect string, dek []byte) (store.DeviceContainer, error) {
	return NewEncryptedContainer(ctx, db, dialect, dek)
}
