package cryptostore

// session.go aporta el helper de apertura del store cifrado POR SESIÓN (ADR-0016 §2/§4, Plan 008 §4):
// dado el path del store.db de una sesión y su DEK, abre/migra el archivo y devuelve el Container
// cifrado listo. Es la pieza que el Session Manager (T3/T4) usará para resolver, dado un session_id,
// su {store, container} AISLADO: N sesiones ⇒ N (db, Container), cada uno en SU archivo y cifrado con
// SU DEK. En T2 se deja la pieza + sus tests, sin cablear todavía Pair/Restore.

import (
	"context"
	"database/sql"
	"fmt"

	"go.mau.fi/whatsmeow/store"

	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
)

// OpenSessionContainer abre (creando) el store.db de UNA sesión en storePath, le aplica SOLO las
// migraciones del store cifrado (set "store": tablas msg_enc_*, vía db.OpenSessionStore) y construye
// el Container cifrado con la DEK dada. Las tablas whatsmeow_* no sensibles las crea el propio
// container (sqlstore.Upgrade), no el runner de migración.
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
	container, err := NewEncryptedContainer(ctx, sdb, dek)
	if err != nil {
		_ = sdb.Close()
		return nil, nil, err
	}
	return container, sdb, nil
}
