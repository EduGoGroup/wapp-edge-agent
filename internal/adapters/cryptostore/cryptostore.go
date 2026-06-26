package cryptostore

// cryptostore: decorator que cifra los blobs sensibles del store de whatsmeow con
// AES-256-GCM (DEK inyectada por construcción), usando un ESQUEMA PROPIO (tablas msg_enc_*
// con BLOB libre) para las columnas cuyo CHECK de longitud del esquema upstream NO admite
// el ciphertext (nonce 12B + datos + tag 16B = +28B).
//
// Patrón: embebe un *sqlstore.SQLStore real para HEREDAR todos los métodos de
// store.AllSessionSpecificStores (contacts, chat settings, privacy tokens, app-state, etc.)
// y SOBREESCRIBE solo los que tocan material criptográfico sensible:
//   - IdentityStore  (identidades, [32]byte)
//   - SessionStore   (sesiones Signal, []byte de tamaño variable)
//   - PreKeyStore    (prekeys, privada de 32B + firma)
//   - SenderKeyStore (sender keys de grupo, []byte)
//
// Copia-adaptación de edugo-api-messaging (ADR-0004): dialecto SQLite (placeholders `?`,
// INSERT OR REPLACE, IN dinámico en vez de pq.Array/ANY), envelope de wapp-shared.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/EduGoGroup/wapp-shared/envelope"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
)

// cryptoStore implementa store.AllSessionSpecificStores cifrando los blobs sensibles.
type cryptoStore struct {
	*sqlstore.SQLStore // embebido: aporta los métodos no sensibles + la conexión y JID
	env                *envelope.Envelope
	db                 *sql.DB // misma BD; usado por las tablas msg_enc_* propias
	jid                string
}

// Compila-time: el decorator sigue cumpliendo la interfaz completa.
var _ store.AllSessionSpecificStores = (*cryptoStore)(nil)

// newCryptoStore envuelve un SQLStore real con cifrado para el JID dado.
func newCryptoStore(inner *sqlstore.SQLStore, raw *sql.DB, env *envelope.Envelope, jid types.JID) *cryptoStore {
	return &cryptoStore{
		SQLStore: inner,
		env:      env,
		db:       raw,
		jid:      jid.String(),
	}
}

// boolToInt traduce un bool de Go al 0/1 que guarda la columna INTEGER `uploaded` en SQLite
// (no hay tipo BOOLEAN nativo; se porta a INTEGER).
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// El esquema de las tablas msg_enc_* lo crea el runner de migración (internal/infra/db), no
// este decorator: el cryptostore asume las tablas ya creadas y solo lee/escribe ciphertext.

// ----------------------------------------------------------------------------
// IdentityStore (override) — la columna upstream identity tiene CHECK length=32.
// ----------------------------------------------------------------------------

func (c *cryptoStore) PutIdentity(ctx context.Context, address string, key [32]byte) error {
	ct, err := c.env.Seal(key[:])
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO msg_enc_identities (our_jid, their_id, identity) VALUES (?,?,?)`,
		c.jid, address, ct)
	return err
}

func (c *cryptoStore) IsTrustedIdentity(ctx context.Context, address string, key [32]byte) (bool, error) {
	var ct []byte
	err := c.db.QueryRowContext(ctx,
		`SELECT identity FROM msg_enc_identities WHERE our_jid=? AND their_id=?`,
		c.jid, address).Scan(&ct)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil // desconocida = confiable (se guardará luego), igual que upstream
	} else if err != nil {
		return false, err
	}
	pt, err := c.env.Open(ct)
	if err != nil {
		return false, err
	}
	if len(pt) != 32 {
		return false, fmt.Errorf("identidad descifrada con longitud inesperada %d", len(pt))
	}
	return *(*[32]byte)(pt) == key, nil
}

func (c *cryptoStore) DeleteAllIdentities(ctx context.Context, phone string) error {
	_, err := c.db.ExecContext(ctx,
		`DELETE FROM msg_enc_identities WHERE our_jid=? AND their_id LIKE ?`, c.jid, phone+":%")
	return err
}

func (c *cryptoStore) DeleteIdentity(ctx context.Context, address string) error {
	_, err := c.db.ExecContext(ctx,
		`DELETE FROM msg_enc_identities WHERE our_jid=? AND their_id=?`, c.jid, address)
	return err
}

// ----------------------------------------------------------------------------
// SessionStore (override) — session es bytea libre upstream, pero lo ciframos igual.
// ----------------------------------------------------------------------------

func (c *cryptoStore) PutSession(ctx context.Context, address string, session []byte) error {
	ct, err := c.env.Seal(session)
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO msg_enc_sessions (our_jid, their_id, session) VALUES (?,?,?)`,
		c.jid, address, ct)
	return err
}

func (c *cryptoStore) GetSession(ctx context.Context, address string) ([]byte, error) {
	var ct []byte
	err := c.db.QueryRowContext(ctx,
		`SELECT session FROM msg_enc_sessions WHERE our_jid=? AND their_id=?`,
		c.jid, address).Scan(&ct)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return c.env.Open(ct)
}

func (c *cryptoStore) HasSession(ctx context.Context, address string) (bool, error) {
	var has bool
	err := c.db.QueryRowContext(ctx,
		`SELECT true FROM msg_enc_sessions WHERE our_jid=? AND their_id=?`,
		c.jid, address).Scan(&has)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return has, err
}

func (c *cryptoStore) GetManySessions(ctx context.Context, addresses []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(addresses))
	for _, addr := range addresses {
		out[addr] = nil // contrato upstream: claves presentes aunque sin sesión
	}
	if len(addresses) == 0 {
		return nil, nil
	}
	// SQLite no tiene array params (pq.Array/ANY de PostgreSQL): construimos un IN dinámico
	// con un placeholder `?` por dirección.
	placeholders := make([]string, len(addresses))
	args := make([]any, 0, len(addresses)+1)
	args = append(args, c.jid)
	for i, addr := range addresses {
		placeholders[i] = "?"
		args = append(args, addr)
	}
	query := `SELECT their_id, session FROM msg_enc_sessions WHERE our_jid=? AND their_id IN (` +
		strings.Join(placeholders, ",") + `)`
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var addr string
		var ct []byte
		if err := rows.Scan(&addr, &ct); err != nil {
			return nil, err
		}
		pt, err := c.env.Open(ct)
		if err != nil {
			return nil, err
		}
		out[addr] = pt
	}
	return out, rows.Err()
}

func (c *cryptoStore) PutManySessions(ctx context.Context, sessions map[string][]byte) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for addr, sess := range sessions {
		ct, err := c.env.Seal(sess)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO msg_enc_sessions (our_jid, their_id, session) VALUES (?,?,?)`,
			c.jid, addr, ct); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (c *cryptoStore) DeleteAllSessions(ctx context.Context, phone string) error {
	_, err := c.db.ExecContext(ctx,
		`DELETE FROM msg_enc_sessions WHERE our_jid=? AND their_id LIKE ?`, c.jid, phone+":%")
	return err
}

func (c *cryptoStore) DeleteSession(ctx context.Context, address string) error {
	_, err := c.db.ExecContext(ctx,
		`DELETE FROM msg_enc_sessions WHERE our_jid=? AND their_id=?`, c.jid, address)
	return err
}

// MigratePNToLID: para el spike no se ejercita migración LID; no-op seguro.
func (c *cryptoStore) MigratePNToLID(ctx context.Context, pn, lid types.JID) error {
	return nil
}

// ----------------------------------------------------------------------------
// PreKeyStore (override) — key es bytea CHECK length=32 upstream.
// ----------------------------------------------------------------------------

func (c *cryptoStore) getNextPreKeyID(ctx context.Context) (uint32, error) {
	var last sql.NullInt32
	err := c.db.QueryRowContext(ctx,
		`SELECT MAX(key_id) FROM msg_enc_prekeys WHERE jid=?`, c.jid).Scan(&last)
	if err != nil {
		return 0, err
	}
	return uint32(last.Int32) + 1, nil
}

func (c *cryptoStore) genOnePreKey(ctx context.Context, id uint32, uploaded bool) (*keys.PreKey, error) {
	key := keys.NewPreKey(id)
	ct, err := c.env.Seal(key.Priv[:])
	if err != nil {
		return nil, err
	}
	_, err = c.db.ExecContext(ctx,
		`INSERT INTO msg_enc_prekeys (jid, key_id, priv, uploaded) VALUES (?,?,?,?)`,
		c.jid, key.KeyID, ct, boolToInt(uploaded))
	return key, err
}

func (c *cryptoStore) GenOnePreKey(ctx context.Context) (*keys.PreKey, error) {
	id, err := c.getNextPreKeyID(ctx)
	if err != nil {
		return nil, err
	}
	return c.genOnePreKey(ctx, id, true)
}

func (c *cryptoStore) GetOrGenPreKeys(ctx context.Context, count uint32) ([]*keys.PreKey, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT key_id, priv FROM msg_enc_prekeys WHERE jid=? AND uploaded=0 ORDER BY key_id LIMIT ?`,
		c.jid, count)
	if err != nil {
		return nil, err
	}
	var existing []*keys.PreKey
	for rows.Next() {
		var id uint32
		var ct []byte
		if err := rows.Scan(&id, &ct); err != nil {
			_ = rows.Close()
			return nil, err
		}
		pt, err := c.env.Open(ct)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		existing = append(existing, &keys.PreKey{
			KeyPair: *keys.NewKeyPairFromPrivateKey(*(*[32]byte)(pt)),
			KeyID:   id,
		})
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	already := uint32(len(existing))
	if count > already {
		next, err := c.getNextPreKeyID(ctx)
		if err != nil {
			return nil, err
		}
		existing = slices.Grow(existing, int(count)-len(existing))[:count]
		for i := already; i < count; i++ {
			existing[i], err = c.genOnePreKey(ctx, next, false)
			if err != nil {
				return nil, err
			}
			next++
		}
	}
	return existing, nil
}

func (c *cryptoStore) GetPreKey(ctx context.Context, id uint32) (*keys.PreKey, error) {
	var ct []byte
	err := c.db.QueryRowContext(ctx,
		`SELECT priv FROM msg_enc_prekeys WHERE jid=? AND key_id=?`, c.jid, id).Scan(&ct)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	pt, err := c.env.Open(ct)
	if err != nil {
		return nil, err
	}
	return &keys.PreKey{
		KeyPair: *keys.NewKeyPairFromPrivateKey(*(*[32]byte)(pt)),
		KeyID:   id,
	}, nil
}

func (c *cryptoStore) RemovePreKey(ctx context.Context, id uint32) error {
	_, err := c.db.ExecContext(ctx, `DELETE FROM msg_enc_prekeys WHERE jid=? AND key_id=?`, c.jid, id)
	return err
}

func (c *cryptoStore) MarkPreKeysAsUploaded(ctx context.Context, upToID uint32) error {
	_, err := c.db.ExecContext(ctx,
		`UPDATE msg_enc_prekeys SET uploaded=1 WHERE jid=? AND key_id<=?`, c.jid, upToID)
	return err
}

func (c *cryptoStore) UploadedPreKeyCount(ctx context.Context) (int, error) {
	var n int
	err := c.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM msg_enc_prekeys WHERE jid=? AND uploaded=1`, c.jid).Scan(&n)
	return n, err
}

// ----------------------------------------------------------------------------
// SenderKeyStore (override) — sender_key es bytea libre upstream; ciframos igual.
// ----------------------------------------------------------------------------

func (c *cryptoStore) PutSenderKey(ctx context.Context, group, user string, session []byte) error {
	ct, err := c.env.Seal(session)
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO msg_enc_sender_keys (our_jid, chat_id, sender_id, sender_key) VALUES (?,?,?,?)`,
		c.jid, group, user, ct)
	return err
}

func (c *cryptoStore) GetSenderKey(ctx context.Context, group, user string) ([]byte, error) {
	var ct []byte
	err := c.db.QueryRowContext(ctx,
		`SELECT sender_key FROM msg_enc_sender_keys WHERE our_jid=? AND chat_id=? AND sender_id=?`,
		c.jid, group, user).Scan(&ct)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return c.env.Open(ct)
}
