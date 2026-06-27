// Package sessionstore persiste los METADATOS DE NEGOCIO de las sesiones del Edge (tabla
// `sessions_v2`, ADR-0016/Plan 008) y resuelve el device pareado del store cifrado para el backfill
// del arranque.
//
// A diferencia de cryptostore (que cifra campo a campo el material whatsmeow con la DEK), aquí todo
// va EN CLARO: session_id, jid, estado, store_dir y timestamps son metadatos de negocio, no secretos
// (CLAUDE.md raíz §1: el zero-knowledge protege credenciales/llaves, no el contenido de negocio). El
// material cripto sigue exclusivamente en las tablas msg_enc_* (de un store.db POR SESIÓN, ADR-0016 §2).
//
// MULTI-SESIÓN (ADR-0016 §3): la clave es el `session_id` (UUIDv4 opaco), NO el JID. El JID es un
// atributo OPCIONAL (NULL mientras la sesión está en 'pairing'; ÚNICO cuando no es NULL, vía el índice
// parcial ux_sessions_jid). El esquema lo retira la 0003; la tabla `sessions` (0002, jid PK) queda
// vacía y sin uso.
package sessionstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cryptostore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// Store implementa app.SessionStore sobre la tabla `sessions_v2` (SQLite, en claro). La BD debe estar
// YA migrada (db.Migrate aplica 0003_sessions_multi.sql).
type Store struct {
	db *sql.DB
}

var _ app.SessionStore = (*Store)(nil)

// New construye el Store sobre la BD de metadatos del Edge.
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// Upsert inserta o actualiza la sesión por su `session_id` (clave primaria). Idempotente: re-upsertar
// el mismo session_id actualiza jid/estado/store_dir/timestamps sin crear filas duplicadas. El JID es
// opcional: vacío se persiste como NULL (sesión en 'pairing'); paired_at cero también como NULL. Los
// time.Time se persisten como epoch-segundos (INTEGER), como el resto del store.
func (s *Store) Upsert(ctx context.Context, sess domain.Session) error {
	if sess.SessionID == "" {
		return fmt.Errorf("sessionstore: session_id vacío")
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO sessions_v2 (session_id, jid, state, store_dir, paired_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(session_id) DO UPDATE SET
		   jid        = excluded.jid,
		   state      = excluded.state,
		   store_dir  = excluded.store_dir,
		   paired_at  = excluded.paired_at,
		   updated_at = excluded.updated_at`,
		sess.SessionID, nullableJID(sess.JID), string(sess.State), sess.StoreDir,
		nullableUnix(sess.PairedAt), unix(sess.UpdatedAt))
	if err != nil {
		return fmt.Errorf("sessionstore: upsert de sesión: %w", err)
	}
	return nil
}

// List devuelve TODAS las sesiones persistidas, ordenadas de forma determinista (updated_at, luego
// session_id). Incluye las de cualquier estado (pairing/active/loggedout).
func (s *Store) List(ctx context.Context) ([]domain.Session, error) {
	return s.query(ctx,
		`SELECT session_id, jid, state, store_dir, paired_at, updated_at
		   FROM sessions_v2 ORDER BY updated_at, session_id`)
}

// ListActive devuelve solo las sesiones en estado 'active' (las que el arranque debe restaurar,
// design §6/T4). Mismo orden determinista que List.
func (s *Store) ListActive(ctx context.Context) ([]domain.Session, error) {
	return s.query(ctx,
		`SELECT session_id, jid, state, store_dir, paired_at, updated_at
		   FROM sessions_v2 WHERE state = ? ORDER BY updated_at, session_id`,
		string(domain.SessionStateActive))
}

// query ejecuta un SELECT que proyecta las columnas de sessions_v2 y materializa las filas a
// domain.Session (compartido por List/ListActive).
func (s *Store) query(ctx context.Context, q string, args ...any) ([]domain.Session, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sessionstore: listar sesiones: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []domain.Session
	for rows.Next() {
		sess, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sess)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sessionstore: iterar sesiones: %w", err)
	}
	return out, nil
}

// Get devuelve la sesión con ese `session_id`, o app.ErrSessionNotFound si no existe.
func (s *Store) Get(ctx context.Context, sessionID string) (domain.Session, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT session_id, jid, state, store_dir, paired_at, updated_at
		   FROM sessions_v2 WHERE session_id = ?`, sessionID)
	sess, err := scan(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Session{}, app.ErrSessionNotFound
	}
	if err != nil {
		return domain.Session{}, err
	}
	return sess, nil
}

// Delete elimina la fila de negocio de la sesión con ese `session_id`. Idempotente: borrar un
// session_id ausente NO es error (DELETE ... WHERE no afecta filas). Es la parte de metadatos del
// borrado quirúrgico (design §7/T5); el material cripto y la DEK los limpia el Manager aparte.
func (s *Store) Delete(ctx context.Context, sessionID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM sessions_v2 WHERE session_id = ?`, sessionID); err != nil {
		return fmt.Errorf("sessionstore: borrar sesión: %w", err)
	}
	return nil
}

// scanner abstrae *sql.Row y *sql.Rows (ambos exponen Scan) para compartir el mapeo fila->Session.
type scanner interface {
	Scan(dest ...any) error
}

// scan mapea una fila (session_id, jid, state, store_dir, paired_at, updated_at) a domain.Session.
// jid y paired_at son NULLABLE: un NULL en jid se materializa como cadena vacía (sesión en pairing) y
// un NULL en paired_at como el cero de Go.
func scan(sc scanner) (domain.Session, error) {
	var (
		sessionID, state, storeDir string
		jid                        sql.NullString
		pairedAt                   sql.NullInt64
		updatedAt                  int64
	)
	if err := sc.Scan(&sessionID, &jid, &state, &storeDir, &pairedAt, &updatedAt); err != nil {
		return domain.Session{}, err
	}
	return domain.Session{
		SessionID: sessionID,
		JID:       jid.String, // "" si NULL
		State:     domain.SessionState(state),
		StoreDir:  storeDir,
		PairedAt:  fromNullableUnix(pairedAt),
		UpdatedAt: fromUnix(updatedAt),
	}, nil
}

// nullableJID convierte un JID a un valor SQL: NULL si está vacío (sesión en pairing), el JID si no.
func nullableJID(jid string) any {
	if jid == "" {
		return nil
	}
	return jid
}

// nullableUnix convierte un time.Time a un epoch-segundos SQL: NULL si es cero, el epoch si no.
func nullableUnix(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.Unix()
}

// unix convierte un time.Time a epoch-segundos; el cero de Go se persiste como 0 (no como negativo).
func unix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// fromUnix reconstruye un time.Time desde epoch-segundos; 0 -> cero de Go.
func fromUnix(sec int64) time.Time {
	if sec == 0 {
		return time.Time{}
	}
	return time.Unix(sec, 0).UTC()
}

// fromNullableUnix reconstruye un time.Time desde un epoch-segundos NULLABLE; NULL/0 -> cero de Go.
func fromNullableUnix(sec sql.NullInt64) time.Time {
	if !sec.Valid || sec.Int64 == 0 {
		return time.Time{}
	}
	return time.Unix(sec.Int64, 0).UTC()
}

// Locator implementa app.PairedDeviceLocator: resuelve el JID del device pareado en el store CIFRADO
// (msg_enc_device) SIN descifrar material, envolviendo cryptostore.FirstDeviceJID. Lo usa
// RestoreSessions para backfillear el registro cuando la BD fue pareada antes de existir `sessions_v2`.
type Locator struct {
	db *sql.DB
}

var _ app.PairedDeviceLocator = (*Locator)(nil)

// NewLocator construye el locator sobre la BD del store cifrado del Edge.
func NewLocator(db *sql.DB) *Locator {
	return &Locator{db: db}
}

// PairedJID devuelve el JID de la sesión pareada (ok=true) o ok=false si el store no tiene device.
func (l *Locator) PairedJID(ctx context.Context) (string, bool, error) {
	jid, err := cryptostore.FirstDeviceJID(ctx, l.db)
	if err != nil {
		// FirstDeviceJID devuelve error si no hay device pareado: lo traducimos a ok=false (no es un
		// fallo del store, es "nada que restaurar").
		return "", false, nil
	}
	return jid.String(), true, nil
}
