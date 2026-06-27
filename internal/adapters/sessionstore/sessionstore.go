// Package sessionstore persiste los METADATOS DE NEGOCIO de las sesiones del Edge (tabla `sessions`,
// T6.1) y resuelve el device pareado del store cifrado para el backfill del arranque.
//
// A diferencia de cryptostore (que cifra campo a campo el material whatsmeow con la DEK), aquí todo
// va EN CLARO: jid, estado y timestamps son metadatos de negocio, no secretos (CLAUDE.md raíz §1: el
// zero-knowledge protege credenciales/llaves, no el contenido de negocio). El material cripto sigue
// exclusivamente en las tablas msg_enc_*.
package sessionstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.mau.fi/whatsmeow/types"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cryptostore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// Store implementa app.SessionStore sobre la tabla `sessions` (SQLite, en claro). La BD debe estar
// YA migrada (db.Migrate aplica 0002_sessions.sql).
type Store struct {
	db *sql.DB
}

var _ app.SessionStore = (*Store)(nil)

// New construye el Store sobre la BD propia del Edge (la misma que el cryptostore; un único .db).
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// Upsert inserta o actualiza la sesión por su JID (clave primaria). Idempotente: re-upsertar el
// mismo JID actualiza estado y timestamps sin crear filas duplicadas. Los time.Time se persisten
// como epoch-segundos (INTEGER), como el resto del store.
func (s *Store) Upsert(ctx context.Context, sess domain.Session) error {
	if sess.JID == "" {
		return fmt.Errorf("sessionstore: JID vacío")
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO sessions (jid, state, paired_at, updated_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(jid) DO UPDATE SET state = excluded.state, updated_at = excluded.updated_at`,
		sess.JID, string(sess.State), unix(sess.PairedAt), unix(sess.UpdatedAt))
	if err != nil {
		return fmt.Errorf("sessionstore: upsert de sesión: %w", err)
	}
	return nil
}

// List devuelve todas las sesiones persistidas, ordenadas por paired_at ascendente (determinista).
func (s *Store) List(ctx context.Context) ([]domain.Session, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT jid, state, paired_at, updated_at FROM sessions ORDER BY paired_at, jid`)
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

// Get devuelve la sesión con ese JID, o app.ErrSessionNotFound si no existe.
func (s *Store) Get(ctx context.Context, jid string) (domain.Session, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT jid, state, paired_at, updated_at FROM sessions WHERE jid = ?`, jid)
	sess, err := scan(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Session{}, app.ErrSessionNotFound
	}
	if err != nil {
		return domain.Session{}, err
	}
	return sess, nil
}

// Delete elimina la fila de negocio de la sesión con ese JID. Idempotente: borrar un JID ausente NO es
// error (DELETE ... WHERE no afecta filas). Es la parte de metadatos de la desvinculación (el material
// cripto lo borra cryptostore.DeleteDevice y la DEK la limpia la custodia; ver app.UnlinkSession).
func (s *Store) Delete(ctx context.Context, jid string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE jid = ?`, jid); err != nil {
		return fmt.Errorf("sessionstore: borrar sesión: %w", err)
	}
	return nil
}

// scanner abstrae *sql.Row y *sql.Rows (ambos exponen Scan) para compartir el mapeo fila->Session.
type scanner interface {
	Scan(dest ...any) error
}

// scan mapea una fila (jid, state, paired_at, updated_at) a domain.Session, convirtiendo los epoch
// a time.Time.
func scan(sc scanner) (domain.Session, error) {
	var (
		jid, state          string
		pairedAt, updatedAt int64
	)
	if err := sc.Scan(&jid, &state, &pairedAt, &updatedAt); err != nil {
		return domain.Session{}, err
	}
	return domain.Session{
		JID:       jid,
		State:     domain.SessionState(state),
		PairedAt:  fromUnix(pairedAt),
		UpdatedAt: fromUnix(updatedAt),
	}, nil
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

// Locator implementa app.PairedDeviceLocator: resuelve el JID del device pareado en el store CIFRADO
// (msg_enc_device) SIN descifrar material, envolviendo cryptostore.FirstDeviceJID. Lo usa
// RestoreSessions para backfillear el registro cuando la BD fue pareada antes de existir `sessions`.
type Locator struct {
	db *sql.DB
}

var _ app.PairedDeviceLocator = (*Locator)(nil)

// NewLocator construye el locator sobre la BD propia del Edge.
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

// DeviceEraser implementa app.DeviceEraser: borra el material cripto local de la sesión `jid` del store
// cifrado, envolviendo cryptostore.DeleteDevice. SIN la DEK (solo borra filas/ciphertext, no descifra),
// igual que el Locator resuelve el JID sin descifrar. Lo usa app.UnlinkSession (DELETE /v1/sessions/{id}).
type DeviceEraser struct {
	db *sql.DB
}

var _ app.DeviceEraser = (*DeviceEraser)(nil)

// NewDeviceEraser construye el eraser sobre la BD propia del Edge.
func NewDeviceEraser(db *sql.DB) *DeviceEraser {
	return &DeviceEraser{db: db}
}

// DeleteDevice parsea el JID y delega en cryptostore.DeleteDevice (idempotente). Un JID inválido es un
// error de entrada (no del store): se reporta sin tocar la BD.
func (e *DeviceEraser) DeleteDevice(ctx context.Context, jid string) error {
	j, err := types.ParseJID(jid)
	if err != nil {
		return fmt.Errorf("sessionstore: JID inválido para borrado de device: %w", err)
	}
	return cryptostore.DeleteDevice(ctx, e.db, j)
}
