// Package sessionstore persiste los METADATOS DE NEGOCIO del Edge sobre la BD ÚNICA (Plan 022 T1,
// ADR-0018): el modelo CUENTA↔DISPOSITIVO (`accounts` ⨝ `devices`, que reemplaza a `sessions_v2`) y
// resuelve el device pareado del store cifrado para el backfill del arranque.
//
// A diferencia de cryptostore (que cifra campo a campo el material whatsmeow con la DEK), aquí todo va
// EN CLARO: session_id, account_id, self_pn, jid, estado, rol y timestamps son metadatos de negocio, no
// secretos (CLAUDE.md raíz §1: el zero-knowledge protege credenciales/llaves, no el contenido de negocio).
// El material cripto sigue exclusivamente en las tablas msg_enc_* (cifradas con la DEK POR DISPOSITIVO, T2).
//
// MODELO (ADR-0018 §2): la identidad canónica del DISPOSITIVO es `session_id` (UUIDv4 opaco); cada device
// cuelga de una CUENTA (número, `self_pn`). Un re-escaneo del mismo número es otro device de la MISMA
// cuenta (misma self_pn ⇒ mismo account_id). El JID es un atributo OPCIONAL (NULL mientras 'pairing';
// ÚNICO cuando no es NULL vía el índice parcial ux_devices_jid). El esquema lo crea la 0004.
//
// PUERTO ESTABLE + EXTRAS T3: el puerto app.SessionStore (Upsert/List/ListActive/Get/Delete) se conserva
// INTACTO para que pairing/listen/restore/unlink y los fakes compilen sin cambios. El campo
// domain.Session.StoreDir YA NO se deriva ni se persiste (Plan 022 T3, BD única: no hay directorio por
// sesión). Los métodos EXTRA sobre el tipo concreto —por CUENTA (GetByAccount/DeleteByAccount) y el borrado
// transaccional por dispositivo (DeleteDeviceCascade)— los usa el runtime T3 vía type-assert (no rompen los
// fakes de los tests). La unificación List/ListActive vive en el helper `list`.
package sessionstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cryptostore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// deviceSelect proyecta un DISPOSITIVO junto con los datos de su CUENTA (⨝ accounts). Compartido por
// List/ListActive/Get/GetByAccount; scan() materializa estas columnas (en este orden) a domain.Session.
const deviceSelect = `SELECT d.session_id, d.account_id, a.self_pn, a.display_name, d.jid, d.state, d.role,
	   d.paired_at, d.updated_at
	  FROM devices d JOIN accounts a ON a.account_id = d.account_id`

// Store implementa app.SessionStore sobre la BD ÚNICA (tablas `accounts`/`devices`, en claro). La BD debe
// estar YA migrada (db.MigrateMeta aplica 0004_accounts_devices.sql; foreign_keys=ON para la FK device→cuenta).
type Store struct {
	db *sql.DB
}

var _ app.SessionStore = (*Store)(nil)

// New construye el Store sobre la BD del Edge.
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// Upsert inserta o actualiza el DISPOSITIVO por su `session_id` (clave primaria), asegurando su CUENTA en
// la misma transacción (la FK devices.account_id→accounts exige la cuenta primero). Idempotente. El JID y
// paired_at son opcionales (vacío/cero ⇒ NULL). Resolución de cuenta (resolveAccount):
//   - AccountID explícito ⇒ se upsertea esa cuenta.
//   - self_pn presente ⇒ misma self_pn ⇒ MISMO account_id (re-escaneo cuelga de la cuenta existente).
//   - sin self_pn ni account_id (device en 'pairing') ⇒ cuenta PROVISIONAL por dispositivo (account_id =
//     session_id, self_pn NULL); T3 la re-vincula a la cuenta real al conocer el número.
func (s *Store) Upsert(ctx context.Context, sess domain.Session) error {
	if sess.SessionID == "" {
		return fmt.Errorf("sessionstore: session_id vacío")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sessionstore: abrir transacción de upsert: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op tras Commit; revierte cuenta+device si algo falla.

	// Cuenta ANTERIOR del device (si ya existía): al RE-VINCULAR por self_pn en PairSuccess (Plan 022 T3),
	// el device pasa de su cuenta PROVISIONAL (account_id=session_id) a la cuenta real (por self_pn); la
	// provisional queda vacía y hay que purgarla en la MISMA tx para no dejar cuentas huérfanas (§10.I).
	var prevAccountID string
	if err := tx.QueryRowContext(ctx,
		`SELECT account_id FROM devices WHERE session_id = ?`, sess.SessionID).Scan(&prevAccountID); err != nil &&
		!errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("sessionstore: leer cuenta previa del device: %w", err)
	}

	accountID, err := resolveAccount(ctx, tx, sess)
	if err != nil {
		return err
	}

	role := sess.Role
	if role == "" {
		role = domain.DeviceRolePrimary
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO devices (session_id, account_id, jid, state, role, paired_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(session_id) DO UPDATE SET
		   account_id = excluded.account_id,
		   jid        = excluded.jid,
		   state      = excluded.state,
		   role       = excluded.role,
		   paired_at  = excluded.paired_at,
		   updated_at = excluded.updated_at`,
		sess.SessionID, accountID, nullableJID(sess.JID), string(sess.State), role,
		nullableUnix(sess.PairedAt), unix(sess.UpdatedAt)); err != nil {
		return fmt.Errorf("sessionstore: upsert de device: %w", err)
	}

	// Re-vinculación: si el device cambió de cuenta y la anterior quedó SIN dispositivos, purgarla.
	if prevAccountID != "" && prevAccountID != accountID {
		if err := deleteAccountIfEmpty(ctx, tx, prevAccountID); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sessionstore: commit de upsert: %w", err)
	}
	return nil
}

// deleteAccountIfEmpty borra la cuenta accountID SOLO si ya no tiene dispositivos (dentro de la tx dada).
// Es la purga de la cuenta provisional que queda vacía al re-vincular un device por self_pn (cero
// huérfanos, §10.I). No-op si la cuenta aún tiene dispositivos.
func deleteAccountIfEmpty(ctx context.Context, tx *sql.Tx, accountID string) error {
	var n int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM devices WHERE account_id = ?`, accountID).Scan(&n); err != nil {
		return fmt.Errorf("sessionstore: contar dispositivos de la cuenta previa: %w", err)
	}
	if n > 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM accounts WHERE account_id = ?`, accountID); err != nil {
		return fmt.Errorf("sessionstore: purgar cuenta previa vacía: %w", err)
	}
	return nil
}

// resolveAccount asegura la fila `accounts` a la que colgar el device (dentro de la MISMA tx) y devuelve
// su account_id. Ver reglas en Upsert.
func resolveAccount(ctx context.Context, tx *sql.Tx, sess domain.Session) (string, error) {
	updatedAt := unix(sess.UpdatedAt)
	switch {
	case sess.AccountID != "":
		if err := upsertAccount(ctx, tx, sess.AccountID, sess.SelfPN, sess.DisplayName, updatedAt); err != nil {
			return "", err
		}
		return sess.AccountID, nil

	case sess.SelfPN != "":
		var id string
		err := tx.QueryRowContext(ctx, `SELECT account_id FROM accounts WHERE self_pn = ?`, sess.SelfPN).Scan(&id)
		switch {
		case err == nil:
			// La cuenta ya existe: misma self_pn ⇒ mismo account_id. Refresca datos sin tocar self_pn/created_at.
			if err := touchAccount(ctx, tx, id, sess.DisplayName, updatedAt); err != nil {
				return "", err
			}
			return id, nil
		case errors.Is(err, sql.ErrNoRows):
			id = uuid.NewString()
			if err := insertAccount(ctx, tx, id, sess.SelfPN, sess.DisplayName, updatedAt); err != nil {
				return "", err
			}
			return id, nil
		default:
			return "", fmt.Errorf("sessionstore: resolver cuenta por self_pn: %w", err)
		}

	default:
		// Provisional: cuenta por dispositivo mientras el número no se conoce (device en 'pairing').
		if err := upsertAccount(ctx, tx, sess.SessionID, "", sess.DisplayName, updatedAt); err != nil {
			return "", err
		}
		return sess.SessionID, nil
	}
}

// upsertAccount inserta o actualiza una cuenta por account_id (self_pn/display_name/updated_at); created_at
// se fija solo en el alta. Se usa para la cuenta explícita y la provisional (self_pn vacío ⇒ NULL).
func upsertAccount(ctx context.Context, tx *sql.Tx, id, selfPN, displayName string, updatedAt int64) error {
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO accounts (account_id, self_pn, display_name, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(account_id) DO UPDATE SET
		   self_pn      = excluded.self_pn,
		   display_name = excluded.display_name,
		   updated_at   = excluded.updated_at`,
		id, nullableStr(selfPN), nullableStr(displayName), updatedAt, updatedAt); err != nil {
		return fmt.Errorf("sessionstore: upsert de cuenta: %w", err)
	}
	return nil
}

// insertAccount da de alta una cuenta NUEVA (self_pn ya verificado inexistente); created_at = updated_at.
func insertAccount(ctx context.Context, tx *sql.Tx, id, selfPN, displayName string, updatedAt int64) error {
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO accounts (account_id, self_pn, display_name, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)`,
		id, nullableStr(selfPN), nullableStr(displayName), updatedAt, updatedAt); err != nil {
		return fmt.Errorf("sessionstore: crear cuenta: %w", err)
	}
	return nil
}

// touchAccount refresca updated_at (y display_name si viene) de una cuenta existente SIN tocar
// self_pn/created_at. COALESCE es portable SQLite/Postgres.
func touchAccount(ctx context.Context, tx *sql.Tx, id, displayName string, updatedAt int64) error {
	if _, err := tx.ExecContext(ctx,
		`UPDATE accounts SET display_name = COALESCE(?, display_name), updated_at = ? WHERE account_id = ?`,
		nullableStr(displayName), updatedAt, id); err != nil {
		return fmt.Errorf("sessionstore: actualizar cuenta: %w", err)
	}
	return nil
}

// List devuelve TODOS los dispositivos persistidos (cualquier estado), orden determinista (updated_at,
// luego session_id). Unifica su implementación con ListActive vía list().
func (s *Store) List(ctx context.Context) ([]domain.Session, error) {
	return s.list(ctx, false)
}

// ListActive devuelve solo los dispositivos en estado 'active' (los que el arranque debe restaurar). Mismo
// orden determinista que List.
func (s *Store) ListActive(ctx context.Context) ([]domain.Session, error) {
	return s.list(ctx, true)
}

// list es la implementación UNIFICADA de List/ListActive (design §T1: `List(activeOnly)`): con
// activeOnly filtra por state='active'; si no, devuelve todos. El puerto app.SessionStore conserva las
// dos firmas públicas (List/ListActive) para no romper a los consumidores runtime (T3).
func (s *Store) list(ctx context.Context, activeOnly bool) ([]domain.Session, error) {
	if activeOnly {
		return s.query(ctx, deviceSelect+` WHERE d.state = ? ORDER BY d.updated_at, d.session_id`,
			string(domain.SessionStateActive))
	}
	return s.query(ctx, deviceSelect+` ORDER BY d.updated_at, d.session_id`)
}

// GetByAccount devuelve TODOS los dispositivos de una cuenta (1..4 por número), orden determinista. Es la
// lectura por CUENTA (design §T1) que apoya el failover/borrado por número (T5); vacío si la cuenta no
// tiene dispositivos.
func (s *Store) GetByAccount(ctx context.Context, accountID string) ([]domain.Session, error) {
	return s.query(ctx, deviceSelect+` WHERE d.account_id = ? ORDER BY d.updated_at, d.session_id`, accountID)
}

// query ejecuta un SELECT con la proyección deviceSelect y materializa las filas a domain.Session.
func (s *Store) query(ctx context.Context, q string, args ...any) ([]domain.Session, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sessionstore: listar dispositivos: %w", err)
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
		return nil, fmt.Errorf("sessionstore: iterar dispositivos: %w", err)
	}
	return out, nil
}

// Get devuelve el dispositivo con ese `session_id` (⨝ su cuenta), o app.ErrSessionNotFound si no existe.
func (s *Store) Get(ctx context.Context, sessionID string) (domain.Session, error) {
	row := s.db.QueryRowContext(ctx, deviceSelect+` WHERE d.session_id = ?`, sessionID)
	sess, err := scan(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Session{}, app.ErrSessionNotFound
	}
	if err != nil {
		return domain.Session{}, err
	}
	return sess, nil
}

// Delete elimina el DISPOSITIVO con ese `session_id`. Idempotente (borrar uno ausente no es error). Es la
// parte de metadatos del borrado quirúrgico por dispositivo (design §7); la cuenta vacía se purga aparte
// (DeleteDeviceCascade) y el material cripto/DEK los limpia el Manager. Se conserva para consumidores del
// puerto app.SessionStore (que solo expone Delete); el runtime T3 usa DeleteDeviceCascade.
func (s *Store) Delete(ctx context.Context, sessionID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM devices WHERE session_id = ?`, sessionID); err != nil {
		return fmt.Errorf("sessionstore: borrar device: %w", err)
	}
	return nil
}

// DeleteDeviceCascade borra el DISPOSITIVO `session_id` y, si su CUENTA queda SIN dispositivos, borra
// también la cuenta — todo en una TRANSACCIÓN (Plan 022 T3, decisión §10.I: cero huérfanos en metadatos).
// Es la parte de metadatos del borrado quirúrgico por dispositivo que usa Manager.Unlink; el material
// cifrado (msg_enc_*/whatsmeow_*) y la DEK los limpia el Manager aparte (cryptostore.DeleteDevice +
// custody.Clear). Idempotente: borrar un session_id ausente no es error (no toca cuentas).
func (s *Store) DeleteDeviceCascade(ctx context.Context, sessionID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sessionstore: abrir transacción de borrado por dispositivo: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op tras Commit.

	var accountID string
	err = tx.QueryRowContext(ctx, `SELECT account_id FROM devices WHERE session_id = ?`, sessionID).Scan(&accountID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // ausente: idempotente, nada que borrar.
	}
	if err != nil {
		return fmt.Errorf("sessionstore: resolver cuenta del dispositivo a borrar: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM devices WHERE session_id = ?`, sessionID); err != nil {
		return fmt.Errorf("sessionstore: borrar device: %w", err)
	}

	var remaining int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM devices WHERE account_id = ?`, accountID).Scan(&remaining); err != nil {
		return fmt.Errorf("sessionstore: contar dispositivos de la cuenta: %w", err)
	}
	if remaining == 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM accounts WHERE account_id = ?`, accountID); err != nil {
			return fmt.Errorf("sessionstore: borrar cuenta vacía: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sessionstore: commit de borrado por dispositivo: %w", err)
	}
	return nil
}

// DeleteByAccount borra una CUENTA entera y sus dispositivos (borrado por número) de forma transaccional.
// Es la contraparte por-cuenta de Delete (design §T1); idempotente (una cuenta ausente no es error). No
// toca material cripto/DEK (eso lo orquesta el Manager en T3).
func (s *Store) DeleteByAccount(ctx context.Context, accountID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sessionstore: abrir transacción de borrado por cuenta: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM devices WHERE account_id = ?`, accountID); err != nil {
		return fmt.Errorf("sessionstore: borrar dispositivos de la cuenta: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM accounts WHERE account_id = ?`, accountID); err != nil {
		return fmt.Errorf("sessionstore: borrar cuenta: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sessionstore: commit de borrado por cuenta: %w", err)
	}
	return nil
}

// scanner abstrae *sql.Row y *sql.Rows (ambos exponen Scan) para compartir el mapeo fila->Session.
type scanner interface {
	Scan(dest ...any) error
}

// scan mapea una fila de deviceSelect (session_id, account_id, self_pn, display_name, jid, state, role,
// paired_at, updated_at) a domain.Session. self_pn/display_name/jid/paired_at son NULLABLE (NULL ⇒ vacío/
// cero). StoreDir YA NO se deriva (Plan 022 T3, BD única): no hay directorio por sesión; el campo queda
// vacío (el modelo `.db`-por-sesión se retiró).
func scan(sc scanner) (domain.Session, error) {
	var (
		sessionID, accountID, state, role string
		selfPN, displayName, jid          sql.NullString
		pairedAt                          sql.NullInt64
		updatedAt                         int64
	)
	if err := sc.Scan(&sessionID, &accountID, &selfPN, &displayName, &jid, &state, &role,
		&pairedAt, &updatedAt); err != nil {
		return domain.Session{}, err
	}
	return domain.Session{
		SessionID:   sessionID,
		AccountID:   accountID,
		SelfPN:      selfPN.String,      // "" si NULL
		DisplayName: displayName.String, // "" si NULL
		JID:         jid.String,         // "" si NULL
		State:       domain.SessionState(state),
		Role:        role,
		PairedAt:    fromNullableUnix(pairedAt),
		UpdatedAt:   fromUnix(updatedAt),
	}, nil
}

// nullableJID convierte un JID a un valor SQL: NULL si está vacío (device en pairing), el JID si no.
func nullableJID(jid string) any {
	return nullableStr(jid)
}

// nullableStr convierte una cadena a un valor SQL: NULL si está vacía, la cadena si no.
func nullableStr(s string) any {
	if s == "" {
		return nil
	}
	return s
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
// (msg_enc_device) SIN descifrar material, envolviendo cryptostore.FirstDeviceJID. Lo usa RestoreSessions
// para backfillear el registro cuando la BD fue pareada antes de existir el registro de negocio.
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
