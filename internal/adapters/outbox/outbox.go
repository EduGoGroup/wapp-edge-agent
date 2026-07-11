// Package outbox implementa el OUTBOX DURABLE del Edge (Plan 027 Ola 3 · T2, cierra H2 / ADR-0003) sobre la
// BD ÚNICA SQLite del Edge (ADR-0018). Es el adapter del puerto app.Outbox: encola los eventos edge->cloud
// (entrantes/acuses) cuando el stream CloudLink está caído y los sirve en orden para reenviar al reconectar.
//
// Sin broker (ADR-0003): una tabla `outbox` en el mismo .db. Convención SQL igual que el resto de adapters de
// la BD única (sessionstore): placeholders `?` y sintaxis SQLite-primary. El orden de drenaje lo da `seq`,
// una secuencia monotónica generada en Go (portable, sin AUTOINCREMENT/SERIAL) sembrada de MAX(seq) al
// abrir; así el orden relativo global y POR SESIÓN (FIFO) sobrevive a reinicios.
package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Store respalda app.Outbox con la BD única del Edge.
type Store struct {
	db  *sql.DB
	log sharedlogger.Logger

	// maxEvents es el tope de eventos retenidos: al alcanzarlo, Enqueue descarta el más viejo (drop-oldest)
	// antes de insertar. ttl (>0) poda los eventos más viejos que ese tiempo al encolar/drenar; 0 lo desactiva.
	maxEvents int
	ttl       time.Duration

	// now inyecta el reloj (tests de TTL/drop). Producción usa time.Now.
	now func() time.Time

	// seq es la secuencia monotónica del orden de drenaje, sembrada de MAX(seq) en New. Atómica: el Edge
	// encola desde varios listeners por sesión en paralelo.
	seq atomic.Int64

	// mu serializa el bloque leer-tope→drop-oldest→insertar de Enqueue (correcto aunque el pool no aísle).
	mu sync.Mutex
}

var _ app.Outbox = (*Store)(nil)

// Option configura el Store (reloj de test, etc.).
type Option func(*Store)

// WithClock inyecta el reloj (tests de TTL/drop-oldest deterministas).
func WithClock(now func() time.Time) Option {
	return func(s *Store) {
		if now != nil {
			s.now = now
		}
	}
}

// New construye el Store sobre la BD YA migrada (db.Migrate creó la tabla `outbox`). Siembra la secuencia
// de MAX(seq) para que el orden de drenaje continúe tras un reinicio. maxEvents<=0 cae a un default sano;
// ttlHours<=0 desactiva el TTL.
func New(ctx context.Context, db *sql.DB, maxEvents, ttlHours int, log sharedlogger.Logger, opts ...Option) (*Store, error) {
	if log == nil {
		log = sharedlogger.Default()
	}
	if maxEvents <= 0 {
		maxEvents = 10000
	}
	var ttl time.Duration
	if ttlHours > 0 {
		ttl = time.Duration(ttlHours) * time.Hour
	}
	s := &Store{db: db, log: log, maxEvents: maxEvents, ttl: ttl, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	var maxSeq sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MAX(seq) FROM outbox`).Scan(&maxSeq); err != nil {
		return nil, fmt.Errorf("outbox: sembrar la secuencia desde MAX(seq): %w", err)
	}
	s.seq.Store(maxSeq.Int64) // 0 si la tabla está vacía (NullInt64 nulo => 0)
	return s, nil
}

// Enqueue persiste un evento (idempotente por DedupeKey vía INSERT OR IGNORE). Antes poda el TTL y, si la
// cola llegó al tope, descarta el más viejo (drop-oldest) con log — nunca crece sin límite (ADR-0003).
func (s *Store) Enqueue(ctx context.Context, item app.OutboxItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().Unix()
	if err := s.pruneTTLLocked(ctx, now); err != nil {
		return err
	}
	if err := s.dropOldestLocked(ctx); err != nil {
		return err
	}

	seq := s.seq.Add(1)
	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO outbox (dedupe_key, seq, session_id, kind, payload, attempts, created_unix, updated_unix)
		 VALUES (?, ?, ?, ?, ?, 0, ?, ?)`,
		item.DedupeKey, seq, item.SessionID, item.Kind, item.Payload, now, now)
	if err != nil {
		return fmt.Errorf("outbox: encolar evento (session_id=%s): %w", item.SessionID, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// dedupe_key ya presente: encolado idempotente (no es error). El seq consumido deja un hueco benigno.
		s.log.Debug("outbox: evento ya encolado (dedupe), ignorado", "session_id", item.SessionID, "kind", item.Kind)
	}
	return nil
}

// Drain devuelve hasta max eventos pendientes en orden de seq (FIFO), sin borrarlos. Poda el TTL antes.
func (s *Store) Drain(ctx context.Context, max int) ([]app.OutboxItem, error) {
	if max <= 0 {
		return nil, nil
	}
	if s.ttl > 0 {
		s.mu.Lock()
		err := s.pruneTTLLocked(ctx, s.now().Unix())
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT dedupe_key, session_id, kind, payload FROM outbox ORDER BY seq ASC LIMIT ?`, max)
	if err != nil {
		return nil, fmt.Errorf("outbox: leer pendientes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var items []app.OutboxItem
	for rows.Next() {
		var it app.OutboxItem
		if err := rows.Scan(&it.DedupeKey, &it.SessionID, &it.Kind, &it.Payload); err != nil {
			return nil, fmt.Errorf("outbox: escanear pendiente: %w", err)
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: recorrer pendientes: %w", err)
	}
	return items, nil
}

// Delete quita un evento ya reenviado (idempotente).
func (s *Store) Delete(ctx context.Context, dedupeKey string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM outbox WHERE dedupe_key = ?`, dedupeKey); err != nil {
		return fmt.Errorf("outbox: borrar evento reenviado: %w", err)
	}
	return nil
}

// Fail incrementa el contador de intentos de un evento (diagnóstico); no lo borra.
func (s *Store) Fail(ctx context.Context, dedupeKey string) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE outbox SET attempts = attempts + 1, updated_unix = ? WHERE dedupe_key = ?`,
		s.now().Unix(), dedupeKey); err != nil {
		return fmt.Errorf("outbox: marcar intento fallido: %w", err)
	}
	return nil
}

// PendingSessions lista los session_id con al menos un evento pendiente (para sembrar el guard de orden).
func (s *Store) PendingSessions(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT session_id FROM outbox`)
	if err != nil {
		return nil, fmt.Errorf("outbox: listar sesiones pendientes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("outbox: escanear sesión pendiente: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: recorrer sesiones pendientes: %w", err)
	}
	return ids, nil
}

// Depth cuenta los eventos pendientes de la sesión sessionID (profundidad del outbox por sesión, Plan 031
// T7). Es una lectura de solo-conteo (no toca payloads): alimenta SessionHealth.outbox_depth del heartbeat
// de salud. No poda TTL (es una foto instantánea; el drenaje/encolado ya podan).
func (s *Store) Depth(ctx context.Context, sessionID string) (int64, error) {
	var n int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM outbox WHERE session_id = ?`, sessionID).Scan(&n); err != nil {
		return 0, fmt.Errorf("outbox: contar profundidad (session_id=%s): %w", sessionID, err)
	}
	return n, nil
}

// pruneTTLLocked borra los eventos más viejos que el TTL (no-op si ttl==0). Debe llamarse bajo s.mu.
func (s *Store) pruneTTLLocked(ctx context.Context, nowUnix int64) error {
	if s.ttl <= 0 {
		return nil
	}
	cutoff := nowUnix - int64(s.ttl.Seconds())
	res, err := s.db.ExecContext(ctx, `DELETE FROM outbox WHERE created_unix < ?`, cutoff)
	if err != nil {
		return fmt.Errorf("outbox: podar TTL: %w", err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		s.log.Warn("outbox: eventos podados por TTL", "expirados", n, "ttl", s.ttl.String())
	}
	return nil
}

// dropOldestLocked descarta los eventos más viejos (menor seq) si la cola alcanzó el tope, dejando sitio
// para uno nuevo (política drop-oldest, ADR-0003). Debe llamarse bajo s.mu.
func (s *Store) dropOldestLocked(ctx context.Context) error {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&count); err != nil {
		return fmt.Errorf("outbox: contar eventos: %w", err)
	}
	if count < s.maxEvents {
		return nil
	}
	// Descarta los que sobran para dejar hueco a 1 nuevo (normalmente 1, pero cubre un tope reducido).
	toDrop := count - s.maxEvents + 1
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM outbox WHERE dedupe_key IN (SELECT dedupe_key FROM outbox ORDER BY seq ASC LIMIT ?)`, toDrop)
	if err != nil {
		return fmt.Errorf("outbox: drop-oldest: %w", err)
	}
	n, _ := res.RowsAffected()
	s.log.Warn("outbox: LLENO, descartando los más viejos (drop-oldest)", "descartados", n, "tope", s.maxEvents)
	return nil
}
