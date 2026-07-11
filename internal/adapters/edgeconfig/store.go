// Package edgeconfig persiste y aplica la CONFIG EMPUJADA POR LA NUBE al Edge (Plan 029 · T10 / ADR-0021).
//
// El Cloud empuja config de negocio por el stream CloudLink (frame ConfigUpdate); hoy el blob de INTENCIONES
// por tenant (kind='intents'), del que el clasificador local (internal/adapters/intent) deriva prompt/schema.
// Este paquete cubre DOS piezas desacopladas:
//
//   - Store: persistencia por `kind` sobre la BD única del Edge (tabla edge_config, migración 0006). Get/Put,
//     idempotente por versión a nivel de Service. Convención SQL igual que los demás adapters de la BD única
//     (outbox/sessionstore): placeholders `?` y sintaxis SQLite-primary; el CREATE TABLE es portable.
//   - Service: la lógica de aplicación (idempotencia por versión, validación por kind, persistencia y
//     notificación en caliente a los suscriptores). Es el ConfigApplier que el adapter CloudLink invoca al
//     recibir un ConfigUpdate, y el que al arrancar recarga (Bootstrap) la config persistida.
//
// ZERO-KNOWLEDGE (ADR-0007): por aquí solo pasa CONFIG DE NEGOCIO (few-shots/vocabulario del tenant), nunca
// la DEK ni llaves privadas.
package edgeconfig

import (
	"context"
	"database/sql"
	"fmt"
)

// Record es una fila de config persistida: el blob validado de un `kind` con su versión.
type Record struct {
	Kind        string
	Version     string
	Payload     []byte
	UpdatedUnix int64
}

// Store abstrae la persistencia de config por kind (una fila por kind). Interfaz pequeña para poder
// inyectar un fake en los tests del Service.
type Store interface {
	// Get devuelve la config persistida de un kind. found=false si no hay fila (sin error).
	Get(ctx context.Context, kind string) (rec Record, found bool, err error)
	// Put inserta o REEMPLAZA la config de un kind (upsert por clave primaria kind).
	Put(ctx context.Context, rec Record) error
}

// SQLStore respalda Store con la BD única del Edge (tabla edge_config). La tabla la crea db.Migrate
// (migración 0006, set "meta"); aquí solo se leen/escriben filas.
type SQLStore struct {
	db *sql.DB
}

var _ Store = (*SQLStore)(nil)

// NewSQLStore construye el Store sobre la BD YA migrada.
func NewSQLStore(db *sql.DB) *SQLStore {
	return &SQLStore{db: db}
}

// Get lee la fila del kind. sql.ErrNoRows se traduce a found=false (no es error).
func (s *SQLStore) Get(ctx context.Context, kind string) (Record, bool, error) {
	rec := Record{Kind: kind}
	err := s.db.QueryRowContext(ctx,
		`SELECT version, payload, updated_unix FROM edge_config WHERE kind = ?`, kind).
		Scan(&rec.Version, &rec.Payload, &rec.UpdatedUnix)
	if err == sql.ErrNoRows {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, fmt.Errorf("edgeconfig: leer config %q: %w", kind, err)
	}
	return rec, true, nil
}

// Put hace upsert por kind (ON CONFLICT DO UPDATE — soportado por modernc SQLite y Postgres): una config
// nueva del mismo kind reemplaza a la anterior. La idempotencia por versión la decide el Service ANTES de
// llamar aquí (no re-escribe si la versión ya está aplicada).
func (s *SQLStore) Put(ctx context.Context, rec Record) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO edge_config (kind, version, payload, updated_unix)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(kind) DO UPDATE SET
		     version = excluded.version,
		     payload = excluded.payload,
		     updated_unix = excluded.updated_unix`,
		rec.Kind, rec.Version, rec.Payload, rec.UpdatedUnix)
	if err != nil {
		return fmt.Errorf("edgeconfig: persistir config %q: %w", rec.Kind, err)
	}
	return nil
}
