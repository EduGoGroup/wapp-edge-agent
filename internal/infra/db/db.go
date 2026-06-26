// Package db abre el store SQLite cifrado del Edge y aplica su migración embebida.
//
// El driver es modernc.org/sqlite (CGO_ENABLED=0, sin SQLCipher): el fichero .db NO se
// cifra a nivel de página, sino que el cryptostore (internal/adapters/cryptostore) cifra
// CADA campo sensible con la DEK antes de escribirlo. Por eso aquí solo nos ocupamos de:
//   - abrir el .db con permisos 0600 (solo el dueño lo lee),
//   - PRAGMA journal_mode=WAL y foreign_keys=ON,
//   - aplicar la migración 0001 (tablas msg_enc_*).
package db

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"os"

	_ "modernc.org/sqlite" // driver "sqlite" (CGO-free)
)

//go:embed migrations/0001_init.sql
var migration0001 string

// Open abre (creando si hace falta) el store SQLite en path con permisos 0600 y deja la
// conexión lista: journal_mode=WAL, foreign_keys=ON, busy_timeout=5s.
//
// Fija SetMaxOpenConns(1): PRAGMA foreign_keys es por-conexión en SQLite, así que limitar el
// pool a una conexión garantiza que el pragma aplicado aquí rige TODAS las operaciones (evita
// que database/sql abra conexiones nuevas sin el pragma). Es suficiente para el daemon del Edge,
// que serializa la escritura del store cifrado.
func Open(path string) (*sql.DB, error) {
	// Garantiza el fichero con 0600 ANTES de que SQLite lo cree con permisos del umask.
	f, err := os.OpenFile(path, os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("db: crear fichero del store: %w", err)
	}
	_ = f.Close()
	if err := os.Chmod(path, 0o600); err != nil { // por si preexistía con otros permisos
		return nil, fmt.Errorf("db: fijar permisos 0600: %w", err)
	}

	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("db: abrir sqlite: %w", err)
	}
	database.SetMaxOpenConns(1)

	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := database.Exec(pragma); err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("db: %q: %w", pragma, err)
		}
	}
	return database, nil
}

// Migrate aplica la migración embebida 0001 (tablas msg_enc_*). Es idempotente
// (CREATE TABLE IF NOT EXISTS), así que reaplicarla sobre un store ya migrado es no-op.
func Migrate(ctx context.Context, database *sql.DB) error {
	if _, err := database.ExecContext(ctx, migration0001); err != nil {
		return fmt.Errorf("db: aplicar migración 0001: %w", err)
	}
	return nil
}

// OpenAndMigrate combina Open + Migrate: deja un *sql.DB con permisos 0600, pragmas fijados
// y las tablas msg_enc_* creadas. Cierra la conexión si la migración falla.
func OpenAndMigrate(ctx context.Context, path string) (*sql.DB, error) {
	database, err := Open(path)
	if err != nil {
		return nil, err
	}
	if err := Migrate(ctx, database); err != nil {
		_ = database.Close()
		return nil, err
	}
	return database, nil
}
