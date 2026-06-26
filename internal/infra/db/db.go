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
	"embed"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"

	_ "modernc.org/sqlite" // driver "sqlite" (CGO-free)
)

// embeddedMigrations embebe TODO el directorio de migraciones (0001_init.sql, 0002_sessions.sql, …).
// Migrate las aplica en orden lexicográfico del nombre (el prefijo NNNN_ garantiza el orden).
//
//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// migrationsFS es la fuente de las migraciones. Es una var (no la embed.FS directa) para que los
// tests puedan inyectar un FS que fuerce los errores de lectura, normalmente inalcanzables con embed.
var migrationsFS fs.FS = embeddedMigrations

const migrationsDir = "migrations"

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

// Migrate aplica TODAS las migraciones embebidas (migrations/*.sql) en orden lexicográfico de su
// nombre. Cada migración es idempotente (CREATE TABLE IF NOT EXISTS), así que:
//   - sobre un store nuevo crea todo el esquema (0001 msg_enc_* + 0002 sessions);
//   - sobre un store que ya tiene la 0001 (p.ej. la sesión real del spike), la 0001 es no-op y solo
//     se añade la 0002 (tabla sessions), sin tocar los datos existentes;
//   - reaplicarla entera sobre un store ya migrado es no-op.
//
// El orden importa (FKs/dependencias futuras): el prefijo NNNN_ del nombre fija la secuencia.
func Migrate(ctx context.Context, database *sql.DB) error {
	names, err := migrationNames()
	if err != nil {
		return err
	}
	for _, name := range names {
		sqlText, err := fs.ReadFile(migrationsFS, migrationsDir+"/"+name)
		if err != nil {
			return fmt.Errorf("db: leer migración %q: %w", name, err)
		}
		if _, err := database.ExecContext(ctx, string(sqlText)); err != nil {
			return fmt.Errorf("db: aplicar migración %q: %w", name, err)
		}
	}
	return nil
}

// migrationNames lista los ficheros .sql embebidos en orden lexicográfico (orden de aplicación).
func migrationNames() ([]string, error) {
	entries, err := fs.ReadDir(migrationsFS, migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("db: listar migraciones: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
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
