// Package db abre los SQLite del Edge y aplica sus migraciones embebidas, separadas en DOS sets
// (ADR-0016 §2/§4, Plan 008 §4):
//
//   - set "store"  (migrations/store, hoy 0001_init.sql → tablas msg_enc_*): es el esquema del
//     cryptostore y se aplica a CADA store.db POR SESIÓN (sessions/<id>/store.db). Solo material
//     whatsmeow cifrado campo a campo con la DEK de esa sesión.
//   - set "meta"   (migrations/meta, hoy 0002_sessions.sql + 0003_sessions_multi.sql → tablas
//     sessions/sessions_v2): metadatos de NEGOCIO en claro de las sesiones; se aplican a la db
//     CENTRAL (<data_dir>/sessions.db).
//
// El driver es modernc.org/sqlite (CGO_ENABLED=0, sin SQLCipher): el fichero .db NO se cifra a nivel
// de página; el cryptostore (internal/adapters/cryptostore) cifra CADA campo sensible con la DEK
// antes de escribirlo. Por eso aquí solo nos ocupamos de: abrir el .db con permisos 0600, fijar los
// pragmas (WAL, foreign_keys, busy_timeout) y aplicar el set de migración que corresponda.
//
// Compatibilidad: Migrate/OpenAndMigrate aplican AMBOS sets a una sola db (camino single-sesión
// legacy de cmd/agent, que T3/T4 recablearán al layout por sesión). Los helpers per-sesión
// (MigrateStore/OpenSessionStore) y central (MigrateMeta/OpenAndMigrateMeta) aplican un set cada uno.
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

// embeddedMigrations embebe los DOS sets de migraciones (store/ y meta/). Cada set se aplica en
// orden lexicográfico del nombre dentro de su subdirectorio (el prefijo NNNN_ garantiza el orden).
//
//go:embed migrations/store/*.sql migrations/meta/*.sql
var embeddedMigrations embed.FS

// migrationsFS es la fuente de las migraciones. Es una var (no la embed.FS directa) para que los
// tests puedan inyectar un FS que fuerce los errores de lectura, normalmente inalcanzables con embed.
var migrationsFS fs.FS = embeddedMigrations

// Subdirectorios de cada set de migración dentro de migrationsFS.
const (
	// storeMigrationsDir aloja el esquema del store cifrado por sesión (tablas msg_enc_*).
	storeMigrationsDir = "migrations/store"
	// metaMigrationsDir aloja el esquema de metadatos de negocio (tablas sessions/sessions_v2).
	metaMigrationsDir = "migrations/meta"
)

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

// MigrateStore aplica el set "store" (migrations/store/*.sql → tablas msg_enc_*) sobre database. Es
// la migración de un store.db POR SESIÓN (ADR-0016 §4): crea SOLO el esquema del cryptostore, sin las
// tablas de metadatos de negocio. Idempotente (CREATE TABLE IF NOT EXISTS).
func MigrateStore(ctx context.Context, database *sql.DB) error {
	return applyMigrations(ctx, database, storeMigrationsDir)
}

// MigrateMeta aplica el set "meta" (migrations/meta/*.sql → tablas sessions/sessions_v2) sobre
// database. Es la migración de la db CENTRAL de metadatos de negocio (ADR-0016 §2). Idempotente.
func MigrateMeta(ctx context.Context, database *sql.DB) error {
	return applyMigrations(ctx, database, metaMigrationsDir)
}

// Migrate aplica AMBOS sets (store y luego meta) sobre una sola db. Es el camino single-sesión
// legacy (cmd/agent abre UN .db con msg_enc_* + sessions_v2 a la vez); el modelo multi-sesión
// (ADR-0016) separa los sets en store.db por sesión vs sessions.db central, vía los helpers de arriba.
// El orden store→meta es el lexicográfico histórico (0001 < 0002 < 0003); no hay FKs cruzadas.
func Migrate(ctx context.Context, database *sql.DB) error {
	if err := MigrateStore(ctx, database); err != nil {
		return err
	}
	return MigrateMeta(ctx, database)
}

// applyMigrations aplica, en orden lexicográfico de nombre, todas las migraciones .sql del
// subdirectorio dir de migrationsFS sobre database. Cada migración es idempotente, así que reaplicarla
// sobre una db ya migrada es no-op.
func applyMigrations(ctx context.Context, database *sql.DB, dir string) error {
	names, err := migrationNames(dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		sqlText, err := fs.ReadFile(migrationsFS, dir+"/"+name)
		if err != nil {
			return fmt.Errorf("db: leer migración %q: %w", name, err)
		}
		if _, err := database.ExecContext(ctx, string(sqlText)); err != nil {
			return fmt.Errorf("db: aplicar migración %q: %w", name, err)
		}
	}
	return nil
}

// migrationNames lista los ficheros .sql del subdirectorio dir en orden lexicográfico (orden de
// aplicación).
func migrationNames(dir string) ([]string, error) {
	entries, err := fs.ReadDir(migrationsFS, dir)
	if err != nil {
		return nil, fmt.Errorf("db: listar migraciones de %q: %w", dir, err)
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

// OpenAndMigrate combina Open + Migrate (AMBOS sets): camino single-sesión legacy. Deja un *sql.DB
// con permisos 0600, pragmas fijados y las tablas msg_enc_* + sessions/sessions_v2 creadas. Cierra la
// conexión si la migración falla.
func OpenAndMigrate(ctx context.Context, path string) (*sql.DB, error) {
	return openAndApply(ctx, path, Migrate)
}

// OpenSessionStore combina Open + MigrateStore: abre (creando) el store.db de UNA sesión y le aplica
// SOLO el set "store" (msg_enc_*). Es el helper que el Manager (T3/T4) usa por sesión; las tablas
// whatsmeow_* no sensibles las crea aparte el cryptostore (sqlstore.Upgrade), no este runner. Cierra
// la conexión si la migración falla.
func OpenSessionStore(ctx context.Context, path string) (*sql.DB, error) {
	return openAndApply(ctx, path, MigrateStore)
}

// OpenAndMigrateMeta combina Open + MigrateMeta: abre (creando) la db CENTRAL de metadatos de negocio
// (<data_dir>/sessions.db) y le aplica SOLO el set "meta" (sessions/sessions_v2). Cierra la conexión
// si la migración falla.
func OpenAndMigrateMeta(ctx context.Context, path string) (*sql.DB, error) {
	return openAndApply(ctx, path, MigrateMeta)
}

// openAndApply abre el .db en path y le aplica migrate (un set o ambos); cierra la conexión si falla.
func openAndApply(ctx context.Context, path string, migrate func(context.Context, *sql.DB) error) (*sql.DB, error) {
	database, err := Open(path)
	if err != nil {
		return nil, err
	}
	if err := migrate(ctx, database); err != nil {
		_ = database.Close()
		return nil, err
	}
	return database, nil
}
