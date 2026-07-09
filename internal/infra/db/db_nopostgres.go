//go:build !postgres

package db

// db_nopostgres.go es el archivo por DEFECTO (sin el tag `postgres`): mantiene el núcleo pure-Go SQLite
// (ADR-0002) sin enlazar ningún driver Postgres. Provee el stub de openPostgres para que db.go compile
// igual en ambas configuraciones (Plan 022 T0). Compilar con `-tags postgres` sustituye este stub por
// la implementación real (db_postgres.go).

import (
	"context"
	"database/sql"
	"errors"
)

// ErrPostgresNotCompiled se devuelve al pedir el dialecto "postgres" en el binario DEFAULT: el driver
// Postgres solo se enlaza al recompilar con `-tags postgres`. Así el binario que se distribuye al Edge
// nunca arrastra pgx/lib-pq y sigue siendo un único binario estático pure-Go.
var ErrPostgresNotCompiled = errors.New(
	`db: dialecto "postgres" no compilado en este binario (recompila con -tags postgres)`)

// openPostgres (stub sin el tag `postgres`): devuelve ErrPostgresNotCompiled sin tocar la red.
func openPostgres(_ context.Context, _ string) (*sql.DB, error) {
	return nil, ErrPostgresNotCompiled
}
