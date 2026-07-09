//go:build postgres

package db

// db_postgres.go SOLO se compila con `-tags postgres` (Plan 022 T0, design §5): enlaza el driver
// PostgreSQL para el dialecto conmutable. El binario DEFAULT (sin este tag) NO incluye este archivo ni
// importa lib/pq, así que sigue siendo pure-Go SQLite (ADR-0002). Requiere que go.mod tenga la
// dependencia github.com/lib/pq (el usuario la añade con `go get github.com/lib/pq`).

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // registra el driver "postgres" para database/sql (solo build `-tags postgres`).
)

// openPostgres abre la BD única en PostgreSQL. A diferencia de SQLite NO fija pragmas (WAL/foreign_keys
// no existen en Postgres) ni el escritor único (SetMaxOpenConns(1)): Postgres gestiona su propia
// concurrencia, así que el pool queda con los defaults de database/sql. Hace PingContext para fallar
// pronto si la cadena/servidor no responde (sql.Open es perezoso). No cifra a nivel de página: el
// cryptostore sigue cifrando campo a campo con la DEK.
func openPostgres(ctx context.Context, dsn string) (*sql.DB, error) {
	database, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("db: abrir postgres: %w", err)
	}
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("db: conectar postgres: %w", err)
	}
	return database, nil
}
