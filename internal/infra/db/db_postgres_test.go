//go:build postgres

package db

import (
	"context"
	"os"
	"testing"
)

// TestOpen_PostgresSmoke es el smoke de Postgres (Plan 022 T0, design §5): SOLO corre al compilar con
// `-tags postgres` Y con la cadena en WAPP_AGENT_TEST_PG_DSN (si falta, se salta: no hay servidor). No
// depende de las migraciones SQLite-flavored (portabilidad del esquema = T1): valida que Open conecta
// con el motor Postgres, que el pool NO queda con el escritor único de SQLite y que un round-trip
// básico funciona.
//
// Ejecutar en local (Plan B):
//
//	WAPP_AGENT_TEST_PG_DSN='postgres://user:pass@localhost:5432/wapp_test?sslmode=disable' \
//	  go test -tags postgres -run TestOpen_PostgresSmoke ./internal/infra/db/...
func TestOpen_PostgresSmoke(t *testing.T) {
	dsn := os.Getenv("WAPP_AGENT_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("WAPP_AGENT_TEST_PG_DSN no definido: se salta el smoke de Postgres")
	}
	ctx := context.Background()

	database, err := Open(ctx, DialectPostgres, dsn)
	if err != nil {
		t.Fatalf("Open(postgres): %v", err)
	}
	defer func() { _ = database.Close() }()

	// Postgres NO usa el escritor único de SQLite: el pool queda con los defaults (0 = ilimitado).
	if n := database.Stats().MaxOpenConnections; n == 1 {
		t.Errorf("pool Postgres = 1 (escritor único de SQLite); esperaba el default sin límite")
	}

	if _, err := database.ExecContext(ctx,
		`CREATE TEMP TABLE wapp_smoke (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("crear tabla temporal: %v", err)
	}
	if _, err := database.ExecContext(ctx,
		`INSERT INTO wapp_smoke (id, v) VALUES (1, 'ok')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var v string
	if err := database.QueryRowContext(ctx, `SELECT v FROM wapp_smoke WHERE id=1`).Scan(&v); err != nil {
		t.Fatalf("select: %v", err)
	}
	if v != "ok" {
		t.Errorf("round-trip Postgres: got %q, esperaba \"ok\"", v)
	}
}
