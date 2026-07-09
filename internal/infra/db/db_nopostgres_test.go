//go:build !postgres

package db

import (
	"context"
	"errors"
	"testing"
)

// TestOpen_PostgresNotCompiledByDefault: el binario DEFAULT (pure-Go SQLite, sin -tags postgres) NO
// enlaza driver Postgres, así que pedir el dialecto "postgres" devuelve ErrPostgresNotCompiled en vez
// de intentar conectar (Plan 022 T0: el binario default sigue pure-Go SQLite).
func TestOpen_PostgresNotCompiledByDefault(t *testing.T) {
	_, err := Open(context.Background(), DialectPostgres, "postgres://user:pass@localhost:5432/db")
	if !errors.Is(err, ErrPostgresNotCompiled) {
		t.Fatalf("esperaba ErrPostgresNotCompiled sin el tag postgres, got %v", err)
	}
}
