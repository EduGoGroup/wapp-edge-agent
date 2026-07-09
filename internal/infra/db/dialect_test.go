package db

import (
	"context"
	"path/filepath"
	"testing"
)

// TestOpen_UnsupportedDialect: un dialecto desconocido falla claro sin tocar disco/red (Plan 022 T0).
func TestOpen_UnsupportedDialect(t *testing.T) {
	if _, err := Open(context.Background(), "mysql", "cadena"); err == nil {
		t.Fatal("Open con un dialecto no soportado debía fallar")
	}
}

// TestOpen_EmptyDialectDefaultsToSQLite: dialecto vacío se trata como SQLite (default robusto), abre el
// fichero con permisos y escritor único como la rama sqlite explícita.
func TestOpen_EmptyDialectDefaultsToSQLite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := Open(context.Background(), "", path)
	if err != nil {
		t.Fatalf("Open(dialect=\"\"): %v", err)
	}
	defer func() { _ = database.Close() }()
	if n := database.Stats().MaxOpenConnections; n != 1 {
		t.Errorf("pool SQLite = %d, esperaba 1 (escritor único)", n)
	}
}

// TestOpen_SameSQLiteDBReopened: abrir la MISMA BD SQLite dos veces (Plan 022 T0: "abrir la misma BD")
// y migrar en cada apertura es idempotente; la fila sembrada en la 1ª apertura sigue visible en la 2ª,
// probando que ambas conexiones ven el mismo fichero.
func TestOpen_SameSQLiteDBReopened(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")

	d1, err := Open(ctx, DialectSQLite, path)
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	if err := Migrate(ctx, d1); err != nil {
		t.Fatalf("Migrate #1: %v", err)
	}
	if _, err := d1.ExecContext(ctx,
		`INSERT INTO msg_enc_sessions (our_jid, their_id, session) VALUES ('a','b',x'00')`); err != nil {
		t.Fatalf("insert #1: %v", err)
	}
	_ = d1.Close()

	d2, err := Open(ctx, DialectSQLite, path)
	if err != nil {
		t.Fatalf("Open #2 (misma BD): %v", err)
	}
	defer func() { _ = d2.Close() }()
	if err := Migrate(ctx, d2); err != nil {
		t.Fatalf("Migrate #2 (idempotente): %v", err)
	}
	var n int
	if err := d2.QueryRowContext(ctx, `SELECT COUNT(*) FROM msg_enc_sessions`).Scan(&n); err != nil {
		t.Fatalf("contar tras reabrir: %v", err)
	}
	if n != 1 {
		t.Errorf("filas tras reabrir la misma BD = %d, esperaba 1", n)
	}
}
