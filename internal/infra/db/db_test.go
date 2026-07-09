package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenSetsPragmasAndPerms(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := Open(context.Background(), DialectSQLite, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("permisos = %o, esperaba 600", perm)
	}

	// SQLite: escritor único (pool limitado a 1) para que el PRAGMA foreign_keys por-conexión rija
	// todas las operaciones y se serialice la escritura del store cifrado (Plan 022 T0, design §9).
	if n := database.Stats().MaxOpenConnections; n != 1 {
		t.Errorf("SetMaxOpenConns(SQLite) = %d, esperaba 1 (escritor único)", n)
	}

	var journal string
	if err := database.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if journal != "wal" {
		t.Errorf("journal_mode = %q, esperaba wal", journal)
	}
	var fk int
	if err := database.QueryRow(`PRAGMA foreign_keys`).Scan(&fk); err != nil {
		t.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	if fk != 1 {
		t.Errorf("foreign_keys = %d, esperaba 1", fk)
	}
}

func TestOpenPreexistingFileGetsChmodded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	// Preexiste con permisos laxos: Open debe reapretarlos a 0600.
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	database, err := Open(context.Background(), DialectSQLite, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()
	info, _ := os.Stat(path)
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("permisos = %o, esperaba 600 tras reapretar", perm)
	}
}

func TestOpenFailsOnUnwritablePath(t *testing.T) {
	// Directorio padre inexistente: os.OpenFile falla y Open propaga el error.
	path := filepath.Join(t.TempDir(), "no-existe", "store.db")
	if _, err := Open(context.Background(), DialectSQLite, path); err == nil {
		t.Fatal("Open debía fallar con un directorio padre inexistente")
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := Open(ctx, DialectSQLite, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()

	for i := 0; i < 2; i++ {
		if err := Migrate(ctx, database); err != nil {
			t.Fatalf("Migrate #%d: %v", i, err)
		}
	}
	// Las 5 tablas msg_enc_* deben existir.
	for _, table := range []string{
		"msg_enc_device", "msg_enc_identities", "msg_enc_sessions",
		"msg_enc_prekeys", "msg_enc_sender_keys",
	} {
		var name string
		err := database.QueryRow(
			`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
		if err != nil {
			t.Errorf("tabla %s no existe tras migrar: %v", table, err)
		}
	}
}

func TestOpenAndMigrate(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := OpenAndMigrate(ctx, path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	defer func() { _ = database.Close() }()

	if _, err := database.ExecContext(ctx,
		`INSERT INTO msg_enc_sessions (our_jid, their_id, session) VALUES ('a','b',x'00')`); err != nil {
		t.Fatalf("insert de prueba en tabla migrada: %v", err)
	}
}

func TestOpenAndMigrateFailsOnBadPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no-existe", "store.db")
	if _, err := OpenAndMigrate(context.Background(), path); err == nil {
		t.Fatal("OpenAndMigrate debía fallar con un directorio padre inexistente")
	}
}

// TestOpenFailsOnCorruptFile cubre el branch de error de PRAGMA: un fichero preexistente que NO
// es una BD SQLite hace fallar el primer PRAGMA (journal_mode lee la cabecera).
func TestOpenFailsOnCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	if err := os.WriteFile(path, []byte("esto no es una base de datos sqlite valida"), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := Open(context.Background(), DialectSQLite, path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open sobre un fichero corrupto debía fallar en el PRAGMA")
	}
}

// TestMigrateFailsOnClosedDB cubre el branch de error de Migrate (ExecContext sobre una conexión
// ya cerrada).
func TestMigrateFailsOnClosedDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	db, err := Open(context.Background(), DialectSQLite, path)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if err := Migrate(context.Background(), db); err == nil {
		t.Fatal("Migrate sobre una BD cerrada debía fallar")
	}
}

// TestOpenAndMigrateFailsWhenMigrationConflicts cubre el branch de fallo de Migrate DENTRO de
// OpenAndMigrate (Open OK, pero la migración falla y debe cerrar la conexión y propagar el error).
// Se fuerza preexistiendo un ÍNDICE con el nombre de una tabla de la migración: CREATE TABLE IF
// NOT EXISTS NO suprime la colisión con un índice del mismo nombre.
func TestOpenAndMigrateFailsWhenMigrationConflicts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	pre, err := Open(context.Background(), DialectSQLite, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pre.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pre.Exec(`CREATE INDEX msg_enc_device ON t(x)`); err != nil {
		t.Fatal(err)
	}
	_ = pre.Close()

	if _, err := OpenAndMigrate(context.Background(), path); err == nil {
		t.Fatal("OpenAndMigrate debía fallar por colisión de nombre en la migración")
	}
}
