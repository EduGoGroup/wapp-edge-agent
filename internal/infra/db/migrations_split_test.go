package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// Las tablas de cada set, para afirmar qué crea (y qué NO) cada migración.
var (
	storeTables = []string{
		"msg_enc_device", "msg_enc_identities", "msg_enc_sessions",
		"msg_enc_prekeys", "msg_enc_sender_keys",
	}
	metaTables = []string{"sessions", "sessions_v2"}
)

// TestMigrateStoreOnlyCreatesStoreTables: el set "store" crea SOLO las msg_enc_* y NO las de metadatos
// (sessions/sessions_v2). Es la garantía de que un store.db POR SESIÓN no arrastra metadatos de negocio
// (ADR-0016 §2/§4: store cifrado separado de la db central).
func TestMigrateStoreOnlyCreatesStoreTables(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := Open(ctx, DialectSQLite, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()

	if err := MigrateStore(ctx, database); err != nil {
		t.Fatalf("MigrateStore: %v", err)
	}
	for _, tbl := range storeTables {
		if !tableExists(t, ctx, database, tbl) {
			t.Errorf("MigrateStore debía crear la tabla %s", tbl)
		}
	}
	for _, tbl := range metaTables {
		if tableExists(t, ctx, database, tbl) {
			t.Errorf("MigrateStore NO debía crear la tabla de metadatos %s en un store por sesión", tbl)
		}
	}
}

// TestMigrateMetaOnlyCreatesMetaTables: el set "meta" crea SOLO sessions/sessions_v2 y NO las msg_enc_*.
// Es la garantía de que la db CENTRAL de metadatos no arrastra el esquema del store cifrado.
func TestMigrateMetaOnlyCreatesMetaTables(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sessions.db")
	database, err := Open(ctx, DialectSQLite, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()

	if err := MigrateMeta(ctx, database); err != nil {
		t.Fatalf("MigrateMeta: %v", err)
	}
	for _, tbl := range metaTables {
		if !tableExists(t, ctx, database, tbl) {
			t.Errorf("MigrateMeta debía crear la tabla %s", tbl)
		}
	}
	for _, tbl := range storeTables {
		if tableExists(t, ctx, database, tbl) {
			t.Errorf("MigrateMeta NO debía crear la tabla de store cifrado %s en la db central", tbl)
		}
	}
}

// TestOpenSessionStoreAppliesStoreSet: OpenSessionStore (Open + MigrateStore) deja un store.db nuevo con
// las msg_enc_* creadas y SIN las de metadatos. Cubre el criterio T2(c): crear un store nuevo aplica las
// migraciones msg_enc_* en ESE archivo.
func TestOpenSessionStoreAppliesStoreSet(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sessions", "store.db")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	database, err := OpenSessionStore(ctx, path)
	if err != nil {
		t.Fatalf("OpenSessionStore: %v", err)
	}
	defer func() { _ = database.Close() }()

	for _, tbl := range storeTables {
		if !tableExists(t, ctx, database, tbl) {
			t.Errorf("OpenSessionStore debía crear la tabla %s en el store de la sesión", tbl)
		}
	}
	if tableExists(t, ctx, database, "sessions_v2") {
		t.Error("el store por sesión NO debía contener sessions_v2 (eso vive en la db central)")
	}
	// El archivo es usable: inserta una fila msg_enc_* sin error.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO msg_enc_sessions (our_jid, their_id, session) VALUES ('a','b',x'00')`); err != nil {
		t.Fatalf("insert de prueba en el store de la sesión: %v", err)
	}
}

// TestOpenAndMigrateMetaIsIdempotent: la db central admite OpenAndMigrateMeta repetido (idempotente) y
// la tabla sessions_v2 acepta una fila (la usa el sessionstore en su propio test).
func TestOpenAndMigrateMetaIsIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sessions.db")
	for i := 0; i < 2; i++ {
		database, err := OpenAndMigrateMeta(ctx, path)
		if err != nil {
			t.Fatalf("OpenAndMigrateMeta #%d: %v", i, err)
		}
		_ = database.Close()
	}
	database, err := OpenAndMigrateMeta(ctx, path)
	if err != nil {
		t.Fatalf("OpenAndMigrateMeta (final): %v", err)
	}
	defer func() { _ = database.Close() }()
	if _, err := database.ExecContext(ctx,
		`INSERT INTO sessions_v2 (session_id, state, store_dir, updated_at) VALUES ('s','active','d',1)`); err != nil {
		t.Fatalf("insert en sessions_v2 de la db central: %v", err)
	}
}
