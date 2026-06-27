package db

import (
	"context"
	"path/filepath"
	"testing"
)

// TestMigrate0003CreatesSessionsV2: sobre una BD nueva, Migrate crea la tabla `sessions_v2` con el
// esquema multi-sesión esperado (session_id PK + jid + state + store_dir + paired_at + updated_at).
func TestMigrate0003CreatesSessionsV2(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := OpenAndMigrate(ctx, path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	defer func() { _ = database.Close() }()

	got := columnsOf(t, ctx, database, "sessions_v2")
	want := []string{"jid", "paired_at", "session_id", "state", "store_dir", "updated_at"}
	if len(got) != len(want) {
		t.Fatalf("columnas de sessions_v2 = %v, esperaba %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("columnas de sessions_v2 = %v, esperaba %v", got, want)
		}
	}

	// session_id debe ser PRIMARY KEY: un segundo INSERT con el mismo session_id debe colisionar.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO sessions_v2 (session_id, state, store_dir, updated_at) VALUES ('s','active','d',1)`); err != nil {
		t.Fatalf("insert inicial: %v", err)
	}
	if _, err := database.ExecContext(ctx,
		`INSERT INTO sessions_v2 (session_id, state, store_dir, updated_at) VALUES ('s','active','d',2)`); err == nil {
		t.Fatal("se esperaba colisión de PRIMARY KEY en session_id duplicado")
	}
}

// TestMigrate0003PartialUniqueJID: el índice único PARCIAL ux_sessions_jid rechaza dos filas con el
// MISMO jid no-NULL, pero permite múltiples filas con jid NULL (sesiones en pairing).
func TestMigrate0003PartialUniqueJID(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := OpenAndMigrate(ctx, path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Dos jid NULL: permitidos (índice parcial WHERE jid IS NOT NULL).
	if _, err := database.ExecContext(ctx,
		`INSERT INTO sessions_v2 (session_id, jid, state, store_dir, updated_at) VALUES ('p1', NULL, 'pairing', 'd1', 1)`); err != nil {
		t.Fatalf("insert p1 (jid NULL): %v", err)
	}
	if _, err := database.ExecContext(ctx,
		`INSERT INTO sessions_v2 (session_id, jid, state, store_dir, updated_at) VALUES ('p2', NULL, 'pairing', 'd2', 1)`); err != nil {
		t.Fatalf("dos jid NULL deberían coexistir: %v", err)
	}

	// Mismo jid no-NULL en dos sesiones: rechazado.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO sessions_v2 (session_id, jid, state, store_dir, updated_at) VALUES ('a1', 'dup@x', 'active', 'd3', 1)`); err != nil {
		t.Fatalf("insert a1: %v", err)
	}
	if _, err := database.ExecContext(ctx,
		`INSERT INTO sessions_v2 (session_id, jid, state, store_dir, updated_at) VALUES ('a2', 'dup@x', 'active', 'd4', 1)`); err == nil {
		t.Fatal("se esperaba violación de unicidad parcial en jid no-NULL duplicado")
	}
}

// TestMigrate0003CoexistsWithLegacy: la 0003 NO elimina la tabla `sessions` (0002); ambas coexisten
// (el retiro real del estado single-sesión lo hace la migración clean-slate de edgemigrate, no un DROP).
func TestMigrate0003CoexistsWithLegacy(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := OpenAndMigrate(ctx, path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	defer func() { _ = database.Close() }()

	if !tableExists(t, ctx, database, "sessions") {
		t.Fatal("la tabla `sessions` (0002) debía seguir existiendo tras la 0003")
	}
	if !tableExists(t, ctx, database, "sessions_v2") {
		t.Fatal("la tabla `sessions_v2` (0003) debía existir")
	}
}
