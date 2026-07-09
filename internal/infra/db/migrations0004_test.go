package db

import (
	"context"
	"path/filepath"
	"testing"
)

// TestMigrate0004CreatesAccountsAndDevices: sobre una BD nueva, Migrate crea `accounts` y `devices` con el
// esquema del modelo cuenta↔dispositivo (Plan 022 T1, ADR-0018): `devices` SIN columna store_dir, CON
// account_id/role.
func TestMigrate0004CreatesAccountsAndDevices(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := OpenAndMigrate(ctx, path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	defer func() { _ = database.Close() }()

	gotAcc := columnsOf(t, ctx, database, "accounts")
	wantAcc := []string{"account_id", "created_at", "display_name", "self_pn", "updated_at"}
	if !equalCols(gotAcc, wantAcc) {
		t.Fatalf("columnas de accounts = %v, esperaba %v", gotAcc, wantAcc)
	}

	gotDev := columnsOf(t, ctx, database, "devices")
	wantDev := []string{"account_id", "jid", "paired_at", "role", "session_id", "state", "updated_at"}
	if !equalCols(gotDev, wantDev) {
		t.Fatalf("columnas de devices = %v, esperaba %v", gotDev, wantDev)
	}
	// SIN store_dir (retirada en la BD única).
	for _, c := range gotDev {
		if c == "store_dir" {
			t.Fatal("devices NO debía tener columna store_dir (BD única, ADR-0018)")
		}
	}
}

// TestMigrate0004SelfPNUnique: `accounts.self_pn` es UNIQUE cuando no es NULL, pero varias cuentas
// PROVISIONALES con self_pn NULL coexisten.
func TestMigrate0004SelfPNUnique(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := OpenAndMigrate(ctx, path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Dos cuentas con self_pn NULL: permitidas.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO accounts (account_id, self_pn, created_at, updated_at) VALUES ('p1', NULL, 1, 1)`); err != nil {
		t.Fatalf("insert cuenta provisional p1: %v", err)
	}
	if _, err := database.ExecContext(ctx,
		`INSERT INTO accounts (account_id, self_pn, created_at, updated_at) VALUES ('p2', NULL, 1, 1)`); err != nil {
		t.Fatalf("dos self_pn NULL deberían coexistir: %v", err)
	}

	// Mismo self_pn no-NULL en dos cuentas: rechazado.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO accounts (account_id, self_pn, created_at, updated_at) VALUES ('a1', '569000', 1, 1)`); err != nil {
		t.Fatalf("insert cuenta a1: %v", err)
	}
	if _, err := database.ExecContext(ctx,
		`INSERT INTO accounts (account_id, self_pn, created_at, updated_at) VALUES ('a2', '569000', 1, 1)`); err == nil {
		t.Fatal("se esperaba violación de UNIQUE(self_pn) en self_pn no-NULL duplicado")
	}
}

// TestMigrate0004DevicesFKAndIndexes: la FK devices.account_id→accounts se enforcea (foreign_keys=ON), el
// índice único PARCIAL ux_devices_jid rechaza jid no-NULL duplicado (permite varios NULL) y role tiene
// default 'primary'.
func TestMigrate0004DevicesFKAndIndexes(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := OpenAndMigrate(ctx, path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	defer func() { _ = database.Close() }()

	// FK: un device que referencia una cuenta inexistente es rechazado.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO devices (session_id, account_id, state, updated_at) VALUES ('d0', 'no-existe', 'pairing', 1)`); err == nil {
		t.Fatal("se esperaba violación de FK devices.account_id→accounts")
	}

	// Cuenta base para colgar dispositivos.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO accounts (account_id, self_pn, created_at, updated_at) VALUES ('acc', '569111', 1, 1)`); err != nil {
		t.Fatalf("insert cuenta: %v", err)
	}

	// role default 'primary' cuando no se especifica.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO devices (session_id, account_id, state, updated_at) VALUES ('d1', 'acc', 'pairing', 1)`); err != nil {
		t.Fatalf("insert device d1: %v", err)
	}
	var role string
	if err := database.QueryRowContext(ctx, `SELECT role FROM devices WHERE session_id='d1'`).Scan(&role); err != nil {
		t.Fatalf("leer role: %v", err)
	}
	if role != "primary" {
		t.Fatalf("role default = %q, esperaba 'primary'", role)
	}

	// Dos devices con jid NULL: permitidos (índice parcial WHERE jid IS NOT NULL).
	if _, err := database.ExecContext(ctx,
		`INSERT INTO devices (session_id, account_id, jid, state, updated_at) VALUES ('d2', 'acc', NULL, 'pairing', 1)`); err != nil {
		t.Fatalf("insert device d2 (jid NULL): %v", err)
	}

	// Mismo jid no-NULL en dos devices: rechazado.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO devices (session_id, account_id, jid, state, updated_at) VALUES ('d3', 'acc', 'dup@x', 'active', 1)`); err != nil {
		t.Fatalf("insert device d3: %v", err)
	}
	if _, err := database.ExecContext(ctx,
		`INSERT INTO devices (session_id, account_id, jid, state, updated_at) VALUES ('d4', 'acc', 'dup@x', 'active', 1)`); err == nil {
		t.Fatal("se esperaba violación de unicidad parcial en jid no-NULL duplicado (ux_devices_jid)")
	}
}

// TestMigrate0004CoexistsWithLegacy: la 0004 NO elimina `sessions_v2` (ni `sessions`); coexisten con
// accounts/devices (el retiro real del estado viejo lo hace la migración clean-slate de edgemigrate).
func TestMigrate0004CoexistsWithLegacy(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := OpenAndMigrate(ctx, path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	defer func() { _ = database.Close() }()

	for _, tbl := range []string{"sessions", "sessions_v2", "accounts", "devices"} {
		if !tableExists(t, ctx, database, tbl) {
			t.Fatalf("la tabla %q debía existir tras la 0004", tbl)
		}
	}

	// Idempotente: reaplicar Migrate no rompe ni borra datos.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO accounts (account_id, self_pn, created_at, updated_at) VALUES ('acc', '569222', 1, 1)`); err != nil {
		t.Fatalf("insert cuenta: %v", err)
	}
	if err := Migrate(ctx, database); err != nil {
		t.Fatalf("Migrate (2ª): %v", err)
	}
	var n int
	if err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("cuentas tras doble Migrate = %d, esperaba 1", n)
	}
}

// equalCols compara dos listas de columnas (ya ordenadas por columnsOf).
func equalCols(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
