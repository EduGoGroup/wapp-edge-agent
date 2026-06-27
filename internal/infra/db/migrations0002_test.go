package db

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"testing/fstest"
)

// columnsOf lee los nombres de columna de una tabla con pragma_table_info.
func columnsOf(t *testing.T, ctx context.Context, database *sql.DB, table string) []string {
	t.Helper()
	rows, err := database.QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		t.Fatalf("pragma_table_info(%s): %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		cols = append(cols, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(cols)
	return cols
}

// tableExists indica si existe una tabla con ese nombre.
func tableExists(t *testing.T, ctx context.Context, database *sql.DB, table string) bool {
	t.Helper()
	var name string
	err := database.QueryRowContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		t.Fatalf("sqlite_master(%s): %v", table, err)
	}
	return true
}

// TestMigrate0002CreatesSessions: sobre una BD nueva, Migrate crea la tabla `sessions` con el
// esquema esperado (jid PK + state + paired_at + updated_at).
func TestMigrate0002CreatesSessions(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := OpenAndMigrate(ctx, path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	defer func() { _ = database.Close() }()

	got := columnsOf(t, ctx, database, "sessions")
	want := []string{"jid", "paired_at", "state", "updated_at"}
	if len(got) != len(want) {
		t.Fatalf("columnas de sessions = %v, esperaba %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("columnas de sessions = %v, esperaba %v", got, want)
		}
	}

	// jid debe ser PRIMARY KEY: un segundo INSERT con el mismo jid debe colisionar.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO sessions (jid, state, paired_at, updated_at) VALUES ('j','active',1,1)`); err != nil {
		t.Fatalf("insert inicial: %v", err)
	}
	if _, err := database.ExecContext(ctx,
		`INSERT INTO sessions (jid, state, paired_at, updated_at) VALUES ('j','active',2,2)`); err == nil {
		t.Fatal("se esperaba colisión de PRIMARY KEY en jid duplicado")
	}
}

// TestMigrate0002OnDBWithOnly0001: simula la BD real del spike (solo 0001 aplicada, con un device
// pareado) y verifica que Migrate añade la 0002 SIN tocar los datos existentes (idempotencia real).
func TestMigrate0002OnDBWithOnly0001(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Aplica SOLO la 0001 (leída del fichero en disco) para emular el estado "pre-0002".
	sql0001, err := os.ReadFile(filepath.Join("migrations", "store", "0001_init.sql"))
	if err != nil {
		t.Fatalf("leer 0001: %v", err)
	}
	if _, err := database.ExecContext(ctx, string(sql0001)); err != nil {
		t.Fatalf("aplicar 0001: %v", err)
	}
	// Inserta un device "pareado" (datos que NO deben perderse al migrar la 0002).
	if _, err := database.ExecContext(ctx,
		`INSERT INTO msg_enc_device (jid, registration_id, signed_pre_key_id, noise_priv,
		   identity_priv, signed_pre_key_priv, signed_pre_key_sig, adv_secret_key, adv_details,
		   adv_account_sig, adv_account_sig_key, adv_device_sig)
		 VALUES ('56999@s.whatsapp.net', 1, 1, x'00', x'00', x'00', x'00', x'00', x'00', x'00', x'00', x'00')`); err != nil {
		t.Fatalf("insert device: %v", err)
	}

	// `sessions` NO debe existir aún.
	if tableExists(t, ctx, database, "sessions") {
		t.Fatal("la tabla sessions no debía existir antes de la 0002")
	}

	// Migrate completo: 0001 es no-op, 0002 crea `sessions`.
	if err := Migrate(ctx, database); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if !tableExists(t, ctx, database, "sessions") {
		t.Fatal("la tabla sessions debía existir tras Migrate")
	}

	// El device pareado sigue intacto.
	var jid string
	if err := database.QueryRowContext(ctx,
		`SELECT jid FROM msg_enc_device LIMIT 1`).Scan(&jid); err != nil {
		t.Fatalf("el device pareado se perdió tras migrar: %v", err)
	}
	if jid != "56999@s.whatsapp.net" {
		t.Fatalf("jid del device = %q tras migrar (cambió)", jid)
	}
}

// TestMigrateDoubleApplyIsNoop: aplicar Migrate dos veces sobre la misma BD no rompe y deja los
// datos intactos (idempotencia de TODAS las migraciones embebidas).
func TestMigrateDoubleApplyIsNoop(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := OpenAndMigrate(ctx, path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	defer func() { _ = database.Close() }()

	if _, err := database.ExecContext(ctx,
		`INSERT INTO sessions (jid, state, paired_at, updated_at) VALUES ('j','active',7,7)`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Segunda aplicación: no-op, no debe borrar la fila.
	if err := Migrate(ctx, database); err != nil {
		t.Fatalf("Migrate (2ª): %v", err)
	}
	var n int
	if err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("filas en sessions = %d tras doble Migrate, esperaba 1", n)
	}
}

// readFileErrFS lista bien el directorio de migraciones (ReadDir promovido de MapFS) pero falla al
// LEER un fichero: fuerza la rama de error de fs.ReadFile dentro de Migrate (inalcanzable con el
// embed real). fs.ReadFile usa el método ReadFile si el FS lo implementa, así que lo sobrescribimos.
type readFileErrFS struct{ fstest.MapFS }

func (f readFileErrFS) ReadFile(string) ([]byte, error) {
	return nil, errors.New("lectura simulada fallida")
}

// withMigrationsFS reemplaza temporalmente la fuente de migraciones y la restaura al terminar.
func withMigrationsFS(t *testing.T, replacement fs.FS) {
	t.Helper()
	prev := migrationsFS
	migrationsFS = replacement
	t.Cleanup(func() { migrationsFS = prev })
}

// TestMigrateFailsWhenDirMissing: si el FS no tiene el directorio de migraciones, migrationNames
// (fs.ReadDir) falla y Migrate propaga el error.
func TestMigrateFailsWhenDirMissing(t *testing.T) {
	withMigrationsFS(t, fstest.MapFS{}) // sin "migrations/"
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if err := Migrate(context.Background(), database); err == nil {
		t.Fatal("Migrate debía fallar si no existe el directorio de migraciones")
	}
}

// TestMigrateFailsWhenReadFileErrors: el directorio lista un .sql pero leerlo falla; Migrate propaga.
func TestMigrateFailsWhenReadFileErrors(t *testing.T) {
	mapFS := fstest.MapFS{
		"migrations/store/0001_x.sql": {Data: []byte("CREATE TABLE t (x);")},
	}
	withMigrationsFS(t, readFileErrFS{mapFS})
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if err := Migrate(context.Background(), database); err == nil {
		t.Fatal("Migrate debía fallar si no puede leer un fichero de migración")
	}
}
