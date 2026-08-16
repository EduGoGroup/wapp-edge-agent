package db

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// Las tablas de cada set, para afirmar qué crea (y qué NO) cada migración.
var (
	storeTables = []string{
		"msg_enc_device", "msg_enc_identities", "msg_enc_sessions",
		"msg_enc_prekeys", "msg_enc_sender_keys",
	}
	metaTables = []string{"sessions", "sessions_v2", "accounts", "devices"}
	colaTables = []string{"cola_entrantes"}
)

// Nombres de los ficheros .db que el arranque real crea bajo <data_dir>. Se repiten aquí como literales
// porque sus constantes son NO exportadas y viven en otros paquetes —edge.db en daemon.singleDBFileName
// y cola_entrantes.db en el layout de sessionmgr (colaDBName)—, que además importan ESTE paquete: no se
// pueden importar desde aquí sin ciclo. Cada nombre está fijado por el test de su propio paquete
// (sessionmgr/layout_test.go afirma Layout.ColaDB()); aquí lo que se verifica es QUÉ tabla acaba en cuál.
const (
	edgeDBFileName = "edge.db"
	colaDBFileName = "cola_entrantes.db"
)

// indexesOf lee los nombres de los índices declarados sobre una tabla, en orden alfabético. Excluye los
// auto-índices internos de SQLite (prefijo sqlite_): solo interesan los que declara el .sql.
func indexesOf(t *testing.T, ctx context.Context, database *sql.DB, table string) []string {
	t.Helper()
	rows, err := database.QueryContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND name NOT LIKE 'sqlite_%'`, table)
	if err != nil {
		t.Fatalf("sqlite_master(indices de %s): %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	return names
}

// indexIsUnique indica si el índice existe sobre la tabla y está declarado UNIQUE.
func indexIsUnique(t *testing.T, ctx context.Context, database *sql.DB, table, index string) bool {
	t.Helper()
	var unique int
	err := database.QueryRowContext(ctx,
		`SELECT "unique" FROM pragma_index_list(?) WHERE name=?`, table, index).Scan(&unique)
	if err == sql.ErrNoRows {
		t.Fatalf("el índice %s no existe sobre %s", index, table)
	}
	if err != nil {
		t.Fatalf("pragma_index_list(%s): %v", table, err)
	}
	return unique == 1
}

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

// TestColaMigrationLandsInItsOwnFile reproduce el ARRANQUE REAL del daemon sobre un data_dir temporal
// (daemon.go: Open+Migrate de <data_dir>/edge.db, y aparte Open+MigrateCola de <data_dir>/cola_entrantes.db)
// y cierra las dos verificaciones que el set "cola" no tenía cubiertas (Plan 051 Ola 1 · T1.1):
//
//   - El fichero CORRECTO: cola_entrantes y sus tres índices nacen en cola_entrantes.db, y la BD ÚNICA
//     del Edge NO los ve. Es la garantía viva del porqué de MigrateCola (ver su doc en db.go): meter el
//     set en Migrate() crearía una tabla fantasma en edge.db que nadie leería.
//   - IDEMPOTENCIA REAL: applyMigrations NO lleva tabla de versión y RE-EJECUTA el .sql entero en CADA
//     arranque, así que se migra DOS veces sobre la misma BD —con una fila ya dentro— y se afirma que el
//     segundo arranque ni falla, ni duplica índices, ni pierde/duplica datos, ni afloja la unicidad.
func TestColaMigrationLandsInItsOwnFile(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()

	// --- Arranque real, paso 1: la BD ÚNICA del Edge (ambos sets: store + meta).
	edgeDB, err := Open(ctx, DialectSQLite, filepath.Join(dataDir, edgeDBFileName))
	if err != nil {
		t.Fatalf("Open(edge.db): %v", err)
	}
	defer func() { _ = edgeDB.Close() }()
	if err := Migrate(ctx, edgeDB); err != nil {
		t.Fatalf("Migrate(edge.db): %v", err)
	}

	// --- Arranque real, paso 2: la COLA, en su PROPIO fichero y con su PROPIO runner.
	colaDB, err := Open(ctx, DialectSQLite, filepath.Join(dataDir, colaDBFileName))
	if err != nil {
		t.Fatalf("Open(cola_entrantes.db): %v", err)
	}
	defer func() { _ = colaDB.Close() }()
	if err := MigrateCola(ctx, colaDB); err != nil {
		t.Fatalf("MigrateCola: %v", err)
	}

	// --- V1: la tabla y sus índices están en cola_entrantes.db...
	for _, tbl := range colaTables {
		if !tableExists(t, ctx, colaDB, tbl) {
			t.Errorf("MigrateCola debía crear la tabla %s en cola_entrantes.db", tbl)
		}
	}
	wantIdx := []string{"ix_cola_conv", "ix_cola_estado_seq", "ux_cola_session_wamid"}
	if got := indexesOf(t, ctx, colaDB, "cola_entrantes"); !equalCols(got, wantIdx) {
		t.Fatalf("índices de cola_entrantes = %v, esperaba %v", got, wantIdx)
	}
	if !indexIsUnique(t, ctx, colaDB, "cola_entrantes", "ux_cola_session_wamid") {
		t.Error("ux_cola_session_wamid debía ser UNIQUE: es la idempotencia local del encolado")
	}
	// ...y la cola NO arrastra el esquema de la BD única (separación en las dos direcciones).
	for _, tbl := range append(append([]string{}, storeTables...), metaTables...) {
		if tableExists(t, ctx, colaDB, tbl) {
			t.Errorf("cola_entrantes.db NO debía contener la tabla %s de edge.db", tbl)
		}
	}

	// --- V1 (el otro lado): la BD ÚNICA del Edge NO tiene la tabla fantasma.
	for _, tbl := range colaTables {
		if tableExists(t, ctx, edgeDB, tbl) {
			t.Errorf("edge.db NO debía contener %s: la cola vive en su propio fichero (db.go, MigrateCola)", tbl)
		}
	}

	// --- V2: idempotencia real del runner sin tabla de versión.
	// Una fila dentro ANTES del segundo arranque: así el re-run se prueba sobre una cola con datos, que es
	// el caso de campo (reinicio del daemon con la cola a medio drenar).
	if _, err := colaDB.ExecContext(ctx,
		`INSERT INTO cola_entrantes (seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc)
		 VALUES (1, 's1', 'c1@s.whatsapp.net', 'wamid-1', 100, x'00')`); err != nil {
		t.Fatalf("insert de prueba en cola_entrantes: %v", err)
	}

	// Segundo arranque: applyMigrations vuelve a ejecutar el .sql ENTERO.
	if err := MigrateCola(ctx, colaDB); err != nil {
		t.Fatalf("MigrateCola (2.º arranque) debía ser no-op y no falló: %v", err)
	}

	if got := indexesOf(t, ctx, colaDB, "cola_entrantes"); !equalCols(got, wantIdx) {
		t.Fatalf("tras re-migrar, índices de cola_entrantes = %v, esperaba %v (sin duplicar)", got, wantIdx)
	}
	var rows int
	if err := colaDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cola_entrantes`).Scan(&rows); err != nil {
		t.Fatalf("contar filas tras re-migrar: %v", err)
	}
	if rows != 1 {
		t.Errorf("filas de cola_entrantes tras re-migrar = %d, esperaba 1 (ni se pierde ni se duplica)", rows)
	}
	// La unicidad sigue vigente tras el re-run: el mismo (session_id, wa_message_id) no entra dos veces.
	_, err = colaDB.ExecContext(ctx,
		`INSERT INTO cola_entrantes (seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc)
		 VALUES (2, 's1', 'c1@s.whatsapp.net', 'wamid-1', 101, x'00')`)
	if err == nil {
		t.Error("ux_cola_session_wamid debía seguir rechazando el duplicado tras el 2.º arranque")
	}
}
