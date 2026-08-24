package db

// migrations_cola_test.go — la migración GUARDADA de la cola de entrantes (Plan 051 Ola 2 · T2.18).
//
// Lo que se prueba aquí NO es «la tabla se crea» (de eso va migrations_split_test.go), sino el caso que
// se coló entero por el hueco del runner: una BD de cola que YA EXISTE, creada por un binario anterior
// cuyo `CREATE TABLE` no declaraba `claim_token`. Sobre ella, `CREATE TABLE IF NOT EXISTS` es un no-op y
// la columna no aparecería nunca: el arranque no falla, `Enqueue` sigue insertando y el primer `Reclamar`
// muere con `no such column: claim_token`. El ALTER guardado (ensureColaClaimToken) es lo único que cierra
// ese camino, y estos tests son los que lo sostienen.
//
// Sin recursos externos (regla de T2.17): SQLite sobre un fichero en t.TempDir(), por el MISMO camino que
// producción (Open + MigrateCola), sin servidores ni variables de entorno.

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// colaSchemaViejo es el DDL de la cola TAL COMO ERA ANTES de que existiera `claim_token` (el 0001
// original). Se escribe a mano —y aquí sí, a propósito, en contra del criterio general de no replicar
// DDL— porque el fichero de migración de producción YA lleva la forma nueva: la única manera de fabricar
// una BD "vieja" es declararla.
const colaSchemaViejo = `
CREATE TABLE IF NOT EXISTS cola_entrantes (
    id            INTEGER PRIMARY KEY,
    seq           INTEGER NOT NULL,
    session_id    TEXT    NOT NULL,
    chat_jid      TEXT    NOT NULL,
    wa_message_id TEXT    NOT NULL,
    ts_whatsapp   INTEGER NOT NULL,
    texto_enc     BLOB    NOT NULL,
    meta_enc      BLOB,
    intent_json   TEXT,
    estado        TEXT    NOT NULL DEFAULT 'nuevo',
    tomado_en     INTEGER,
    despachado_en INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_cola_session_wamid ON cola_entrantes(session_id, wa_message_id);
CREATE INDEX IF NOT EXISTS ix_cola_estado_seq ON cola_entrantes(estado, seq);
CREATE INDEX IF NOT EXISTS ix_cola_conv ON cola_entrantes(session_id, chat_jid, estado);
`

// openColaDB abre una BD de cola vacía en t.TempDir() por el camino de producción (OpenCola, que es el
// constructor de ESTA BD desde T3.15 — no Open), SIN migrar.
func openColaDB(t *testing.T) *sql.DB {
	t.Helper()
	path := ColaDBPath(filepath.Join(t.TempDir(), "cola_entrantes.db"))
	database, err := OpenCola(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenCola: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

// tieneColumna indica si la tabla declara esa columna (sobre el helper columnsOf de migrations0002_test).
//
// ⚠️ El orden `(t, ctx, …)` es EL DEL PAQUETE, no un descuido: columnsOf, tableExists e indexesOf —los tres
// preexistentes de este mismo paquete— lo hacen así, y lo mismo los de cryptostore y cloudlink (11 helpers
// en total, cero excepciones). `revive:context-as-argument` pediría `ctx` primero, pero revive no está
// activo (el repo no tiene .golangci.yml, así que el gate «Lint» corre golangci-lint con sus linters por
// defecto y revive no está entre ellos). Si se adopta, se cambian todos a la vez o se configura
// `allowTypesBefore: "*testing.T"`, que es la opción que la propia regla trae para este caso.
func tieneColumna(t *testing.T, ctx context.Context, database *sql.DB, table, col string) bool {
	t.Helper()
	for _, c := range columnsOf(t, ctx, database, table) {
		if c == col {
			return true
		}
	}
	return false
}

// TestMigrateColaCreaClaimTokenEnBaseVirgen: el camino nuevo. Sobre una BD recién creada, MigrateCola deja
// `claim_token` presente aunque el .sql ya no la declare — la pone el ALTER guardado.
func TestMigrateColaCreaClaimTokenEnBaseVirgen(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t)

	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola: %v", err)
	}
	if !tieneColumna(t, ctx, database, "cola_entrantes", "claim_token") {
		t.Fatalf("cola_entrantes debía tener la columna claim_token tras MigrateCola; columnas: %v",
			columnsOf(t, ctx, database, "cola_entrantes"))
	}
	// Y es una columna USABLE, no solo un nombre en el pragma: el fence del cajero escribe y lee aquí.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO cola_entrantes (id, seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc, estado, claim_token)
		 VALUES (1, 1, 's1', 'c1@s.whatsapp.net', 'wa1', 1700000000, X'00', 'tomado', 'deadbeef')`); err != nil {
		t.Fatalf("insertar con claim_token sobre la BD migrada: %v", err)
	}
}

// TestMigrateColaAniadeClaimTokenSobreEsquemaViejo ES EL TEST QUE IMPORTA (T2.18): reproduce la BD que ya
// está en campo —creada por un binario cuyo CREATE TABLE no tenía la columna, y con filas dentro— y exige
// que MigrateCola la ponga al día SIN perder los datos.
//
// Sin el ALTER guardado este test falla en la aserción de la columna; y en producción el síntoma no sería
// este, sino un `no such column: claim_token` en el primer Reclamar, semanas después.
func TestMigrateColaAniadeClaimTokenSobreEsquemaViejo(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t)

	// 1. La BD "vieja": esquema sin claim_token y con una fila ya encolada.
	if _, err := database.ExecContext(ctx, colaSchemaViejo); err != nil {
		t.Fatalf("crear el esquema viejo de la cola: %v", err)
	}
	if _, err := database.ExecContext(ctx,
		`INSERT INTO cola_entrantes (id, seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc, estado)
		 VALUES (7, 42, 'sesion-vieja', 'chat@s.whatsapp.net', 'wa-viejo', 1700000000, X'0102', 'nuevo')`); err != nil {
		t.Fatalf("sembrar fila en la BD vieja: %v", err)
	}
	if tieneColumna(t, ctx, database, "cola_entrantes", "claim_token") {
		t.Fatalf("premisa del test rota: el esquema viejo NO debía traer claim_token")
	}

	// 2. Arranca el binario nuevo.
	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola sobre BD vieja: %v", err)
	}

	// 3. La columna está…
	if !tieneColumna(t, ctx, database, "cola_entrantes", "claim_token") {
		t.Fatalf("MigrateCola debía AÑADIR claim_token a una BD vieja; columnas: %v",
			columnsOf(t, ctx, database, "cola_entrantes"))
	}
	// …y los datos preexistentes SIGUEN AHÍ, con la columna nueva a NULL (que es justo el valor de "esta
	// fila no la tiene ningún cajero", el mismo que dejan el cierre de lote y el barrido de leases).
	var (
		seq      int64
		session  string
		waID     string
		estado   string
		tokenNew sql.NullString
	)
	if err := database.QueryRowContext(ctx,
		`SELECT seq, session_id, wa_message_id, estado, claim_token FROM cola_entrantes WHERE id = 7`).
		Scan(&seq, &session, &waID, &estado, &tokenNew); err != nil {
		t.Fatalf("leer la fila preexistente tras migrar: %v", err)
	}
	if seq != 42 || session != "sesion-vieja" || waID != "wa-viejo" || estado != "nuevo" {
		t.Fatalf("la fila preexistente no sobrevivió intacta: seq=%d session_id=%q wa_message_id=%q estado=%q",
			seq, session, waID, estado)
	}
	if tokenNew.Valid {
		t.Fatalf("claim_token de una fila preexistente debía ser NULL, es %q", tokenNew.String)
	}

	// 4. Y la columna es usable: el fence puede escribirse sobre esa misma fila vieja.
	if _, err := database.ExecContext(ctx,
		`UPDATE cola_entrantes SET estado = 'tomado', tomado_en = 1700000060, claim_token = 'cafe1234' WHERE id = 7`); err != nil {
		t.Fatalf("escribir claim_token sobre la fila migrada: %v", err)
	}
}

// TestMigrateColaEsIdempotente: el runner re-ejecuta los .sql en CADA arranque y el ALTER no es
// idempotente por sí mismo, así que la segunda pasada es exactamente donde un `ADD COLUMN` pelado
// reventaría con "duplicate column". Tres pasadas: la 1.ª crea, la 2.ª y la 3.ª deben ser no-op.
func TestMigrateColaEsIdempotente(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t)

	for i := 1; i <= 3; i++ {
		if err := MigrateCola(ctx, database); err != nil {
			t.Fatalf("MigrateCola (pasada %d) debía ser no-op y falló: %v", i, err)
		}
	}
	cols := columnsOf(t, ctx, database, "cola_entrantes")
	var n int
	for _, c := range cols {
		if c == "claim_token" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("claim_token debía aparecer UNA vez tras tres migraciones, aparece %d; columnas: %v", n, cols)
	}
}

// TestMigrateColaIdempotenteSobreEsquemaViejo: la misma idempotencia, pero por el camino del ALTER (una BD
// vieja migra en la 1.ª pasada y las siguientes tienen que ver la columna ya puesta y callarse).
func TestMigrateColaIdempotenteSobreEsquemaViejo(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t)

	if _, err := database.ExecContext(ctx, colaSchemaViejo); err != nil {
		t.Fatalf("crear el esquema viejo de la cola: %v", err)
	}
	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola (1.ª pasada sobre BD vieja): %v", err)
	}
	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola (2.ª pasada sobre BD vieja) debía ser no-op y falló: %v", err)
	}
	if !tieneColumna(t, ctx, database, "cola_entrantes", "claim_token") {
		t.Fatalf("claim_token debía seguir presente tras la 2.ª pasada")
	}
}

// TestEnsureColaClaimTokenSinLaTablaDaUnErrorQueApuntaALaCAUSA: el paso guardado sobre una BD donde
// `cola_entrantes` NO existe.
//
// 🔴 EL MODO DE FALLO QUE CIERRA ES DE DIAGNÓSTICO, no de corrección. `PRAGMA table_info` de una tabla
// inexistente NO falla: devuelve cero filas. Sin la guarda explícita, el mapa `existing` sale vacío, el
// flujo cae al ALTER y lo que llega al operador es `db: añadir columna "claim_token" a cola_entrantes: no
// such table: cola_entrantes` — un mensaje que habla de una columna cuando el problema es que no se aplicó
// el set de migraciones (BD equivocada, embed roto, applyMigrations que no corrió). Se pierde media hora
// mirando el ALTER.
func TestEnsureColaClaimTokenSinLaTablaDaUnErrorQueApuntaALaCAUSA(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t) // abierta, VACÍA y SIN migrar

	err := ensureColaClaimToken(ctx, database)
	if err == nil {
		t.Fatal("ensureColaClaimToken sobre una BD sin la tabla debía fallar con un error explícito")
	}
	msg := err.Error()
	for _, frag := range []string{"cola_entrantes", "no existe", colaMigrationsDir} {
		if !strings.Contains(msg, frag) {
			t.Errorf("el error debía mencionar %q para que se entienda la causa; es: %v", frag, err)
		}
	}
	// Y NO debe ser el error crudo del driver: si aparece, es que la guarda no se ejecutó y el ALTER llegó
	// a emitirse.
	if strings.Contains(msg, "no such table") {
		t.Errorf("el error viene del ALTER, no de la guarda: la comprobación de tabla ausente no está actuando (%v)", err)
	}

	// Y la vía normal (MigrateCola, que crea la tabla ANTES) sigue sin ver este camino.
	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola sobre la misma BD debía funcionar: %v", err)
	}
	if !tieneColumna(t, ctx, database, "cola_entrantes", "claim_token") {
		t.Fatal("tras MigrateCola la columna claim_token debía estar")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// T2.19 · la columna `intentos` (el freno del lote venenoso)
// ─────────────────────────────────────────────────────────────────────────────
//
// Mismo camino y mismo peligro que `claim_token`, y por eso los tests son los mismos: sobre una cola que
// YA EXISTE en disco —y todas las de campo existen— editar el `CREATE TABLE` del 0001 no añade nada.
// Sin el ALTER guardado, el arranque no falla, `Enqueue` sigue insertando y el primer `Reclamar` muere con
// `no such column: intentos`, dejando la cola sin vaciar.

// TestMigrateColaCreaIntentosEnBaseVirgen: sobre una BD recién creada, MigrateCola deja `intentos`
// presente y usable aunque el .sql no la declare.
func TestMigrateColaCreaIntentosEnBaseVirgen(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t)

	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola: %v", err)
	}
	if !tieneColumna(t, ctx, database, "cola_entrantes", "intentos") {
		t.Fatalf("cola_entrantes debía tener la columna intentos tras MigrateCola; columnas: %v",
			columnsOf(t, ctx, database, "cola_entrantes"))
	}

	// EL DEFAULT ES LO QUE SE PRUEBA AQUÍ, no la presencia: el INSERT no nombra `intentos` (igual que el
	// de Enqueue en producción, que se escribió antes de que la columna existiera), así que una columna
	// sin `DEFAULT 0` reventaría por el NOT NULL y una nullable dejaría NULL — y `intentos + 1` sobre NULL
	// da NULL, o sea un contador que no avanza NUNCA y un freno que no frena.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO cola_entrantes (id, seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc, estado)
		 VALUES (1, 1, 's1', 'c1@s.whatsapp.net', 'wa1', 1700000000, X'00', 'nuevo')`); err != nil {
		t.Fatalf("insertar SIN nombrar intentos sobre la BD migrada: %v", err)
	}
	var intentos sql.NullInt64
	if err := database.QueryRowContext(ctx, `SELECT intentos FROM cola_entrantes WHERE id = 1`).Scan(&intentos); err != nil {
		t.Fatalf("leer intentos: %v", err)
	}
	if !intentos.Valid || intentos.Int64 != 0 {
		t.Fatalf("una fila nueva debe nacer con intentos = 0 (NOT NULL DEFAULT 0), got %+v", intentos)
	}
	// Y el incremento del claim funciona sobre ella (que es lo único para lo que existe la columna).
	if _, err := database.ExecContext(ctx, `UPDATE cola_entrantes SET intentos = intentos + 1 WHERE id = 1`); err != nil {
		t.Fatalf("incrementar intentos: %v", err)
	}
	if err := database.QueryRowContext(ctx, `SELECT intentos FROM cola_entrantes WHERE id = 1`).Scan(&intentos); err != nil {
		t.Fatalf("releer intentos: %v", err)
	}
	if intentos.Int64 != 1 {
		t.Fatalf("intentos tras un incremento: got %d want 1", intentos.Int64)
	}
}

// TestMigrateColaAniadeIntentosSobreEsquemaViejo ES EL TEST QUE IMPORTA (T2.19): la BD que ya está en
// campo —creada por un binario cuyo CREATE TABLE no tenía la columna, y con filas dentro— se pone al día
// SIN perder datos, y las filas preexistentes quedan con 0, no con NULL.
//
// Ese 0 es la mitad del arreglo: una fila vieja con NULL nunca incrementaría (`NULL + 1` es NULL), así que
// el corte del cajero jamás se dispararía sobre las filas más antiguas — que son precisamente las que
// llevan más tiempo atascadas y las candidatas naturales a ser el lote venenoso.
func TestMigrateColaAniadeIntentosSobreEsquemaViejo(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t)

	if _, err := database.ExecContext(ctx, colaSchemaViejo); err != nil {
		t.Fatalf("crear el esquema viejo de la cola: %v", err)
	}
	if _, err := database.ExecContext(ctx,
		`INSERT INTO cola_entrantes (id, seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc, estado)
		 VALUES (7, 42, 'sesion-vieja', 'chat@s.whatsapp.net', 'wa-viejo', 1700000000, X'0102', 'nuevo')`); err != nil {
		t.Fatalf("sembrar fila en la BD vieja: %v", err)
	}
	if tieneColumna(t, ctx, database, "cola_entrantes", "intentos") {
		t.Fatalf("premisa del test rota: el esquema viejo NO debía traer intentos")
	}

	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola sobre BD vieja: %v", err)
	}

	if !tieneColumna(t, ctx, database, "cola_entrantes", "intentos") {
		t.Fatalf("MigrateCola debía AÑADIR intentos a una BD vieja; columnas: %v",
			columnsOf(t, ctx, database, "cola_entrantes"))
	}
	var (
		seq      int64
		session  string
		estado   string
		intentos sql.NullInt64
	)
	if err := database.QueryRowContext(ctx,
		`SELECT seq, session_id, estado, intentos FROM cola_entrantes WHERE id = 7`).
		Scan(&seq, &session, &estado, &intentos); err != nil {
		t.Fatalf("leer la fila preexistente tras migrar: %v", err)
	}
	if seq != 42 || session != "sesion-vieja" || estado != "nuevo" {
		t.Fatalf("la fila preexistente no sobrevivió intacta: seq=%d session_id=%q estado=%q", seq, session, estado)
	}
	if !intentos.Valid {
		t.Fatal("una fila preexistente debe quedar con intentos = 0, NO con NULL: con NULL el `intentos + 1` " +
			"del claim seguiría dando NULL y el freno del lote venenoso no se dispararía nunca")
	}
	if intentos.Int64 != 0 {
		t.Fatalf("intentos de una fila preexistente: got %d want 0", intentos.Int64)
	}

	// Y la fila migrada cuenta: el UPDATE del claim la incrementa como a cualquier otra.
	if _, err := database.ExecContext(ctx, `UPDATE cola_entrantes SET intentos = intentos + 1 WHERE id = 7`); err != nil {
		t.Fatalf("incrementar intentos sobre la fila migrada: %v", err)
	}
	if err := database.QueryRowContext(ctx, `SELECT intentos FROM cola_entrantes WHERE id = 7`).Scan(&intentos); err != nil {
		t.Fatalf("releer intentos de la fila migrada: %v", err)
	}
	if intentos.Int64 != 1 {
		t.Fatalf("intentos de la fila migrada tras un incremento: got %d want 1", intentos.Int64)
	}
}

// TestMigrateColaIntentosEsIdempotente: el runner re-ejecuta todo en cada arranque, así que la 2.ª y 3.ª
// pasadas son donde un `ADD COLUMN` pelado reventaría con "duplicate column". Y el valor ya contado NO
// puede reiniciarse: un re-arranque del cajero que pusiera los intentos a cero devolvería al lote venenoso
// su inmunidad, que es justo lo que este freno viene a quitarle.
func TestMigrateColaIntentosEsIdempotente(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t)

	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola (1.ª pasada): %v", err)
	}
	if _, err := database.ExecContext(ctx,
		`INSERT INTO cola_entrantes (id, seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc, estado, intentos)
		 VALUES (1, 1, 's1', 'c1@s.whatsapp.net', 'wa1', 1700000000, X'00', 'nuevo', 2)`); err != nil {
		t.Fatalf("sembrar fila con intentos ya contados: %v", err)
	}

	for i := 2; i <= 3; i++ {
		if err := MigrateCola(ctx, database); err != nil {
			t.Fatalf("MigrateCola (pasada %d) debía ser no-op y falló: %v", i, err)
		}
	}

	cols := columnsOf(t, ctx, database, "cola_entrantes")
	var n int
	for _, c := range cols {
		if c == "intentos" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("intentos debía aparecer UNA vez tras tres migraciones, aparece %d; columnas: %v", n, cols)
	}
	var intentos int64
	if err := database.QueryRowContext(ctx, `SELECT intentos FROM cola_entrantes WHERE id = 1`).Scan(&intentos); err != nil {
		t.Fatalf("leer intentos tras remigrar: %v", err)
	}
	if intentos != 2 {
		t.Fatalf("remigrar NO puede reiniciar el contador de una fila viva: got %d want 2", intentos)
	}
}

// TestEnsureColaIntentosSinLaTablaDaUnErrorQueApuntaALaCAUSA: la misma guarda que su gemela, por la misma
// razón — `PRAGMA table_info` de una tabla inexistente devuelve cero filas en vez de fallar, y sin la
// guarda el operador recibiría un `no such table` envuelto en un mensaje que habla de añadir una columna.
func TestEnsureColaIntentosSinLaTablaDaUnErrorQueApuntaALaCAUSA(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t) // abierta, VACÍA y SIN migrar

	err := ensureColaIntentos(ctx, database)
	if err == nil {
		t.Fatal("ensureColaIntentos sobre una BD sin la tabla debía fallar con un error explícito")
	}
	msg := err.Error()
	for _, frag := range []string{"cola_entrantes", "no existe", colaMigrationsDir} {
		if !strings.Contains(msg, frag) {
			t.Errorf("el error debía mencionar %q para que se entienda la causa; es: %v", frag, err)
		}
	}
	if strings.Contains(msg, "no such table") {
		t.Errorf("el error viene del ALTER, no de la guarda: la comprobación de tabla ausente no está actuando (%v)", err)
	}

	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola sobre la misma BD debía funcionar: %v", err)
	}
	if !tieneColumna(t, ctx, database, "cola_entrantes", "intentos") {
		t.Fatal("tras MigrateCola la columna intentos debía estar")
	}
}

// TestNingunIndiceDeLaColaDependeDeClaimToken guarda la decisión del punto 3 de T2.18: si algún
// `CREATE INDEX` del .sql mencionara `claim_token`, sobre una BD VIEJA se ejecutaría ANTES del ALTER que
// crea la columna y la migración reventaría en el arranque. Hoy ninguno lo hace; este test lo mantiene así.
func TestNingunIndiceDeLaColaDependeDeClaimToken(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t)
	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola: %v", err)
	}

	rows, err := database.QueryContext(ctx,
		`SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='cola_entrantes'`)
	if err != nil {
		t.Fatalf("listar índices de cola_entrantes: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			name string
			ddl  sql.NullString
		)
		if err := rows.Scan(&name, &ddl); err != nil {
			t.Fatal(err)
		}
		if ddl.Valid && strings.Contains(ddl.String, "claim_token") {
			t.Fatalf("el índice %s menciona claim_token: tiene que crearse DESPUÉS del ALTER (en ensureColaClaimToken), no en el .sql", name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

// TestMigrateColaCreaParteWorkerSobreColaPreexistente es el gemelo de T4.5 del test de claim_token, y
// existe porque el fallo que cubre es INVISIBLE POR DISEÑO.
//
// La Ola 4 decidió —bien— que un fallo al publicar el parte NO puede tumbar el bucle del cajero: es
// telemetría, y el trabajo es la cola. Sale por un log.Warn y el bucle sigue. Combínalo con una tabla
// `parte_worker` que no llegara a crearse sobre las colas QUE YA EXISTEN en UAT y en campo, y el
// resultado es el peor de los posibles: el cajero funciona perfectamente, cada publicación falla con
// `no such table: parte_worker`, y `intent_circuit` viaja VACÍO para siempre. Vacío es exactamente lo
// que la ola define como «no lo sé» ⇒ el síntoma de la tabla ausente sería INDISTINGUIBLE del síntoma
// de un cajero muerto. Sería un canal de salud que falla en el único modo que no sabe reportar.
//
// Que `applyMigrations` no lleve tabla de versión y re-ejecute los .sql en cada arranque es lo que
// hace que esto funcione — pero eso era una afirmación LEÍDA. Este test la ejecuta.
func TestMigrateColaCreaParteWorkerSobreColaPreexistente(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t)

	// 1. La cola que ya está en disco: nacida de un binario ANTERIOR al 0002, sin parte_worker.
	if _, err := database.ExecContext(ctx, colaSchemaViejo); err != nil {
		t.Fatalf("crear el esquema viejo de la cola: %v", err)
	}
	if tableExists(t, ctx, database, "parte_worker") {
		t.Fatal("premisa del test rota: el esquema viejo NO debía traer parte_worker")
	}

	// 2. Arranca el binario nuevo sobre ella.
	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola sobre una cola preexistente: %v", err)
	}

	// 3. La tabla está…
	if !tableExists(t, ctx, database, "parte_worker") {
		t.Fatal("MigrateCola debía CREAR parte_worker sobre una cola que ya existía; sin ella, el cajero " +
			"corre perfecto y intent_circuit viaja vacío para siempre, indistinguible de un cajero muerto")
	}

	// 4. …y es USABLE por el camino real del cajero: fila única (id=1) con UPSERT. Comprobar solo el
	//    sqlite_master dejaría pasar una tabla creada con otra forma —y el fallo volvería a ser un Warn
	//    que nadie mira.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO parte_worker (id, ts_unix, circuito, taskset, p50_ms) VALUES (1, 1700000000, 'closed', 'disjunta', 1234)
		 ON CONFLICT(id) DO UPDATE SET ts_unix=excluded.ts_unix, circuito=excluded.circuito,
		                               taskset=excluded.taskset, p50_ms=excluded.p50_ms`); err != nil {
		t.Fatalf("publicar el parte sobre la cola migrada: %v", err)
	}
	var circuito string
	if err := database.QueryRowContext(ctx, `SELECT circuito FROM parte_worker WHERE id = 1`).Scan(&circuito); err != nil {
		t.Fatalf("leer el parte recién publicado: %v", err)
	}
	if circuito != "closed" {
		t.Fatalf("el parte leído no es el publicado: got %q, want %q", circuito, "closed")
	}

	// 5. Y un segundo arranque no rompe nada (el runner re-ejecuta los .sql SIEMPRE).
	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola (2.º arranque) debía ser no-op y falló: %v", err)
	}
	if err := database.QueryRowContext(ctx, `SELECT circuito FROM parte_worker WHERE id = 1`).Scan(&circuito); err != nil {
		t.Fatalf("el 2.º arranque no debía borrar el parte: %v", err)
	}
	if circuito != "closed" {
		t.Fatalf("el 2.º arranque alteró el parte: got %q", circuito)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Plan 044 · Ola 1.7 · T1.7-5 — las SEIS columnas del reparto de la inferencia
// ─────────────────────────────────────────────────────────────────────────────

// parteWorkerSchemaViejo es `parte_worker` TAL COMO LA CREÓ EL 0002: cinco columnas, sin el reparto de la
// inferencia. Se escribe a mano por el mismo motivo que colaSchemaViejo —el .sql de producción ya trae la
// forma de hoy—, y es la ÚNICA manera de fabricar el estado que este test tiene que probar.
//
// 🔴 SIN ESTE DDL EL TEST SERÍA HUECO. Sobre una BD recién migrada las seis columnas están porque el
// propio MigrateCola acaba de ponerlas: comprobarlas ahí demuestra que el paso corre, no que SIRVA. Lo
// que hay que probar es el caso de campo —el VPS de UAT y cualquier instalación con un binario desde
// T4.5— donde la tabla YA existe con cinco columnas y `CREATE TABLE IF NOT EXISTS` es un no-op.
const parteWorkerSchemaViejo = `
CREATE TABLE IF NOT EXISTS parte_worker (
    id       INTEGER PRIMARY KEY CHECK (id = 1),
    ts_unix  INTEGER NOT NULL,
    circuito TEXT    NOT NULL DEFAULT '',
    taskset  TEXT    NOT NULL DEFAULT '',
    p50_ms   INTEGER NOT NULL DEFAULT 0
);
`

// TestMigrateColaAniadeElRepartoSobreUnParteViejo es el test de campo: una cola con `parte_worker` de
// CINCO columnas y una fila dentro, como la que tiene ahora mismo cualquier Edge desplegado.
//
// Lo que se comprueba en orden: que la premisa es cierta (las columnas NO están), que tras migrar SÍ
// están, que la fila que ya existía SOBREVIVE con el default en las nuevas —el UPSERT del cajero no
// reescribe datos— y que el UPSERT de once columnas funciona sobre ella.
func TestMigrateColaAniadeElRepartoSobreUnParteViejo(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t)

	if _, err := database.ExecContext(ctx, colaSchemaViejo+parteWorkerSchemaViejo); err != nil {
		t.Fatalf("crear el esquema viejo: %v", err)
	}
	// Un parte que ya estaba publicado, con datos que NO se pueden perder al migrar.
	if _, err := database.ExecContext(ctx,
		`INSERT INTO parte_worker (id, ts_unix, circuito, taskset, p50_ms)
		 VALUES (1, 1700000000, 'closed', 'disjunta', 8100)`); err != nil {
		t.Fatalf("sembrar el parte viejo: %v", err)
	}
	for _, col := range parteInferenciaColumnas {
		if tieneColumna(t, ctx, database, "parte_worker", col.nombre) {
			t.Fatalf("premisa del test rota: el esquema viejo NO debía traer %q", col.nombre)
		}
	}

	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola sobre un parte_worker preexistente: %v", err)
	}

	for _, col := range parteInferenciaColumnas {
		if !tieneColumna(t, ctx, database, "parte_worker", col.nombre) {
			t.Errorf("MigrateCola no añadió %q: el primer PublicarParte del cajero moriría con `no such "+
				"column`, y el daemon publicaría intent_circuit VACÍO a los 90 s", col.nombre)
		}
	}

	// La fila que ya estaba sigue ahí, con lo suyo intacto y las nuevas a su DEFAULT (nunca NULL: un NULL
	// reventaría el Scan del lector, que no usa sql.NullString).
	var (
		circuito      string
		p50           int64
		prefillN      int64
		regimenesJSON string
	)
	if err := database.QueryRowContext(ctx,
		`SELECT circuito, p50_ms, prefill_muestras, regimenes_json FROM parte_worker WHERE id = 1`).
		Scan(&circuito, &p50, &prefillN, &regimenesJSON); err != nil {
		t.Fatalf("leer el parte migrado: %v", err)
	}
	if circuito != "closed" || p50 != 8100 {
		t.Errorf("la migración pisó datos del parte viejo: circuito=%q p50=%d", circuito, p50)
	}
	if prefillN != 0 || regimenesJSON != "" {
		t.Errorf("las columnas nuevas debían nacer a su cero: prefill_muestras=%d regimenes_json=%q",
			prefillN, regimenesJSON)
	}

	// Y un segundo arranque no reemite los ALTER (el runner re-ejecuta los .sql SIEMPRE, y un `ADD COLUMN`
	// repetido revienta con «duplicate column»: es la razón entera de que estos pasos sean guardados).
	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola (2.º arranque) debía ser no-op y falló: %v", err)
	}
}

// TestMigrateColaCreaElRepartoEnBaseVirgen: sobre una BD nueva las seis columnas también aparecen, aunque
// el `CREATE TABLE` del 0002 no las declare. Es el gemelo del caso de arriba y el que garantiza que una
// instalación NUEVA y una MIGRADA acaban con el mismo esquema — dos formas distintas serían un bug que
// sólo se ve en producción.
func TestMigrateColaCreaElRepartoEnBaseVirgen(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t)

	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola: %v", err)
	}
	for _, col := range parteInferenciaColumnas {
		if !tieneColumna(t, ctx, database, "parte_worker", col.nombre) {
			t.Errorf("en base virgen falta %q", col.nombre)
		}
	}
}

// TestEnsureParteInferenciaSinLaTablaDaUnErrorQueApuntaALaCAUSA: `PRAGMA table_info` de una tabla que no
// existe NO falla, devuelve cero filas. Sin la guarda, el operador recibiría un `no such table:
// parte_worker` envuelto en un mensaje que habla de «añadir columna prefill_p50_ms» — apuntando al sitio
// equivocado cuando el fallo real es que el 0002 no se aplicó.
func TestEnsureParteInferenciaSinLaTablaDaUnErrorQueApuntaALaCAUSA(t *testing.T) {
	ctx := context.Background()
	database := openColaDB(t)

	err := ensureParteInferencia(ctx, database)
	if err == nil {
		t.Fatal("sin la tabla parte_worker, ensureParteInferencia debía fallar")
	}
	if !strings.Contains(err.Error(), "no existe") {
		t.Errorf("el error no apunta a la causa (la tabla ausente): %v", err)
	}
}
