// Package db abre la BD del Edge y aplica sus migraciones embebidas, separadas en DOS sets:
//
//   - set "store" (migrations/store, hoy 0001_init.sql → tablas msg_enc_*): es el esquema del
//     cryptostore. Solo material whatsmeow cifrado campo a campo con la DEK del dispositivo.
//   - set "meta"  (migrations/meta, hoy 0002_sessions.sql + 0003_sessions_multi.sql +
//     0004_accounts_devices.sql → tablas accounts/devices; sessions/sessions_v2 son legacy):
//     metadatos de NEGOCIO en claro de las sesiones (número/JID/estado/rol/timestamps).
//
// Desde el Plan 051 hay además un TERCER set, "cola" (migrations/cola → tabla cola_entrantes), que NO
// va a edge.db sino a un fichero APARTE (<data_dir>/cola_entrantes.db): se abre con OpenCola (perfil de
// escritura propio, T3.15) y se aplica solo con MigrateCola, nunca desde Migrate. Ver los comentarios de
// esas dos funciones para el porqué de la separación.
//
// MODELO BD ÚNICA (ADR-0018, Plan 022): el Edge usa UNA sola *sql.DB (<data_dir>/edge.db en SQLite,
// o la cadena Postgres) que aloja AMBOS sets — metadatos (accounts/devices), el Container whatsmeow
// compartido y el store cifrado per-device (msg_enc_*) — retirando el modelo previo de un store.db
// POR SESIÓN + una db CENTRAL de metadatos. El daemon `serve` la abre con Open (por dialecto) y aplica
// los dos sets con Migrate. La DEK sigue custodiada FUERA de la BD, por dispositivo (Plan 022 §3).
//
// El driver por defecto es modernc.org/sqlite (CGO_ENABLED=0, sin SQLCipher): el fichero .db NO se
// cifra a nivel de página; el cryptostore (internal/adapters/cryptostore) cifra CADA campo sensible con
// la DEK antes de escribirlo. Por eso aquí solo nos ocupamos de: abrir la BD con permisos 0600 (SQLite),
// fijar los pragmas (WAL, foreign_keys, busy_timeout + el perfil de escritura, ver sqliteTuning) y aplicar
// el set de migración que corresponda.
//
// Helpers legacy single-sesión (OpenAndMigrate = ambos sets a una db; OpenSessionStore = solo el set
// store; OpenAndMigrateMeta/MigrateMeta/MigrateStore = un set) se CONSERVAN como COSTURA DE TESTS:
// verificado 2026-08-12, ningún camino de producción los llama —solo los ejercitan los tests de este
// paquete, de cryptostore, sessionstore y edgemigrate—. El daemon `serve` usa Open+Migrate.
package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"

	_ "modernc.org/sqlite" // driver "sqlite" (CGO-free)
)

// embeddedMigrations embebe los sets de migraciones (store/, meta/ y cola/). Cada set se aplica en
// orden lexicográfico del nombre dentro de su subdirectorio (el prefijo NNNN_ garantiza el orden). El
// patrón NO es recursivo: cada subdirectorio nuevo hay que añadirlo explícitamente aquí.
//
//go:embed migrations/store/*.sql migrations/meta/*.sql migrations/cola/*.sql
var embeddedMigrations embed.FS

// migrationsFS es la fuente de las migraciones. Es una var (no la embed.FS directa) para que los
// tests puedan inyectar un FS que fuerce los errores de lectura, normalmente inalcanzables con embed.
var migrationsFS fs.FS = embeddedMigrations

// Subdirectorios de cada set de migración dentro de migrationsFS.
const (
	// storeMigrationsDir aloja el esquema del store cifrado del cryptostore (tablas msg_enc_*).
	storeMigrationsDir = "migrations/store"
	// metaMigrationsDir aloja el esquema de metadatos de negocio (tablas accounts/devices; sessions/
	// sessions_v2 son legacy).
	metaMigrationsDir = "migrations/meta"
	// colaMigrationsDir aloja el esquema de la COLA DE ENTRANTES (tabla cola_entrantes, Plan 051). Va a
	// una BD PROPIA (<data_dir>/cola_entrantes.db), NO a edge.db: ver MigrateCola.
	colaMigrationsDir = "migrations/cola"
)

// Dialectos SQL soportados por Open (Plan 022 T0, design §5). El default del Edge es SQLite embebido
// pure-Go (ADR-0002); Postgres es OPCIONAL y solo se enlaza al compilar con el build-tag `postgres`
// (ver db_postgres.go / db_nopostgres.go): el binario default nunca importa un driver Postgres.
const (
	DialectSQLite   = "sqlite"
	DialectPostgres = "postgres"
)

// Open abre la BD ÚNICA del Edge según el dialecto (Plan 022 T0, design §5). Es el punto de entrada
// CONMUTABLE por config (WAPP_AGENT_DB_DIALECT + WAPP_AGENT_DB_DSN):
//
//   - DialectSQLite (default, "" también): abre/crea el fichero SQLite en dsn con permisos 0600 y deja
//     la conexión lista (journal_mode=WAL, foreign_keys=ON, busy_timeout=5s, más el perfil de escritura
//     CONSERVADOR defaultTuning: synchronous=FULL, wal_autocheckpoint=1000) con UN único escritor
//     (SetMaxOpenConns(1)). Driver modernc.org/sqlite (CGO-free, pure-Go): el .db NO se cifra a nivel
//     de página; el cryptostore cifra cada campo sensible con la DEK.
//   - DialectPostgres: abre la conexión con el driver enlazado SOLO bajo el build-tag `postgres`
//     (openPostgres en db_postgres.go); pool con los defaults de database/sql (Postgres gestiona su
//     propia concurrencia, SIN el escritor único de SQLite). Sin el tag devuelve ErrPostgresNotCompiled.
//
// WAL/PRAGMA y el escritor único son EXCLUSIVOS de SQLite (no existen en Postgres); Open los aplica
// solo en la rama SQLite.
//
// ⚠️ LA COLA DE ENTRANTES NO SE ABRE CON Open: usa OpenCola, que aplica un perfil de escritura DISTINTO
// (Plan 051 · T3.15). Y desde T3.16 esto ya NO es solo una advertencia en un comentario: la ruta de la
// cola tiene tipo propio (ColaDBPath), así que `Open(ctx, dialect, layout.ColaDB())` NO COMPILA. Ver
// ColaDBPath para el porqué de esa barrera.
func Open(ctx context.Context, dialect, dsn string) (*sql.DB, error) {
	switch dialect {
	case DialectSQLite, "":
		return openSQLite(ctx, dsn, defaultTuning)
	case DialectPostgres:
		return openPostgres(ctx, dsn)
	default:
		return nil, fmt.Errorf("db: dialecto no soportado: %q", dialect)
	}
}

// ColaDBPath es la ruta del fichero de la COLA DE ENTRANTES (<data_dir>/cola_entrantes.db). Es un TIPO
// PROPIO y no un `string` a propósito, y esa es toda su razón de ser: hace que el error no se pueda
// ESCRIBIR (Plan 051 · T3.16).
//
// EL FALLO QUE ESTE TIPO EXISTE PARA IMPEDIR. El perfil de escritura de la cola (colaTuning:
// synchronous=NORMAL + WAL 4× más ancho) lo aplica OpenCola AL ABRIR, y `synchronous`/`wal_autocheckpoint`
// son PRAGMAs POR-CONEXIÓN. La cola la abren DOS PROCESOS DISTINTOS —`agent serve`
// (internal/infra/daemon), que hace el Enqueue del handler, y `agent cajero` (cmd/agent/cajero.go), que
// hace los UPDATE de lote— y el perfil solo sirve si lo aplican LOS DOS: si uno se quedara en Open(),
// seguiría haciendo fsync en cada commit y disparando checkpoints cada 4 MiB en mitad del tráfico del
// otro, que es EXACTAMENTE el mecanismo medido detrás de los picos de 250-471 ms en el p99 del handler
// (PC-11). Media palanca no arregla nada.
//
// Y ese error era invisible: con la ruta siendo un `string` pelado, revertir una de las dos líneas a
// `db.Open(ctx, dialecto, layout.ColaDB())` COMPILA, pasa todos los tests —los que hay miran los pragmas
// EFECTIVOS de una conexión abierta con OpenCola, no quién la abre en producción— y el pragma
// simplemente no se aplica en campo. Un guardarraíl de test podía cazarlo; un tipo lo hace imposible de
// teclear, que es la línea que ya se siguió en T3.13 con buildLatencia.
//
// CÓMO FUNCIONA LA BARRERA: Go no convierte implícitamente entre un tipo nombrado y su subyacente, así
// que ColaDBPath no encaja en el `dsn string` de Open ni en ningún otro constructor de este paquete —solo
// en OpenCola—. La ruta se construye en UN solo sitio (sessionmgr.Layout.ColaDB, la fuente de verdad del
// layout en disco) y llega tipada hasta aquí. Sí sigue siendo formateable sin ceremonia (`%s` en un
// fmt.Errorf, valor de un par clave/valor del logger): el tipo estorba justo donde tiene que estorbar y
// en ningún otro sitio.
//
// LO QUE EL COMPILADOR NO PUEDE IMPEDIR es que alguien escriba la conversión explícita
// `db.Open(ctx, d, string(layout.ColaDB()))`. Eso ya no es un descuido —es una frase que hay que querer
// escribir—, y de ella se ocupa el guardarraíl de cableado (cola_cableado_ast_test.go), que además vigila
// que las dos aperturas de producción sigan existiendo.
type ColaDBPath string

// OpenCola abre la BD PROPIA de la cola de entrantes (<data_dir>/cola_entrantes.db, Layout.ColaDB())
// —SQLite siempre, como MigrateCola— con el PERFIL DE ESCRITURA de la cola (colaTuning) en vez del
// perfil conservador del resto de bases del Edge. Es la ÚNICA vía por la que se puede abrir ese fichero:
// su parámetro es ColaDBPath, el tipo que solo produce Layout.ColaDB(), así que la exclusividad ya no
// depende de que el llamante lea este comentario. El porqué —los pragmas por-conexión y los dos procesos
// escritores— está en el doc de ColaDBPath.
//
// NO migra: el llamante sigue haciendo MigrateCola con su propio *sql.DB (ver el doc de MigrateCola para
// por qué la cola no se migra desde Migrate). Abrir y migrar siguen separados porque hay tres llamantes y
// no todos migran en el mismo punto del arranque.
func OpenCola(ctx context.Context, path ColaDBPath) (*sql.DB, error) {
	return openSQLite(ctx, string(path), colaTuning)
}

// sqliteTuning es el PERFIL DE DURABILIDAD Y CHECKPOINTING de una BD SQLite del Edge: los dos pragmas
// que openSQLite NO puede fijar igual para todas sus bases porque la respuesta correcta depende de qué
// guarda cada fichero y de cuántos procesos escriben en él. El resto de pragmas (WAL, foreign_keys,
// busy_timeout) sí son universales y viven fuera de aquí.
//
// Los DOS son PRAGMAs por-conexión, igual que foreign_keys: es el mismo motivo por el que openSQLite fija
// SetMaxOpenConns(1) (una sola conexión ⇒ el pragma aplicado al abrir rige TODAS las operaciones, sin que
// database/sql pueda abrir por detrás conexiones sin él). Cambiar ese pool obligaría a fijar también
// estos dos en cada conexión nueva.
type sqliteTuning struct {
	// synchronous es el valor de PRAGMA synchronous ("FULL" o "NORMAL"): cuándo se hace fsync.
	synchronous string
	// walAutocheckpoint es el umbral de PRAGMA wal_autocheckpoint EN PÁGINAS (page_size = 4096 B en este
	// driver, verificado): a partir de cuántas páginas de WAL un commit arrastra además un checkpoint.
	walAutocheckpoint int
}

// colaWALAutocheckpointPages es el umbral de checkpoint automático de la cola de entrantes, EN PÁGINAS.
// Con el page_size de 4096 B del driver son 16 MiB de WAL, cuatro veces el default de SQLite (1000
// páginas = 4 MiB).
//
// EL CRITERIO, que sale de una medición y no de un número redondo: en el VPS, con la cola drenando ~1500
// filas de backlog, el WAL se midió en 4.136.512 B contra un umbral de 4.096.000 B. Es decir, el WAL vivía
// PEGADO al techo: cada commit del drenaje disparaba un checkpoint, y un checkpoint BLOQUEA A LOS
// ESCRITORES —los dos, el que encola y el que cierra lotes, que son procesos distintos sobre el mismo
// fichero—. Ese es el mecanismo que metía las esperas de 250-471 ms en el p99 del handler (PC-11).
//
// El trabajo que hay que dejar entrar ENTERO en el WAL, sin checkpoint por el medio, es un backlog
// completo drenando: ~4 MiB medidos. 16 MiB da margen para una ráfaga cuatro veces mayor y hace que el
// checkpoint caiga DESPUÉS, cuando el cajero ya no está escribiendo y a nadie le duele esperar.
//
// EL COSTE, que es real y por eso el valor no es «enorme»: (a) el fichero `cola_entrantes.db-wal` puede
// ocupar hasta ~16 MiB de disco —efímero, el checkpoint lo trunca—; (b) cuando el checkpoint llega mueve
// hasta 16 MiB de una vez en lugar de 4, o sea es 4× más caro… pero ocurre 4× menos y ya no en mitad de
// la ráfaga, que es justo el cambio que se busca; (c) mientras el WAL está lleno las LECTURAS cuestan algo
// más, porque una lectura busca primero la página en el WAL a través del wal-index, y ese índice crece
// con él. Por (a) y (c) NO se desactiva el autocheckpoint (PRAGMA wal_autocheckpoint=0) ni se pone en un
// valor gigante: en un proceso 24/7 el WAL crecería sin techo y la degradación de lectura sería
// permanente. El objetivo es MOVER el checkpoint fuera de la ráfaga, no suprimirlo.
const colaWALAutocheckpointPages = 4000

var (
	// defaultTuning es el perfil CONSERVADOR: los propios defaults de SQLite, fijados explícitamente para
	// que sean visibles y verificables aquí en vez de heredados en silencio del driver. Rige edge.db —el
	// store cifrado de whatsmeow (device, prekeys, sesiones Signal) y los metadatos de negocio— y todos
	// los helpers legacy. Ahí no se toca nada: no hay ningún problema medido de latencia en esa BD (su
	// tráfico es de órdenes de magnitud menos), así que relajar su durabilidad sería pagar un riesgo sin
	// comprar nada. Ver OpenCola para la única base que sí se aparta de este perfil, y el informe de la
	// decisión de alcance.
	defaultTuning = sqliteTuning{synchronous: "FULL", walAutocheckpoint: 1000}

	// colaTuning es el perfil de la COLA DE ENTRANTES (Plan 051 · T3.15): la única BD del Edge con dos
	// procesos escribiéndola a la vez y con un criterio de latencia colgando de ella (INV-051.2, handler
	// < 50 ms p99).
	//
	// synchronous=NORMAL Y POR QUÉ ES ACEPTABLE (decisión de Jhoan, 2026-08-17): en modo WAL, NORMAL no
	// significa «escribir más tarde». El commit escribe el dato en el WAL igual que con FULL —la escritura
	// ya salió del proceso y está en el page cache del SO ANTES de que se acuse la transacción—; lo que se
	// omite es el fsync que fuerza esa escritura al plato en CADA commit (se hace en el checkpoint). Por
	// eso un crash del proceso, un pánico o un `kill -9` NO pierden nada: el SO conserva la escritura
	// aunque el proceso muera. Solo un corte de energía o un pánico del KERNEL pueden perder las últimas
	// transacciones no sincronizadas.
	//
	// Y NO PUEDE CORROMPER LA BASE: en WAL mode, SQLite garantiza la integridad con synchronous=NORMAL —el
	// WAL nunca deja la base a medias, un commit que no llegó simplemente no existe—. Lo que se debilita
	// es la DURABILIDAD de las últimas transacciones, no la consistencia. En términos de la cola, el peor
	// caso concreto es: un cierre de lote perdido (la fila vuelve a ser reclamable por TTL de lease y el
	// cajero la rehace; el UNIQUE (session_id, wa_message_id) impide duplicar) o un Enqueue recién acusado
	// perdido (un entrante menos) — el mismo mensaje que se habría perdido si el corte de luz llega un
	// milisegundo antes, mientras el paquete aún viajaba. Ese riesgo ya existía y es mucho mayor por otras
	// vías; el que se añade aquí es marginal frente a lo que compra.
	colaTuning = sqliteTuning{synchronous: "NORMAL", walAutocheckpoint: colaWALAutocheckpointPages}
)

// openSQLite implementa Open para el motor SQLite embebido (pure-Go), con el perfil de escritura `tuning`
// (ver sqliteTuning: defaultTuning para el resto del Edge, colaTuning para la cola de entrantes). Fija
// SetMaxOpenConns(1): PRAGMA foreign_keys —y también synchronous y wal_autocheckpoint— es por-conexión en
// SQLite, así que limitar el pool a una conexión garantiza que los pragmas aplicados aquí rigen TODAS las
// operaciones (evita que database/sql abra conexiones nuevas sin ellos) y serializa la escritura del store
// cifrado (suficiente para el daemon del Edge).
func openSQLite(ctx context.Context, path string, tuning sqliteTuning) (*sql.DB, error) {
	// Garantiza el fichero con 0600 ANTES de que SQLite lo cree con permisos del umask.
	f, err := os.OpenFile(path, os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("db: crear fichero del store: %w", err)
	}
	_ = f.Close()
	if err := os.Chmod(path, 0o600); err != nil { // por si preexistía con otros permisos
		return nil, fmt.Errorf("db: fijar permisos 0600: %w", err)
	}

	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("db: abrir sqlite: %w", err)
	}
	database.SetMaxOpenConns(1)

	// journal_mode=WAL va PRIMERO a propósito: synchronous=NORMAL solo es seguro (no-corrupción) en modo
	// WAL, y wal_autocheckpoint no tiene sentido fuera de él. Fijarlos antes sería fijarlos sobre un
	// journal que aún es `delete`.
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
		// Los dos del perfil se emiten SIEMPRE, también cuando coinciden con el default de SQLite: así el
		// valor efectivo lo decide este código y no la versión del driver, y un test puede leerlos.
		// Interpolación segura: los valores salen de constantes de este fichero, nunca de entrada externa,
		// y SQLite no admite placeholders en un PRAGMA.
		fmt.Sprintf("PRAGMA synchronous=%s", tuning.synchronous),
		fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", tuning.walAutocheckpoint),
	} {
		if _, err := database.ExecContext(ctx, pragma); err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("db: %q: %w", pragma, err)
		}
	}
	return database, nil
}

// MigrateStore aplica el set "store" (migrations/store/*.sql → tablas msg_enc_*) sobre database. Es
// la migración de un store.db POR SESIÓN (ADR-0016 §4): crea SOLO el esquema del cryptostore, sin las
// tablas de metadatos de negocio. Idempotente (CREATE TABLE IF NOT EXISTS).
//
// Tras los .sql, aplica las migraciones GUARDADAS de columnas nuevas que un ALTER pelado no puede
// hacer idempotentes (el runner re-ejecuta todo en cada arranque y modernc SQLite no soporta ADD
// COLUMN IF NOT EXISTS): ver ensureDeviceMetadataColumns.
func MigrateStore(ctx context.Context, database *sql.DB) error {
	if err := applyMigrations(ctx, database, storeMigrationsDir); err != nil {
		return err
	}
	return ensureDeviceMetadataColumns(ctx, database)
}

// deviceMetadataColumns son las columnas de metadata NO-clave del device propio (Device.PushName,
// BusinessName, LID) añadidas a msg_enc_device después del esquema inicial. Se guardan CIFRADAS con la
// DEK (BLOB de ciphertext, como el resto de la tabla); aquí solo se declara su forma en disco.
var deviceMetadataColumns = []string{"push_name", "business_name", "lid"}

// ensureDeviceMetadataColumns añade de forma IDEMPOTENTE las columnas de deviceMetadataColumns a
// msg_enc_device si aún no existen. Es la migración para stores YA EMPAREJADOS creados antes de que la
// tabla las declarara: para stores nuevos ya vienen en el CREATE TABLE (0001_init.sql) y este paso es
// no-op.
//
// El runner (applyMigrations) re-ejecuta TODOS los .sql en cada arranque y NO lleva tabla de versión;
// por eso un `ALTER TABLE ... ADD COLUMN` pelado en un .sql fallaría en el 2º arranque ("duplicate
// column", y modernc SQLite sin CGO no soporta ADD COLUMN IF NOT EXISTS). Aquí el ALTER se GUARDA leyendo
// PRAGMA table_info(msg_enc_device): solo se emite para las columnas ausentes, así reabrir el store dos
// veces es seguro. Las columnas son nullable, así que un store viejo no re-empareja: la fila existente
// queda con NULL y degrada al comportamiento previo hasta el próximo Device.Save.
func ensureDeviceMetadataColumns(ctx context.Context, database *sql.DB) error {
	rows, err := database.QueryContext(ctx, `PRAGMA table_info(msg_enc_device)`)
	if err != nil {
		return fmt.Errorf("db: leer columnas de msg_enc_device: %w", err)
	}
	existing := make(map[string]struct{})
	for rows.Next() {
		var (
			cid       int
			name      string
			colType   string
			notNull   int
			dfltValue sql.NullString
			pk        int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			_ = rows.Close()
			return fmt.Errorf("db: escanear PRAGMA table_info(msg_enc_device): %w", err)
		}
		existing[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("db: recorrer PRAGMA table_info(msg_enc_device): %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("db: cerrar PRAGMA table_info(msg_enc_device): %w", err)
	}

	for _, col := range deviceMetadataColumns {
		if _, ok := existing[col]; ok {
			continue // ya existe (store nuevo o 2º arranque): no reemitir el ALTER.
		}
		// Nombre de columna de una lista fija en código (no viene de entrada externa): la interpolación
		// es segura y necesaria porque SQLite no admite placeholder para identificadores.
		stmt := fmt.Sprintf(`ALTER TABLE msg_enc_device ADD COLUMN %s BLOB`, col)
		if _, err := database.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("db: añadir columna %q a msg_enc_device: %w", col, err)
		}
	}
	return nil
}

// MigrateMeta aplica el set "meta" (migrations/meta/*.sql → tablas sessions/sessions_v2) sobre
// database. Es la migración de la db CENTRAL de metadatos de negocio (ADR-0016 §2). Idempotente.
func MigrateMeta(ctx context.Context, database *sql.DB) error {
	return applyMigrations(ctx, database, metaMigrationsDir)
}

// MigrateCola aplica el set "cola" (migrations/cola/*.sql → tabla cola_entrantes) sobre database. Es
// la migración de la COLA DE ENTRANTES del Edge (Plan 051 Ola 1 · T1.1 / ADR-0038 Enmienda 1).
// Idempotente.
//
// Tras los .sql aplica, igual que MigrateStore, las migraciones GUARDADAS de columnas nuevas que un ALTER
// pelado no puede hacer idempotentes: ver ensureColaClaimToken (columna `claim_token`, T2.18) y
// ensureColaIntentos (columna `intentos`, T2.19). Esos pasos son los que hacen que una cola YA CREADA por
// un binario anterior se ponga al día en vez de quedarse coja en silencio.
//
// 🔴 ESTE MIGRADOR ES **SQLite-ONLY**, y a diferencia de Migrate() no admite Postgres ni con el build-tag.
// No es una limitación temporal que se pueda quitar cambiando el DSN: los pasos guardados leen
// `PRAGMA table_info(...)`, que es sintaxis EXCLUSIVA de SQLite (en Postgres se consultaría
// `information_schema.columns`), así que sobre una conexión Postgres esta función falla en el paso
// guardado, no en los .sql. Se documenta en vez de discriminar por dialecto en tiempo de ejecución por dos
// razones concretas:
//
//   - No hay dialecto que discriminar. La cola NO es la BD conmutable del Edge: es un fichero PROPIO
//     (<data_dir>/cola_entrantes.db) que el daemon abre SIEMPRE con Open(DialectSQLite, …) —el `dialect` de
//     config solo rige edge.db—, así que un chequeo aquí sería una rama muerta comprobando algo que el
//     llamante ya fijó. Meter el dialecto en la firma obligaría además a tocar a los tres llamantes
//     (daemon.go, cmd/agent/cajero.go y los tests) para propagar una constante.
//   - Los .sql del set "cola" tampoco son portables por su cuenta (ver migrations/cola/0001), así que la
//     no-portabilidad no la introduce este paso: solo la hereda.
//
// EL DÍA QUE LA COLA TENGA QUE HABLAR POSTGRES —worker-cajero remoto, varias colas compartidas—, lo que
// hay que cambiar es este migrador ENTERO (firma con dialecto + el .sql + una variante de CADA paso
// guardado sobre information_schema), no envolver estos pasos en un `if`.
//
// 🔴 NO la llames desde Migrate(): NO es un descuido. Migrate() migra la BD ÚNICA del Edge
// (<data_dir>/edge.db), y la cola vive en OTRA base de datos, un fichero APARTE
// (<data_dir>/cola_entrantes.db, Layout.ColaDB()). Están separadas a propósito (design §2, D-2): la
// poda agresiva por TTL de la cola no debe tocar la BD principal, y el SetMaxOpenConns(1) de edge.db
// se volvería el cuello de botella entre el agente y el worker-cajero, que es OTRO proceso. Meter este
// set en Migrate() crearía una tabla cola_entrantes fantasma dentro de edge.db —que nadie leería— y no
// migraría la cola real. La llama quien abre la cola, con SU propio *sql.DB.
func MigrateCola(ctx context.Context, database *sql.DB) error {
	if err := applyMigrations(ctx, database, colaMigrationsDir); err != nil {
		return err
	}
	// DESPUÉS de los .sql, y no dentro de ellos: la columna `claim_token` se añade con un ALTER GUARDADO
	// en Go (ver ensureColaClaimToken). Sobre una BD virgen el CREATE TABLE del 0001 acaba de correr y el
	// ALTER se emite sobre la tabla ya creada; sobre una BD que nació SIN la columna, este es el único
	// paso que la añade.
	if err := ensureColaClaimToken(ctx, database); err != nil {
		return err
	}
	// Y lo mismo con `intentos` (T2.19), por la MISMA razón mecánica y con el mismo camino: editar el
	// CREATE TABLE del 0001 sería un no-op silencioso sobre las colas que ya existen en disco. El orden
	// entre los dos pasos guardados es indiferente —columnas distintas, sin dependencia—, pero se dejan en
	// orden cronológico para que la lista se lea como la historia del esquema.
	return ensureColaIntentos(ctx, database)
}

// colaClaimTokenColumn es la columna de FENCING del claim del cajero (Plan 051 Ola 2): identifica al
// CLAIM, no a la fila. TEXT nullable (hex de 16 bytes de CSPRNG); NULL = «esta fila no la tiene nadie».
const colaClaimTokenColumn = "claim_token"

// ensureColaClaimToken añade de forma IDEMPOTENTE la columna `claim_token` a `cola_entrantes` si aún no
// existe. Es la gemela de ensureDeviceMetadataColumns, y existe por la misma razón mecánica: el runner
// (applyMigrations) NO lleva tabla de versión y RE-EJECUTA cada .sql en cada arranque, así que un
// `ALTER TABLE … ADD COLUMN` pelado dentro del .sql reventaría en el 2.º arranque con "duplicate column"
// (y modernc SQLite sin CGO no soporta `ADD COLUMN IF NOT EXISTS`). Leyendo PRAGMA table_info(...) el
// ALTER solo se emite cuando falta, y reabrir la cola N veces es seguro.
//
// 🔴 POR QUÉ NO BASTABA EDITAR EL `CREATE TABLE` DEL 0001, que es como nació esta columna (T2.15): se
// apoyaba en que «la cola no ha corrido todavía en ningún entorno», y esa premisa caduca sola —el commit
// del 0001 ya está en `dev` y el daemon aplica MigrateCola al arrancar, así que cualquier binario de `dev`
// ya creó <data_dir>/cola_entrantes.db sin que nadie usara la cola—. Sobre una BD así, el CREATE TABLE
// editado es un NO-OP SILENCIOSO: no falla al arrancar, `Enqueue` sigue insertando (su INSERT no nombra la
// columna), la cola se llena… y el PRIMER Reclamar muere con `no such column: claim_token`, dejando
// mensajes que ningún cajero podrá vaciar hasta que el tope los descarte. Fallo mudo al arrancar, pérdida
// de mensajes al cabo del tiempo.
//
// La columna es NULLABLE a propósito: las filas preexistentes quedan con NULL, que es exactamente el mismo
// valor que deja el cierre de lote y el barrido de leases ("de nadie"), así que una cola migrada en caliente
// se comporta igual que una nueva sin ninguna reescritura de datos.
//
// NINGÚN ÍNDICE del 0001 menciona `claim_token` (ux_cola_session_wamid, ix_cola_estado_seq e ix_cola_conv
// se apoyan en session_id/wa_message_id/estado/seq/chat_jid), así que no hay que mover ningún CREATE INDEX
// detrás de este ALTER. Si algún día se indexa el token, su índice va AQUÍ, después del ALTER, nunca en el
// .sql: en una BD vieja el .sql se ejecuta antes de que la columna exista.
//
// ⚠️ SQLite-ONLY por el `PRAGMA table_info`: el porqué y qué habría que cambiar para portarlo, en el doc de
// MigrateCola (su único llamante).
func ensureColaClaimToken(ctx context.Context, database *sql.DB) error {
	rows, err := database.QueryContext(ctx, `PRAGMA table_info(cola_entrantes)`)
	if err != nil {
		return fmt.Errorf("db: leer columnas de cola_entrantes: %w", err)
	}
	existing := make(map[string]struct{})
	for rows.Next() {
		var (
			cid       int
			name      string
			colType   string
			notNull   int
			dfltValue sql.NullString
			pk        int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			_ = rows.Close()
			return fmt.Errorf("db: escanear PRAGMA table_info(cola_entrantes): %w", err)
		}
		existing[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("db: recorrer PRAGMA table_info(cola_entrantes): %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("db: cerrar PRAGMA table_info(cola_entrantes): %w", err)
	}

	// TABLA AUSENTE ⇒ ERROR EXPLÍCITO, y esta guarda no es defensiva por gusto: `PRAGMA table_info` de una
	// tabla que NO existe no falla, devuelve CERO filas. Sin esto, `existing` sale vacío, el flujo cae al
	// ALTER de abajo y el usuario recibe un `no such table: cola_entrantes` crudo del driver, envuelto en un
	// mensaje que habla de «añadir columna claim_token» — que apunta al sitio equivocado. El fallo real es
	// que los .sql del set "cola" no se aplicaron (BD que no es la de la cola, un embed roto, un
	// applyMigrations que no corrió), y eso es lo que hay que decir. Una tabla que existe siempre tiene al
	// menos una columna, así que `len(existing) == 0` no tiene otra causa.
	if len(existing) == 0 {
		return fmt.Errorf("db: la tabla cola_entrantes no existe en esta BD; el set de migraciones %q no se aplicó "+
			"(¿es esta la BD de la cola, <data_dir>/cola_entrantes.db?)", colaMigrationsDir)
	}

	if _, ok := existing[colaClaimTokenColumn]; ok {
		return nil // ya existe (BD nueva o 2.º arranque): no reemitir el ALTER.
	}
	// Identificador de una constante en código (no viene de entrada externa): SQLite no admite placeholder
	// para nombres de columna, así que la interpolación es necesaria y aquí es segura.
	stmt := fmt.Sprintf(`ALTER TABLE cola_entrantes ADD COLUMN %s TEXT`, colaClaimTokenColumn)
	if _, err := database.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("db: añadir columna %q a cola_entrantes: %w", colaClaimTokenColumn, err)
	}
	return nil
}

// colaIntentosColumn es el contador de RECLAMOS de una fila de la cola (Plan 051 Ola 2 · T2.19): cuántas
// veces un cajero se la ha llevado. INTEGER NOT NULL DEFAULT 0; lo incrementa el propio UPDATE del claim.
const colaIntentosColumn = "intentos"

// ensureColaIntentos añade de forma IDEMPOTENTE la columna `intentos` a `cola_entrantes` si aún no existe.
// Es la gemela de ensureColaClaimToken —mismo patrón, misma guarda, mismo modo de fallo— y la duplicación
// del bloque de lectura de columnas es deliberada: es el tercer sitio del repo que hace esto
// (ensureDeviceMetadataColumns fue el primero) y factorizar un helper genérico ahora obligaría a reescribir
// dos funciones que ya están probadas, a cambio de ahorrar veinte líneas. Cuando aparezca el cuarto.
//
// 🔴 POR QUÉ NO SE EDITA EL `CREATE TABLE` DEL 0001, que es la tentación obvia: `CREATE TABLE IF NOT EXISTS`
// es un NO-OP sobre una BD que ya existe, y las colas del campo YA existen (cualquier binario de `dev` creó
// <data_dir>/cola_entrantes.db al arrancar). Sobre una de ellas la columna editada no aparecería nunca: el
// arranque no falla, `Enqueue` sigue insertando, y el primer `Reclamar` muere con `no such column:
// intentos` — el mismo camino que T2.18 ya recorrió con `claim_token`, y por el que el 0001 se revirtió a
// propósito para no volver a tocarlo.
//
// LA COLUMNA ES `NOT NULL DEFAULT 0` Y ESO ES LO QUE LA HACE SEGURA EN CALIENTE: SQLite exige un DEFAULT
// no nulo para un ADD COLUMN con NOT NULL, y ese default rellena las filas preexistentes con 0 en el propio
// ALTER. Así una cola migrada en caliente se comporta EXACTAMENTE como una nueva —las filas que ya estaban
// esperando empiezan a contar desde cero, con sus intentos íntegros— sin reescribir un solo dato. Un
// NULLABLE, en cambio, dejaría filas con NULL y `intentos + 1` sobre NULL da NULL: el contador no avanzaría
// nunca y el corte del cajero no se dispararía justo en las filas más viejas, que son las candidatas a ser
// el lote venenoso.
//
// NINGÚN ÍNDICE del 0001 menciona `intentos`, así que no hay CREATE INDEX que mover detrás de este ALTER.
// Si algún día se indexa (una consulta de «lotes a punto de agotar intentos», por ejemplo), su índice va
// AQUÍ, después del ALTER, nunca en el .sql.
//
// ⚠️ SQLite-ONLY por el `PRAGMA table_info`: el porqué y qué habría que cambiar para portarlo, en el doc de
// MigrateCola (su único llamante).
func ensureColaIntentos(ctx context.Context, database *sql.DB) error {
	rows, err := database.QueryContext(ctx, `PRAGMA table_info(cola_entrantes)`)
	if err != nil {
		return fmt.Errorf("db: leer columnas de cola_entrantes: %w", err)
	}
	existing := make(map[string]struct{})
	for rows.Next() {
		var (
			cid       int
			name      string
			colType   string
			notNull   int
			dfltValue sql.NullString
			pk        int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			_ = rows.Close()
			return fmt.Errorf("db: escanear PRAGMA table_info(cola_entrantes): %w", err)
		}
		existing[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("db: recorrer PRAGMA table_info(cola_entrantes): %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("db: cerrar PRAGMA table_info(cola_entrantes): %w", err)
	}

	// TABLA AUSENTE ⇒ ERROR EXPLÍCITO, por la MISMA razón que en ensureColaClaimToken: `PRAGMA table_info`
	// de una tabla inexistente no falla, devuelve cero filas, y sin esta guarda el operador recibiría un
	// `no such table` envuelto en un mensaje que habla de añadir una columna — apuntando al sitio
	// equivocado cuando el fallo real es que el set de migraciones no se aplicó.
	if len(existing) == 0 {
		return fmt.Errorf("db: la tabla cola_entrantes no existe en esta BD; el set de migraciones %q no se aplicó "+
			"(¿es esta la BD de la cola, <data_dir>/cola_entrantes.db?)", colaMigrationsDir)
	}

	if _, ok := existing[colaIntentosColumn]; ok {
		return nil // ya existe (BD nueva o 2.º arranque): no reemitir el ALTER.
	}
	// Identificador de una constante en código (no viene de entrada externa): SQLite no admite placeholder
	// para nombres de columna, así que la interpolación es necesaria y aquí es segura.
	stmt := fmt.Sprintf(`ALTER TABLE cola_entrantes ADD COLUMN %s INTEGER NOT NULL DEFAULT 0`, colaIntentosColumn)
	if _, err := database.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("db: añadir columna %q a cola_entrantes: %w", colaIntentosColumn, err)
	}
	return nil
}

// Migrate aplica AMBOS sets (store y luego meta) sobre una sola db. Es el camino single-sesión
// legacy (cmd/agent abre UN .db con msg_enc_* + sessions_v2 a la vez); el modelo multi-sesión
// (ADR-0016) separa los sets en store.db por sesión vs sessions.db central, vía los helpers de arriba.
// El orden store→meta es el lexicográfico histórico (0001 < 0002 < 0003); no hay FKs cruzadas.
func Migrate(ctx context.Context, database *sql.DB) error {
	if err := MigrateStore(ctx, database); err != nil {
		return err
	}
	return MigrateMeta(ctx, database)
}

// applyMigrations aplica, en orden lexicográfico de nombre, todas las migraciones .sql del
// subdirectorio dir de migrationsFS sobre database. Cada migración es idempotente, así que reaplicarla
// sobre una db ya migrada es no-op.
func applyMigrations(ctx context.Context, database *sql.DB, dir string) error {
	names, err := migrationNames(dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		sqlText, err := fs.ReadFile(migrationsFS, dir+"/"+name)
		if err != nil {
			return fmt.Errorf("db: leer migración %q: %w", name, err)
		}
		if _, err := database.ExecContext(ctx, string(sqlText)); err != nil {
			return fmt.Errorf("db: aplicar migración %q: %w", name, err)
		}
	}
	return nil
}

// migrationNames lista los ficheros .sql del subdirectorio dir en orden lexicográfico (orden de
// aplicación).
func migrationNames(dir string) ([]string, error) {
	entries, err := fs.ReadDir(migrationsFS, dir)
	if err != nil {
		return nil, fmt.Errorf("db: listar migraciones de %q: %w", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// OpenAndMigrate combina Open + Migrate (AMBOS sets): camino single-sesión legacy. Deja un *sql.DB
// con permisos 0600, pragmas fijados y las tablas msg_enc_* + sessions/sessions_v2 creadas. Cierra la
// conexión si la migración falla.
func OpenAndMigrate(ctx context.Context, path string) (*sql.DB, error) {
	return openAndApply(ctx, path, Migrate)
}

// OpenSessionStore combina Open + MigrateStore: abre (creando) el store.db de UNA sesión y le aplica
// SOLO el set "store" (msg_enc_*). Es el helper que el Manager (T3/T4) usa por sesión; las tablas
// whatsmeow_* no sensibles las crea aparte el cryptostore (sqlstore.Upgrade), no este runner. Cierra
// la conexión si la migración falla.
func OpenSessionStore(ctx context.Context, path string) (*sql.DB, error) {
	return openAndApply(ctx, path, MigrateStore)
}

// OpenAndMigrateMeta combina Open + MigrateMeta: abre (creando) la db CENTRAL de metadatos de negocio
// (<data_dir>/sessions.db) y le aplica SOLO el set "meta" (sessions/sessions_v2). Cierra la conexión
// si la migración falla.
func OpenAndMigrateMeta(ctx context.Context, path string) (*sql.DB, error) {
	return openAndApply(ctx, path, MigrateMeta)
}

// openAndApply abre el .db SQLite en path y le aplica migrate (un set o ambos); cierra la conexión si
// falla. Los helpers OpenAndMigrate/OpenSessionStore/OpenAndMigrateMeta son la vía por FICHERO SQLite
// (per-sesión / central, ADR-0016 §4): abren siempre en DialectSQLite. La unificación a BD única
// dialecto-aware (Open(ctx, cfg.DBDialect, cfg.DBDSN)) la cablea T1 sobre esta misma base.
func openAndApply(ctx context.Context, path string, migrate func(context.Context, *sql.DB) error) (*sql.DB, error) {
	database, err := Open(ctx, DialectSQLite, path)
	if err != nil {
		return nil, err
	}
	if err := migrate(ctx, database); err != nil {
		_ = database.Close()
		return nil, err
	}
	return database, nil
}
