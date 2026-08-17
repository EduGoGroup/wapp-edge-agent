package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

// Valores EFECTIVOS que SQLite devuelve al leer los pragmas del perfil de escritura. Se escriben aquí
// como números crudos —y no derivados de defaultTuning/colaTuning— a propósito: si el test leyera el
// valor esperado de la misma constante que configura la conexión, la aserción sería una tautología y
// cambiar el perfil no pondría rojo nada. `PRAGMA synchronous` devuelve el ENTERO (0=OFF, 1=NORMAL,
// 2=FULL, 3=EXTRA), no la palabra con la que se fija.
const (
	synchronousNORMAL = 1
	synchronousFULL   = 2

	// Umbral por defecto de SQLite, en páginas (4 MiB con page_size=4096).
	autocheckpointDefaultPages = 1000
	// Umbral de la cola: 16 MiB. Ver colaWALAutocheckpointPages para el criterio.
	autocheckpointColaPages = 4000
)

// leePerfil devuelve los pragmas de escritura EFECTIVOS de la conexión, que es lo único que vale como
// evidencia: fijar un pragma y que el motor lo acepte son dos cosas distintas (un valor inválido o un
// pragma emitido antes de journal_mode=WAL se traga sin error y deja el default).
func leePerfil(t *testing.T, database *sql.DB) (sync, autocheckpoint int) {
	t.Helper()
	if err := database.QueryRow(`PRAGMA synchronous`).Scan(&sync); err != nil {
		t.Fatalf("PRAGMA synchronous: %v", err)
	}
	if err := database.QueryRow(`PRAGMA wal_autocheckpoint`).Scan(&autocheckpoint); err != nil {
		t.Fatalf("PRAGMA wal_autocheckpoint: %v", err)
	}
	return sync, autocheckpoint
}

// TestOpenAplicaPerfilConservador custodia el ALCANCE de T3.15: Open —la vía de edge.db (store cifrado de
// whatsmeow + metadatos de negocio) y de todos los helpers legacy— NO se relaja. Si alguien decide algún
// día extender el perfil de la cola a todas las bases, que sea una decisión explícita que rompa este test,
// y no un efecto colateral de tocar openSQLite.
func TestOpenAplicaPerfilConservador(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edge.db")
	database, err := Open(context.Background(), DialectSQLite, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()

	sync, autock := leePerfil(t, database)
	if sync != synchronousFULL {
		t.Errorf("synchronous = %d, esperaba %d (FULL): edge.db guarda el material criptográfico de "+
			"whatsmeow y no tiene ningún problema de latencia medido; su durabilidad no se relaja", sync, synchronousFULL)
	}
	if autock != autocheckpointDefaultPages {
		t.Errorf("wal_autocheckpoint = %d páginas, esperaba %d (default de SQLite)", autock, autocheckpointDefaultPages)
	}
}

// TestOpenColaAplicaPerfilDeEscritura custodia la INVARIANTE de T3.15: la BD de la cola de entrantes se
// abre con synchronous=NORMAL y con el WAL ancho. Es lo que impide que un checkpoint caiga en mitad del
// tráfico y bloquee al otro proceso escritor, que es la causa medida de los picos de 250-471 ms en el p99
// del handler (PC-11). Romper cualquiera de los dos pragmas devuelve ese p99 al estado anterior sin que
// falle nada más: por eso hay un test mirándolos.
func TestOpenColaAplicaPerfilDeEscritura(t *testing.T) {
	// La conversión a ColaDBPath es explícita porque en producción la ruta la fabrica UN solo sitio
	// (sessionmgr.Layout.ColaDB) y ya llega tipada; aquí no hay Layout, así que el test dice a las claras
	// «esta ruta es la de una cola». Que haga falta escribirlo es justamente la barrera de T3.16.
	path := ColaDBPath(filepath.Join(t.TempDir(), "cola_entrantes.db"))
	database, err := OpenCola(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenCola: %v", err)
	}
	defer func() { _ = database.Close() }()

	sync, autock := leePerfil(t, database)
	if sync != synchronousNORMAL {
		t.Errorf("synchronous = %d, esperaba %d (NORMAL): con FULL hay un fsync en CADA commit de la cola "+
			"y el handler vuelve a pagarlo en su p99 (T3.15)", sync, synchronousNORMAL)
	}
	if autock != autocheckpointColaPages {
		t.Errorf("wal_autocheckpoint = %d páginas, esperaba %d (16 MiB): con el default de %d el WAL vive "+
			"pegado al techo y cada commit del drenaje arrastra un checkpoint que bloquea a los escritores",
			autock, autocheckpointColaPages, autocheckpointDefaultPages)
	}

	// El perfil de escritura NO puede haberse llevado por delante lo que ya garantizaba openSQLite: sin
	// journal_mode=WAL, synchronous=NORMAL dejaría de ser seguro (en rollback journal sí puede corromper),
	// y sin el escritor único los pragmas por-conexión no regirían todas las operaciones.
	var journal string
	if err := database.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if journal != "wal" {
		t.Errorf("journal_mode = %q, esperaba wal: synchronous=NORMAL solo es seguro en modo WAL", journal)
	}
	if n := database.Stats().MaxOpenConnections; n != 1 {
		t.Errorf("SetMaxOpenConns = %d, esperaba 1: synchronous y wal_autocheckpoint son POR-CONEXIÓN", n)
	}
}

// TestOpenColaMigraYEscribe comprueba que el perfil no rompe el uso real de la cola: OpenCola + MigrateCola
// dejan la tabla lista y una escritura pasa. Un pragma mal formado no falla al emitirse en todos los casos,
// pero un journal mal configurado sí se nota al escribir.
func TestOpenColaMigraYEscribe(t *testing.T) {
	ctx := context.Background()
	path := ColaDBPath(filepath.Join(t.TempDir(), "cola_entrantes.db"))
	database, err := OpenCola(ctx, path)
	if err != nil {
		t.Fatalf("OpenCola: %v", err)
	}
	defer func() { _ = database.Close() }()

	if err := MigrateCola(ctx, database); err != nil {
		t.Fatalf("MigrateCola: %v", err)
	}
	if _, err := database.ExecContext(ctx,
		`INSERT INTO cola_entrantes (seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc, estado)
		 VALUES (1, 's1', 'jid@s.whatsapp.net', 'wamid-1', 0, x'00', 'nuevo')`); err != nil {
		t.Fatalf("insert en la cola recién abierta con el perfil de escritura: %v", err)
	}
}

// TestOpenColaFallaConRutaImposible cubre la propagación de error de OpenCola (mismo camino que Open: el
// os.OpenFile del fichero).
func TestOpenColaFallaConRutaImposible(t *testing.T) {
	path := ColaDBPath(filepath.Join(t.TempDir(), "no-existe", "cola_entrantes.db"))
	if _, err := OpenCola(context.Background(), path); err == nil {
		t.Fatal("OpenCola debía fallar con un directorio padre inexistente")
	}
}
