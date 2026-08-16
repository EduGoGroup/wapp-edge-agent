package colaentrantes

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	infradb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-shared/envelope"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

func testLogger() sharedlogger.Logger {
	return sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}), sharedlogger.WithJSON(true))
}

// openDB abre una BD de cola temporal POR EL MISMO CAMINO QUE PRODUCCIÓN (db.Open, el que usa el daemon
// en daemon.go para <data_dir>/cola_entrantes.db) y le aplica la MIGRACIÓN REAL (db.MigrateCola), no una
// copia a mano del DDL. Es deliberado por partida doble:
//
//   - El esquema: un DDL replicado en el test se desincroniza del de producción sin que nadie se entere
//     —así fue como el índice único ux_cola_session_wamid faltó en los tests y la idempotencia del
//     Enqueue quedó sin cubrir—.
//   - LOS PRAGMAS: antes se abría con un `sql.Open` pelado, así que los tests corrían en journal
//     ROLLBACK y sin busy_timeout mientras producción va en WAL con 5 s de espera (db.go, openSQLite).
//     Eso es el MODO DE CONCURRENCIA del motor, no un detalle cosmético: ningún test de esta cola estaba
//     ejercitando el modo real, y justo aquí (drop-oldest, claim, barrido) es donde la concurrencia
//     decide si algo es correcto. Llamando a db.Open no se replican los pragmas: se HEREDAN, y el día
//     que producción cambie uno los tests lo cambian con ella.
//
// Sigue siendo autocontenido: SQLite sobre un fichero en t.TempDir(), sin servidores, sin Docker y sin
// variables de entorno. Aquí no hay ciclo de imports: internal/infra/db no importa ningún paquete
// interno del agente (y es él quien enlaza el driver modernc.org/sqlite, por eso este fichero ya no
// necesita el blank import).
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cola_entrantes.db")
	database, err := infradb.Open(context.Background(), infradb.DialectSQLite, path)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := infradb.MigrateCola(context.Background(), database); err != nil {
		t.Fatalf("MigrateCola: %v", err)
	}
	return database
}

// fakeCrypterFor cuenta cuántas veces se resolvió el sobre de cada sesión (para probar el caché) y
// devuelve un envelope real con una DEK derivada del session_id (32 bytes).
type fakeCrypterFor struct {
	mu       sync.Mutex
	llamadas map[string]int
}

func newFakeCrypterFor() *fakeCrypterFor {
	return &fakeCrypterFor{llamadas: make(map[string]int)}
}

func (f *fakeCrypterFor) fn(sessionID string) (envelope.Crypter, error) {
	f.mu.Lock()
	f.llamadas[sessionID]++
	f.mu.Unlock()
	return envelope.NewEnvelope(dekFor(sessionID))
}

func (f *fakeCrypterFor) count(sessionID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.llamadas[sessionID]
}

// dekFor fabrica una DEK determinista de 32 bytes para una sesión (solo tests).
func dekFor(sessionID string) []byte {
	dek := make([]byte, envelope.DEKSize)
	for i := range dek {
		dek[i] = byte(i)
	}
	for i := 0; i < len(sessionID) && i < envelope.DEKSize; i++ {
		dek[i] ^= sessionID[i]
	}
	return dek
}

func newStore(t *testing.T, db *sql.DB, cf CrypterFor, maxRows, ttlHours int, opts ...Option) *Store {
	t.Helper()
	s, err := New(context.Background(), db, cf, maxRows, ttlHours, testLogger(), opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// item fabrica un entrante "recién llegado": ts_whatsapp = ahora, para que la poda TTL por defecto
// (24 h, medida sobre ts_whatsapp) no se lo lleve en los tests que no van del TTL.
func item(session, chat, waID, texto string) app.ColaItem {
	return app.ColaItem{
		SessionID:   session,
		ChatJID:     chat,
		WAMessageID: waID,
		TSWhatsApp:  time.Now().Unix(),
		Texto:       texto,
	}
}

// TestSeedSeqTablaVacia: con la tabla vacía la secuencia arranca en 0 y el primer INSERT usa seq=1.
func TestSeedSeqTablaVacia(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	if got := s.seq.Load(); got != 0 {
		t.Fatalf("seq inicial con tabla vacía: got %d want 0", got)
	}
	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-1", "hola")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	var seq int64
	if err := db.QueryRow(`SELECT seq FROM cola_entrantes WHERE wa_message_id='wa-1'`).Scan(&seq); err != nil {
		t.Fatalf("leer seq: %v", err)
	}
	if seq != 1 {
		t.Fatalf("primer seq: got %d want 1", seq)
	}
}

// TestSeedSeqConFilasPrevias: la secuencia se siembra de MAX(seq) (el orden sobrevive a un reinicio).
func TestSeedSeqConFilasPrevias(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	if _, err := db.Exec(
		`INSERT INTO cola_entrantes (seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc, estado)
		 VALUES (7, 'A', 'chat@s', 'previo', 1700000000, X'00', 'nuevo')`); err != nil {
		t.Fatalf("sembrar fila previa: %v", err)
	}
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)
	if got := s.seq.Load(); got != 7 {
		t.Fatalf("seq sembrada: got %d want 7", got)
	}
	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-1", "hola")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	var seq int64
	if err := db.QueryRow(`SELECT seq FROM cola_entrantes WHERE wa_message_id='wa-1'`).Scan(&seq); err != nil {
		t.Fatalf("leer seq: %v", err)
	}
	if seq != 8 {
		t.Fatalf("seq tras MAX(seq)=7: got %d want 8", seq)
	}
}

// TestSeqMonotonicoConcurrente: N Enqueue en paralelo producen N seq DISTINTOS y estrictamente
// crecientes: la secuencia es atómica y el bloque poda→tope→insert está serializado.
//
// NO se exige contigüidad (ni min=1/max=N): un duplicado consume su número de orden y lo pierde (antes
// por la vía del INSERT OR IGNORE, desde T2.11 por la del error de unicidad tragado), así que la
// secuencia puede tener HUECOS. Es aceptable por contrato — el claim del cajero y el despachador ordenan
// con ORDER BY seq y no necesitan que los números sean consecutivos.
func TestSeqMonotonicoConcurrente(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 1000, 0)

	const n = 50
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.Enqueue(ctx, item("A", "chat@s", string(rune('a'+i%26))+string(rune('0'+i/26)), "hola"))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	var distintos, total int64
	if err := db.QueryRow(
		`SELECT COUNT(DISTINCT seq), COUNT(*) FROM cola_entrantes`).
		Scan(&distintos, &total); err != nil {
		t.Fatalf("agregados: %v", err)
	}
	if total != n || distintos != n {
		t.Fatalf("filas=%d seq distintos=%d, esperaba %d y %d", total, distintos, n, n)
	}
	// Estrictamente creciente en el orden de inserción; los huecos son legítimos (ver cabecera).
	rows, err := db.Query(`SELECT seq FROM cola_entrantes ORDER BY id ASC`)
	if err != nil {
		t.Fatalf("listar seq: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var previo int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatalf("escanear seq: %v", err)
		}
		if seq <= previo {
			t.Fatalf("seq no estrictamente creciente: %d tras %d", seq, previo)
		}
		previo = seq
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("recorrer seq: %v", err)
	}
}

// TestTextoSeCifraYDescifra: lo guardado en texto_enc NO es el plaintext y descifra al original con la
// DEK de la sesión (round-trip Seal/Open).
func TestTextoSeCifraYDescifra(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	const texto = "quiero dos empanadas"
	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-1", texto)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	var blob []byte
	if err := db.QueryRow(`SELECT texto_enc FROM cola_entrantes WHERE wa_message_id='wa-1'`).Scan(&blob); err != nil {
		t.Fatalf("leer texto_enc: %v", err)
	}
	if bytes.Contains(blob, []byte(texto)) {
		t.Fatal("texto_enc contiene el plaintext: la fila NO está cifrada")
	}
	if len(blob) != len(texto)+envelope.Overhead {
		t.Fatalf("tamaño del blob: got %d want %d", len(blob), len(texto)+envelope.Overhead)
	}
	env, err := envelope.NewEnvelope(dekFor("A"))
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	pt, err := env.Open(blob)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if string(pt) != texto {
		t.Fatalf("round-trip: got %q want %q", string(pt), texto)
	}
}

// TestMetaNilQuedaNULL / meta presente se sella y descifra.
func TestMetaNilQuedaNULL(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	sinMeta := item("A", "chat@s", "wa-nil", "hola")
	if err := s.Enqueue(ctx, sinMeta); err != nil {
		t.Fatalf("Enqueue sin meta: %v", err)
	}
	conMeta := item("A", "chat@s", "wa-meta", "hola")
	conMeta.Meta = []byte(`{"push_name":"Jhoan"}`)
	if err := s.Enqueue(ctx, conMeta); err != nil {
		t.Fatalf("Enqueue con meta: %v", err)
	}

	var nulo sql.NullString
	if err := db.QueryRow(`SELECT meta_enc FROM cola_entrantes WHERE wa_message_id='wa-nil'`).Scan(&nulo); err != nil {
		t.Fatalf("leer meta_enc nil: %v", err)
	}
	if nulo.Valid {
		t.Fatal("meta nil debía quedar NULL en la columna")
	}

	var blob []byte
	if err := db.QueryRow(`SELECT meta_enc FROM cola_entrantes WHERE wa_message_id='wa-meta'`).Scan(&blob); err != nil {
		t.Fatalf("leer meta_enc: %v", err)
	}
	if bytes.Contains(blob, []byte("Jhoan")) {
		t.Fatal("meta_enc contiene el plaintext: la meta NO está cifrada")
	}
	env, _ := envelope.NewEnvelope(dekFor("A"))
	pt, err := env.Open(blob)
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	if string(pt) != string(conMeta.Meta) {
		t.Fatalf("round-trip meta: got %q", string(pt))
	}
}

// TestIntentVacioQuedaNULL: intent_json "" ⇒ NULL; con valor ⇒ se guarda tal cual.
func TestIntentVacioQuedaNULL(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-sin", "hola")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	conIntent := item("A", "chat@s", "wa-con", "hola")
	conIntent.IntentJSON = `{"intent":"saludo"}`
	if err := s.Enqueue(ctx, conIntent); err != nil {
		t.Fatalf("Enqueue con intent: %v", err)
	}

	var sinIntent sql.NullString
	if err := db.QueryRow(`SELECT intent_json FROM cola_entrantes WHERE wa_message_id='wa-sin'`).Scan(&sinIntent); err != nil {
		t.Fatalf("leer intent_json: %v", err)
	}
	if sinIntent.Valid {
		t.Fatalf("intent_json vacío debía quedar NULL, got %q", sinIntent.String)
	}
	var got sql.NullString
	if err := db.QueryRow(`SELECT intent_json FROM cola_entrantes WHERE wa_message_id='wa-con'`).Scan(&got); err != nil {
		t.Fatalf("leer intent_json: %v", err)
	}
	if !got.Valid || got.String != conIntent.IntentJSON {
		t.Fatalf("intent_json: got %+v want %q", got, conIntent.IntentJSON)
	}
}

// TestEstadoPorDefectoYFastlane: sin estado la fila nace 'nuevo'; el fastlane la puede parir 'clasificado'.
func TestEstadoPorDefectoYFastlane(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-nuevo", "hola")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	fast := item("A", "chat@s", "wa-fast", "hola")
	fast.Estado = app.EstadoClasificado
	fast.IntentJSON = `{"intent":"saludo"}`
	if err := s.Enqueue(ctx, fast); err != nil {
		t.Fatalf("Enqueue fastlane: %v", err)
	}

	var estado string
	if err := db.QueryRow(`SELECT estado FROM cola_entrantes WHERE wa_message_id='wa-nuevo'`).Scan(&estado); err != nil {
		t.Fatalf("leer estado: %v", err)
	}
	if estado != app.EstadoNuevo {
		t.Fatalf("estado por defecto: got %q want %q", estado, app.EstadoNuevo)
	}
	if err := db.QueryRow(`SELECT estado FROM cola_entrantes WHERE wa_message_id='wa-fast'`).Scan(&estado); err != nil {
		t.Fatalf("leer estado fastlane: %v", err)
	}
	if estado != app.EstadoClasificado {
		t.Fatalf("estado fastlane: got %q want %q", estado, app.EstadoClasificado)
	}
}

// TestTTLPoda: con TTL de 1 h, la poda se lleva la fila YA DESPACHADA cuyo `despachado_en` superó el TTL.
func TestTTLPoda(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 1, WithClock(clock.Now)) // ttl 1h

	despachado := item("A", "chat@s", "wa-despachado", "hola")
	despachado.TSWhatsApp = clock.t.Unix()
	if err := s.Enqueue(ctx, despachado); err != nil {
		t.Fatalf("Enqueue despachado: %v", err)
	}
	// Simula el trabajo del despachador (Ola 3): la fila queda 'despachado' con su sello.
	if _, err := db.Exec(
		`UPDATE cola_entrantes SET estado = ?, despachado_en = ? WHERE wa_message_id = 'wa-despachado'`,
		app.EstadoDespachado, clock.t.Unix()); err != nil {
		t.Fatalf("marcar despachado: %v", err)
	}

	clock.t = clock.t.Add(2 * time.Hour) // supera el TTL
	nuevo := item("A", "chat@s", "wa-nuevo", "hola")
	nuevo.TSWhatsApp = clock.t.Unix()
	if err := s.Enqueue(ctx, nuevo); err != nil {
		t.Fatalf("Enqueue nuevo: %v", err)
	}

	ids := waIDs(t, db)
	if len(ids) != 1 || ids[0] != "wa-nuevo" {
		t.Fatalf("TTL: esperaba solo [wa-nuevo], got %v", ids)
	}
}

// TestTTLNoPodaLoNoDespachado es LA REGRESIÓN QUE IMPORTA (REQ-051.7 · ADR-0038 §Enmienda 1): una fila
// pendiente, por vieja que sea, SOBREVIVE a la poda. Con el corte antiguo por `ts_whatsapp` bastaba con
// el worker-cajero caído más que el TTL para perder mensajes JAMÁS despachados.
func TestTTLNoPodaLoNoDespachado(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 1, WithClock(clock.Now)) // ttl 1h

	pendiente := item("A", "chat@s", "wa-pendiente", "hola")
	pendiente.TSWhatsApp = clock.t.Unix()
	if err := s.Enqueue(ctx, pendiente); err != nil {
		t.Fatalf("Enqueue pendiente: %v", err)
	}
	// Estados intermedios del ciclo: tampoco se podan nunca.
	tomado := item("A", "chat@s", "wa-tomado", "hola")
	tomado.TSWhatsApp = clock.t.Unix()
	tomado.Estado = app.EstadoTomado
	if err := s.Enqueue(ctx, tomado); err != nil {
		t.Fatalf("Enqueue tomado: %v", err)
	}
	clasificado := item("A", "chat@s", "wa-clasificado", "hola")
	clasificado.TSWhatsApp = clock.t.Unix()
	clasificado.Estado = app.EstadoClasificado
	if err := s.Enqueue(ctx, clasificado); err != nil {
		t.Fatalf("Enqueue clasificado: %v", err)
	}

	clock.t = clock.t.Add(48 * time.Hour) // 48 h, muy por encima del TTL de 1 h
	nuevo := item("A", "chat@s", "wa-nuevo", "hola")
	nuevo.TSWhatsApp = clock.t.Unix()
	if err := s.Enqueue(ctx, nuevo); err != nil {
		t.Fatalf("Enqueue nuevo: %v", err)
	}

	ids := waIDs(t, db)
	if len(ids) != 4 {
		t.Fatalf("la poda NO puede tocar filas sin despachar (nuevo/tomado/clasificado): quedan %v", ids)
	}
}

// TestTTLPorDefecto24h: ttlHours<=0 NO desactiva el TTL (a diferencia del outbox), cae al default de 24 h.
func TestTTLPorDefecto24h(t *testing.T) {
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 0, 0)
	if s.ttl != 24*time.Hour {
		t.Fatalf("ttl default: got %s want 24h", s.ttl)
	}
	if s.maxRows != defaultMaxRows {
		t.Fatalf("maxRows default: got %d want %d", s.maxRows, defaultMaxRows)
	}
}

// TestDropOldestAlTope: al alcanzar el tope, Enqueue tira lo más viejo y conserva lo más nuevo.
//
// Una conversación (chat_jid) DISTINTA por mensaje a propósito: desde T2.10 la unidad de sacrificio de
// lo pendiente es la CONVERSACIÓN entera, así que con un fragmento por conversación "la conversación
// más vieja" y "la fila más vieja" coinciden y el drop-oldest se ve en su forma pura, fila a fila. El
// caso de la conversación de varios fragmentos lo cubren los tests de T2.10 de más abajo.
func TestDropOldestAlTope(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 3, 0) // tope 3

	for _, id := range []string{"wa-1", "wa-2", "wa-3", "wa-4", "wa-5"} {
		if err := s.Enqueue(ctx, item("A", "chat-"+id+"@s", id, "hola")); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}
	ids := waIDs(t, db)
	if len(ids) != 3 || ids[0] != "wa-3" || ids[1] != "wa-4" || ids[2] != "wa-5" {
		t.Fatalf("drop-oldest: esperaba [wa-3,wa-4,wa-5], got %v", ids)
	}
}

// TestCrypterSeCacheaPorSesion: CrypterFor se invoca UNA sola vez por sesión, no en cada INSERT.
func TestCrypterSeCacheaPorSesion(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	cf := newFakeCrypterFor()
	s := newStore(t, db, cf.fn, 100, 0)

	for i := 0; i < 5; i++ {
		if err := s.Enqueue(ctx, item("A", "chat@s", "wa-a"+string(rune('0'+i)), "hola")); err != nil {
			t.Fatalf("Enqueue A%d: %v", i, err)
		}
		if err := s.Enqueue(ctx, item("B", "chat@s", "wa-b"+string(rune('0'+i)), "hola")); err != nil {
			t.Fatalf("Enqueue B%d: %v", i, err)
		}
	}
	if n := cf.count("A"); n != 1 {
		t.Fatalf("CrypterFor(A) llamado %d veces, esperaba 1 (caché)", n)
	}
	if n := cf.count("B"); n != 1 {
		t.Fatalf("CrypterFor(B) llamado %d veces, esperaba 1 (caché)", n)
	}
	// Cada sesión se sella con SU DEK: el blob de B no abre con la DEK de A.
	var blobB []byte
	if err := db.QueryRow(`SELECT texto_enc FROM cola_entrantes WHERE wa_message_id='wa-b0'`).Scan(&blobB); err != nil {
		t.Fatalf("leer texto_enc B: %v", err)
	}
	envA, _ := envelope.NewEnvelope(dekFor("A"))
	if _, err := envA.Open(blobB); err == nil {
		t.Fatal("la DEK de A no debería abrir una fila de B")
	}
}

// failingCrypterFor es un CrypterFor que cuenta invocaciones y falla mientras `falla` esté puesto (una
// sesión cuya DEK no se puede leer: la custodia no está montada, el Guardián no ha entregado la llave…).
type failingCrypterFor struct {
	mu       sync.Mutex
	llamadas int
	falla    bool
}

func (f *failingCrypterFor) fn(sessionID string) (envelope.Crypter, error) {
	f.mu.Lock()
	f.llamadas++
	falla := f.falla
	f.mu.Unlock()
	if falla {
		return nil, errors.New("custodia: DEK no disponible para la sesión")
	}
	return envelope.NewEnvelope(dekFor(sessionID))
}

func (f *failingCrypterFor) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.llamadas
}

func (f *failingCrypterFor) recupera() {
	f.mu.Lock()
	f.falla = false
	f.mu.Unlock()
}

// TestCrypterFalloSeCacheaEnNegativo: LA TORMENTA DE LOGS. Con la DEK de una sesión ilegible, CrypterFor
// falla para TODOS sus mensajes; sin caché negativo se reinvocaría la custodia (y se gritaría en el log)
// una vez POR MENSAJE, a ritmo de socket. Dentro del enfriamiento se resuelve UNA sola vez y las demás
// devuelven el error memorizado, marcado con app.ErrColaFalloRepetido para que el listener no lo repita.
func TestCrypterFalloSeCacheaEnNegativo(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	cf := &failingCrypterFor{falla: true}
	s := newStore(t, db, cf.fn, 100, 0, WithClock(clock.Now))

	for i := 0; i < 5; i++ {
		err := s.Enqueue(ctx, item("A", "chat@s", "wa-"+string(rune('0'+i)), "hola"))
		if err == nil {
			t.Fatalf("Enqueue %d: sin sobre NO se puede encolar, debía fallar", i)
		}
		// El primero es el fallo REAL (se grita); los siguientes salen del caché negativo (se silencian).
		if repetido := errors.Is(err, app.ErrColaFalloRepetido); repetido != (i > 0) {
			t.Fatalf("Enqueue %d: marca de fallo repetido = %v, quería %v", i, repetido, i > 0)
		}
	}
	if n := cf.count(); n != 1 {
		t.Fatalf("CrypterFor llamado %d veces, esperaba 1 (caché NEGATIVO con enfriamiento)", n)
	}
	if n := waIDs(t, db); len(n) != 0 {
		t.Fatalf("sin sobre no debía escribirse ninguna fila, hay %d", len(n))
	}
}

// TestCrypterFalloSeReintentaTrasElEnfriamiento: el caché negativo NO es definitivo. Pasado el
// enfriamiento se vuelve a preguntar a la custodia (una vez), que es lo que permite recuperarse solo.
func TestCrypterFalloSeReintentaTrasElEnfriamiento(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	cf := &failingCrypterFor{falla: true}
	s := newStore(t, db, cf.fn, 100, 0, WithClock(clock.Now))

	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-1", "hola")); err == nil {
		t.Fatal("el primer Enqueue debía fallar")
	}
	clock.t = clock.t.Add(crypterFailureCooldown + time.Second) // vence el enfriamiento
	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-2", "hola")); err == nil {
		t.Fatal("la custodia sigue caída: el reintento también debía fallar")
	}
	if n := cf.count(); n != 2 {
		t.Fatalf("CrypterFor llamado %d veces, esperaba 2 (uno por ventana de enfriamiento)", n)
	}
}

// TestCrypterSeRecuperaSoloTrasElEnfriamiento: si la custodia vuelve (el Guardián entrega la DEK), el
// reintento posterior al enfriamiento tiene éxito, el sobre pasa al caché POSITIVO y la sesión deja de
// fallar sin reiniciar nada.
func TestCrypterSeRecuperaSoloTrasElEnfriamiento(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	cf := &failingCrypterFor{falla: true}
	s := newStore(t, db, cf.fn, 100, 0, WithClock(clock.Now))

	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-1", "hola")); err == nil {
		t.Fatal("el primer Enqueue debía fallar")
	}
	cf.recupera()
	clock.t = clock.t.Add(crypterFailureCooldown + time.Second)

	for i := 2; i <= 4; i++ {
		if err := s.Enqueue(ctx, item("A", "chat@s", "wa-"+string(rune('0'+i)), "hola")); err != nil {
			t.Fatalf("con la custodia recuperada, Enqueue %d debía funcionar: %v", i, err)
		}
	}
	if n := cf.count(); n != 2 {
		t.Fatalf("CrypterFor llamado %d veces, esperaba 2 (el fallo, el reintento con éxito, y ya en caché)", n)
	}
	if ids := waIDs(t, db); len(ids) != 3 {
		t.Fatalf("esperaba 3 filas encoladas tras la recuperación, hay %d (%v)", len(ids), ids)
	}
}

// TestDescartadasPorTopeSeCuenta (INV-051.3): el drop-oldest deja un CONTADOR acumulado, no solo un log.
// Una conversación por mensaje (ver TestDropOldestAlTope) para que el conteo sea fila a fila.
func TestDescartadasPorTopeSeCuenta(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 3, 0) // tope 3

	if got := s.DescartadasPorTope(); got != 0 {
		t.Fatalf("contador inicial = %d, quería 0", got)
	}
	for _, id := range []string{"wa-1", "wa-2", "wa-3", "wa-4", "wa-5"} {
		if err := s.Enqueue(ctx, item("A", "chat-"+id+"@s", id, "hola")); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}
	// Con tope 3, los encolados 4º y 5º tiran una conversación (de una fila) cada uno.
	if got := s.DescartadasPorTope(); got != 2 {
		t.Fatalf("DescartadasPorTope = %d, quería 2", got)
	}
	// Todas eran `nuevo`, así que la pérdida es ÍNTEGRAMENTE de la clase que el cajero nunca leerá.
	if got := s.PendientesDescartadasPorTope(); got != 2 {
		t.Fatalf("PendientesDescartadasPorTope = %d, quería 2", got)
	}
}

// TestTopeNoParteConversaciones (T2.10) es LA REGRESIÓN QUE IMPORTA de esta tarea: el drop-oldest
// original borraba por seq sin mirar el estado y podía llevarse UN FRAGMENTO INTERMEDIO de una
// conversación pendiente. El claim del cajero concatena todas las filas `nuevo` de una misma
// (session_id, chat_jid) en una sola inferencia, así que ese hueco no da error ni log: el LLM
// simplemente lee un texto al que le falta un trozo. Una mentira semántica, silenciosa.
//
// Aserción explícita: para CADA conversación con filas `nuevo`, el conjunto de seq presentes es
// EXACTAMENTE el que se insertó — o está vacío (la conversación entera se sacrificó). Nunca a medias.
func TestTopeNoParteConversaciones(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 6, 0) // tope 6

	// Tres conversaciones de tres fragmentos cada una. Al encolar el primer fragmento de la tercera la
	// cola está en el tope, así que el sacrificio cae sobre la conversación pendiente MÁS VIEJA (chat-1).
	esperado := make(map[string][]int64)
	conversaciones := []struct{ chat, prefijo string }{
		{"chat-1@s", "wa-a"},
		{"chat-2@s", "wa-b"},
		{"chat-3@s", "wa-c"},
	}
	for _, c := range conversaciones {
		for i := 1; i <= 3; i++ {
			waID := c.prefijo + string(rune('0'+i))
			if err := s.Enqueue(ctx, item("A", c.chat, waID, "fragmento")); err != nil {
				t.Fatalf("Enqueue %s: %v", waID, err)
			}
			// El seq se captura JUSTO tras insertar (la fila aún existe); es el conjunto contra el que se
			// compara al final. Las conversaciones se encolan sin intercalar y ninguna se re-encola
			// después de un sacrificio, así que "lo insertado" es estable.
			clave := "A|" + c.chat
			esperado[clave] = append(esperado[clave], seqDe(t, db, waID))
		}
	}

	// El tope TIENE que haber actuado: si no, el test pasaría trivialmente sin probar nada.
	if got := s.DescartadasPorTope(); got == 0 {
		t.Fatal("el tope no llegó a actuar: el test no está probando nada")
	}

	presentes := seqsNuevasPorConversacion(t, db)
	var vaciadas int
	for clave, quiero := range esperado {
		hay, ok := presentes[clave]
		if !ok || len(hay) == 0 {
			vaciadas++ // conversación sacrificada ENTERA: correcto.
			continue
		}
		if !mismosSeqs(hay, quiero) {
			t.Fatalf("conversación %s CON HUECO: seq presentes %v, insertados %v", clave, hay, quiero)
		}
	}
	if vaciadas == 0 {
		t.Fatal("ninguna conversación se sacrificó entera pese a haber descartes: revisa la capa 3")
	}
	// Y no puede quedar en la cola una conversación que nadie insertó.
	for clave := range presentes {
		if _, ok := esperado[clave]; !ok {
			t.Fatalf("conversación inesperada en la cola: %s", clave)
		}
	}
}

// TestTopeSacrificaPorCapas (T2.10): el orden del sacrificio es despachado → clasificado → pendiente,
// y no es un capricho. Las `despachado` ya salieron al cable (basura pura); las `clasificado` ya tienen
// su intent y el claim NUNCA las vuelve a concatenar, así que perderlas duele pero no abre hueco; las
// `nuevo`/`tomado` son las únicas que pueden partir una conversación, y por eso van las últimas.
func TestTopeSacrificaPorCapas(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 3, 0) // tope 3

	// Una fila por estado, cada una en SU conversación (para que la capa 3 sea predecible: se lleva una
	// conversación de una sola fila). La `despachado` se encola SIN `despachado_en`, así que la poda por
	// TTL no la toca y el único que puede llevársela es el tope.
	desp := item("A", "chat-d@s", "wa-desp", "hola")
	desp.Estado = app.EstadoDespachado
	if err := s.Enqueue(ctx, desp); err != nil {
		t.Fatalf("Enqueue despachado: %v", err)
	}
	clas := item("A", "chat-c@s", "wa-clas", "hola")
	clas.Estado = app.EstadoClasificado
	if err := s.Enqueue(ctx, clas); err != nil {
		t.Fatalf("Enqueue clasificado: %v", err)
	}
	if err := s.Enqueue(ctx, item("A", "chat-n@s", "wa-nue", "hola")); err != nil {
		t.Fatalf("Enqueue nuevo: %v", err)
	}

	// 1er empujón: la cola está en el tope ⇒ capa 1, se va la DESPACHADA y nadie más.
	if err := s.Enqueue(ctx, item("A", "chat-x@s", "wa-x1", "hola")); err != nil {
		t.Fatalf("Enqueue x1: %v", err)
	}
	if existe(t, db, "wa-desp") {
		t.Fatal("capa 1: la fila 'despachado' debía ser la primera en caer")
	}
	if !existe(t, db, "wa-clas") || !existe(t, db, "wa-nue") {
		t.Fatal("capa 1: solo debía caer la 'despachado'; 'clasificado' y 'nuevo' siguen vivas")
	}
	// 🔴 Y LA CAPA 1 NO CUENTA COMO PÉRDIDA: esa fila ya salió al cable, borrarla es limpieza. Si el
	// acumulado subiera aquí, una cola que rota SANO alarmaría igual que una que está tirando mensajes,
	// y la señal de degradación de INV-051.3 dejaría de significar nada.
	if got := s.DescartadasPorTope(); got != 0 {
		t.Fatalf("capa 1: DescartadasPorTope = %d, quería 0 (una 'despachado' borrada no es pérdida)", got)
	}

	// 2º empujón: ya no quedan despachadas ⇒ capa 2, se va la CLASIFICADA, no la 'nuevo'.
	if err := s.Enqueue(ctx, item("A", "chat-y@s", "wa-x2", "hola")); err != nil {
		t.Fatalf("Enqueue x2: %v", err)
	}
	if existe(t, db, "wa-clas") {
		t.Fatal("capa 2: la fila 'clasificado' debía caer antes que la 'nuevo'")
	}
	if !existe(t, db, "wa-nue") {
		t.Fatal("capa 2: la 'nuevo' NO debía caer mientras quedara una 'clasificado'")
	}
	// La capa 2 SÍ es pérdida (esa fila tenía intent y nunca se despachará), pero no es pendiente.
	if got := s.DescartadasPorTope(); got != 1 {
		t.Fatalf("capa 2: DescartadasPorTope = %d, quería 1 (la 'clasificado' sí se pierde)", got)
	}
	if got := s.PendientesDescartadasPorTope(); got != 0 {
		t.Fatalf("capa 2: PendientesDescartadasPorTope = %d, quería 0 (aún no se tocó ninguna pendiente)", got)
	}

	// 3er empujón: solo quedan pendientes ⇒ capa 3, cae la conversación pendiente MÁS VIEJA (chat-n@s).
	if err := s.Enqueue(ctx, item("A", "chat-z@s", "wa-x3", "hola")); err != nil {
		t.Fatalf("Enqueue x3: %v", err)
	}
	if existe(t, db, "wa-nue") {
		t.Fatal("capa 3: agotadas las otras capas, debía caer la conversación pendiente más vieja")
	}
	if got := s.DescartadasPorTope(); got != 2 {
		t.Fatalf("capa 3: DescartadasPorTope = %d, quería 2 (clasificada + pendiente, nunca la despachada)", got)
	}
	if got := s.PendientesDescartadasPorTope(); got != 1 {
		t.Fatalf("capa 3: PendientesDescartadasPorTope = %d, quería 1 (el mensaje que el cajero nunca leerá)", got)
	}
}

// TestTopeConUnaSolaConversacionTermina (T2.10): con la cola llena de fragmentos `nuevo` de UNA sola
// conversación, la capa 3 se lleva la conversación ENTERA de una vez. Se pasa de largo (borra 3 cuando
// sobraba 1) y es deliberado: se prefiere sacrificar de más antes que dejar un hueco. Lo que este test
// vigila es que la función TERMINE (no gire) y deje la cola coherente.
func TestTopeConUnaSolaConversacionTermina(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 3, 0) // tope 3

	for _, id := range []string{"wa-1", "wa-2", "wa-3", "wa-4"} {
		if err := s.Enqueue(ctx, item("A", "chat@s", id, "hola")); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}
	// Los tres primeros eran UNA conversación: se fue entera, y solo queda el que la desalojó.
	ids := waIDs(t, db)
	if len(ids) != 1 || ids[0] != "wa-4" {
		t.Fatalf("capa 3 con una sola conversación: esperaba [wa-4], got %v", ids)
	}
	if got := s.DescartadasPorTope(); got != 3 {
		t.Fatalf("DescartadasPorTope = %d, quería 3 (la conversación entera)", got)
	}
}

// TestCapa3ArrastraLosTomadosDeLaConversacion: la capa 3 borra `nuevo` Y `tomado` de la conversación
// sacrificada, y hasta esta tarea NADIE lo probaba —el único `EstadoTomado` del fichero estaba en el test
// del TTL—. Es el estado peligroso: un `tomado` es un `nuevo` con lease vivo, así que si el barrido de
// leases lo devuelve a `nuevo` vuelve a ser candidato del claim; dejarlo atrás al sacrificar sus hermanas
// reintroduce el HUECO exacto que la capa 3 existe para impedir, y el cajero concatenaría un fragmento
// suelto como si fuera la conversación entera.
func TestCapa3ArrastraLosTomadosDeLaConversacion(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 4, 0) // tope 4

	// La conversación MÁS VIEJA va mezclada: un fragmento `nuevo` y otro ya reclamado (`tomado`).
	if err := s.Enqueue(ctx, item("A", "chat-1@s", "wa-a1", "quiero dos")); err != nil {
		t.Fatalf("Enqueue a1: %v", err)
	}
	tomada := item("A", "chat-1@s", "wa-a2", "empanadas")
	tomada.Estado = app.EstadoTomado
	if err := s.Enqueue(ctx, tomada); err != nil {
		t.Fatalf("Enqueue a2 (tomado): %v", err)
	}
	// Una segunda conversación, más nueva: NO se la puede llevar el sacrificio.
	for _, id := range []string{"wa-b1", "wa-b2"} {
		if err := s.Enqueue(ctx, item("A", "chat-2@s", id, "hola")); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}

	// Con la cola en el tope, este encolado dispara la capa 3 sobre chat-1@s (la del MIN(seq) más bajo).
	if err := s.Enqueue(ctx, item("A", "chat-3@s", "wa-c1", "hola")); err != nil {
		t.Fatalf("Enqueue c1: %v", err)
	}

	if existe(t, db, "wa-a2") {
		t.Fatal("capa 3: el fragmento 'tomado' sobrevivió al sacrificio de sus hermanas — eso es el hueco")
	}
	if existe(t, db, "wa-a1") {
		t.Fatal("capa 3: la conversación más vieja debía irse ENTERA")
	}
	ids := waIDs(t, db)
	if len(ids) != 3 || ids[0] != "wa-b1" || ids[1] != "wa-b2" || ids[2] != "wa-c1" {
		t.Fatalf("capa 3: esperaba [wa-b1,wa-b2,wa-c1], got %v", ids)
	}
	// Las dos filas de la conversación sacrificada son pérdida, y de la clase que más duele.
	if got := s.DescartadasPorTope(); got != 2 {
		t.Fatalf("DescartadasPorTope = %d, quería 2 (nuevo + tomado)", got)
	}
	if got := s.PendientesDescartadasPorTope(); got != 2 {
		t.Fatalf("PendientesDescartadasPorTope = %d, quería 2 (ninguna de las dos llegó al cajero)", got)
	}
}

// TestCapa3EligeConversacionSoloTomada: la conversación más vieja puede no tener NI UNA fila `nuevo` —el
// cajero la reclamó entera y aún no ha cerrado el lote— y aun así es la que toca sacrificar. Fija que la
// elección mira `estado IN (nuevo, tomado)` en las DOS mitades de la sentencia (la que elige y la que
// borra): si la subconsulta solo mirara `nuevo`, esta conversación sería invisible para la capa 3 y el
// bucle del llamante se iría por el guardarraíl dejando la cola por encima del tope sin motivo.
func TestCapa3EligeConversacionSoloTomada(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 4, 0) // tope 4

	for _, id := range []string{"wa-a1", "wa-a2"} {
		tomada := item("A", "chat-1@s", id, "hola")
		tomada.Estado = app.EstadoTomado
		if err := s.Enqueue(ctx, tomada); err != nil {
			t.Fatalf("Enqueue %s (tomado): %v", id, err)
		}
	}
	for _, id := range []string{"wa-b1", "wa-b2"} {
		if err := s.Enqueue(ctx, item("A", "chat-2@s", id, "hola")); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}

	if err := s.Enqueue(ctx, item("A", "chat-3@s", "wa-c1", "hola")); err != nil {
		t.Fatalf("Enqueue c1: %v", err)
	}

	ids := waIDs(t, db)
	if len(ids) != 3 || ids[0] != "wa-b1" || ids[1] != "wa-b2" || ids[2] != "wa-c1" {
		t.Fatalf("la conversación 100%% 'tomado' era la más vieja y debía caer entera: got %v", ids)
	}
	if got := s.PendientesDescartadasPorTope(); got != 2 {
		t.Fatalf("PendientesDescartadasPorTope = %d, quería 2", got)
	}
}

// TestTopeGuardarrailSinConversacionPendiente (T2.10): el anti-bucle-infinito. Si la cola está en el
// tope pero NINGUNA de las tres capas encuentra qué borrar —solo puede pasar con filas en un estado
// ajeno a los cuatro del ciclo, que es justo lo que se siembra aquí a mano—, la capa 3 devuelve 0 y el
// bucle SALE con lo hecho en vez de girar para siempre. El Enqueue sigue adelante y la cola se pasa del
// tope: es el mal menor frente a colgar el listener.
func TestTopeGuardarrailSinConversacionPendiente(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	for i := 1; i <= 3; i++ {
		if _, err := db.Exec(
			`INSERT INTO cola_entrantes (seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc, estado)
			 VALUES (?, 'A', 'chat@s', ?, 1700000000, X'00', 'zombi')`,
			i, "wa-zombi-"+string(rune('0'+i))); err != nil {
			t.Fatalf("sembrar fila zombi %d: %v", i, err)
		}
	}
	s := newStore(t, db, newFakeCrypterFor().fn, 3, 0) // tope 3, ya alcanzado por los zombis

	// Si el guardarraíl no estuviera, esta llamada no volvería nunca.
	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-nuevo", "hola")); err != nil {
		t.Fatalf("Enqueue con la cola bloqueada por zombis: %v", err)
	}
	if got := s.DescartadasPorTope(); got != 0 {
		t.Fatalf("no había nada que las capas pudieran borrar: DescartadasPorTope = %d, quería 0", got)
	}
	if ids := waIDs(t, db); len(ids) != 4 {
		t.Fatalf("se esperaban las 3 zombis + la nueva (el tope cede antes que colgar), hay %d: %v", len(ids), ids)
	}
}

// TestEnqueueSoloTragaLaViolacionDeUnicidad (T2.11) es la razón de ser de la tarea: con
// `INSERT OR IGNORE`, CUALQUIER violación de restricción se convertía en un no-op silencioso con un
// log.Debug —el mensaje se perdía sin que nadie se enterara—. Con el INSERT pelado solo se traga el
// código extendido 2067 (SQLITE_CONSTRAINT_UNIQUE); el resto sube como error.
//
// El DDL de hoy no tiene ningún CHECK (y ninguna columna NOT NULL puede llegar a NULL desde onMessage),
// así que la única forma PORTABLE de provocar "otra violación de restricción" pasando por el camino real
// de Enqueue es un trigger con RAISE(ABORT), que da SQLITE_CONSTRAINT_TRIGGER. Es exactamente el futuro
// que T2.11 blinda: una restricción nueva que NO es la de unicidad.
func TestEnqueueSoloTragaLaViolacionDeUnicidad(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	if _, err := db.Exec(`
		CREATE TRIGGER veta_sesion BEFORE INSERT ON cola_entrantes
		WHEN NEW.session_id = 'VETADA'
		BEGIN
			SELECT RAISE(ABORT, 'restriccion de prueba distinta de la unicidad');
		END`); err != nil {
		t.Fatalf("crear trigger de prueba: %v", err)
	}
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	const secreto = "quiero dos empanadas"
	err := s.Enqueue(ctx, item("VETADA", "chat@s", "wa-1", secreto))
	if err == nil {
		t.Fatal("una violación de restricción que NO es la de unicidad debe propagarse, no tragarse")
	}
	// INV-051.1: el error habla de identificadores, jamás del contenido.
	if strings.Contains(err.Error(), secreto) {
		t.Fatalf("el error filtra el texto del mensaje: %v", err)
	}
	if n := waIDs(t, db); len(n) != 0 {
		t.Fatalf("la fila vetada no debía escribirse, hay %d", len(n))
	}
	// Y la sesión sin vetar sigue encolando con normalidad (el trigger no rompe el camino feliz).
	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-2", "hola")); err != nil {
		t.Fatalf("Enqueue de una sesión no vetada: %v", err)
	}
}

// TestEnqueueDuplicadoConsumeSuSeq (T2.11): el duplicado se traga por la vía del ERROR de unicidad, no
// por la de "0 filas afectadas", pero el HUECO en la secuencia sigue siendo el mismo — el `seq` ya se
// reservó antes del INSERT y se pierde. Se fija aquí porque es el efecto observable del cambio y el
// contrato del que dependen el claim y el despachador (ordenan por seq, no exigen contigüidad).
func TestEnqueueDuplicadoConsumeSuSeq(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	dup := item("A", "chat@s", "wa-dup", "hola")
	if err := s.Enqueue(ctx, dup); err != nil {
		t.Fatalf("Enqueue (1ª vez): %v", err)
	}
	if err := s.Enqueue(ctx, dup); err != nil { // consume el seq 2 y lo tira
		t.Fatalf("el duplicado NO es un fallo: %v", err)
	}
	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-otro", "hola")); err != nil {
		t.Fatalf("Enqueue posterior: %v", err)
	}
	if got := seqDe(t, db, "wa-dup"); got != 1 {
		t.Fatalf("seq del original: got %d want 1", got)
	}
	if got := seqDe(t, db, "wa-otro"); got != 3 {
		t.Fatalf("seq tras el duplicado: got %d want 3 (el 2 se lo comió el duplicado)", got)
	}
}

// TestEnqueueDuplicadoEsIdempotente: el MISMO (session_id, wa_message_id) dos veces ⇒ SIN error y UNA
// sola fila. whatsmeow re-emite eventos al reconectar, así que este es el caso normal, no una anomalía:
// devolver error aquí llenaría el log de campo. Otra sesión con el mismo wa_message_id SÍ es otra fila.
func TestEnqueueDuplicadoEsIdempotente(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	dup := item("A", "chat@s", "wa-dup", "hola")
	if err := s.Enqueue(ctx, dup); err != nil {
		t.Fatalf("Enqueue (1ª vez): %v", err)
	}
	if err := s.Enqueue(ctx, dup); err != nil {
		t.Fatalf("el duplicado NO es un fallo, debía devolver nil: %v", err)
	}

	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM cola_entrantes WHERE session_id='A' AND wa_message_id='wa-dup'`).Scan(&n); err != nil {
		t.Fatalf("contar: %v", err)
	}
	if n != 1 {
		t.Fatalf("el duplicado no debía crear fila: hay %d", n)
	}

	// La unicidad es POR SESIÓN: el mismo id de WhatsApp en otra sesión es otro mensaje.
	otra := item("B", "chat@s", "wa-dup", "hola")
	if err := s.Enqueue(ctx, otra); err != nil {
		t.Fatalf("Enqueue en otra sesión: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM cola_entrantes WHERE wa_message_id='wa-dup'`).Scan(&n); err != nil {
		t.Fatalf("contar total: %v", err)
	}
	if n != 2 {
		t.Fatalf("la unicidad es (session_id, wa_message_id): esperaba 2 filas, hay %d", n)
	}
}

// TestMigracionColaEsIdempotente (T1.9): el runner de migraciones NO lleva tabla de versión y RE-EJECUTA
// el fichero entero en CADA arranque, así que aplicarlo dos veces sobre la misma BD no puede fallar.
func TestMigracionColaEsIdempotente(t *testing.T) {
	ctx := context.Background()
	db := openDB(t) // ya aplicó la migración una vez
	if err := infradb.MigrateCola(ctx, db); err != nil {
		t.Fatalf("segunda aplicación de MigrateCola: %v", err)
	}
	// Y la tabla sigue usable tras reaplicarla.
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)
	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-1", "hola")); err != nil {
		t.Fatalf("Enqueue tras remigrar: %v", err)
	}
}

// waIDs devuelve los wa_message_id de la cola en orden de seq.
func waIDs(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT wa_message_id FROM cola_entrantes ORDER BY seq ASC`)
	if err != nil {
		t.Fatalf("listar filas: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("escanear: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("recorrer: %v", err)
	}
	return ids
}

// seqDe devuelve el seq de la fila con ese wa_message_id (falla el test si no está).
func seqDe(t *testing.T, db *sql.DB, waID string) int64 {
	t.Helper()
	var seq int64
	if err := db.QueryRow(`SELECT seq FROM cola_entrantes WHERE wa_message_id = ?`, waID).Scan(&seq); err != nil {
		t.Fatalf("leer seq de %s: %v", waID, err)
	}
	return seq
}

// existe dice si la fila con ese wa_message_id sigue en la cola.
func existe(t *testing.T, db *sql.DB, waID string) bool {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM cola_entrantes WHERE wa_message_id = ?`, waID).Scan(&n); err != nil {
		t.Fatalf("contar %s: %v", waID, err)
	}
	return n > 0
}

// seqsNuevasPorConversacion agrupa los seq de las filas `nuevo` por conversación ("session|chat"), en
// orden ascendente. Es la foto contra la que se comprueba que ninguna conversación quedó con un hueco:
// el claim del cajero solo concatena filas `nuevo` de una misma (session_id, chat_jid), así que ese es
// el conjunto exacto donde un hueco se convertiría en una mentira semántica.
func seqsNuevasPorConversacion(t *testing.T, db *sql.DB) map[string][]int64 {
	t.Helper()
	rows, err := db.Query(
		`SELECT session_id, chat_jid, seq FROM cola_entrantes WHERE estado = ? ORDER BY seq ASC`,
		app.EstadoNuevo)
	if err != nil {
		t.Fatalf("listar conversaciones: %v", err)
	}
	defer func() { _ = rows.Close() }()
	porConv := make(map[string][]int64)
	for rows.Next() {
		var sessionID, chatJID string
		var seq int64
		if err := rows.Scan(&sessionID, &chatJID, &seq); err != nil {
			t.Fatalf("escanear conversación: %v", err)
		}
		porConv[sessionID+"|"+chatJID] = append(porConv[sessionID+"|"+chatJID], seq)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("recorrer conversaciones: %v", err)
	}
	return porConv
}

// mismosSeqs compara dos listas de seq YA ORDENADAS ascendentemente (las dos lo están: la esperada se
// construye en orden de inserción y la presente sale con ORDER BY seq ASC).
func mismosSeqs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time { return c.t }
