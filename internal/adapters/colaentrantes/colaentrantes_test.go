package colaentrantes

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	infradb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-shared/envelope"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	_ "modernc.org/sqlite" // driver "sqlite" (CGO-free), el mismo que internal/infra/db
)

func testLogger() sharedlogger.Logger {
	return sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}), sharedlogger.WithJSON(true))
}

// openDB abre una BD de cola temporal y le aplica la MIGRACIÓN REAL (db.MigrateCola), no una copia a
// mano del DDL. Es deliberado: un esquema replicado en el test se desincroniza del de producción sin que
// nadie se entere —así fue como el índice único ux_cola_session_wamid faltó en los tests y la
// idempotencia del Enqueue quedó sin cubrir—. Aquí no hay ciclo de imports: internal/infra/db no importa
// ningún paquete interno del agente.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cola_entrantes.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	database.SetMaxOpenConns(1)
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
// NO se exige contigüidad (ni min=1/max=N): con INSERT OR IGNORE un duplicado consume su número de orden
// y lo pierde, así que la secuencia puede tener HUECOS. Es aceptable por contrato — el claim del cajero y
// el despachador ordenan con ORDER BY seq y no necesitan que los números sean consecutivos.
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

// TestDropOldestAlTope: al alcanzar el tope, Enqueue tira las de menor seq y conserva las más nuevas.
func TestDropOldestAlTope(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 3, 0) // tope 3

	for _, id := range []string{"wa-1", "wa-2", "wa-3", "wa-4", "wa-5"} {
		if err := s.Enqueue(ctx, item("A", "chat@s", id, "hola")); err != nil {
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
func TestDescartadasPorTopeSeCuenta(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 3, 0) // tope 3

	if got := s.DescartadasPorTope(); got != 0 {
		t.Fatalf("contador inicial = %d, quería 0", got)
	}
	for _, id := range []string{"wa-1", "wa-2", "wa-3", "wa-4", "wa-5"} {
		if err := s.Enqueue(ctx, item("A", "chat@s", id, "hola")); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}
	// Con tope 3, los encolados 4º y 5º tiran una fila cada uno.
	if got := s.DescartadasPorTope(); got != 2 {
		t.Fatalf("DescartadasPorTope = %d, quería 2", got)
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

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time { return c.t }
