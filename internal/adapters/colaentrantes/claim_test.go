package colaentrantes

// claim_test.go — el lado CAJERO del store (Plan 051 Ola 2 · T2.1 / T2.7).
//
// Reutiliza los helpers de colaentrantes_test.go (openDB con la migración REAL, newStore, el
// fakeCrypterFor con DEK derivada del session_id, fakeClock): son el mismo paquete a propósito, para que
// el esquema de los tests no se desincronice del de producción.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-shared/envelope"
)

// ─────────────────────────── helpers propios del lado cajero ───────────────────────────

// resumenLote describe un lote para un mensaje de fallo SIN volcar su contenido: solo identificadores y
// cardinalidades.
//
// 🔴 EXISTE PARA NO ESCRIBIR `%+v` DE UN *app.ColaLote. Un lote reclamado lleva el Texto YA DESCIFRADO, y
// `%+v` lo imprime entero — que es exactamente lo que INV-051.1 prohíbe. Aquí son fixtures («m1», «hola»)
// y no hay PII real, pero el patrón es el que acaba copiado a un log de producción; se corta en el test.
func resumenLote(l *app.ColaLote) string {
	if l == nil {
		return "<nil>"
	}
	partes := make([]string, 0, len(l.Mensajes))
	for _, m := range l.Mensajes {
		partes = append(partes, resumenMensaje(&m))
	}
	return fmt.Sprintf("lote(session_id=%s chat_jid=%s tomado_en=%d filas=%d)[%s]",
		l.SessionID, l.ChatJID, l.TomadoEn, len(l.Mensajes), strings.Join(partes, " "))
}

// resumenMensaje: la misma regla para UNA fila (p.ej. el Ultimo() del lote).
func resumenMensaje(m *app.ColaMensaje) string {
	if m == nil {
		return "<nil>"
	}
	return fmt.Sprintf("{id=%d seq=%d wa_message_id=%s}", m.ID, m.Seq, m.WAMessageID)
}

// sellar cifra un texto con la MISMA DEK que usaría el Store para esa sesión. Hace falta para sembrar
// filas a mano por *sql.DB (con `id` y `seq` elegidos), que es la única forma de DESALINEAR el rowid del
// seq y probar así que el orden del lote no depende del cursor.
func sellar(t *testing.T, session, texto string) []byte {
	t.Helper()
	env, err := envelope.NewEnvelope(dekFor(session))
	if err != nil {
		t.Fatalf("NewEnvelope(%s): %v", session, err)
	}
	blob, err := env.Seal([]byte(texto))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return blob
}

// sembrar inserta una fila con id, seq, estado y tomado_en EXPLÍCITOS, saltándose Enqueue (que asigna
// seq = id por construcción y nunca produciría el desalineamiento que hay que probar).
func sembrar(t *testing.T, db *sql.DB, id, seq int64, session, chat, waID, texto, estado string, tomadoEn sql.NullInt64) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO cola_entrantes (id, seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc, estado, tomado_en)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, seq, session, chat, waID, int64(1_700_000_000), sellar(t, session, texto), estado, tomadoEn); err != nil {
		t.Fatalf("sembrar fila id=%d seq=%d: %v", id, seq, err)
	}
}

// sembrarNuevo: fila pendiente de reclamar, sin lease.
func sembrarNuevo(t *testing.T, db *sql.DB, id, seq int64, session, chat, waID, texto string) {
	t.Helper()
	sembrar(t, db, id, seq, session, chat, waID, texto, app.EstadoNuevo, sql.NullInt64{})
}

// sembrarTomado: fila con lease vivo o vencido según tomadoEn (epoch-segundos).
func sembrarTomado(t *testing.T, db *sql.DB, id, seq int64, session, chat, waID, texto string, tomadoEn int64) {
	t.Helper()
	sembrar(t, db, id, seq, session, chat, waID, texto, app.EstadoTomado, sql.NullInt64{Int64: tomadoEn, Valid: true})
}

func estadoDe(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var estado string
	if err := db.QueryRow(`SELECT estado FROM cola_entrantes WHERE id = ?`, id).Scan(&estado); err != nil {
		t.Fatalf("leer estado de id=%d: %v", id, err)
	}
	return estado
}

func intentDe(t *testing.T, db *sql.DB, id int64) sql.NullString {
	t.Helper()
	var intent sql.NullString
	if err := db.QueryRow(`SELECT intent_json FROM cola_entrantes WHERE id = ?`, id).Scan(&intent); err != nil {
		t.Fatalf("leer intent_json de id=%d: %v", id, err)
	}
	return intent
}

// claimTokenDe lee el token de fencing persistido en una fila. Es la ÚNICA forma de comprobar de quién
// es la fila realmente: el estado dice que está tomada, pero no por quién.
func claimTokenDe(t *testing.T, db *sql.DB, id int64) sql.NullString {
	t.Helper()
	var tok sql.NullString
	if err := db.QueryRow(`SELECT claim_token FROM cola_entrantes WHERE id = ?`, id).Scan(&tok); err != nil {
		t.Fatalf("leer claim_token de id=%d: %v", id, err)
	}
	return tok
}

// contarEnEstado devuelve cuántas filas hay en un estado (cardinalidad, nunca contenido).
func contarEnEstado(t *testing.T, db *sql.DB, estado string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM cola_entrantes WHERE estado = ?`, estado).Scan(&n); err != nil {
		t.Fatalf("contar filas en estado %q: %v", estado, err)
	}
	return n
}

// ─────────────────────────── T2.1 · el claim ───────────────────────────

// TestReclamarSeLlevaLaConversacionEntera: 5 filas `nuevo` de la MISMA conversación ⇒ UN claim se las
// lleva LAS 5, en un solo lote. Es la razón de ser del claim por conversación (design §4): mensaje +
// hijos son un solo turno semántico, y se concatenan para UNA sola inferencia. Cinco filas ⇒ un lote.
// maxFilas=0 comprueba de paso el default (20).
func TestReclamarSeLlevaLaConversacionEntera(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	for i := 1; i <= 5; i++ {
		if err := s.Enqueue(ctx, item("A", "chat@s", fmt.Sprintf("wa-%d", i), fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	lote, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar: %v", err)
	}
	if lote == nil {
		t.Fatal("el claim devolvió lote nil con 5 filas nuevas en la cola")
	}
	if lote.SessionID != "A" || lote.ChatJID != "chat@s" {
		t.Fatalf("conversación del lote: got (%s,%s) want (A,chat@s)", lote.SessionID, lote.ChatJID)
	}
	if len(lote.Mensajes) != 5 {
		t.Fatalf("filas del lote: got %d want 5 (mensaje + hijos en UN solo claim)", len(lote.Mensajes))
	}
	for i, m := range lote.Mensajes {
		if want := fmt.Sprintf("m%d", i+1); m.Texto != want {
			t.Fatalf("mensaje %d: texto %q, esperaba %q", i, m.Texto, want)
		}
	}
	// Y quedaron marcadas con su lease: nadie más se las puede llevar.
	if n := contarEnEstado(t, db, app.EstadoTomado); n != 5 {
		t.Fatalf("filas en 'tomado' tras el claim: got %d want 5", n)
	}
	if n := contarEnEstado(t, db, app.EstadoNuevo); n != 0 {
		t.Fatalf("no debía quedar ninguna fila 'nuevo', quedan %d", n)
	}
}

// TestReclamarToperaEnMaxFilas: 25 filas de la MISMA conversación ⇒ el claim se lleva 20 (el default) y
// deja 5 para el siguiente. Es el guardarraíl del design §4: una conversación gigante NO monopoliza al
// cajero — y, sobre todo, no se pierde nada, se reclama en el turno siguiente.
func TestReclamarToperaEnMaxFilas(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	for i := 1; i <= 25; i++ {
		if err := s.Enqueue(ctx, item("A", "chat@s", fmt.Sprintf("wa-%d", i), "hola")); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	primero, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar (1º): %v", err)
	}
	if primero == nil {
		t.Fatal("el primer claim devolvió nil con 25 filas nuevas en la cola")
	}
	if len(primero.Mensajes) != defaultClaimMaxFilas {
		t.Fatalf("primer claim: got %d filas, want %d (el tope por claim)", len(primero.Mensajes), defaultClaimMaxFilas)
	}
	segundo, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar (2º): %v", err)
	}
	if segundo == nil || len(segundo.Mensajes) != 5 {
		t.Fatalf("segundo claim: esperaba las 5 restantes, got %s", resumenLote(segundo))
	}
	// El tope RECORTA por seq, no al azar: el primer lote son las 20 más viejas.
	//
	// Se indexa la ÚLTIMA fila por len()-1 y no por un 19 fijo: si el default bajara, un índice fijo haría
	// panic («index out of range») en vez de fallar con un mensaje que dice qué pasó.
	ultimoDelPrimero := primero.Mensajes[len(primero.Mensajes)-1]
	if primero.Mensajes[0].Seq != 1 || ultimoDelPrimero.Seq != int64(defaultClaimMaxFilas) ||
		segundo.Mensajes[0].Seq != int64(defaultClaimMaxFilas)+1 {
		t.Fatalf("el tope debía recortar por seq: lote1=[%d..%d] lote2 empieza en %d (tope=%d)",
			primero.Mensajes[0].Seq, ultimoDelPrimero.Seq, segundo.Mensajes[0].Seq, defaultClaimMaxFilas)
	}
}

// TestReclamarDevuelveOrdenadoPorSeq ES LA ASERCIÓN QUE IMPORTA, la que caza el fallo que pasa todos los
// demás gates: `UPDATE … RETURNING` NO respeta el `ORDER BY` y entrega en orden de `rowid` (medido en
// T2.0: se pidió [seq 10, seq 20] y llegó [seq 20, seq 10]).
//
// Se siembran las filas a mano DESALINEANDO id y seq (id 1→seq 30, id 2→seq 10, id 3→seq 20), que es lo
// que Enqueue nunca produciría. Sin el sort.Slice de Reclamar, el lote sale en orden de rowid y el
// cajero concatena el párrafo del revés: nada falla, nada se loguea, y el LLM clasifica mal.
func TestReclamarDevuelveOrdenadoPorSeq(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)

	sembrarNuevo(t, db, 1, 30, "A", "chat@s", "wa-c", "tercero")
	sembrarNuevo(t, db, 2, 10, "A", "chat@s", "wa-a", "primero")
	sembrarNuevo(t, db, 3, 20, "A", "chat@s", "wa-b", "segundo")

	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)
	lote, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar: %v", err)
	}
	if lote == nil || len(lote.Mensajes) != 3 {
		t.Fatalf("esperaba un lote de 3 filas, got %s", resumenLote(lote))
	}

	wantSeq := []int64{10, 20, 30}
	wantTexto := []string{"primero", "segundo", "tercero"}
	for i, m := range lote.Mensajes {
		if m.Seq != wantSeq[i] {
			t.Fatalf("posición %d: seq %d, esperaba %d — el lote NO viene ordenado por seq (¿falta el sort?)",
				i, m.Seq, wantSeq[i])
		}
		if m.Texto != wantTexto[i] {
			t.Fatalf("posición %d: texto %q, esperaba %q (orden semántico del turno)", i, m.Texto, wantTexto[i])
		}
	}
	// Y el último del lote —donde se escribe el intent— es el de MAYOR seq, no el de mayor rowid.
	if u := lote.Ultimo(); u == nil || u.Seq != 30 || u.ID != 1 {
		t.Fatalf("Ultimo(): got %s, esperaba la fila seq=30 (id=1)", resumenMensaje(u))
	}
}

// TestReclamarEligeLaConversacionMasAntiguaYNoMezcla: con varias conversaciones pendientes, el claim
// elige la que tiene la fila `nuevo` de seq MÁS BAJO y se lleva SOLO las suyas. Mezclar dos
// conversaciones en un lote sería concatenar dos turnos ajenos en el mismo prompt.
func TestReclamarEligeLaConversacionMasAntiguaYNoMezcla(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)

	// La más antigua es (A, chat-1): tiene la seq 10. (B, chat-2) es OTRA sesión, y (A, chat-3) es la
	// misma sesión pero otro chat: ninguna de las dos puede colarse en el lote.
	sembrarNuevo(t, db, 1, 10, "A", "chat-1", "wa-1", "primero de A")
	sembrarNuevo(t, db, 2, 20, "B", "chat-2", "wa-2", "de otra sesión")
	sembrarNuevo(t, db, 3, 30, "A", "chat-3", "wa-3", "otro chat de A")
	sembrarNuevo(t, db, 4, 40, "A", "chat-1", "wa-4", "hijo de A")

	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)
	lote, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar: %v", err)
	}
	if lote == nil {
		t.Fatal("Reclamar devolvió nil con cuatro filas nuevas")
	}
	if lote.SessionID != "A" || lote.ChatJID != "chat-1" {
		t.Fatalf("conversación elegida: got (%s,%s), esperaba la de la seq más baja (A,chat-1)",
			lote.SessionID, lote.ChatJID)
	}
	if len(lote.Mensajes) != 2 {
		t.Fatalf("el lote debía traer SOLO las 2 filas de (A,chat-1), trae %d", len(lote.Mensajes))
	}
	for _, m := range lote.Mensajes {
		if m.Seq != 10 && m.Seq != 40 {
			t.Fatalf("fila ajena en el lote: seq=%d", m.Seq)
		}
	}
	// Las otras dos conversaciones siguen intactas, esperando su turno.
	if got := estadoDe(t, db, 2); got != app.EstadoNuevo {
		t.Fatalf("la fila de la sesión B quedó en %q, debía seguir 'nuevo'", got)
	}
	if got := estadoDe(t, db, 3); got != app.EstadoNuevo {
		t.Fatalf("la fila de (A,chat-3) quedó en %q, debía seguir 'nuevo'", got)
	}
}

// TestReclamarConcurrenteNoSolapa: dos cajeros reclamando a la vez NO pueden llevarse la misma fila
// (T2.0 lo midió: 0 ids comunes). Es lo que hace que el claim pueda ser una sola sentencia sin
// transacción IMMEDIATE a mano — y lo que evita pagar DOS inferencias por el mismo texto.
func TestReclamarConcurrenteNoSolapa(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)

	// Dos conversaciones para que ambas goroutines puedan llevarse algo (si solo hubiera una, la segunda
	// obtendría nil y el test no probaría el solapamiento).
	sembrarNuevo(t, db, 1, 10, "A", "chat-1", "wa-1", "a1")
	sembrarNuevo(t, db, 2, 20, "A", "chat-1", "wa-2", "a2")
	sembrarNuevo(t, db, 3, 30, "B", "chat-2", "wa-3", "b1")
	sembrarNuevo(t, db, 4, 40, "B", "chat-2", "wa-4", "b2")

	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	var wg sync.WaitGroup
	lotes := make([]*app.ColaLote, 2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lotes[i], errs[i] = s.Reclamar(ctx, 0)
		}(i)
	}
	wg.Wait()

	vistos := make(map[int64]int)
	total := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Reclamar concurrente %d: %v", i, err)
		}
		if lotes[i] == nil {
			continue
		}
		for _, m := range lotes[i].Mensajes {
			vistos[m.ID]++
			total++
		}
	}
	for id, veces := range vistos {
		if veces != 1 {
			t.Fatalf("la fila id=%d se reclamó %d veces: dos claims se solaparon", id, veces)
		}
	}
	if total != len(vistos) {
		t.Fatalf("filas devueltas=%d pero ids distintos=%d: hay solapamiento", total, len(vistos))
	}
	// Y ninguna fila quedó reclamada dos veces en la BD.
	if n := contarEnEstado(t, db, app.EstadoTomado); n != total {
		t.Fatalf("filas 'tomado' en la BD = %d, pero los claims devolvieron %d", n, total)
	}
}

// TestReclamarColaVaciaDevuelveNilNil: la cola vacía es el estado NORMAL de un worker que va al día, no
// un error. Si devolviera error, el bucle del worker tendría que distinguir «no hay nada» de «algo se
// rompió» leyendo el texto del error.
func TestReclamarColaVaciaDevuelveNilNil(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	lote, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("cola vacía NO es un error: %v", err)
	}
	if lote != nil {
		t.Fatalf("cola vacía debía devolver lote nil, got %s", resumenLote(lote))
	}
}

// TestReclamarIgnoraLasClasificadas: las filas que nacieron `clasificado` (fastlane: el regex resolvió el
// intent en µs) NO se reclaman NUNCA. Reclamarlas sería pagar una inferencia por un mensaje que ya tiene
// intent, y pisar el intent del fastlane.
func TestReclamarIgnoraLasClasificadas(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	fast := item("A", "chat@s", "wa-fast", "hola")
	fast.Estado = app.EstadoClasificado
	fast.IntentJSON = app.SobreOmitido(app.MotivoFastlane)
	if err := s.Enqueue(ctx, fast); err != nil {
		t.Fatalf("Enqueue fastlane: %v", err)
	}

	lote, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar: %v", err)
	}
	if lote != nil {
		t.Fatalf("una fila 'clasificado' (fastlane) NO se reclama, got %s", resumenLote(lote))
	}

	// Con una fila `nuevo` de la MISMA conversación, el claim se lleva esa y solo esa.
	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-nueva", "quiero dos empanadas")); err != nil {
		t.Fatalf("Enqueue nueva: %v", err)
	}
	lote, err = s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar (2ª): %v", err)
	}
	if lote == nil || len(lote.Mensajes) != 1 || lote.Mensajes[0].WAMessageID != "wa-nueva" {
		t.Fatalf("el claim debía llevarse solo la fila nueva, got %s", resumenLote(lote))
	}
}

// TestReclamarDescifraTextoYMeta: el lote sale EN CLARO EN MEMORIA (es lo que el cajero concatena) y la
// meta NULL se distingue de una meta vacía: Meta queda nil, sin llamar a Open.
func TestReclamarDescifraTextoYMeta(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	sinMeta := item("A", "chat@s", "wa-1", "quiero dos empanadas")
	if err := s.Enqueue(ctx, sinMeta); err != nil {
		t.Fatalf("Enqueue sin meta: %v", err)
	}
	conMeta := item("A", "chat@s", "wa-2", "y una arepa")
	conMeta.Meta = []byte(`{"push_name":"Jhoan"}`)
	if err := s.Enqueue(ctx, conMeta); err != nil {
		t.Fatalf("Enqueue con meta: %v", err)
	}

	lote, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar: %v", err)
	}
	if lote == nil || len(lote.Mensajes) != 2 {
		t.Fatalf("esperaba un lote de 2 filas, got %s", resumenLote(lote))
	}
	if lote.Mensajes[0].Texto != "quiero dos empanadas" || lote.Mensajes[1].Texto != "y una arepa" {
		t.Fatalf("textos descifrados: got %q y %q", lote.Mensajes[0].Texto, lote.Mensajes[1].Texto)
	}
	if lote.Mensajes[0].Meta != nil {
		t.Fatalf("meta NULL debía quedar nil, got %q", string(lote.Mensajes[0].Meta))
	}
	if string(lote.Mensajes[1].Meta) != string(conMeta.Meta) {
		t.Fatalf("meta descifrada: got %q want %q", string(lote.Mensajes[1].Meta), string(conMeta.Meta))
	}
	// Los identificadores de trazabilidad viajan intactos.
	if lote.Mensajes[0].WAMessageID != "wa-1" || lote.Mensajes[1].TSWhatsApp == 0 {
		t.Fatalf("identificadores del lote incompletos: %s", resumenLote(lote))
	}
}

// ─────────────────────────── T2.1 · el cierre del lote ───────────────────────────

// TestMarcarClasificadoCierraElLoteEntero: el intent va SOLO en la última fila (las anteriores son
// fragmentos del mismo párrafo, design §4) y TODAS quedan `clasificado`, listas para el despachador.
func TestMarcarClasificadoCierraElLoteEntero(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	for i := 1; i <= 3; i++ {
		if err := s.Enqueue(ctx, item("A", "chat@s", fmt.Sprintf("wa-%d", i), fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	lote, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar: %v", err)
	}
	if lote == nil || len(lote.Mensajes) != 3 {
		t.Fatalf("esperaba un lote de 3 filas antes de cerrarlo, got %s", resumenLote(lote))
	}

	const intentJSON = `{"intent":"pedido","confidence":0.91}`
	if err := s.MarcarClasificado(ctx, lote, intentJSON); err != nil {
		t.Fatalf("MarcarClasificado: %v", err)
	}

	if n := contarEnEstado(t, db, app.EstadoClasificado); n != 3 {
		t.Fatalf("filas 'clasificado': got %d want 3 (el cierre es todo o nada)", n)
	}
	if n := contarEnEstado(t, db, app.EstadoTomado); n != 0 {
		t.Fatalf("no debía quedar ninguna fila 'tomado', quedan %d", n)
	}
	ultimo := lote.Ultimo()
	if got := intentDe(t, db, ultimo.ID); !got.Valid || got.String != intentJSON {
		t.Fatalf("intent en la última fila (seq=%d): got %+v want %q", ultimo.Seq, got, intentJSON)
	}
	for _, m := range lote.Mensajes[:len(lote.Mensajes)-1] {
		if got := intentDe(t, db, m.ID); got.Valid {
			t.Fatalf("la fila seq=%d NO debía llevar intent propio (es un fragmento del párrafo): %q",
				m.Seq, got.String)
		}
	}
}

// TestMarcarClasificadoIntentVacioQuedaNULL: "" ⇒ NULL, mismo criterio que Enqueue. Una cadena vacía en
// la columna significaría «tiene un intent que es la cadena vacía», que es otra cosa.
func TestMarcarClasificadoIntentVacioQuedaNULL(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-1", "hola")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	lote, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar: %v", err)
	}
	if lote == nil || len(lote.Mensajes) != 1 {
		t.Fatalf("esperaba un lote de 1 fila, got %s", resumenLote(lote))
	}
	if err := s.MarcarClasificado(ctx, lote, ""); err != nil {
		t.Fatalf("MarcarClasificado: %v", err)
	}
	if got := intentDe(t, db, lote.Mensajes[0].ID); got.Valid {
		t.Fatalf("intent vacío debía quedar NULL, got %q", got.String)
	}
	if got := estadoDe(t, db, lote.Mensajes[0].ID); got != app.EstadoClasificado {
		t.Fatalf("estado tras el cierre: got %q want %q", got, app.EstadoClasificado)
	}
}

// TestMarcarClasificadoLoteVacioEsNoOp: lote nil o sin mensajes ⇒ no-op SIN error. Un lote vacío es lo
// que devuelve Reclamar cuando la cola está al día, y el worker no debería tener que comprobarlo.
func TestMarcarClasificadoLoteVacioEsNoOp(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	if err := s.MarcarClasificado(ctx, nil, `{"intent":"x"}`); err != nil {
		t.Fatalf("lote nil debía ser no-op: %v", err)
	}
	if err := s.MarcarClasificado(ctx, &app.ColaLote{SessionID: "A", ChatJID: "chat@s"}, `{"intent":"x"}`); err != nil {
		t.Fatalf("lote vacío debía ser no-op: %v", err)
	}
}

// ────────────── T2.1 · EL FENCING DEL CIERRE (la carrera del lease relevado) ──────────────

// TestMarcarClasificadoTardioNoPisaAlRelevo cubre el cierre tardío contra un relevo QUE YA TERMINÓ.
//
// La secuencia es la de campo, paso a paso: (1) el cajero A reclama y empieza la inferencia; (2) la
// inferencia se pasa de los 60 s del lease; (3) el barrido devuelve las filas a `nuevo`; (4) el cajero B
// las reclama, clasifica y CIERRA; (5) A termina y cierra igual, y debe descartarse.
//
// ⚠️ NO ES EL TEST DEL FENCE, aunque lo pareciera: en el paso (5) las filas ya están `clasificado` por B,
// así que el `AND estado = 'tomado'` del WHERE las excluye sin ayuda del token. Medido por mutación el
// 2026-08-16: quitando el predicado del token, este test sigue en VERDE. Lo que prueba de verdad es que
// un cierre que llega tarde no revive filas ya cerradas, que es un camino distinto y también hay que
// sostener. El fence lo prueba TestMarcarClasificadoTardioConElRelevoAUNVIVO, justo debajo.
//
// El reloj va INYECTADO para materializar el vencimiento del lease de forma determinista (61 s a mano),
// sin dormir.
func TestMarcarClasificadoTardioNoPisaAlRelevo(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0, WithClock(clock.Now))

	for i := 1; i <= 2; i++ {
		if err := s.Enqueue(ctx, item("A", "chat@s", fmt.Sprintf("wa-%d", i), fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	// (1) El cajero A reclama el lote y se pone a clasificar.
	loteA, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar (cajero A): %v", err)
	}
	if loteA == nil || len(loteA.Mensajes) != 2 {
		t.Fatalf("el cajero A debía llevarse 2 filas, got %s", resumenLote(loteA))
	}
	if loteA.ClaimToken == "" {
		t.Fatal("Reclamar no devolvió el token de fencing en ColaLote.ClaimToken: sin él NO hay fence posible")
	}

	// (2) y (3): la inferencia de A se pasa del lease y el barrido rescata las filas.
	clock.t = clock.t.Add(61 * time.Second)
	n, err := s.BarrerLeasesVencidos(ctx, 0) // 0 ⇒ default de 60 s
	if err != nil {
		t.Fatalf("BarrerLeasesVencidos: %v", err)
	}
	if n != 2 {
		t.Fatalf("el barrido debía rescatar las 2 filas del lease vencido, rescató %d", n)
	}

	// (4) El cajero B las reclama y las cierra con SU clasificación.
	loteB, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar (cajero B): %v", err)
	}
	if loteB == nil || len(loteB.Mensajes) != 2 {
		t.Fatalf("el cajero B debía re-reclamar las 2 filas, got %s", resumenLote(loteB))
	}
	if loteB.ClaimToken == loteA.ClaimToken {
		t.Fatal("los dos claims comparten token: el fence no podría distinguirlos")
	}
	const intentB = `{"intent":"pedido","confidence":0.91}`
	if err := s.MarcarClasificado(ctx, loteB, intentB); err != nil {
		t.Fatalf("MarcarClasificado (cajero B, el que sí llegó a tiempo): %v", err)
	}

	// (5) A TERMINA TARDE Y CIERRA IGUAL. Aquí es donde el fence tiene que morder.
	const intentA = `{"intent":"saludo","confidence":0.42}`
	err = s.MarcarClasificado(ctx, loteA, intentA)
	if !errors.Is(err, app.ErrLoteRelevado) {
		t.Fatalf("el cierre TARDÍO debía devolver app.ErrLoteRelevado (INV-051.3: la degradación se cuenta, no solo se loguea); got %v", err)
	}

	// Y —lo que de verdad importa— NO tocó una sola fila: el intent es el de B, no el de A.
	ultimo := loteB.Ultimo()
	if got := intentDe(t, db, ultimo.ID); !got.Valid || got.String != intentB {
		t.Fatalf("el cierre tardío PISÓ el intent del relevo en la fila %s: got %+v, want %q",
			resumenMensaje(ultimo), got, intentB)
	}
	// Ninguna fila anterior ganó un intent propio por el camino.
	for i := range loteB.Mensajes[:len(loteB.Mensajes)-1] {
		m := loteB.Mensajes[i]
		if got := intentDe(t, db, m.ID); got.Valid {
			t.Fatalf("la fila %s no debía llevar intent propio: %q", resumenMensaje(&m), got.String)
		}
	}
	// Estados intactos: las 2 siguen `clasificado` (como las dejó B) y no volvieron a `tomado` ni a `nuevo`.
	if got := contarEnEstado(t, db, app.EstadoClasificado); got != 2 {
		t.Fatalf("filas 'clasificado' tras el cierre tardío: got %d want 2", got)
	}
	if got := contarEnEstado(t, db, app.EstadoTomado); got != 0 {
		t.Fatalf("el cierre tardío dejó %d filas en 'tomado': la transacción debía revertirse ENTERA", got)
	}
	if got := contarEnEstado(t, db, app.EstadoNuevo); got != 0 {
		t.Fatalf("el cierre tardío dejó %d filas en 'nuevo'", got)
	}
}

// TestMarcarClasificadoTardioConElRelevoAUNVIVO ES EL TEST QUE PRUEBA EL FENCE DE VERDAD, y existe
// porque el de arriba NO lo prueba: allí B ya ha CERRADO cuando A llega tarde, así que sus filas están en
// `clasificado` y el `AND estado = 'tomado'` del WHERE ya las excluye él solo. Se verificó por mutación
// (2026-08-16): quitando el predicado del token, aquel test seguía en verde. Cubre otro camino —el del
// relevo que ya terminó—, y por eso se queda; pero la ventana peligrosa es esta otra.
//
// LA VENTANA REAL: B ha reclamado y TODAVÍA ESTÁ CLASIFICANDO. Sus filas siguen `tomado`, que es
// exactamente el estado que A cree tener. Todo lo que distingue el lote de A del de B en ese instante es
// el token del claim: sin él, el `WHERE id IN (…) AND estado = 'tomado'` de A casa las dos filas, el
// RowsAffected cuadra, se hace COMMIT y A cierra el lote de B —le escribe SU intent y se lo pasa a
// `clasificado`— mientras B sigue infiriendo. B terminará y su cierre será el que se descarte. Nada falla,
// nada se loguea: sale a la nube la clasificación del cajero equivocado.
func TestMarcarClasificadoTardioConElRelevoAUNVIVO(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0, WithClock(clock.Now))

	for i := 1; i <= 2; i++ {
		if err := s.Enqueue(ctx, item("A", "chat@s", fmt.Sprintf("wa-%d", i), fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	// (1) El cajero A reclama y se pone a clasificar.
	loteA, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar (cajero A): %v", err)
	}
	if loteA == nil || len(loteA.Mensajes) != 2 {
		t.Fatalf("el cajero A debía llevarse 2 filas, got %s", resumenLote(loteA))
	}
	if loteA.ClaimToken == "" {
		t.Fatal("Reclamar no devolvió token de fencing: sin él no hay nada que probar aquí")
	}

	// (2) y (3): la inferencia de A se pasa del lease y el barrido devuelve las filas a `nuevo`.
	clock.t = clock.t.Add(61 * time.Second)
	n, err := s.BarrerLeasesVencidos(ctx, 0) // 0 ⇒ default de 60 s
	if err != nil {
		t.Fatalf("BarrerLeasesVencidos: %v", err)
	}
	if n != 2 {
		t.Fatalf("el barrido debía rescatar las 2 filas del lease vencido, rescató %d", n)
	}
	// El rescate suelta la fila del todo: si el token de A sobreviviera en una fila ya libre, quedaría
	// diciendo que pertenece a un cajero que la soltó.
	for _, m := range loteA.Mensajes {
		if tok := claimTokenDe(t, db, m.ID); tok.Valid {
			t.Fatalf("la fila %s volvió a 'nuevo' con el token del claim muerto (%q): el barrido debe limpiarlo",
				resumenMensaje(&m), tok.String)
		}
	}

	// (4) EL RELEVO: B las reclama y SE QUEDA CLASIFICANDO. No cierra. Sus filas están `tomado`, igual que
	// A cree que están las suyas — es el instante en el que el estado ya no distingue a nadie.
	loteB, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar (cajero B, el relevo): %v", err)
	}
	if loteB == nil || len(loteB.Mensajes) != 2 {
		t.Fatalf("el cajero B debía re-reclamar las 2 filas, got %s", resumenLote(loteB))
	}
	if loteB.ClaimToken == loteA.ClaimToken {
		t.Fatal("los dos claims comparten token: el fence no podría distinguirlos")
	}
	if got := contarEnEstado(t, db, app.EstadoTomado); got != 2 {
		t.Fatalf("las 2 filas debían quedar 'tomado' por B (lote vivo, sin cerrar), hay %d", got)
	}

	// (5) A TERMINA TARDE Y CIERRA IGUAL, contra un lote que ahora es de B y que sigue abierto. Aquí es
	// donde el fence tiene que morder, y donde SOLO el token puede hacerlo.
	const intentA = `{"intent":"saludo","confidence":0.42}`
	err = s.MarcarClasificado(ctx, loteA, intentA)
	if !errors.Is(err, app.ErrLoteRelevado) {
		t.Fatalf("el cierre tardío sobre un lote AÚN VIVO del relevo debía devolver app.ErrLoteRelevado; got %v", err)
	}

	// Y no tocó nada: las filas siguen siendo de B, abiertas y sin intent.
	for _, m := range loteB.Mensajes {
		if got := estadoDe(t, db, m.ID); got != app.EstadoTomado {
			t.Fatalf("la fila %s quedó en %q: el cierre de A la sacó del lote VIVO de B (debía seguir 'tomado')",
				resumenMensaje(&m), got)
		}
		tok := claimTokenDe(t, db, m.ID)
		if !tok.Valid || tok.String != loteB.ClaimToken {
			t.Fatalf("la fila %s perdió el token del relevo: got %+v, want el de B", resumenMensaje(&m), tok)
		}
		if got := intentDe(t, db, m.ID); got.Valid {
			t.Fatalf("la fila %s ganó el intent del cajero TARDÍO: %q", resumenMensaje(&m), got.String)
		}
	}
	if got := contarEnEstado(t, db, app.EstadoClasificado); got != 0 {
		t.Fatalf("el cierre tardío dejó %d filas en 'clasificado': la transacción debía revertirse ENTERA", got)
	}

	// Y B, cuando termine, SÍ puede cerrar: el fence descarta al tardío, no bloquea al dueño.
	const intentB = `{"intent":"pedido","confidence":0.91}`
	if err := s.MarcarClasificado(ctx, loteB, intentB); err != nil {
		t.Fatalf("el dueño del lote debía poder cerrar tras el intento tardío de A: %v", err)
	}
	if got := intentDe(t, db, loteB.Ultimo().ID); !got.Valid || got.String != intentB {
		t.Fatalf("intent de la última fila tras el cierre de B: got %+v want %q", got, intentB)
	}
	if got := contarEnEstado(t, db, app.EstadoClasificado); got != 2 {
		t.Fatalf("filas 'clasificado' tras el cierre de B: got %d want 2", got)
	}
}

// TestMarcarClasificadoCierraYLimpiaElSello: con el fence puesto, EL CAMINO FELIZ SIGUE INTACTO —
// reclamar y cerrar inmediatamente escribe el intent en la última fila y deja todas en `clasificado`.
//
// Y comprueba además que el cierre PONE `tomado_en` A NULL: el sello ya cumplió su papel de fence y
// dejarlo puesto haría que una fila `clasificado` siguiera pareciendo reclamada por alguien. Hoy sería
// inocuo (el barrido filtra por estado='tomado'), pero el despachador de la Ola 3 lee esta misma tabla y
// no debe encontrarse un campo que miente sobre quién tiene la fila.
func TestMarcarClasificadoCierraYLimpiaElSello(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0, WithClock(clock.Now))

	for i := 1; i <= 3; i++ {
		if err := s.Enqueue(ctx, item("A", "chat@s", fmt.Sprintf("wa-%d", i), fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	lote, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar: %v", err)
	}
	if lote == nil || len(lote.Mensajes) != 3 {
		t.Fatalf("esperaba un lote de 3 filas, got %s", resumenLote(lote))
	}

	const intentJSON = `{"intent":"pedido","confidence":0.91}`
	// Cierre INMEDIATO: mismo instante del reloj, lease vivo, el lote sigue siendo suyo.
	if err := s.MarcarClasificado(ctx, lote, intentJSON); err != nil {
		t.Fatalf("el camino feliz NO debe devolver error con el fence puesto: %v", err)
	}

	ultimo := lote.Ultimo()
	if got := intentDe(t, db, ultimo.ID); !got.Valid || got.String != intentJSON {
		t.Fatalf("intent en la última fila %s: got %+v want %q", resumenMensaje(ultimo), got, intentJSON)
	}
	if got := contarEnEstado(t, db, app.EstadoClasificado); got != 3 {
		t.Fatalf("filas 'clasificado': got %d want 3 (el cierre es todo o nada)", got)
	}
	// El sello Y el token se limpian en TODAS las filas del lote, no solo en la última.
	for i := range lote.Mensajes {
		m := lote.Mensajes[i]
		var tomadoEn sql.NullInt64
		if err := db.QueryRow(`SELECT tomado_en FROM cola_entrantes WHERE id = ?`, m.ID).Scan(&tomadoEn); err != nil {
			t.Fatalf("leer tomado_en de %s: %v", resumenMensaje(&m), err)
		}
		if tomadoEn.Valid {
			t.Fatalf("la fila %s quedó `clasificado` con el sello vivo (tomado_en=%d): el cierre debe ponerlo a NULL",
				resumenMensaje(&m), tomadoEn.Int64)
		}
		if tok := claimTokenDe(t, db, m.ID); tok.Valid {
			t.Fatalf("la fila %s quedó `clasificado` con el token del claim vivo (%q): el cierre debe ponerlo a NULL",
				resumenMensaje(&m), tok.String)
		}
	}
}

// ─────────────────────────── T2.7 · el barrido de leases ───────────────────────────

// TestBarrerLeasesVencidos: con el reloj inyectado, la fila `tomado` desde hace 61 s vuelve a `nuevo` (y
// el claim siguiente se la lleva); la de hace 10 s NO se toca — su cajero sigue vivo, y rescatarla
// pagaría una segunda inferencia por el mismo texto. lease=0 comprueba de paso el default (60 s).
func TestBarrerLeasesVencidos(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	ahora := clock.t.Unix()

	sembrarTomado(t, db, 1, 10, "A", "chat-viejo", "wa-1", "lease vencido", ahora-61)
	sembrarTomado(t, db, 2, 20, "B", "chat-vivo", "wa-2", "lease vivo", ahora-10)

	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0, WithClock(clock.Now))

	n, err := s.BarrerLeasesVencidos(ctx, 0) // 0 ⇒ default de 60 s
	if err != nil {
		t.Fatalf("BarrerLeasesVencidos: %v", err)
	}
	if n != 1 {
		t.Fatalf("filas rescatadas: got %d want 1 (solo la de lease vencido)", n)
	}
	if got := estadoDe(t, db, 1); got != app.EstadoNuevo {
		t.Fatalf("la fila con lease vencido quedó en %q, debía volver a 'nuevo'", got)
	}
	if got := estadoDe(t, db, 2); got != app.EstadoTomado {
		t.Fatalf("la fila con lease VIVO quedó en %q, no se debía tocar", got)
	}
	// El sello se limpia: si no, un barrido posterior la volvería a contar como vencida.
	var tomadoEn sql.NullInt64
	if err := db.QueryRow(`SELECT tomado_en FROM cola_entrantes WHERE id = 1`).Scan(&tomadoEn); err != nil {
		t.Fatalf("leer tomado_en: %v", err)
	}
	if tomadoEn.Valid {
		t.Fatalf("tomado_en debía quedar NULL tras el rescate, got %d", tomadoEn.Int64)
	}

	// Y la rescatada vuelve a ser reclamable: es el punto entero del barrido.
	lote, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar tras el barrido: %v", err)
	}
	if lote == nil || len(lote.Mensajes) != 1 || lote.Mensajes[0].ID != 1 {
		t.Fatalf("el claim debía recuperar la fila rescatada, got %s", resumenLote(lote))
	}
}

// TestBarrerLeasesSinNadaQueRescatar: el caso SANO. Una cola sin leases vencidos devuelve 0 sin error, y
// una fila `tomado` SIN sello (bug del claim) no se toca: ante la duda, no se rescata.
func TestBarrerLeasesSinNadaQueRescatar(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}

	sembrar(t, db, 1, 10, "A", "chat@s", "wa-1", "sin sello", app.EstadoTomado, sql.NullInt64{})
	sembrarNuevo(t, db, 2, 20, "A", "chat@s", "wa-2", "pendiente")

	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0, WithClock(clock.Now))

	n, err := s.BarrerLeasesVencidos(ctx, time.Minute)
	if err != nil {
		t.Fatalf("BarrerLeasesVencidos: %v", err)
	}
	if n != 0 {
		t.Fatalf("no había nada que rescatar, got %d", n)
	}
	if got := estadoDe(t, db, 1); got != app.EstadoTomado {
		t.Fatalf("una fila 'tomado' SIN tomado_en no se toca; quedó en %q", got)
	}
}
