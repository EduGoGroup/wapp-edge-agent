package colaentrantes

// circuito_ola3_test.go — EL CIRCUITO COMPLETO DE LA OLA 3, CONTRA LA BD REAL (Plan 051 · T3.5).
//
// QUÉ AÑADE ESTE FICHERO Y POR QUÉ NO SOBRA. Los tests de `internal/app/despachador` ejercitan el bucle
// contra una `colaFake` (una lista en memoria que imita el SQL), y los de `despacho_test.go` ejercitan el
// SQL sin bucle. Cada mitad está bien probada; lo que NADIE probaba es la COSTURA entre ambas — que la
// fila que el store devuelve sea la que el bucle sabe leer, que el sobre que el store persiste sea el que
// el bucle sabe interpretar, y que el sello que el bucle pide sea el que la poda sabe borrar. Un fake se
// adapta al código sin protestar; una BD no.
//
// Por eso aquí el despachador REAL (internal/app/despachador) se cablea contra el *Store REAL, sobre una
// BD abierta por `openDB` — es decir, por `infradb.Open` + `db.MigrateCola`, el mismo camino que
// producción. 🔴 NINGÚN DDL TRANSCRITO A MANO (reglas T2.17/T2.18): la tabla y sus columnas guardadas en
// Go (`claim_token`, `intentos`) las crea la migración de verdad.
//
// ─── DETERMINISMO: LAS DOS COSTURAS, Y CERO time.Sleep COMO SINCRONÍA ───
//
//  1. EL RELOJ es uno solo y es INYECTADO EN LOS DOS LADOS: `WithClock(reloj.ahora)` en el store (que es
//     quien escribe `despachado_en` y quien corta la poda) y `Deps.Ahora` en el despachador (que es quien
//     mide el presupuesto). Que sea el MISMO reloj es lo que permite escribir un test que avanza 25 h y
//     significa lo mismo para ambos.
//
//     ⚠️ NO SE REUSA `fakeClock` (colaentrantes_test.go), y no es capricho: ese struct es `{t time.Time}`
//     SIN candado, y sus tests lo mutan en el mismo hilo. Aquí el despachador LEE el reloj desde OTRA
//     goroutine mientras el test lo ESCRIBE — con `fakeClock` eso es una carrera de libro, y `-race` la
//     cazaría (o peor: no la cazaría hoy y sí el martes). `o3Reloj` es el mismo juguete con un mutex.
//
//  2. EL DESPERTADOR es manual: «una vuelta del bucle» pasa a ser un evento que el test observa
//     (`esperarParada`) y libera (`despertar`), en vez de una apuesta sobre cuánto tarda algo. Los
//     `time.After` que aparecen abajo NO son sincronía: son guardias anti-cuelgue para que un fallo salga
//     como un test rojo en 3 s y no como un CI colgado.
//
// La ÚNICA excepción es el test de cierre limpio multisesión, que corre con el `PollFijo` de producción a
// propósito (ver su cabecera): allí lo que se ejercita es justamente la concurrencia real bajo `-race`, y
// aun ahí la sincronía del test son canales y un `Wait`, nunca un sleep.
//
// INV-051.1 se respeta igual que en el resto del paquete: ningún mensaje de fallo imprime un
// `app.ColaCabeza` ni un `domain.InboundEvent` con `%+v` — eso sacaría el texto y el push_name a la salida
// de CI, que es un log más. Se imprimen identificadores y cardinalidades.

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/despachador"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// guardiaO3 es la guardia ANTI-CUELGUE de este fichero. No es un tiempo de espera «a ver si llega»: todo
// lo que se espera aquí llega por un canal en cuanto ocurre, y este plazo solo existe para que un bucle
// que se atascó muera como test rojo en vez de colgar el CI.
const guardiaO3 = 3 * time.Second

// ─────────────────────────────────────────────────────────────────────────────
// Dobles y arnés
// ─────────────────────────────────────────────────────────────────────────────

// o3Reloj es el reloj falso COMPARTIDO por el store y el despachador. Con candado: lo lee la goroutine del
// bucle y lo escribe el test (ver la cabecera del fichero).
type o3Reloj struct {
	mu sync.Mutex
	t  time.Time
}

func nuevoO3Reloj() *o3Reloj {
	return &o3Reloj{t: time.Unix(1_700_000_000, 0).UTC()}
}

func (r *o3Reloj) ahora() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.t
}

func (r *o3Reloj) avanzar(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.t = r.t.Add(d)
}

// o3Sink es el destino de la entrega: registra los eventos EN ORDEN y los publica por un canal para que el
// test pueda esperarlos sin dormir.
//
// `cerrado` es la red del bloque (g): una vez el test ha hecho el join de los despachadores, lo levanta, y
// cualquier entrega posterior queda registrada como VIOLACIÓN en vez de pasar desapercibida.
type o3Sink struct {
	mu         sync.Mutex
	entregados []domain.InboundEvent
	entregas   chan domain.InboundEvent

	cerrado          atomic.Bool
	entregasTrasStop atomic.Int64
}

func nuevoO3Sink() *o3Sink {
	return &o3Sink{entregas: make(chan domain.InboundEvent, 256)}
}

func (s *o3Sink) Deliver(_ context.Context, evt domain.InboundEvent) error {
	if s.cerrado.Load() {
		s.entregasTrasStop.Add(1)
	}
	s.mu.Lock()
	s.entregados = append(s.entregados, evt)
	s.mu.Unlock()
	select {
	case s.entregas <- evt:
	default:
	}
	return nil
}

func (s *o3Sink) cuantos() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entregados)
}

// ids proyecta lo entregado a SOLO los identificadores de mensaje. Existe para que ningún mensaje de fallo
// imprima un domain.InboundEvent entero (INV-051.1: lleva Text y PushName).
func (s *o3Sink) ids() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.entregados))
	for _, e := range s.entregados {
		out = append(out, e.MessageID)
	}
	return out
}

var _ app.InboundSink = (*o3Sink)(nil)

// o3Despertador convierte las DOS mitades de «el bucle espera» en eventos observables: `esperando` avisa de
// que la vuelta anterior TERMINÓ DEL TODO y el bucle acaba de aparcar; `c` lo libera para la siguiente.
// Tenerlas separadas es lo que permite leer un contador sabiendo que ya se escribió, y sembrar trabajo
// nuevo sabiendo con certeza que el bucle aún no lo ha mirado.
type o3Despertador struct {
	c         chan struct{}
	esperando chan struct{}
}

func nuevoO3Despertador() *o3Despertador {
	return &o3Despertador{c: make(chan struct{}), esperando: make(chan struct{}, 256)}
}

func (d *o3Despertador) Esperar(ctx context.Context) error {
	select {
	case d.esperando <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.c:
		return nil
	}
}

var _ despachador.Despertador = (*o3Despertador)(nil)

// o3Arnes arranca UN despachador real sobre la cola real y da las costuras para gobernarlo.
type o3Arnes struct {
	d       *despachador.Despachador
	sink    *o3Sink
	desp    *o3Despertador
	cancel  context.CancelFunc
	retorno chan error
}

// arrancarDespachador cablea el despachador REAL contra la cola REAL. `ahora` es el mismo reloj que se le
// pasó al store con WithClock: ver la cabecera del fichero.
func arrancarDespachador(t *testing.T, cola app.ColaDespachador, sessionID string, ahora func() time.Time, presupuesto time.Duration) *o3Arnes {
	t.Helper()
	sink := nuevoO3Sink()
	desp := nuevoO3Despertador()
	d, err := despachador.New(despachador.Deps{
		Cola:        cola,
		Sink:        sink,
		SessionID:   sessionID,
		Log:         testLogger(),
		Ahora:       ahora,
		Presupuesto: presupuesto,
		Despertador: desp,
	})
	if err != nil {
		t.Fatalf("despachador.New(%s): %v", sessionID, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := &o3Arnes{d: d, sink: sink, desp: desp, cancel: cancel, retorno: make(chan error, 1)}
	go func() { a.retorno <- d.Run(ctx) }()
	t.Cleanup(a.parar)
	return a
}

// esperarParada bloquea hasta que el bucle APARQUE en Esperar, SIN liberarlo. Al volver se sabe dos cosas:
// que la vuelta anterior terminó del todo (los contadores ya están escritos) y que el bucle está detenido
// en un punto conocido.
func (a *o3Arnes) esperarParada(t *testing.T) {
	t.Helper()
	select {
	case <-a.desp.esperando:
	case <-time.After(guardiaO3):
		t.Fatal("el bucle del despachador no llegó a aparcar en Esperar")
	}
}

// despertar libera UNA espera. El envío es sobre un canal SIN buffer, así que también sincroniza: al
// volver, el bucle ha recibido el permiso y arranca la vuelta siguiente.
func (a *o3Arnes) despertar(t *testing.T) {
	t.Helper()
	select {
	case a.desp.c <- struct{}{}:
	case <-time.After(guardiaO3):
		t.Fatal("el bucle no estaba esperando (¿se quedó atascado entregando o leyendo?)")
	}
}

// vuelta ejecuta UNA iteración completa y observable: libera al bucle y espera a que vuelva a aparcar.
//
// ⚠️ SOLO VALE CUANDO NO HAY PROGRESO. Si la vuelta entrega o sella, el bucle NO aparca (vuelve a mirar de
// inmediato, por diseño), así que el `esperarParada` se resolvería con la parada siguiente. Se usa
// justamente en los tramos en que la afirmación del test es «aquí no debe pasar nada».
func (a *o3Arnes) vuelta(t *testing.T) {
	t.Helper()
	a.despertar(t)
	a.esperarParada(t)
}

func (a *o3Arnes) esperarEntrega(t *testing.T) domain.InboundEvent {
	t.Helper()
	select {
	case evt := <-a.sink.entregas:
		return evt
	case <-time.After(guardiaO3):
		t.Fatalf("no llegó la entrega esperada (entregados hasta ahora: %v)", a.sink.ids())
		return domain.InboundEvent{}
	}
}

// parar cancela y espera a que `Run` salga, para que ningún test deje una goroutine viva detrás.
//
// NO comprueba que el retorno sea nil, y es deliberado: `parar` se usa desde `t.Cleanup`, donde un
// `t.Fatalf` corre en una goroutine distinta de la del test. La afirmación de que **`Run` retorna
// limpio (nil) en una parada ordenada** —que a una sesión la cancelen no es un fallo, y devolver
// error ahí haría que el supervisor lo tratara como una caída— vive donde puede fallar de verdad:
// en `TestCircuitoCierreLimpioTresSesionesConcurrentes`, que hace el JOIN de los tres `Run` en el
// cuerpo del test y comprueba el error de cada uno.
func (a *o3Arnes) parar() {
	a.cancel()
	select {
	case <-a.retorno:
	case <-time.After(guardiaO3):
	}
}

// o3ItemConMeta es el `item` del paquete CON metadatos de negocio: hacen falta para comprobar que el
// `meta_enc` sobrevive al viaje entero (sellado con la DEK de la sesión, guardado, descifrado y
// deserializado por `app.DecodeColaMeta` en el despachador). Sin meta, ese contrato de claves JSON —el
// fallo caro de la ola— no se ejercitaría.
func o3ItemConMeta(session, chat, waID, texto string) app.ColaItem {
	it := item(session, chat, waID, texto)
	it.Meta = []byte(`{"sender":"593999123456@s.whatsapp.net","sender_alt":"111@lid","addressing_mode":"pn","push_name":"Ana","type":"text"}`)
	return it
}

// ─────────────────────────────────────────────────────────────────────────────
// (a) FIFO — la fila lista NO adelanta a la que espera a un worker LENTO
// ─────────────────────────────────────────────────────────────────────────────

// TestCircuitoFIFOLaFilaListaNoAdelantaALaQueEsperaAlWorkerLento es LA INVARIANTE QUE COMPRA LA OLA
// (REQ-051.18), ejercitada de punta a punta contra la BD real.
//
// El escenario es EXACTAMENTE el que la rompería si el FIFO no existiera:
//   - la fila 1 (seq menor) es un texto normal: nace `nuevo` y el WORKER LENTO se la lleva a `tomado` y no
//     la cierra — está «infiriendo»;
//   - la fila 2 es POSTERIOR y ya nació `clasificado` (el fastlane la resolvió en µs, que es el camino más
//     rápido que existe en el Edge).
//
// Sin FIFO, la 2 saldría primero y la conversación llegaría al cliente final del revés. Con FIFO, la 2
// espera detrás de la 1 aunque esté lista, y las dos salen EN ORDEN cuando el worker por fin cierra.
//
// El worker lento se simula con `Reclamar` SIN `MarcarClasificado`: no es una imitación del cajero, es el
// cajero de verdad a medio trabajo. Y no hace falta dormir para que sea «lento» — el reloj del test no
// avanza, así que el presupuesto (un minuto) no puede vencer y la única razón por la que la fila 2 no sale
// es el orden.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (leído del SQL y del bucle, no supuesto):
//   - quitar el `ORDER BY seq` de `sqlCabezaDeSesion` ⇒ la cabeza deja de ser la de menor seq y el orden de
//     salida pasa a depender del plan del motor: FALLA en la aserción de orden;
//   - cambiar el `estado <> ?` de `sqlCabezaDeSesion` por `estado = 'clasificado'` ⇒ la fila 1, que está
//     `tomado`, se vuelve INVISIBLE y la 2 sale la primera: FALLA en la aserción de «cero entregas» del
//     primer tramo;
//   - en `Despachador.vuelta`, sustituir el `correrPresupuesto` de la rama no-clasificada por un
//     `return false` que saltase a la fila siguiente ⇒ mismo fallo.
func TestCircuitoFIFOLaFilaListaNoAdelantaALaQueEsperaAlWorkerLento(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	reloj := nuevoO3Reloj()
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 24, WithClock(reloj.ahora))

	// Fila 1 (seq 1): texto normal ⇒ nace `nuevo`, reclamable por el cajero.
	if err := s.Enqueue(ctx, o3ItemConMeta("A", "chat-1@s", "wa-1", "quiero dos empanadas")); err != nil {
		t.Fatalf("Enqueue de la fila lenta: %v", err)
	}
	// Fila 2 (seq 2, POSTERIOR): el fastlane la resolvió al nacer ⇒ ya `clasificado`, lista para salir.
	// Es el camino literal del listener (ver enqueueCola: Estado + SobreOmitido).
	rapida := o3ItemConMeta("A", "chat-2@s", "wa-2", "2")
	rapida.Estado = app.EstadoClasificado
	rapida.IntentJSON = app.SobreOmitido(app.MotivoFastlane)
	if err := s.Enqueue(ctx, rapida); err != nil {
		t.Fatalf("Enqueue de la fila rápida: %v", err)
	}

	// EL WORKER LENTO: reclama la fila 1 y NO la cierra. `Reclamar` elige la conversación con el `nuevo` de
	// menor seq, que es chat-1; la fila 2 no es candidata porque ya está `clasificado`.
	lote, err := s.Reclamar(ctx, 0)
	if err != nil || lote == nil || len(lote.Mensajes) != 1 {
		t.Fatalf("Reclamar (worker lento): lote=%s err=%v", resumenLote(lote), err)
	}
	if lote.Mensajes[0].WAMessageID != "wa-1" {
		t.Fatalf("el worker debía llevarse wa-1, se llevó %s", resumenLote(lote))
	}

	// Presupuesto de un minuto con el reloj PARADO: en este test el presupuesto no puede entrar en juego, y
	// eso es a propósito — lo único que puede retener a la fila 2 es el orden.
	a := arrancarDespachador(t, s, "A", reloj.ahora, time.Minute)

	// Vuelta 1 (la que hace el bucle nada más arrancar) + tres más: ni una entrega, ni un sello.
	a.esperarParada(t)
	for i := 0; i < 3; i++ {
		a.vuelta(t)
	}
	if n := a.sink.cuantos(); n != 0 {
		t.Fatalf("SE ROMPIÓ EL FIFO: salieron %d mensajes (%v) con la cabeza aún en `tomado`. "+
			"La fila 2 estaba lista, pero la 1 la precede por seq", n, a.sink.ids())
	}
	if got := estadoDe(t, db, lote.Mensajes[0].ID); got != app.EstadoTomado {
		t.Fatalf("la cabeza cambió de estado sola: %q (el worker sigue con ella)", got)
	}

	// EL WORKER TERMINA. A partir de aquí salen las dos, y en orden.
	const intentReal = `{"intent":"crear_pedido","params":{"producto":"empanada","cantidad":"2"},"confidence":0.93,"config_version":"v7"}`
	if err := s.MarcarClasificado(ctx, lote, intentReal); err != nil {
		t.Fatalf("MarcarClasificado (el worker cierra): %v", err)
	}
	a.despertar(t)

	primera := a.esperarEntrega(t)
	segunda := a.esperarEntrega(t)
	if primera.MessageID != "wa-1" || segunda.MessageID != "wa-2" {
		t.Fatalf("ORDEN ROTO: salieron %q y luego %q (esperado wa-1, wa-2)", primera.MessageID, segunda.MessageID)
	}
	// La fila 1 sale CON la intención que el worker le puso: el viaje entero por disco (JSON → columna →
	// lectura → app.LeerSobreClasificado → domain.ClassifiedIntent) sin perder una clave.
	if primera.Intent == nil {
		t.Fatal("la fila 1 salió SIN intención pese a que el worker la clasificó: el sobre se perdió en el viaje por disco")
	}
	if primera.Intent.Name != "crear_pedido" || primera.Intent.Confidence != 0.93 || primera.Intent.ConfigVersion != "v7" {
		t.Fatalf("intención mal reconstruida: name=%q confidence=%v config_version=%q",
			primera.Intent.Name, primera.Intent.Confidence, primera.Intent.ConfigVersion)
	}
	if primera.Intent.Params["producto"] != "empanada" || primera.Intent.Params["cantidad"] != "2" {
		t.Fatalf("params mal reconstruidos: %d claves", len(primera.Intent.Params))
	}
	// Y el `meta_enc` sobrevivió al sellado con la DEK, al disco y al Decode.
	if primera.Sender != "593999123456@s.whatsapp.net" || primera.PushName != "Ana" ||
		primera.Type != "text" || primera.AddressingMode != "pn" || primera.SenderAlt != "111@lid" {
		t.Fatalf("meta mal reconstruida tras el viaje por disco (wa_id=%s): sender_vacio=%t push_vacio=%t type=%q mode=%q",
			primera.MessageID, primera.Sender == "", primera.PushName == "", primera.Type, primera.AddressingMode)
	}
	if primera.Timestamp.Unix() == 0 {
		t.Fatalf("la fila perdió su ts_whatsapp en el viaje (wa_id=%s)", primera.MessageID)
	}
	// El sobre de OMISIÓN de la fila 2 muere en el Edge: no viaja como intención (ADR-0038 §(e)).
	if segunda.Intent != nil {
		t.Fatalf("un sobre de omisión viajó como intención (wa_id=%s)", segunda.MessageID)
	}

	// Y las dos quedan selladas en disco: es lo que hace que la poda tenga algo que borrar.
	a.esperarParada(t)
	for _, waID := range []string{"wa-1", "wa-2"} {
		id := idDeWaID(t, db, waID)
		if got := estadoDe(t, db, id); got != app.EstadoDespachado {
			t.Fatalf("%s quedó en %q tras entregarse: sin el sello, el despachador la re-entregaría en cada poll", waID, got)
		}
		if sello := despachadoEnDe(t, db, id); !sello.Valid {
			t.Fatalf("%s se entregó sin sellar despachado_en: el TTL no podría podarla nunca", waID)
		}
	}
	if cab, err := s.CabezaDeSesion(ctx, "A"); err != nil || cab != nil {
		t.Fatalf("la sesión debía quedar al día, got cabeza=%s err=%v", resumenCabeza(cab), err)
	}
}

// idDeWaID resuelve la clave primaria de una fila por su wa_message_id (los tests siembran por Enqueue, que
// no devuelve el id).
func idDeWaID(t *testing.T, db *sql.DB, waID string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`SELECT id FROM cola_entrantes WHERE wa_message_id = ?`, waID).Scan(&id); err != nil {
		t.Fatalf("resolver id de %s: %v", waID, err)
	}
	return id
}

// ─────────────────────────────────────────────────────────────────────────────
// (b) Presupuesto — vence, se entrega sin intent, se cuenta y la fila queda sellada
// ─────────────────────────────────────────────────────────────────────────────

// TestCircuitoPresupuestoVencidoEntregaSinIntentYDejaLaFilaSellada cierra el bloque (b) ENTERO en un solo
// test, que es justamente lo que faltaba: hasta ahora la mitad del hecho vivía en el test del bucle (se
// entrega sin intent, se cuenta) y la otra mitad en el del SQL (la fila queda con su sobre y su sello), sin
// que nada comprobase que las dos mitades son la misma historia.
//
// Las CUATRO afirmaciones del criterio, sobre el mismo mensaje:
//  1. el presupuesto VENCE (el reloj cruza el umbral y no antes: el borde es un `<`);
//  2. el mensaje se ENTREGA, sin intención;
//  3. se CUENTA — y en los dos contadores, que NO miden lo mismo: `PresupuestosVencidos` cuenta el DISPARO
//     y `OmitidosPorMotivo[presupuesto]` cuenta la ENTREGA resultante (ver el comentario de `entregar`);
//  4. la fila queda en disco con el LITERAL `{"omitido":"presupuesto"}` y `estado='despachado'`.
//
// Y de propina el bloque (e): con el sello puesto, +25 h y un Enqueue cualquiera, la poda por fin se la
// lleva. Ese tramo importa porque comprueba que la fila que llegó al sello POR EL CAMINO DEL PRESUPUESTO
// —dos sentencias: primero el sobre, luego el sello tras la entrega— es exactamente igual de podable que
// la que llegó por el camino del cajero.
//
// 🔴🔴 ESTE TEST NACIÓ EN ROJO, Y ESA FUE SU RAZÓN DE SER (hallazgo de T3.5, 2026-08-17). El circuito real
// PERDÍA el mensaje cuando vencía el presupuesto, y ninguna de las dos mitades lo veía por separado:
//
//   - PRODUCCIÓN: `sqlDespacharSinIntent` dejaba la fila en `estado='despachado'`, y `sqlCabezaDeSesion`
//     filtra con `estado <> 'despachado'`. La RELECTURA que `correrPresupuesto` hace tras sellar (su
//     `return true`) ya no encontraba la fila, y `entregar` —la única puerta al sink, en la rama
//     `EstadoClasificado` de `vuelta`— no se ejecutaba jamás. La fila quedaba sellada en disco, el TTL se
//     la llevaba a las 24 h y el mensaje NUNCA llegaba a la nube. Exactamente lo contrario de REQ-051.19.
//   - EL TEST DEL BUCLE no lo veía porque su doble (`colaFake.DespacharSinIntent`, despachador_test.go)
//     dejaba la fila en `clasificado`: el fake modelaba la INTENCIÓN del bucle, no la sentencia del store.
//   - EL TEST DEL SQL no lo veía porque no hay bucle: comprueba que la sentencia escribe lo que dice.
//
// EL ARREGLO ELEGIDO (decisión de Jhoan, 2026-08-17): `DespacharSinIntent` deja la fila `clasificado` con
// su sobre y suelta el claim, y la entrega + el sello los hace el camino NORMAL. Es la opción crash-safe
// —si el proceso muere entre el sobre y la entrega, la fila sigue pendiente y se reintenta— y la que
// respeta «entrega antes de sello». Las aserciones de abajo no cambiaron con el arreglo: siguen exigiendo
// que la fila acabe `despachado`, con su sobre y su sello en epoch-segundos. Lo que cambió es CUÁNDO:
// tras la entrega, no en el instante del vencimiento.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - volver a poner `estado = 'despachado'` (o añadir `despachado_en = ?`) en `sqlDespacharSinIntent` ⇒
//     el bug original: FALLA en «el mensaje se perdió» con 0 entregas al sink;
//   - cambiar el `<` de `correrPresupuesto` por `<=` (o mover el umbral) ⇒ el tramo de 3999 ms resuelve
//     antes de tiempo: FALLA en «venció antes de tiempo»;
//   - quitar `intent_json = ?` del SET de `sqlDespacharSinIntent` ⇒ la fila queda `clasificado` con NULL,
//     que en disco es la forma de un FRAGMENTO de lote: FALLA en la aserción del literal y en
//     `fragmentos_de_lote = 0`;
//   - quitar `tomado_en = NULL` / `claim_token = NULL` del mismo SET ⇒ FALLA en las dos aserciones del
//     claim (y, peor, dejaría vivo el fence contra el cierre tardío del cajero);
//   - quitar `despachado_en = ?` de `sqlMarcarDespachada`, o escribirlo con `UnixMilli()` en vez de
//     `Unix()` ⇒ FALLA en la aserción de epoch-segundos Y en el tramo de la poda (con milis el corte no
//     alcanza a la fila jamás);
//   - hacer que `evento` rellene `evt.Intent` para un sobre de omisión ⇒ FALLA en «llevó intención»;
//   - contar el disparo en el contador de omitidos (o al revés) ⇒ FALLA en uno de los dos contadores.
func TestCircuitoPresupuestoVencidoEntregaSinIntentYDejaLaFilaSellada(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	reloj := nuevoO3Reloj()
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 24, WithClock(reloj.ahora)) // TTL de 24 h, el de producción

	if err := s.Enqueue(ctx, o3ItemConMeta("A", "chat-1@s", "wa-lenta", "quiero dos empanadas")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// El cajero se la lleva y se queda «infiriendo» para siempre: es el caso para el que existe el
	// presupuesto (un Ollama atascado no puede detener la conversación de un cliente).
	lote, err := s.Reclamar(ctx, 0)
	if err != nil || lote == nil || len(lote.Mensajes) != 1 {
		t.Fatalf("Reclamar: lote=%s err=%v", resumenLote(lote), err)
	}
	id := lote.Mensajes[0].ID

	const presupuesto = 4 * time.Second // WAPP_AGENT_INTENT_WAIT_MS = 4000, el default de producción
	a := arrancarDespachador(t, s, "A", reloj.ahora, presupuesto)

	// Vuelta 1: se ve la cabeza por primera vez ⇒ arranca SU reloj (no el del encolado: ver cabezaEnCurso).
	a.esperarParada(t)
	if got := estadoDe(t, db, id); got != app.EstadoTomado {
		t.Fatalf("la primera vuelta ya selló la fila (estado %q): el presupuesto no había empezado a correr siquiera", got)
	}

	// Justo por debajo del umbral: NO vence. El borde importa — es un `<`, no un `<=`.
	reloj.avanzar(presupuesto - time.Millisecond)
	a.vuelta(t)
	if got := estadoDe(t, db, id); got != app.EstadoTomado {
		t.Fatalf("venció antes de tiempo: a %v del arranque la fila ya estaba en %q", presupuesto-time.Millisecond, got)
	}
	if n := a.sink.cuantos(); n != 0 {
		t.Fatalf("se entregó algo antes de que venciera el presupuesto: %v", a.sink.ids())
	}

	// Se cruza el umbral. El sobre de omisión muerde sobre `tomado`, que es donde está esta fila; la deja
	// `clasificado` y el bucle —sin pagar poll, porque hubo progreso— la relee, la entrega y la sella. Tres
	// vueltas encadenadas hasta que la sesión queda al día.
	//
	// EL RELOJ NO SE MUEVE ENTRE EL VENCIMIENTO Y EL SELLO, y por eso `vencimiento.Unix()` sirve abajo para
	// afirmar `despachado_en`: no es que el sello sea simultáneo al vencimiento (no lo es, ocurre dos
	// vueltas después), es que este reloj es manual y el test no lo adelanta en medio.
	reloj.avanzar(2 * time.Millisecond)
	vencimiento := reloj.ahora()
	a.despertar(t)
	// Se espera a que el bucle APARQUE (cola al día) antes de mirar el sink: así el «no se entregó» de
	// abajo es un hecho, no una carrera. Y falla con un mensaje que explica QUÉ pasó, en vez de colgarse
	// tres segundos esperando una entrega que no va a llegar.
	a.esperarParada(t)

	if n := a.sink.cuantos(); n != 1 {
		t.Fatalf("🔴 EL MENSAJE SE PERDIÓ AL VENCER EL PRESUPUESTO: hubo %d entregas al sink (se esperaba 1). "+
			"REQ-051.19 promete que un cajero atascado RETRASA el mensaje, nunca lo pierde. "+
			"El camino correcto es: `DespacharSinIntent` deja la fila en estado='clasificado' con su sobre "+
			"(sqlDespacharSinIntent), `sqlCabezaDeSesion` SÍ la ve (solo excluye 'despachado'), la relectura "+
			"de `correrPresupuesto` la reencuentra y `entregar` —única puerta al sink, rama EstadoClasificado "+
			"de `vuelta`— la manda y solo entonces la sella. Si aquí hay 0, revisa si esa sentencia ha vuelto "+
			"a sellar 'despachado' (el bug del 2026-08-17). Entregas registradas: %v", n, a.sink.ids())
	}

	evt := a.esperarEntrega(t)
	if evt.MessageID != "wa-lenta" {
		t.Fatalf("se entregó otra fila: %q", evt.MessageID)
	}
	if evt.Intent != nil {
		t.Fatalf("un despacho por presupuesto llevó intención (wa_id=%s): el cajero nunca la calculó", evt.MessageID)
	}
	// El mensaje sale ÍNTEGRO: lo que se pierde es la clasificación, jamás el mensaje.
	if evt.Text != "quiero dos empanadas" || evt.PushName != "Ana" {
		t.Fatalf("el mensaje salió mutilado (wa_id=%s): texto_vacio=%t push_vacio=%t",
			evt.MessageID, evt.Text == "", evt.PushName == "")
	}

	// Los contadores ya están escritos: el `esperarParada` de arriba es la barrera (el bucle no aparca hasta
	// haber terminado la vuelta, y los contadores se incrementan DESPUÉS de que Deliver retorne). No se
	// vuelve a esperar una parada aquí porque el bucle solo aparca UNA vez en este tramo: sella (progreso),
	// entrega (progreso) y solo entonces se encuentra la sesión al día.
	if got := a.d.PresupuestosVencidos(); got != 1 {
		t.Fatalf("presupuestos_vencidos = %d, se esperaba 1 (cuenta el DISPARO del sello)", got)
	}
	if got := a.d.OmitidosPorMotivo()[app.MotivoPresupuesto]; got != 1 {
		t.Fatalf("omitido_presupuesto = %d, se esperaba 1 (cuenta la ENTREGA sin intención)", got)
	}
	if got := a.d.ConIntent(); got != 0 {
		t.Fatalf("con_intent = %d, se esperaba 0: no hubo ninguna intención que entregar", got)
	}
	if got := a.d.FragmentosDeLote(); got != 0 {
		t.Fatalf("fragmentos_de_lote = %d: un despacho por presupuesto SÍ deja sobre, así que no es un fragmento", got)
	}
	// Este NO se movió: el sello sí aterrizó (la fila estaba `tomado`, no `clasificado`).
	if got := s.DespachosSinIntentNoAplicados(); got != 0 {
		t.Fatalf("DespachosSinIntentNoAplicados = %d, se esperaba 0: el sello aterrizó", got)
	}

	// ─── LA FILA EN DISCO ───
	if got := estadoDe(t, db, id); got != app.EstadoDespachado {
		t.Fatalf("estado en disco = %q, want %q", got, app.EstadoDespachado)
	}
	// 🔴 LITERAL A PROPÓSITO, no app.SobreOmitido(app.MotivoPresupuesto): este test fija el FORMATO que
	// queda escrito en la columna. Afirmarlo contra la misma función que lo produce no probaría nada — un
	// cambio de formato arrastraría el test consigo y pasaría verde (mismo criterio que los tests del
	// listener con `{"omitido":"sin_texto"}`).
	if got := intentDe(t, db, id); !got.Valid || got.String != `{"omitido":"presupuesto"}` {
		t.Fatalf("intent_json en disco = %+v, want el literal {\"omitido\":\"presupuesto\"}", got)
	}
	sello := despachadoEnDe(t, db, id)
	if !sello.Valid {
		t.Fatal("despachado_en quedó NULL: la poda exige el sello ADEMÁS del estado, así que esta fila no se podaría jamás")
	}
	if sello.Int64 != vencimiento.Unix() {
		t.Fatalf("despachado_en = %d, want %d (EPOCH-SEGUNDOS, la unidad que compara pruneTTLLocked; "+
			"en milis serían %d y el corte no la alcanzaría nunca)", sello.Int64, vencimiento.Unix(), vencimiento.UnixMilli())
	}
	// Una fila despachada no puede seguir diciendo «me tiene el cajero X».
	if got := tomadoEnDe(t, db, id); got.Valid {
		t.Fatalf("tomado_en quedó en %d sobre una fila ya despachada", got.Int64)
	}
	if got := claimTokenDe(t, db, id); got.Valid {
		t.Fatalf("claim_token sobrevivió al despacho (%q): la fila ya no pertenece a ningún claim", got.String)
	}

	// ─── (e) EL TTL, SOBRE UNA FILA QUE LLEGÓ AL SELLO POR EL CAMINO DEL PRESUPUESTO ───
	// `pruneTTLLocked` corre en cada Enqueue; hasta la Ola 3 no podía borrar nada porque nadie escribía
	// `estado='despachado'` NI `despachado_en`. Lo que este tramo añade sobre el test del TTL de
	// despacho_test.go es la PROCEDENCIA de la fila: no llegó al sello por el camino del cajero
	// (Reclamar → MarcarClasificado → entrega → MarcarDespachada) sino por el del presupuesto vencido
	// (DespacharSinIntent → entrega → MarcarDespachada). El sello es el mismo y la poda no distingue,
	// que es justamente lo que hay que demostrar.
	reloj.avanzar(25 * time.Hour)
	if err := s.Enqueue(ctx, item("A", "chat-9@s", "wa-disparador", "hola")); err != nil {
		t.Fatalf("Enqueue disparador de la poda: %v", err)
	}
	if existe(t, db, "wa-lenta") {
		t.Fatal("EL TTL SIGUE INERTE para el camino del presupuesto: la fila que se resolvió por " +
			"DespacharSinIntent y se selló al entregarse sobrevivió a las 25 h. Revisa que MarcarDespachada " +
			"escriba estado='despachado' Y despachado_en en EPOCH-SEGUNDOS, y que la entrega llegue a ocurrir")
	}
	if !existe(t, db, "wa-disparador") {
		t.Fatal("la poda se llevó una fila JAMÁS despachada (REQ-051.7): eso es perder un mensaje")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (g) Cierre limpio — tres sesiones concurrentes, para `-race`
// ─────────────────────────────────────────────────────────────────────────────

// TestCircuitoCierreLimpioTresSesionesConcurrentes es el test del bloque (g), y el único de este fichero que
// corre con el despertador de PRODUCCIÓN (`PollFijo`) en vez del manual.
//
// 🔴 POR QUÉ AQUÍ SÍ, Y POR QUÉ ESO NO ES «SINCRONÍA POR RELOJ DE PARED». Lo que este test ejercita es
// justamente lo que un despertador manual APAGA: tres bucles reales girando a la vez sobre el MISMO *Store
// y el MISMO fichero SQLite, que es donde `-race` tiene algo que decir y donde vive el riesgo real (tres
// goroutines leyendo la misma tabla, sellando filas y compartiendo el caché de sobres del store). El poll
// es el MECANISMO del código bajo prueba, no la sincronía del test: el test no duerme ni supone plazos —
// espera por CANAL a que lleguen las N entregas y hace `Wait` sobre el retorno de los tres `Run`. Las
// aserciones no dependen de cuánto tarde nada.
//
// Las tres cosas que se afirman:
//   - AISLAMIENTO POR SESIÓN: cada despachador entrega SOLO lo suyo. Es la mitad silenciosa del contrato —
//     una fuga cruzada no rompe nada visible: manda los mensajes de un cliente por el stream de otro.
//   - FIFO BAJO CONCURRENCIA: dentro de cada sesión, el orden de entrega es el de `seq`, con tres bucles
//     compitiendo por el mismo motor.
//   - CIERRE LIMPIO: al cancelar, los tres `Run` retornan nil y NINGUNA entrega ocurre después. La prueba
//     de «después» no es un plazo: es el JOIN. Cuando los tres `Run` han retornado, no queda goroutine que
//     pueda entregar; a partir de ahí el sink levanta su bandera y cualquier entrega quedaría contada.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - quitar el `session_id = ?` de `sqlCabezaDeSesion` ⇒ los tres bucles se pisan las filas: FALLA en el
//     aislamiento (y muy probablemente también en el conteo por sesión);
//   - quitar el `ORDER BY seq` ⇒ FALLA en el orden;
//   - hacer que `PollFijo.Esperar` use `time.Sleep` en vez del `select` sobre `ctx.Done()` ⇒ el apagado
//     tendría que esperar al tick pendiente; con un poll largo, FALLA por la guardia de retorno;
//   - quitar los `ctx.Err()` de `Run` ⇒ el bucle podría dar una vuelta más tras la cancelación: la bandera
//     `entregasTrasStop` lo caza.
func TestCircuitoCierreLimpioTresSesionesConcurrentes(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	reloj := nuevoO3Reloj()
	s := newStore(t, db, newFakeCrypterFor().fn, 1000, 24, WithClock(reloj.ahora))

	sesiones := []string{"A", "B", "C"}
	const porSesion = 12

	// Todo nace ya `clasificado` (el camino del fastlane): aquí no se prueba la clasificación, se prueba el
	// DRENADO concurrente. Así el test no depende de ningún worker y el trabajo está disponible desde el
	// primer poll.
	for _, sid := range sesiones {
		for i := 0; i < porSesion; i++ {
			it := o3ItemConMeta(sid, "chat@s", fmt.Sprintf("%s-wa-%02d", sid, i), "hola")
			it.Estado = app.EstadoClasificado
			it.IntentJSON = app.SobreOmitido(app.MotivoFastlane)
			if err := s.Enqueue(ctx, it); err != nil {
				t.Fatalf("Enqueue %s/%d: %v", sid, i, err)
			}
		}
	}

	type corriendo struct {
		sink    *o3Sink
		cancel  context.CancelFunc
		retorno chan error
	}
	bucles := make(map[string]*corriendo, len(sesiones))
	for _, sid := range sesiones {
		sink := nuevoO3Sink()
		d, err := despachador.New(despachador.Deps{
			Cola:      s,
			Sink:      sink,
			SessionID: sid,
			Log:       testLogger(),
			Ahora:     reloj.ahora,
			// Presupuesto amplio: no hay nada que esperar (todo nace clasificado), y si algo lo hubiera,
			// que fuera el orden y no el reloj quien lo explicara.
			Presupuesto: time.Minute,
			// El despertador de PRODUCCIÓN, corto: ver la cabecera de este test.
			Despertador: despachador.NewPollFijo(time.Millisecond),
		})
		if err != nil {
			t.Fatalf("despachador.New(%s): %v", sid, err)
		}
		cctx, cancel := context.WithCancel(ctx)
		ret := make(chan error, 1)
		go func() { ret <- d.Run(cctx) }()
		bucles[sid] = &corriendo{sink: sink, cancel: cancel, retorno: ret}
	}
	// Red por si una aserción aborta el test a media faena: nada de goroutines huérfanas.
	t.Cleanup(func() {
		for _, b := range bucles {
			b.cancel()
		}
	})

	// Se espera POR CANAL a que cada sesión haya entregado lo suyo. Ni un sleep.
	for _, sid := range sesiones {
		b := bucles[sid]
		for i := 0; i < porSesion; i++ {
			select {
			case <-b.sink.entregas:
			case <-time.After(10 * time.Second): // guardia anti-cuelgue, no sincronía
				t.Fatalf("sesión %s: solo llegaron %d de %d entregas (%v)", sid, b.sink.cuantos(), porSesion, b.sink.ids())
			}
		}
	}

	// CIERRE. Se cancelan los tres y se hace el JOIN: cuando los tres Run han retornado, no queda nadie que
	// pueda entregar. Eso —y no un plazo— es lo que hace decidible el «ninguna entrega después».
	for _, sid := range sesiones {
		bucles[sid].cancel()
	}
	for _, sid := range sesiones {
		select {
		case err := <-bucles[sid].retorno:
			if err != nil {
				t.Fatalf("sesión %s: Run devolvió error en una parada ordenada: %v", sid, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("sesión %s: Run no retornó tras cancelar el contexto", sid)
		}
	}
	for _, sid := range sesiones {
		bucles[sid].sink.cerrado.Store(true)
	}

	// AISLAMIENTO + FIFO, sesión a sesión.
	for _, sid := range sesiones {
		ids := bucles[sid].sink.ids()
		if len(ids) != porSesion {
			t.Fatalf("sesión %s entregó %d mensajes, se esperaban %d: %v", sid, len(ids), porSesion, ids)
		}
		for i, got := range ids {
			want := fmt.Sprintf("%s-wa-%02d", sid, i)
			if got != want {
				t.Fatalf("sesión %s: entrega #%d fue %q, se esperaba %q. "+
					"Si el prefijo no casa es una FUGA ENTRE SESIONES (mensajes de un cliente por el stream de otro); "+
					"si casa pero el número no, se rompió el FIFO por seq. Orden completo: %v", sid, i, got, want, ids)
			}
		}
	}

	// Todas las filas quedan selladas: 36 entregas, 36 sellos.
	if n := contarEnEstado(t, db, app.EstadoDespachado); n != len(sesiones)*porSesion {
		t.Fatalf("filas selladas = %d, se esperaban %d: sin sello, el despachador las re-entregaría",
			n, len(sesiones)*porSesion)
	}

	// Trabajo NUEVO después del cierre. Con los bucles ya unidos nadie puede recogerlo; si alguno siguiera
	// vivo (un `Run` que retornó sin que su goroutine muriera, o un `Esperar` que ignora el ctx), la bandera
	// del sink lo delataría.
	for _, sid := range sesiones {
		it := o3ItemConMeta(sid, "chat@s", sid+"-wa-tarde", "esto llega después del cierre")
		it.Estado = app.EstadoClasificado
		it.IntentJSON = app.SobreOmitido(app.MotivoFastlane)
		if err := s.Enqueue(ctx, it); err != nil {
			t.Fatalf("Enqueue tardío %s: %v", sid, err)
		}
	}
	for _, sid := range sesiones {
		b := bucles[sid]
		if got := b.sink.entregasTrasStop.Load(); got != 0 {
			t.Fatalf("sesión %s: %d entregas DESPUÉS de la parada ordenada", sid, got)
		}
		if got := b.sink.cuantos(); got != porSesion {
			t.Fatalf("sesión %s: el sink pasó de %d a %d entregas tras el cierre", sid, porSesion, got)
		}
	}
	// Y las filas tardías siguen intactas en disco: se retrasan hasta el próximo arranque, no se pierden.
	for _, sid := range sesiones {
		if !existe(t, db, sid+"-wa-tarde") {
			t.Fatalf("sesión %s: la fila encolada tras el cierre desapareció", sid)
		}
	}
}
