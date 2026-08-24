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
//     quien escribe `despachado_en` y quien corta la poda) y `Deps.Ahora` en el despachador (que desde
//     T1.6-5 sólo mide el espaciado de las re-entregas). Que sea el MISMO reloj es lo que permite escribir
//     un test que avanza 25 h y significa lo mismo para ambos.
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
func arrancarDespachador(t *testing.T, cola app.ColaDespachador, sessionID string, ahora func() time.Time) *o3Arnes {
	t.Helper()
	sink := nuevoO3Sink()
	desp := nuevoO3Despertador()
	d, err := despachador.New(despachador.Deps{
		Cola:        cola,
		Sink:        sink,
		SessionID:   sessionID,
		Log:         testLogger(),
		Ahora:       ahora,
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
// (a) FIFO Y CERO RETENCIÓN — las dos filas salen EN ORDEN y EN LA PRIMERA RÁFAGA
// ─────────────────────────────────────────────────────────────────────────────

// TestCircuitoDosFilasSalenEnOrdenYSinRetencion cubre a la vez la invariante que compra la Ola 3
// (REQ-051.18: la fila N+1 no sale antes que la N) y el cambio que trae T1.6-5 (ADR-0045, REQ-35: no se
// retiene NADA), ejercitados de punta a punta contra la BD real.
//
// 🔴 ESTE TEST AFIRMABA ALGO CASI OPUESTO HASTA EL 2026-08-24. Se llamaba
// `…LaFilaListaNoAdelantaALaQueEsperaAlWorkerLento` y montaba este escenario: la fila 1 la reclamaba un
// worker lento y se quedaba `tomado` «infiriendo»; la fila 2, posterior, ya nacía `clasificado` porque el
// fastlane la había resuelto en µs. La afirmación era que la 2 **esperaba detrás** de la 1 —tres vueltas
// del bucle sin una sola entrega— hasta que el worker cerrara con `MarcarClasificado`.
//
// Bajo pull no hay a quién esperar, así que la mitad «espera» desapareció y la mitad «orden» se quedó. El
// escenario se conserva casi entero A PROPÓSITO —la fila 1 SIGUE estando `tomado` cuando el bucle la mira—
// porque es el que demuestra la parte menos obvia del ADR-0045 §Decisión.4: el claim con fencing se
// conserva, pero NUNCA fue un derecho de retención sobre la entrega. Antes daba igual (la fila esperaba de
// todos modos); ahora es la diferencia entre entregar y no entregar.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (leídas del SQL y del bucle, no supuestas):
//   - quitar el `ORDER BY seq` de `sqlCabezaDeSesion` ⇒ la cabeza deja de ser la de menor seq y el orden de
//     salida pasa a depender del plan del motor: FALLA en la aserción de orden;
//   - devolver a `Despachador.vuelta` cualquier condición sobre `cabeza.Estado` antes de entregar ⇒ la
//     fila 1, que está `tomado`, no sale: FALLA en la primera entrega;
//   - devolver el fence `estado = 'clasificado'` a `sqlMarcarDespachada` ⇒ ninguna de las dos se sella,
//     las dos se re-entregan en cada poll y el TTL no puede podarlas: FALLA en las aserciones de disco.
func TestCircuitoDosFilasSalenEnOrdenYSinRetencion(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	reloj := nuevoO3Reloj()
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 24, WithClock(reloj.ahora))

	if err := s.Enqueue(ctx, o3ItemConMeta("A", "chat-1@s", "wa-1", "quiero dos empanadas")); err != nil {
		t.Fatalf("Enqueue de la fila 1: %v", err)
	}
	if err := s.Enqueue(ctx, o3ItemConMeta("A", "chat-2@s", "wa-2", "2")); err != nil {
		t.Fatalf("Enqueue de la fila 2: %v", err)
	}

	// EL CLAIM SOBRE LA CABEZA: el worker se lleva la fila 1 a `tomado` y no la cierra. No es una imitación
	// del cajero, es el cajero de verdad a medio trabajo. `Reclamar` elige la conversación con el `nuevo` de
	// menor seq, que es chat-1.
	lote, err := s.Reclamar(ctx, 0)
	if err != nil || lote == nil || len(lote.Mensajes) != 1 {
		t.Fatalf("Reclamar: lote=%s err=%v", resumenLote(lote), err)
	}
	if lote.Mensajes[0].WAMessageID != "wa-1" {
		t.Fatalf("el worker debía llevarse wa-1, se llevó %s", resumenLote(lote))
	}
	if got := estadoDe(t, db, lote.Mensajes[0].ID); got != app.EstadoTomado {
		t.Fatalf("la fila 1 no quedó `tomado` tras el claim: %q — el escenario no es el que el test cree", got)
	}

	// 🔴 EL RELOJ NO SE TOCA EN TODO EL TEST hasta el tramo del TTL: si hiciera falta avanzarlo para que
	// algo saliera, es que algo sigue reteniendo.
	a := arrancarDespachador(t, s, "A", reloj.ahora)

	primera := a.esperarEntrega(t)
	segunda := a.esperarEntrega(t)
	if primera.MessageID != "wa-1" || segunda.MessageID != "wa-2" {
		t.Fatalf("ORDEN ROTO: salieron %q y luego %q (esperado wa-1, wa-2)", primera.MessageID, segunda.MessageID)
	}
	// El `meta_enc` sobrevivió al sellado con la DEK, al disco y al Decode: es el contrato por claves JSON
	// que sigue vivo tras el retiro del sobre de intención.
	if primera.Sender != "593999123456@s.whatsapp.net" || primera.PushName != "Ana" ||
		primera.Type != "text" || primera.AddressingMode != "pn" || primera.SenderAlt != "111@lid" {
		t.Fatalf("meta mal reconstruida tras el viaje por disco (wa_id=%s): sender_vacio=%t push_vacio=%t type=%q mode=%q",
			primera.MessageID, primera.Sender == "", primera.PushName == "", primera.Type, primera.AddressingMode)
	}
	if primera.Text != "quiero dos empanadas" {
		t.Fatalf("el texto no sobrevivió al cifrado/descifrado (wa_id=%s)", primera.MessageID)
	}
	if primera.Timestamp.Unix() == 0 {
		t.Fatalf("la fila perdió su ts_whatsapp en el viaje (wa_id=%s)", primera.MessageID)
	}

	// Y las dos quedan selladas en disco: es lo que hace que la poda tenga algo que borrar. 🔴 Es la mitad
	// del cambio de fence de `MarcarDespachada`: con el fence viejo (`= 'clasificado'`) ninguna de estas dos
	// —`tomado` y `nuevo`— habría llegado nunca a `despachado`.
	a.esperarParada(t)
	for _, waID := range []string{"wa-1", "wa-2"} {
		id := idDeWaID(t, db, waID)
		if got := estadoDe(t, db, id); got != app.EstadoDespachado {
			t.Fatalf("%s quedó en %q tras entregarse: sin el sello, el despachador la re-entregaría en cada poll "+
				"PARA SIEMPRE y el TTL no podría podarla nunca", waID, got)
		}
		if sello := despachadoEnDe(t, db, id); !sello.Valid {
			t.Fatalf("%s se entregó sin sellar despachado_en: el TTL no podría podarla nunca", waID)
		}
	}
	if cab, err := s.CabezaDeSesion(ctx, "A"); err != nil || cab != nil {
		t.Fatalf("la sesión debía quedar al día, got cabeza=%s err=%v", resumenCabeza(cab), err)
	}

	// Y NO SALE NADA MÁS. Tres vueltas completas del bucle sobre una sesión al día: si aquí apareciera una
	// tercera entrega sería una RE-ENTREGA —la fila que no se selló bien volviendo a salir en cada poll—,
	// que es exactamente el modo de fallo que el cambio de fence de `MarcarDespachada` podía introducir y
	// que ningún contador delataría (el cable deduplica por `wa_message_id`, así que en campo se vería como
	// tráfico de más y nada más).
	for i := 0; i < 3; i++ {
		a.vuelta(t)
	}
	if got := a.sink.cuantos(); got != 2 {
		t.Fatalf("entregas = %d tras tres vueltas sobre una sesión al día, se esperaban 2: alguna fila se "+
			"está RE-ENTREGANDO, así que su sello no aterrizó (%v)", got, a.sink.ids())
	}
	if got := a.d.Despachados(); got != 2 {
		t.Fatalf("despachados = %d, se esperaban 2", got)
	}
	if got := a.d.IntentsDescartados(); got != 0 {
		t.Fatalf("intents_descartados = %d: ninguna de las dos filas traía sobre", got)
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
// (b) LA MIGRACIÓN — una fila ANTIGUA `clasificado` se drena, se sella y se poda
// ─────────────────────────────────────────────────────────────────────────────

// TestCircuitoFilaAntiguaClasificadaSeDrenaSeSellaYSePoda es el test que sustituye al del PRESUPUESTO, y
// es hoy el más importante de este fichero: cubre EL ÚNICO RIESGO REAL DE T1.6-5 en campo.
//
// 🔴 QUÉ CUBRÍA ANTES, Y POR QUÉ SE SUSTITUYE ENTERO. Se llamaba
// `…PresupuestoVencidoEntregaSinIntentYDejaLaFilaSellada` y cerraba el bloque (b) de la Ola 3: el reloj
// cruzaba los 4 s, `DespacharSinIntent` escribía `{"omitido":"presupuesto"}`, el bucle releía, entregaba
// sin intención y sellaba. Nació ROJO el 2026-08-17 y destapó un bug de PÉRDIDA DE MENSAJES que ninguna de
// las dos mitades veía por separado (el doble del bucle mentía sobre el estado destino de la sentencia).
// Todo ese mecanismo —variable, reloj, sentencia y motivo— se retiró el 2026-08-24 con el ADR-0045.
//
// 🔴 EL RIESGO QUE OCUPA SU SITIO. Cuando este binario llegue a un equipo de campo se encontrará una
// `cola_entrantes.db` YA ESCRITA por el binario anterior, con filas en `clasificado` y con sus sobres. Ese
// estado ya no existe en el ciclo. Si el despachador no las entregara —o si `MarcarDespachada` no pudiera
// sellarlas— esas filas se quedarían en disco para siempre: mensajes reales de clientes reales que nunca
// llegan a la nube, y una cola que sólo crece hasta chocar con su tope. Es exactamente la misma FAMILIA de
// fallo que el bug del 2026-08-17, por eso hereda su sitio.
//
// LAS TRES AFIRMACIONES, sobre la misma fila vieja:
//  1. se ENTREGA, con su contenido íntegro (el mensaje es lo que no se puede perder);
//  2. su clasificación se TIRA y se CUENTA en `intents_descartados` — la única pérdida que la migración
//     admite, y no ocurre en silencio;
//  3. queda `despachado` con su `despachado_en` en EPOCH-SEGUNDOS, así que la poda por TTL se la lleva.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - devolver el fence `estado = 'clasificado'` a `sqlMarcarDespachada` ⇒ ESTE test seguiría verde (la
//     fila SÍ está `clasificado`) y el de arriba se pondría rojo. Es a propósito que se necesiten los dos:
//     el fence tiene que valer para los dos mundos a la vez, y ningún test solo lo demuestra;
//   - hacer que `evento` cuente la clasificación descartada como `omitido` o como `ilegible` ⇒ FALLA la
//     aserción de la serie;
//   - quitar `despachado_en = ?` de `sqlMarcarDespachada`, o escribirlo con `UnixMilli()` en vez de
//     `Unix()` ⇒ FALLA en la aserción de epoch-segundos Y en el tramo de la poda (con milis el corte no
//     alcanza a la fila jamás).
func TestCircuitoFilaAntiguaClasificadaSeDrenaSeSellaYSePoda(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	reloj := nuevoO3Reloj()
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 24, WithClock(reloj.ahora)) // TTL de 24 h, el de producción

	// LA FILA VIEJA, escrita como la escribía el binario anterior: `clasificado` y con el sobre del cajero.
	// Se pasa por `Enqueue` —no por un INSERT a mano— para que el cifrado, el sellado con la DEK y el
	// esquema sean EXACTAMENTE los de producción (reglas T2.17/T2.18: ningún DDL ni escritura transcritos).
	vieja := o3ItemConMeta("A", "chat-1@s", "wa-vieja", "quiero dos empanadas")
	vieja.Estado = app.EstadoClasificado
	vieja.IntentJSON = `{"intent":"crear_pedido","params":{"producto":"empanada","cantidad":"2"},"confidence":0.93,"config_version":"v7"}`
	if err := s.Enqueue(ctx, vieja); err != nil {
		t.Fatalf("Enqueue de la fila vieja: %v", err)
	}
	id := idDeWaID(t, db, "wa-vieja")
	if got := estadoDe(t, db, id); got != app.EstadoClasificado {
		t.Fatalf("la fila no quedó `clasificado` en disco (%q): el escenario no es el que el test cree", got)
	}

	a := arrancarDespachador(t, s, "A", reloj.ahora)

	evt := a.esperarEntrega(t)
	if evt.MessageID != "wa-vieja" {
		t.Fatalf("se entregó otra fila: %q", evt.MessageID)
	}
	// (1) EL MENSAJE SALE ÍNTEGRO: lo que se pierde es la etiqueta, jamás el mensaje.
	if evt.Text != "quiero dos empanadas" || evt.PushName != "Ana" || evt.Sender == "" {
		t.Fatalf("el mensaje salió mutilado (wa_id=%s): texto_vacio=%t push_vacio=%t sender_vacio=%t",
			evt.MessageID, evt.Text == "", evt.PushName == "", evt.Sender == "")
	}

	a.esperarParada(t)
	// (2) LA CLASIFICACIÓN SE TIRA, Y SE CUENTA.
	if got := a.d.IntentsDescartados(); got != 1 {
		t.Fatalf("intents_descartados = %d, se esperaba 1: la clasificación de una fila vieja ya no tiene "+
			"por dónde viajar (el ADR-0045 retiró ClassifiedIntent del proto), pero perderla NO puede ser "+
			"silencioso — es la serie con la que se vigila que las colas viejas se vacían", got)
	}
	if got := a.d.Omitidos(); got != 0 {
		t.Fatalf("omitidos = %d: un sobre de CLASIFICACIÓN no es un sobre de omisión", got)
	}
	if got := a.d.SobresIlegibles(); got != 0 {
		t.Fatalf("sobres_ilegibles = %d: el sobre era perfectamente legible, sólo que ya no tiene destino", got)
	}

	// (3) LA FILA EN DISCO, y el sello que la hace podable.
	sellado := reloj.ahora()
	if got := estadoDe(t, db, id); got != app.EstadoDespachado {
		t.Fatalf("estado en disco = %q, want %q: una fila `clasificado` de un binario viejo TIENE que poder "+
			"sellarse, o se queda en el disco del cliente para siempre", got, app.EstadoDespachado)
	}
	sello := despachadoEnDe(t, db, id)
	if !sello.Valid {
		t.Fatal("despachado_en quedó NULL: la poda exige el sello ADEMÁS del estado, así que esta fila no se podaría jamás")
	}
	if sello.Int64 != sellado.Unix() {
		t.Fatalf("despachado_en = %d, want %d (EPOCH-SEGUNDOS, la unidad que compara pruneTTLLocked; "+
			"en milis serían %d y el corte no la alcanzaría nunca)", sello.Int64, sellado.Unix(), sellado.UnixMilli())
	}

	// ─── EL TTL, SOBRE UNA FILA HEREDADA ───
	// `pruneTTLLocked` corre en cada Enqueue y sólo borra `estado='despachado'` CON `despachado_en`. Lo que
	// este tramo añade es la PROCEDENCIA: una fila que nació bajo el modelo push y se selló bajo el pull es
	// igual de podable que cualquier otra. Si no lo fuera, la migración dejaría basura permanente.
	reloj.avanzar(25 * time.Hour)
	if err := s.Enqueue(ctx, item("A", "chat-9@s", "wa-disparador", "hola")); err != nil {
		t.Fatalf("Enqueue disparador de la poda: %v", err)
	}
	if existe(t, db, "wa-vieja") {
		t.Fatal("EL TTL NO ALCANZA A LAS FILAS HEREDADAS: la que venía `clasificado` de un binario anterior " +
			"sobrevivió a las 25 h. Revisa que MarcarDespachada escriba estado='despachado' Y despachado_en " +
			"en EPOCH-SEGUNDOS")
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

	// Todo nace `nuevo` y sin sobre, que es lo ÚNICO que produce el listener desde T1.6-5. Aquí no se prueba
	// ninguna clasificación: se prueba el DRENADO concurrente, y el trabajo está disponible desde el primer
	// poll sin depender de ningún worker.
	//
	// (Hasta el 2026-08-24 estas filas se sembraban ya `clasificado` con el sobre `{"omitido":"fastlane"}`,
	// porque ése era entonces el único camino que garantizaba «lista para salir sin esperar a nadie». Hoy lo
	// garantiza cualquier fila.)
	for _, sid := range sesiones {
		for i := 0; i < porSesion; i++ {
			it := o3ItemConMeta(sid, "chat@s", fmt.Sprintf("%s-wa-%02d", sid, i), "hola")
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
