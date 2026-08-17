package despachador

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// despachador_test.go — LOS TESTS MÍNIMOS DEL DESPACHADOR (Plan 051 Ola 3 · T3.3/T3.4).
//
// ⚠️ NO son los siete bloques de la ola: T3.5 tiene su propio dueño y cubre el circuito completo contra
// una BD real. Estos cuatro son los que el propio código necesita para no nacer a ciegas — la invariante
// FIFO, el presupuesto, la regla del sobre sobre los OCHO motivos y el cierre limpio.
//
// 🔴 DETERMINISMO POR CONSTRUCCIÓN, SIN UN SOLO time.Sleep COMO MECANISMO DE SINCRONÍA. Las dos costuras
// que lo permiten son las que el despachador expone a propósito: el RELOJ inyectable (el presupuesto se
// mide contra `relojFalso`, que sólo avanza cuando el test lo dice) y el DESPERTADOR (aquí es un canal
// que el test libera de uno en uno, así que «una vuelta del bucle» es un evento observable y no una
// apuesta sobre cuánto tarda algo). Los `time.After` que aparecen abajo NO son sincronía: son guardias
// anti-cuelgue para que un fallo se manifieste como un test rojo en 2 s y no como un CI colgado.

// ─────────────────────────────────────────────────────────────────────────────
// Dobles
// ─────────────────────────────────────────────────────────────────────────────

// colaFake modela la cola como lo que es: una LISTA ORDENADA POR seq de la que la cabeza es la primera
// fila no despachada. Modelarla así —y no como «una cabeza suelta»— es lo que permite que el test del
// FIFO signifique algo: la fila 2 existe, está lista, y aun así no puede salir.
type colaFake struct {
	mu    sync.Mutex
	filas []*app.ColaCabeza

	errCabeza error

	marcadas  []int64
	sinIntent []selloOmision

	// lecturas señala CADA CabezaDeSesion. Buffer generoso + envío no bloqueante: la señal es para que el
	// test pueda esperar a que una vuelta haya ocurrido, nunca para frenar al bucle.
	lecturas chan struct{}
}

type selloOmision struct {
	id     int64
	motivo app.MotivoOmitido
}

func nuevaColaFake(filas ...*app.ColaCabeza) *colaFake {
	return &colaFake{filas: filas, lecturas: make(chan struct{}, 256)}
}

func (c *colaFake) CabezaDeSesion(_ context.Context, _ string) (*app.ColaCabeza, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case c.lecturas <- struct{}{}:
	default:
	}
	if c.errCabeza != nil {
		return nil, c.errCabeza
	}
	for _, f := range c.filas {
		if f.Estado != app.EstadoDespachado {
			// COPIA: el despachador no debe poder mutar la fila «en disco», y así una mutación accidental
			// del bucle se vería como un test rojo en vez de como un fake que se adapta al código.
			cp := *f
			return &cp, nil
		}
	}
	return nil, nil
}

// MarcarDespachada aplica el mismo fence que el SQL real: sólo sella lo que sigue `clasificado`, y 0 filas
// afectadas NO es error.
func (c *colaFake) MarcarDespachada(_ context.Context, id int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.marcadas = append(c.marcadas, id)
	for _, f := range c.filas {
		if f.ID == id && f.Estado == app.EstadoClasificado {
			f.Estado = app.EstadoDespachado
		}
	}
	return nil
}

// DespacharSinIntent reproduce, CLÁUSULA A CLÁUSULA, la sentencia única del adaptador
// (`sqlDespacharSinIntent`, adapters/colaentrantes/despacho.go):
//
//	UPDATE cola_entrantes
//	SET intent_json = <sobre>, estado = 'clasificado', tomado_en = NULL, claim_token = NULL
//	WHERE id = ? AND estado IN ('nuevo','tomado')
//
// Es decir: escribe el sobre de omisión y RESUELVE la fila dejándola `clasificado`, listo para que el
// camino normal la entregue y la selle. NO escribe `despachado` ni `despachado_en`. Si el cajero llegó
// antes (la fila ya está `clasificado`) es no-op silencioso — la carrera que el bucle tiene que releer.
//
// 🔴 ESTE DOBLE MINTIÓ, Y SU MENTIRA ESCONDIÓ UN BUG DE PÉRDIDA DE MENSAJES (T3.5, 2026-08-17). Decía
// `EstadoClasificado` cuando la sentencia real escribía `EstadoDespachado`: modelaba lo que el BUCLE
// necesitaba para poder releer la fila, no lo que el COLABORADOR hacía. Con el fake así, este paquete
// pasaba en verde mientras el circuito real perdía el mensaje (`sqlCabezaDeSesion` filtra
// `estado <> 'despachado'`, la relectura de `correrPresupuesto` no volvía a ver la fila y `entregar` —la
// única puerta al sink— no se ejecutaba nunca). El arreglo NO fue tocar el fake para que reflejara el bug
// y luego enseñarle a mentir de otra forma: fue cambiar la SENTENCIA REAL para que dejara la fila
// `clasificado`, que es lo crash-safe, y realinear este doble con ella.
//
// LA REGLA QUE DEJA ESCRITA ESTE COMENTARIO: un doble que modela lo que el llamante NECESITA, en vez de
// lo que el colaborador HACE, no es un doble — es una segunda implementación que se adapta al código bajo
// prueba, y su forma de fallar es un test verde sobre producción rota.
//
// ⚠️ LO QUE ESTE DOBLE NO MODELA, a sabiendas: `tomado_en` y `claim_token` no existen en `app.ColaCabeza`
// (el despachador nunca los mira), así que su puesta a NULL no se puede reproducir aquí. Esa mitad de la
// sentencia —que es la que sostiene el fence contra el cierre tardío del cajero— se prueba contra la BD
// real, en adapters/colaentrantes/despacho_test.go. Aquí no hay nada que afirmar sobre ella.
func (c *colaFake) DespacharSinIntent(_ context.Context, id int64, motivo app.MotivoOmitido) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sinIntent = append(c.sinIntent, selloOmision{id: id, motivo: motivo})
	for _, f := range c.filas {
		if f.ID != id {
			continue
		}
		if f.Estado == app.EstadoNuevo || f.Estado == app.EstadoTomado {
			f.Estado = app.EstadoClasificado
			f.IntentJSON = app.SobreOmitido(motivo)
			f.TieneIntent = f.IntentJSON != ""
		}
	}
	return nil
}

// clasificar simula al CAJERO cerrando una fila: la deja `clasificado` con su sobre.
func (c *colaFake) clasificar(id int64, intentJSON string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range c.filas {
		if f.ID == id {
			f.Estado = app.EstadoClasificado
			f.IntentJSON = intentJSON
			f.TieneIntent = intentJSON != ""
		}
	}
}

func (c *colaFake) sellosSinIntent() []selloOmision {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]selloOmision(nil), c.sinIntent...)
}

var _ app.ColaDespachador = (*colaFake)(nil)

// sinkFake registra las entregas en ORDEN y las publica por un canal para que el test pueda esperarlas.
type sinkFake struct {
	mu         sync.Mutex
	entregados []domain.InboundEvent
	err        error
	entregas   chan domain.InboundEvent
}

func nuevoSinkFake() *sinkFake {
	return &sinkFake{entregas: make(chan domain.InboundEvent, 256)}
}

func (s *sinkFake) Deliver(_ context.Context, evt domain.InboundEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.entregados = append(s.entregados, evt)
	select {
	case s.entregas <- evt:
	default:
	}
	return nil
}

func (s *sinkFake) cuantos() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entregados)
}

var _ app.InboundSink = (*sinkFake)(nil)

// despertadorManual convierte las DOS mitades de «el bucle espera» en eventos que el test observa:
// `esperando` avisa de que el bucle acaba de APARCAR (la vuelta anterior terminó del todo) y `c` lo
// libera. Tener las dos por separado es lo que permite comprobar el cierre limpio sin adivinar: se espera
// a que aparque, se cambia el mundo bajo sus pies y se cancela sin haberlo soltado nunca.
type despertadorManual struct {
	c         chan struct{}
	esperando chan struct{}
}

func nuevoDespertadorManual() *despertadorManual {
	return &despertadorManual{c: make(chan struct{}), esperando: make(chan struct{}, 256)}
}

func (d *despertadorManual) Esperar(ctx context.Context) error {
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

var _ Despertador = (*despertadorManual)(nil)

// relojFalso es el reloj inyectado: sólo avanza cuando el test lo adelanta.
type relojFalso struct {
	mu sync.Mutex
	t  time.Time
}

func nuevoReloj() *relojFalso {
	return &relojFalso{t: time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)}
}

func (r *relojFalso) ahora() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.t
}

func (r *relojFalso) avanzar(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.t = r.t.Add(d)
}

// ─────────────────────────────────────────────────────────────────────────────
// Arnés
// ─────────────────────────────────────────────────────────────────────────────

const guardia = 2 * time.Second // guardia anti-cuelgue, NO sincronía

type arnes struct {
	d       *Despachador
	cola    *colaFake
	sink    *sinkFake
	reloj   *relojFalso
	desp    *despertadorManual
	cancel  context.CancelFunc
	retorno chan error
	// retornado evita que el Cleanup vuelva a esperar el retorno de Run cuando el test ya lo consumió
	// (si no, ese test pagaría la guardia entera de 2 s por nada).
	retornado atomic.Bool
}

func arrancar(t *testing.T, cola *colaFake, presupuesto time.Duration) *arnes {
	t.Helper()
	sink := nuevoSinkFake()
	reloj := nuevoReloj()
	desp := nuevoDespertadorManual()

	d, err := New(Deps{
		Cola:        cola,
		Sink:        sink,
		SessionID:   "sesion-1",
		Ahora:       reloj.ahora,
		Presupuesto: presupuesto,
		Despertador: desp,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := &arnes{d: d, cola: cola, sink: sink, reloj: reloj, desp: desp, cancel: cancel, retorno: make(chan error, 1)}
	go func() { a.retorno <- d.Run(ctx) }()
	t.Cleanup(a.parar)
	return a
}

// esperarLectura bloquea hasta que el bucle haya consultado la cabeza una vez más.
func (a *arnes) esperarLectura(t *testing.T) {
	t.Helper()
	select {
	case <-a.cola.lecturas:
	case <-time.After(guardia):
		t.Fatal("el bucle no volvió a consultar la cabeza")
	}
}

// despertar libera UNA espera. El envío bloquea hasta que el bucle esté de verdad esperando, así que
// también sirve como sincronía: al volver, se sabe que la vuelta anterior terminó.
func (a *arnes) despertar(t *testing.T) {
	t.Helper()
	select {
	case a.desp.c <- struct{}{}:
	case <-time.After(guardia):
		t.Fatal("el bucle no llegó a esperar (¿se quedó entregando?)")
	}
}

// sincronizar bloquea hasta que el bucle esté OTRA VEZ esperando, es decir hasta que la vuelta anterior
// terminó del todo. Es lo que hay que llamar antes de leer un CONTADOR: los contadores se incrementan
// DESPUÉS de que Deliver retorne, así que asertar justo al recibir la entrega leería el valor de la vuelta
// anterior una de cada tantas ejecuciones — un test intermitente, que es peor que uno rojo.
func (a *arnes) sincronizar(t *testing.T) {
	t.Helper()
	a.despertar(t)
}

// esperarParada bloquea hasta que el bucle APARQUE en Esperar, SIN liberarlo. Es la costura del test de
// cierre limpio: deja al bucle detenido en un punto conocido para poder sembrar trabajo nuevo y cancelar
// sabiendo con certeza que no llegó a mirarlo.
func (a *arnes) esperarParada(t *testing.T) {
	t.Helper()
	select {
	case <-a.desp.esperando:
	case <-time.After(guardia):
		t.Fatal("el bucle no llegó a aparcar en Esperar")
	}
}

func (a *arnes) esperarEntrega(t *testing.T) domain.InboundEvent {
	t.Helper()
	select {
	case evt := <-a.sink.entregas:
		return evt
	case <-time.After(guardia):
		t.Fatal("no llegó la entrega esperada")
		return domain.InboundEvent{}
	}
}

// esperarRetorno comprueba que Run salió, y salió limpio. Marca el arnés para que el Cleanup no vuelva a
// esperar lo que ya se consumió aquí.
func (a *arnes) esperarRetorno(t *testing.T) {
	t.Helper()
	select {
	case err := <-a.retorno:
		a.retornado.Store(true)
		if err != nil {
			t.Fatalf("Run devolvió error en una parada ordenada: %v", err)
		}
	case <-time.After(guardia):
		t.Fatal("Run no retornó tras cancelar el contexto")
	}
}

func (a *arnes) parar() {
	a.cancel()
	if a.retornado.Load() {
		return
	}
	select {
	case <-a.retorno:
	case <-time.After(guardia):
	}
}

func filaClasificada(id, seq int64, intentJSON string) *app.ColaCabeza {
	return &app.ColaCabeza{
		ID: id, Seq: seq, SessionID: "sesion-1", ChatJID: "593999@s.whatsapp.net",
		WAMessageID: "wamid-" + string(rune('0'+id)), TSWhatsApp: 1_755_000_000 + seq,
		Estado: app.EstadoClasificado, IntentJSON: intentJSON, TieneIntent: intentJSON != "",
		Texto: "hola", Meta: []byte(`{"sender":"593999@s.whatsapp.net","push_name":"Ana","type":"text"}`),
	}
}

func filaPendiente(id, seq int64) *app.ColaCabeza {
	f := filaClasificada(id, seq, "")
	f.Estado = app.EstadoNuevo
	return f
}

// ─────────────────────────────────────────────────────────────────────────────
// (1) FIFO — la fila N+1 NO se adelanta a la N que espera al cajero (REQ-051.18)
// ─────────────────────────────────────────────────────────────────────────────

// Es LA invariante que compra la ola, y el escenario es exactamente el que la rompería si no existiera:
// la fila 1 sigue `nuevo` (el worker la tiene) y la fila 2, POSTERIOR, ya está lista con su intención (el
// fastlane la resolvió en µs al nacer). Sin FIFO, la 2 saldría primero y la conversación llegaría al
// cliente del revés.
func TestFIFONoAdelantaALaCabezaQueEsperaAlCajero(t *testing.T) {
	cola := nuevaColaFake(
		filaPendiente(1, 10),
		filaClasificada(2, 20, app.SobreOmitido(app.MotivoFastlane)),
	)
	// Presupuesto amplio y reloj parado: en este test el presupuesto NO debe entrar en juego.
	a := arrancar(t, cola, time.Minute)

	// Tres vueltas completas con la cabeza sin clasificar: ni una entrega, ni un sello.
	a.esperarLectura(t)
	for i := 0; i < 3; i++ {
		a.despertar(t)
		a.esperarLectura(t)
	}
	if n := a.sink.cuantos(); n != 0 {
		t.Fatalf("se entregó algo con la cabeza aún sin clasificar: %d entregas", n)
	}
	if s := a.cola.sellosSinIntent(); len(s) != 0 {
		t.Fatalf("se selló por presupuesto sin que venciera: %+v", s)
	}

	// Llega el cajero: la fila 1 se cierra. A partir de aquí salen LAS DOS, y en orden.
	cola.clasificar(1, `{"intent":"crear_pedido","params":{"producto":"pan"},"confidence":0.9,"config_version":"v7"}`)
	a.despertar(t)

	primera := a.esperarEntrega(t)
	segunda := a.esperarEntrega(t)
	if primera.MessageID != "wamid-1" || segunda.MessageID != "wamid-2" {
		t.Fatalf("orden roto: salieron %q y luego %q", primera.MessageID, segunda.MessageID)
	}
	if primera.Intent == nil || primera.Intent.Name != "crear_pedido" {
		t.Fatalf("la fila 1 salió sin su intención real: %+v", primera.Intent)
	}
	// El sobre `omitido` de la fila 2 MUERE EN EL EDGE: no viaja al cable.
	if segunda.Intent != nil {
		t.Fatalf("un sobre de omisión viajó como intención: %+v", segunda.Intent)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (2) El presupuesto (T3.4a): el reloj arranca al ver la cabeza, no al encolarla
// ─────────────────────────────────────────────────────────────────────────────

// TestPresupuestoVencidoDespachaSinIntent NACIÓ ROJO EL 2026-08-17 y lo puso verde el arreglo de la
// sentencia, no un retoque de sus aserciones: hasta ese día `DespacharSinIntent` sellaba `despachado`, la
// relectura de `correrPresupuesto` no volvía a encontrar la fila y al sink no llegaba nada. Las
// aserciones de abajo NO se relajaron para acomodarlo.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (leídas del código, no supuestas):
//   - devolver `EstadoDespachado` en el doble `colaFake.DespacharSinIntent` (es decir, volver a la
//     sentencia vieja) ⇒ `CabezaDeSesion` deja de ver la fila y NO hay entrega: FALLA en «el mensaje se
//     perdió» con 0 entregas;
//   - cambiar el `<` de `correrPresupuesto` por `<=` ⇒ el tramo de 3999 ms sella antes de tiempo: FALLA en
//     «venció antes de tiempo»;
//   - quitar el `return true` (progreso) de `correrPresupuesto` ⇒ la entrega no ocurre en la misma ráfaga
//     y `sincronizar` devuelve con el sink aún vacío: FALLA en «el mensaje se perdió»;
//   - quitar el `olvidarCabeza()` de la rama `EstadoClasificado` de `vuelta` ⇒ el `disparado=true` de esta
//     fila sobrevive; no rompe ESTE test, lo rompe el siguiente reuso del id (ver el comentario allí);
//   - hacer que `evento` rellene `evt.Intent` para un sobre de omisión ⇒ FALLA en «llevó intención»;
//   - cruzar los contadores (contar el disparo en el desglose de omitidos, o al revés) ⇒ FALLA en uno de
//     los dos: miden cosas distintas a propósito (ver PresupuestosVencidos()).
func TestPresupuestoVencidoDespachaSinIntent(t *testing.T) {
	cola := nuevaColaFake(filaPendiente(1, 10))
	a := arrancar(t, cola, 4*time.Second)

	// Vuelta 1: se ve la cabeza por primera vez ⇒ arranca su reloj. Nada más.
	a.esperarLectura(t)
	if s := a.cola.sellosSinIntent(); len(s) != 0 {
		t.Fatalf("se selló en la primera vuelta, sin haber esperado nada: %+v", s)
	}

	// Justo por debajo del presupuesto: sigue sin vencer (el borde importa, es un `<`).
	a.reloj.avanzar(3999 * time.Millisecond)
	a.despertar(t)
	a.esperarLectura(t)
	if s := a.cola.sellosSinIntent(); len(s) != 0 {
		t.Fatalf("venció antes de tiempo: %+v", s)
	}

	// Se cruza el umbral: sobre de omisión + RELECTURA inmediata (progreso). La relectura encuentra la fila
	// en `clasificado` con su sobre y la entrega SIN intención, y sólo entonces la sella.
	a.reloj.avanzar(2 * time.Millisecond)
	a.despertar(t)

	// BARRERA antes de mirar el sink: `despertar` bloquea hasta que el bucle esté OTRA VEZ aparcado, así que
	// al volver se sabe que el sobre, su relectura y la entrega ya ocurrieron del todo. Sin esta barrera, el
	// fallo de abajo se manifestaría como un cuelgue de dos segundos en `esperarEntrega` en vez de como un
	// mensaje que explica qué pasó (T3.5, 2026-08-17).
	a.sincronizar(t)
	if n := a.sink.cuantos(); n != 1 {
		t.Fatalf("🔴 EL MENSAJE SE PERDIÓ AL VENCER EL PRESUPUESTO: %d entregas al sink (se esperaba 1). "+
			"El contrato es: `DespacharSinIntent` deja la fila en `clasificado` con su sobre, la vuelta "+
			"siguiente la relee y `entregar` —la única puerta al sink— la manda sin intención. Si aquí hay 0, "+
			"lo más probable es que la fila haya vuelto a acabar en `despachado` (el bug del 2026-08-17: "+
			"`CabezaDeSesion` excluye ese estado y la relectura no la encuentra). REQ-051.19 promete "+
			"retraso, no pérdida", n)
	}

	evt := a.esperarEntrega(t)
	if evt.MessageID != "wamid-1" {
		t.Fatalf("se entregó otra fila: %q", evt.MessageID)
	}
	if evt.Intent != nil {
		t.Fatalf("un despacho por presupuesto llevó intención: %+v", evt.Intent)
	}
	sellos := a.cola.sellosSinIntent()
	if len(sellos) != 1 || sellos[0].id != 1 || sellos[0].motivo != app.MotivoPresupuesto {
		t.Fatalf("sello inesperado: %+v", sellos)
	}
	a.sincronizar(t)
	if got := a.d.PresupuestosVencidos(); got != 1 {
		t.Fatalf("presupuestos_vencidos = %d, se esperaba 1", got)
	}
	if got := a.d.OmitidosPorMotivo()[app.MotivoPresupuesto]; got != 1 {
		t.Fatalf("omitido_presupuesto = %d, se esperaba 1", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (3) La regla del sobre (T3.4b) recorriendo los OCHO motivos
// ─────────────────────────────────────────────────────────────────────────────

// 🔴 EL TEST RECORRE app.MotivosOmitido(), NUNCA UNA LISTA ESCRITA A MANO. Ese es justo el punto: el día
// que entre un noveno motivo, este test lo cubre solo. Una lista copiada aquí se habría quedado corta las
// dos veces que la lista canónica creció (T1.8 y T2.19).
func TestReglaDelSobreCubreTodosLosMotivos(t *testing.T) {
	motivos := app.MotivosOmitido()
	if len(motivos) < 8 {
		t.Fatalf("la lista canónica encogió: %d motivos", len(motivos))
	}

	filas := make([]*app.ColaCabeza, 0, len(motivos))
	for i, m := range motivos {
		id := int64(i + 1)
		filas = append(filas, filaClasificada(id, id*10, app.SobreOmitido(m)))
	}
	cola := nuevaColaFake(filas...)
	a := arrancar(t, cola, time.Minute)

	for range motivos {
		evt := a.esperarEntrega(t)
		// LA REGLA: el sobre de omisión NO viaja al cable, en ninguno de sus ocho sabores.
		if evt.Intent != nil {
			t.Fatalf("un sobre de omisión viajó como intención (%s): %+v", evt.MessageID, evt.Intent)
		}
		// El contenido sí viaja: lo que muere es el sobre, no el mensaje.
		if evt.Text != "hola" {
			t.Fatalf("el mensaje salió sin texto: %+v", evt.MessageID)
		}
	}

	a.sincronizar(t)
	desglose := a.d.OmitidosPorMotivo()
	if len(desglose) != len(motivos) {
		t.Fatalf("el desglose no tiene una entrada por motivo: %d vs %d", len(desglose), len(motivos))
	}
	for _, m := range motivos {
		if desglose[m] != 1 {
			t.Fatalf("motivo %q contado %d veces (se esperaba 1); ¿falta en el desglose?", m, desglose[m])
		}
	}
	if got := a.d.ConIntent(); got != 0 {
		t.Fatalf("con_intent = %d, se esperaba 0: ningún omitido lleva intención", got)
	}
	if got := a.d.Despachados(); got != int64(len(motivos)) {
		t.Fatalf("despachados = %d, se esperaban %d", got, len(motivos))
	}
}

// TestSobreDelCajeroSeLeeConSusClaves blinda LAS CLAVES del `intent_json`, que es el fallo caro de esta
// tarea: una clave mal escrita no rompe nada, no falla ningún Unmarshal y no aparece en ningún log —
// simplemente el Cloud recibe una intención vacía.
//
// El JSON de abajo está copiado LITERAL de la forma que serializa `sobreCajero`
// (internal/app/cajero/cajero.go). El round-trip de verdad —el cajero escribiendo y este código
// leyendo— es de T3.5; esto es la red de abajo.
func TestSobreDelCajeroSeLeeConSusClaves(t *testing.T) {
	const sobre = `{"intent":"crear_pedido","params":{"producto":"pan","cantidad":"2"},"confidence":0.87,"config_version":"v7"}`
	cola := nuevaColaFake(filaClasificada(1, 10, sobre))
	a := arrancar(t, cola, time.Minute)

	evt := a.esperarEntrega(t)
	if evt.Intent == nil {
		t.Fatal("el sobre del cajero no se tradujo a una intención")
	}
	if evt.Intent.Name != "crear_pedido" {
		t.Fatalf("clave `intent` mal leída: %q", evt.Intent.Name)
	}
	if evt.Intent.Params["producto"] != "pan" || evt.Intent.Params["cantidad"] != "2" {
		t.Fatalf("clave `params` mal leída: %+v", evt.Intent.Params)
	}
	if evt.Intent.Confidence != 0.87 {
		t.Fatalf("clave `confidence` mal leída: %v", evt.Intent.Confidence)
	}
	if evt.Intent.ConfigVersion != "v7" {
		t.Fatalf("clave `config_version` mal leída: %q", evt.Intent.ConfigVersion)
	}
	// Y los metadatos del `meta_enc`, que es el otro contrato por claves JSON.
	if evt.Sender != "593999@s.whatsapp.net" || evt.PushName != "Ana" || evt.Type != "text" {
		t.Fatalf("meta mal reconstruida: sender=%q push_name=%q type=%q", evt.Sender, evt.PushName, evt.Type)
	}
	if evt.IsFromMe {
		t.Fatal("IsFromMe debe ser false: el eco propio no llega a la cola")
	}
	a.sincronizar(t)
	if got := a.d.ConIntent(); got != 1 {
		t.Fatalf("con_intent = %d, se esperaba 1", got)
	}
}

// TestFilaSinSobreEsFragmentoDeLoteNoPresupuesto: una fila `clasificado` con `intent_json` NULL es un
// FRAGMENTO intermedio de un lote (el cajero escribe el intent sólo en la última fila del turno), y NO un
// despacho por presupuesto — que siempre deja su sobre escrito. Cargarlo al contador de `presupuesto`
// inflaría con tráfico sano la serie que el operador usa para decidir si sube WAPP_AGENT_INTENT_WAIT_MS.
func TestFilaSinSobreEsFragmentoDeLoteNoPresupuesto(t *testing.T) {
	fila := filaClasificada(1, 10, "")
	fila.TieneIntent = false
	cola := nuevaColaFake(fila)
	a := arrancar(t, cola, time.Minute)

	evt := a.esperarEntrega(t)
	if evt.Intent != nil {
		t.Fatalf("un fragmento sin sobre llevó intención: %+v", evt.Intent)
	}
	a.sincronizar(t)
	if got := a.d.FragmentosDeLote(); got != 1 {
		t.Fatalf("fragmentos_de_lote = %d, se esperaba 1", got)
	}
	if got := a.d.OmitidosPorMotivo()[app.MotivoPresupuesto]; got != 0 {
		t.Fatalf("omitido_presupuesto = %d: un fragmento de lote NO es un presupuesto vencido", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (4) Cierre limpio: al cancelar, Run retorna y NINGUNA entrega ocurre después
// ─────────────────────────────────────────────────────────────────────────────

func TestCierreLimpioNoEntregaDespuesDeCancelar(t *testing.T) {
	cola := nuevaColaFake(
		filaClasificada(1, 10, app.SobreOmitido(app.MotivoFastlane)),
		filaClasificada(2, 20, app.SobreOmitido(app.MotivoFastlane)),
		filaClasificada(3, 30, app.SobreOmitido(app.MotivoFastlane)),
	)
	a := arrancar(t, cola, time.Minute)

	// Se drenan las tres y el bucle acaba esperando (cola vacía).
	for i := 0; i < 3; i++ {
		a.esperarEntrega(t)
	}
	// Las tres salen encadenadas (progreso ⇒ sin esperar), así que el bucle APARCA exactamente una vez: al
	// encontrarse la cola vacía. Se espera a esa parada —sin liberarla— para saber con certeza dónde está.
	a.esperarParada(t)

	// Se siembra una CUARTA fila y se cancela SIN despertar: el bucle sigue bloqueado en Esperar, así que la
	// cancelación tiene que sacarlo de ahí sin llegar a entregarla.
	cola.mu.Lock()
	cola.filas = append(cola.filas, filaClasificada(4, 40, app.SobreOmitido(app.MotivoFastlane)))
	cola.mu.Unlock()

	a.cancel()
	a.esperarRetorno(t)
	if n := a.sink.cuantos(); n != 3 {
		t.Fatalf("se entregó algo después de cancelar: %d entregas (se esperaban 3)", n)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (5) Construcción: lo que hace IMPOSIBLE el bucle se rechaza en frío
// ─────────────────────────────────────────────────────────────────────────────

// Un despachador construido sobre una cola nil sería un PÁNICO, no un no-op, así que se rechaza en frío.
//
// ⚠️ DESDE EL 2026-08-17 EL nil YA NO ES ALCANZABLE POR EL CAMINO QUE MOTIVÓ ESTA GUARDA: la apertura y
// migración de `cola_entrantes.db` en daemon.go es FATAL, así que un daemon vivo tiene cola por
// construcción. La guarda se conserva igual porque sigue habiendo llamantes que pueden pasar nil (los
// tests, y cualquier cableado futuro que no venga del daemon), y porque una dependencia obligatoria se
// valida donde se construye, no donde se usa.
func TestNewRechazaLasDependenciasImposibles(t *testing.T) {
	casos := []struct {
		nombre string
		deps   Deps
	}{
		{"sin cola", Deps{Sink: nuevoSinkFake(), SessionID: "s"}},
		{"sin sink", Deps{Cola: nuevaColaFake(), SessionID: "s"}},
		{"sin session_id", Deps{Cola: nuevaColaFake(), Sink: nuevoSinkFake()}},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			if _, err := New(c.deps); err == nil {
				t.Fatal("se construyó un despachador que no puede funcionar")
			}
		})
	}
}

// TestNewAplicaLosDefaults: lo que sí tiene default seguro no puede impedir el arranque.
func TestNewAplicaLosDefaults(t *testing.T) {
	d, err := New(Deps{Cola: nuevaColaFake(), Sink: nuevoSinkFake(), SessionID: "s"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if d.presupuesto != DefaultPresupuestoMS*time.Millisecond {
		t.Fatalf("presupuesto por defecto = %v", d.presupuesto)
	}
	if d.despertador == nil || d.ahora == nil || d.log == nil {
		t.Fatal("faltó aplicar algún default")
	}
	// El desglose nace con las OCHO entradas: un motivo a 0 es información, no un hueco.
	if len(d.OmitidosPorMotivo()) != len(app.MotivosOmitido()) {
		t.Fatalf("el desglose no arrancó con todos los motivos: %d", len(d.OmitidosPorMotivo()))
	}
}
