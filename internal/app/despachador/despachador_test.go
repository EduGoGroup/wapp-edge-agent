package despachador

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// despachador_test.go — LOS TESTS MÍNIMOS DEL DESPACHADOR (Plan 051 Ola 3 · T3.3/T3.4, reescritos por el
// Plan 044 · Ola 1.6 · T1.6-5).
//
// ⚠️ NO son los siete bloques de la ola: T3.5 tiene su propio dueño y cubre el circuito completo contra
// una BD real. Estos son los que el propio código necesita para no nacer a ciegas — que NO se retenga
// nada, la invariante FIFO, la regla del sobre sobre los OCHO motivos y el cierre limpio.
//
// 🔴 EL BLOQUE (2) CAMBIÓ DE SIGNO EL 2026-08-24. Era «el presupuesto»: probaba que la cabeza se retenía
// hasta 4 s esperando al cajero y que al vencer salía sin intención. El ADR-0045 disolvió esa espera, así
// que hoy prueba lo contrario —que NO se retiene NADA, nunca— y el reloj falso dejó de gobernar la
// entrega. Se conserva inyectable porque el backoff de la re-entrega sigue midiéndose con él.
//
// 🔴 DETERMINISMO POR CONSTRUCCIÓN, SIN UN SOLO time.Sleep COMO MECANISMO DE SINCRONÍA. La costura que lo
// permite es el DESPERTADOR (aquí un canal que el test libera de uno en uno, así que «una vuelta del
// bucle» es un evento observable y no una apuesta sobre cuánto tarda algo). Los `time.After` que aparecen
// abajo NO son sincronía: son guardias anti-cuelgue para que un fallo se manifieste como un test rojo en
// 2 s y no como un CI colgado.

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

	marcadas []int64

	// lecturas señala CADA CabezaDeSesion. Buffer generoso + envío no bloqueante: la señal es para que el
	// test pueda esperar a que una vuelta haya ocurrido, nunca para frenar al bucle.
	lecturas chan struct{}
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

// MarcarDespachada aplica el mismo fence que el SQL real: `estado <> 'despachado'` (T1.6-5), y 0 filas
// afectadas NO es error.
//
// 🔴 EL FENCE DEL DOBLE ES LA MITAD DE LO QUE ESTE PAQUETE PRUEBA, y ya mintió una vez (ver el bloque de
// abajo sobre el doble retirado). Si aquí se dejara el viejo `== app.EstadoClasificado`, ninguna fila
// nueva llegaría a `despachado`, la cola nunca se vaciaría y varios tests se colgarían contra su guardia
// en vez de fallar diciendo por qué. Se copia la cláusula real, no la que le conviene al test.
func (c *colaFake) MarcarDespachada(_ context.Context, id int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.marcadas = append(c.marcadas, id)
	for _, f := range c.filas {
		if f.ID == id && f.Estado != app.EstadoDespachado {
			f.Estado = app.EstadoDespachado
		}
	}
	return nil
}

// 🔴 AQUÍ VIVÍA EL DOBLE DE `DespacharSinIntent`, Y SE FUE CON EL MÉTODO EL 2026-08-24 (Plan 044 · Ola
// 1.6 · T1.6-5 · ADR-0045): era el camino del presupuesto vencido —escribir el sobre de omisión y dejar
// la fila resuelta— y ya no existe ni en el puerto ni en el adaptador.
//
// 🔴 SE CONSERVA SU LECCIÓN, QUE COSTÓ UN BUG DE PÉRDIDA DE MENSAJES (T3.5, 2026-08-17). Aquel doble
// decía `EstadoClasificado` cuando la sentencia real escribía `EstadoDespachado`: modelaba lo que el
// BUCLE necesitaba para poder releer la fila, no lo que el COLABORADOR hacía. Con el fake así, este
// paquete pasaba en verde mientras el circuito real perdía el mensaje.
//
// LA REGLA QUE DEJÓ ESCRITA, y que sigue gobernando `MarcarDespachada` aquí arriba: un doble que modela
// lo que el llamante NECESITA, en vez de lo que el colaborador HACE, no es un doble — es una segunda
// implementación que se adapta al código bajo prueba, y su forma de fallar es un test verde sobre
// producción rota.

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

func arrancar(t *testing.T, cola *colaFake) *arnes {
	t.Helper()
	sink := nuevoSinkFake()
	reloj := nuevoReloj()
	desp := nuevoDespertadorManual()

	d, err := New(Deps{
		Cola:        cola,
		Sink:        sink,
		SessionID:   "sesion-1",
		Ahora:       reloj.ahora,
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

// filaNueva es la fila que produce el listener HOY: `nuevo`, sin sobre. Es el 100 % del tráfico bajo pull.
func filaNueva(id, seq int64) *app.ColaCabeza {
	return &app.ColaCabeza{
		ID: id, Seq: seq, SessionID: "sesion-1", ChatJID: "593999@s.whatsapp.net",
		WAMessageID: "wamid-" + string(rune('0'+id)), TSWhatsApp: 1_755_000_000 + seq,
		Estado: app.EstadoNuevo,
		Texto:  "hola", Meta: []byte(`{"sender":"593999@s.whatsapp.net","push_name":"Ana","type":"text"}`),
	}
}

// filaVieja es una fila escrita por un binario ANTERIOR a T1.6-5: `clasificado` y con su sobre. No se
// puede producir hoy, pero está en los discos de campo y hay que drenarla; de eso van varios tests.
func filaVieja(id, seq int64, intentJSON string) *app.ColaCabeza {
	f := filaNueva(id, seq)
	f.Estado = app.EstadoClasificado
	f.IntentJSON = intentJSON
	f.TieneIntent = intentJSON != ""
	return f
}

// ─────────────────────────────────────────────────────────────────────────────
// (1) NO SE RETIENE NADA (Plan 044 · Ola 1.6 · T1.6-5 · ADR-0045 · D-044.31 · REQ-35)
// ─────────────────────────────────────────────────────────────────────────────

// TestCabezaNuevaSaleEnLaPRIMERAVuelta es EL test de la tarea, y su forma lo dice todo: el reloj NO se
// toca ni una vez.
//
// 🔴 QUÉ AFIRMABA ESTE BLOQUE ANTES, para que el diff se lea solo. Hasta el 2026-08-24 una cabeza `nuevo`
// era una cabeza que ESPERABA al cajero: el bucle la miraba, arrancaba su reloj, la dejaba pasar poll
// tras poll hasta agotar `WAPP_AGENT_INTENT_WAIT_MS` (4 s) y sólo entonces la resolvía sin intención. El
// test correspondiente adelantaba el reloj a 3999 ms para comprobar el borde y luego 2 ms más para verla
// vencer. Hoy no hay borde que comprobar porque no hay reloj: se entrega en el acto.
//
// LA MEDIDA QUE MATÓ AQUELLO: de 430 inferencias, UNA cupo en la ventana; hubo descartes a 8 ms de llegar
// la etiqueta. Retener 4 s todos los mensajes para acertar 1 de cada 430 es el trato que el ADR-0045
// deshizo.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - devolver a `vuelta` cualquier condición sobre `cabeza.Estado` antes de entregar (p. ej.
//     `if cabeza.Estado != app.EstadoClasificado { return false }`) ⇒ la entrega no llega y el test falla
//     contra su guardia con el mensaje de abajo;
//   - hacer que `entregar` selle antes de entregar ⇒ no cambia ESTE test (lo caza el circuito real).
func TestCabezaNuevaSaleEnLaPRIMERAVuelta(t *testing.T) {
	cola := nuevaColaFake(filaNueva(1, 10))
	a := arrancar(t, cola)

	// NI UN `a.reloj.avanzar(...)`, NI UN `a.despertar(...)`: si hiciera falta cualquiera de los dos, es
	// que algo sigue reteniendo la cabeza. La primera vuelta del bucle tiene que bastar.
	evt := a.esperarEntrega(t)
	if evt.MessageID != "wamid-1" {
		t.Fatalf("se entregó otra fila: %q", evt.MessageID)
	}
	if evt.Text != "hola" {
		t.Fatalf("el mensaje salió sin texto: %+v", evt)
	}

	a.sincronizar(t)
	if got := a.d.Despachados(); got != 1 {
		t.Fatalf("despachados = %d, se esperaba 1", got)
	}
	// Y quedó SELLADA: sin sello la poda por TTL no puede tocarla nunca y la fila se re-entregaría en cada
	// poll. Es la mitad del cambio de fence de `MarcarDespachada` (de `= 'clasificado'` a `<> 'despachado'`).
	cola.mu.Lock()
	estado := cola.filas[0].Estado
	cola.mu.Unlock()
	if estado != app.EstadoDespachado {
		t.Fatalf("la fila entregada quedó en %q y debía quedar %q: sin sello se re-entrega en cada poll "+
			"y el TTL no puede podarla NUNCA", estado, app.EstadoDespachado)
	}
}

// TestCabezaClasificadaDeUnBinarioAnteriorSeEntrega es EL TEST DE LA MIGRACIÓN a nivel de bucle, y existe
// aunque parezca redundante con los de arriba.
//
// 🔴 QUÉ CUBRE QUE NINGÚN OTRO CUBRE. `clasificado` era, hasta el 2026-08-24, EL ÚNICO estado desde el que
// este bucle entregaba; hoy es el único que ya no puede producirse (ADR-0045 §Decisión.4). Esa inversión
// deja un hueco fácil de no ver: alguien podría «limpiar» el estado del ciclo y dejar sin salida a las
// filas que YA ESTÁN ESCRITAS en `<data_dir>/cola_entrantes.db` en los equipos de clientes. Esas filas no
// se pueden migrar con un `.sql` —la cola es un fichero local por instalación— así que la única garantía
// de que salgan es que el bucle las entregue, y la única garantía de eso es este test.
//
// LO QUE PASARÍA SIN ESTA GARANTÍA no es un error visible: son mensajes reales de clientes reales que se
// quedan en disco para siempre, con la cola creciendo hasta chocar con su tope y empezar a sacrificar
// conversaciones. Silencioso de principio a fin.
//
// ⚠️ SE COMPRUEBAN LAS DOS FORMAS QUE TIENE UNA FILA HEREDADA, porque el `intent_json` las separa y el
// veredicto de `evento` no es el mismo: con sobre de omisión y con clasificación real. La tercera forma
// —`clasificado` sin sobre, el fragmento intermedio de un lote— entra por el camino del NULL y la cubre
// `TestFilaSinSobreNoCuentaEnNingunaSerie`.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): devolver a `vuelta` cualquier condición sobre
// `cabeza.Estado` que excluya `clasificado`. Su gemelo contra la BD REAL —donde además se comprueba el
// sello y la poda— es `TestCircuitoFilaAntiguaClasificadaSeDrenaSeSellaYSePoda`.
func TestCabezaClasificadaDeUnBinarioAnteriorSeEntrega(t *testing.T) {
	for _, c := range []struct {
		nombre string
		sobre  string
	}{
		{"con sobre de omisión", app.SobreOmitido(app.MotivoFastlane)},
		{"con clasificación real", `{"intent":"crear_pedido","confidence":0.9,"config_version":"v7"}`},
	} {
		t.Run(c.nombre, func(t *testing.T) {
			cola := nuevaColaFake(filaVieja(1, 10, c.sobre))
			a := arrancar(t, cola)

			// Sin tocar el reloj y sin despertar a nadie: una fila heredada sale igual de rápido que una nueva.
			evt := a.esperarEntrega(t)
			if evt.MessageID != "wamid-1" || evt.Text != "hola" {
				t.Fatalf("la fila heredada no salió entera: %+v", evt)
			}

			a.sincronizar(t)
			// Y QUEDA SELLADA. Es la mitad que se olvida: entregarla sin poder sellarla la dejaría
			// re-entregándose en cada poll para siempre y fuera del alcance de la poda por TTL.
			cola.mu.Lock()
			estado := cola.filas[0].Estado
			cola.mu.Unlock()
			if estado != app.EstadoDespachado {
				t.Fatalf("la fila heredada quedó en %q y debía quedar %q: se entregó pero no se pudo sellar, "+
					"así que se re-entregará en cada poll y el TTL no podrá podarla nunca", estado, app.EstadoDespachado)
			}
		})
	}
}

// TestCabezaTomadaSaleIgual: una fila con un claim vivo (`tomado`) se entrega como cualquier otra.
//
// 🔴 ES LA MITAD MENOS OBVIA DEL ADR-0045 §Decisión.4: el claim con fencing SE CONSERVA (ADR-0038) — es
// lo que impide que dos procesos cierren el mismo lote— pero NO es, ni fue nunca, un derecho de retención
// sobre la entrega. Bajo push daba igual, porque una fila `tomado` se quedaba esperando al presupuesto de
// todos modos; hoy la diferencia es visible y hay que fijarla, porque el reflejo natural al leer «claim»
// es «no toques esto».
func TestCabezaTomadaSaleIgual(t *testing.T) {
	fila := filaNueva(1, 10)
	fila.Estado = app.EstadoTomado
	cola := nuevaColaFake(fila)
	a := arrancar(t, cola)

	evt := a.esperarEntrega(t)
	if evt.MessageID != "wamid-1" {
		t.Fatalf("se entregó otra fila: %q", evt.MessageID)
	}
}

// TestCabezaEnEstadoImprevistoSaleTambien: el FALLO SEGURO cambió de «retener» a «entregar».
//
// 🔴 ESTE TEST AFIRMA LO CONTRARIO DE `TestCabezaAtascadaSeCuenta`, QUE ESTABA EN freno_entrega_test.go Y
// SE BORRÓ CON ÉL. Hasta el 2026-08-24, una cabeza en un estado que esta versión no conocía bloqueaba la
// sesión ENTERA para siempre y se contaba en dos series propias (`cabezas_atascadas`,
// `polls_cabeza_atascada`): se prefería una parada visible y acotada a un salto de orden invisible.
//
// La disyuntiva era esa porque `vuelta` sólo entregaba desde `clasificado`. Al dejar de mirar el estado,
// la tercera opción —entregar— pasó a existir, y es estrictamente mejor que las otras dos: no rompe el
// FIFO (sale la cabeza, no la siguiente) y no pierde ni retiene el mensaje. Por eso las dos series se
// quedaron sin productor y sus campos de proto (`stuck_heads`, `stuck_head_polls`) están hoy clavados a 0.
func TestCabezaEnEstadoImprevistoSaleTambien(t *testing.T) {
	fila := filaNueva(1, 10)
	fila.Estado = "un-estado-de-mañana"
	cola := nuevaColaFake(fila, filaNueva(2, 20))
	a := arrancar(t, cola)

	primera := a.esperarEntrega(t)
	segunda := a.esperarEntrega(t)
	if primera.MessageID != "wamid-1" || segunda.MessageID != "wamid-2" {
		t.Fatalf("una cabeza en estado imprevisto ni bloqueó ni se saltó: salieron %q y %q "+
			"(se esperaba wamid-1 y luego wamid-2)", primera.MessageID, segunda.MessageID)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (2) FIFO — la fila N+1 NO se adelanta a la N (REQ-051.18)
// ─────────────────────────────────────────────────────────────────────────────

// Es LA invariante que compra la ola. 🔴 EL ESCENARIO QUE LA PRUEBA CAMBIÓ CON EL PUSH: antes era «la N
// espera al cajero y la N+1 ya está lista», y bajo pull nadie espera a nadie, así que ese escenario dejó
// de existir. Lo que SÍ queda —y es hoy la única forma de que una cabeza retenga a su sesión— es una
// entrega que FALLA: la fila no se sella, sigue siendo cabeza, y la N+1 no puede adelantarla.
//
// Sin FIFO, la 2 saldría mientras la 1 se reintenta, y la conversación llegaría al cliente del revés —
// que es un daño que no se arregla aguas arriba.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada, y el rojo NO es el que uno esperaría): que el freno de
// `vuelta` devuelva «progreso» en vez de «sin progreso» cuando la entrega viene fallando. El test cae en
// `el bucle no llegó a esperar (¿se quedó entregando?)`, no en la aserción de orden — con «progreso» el
// bucle no aparca nunca y la guardia anti-cuelgue muerde antes. Se anota tal cual en vez de prometer el
// mensaje bonito: un docstring que promete un rojo que no ocurre miente igual que un test verde que no
// mira.
func TestFIFONoAdelantaALaCabezaQueNoSeHaPodidoEntregar(t *testing.T) {
	cola := nuevaColaFake(filaNueva(1, 10), filaNueva(2, 20))
	a := arrancar(t, cola)

	// El sink rechaza TODO desde el arranque: la cabeza no se sella y sigue siendo la cabeza.
	a.sink.fallarCon(errors.New("el sink está caído"))

	// Tres vueltas completas con el sink caído: ni una entrega registrada, y la fila 2 quieta detrás.
	a.esperarLectura(t)
	for i := 0; i < 3; i++ {
		a.despertar(t)
		a.esperarLectura(t)
	}
	if n := a.sink.cuantos(); n != 0 {
		t.Fatalf("el sink caído registró %d entregas", n)
	}
	cola.mu.Lock()
	sellada := cola.filas[0].Estado == app.EstadoDespachado
	cola.mu.Unlock()
	if sellada {
		t.Fatal("se selló una fila cuya entrega falló: eso es una PÉRDIDA de mensaje (entrega ANTES de sello)")
	}

	// Vuelve el sink: salen las dos, y EN ORDEN.
	a.sink.fallarCon(nil)
	// El freno de re-entrega ya espació el próximo intento; se adelanta el reloj para soltarlo.
	a.reloj.avanzar(time.Minute)
	a.despertar(t)

	primera := a.esperarEntrega(t)
	segunda := a.esperarEntrega(t)
	if primera.MessageID != "wamid-1" || segunda.MessageID != "wamid-2" {
		t.Fatalf("orden roto: salieron %q y luego %q", primera.MessageID, segunda.MessageID)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (3) La regla del sobre (T3.4b) recorriendo los OCHO motivos
// ─────────────────────────────────────────────────────────────────────────────

// 🔴 EL TEST RECORRE app.MotivosOmitido(), NUNCA UNA LISTA ESCRITA A MANO. Ese es justo el punto: el día
// que entre un noveno motivo, este test lo cubre solo. Una lista copiada aquí se habría quedado corta las
// dos veces que la lista canónica creció (T1.8 y T2.19).
//
// ⚠️ DESDE T1.6-5 LOS OCHO SOBRES SÓLO PUEDEN VENIR DE FILAS ANTIGUAS (por eso el molde es `filaVieja`):
// ningún productor del Edge escribe ya sobres de omisión. El test SIGUE SIENDO NECESARIO, y más que
// antes: es lo único que garantiza que una cola escrita por un binario viejo se drena entera y contada,
// en vez de atascarse o perder su desglose mientras se vacía.
func TestReglaDelSobreCubreTodosLosMotivos(t *testing.T) {
	motivos := app.MotivosOmitido()
	if len(motivos) < 8 {
		t.Fatalf("la lista canónica encogió: %d motivos", len(motivos))
	}

	filas := make([]*app.ColaCabeza, 0, len(motivos))
	for i, m := range motivos {
		id := int64(i + 1)
		filas = append(filas, filaVieja(id, id*10, app.SobreOmitido(m)))
	}
	cola := nuevaColaFake(filas...)
	a := arrancar(t, cola)

	for range motivos {
		evt := a.esperarEntrega(t)
		// LA REGLA: el sobre muere en el Edge, en cualquiera de sus ocho sabores; el MENSAJE sale entero.
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
	if got := a.d.IntentsDescartados(); got != 0 {
		t.Fatalf("intents_descartados = %d, se esperaba 0: un sobre de OMISIÓN no es una clasificación", got)
	}
	if got := a.d.Despachados(); got != int64(len(motivos)) {
		t.Fatalf("despachados = %d, se esperaban %d", got, len(motivos))
	}
}

// TestSobreDelCajeroSeDESCARTAYSeCuenta: una fila ANTIGUA que trae una clasificación REAL se entrega
// entera y la clasificación se TIRA.
//
// 🔴 ESTE TEST AFIRMA HOY LO CONTRARIO DE LO QUE AFIRMABA. Se llamaba `…SeLeeConSusClaves` y blindaba las
// cuatro claves del `intent_json` (`intent`/`params`/`confidence`/`config_version`) porque una clave mal
// escrita no rompía nada, no fallaba ningún Unmarshal y no aparecía en ningún log: simplemente el Cloud
// recibía una intención vacía. Desde el 2026-08-24 el Cloud no recibe NINGUNA intención por este camino
// —el ADR-0045 retiró `ClassifiedIntent` del proto—, así que no hay claves que blindar aquí.
//
// LO QUE SÍ HAY QUE FIJAR, Y ES LO QUE ESTE TEST HACE: que esa fila no se atasque, que su MENSAJE salga
// completo (texto y metadatos incluidos) y que la pérdida de la etiqueta se CUENTE en vez de ocurrir en
// silencio. Es la única pérdida que la migración de push a pull admite, es acotada —sólo las filas ya
// escritas— y es de una etiqueta, jamás de un mensaje.
//
// ⚠️ LAS CUATRO CLAVES NO SE QUEDAN SIN CUSTODIA: el round-trip escritor↔lector sigue probado en el
// paquete del ESCRITOR (internal/app/cajero/sobre_roundtrip_test.go), que es donde debe estar mientras el
// cajero siga escribiéndolas.
func TestSobreDelCajeroSeDESCARTAYSeCuenta(t *testing.T) {
	const sobre = `{"intent":"crear_pedido","params":{"producto":"pan","cantidad":"2"},"confidence":0.87,"config_version":"v7"}`
	cola := nuevaColaFake(filaVieja(1, 10, sobre))
	a := arrancar(t, cola)

	evt := a.esperarEntrega(t)
	if evt.MessageID != "wamid-1" || evt.Text != "hola" {
		t.Fatalf("el mensaje no salió entero: %+v", evt)
	}
	// Y los metadatos del `meta_enc`, que es el contrato por claves JSON que SÍ sigue vivo.
	if evt.Sender != "593999@s.whatsapp.net" || evt.PushName != "Ana" || evt.Type != "text" {
		t.Fatalf("meta mal reconstruida: sender=%q push_name=%q type=%q", evt.Sender, evt.PushName, evt.Type)
	}
	if evt.IsFromMe {
		t.Fatal("IsFromMe debe ser false: el eco propio no llega a la cola")
	}

	a.sincronizar(t)
	if got := a.d.IntentsDescartados(); got != 1 {
		t.Fatalf("intents_descartados = %d, se esperaba 1: la clasificación de una fila vieja se tira, "+
			"pero NO en silencio — es la serie con la que se vigila que las colas viejas se vacían", got)
	}
	if got := a.d.SobresIlegibles(); got != 0 {
		t.Fatalf("sobres_ilegibles = %d: este sobre es perfectamente legible, sólo que ya no tiene destino", got)
	}
	if got := a.d.Omitidos(); got != 0 {
		t.Fatalf("omitidos = %d: un sobre de clasificación NO es un sobre de omisión", got)
	}
}

// TestFilaSinSobreNoCuentaEnNingunaSerie: el CASO NORMAL bajo pull — `intent_json` a NULL — no incrementa
// ninguna de las series de sobre.
//
// 🔴 ESTE TEST AFIRMA HOY LO CONTRARIO DE LO QUE AFIRMABA. Se llamaba
// `…EsFragmentoDeLoteNoPresupuesto`: un `clasificado` con `intent_json` NULL era un FRAGMENTO intermedio
// de un lote (el cajero escribía el intent sólo en la última fila del turno), y se contaba en
// `fragmentos_de_lote` precisamente para no inflar la serie `presupuesto`, que era la que el operador
// miraba para decidir si subía `WAPP_AGENT_INTENT_WAIT_MS`.
//
// Bajo pull ese mismo NULL es el 100 % del tráfico, así que `fragmentos_de_lote` se retiró: un contador
// que sube en cada mensaje y sigue rotulado «fragmentos de lote» es la clase de dato que hace tomar
// decisiones al revés. Y `presupuesto` ya no tiene palanca que calibrar.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: hacer que `evento` cuente el NULL como omitido, ilegible o descartado
// ⇒ falla la serie correspondiente.
func TestFilaSinSobreNoCuentaEnNingunaSerie(t *testing.T) {
	cola := nuevaColaFake(filaNueva(1, 10))
	a := arrancar(t, cola)

	if evt := a.esperarEntrega(t); evt.MessageID != "wamid-1" {
		t.Fatalf("se entregó otra fila: %q", evt.MessageID)
	}
	a.sincronizar(t)
	if got := a.d.Despachados(); got != 1 {
		t.Fatalf("despachados = %d, se esperaba 1", got)
	}
	if got := a.d.Omitidos(); got != 0 {
		t.Fatalf("omitidos = %d: una fila sin sobre no omite nada, es el camino normal", got)
	}
	if got := a.d.SobresIlegibles(); got != 0 {
		t.Fatalf("sobres_ilegibles = %d: no había sobre que leer", got)
	}
	if got := a.d.IntentsDescartados(); got != 0 {
		t.Fatalf("intents_descartados = %d: no había clasificación que tirar", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (4) Cierre limpio: al cancelar, Run retorna y NINGUNA entrega ocurre después
// ─────────────────────────────────────────────────────────────────────────────

func TestCierreLimpioNoEntregaDespuesDeCancelar(t *testing.T) {
	cola := nuevaColaFake(filaNueva(1, 10), filaNueva(2, 20), filaNueva(3, 30))
	a := arrancar(t, cola)

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
	cola.filas = append(cola.filas, filaNueva(4, 40))
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
	if d.despertador == nil || d.ahora == nil || d.log == nil {
		t.Fatal("faltó aplicar algún default")
	}
	// El desglose nace con las OCHO entradas: un motivo a 0 es información, no un hueco.
	if len(d.OmitidosPorMotivo()) != len(app.MotivosOmitido()) {
		t.Fatalf("el desglose no arrancó con todos los motivos: %d", len(d.OmitidosPorMotivo()))
	}
}
