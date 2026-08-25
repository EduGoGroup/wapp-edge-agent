package despachador

// despertador_aviso_test.go — EL DESPERTADOR POR AVISO, PIEZA A PIEZA (Plan 044 · Ola 1.8 · T1.8-7).
//
// QUÉ CUBRE ESTE FICHERO Y QUÉ NO. Aquí se interroga SÓLO a `AvisoConRespaldo`: sus tres salidas (aviso,
// respaldo, cancelación) y sus dos defaults. Que el circuito COMPLETO —listener → canal → bucle → sink—
// entregue en milisegundos contra SQLite real es otra afirmación, y vive donde se puede montar el circuito
// entero: `internal/adapters/whatsmeow/circuito_aviso_test.go` (los criterios (a)–(d) de la tarea).
//
// LOS PLAZOS DE AQUÍ SON DEL TEST, NO DE PRODUCCIÓN, y a propósito: un test que asertara contra
// `DefaultRespaldoMS` pasaría con cualquier valor de esa constante —incluido uno que la desactivara—, que
// es la definición de test tautológico. Lo que se prueba es la CONDUCTA del mecanismo; el número de campo
// y su porqué se deciden y se justifican en despertador.go.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo, y todas se ejecutaron.

import (
	"context"
	"testing"
	"time"
)

// guardiaAviso es la guardia ANTI-CUELGUE del fichero: lo que se espera aquí llega en microsegundos, y
// este plazo sólo existe para que un `Esperar` que no vuelve muera como rojo en vez de colgar el CI.
const guardiaAviso = 3 * time.Second

// esperarEnGoroutine corre `Esperar` aparte y publica su error por un canal, para que el test pueda
// afirmar «volvió» y «no volvió» sin bloquearse en ninguno de los dos casos.
func esperarEnGoroutine(ctx context.Context, d Despertador) <-chan error {
	out := make(chan error, 1)
	go func() { out <- d.Esperar(ctx) }()
	return out
}

// TestAvisoConRespaldo_DespiertaPorElAvisoSinTocarElRespaldo es la mitad que compra la tarea: con el
// respaldo desactivado a todos los efectos (una hora), un aviso basta para que el bucle vuelva a mirar.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): quitar el `case <-a.aviso` de `Esperar` ⇒ el despertador se
// convierte en un poll de una hora y el test muere en la guardia.
func TestAvisoConRespaldo_DespiertaPorElAvisoSinTocarElRespaldo(t *testing.T) {
	canal := make(chan struct{}, 1)
	d := NewAvisoConRespaldo(canal, time.Hour)

	vuelto := esperarEnGoroutine(context.Background(), d)
	canal <- struct{}{}

	select {
	case err := <-vuelto:
		if err != nil {
			t.Fatalf("Esperar devolvió %v tras un aviso; nil significa «vuelve a mirar la cola» y es lo único "+
				"que el bucle sabe interpretar como trabajo", err)
		}
	case <-time.After(guardiaAviso):
		t.Fatal("el aviso NO despertó al despertador con el respaldo en una hora.\n" +
			"    CONSECUENCIA: el entrante espera al respaldo (5 s en producción) en vez de a su propio\n" +
			"    INSERT — la tarea entera, que va de 500 ms a milisegundos, quedaría deshecha.")
	}
}

// TestAvisoConRespaldo_SinAvisoDespiertaPorElRespaldo es la otra mitad, y la que sostiene que el `INSERT`
// siga siendo la verdad durable: lo que no pasa por el canal —`cmd/colaseed`, cualquier escritor externo—
// sale igual, sólo que más tarde.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): quitar el `case <-t.C` de `Esperar` ⇒ sin aviso no despierta
// NADIE y las filas escritas desde fuera del proceso no se entregan jamás.
func TestAvisoConRespaldo_SinAvisoDespiertaPorElRespaldo(t *testing.T) {
	const respaldo = 40 * time.Millisecond

	canal := make(chan struct{}, 1) // existe, pero nadie lo toca
	d := NewAvisoConRespaldo(canal, respaldo)

	inicio := time.Now()
	select {
	case err := <-esperarEnGoroutine(context.Background(), d):
		if err != nil {
			t.Fatalf("Esperar devolvió %v al vencer el respaldo; se esperaba nil", err)
		}
	case <-time.After(guardiaAviso):
		t.Fatal("el respaldo no venció: el despertador se quedó esperando un aviso que nadie iba a mandar")
	}
	if transcurrido := time.Since(inicio); transcurrido < respaldo {
		t.Errorf("el respaldo de %s venció a los %s: está volviendo ANTES de tiempo.\n"+
			"    CONSECUENCIA: el bucle mira la cola más a menudo de lo pactado, que con una sesión por caja\n"+
			"    es ruido y con decenas es carga inventada contra SQLite.", respaldo, transcurrido)
	}
}

// TestAvisoConRespaldo_SaleConElErrorDelContexto custodia la señal de parada: el bucle usa el ERROR del
// contexto, no un bool, para saber que la sesión se está apagando (ver la doc de Despertador y de Run).
//
// Y custodia además que la salida sea INMEDIATA y no al vencer el respaldo, que es la mitad del criterio
// «ninguna entrega ocurre después de cancelar el ctx»: con el respaldo de producción en 5 s, esperar al
// tick pendiente convertiría cada apagado en cinco segundos por sesión.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - quitar el `case <-ctx.Done()` ⇒ el apagado espera al respaldo entero y falla el plazo de abajo;
//   - devolver `nil` en esa rama en vez de `ctx.Err()` ⇒ el bucle lee «hay trabajo» donde había una
//     cancelación y da una vuelta de más antes de parar.
func TestAvisoConRespaldo_SaleConElErrorDelContexto(t *testing.T) {
	d := NewAvisoConRespaldo(make(chan struct{}, 1), time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	vuelto := esperarEnGoroutine(ctx, d)
	cancel()

	select {
	case err := <-vuelto:
		if err == nil {
			t.Fatal("Esperar devolvió nil tras cancelar el ctx: el bucle lo leería como «vuelve a mirar la " +
				"cola» y daría una vuelta más — con una entrega dentro— después del apagado")
		}
	case <-time.After(guardiaAviso):
		t.Fatal("Esperar no salió al cancelar el ctx: el apagado de la sesión tendría que esperar al respaldo")
	}
}

// TestNewAvisoConRespaldo_IntervaloNoPositivoNoSeAcepta cubre el mismo guardián que `NewPollFijo` y por la
// misma razón: un intervalo de 0 convertiría el bucle en una espera activa que quemaría un core POR SESIÓN
// con la cola vacía.
//
// SE AFIRMA «> 0» Y NO «== DefaultRespaldoMS», y la diferencia es deliberada: lo que hay que impedir es el
// cero, no fijar el número. Un test que comparase contra la constante pasaría igual si mañana alguien la
// pusiera a 0, que es exactamente el fallo que este test existe para cazar.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): borrar el `if respaldo <= 0` del constructor ⇒ el caso del 0
// y el del negativo pasan tal cual y el `time.NewTimer` vence de inmediato, una y otra vez.
func TestNewAvisoConRespaldo_IntervaloNoPositivoNoSeAcepta(t *testing.T) {
	for _, malo := range []time.Duration{0, -1, -time.Hour} {
		d := NewAvisoConRespaldo(nil, malo)
		if d.Respaldo() <= 0 {
			t.Errorf("con respaldo=%s el despertador quedó en %s.\n"+
				"    CONSECUENCIA: `Esperar` vuelve de inmediato para siempre y el bucle pasa a ser una espera\n"+
				"    activa contra SQLite, una por sesión viva.", malo, d.Respaldo())
		}
	}
}

// TestAvisoConRespaldo_CanalNilDegradaAPollPuro deja escrito —y ejercitado— lo que pasa con un canal nil:
// un canal nil nunca está listo en un `select`, así que este despertador se convierte en un poll del
// intervalo del respaldo. No entra en pánico y no se cuelga.
//
// 🔴 QUE NO SEA UN ERROR NO LO CONVIERTE EN UN CASO SANO, y por eso está escrito aquí: en producción sería
// una DEGRADACIÓN SILENCIOSA (de mirar cada 500 ms a mirar cada 5 s, diez veces peor que antes de la
// tarea). Quien impide que ocurra no es este constructor sino el cableado: `sessionmgr.startDespachador`
// deja el `Despertador` a nil cuando la sesión no tiene canal, y entonces manda el `PollFijo(500 ms)` de
// `New`. Este test fija la conducta para que el día que alguien mueva esa decisión sepa qué está moviendo.
func TestAvisoConRespaldo_CanalNilDegradaAPollPuro(t *testing.T) {
	d := NewAvisoConRespaldo(nil, 40*time.Millisecond)

	select {
	case err := <-esperarEnGoroutine(context.Background(), d):
		if err != nil {
			t.Fatalf("Esperar devolvió %v con el canal nil; se esperaba nil al vencer el respaldo", err)
		}
	case <-time.After(guardiaAviso):
		t.Fatal("con el canal nil el despertador se colgó: un canal nil nunca está listo, así que el " +
			"`case <-t.C` del respaldo es lo ÚNICO que puede sacarlo de ahí")
	}
}

// TestNewSinDespertador_SIGUE_SIENDO_ElPollDeQuinientos es el test que impide que esta tarea se cuele donde
// no debe. `DefaultPollMS` NO SE TOCA —el 051 lo prohíbe explícitamente
// (HANDOFF-CLI-O3-2026-08-17.md:396)— y el default de `New` tiene que seguir siendo el poll de siempre para
// todo el que no tenga aviso cableado: los tests del paquete, los cableados que no vienen del daemon y
// cualquier sesión que no pase por `arm`.
//
// 🔴 EL PLAZO SE ESCRIBE COMO LITERAL (`500 * time.Millisecond`) Y NO COMO `DefaultPollMS`, Y ES EL PUNTO
// DEL TEST: comparar contra la constante lo haría pasar con cualquier valor que alguien le pusiera, que es
// justo lo que hay que impedir. Si alguien mueve ese número, ESTE test es el que tiene que dolerle.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - `DefaultPollMS = 5000` (o cualquier otro valor) ⇒ cae la comparación con el literal;
//   - hacer que el default de `New` sea `NewAvisoConRespaldo(nil, …)` ⇒ cae la comprobación de tipo, y con
//     ella la promesa de que un despachador sin aviso se comporta exactamente como antes de T1.8-7.
func TestNewSinDespertador_SIGUE_SIENDO_ElPollDeQuinientos(t *testing.T) {
	d, err := New(Deps{Cola: nuevaColaFake(), Sink: nuevoSinkFake(), SessionID: "s"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	poll, ok := d.despertador.(*PollFijo)
	if !ok {
		t.Fatalf("el default de New es %T y tiene que ser *PollFijo.\n"+
			"    CONSECUENCIA: un despachador SIN aviso cableado dejaría de comportarse como antes de\n"+
			"    T1.8-7. El aviso se AÑADE al mecanismo; no sustituye el default de nadie.", d.despertador)
	}
	if got := poll.Intervalo(); got != 500*time.Millisecond {
		t.Errorf("el poll por defecto es %s y tiene que ser 500 ms.\n"+
			"    CONSECUENCIA: T1.8-7 prometió que el poll no sube, no baja y no se toca — sólo cambia de\n"+
			"    papel, de disparador a respaldo. Este número es esa promesa.", got)
	}
}
