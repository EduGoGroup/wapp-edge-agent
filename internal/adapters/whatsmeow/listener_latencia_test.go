package whatsmeow

// listener_latencia_test.go — EL CRONÓMETRO DEL HANDLER (Plan 051 Ola 3 · T3.13).
//
// Lo que se custodia aquí es que TODOS los caminos de salida de onMessage quedan medidos, y que cada uno
// cae en la SERIE que le toca. Es la pieza de la que cuelga el criterio de cierre de la ola —«handler
// < 50 ms p99», INV-051.2—: un camino sin medir es un trozo del handler que el número no ve.

import (
	"context"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app/latencia"
)

// TestOnMessage_Latencia_CadaCaminoEnSuSerie: tres mensajes por los tres caminos que existen —uno que
// encola y dos que descartan— y cada uno tiene que aparecer en su serie y solo en la suya.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - quitar el `defer` del cronómetro (o mover el `Observar` detrás del `return` de una rama) ⇒ ese camino
//     deja de contarse: encolado.N o descartado.N bajan.
//   - no reasignar `camino` en la rama de la ventana (o en la del eco propio) ⇒ el descarte se cuenta como
//     encolado: encolado.N = 2 o 3 y descartado.N baja. Es la MUTACIÓN INVERSA, la que infla la población
//     buena con muestras de microsegundos y diluye el p99 exactamente cuando hay ráfaga.
//   - devolver antes de instalar el cronómetro ⇒ N = 0 en las dos series.
func TestOnMessage_Latencia_CadaCaminoEnSuSerie(t *testing.T) {
	h := latencia.Nuevo()
	cola := &spyCola{calls: &callLog{}}
	l := listenerConCola(cola, WithLatencia(h))
	// Sello = ahora ⇒ umbral = ahora − margen; el mensaje de hace 6 h cae fuera de la ventana (ADR-0037).
	l.SetConnectSeal(func() time.Time { return time.Now() })

	// 1 · Normal: entra, se anota en la cola. Serie ENCOLADO.
	l.handleEvent(context.Background(), liveMessage("MSG-OK", "quiero dos empanadas"))

	// 2 · Eco propio: se descarta en la puerta. Serie DESCARTADO.
	eco := liveMessage("MSG-ECO", "esto lo mandé yo")
	eco.Info.IsFromMe = true
	l.handleEvent(context.Background(), eco)

	// 3 · Fuera de la ventana temporal: se descarta. Serie DESCARTADO.
	viejo := liveMessage("MSG-VIEJO", "mensaje de la ráfaga")
	viejo.Info.Timestamp = time.Now().Add(-6 * time.Hour)
	l.handleEvent(context.Background(), viejo)

	enc := h.Snapshot(latencia.Encolado)
	desc := h.Snapshot(latencia.Descartado)
	if enc.N != 1 {
		t.Errorf("serie ENCOLADO: N = %d, se esperaba 1 (solo el mensaje normal). Un 2 o un 3 significa que "+
			"un DESCARTE se está contando como encolado, y eso diluye el p99 con muestras de microsegundos "+
			"justo cuando hay ráfaga (INV-051.2 se juzga contra esta serie)", enc.N)
	}
	if desc.N != 2 {
		t.Errorf("serie DESCARTADO: N = %d, se esperaban 2 (el eco propio y el fuera de ventana). Menos "+
			"significa que hay un camino de salida sin cronómetro: ese tiempo ata el hilo del socket igual "+
			"y el número no lo vería", desc.N)
	}
	// Testigo independiente de que los tres mensajes recorrieron lo que se cree: solo el normal deja fila.
	if len(cola.got) != 1 {
		t.Fatalf("filas anotadas = %d, se esperaba 1: el escenario no es el que el test cree (%v)",
			len(cola.got), colaWAIDs(cola.got))
	}
}

// TestOnMessage_Latencia_ElSinHoraUtilizableCuentaComoENCOLADO: el entrante con Info.Timestamp en cero se
// ADMITE por precaución (ADR-0037: ante la duda, dejar pasar) y ENCOLA, así que su tiempo es tiempo de
// encolar y no de descartar.
//
// Se prueba aparte porque es la rama que más fácil se clasifica mal: sale por un `return` propio, en medio
// de los dos descartes, y es la única salida temprana que SÍ deja fila.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: marcar `camino = latencia.Descartado` en esa rama (el error natural al
// ver un `return` temprano) ⇒ la muestra se va a la serie de descartes.
func TestOnMessage_Latencia_ElSinHoraUtilizableCuentaComoEncolado(t *testing.T) {
	h := latencia.Nuevo()
	cola := &spyCola{calls: &callLog{}}
	l := listenerConCola(cola, WithLatencia(h))

	sinHora := liveMessage("MSG-T0", "hola")
	sinHora.Info.Timestamp = time.Time{} // GetUnixTime devuelve el cero con ok=true cuando el atributo t="0"
	l.handleEvent(context.Background(), sinHora)

	if got := h.Snapshot(latencia.Encolado).N; got != 1 {
		t.Errorf("serie ENCOLADO: N = %d, se esperaba 1: el entrante sin hora utilizable SE ADMITE y encola", got)
	}
	if got := h.Snapshot(latencia.Descartado).N; got != 0 {
		t.Errorf("serie DESCARTADO: N = %d, se esperaba 0: esa rama no descarta nada, admite por precaución", got)
	}
	if len(cola.got) != 1 {
		t.Fatalf("filas anotadas = %d, se esperaba 1: el escenario no es el que el test cree", len(cola.got))
	}
}

// TestOnMessage_Latencia_UnEnqueueQuePanicaSIGUE_MIDIENDOSE: REQ-051.8 dice que un panic del Enqueue no
// tumba la escucha (hay un recover en enqueueCola). Ese camino gastó tiempo del hilo de whatsmeow —a
// menudo MÁS que el normal— así que tiene que quedar medido; si no, el peor caso real sería justo el que
// el p99 no ve.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: bajar la medición a `enqueueCola` como última sentencia de su cuerpo
// —el refactor «mídelo donde se hace el trabajo», que parece más limpio— ⇒ el panic salta esa línea y el
// camino se queda sin muestra. (Quitar el `defer` de onMessage y medir al final de su cuerpo NO lo pone
// rojo, porque el recover vive dentro de enqueueCola y onMessage sigue su curso; eso lo cazan los dos
// tests de arriba.)
func TestOnMessage_Latencia_UnEnqueueQuePanicaSigueMidiendose(t *testing.T) {
	h := latencia.Nuevo()
	cola := &spyCola{calls: &callLog{}, panicMsg: "driver muerto"}
	l := listenerConCola(cola, WithLatencia(h))

	l.handleEvent(context.Background(), liveMessage("MSG-PANIC", "quiero dos empanadas"))

	if got := h.Snapshot(latencia.Encolado).N; got != 1 {
		t.Fatalf("serie ENCOLADO: N = %d, se esperaba 1. El camino del panic gastó tiempo del hilo de "+
			"whatsmeow y el cronómetro tiene que verlo: es el `defer` lo que lo garantiza", got)
	}
}

// TestOnMessage_SinCronometro_SeComportaIgual: el campo es nil-safe a propósito. Sin WithLatencia el
// listener tiene que hacer exactamente lo de antes de T3.13 —no un pánico en el hilo del socket—, y esa es
// la red de todos los cableados que no vienen del daemon (tests incluidos).
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: quitar la guarda `h == nil` de Histograma.Observar ⇒ pánico aquí.
func TestOnMessage_SinCronometro_SeComportaIgual(t *testing.T) {
	cola := &spyCola{calls: &callLog{}}
	l := listenerConCola(cola) // sin WithLatencia

	l.handleEvent(context.Background(), liveMessage("MSG-SINMEDIR", "quiero dos empanadas"))

	if len(cola.got) != 1 {
		t.Fatalf("filas anotadas = %d, se esperaba 1: sin cronómetro el listener se comporta igual", len(cola.got))
	}
}

// TestWithLatencia_NilSeIgnora cierra la simetría con el resto de opciones del Listener: un nil no puede
// borrar un cronómetro ya cableado.
func TestWithLatencia_NilSeIgnora(t *testing.T) {
	h := latencia.Nuevo()
	cola := &spyCola{calls: &callLog{}}
	l := listenerConCola(cola, WithLatencia(h), WithLatencia(nil))

	l.handleEvent(context.Background(), liveMessage("MSG-OPT", "hola"))

	if got := h.Snapshot(latencia.Encolado).N; got != 1 {
		t.Fatalf("N = %d: un WithLatencia(nil) posterior desactivó el cronómetro ya cableado", got)
	}
}
