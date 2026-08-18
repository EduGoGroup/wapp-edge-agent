package latencia

// latido_puerta_test.go — LOS CONTADORES DE LA PUERTA EN EL BLOQUE (Plan 051 · T1.13).
//
// 🔴 QUÉ SE CUSTODIA AQUÍ. T1.13 cambió el comportamiento del Edge —lo que no deja fila ya no se acusa, así
// que WhatsApp lo reofrece— y hasta hoy ese cambio era INOBSERVABLE: los contadores existían en el listener
// y `InboundStats()` no tenía ni un llamante de producción. Estos dos campos son la única ventana a eso, así
// que la línea sin ellos es una garantía cambiada a ciegas.

import (
	"testing"
	"time"
)

// TestLatido_LosContadoresDeLaPuerta_SalenConSuValor comprueba lo que el bloque promete: que los dos
// campos llevan el acumulado del EDGE (no el de una sesión) y que llegan hasta la línea.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - borrar el `args = append(...)` de la puerta en bloque() ⇒ desaparecen de la línea;
//   - leer `Puerta().Snapshot()` de un histograma distinto del que llenan los listeners ⇒ ceros.
func TestLatido_LosContadoresDeLaPuerta_SalenConSuValor(t *testing.T) {
	log := &logCaptura{}
	h := Nuevo()
	observarMS(h, Encolado, 1, 3)

	// Dos sesiones distintas degradando: lo que sale es la SUMA del Edge, que es la decisión de diseño
	// (ver la cabecera de puerta.go). Aquí se emula anotando sobre el mismo instrumento compartido.
	h.Puerta().AnotaEnqueueError()
	h.Puerta().AnotaEnqueueError()
	h.Puerta().AnotaEnqueuePanic()

	correrLatido(t, Deps{Hist: h, Cada: 0, Log: log}, 20*time.Millisecond)

	finales := log.porEmision(emisionFinal)
	if len(finales) != 1 {
		t.Fatalf("se esperaba 1 bloque final, hubo %d", len(finales))
	}
	casos := []struct {
		clave  string
		quiero uint64
		porque string
	}{
		{"cola_enqueue_errores", 2, "es el número de entrantes que WhatsApp tendrá que REOFRECER; sin él, " +
			"un Edge con la cola rota reintenta en bucle y la única huella son líneas por mensaje, las " +
			"repetidas en Debug"},
		{"cola_enqueue_panics", 1, "cualquier valor > 0 aquí es un DEFECTO, no una condición de campo, y es " +
			"la diferencia entre «el disco está lleno» y «hay un bug en el adaptador»"},
	}
	for _, c := range casos {
		v, ok := finales[0].clave(c.clave)
		if !ok {
			t.Errorf("el bloque no lleva %q.\n    CONSECUENCIA: %s", c.clave, c.porque)
			continue
		}
		if got, _ := v.(uint64); got != c.quiero {
			t.Errorf("%s = %d, quería %d: el bloque publica un instrumento distinto del que llenan los "+
				"listeners, así que saldría en cero con el Edge degradándose", c.clave, got, c.quiero)
		}
	}
}

// TestLatido_LosContadoresDeLaPuerta_SalenAUNQUE_SeanCeroYNoHayaCola fija la regla del cero explícito, que
// es la que hace el campo útil: un cero DICE «no se ha degradado nada en toda la vida del proceso»; un
// campo ausente solo deja la duda de si dejó de mirarse.
//
// Y afirma la segunda mitad: salen aunque NO haya contador de cola cableado. Son cosas independientes —
// estos dos los lleva el instrumento en memoria y no dependen del COUNT—, y un Edge cuyo adaptador no
// respalde el lado contador sigue teniendo derecho a ver si está reofreciendo mensajes.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): mover el append de la puerta DENTRO del `if d.Cola != nil`.
func TestLatido_LosContadoresDeLaPuerta_SalenAunqueSeanCeroYNoHayaCola(t *testing.T) {
	log := &logCaptura{}
	h := Nuevo()
	observarMS(h, Encolado, 1, 1)

	correrLatido(t, Deps{Hist: h, Cada: 0, Log: log}, 20*time.Millisecond) // SIN Cola

	finales := log.porEmision(emisionFinal)
	if len(finales) != 1 {
		t.Fatalf("se esperaba 1 bloque final, hubo %d", len(finales))
	}
	for _, clave := range []string{"cola_enqueue_errores", "cola_enqueue_panics"} {
		v, ok := finales[0].clave(clave)
		if !ok {
			t.Errorf("%q desapareció de la línea por valer cero (o por no haber contador de cola).\n"+
				"    CONSECUENCIA: un campo ausente es una DUDA («¿no pasó nada, o dejó de mirarse?») donde\n"+
				"    un cero explícito era un DATO. Y peor: el día que empiece a pasar algo, el campo\n"+
				"    aparecerá de la nada y quien tenga un grep por columnas fijas lo verá descuadrado.", clave)
			continue
		}
		if got, _ := v.(uint64); got != 0 {
			t.Errorf("%s = %d, quería 0: nadie degradó nada en este test", clave, got)
		}
	}
}
