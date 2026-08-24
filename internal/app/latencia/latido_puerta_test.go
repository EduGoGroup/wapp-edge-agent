package latencia

// latido_puerta_test.go — LOS CONTADORES DE LA PUERTA EN EL BLOQUE (Plan 051 · T1.13).
//
// 🔴 QUÉ SE CUSTODIA AQUÍ. T1.13 cambió el comportamiento del Edge —lo que no deja fila ya no se acusa, así
// que WhatsApp lo reofrece— y hasta hoy ese cambio era INOBSERVABLE: los contadores existían en el listener
// y `InboundStats()` no tenía ni un llamante de producción. Estos campos son la única ventana a eso, así
// que la línea sin ellos es una garantía cambiada a ciegas.
//
// El Plan 046 · T2.3 sumó el TERCERO —`descartes_perfil_pasivo`— por la misma lección y antes de que doliera:
// el corte de la sesión pasiva no deja fila, no sube al cable y acusa igual que si hubiera entregado, así que
// sin este campo un filtro roto (0 con tráfico) o un filtro que corta de más son invisibles.

import (
	"testing"
	"time"
)

// TestLatido_LosContadoresDeLaPuerta_SalenConSuValor comprueba lo que el bloque promete: que los cuatro
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

	// Plan 046 · T2.3 — el tercer contador de la puerta, con un valor DISTINTO de los otros dos a propósito:
	// con todo a 1 (o a 2), un cruce de campos en `bloque()` —publicar los panics en el hueco del pasivo, o
	// al revés— pasaría en verde y el operador leería un defecto donde hay una sesión callada, o al revés.
	for i := 0; i < 4; i++ {
		h.Puerta().AnotaDescartePasivo()
	}

	// Plan 044 · T1.5-3 — el cuarto contador de la puerta, otra vez con un valor DISTINTO de los tres
	// anteriores y por el mismo motivo: es el único cruce de campos que un test puede cazar sin leer el
	// código. Confundir `descartes_grupo` con `descartes_perfil_pasivo` en la línea haría creer que hay
	// sesiones calladas donde lo que hay es tráfico de grupos, que es una conclusión operativa distinta.
	for i := 0; i < 7; i++ {
		h.Puerta().AnotaDescarteGrupo()
	}

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
		{"descartes_perfil_pasivo", 4, "es la ÚNICA huella de un filtro que no deja fila, no sube al cable y " +
			"ACUSA a WhatsApp igual que si hubiera entregado: sin este campo, «la sesión pasiva está " +
			"filtrando» y «a esa sesión no le escribe nadie» son indistinguibles desde fuera"},
		{"descartes_grupo", 7, "desde REQ-36 el entrante de GRUPO no deja fila, no sube al cable y ACUSA " +
			"igual que si se hubiera entregado: este número es la única prueba de que ese tráfico existe, " +
			"y sin él la caída de `cola_pendientes` se lee como «entra menos tráfico»"},
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
	for _, clave := range []string{"cola_enqueue_errores", "cola_enqueue_panics", "descartes_perfil_pasivo", "descartes_grupo"} {
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
