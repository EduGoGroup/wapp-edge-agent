package cajero

// calentamiento_test.go — LA COSTURA DEL CALENTAMIENTO (T1.7-2, Plan 044 · Ola 1.7).
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 QUÉ ES ESTE FICHERO Y QUÉ NO ES
// ─────────────────────────────────────────────────────────────────────────────
// T1.7-2 no construyó el calentamiento: lo construye T1.7-4, que es quien enseñará al Cloud a emitirlo y
// quien decidirá cómo se marca EN EL FRAME (`inference_request`). Lo que esta tarea dejó es el PUNTO DE
// EXCLUSIÓN —`app.PeticionInferencia.Calentamiento`, consultado en `Inferir` antes de que el breaker
// evalúe nada— y estos tests son los que lo mantienen vivo mientras tanto.
//
// SIN ELLOS ESTO SERÍA UNA GUARDA SOBRE UN CAMINO MUERTO: un `if` que nadie ejecuta, verde para siempre
// y roto el día que alguien lo estrene. Con ellos, la conducta que T1.7-4 va a necesitar está fijada y
// medida ANTES de que exista el emisor, así que esa tarea sólo tiene que rellenar el campo.
//
// LO QUE FIJAN, en una frase: un calentamiento se sirve EXACTAMENTE igual que cualquier otra inferencia
// —mismo aforo, mismo plazo, mismos contadores, mismo histograma— y no le enseña NADA al circuito.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/breaker"
)

// peticionDeCalentamiento es la misma petición de siempre con la marca puesta. Vive aquí y no junto a
// `peticionDe` para que el día que T1.7-4 traiga el emisor de verdad se vea de un vistazo qué era andamio.
func peticionDeCalentamiento(prompt string, plazo time.Duration) app.PeticionInferencia {
	p := peticionDe(prompt, plazo)
	p.Calentamiento = true
	return p
}

// correrCalentamientos sirve N peticiones marcadas contra un guion de latencias, con el breaker REAL.
func correrCalentamientos(t *testing.T, pasos []pasoGuion, plazo time.Duration) *Cajero {
	t.Helper()

	reloj := nuevoRelojFalso()
	c, s := servidorDe(t, Deps{
		Cola:          &colaFake{},
		Ollama:        &chateadorGuionado{reloj: reloj, pasos: pasos},
		Log:           &logCaptura{},
		Ahora:         reloj.ahora,
		MaxConcurrent: 1,
		Timeout:       plazo,
	})
	for range pasos {
		// El error se ignora igual que en correrGuion: lo que se mide es en qué estado quedan el circuito y
		// los contadores, no el desenlace de cada llamada.
		_, _ = s.Inferir(context.Background(), peticionDeCalentamiento("calienta esto", plazo))
	}
	return c
}

// TestCalentamiento_LentoNoAbreElCircuito es la mitad que importa: un calentamiento es EL MÁS LENTO de
// todos por construcción —se emite cuando no hay tráfico, con el modelo descargado y la caché de prefijo
// fría, que es exactamente su razón de ser—, así que contarlo tendría el efecto contrario al que el
// breaker existe para producir: una máquina OCIOSA se abriría el circuito sola a base de calentamientos
// lentos y rechazaría con BREAKER_OPEN la primera petición REAL que llegase. El remedio provocando la
// enfermedad.
//
// El contrapunto está abajo: las mismas cinco latencias SIN la marca sí lo abren.
func TestCalentamiento_LentoNoAbreElCircuito(t *testing.T) {
	lentas := []pasoGuion{resp(13_000), resp(13_100), resp(12_400), resp(14_878), resp(13_500)}

	c := correrCalentamientos(t, lentas, timeoutDeLaMedicion)

	if c.Circuito() != breaker.StateClosed {
		t.Errorf("cinco CALENTAMIENTOS lentos no pueden abrir el circuito, got %q", c.Circuito())
	}
	if c.Lentas() != 0 {
		t.Errorf("Lentas: got %d want 0 — un calentamiento no entra en la población con la que se juzga al "+
			"proveedor", c.Lentas())
	}
	if c.AperturasBreaker() != 0 {
		t.Errorf("AperturasBreaker: got %d want 0", c.AperturasBreaker())
	}

	// 🔴 PERO SE SIRVIÓ Y SE MIDIÓ COMO TODO LO DEMÁS. Un calentamiento invisible sería una inferencia
	// ocupando la máquina del cliente sin dejar rastro en la telemetría, que es justo el agujero que el
	// contador `abortadas` existe para tapar en el otro camino.
	if c.Servidas() != 5 {
		t.Errorf("Servidas: got %d want 5 — un calentamiento se sirve igual que cualquier otra inferencia", c.Servidas())
	}
	if got := c.inferencia.muestras(); got != 5 {
		t.Errorf("el histograma mide los calentamientos igual que el resto: muestras got %d want 5", got)
	}

	// El contrapunto, y es lo que impide que este test pase con la exclusión puesta EN TODAS PARTES: sin
	// la marca, esas mismas cinco latencias abren el circuito (es TestLentitud_CincoAciertosLentos…).
	sinMarca, _ := correrGuion(t, lentas, timeoutDeLaMedicion)
	if sinMarca.Circuito() != breaker.StateOpen {
		t.Errorf("las MISMAS latencias sin la marca de calentamiento SÍ abren el circuito, got %q — si esto "+
			"falla, la exclusión se ha comido también al tráfico real", sinMarca.Circuito())
	}
}

// TestCalentamiento_NoSaltaLaColaNiSeSaltaElPlazo fija los dos límites de la exclusión, que son los que
// impiden que «no cuenta para el breaker» se convierta en «es tráfico privilegiado»:
//
//   - PAGA EL AFORO como todos. Sin esto, un calentamiento adelantaría a una petición real contra el
//     mismo Ollama de una sola plaza, que es el solapamiento que la O0 midió como causa de que el p50 se
//     dispare.
//   - RESPETA SU PLAZO. Es el mismo `plazoDe` de siempre.
//
// Se mide con el aforo LLENO: con la única plaza tomada, un calentamiento con plazo corto tiene que
// rebotar con EDGE_SIN_CAPACIDAD igual que rebotaría una petición del Cloud.
func TestCalentamiento_NoSaltaLaColaNiSeSaltaElPlazo(t *testing.T) {
	c, s := servidorDe(t, Deps{
		Cola:          &colaFake{},
		Ollama:        &chateadorFake{},
		Log:           &logCaptura{},
		MaxConcurrent: 1,
	})

	// Se ocupa la única plaza a mano y no se suelta: es la forma barata de tener el aforo lleno sin montar
	// una inferencia concurrente que compita por el reloj.
	tomada, err := c.aforo.TomarHasta(context.Background(), time.Second)
	if !tomada {
		t.Fatalf("precondición: la primera plaza debe conseguirse, err=%v", err)
	}
	defer c.aforo.Soltar()

	_, err = s.Inferir(context.Background(), peticionDeCalentamiento("calienta esto", 10*time.Millisecond))
	if !errors.Is(err, app.ErrInferenciaSinCapacidad) {
		t.Errorf("un calentamiento con el aforo LLENO rebota por capacidad como cualquier otra petición: got %v", err)
	}
	if c.ErroresSinCapacidad() != 1 {
		t.Errorf("y se CUENTA: ErroresSinCapacidad got %d want 1", c.ErroresSinCapacidad())
	}
	if c.Servidas() != 0 {
		t.Errorf("Servidas: got %d want 0 — no llegó a llamarse al proveedor", c.Servidas())
	}
}
