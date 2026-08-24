package health

// collector_inferencia_test.go — LA PRESENCIA ES LA SEMÁNTICA (Plan 044 · Ola 1.7 · T1.7-5).
//
// El contrato del heartbeat estrena aquí una distinción que `intent_p50_ms` (campo 10) NO puede hacer:
// `inference_prefill` e `inference_generation` son SUB-MENSAJES, y su AUSENCIA significa «no medible».
// `intent_p50_ms` gasta el valor 0 en las dos cosas —«no lo mido» y «lo mido y sale 0»— y ha tenido que
// advertirlo por escrito para que nadie lo lea como «instantáneo».
//
// 🔴 LO QUE ESTOS TESTS IMPIDEN es que esa distinción se pierda EN EL COLECTOR, que es donde se decide.
// El error natural es decidir por el p50 (`if p50 > 0`): con eso, la máquina que va RÁPIDA —p50 por
// debajo de la resolución de la rejilla— desaparecería del heartbeat como si no midiera nada, y sólo se
// verían las lentas. Un instrumento que sólo reporta cuando va mal no es un instrumento.

import (
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// parteConFases es un parte fresco con el reparto de la inferencia puesto.
func parteConFases(now time.Time, prefillP50, prefillN, generacionP50, generacionN int64) parteFijo {
	return parteFijo{ok: true, p: app.ParteWorker{
		TS:                 now.Add(-time.Second),
		Circuito:           "closed",
		Taskset:            "disjunta",
		P50ms:              8100,
		PrefillP50ms:       prefillP50,
		PrefillMuestras:    prefillN,
		GeneracionP50ms:    generacionP50,
		GeneracionMuestras: generacionN,
		PorRegimen:         map[string]int64{"frio": 3, "templado": 0, "caliente": 41},
		PorClase:           map[string]int64{"interactivo": 44, "lote": 0},
	}}
}

// TestCollector_FasesFrescas_LleganAtadasASuMuestra: el camino feliz, con el cuantil y su `n` juntos.
func TestCollector_FasesFrescas_LleganAtadasASuMuestra(t *testing.T) {
	now := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	c := NewCollector(regConSesion("S"), nil, parteConFases(now, 1200, 44, 6500, 44), "v", now,
		WithClock(func() time.Time { return now }))

	r, ok := c.Collect(t.Context(), "S")
	if !ok {
		t.Fatal("Collect: ok=false con la sesión viva")
	}
	if r.InferPrefill == nil {
		t.Fatal("InferPrefill nil con 44 muestras medidas: el heartbeat diría «no medible»")
	}
	if r.InferPrefill.P50Ms != 1200 || r.InferPrefill.Muestras != 44 {
		t.Errorf("InferPrefill: got %+v want {1200 44}", *r.InferPrefill)
	}
	if r.InferGeneracion == nil || r.InferGeneracion.Muestras != 44 {
		t.Errorf("InferGeneracion: got %+v", r.InferGeneracion)
	}
	// Los mapas se pasan TAL CUAL: ni se filtran claves ni se completan. Quien conoce las categorías (y sus
	// umbrales, que son política suya) es el cajero.
	if got := r.InferPorRegimen["caliente"]; got != 41 {
		t.Errorf("InferPorRegimen[caliente]: got %d want 41", got)
	}
	if got, existe := r.InferPorRegimen["templado"]; !existe || got != 0 {
		t.Errorf("la clave `templado` a 0 tiene que SOBREVIVIR: es el dato que dice que los umbrales "+
			"parten bien la población; got existe=%t valor=%d", existe, got)
	}
	if got := r.InferPorClase["interactivo"]; got != 44 {
		t.Errorf("InferPorClase[interactivo]: got %d want 44", got)
	}
}

// TestCollector_UnP50DeCeroConMuestrasSIGUESIENDOMEDIBLE es el test que caza la decisión equivocada.
//
// 🔴 Un p50 de 0 CON muestras significa «todas por debajo de la resolución de la rejilla», que es una
// máquina que va de maravilla; un p50 de 0 SIN muestras significa «no lo mido». Decidir la presencia por
// el p50 (`if p50 > 0`) los colapsa, y el efecto práctico es que las máquinas rápidas desaparecen de la
// telemetría. QUIEN DECIDE SON LAS MUESTRAS.
func TestCollector_UnP50DeCeroConMuestrasSIGUESIENDOMEDIBLE(t *testing.T) {
	now := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	c := NewCollector(regConSesion("S"), nil, parteConFases(now, 0, 500, 0, 500), "v", now,
		WithClock(func() time.Time { return now }))

	r, _ := c.Collect(t.Context(), "S")
	if r.InferPrefill == nil {
		t.Fatal("p50=0 con 500 muestras se publicó como NO MEDIBLE: una máquina rápida no puede " +
			"desaparecer del heartbeat")
	}
	if r.InferPrefill.Muestras != 500 {
		t.Errorf("Muestras: got %d want 500", r.InferPrefill.Muestras)
	}
}

// TestCollector_SinMuestras_NoSePublicaElCuantil es la mitad opuesta: sin población no hay cuantil que
// publicar, y publicar un cero se leería como «instantáneo».
func TestCollector_SinMuestras_NoSePublicaElCuantil(t *testing.T) {
	now := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	c := NewCollector(regConSesion("S"), nil, parteConFases(now, 0, 0, 0, 0), "v", now,
		WithClock(func() time.Time { return now }))

	r, _ := c.Collect(t.Context(), "S")
	if r.InferPrefill != nil {
		t.Errorf("InferPrefill: got %+v want nil (cero muestras ⇒ no medible)", *r.InferPrefill)
	}
	if r.InferGeneracion != nil {
		t.Errorf("InferGeneracion: got %+v want nil", *r.InferGeneracion)
	}
}

// TestCollector_ParteRancio_ElRepartoCaeCONLosDemas: la regla de rancidez alcanza a los campos nuevos.
//
// 🔴 ESTE ES EL TEST QUE JUSTIFICA QUE parteDelWorker DEVUELVA EL PARTE ENTERO. Con la firma anterior
// —tres valores de retorno y cuatro `return` a cero repartidos por la función— un campo nuevo heredaba la
// regla sólo si quien lo añadía se acordaba de ponerlo a su cero en los cuatro sitios. Devolviendo el
// parte y un bool, el `false` lo tira TODO y la regla ya no depende de la memoria de nadie.
//
// Un parte de hace media hora publicando `caliente: 41` sería exactamente la señal INVENTADA que la regla
// de rancidez existe para impedir, sólo que ahora con un número más creíble.
func TestCollector_ParteRancio_ElRepartoCaeCONLosDemas(t *testing.T) {
	now := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	viejo := parteConFases(now, 1200, 44, 6500, 44)
	viejo.p.TS = now.Add(-app.ParteRancio - time.Second) // justo fuera de la ventana

	c := NewCollector(regConSesion("S"), nil, viejo, "v", now, WithClock(func() time.Time { return now }))

	r, _ := c.Collect(t.Context(), "S")
	if r.InferPrefill != nil || r.InferGeneracion != nil {
		t.Errorf("un parte RANCIO publicó cuantiles: prefill=%+v generacion=%+v",
			r.InferPrefill, r.InferGeneracion)
	}
	if r.InferPorRegimen != nil || r.InferPorClase != nil {
		t.Errorf("un parte RANCIO publicó repartos: regimen=%v clase=%v", r.InferPorRegimen, r.InferPorClase)
	}
	// Premisa: los tres de T4.3 también cayeron. Si esto pasara, el test de arriba estaría midiendo otra
	// cosa (un parte que no llegó, por ejemplo).
	if r.IntentCircuit != "" {
		t.Errorf("premisa rota: con el parte rancio, IntentCircuit debía quedar vacío; got %q", r.IntentCircuit)
	}
}
