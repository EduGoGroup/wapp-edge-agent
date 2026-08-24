package cajero

// Tests del cajero con N COLAS — una por instalación (Plan 051 Ola 4 · T4.1).
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 EL ROUND-ROBIN DEL CLAIM MURIÓ EL 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-2, ADR-0045 §8)
// ─────────────────────────────────────────────────────────────────────────────
// El fichero conserva su nombre pero ya no queda un solo test del REPARTO: el cursor y `vaciasSeguidas`
// existían para repartir EL CLAIM con equidad entre N instalaciones, y sin claim no hay nada que
// repartir. Los tres tests que lo medían (la parlanchina contra la callada, el cursor que avanza con la
// cola vacía y la no-regresión de una sola cola) se borraron por falta de sujeto, no por molestar: sus
// mutaciones apuntaban a una línea de `bucle()` que ya no existe.
//
// LA EQUIDAD DE T4.1 SIGUE SATISFECHA, y ahora POR CONSTRUCCIÓN en vez de por un cursor: el parte se
// publica en TODAS las colas en cada tick y el barrido las recorre TODAS dentro del mismo tick. Eso es
// justo lo que cubren los dos tests que quedan aquí, más el que impide la tentación de fondo de esta
// tarea: darle un aforo a cada cola.
//
// Los dobles (colaFake, chateadorVigilante, breakerFake, logCaptura) y las ayudas (correr, servidorDe,
// inferirEnParalelo) se REUTILIZAN de cajero_test.go: son del mismo paquete y duplicarlos sería tener
// dos colas falsas que pueden divergir.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// ─────────────────────────────────────────────────────────────────────────────
// EL AFORO SIGUE SIENDO UNO POR MÁQUINA, no uno por cola
// ─────────────────────────────────────────────────────────────────────────────

// TestRoundRobin_ElSemaforoNoSeMultiplicaPorCola es el guardarraíl de la decisión de diseño de T4.1: el
// aforo (y el breaker) son UNO POR PROCESO porque protegen a Ollama, que es uno por máquina.
//
// 🔴 SE REESCRIBIÓ CONTRA EL AFORO REAL EN T1.6-2, Y EL FALLO QUE CIERRA ES MÁS FÁCIL DE COLAR QUE ANTES.
// Antes la simultaneidad la acotaba el bucle, que era el único que tomaba plaza; hoy el aforo tiene DOS
// consumidores y el segundo —el servidor que atiende al Cloud— se construye desde `ServidorInferencia()`,
// que es exactamente el sitio donde alguien podría escribir `NuevoAforo(1)` en vez de reusar `c.aforo`.
// Con tres instalaciones, un aforo por cola daría TRES inferencias simultáneas contra la misma instancia
// —el solapamiento que la O0 midió como causa de que la p50 se dispare— y en un test sin Ollama real todo
// seguiría en verde. Por eso se mide la SIMULTANEIDAD, no el resultado.
func TestRoundRobin_ElSemaforoNoSeMultiplicaPorCola(t *testing.T) {
	const (
		nColas     = 3
		enParalelo = 6 // dos por instalación: más peticiones que colas, para que la cola de espera exista
	)

	vigilante := &chateadorVigilante{}
	c, s := servidorDe(t, Deps{
		Colas: []ColaNombrada{
			{Nombre: "inst-a", Cola: &colaFake{}},
			{Nombre: "inst-b", Cola: &colaFake{}},
			{Nombre: "inst-c", Cola: &colaFake{}},
		},
		Ollama:        vigilante,
		Breaker:       nuevoBreakerFake(),
		Log:           &logCaptura{},
		MaxConcurrent: 1,
	})

	if c.Colas() != nColas {
		t.Fatalf("el test necesita las %d colas montadas, hay %d", nColas, c.Colas())
	}
	if plazas := c.Aforo().Plazas(); plazas != 1 {
		t.Fatalf("el aforo del PROCESO es de una plaza con %d colas, tiene %d", nColas, plazas)
	}

	inferirEnParalelo(context.Background(), t, s, enParalelo)

	if n := vigilante.maxSimultaneas(); n != 1 {
		t.Errorf("con MaxConcurrent=1 y %d colas NUNCA puede haber dos inferencias solapadas, hubo %d", nColas, n)
	}
	if n := vigilante.inferencias(); n != enParalelo {
		t.Errorf("las %d peticiones deben servirse todas, se sirvieron %d", enParalelo, n)
	}
	if c.Servidas() != enParalelo {
		t.Errorf("Servidas: got %d want %d", c.Servidas(), enParalelo)
	}

	// 🔴 LA IDENTIDAD DEL AFORO, que es lo que el pico de simultaneidad NO puede demostrar: N aforos de una
	// plaza serializarían cada uno por su lado y el vigilante seguiría viendo 1. Ocupando la ÚNICA plaza
	// del PROCESO desde fuera, cualquier petición debe rendirse — con tres colas montadas y ninguna
	// implicada en la decisión.
	if !c.Aforo().Tomar(context.Background()) {
		t.Fatal("el aforo del proceso está libre: Tomar tenía que conseguir la plaza")
	}
	_, err := s.Inferir(context.Background(), peticionDe("clasifica esto", 50*time.Millisecond))
	c.Aforo().Soltar()
	if !errors.Is(err, app.ErrInferenciaSinCapacidad) {
		t.Fatalf("con la única plaza de la MÁQUINA ocupada ninguna instalación puede colarse: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// El BARRIDO de leases con N colas
// ─────────────────────────────────────────────────────────────────────────────

// TestBarridoDeLeases_RecorreTodasLasColasEnElMismoTick fija la otra decisión de T4.1: el barrido itera
// las N colas DENTRO del mismo tick, con un solo ticker, y suma en un agregado que sigue existiendo más
// un desglose por cola.
//
// La mutación que caza: dejar el barrido apuntando a una sola cola (por ejemplo, la primera). Las filas
// que un cajero muerto dejó en `tomado` en las OTRAS instalaciones no volverían nunca a `nuevo`, y el
// síntoma en campo sería una cola que deja de avanzar sin un solo error.
func TestBarridoDeLeases_RecorreTodasLasColasEnElMismoTick(t *testing.T) {
	primera := &colaFake{rescatables: 3}
	segunda := &colaFake{rescatables: 2}
	log := &logCaptura{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := New(Deps{
		Colas: []ColaNombrada{
			{Nombre: "instalacion-a", Cola: primera},
			{Nombre: "instalacion-b", Cola: segunda},
		},
		Ollama:      &chateadorFake{},
		Breaker:     nuevoBreakerFake(),
		Despertador: NewPollFijo(5 * time.Millisecond),
		Log:         log,
		Lease:       10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	hecho := make(chan error, 1)
	go func() { hecho <- c.Run(ctx) }()

	plazo := time.After(3 * time.Second)
	for c.Rescatados() < 5 {
		select {
		case <-plazo:
			cancel()
			t.Fatalf("el barrido no rescató las 5 filas de las DOS colas en 3 s (barridos: a=%d b=%d)",
				primera.barridosN(), segunda.barridosN())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	cancel()

	select {
	case err := <-hecho:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run no terminó tras cancelar (goroutine del barrido colgada)")
	}

	// El DESGLOSE: el agregado dice «alguien murió a mitad», esto dice CUÁL instalación.
	desglose := c.RescatadosPorCola()
	if desglose["instalacion-a"] != 3 {
		t.Errorf("la instalación a rescató 3 filas, el desglose dice %d", desglose["instalacion-a"])
	}
	if desglose["instalacion-b"] != 2 {
		t.Errorf("la instalación b rescató 2 filas, el desglose dice %d", desglose["instalacion-b"])
	}

	// Y el log nombra la cola: con cinco instalaciones, un Warn sin `cola` no es un diagnóstico.
	e, ok := log.buscar("leases vencidos rescatados")
	if !ok {
		t.Fatal("el barrido con n>0 debe dejar una línea de log")
	}
	if e.nivel != "warn" {
		t.Errorf("rescatar filas se avisa en Warn (alguien murió a mitad), got %q", e.nivel)
	}
	if !strings.Contains(log.texto(), "instalacion-a") {
		t.Error("el aviso del barrido debe nombrar la instalación afectada")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Construcción: las dos formas de pasar colas, y el nil que no puede pasar
// ─────────────────────────────────────────────────────────────────────────────

func TestNew_ListaDeColas(t *testing.T) {
	t.Run("sin Cola ni Colas no se construye", func(t *testing.T) {
		if _, err := New(Deps{Ollama: &chateadorFake{}, Log: &logCaptura{}}); err == nil {
			t.Fatal("un cajero sin ninguna cola no puede existir")
		}
	})

	t.Run("una cola nil DENTRO de la lista falla en el arranque, no en el primer uso", func(t *testing.T) {
		// El caso real: el cableado abre N data_dir's y uno no pudo construir su Store. Sin esta guarda el
		// pánico llegaría minutos después, la primera vez que alguien tocara esa posición — hoy, el barrido
		// de leases o la publicación del parte.
		_, err := New(Deps{
			Colas: []ColaNombrada{
				{Nombre: "buena", Cola: &colaFake{}},
				{Nombre: "/srv/wapp/rota", Cola: nil},
			},
			Ollama: &chateadorFake{},
			Log:    &logCaptura{},
		})
		if err == nil {
			t.Fatal("una cola nil en la lista debe impedir el arranque")
		}
		if !strings.Contains(err.Error(), "/srv/wapp/rota") {
			t.Errorf("el error debe NOMBRAR la cola rota (un operador con 5 instalaciones necesita saber cuál): %v", err)
		}
	})

	t.Run("Colas manda sobre el atajo Cola", func(t *testing.T) {
		atajo := &colaFake{}
		lista := &colaFake{}
		c, err := New(Deps{
			Cola:   atajo,
			Colas:  []ColaNombrada{{Nombre: "la-de-la-lista", Cola: lista}},
			Ollama: &chateadorFake{},
			Log:    &logCaptura{},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if c.Colas() != 1 {
			t.Fatalf("con las dos vías puestas manda la lista: se esperaba 1 cola, hay %d", c.Colas())
		}
		if _, ok := c.RescatadosPorCola()["la-de-la-lista"]; !ok {
			t.Errorf("la cola montada debe ser la de la LISTA, no la del atajo: %v", c.RescatadosPorCola())
		}
	})

	t.Run("una cola sin nombre recibe uno que la distinga", func(t *testing.T) {
		c, err := New(Deps{Cola: &colaFake{}, Ollama: &chateadorFake{}, Log: &logCaptura{}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if _, ok := c.RescatadosPorCola()["cola-0"]; !ok {
			t.Errorf("sin etiqueta el nombre cae al índice, para que dos líneas de log no salgan idénticas: %v",
				c.RescatadosPorCola())
		}
	})
}
