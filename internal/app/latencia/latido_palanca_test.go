package latencia

// latido_palanca_test.go — EL ESTADO DEL DESPACHADOR EN LA LÍNEA (Plan 051 Ola 5 · T3.17).
//
// 🔴 POR QUÉ ESTE CAMPO ES DE ESTE FICHERO Y NO DEL DE ARRANQUE. La palanca de diagnóstico deja al Edge
// recibiendo y sin entregar. El aviso del arranque explica eso UNA vez, y los logs del VPS son un fichero
// de cientos de miles de líneas: al día siguiente, quien vea la cola creciendo no tiene forma de distinguir
// una palanca olvidada de un despachador roto, de la nube caída o de un Edge saturado. Los tres se ven
// exactamente igual desde fuera. Esta línea —la que el runbook manda mirar— lo dice cada vez que se emite.
//
// Que el campo VAYA SIEMPRE lo custodia `camposObligatorios` en latido_test.go. Lo que se fija aquí es lo
// otro: que su VALOR diga la verdad en los dos estados, y que el estado malo se explique solo.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo, y todas se ejecutaron.

import (
	"strings"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// depsConPalanca arma unas Deps mínimas pero completas (histograma con muestras y cola que cuenta) donde lo
// único que cambia entre casos es la palanca.
func depsConPalanca(log *logCaptura, apagado bool) Deps {
	h := Nuevo()
	observarMS(h, Encolado, 12, 5)
	return Deps{
		Hist:               h,
		Cada:               5 * time.Millisecond,
		Log:                log,
		Cola:               &colaFake{p: app.ColaPendientes{Nuevo: 3, Total: 3}},
		DespachadorApagado: apagado,
	}
}

// TestLatido_ConLaPalancaBAJADAElCampoDiceActivo: el caso de todos los Edge en campo. El valor sano es
// corto y aburrido a propósito — la línea se emite cada minuto durante meses.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - en estadoDespachador, devolver `despachadorApagado` siempre ⇒ todos los Edge del mundo publicarían
//     una alarma falsa, y una alarma que sale siempre deja de leerse.
//   - borrar el par `"despachador", estadoDespachador(...)` del bloque ⇒ desaparece el campo (lo cazan
//     también los tests de `camposObligatorios`).
func TestLatido_ConLaPalancaBAJADAElCampoDiceActivo(t *testing.T) {
	log := &logCaptura{}
	correrLatido(t, depsConPalanca(log, false), 30*time.Millisecond)

	for _, e := range log.latidos() {
		v, ok := e.clave("despachador")
		if !ok {
			t.Fatal("el bloque salió SIN el campo `despachador`: quien lea la línea no puede saber si la " +
				"cola está drenando, que es la primera pregunta ante una cola que crece")
		}
		if v != despachadorActivo {
			t.Errorf("con la palanca bajada el campo debe decir %q y dijo %v", despachadorActivo, v)
		}
	}
}

// TestLatido_ConLaPalancaECHADALaLineaSeExplicaSola: el valor del estado malo tiene que bastarse solo,
// porque la lectura de campo es un `grep … | tail -3` que se pega crudo en el journal. Se exige que nombre
// la VARIABLE (lo que hay que quitar) y la CONSECUENCIA (que se encola y no se drena); sin lo segundo, un
// operador puede leerlo como un aviso menor y dejarlo puesto.
//
// Se comprueba en la emisión PERIÓDICA y en la FINAL: son las dos que el mismo grep trae, y la final es la
// que resume la sesión de medida entera.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - en estadoDespachador, devolver `despachadorActivo` siempre ⇒ la línea MIENTE justo en el caso que
//     este campo existe para cubrir: el Edge no entrega y el latido dice que todo va bien.
//   - recortar la constante `despachadorApagado` a una etiqueta corta (p. ej. "off") ⇒ la línea deja de
//     bastarse sola y vuelve a hacer falta ir a buscar qué significa.
func TestLatido_ConLaPalancaECHADALaLineaSeExplicaSola(t *testing.T) {
	log := &logCaptura{}
	correrLatido(t, depsConPalanca(log, true), 30*time.Millisecond)

	emisiones := log.latidos()
	if len(emisiones) < 2 {
		t.Fatalf("hacen falta al menos una emisión periódica y la final para juzgar las dos: got %d", len(emisiones))
	}
	if len(log.porEmision(emisionFinal)) != 1 {
		t.Error("el bloque final no salió: es el que resume la sesión de medida, y es donde más caro sale " +
			"no enterarse de que la palanca estaba puesta")
	}

	for _, e := range emisiones {
		v, _ := e.clave("despachador")
		texto, ok := v.(string)
		if !ok {
			t.Fatalf("el campo `despachador` no es una cadena: got %T", v)
		}
		if !strings.Contains(texto, "APAGADO") {
			t.Errorf("con la palanca echada el campo no marca el estado como APAGADO: got %q", texto)
		}
		if !strings.Contains(texto, "WAPP_AGENT_DESPACHADOR_APAGADO") {
			t.Errorf("la línea no nombra la variable que hay que quitar, así que no basta para reparar el "+
				"Edge sin ir a buscar el código: got %q", texto)
		}
		if !strings.Contains(texto, "NO se drena") {
			t.Errorf("la línea no dice la CONSECUENCIA (se encola y no se drena): sin ella se lee como un "+
				"aviso menor y la palanca se queda puesta: got %q", texto)
		}
	}
}
