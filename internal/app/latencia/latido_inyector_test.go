package latencia

// latido_inyector_test.go — EL ESTADO DEL INYECTOR EN LA LÍNEA (MP-10 Parte A).
//
// 🔴 POR QUÉ ESTE CAMPO NO PUEDE VIVIR SOLO EN EL AVISO DE ARRANQUE. Es el mismo argumento que el de
// `despachador` (latido_palanca_test.go) con un agravante propio. Allí el aviso del arranque se pierde entre
// cientos de miles de líneas y el síntoma en campo —la cola creciendo— es ambiguo. Aquí no hay ni siquiera
// síntoma: con el inyector encendido, un Edge sin un solo mensaje de cliente publica un `n=487` y un
// `p99_ms=31` PERFECTAMENTE SANOS, hechos de entrantes fabricados dentro del proceso. La línea no se
// distingue en nada de una de producción.
//
// Y esa línea es exactamente la que se pega en el journal como evidencia de INV-051.2 («handler < 50 ms
// p99»). Sin este campo, la forma más fácil de «cumplir» el criterio de la ola sería medirse a sí mismo y no
// enterarse. Este campo es lo que impide que una medición sintética se lea como un dato de producción.
//
// Que el campo VAYA SIEMPRE lo custodia `camposObligatorios` en latido_test.go. Lo que se fija aquí es lo
// otro: que su VALOR diga la verdad en los dos estados, y que el estado que cambia el significado de la
// línea se explique solo.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo. NO se han ejecutado (este
// entorno no tiene toolchain de Go): están RAZONADAS contra el código, no verificadas en verde.

import (
	"strings"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// depsConInyector arma unas Deps mínimas pero completas (histograma con muestras y cola que cuenta) donde lo
// único que cambia entre casos es la palanca del inyector. Molde: depsConPalanca.
func depsConInyector(log *logCaptura, activo bool) Deps {
	h := Nuevo()
	observarMS(h, Encolado, 12, 5)
	return Deps{
		Hist:              h,
		Cada:              5 * time.Millisecond,
		Log:               log,
		Cola:              &colaFake{p: app.ColaPendientes{Nuevo: 3, Total: 3}},
		InyectorEntrantes: activo,
	}
}

// TestLatido_SinInyectorElCampoDiceQueNo: el caso de todos los Edge en campo. El valor sano es corto y
// aburrido a propósito — la línea se emite cada minuto durante meses y no puede gastar la atención del
// lector en el estado normal.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas: sin toolchain de Go en este entorno):
//   - en estadoInyector, devolver `inyectorActivo` siempre ⇒ todos los Edge del mundo publicarían la
//     advertencia de «estos números pueden no ser reales» sobre números que sí lo son, y una advertencia que
//     sale siempre deja de leerse (que es como se pierde la de verdad).
//   - borrar el par `"inyector", estadoInyector(...)` del bloque ⇒ desaparece el campo; lo cazan también los
//     tests de `camposObligatorios`, y este deja el rojo con el nombre del campo en el mensaje.
func TestLatido_SinInyectorElCampoDiceQueNo(t *testing.T) {
	log := &logCaptura{}
	correrLatido(t, depsConInyector(log, false), 30*time.Millisecond)

	emisiones := log.latidos()
	if len(emisiones) == 0 {
		t.Fatal("el latido no emitió nada: sin emisiones este test no afirma nada y pasaría en verde por vacío")
	}
	for _, e := range emisiones {
		v, ok := e.clave("inyector")
		if !ok {
			t.Fatal("el bloque salió SIN el campo `inyector`: quien lea la línea no puede saber si los " +
				"percentiles son de tráfico real o de una tanda sintética, que es la primera pregunta antes de " +
				"pegar esa línea en el journal como evidencia de INV-051.2")
		}
		if v != inyectorInactivo {
			t.Errorf("sin inyector el campo debe decir %q y dijo %v", inyectorInactivo, v)
		}
	}
}

// TestLatido_ConElInyectorACTIVOLaLineaSeExplicaSola: el valor del estado que cambia el significado de la
// línea tiene que bastarse solo, porque la lectura de campo es un `grep … | tail -3` que se pega crudo en el
// journal. Se exige que nombre la VARIABLE (lo que hay que quitar) y la CONSECUENCIA (que hay entrantes
// sintéticos y los números pueden no ser reales); sin lo segundo, quien lea la línea puede tomarla por un
// aviso menor y publicar el p99 como si fuera de producción.
//
// Se comprueba en la emisión PERIÓDICA y en la FINAL: son las dos que el mismo grep trae, y la final es la
// que resume la sesión de medida entera — precisamente la que se cita cuando se cierra una medición.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas: sin toolchain de Go en este entorno):
//   - en estadoInyector, devolver `inyectorInactivo` siempre ⇒ la línea MIENTE justo en el caso para el que
//     este campo existe: la medición sintética se publica como si fuera tráfico de clientes.
//   - recortar la constante `inyectorActivo` a una etiqueta corta (p. ej. "si") ⇒ la línea deja de bastarse
//     sola y hay que ir al código a saber qué implica; falla la exigencia de la variable y la de la
//     consecuencia.
//   - publicar el campo solo en la emisión periódica (o solo en la final) ⇒ el bucle de abajo recorre las
//     dos y una de ellas se queda sin el campo.
func TestLatido_ConElInyectorACTIVOLaLineaSeExplicaSola(t *testing.T) {
	log := &logCaptura{}
	correrLatido(t, depsConInyector(log, true), 30*time.Millisecond)

	emisiones := log.latidos()
	if len(emisiones) < 2 {
		t.Fatalf("hacen falta al menos una emisión periódica y la final para juzgar las dos: got %d", len(emisiones))
	}
	if len(log.porEmision(emisionPeriodica)) == 0 {
		t.Error("no salió ninguna emisión periódica: es la que se lee mientras la medición corre")
	}
	if len(log.porEmision(emisionFinal)) != 1 {
		t.Error("el bloque final no salió: es el que resume la sesión de medida, y es justo el que se copia al " +
			"journal cuando se cierra una medición — el peor sitio donde perder la marca de «esto era sintético»")
	}

	for _, e := range emisiones {
		v, _ := e.clave("inyector")
		texto, ok := v.(string)
		if !ok {
			t.Fatalf("el campo `inyector` no es una cadena: got %T", v)
		}
		if !strings.Contains(texto, "ACTIVO") {
			t.Errorf("con la palanca echada el campo no marca el estado como ACTIVO: got %q", texto)
		}
		if !strings.Contains(texto, "WAPP_AGENT_INYECTOR_ENTRANTES") {
			t.Errorf("la línea no nombra la variable que hay que quitar, así que no basta para devolver el Edge "+
				"a su estado normal sin ir a buscar el código: got %q", texto)
		}
		if !strings.Contains(texto, "SINTETICOS") {
			t.Errorf("la línea no dice la CONSECUENCIA (que hay entrantes sintéticos en la cola): sin ella los "+
				"percentiles de esta misma línea se leen como si fueran de tráfico real: got %q", texto)
		}
		if !strings.Contains(texto, "pueden no ser de trafico real") {
			t.Errorf("la línea no advierte de que sus PROPIOS números pueden no ser reales, que es la única "+
				"frase que impide citar este p99 como evidencia de INV-051.2: got %q", texto)
		}
	}
}
