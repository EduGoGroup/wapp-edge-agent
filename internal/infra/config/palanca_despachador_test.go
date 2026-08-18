package config

// palanca_despachador_test.go — EL GUARDARRAÍL DE LA PALANCA DE DIAGNÓSTICO (Plan 051 Ola 5 · T3.17).
//
// 🔴 QUÉ SE CUSTODIA. WAPP_AGENT_DESPACHADOR_APAGADO es la única variable del Edge cuyo valor equivocado
// deja al daemon RECIBIENDO Y SIN ENTREGAR: la cola crece, `/v1/health` sigue en verde, el socket de
// WhatsApp sigue vivo y ni un mensaje llega a la nube. Por eso su contrato no es «lee un booleano», es
// «TODO camino que no sea un true explícito tiene que dejar el despachador ACTIVO».
//
// Los cuatro caminos de abajo son los cuatro que un operador recorre de verdad en un VPS: no poner la
// variable, dejarla puesta y vacía tras editar la unidad, teclear `si`/`yes`/`1.0` en vez de `true`, y
// escribir mal el NOMBRE. Ninguno puede apagar la entrega.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo, y todas se ejecutaron.

import (
	"path/filepath"
	"testing"
)

// cargaLimpia carga la config sin fichero YAML (la ruta no existe: Load la ignora) para que lo único que
// decida el valor sea el default y el overlay de entorno, que es justo lo que se está midiendo.
func cargaLimpia(t *testing.T) Config {
	t.Helper()
	cfg, err := Load(filepath.Join(t.TempDir(), "no-existe.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return cfg
}

// TestPalancaDespachador_ElDefaultEsDRENAR: sin nadie que diga nada, el despachador arranca. Es el caso
// del 100 % de los Edge en campo y el que sostiene la entrega de entrantes entera.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada):
//   - en defaults(), `DespachadorApagado: false` → `true` ⇒ ningún Edge del mundo drenaría su cola, y los
//     tres gates seguirían verdes porque no hay ningún test de integración que despache un entrante.
func TestPalancaDespachador_ElDefaultEsDRENAR(t *testing.T) {
	if cargaLimpia(t).DespachadorApagado {
		t.Fatal("con la variable SIN PONER el Edge no arrancaría su despachador: recibiría mensajes, los " +
			"encolaría y no subiría ninguno a la nube, sin un solo error en el log")
	}
}

// TestPalancaDespachador_SoloUnTrueExplicitoLaEcha recorre los tres valores que un operador teclea mal y
// el nombre mal escrito. Los cuatro tienen que acabar en el MISMO sitio: despachador activo.
//
// El caso de la variable VACÍA no es teórico: es lo que queda al comentar a medias una línea de un
// EnvironmentFile o al dejar `WAPP_AGENT_DESPACHADOR_APAGADO=` tras quitar el valor. `strconv.ParseBool("")`
// falla, y lo que importa es hacia DÓNDE cae ese fallo.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - en Load, `cfg.DespachadorApagado = loader.GetBool("DESPACHADOR_APAGADO", cfg.DespachadorApagado)` →
//     `= loader.GetString("DESPACHADOR_APAGADO", "") != ""` ⇒ cualquier valor presente, incluido un `false`
//     o una errata, echaría la palanca. Es la forma exacta en que una variable «de flag» se rompe.
//   - la misma línea con el default invertido (`!cfg.DespachadorApagado`) ⇒ el caso ausente/inválido pasa
//     a apagar el despachador.
func TestPalancaDespachador_SoloUnTrueExplicitoLaEcha(t *testing.T) {
	casos := []struct {
		nombre  string
		clave   string
		valor   string
		apagado bool
	}{
		{"vacia (linea a medio editar en el EnvironmentFile)", EnvPrefix + "DESPACHADOR_APAGADO", "", false},
		{"tecleada en castellano", EnvPrefix + "DESPACHADOR_APAGADO", "si", false},
		{"tecleada en ingles", EnvPrefix + "DESPACHADOR_APAGADO", "yes", false},
		{"numero que no es booleano", EnvPrefix + "DESPACHADOR_APAGADO", "2", false},
		{"nombre mal escrito", EnvPrefix + "DESPACHADR_APAGADO", "true", false},
		{"false explicito", EnvPrefix + "DESPACHADOR_APAGADO", "false", false},
		// El único camino que la echa, y va en la misma tabla para que se vea que la puerta EXISTE: un test
		// que solo comprobase los fallos pasaría también con una palanca que no funciona.
		{"true explicito: la palanca SI se echa", EnvPrefix + "DESPACHADOR_APAGADO", "true", true},
		{"1 explicito (ParseBool lo acepta)", EnvPrefix + "DESPACHADOR_APAGADO", "1", true},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			t.Setenv(c.clave, c.valor)
			got := cargaLimpia(t).DespachadorApagado
			if got == c.apagado {
				return
			}
			if c.apagado {
				t.Fatalf("%s=%q NO echó la palanca: la medición de T3.17 correría con el despachador vivo y "+
					"el p99 mediría otra cosa que la que se cree", c.clave, c.valor)
			}
			t.Fatalf("%s=%q ECHÓ la palanca: el Edge dejaría de entregar entrantes por un valor que nadie "+
				"escribió con esa intención", c.clave, c.valor)
		})
	}
}
