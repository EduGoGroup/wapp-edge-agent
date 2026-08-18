package daemon

// palanca_despachador_test.go — LAS DOS PUNTAS DE LA PALANCA DE DIAGNÓSTICO (Plan 051 Ola 5 · T3.17).
//
// 🔴 EL AGUJERO QUE ESTE FICHERO CIERRA, que es el MISMO que destapó T3.13 y por eso reusa su molde. La
// palanca sale de un campo de config y tiene que llegar a dos sitios distintos: al Manager (que es quien
// no arranca el despachador) y a las Deps del latido (que es quien lo publica). Las dos llegadas se
// deciden en `Run` y en `buildLatencia`, y `Run` no lo ejercita ningún test: sus únicos importadores son
// `cmd/agent`, cuyos tests no lo llaman.
//
// Consecuencia MEDIDA, no supuesta (ver la tabla de mutaciones de cada test): una negación invertida en la
// línea de la opción apaga la entrega de entrantes en TODOS los Edge —con la palanca sin echar, sin un
// solo error en el log y con los cuatro gates en verde—, y una constante en la línea del latido deja al
// bloque publicando `despachador=activo` para siempre, incluida la vez que sí está apagado. Los dos fallos
// son de cableado: no tienen ramas, no devuelven errores y «se ven bien».
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo, y todas se ejecutaron.

import (
	"reflect"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
)

// palancaEnElManager arma un Manager REAL con la opción que produce el daemon y lee por reflexión el campo
// no exportado donde acaba la palanca. Se construye un Manager de verdad —y no se da por buena la Option—
// porque «devuelve una Option» no prueba que la Option guarde nada: WithDespachadorApagado IGNORA el
// `false` por diseño, así que una opción bien formada puede no cablear absolutamente nada.
//
// ⚠️ SI EL `Fatal` DEL CAMPO SALTA, no es este test el que está roto: alguien renombró
// `Manager.despachadorApagado` y hay que actualizar el nombre aquí. Se prefiere ese rojo ruidoso a un test
// que deje de mirar en silencio.
func palancaEnElManager(t *testing.T, cfg config.Config) bool {
	t.Helper()
	mgr := sessionmgr.NewManager(sessionmgr.NewLayout(t.TempDir()), nil, 1, logMudo(), opcionPalancaDespachador(cfg))

	campo := reflect.ValueOf(mgr).Elem().FieldByName("despachadorApagado")
	if !campo.IsValid() {
		t.Fatal("sessionmgr.Manager ya no tiene el campo `despachadorApagado`: ¿se renombró? Este test mira " +
			"ese campo a propósito, porque es el que decide si la sesión arranca su despachador")
	}
	return campo.Bool()
}

// TestPalancaDespachador_LaOpcionDelManagerNoInvierteElSentido es el test caro de esta tarea: comprueba los
// DOS estados, porque un solo caso no distingue una opción correcta de una invertida.
//
// El caso que de verdad importa es el de abajo (palanca bajada ⇒ el Manager drena): es el de todos los Edge
// en campo, y el único fallo de esta tarea capaz de tumbar la recepción del ecosistema entero.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - en opcionPalancaDespachador, `WithDespachadorApagado(cfg.DespachadorApagado)` →
//     `WithDespachadorApagado(!cfg.DespachadorApagado)` ⇒ ningún Edge drenaría su cola.
//   - la misma línea con un literal `true` ⇒ idéntico desastre, por otro camino.
//   - la misma línea con un literal `false` ⇒ la palanca deja de existir en silencio y T3.17 no se puede medir.
func TestPalancaDespachador_LaOpcionDelManagerNoInvierteElSentido(t *testing.T) {
	if !palancaEnElManager(t, config.Config{DespachadorApagado: true}) {
		t.Error("con la palanca ECHADA en config el Manager no se entera: el despachador arrancaría igual y " +
			"la medición de T3.17 mediría con la cola drenando")
	}
	if palancaEnElManager(t, config.Config{DespachadorApagado: false}) {
		t.Fatal("con la palanca BAJADA en config el Manager la recibe ECHADA: cada Edge del ecosistema " +
			"recibiría mensajes y no subiría ninguno a la nube, sin un solo error y con la variable sin poner")
	}
}

// TestPalancaDespachador_ElLatidoPublicaLaPalancaQueSeCableo cubre la SEGUNDA punta. Si esta se rompe no se
// pierde ninguna entrega —el Edge sigue haciendo lo correcto—, se pierde la única forma de enterarse de que
// alguien dejó la palanca puesta: el bloque diría `despachador=activo` con la cola sin drenar.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - en buildLatencia, `DespachadorApagado: cfg.DespachadorApagado` → un literal `false` ⇒ el latido
//     publica el estado sano SIEMPRE, incluido cuando la palanca está echada.
//   - la misma línea negada ⇒ el bloque grita en los Edge sanos y calla en el averiado.
func TestPalancaDespachador_ElLatidoPublicaLaPalancaQueSeCableo(t *testing.T) {
	echada := buildLatencia(config.Config{InboundStatsEveryMS: 1234, DespachadorApagado: true}, colaCompleta{}, logMudo())
	if !echada.deps.DespachadorApagado {
		t.Error("el latido no recibió la palanca ECHADA: el bloque periódico publicaría `despachador=activo` " +
			"con la cola sin drenar, que es la línea del runbook mintiendo justo cuando hace falta")
	}

	bajada := buildLatencia(config.Config{InboundStatsEveryMS: 1234, DespachadorApagado: false}, colaCompleta{}, logMudo())
	if bajada.deps.DespachadorApagado {
		t.Error("el latido recibió una palanca que nadie echó: publicaría una alarma en todos los Edge sanos, " +
			"y una alarma que sale siempre deja de leerse")
	}
}
