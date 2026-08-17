package daemon

// latencia_cableado_test.go — EL PRIMER TEST DE ESTE PAQUETE, y existe por lo que NO se veía
// (Plan 051 Ola 3 · T3.13, cierre del agujero de cableado).
//
// 🔴 QUÉ SE CUSTODIA AQUÍ Y POR QUÉ NO LO CUSTODIABA NADIE. `internal/infra/daemon` no tenía ni un solo
// `_test.go`, y sus únicos importadores (`cmd/agent`) no ejercitan `Run`. Consecuencia medida, no supuesta:
// sustituir el histograma del latido por un `latencia.Nuevo()` recién construido dejaba `go build`,
// `go vet` y `go test ./... -p 1` COMPLETAMENTE VERDES. En campo eso publica un bloque de latencia «sin
// dato» para siempre —`n=0`, sin percentiles— con el Edge funcionando perfectamente, y el síntoma no
// apunta a la causa.
//
// El cableado es la clase de código que ningún test de unidad mira: no tiene ramas, no devuelve errores y
// «se ve bien». Lo único que se le puede preguntar es si las dos puntas apuntan AL MISMO instrumento, y
// eso es lo que se pregunta aquí.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo, y todas se ejecutaron.

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// logMudo es el logger de estos tests: escribe a un buffer que nadie lee. Aquí no se afirma sobre logs
// —lo que se mide son punteros—, y un `colaContador` con un adaptador sordo emite un Warn legítimo que no
// debe ensuciar la salida del `go test`.
func logMudo() sharedlogger.Logger {
	return sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}))
}

// colaCompleta es una cola que respalda LOS DOS papeles que el daemon le pide: encolar (app.ColaEntrantes)
// y contar (app.ColaContador). Es la forma del *Store real, que es el que corre en campo.
type colaCompleta struct{}

func (colaCompleta) Enqueue(context.Context, app.ColaItem) error { return nil }

func (colaCompleta) Pendientes(context.Context) (app.ColaPendientes, error) {
	return app.ColaPendientes{}, nil
}

// colaSorda solo encola: NO respalda el lado contador. Existe para fijar la degradación HONESTA que
// `colaContador` documenta (bloque sin los campos de cola en vez de un pánico), no para bendecirla.
type colaSorda struct{}

func (colaSorda) Enqueue(context.Context, app.ColaItem) error { return nil }

// TestBuildLatencia_ElInstrumentoQueSeLLENAEsElQueSePUBLICA es EL test de este fichero: el histograma que
// reciben los listeners y el que lee el latido tienen que ser EL MISMO OBJETO, no dos iguales.
//
// Se comprueba por IDENTIDAD DE PUNTERO y por los dos extremos:
//
//   - `deps.Hist` contra el instrumento — la punta que publica;
//   - el campo que la opción deja dentro de un `sessionmgr.Manager` de verdad, leído por reflexión, contra
//     ese mismo instrumento — la punta que lo llena. Se construye un Manager real y no se da por buena la
//     opción, porque «devuelve una Option» no prueba que la Option guarde nada: la de `WithLatencia`
//     IGNORA los nil por diseño, así que una opción bien formada puede no cablear absolutamente nada.
//
// Un test que solo comparase `deps.Hist` con lo que devuelve el constructor sería tautológico. Lo que
// hace que esto valga es que las dos afirmaciones salen de puntos DISTINTOS del cableado.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - en buildLatencia, `Hist: h` → `Hist: latencia.Nuevo()` ⇒ el latido publicaría un histograma que
//     nadie llena (el fallo real que destapó esto).
//   - en buildLatencia, `sessionmgr.WithLatencia(h)` → `sessionmgr.WithLatencia(latencia.Nuevo())` ⇒ los
//     listeners llenarían un histograma que nadie publica (el mismo agujero por el otro extremo).
//   - `opt: nil` (o cualquier opción que no cablee) ⇒ el campo del Manager queda a cero.
func TestBuildLatencia_ElInstrumentoQueSeLLENAEsElQueSePUBLICA(t *testing.T) {
	lat := buildLatencia(config.Config{InboundStatsEveryMS: 1234}, colaCompleta{}, logMudo())

	if lat.hist == nil {
		t.Fatal("buildLatencia no construyó el histograma: sin instrumento no hay criterio que medir (INV-051.2)")
	}
	if lat.deps.Hist != lat.hist {
		t.Errorf("el latido publica OTRO histograma que el que se cablea (%p != %p): en campo el bloque "+
			"saldría con n=0 y sin percentiles para siempre", lat.deps.Hist, lat.hist)
	}

	// El Manager REAL, armado con la misma opción que recibe `Run`. El store nil no se usa en la
	// construcción (NewManager solo le hace interface-upgrade) y el Layout necesita un directorio, no una
	// sesión: no se arranca ningún listener aquí.
	mgr := sessionmgr.NewManager(sessionmgr.NewLayout(t.TempDir()), nil, 1, logMudo(), lat.opt)

	// Lectura por reflexión del campo NO EXPORTADO donde WithLatencia deja el histograma. `Pointer()` sí
	// está permitido sobre un campo no exportado (a diferencia de `Interface()`), y comparar direcciones es
	// justo lo que hace falta.
	//
	// ⚠️ SI ESTE `Fatal` SALTA, no es este test el que está roto: alguien renombró `Manager.latencia` y hay
	// que actualizar el nombre aquí. Se prefiere ese rojo ruidoso a un test que deje de mirar en silencio.
	campo := reflect.ValueOf(mgr).Elem().FieldByName("latencia")
	if !campo.IsValid() {
		t.Fatal("sessionmgr.Manager ya no tiene el campo `latencia`: ¿se renombró? Este test mira ese campo " +
			"a propósito, porque es donde acaba el histograma que llenan los listeners")
	}
	enManager := campo.Pointer()
	if enManager == 0 {
		t.Fatal("la opción de buildLatencia NO cableó el histograma en el Manager: los listeners no medirían " +
			"nada y el bloque de latencia saldría vacío sin un solo error")
	}
	if enManager != reflect.ValueOf(lat.hist).Pointer() {
		t.Errorf("el Manager llena un histograma DISTINTO del que publica el latido (%#x != %#x)",
			enManager, reflect.ValueOf(lat.hist).Pointer())
	}
}

// TestBuildLatencia_LaCadenciaYElContadorSalenDeLaConfigYDeLaCola cubre el resto de la punta que publica:
// sin cadencia el bloque periódico no sale, sin logger el latido ni arranca (`Latido` retorna con Log nil)
// y sin contador el bloque va sin `cola_pendientes`.
//
// El caso de la cola SORDA no bendice esa degradación: fija que sea SILENCIOSA para el arranque (nada de
// pánicos ni de fallos por una línea de observabilidad) y a la vez NIL, que es lo que hace que el bloque
// omita los campos en vez de publicar unos ceros que se leerían como «la cola está vacía».
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - `Cada:` fijado a una constante en vez de venir de cfg ⇒ la cadencia dejaría de gobernarse por
//     WAPP_AGENT_INBOUND_STATS_EVERY_MS y el 0 no podría apagar el latido periódico.
//   - `Cola: nil` fijo ⇒ el bloque perdería `cola_pendientes` EN SILENCIO (el modo de fallo que denuncia
//     el comentario de colaContador).
func TestBuildLatencia_LaCadenciaYElContadorSalenDeLaConfigYDeLaCola(t *testing.T) {
	log := logMudo()

	lat := buildLatencia(config.Config{InboundStatsEveryMS: 1234}, colaCompleta{}, log)
	if lat.deps.Cada != 1234*time.Millisecond {
		t.Errorf("la cadencia del latido no sale de la config: got %v, se esperaba 1.234 ms", lat.deps.Cada)
	}
	if lat.deps.Log == nil {
		t.Error("el latido sin logger NI ARRANCA (Latido retorna de inmediato): el bloque no se emitiría nunca")
	}
	if lat.deps.Cola == nil {
		t.Error("la cola respalda app.ColaContador y aun así el bloque saldría sin `cola_pendientes`: " +
			"eso es el latido midiendo a ciegas la mitad de lo que la línea promete")
	}

	// Cadencia 0: es un valor LEGÍTIMO («cállate»), no un dedazo — el bloque final se emite igual.
	if apagado := buildLatencia(config.Config{InboundStatsEveryMS: 0}, colaCompleta{}, log); apagado.deps.Cada != 0 {
		t.Errorf("el 0 de config DESACTIVA la emisión periódica y debe llegar tal cual: got %v", apagado.deps.Cada)
	}

	// Cola sorda: se degrada, pero ni entra en pánico ni finge tener el dato.
	sorda := buildLatencia(config.Config{InboundStatsEveryMS: 1234}, colaSorda{}, log)
	if sorda.deps.Cola != nil {
		t.Error("una cola que no respalda app.ColaContador no puede colarse como contador: el latido " +
			"llamaría a Pendientes sobre algo que no lo implementa")
	}
	if sorda.deps.Hist != sorda.hist || sorda.hist == nil {
		t.Error("el cronómetro tiene que seguir cableado con una cola sorda: la falta del CONTADOR no puede " +
			"llevarse por delante la medida del handler, que es el criterio de cierre de la ola")
	}
}
