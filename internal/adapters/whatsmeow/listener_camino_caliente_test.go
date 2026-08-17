package whatsmeow

// listener_camino_caliente_test.go — EL CAMINO CALIENTE TRAS EL RETIRO DEL INLINE
// (Plan 051 Ola 3 · T3.5, bloque (f)).
//
// QUÉ SE FIJA AQUÍ Y POR QUÉ NO ESTABA. `listener_test.go` ya comprueba, mensaje a mensaje, QUÉ FILA se
// anota; lo que nadie comprobaba es la propiedad NEGATIVA que T3.0 compró y que es la razón entera de la
// ola: que el handler de whatsmeow no llame a NADIE MÁS. Esa propiedad no se ve mirando el resultado (la
// fila es idéntica con o sin una llamada de más); solo se ve contando llamadas y mirando la ESTRUCTURA.
//
// El motivo por el que importa es de latencia, no de estética: `onMessage` corre en el bucle de eventos del
// socket de whatsmeow. Cada milisegundo que se pasa ahí es un milisegundo que la sesión no procesa el
// evento siguiente. Antes de T3.0 ese hilo esperaba a OLLAMA (el decorador inline clasificaba en línea) y
// luego a la NUBE (el `sink.Deliver`); la ola entera consiste en que ya no espera a ninguno de los dos.
//
// LOS DOS ÁNGULOS, porque uno solo no basta:
//   - ESTRUCTURAL — el `Listener` no puede TENER por dónde llamar. Un contador de llamadas solo ve los
//     colaboradores que el test conoce; un barrido de los campos ve también los que alguien añada mañana.
//   - DE COMPORTAMIENTO — con lo que sí tiene cableado, el handler hace exactamente dos cosas y la última
//     es el `Enqueue`.
//
// Reutiliza los dobles de `listener_test.go` (callLog, spyCola, listenerConCola, liveMessage, quietLogger):
// mismo paquete a propósito.

import (
	"context"
	"reflect"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// TestListener_NoTienePorDondeEntregarNiClasificar es el ángulo ESTRUCTURAL del bloque (f).
//
// 🔴 EL CONTADOR DE LLAMADAS NO PUEDE PROBAR ESTO, y por eso hay un test aparte. Un `callLog` solo registra
// los colaboradores que el TEST inyecta: si mañana alguien vuelve a colgar un `app.InboundSink` (o un
// clasificador) del `Listener` y lo llama desde `onMessage`, el registro de llamadas del test seguiría
// diciendo `[enqueue]` — porque el nuevo colaborador no pasa por el doble. Lo que sí lo delata es que el
// campo EXISTA.
//
// Se barre la estructura y se rechazan TRES formas:
//   - cualquier campo cuyo tipo cumpla `app.InboundSink` — el puntero al cable que T3.0 arrancó de raíz
//     («no se dejó nil», dice la cabecera de `Listener`, «porque un puntero al cable colgando de esta
//     estructura es una invitación a volver a entregar desde aquí»);
//   - cualquier campo cuyo tipo exponga `Classify` — el clasificador del decorador retirado
//     (`intent.classifierPort`);
//   - cualquier campo FUNCIÓN que reciba un `domain.InboundEvent`. Esta tercera se añadió en la revisión
//     de la ola porque las dos primeras dejaban abierta la puerta más probable: el `Listener` YA tiene
//     hooks de tipo función (`onReceipt`, `onConnect`, `onLoggedOutHook`), así que la forma natural de
//     reintroducir la entrega no es un `app.InboundSink` —que salta a la vista— sino un
//     `onInbound func(context.Context, domain.InboundEvent) error` que se mimetiza con los que ya hay.
//     Ninguna de las dos primeras comprobaciones lo veía: un tipo `func` no implementa la interfaz ni
//     expone métodos.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: añadir de vuelta `sink app.InboundSink` al struct `Listener` (que es el
// primer paso obligado de cualquier reintroducción del camino inline) ⇒ rojo, aunque nadie lo llame
// todavía. Ese «aunque nadie lo llame todavía» es el punto: caza la regresión en el commit que la prepara,
// no en el que la consuma.
//
// LO QUE ESTE TEST *NO* PRUEBA, para que nadie lo lea como más de lo que es: no impide llamar a un
// clasificador construido DENTRO de `onMessage` (sin campo). Eso lo cubre el test de comportamiento de
// abajo, que exige que las llamadas sean exactamente dos.
func TestListener_NoTienePorDondeEntregarNiClasificar(t *testing.T) {
	sinkType := reflect.TypeOf((*app.InboundSink)(nil)).Elem()
	// El tipo del evento de dominio, y también su puntero: un hook de entrega podría recibir cualquiera
	// de los dos.
	eventoType := reflect.TypeOf(domain.InboundEvent{})
	eventoPtrType := reflect.PointerTo(eventoType)
	// Se toma el tipo por PUNTERO-NIL y no con `reflect.TypeOf(Listener{})`: el Listener lleva un
	// `sync.Mutex`, y construir el literal para pasarlo por valor es justo lo que `go vet` (copylocks)
	// marca. Esta forma no construye ningún valor.
	lt := reflect.TypeOf((*Listener)(nil)).Elem()

	for i := 0; i < lt.NumField(); i++ {
		f := lt.Field(i)
		if f.Type.Implements(sinkType) {
			t.Fatalf("el campo %q (%s) del Listener cumple app.InboundSink: el listener volvió a tener por dónde "+
				"ENTREGAR. La entrega es del despachador (INV-051.2, Plan 051 Ola 3 · T3.0); un puntero al cable "+
				"colgando de esta estructura es la invitación a reintroducir el camino inline",
				f.Name, f.Type)
		}
		if _, ok := f.Type.MethodByName("Classify"); ok {
			t.Fatalf("el campo %q (%s) del Listener expone Classify: el listener volvió a tener por dónde "+
				"CLASIFICAR en el hilo de whatsmeow. Quien clasifica es el worker-cajero, en otro proceso "+
				"(ADR-0038); el decorador inline murió en T3.0",
				f.Name, f.Type)
		}
		// La tercera puerta: un HOOK de función que reciba el evento de dominio. `onReceipt` y compañía
		// establecieron el precedente de colgar callbacks del Listener, así que este es el disfraz con el
		// que la entrega volvería sin que las dos comprobaciones de arriba pestañearan.
		if f.Type.Kind() != reflect.Func {
			continue
		}
		for j := 0; j < f.Type.NumIn(); j++ {
			if in := f.Type.In(j); in == eventoType || in == eventoPtrType {
				t.Fatalf("el campo %q (%s) del Listener es una función que recibe un domain.InboundEvent: "+
					"el listener volvió a tener por dónde ENTREGAR el entrante desde el hilo de whatsmeow "+
					"(INV-051.2, Plan 051 Ola 3 · T3.0). El entrante se ANOTA en la cola y ahí acaba; quien "+
					"lo entrega es el despachador de la sesión, desde su propia goroutine",
					f.Name, f.Type)
			}
		}
	}
}

// TestOnMessage_CaminoCaliente_SoloElCarrilRapidoLocalYElEnqueue es el ángulo de COMPORTAMIENTO.
//
// Las llamadas del camino caliente son EXACTAMENTE DOS, y las dos son locales:
//  1. `fastLane` — un regex léxico en memoria (µs). NO es «el clasificador»: no toca Ollama, no abre un
//     socket y no puede bloquear. Su semántica es «este texto no necesita LLM», no «este texto significa X».
//  2. `Enqueue` — el INSERT cifrado en SQLite local, y EL ÚLTIMO ACTO DEL HANDLER.
//
// Que el `Enqueue` sea el último no es un detalle de orden: es lo que sostiene la promesa «mensaje durable
// ANTES del acuse». Cualquier cosa que se colara detrás (una entrega, una métrica remota, un flush) volvería
// a atar el hilo del socket a un tercero.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - añadir cualquier llamada a un colaborador registrado detrás del `Enqueue` ⇒ FALLA la aserción de que
//     la última llamada es `enqueue`;
//   - reintroducir una clasificación o una entrega en `onMessage` con un doble que se registre ⇒ FALLA el
//     recuento exacto de dos llamadas;
//   - mover la rama del fastlane por delante de la de `text == ""` en el switch de `enqueueCola` ⇒ FALLA el
//     subtest del mensaje sin texto, que exige que el carril rápido ni se consulte (es la regresión que
//     falsearía el desglose de INV-051.3, la que T1.8 vino a arreglar).
func TestOnMessage_CaminoCaliente_SoloElCarrilRapidoLocalYElEnqueue(t *testing.T) {
	t.Run("texto normal: fastlane local y luego enqueue, y nada mas", func(t *testing.T) {
		calls := &callLog{}
		cola := &spyCola{calls: calls}
		l := listenerConCola(cola, WithFastLane(func(string) bool {
			calls.record("fastlane")
			return false
		}))

		l.handleEvent(context.Background(), liveMessage("MSG-HOT", "quiero dos empanadas"))

		got := calls.snapshot()
		if len(got) != 2 || got[0] != "fastlane" || got[1] != "enqueue" {
			t.Fatalf("llamadas = %v, se esperaba exactamente [fastlane enqueue]. "+
				"El camino caliente del listener es un regex en memoria y un INSERT local: ni Ollama ni la nube "+
				"(INV-051.2, T3.0)", got)
		}
		// Redundante con lo de arriba, y a propósito: es LA afirmación del bloque (f) escrita como tal, para
		// que un cambio en la primera llamada no se lleve por delante la segunda.
		if got[len(got)-1] != "enqueue" {
			t.Fatalf("la última llamada del handler fue %q: el Enqueue tiene que ser el ÚLTIMO acto "+
				"(es lo que hace durable el mensaje ANTES del acuse)", got[len(got)-1])
		}
	})

	t.Run("sin texto: ni el carril rapido se consulta", func(t *testing.T) {
		calls := &callLog{}
		cola := &spyCola{calls: calls}
		l := listenerConCola(cola, WithFastLane(func(string) bool {
			calls.record("fastlane")
			return true
		}))

		sinTexto := liveMessage("MSG-IMG", "")
		sinTexto.Message = nil // imagen / audio / sticker: no hay cuerpo textual
		l.handleEvent(context.Background(), sinTexto)

		if got := calls.snapshot(); len(got) != 1 || got[0] != "enqueue" {
			t.Fatalf("llamadas = %v, se esperaba solo [enqueue]. La rama `text == \"\"` va POR DELANTE del "+
				"fastlane: el carril rápido devuelve true para la cadena vacía, así que consultarlo aquí "+
				"contaría como `fastlane` todo el tráfico no textual y la telemetría mentiría sobre cuánto LLM "+
				"ahorra el léxico (T1.8)", got)
		}
		if len(cola.got) != 1 || cola.got[0].IntentJSON != `{"omitido":"sin_texto"}` {
			t.Fatalf("la fila no nació con la marca de omisión por falta de texto: %v", colaWAIDs(cola.got))
		}
	})

	// EL CTX YA CANCELADO es la prueba barata de que el handler no espera a nadie: cualquier llamada
	// remota (una clasificación, una entrega, un flush a la nube) abortaría con el contexto muerto y la fila
	// no se anotaría. El INSERT local, en cambio, sigue su curso — el `Enqueue` del store recibe el ctx pero
	// el driver de SQLite no depende de la red.
	//
	// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: meter en `onMessage` cualquier paso que ceda ante `ctx.Done()` antes
	// del Enqueue (un `select` con timeout, una llamada gRPC, un `classifier.Classify` con contexto) ⇒ deja
	// de encolarse la fila.
	t.Run("con el contexto ya cancelado se encola igual: el handler no espera a nadie", func(t *testing.T) {
		calls := &callLog{}
		cola := &spyCola{calls: calls}
		l := listenerConCola(cola)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		l.handleEvent(ctx, liveMessage("MSG-CTX", "quiero dos empanadas"))

		if len(cola.got) != 1 {
			t.Fatalf("con el contexto cancelado no se anotó la fila (%d filas): el camino caliente está "+
				"esperando a algo que depende del ctx, y eso es exactamente lo que T3.0 retiró", len(cola.got))
		}
		if got := calls.snapshot(); len(got) != 1 || got[0] != "enqueue" {
			t.Fatalf("llamadas = %v, se esperaba [enqueue]", got)
		}
	})
}

// TestOnMessage_CaminoCaliente_ElEcoPropioNiSiquieraLlegaAlCarrilRapido cierra el filtro 1 de la puerta por
// el mismo criterio de «coste cero»: el eco propio se descarta ANTES de tocar nada, así que ni el regex ni
// el store se llegan a consultar.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: mover el filtro `e.Info.IsFromMe` por detrás del Enqueue (o quitarlo) ⇒
// aparecen llamadas y una fila con un mensaje PROPIO, que además rompería la propiedad sobre la que el
// despachador construye `IsFromMe = false` (ver app.ColaMeta).
func TestOnMessage_CaminoCaliente_ElEcoPropioNiSiquieraLlegaAlCarrilRapido(t *testing.T) {
	calls := &callLog{}
	cola := &spyCola{calls: calls}
	l := listenerConCola(cola, WithFastLane(func(string) bool {
		calls.record("fastlane")
		return false
	}))

	eco := liveMessage("MSG-ECO", "esto lo mandé yo")
	eco.Info.IsFromMe = true
	l.handleEvent(context.Background(), eco)

	if got := calls.snapshot(); len(got) != 0 {
		t.Fatalf("llamadas = %v, no debía haber ninguna: el eco propio se descarta en la puerta, antes de "+
			"gastar un regex o un INSERT", got)
	}
	if len(cola.got) != 0 {
		t.Fatalf("el eco propio generó %d filas: no existe fila de la cola con un mensaje propio, y el "+
			"despachador construye IsFromMe=false apoyándose en esa propiedad", len(cola.got))
	}
}
