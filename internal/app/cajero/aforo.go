package cajero

// aforo.go — EL AFORO DE OLLAMA (Plan 044 · Ola 1.6 · T1.6-2, ADR-0045).
//
// QUÉ ES: el semáforo de inferencias del proceso, extraído del campo `Cajero.sem` a un tipo propio
// porque desde esta tarea tiene DOS PUERTAS y no una.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 UN SOLO AFORO POR PROCESO — JAMÁS DOS SEMÁFOROS
// ─────────────────────────────────────────────────────────────────────────────
// El aforo protege a OLLAMA, que es UNO POR MÁQUINA. Esta tarea añade un segundo consumidor de
// inferencia (el servidor que atiende `inference_request` del Cloud) al lado del que ya había, y la
// tentación evidente —darle su propio semáforo— es exactamente el error que la medición de la O0
// descartó: dos semáforos de 1 plaza son DOS INFERENCIAS SIMULTÁNEAS contra la misma instancia, que es
// el solapamiento que hace que la latencia p50 se dispare (ADR-0038 Enmienda 1 §(d), por eso
// DefaultMaxConcurrent es 1 y no 2).
//
// El aforo es del PROCESO, igual que el breaker y por el mismo argumento (ver Deps.Colas): describen a
// Ollama, no a una cola ni a una instalación. Con N instalaciones en round-robin sigue habiendo UN
// aforo. Si algún día hay dos instancias de Ollama aisladas por `taskset`, el número sube por config;
// el aforo sigue siendo uno.
//
// ─────────────────────────────────────────────────────────────────────────────
// POR QUÉ DOS PUERTAS, Y NO UN `select` CON ctx EN EL LLAMANTE
// ─────────────────────────────────────────────────────────────────────────────
// La diferencia entre las dos es SI HAY PRESUPUESTO DE TIEMPO, y no es de estilo:
//
//   - `TomarHasta` (ACOTADA por un plazo) es la puerta NORMAL, la de una petición del Cloud: hay alguien
//     esperando al otro lado del cable con un presupuesto, y agotarlo EN LA COLA DE ESPERA tiene que
//     responderse `EDGE_SIN_CAPACIDAD` — nunca `TIMEOUT`.
//   - `Tomar` (BLOQUEANTE hasta el ctx) es la puerta de «sin plazo»: cuando ni el Cloud fijó `timeout_ms`
//     ni el Edge tiene default (Deps.Timeout <= 0), no hay ningún instante en el que tenga sentido dejar
//     de esperar salvo la cancelación del ctx. Es a donde delega `TomarHasta` con `plazo <= 0`, y es lo
//     que hace que la promesa de Deps.Timeout <= 0 («manda el ctx de quien pide») se cumpla de verdad.
//
// ⚠️ ANOTADO PARA QUE NO CONFUNDA: hasta el 2026-08-24 `Tomar` tenía un llamante propio y distinto —el
// bucle que reclamaba lotes de la cola, que esperaba plaza sin límite porque no había nadie a quien
// decepcionar—. Ese consumidor murió con el push (ADR-0045 §8), así que hoy `Tomar` sólo se alcanza por
// el camino de arriba. Se conserva como método propio y no se disuelve dentro de `TomarHasta` porque es
// la mitad que hace legible el `select`: sin ella habría que escribir el caso «sin plazo» como un
// temporizador que nunca dispara.
//
// 🔴 ESA DISTINCIÓN ES TODA LA RAZÓN DE QUE `TomarHasta` DEVUELVA EL MOTIVO Y NO UN BOOL. Las dos
// condiciones (plazo agotado esperando plaza / el modelo tardó demasiado) se observan igual desde el
// llamante —un contexto que vence— y significan lo contrario: la primera dice «tu equipo va corto», la
// segunda «el modelo tarda». Un `select` que no separe las dos fases devolverá TIMEOUT siempre y mandará
// al dueño del equipo a mirar su red en vez de su hardware (ver app.ErrInferenciaSinCapacidad). Que el
// aforo sea quien lo diga es lo que hace la distinción IMPOSIBLE DE OLVIDAR: no hay ninguna forma de
// esperar plaza que no devuelva el motivo.

import (
	"context"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// Aforo acota cuántas inferencias corren A LA VEZ contra el proveedor local. Construir con NuevoAforo;
// el cero-valor NO sirve (un canal nil bloquea para siempre).
//
// Seguro para uso concurrente: es un canal con buffer, sin más estado.
type Aforo struct {
	plazas chan struct{}
}

// NuevoAforo construye el aforo con n plazas. n <= 0 cae a DefaultMaxConcurrent (1): un aforo de cero
// plazas no dejaría pasar nada y el síntoma sería un Edge que no clasifica ni sirve una sola inferencia
// sin un solo error en el log.
func NuevoAforo(n int) *Aforo {
	if n <= 0 {
		n = DefaultMaxConcurrent
	}
	return &Aforo{plazas: make(chan struct{}, n)}
}

// Plazas es la capacidad del aforo. Sólo para el log del arranque y los tests.
func (a *Aforo) Plazas() int {
	if a == nil {
		return 0
	}
	return cap(a.plazas)
}

// Ocupadas son las plazas tomadas AHORA MISMO. Es una foto y puede quedar obsoleta antes de que el
// llamante la lea: sirve para el log, nunca para decidir (decidir se hace tomando la plaza).
func (a *Aforo) Ocupadas() int {
	if a == nil {
		return 0
	}
	return len(a.plazas)
}

// Tomar espera una plaza SIN LÍMITE hasta que la haya o el ctx se cancele. Devuelve true si la tomó;
// quien reciba true DEBE llamar a Soltar (el patrón habitual es un `defer`).
//
// Es la puerta del trabajo PROPIO del Edge: nadie espera del otro lado, así que rechazar no compraría
// nada. La cancelación del ctx es lo único que la saca de la espera, y por eso Run puede parar limpio.
func (a *Aforo) Tomar(ctx context.Context) bool {
	select {
	case a.plazas <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// TomarHasta espera una plaza COMO MUCHO `plazo` (además de honrar el ctx). Devuelve si tomó la plaza y,
// cuando NO la tomó, el motivo — que es el punto entero de esta puerta:
//
//   - plazo agotado o ctx vencido esperando plaza ⇒ (false, app.ErrInferenciaSinCapacidad)
//   - ctx cancelado por el apagado del proceso    ⇒ (false, app.ErrInferenciaSinCapacidad) también, y es
//     la lectura honesta: el Edge no va a servir esa inferencia y el Cloud tiene que degradar. Que la
//     causa sea un SIGTERM no la convierte en un problema del modelo ni de la red.
//
// Un `plazo <= 0` se trata como «sin plazo propio»: manda sólo el ctx. No se cae a un default a
// propósito — el llamante de esta puerta SIEMPRE tiene un plazo (se lo dio el Cloud o se lo puso el
// Edge), y fabricar aquí uno distinto escondería un cableado roto.
//
// Quien reciba true DEBE llamar a Soltar.
func (a *Aforo) TomarHasta(ctx context.Context, plazo time.Duration) (bool, error) {
	if plazo <= 0 {
		if a.Tomar(ctx) {
			return true, nil
		}
		return false, app.ErrInferenciaSinCapacidad
	}

	// El temporizador se para SIEMPRE (defer), incluso en el camino feliz: sin eso, una ráfaga de
	// peticiones que sí encuentran plaza dejaría un time.Timer vivo por cada una hasta que venciera su
	// plazo — con plazos de decenas de segundos, eso es basura acumulada en el heap de un proceso que
	// vive semanas.
	t := time.NewTimer(plazo)
	defer t.Stop()

	select {
	case a.plazas <- struct{}{}:
		return true, nil
	case <-t.C:
		return false, app.ErrInferenciaSinCapacidad
	case <-ctx.Done():
		return false, app.ErrInferenciaSinCapacidad
	}
}

// Soltar devuelve una plaza. Llamarlo sin haberla tomado es un BUG del llamante y aquí no se puede
// detectar (un canal con buffer acepta la lectura de otro), así que la única defensa real es el patrón
// `if tomada { defer aforo.Soltar() }` en los DOS consumidores.
//
// NO BLOQUEA: sólo lo llama quien tomó, así que siempre hay algo que sacar.
func (a *Aforo) Soltar() {
	<-a.plazas
}
