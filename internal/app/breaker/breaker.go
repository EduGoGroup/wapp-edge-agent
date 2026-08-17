// Package breaker es el CIRCUIT BREAKER del clasificador local, extraído para que lo compartan sus dos
// consumidores (Plan 051 Ola 2 · T2.4): el decorador inline de `agent serve`
// (internal/adapters/intent/sink.go, ADR-0020) y el worker-cajero (internal/app/cajero, ADR-0038).
//
// 🔴 ES UNA MIGRACIÓN, NO UN REDISEÑO. La lógica viene ÍNTEGRA de sink.go (beginAttempt/recordSuccess/
// recordFailure), con sus dos números intactos: 5 fallos CONSECUTIVOS abren el circuito por 60 s, y
// pasado ese tiempo el circuito entra en MEDIO-ABIERTO y deja pasar UN solo sondeo (éxito ⇒ cierra,
// fallo ⇒ reabre otros 60 s). Esa es la calibración probada en el e2e del Plan 029 y el criterio de
// T2.4 dice literalmente que ningún umbral puede cambiar de valor durante la migración: si vienes a
// «afinarlos de paso», no lo hagas aquí — hazlo con una medición delante y un ADR que lo respalde.
//
// LO QUE EL BREAKER NO CUENTA COMO FALLO (semántica heredada, se sostiene en los llamantes, no aquí):
// un intent vacío o la intención reservada «desconocido» NO son fallos —el clasificador respondió
// bien, simplemente sin intención accionable— y por tanto el llamante debe invocar RecordSuccess, no
// RecordFailure. Fallo es error de red, timeout o pánico del clasificador.
//
// Seguro para uso concurrente: todo el estado va bajo un único mutex. El reloj es inyectable
// (WithClock) para que los tests no dependan de esperas reales; por defecto es time.Now.
package breaker

import (
	"sync"
	"time"
)

// DefaultThreshold es el número de fallos CONSECUTIVOS que abren el circuito. Heredado tal cual de
// `cbThreshold` (intent/sink.go), calibrado en el Plan 029. No se re-ajusta sin medición (T2.4).
const DefaultThreshold = 5

// DefaultOpenFor es cuánto permanece ABIERTO el circuito antes de pasar a medio-abierto. Heredado tal
// cual de `cbOpenFor` (intent/sink.go). No se re-ajusta sin medición (T2.4).
const DefaultOpenFor = 60 * time.Second

// Los TRES estados observables del circuito. Son las etiquetas EXACTAS que `GET /v1/intent/status`
// lleva publicando desde el Plan 029 (`intent_circuit`, ADR-0023): cambiarlas rompería a la consola y
// al heartbeat, así que son contrato, no detalle interno.
const (
	// StateClosed: se clasifica con normalidad (fallos consecutivos por debajo del umbral).
	StateClosed = "closed"
	// StateOpen: el umbral se alcanzó y la ventana sigue viva; no se intenta clasificar.
	StateOpen = "open"
	// StateHalfOpen: la ventana venció; se deja pasar UN sondeo para ver si el clasificador volvió.
	StateHalfOpen = "half-open"
)

// Option configura un Breaker en su construcción.
type Option func(*Breaker)

// WithClock inyecta el reloj del breaker (por defecto time.Now). Existe para los tests, que no pueden
// permitirse esperar 60 s reales para observar el medio-abierto, y para el decorador de `intent`, que
// expone su propio `now` sustituible y necesita que el breaker lea ESE reloj y no otro. Un now nil se
// ignora (se conserva el default) en vez de dejar el breaker con un reloj que pancaría al llamarlo.
func WithClock(now func() time.Time) Option {
	return func(b *Breaker) {
		if now != nil {
			b.now = now
		}
	}
}

// Breaker es el circuito compartido. El cero-valor NO sirve: usa New (necesita umbral, ventana y reloj).
type Breaker struct {
	threshold int
	openFor   time.Duration
	now       func() time.Time

	mu sync.Mutex
	// failures son los fallos CONSECUTIVOS: un éxito lo devuelve a 0. No es una tasa ni una ventana
	// deslizante a propósito — el fallo que interesa aquí es «Ollama no está», que es consecutivo por
	// naturaleza.
	failures int
	// openUntil es el instante hasta el que el circuito está abierto.
	openUntil time.Time
	// probing indica que hay un sondeo de medio-abierto EN CURSO, para que N llamadas concurrentes no
	// sondeen todas a la vez contra un clasificador que probablemente sigue caído.
	probing bool
}

// New construye el breaker. threshold <= 0 cae a DefaultThreshold y openFor <= 0 a DefaultOpenFor: un
// umbral de 0 abriría el circuito antes del primer fallo y una ventana de 0 lo dejaría permanentemente
// en medio-abierto, que son las dos formas fáciles de romperlo tecleando un cero en la config.
func New(threshold int, openFor time.Duration, opts ...Option) *Breaker {
	if threshold <= 0 {
		threshold = DefaultThreshold
	}
	if openFor <= 0 {
		openFor = DefaultOpenFor
	}
	b := &Breaker{threshold: threshold, openFor: openFor, now: time.Now}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// BeginAttempt decide si se permite UN intento de clasificación y, si el circuito está medio-abierto,
// RESERVA el sondeo (de ahí que no sea un simple lector: tiene efecto). Cerrado ⇒ true; abierto y
// dentro de la ventana ⇒ false; ventana vencida ⇒ true para el PRIMER llamante y false para el resto
// hasta que ese sondeo se resuelva con RecordSuccess o RecordFailure.
//
// 🔴 QUIEN RECIBE true SE COMPROMETE a llamar después a RecordSuccess o RecordFailure. Si no lo hace
// (p. ej. porque abandona el intento a mitad), el flag de sondeo queda reservado y el circuito no
// vuelve a dejar pasar nada hasta el siguiente resultado — que es exactamente el estado en que un
// worker que muere a mitad del sondeo deja el proceso. Es aceptable porque el proceso muere entero y
// el breaker nace limpio con él, pero NO lo es dentro de un mismo proceso.
func (b *Breaker) BeginAttempt() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures < b.threshold {
		return true // cerrado
	}
	if b.now().Before(b.openUntil) {
		return false // abierto
	}
	if b.probing {
		return false // medio-abierto, sondeo ya en curso
	}
	b.probing = true
	return true
}

// RecordSuccess cierra el circuito: reinicia el contador de fallos consecutivos, libera el sondeo y
// borra la ventana. Un éxito basta para cerrar (no hace falta una racha): el fallo del que protege es
// «el servicio no está», y si respondió, está.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.probing = false
	b.openUntil = time.Time{}
}

// RecordFailure suma un fallo consecutivo y, al alcanzar el umbral, abre el circuito por openFor. Un
// fallo del sondeo de medio-abierto REABRE la ventana igual (failures ya está por encima del umbral),
// que es justo lo que se quiere: si el sondeo falló, el clasificador sigue sin estar.
//
// DEVUELVE SI ESTA LLAMADA ABRIÓ EL CIRCUITO (el FLANCO no-abierto → abierto), y ese bool es la razón
// de que la firma no sea `func()`. El llamante que quiere contar APERTURAS —no «instantes en que el
// circuito estaba abierto», que es otra cosa— necesita comparar el estado de antes con el de después,
// y hacerlo desde fuera obliga a la secuencia State()/RecordFailure()/State(); esa secuencia NO es
// atómica aunque cada llamada lo sea, así que con dos fallos concurrentes puede contar dos aperturas
// de la misma (o ninguna). Aquí el flanco se calcula DENTRO del mismo lock que lo produce, que es el
// único sitio donde la respuesta no puede mentir.
//
// Ignorar el resultado es legítimo y no cambia nada: `b.RecordFailure()` como sentencia sigue siendo
// código válido (es lo que hace el decorador inline de internal/adapters/intent).
//
// 🔴 «Abrir» incluye el fallo del sondeo de MEDIO-ABIERTO: el estado previo era half-open —no open—,
// así que el flanco existe y se cuenta. Es deliberado y es la semántica que ya tenía el cajero: cada
// reapertura de la ventana es un evento operativo por derecho propio.
func (b *Breaker) RecordFailure() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// El estado ANTES del incremento, con el mismo criterio exacto que State().
	abiertoAntes := b.failures >= b.threshold && b.now().Before(b.openUntil)

	b.failures++
	b.probing = false
	if b.failures >= b.threshold {
		b.openUntil = b.now().Add(b.openFor)
		// Tras fijar openUntil = now+openFor (openFor > 0 siempre, ver New) el estado es Open por
		// construcción, así que el flanco es exactamente «no estaba abierto antes».
		return !abiertoAntes
	}
	return false
}

// State devuelve el estado observable del circuito (StateClosed/StateOpen/StateHalfOpen). Es un lector
// PURO: a diferencia de BeginAttempt no reserva el sondeo, así que preguntarlo mil veces no consume
// nada. Lo consumen `GET /v1/intent/status` y el bucle del cajero (que deja de reclamar en StateOpen).
func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures < b.threshold {
		return StateClosed
	}
	if b.now().Before(b.openUntil) {
		return StateOpen
	}
	return StateHalfOpen
}
