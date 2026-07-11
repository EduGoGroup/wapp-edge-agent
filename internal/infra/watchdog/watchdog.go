// Package watchdog aporta el patrón "abandona y reporta" para acotar llamadas potencialmente
// BLOQUEANTES y NO CANCELABLES (Plan 031 T6, regla dura: "ningún camino bloqueante sin timeout").
//
// Motivación (incidente real 2026-07-11, runbook edge-troubleshooting §1): la carga de la DEK quedó
// 31 minutos colgada en una llamada cgo al Keychain de macOS (SecItemCopyMatching esperando un diálogo
// de permiso en pantalla) SIN timeout ni traza, mientras el resto del sistema seguía reportándose "sano".
// Una llamada cgo NO se puede cancelar desde Go (no acepta context): lo único que podemos hacer es
// ABANDONAR la espera al vencer un plazo, reportar la degradación y dejar que la goroutine que ejecuta la
// llamada MUERA cuando la llamada por fin retorne (fuga CONTROLADA y documentada, no un leak accidental).
//
// A diferencia de un context.WithTimeout —que sirve para llamadas cancelables (dial de red, SQL con ctx)—
// aquí la llamada sigue su curso: Guard solo deja de ESPERARLA. Para llamadas cancelables usa el context
// normal; este helper es para el hueco que el context no cubre (cgo).
package watchdog

import "time"

// Result es el desenlace de una llamada vigilada por Guard.
type Result[T any] struct {
	// Value es el valor que devolvió la llamada (cero si TimedOut y la llamada aún no retornó).
	Value T
	// Err es el error que devolvió la llamada (nil si TimedOut y la llamada aún no retornó).
	Err error
	// Elapsed es la duración REAL hasta que la llamada retornó; si TimedOut es == timeout (la llamada sigue
	// en curso). En el callback onLate (retorno tardío de una llamada abandonada) Elapsed es la duración
	// real total, para poder registrar métricas como dek_load_duration_ms aun cuando el retorno llegó tarde.
	Elapsed time.Duration
	// TimedOut indica que venció el plazo antes de que la llamada retornara: Guard abandonó la espera.
	TimedOut bool
}

// Guard ejecuta call en SU PROPIA goroutine y espera como mucho timeout a que retorne.
//
//   - Si call retorna a tiempo: devuelve Result{Value, Err, Elapsed real, TimedOut:false}.
//   - Si vence timeout primero: ABANDONA la espera y devuelve Result{TimedOut:true, Elapsed:timeout}
//     (Value/Err en cero). La goroutine de call SIGUE viva hasta que call retorne — fuga CONTROLADA:
//     una llamada no cancelable (cgo) no se puede interrumpir. Si onLate != nil, se invoca DESDE esa
//     goroutine cuando call finalmente retorna, con la Result real (Elapsed total, Value/Err reales):
//     así un retorno tardío (p. ej. el usuario atendió el diálogo del Keychain) todavía puede registrar
//     su duración y liberar/limpiar lo que la llamada haya devuelto.
//
// El canal interno está BUFFERIZADO (cap 1) para que la goroutine de call nunca se bloquee al enviar,
// incluso si Guard ya abandonó la espera (sin onLate el valor simplemente se descarta con la goroutine).
//
// timeout <= 0 desactiva la vigilancia: espera a call indefinidamente (equivale a llamarla directo). Es
// una salida explícita para desactivar el watchdog por config sin ramificar en el llamador.
func Guard[T any](timeout time.Duration, call func() (T, error), onLate func(Result[T])) Result[T] {
	start := time.Now()
	type outcome struct {
		value T
		err   error
	}
	ch := make(chan outcome, 1) // buffer 1: la goroutine nunca se bloquea al enviar, la abandonemos o no.
	go func() {
		v, err := call()
		ch <- outcome{v, err}
	}()

	if timeout <= 0 {
		o := <-ch
		return Result[T]{Value: o.value, Err: o.err, Elapsed: time.Since(start)}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case o := <-ch:
		return Result[T]{Value: o.value, Err: o.err, Elapsed: time.Since(start)}
	case <-timer.C:
		// Abandono: no se puede cancelar la llamada (cgo). La goroutine de arriba muere cuando call
		// retorne. Si hay onLate, otra goroutine espera ese retorno tardío y lo reporta con la duración real.
		if onLate != nil {
			go func() {
				o := <-ch
				onLate(Result[T]{Value: o.value, Err: o.err, Elapsed: time.Since(start)})
			}()
		}
		var zero T
		return Result[T]{Value: zero, Elapsed: timeout, TimedOut: true}
	}
}
