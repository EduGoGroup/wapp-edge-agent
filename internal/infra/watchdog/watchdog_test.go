package watchdog

import (
	"errors"
	"testing"
	"time"
)

// TestGuard_ReturnsInTime: si la llamada retorna antes del plazo, Guard devuelve su valor/error y NO marca
// TimedOut, con una Elapsed razonable (> 0, < timeout).
func TestGuard_ReturnsInTime(t *testing.T) {
	res := Guard(time.Second, func() (int, error) {
		time.Sleep(5 * time.Millisecond)
		return 42, nil
	}, nil)

	if res.TimedOut {
		t.Fatal("no debía marcar TimedOut: la llamada retornó a tiempo")
	}
	if res.Value != 42 || res.Err != nil {
		t.Fatalf("valor/err inesperados: %d / %v", res.Value, res.Err)
	}
	if res.Elapsed <= 0 || res.Elapsed >= time.Second {
		t.Fatalf("Elapsed fuera de rango: %v", res.Elapsed)
	}
}

// TestGuard_PropagatesError: un error de la llamada se propaga tal cual (sin TimedOut).
func TestGuard_PropagatesError(t *testing.T) {
	sentinel := errors.New("boom")
	res := Guard(time.Second, func() (int, error) { return 0, sentinel }, nil)
	if res.TimedOut {
		t.Fatal("no debía marcar TimedOut")
	}
	if !errors.Is(res.Err, sentinel) {
		t.Fatalf("err = %v, quería %v", res.Err, sentinel)
	}
}

// TestGuard_TimeoutAbandonsAndReportsLate: una llamada más lenta que el plazo hace que Guard ABANDONE (marca
// TimedOut, Elapsed==timeout, valor en cero) SIN bloquear; cuando la llamada abandonada por fin retorna, se
// invoca onLate con la duración real y el valor real. Es el patrón del cuelgue cgo del Keychain.
func TestGuard_TimeoutAbandonsAndReportsLate(t *testing.T) {
	release := make(chan struct{})
	lateCh := make(chan Result[string], 1)

	res := Guard(20*time.Millisecond, func() (string, error) {
		<-release // simula la cgo bloqueada (Keychain esperando el diálogo)
		return "dek-tardía", nil
	}, func(late Result[string]) {
		lateCh <- late
	})

	if !res.TimedOut {
		t.Fatal("debía marcar TimedOut: la llamada excedió el plazo")
	}
	if res.Value != "" || res.Err != nil {
		t.Fatalf("al abandonar, valor/err deben ir en cero: %q / %v", res.Value, res.Err)
	}
	if res.Elapsed != 20*time.Millisecond {
		t.Fatalf("Elapsed debía ser el timeout, fue %v", res.Elapsed)
	}

	// Aún no debería haber llegado onLate (la llamada sigue bloqueada).
	select {
	case <-lateCh:
		t.Fatal("onLate no debía dispararse mientras la llamada sigue bloqueada")
	case <-time.After(10 * time.Millisecond):
	}

	// Libera la llamada abandonada: onLate debe llegar con el valor real y una duración > timeout.
	close(release)
	select {
	case late := <-lateCh:
		if late.Value != "dek-tardía" || late.Err != nil {
			t.Fatalf("onLate con valor/err inesperados: %q / %v", late.Value, late.Err)
		}
		if late.Elapsed < 20*time.Millisecond {
			t.Fatalf("la duración tardía debía superar el plazo, fue %v", late.Elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("onLate no se disparó tras liberar la llamada")
	}
}

// TestGuard_TimeoutWithoutOnLateDoesNotLeak: sin onLate, abandonar no debe bloquear ni entrar en pánico; la
// goroutine de la llamada muere sola al retornar (el canal bufferizado la desbloquea).
func TestGuard_TimeoutWithoutOnLateDoesNotLeak(t *testing.T) {
	release := make(chan struct{})
	res := Guard(10*time.Millisecond, func() (int, error) {
		<-release
		return 1, nil
	}, nil)
	if !res.TimedOut {
		t.Fatal("debía marcar TimedOut")
	}
	close(release) // la goroutine envía al canal cap-1 y muere sin bloquear.
}

// TestGuard_ZeroTimeoutWaits: timeout <= 0 desactiva la vigilancia y espera a la llamada.
func TestGuard_ZeroTimeoutWaits(t *testing.T) {
	res := Guard(0, func() (int, error) {
		time.Sleep(5 * time.Millisecond)
		return 7, nil
	}, nil)
	if res.TimedOut {
		t.Fatal("timeout<=0 no debe marcar TimedOut nunca")
	}
	if res.Value != 7 {
		t.Fatalf("valor = %d, quería 7", res.Value)
	}
}
