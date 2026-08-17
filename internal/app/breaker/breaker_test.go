package breaker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// relojFijo es un reloj controlado por el test: el medio-abierto se observa avanzando el puntero, no
// esperando 60 s reales. No lleva lock porque cada test que lo avanza lo hace de forma SERIAL (el de
// concurrencia usa el reloj real precisamente para no introducir una carrera en el propio test).
type relojFijo struct{ t time.Time }

func (r *relojFijo) ahora() time.Time {
	return r.t
}

func (r *relojFijo) avanzar(d time.Duration) {
	r.t = r.t.Add(d)
}

func nuevoReloj() *relojFijo { return &relojFijo{t: time.Unix(1_700_000_000, 0)} }

// TestNew_DefaultsAnteValoresNoPositivos fija la calibración heredada: un 0 no puede degradar el
// breaker a «abierto desde el primer fallo» ni a «ventana nula».
func TestNew_DefaultsAnteValoresNoPositivos(t *testing.T) {
	b := New(0, 0)
	if b.threshold != DefaultThreshold {
		t.Errorf("threshold: got %d want %d", b.threshold, DefaultThreshold)
	}
	if b.openFor != DefaultOpenFor {
		t.Errorf("openFor: got %v want %v", b.openFor, DefaultOpenFor)
	}
	if DefaultThreshold != 5 {
		t.Errorf("la calibración del Plan 029 es 5 fallos consecutivos, got %d", DefaultThreshold)
	}
	if DefaultOpenFor != 60*time.Second {
		t.Errorf("la calibración del Plan 029 es una ventana de 60 s, got %v", DefaultOpenFor)
	}
}

// TestUmbralExacto comprueba que el circuito abre EN el quinto fallo y no en el cuarto: el off-by-one
// aquí no rompe nada visible, solo cambia la calibración en silencio.
func TestUmbralExacto(t *testing.T) {
	r := nuevoReloj()
	b := New(DefaultThreshold, DefaultOpenFor, WithClock(r.ahora))

	for i := 1; i < DefaultThreshold; i++ {
		if !b.BeginAttempt() {
			t.Fatalf("fallo %d: el circuito debe seguir permitiendo intentos", i)
		}
		b.RecordFailure()
		if got := b.State(); got != StateClosed {
			t.Fatalf("tras %d fallos el circuito debe seguir cerrado, got %q", i, got)
		}
	}

	if !b.BeginAttempt() {
		t.Fatal("el quinto intento aún debe permitirse (el circuito abre DESPUÉS del quinto fallo)")
	}
	b.RecordFailure()
	if got := b.State(); got != StateOpen {
		t.Fatalf("tras %d fallos el circuito debe estar abierto, got %q", DefaultThreshold, got)
	}
	if b.BeginAttempt() {
		t.Error("con el circuito abierto no se permite ningún intento")
	}
}

// TestVentanaDe60s comprueba que la ventana dura exactamente openFor: a los 59 s sigue abierto y a los
// 60 s pasa a medio-abierto.
func TestVentanaDe60s(t *testing.T) {
	r := nuevoReloj()
	b := New(DefaultThreshold, DefaultOpenFor, WithClock(r.ahora))
	for range DefaultThreshold {
		b.RecordFailure()
	}

	r.avanzar(59 * time.Second)
	if got := b.State(); got != StateOpen {
		t.Fatalf("a los 59 s el circuito debe seguir abierto, got %q", got)
	}
	if b.BeginAttempt() {
		t.Error("a los 59 s no se permite ningún intento")
	}

	r.avanzar(time.Second) // justo en el borde: now == openUntil ⇒ ya NO es Before ⇒ medio-abierto
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("a los 60 s el circuito debe estar medio-abierto, got %q", got)
	}
}

// TestMedioAbierto_UnSoloSondeo es la propiedad más fácil de romper al refactorizar: con el circuito
// medio-abierto sólo UNO de los llamantes pasa; el resto siguen rechazados hasta que ese sondeo se
// resuelve.
func TestMedioAbierto_UnSoloSondeo(t *testing.T) {
	r := nuevoReloj()
	b := New(DefaultThreshold, DefaultOpenFor, WithClock(r.ahora))
	for range DefaultThreshold {
		b.RecordFailure()
	}
	r.avanzar(DefaultOpenFor)

	if !b.BeginAttempt() {
		t.Fatal("el medio-abierto debe dejar pasar el primer sondeo")
	}
	for i := range 3 {
		if b.BeginAttempt() {
			t.Fatalf("intento extra %d: con un sondeo en curso no debe pasar nadie más", i)
		}
	}

	// El sondeo falla ⇒ reabre la ventana y libera el flag: a los 60 s siguientes hay OTRO sondeo.
	b.RecordFailure()
	if got := b.State(); got != StateOpen {
		t.Fatalf("un sondeo fallido debe REABRIR la ventana, got %q", got)
	}
	r.avanzar(DefaultOpenFor)
	if !b.BeginAttempt() {
		t.Error("tras reabrir y vencer la ventana debe haber un sondeo nuevo")
	}
}

// TestExitoResetea comprueba las dos mitades del reset: el éxito en medio-abierto CIERRA, y un éxito
// intercalado borra la racha (los fallos son consecutivos, no acumulados).
func TestExitoResetea(t *testing.T) {
	r := nuevoReloj()
	b := New(DefaultThreshold, DefaultOpenFor, WithClock(r.ahora))

	// Racha rota por un éxito: 4 fallos + éxito + 4 fallos NO abren.
	for range DefaultThreshold - 1 {
		b.RecordFailure()
	}
	b.RecordSuccess()
	for range DefaultThreshold - 1 {
		b.RecordFailure()
	}
	if got := b.State(); got != StateClosed {
		t.Fatalf("un éxito intercalado rompe la racha: el circuito debe seguir cerrado, got %q", got)
	}

	// Éxito en medio-abierto ⇒ cierra.
	b.RecordFailure() // quinto ⇒ abre
	if got := b.State(); got != StateOpen {
		t.Fatalf("el circuito debe estar abierto, got %q", got)
	}
	r.avanzar(DefaultOpenFor)
	if !b.BeginAttempt() {
		t.Fatal("medio-abierto debe permitir el sondeo")
	}
	b.RecordSuccess()
	if got := b.State(); got != StateClosed {
		t.Errorf("un éxito en medio-abierto debe CERRAR el circuito, got %q", got)
	}
	if !b.BeginAttempt() {
		t.Error("con el circuito cerrado se permiten intentos sin límite")
	}
}

// TestConcurrencia_SinCarreras ejercita el breaker desde N goroutines para que `-race` tenga algo que
// mirar, y de paso comprueba la invariante que sí es observable bajo concurrencia: en medio-abierto
// exactamente UN sondeo pasa, ni cero ni dos.
func TestConcurrencia_SinCarreras(t *testing.T) {
	b := New(DefaultThreshold, DefaultOpenFor) // reloj REAL: el test no lo avanza, así que no hay carrera propia

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if b.BeginAttempt() {
					b.RecordSuccess()
				}
				_ = b.State()
			}
		}()
	}
	wg.Wait()
	if got := b.State(); got != StateClosed {
		t.Errorf("sólo hubo éxitos: el circuito debe estar cerrado, got %q", got)
	}

	// Ahora el sondeo único bajo concurrencia real: se abre el circuito con una ventana ya vencida
	// (openFor mínimo) y se lanzan N BeginAttempt a la vez; sólo uno puede recibir true.
	b2 := New(DefaultThreshold, time.Nanosecond)
	for range DefaultThreshold {
		b2.RecordFailure()
	}
	time.Sleep(time.Millisecond) // la ventana de 1 ns ya venció: estado medio-abierto

	var pasaron atomic.Int64
	var arranque sync.WaitGroup
	arranque.Add(1)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			arranque.Wait()
			if b2.BeginAttempt() {
				pasaron.Add(1)
			}
		}()
	}
	arranque.Done()
	wg.Wait()
	if got := pasaron.Load(); got != 1 {
		t.Errorf("en medio-abierto debe pasar EXACTAMENTE un sondeo, pasaron %d", got)
	}
}

// TestRecordFailure_DevuelveElFlanco fija el contrato del bool: true SÓLO en la llamada que lleva el
// circuito de no-abierto a ABIERTO. Es lo que hace contable la métrica `aperturas_breaker` del cajero;
// si esto se rompe, el contador pasa a medir «fallos con el circuito abierto», que no es lo mismo.
func TestRecordFailure_DevuelveElFlanco(t *testing.T) {
	r := nuevoReloj()
	b := New(DefaultThreshold, DefaultOpenFor, WithClock(r.ahora))

	for i := 1; i < DefaultThreshold; i++ {
		if b.RecordFailure() {
			t.Fatalf("fallo %d: el circuito aún no abre, no puede haber flanco", i)
		}
	}
	if !b.RecordFailure() {
		t.Fatal("el fallo que alcanza el umbral SÍ es el flanco: debe devolver true")
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("tras el flanco el circuito debe estar abierto, got %q", got)
	}

	// Con el circuito YA abierto, más fallos no vuelven a abrirlo: no hay flanco que contar.
	for i := range 3 {
		if b.RecordFailure() {
			t.Errorf("fallo extra %d con el circuito ya abierto: no debe haber flanco", i)
		}
	}
}

// TestRecordFailure_FlancoDesdeMedioAbierto: el sondeo fallido de medio-abierto SÍ es un flanco (el
// estado previo era half-open, no open) y reabre la ventana. Es la semántica que el cajero heredó de
// su secuencia State/RecordFailure/State, y se conserva a propósito.
func TestRecordFailure_FlancoDesdeMedioAbierto(t *testing.T) {
	r := nuevoReloj()
	b := New(DefaultThreshold, DefaultOpenFor, WithClock(r.ahora))
	for range DefaultThreshold - 1 {
		b.RecordFailure()
	}
	if !b.RecordFailure() {
		t.Fatal("el fallo del umbral debe devolver el flanco")
	}

	r.avanzar(DefaultOpenFor) // la ventana venció: medio-abierto
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("se esperaba medio-abierto, got %q", got)
	}
	if !b.BeginAttempt() {
		t.Fatal("el medio-abierto debe dejar pasar el sondeo")
	}
	if !b.RecordFailure() {
		t.Error("un sondeo fallido REABRE la ventana desde half-open: eso es un flanco y debe contarse")
	}

	// Y un éxito que cierra el circuito devuelve el flanco a su sitio: el siguiente ciclo completo de
	// fallos vuelve a producir exactamente UN true.
	r.avanzar(DefaultOpenFor)
	if !b.BeginAttempt() {
		t.Fatal("nuevo sondeo tras la reapertura")
	}
	b.RecordSuccess()
	flancos := 0
	for range DefaultThreshold {
		if b.RecordFailure() {
			flancos++
		}
	}
	if flancos != 1 {
		t.Errorf("un ciclo completo de fallos produce UN solo flanco, hubo %d", flancos)
	}
}

// TestRecordFailure_FlancoBajoConcurrencia es el motivo de que el bool viva dentro del breaker: con N
// goroutines fallando a la vez, el número de flancos observados debe ser EXACTAMENTE 1 (una sola
// apertura). La versión anterior —State()/RecordFailure()/State() desde el cajero, con su propio
// mutex— podía contar de más o de menos porque la secuencia no era atómica respecto del breaker.
func TestRecordFailure_FlancoBajoConcurrencia(t *testing.T) {
	b := New(DefaultThreshold, DefaultOpenFor) // reloj real: la ventana de 60 s no vence durante el test

	var flancos atomic.Int64
	var arranque, wg sync.WaitGroup
	arranque.Add(1)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			arranque.Wait()
			if b.RecordFailure() {
				flancos.Add(1)
			}
		}()
	}
	arranque.Done()
	wg.Wait()

	if got := flancos.Load(); got != 1 {
		t.Errorf("32 fallos concurrentes abren el circuito UNA vez, se contaron %d aperturas", got)
	}
	if got := b.State(); got != StateOpen {
		t.Errorf("el circuito debe quedar abierto, got %q", got)
	}
}
