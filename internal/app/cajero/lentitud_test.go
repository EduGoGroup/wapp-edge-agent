package cajero

// lentitud_test.go — EL TEST DEL MP-09: el circuito frente a un Ollama LENTO (no caído).
//
// ─────────────────────────────────────────────────────────────────────────────
// POR QUÉ ESTE FICHERO EXISTE, SI EL BREAKER YA TENÍA NUEVE TESTS VERDES
// ─────────────────────────────────────────────────────────────────────────────
// Los tests de internal/app/breaker calibran el circuito con fallos INSTANTÁNEOS y son correctos: el
// breaker hace exactamente lo que promete. El defecto que el MP-09 persigue no está en ninguna de las
// tres piezas por separado —todas verdes hoy y verdes con el defecto puesto— sino en su INTERACCIÓN:
//
//	contador de fallos consecutivos + umbral  ·  el plazo de UNA inferencia  ·  el semáforo de una plaza
//
// Viven en tres sitios distintos y ningún test los juntaba. Estos tests los juntan: breaker REAL (no el
// doble), plazo de inferencia real, semáforo de una plaza y un guion de latencias MEDIDO EN CAMPO.
//
// 🔴 EL CASO QUE MANDA ES EL DEL ACIERTO INTERCALADO, que ningún test de hoy ejercitaba y que es el que
// impedía la apertura PARA SIEMPRE: un solo éxito pone `failures` a cero, y un Ollama lento acierta de
// vez en cuando. Ver FraccionLentitud para la medición completa.
//
// VERIFICADO POR MUTACIÓN (criterio del MP-09): con FraccionLentitud devuelta al comportamiento anterior
// —o sea, sin criterio de lentitud— TestLentitud_LaSecuenciaMedidaEnCampo y
// TestLentitud_CincoAciertosLentosAbrenElCircuito_SinUnSoloFallo se ponen ROJOS, y lo hacen con el
// mensaje exacto del defecto: el circuito se queda `closed`.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/breaker"
	"github.com/EduGoGroup/wapp-edge-intent/classifier"
)

// timeoutDeLaMedicion es el plazo con el que se midió en campo (y el default del worker). Se escribe
// aquí explícito y no como DefaultInferenceTimeoutMS porque el guion de latencias de abajo SÓLO tiene
// sentido contra este número: si el default cambiara algún día, el guion habría que remedirlo, no
// reajustarlo solo.
const timeoutDeLaMedicion = 15 * time.Second

// relojFalso es un reloj que sólo avanza cuando el guion lo dice. Lo comparten el cajero (Deps.Ahora) y
// el breaker real (New se lo inyecta con WithClock), que es justo lo que hace falta: las dos piezas
// tienen que estar de acuerdo sobre cuánto tiempo pasó.
type relojFalso struct{ nanos atomic.Int64 }

func nuevoRelojFalso() *relojFalso {
	r := &relojFalso{}
	r.nanos.Store(int64(1_700_000_000) * int64(time.Second)) // un epoch realista, no el año cero
	return r
}

func (r *relojFalso) ahora() time.Time        { return time.Unix(0, r.nanos.Load()) }
func (r *relojFalso) avanzar(d time.Duration) { r.nanos.Add(int64(d)) }

// pasoGuion es UNA inferencia del guion: cuánto tarda y si acaba en error.
//
// `falla: true` con dur = el plazo entero es como se escribe un TIMEOUT, y es fiel a lo que el cajero ve
// de uno: un error y el plazo consumido. No se puede provocar el timeout de verdad porque
// context.WithTimeout usa el reloj del SISTEMA y no este; escribir el test con esperas reales costaría
// 15 s por muestra y no probaría nada distinto.
type pasoGuion struct {
	dur   time.Duration
	falla bool
}

func resp(ms int) pasoGuion   { return pasoGuion{dur: time.Duration(ms) * time.Millisecond} }
func plazoAgotado() pasoGuion { return pasoGuion{dur: timeoutDeLaMedicion, falla: true} }

// clasificadorGuionado reproduce una secuencia de inferencias avanzando el reloj falso. Agotado el
// guion responde instantáneo y bien, para que un lote de más no altere lo que el test mide.
type clasificadorGuionado struct {
	reloj *relojFalso
	mu    sync.Mutex
	pasos []pasoGuion
	i     int
}

func (c *clasificadorGuionado) Classify(_ context.Context, _ string) (classifier.Classification, error) {
	c.mu.Lock()
	p := resp(1)
	if c.i < len(c.pasos) {
		p = c.pasos[c.i]
		c.i++
	}
	c.mu.Unlock()

	c.reloj.avanzar(p.dur)
	if p.falla {
		return classifier.Classification{}, context.DeadlineExceeded
	}
	return classifier.Classification{Intent: "crear_pedido", Confidence: 0.9}, nil
}

// correrGuion monta el cajero con el breaker REAL, el semáforo en una plaza y el plazo de la medición, y
// le da de comer tantos lotes como pasos tenga el guion. Devuelve el cajero y el log capturado.
func correrGuion(t *testing.T, pasos []pasoGuion, timeout time.Duration) (*Cajero, *logCaptura) {
	t.Helper()

	var lotes []*app.ColaLote
	for i := range pasos {
		lotes = append(lotes, loteDe(fmt.Sprintf("s%d", i), "quiero una pizza"))
	}
	reloj := nuevoRelojFalso()
	log := &logCaptura{}

	// Breaker: NIL a propósito ⇒ New construye el REAL con la calibración de producción (5 fallos
	// consecutivos / 60 s) y con este mismo reloj. Con el doble (nuevoBreakerFake) este test no probaría
	// nada: el defecto vive en la interacción con el breaker de verdad.
	c, err := correr(t, Deps{
		Cola:          &colaFake{pendientes: lotes},
		Clasificador:  &clasificadorGuionado{reloj: reloj, pasos: pasos},
		Log:           log,
		Ahora:         reloj.ahora,
		MaxConcurrent: 1, // el semáforo, tercera pieza de la interacción
		Timeout:       timeout,
		MaxIntentos:   len(pasos) + 1, // que el freno del lote venenoso no se meta en medio
	}, 3)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return c, log
}

// TestLentitud_LaSecuenciaMedidaEnCampo es el test que da nombre al MP-09: la secuencia EXACTA que el
// VPS de UAT produjo el 2026-08-20 con Ollama lento y un backlog de 240 entrantes.
//
// Hasta el MP-09 esta secuencia dejaba el circuito CERRADO para siempre —racha máxima de timeouts
// consecutivos 2, umbral 5, y cada acierto intercalado borrando el contador—. El acierto de 12.190 ms y
// el de 12.626 ms son los que lo borraban, y son los que ahora suman.
func TestLentitud_LaSecuenciaMedidaEnCampo(t *testing.T) {
	// El guion es la medición, no un ejemplo inventado. Ver FraccionLentitud.
	c, log := correrGuion(t, []pasoGuion{
		plazoAgotado(), // failures 1
		resp(12_190),   // failures 2  ← ANTES: 0
		resp(12_626),   // failures 3  ← ANTES: 0
		plazoAgotado(), // failures 4
		plazoAgotado(), // failures 5  ⇒ ABRE
	}, timeoutDeLaMedicion)

	if c.Circuito() != breaker.StateOpen {
		t.Errorf("EL DEFECTO DEL MP-09 SIGUE VIVO: con la secuencia medida en campo el circuito debe "+
			"quedar ABIERTO, got %q (contando sólo fallos consecutivos nunca llegaba a 5)", c.Circuito())
	}
	if c.AperturasBreaker() != 1 {
		t.Errorf("la apertura se cuenta UNA vez (el flanco), got %d", c.AperturasBreaker())
	}
	// Las dos inferencias que respondieron por encima de 12 s (0,8 × 15 s).
	if c.Lentas() != 2 {
		t.Errorf("Lentas: got %d want 2 (12.190 ms y 12.626 ms)", c.Lentas())
	}
	// 🔴 Y NO 5: los aciertos lentos castigan al breaker pero NO son fallos del lote. Si esto sube, el
	// freno del lote venenoso (MaxIntentos) estaría abandonando mensajes por culpa de la lentitud de
	// Ollama, que es un problema ajeno a ellos.
	if c.Fallos() != 3 {
		t.Errorf("Fallos: got %d want 3 (sólo los timeouts; un acierto lento NO es un fallo del lote)", c.Fallos())
	}
	if _, ok := log.buscar("inferencia LENTA"); !ok {
		t.Error("una inferencia lenta debe dejar su propia línea de Warn antes de la apertura")
	}
}

// TestLentitud_CincoAciertosLentosAbrenElCircuito_SinUnSoloFallo es el caso PURO, y el que enseña que lo
// que cambió es la definición de éxito y no la del umbral: Ollama responde a TODO, no falla ni una vez,
// y el circuito abre igual porque cada respuesta se comió su plazo.
func TestLentitud_CincoAciertosLentosAbrenElCircuito_SinUnSoloFallo(t *testing.T) {
	c, log := correrGuion(t, []pasoGuion{
		resp(13_000), resp(13_100), resp(12_400), resp(14_878), resp(13_500),
	}, timeoutDeLaMedicion)

	if c.Circuito() != breaker.StateOpen {
		t.Errorf("cinco respuestas por encima del umbral abren el circuito aunque NINGUNA falle, got %q", c.Circuito())
	}
	if c.Fallos() != 0 {
		t.Errorf("Fallos: got %d want 0 — aquí Ollama no falló nunca, sólo tardó", c.Fallos())
	}
	if c.Lentas() != 5 {
		t.Errorf("Lentas: got %d want 5", c.Lentas())
	}
	// La CAUSA distingue en campo un Ollama caído de uno lento: los dos dejan la misma línea de apertura.
	e, ok := log.buscar("se ABRIÓ")
	if !ok {
		t.Fatal("la apertura del circuito debe dejar una línea de Warn")
	}
	if !strings.Contains(fmt.Sprint(e.args...), causaLentitud) {
		t.Errorf("la apertura debe declarar causa=%q, args: %v", causaLentitud, e.args)
	}
	// Los cinco lotes se clasificaron y se cerraron: la lentitud castiga al CIRCUITO, no a los mensajes.
	if c.Clasificados() != 5 {
		t.Errorf("Clasificados: got %d want 5 (un acierto lento se cierra con su intent)", c.Clasificados())
	}
}

// TestLentitud_OllamaSano_NiUnPicoAisladoAbreElCircuito es la otra mitad del criterio de cierre, y la que
// impide comprar sensibilidad a cambio de falsos positivos. Es exactamente lo que la salida «bajar el
// umbral de 5 a 3 (o a 2)» no podía sostener.
//
// Las latencias sanas son las que midió la O0 (p50 2.613 ms, p95 3.736 ms) y los picos son de 13 s: tres
// de ellos, nunca dos seguidos. El contador se reinicia en cada respuesta sana y no llega nunca a cinco.
func TestLentitud_OllamaSano_NiUnPicoAisladoAbreElCircuito(t *testing.T) {
	c, _ := correrGuion(t, []pasoGuion{
		resp(2_613), resp(3_736), resp(13_000), // pico
		resp(2_613), resp(3_736), resp(13_000), // pico
		resp(2_613), resp(3_736), resp(13_000), // pico
		resp(2_613), resp(3_736),
	}, timeoutDeLaMedicion)

	if c.Circuito() != breaker.StateClosed {
		t.Errorf("FALSO POSITIVO: con Ollama sano y tres picos AISLADOS el circuito debe seguir cerrado, got %q", c.Circuito())
	}
	if c.AperturasBreaker() != 0 {
		t.Errorf("AperturasBreaker: got %d want 0", c.AperturasBreaker())
	}
	if c.Lentas() != 3 {
		t.Errorf("Lentas: got %d want 3 — los picos SÍ se anotan, simplemente no bastan para abrir", c.Lentas())
	}
	if c.Clasificados() != 11 {
		t.Errorf("Clasificados: got %d want 11", c.Clasificados())
	}
}

// TestLentitud_SinPresupuestoPropio_ElCriterioSeApaga protege la promesa de Deps.Timeout <= 0 («sin plazo
// propio: manda el ctx del proceso»). Sin presupuesto no hay «demasiado tarde» que definir, así que el
// criterio no puede inventarse uno: el breaker vuelve exactamente a la conducta de antes del MP-09.
func TestLentitud_SinPresupuestoPropio_ElCriterioSeApaga(t *testing.T) {
	c, _ := correrGuion(t, []pasoGuion{
		resp(13_000), resp(13_100), resp(12_400), resp(14_878), resp(13_500),
	}, 0) // ← sin plazo propio

	if c.Circuito() != breaker.StateClosed {
		t.Errorf("sin plazo propio no hay umbral de lentitud que aplicar, got %q", c.Circuito())
	}
	if c.Lentas() != 0 {
		t.Errorf("Lentas: got %d want 0", c.Lentas())
	}
}

// TestUmbralLentoDe_SeDerivaDelPresupuesto fija la aritmética por si alguien mueve el plazo: el umbral lo
// sigue, y el caso «sin plazo» devuelve 0 (criterio apagado), no un default inventado.
func TestUmbralLentoDe_SeDerivaDelPresupuesto(t *testing.T) {
	casos := []struct {
		timeout time.Duration
		want    time.Duration
	}{
		{15 * time.Second, 12 * time.Second}, // el default: 0,8 × 15 s
		{30 * time.Second, 24 * time.Second}, // quien sube el plazo sube el umbral con él
		{0, 0},                               // sin plazo propio ⇒ criterio apagado
		{-1, 0},                              // idem, defensivo
	}
	for _, c := range casos {
		if got := umbralLentoDe(c.timeout); got != c.want {
			t.Errorf("umbralLentoDe(%v): got %v want %v", c.timeout, got, c.want)
		}
	}
}

// TestLentitud_ElSondeoLentoDelMedioAbierto_REABRE cierra el hueco que quedaba: qué pasa cuando el
// circuito ya se abrió, cumple su descanso de 60 s y el sondeo de medio-abierto SÍ responde — pero
// tarda lo mismo que antes.
//
// 🔴 SIN ESTO EL ARREGLO SE DESHARÍA SOLO. RecordSuccess borra el contador Y la ventana, así que un
// sondeo lento tratado como éxito cerraría el circuito entero, el cajero volvería a reclamar y el
// episodio empezaría de cero cada minuto: la lentitud «curaría» al breaker una y otra vez. Con el
// criterio puesto, el sondeo lento es un fallo y la ventana se REABRE, que es la conducta que ya tenía
// con Ollama caído.
//
// Va contra registrarAcierto directamente y no por el bucle porque el medio-abierto exige que pasen 60 s
// SIN inferencias, y el guion sólo puede avanzar el reloj clasificando.
func TestLentitud_ElSondeoLentoDelMedioAbierto_REABRE(t *testing.T) {
	reloj := nuevoRelojFalso()
	c, err := New(Deps{
		Cola:         &colaFake{},
		Clasificador: &clasificadorFake{},
		Log:          &logCaptura{},
		Ahora:        reloj.ahora,
		Timeout:      timeoutDeLaMedicion,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for range breaker.DefaultThreshold {
		c.registrarAcierto(13 * time.Second)
	}
	if c.Circuito() != breaker.StateOpen {
		t.Fatalf("precondición: el circuito debe estar abierto, got %q", c.Circuito())
	}

	reloj.avanzar(breaker.DefaultOpenFor + time.Second) // vence el descanso
	if c.Circuito() != breaker.StateHalfOpen {
		t.Fatalf("vencida la ventana el circuito debe estar medio-abierto, got %q", c.Circuito())
	}

	// El sondeo responde, y tarda lo mismo que antes: Ollama sigue enfermo.
	c.registrarAcierto(13 * time.Second)
	if c.Circuito() != breaker.StateOpen {
		t.Errorf("un sondeo LENTO debe REABRIR la ventana, no cerrar el circuito, got %q", c.Circuito())
	}
	if c.AperturasBreaker() != 2 {
		t.Errorf("cada reapertura es un evento operativo propio: got %d want 2", c.AperturasBreaker())
	}

	// Y el contrapunto: un sondeo RÁPIDO sí cierra. Sin esto, el circuito no podría recuperarse nunca.
	reloj.avanzar(breaker.DefaultOpenFor + time.Second)
	c.registrarAcierto(2 * time.Second)
	if c.Circuito() != breaker.StateClosed {
		t.Errorf("un sondeo dentro de plazo CIERRA el circuito, got %q", c.Circuito())
	}
}
