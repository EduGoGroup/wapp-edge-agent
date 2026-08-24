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
//	contador de fallos consecutivos + umbral  ·  el plazo de UNA inferencia  ·  el aforo de una plaza
//
// Viven en tres sitios distintos y ningún test los juntaba. Estos tests los juntan: breaker REAL (no el
// doble), plazo de inferencia real, aforo de una plaza y un guion de latencias MEDIDO EN CAMPO.
//
// 🔴 EL CASO QUE MANDA ES EL DEL ACIERTO INTERCALADO, que ningún test de hoy ejercitaba y que es el que
// impedía la apertura PARA SIEMPRE: un solo éxito pone `failures` a cero, y un Ollama lento acierta de
// vez en cuando. Ver FraccionLentitud para la medición completa.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 QUÉ CAMBIÓ EN T1.6-2 (Plan 044 · Ola 1.6, ADR-0045) Y QUÉ NO
// ─────────────────────────────────────────────────────────────────────────────
// NO CAMBIÓ EL SUJETO: `registrarAcierto` sigue vivo, sigue siendo donde vive el criterio del MP-09 y
// sigue teniendo la última palabra sobre lo que el breaker aprende de una inferencia que RESPONDIÓ.
//
// CAMBIÓ QUIÉN LO LLAMA: era el bucle, que clasificaba por iniciativa propia; hoy es el SERVIDOR DE
// INFERENCIA, que atiende peticiones del Cloud. Por eso el guion ya no se le da de comer a la cola en
// forma de lotes, sino al servidor en forma de `PeticionInferencia` — un cambio de cableado del test que
// deja la propiedad medida intacta y que además la mide más cerca: sin el bucle en medio, lo que se
// ejercita es exactamente la ruta que el ADR-0045 dejó en pie.
//
// VERIFICADO POR MUTACIÓN (criterio del MP-09): con FraccionLentitud devuelta al comportamiento anterior
// —o sea, sin criterio de lentitud— TestLentitud_LaSecuenciaMedidaEnCampo y
// TestLentitud_CincoAciertosLentosAbrenElCircuito_SinUnSoloFallo se ponen ROJOS, y lo hacen con el
// mensaje exacto del defecto: el circuito se queda `closed`.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/breaker"
	"github.com/EduGoGroup/wapp-edge-intent/ollama"
)

// timeoutDeLaMedicion es el plazo con el que se midió en campo el episodio del MP-09 (2026-08-20). Se
// escribe aquí explícito y NO como DefaultInferenceTimeoutMS —que hoy vale 45 s, no 15— porque el guion
// de latencias de abajo sólo tiene sentido contra ESTE número: las latencias son las que aquella máquina
// produjo con este presupuesto encima, y un guion re-escalado a otro plazo sería un ejemplo inventado.
// Si algún día hay que probar el criterio con el plazo nuevo, lo que hace falta es REMEDIR, no reajustar.
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
// 🔴 CÓMO ATERRIZA HOY UN PASO `falla: true`, y por qué el nombre del ayudante cambió. Con reloj falso no
// se puede provocar un timeout de VERDAD: `context.WithTimeout` usa el reloj del SISTEMA y no éste, así
// que el contexto de la inferencia nunca vence y `registrarFalloDeInferencia` clasifica el paso como
// OLLAMA_DOWN, no como TIMEOUT. Para lo que estos tests miden da exactamente igual —los dos desenlaces
// pasan por `registrarFallo`, que es lo único que el breaker ve—, pero el nombre viejo (`plazoAgotado`)
// prometía un camino por el que el test ya no pasa, y un nombre que miente cuesta más que uno feo.
// Escribir el test con esperas reales costaría 15 s por muestra y no probaría nada distinto.
type pasoGuion struct {
	dur   time.Duration
	falla bool
}

func resp(ms int) pasoGuion { return pasoGuion{dur: time.Duration(ms) * time.Millisecond} }

// falloTrasElPlazo es una inferencia que consume el plazo entero y acaba en error: es como el cajero ve
// un timeout —tiempo gastado y nada que servir— y cuenta lo mismo para el breaker (ver pasoGuion).
func falloTrasElPlazo() pasoGuion { return pasoGuion{dur: timeoutDeLaMedicion, falla: true} }

// chateadorGuionado reproduce una secuencia de inferencias avanzando el reloj falso. Agotado el guion
// responde instantáneo y bien, para que una petición de más no altere lo que el test mide.
type chateadorGuionado struct {
	reloj *relojFalso
	mu    sync.Mutex
	pasos []pasoGuion
	i     int
}

var _ Chateador = (*chateadorGuionado)(nil)

func (c *chateadorGuionado) Chat(_ context.Context, _ ollama.ChatRequest) (*ollama.ChatResponse, error) {
	c.mu.Lock()
	p := resp(1)
	if c.i < len(c.pasos) {
		p = c.pasos[c.i]
		c.i++
	}
	c.mu.Unlock()

	c.reloj.avanzar(p.dur)
	if p.falla {
		return nil, errors.New("el proveedor local no respondió")
	}
	return &ollama.ChatResponse{
		Message: ollama.Message{Role: "assistant", Content: `{"intent":"crear_pedido"}`},
		Done:    true,
	}, nil
}

func (c *chateadorGuionado) SupportsThinking(_ context.Context, _ string) bool { return false }

// correrGuion monta el cajero con el breaker REAL, el aforo en una plaza y el presupuesto de la
// medición, y le pide al servidor de inferencia tantas inferencias como pasos tenga el guion.
//
// 🔴 EL PLAZO DE CADA PETICIÓN LO PONE EL «CLOUD» (p.Timeout) Y NO Deps.Timeout, y esa separación es la
// que hace que el último caso del fichero se pueda escribir: desde el ADR-0045 `Deps.Timeout` gobierna
// SÓLO dos cosas —el default cuando el Cloud no fija plazo, y el umbral de lentitud que se deriva de
// él—, así que se le puede pasar 0 para apagar el criterio del MP-09 sin dejar a las peticiones sin
// presupuesto.
//
// Las inferencias se piden EN SERIE a propósito: el guion es una secuencia temporal (el orden de los
// aciertos y los fallos es lo que decide si el circuito abre) y el reloj falso es compartido.
func correrGuion(t *testing.T, pasos []pasoGuion, umbral time.Duration) (*Cajero, *logCaptura) {
	t.Helper()

	reloj := nuevoRelojFalso()
	log := &logCaptura{}

	// Breaker: NIL a propósito ⇒ New construye el REAL con la calibración de producción (5 fallos
	// consecutivos / 60 s) y con este mismo reloj. Con el doble (nuevoBreakerFake) este test no probaría
	// nada: el defecto vive en la interacción con el breaker de verdad.
	c, s := servidorDe(t, Deps{
		Cola:          &colaFake{},
		Ollama:        &chateadorGuionado{reloj: reloj, pasos: pasos},
		Log:           log,
		Ahora:         reloj.ahora,
		MaxConcurrent: 1, // el aforo, tercera pieza de la interacción
		Timeout:       umbral,
	})

	for i := range pasos {
		_, err := s.Inferir(context.Background(), peticionDe("clasifica esto", timeoutDeLaMedicion))
		// El error se IGNORA a propósito y sólo aquí: el guion decide qué pasos fallan, y lo que el test
		// mide es el estado en que quedan el circuito y los contadores, no el desenlace de cada llamada.
		// Se comprueba, eso sí, que no sea uno que delate un test mal montado.
		if errors.Is(err, app.ErrInferenciaSinCapacidad) {
			t.Fatalf("el paso %d se quedó SIN PLAZA con el aforo libre: el guion no está midiendo lo que dice", i)
		}
	}
	return c, log
}

// TestLentitud_LaSecuenciaMedidaEnCampo es el test que da nombre al MP-09: la secuencia EXACTA que el
// VPS de UAT produjo el 2026-08-20 con Ollama lento y un backlog de 240 entrantes.
//
// Hasta el MP-09 esta secuencia dejaba el circuito CERRADO para siempre —racha máxima de fallos
// consecutivos 2, umbral 5, y cada acierto intercalado borrando el contador—. El acierto de 12.190 ms y
// el de 12.626 ms son los que lo borraban, y son los que ahora suman.
func TestLentitud_LaSecuenciaMedidaEnCampo(t *testing.T) {
	// El guion es la medición, no un ejemplo inventado. Ver FraccionLentitud.
	c, log := correrGuion(t, []pasoGuion{
		falloTrasElPlazo(), // failures 1
		resp(12_190),       // failures 2  ← ANTES: 0
		resp(12_626),       // failures 3  ← ANTES: 0
		falloTrasElPlazo(), // failures 4
		falloTrasElPlazo(), // failures 5  ⇒ ABRE
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
	// 🔴 Y NO 5: los aciertos lentos castigan al breaker pero NO son fallos del proveedor. Mezclarlos
	// haría imposible distinguir en el log un circuito abierto por LENTITUD de uno abierto por CAÍDA, que
	// es la única razón por la que `lentas` es un contador aparte.
	if c.Fallos() != 3 {
		t.Errorf("Fallos: got %d want 3 (sólo los fallos de verdad; un acierto lento NO lo es)", c.Fallos())
	}
	// Y las dos lentas SÍ se sirvieron: la salida subió al Cloud. La lentitud castiga al CIRCUITO, nunca
	// a la respuesta que ya se pagó.
	if c.Servidas() != 2 {
		t.Errorf("Servidas: got %d want 2 (las dos inferencias lentas respondieron y se sirvieron)", c.Servidas())
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
	// Las cinco se sirvieron: la lentitud castiga al CIRCUITO, no a las respuestas.
	if c.Servidas() != 5 {
		t.Errorf("Servidas: got %d want 5 (un acierto lento se sirve igual)", c.Servidas())
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
	if c.Servidas() != 11 {
		t.Errorf("Servidas: got %d want 11", c.Servidas())
	}
}

// TestLentitud_SinPresupuestoPropio_ElCriterioSeApaga protege la promesa de Deps.Timeout <= 0 en lo que
// hoy gobierna: el UMBRAL DE LENTITUD. Sin presupuesto propio no hay «demasiado tarde» que definir, así
// que el criterio no puede inventarse uno y el breaker vuelve exactamente a la conducta de antes del
// MP-09.
//
// ⚠️ Las peticiones SÍ llevan plazo (se lo pone el Cloud, ver correrGuion): lo que se apaga con
// `Deps.Timeout = 0` es el umbral derivado, no el presupuesto de cada inferencia. Los dos eran lo mismo
// antes del ADR-0045 y desde entonces no lo son.
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
	if c.Servidas() != 5 {
		t.Errorf("Servidas: got %d want 5 (sin umbral las cinco son aciertos a secas)", c.Servidas())
	}
}

// TestUmbralLentoDe_SeDerivaDelPresupuesto fija la aritmética por si alguien mueve el plazo: el umbral lo
// sigue, y el caso «sin plazo» devuelve 0 (criterio apagado), no un default inventado.
func TestUmbralLentoDe_SeDerivaDelPresupuesto(t *testing.T) {
	casos := []struct {
		timeout time.Duration
		want    time.Duration
	}{
		{15 * time.Second, 12 * time.Second}, // el plazo de la medición del MP-09: 0,8 × 15 s
		{45 * time.Second, 36 * time.Second}, // el default de hoy (DefaultInferenceTimeoutMS)
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
// sondeo lento tratado como éxito cerraría el circuito entero, el Edge volvería a aceptar inferencias y
// el episodio empezaría de cero cada minuto: la lentitud «curaría» al breaker una y otra vez. Con el
// criterio puesto, el sondeo lento es un fallo y la ventana se REABRE, que es la conducta que ya tenía
// con Ollama caído.
//
// Va contra registrarAcierto directamente y no por el servidor porque el medio-abierto exige que pasen
// 60 s SIN inferencias, y el guion sólo puede avanzar el reloj sirviéndolas.
func TestLentitud_ElSondeoLentoDelMedioAbierto_REABRE(t *testing.T) {
	reloj := nuevoRelojFalso()
	c, err := New(Deps{
		Cola:    &colaFake{},
		Ollama:  &chateadorFake{},
		Log:     &logCaptura{},
		Ahora:   reloj.ahora,
		Timeout: timeoutDeLaMedicion,
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
