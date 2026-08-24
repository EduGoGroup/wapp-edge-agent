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
// medición, y le pide al servidor de inferencia tantas inferencias como pasos tenga el guion. Los dos
// plazos —el del proceso y el de las peticiones— valen lo mismo, que es el caso de siempre.
func correrGuion(t *testing.T, pasos []pasoGuion, plazo time.Duration) (*Cajero, *logCaptura) {
	t.Helper()
	return correrGuionCon(t, pasos, plazo, plazo)
}

// correrGuionCon es la versión que SEPARA los dos plazos, y esa separación es lo que hace medibles los
// casos de T1.7-2.
//
// 🔴 SON DOS NÚMEROS DISTINTOS Y ESE ES EL SUJETO DEL FICHERO DESDE T1.7-2:
//
//   - `plazoPropio` es Deps.Timeout, el DEFAULT del Edge. Desde T1.7-2 gobierna una sola cosa: qué plazo
//     se le pone a una petición que llegue sin él.
//   - `plazoPeticion` es el `timeout_ms` que manda el Cloud, y es DE ÉL de quien sale el umbral de
//     lentitud con el que se juzga ESA inferencia.
//
// Antes de T1.7-2 el umbral salía del primero para todas, y esa es exactamente la mutación que estos
// tests cazan.
//
// Las inferencias se piden EN SERIE a propósito: el guion es una secuencia temporal (el orden de los
// aciertos y los fallos es lo que decide si el circuito abre) y el reloj falso es compartido.
func correrGuionCon(t *testing.T, pasos []pasoGuion, plazoPropio, plazoPeticion time.Duration) (*Cajero, *logCaptura) {
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
		Timeout:       plazoPropio,
	})

	for i := range pasos {
		_, err := s.Inferir(context.Background(), peticionDe("clasifica esto", plazoPeticion))
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

// TestLentitud_SinPlazoEFECTIVO_ElCriterioSeApaga protege la promesa de «sin plazo no hay criterio»: sin
// presupuesto no hay «demasiado tarde» que definir, así que el criterio no puede inventarse uno y el
// breaker vuelve exactamente a la conducta de antes del MP-09.
//
// 🔴 QUÉ CAMBIÓ EN T1.7-2, Y ES UN CAMBIO DE SEMÁNTICA DECLARADO. Antes bastaba `Deps.Timeout = 0` para
// apagarlo, porque el umbral salía de ahí y de ningún otro sitio. Hoy sale del plazo EFECTIVO de cada
// petición, así que apagarlo exige las DOS cosas: que el Cloud tampoco fije `timeout_ms`. Y es lo
// correcto: una petición que llega con 15 s encima TIENE un plazo, lo haya puesto quien lo haya puesto, y
// tratarla como si no lo tuviera era precisamente el defecto —el Edge miraba su propia configuración para
// juzgar un presupuesto que ya no era suyo—.
//
// El caso complementario —`Deps.Timeout = 0` pero el Cloud SÍ fija plazo, que antes apagaba el criterio y
// ahora no— es el que mide TestLentitud_ElDefaultDelProcesoNoGobierna.
func TestLentitud_SinPlazoEFECTIVO_ElCriterioSeApaga(t *testing.T) {
	c, _ := correrGuionCon(t, []pasoGuion{
		resp(13_000), resp(13_100), resp(12_400), resp(14_878), resp(13_500),
	}, 0, 0) // ← ni plazo propio ni plazo del Cloud

	if c.Circuito() != breaker.StateClosed {
		t.Errorf("sin plazo EFECTIVO no hay umbral de lentitud que aplicar, got %q", c.Circuito())
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
		c.registrarAcierto(13*time.Second, timeoutDeLaMedicion)
	}
	if c.Circuito() != breaker.StateOpen {
		t.Fatalf("precondición: el circuito debe estar abierto, got %q", c.Circuito())
	}

	reloj.avanzar(breaker.DefaultOpenFor + time.Second) // vence el descanso
	if c.Circuito() != breaker.StateHalfOpen {
		t.Fatalf("vencida la ventana el circuito debe estar medio-abierto, got %q", c.Circuito())
	}

	// El sondeo responde, y tarda lo mismo que antes: Ollama sigue enfermo.
	c.registrarAcierto(13*time.Second, timeoutDeLaMedicion)
	if c.Circuito() != breaker.StateOpen {
		t.Errorf("un sondeo LENTO debe REABRIR la ventana, no cerrar el circuito, got %q", c.Circuito())
	}
	if c.AperturasBreaker() != 2 {
		t.Errorf("cada reapertura es un evento operativo propio: got %d want 2", c.AperturasBreaker())
	}

	// Y el contrapunto: un sondeo RÁPIDO sí cierra. Sin esto, el circuito no podría recuperarse nunca.
	reloj.avanzar(breaker.DefaultOpenFor + time.Second)
	c.registrarAcierto(2*time.Second, timeoutDeLaMedicion)
	if c.Circuito() != breaker.StateClosed {
		t.Errorf("un sondeo dentro de plazo CIERRA el circuito, got %q", c.Circuito())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// T1.7-2 · EL UMBRAL ES DE LA PETICIÓN, NO DEL PROCESO (Plan 044 · Ola 1.7)
// ─────────────────────────────────────────────────────────────────────────────
//
// Los plazos de abajo no son ejemplos redondos: son las DOS VÍAS del ADR-0044 conviviendo en el mismo
// Edge, que es lo que rompió el umbral único.
const (
	// plazoDeLote es el `timeout_ms` de la vía B/C: un lote puede tardar, para eso se sacó a un proceso
	// aparte. Su umbral de lentitud son 72 s (0,8 × 90).
	plazoDeLote = 90 * time.Second
	// plazoInteractivo es el `timeout_ms` de la vía A: hay una persona esperando al otro lado y el plazo
	// es corto a propósito. Su umbral son 8 s (0,8 × 10).
	plazoInteractivo = 10 * time.Second
)

// TestLentitud_LaMISMALatenciaEsSanaOEnfermaSEGUNSuPlazo es el criterio (a) de T1.7-2, y la propiedad
// entera cabe en su nombre: VEINTE SEGUNDOS no son ni sanos ni enfermos por sí mismos. Lo son contra un
// plazo, y con dos vías vivas (ADR-0044) hay dos plazos distintos a la vez.
//
// Cinco respuestas idénticas de 20 s, dos veredictos opuestos:
//
//	timeout_ms = 90.000 (lote)         → umbral 72 s → 20 s va HOLGADA      → circuito CERRADO
//	timeout_ms = 10.000 (interactiva)  → umbral  8 s → 20 s se comió TODO   → circuito ABIERTO
//
// ⚠️ CÓMO ATERRIZA EL SEGUNDO CASO, para que nadie lo lea como una promesa de campo: con reloj falso el
// `context.WithTimeout` de la inferencia NO vence (usa el reloj del sistema, ver pasoGuion), así que la
// respuesta de 20 s llega hasta `registrarAcierto`. En campo esa misma inferencia habría muerto por
// TIMEOUT a los 10 s y habría castigado al breaker por el otro camino. Lo que este test mide es el
// VEREDICTO del criterio de lentitud, que es lo que T1.7-2 cambió; el caso realista —una respuesta que
// llega DENTRO de su plazo pero rozándolo— es el de abajo, TestLentitud_NueveComaNueveSobreDiez.
func TestLentitud_LaMISMALatenciaEsSanaOEnfermaSEGUNSuPlazo(t *testing.T) {
	guion := func() []pasoGuion {
		return []pasoGuion{resp(20_000), resp(20_000), resp(20_000), resp(20_000), resp(20_000)}
	}

	// El plazo propio se deja en el default de hoy (45 s ⇒ umbral 36 s) EN LOS DOS CASOS a propósito: es
	// un número que no explica ninguno de los dos veredictos, así que si algo los explicara sería él.
	porDefecto := DefaultInferenceTimeoutMS * time.Millisecond

	lote, _ := correrGuionCon(t, guion(), porDefecto, plazoDeLote)
	if lote.Lentas() != 0 {
		t.Errorf("bajo plazo de LOTE (90 s ⇒ umbral 72 s) una respuesta de 20 s va holgada: Lentas got %d want 0",
			lote.Lentas())
	}
	if lote.Circuito() != breaker.StateClosed {
		t.Errorf("bajo plazo de lote el circuito debe seguir CERRADO, got %q", lote.Circuito())
	}
	if lote.Servidas() != 5 {
		t.Errorf("Servidas: got %d want 5", lote.Servidas())
	}

	interactiva, _ := correrGuionCon(t, guion(), porDefecto, plazoInteractivo)
	if interactiva.Lentas() != 5 {
		t.Errorf("bajo plazo INTERACTIVO (10 s ⇒ umbral 8 s) esas MISMAS respuestas de 20 s son lentas: "+
			"Lentas got %d want 5", interactiva.Lentas())
	}
	if interactiva.Circuito() != breaker.StateOpen {
		t.Errorf("cinco respuestas por encima del umbral de SU plazo abren el circuito, got %q", interactiva.Circuito())
	}
	if interactiva.Fallos() != 0 {
		t.Errorf("Fallos: got %d want 0 — el proveedor no falló ni una vez, sólo tardó", interactiva.Fallos())
	}
}

// TestLentitud_NueveComaNueveSobreDiez es el criterio (b), y es EL DEFECTO que esta tarea cerró contado
// con un solo número. Una interactiva con 10 s de presupuesto que responde a los 9.900 ms se quedó a
// 100 ms de morir: es la definición literal de «se comió su plazo». Hasta T1.7-2 el breaker la contaba
// como ÉXITO —9,9 s está muy por debajo de los 36 s que salían del default del proceso— y encima BORRABA
// la racha de fallos acumulada, que es el mecanismo exacto que el MP-09 encontró roto, reaparecido en la
// vía estrecha.
//
// Cinco seguidas y el circuito abre, sin un solo fallo del proveedor.
func TestLentitud_NueveComaNueveSobreDiez(t *testing.T) {
	c, log := correrGuionCon(t, []pasoGuion{
		resp(9_900), resp(9_900), resp(9_900), resp(9_900), resp(9_900),
	}, DefaultInferenceTimeoutMS*time.Millisecond, plazoInteractivo)

	if c.Lentas() != 5 {
		t.Errorf("una respuesta de 9,9 s con 10 s de plazo se comió su presupuesto y cuenta como LENTA: "+
			"Lentas got %d want 5 (hasta T1.7-2 pasaban las cinco por sanas)", c.Lentas())
	}
	if c.Circuito() != breaker.StateOpen {
		t.Errorf("cinco respuestas al borde de su plazo abren el circuito, got %q", c.Circuito())
	}
	if c.Servidas() != 5 {
		t.Errorf("Servidas: got %d want 5 — la lentitud castiga al CIRCUITO, no a la respuesta ya pagada", c.Servidas())
	}

	// La línea tiene que decir contra QUÉ se la juzgó, o en campo no se puede distinguir de una lenta de
	// lote: la latencia sola no basta cuando hay dos plazos vivos.
	e, ok := log.buscar("inferencia LENTA")
	if !ok {
		t.Fatal("una inferencia lenta debe dejar su propia línea de Warn")
	}
	claves := clavesDe(e)
	for _, k := range []string{"latencia_ms", "umbral_lento_ms", "plazo_ms"} {
		if !claves[k] {
			t.Errorf("la línea de lenta debe llevar %q para poder auditarse sola, args: %v", k, e.args)
		}
	}
}

// TestLentitud_ElDefaultDelProcesoNoGobierna es EL TEST DE LA MUTACIÓN, y por eso ataca por los dos
// lados: en los dos casos el default del Edge y el plazo de la petición apuntan a veredictos OPUESTOS,
// así que sólo puede pasar si manda el de la petición.
//
//	Deps.Timeout = 45 s (umbral 36 s) · petición 10 s (umbral 8 s) · 9,9 s ⇒ LENTA   (el default diría sana)
//	Deps.Timeout = 10 s (umbral  8 s) · petición 90 s (umbral 72 s) · 20 s ⇒ SANA    (el default diría lenta)
//
// 🔴 MUTACIÓN EJECUTADA (criterio (c) de T1.7-2): devolver `registrarAcierto` al umbral fijo del proceso
// —`umbral := umbralLentoDe(c.timeout)` en vez de `umbralLentoDe(plazo)`, un cambio que COMPILA— pone en
// rojo los dos subcasos de este test y también
// TestLentitud_LaMISMALatenciaEsSanaOEnfermaSEGUNSuPlazo. Y NO pone en rojo ninguno de los tests del
// MP-09, que es justo el motivo por el que el defecto llevaba una ola entera vivo con la suite verde:
// allí los dos plazos valen lo mismo.
func TestLentitud_ElDefaultDelProcesoNoGobierna(t *testing.T) {
	lentaAunqueElDefaultDiriaSana, _ := correrGuionCon(t, []pasoGuion{
		resp(9_900), resp(9_900), resp(9_900), resp(9_900), resp(9_900),
	}, DefaultInferenceTimeoutMS*time.Millisecond, plazoInteractivo)
	if lentaAunqueElDefaultDiriaSana.Lentas() != 5 {
		t.Errorf("MANDA EL PLAZO DE LA PETICIÓN (10 s ⇒ umbral 8 s), no el default del proceso (45 s ⇒ 36 s): "+
			"Lentas got %d want 5", lentaAunqueElDefaultDiriaSana.Lentas())
	}

	sanaAunqueElDefaultDiriaLenta, _ := correrGuionCon(t, []pasoGuion{
		resp(20_000), resp(20_000), resp(20_000), resp(20_000), resp(20_000),
	}, plazoInteractivo, plazoDeLote)
	if sanaAunqueElDefaultDiriaLenta.Lentas() != 0 {
		t.Errorf("MANDA EL PLAZO DE LA PETICIÓN (90 s ⇒ umbral 72 s), no el default del proceso (10 s ⇒ 8 s): "+
			"Lentas got %d want 0 — castigar al breaker por una inferencia que va holgada dentro de SU "+
			"presupuesto es el mismo error, con el signo cambiado", sanaAunqueElDefaultDiriaLenta.Lentas())
	}
	if sanaAunqueElDefaultDiriaLenta.Circuito() != breaker.StateClosed {
		t.Errorf("el circuito debe seguir CERRADO, got %q", sanaAunqueElDefaultDiriaLenta.Circuito())
	}
}
