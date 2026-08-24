package cajero

// cajero_test.go — los dobles del paquete y los tests del proceso: construcción, bucle, aforo y log.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 QUÉ SE BORRÓ DE AQUÍ EL 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-2, ADR-0045)
// ─────────────────────────────────────────────────────────────────────────────
// El cajero DEJÓ DE CLASIFICAR POR INICIATIVA PROPIA, y con el camino del claim desaparecieron sus
// tests: no se «adaptaron» porque no les quedaba SUJETO — probaban `procesar`, el sobre, el fencing del
// cierre y el freno del lote venenoso, y ninguna de esas funciones existe ya (ver el bloque «EL BUCLE YA
// NO CLASIFICA» en cajero.go). La lista completa, con el motivo de cada uno, está en el journal de la
// tarea; aquí basta con saber que un test de esta familia que reaparezca es un test sin código debajo.
//
// LO QUE SÍ SIGUE VIVO, y es lo que este fichero cubre ahora:
//
//   - la CONSTRUCCIÓN (New) con su nueva asimetría: la cola es obligatoria, el proveedor NO;
//   - el BUCLE, que ya sólo late (contadores y parte) y que por tanto NO puede reclamar nada;
//   - el BARRIDO de leases, que se conserva para limpiar el estado heredado de binarios anteriores;
//   - el AFORO, que ahora importa MÁS que antes porque tiene DOS consumidores (ver aforo.go);
//   - INV-051.1, cuyo sujeto se muda: ya no es el texto del mensaje, es el PROMPT y la SALIDA.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/breaker"
	"github.com/EduGoGroup/wapp-edge-intent/ollama"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// ─────────────────────────────────────────────────────────────────────────────
// Dobles
// ─────────────────────────────────────────────────────────────────────────────

// colaFake es el puerto de la cola en versión de escritorio.
//
// ⚠️ SIGUE IMPLEMENTANDO LOS TRES MÉTODOS aunque el cajero sólo llame a uno (BarrerLeasesVencidos): la
// interfaz `app.ColaCajero` no se ha recortado, y los dos métodos huérfanos CUENTAN sus llamadas a
// propósito — es lo que permite aseverar que el bucle ya no reclama ni cierra nada
// (TestBucle_YaNoReclamaNiCierra). Un doble que los dejara en no-op mudo no podría probar una ausencia.
type colaFake struct {
	mu          sync.Mutex
	reclamos    int
	cierres     int
	rescatables int64
	barridos    int
}

var _ app.ColaCajero = (*colaFake)(nil)

func (c *colaFake) Reclamar(_ context.Context, _ int) (*app.ColaLote, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reclamos++
	return nil, nil
}

func (c *colaFake) MarcarClasificado(_ context.Context, _ *app.ColaLote, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cierres++
	return nil
}

func (c *colaFake) BarrerLeasesVencidos(_ context.Context, _ time.Duration) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.barridos++
	n := c.rescatables
	c.rescatables = 0
	return n, nil
}

// snapshot devuelve las llamadas a los dos métodos que el cajero YA NO usa.
func (c *colaFake) snapshot() (reclamos, cierres int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reclamos, c.cierres
}

// barridosN se lee bajo el lock porque el barrido corre en su propia goroutine y hay un test que lo
// consulta ANTES de que Run haya devuelto (sin lock sería una carrera que `-race` cazaría).
func (c *colaFake) barridosN() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.barridos
}

// chateadorFake es el proveedor local de LLM en versión de escritorio. SUSTITUYE AL `clasificadorFake`
// de antes de T1.6-2, y la diferencia no es de nombre: aquél recibía un texto y devolvía una intención
// ya interpretada; éste recibe la petición ENTERA tal como se le manda al modelo y devuelve la salida
// cruda, que es exactamente el contrato del ADR-0045 §1 («prompt entra → JSON sale»).
//
// Guarda las peticiones recibidas para poder comprobar QUÉ se le mandó (modelo, opciones, `think`,
// `format`) sin levantar un Ollama.
type chateadorFake struct {
	mu        sync.Mutex
	salida    string
	err       error
	thinking  bool
	recibidas []ollama.ChatRequest
}

var _ Chateador = (*chateadorFake)(nil)

func (c *chateadorFake) Chat(_ context.Context, req ollama.ChatRequest) (*ollama.ChatResponse, error) {
	c.mu.Lock()
	c.recibidas = append(c.recibidas, req)
	salida, err := c.salida, c.err
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &ollama.ChatResponse{
		Message: ollama.Message{Role: "assistant", Content: salida},
		Done:    true,
		// Métricas verosímiles: el log de la inferencia servida las emite, y con todo a cero no se
		// distinguiría «no las hubo» de «no se emitieron».
		TotalDuration:   1_234_000_000,
		LoadDuration:    10_000_000,
		PromptEvalCount: 420,
		EvalCount:       31,
		EvalDuration:    2_480_000_000,
	}, nil
}

func (c *chateadorFake) SupportsThinking(_ context.Context, _ string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.thinking
}

// peticiones devuelve copia de lo que el proveedor recibió.
func (c *chateadorFake) peticiones() []ollama.ChatRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ollama.ChatRequest(nil), c.recibidas...)
}

// chateadorVigilante mide cuántas inferencias COINCIDEN EN EL TIEMPO dentro del proveedor. Es el
// instrumento de los dos tests del aforo: la propiedad que hay que medir es la simultaneidad, no el
// resultado, porque un aforo roto devuelve exactamente las mismas respuestas.
type chateadorVigilante struct {
	mu     sync.Mutex
	vivas  int
	pico   int
	total  int
	dentro time.Duration
}

var _ Chateador = (*chateadorVigilante)(nil)

func (c *chateadorVigilante) Chat(_ context.Context, _ ollama.ChatRequest) (*ollama.ChatResponse, error) {
	c.mu.Lock()
	c.vivas++
	c.total++
	if c.vivas > c.pico {
		c.pico = c.vivas
	}
	dentro := c.dentro
	c.mu.Unlock()

	if dentro <= 0 {
		dentro = 5 * time.Millisecond // ventana suficiente para que un solapamiento se vea
	}
	time.Sleep(dentro)

	c.mu.Lock()
	c.vivas--
	c.mu.Unlock()
	return &ollama.ChatResponse{Message: ollama.Message{Role: "assistant", Content: `{"ok":true}`}, Done: true}, nil
}

func (c *chateadorVigilante) SupportsThinking(_ context.Context, _ string) bool { return false }

func (c *chateadorVigilante) maxSimultaneas() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pico
}

func (c *chateadorVigilante) inferencias() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}

// chateadorQueMuere cancela el ctx del PROCESO desde dentro de la inferencia y devuelve el error del
// contexto: reproduce el SIGTERM que llega con el modelo trabajando.
type chateadorQueMuere struct {
	cancelar context.CancelFunc
	una      sync.Once
}

var _ Chateador = (*chateadorQueMuere)(nil)

func (c *chateadorQueMuere) Chat(ctx context.Context, _ ollama.ChatRequest) (*ollama.ChatResponse, error) {
	c.una.Do(func() { c.cancelar() })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *chateadorQueMuere) SupportsThinking(_ context.Context, _ string) bool { return false }

// breakerFake permite fijar el estado del circuito sin simular cinco fallos.
type breakerFake struct {
	mu       sync.Mutex
	estado   string
	permitir bool
	exitos   int
	fallos   int
}

func nuevoBreakerFake() *breakerFake {
	return &breakerFake{estado: breaker.StateClosed, permitir: true}
}

func (b *breakerFake) BeginAttempt() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.permitir
}

func (b *breakerFake) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.exitos++
}

// RecordFailure devuelve el FLANCO (esta llamada abrió el circuito), igual que *breaker.Breaker: el
// cajero ya no lo deduce por su cuenta con State()/RecordFailure()/State(), porque esa secuencia no era
// atómica y contaba mal las aperturas bajo concurrencia.
func (b *breakerFake) RecordFailure() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	antes := b.estado
	b.fallos++
	if b.fallos >= breaker.DefaultThreshold {
		b.estado = breaker.StateOpen
	}
	return antes != breaker.StateOpen && b.estado == breaker.StateOpen
}

func (b *breakerFake) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.estado
}

// ponerEstado fuerza el estado observable del circuito. Existe para poder situar un test EN el
// medio-abierto sin simular cinco fallos y esperar la ventana de 60 s.
func (b *breakerFake) ponerEstado(estado string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.estado = estado
}

// cuentas devuelve los aciertos y fallos registrados. Se lee bajo el lock: el servidor de inferencia
// registra desde la goroutine de quien pide, no desde la del test.
func (b *breakerFake) cuentas() (exitos, fallos int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.exitos, b.fallos
}

// despertadorCuenta cancela el contexto tras N esperas, para que el bucle termine solo.
type despertadorCuenta struct {
	mu     sync.Mutex
	n      int
	tope   int
	cancel context.CancelFunc
}

func (d *despertadorCuenta) Esperar(ctx context.Context) error {
	d.mu.Lock()
	d.n++
	llegó := d.n >= d.tope
	d.mu.Unlock()
	if llegó && d.cancel != nil {
		d.cancel()
	}
	return ctx.Err()
}

// entradaLog es una línea capturada del logger.
type entradaLog struct {
	nivel string
	msg   string
	args  []any
}

// logCaptura implementa sharedlogger.Logger guardando todo lo emitido, para poder ASEVERAR sobre el
// contenido del log (INV-051.1) en vez de confiar en la revisión visual.
type logCaptura struct {
	mu       sync.Mutex
	entradas []entradaLog
}

var _ sharedlogger.Logger = (*logCaptura)(nil)

func (l *logCaptura) añadir(nivel, msg string, args []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entradas = append(l.entradas, entradaLog{nivel: nivel, msg: msg, args: append([]any(nil), args...)})
}

func (l *logCaptura) Debug(msg string, args ...any) {
	l.añadir("debug", msg, args)
}

func (l *logCaptura) Info(msg string, args ...any) {
	l.añadir("info", msg, args)
}

func (l *logCaptura) Warn(msg string, args ...any) {
	l.añadir("warn", msg, args)
}

func (l *logCaptura) Error(msg string, args ...any) {
	l.añadir("error", msg, args)
}

func (l *logCaptura) With(_ ...any) sharedlogger.Logger {
	return l
}

func (l *logCaptura) todo() []entradaLog {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]entradaLog(nil), l.entradas...)
}

// texto aplana TODO lo que el logger recibió (mensajes y argumentos) en una sola cadena.
func (l *logCaptura) texto() string {
	var b strings.Builder
	for _, e := range l.todo() {
		b.WriteString(e.nivel)
		b.WriteString(" ")
		b.WriteString(e.msg)
		for _, a := range e.args {
			b.WriteString(" ")
			fmt.Fprint(&b, a)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func (l *logCaptura) buscar(msg string) (entradaLog, bool) {
	for _, e := range l.todo() {
		if strings.Contains(e.msg, msg) {
			return e, true
		}
	}
	return entradaLog{}, false
}

// clavesDe extrae los NOMBRES de los pares clave/valor de una línea de log. Se usa para aseverar tanto
// lo que una línea debe llevar como lo que NO puede llevar (INV-051.1).
func clavesDe(e entradaLog) map[string]bool {
	claves := map[string]bool{}
	for i := 0; i+1 < len(e.args); i += 2 {
		if k, ok := e.args[i].(string); ok {
			claves[k] = true
		}
	}
	return claves
}

// ─────────────────────────────────────────────────────────────────────────────
// Ayudas
// ─────────────────────────────────────────────────────────────────────────────

// correr monta el cajero con los dobles dados y lo ejecuta hasta que el despertador cancela.
func correr(t *testing.T, deps Deps, esperas int) (*Cajero, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := &despertadorCuenta{tope: esperas, cancel: cancel}
	deps.Despertador = d
	if deps.Lease == 0 {
		deps.Lease = time.Hour // el barrido no debe dispararse salvo que el test lo pida
	}

	c, err := New(deps)
	if err != nil {
		return nil, err
	}

	hecho := make(chan error, 1)
	go func() { hecho <- c.Run(ctx) }()
	select {
	case err := <-hecho:
		return c, err
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("Run no terminó dentro del plazo (goroutine colgada)")
		return nil, nil
	}
}

// servidorDe construye el cajero y devuelve su servidor de inferencia YA COMPROBADO.
//
// La comprobación del nil no es defensiva: `ServidorInferencia()` devuelve nil cuando el cajero se
// construyó sin proveedor, y un nil que se cuela aquí reaparecería como un pánico diez líneas más
// abajo, en la llamada a Inferir, contando una historia equivocada.
//
// No arranca Run a propósito: servir inferencia NO depende del bucle (el frame lo trae un socket, no la
// cola), y arrancar Run metería en medio la lectura de afinidad de la máquina real.
func servidorDe(t *testing.T, deps Deps) (*Cajero, app.ServidorInferencia) {
	t.Helper()
	if deps.Cola == nil && len(deps.Colas) == 0 {
		deps.Cola = &colaFake{}
	}
	if deps.Log == nil {
		deps.Log = &logCaptura{}
	}
	c, err := New(deps)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := c.ServidorInferencia()
	if s == nil {
		t.Fatal("ServidorInferencia() devolvió nil: el cajero se construyó SIN proveedor (Deps.Ollama)")
	}
	return c, s
}

// peticionDe arma una petición del Cloud con el prompt dado y un plazo explícito.
//
// EL PLAZO SE PASA SIEMPRE Y NO SE DEJA EN 0 en los tests que llaman al modelo: desde el ADR-0045 el
// presupuesto lo fija el Cloud, y un 0 significa «no lo fijé» — un caso distinto que tiene su propio
// camino (Cajero.plazoDe) y que no debe colarse de tapadillo en tests que van de otra cosa.
func peticionDe(prompt string, plazo time.Duration) app.PeticionInferencia {
	return app.PeticionInferencia{CommandID: "cmd-1", Prompt: prompt, Format: "json", Timeout: plazo}
}

// ─────────────────────────────────────────────────────────────────────────────
// Construcción
// ─────────────────────────────────────────────────────────────────────────────

// TestNew_ValidaDependencias fija la ASIMETRÍA NUEVA de T1.6-2, que es lo contrario de la que había: sin
// cola no hay cajero posible, pero SIN PROVEEDOR SÍ.
//
// El porqué de la inversión está en New: un cajero sin proveedor sigue barriendo leases y publicando el
// parte de salud —las dos cosas que el daemon de cada instalación necesita de él—, así que negarse a
// arrancar dejaría a TODOS los daemons de la máquina sin `intent_circuit` por una feature apagada. Lo
// único que pierde es poder servir inferencia, y eso lo dice `ServidorInferencia()` devolviendo nil.
func TestNew_ValidaDependencias(t *testing.T) {
	t.Run("sin cola no se construye", func(t *testing.T) {
		if _, err := New(Deps{Ollama: &chateadorFake{}}); err == nil {
			t.Error("sin cola debe fallar")
		}
	})

	t.Run("sin proveedor SÍ se construye, y lo dice", func(t *testing.T) {
		log := &logCaptura{}
		c, err := New(Deps{Cola: &colaFake{}, Log: log})
		if err != nil {
			t.Fatalf("el proveedor dejó de ser obligatorio en T1.6-2: %v", err)
		}
		if s := c.ServidorInferencia(); s != nil {
			t.Errorf("sin proveedor NO hay nada que servir: ServidorInferencia() debe ser nil, got %T", s)
		}
		if _, ok := log.buscar("sin proveedor local de LLM"); !ok {
			t.Error("arrancar sin proveedor es una degradación silenciosa si no se avisa: falta el Warn")
		}
	})

	t.Run("con proveedor hay servidor de inferencia", func(t *testing.T) {
		c, err := New(Deps{Cola: &colaFake{}, Ollama: &chateadorFake{}, Log: &logCaptura{}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if c.ServidorInferencia() == nil {
			t.Error("con proveedor el cajero DEBE poder servir inferencia")
		}
	})

	t.Run("los defaults se aplican", func(t *testing.T) {
		c, err := New(Deps{Cola: &colaFake{}, Ollama: &chateadorFake{}, Log: &logCaptura{}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if c.aforo.Plazas() != DefaultMaxConcurrent {
			t.Errorf("el aforo por defecto es de %d plaza(s), got %d", DefaultMaxConcurrent, c.aforo.Plazas())
		}
		if c.lease != app.DefaultColaLeaseSegundos*time.Second {
			t.Errorf("Lease por defecto: got %v want %v", c.lease, app.DefaultColaLeaseSegundos*time.Second)
		}
		if c.timeoutMax != DefaultMaxTimeoutMS*time.Millisecond {
			t.Errorf("TimeoutMax por defecto: got %v want %v", c.timeoutMax, DefaultMaxTimeoutMS*time.Millisecond)
		}
		if c.temperatura != DefaultTemperatura {
			t.Errorf("Temperatura por defecto: got %v want %v", c.temperatura, DefaultTemperatura)
		}
	})

	// 🔴 EL NÚMERO, aparte de que el default se aplique: un aforo de 2 son DOS inferencias simultáneas
	// contra la ÚNICA instancia de Ollama de la máquina, que es el solapamiento que la medición de la O0
	// señaló como causa de que la p50 se dispare (ADR-0038 Enmienda 1 §(d)). Si alguien sube esta
	// constante, que sea una decisión con su medición detrás y no un efecto colateral.
	if DefaultMaxConcurrent != 1 {
		t.Errorf("DefaultMaxConcurrent = %d, want 1 (una sola instancia de Ollama por máquina)", DefaultMaxConcurrent)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// El bucle
// ─────────────────────────────────────────────────────────────────────────────

// TestBucle_YaNoReclamaNiCierra es el candado del ADR-0045 §8 en su forma más directa: el bucle DEJÓ DE
// CLASIFICAR POR INICIATIVA PROPIA, y eso es una AUSENCIA — sólo se puede aseverar contando llamadas que
// no ocurren.
//
// POR QUÉ MERECE UN TEST PROPIO Y NO BASTA CON QUE EL CÓDIGO YA NO ESTÉ. La medición que mató al push
// (430 inferencias en campo el 2026-08-23, UNA llegó a tiempo a su ventana) no produce ningún error
// cuando se deshace: un cajero que volviera a reclamar seguiría pasando todos los demás tests, gastaría
// la única plaza de Ollama en inferencias que el despachador ya no espera, y el síntoma sería
// latencia — no un rojo.
func TestBucle_YaNoReclamaNiCierra(t *testing.T) {
	cola := &colaFake{}

	c, err := correr(t, Deps{Cola: cola, Ollama: &chateadorFake{salida: `{"ok":true}`},
		Breaker: nuevoBreakerFake(), Log: &logCaptura{}}, 5)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	reclamos, cierres := cola.snapshot()
	if reclamos != 0 {
		t.Errorf("el bucle NO puede reclamar lotes desde el ADR-0045 §8; reclamó %d veces", reclamos)
	}
	if cierres != 0 {
		t.Errorf("sin claim no hay nada que cerrar; hubo %d cierres", cierres)
	}
	if c.Servidas() != 0 {
		t.Errorf("nadie pidió inferencia: Servidas()=%d", c.Servidas())
	}
}

// TestBarridoDeLeases_SumaRescatadosYAvisa comprueba la goroutine del barrido: llama al puerto con el
// lease configurado, suma al contador y avisa en Warn.
//
// SE CONSERVA PESE A QUE NADIE RECLAMA, y el porqué está en el bloque del bucle: ya no rescata trabajo
// propio, pero sí devuelve a `nuevo` las filas que quedaron en `tomado` en el disco de un cliente que
// venía de un binario ANTERIOR. Es limpieza de estado heredado, y retirarla es una decisión aparte.
func TestBarridoDeLeases_SumaRescatadosYAvisa(t *testing.T) {
	cola := &colaFake{rescatables: 3}
	log := &logCaptura{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := New(Deps{
		Cola:        cola,
		Ollama:      &chateadorFake{},
		Breaker:     nuevoBreakerFake(),
		Despertador: NewPollFijo(5 * time.Millisecond),
		Log:         log,
		Lease:       10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	hecho := make(chan error, 1)
	go func() { hecho <- c.Run(ctx) }()

	plazo := time.After(3 * time.Second)
	for c.Rescatados() < 3 {
		select {
		case <-plazo:
			cancel()
			t.Fatalf("el barrido no rescató nada en 3 s (barridos=%d)", cola.barridosN())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	cancel()

	select {
	case err := <-hecho:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run no terminó tras cancelar (goroutine del barrido colgada)")
	}

	e, ok := log.buscar("leases vencidos rescatados")
	if !ok {
		t.Fatal("el barrido con n>0 debe dejar una línea de log")
	}
	if e.nivel != "warn" {
		t.Errorf("rescatar filas se avisa en Warn (alguien murió a mitad), got %q", e.nivel)
	}
}

// TestLatidoDeContadores_SeEmitePeriodicamente cubre el hueco del «se construye y no se cablea»: los
// contadores no los lee ningún plano de control (el cajero no tiene), así que sin este latido sólo
// serían legibles cuando el proceso muere — justo cuando ya no sirven para diagnosticar nada.
//
// LA LISTA DE CLAVES CAMBIÓ ENTERA en T1.6-2 y por eso este test no es cosmético: los cinco desenlaces
// de la inferencia servida van UNO A UNO (INV-051.3) porque piden intervenciones DISTINTAS —arrancar
// Ollama, esperar a que el circuito cierre, mirar por qué el modelo tarda, o mirar el hardware—, y un
// agregado de «fallidas» no distingue ninguna de las cuatro.
func TestLatidoDeContadores_SeEmitePeriodicamente(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := &logCaptura{}
	c, err := New(Deps{
		Cola:        &colaFake{},
		Ollama:      &chateadorFake{},
		Breaker:     nuevoBreakerFake(),
		Despertador: NewPollFijo(time.Millisecond),
		Log:         log,
		Lease:       time.Hour,
		StatsEvery:  5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	hecho := make(chan error, 1)
	go func() { hecho <- c.Run(ctx) }()

	plazo := time.After(3 * time.Second)
	var e entradaLog
esperar:
	for {
		if entrada, ok := log.buscar("cajero: contadores"); ok {
			e = entrada
			break esperar
		}
		select {
		case <-plazo:
			cancel()
			t.Fatal("el latido de contadores no se emitió en 3 s")
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	cancel()
	select {
	case <-hecho:
	case <-time.After(5 * time.Second):
		t.Fatal("Run no terminó tras cancelar")
	}

	if e.nivel != "info" {
		t.Errorf("el latido es Info (no es una anomalía), got %q", e.nivel)
	}
	claves := clavesDe(e)
	for _, k := range []string{"servidas", "err_ollama_caido", "err_breaker_abierto", "err_timeout",
		"err_sin_capacidad", "fallos", "lentas", "rescatados", "aperturas_breaker", "circuito"} {
		if !claves[k] {
			t.Errorf("el latido debe llevar el bloque COMPLETO: falta %q", k)
		}
	}
}

// TestLatidoDeContadores_CeroLoDesactiva: StatsEvery <= 0 es «cállate», no «usa el default».
func TestLatidoDeContadores_CeroLoDesactiva(t *testing.T) {
	log := &logCaptura{}
	if _, err := correr(t, Deps{Cola: &colaFake{}, Ollama: &chateadorFake{},
		Breaker: nuevoBreakerFake(), Log: log, StatsEvery: 0}, 5); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, ok := log.buscar("cajero: contadores"); ok {
		t.Error("con StatsEvery=0 el latido periódico NO debe emitirse")
	}
	// El bloque FINAL de Run sí se emite siempre: es el que cierra el proceso.
	if _, ok := log.buscar("detenido limpiamente"); !ok {
		t.Error("el bloque final de contadores se emite pase lo que pase")
	}
}

// TestAfinidadOllama_NuncaEsFatal: la comprobación de T2.8 puede fallar, y cuando falla el cajero
// arranca igual dejando el hecho en Warn.
//
// Este pasa por el camino REAL (Run → registrarAfinidad → leerAfinidades, la que toca /proc). El
// contenido de cada veredicto se prueba aparte, en afinidad_test.go, contra lecturas sintéticas: aquí lo
// que se defiende es que la comprobación no puede impedir un arranque.
func TestAfinidadOllama_NuncaEsFatal(t *testing.T) {
	log := &logCaptura{}

	if _, err := correr(t, Deps{Cola: &colaFake{}, Ollama: &chateadorFake{}, Breaker: nuevoBreakerFake(),
		Log: log, OllamaURL: "http://198.51.100.7:11434"}, 2); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Una URL remota no es observable en Linux, y en macOS no lo es ninguna: en ambos casos el
	// resultado esperado es el MISMO — un aviso, no un fallo de arranque.
	if _, ok := log.buscar("(T2.8)"); !ok {
		t.Error("el arranque debe dejar constancia de la comprobación de afinidad (T2.8)")
	}
	if _, ok := log.buscar("arrancando"); !ok {
		t.Error("el cajero debe haber arrancado pese al fallo de la comprobación")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// INV-051.1 · el log del camino que SÍ existe
// ─────────────────────────────────────────────────────────────────────────────

// TestINV0511_ElLogNoLlevaNiTextoNiParametros es la aserción del invariante, no una revisión visual.
//
// 🔴 EL SUJETO CAMBIÓ CON EL OFICIO. Hasta T1.6-2 lo que no podía salir por el log era el TEXTO del
// cliente y los PARÁMETROS que el clasificador extraía de él. Bajo el ADR-0045 el cajero no ve ni una
// cosa ni la otra: lo que le llega es un PROMPT ya construido por el Cloud —que lleva el texto del
// cliente dentro— y lo que produce es una SALIDA cruda que lleva los parámetros. Los dos son contenido
// de negocio y el invariante es el mismo; lo que hay que vigilar son otras dos variables.
//
// Se recorren los DOS desenlaces con contenido en la mano (servida y fallida) porque el camino de fallo
// es el que más fácil rompe el invariante: es donde alguien añade «y esto es lo que le mandé» para
// depurar.
func TestINV0511_ElLogNoLlevaNiTextoNiParametros(t *testing.T) {
	const promptSecreto = "clasifica esto: mi tarjeta termina en 4242 y vivo en la calle falsa"
	const salidaSecreta = `{"intent":"crear_pedido","params":{"direccion":"calle falsa 123"}}`

	t.Run("inferencia SERVIDA", func(t *testing.T) {
		log := &logCaptura{}
		c, s := servidorDe(t, Deps{Ollama: &chateadorFake{salida: salidaSecreta}, Breaker: nuevoBreakerFake(),
			Log: log})

		resp, err := s.Inferir(context.Background(), peticionDe(promptSecreto, 5*time.Second))
		if err != nil {
			t.Fatalf("Inferir: %v", err)
		}
		// La salida SÍ vuelve al llamante entera: lo que se prohíbe es loguearla, no producirla.
		if resp.RawJSON != salidaSecreta {
			t.Fatalf("la salida sube CRUDA, sin tocar: got %q", resp.RawJSON)
		}
		if c.Servidas() != 1 {
			t.Fatalf("Servidas: got %d want 1", c.Servidas())
		}

		volcado := log.texto()
		for _, prohibido := range []string{promptSecreto, salidaSecreta, "4242", "calle falsa"} {
			if strings.Contains(volcado, prohibido) {
				t.Errorf("INV-051.1 ROTA: %q apareció en el log", prohibido)
			}
		}

		// …y a la vez la línea de la inferencia servida SÍ debe existir con lo que hace falta para
		// diagnosticar: el hilo de correlación, los TAMAÑOS y el desenlace.
		e, ok := log.buscar("inferencia SERVIDA")
		if !ok {
			t.Fatal("servir una inferencia tiene que dejar su línea de Info: sin ella no hay forma de seguirla")
		}
		claves := clavesDe(e)
		for _, k := range []string{"command_id", "latencia_ms", "prompt_bytes", "salida_bytes", "plazo_ms", "lento"} {
			if !claves[k] {
				t.Errorf("la línea de la inferencia servida no lleva la clave %q", k)
			}
		}
		if claves["prompt"] || claves["salida"] || claves["raw_json"] {
			t.Error("INV-051.1 ROTA: la línea declara una clave de CONTENIDO, no de forma")
		}
	})

	t.Run("inferencia FALLIDA", func(t *testing.T) {
		log := &logCaptura{}
		_, s := servidorDe(t, Deps{
			Ollama:  &chateadorFake{err: errors.New("ollama no responde en :11434")},
			Breaker: nuevoBreakerFake(), Log: log,
		})

		if _, err := s.Inferir(context.Background(), peticionDe(promptSecreto, 5*time.Second)); err == nil {
			t.Fatal("con el proveedor caído Inferir debe devolver error")
		}

		volcado := log.texto()
		if strings.Contains(volcado, promptSecreto) || strings.Contains(volcado, "4242") {
			t.Error("INV-051.1 ROTA: el prompt que provocó el fallo apareció en el log")
		}
		e, ok := log.buscar("la inferencia FALLÓ")
		if !ok {
			t.Fatal("un fallo tiene que dejar su línea de Warn")
		}
		claves := clavesDe(e)
		if !claves["codigo"] || !claves["prompt_bytes"] {
			t.Errorf("el fallo debe decir su código canónico y el TAMAÑO del prompt, lleva %v", claves)
		}
		if claves["prompt"] {
			t.Error("INV-051.1 ROTA: el camino de fallo declara el prompt")
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// El AFORO — la propiedad que más importa desde que tiene DOS consumidores
// ─────────────────────────────────────────────────────────────────────────────

// TestSemaforoDeUnaPlaza_NoSolapaInferencias es el criterio de T2.3 en versión de escritorio: con el
// aforo en 1 nunca hay dos llamadas al proveedor a la vez.
//
// 🔴 SE REESCRIBIÓ CONTRA EL AFORO REAL EN T1.6-2, Y VALE MÁS QUE ANTES. Antes la simultaneidad la
// acotaba el propio bucle (una goroutine que tomaba plaza antes de clasificar), así que el semáforo era
// medio redundante. Ahora el aforo lo comparten DOS consumidores —el bucle, que ya no lo toma, y el
// servidor de inferencia, que atiende peticiones del Cloud que llegan por un socket y en paralelo—, y es
// LO ÚNICO que impide que dos peticiones concurrentes se pisen los hilos dentro de la misma instancia de
// Ollama. Ver el bloque «UN SOLO AFORO» en aforo.go.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: darle al servidor su propio aforo (`NuevoAforo(1)` dentro de
// `ServidorInferencia()` en vez de `c.aforo`), o subir DefaultMaxConcurrent sin tocar el test.
func TestSemaforoDeUnaPlaza_NoSolapaInferencias(t *testing.T) {
	t.Run("dos peticiones concurrentes no se solapan dentro del proveedor", func(t *testing.T) {
		const enParalelo = 4

		vigilante := &chateadorVigilante{}
		c, s := servidorDe(t, Deps{Ollama: vigilante, Breaker: nuevoBreakerFake(), Log: &logCaptura{},
			MaxConcurrent: 1})

		inferirEnParalelo(context.Background(), t, s, enParalelo)

		if n := vigilante.maxSimultaneas(); n != 1 {
			t.Errorf("con el aforo en 1 NUNCA puede haber dos inferencias solapadas, hubo %d", n)
		}
		if n := vigilante.inferencias(); n != enParalelo {
			t.Errorf("las %d peticiones deben servirse todas (el aforo hace COLA, no descarta), se sirvieron %d",
				enParalelo, n)
		}
		if c.Servidas() != enParalelo {
			t.Errorf("Servidas: got %d want %d", c.Servidas(), enParalelo)
		}
		if c.ErroresSinCapacidad() != 0 {
			t.Errorf("con plazo de sobra ninguna petición se queda sin capacidad, hubo %d", c.ErroresSinCapacidad())
		}
		// Y la plaza se DEVUELVE: un `Soltar` que faltara dejaría el aforo lleno para siempre y el síntoma
		// sería un Edge que responde EDGE_SIN_CAPACIDAD a todo con Ollama perfectamente sano.
		if ocupadas := c.Aforo().Ocupadas(); ocupadas != 0 {
			t.Errorf("terminadas las inferencias el aforo queda VACÍO, quedan %d plazas tomadas", ocupadas)
		}
	})

	// 🔴 ESTE SUBTEST ES EL QUE CAZA LA MUTACIÓN DE VERDAD, y el de arriba por sí solo NO puede: un
	// servidor que se construyera SU PROPIO aforo de una plaza serializaría igual y el vigilante seguiría
	// viendo un pico de 1. Lo que hay que demostrar es la IDENTIDAD del aforo, no su tamaño — y la única
	// forma de observarla desde fuera es ocupar la plaza DESDE EL PROCESO y ver si el servidor se entera.
	//
	// De paso es el único sitio del paquete que ejercita `Aforo.Tomar` (la puerta bloqueante), que hoy no
	// tiene llamante en producción: el consumidor para el que se escribió —el bucle que reclamaba— murió
	// en esta misma tarea.
	t.Run("el aforo que usa el servidor es EL DEL PROCESO", func(t *testing.T) {
		ctx := context.Background()
		c, s := servidorDe(t, Deps{Ollama: &chateadorFake{salida: `{"ok":true}`},
			Breaker: nuevoBreakerFake(), Log: &logCaptura{}, MaxConcurrent: 1})

		if !c.Aforo().Tomar(ctx) {
			t.Fatal("el aforo del proceso está libre: Tomar tenía que conseguir la plaza")
		}

		// Plazo corto: la petición NO tiene que esperar a nadie, tiene que rendirse y decir por qué.
		_, err := s.Inferir(ctx, peticionDe("clasifica esto", 50*time.Millisecond))
		c.Aforo().Soltar()

		if !errors.Is(err, app.ErrInferenciaSinCapacidad) {
			t.Fatalf("con la ÚNICA plaza del proceso ocupada, el servidor debe responder EDGE_SIN_CAPACIDAD "+
				"(si respondió otra cosa, está usando un aforo distinto del de la máquina): %v", err)
		}
		if c.ErroresSinCapacidad() != 1 {
			t.Errorf("err_sin_capacidad: got %d want 1", c.ErroresSinCapacidad())
		}
		// 🔴 Y NO TIMEOUT: el plazo se agotó ESPERANDO PLAZA, sin llegar a llamar al modelo. Confundir las
		// dos condiciones no da un error, da un diagnóstico invertido — manda al dueño del equipo a mirar
		// su red en vez de su hardware (ver app.ErrInferenciaSinCapacidad).
		if c.ErroresTimeout() != 0 {
			t.Errorf("nunca se llamó al modelo: esto NO es un timeout (err_timeout=%d)", c.ErroresTimeout())
		}
		if c.Servidas() != 0 {
			t.Errorf("no se sirvió nada: Servidas=%d", c.Servidas())
		}
	})
}

// inferirEnParalelo lanza n peticiones SIMULTÁNEAS contra el servidor y espera a todas. Es la forma de
// medir el aforo: con llamadas secuenciales, un aforo roto daría exactamente el mismo resultado.
//
// El `ctx` va PRIMERO (revive/context-as-argument), antes del *testing.T.
func inferirEnParalelo(ctx context.Context, t *testing.T, s app.ServidorInferencia, n int) {
	t.Helper()

	var wg sync.WaitGroup
	errs := make([]error, n)
	arranque := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-arranque // que salgan juntas: un arranque escalonado no probaría solapamiento
			_, errs[i] = s.Inferir(ctx, peticionDe(fmt.Sprintf("prompt %d", i), 10*time.Second))
		}()
	}
	close(arranque)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("la petición %d falló y el test necesita que las %d se sirvan: %v", i, n, err)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// El BREAKER desde el camino que hoy lo alimenta
// ─────────────────────────────────────────────────────────────────────────────

// TestAperturasBreaker_CuentaElFlancoUnaVez usa el breaker REAL (Breaker nil ⇒ el default) para
// comprobar que la métrica sigue viva tras mover la detección del flanco dentro del breaker: cinco
// inferencias fallidas abren el circuito UNA vez, no cinco ni cero.
//
// El camino que alimenta al breaker cambió de dueño en T1.6-2 (era el bucle, hoy es el servidor de
// inferencia), y por eso el test se ejercita desde ahí: lo que se protege no es el breaker —que tiene
// sus propios tests— sino que ESTE proceso siga contándole lo que le pasa.
func TestAperturasBreaker_CuentaElFlancoUnaVez(t *testing.T) {
	log := &logCaptura{}
	// Breaker: nil ⇒ New construye el real con la calibración por defecto (5 fallos / 60 s).
	c, s := servidorDe(t, Deps{
		Ollama: &chateadorFake{err: errors.New("ollama caído")},
		Log:    log,
	})

	for i := range breaker.DefaultThreshold {
		_, err := s.Inferir(context.Background(), peticionDe("clasifica esto", 5*time.Second))
		if !errors.Is(err, app.ErrInferenciaOllamaCaido) {
			t.Fatalf("la inferencia %d debía fallar con OLLAMA_DOWN, dio %v", i, err)
		}
	}

	if c.Fallos() != int64(breaker.DefaultThreshold) {
		t.Errorf("Fallos: got %d want %d", c.Fallos(), breaker.DefaultThreshold)
	}
	if c.ErroresOllamaCaido() != int64(breaker.DefaultThreshold) {
		t.Errorf("los desenlaces se cuentan uno a uno (INV-051.3): err_ollama_caido=%d want %d",
			c.ErroresOllamaCaido(), breaker.DefaultThreshold)
	}
	if c.AperturasBreaker() != 1 {
		t.Errorf("cinco fallos consecutivos abren el circuito UNA vez, se contaron %d", c.AperturasBreaker())
	}
	if c.Circuito() != breaker.StateOpen {
		t.Errorf("el circuito debe quedar abierto, got %q", c.Circuito())
	}
	if _, ok := log.buscar("se ABRIÓ"); !ok {
		t.Error("la apertura del circuito debe dejar una línea de Warn")
	}

	// Y con el circuito ya abierto, la petición siguiente se rechaza SIN tocar al proveedor: es la
	// propiedad que el ADR-0045 exige (respuesta inmediata) y la que hace que un Ollama caído no cueste
	// un plazo entero por petición.
	_, err := s.Inferir(context.Background(), peticionDe("clasifica esto otro", 5*time.Second))
	if !errors.Is(err, app.ErrInferenciaBreakerAbierto) {
		t.Fatalf("con el circuito abierto se responde BREAKER_OPEN, dio %v", err)
	}
	if c.ErroresBreakerAbierto() != 1 {
		t.Errorf("err_breaker_abierto: got %d want 1", c.ErroresBreakerAbierto())
	}
	if c.Fallos() != int64(breaker.DefaultThreshold) {
		t.Errorf("un rechazo por circuito abierto NO es un fallo del proveedor (no se le llamó): fallos=%d", c.Fallos())
	}
}

// TestApagadoAMitadDeInferencia_NoEnvenenaElBreaker: una inferencia cortada por el APAGADO no es un
// fallo del proveedor y el cajero no debe contarla como tal.
//
// El bug que este test cerró en su día era silencioso y acumulativo: se contaba un fallo Y se logueaba
// «no es un fallo del clasificador» justo después, así que cada SIGTERM con trabajo en vuelo acercaba el
// circuito a su umbral y un cajero reiniciado varias veces podía encontrárselo abierto por su propia
// muerte anterior.
//
// 🔴 LA ASIMETRÍA QUE T1.6-2 INTRODUJO, Y QUE ESTE TEST FIJA: el COMPROMISO del breaker sí se salda. El
// bucle viejo podía volver sin registrar nada «sólo porque el proceso entero se está yendo», y bajo el
// socket esa justificación se cae — quien aborta puede ser un CLIENTE (el daemon que se rinde, una
// reconexión de CloudLink) sin que este proceso se vaya a ninguna parte, y un `BeginAttempt` que fuera el
// sondeo del medio-abierto dejaría el flag reservado para siempre. Ver `cerrarSinIntentar`.
func TestApagadoAMitadDeInferencia_NoEnvenenaElBreaker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	br := nuevoBreakerFake()
	log := &logCaptura{}
	c, s := servidorDe(t, Deps{Ollama: &chateadorQueMuere{cancelar: cancel}, Breaker: br, Log: log})

	_, err := s.Inferir(ctx, peticionDe("clasifica esto", 5*time.Second))
	if !errors.Is(err, app.ErrInferenciaTimeout) {
		t.Fatalf("una inferencia cortada por el apagado se responde como TIMEOUT, dio %v", err)
	}

	if c.Fallos() != 0 {
		t.Errorf("el apagado NO es un fallo del proveedor (Fallos=%d)", c.Fallos())
	}
	if c.AperturasBreaker() != 0 {
		t.Errorf("el apagado no puede contar una apertura del circuito (aperturas=%d)", c.AperturasBreaker())
	}

	// 🔴 CON EL CIRCUITO CERRADO NO SE REGISTRA NADA EN EL BREAKER, y ese cero es la mitad importante del
	// test. `BeginAttempt` con el circuito cerrado NO reserva ningún estado —mira `failures` y devuelve
	// true—, así que no hay compromiso que saldar: registrar un fallo aquí castigaría al proveedor por un
	// aborto de QUIEN PIDIÓ, y cinco reconexiones de CloudLink seguidas abrirían un circuito que protege a
	// un Ollama perfectamente sano. La otra mitad —el sondeo del medio-abierto, que SÍ hay que saldar—
	// vive en TestApagadoDurante ElSondeo_SaldaElCompromiso.
	if _, fallos := br.cuentas(); fallos != 0 {
		t.Errorf("con el circuito CERRADO un aborto no debe registrar fallo (el proveedor no tiene la culpa): "+
			"fallos registrados en el breaker = %d", fallos)
	}
	// El desenlace SÍ se cuenta, aunque no suba por el cable: sin este contador, un daemon que abortase
	// sistemáticamente quemaría el LLM del cliente sin dejar rastro en ninguna serie (INV-051.3).
	if c.Abortadas() != 1 {
		t.Errorf("una inferencia abandonada por quien la pidió tiene que contarse (abortadas=%d)", c.Abortadas())
	}
	if c.Servidas() != 0 {
		t.Errorf("una inferencia abortada no se sirvió (servidas=%d)", c.Servidas())
	}
	if _, ok := log.buscar("ABORTADA por quien la pidió"); !ok {
		t.Error("el aborto a mitad de inferencia debe dejar constancia en el log")
	}
}

// TestApagadoDuranteElSondeo_SaldaElCompromiso es la OTRA MITAD del test de arriba, y la que de verdad
// justifica que el camino del aborto toque el breaker.
//
// 🔴 EL DEFECTO QUE CIERRA: `BeginAttempt` en MEDIO-ABIERTO reserva el sondeo (`probing = true`) y quien
// recibe ese true se compromete a resolverlo. Si la petición se abandona sin registrar, el flag queda
// reservado y el circuito NO VUELVE A DEJAR PASAR NADA — nunca, ni con el proveedor ya recuperado. El
// bucle de clasificación tenía esa excepción y era segura porque el proceso entero moría; bajo el socket
// NO, porque quien aborta es un CLIENTE (el daemon que se rinde, un stream que se reconecta) y el cajero
// sigue vivo. Se registra fallo, que es lo conservador: reabre la ventana en vez de dejar el circuito mudo.
func TestApagadoDuranteElSondeo_SaldaElCompromiso(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	br := nuevoBreakerFake()
	br.ponerEstado(breaker.StateHalfOpen)
	log := &logCaptura{}
	c, s := servidorDe(t, Deps{Ollama: &chateadorQueMuere{cancelar: cancel}, Breaker: br, Log: log})

	if _, err := s.Inferir(ctx, peticionDe("clasifica esto", 5*time.Second)); !errors.Is(err, app.ErrInferenciaTimeout) {
		t.Fatalf("una inferencia abortada se responde como TIMEOUT, dio %v", err)
	}

	if _, fallos := br.cuentas(); fallos != 1 {
		t.Errorf("un SONDEO de medio-abierto abandonado hay que saldarlo o el circuito queda mudo para "+
			"siempre: fallos registrados en el breaker = %d", fallos)
	}
	if c.Abortadas() != 1 {
		t.Errorf("el desenlace se cuenta igual (abortadas=%d)", c.Abortadas())
	}
	if _, ok := log.buscar("SONDEO del medio-abierto"); !ok {
		t.Error("saldar un sondeo abandonado tiene que decirse: es una reapertura que el operador verá")
	}
}

// TestPoliticaDelEdge_LoQueDECIDEElEdgeLlegaALaPeticionDeOllama cierra las CUATRO cosas que el ADR-0045
// deja del lado del Edge y no del Cloud. Ninguna viaja en el frame, así que si alguna se pierde por el
// camino NO hay error: la inferencia sale igual, sólo que peor.
//
//   - EL MODELO. No viaja en el frame a propósito: es propiedad de la máquina del cliente (qué cabe en
//     su RAM, qué tolera su CPU). Un modelo pedido desde la nube sería una orden que el Edge no siempre
//     puede cumplir, y el fallo aparecería como lentitud, no como negativa.
//   - `think:false`, POLÍTICA FIJA (ADR-0045 §5) y condicionada a la capability. Su único valor
//     no-por-defecto degrada en ÓRDENES DE MAGNITUD: medido, precargar sin él convirtió una inferencia
//     de 4 s en 4 MINUTOS. Por eso el Cloud no tiene perilla.
//   - LAS OPCIONES DEL MODELO (num_thread/num_ctx/num_predict), calibradas en la O0 sobre el VPS real.
//   - EL `format` NORMALIZADO. 🔴 Ésta es la que muerde: el campo del proveedor es un VALOR JSON crudo,
//     así que la palabra `json` sin comillas produce `"format":json` — sintaxis inválida, 400 del
//     proveedor, y ese 400 se traduce a OLLAMA_DOWN: culparíamos a la máquina del cliente de un error de
//     serialización NUESTRO. Se comprueba serializando de verdad, que es donde el defecto aparece.
func TestPoliticaDelEdge_LoQueDECIDEElEdgeLlegaALaPeticionDeOllama(t *testing.T) {
	ctx := context.Background()

	casos := []struct {
		nombre        string
		thinking      bool
		formatPedido  string
		formatEnCable string
		temperatura   *float32
		esperada      float64
	}{
		{
			nombre: "modelo razonador: think:false y `json` CITADO",
			// El caso que rompe si alguien retira NormalizarFormato: `json` a secas es lo que más se va a
			// pedir (el Cloud clasifica), y verbatim no es un valor JSON.
			thinking: true, formatPedido: "json", formatEnCable: `"json"`, esperada: DefaultTemperatura,
		},
		{
			nombre:   "modelo sin capability: NO se manda think (se lo comería como error)",
			thinking: false, formatPedido: `{"type":"object"}`, formatEnCable: `{"type":"object"}`,
			esperada: DefaultTemperatura,
		},
		{
			nombre: "la temperatura del Cloud MANDA sobre el default del Edge",
			// 🔴 Y el valor de prueba es 0, que es el que sólo se distingue de «no dije nada» porque el
			// campo es un puntero. Con un float pelado este caso sería indistinguible del de arriba.
			thinking: false, formatPedido: "", formatEnCable: "",
			temperatura: ptr[float32](0), esperada: 0,
		},
	}

	for _, caso := range casos {
		t.Run(caso.nombre, func(t *testing.T) {
			prov := &chateadorFake{salida: `{"intent":"saludo"}`, thinking: caso.thinking}
			_, s := servidorDe(t, Deps{
				Ollama:   prov,
				Modelo:   "qwen3:1.7b",
				Opciones: map[string]any{"num_thread": 5, "num_ctx": 4096, "num_predict": 256},
			})

			if _, err := s.Inferir(ctx, app.PeticionInferencia{
				CommandID: "cmd-politica", Prompt: "hola", Format: caso.formatPedido,
				Temperature: caso.temperatura, Timeout: 5 * time.Second,
			}); err != nil {
				t.Fatalf("Inferir: %v", err)
			}

			recibidas := prov.peticiones()
			if len(recibidas) != 1 {
				t.Fatalf("el proveedor recibió %d peticiones, se esperaba 1", len(recibidas))
			}
			req := recibidas[0]

			if req.Model != "qwen3:1.7b" {
				t.Errorf("el modelo lo elige el EDGE y no viaja en el frame: got %q", req.Model)
			}
			switch {
			case caso.thinking && (req.Think == nil || *req.Think):
				t.Errorf("`think:false` es política FIJA del Edge (ADR-0045 §5): got %v", req.Think)
			case !caso.thinking && req.Think != nil:
				t.Errorf("a un modelo SIN la capability no se le manda `think` (es un error del proveedor): got %v",
					*req.Think)
			}
			for clave, quiero := range map[string]any{"num_thread": 5, "num_ctx": 4096, "num_predict": 256} {
				if req.Options[clave] != quiero {
					t.Errorf("opción %q calibrada en la O0: got %v want %v", clave, req.Options[clave], quiero)
				}
			}
			if req.Options["temperature"] != caso.esperada {
				t.Errorf("temperatura: got %v want %v", req.Options["temperature"], caso.esperada)
			}
			if string(req.Format) != caso.formatEnCable {
				t.Errorf("format: got %q want %q", string(req.Format), caso.formatEnCable)
			}

			// 🔴 LA PRUEBA QUE DE VERDAD CUENTA: serializar la petición como la serializa el proveedor. El
			// `Format` es un json.RawMessage y se escribe CRUDO, así que un valor sin citar rompe el cuerpo
			// ENTERO — y el síntoma en campo no sería un panic sino un 400 que traduciríamos a OLLAMA_DOWN.
			cuerpo, err := json.Marshal(struct {
				Model  string          `json:"model"`
				Format json.RawMessage `json:"format,omitempty"`
			}{Model: req.Model, Format: req.Format})
			if err != nil {
				t.Fatalf("el `format` produjo un cuerpo que NO serializa (el proveedor devolvería 400 y lo "+
					"leeríamos como OLLAMA_DOWN): %v", err)
			}
			if !json.Valid(cuerpo) {
				t.Fatalf("el cuerpo de la petición no es JSON válido: %s", cuerpo)
			}
		})
	}
}

// ptr devuelve un puntero al valor. Existe para poder distinguir en los casos de arriba «temperatura 0»
// de «el Cloud no dijo nada», que es la única razón de que el campo sea un puntero.
func ptr[T any](v T) *T { return &v }
