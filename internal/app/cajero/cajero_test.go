package cajero

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
	"github.com/EduGoGroup/wapp-edge-intent/classifier"
	"github.com/EduGoGroup/wapp-shared/intents"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// ─────────────────────────────────────────────────────────────────────────────
// Dobles
// ─────────────────────────────────────────────────────────────────────────────

type cierre struct {
	lote       *app.ColaLote
	intentJSON string
}

// colaFake sirve los lotes preparados en orden y, agotados, devuelve (nil, nil) como la cola real
// cuando está vacía.
type colaFake struct {
	mu          sync.Mutex
	pendientes  []*app.ColaLote
	reclamos    int
	cierres     []cierre
	errReclamar error
	errMarcar   error
	rescatables int64
	barridos    int
}

var _ app.ColaCajero = (*colaFake)(nil)

func (c *colaFake) Reclamar(_ context.Context, _ int) (*app.ColaLote, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reclamos++
	if c.errReclamar != nil {
		return nil, c.errReclamar
	}
	if len(c.pendientes) == 0 {
		return nil, nil
	}
	lote := c.pendientes[0]
	c.pendientes = c.pendientes[1:]
	return lote, nil
}

func (c *colaFake) MarcarClasificado(_ context.Context, lote *app.ColaLote, intentJSON string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cierres = append(c.cierres, cierre{lote: lote, intentJSON: intentJSON})
	return c.errMarcar
}

func (c *colaFake) BarrerLeasesVencidos(_ context.Context, _ time.Duration) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.barridos++
	n := c.rescatables
	c.rescatables = 0
	return n, nil
}

func (c *colaFake) snapshot() (reclamos int, cierres []cierre) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reclamos, append([]cierre(nil), c.cierres...)
}

// barridosN se lee bajo el lock porque el barrido corre en su propia goroutine y hay un test que lo
// consulta ANTES de que Run haya devuelto (sin lock sería una carrera que `-race` cazaría).
func (c *colaFake) barridosN() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.barridos
}

// clasificadorFake devuelve un resultado fijo y guarda el texto EXACTO que recibió (para comprobar la
// concatenación en orden y la única inferencia por lote).
type clasificadorFake struct {
	mu       sync.Mutex
	res      classifier.Classification
	err      error
	entradas []string
	panico   bool
}

func (f *clasificadorFake) Classify(_ context.Context, texto string) (classifier.Classification, error) {
	f.mu.Lock()
	f.entradas = append(f.entradas, texto)
	panico, res, err := f.panico, f.res, f.err
	f.mu.Unlock()
	if panico {
		panic("el clasificador explotó")
	}
	return res, err
}

func (f *clasificadorFake) recibidas() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.entradas...)
}

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

// ─────────────────────────────────────────────────────────────────────────────
// Ayudas
// ─────────────────────────────────────────────────────────────────────────────

func loteDe(sesion string, textos ...string) *app.ColaLote {
	l := &app.ColaLote{SessionID: sesion, ChatJID: "593999@s.whatsapp.net", ClaimToken: "tok", TomadoEn: 1_700_000_000}
	for i, t := range textos {
		l.Mensajes = append(l.Mensajes, app.ColaMensaje{
			ID: int64(i + 1), Seq: int64((i + 1) * 10), WAMessageID: fmt.Sprintf("m%d", i+1), Texto: t,
		})
	}
	return l
}

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

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

// TestCiclo_UnaInferenciaPorLote_EnOrdenDeSeq es el corazón del design §4: los fragmentos de un turno
// se concatenan EN ORDEN y producen UNA sola inferencia, y el sobre se escribe una vez.
func TestCiclo_UnaInferenciaPorLote_EnOrdenDeSeq(t *testing.T) {
	cola := &colaFake{pendientes: []*app.ColaLote{loteDe("s1", "hola", "quiero pedir", "una pizza")}}
	cls := &clasificadorFake{res: classifier.Classification{
		Intent: "crear_pedido", Params: map[string]string{"producto": "pizza"}, Confidence: 0.91,
	}}
	log := &logCaptura{}

	c, err := correr(t, Deps{Cola: cola, Clasificador: cls, Breaker: nuevoBreakerFake(), Log: log,
		ConfigVersion: func() string { return "v7" }}, 2)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	recibidas := cls.recibidas()
	if len(recibidas) != 1 {
		t.Fatalf("un lote de 3 mensajes debe producir UNA inferencia, hubo %d", len(recibidas))
	}
	if recibidas[0] != "hola\nquiero pedir\nuna pizza" {
		t.Errorf("concatenación fuera de orden o con otro separador: %q", recibidas[0])
	}

	_, cierres := cola.snapshot()
	if len(cierres) != 1 {
		t.Fatalf("el lote debe cerrarse UNA vez, hubo %d cierres", len(cierres))
	}
	var sobre sobreCajero
	if err := json.Unmarshal([]byte(cierres[0].intentJSON), &sobre); err != nil {
		t.Fatalf("el sobre del cajero no es JSON válido (%q): %v", cierres[0].intentJSON, err)
	}
	if sobre.Intent != "crear_pedido" || sobre.Confidence != 0.91 || sobre.ConfigVersion != "v7" {
		t.Errorf("sobre mal formado: %+v", sobre)
	}
	if c.Clasificados() != 1 {
		t.Errorf("Clasificados: got %d want 1", c.Clasificados())
	}
	if c.Omitidos() != 0 || c.Fallos() != 0 || c.Relevados() != 0 {
		t.Errorf("contadores sucios: omitidos=%d fallos=%d relevados=%d", c.Omitidos(), c.Fallos(), c.Relevados())
	}
}

// TestINV0511_ElLogNoLlevaNiTextoNiParametros es la aserción del invariante, no una revisión visual: el
// texto del cliente y los valores extraídos NO pueden aparecer en NINGUNA línea del log.
func TestINV0511_ElLogNoLlevaNiTextoNiParametros(t *testing.T) {
	const secreto = "mi tarjeta termina en 4242 y vivo en la calle falsa"
	const paramSecreto = "calle falsa 123"

	cola := &colaFake{pendientes: []*app.ColaLote{loteDe("s1", secreto)}}
	cls := &clasificadorFake{res: classifier.Classification{
		Intent:     "crear_pedido",
		Params:     map[string]string{"direccion": paramSecreto},
		Confidence: 0.88,
		Metrics:    classifier.Metrics{TotalMs: 1234, LoadMs: 10, PromptTokens: 420, OutputTokens: 31, TokensPerSec: 12.5},
		Truncado:   true,
	}}
	log := &logCaptura{}

	if _, err := correr(t, Deps{Cola: cola, Clasificador: cls, Breaker: nuevoBreakerFake(), Log: log}, 2); err != nil {
		t.Fatalf("Run: %v", err)
	}

	volcado := log.texto()
	if strings.Contains(volcado, secreto) {
		t.Error("INV-051.1 ROTA: el texto del mensaje apareció en el log")
	}
	if strings.Contains(volcado, paramSecreto) {
		t.Error("INV-051.1 ROTA: un parámetro extraído apareció en el log")
	}
	if strings.Contains(volcado, "4242") {
		t.Error("INV-051.1 ROTA: un fragmento del texto apareció en el log")
	}

	// …y a la vez el log de métricas SÍ debe existir con lo que T2.6 exige.
	e, ok := log.buscar("lote clasificado")
	if !ok {
		t.Fatal("falta el log Info de métricas de la clasificación (T2.6)")
	}
	claves := map[string]bool{}
	for i := 0; i+1 < len(e.args); i += 2 {
		if k, ok := e.args[i].(string); ok {
			claves[k] = true
		}
	}
	for _, k := range []string{"intent", "confidence", "total_ms", "prompt_tokens", "output_tokens",
		"tokens_per_sec", "mensajes", "runas", "truncado"} {
		if !claves[k] {
			t.Errorf("el log de métricas no lleva la clave %q", k)
		}
	}
	if claves["params"] || claves["texto"] {
		t.Error("INV-051.1 ROTA: el log de métricas declara una clave de contenido")
	}
}

// TestDesconocido_EscribeSobreOmitido_YNoCastigaAlBreaker: «desconocido» es una respuesta válida, no un
// fallo.
func TestDesconocido_EscribeSobreOmitido_YNoCastigaAlBreaker(t *testing.T) {
	cola := &colaFake{pendientes: []*app.ColaLote{loteDe("s1", "mensaje ambiguo")}}
	cls := &clasificadorFake{res: classifier.Classification{Intent: intents.ReservedUnknown}}
	br := nuevoBreakerFake()

	c, err := correr(t, Deps{Cola: cola, Clasificador: cls, Breaker: br, Log: &logCaptura{}}, 2)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	_, cierres := cola.snapshot()
	if len(cierres) != 1 || cierres[0].intentJSON != app.SobreOmitido(app.MotivoDesconocido) {
		t.Fatalf("se esperaba el sobre `desconocido`, got %+v", cierres)
	}
	if br.fallos != 0 || br.exitos != 1 {
		t.Errorf("«desconocido» debe contar como ÉXITO en el breaker (exitos=%d fallos=%d)", br.exitos, br.fallos)
	}
	if c.Omitidos() != 1 || c.Clasificados() != 0 {
		t.Errorf("contadores: omitidos=%d clasificados=%d", c.Omitidos(), c.Clasificados())
	}
}

// TestBreakerAbierto_NoReclamaNada es el criterio literal de T2.4: con el circuito abierto no hay claims
// nuevos. Ni uno.
func TestBreakerAbierto_NoReclamaNada(t *testing.T) {
	cola := &colaFake{pendientes: []*app.ColaLote{loteDe("s1", "quiero una pizza")}}
	br := nuevoBreakerFake()
	br.estado = breaker.StateOpen

	if _, err := correr(t, Deps{Cola: cola, Clasificador: &clasificadorFake{}, Breaker: br, Log: &logCaptura{}}, 3); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reclamos, cierres := cola.snapshot()
	if reclamos != 0 {
		t.Errorf("con el circuito ABIERTO no puede haber ni un claim, hubo %d", reclamos)
	}
	if len(cierres) != 0 {
		t.Errorf("con el circuito abierto no se cierra nada (no se reclama, no se marca omitido), hubo %d", len(cierres))
	}
}

// TestBeginAttemptDenegado_EscribeMotivoBreaker cubre el ÚNICO camino que escribe `breaker`: el lote ya
// está en la mano y el circuito niega el intento (carrera con MAX_CONCURRENT>1 o sondeo ya reservado).
func TestBeginAttemptDenegado_EscribeMotivoBreaker(t *testing.T) {
	cola := &colaFake{pendientes: []*app.ColaLote{loteDe("s1", "quiero una pizza")}}
	cls := &clasificadorFake{}
	br := nuevoBreakerFake()
	br.permitir = false // el circuito niega el intento aunque State() diga "closed"

	c, err := correr(t, Deps{Cola: cola, Clasificador: cls, Breaker: br, Log: &logCaptura{}}, 2)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(cls.recibidas()) != 0 {
		t.Error("con el intento denegado NO se llama a Ollama (esa es la semántica del motivo `breaker`)")
	}
	_, cierres := cola.snapshot()
	if len(cierres) != 1 || cierres[0].intentJSON != app.SobreOmitido(app.MotivoBreaker) {
		t.Fatalf("se esperaba el sobre `breaker`, got %+v", cierres)
	}
	if c.Omitidos() != 1 {
		t.Errorf("Omitidos: got %d want 1", c.Omitidos())
	}
}

// TestFalloDeInferencia_DejaElLoteEnTomado: un fallo NO cierra el lote (lo rescata el barrido) y sí
// castiga al breaker.
func TestFalloDeInferencia_DejaElLoteEnTomado(t *testing.T) {
	cola := &colaFake{pendientes: []*app.ColaLote{loteDe("s1", "quiero una pizza")}}
	cls := &clasificadorFake{err: errors.New("ollama caído")}
	br := nuevoBreakerFake()

	c, err := correr(t, Deps{Cola: cola, Clasificador: cls, Breaker: br, Log: &logCaptura{}}, 2)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	_, cierres := cola.snapshot()
	if len(cierres) != 0 {
		t.Errorf("un fallo de inferencia NO debe cerrar el lote (se reintenta por el barrido), hubo %d cierres", len(cierres))
	}
	if br.fallos != 1 {
		t.Errorf("el fallo debe castigar al breaker (fallos=%d)", br.fallos)
	}
	if c.Fallos() != 1 {
		t.Errorf("Fallos: got %d want 1", c.Fallos())
	}
}

// TestPanicoDelClasificador_SeConvierteEnError: el aislamiento heredado del Plan 029 sigue vivo en el
// worker — un pánico nunca tumba el proceso que vacía la cola.
func TestPanicoDelClasificador_SeConvierteEnError(t *testing.T) {
	cola := &colaFake{pendientes: []*app.ColaLote{loteDe("s1", "quiero una pizza")}}
	cls := &clasificadorFake{panico: true}
	br := nuevoBreakerFake()

	c, err := correr(t, Deps{Cola: cola, Clasificador: cls, Breaker: br, Log: &logCaptura{}}, 2)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if c.Fallos() != 1 || br.fallos != 1 {
		t.Errorf("un pánico debe contarse como fallo (fallos=%d breaker=%d)", c.Fallos(), br.fallos)
	}
}

// TestLoteRelevado_SeCuentaEnInfo_YNoSeReintenta es la carrera del lease funcionando como debe.
func TestLoteRelevado_SeCuentaEnInfo_YNoSeReintenta(t *testing.T) {
	cola := &colaFake{
		pendientes: []*app.ColaLote{loteDe("s1", "quiero una pizza")},
		errMarcar:  fmt.Errorf("cerrar lote: %w", app.ErrLoteRelevado),
	}
	cls := &clasificadorFake{res: classifier.Classification{Intent: "crear_pedido", Confidence: 0.9}}
	log := &logCaptura{}

	c, err := correr(t, Deps{Cola: cola, Clasificador: cls, Breaker: nuevoBreakerFake(), Log: log}, 2)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if c.Relevados() != 1 {
		t.Errorf("Relevados: got %d want 1", c.Relevados())
	}
	if c.Fallos() != 0 {
		t.Errorf("un relevo NO es un fallo (Fallos=%d)", c.Fallos())
	}
	if c.Clasificados() != 0 {
		t.Errorf("un cierre relevado no cuenta como clasificado (Clasificados=%d)", c.Clasificados())
	}
	_, cierres := cola.snapshot()
	if len(cierres) != 1 {
		t.Errorf("el cierre relevado NO se reintenta, hubo %d intentos", len(cierres))
	}
	e, ok := log.buscar("relevado")
	if !ok {
		t.Fatal("el relevo debe dejar una línea de log")
	}
	if e.nivel != "info" {
		t.Errorf("el relevo se registra en Info (no escala a Error), got %q", e.nivel)
	}
}

// TestBarridoDeLeases_SumaRescatadosYAvisa comprueba la goroutine del barrido: llama al puerto con el
// lease configurado, suma al contador y avisa en Warn.
func TestBarridoDeLeases_SumaRescatadosYAvisa(t *testing.T) {
	cola := &colaFake{rescatables: 3}
	log := &logCaptura{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := New(Deps{
		Cola:         cola,
		Clasificador: &clasificadorFake{},
		Breaker:      nuevoBreakerFake(),
		Despertador:  NewPollFijo(5 * time.Millisecond),
		Log:          log,
		Lease:        10 * time.Millisecond,
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

// TestParadaLimpia_SinLotesAMedias: al cancelar, Run devuelve, y el lote cuya inferencia YA terminó se
// cierra igualmente (el UPDATE va sobre un contexto desligado).
func TestParadaLimpia_CierraElLoteYaClasificado(t *testing.T) {
	cola := &colaFake{pendientes: []*app.ColaLote{loteDe("s1", "quiero una pizza")}}
	cls := &clasificadorFake{res: classifier.Classification{Intent: "crear_pedido", Confidence: 0.9}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := New(Deps{
		Cola:         cola,
		Clasificador: &clasificadorLento{inner: cls, cancelar: cancel},
		Breaker:      nuevoBreakerFake(),
		Despertador:  NewPollFijo(time.Millisecond),
		Log:          &logCaptura{},
		Lease:        time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	hecho := make(chan error, 1)
	go func() { hecho <- c.Run(ctx) }()

	select {
	case err := <-hecho:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("Run no terminó tras la cancelación")
	}

	_, cierres := cola.snapshot()
	if len(cierres) != 1 {
		t.Fatalf("una inferencia ya pagada debe cerrarse aunque el proceso se esté apagando, hubo %d cierres", len(cierres))
	}
	if c.Clasificados() != 1 {
		t.Errorf("Clasificados: got %d want 1", c.Clasificados())
	}
}

// clasificadorLento cancela el contexto del proceso JUSTO ANTES de devolver el resultado: reproduce el
// SIGTERM que llega con la inferencia recién terminada y el UPDATE aún sin hacer.
type clasificadorLento struct {
	inner    *clasificadorFake
	cancelar context.CancelFunc
	una      sync.Once
}

func (c *clasificadorLento) Classify(ctx context.Context, texto string) (classifier.Classification, error) {
	res, err := c.inner.Classify(ctx, texto)
	c.una.Do(func() { c.cancelar() })
	return res, err
}

// TestNew_ValidaDependencias: sin cola o sin clasificador no hay bucle posible; el resto tiene default.
func TestNew_ValidaDependencias(t *testing.T) {
	if _, err := New(Deps{Clasificador: &clasificadorFake{}}); err == nil {
		t.Error("sin cola debe fallar")
	}
	if _, err := New(Deps{Cola: &colaFake{}}); err == nil {
		t.Error("sin clasificador debe fallar")
	}
	c, err := New(Deps{Cola: &colaFake{}, Clasificador: &clasificadorFake{}})
	if err != nil {
		t.Fatalf("con las dos obligatorias debe construir: %v", err)
	}
	if cap(c.sem) != DefaultMaxConcurrent {
		t.Errorf("el semáforo por defecto es de %d plaza(s), got %d", DefaultMaxConcurrent, cap(c.sem))
	}
	if c.maxFilas != app.DefaultColaClaimMaxFilas {
		t.Errorf("MaxFilas por defecto: got %d want %d", c.maxFilas, app.DefaultColaClaimMaxFilas)
	}
	if c.lease != app.DefaultColaLeaseSegundos*time.Second {
		t.Errorf("Lease por defecto: got %v want %v", c.lease, app.DefaultColaLeaseSegundos*time.Second)
	}
}

// TestSemaforoDeUnaPlaza_NoSolapaInferencias es el criterio de T2.3 en versión de escritorio: con el
// semáforo en 1 nunca hay dos Classify a la vez.
func TestSemaforoDeUnaPlaza_NoSolapaInferencias(t *testing.T) {
	cola := &colaFake{pendientes: []*app.ColaLote{
		loteDe("s1", "uno"), loteDe("s2", "dos"), loteDe("s3", "tres"),
	}}
	vigilante := &clasificadorVigilante{res: classifier.Classification{Intent: "crear_pedido", Confidence: 0.9}}

	if _, err := correr(t, Deps{Cola: cola, Clasificador: vigilante, Breaker: nuevoBreakerFake(),
		Log: &logCaptura{}, MaxConcurrent: 1}, 2); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if vigilante.maxSimultaneas() != 1 {
		t.Errorf("con el semáforo en 1 nunca puede haber dos inferencias solapadas, hubo %d", vigilante.maxSimultaneas())
	}
	if _, cierres := cola.snapshot(); len(cierres) != 3 {
		t.Errorf("los tres lotes deben cerrarse, hubo %d", len(cierres))
	}
}

// clasificadorVigilante mide cuántas inferencias coinciden en el tiempo.
type clasificadorVigilante struct {
	mu    sync.Mutex
	res   classifier.Classification
	vivas int
	pico  int
}

func (c *clasificadorVigilante) Classify(_ context.Context, _ string) (classifier.Classification, error) {
	c.mu.Lock()
	c.vivas++
	if c.vivas > c.pico {
		c.pico = c.vivas
	}
	c.mu.Unlock()

	time.Sleep(5 * time.Millisecond) // ventana suficiente para que un solapamiento se vea

	c.mu.Lock()
	c.vivas--
	res := c.res
	c.mu.Unlock()
	return res, nil
}

func (c *clasificadorVigilante) maxSimultaneas() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pico
}

// clasificadorQueMuere cancela el ctx del PROCESO desde dentro de la inferencia y devuelve el error
// del contexto: reproduce el SIGTERM que llega con el lote a mitad de clasificar.
type clasificadorQueMuere struct {
	cancelar context.CancelFunc
	una      sync.Once
}

func (c *clasificadorQueMuere) Classify(ctx context.Context, _ string) (classifier.Classification, error) {
	c.una.Do(func() { c.cancelar() })
	<-ctx.Done()
	return classifier.Classification{}, ctx.Err()
}

// TestApagadoAMitadDeInferencia_NoEnvenenaElBreaker fija el arreglo del orden en procesar(): una
// inferencia cortada por el APAGADO no es un fallo del clasificador y el breaker no debe aprender nada
// de ella.
//
// El bug que cierra este test era silencioso y acumulativo: se llamaba a registrarFallo() —que suma a
// `fallos` Y llama a breaker.RecordFailure()— y JUSTO DESPUÉS se logueaba «no es un fallo del
// clasificador». Cada SIGTERM con lotes en vuelo acercaba el circuito a su umbral, así que un cajero
// reiniciado varias veces podía encontrarse el breaker abierto por su propia muerte anterior.
func TestApagadoAMitadDeInferencia_NoEnvenenaElBreaker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cola := &colaFake{pendientes: []*app.ColaLote{loteDe("s1", "quiero una pizza")}}
	br := nuevoBreakerFake()
	log := &logCaptura{}

	c, err := New(Deps{
		Cola:         cola,
		Clasificador: &clasificadorQueMuere{cancelar: cancel},
		Breaker:      br,
		Despertador:  NewPollFijo(time.Millisecond),
		Log:          log,
		Lease:        time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	hecho := make(chan error, 1)
	go func() { hecho <- c.Run(ctx) }()
	select {
	case err := <-hecho:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("Run no terminó tras la cancelación")
	}

	br.mu.Lock()
	fallosBreaker := br.fallos
	br.mu.Unlock()
	if fallosBreaker != 0 {
		t.Errorf("el apagado NO puede castigar al breaker, se le registraron %d fallos", fallosBreaker)
	}
	if c.Fallos() != 0 {
		t.Errorf("el apagado NO es un fallo del cajero (Fallos=%d)", c.Fallos())
	}
	if c.AperturasBreaker() != 0 {
		t.Errorf("el apagado no puede abrir el circuito (aperturas=%d)", c.AperturasBreaker())
	}
	if _, cierres := cola.snapshot(); len(cierres) != 0 {
		t.Errorf("el lote cortado NO se cierra: lo rescata el barrido de leases, hubo %d cierres", len(cierres))
	}
	if _, ok := log.buscar("cortada por el apagado"); !ok {
		t.Error("el apagado a mitad de inferencia debe dejar constancia en el log")
	}
}

// TestLoteSinTexto_AvisaEnWarn_YNoLlamaAOllama: la defensa en profundidad contra un lote sin texto ya
// existía, pero cerraba el lote EN SILENCIO. Una defensa que se dispara y no lo dice es una defensa
// invisible: si se activa, es que el listener no marcó `sin_texto` en T1.8 o el descifrado devolvió
// cadena vacía — un bug aguas arriba que hay que poder ver en el log.
func TestLoteSinTexto_AvisaEnWarn_YNoLlamaAOllama(t *testing.T) {
	cola := &colaFake{pendientes: []*app.ColaLote{loteDe("s1", "   ", "\n\t")}}
	cls := &clasificadorFake{}
	log := &logCaptura{}

	c, err := correr(t, Deps{Cola: cola, Clasificador: cls, Breaker: nuevoBreakerFake(), Log: log}, 2)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(cls.recibidas()) != 0 {
		t.Error("un lote sin texto NO debe quemar una inferencia")
	}
	_, cierres := cola.snapshot()
	if len(cierres) != 1 || cierres[0].intentJSON != app.SobreOmitido(app.MotivoSinTexto) {
		t.Fatalf("se esperaba el sobre `sin_texto`, got %+v", cierres)
	}
	if c.Omitidos() != 1 {
		t.Errorf("Omitidos: got %d want 1", c.Omitidos())
	}

	e, ok := log.buscar("SIN TEXTO")
	if !ok {
		t.Fatal("el camino `sin_texto` del cajero debe AVISAR, no cerrarse en silencio")
	}
	if e.nivel != "warn" {
		t.Errorf("es un aviso (algo aguas arriba falló), no un Info: got %q", e.nivel)
	}
	claves := map[string]bool{}
	for i := 0; i+1 < len(e.args); i += 2 {
		if k, ok := e.args[i].(string); ok {
			claves[k] = true
		}
	}
	if !claves["session_id"] || !claves["mensajes"] {
		t.Errorf("el aviso debe llevar session_id y mensajes, lleva %v", claves)
	}
	// 🔴 INV-051.1: ni el chat_jid completo ni el texto.
	if strings.Contains(log.texto(), "593999@s.whatsapp.net") {
		t.Error("INV-051.1 ROTA: el chat_jid completo apareció en el log")
	}
}

// TestAperturasBreaker_CuentaElFlancoUnaVez usa el breaker REAL (Breaker nil ⇒ el default) para
// comprobar que la métrica sigue viva tras mover la detección del flanco dentro del breaker: cinco
// inferencias fallidas abren el circuito UNA vez, no cinco ni cero.
func TestAperturasBreaker_CuentaElFlancoUnaVez(t *testing.T) {
	var lotes []*app.ColaLote
	for i := range breaker.DefaultThreshold {
		lotes = append(lotes, loteDe(fmt.Sprintf("s%d", i), "quiero una pizza"))
	}
	cola := &colaFake{pendientes: lotes}
	cls := &clasificadorFake{err: errors.New("ollama caído")}
	log := &logCaptura{}

	// Breaker: nil ⇒ New construye el real con la calibración por defecto (5 fallos / 60 s).
	c, err := correr(t, Deps{Cola: cola, Clasificador: cls, Log: log, MaxConcurrent: 1}, 3)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if c.Fallos() != int64(breaker.DefaultThreshold) {
		t.Errorf("Fallos: got %d want %d", c.Fallos(), breaker.DefaultThreshold)
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
}

// TestLatidoDeContadores_SeEmitePeriodicamente cubre el hueco del «se construye y no se cablea»: los
// seis contadores no los lee ningún plano de control (el cajero no tiene), así que sin este latido sólo
// serían legibles cuando el proceso muere — justo cuando ya no sirven para diagnosticar nada.
func TestLatidoDeContadores_SeEmitePeriodicamente(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := &logCaptura{}
	c, err := New(Deps{
		Cola:         &colaFake{},
		Clasificador: &clasificadorFake{},
		Breaker:      nuevoBreakerFake(),
		Despertador:  NewPollFijo(time.Millisecond),
		Log:          log,
		Lease:        time.Hour,
		StatsEvery:   5 * time.Millisecond,
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
	claves := map[string]bool{}
	for i := 0; i+1 < len(e.args); i += 2 {
		if k, ok := e.args[i].(string); ok {
			claves[k] = true
		}
	}
	for _, k := range []string{"clasificados", "omitidos", "relevados", "fallos", "rescatados",
		"aperturas_breaker", "circuito"} {
		if !claves[k] {
			t.Errorf("el latido debe llevar el bloque COMPLETO: falta %q", k)
		}
	}
}

// TestLatidoDeContadores_CeroLoDesactiva: StatsEvery <= 0 es «cállate», no «usa el default».
func TestLatidoDeContadores_CeroLoDesactiva(t *testing.T) {
	log := &logCaptura{}
	if _, err := correr(t, Deps{Cola: &colaFake{}, Clasificador: &clasificadorFake{},
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

// TestCierreTimeout_PorDebajoDelSigkillDelSupervisor: el plazo del UPDATE de cierre vive por debajo del
// StopTimeout del supervisor (10 s) a propósito. Con los dos iguales, el peor caso es un empate en el
// que el SIGKILL mata el UPDATE que salva una inferencia YA PAGADA, que es justo lo que el
// context.WithoutCancel de cerrar() venía a evitar. Este test es la nota adhesiva sobre ese número.
func TestCierreTimeout_PorDebajoDelSigkillDelSupervisor(t *testing.T) {
	// El default del supervisor (internal/adapters/supervisor · defaultStopTimeout). No se importa
	// para no atar el paquete del bucle a un adaptador; se replica con el porqué escrito al lado.
	const stopTimeoutDelSupervisor = 10 * time.Second
	if cierreTimeout >= stopTimeoutDelSupervisor {
		t.Fatalf("cierreTimeout (%v) debe quedar POR DEBAJO del StopTimeout del supervisor (%v): "+
			"si no, el SIGKILL puede matar el UPDATE de una inferencia ya pagada", cierreTimeout, stopTimeoutDelSupervisor)
	}
}

// TestAfinidadOllama_NuncaEsFatal: la comprobación de T2.8 puede fallar, y cuando falla el cajero
// arranca igual dejando el hecho en Warn.
func TestAfinidadOllama_NuncaEsFatal(t *testing.T) {
	log := &logCaptura{}
	cola := &colaFake{}

	if _, err := correr(t, Deps{Cola: cola, Clasificador: &clasificadorFake{}, Breaker: nuevoBreakerFake(),
		Log: log, OllamaURL: "http://198.51.100.7:11434"}, 2); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Una URL remota no es observable en Linux, y en macOS no lo es ninguna: en ambos casos el
	// resultado esperado es el MISMO — un aviso, no un fallo de arranque.
	if _, ok := log.buscar("afinidad de CPU"); !ok {
		t.Error("el arranque debe dejar constancia de la comprobación de afinidad (T2.8)")
	}
	if _, ok := log.buscar("arrancando"); !ok {
		t.Error("el cajero debe haber arrancado pese al fallo de la comprobación")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// T2.19 · el freno del LOTE VENENOSO
// ─────────────────────────────────────────────────────────────────────────────

// colaVenenosa modela el CICLO COMPLETO que hacía inmortal a un lote que siempre falla, y por eso no
// vale el colaFake de arriba: aquel sirve cada lote UNA vez y agota la lista, que es justo lo contrario de
// lo que pasa en producción.
//
// El ciclo real: el cajero reclama el lote → la inferencia falla → nadie lo cierra → las filas se quedan
// en `tomado` → el barrido las devuelve a `nuevo` SIN TOCAR SU `seq` → y como el claim elige la
// conversación de `seq` más bajo, el siguiente claim SE LLEVA EL MISMO LOTE. Con MAX_CONCURRENT=1 eso deja
// la cola entera parada para siempre. Este doble reproduce esa inmortalidad: mientras nadie cierre el
// veneno, `Reclamar` lo vuelve a servir con el contador de intentos una vuelta más alto, y el lote sano
// que espera detrás NO se sirve jamás.
//
// `topeReclamos` es una válvula de seguridad para el propio test: sin ella, un cajero que no cortara
// giraría hasta el plazo de `correr` y el rojo diría «goroutine colgada» en vez de decir qué falló.
type colaVenenosa struct {
	mu               sync.Mutex
	veneno           *app.ColaLote // el lote cuya inferencia falla siempre
	siguiente        *app.ColaLote // el lote sano que espera DETRÁS del veneno (la cola bloqueada)
	topeReclamos     int
	intentos         int64
	reclamosVeneno   int
	cerrado          bool
	servidoSiguiente bool
	cierres          []cierre
}

var _ app.ColaCajero = (*colaVenenosa)(nil)

func (c *colaVenenosa) Reclamar(_ context.Context, _ int) (*app.ColaLote, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.cerrado {
		if c.reclamosVeneno >= c.topeReclamos {
			return nil, nil // válvula del test: el cajero no cortó, deja de girar para que asevere
		}
		c.reclamosVeneno++
		c.intentos++
		// Copia con el contador al día: el store devuelve el valor YA incrementado por el UPDATE del claim.
		lote := *c.veneno
		lote.Intentos = c.intentos
		return &lote, nil
	}
	if c.siguiente != nil && !c.servidoSiguiente {
		c.servidoSiguiente = true
		lote := *c.siguiente
		lote.Intentos = 1
		return &lote, nil
	}
	return nil, nil
}

func (c *colaVenenosa) MarcarClasificado(_ context.Context, lote *app.ColaLote, intentJSON string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cierres = append(c.cierres, cierre{lote: lote, intentJSON: intentJSON})
	if lote.SessionID == c.veneno.SessionID {
		c.cerrado = true
	}
	return nil
}

func (c *colaVenenosa) BarrerLeasesVencidos(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

func (c *colaVenenosa) estado() (reclamosVeneno int, cerrado, servidoSiguiente bool, cierres []cierre) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reclamosVeneno, c.cerrado, c.servidoSiguiente, append([]cierre(nil), c.cierres...)
}

// clasificadorVenenoso falla SIEMPRE con un texto concreto y responde bien con cualquier otro. Es la
// diferencia entre un fallo transitorio (que se arregla solo y merece el reintento gratis) y uno
// permanente (que no se arregla nunca y bloquea la cola), y sin ella el test no puede distinguirlos.
type clasificadorVenenoso struct {
	mu       sync.Mutex
	veneno   string
	res      classifier.Classification
	entradas []string
}

func (f *clasificadorVenenoso) Classify(_ context.Context, texto string) (classifier.Classification, error) {
	f.mu.Lock()
	f.entradas = append(f.entradas, texto)
	veneno, res := f.veneno, f.res
	f.mu.Unlock()
	if strings.Contains(texto, veneno) {
		return classifier.Classification{}, errors.New("el modelo se atraganta con este texto")
	}
	return res, nil
}

func (f *clasificadorVenenoso) inferencias() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.entradas)
}

// TestLoteVenenoso_SeAbandonaTrasElTope_YLaColaAVANZA ES EL TEST QUE JUSTIFICA T2.19, y lo que asevera no
// es «se escribe un sobre nuevo» sino «LA COLA VUELVE A MOVERSE».
//
// El defecto: un lote cuya inferencia falla siempre conserva el `seq` más bajo de la cola, vuelve a `nuevo`
// por el barrido y se lo lleva otra vez el claim siguiente. Con WAPP_WORKER_MAX_CONCURRENT=1 —el default—
// eso es la cola ENTERA congelada: el lote sano que está detrás no se clasifica NUNCA, y el único síntoma
// es el contador de fallos subiendo. Nada falla, nada avisa, y los mensajes de todos los demás clientes se
// quedan sin clasificar hasta que el tope de la cola los descarta.
//
// Aquí el veneno se reclama exactamente `MaxIntentos` veces, en la última se cierra con `fallo_repetido`,
// y —lo que importa— el lote que estaba detrás se clasifica.
func TestLoteVenenoso_SeAbandonaTrasElTope_YLaColaAVANZA(t *testing.T) {
	const maxIntentos = 3
	veneno := loteDe("sesion-envenenada", "un texto que el modelo no traga")
	sano := loteDe("sesion-sana", "quiero dos empanadas")

	cola := &colaVenenosa{veneno: veneno, siguiente: sano, topeReclamos: 10}
	cls := &clasificadorVenenoso{
		veneno: "no traga",
		res:    classifier.Classification{Intent: "crear_pedido", Confidence: 0.91},
	}
	log := &logCaptura{}

	c, err := correr(t, Deps{
		Cola:         cola,
		Clasificador: cls,
		Breaker:      nuevoBreakerFake(),
		Log:          log,
		MaxIntentos:  maxIntentos,
	}, 1)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	reclamos, cerrado, servidoSiguiente, cierres := cola.estado()

	// (1) El veneno se abandonó, y ni una vuelta antes ni una después.
	if !cerrado {
		t.Fatalf("el lote venenoso NUNCA se cerró: se reclamó %d veces y la cola sigue bloqueada (ESTE es el bug)", reclamos)
	}
	if reclamos != maxIntentos {
		t.Errorf("reclamos del lote venenoso: got %d want %d (se abandona EN el intento del tope, no antes ni después)",
			reclamos, maxIntentos)
	}

	// (2) Se cerró con el sobre correcto. El mensaje sale sin intent: se pierde la clasificación, nunca el
	// mensaje.
	if len(cierres) != 2 {
		t.Fatalf("cierres: got %d want 2 (el veneno abandonado y el lote sano clasificado): %+v", len(cierres), cierres)
	}
	if cierres[0].lote.SessionID != veneno.SessionID {
		t.Fatalf("el primer cierre debía ser el del lote venenoso, fue de %q", cierres[0].lote.SessionID)
	}
	if got, want := cierres[0].intentJSON, app.SobreOmitido(app.MotivoFalloRepetido); got != want {
		t.Errorf("sobre del lote abandonado: got %q want %q", got, want)
	}

	// (3) 🔴 Y LA COLA AVANZÓ. Esto es lo que el plan compra; lo demás es contabilidad.
	if !servidoSiguiente {
		t.Fatal("la cola NO avanzó: el lote que esperaba detrás del veneno nunca se reclamó")
	}
	if cierres[1].lote.SessionID != sano.SessionID {
		t.Fatalf("el segundo cierre debía ser el del lote sano, fue de %q", cierres[1].lote.SessionID)
	}
	if !strings.Contains(cierres[1].intentJSON, `"intent":"crear_pedido"`) {
		t.Errorf("el lote sano debía clasificarse de verdad, su sobre es %q", cierres[1].intentJSON)
	}

	// (4) Contadores. `abandonados` va aparte de `omitidos` porque es el único motivo que señala una
	// conversación atascada, pero es un SUBCONJUNTO: el lote abandonado suma en los dos.
	if c.Abandonados() != 1 {
		t.Errorf("Abandonados: got %d want 1", c.Abandonados())
	}
	if c.Omitidos() != 1 {
		t.Errorf("Omitidos: got %d want 1 (un abandono también es un cierre con sobre de omisión)", c.Omitidos())
	}
	if c.Clasificados() != 1 {
		t.Errorf("Clasificados: got %d want 1 (el lote sano)", c.Clasificados())
	}
	if c.Fallos() != maxIntentos {
		t.Errorf("Fallos: got %d want %d (los tres fallos de inferencia ocurrieron y cuentan)", c.Fallos(), maxIntentos)
	}
	// El abandono NO quema una inferencia extra: se paga una por intento y ninguna más.
	if got := cls.inferencias(); got != maxIntentos+1 {
		t.Errorf("inferencias: got %d want %d (%d del veneno + 1 del lote sano)", got, maxIntentos+1, maxIntentos)
	}

	// (5) El log dice con todas las letras que se ABANDONA, y respeta INV-051.1.
	e, ok := log.buscar("ABANDONADO")
	if !ok {
		t.Fatal("abandonar un lote tiene que dejar una línea de log explícita: un mensaje que sale sin intent " +
			"porque el modelo no pudo con él es algo que el operador querrá ver")
	}
	if e.nivel != "warn" {
		t.Errorf("el abandono se registra en Warn (el mensaje NO se pierde, sale sin intent), got %q", e.nivel)
	}
	texto := log.texto()
	if strings.Contains(texto, veneno.ChatJID) {
		t.Errorf("INV-051.1: el chat_jid EN CLARO (el teléfono del cliente) no puede entrar en el log; va hasheado")
	}
	if strings.Contains(texto, "no traga") {
		t.Errorf("INV-051.1: el texto del mensaje no puede entrar en el log")
	}
	if !strings.Contains(texto, chatJIDHash(veneno.ChatJID)) {
		t.Errorf("el log del abandono debe llevar chat_jid_hash para poder identificar la conversación atascada")
	}
}

// TestFalloDeInferencia_PorDebajoDelTopeNoCierra: el reintento GRATIS sigue vivo. Un Ollama que se
// reinicia o un pico de carga son fallos transitorios que se arreglan solos, y cerrar el lote a la primera
// convertiría una clasificación recuperable en una pérdida definitiva. El freno solo muerde en el tope.
func TestFalloDeInferencia_PorDebajoDelTopeNoCierra(t *testing.T) {
	lote := loteDe("s1", "quiero una pizza")
	lote.Intentos = 2 // le queda uno: con MaxIntentos=3 el corte aún NO debe actuar
	cola := &colaFake{pendientes: []*app.ColaLote{lote}}
	cls := &clasificadorFake{err: errors.New("ollama reiniciándose")}

	c, err := correr(t, Deps{
		Cola:         cola,
		Clasificador: cls,
		Breaker:      nuevoBreakerFake(),
		Log:          &logCaptura{},
		MaxIntentos:  3,
	}, 2)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	_, cierres := cola.snapshot()
	if len(cierres) != 0 {
		t.Fatalf("con intentos=2 y max=3 el lote NO se cierra (le queda un reintento gratis), hubo %d cierres: %+v",
			len(cierres), cierres)
	}
	if c.Abandonados() != 0 {
		t.Errorf("Abandonados: got %d want 0", c.Abandonados())
	}
	if c.Fallos() != 1 {
		t.Errorf("Fallos: got %d want 1 (el fallo ocurrió y cuenta igual)", c.Fallos())
	}
}

// TestFalloDeInferencia_CierraJustoEnElTope es la otra mitad de la frontera: con intentos = max se cierra
// en ESTE fallo, sin esperar a una vuelta más. Los dos tests juntos fijan el `>=` exacto; con solo uno de
// ellos, un `>` de más o de menos pasaría desapercibido.
func TestFalloDeInferencia_CierraJustoEnElTope(t *testing.T) {
	lote := loteDe("s1", "quiero una pizza")
	lote.Intentos = 3
	cola := &colaFake{pendientes: []*app.ColaLote{lote}}
	cls := &clasificadorFake{err: errors.New("el modelo se atraganta")}

	c, err := correr(t, Deps{
		Cola:         cola,
		Clasificador: cls,
		Breaker:      nuevoBreakerFake(),
		Log:          &logCaptura{},
		MaxIntentos:  3,
	}, 2)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	_, cierres := cola.snapshot()
	if len(cierres) != 1 {
		t.Fatalf("con intentos=3 y max=3 el lote se abandona EN este fallo, hubo %d cierres", len(cierres))
	}
	if got, want := cierres[0].intentJSON, app.SobreOmitido(app.MotivoFalloRepetido); got != want {
		t.Errorf("sobre del lote abandonado: got %q want %q", got, want)
	}
	if c.Abandonados() != 1 || c.Omitidos() != 1 {
		t.Errorf("contadores: abandonados=%d omitidos=%d, want 1 y 1", c.Abandonados(), c.Omitidos())
	}
}

// TestMaxIntentos_CaeAlDefault: un Deps sin MaxIntentos (o con 0) usa DefaultMaxIntentos, no 0 — que
// abandonaría TODOS los lotes en su primer claim, sin llamar a Ollama ni una vez, apagando la
// clasificación entera en silencio.
func TestMaxIntentos_CaeAlDefault(t *testing.T) {
	c, err := New(Deps{Cola: &colaFake{}, Clasificador: &clasificadorFake{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.maxIntentos != DefaultMaxIntentos {
		t.Fatalf("maxIntentos sin fijar: got %d want %d", c.maxIntentos, DefaultMaxIntentos)
	}
	if DefaultMaxIntentos != 3 {
		t.Fatalf("DefaultMaxIntentos = %d, want 3 (dos reintentos gratis y a la tercera se abandona)", DefaultMaxIntentos)
	}
}
