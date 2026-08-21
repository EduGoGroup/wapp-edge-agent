package latencia

// latido_test.go — los tests del EMISOR (Plan 051 Ola 3 · T3.13).
//
// Molde: internal/app/cajero/cajero_test.go (TestLatidoDeContadores_CeroLoDesactiva). El cero de la
// cadencia es un valor legítimo («cállate»), no un dedazo que caiga al default; y el bloque FINAL se
// emite pase lo que pase, porque es el que cierra la sesión de medida.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// --- dobles ---

type entradaLog struct {
	msg  string
	args []any
}

// clave busca el valor de una clave del bloque. Los args son pares (clave, valor) con semántica slog.
func (e entradaLog) clave(k string) (any, bool) {
	for i := 0; i+1 < len(e.args); i += 2 {
		if s, ok := e.args[i].(string); ok && s == k {
			return e.args[i+1], true
		}
	}
	return nil, false
}

// claves proyecta el JUEGO DE CAMPOS de la entrada, en orden.
func (e entradaLog) claves() []string {
	var ks []string
	for i := 0; i+1 < len(e.args); i += 2 {
		if s, ok := e.args[i].(string); ok {
			ks = append(ks, s)
		}
	}
	return ks
}

type logCaptura struct {
	mu       sync.Mutex
	entradas []entradaLog
}

var _ sharedlogger.Logger = (*logCaptura)(nil)

func (l *logCaptura) añadir(msg string, args []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entradas = append(l.entradas, entradaLog{msg: msg, args: append([]any(nil), args...)})
}

func (l *logCaptura) Debug(msg string, args ...any)     { l.añadir(msg, args) }
func (l *logCaptura) Info(msg string, args ...any)      { l.añadir(msg, args) }
func (l *logCaptura) Warn(msg string, args ...any)      { l.añadir(msg, args) }
func (l *logCaptura) Error(msg string, args ...any)     { l.añadir(msg, args) }
func (l *logCaptura) With(_ ...any) sharedlogger.Logger { return l }

// latidos devuelve solo las emisiones de ESTE bloque, filtradas por el prefijo de grep.
func (l *logCaptura) latidos() []entradaLog {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []entradaLog
	for _, e := range l.entradas {
		if e.msg == msgLatido {
			out = append(out, e)
		}
	}
	return out
}

// porEmision separa las periódicas de la final.
func (l *logCaptura) porEmision(quien string) []entradaLog {
	var out []entradaLog
	for _, e := range l.latidos() {
		if v, ok := e.clave("emision"); ok && v == quien {
			out = append(out, e)
		}
	}
	return out
}

// colaFake responde el COUNT sin tocar disco.
type colaFake struct {
	p     app.ColaPendientes
	err   error
	veces int
}

var _ app.ColaContador = (*colaFake)(nil)

func (c *colaFake) Pendientes(_ context.Context) (app.ColaPendientes, error) {
	c.veces++
	return c.p, c.err
}

// correrLatido arranca el latido, lo deja vivir `vida` y lo para esperando a que emita su bloque final.
func correrLatido(t *testing.T, d Deps, vida time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	fin := make(chan struct{})
	go func() {
		defer close(fin)
		Latido(ctx, d)
	}()
	time.Sleep(vida)
	cancel()
	select {
	case <-fin:
	case <-time.After(5 * time.Second):
		t.Fatal("Latido no retornó tras cancelar el contexto: el daemon se quedaría esperándolo en el cierre")
	}
}

// camposObligatorios es el CONTRATO DE LA LÍNEA con quien la lee en el VPS. Cada campo está aquí porque sin
// él la línea deja de ser interpretable: `n` da la población (un p99 sin ella no significa nada),
// `p99_bucket` da la resolución, la serie de descartes dice de qué población salió el número bueno, el
// acumulado sobrevive a que nadie mirara la ventana interesante, `conteo_ms` publica el efecto observador,
// y los dos de la puerta (T1.13) dicen si el Edge está teniendo que hacer reofrecer entrantes a WhatsApp —
// que es el único sitio donde eso se ve, porque el acumulado por sesión del listener no lo publica nadie.
//
// `despachador` (T3.17) entró aquí por el mismo criterio, y es el único campo que no habla del cronómetro:
// dice si la palanca de diagnóstico está echada, y con ella echada TODOS los demás números de la línea
// significan otra cosa (la cola sube porque nadie drena, no porque haya carga). Un campo que solo apareciera
// cuando la palanca está puesta no serviría: la pregunta que se hace en el VPS es «¿está puesta?», y esa
// pregunta no se puede contestar con una ausencia.
//
// `inyector` (MP-10 Parte A) entró por ese mismo criterio y es el segundo campo que no habla del cronómetro:
// dice si la palanca del inyector de entrantes sintéticos está echada, y con ella echada la POBLACIÓN de los
// percentiles puede estar hecha de mensajes fabricados dentro del proceso en vez de tráfico de clientes. Los
// dos casos producen una línea idéntica en todo lo demás, así que sin este campo un p99 de una tanda de
// prueba es indistinguible de un p99 de producción — y el criterio que se juzga con esta línea (INV-051.2)
// es sobre el camino de producción.
//
// `descartes_perfil_pasivo` (Plan 046 · T2.3) entró como TERCER contador de la puerta, y es el único que no
// habla de una degradación: son los entrantes cortados por venir a una sesión con perfil PASIVO. Está en la
// lista por dos razones — es la ÚNICA huella de un filtro que por diseño no deja fila, no sube al cable y
// acusa igual que si hubiera entregado; y porque LE QUITA CUENTA a los descartes por ventana (el corte va
// antes del ADR-0037), así que sin él una caída de aquella serie se lee como una mejora del Edge cuando lo
// que ha pasado es que hay más sesiones calladas.
var camposObligatorios = []string{
	"emision", "despachador", "inyector", "ventana_ms", "uptime_s",
	"n", "p50_ms", "p95_ms", "p99_ms", "p99_bucket",
	"n_descartes", "p99_ms_descartes",
	"n_acum", "p99_ms_acum", "max_ms_acum",
	"cola_enqueue_errores", "cola_enqueue_panics", "descartes_perfil_pasivo",
	"cola_pendientes", "cola_nuevo", "cola_tomado", "cola_clasificado", "conteo_ms",
}

func exigirCampos(t *testing.T, e entradaLog, quien string) {
	t.Helper()
	for _, k := range camposObligatorios {
		if _, ok := e.clave(k); !ok {
			t.Errorf("el bloque %s debe llevar %q: sin él la línea no es interpretable en el journal", quien, k)
		}
	}
}

// --- tests ---

// TestLatido_ElCeroLoDesactivaPeroElBloqueFinalSaleIgual: apagar el latido periódico NO puede significar
// apagar también el resumen. Con la periódica apagada, el bloque final es la ÚNICA forma de saber qué pasó
// en toda la sesión de medida.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - cambiar `if d.Cada > 0` por `>= 0` ⇒ time.NewTicker(0) entra en pánico y tumba el test.
//   - hacer que un Cada <= 0 caiga a un default ⇒ aparecen emisiones periódicas donde no debe haber ninguna.
//   - mover el bloque final fuera del `case <-ctx.Done()` ⇒ desaparece el resumen.
func TestLatido_ElCeroLoDesactivaPeroElBloqueFinalSaleIgual(t *testing.T) {
	log := &logCaptura{}
	h := Nuevo()
	observarMS(h, Encolado, 1, 3)
	observarMS(h, Descartado, 0.1, 1)
	cola := &colaFake{p: app.ColaPendientes{Total: 2}}

	correrLatido(t, Deps{Hist: h, Cada: 0, Log: log, Cola: cola}, 30*time.Millisecond)

	if n := len(log.porEmision(emisionPeriodica)); n != 0 {
		t.Errorf("con Cada=0 el latido periódico NO debe emitirse, y emitió %d veces", n)
	}
	finales := log.porEmision(emisionFinal)
	if len(finales) != 1 {
		t.Fatalf("el bloque FINAL se emite siempre y exactamente una vez: got %d", len(finales))
	}
	// El bloque final lleva el juego COMPLETO: con la periódica apagada es lo único que se va a leer.
	exigirCampos(t, finales[0], "final")
	if v, ok := finales[0].clave("n"); !ok || v.(uint64) != 3 {
		t.Errorf("el bloque final debe traer la población del intervalo (n=3): got %v", v)
	}
}

// TestLatido_ConCadenciaEmitePeriodicamente y la línea lleva el bloque COMPLETO.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: no arrancar el ticker (tick siempre nil) ⇒ cero emisiones periódicas.
func TestLatido_ConCadenciaEmitePeriodicamente(t *testing.T) {
	log := &logCaptura{}
	h := Nuevo()
	observarMS(h, Encolado, 45, 100)
	observarMS(h, Descartado, 0.2, 20)
	cola := &colaFake{p: app.ColaPendientes{Nuevo: 980, Tomado: 5, Clasificado: 219, Total: 1204}}

	correrLatido(t, Deps{Hist: h, Cada: 5 * time.Millisecond, Log: log, Cola: cola}, 60*time.Millisecond)

	periodicas := log.porEmision(emisionPeriodica)
	if len(periodicas) == 0 {
		t.Fatal("con Cada=5ms el latido tiene que haber emitido al menos una vez")
	}

	// 🔴 El bloque va en UNA SOLA LÍNEA y con TODOS los campos: los logs del VPS van a un fichero y la
	// lectura de campo es un grep por el prefijo. Un bloque repartido en varias líneas es ilegible ahí.
	e := periodicas[0]
	if e.msg != msgLatido {
		t.Fatalf("el prefijo de grep cambió: %q (el runbook busca %q)", e.msg, msgLatido)
	}
	exigirCampos(t, e, "periódico")
	// El veredicto de la ola se lee de estos dos, así que su valor se comprueba: 100 muestras de 45 ms.
	if v, _ := e.clave("p99_ms"); v != 50.0 {
		t.Errorf("p99_ms = %v, se esperaba 50 (100 muestras de 45 ms caen en el tramo 40-50)", v)
	}
	if v, _ := e.clave("p99_bucket"); v != "40-50" {
		t.Errorf("p99_bucket = %v, se esperaba \"40-50\"", v)
	}
	if v, _ := e.clave("n"); v.(uint64) != 100 {
		t.Errorf("n = %v, se esperaba 100: un p99 sin población no significa nada", v)
	}
	if v, _ := e.clave("cola_pendientes"); v.(int64) != 1204 {
		t.Errorf("cola_pendientes = %v, se esperaba 1204", v)
	}
}

// TestBloque_ElFinalNoPuedeDivergirDelPeriodico: los dos bloques salen de `bloque()` a propósito, para que
// un campo nuevo se añada UNA vez y aparezca en los dos (misma razón que Cajero.contadores, que fue quien
// estableció el patrón).
//
// Se ejercita `bloque` DIRECTAMENTE y con el mismo estado en las dos llamadas: pasar por Latido metería el
// reloj de por medio y la comparación dejaría de ser determinista (una ventana sin muestras omite los
// percentiles, y eso es correcto pero no es lo que aquí se juzga).
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: escribir a mano la lista de campos en cualquiera de las dos emisiones
// (p. ej. añadir un campo solo en el camino del `case <-ctx.Done()`).
func TestBloque_ElFinalNoPuedeDivergirDelPeriodico(t *testing.T) {
	h := Nuevo()
	observarMS(h, Encolado, 1, 5)
	observarMS(h, Descartado, 0.1, 2)
	d := Deps{Hist: h, Cada: time.Second, Cola: &colaFake{p: app.ColaPendientes{Total: 1}}}
	t0 := time.Now()

	var prevEncA, prevDescA Muestra
	periodico := d.bloque(context.Background(), emisionPeriodica, t0, t0, time.Now, &prevEncA, &prevDescA)
	var prevEncB, prevDescB Muestra
	final := d.bloque(context.Background(), emisionFinal, t0, t0, time.Now, &prevEncB, &prevDescB)

	got := strings.Join(entradaLog{args: final}.claves(), ",")
	want := strings.Join(entradaLog{args: periodico}.claves(), ",")
	if got != want {
		t.Fatalf("el bloque final y el periódico llevan campos DISTINTOS:\n  final     = %s\n  periodico = %s", got, want)
	}
}

// TestLatido_CadaMuestraCaeEnUnaVentanaYSoloEnUna es la propiedad que hace utilizable la serie de bloques
// que el operador pega en el journal: los `n` de todas las emisiones SUMAN el acumulado. Ni una muestra se
// cuenta dos veces (el delta se calcula) ni se pierde ninguna entre ventanas (las fotos arrancan en cero).
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - no avanzar prevEnc/prevDesc en bloque() ⇒ cada emisión republica el acumulado y la suma se dispara.
//   - arrancar prevEnc/prevDesc con un Snapshot en vez de en cero ⇒ lo medido antes de que la goroutine
//     llegue a ejecutarse no cae en NINGUNA ventana y la suma queda por debajo.
func TestLatido_CadaMuestraCaeEnUnaVentanaYSoloEnUna(t *testing.T) {
	const muestras = 20

	log := &logCaptura{}
	h := Nuevo()
	// Una muestra ANTES de arrancar el latido: es la que caza el arranque con Snapshot.
	observarMS(h, Encolado, 1, 1)

	ctx, cancel := context.WithCancel(context.Background())
	fin := make(chan struct{})
	go func() {
		defer close(fin)
		Latido(ctx, Deps{Hist: h, Cada: 5 * time.Millisecond, Log: log})
	}()
	for i := 0; i < muestras-1; i++ {
		observarMS(h, Encolado, 1, 1)
		time.Sleep(3 * time.Millisecond)
	}
	cancel()
	<-fin

	emisiones := log.latidos()
	if len(emisiones) < 3 {
		t.Fatalf("se esperaban varias emisiones con Cada=5ms sobre ~57ms: got %d", len(emisiones))
	}
	var suma uint64
	for _, e := range emisiones {
		v, ok := e.clave("n")
		if !ok {
			t.Fatalf("una emisión salió sin `n`: %v", e.claves())
		}
		suma += v.(uint64)
	}
	ultima := emisiones[len(emisiones)-1]
	nAcum, _ := ultima.clave("n_acum")
	if nAcum.(uint64) != muestras {
		t.Fatalf("n_acum de la última emisión = %v, se esperaban las %d muestras de toda la vida del proceso",
			nAcum, muestras)
	}
	if suma != muestras {
		t.Fatalf("Σ de los `n` de las %d emisiones = %d, se esperaban %d. Por encima significa que el delta "+
			"no se calcula (cada bloque republica el acumulado); por debajo, que hay muestras que no caen en "+
			"ninguna ventana", len(emisiones), suma, muestras)
	}
}

// TestLatido_SinPoblacionNoPublicaPercentiles: `n=0` con un `p99_ms=0` al lado es un verde falso servido
// en bandeja. Sin muestras, los percentiles NO salen y el `n` explica la ausencia.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: emitir el percentil ignorando el ok=false de Percentil.
func TestLatido_SinPoblacionNoPublicaPercentiles(t *testing.T) {
	log := &logCaptura{}
	correrLatido(t, Deps{Hist: Nuevo(), Cada: 0, Log: log}, 10*time.Millisecond)

	finales := log.porEmision(emisionFinal)
	if len(finales) != 1 {
		t.Fatalf("se esperaba un bloque final: got %d", len(finales))
	}
	if v, ok := finales[0].clave("n"); !ok || v.(uint64) != 0 {
		t.Fatalf("n = %v: sin muestras el campo obligatorio sigue estando y vale 0", v)
	}
	for _, k := range []string{"p50_ms", "p95_ms", "p99_ms", "p99_bucket"} {
		if v, ok := finales[0].clave(k); ok {
			t.Errorf("con n=0 el campo %q no debe emitirse (salió %v): un 0 ahí se lee como «tardó 0 ms», "+
				"que es lo contrario de «no hubo muestras»", k, v)
		}
	}
}

// TestLatido_SinColaElBloqueSaleSinLosCamposDeCola: unos ceros de cola se leerían como «la cola está
// vacía», que es exactamente lo contrario de «no hay de dónde contar».
func TestLatido_SinColaElBloqueSaleSinLosCamposDeCola(t *testing.T) {
	log := &logCaptura{}
	h := Nuevo()
	observarMS(h, Encolado, 1, 1)
	correrLatido(t, Deps{Hist: h, Cada: 0, Log: log, Cola: nil}, 10*time.Millisecond)

	finales := log.porEmision(emisionFinal)
	if len(finales) != 1 {
		t.Fatalf("se esperaba un bloque final: got %d", len(finales))
	}
	for _, k := range []string{"cola_pendientes", "cola_nuevo", "cola_tomado", "cola_clasificado", "conteo_ms"} {
		if _, ok := finales[0].clave(k); ok {
			t.Errorf("sin ColaContador el campo %q no debe salir: un 0 ahí mentiría", k)
		}
	}
}

// TestLatido_ElCountDelCierreCorreConUnContextoVIVO es el gotcha del bloque final: lo dispara la
// CANCELACIÓN del contexto del daemon, así que un COUNT sobre ese mismo contexto fallaría SIEMPRE y el
// bloque más interesante de todos saldría sin el estado de la cola.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: pasar `ctx` (el cancelado) en vez de `context.WithoutCancel(ctx)` al
// bloque final ⇒ el fake ve un contexto muerto y el bloque sale con `cola_error`.
func TestLatido_ElCountDelCierreCorreConUnContextoVivo(t *testing.T) {
	log := &logCaptura{}
	h := Nuevo()
	observarMS(h, Encolado, 1, 1)
	// El fake devuelve el error del contexto si viene muerto: es la forma directa de ver la mutación.
	cola := &colaContextoFake{p: app.ColaPendientes{Total: 7}}

	correrLatido(t, Deps{Hist: h, Cada: 0, Log: log, Cola: cola}, 10*time.Millisecond)

	finales := log.porEmision(emisionFinal)
	if len(finales) != 1 {
		t.Fatalf("se esperaba un bloque final: got %d", len(finales))
	}
	if v, ok := finales[0].clave("cola_error"); ok {
		t.Fatalf("el COUNT del bloque final corrió con el contexto YA CANCELADO (%v): tiene que ir sobre un "+
			"contexto desligado con plazo propio", v)
	}
	if v, _ := finales[0].clave("cola_pendientes"); v.(int64) != 7 {
		t.Fatalf("cola_pendientes = %v, se esperaba 7", v)
	}
}

// colaContextoFake falla si el contexto que recibe ya está muerto.
type colaContextoFake struct{ p app.ColaPendientes }

var _ app.ColaContador = (*colaContextoFake)(nil)

func (c *colaContextoFake) Pendientes(ctx context.Context) (app.ColaPendientes, error) {
	if err := ctx.Err(); err != nil {
		return app.ColaPendientes{}, err
	}
	return c.p, nil
}

// TestLatido_UnCountQueFallaNoSeCome_conteo_ms: un conteo lento que acaba en error es justo el dato que
// hay que ver, así que el efecto observador se publica falle o no.
func TestLatido_UnCountQueFallaNoSeComeElConteoMS(t *testing.T) {
	log := &logCaptura{}
	h := Nuevo()
	observarMS(h, Encolado, 1, 1)
	cola := &colaFake{err: errors.New("colaentrantes: base ocupada")}

	correrLatido(t, Deps{Hist: h, Cada: 0, Log: log, Cola: cola}, 10*time.Millisecond)

	finales := log.porEmision(emisionFinal)
	if len(finales) != 1 {
		t.Fatalf("se esperaba un bloque final: got %d", len(finales))
	}
	if _, ok := finales[0].clave("cola_error"); !ok {
		t.Error("un COUNT fallido tiene que dejar `cola_error` en la línea")
	}
	if _, ok := finales[0].clave("conteo_ms"); !ok {
		t.Error("`conteo_ms` se publica también cuando el COUNT falla: es el efecto observador")
	}
}

// TestLatido_SinHistogramaOSinLogNoArranca: el latido es observabilidad y no puede ser la causa de que
// algo se caiga. Sin nada que publicar (o sin dónde), retorna en el acto.
func TestLatido_SinHistogramaOSinLogNoArranca(t *testing.T) {
	hecho := make(chan struct{})
	go func() {
		defer close(hecho)
		Latido(context.Background(), Deps{Hist: nil, Log: &logCaptura{}, Cada: time.Millisecond})
		Latido(context.Background(), Deps{Hist: Nuevo(), Log: nil, Cada: time.Millisecond})
	}()
	select {
	case <-hecho:
	case <-time.After(2 * time.Second):
		t.Fatal("Latido se quedó colgado sin histograma o sin logger, con un contexto que NUNCA se cancela")
	}
}
