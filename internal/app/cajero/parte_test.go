package cajero

// parte_test.go — el lado ESCRITOR del tubo cajero→daemon (Plan 051 Ola 4 · T4.5).
//
// Lo que aquí se protege no es «que el parte se escriba»: es que lo que se escriba sea VERDAD. El modo
// de fallo caro de este canal no es el silencio —el daemon sabe tratarlo, tira el parte rancio y publica
// `intent_circuit` vacío—, es una señal INVENTADA: un taskset con valor por defecto, o un parte que
// sigue saliendo cuando el cajero ya no puede escribirlo. Por eso los tests de abajo miran tanto lo que
// se publica como lo que NO se publica.
//
// ⚠️ EL PARTE IMPORTA MÁS DESDE T1.6-2 (ADR-0045), no menos: es el mismo proceso que decide si una
// `inference_request` del Cloud se sirve, así que su circuito es ahora la señal que dice si el Edge puede
// clasificar. Lo que cambió es QUIÉN alimenta el p50 —antes el bucle al clasificar, hoy el servidor de
// inferencia— y por eso los tests que necesitan una muestra medida piden una inferencia en vez de
// encolar un lote.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// escritorParteFake guarda todos los partes publicados y puede fallar a voluntad.
type escritorParteFake struct {
	mu     sync.Mutex
	partes []app.ParteWorker
	err    error
}

var _ app.ParteWorkerEscritor = (*escritorParteFake)(nil)

func (e *escritorParteFake) PublicarParte(_ context.Context, p app.ParteWorker) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.partes = append(e.partes, p)
	return e.err
}

func (e *escritorParteFake) todos() []app.ParteWorker {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]app.ParteWorker(nil), e.partes...)
}

// ultimo devuelve el parte más reciente. El segundo valor es «hubo alguno».
func (e *escritorParteFake) ultimo() (app.ParteWorker, bool) {
	ps := e.todos()
	if len(ps) == 0 {
		return app.ParteWorker{}, false
	}
	return ps[len(ps)-1], true
}

// nuevoCajeroDePrueba construye un Cajero SIN correrlo. Se usa cuando el test necesita gobernar el
// estado interno (el veredicto del taskset, el histograma) sin que el arranque de Run lo pise con lo
// que lea de la máquina real — que en Linux depende del `taskset` del entorno de CI y en macOS no se
// puede leer en absoluto.
func nuevoCajeroDePrueba(t *testing.T, deps Deps) *Cajero {
	t.Helper()
	if deps.Breaker == nil {
		deps.Breaker = nuevoBreakerFake()
	}
	if deps.Log == nil {
		deps.Log = &logCaptura{}
	}
	c, err := New(deps)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// TestParte_SePublicaAlArrancar: el primer parte sale ANTES del bucle, no en el primer tick. Sin esto
// habría un agujero de ParteCada (30 s) en cada arranque en el que el daemon leería «sin parte» con el
// cajero vivo — y en un portátil que se reinicia varias veces al día ese hueco no es despreciable.
func TestParte_SePublicaAlArrancar(t *testing.T) {
	esc := &escritorParteFake{}
	cola := &colaFake{}

	if _, err := correr(t, Deps{
		Colas:   []ColaNombrada{{Nombre: "inst-a", Cola: cola, Parte: esc}},
		Ollama:  &chateadorFake{},
		Breaker: nuevoBreakerFake(),
		Log:     &logCaptura{},
	}, 2); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(esc.todos()) == 0 {
		t.Fatal("el cajero debe publicar su parte AL ARRANCAR, sin esperar al primer tick de ParteCada")
	}
}

// TestParte_LlevaLasTresSeñales: circuito, taskset y p50 viajan en el mismo parte.
//
// El p50 se comprueba DESPUÉS de una inferencia real SERVIDA: es la única forma de ver que el histograma
// está CABLEADO (que el servidor observa la latencia) y no sólo escrito. Se afirma «> 0» y no un valor
// concreto porque lo que se publica es la COTA SUPERIOR del bucket donde cayó la muestra
// (bordesInferenciaMS), y con un proveedor falso que responde en microsegundos esa cota es el primer
// borde de la rejilla — un número que cambiaría si algún día se recalibra la rejilla, y este test no va
// de eso.
func TestParte_LlevaLasTresSeñales(t *testing.T) {
	esc := &escritorParteFake{}

	c, s := servidorDe(t, Deps{
		Colas:   []ColaNombrada{{Nombre: "inst-a", Cola: &colaFake{}, Parte: esc}},
		Ollama:  &chateadorFake{salida: `{"intent":"crear_pedido"}`},
		Breaker: nuevoBreakerFake(),
		Log:     &logCaptura{},
	})

	if _, err := s.Inferir(context.Background(), peticionDe("clasifica esto", 5*time.Second)); err != nil {
		t.Fatalf("el test necesita una inferencia MEDIDA para que el p50 exista: %v", err)
	}
	if n := c.inferencia.muestras(); n != 1 {
		t.Fatalf("una inferencia ⇒ una muestra en el histograma; hay %d (¿el servidor no observa la latencia?)", n)
	}

	c.publicarParte(context.Background())

	p, hay := esc.ultimo()
	if !hay {
		t.Fatal("no se publicó ningún parte")
	}
	if p.Circuito == "" {
		t.Error("el circuito del breaker SIEMPRE se conoce en este proceso: no puede viajar vacío")
	}
	if p.P50ms <= 0 {
		t.Errorf("tras una inferencia medida el p50 no puede ser 0 (0 significa «sin muestras»): got %d", p.P50ms)
	}
	if p.TS.IsZero() {
		t.Error("sin sello de tiempo el lector no puede juzgar la frescura: el parte nacería rancio")
	}
	if p.Taskset != c.Taskset() {
		t.Errorf("el parte debe llevar el veredicto retenido: parte=%q cajero=%q", p.Taskset, c.Taskset())
	}
}

// TestParte_TasksetDesconocido_ViajaVacío es la regla dura del canal, del lado del escritor: cuando el
// reparto de CPUs no se pudo leer —no-Linux (taskset_other.go devuelve error), Ollama corriendo con otro
// usuario, /proc ilegible— el veredicto es VACÍO y vacío se publica. Ni "disjunta" por defecto, ni el
// valor del arranque anterior. Un veredicto inventado mandaría a la nube la señal contraria a la que la
// medición avisa (con los conjuntos solapados, el 17,2 % de las clasificaciones pasó del timeout).
//
// El Cajero se construye SIN correrlo a propósito: Run llama a registrarAfinidad, que en un Linux de CI
// leería la afinidad REAL de la máquina y produciría un veredicto legítimo — con lo que el test dejaría
// de comprobar lo que dice comprobar y sería verde en macOS y rojo en Linux.
func TestParte_TasksetDesconocido_ViajaVacío(t *testing.T) {
	esc := &escritorParteFake{}
	c := nuevoCajeroDePrueba(t, Deps{
		Colas: []ColaNombrada{{Nombre: "inst-a", Cola: &colaFake{}, Parte: esc}},
	})

	if c.Taskset() != "" {
		t.Fatalf("sin comprobación de afinidad el veredicto es «no se sabe» (vacío), got %q", c.Taskset())
	}

	c.publicarParte(context.Background())

	p, hay := esc.ultimo()
	if !hay {
		t.Fatal("no se publicó ningún parte")
	}
	if p.Taskset != "" {
		t.Errorf("un taskset desconocido viaja VACÍO, jamás con un default: got %q", p.Taskset)
	}
	if p.P50ms != 0 {
		t.Errorf("sin inferencias, el p50 es 0 («sin muestras»), no un valor de relleno: got %d", p.P50ms)
	}
	// Y el circuito SÍ va: es lo único de las tres señales que este proceso conoce siempre.
	if p.Circuito == "" {
		t.Error("el circuito se conoce siempre; que el taskset falte no puede vaciar el resto del parte")
	}
}

// TestParte_VeredictoDeAfinidadSeRetiene: la mitad decidible de T2.8 ahora deja huella. Hasta la Ola 4
// el veredicto se calculaba, se logueaba y se tiraba; el parte lo necesita retenido. Se ejercita
// registrarReparto —la mitad que no toca /proc— para que el test valga en cualquier plataforma, que es
// el mismo motivo por el que esa función está separada de la lectura.
func TestParte_VeredictoDeAfinidadSeRetiene(t *testing.T) {
	casos := []struct {
		nombre string
		lec    lecturaAfinidad
		quiero string
	}{
		{"disjunta", lecturaAfinidad{Ollama: "0-1", Cajero: "2-3", Presentes: "0-3"}, string(afinidadDisjunta)},
		{"solapada", lecturaAfinidad{Ollama: "0-2", Cajero: "2-3", Presentes: "0-5"}, string(afinidadSolapada)},
		{"cajero sin confinar", lecturaAfinidad{Ollama: "0-1", Cajero: "0-3", Presentes: "0-3"}, string(afinidadCajeroSinConfinar)},
		// El caso que importa de verdad: si la lectura falló, NO se retiene nada.
		{"lectura fallida ⇒ vacío", lecturaAfinidad{ErrCajero: errors.New("no se pudo leer /proc")}, ""},
	}
	for _, caso := range casos {
		t.Run(caso.nombre, func(t *testing.T) {
			// Mismo molde que el resto de tests de afinidad (afinidad_test.go): un Cajero literal con lo
			// justo, sin pasar por New ni por Run — registrarReparto sólo necesita el log y numThread.
			cj := &Cajero{log: &logCaptura{}, numThread: 1}
			cj.registrarReparto(caso.lec)
			if cj.Taskset() != caso.quiero {
				t.Errorf("Taskset(): quiero %q, tengo %q", caso.quiero, cj.Taskset())
			}
		})
	}
}

// TestParte_ErrorDelEscritorNoRompeElBucle: el parte es TELEMETRÍA, y ninguna de las dos cosas que este
// proceso hace de verdad —barrer leases y servir inferencia— puede depender de él. Un buzón que falla en
// cada publicación no puede hacer que Run devuelva error (el supervisor lo trataría como una caída y
// reiniciaría el worker en bucle por un fallo de telemetría) ni contaminar los contadores del cajero.
// Lo que sí tiene que hacer es DECIRLO: en Warn, con la cola nombrada.
func TestParte_ErrorDelEscritorNoRompeElBucle(t *testing.T) {
	esc := &escritorParteFake{err: errors.New("BD bloqueada")}
	log := &logCaptura{}

	c, err := correr(t, Deps{
		Colas:   []ColaNombrada{{Nombre: "inst-a", Cola: &colaFake{}, Parte: esc}},
		Ollama:  &chateadorFake{},
		Breaker: nuevoBreakerFake(),
		Log:     log,
	}, 3)
	if err != nil {
		t.Fatalf("un fallo al publicar el parte NO puede hacer que Run devuelva error: %v", err)
	}
	if len(esc.todos()) == 0 {
		t.Fatal("el test necesita que se haya INTENTADO publicar; si no, el error del buzón no se ejercita")
	}
	if c.Fallos() != 0 {
		t.Errorf("un fallo de telemetría NO es un fallo del proveedor de LLM: fallos=%d", c.Fallos())
	}

	e, ok := log.buscar("no se pudo publicar el parte")
	if !ok {
		t.Fatal("un buzón roto tiene que dejar rastro en el log, o la pérdida de visibilidad es invisible")
	}
	if e.nivel != "warn" {
		t.Errorf("es una degradación de telemetría, no un error del worker: nivel %q", e.nivel)
	}
	if !strings.Contains(log.texto(), "inst-a") {
		t.Error("el aviso debe NOMBRAR la instalación: con cinco colas, «no se pudo publicar» no es un diagnóstico")
	}
}

// TestParte_SePublicaEnTodasLasColas: con N instalaciones hay N BDs y N daemons, y cada uno lee LA SUYA.
// Las tres señales son por PROCESO (el breaker, el taskset y Ollama son uno por máquina), así que el
// mismo parte se escribe en todas: publicar en una sola dejaría a las N-1 restantes con
// `intent_circuit` vacío para siempre y sin ningún síntoma.
func TestParte_SePublicaEnTodasLasColas(t *testing.T) {
	escA, escB := &escritorParteFake{}, &escritorParteFake{}

	c, err := correr(t, Deps{
		Colas: []ColaNombrada{
			{Nombre: "inst-a", Cola: &colaFake{}, Parte: escA},
			{Nombre: "inst-b", Cola: &colaFake{}, Parte: escB},
		},
		Ollama:  &chateadorFake{},
		Breaker: nuevoBreakerFake(),
		Log:     &logCaptura{},
	}, 2)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if c.Colas() != 2 {
		t.Fatalf("el test necesita dos colas montadas, hay %d", c.Colas())
	}

	pa, hayA := escA.ultimo()
	pb, hayB := escB.ultimo()
	if !hayA || !hayB {
		t.Fatalf("las DOS instalaciones deben recibir el parte: a=%v b=%v", hayA, hayB)
	}
	if pa.Circuito != pb.Circuito || pa.Taskset != pb.Taskset || pa.P50ms != pb.P50ms {
		t.Errorf("las señales son POR PROCESO: los dos partes deben ser el mismo (a=%+v b=%+v)", pa, pb)
	}
}

// TestParte_ColaSinBuzon_NoRompeNada: `Parte` es opcional (nil ⇒ esta instalación no recibe parte) y una
// cola sin buzón no puede impedir que las demás lo reciban ni provocar un pánico por puntero nil. Es el
// caso real del cableado cuando la aserción de interfaz de abrirCola no encaja.
func TestParte_ColaSinBuzon_NoRompeNada(t *testing.T) {
	esc := &escritorParteFake{}

	c, err := correr(t, Deps{
		Colas: []ColaNombrada{
			{Nombre: "sin-buzon", Cola: &colaFake{}}, // Parte nil a propósito
			{Nombre: "con-buzon", Cola: &colaFake{}, Parte: esc},
		},
		Ollama:  &chateadorFake{},
		Breaker: nuevoBreakerFake(),
		Log:     &logCaptura{},
	}, 2)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if c.Colas() != 2 {
		t.Fatalf("el test necesita dos colas montadas, hay %d", c.Colas())
	}
	if _, hay := esc.ultimo(); !hay {
		t.Error("la cola CON buzón debe recibir su parte aunque su vecina no tenga")
	}
}

// TestParteRancio_EsTresVecesLaCadencia es un candado sobre el contrato, no sobre el código: el lector
// tira el parte a los ParteRancio y el escritor lo reescribe cada ParteCada. Si alguien sube la cadencia
// sin subir el umbral, el daemon empezaría a declarar rancios los partes de un cajero perfectamente
// vivo, y el síntoma sería un `intent_circuit` que parpadea sin motivo.
func TestParteRancio_EsTresVecesLaCadencia(t *testing.T) {
	if app.ParteRancio < 3*app.ParteCada {
		t.Fatalf("ParteRancio (%s) debe ser >= 3 x ParteCada (%s): un solo retraso del bucle bastaría "+
			"para que un cajero vivo pareciera muerto", app.ParteRancio, app.ParteCada)
	}
}

// TestParteRancio_SigueCubriendoElPeorCasoDelBucle ata `app.ParteRancio` a las constantes de las que su
// cálculo DEPENDE, que viven en ficheros distintos y nadie relaciona.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 EL PEOR CASO ENCOGIÓ EN T1.6-2, Y ESA ES LA MITAD IMPORTANTE DE ESTA REESCRITURA
// ─────────────────────────────────────────────────────────────────────────────
// Hasta el ADR-0045 el término dominante de esta suma era UNA INFERENCIA ENTERA: el parte se publicaba
// desde el bucle, y la vuelta siguiente se quedaba esperando plaza del semáforo mientras una `procesar`
// clasificaba. El peor intervalo entre dos publicaciones era `Timeout + cierre + ParteCada`, y con eso
// el margen estaba tan justo que subir el plazo de inferencia a 60 s lo rompía.
//
// Hoy el bucle NO TOMA PLAZA: sólo atiende dos latidos en un select no bloqueante y espera al
// despertador (ver «EL BUCLE YA NO CLASIFICA» en cajero.go). Las inferencias las sirve OTRA goroutine, la
// del servidor sobre el socket, y por lentas que sean no pueden retrasar el parte ni un milisegundo.
// Consecuencia directa y verificable aquí: la subida de DefaultInferenceTimeoutMS de 15 s a 45 s —que con
// la aritmética vieja habría dejado el margen en negativo— no toca este cálculo.
//
// LO QUE QUEDA EN EL PEOR CASO: el tick de ParteCada, más lo que tarde el bucle en volver a pasar por el
// select (un poll), más lo que cueste la publicación anterior (parteTimeout por cola).
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO: app.ParteCada 30 s → 60 s; app.ParteRancio 3× → 2×; parteTimeout
// 2 s → 40 s; DefaultPollMS 500 → 30.000.
func TestParteRancio_SigueCubriendoElPeorCasoDelBucle(t *testing.T) {
	// UNA instalación, que es el 99 % de los despliegues. Con N el término del escritor es N·parteTimeout,
	// y el margen que se comprueba abajo dice cuántas caben.
	poll := DefaultPollMS * time.Millisecond
	peorCaso := app.ParteCada + poll + parteTimeout

	if peorCaso >= app.ParteRancio {
		t.Fatalf("el peor retraso del escritor (%v = tick %v + poll %v + publicación %v) alcanza o supera "+
			"ParteRancio (%v): un cajero SANO publicaría partes que el lector tira por rancios, y la nube "+
			"vería intent_circuit parpadeando",
			peorCaso, app.ParteCada, poll, parteTimeout, app.ParteRancio)
	}

	// Y que quede margen de verdad, no un empate técnico: si el margen baja de un ParteCada, la ventana ya
	// no absorbe una tardanza extra (una publicación que reintenta contra la BD con busy_timeout, p. ej.)
	// ni deja sitio para las colas de una máquina multi-instalación.
	if margen := app.ParteRancio - peorCaso; margen < app.ParteCada {
		t.Errorf("el margen sobre el peor caso es %v, menos de un tick (%v): la ventana ya no absorbe una "+
			"tardanza extra del escritor y el parpadeo pasa a depender de la suerte", margen, app.ParteCada)
	}
}
