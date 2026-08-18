package cajero

// Tests del ROUND-ROBIN del cajero entre N colas (Plan 051 Ola 4 · T4.1).
//
// Van en su propio fichero y no en cajero_test.go porque miden una propiedad distinta de todas las de
// allí: aquélla es «¿este bucle clasifica bien UN lote?», ésta es «¿el bucle reparte su atención de forma
// JUSTA entre N instalaciones?». Los dobles (colaFake, clasificadorFake, clasificadorVigilante,
// despertadorCuenta, logCaptura) y las ayudas (loteDe, correr) se REUTILIZAN de cajero_test.go: son del
// mismo paquete y duplicarlos sería tener dos colas falsas que pueden divergir.
//
// Lo único que se añade aquí es el REGISTRO DE TURNOS, porque la equidad no se puede medir con los
// contadores de colaFake: dos colas con el mismo número de claims pueden haber sido atendidas de forma
// perfectamente injusta (30 seguidos a una y luego 30 a la otra). Lo que hay que mirar es el ORDEN.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-intent/classifier"
)

// ─────────────────────────────────────────────────────────────────────────────
// El registro de turnos — el doble que mide la EQUIDAD, no el volumen
// ─────────────────────────────────────────────────────────────────────────────

// registroTurnos apunta, en orden, el índice de la cola de cada claim que el bucle intenta.
//
// ES LA ÚNICA FORMA DE MEDIR LA INANICIÓN. Un contador de claims por cola no vale: «30 y 30» es el mismo
// número tanto si el bucle alternó como si vació una cola entera antes de mirar la otra, y la segunda es
// exactamente el fallo que T4.1 viene a cerrar. La secuencia sí distingue las dos.
type registroTurnos struct {
	mu    sync.Mutex
	orden []int
}

func (r *registroTurnos) anotar(idx int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.orden = append(r.orden, idx)
}

func (r *registroTurnos) instantanea() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.orden...)
}

// colaTurno es una cola falsa que APUNTA SU TURNO en un registro compartido antes de servir (o no) un
// lote. Sirve los lotes preparados en orden y, agotados, devuelve (nil, nil) como la cola real vacía.
type colaTurno struct {
	idx      int
	registro *registroTurnos

	mu         sync.Mutex
	pendientes []*app.ColaLote
	reclamos   int
	cierres    int
}

var _ app.ColaCajero = (*colaTurno)(nil)

func (c *colaTurno) Reclamar(_ context.Context, _ int) (*app.ColaLote, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reclamos++
	// El registro tiene su PROPIO candado y nadie toma c.mu teniéndolo: no hay orden de bloqueo que
	// invertir, así que anotar desde dentro de c.mu es seguro.
	c.registro.anotar(c.idx)
	if len(c.pendientes) == 0 {
		return nil, nil
	}
	lote := c.pendientes[0]
	c.pendientes = c.pendientes[1:]
	return lote, nil
}

func (c *colaTurno) MarcarClasificado(_ context.Context, _ *app.ColaLote, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cierres++
	return nil
}

func (c *colaTurno) BarrerLeasesVencidos(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

func (c *colaTurno) cuentas() (reclamos, cierres int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reclamos, c.cierres
}

// nuevaColaTurno arma una cola con `lotes` lotes de un mensaje cada uno.
func nuevaColaTurno(reg *registroTurnos, idx, lotes int) *colaTurno {
	c := &colaTurno{idx: idx, registro: reg}
	for i := 0; i < lotes; i++ {
		c.pendientes = append(c.pendientes, loteDe("s", "quiero una pizza"))
	}
	return c
}

// ─────────────────────────────────────────────────────────────────────────────
// (a) EQUIDAD — el criterio de aceptación de T4.1
// ─────────────────────────────────────────────────────────────────────────────

// TestRoundRobin_LaParlanchinaNoMataDeHambreALaCallada es EL test de T4.1: con N colas donde una está
// siempre llena y las otras casi vacías, tras M rondas ninguna cola espera más de N claims.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: mover el `c.cursor = (c.cursor + 1) % len(c.colas)` de bucle() para
// que sólo avance cuando el claim TRAJO lote. Con esa versión el bucle se queda pegado a la cola 0
// mientras le quede una sola fila —30 vueltas seguidas— y las colas 1 y 2 no reciben NI UN claim hasta
// que aquélla se seca. La secuencia registrada deja de cumplir `orden[i] == i % N` en el segundo
// elemento, y la espera máxima de la cola callada pasa de N a 30·N.
func TestRoundRobin_LaParlanchinaNoMataDeHambreALaCallada(t *testing.T) {
	const (
		nColas      = 3
		lotesRicos  = 30 // la parlanchina: sigue teniendo trabajo mucho después de que las otras se sequen
		lotesPobres = 2
	)

	reg := &registroTurnos{}
	parlanchina := nuevaColaTurno(reg, 0, lotesRicos)
	callada := nuevaColaTurno(reg, 1, lotesPobres)
	muda := nuevaColaTurno(reg, 2, 0)

	c, err := correr(t, Deps{
		Colas: []ColaNombrada{
			{Nombre: "parlanchina", Cola: parlanchina},
			{Nombre: "callada", Cola: callada},
			{Nombre: "muda", Cola: muda},
		},
		Clasificador:  &clasificadorFake{res: classifier.Classification{Intent: "crear_pedido", Confidence: 0.9}},
		Breaker:       nuevoBreakerFake(),
		Log:           &logCaptura{},
		MaxConcurrent: 1,
	}, 1)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if c.Colas() != nColas {
		t.Fatalf("el cajero debe atender las %d colas de la lista, atiende %d", nColas, c.Colas())
	}

	orden := reg.instantanea()
	if len(orden) < lotesRicos {
		t.Fatalf("con %d lotes en la cola llena tenía que haber al menos %d claims, hubo %d",
			lotesRicos, lotesRicos, len(orden))
	}

	// (1) ROUND-ROBIN ESTRICTO: el turno k-ésimo es SIEMPRE de la cola k mod N. Es la forma más fuerte de
	// decir «el cursor avanza una posición por claim intentado».
	for i, idx := range orden {
		if idx != i%nColas {
			t.Fatalf("el claim %d fue de la cola %d y el round-robin estricto exige la %d (secuencia: %v)",
				i, idx, i%nColas, orden)
		}
	}

	// (2) EL CRITERIO, DICHO TAL CUAL: ninguna cola espera más de N claims entre dos turnos suyos. Se mide
	// sobre la CALLADA, que es la que se muere de hambre si el reparto es injusto.
	maxEspera, ultima := 0, -1
	for i, idx := range orden {
		if idx != callada.idx {
			continue
		}
		if ultima >= 0 && i-ultima > maxEspera {
			maxEspera = i - ultima
		}
		ultima = i
	}
	if maxEspera > nColas {
		t.Errorf("la cola callada esperó %d claims entre turnos; con %d colas el techo es %d", maxEspera, nColas, nColas)
	}

	// (3) Y LA CONSECUENCIA QUE LE IMPORTA AL NEGOCIO: la callada llegó a clasificar sus lotes, no se
	// quedó esperando a que la vecina se vaciara.
	if _, cierres := callada.cuentas(); cierres != lotesPobres {
		t.Errorf("la cola callada debía cerrar sus %d lotes, cerró %d", lotesPobres, cierres)
	}

	// (4) El reparto de claims es parejo HASTA ±1 — y ese «±1» es la propiedad que el round-robin estricto
	// garantiza de verdad, no la igualdad exacta. Todas las colas se sondean, incluida la que nunca tuvo
	// nada (la muda sigue recibiendo su claim, que es lo que la mantiene viva el día que le llegue trabajo).
	//
	// 🔴 POR QUÉ ±1 Y NO «=». La última vuelta se corta A MITAD, por construcción del bucle. La traza, con
	// los números de este test (30/2/0 lotes y UNA sola espera permitida):
	//
	//   - claim #1 parlanchina (lote) · #2 callada (lote) · #3 muda (vacío, vaciasSeguidas=1)
	//   - #4 parlanchina (lote) ⇒ vaciasSeguidas=0 … y así: mientras la parlanchina tenga lotes, el
	//     contador se reinicia una vez por vuelta y NUNCA llega a 3, así que no se duerme.
	//   - la parlanchina sirve su lote nº 30 en el claim #88 (los suyos son los 3k+1).
	//   - #89 callada (vacío, 1) · #90 muda (vacío, 2) · #91 parlanchina, YA SECA (vacío, 3 ≥ 3 colas)
	//     ⇒ se duerme ⇒ el despertador de `correr(..., 1)` cancela en la primera espera y el bucle sale.
	//
	//   Total: 91 claims ⇒ parlanchina 31, callada 30, muda 30. Exigir `rp == rc == rm` era exigir que la
	//   última vuelta TERMINARA, cosa que el bucle no promete ni debe: la vuelta 31 murió en su primer
	//   claim porque ese claim fue el que completó la vuelta en blanco.
	//
	// 🔴 Y SIGUE CAZANDO UN ROUND-ROBIN ROTO, que es lo que esta aserción existe para hacer. Con la mutación
	// del comentario de arriba (cursor que sólo avanza si el claim trajo lote), la parlanchina se lleva sus
	// 30 lotes seguidos MÁS los 3 claims vacíos que la duermen, y las otras dos no reciben ninguno: 33/0/0
	// rompe el ±1 por 33. La forma laxa que sí habría que evitar —`rp >= rc >= rm` a secas— no lo cazaría;
	// el techo de UNO es lo que la hace una aserción y no un adorno.
	rp, _ := parlanchina.cuentas()
	rc, _ := callada.cuentas()
	rm, _ := muda.cuentas()
	// El orden (rp >= rc >= rm) no es cosmético: el cursor arranca en 0 y recorre 0→1→2, así que una vuelta
	// a medias sólo puede dejar de MÁS a las colas de delante, jamás a las de detrás.
	if rp < rc || rc < rm {
		t.Errorf("los claims deben decrecer con la posición del cursor (la vuelta se corta por el final): "+
			"parlanchina=%d callada=%d muda=%d", rp, rc, rm)
	}
	if rp-rm > 1 {
		t.Errorf("ninguna cola puede llevarse más de UN claim de ventaja sobre otra (round-robin estricto, "+
			"última vuelta a medias): parlanchina=%d callada=%d muda=%d", rp, rc, rm)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (b) NO-REGRESIÓN — con UNA sola cola, el bucle de siempre
// ─────────────────────────────────────────────────────────────────────────────

// TestRoundRobin_UnaSolaCola_ComportamientoIdenticoAlDeHoy es la otra mitad del criterio de T4.1, y la
// que protege al 99 % de las instalaciones: una máquina con un solo data_dir no debe notar NADA.
//
// La propiedad concreta que se fija es la que el round-robin podía romper sin que nada fallara: con una
// cola, la vuelta completa es de UN claim, así que CADA claim vacío duerme en el despertador — ni se
// duerme de más (latencia) ni se deja de dormir (espera activa quemando un core con la cola vacía, que es
// justo lo contrario de lo que la O0 pide de esta máquina).
func TestRoundRobin_UnaSolaCola_ComportamientoIdenticoAlDeHoy(t *testing.T) {
	t.Run("cada claim vacío duerme: un claim por espera, ni uno más", func(t *testing.T) {
		cola := &colaFake{}

		c, err := correr(t, Deps{
			Cola:         cola,
			Clasificador: &clasificadorFake{},
			Breaker:      nuevoBreakerFake(),
			Log:          &logCaptura{},
		}, 3)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if c.Colas() != 1 {
			t.Fatalf("el atajo Deps.Cola monta EXACTAMENTE una cola, montó %d", c.Colas())
		}
		reclamos, _ := cola.snapshot()
		if reclamos != 3 {
			t.Errorf("con una cola vacía y 3 esperas hay 3 claims (uno por espera), hubo %d", reclamos)
		}
	})

	t.Run("el atajo Deps.Cola drena la cola entera", func(t *testing.T) {
		cola := &colaFake{pendientes: []*app.ColaLote{
			loteDe("s1", "uno"), loteDe("s2", "dos"), loteDe("s3", "tres"),
		}}

		if _, err := correr(t, Deps{
			Cola:         cola,
			Clasificador: &clasificadorFake{res: classifier.Classification{Intent: "crear_pedido", Confidence: 0.9}},
			Breaker:      nuevoBreakerFake(),
			Log:          &logCaptura{},
		}, 1); err != nil {
			t.Fatalf("Run: %v", err)
		}

		reclamos, cierres := cola.snapshot()
		if len(cierres) != 3 {
			t.Errorf("los tres lotes deben cerrarse, se cerraron %d", len(cierres))
		}
		// 3 claims con lote + 1 vacío (el que provoca la única espera y con ella la cancelación).
		if reclamos != 4 {
			t.Errorf("claims esperados 4 (3 con lote + 1 vacío), hubo %d", reclamos)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// (c) ROUND-ROBIN ESTRICTO — el cursor avanza aunque la cola devuelva vacío
// ─────────────────────────────────────────────────────────────────────────────

// TestRoundRobin_ElCursorAvanzaAunqueLaColaDevuelvaVacio aísla la regla del cursor del test de equidad:
// aquél la mide sobre una secuencia larga, éste la enseña en su forma mínima y sin ambigüedad.
func TestRoundRobin_ElCursorAvanzaAunqueLaColaDevuelvaVacio(t *testing.T) {
	t.Run("una cola vacía por delante no tapa a la que sí tiene trabajo", func(t *testing.T) {
		// Si el cursor sólo avanzara con lote en la mano, el bucle se quedaría clavado en la cola 0 —que
		// nunca tiene nada— y el lote de la cola 1 no se clasificaría JAMÁS.
		vacia := &colaFake{}
		conTrabajo := &colaFake{pendientes: []*app.ColaLote{loteDe("s1", "quiero una pizza")}}

		if _, err := correr(t, Deps{
			Colas: []ColaNombrada{
				{Nombre: "vacia", Cola: vacia},
				{Nombre: "con-trabajo", Cola: conTrabajo},
			},
			Clasificador:  &clasificadorFake{res: classifier.Classification{Intent: "crear_pedido", Confidence: 0.9}},
			Breaker:       nuevoBreakerFake(),
			Log:           &logCaptura{},
			MaxConcurrent: 1,
		}, 1); err != nil {
			t.Fatalf("Run: %v", err)
		}

		if _, cierres := conTrabajo.snapshot(); len(cierres) != 1 {
			t.Fatalf("la segunda cola tenía un lote y debía clasificarlo; cierres=%d", len(cierres))
		}
		if reclamos, _ := vacia.snapshot(); reclamos == 0 {
			t.Error("la cola vacía también se sondea: sin sus claims el cursor no estaría rotando")
		}
	})

	t.Run("sólo se duerme tras la VUELTA COMPLETA en blanco", func(t *testing.T) {
		// Tres colas vacías y UNA sola espera permitida: si el bucle durmiera al primer vacío, la segunda y
		// la tercera no llegarían a recibir su claim antes de la cancelación.
		colas := []*colaFake{{}, {}, {}}
		if _, err := correr(t, Deps{
			Colas: []ColaNombrada{
				{Nombre: "a", Cola: colas[0]},
				{Nombre: "b", Cola: colas[1]},
				{Nombre: "c", Cola: colas[2]},
			},
			Clasificador: &clasificadorFake{},
			Breaker:      nuevoBreakerFake(),
			Log:          &logCaptura{},
		}, 1); err != nil {
			t.Fatalf("Run: %v", err)
		}

		for i, cola := range colas {
			reclamos, _ := cola.snapshot()
			if reclamos != 1 {
				t.Errorf("la cola %d debía recibir EXACTAMENTE un claim antes del primer sueño, recibió %d", i, reclamos)
			}
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// (d) EL SEMÁFORO SIGUE SIENDO UNO POR MÁQUINA
// ─────────────────────────────────────────────────────────────────────────────

// TestRoundRobin_ElSemaforoNoSeMultiplicaPorCola es el guardarraíl de la decisión de diseño de T4.1: el
// semáforo (y el breaker) son UNO POR PROCESO porque protegen a Ollama, que es uno por máquina.
//
// El fallo que cierra sería fácil de introducir y silencioso en los tests de escritorio: un semáforo por
// cola con N=1 y tres instalaciones daría TRES inferencias simultáneas contra la misma instancia de
// Ollama —el solapamiento que la O0 midió como la causa de que la p50 se dispare—, y en un test sin
// Ollama real todo seguiría pasando en verde. Por eso se mide la SIMULTANEIDAD, no el resultado.
func TestRoundRobin_ElSemaforoNoSeMultiplicaPorCola(t *testing.T) {
	const nColas = 3

	vigilante := &clasificadorVigilante{res: classifier.Classification{Intent: "crear_pedido", Confidence: 0.9}}
	colas := []*colaFake{
		{pendientes: []*app.ColaLote{loteDe("a1", "uno"), loteDe("a2", "dos")}},
		{pendientes: []*app.ColaLote{loteDe("b1", "tres"), loteDe("b2", "cuatro")}},
		{pendientes: []*app.ColaLote{loteDe("c1", "cinco"), loteDe("c2", "seis")}},
	}

	if _, err := correr(t, Deps{
		Colas: []ColaNombrada{
			{Nombre: "a", Cola: colas[0]},
			{Nombre: "b", Cola: colas[1]},
			{Nombre: "c", Cola: colas[2]},
		},
		Clasificador:  vigilante,
		Breaker:       nuevoBreakerFake(),
		Log:           &logCaptura{},
		MaxConcurrent: 1,
	}, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if n := vigilante.maxSimultaneas(); n != 1 {
		t.Errorf("con MaxConcurrent=1 y %d colas NUNCA puede haber dos inferencias solapadas, hubo %d", nColas, n)
	}

	total := 0
	for _, cola := range colas {
		_, cierres := cola.snapshot()
		total += len(cierres)
	}
	if total != 2*nColas {
		t.Errorf("los %d lotes de las %d colas deben cerrarse todos, se cerraron %d", 2*nColas, nColas, total)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// El BARRIDO de leases con N colas
// ─────────────────────────────────────────────────────────────────────────────

// TestBarridoDeLeases_RecorreTodasLasColasEnElMismoTick fija la otra decisión de T4.1: el barrido itera
// las N colas DENTRO del mismo tick, con un solo ticker, y suma en un agregado que sigue existiendo más
// un desglose por cola.
//
// La mutación que caza: dejar el barrido apuntando a una sola cola (por ejemplo, la primera). Las filas
// que un cajero muerto dejó en `tomado` en las OTRAS instalaciones no volverían nunca a `nuevo`, y el
// síntoma en campo sería una cola que deja de avanzar sin un solo error.
func TestBarridoDeLeases_RecorreTodasLasColasEnElMismoTick(t *testing.T) {
	primera := &colaFake{rescatables: 3}
	segunda := &colaFake{rescatables: 2}
	log := &logCaptura{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := New(Deps{
		Colas: []ColaNombrada{
			{Nombre: "instalacion-a", Cola: primera},
			{Nombre: "instalacion-b", Cola: segunda},
		},
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
	for c.Rescatados() < 5 {
		select {
		case <-plazo:
			cancel()
			t.Fatalf("el barrido no rescató las 5 filas de las DOS colas en 3 s (barridos: a=%d b=%d)",
				primera.barridosN(), segunda.barridosN())
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

	// El DESGLOSE: el agregado dice «alguien murió a mitad», esto dice CUÁL instalación.
	desglose := c.RescatadosPorCola()
	if desglose["instalacion-a"] != 3 {
		t.Errorf("la instalación a rescató 3 filas, el desglose dice %d", desglose["instalacion-a"])
	}
	if desglose["instalacion-b"] != 2 {
		t.Errorf("la instalación b rescató 2 filas, el desglose dice %d", desglose["instalacion-b"])
	}

	// Y el log nombra la cola: con cinco instalaciones, un Warn sin `cola` no es un diagnóstico.
	e, ok := log.buscar("leases vencidos rescatados")
	if !ok {
		t.Fatal("el barrido con n>0 debe dejar una línea de log")
	}
	if e.nivel != "warn" {
		t.Errorf("rescatar filas se avisa en Warn (alguien murió a mitad), got %q", e.nivel)
	}
	if !strings.Contains(log.texto(), "instalacion-a") {
		t.Error("el aviso del barrido debe nombrar la instalación afectada")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Construcción: las dos formas de pasar colas, y el nil que no puede pasar
// ─────────────────────────────────────────────────────────────────────────────

func TestNew_ListaDeColas(t *testing.T) {
	t.Run("sin Cola ni Colas no se construye", func(t *testing.T) {
		if _, err := New(Deps{Clasificador: &clasificadorFake{}}); err == nil {
			t.Fatal("un cajero sin ninguna cola no puede existir")
		}
	})

	t.Run("una cola nil DENTRO de la lista falla en el arranque, no en el primer claim", func(t *testing.T) {
		// El caso real: el cableado abre N data_dir's y uno no pudo construir su Store. Sin esta guarda el
		// pánico llegaría minutos después, la primera vez que el cursor cayera en esa posición.
		_, err := New(Deps{
			Colas: []ColaNombrada{
				{Nombre: "buena", Cola: &colaFake{}},
				{Nombre: "/srv/wapp/rota", Cola: nil},
			},
			Clasificador: &clasificadorFake{},
		})
		if err == nil {
			t.Fatal("una cola nil en la lista debe impedir el arranque")
		}
		if !strings.Contains(err.Error(), "/srv/wapp/rota") {
			t.Errorf("el error debe NOMBRAR la cola rota (un operador con 5 instalaciones necesita saber cuál): %v", err)
		}
	})

	t.Run("Colas manda sobre el atajo Cola", func(t *testing.T) {
		atajo := &colaFake{}
		lista := &colaFake{}
		c, err := New(Deps{
			Cola:         atajo,
			Colas:        []ColaNombrada{{Nombre: "la-de-la-lista", Cola: lista}},
			Clasificador: &clasificadorFake{},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if c.Colas() != 1 {
			t.Fatalf("con las dos vías puestas manda la lista: se esperaba 1 cola, hay %d", c.Colas())
		}
		if _, ok := c.RescatadosPorCola()["la-de-la-lista"]; !ok {
			t.Errorf("la cola montada debe ser la de la LISTA, no la del atajo: %v", c.RescatadosPorCola())
		}
	})

	t.Run("una cola sin nombre recibe uno que la distinga", func(t *testing.T) {
		c, err := New(Deps{Cola: &colaFake{}, Clasificador: &clasificadorFake{}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if _, ok := c.RescatadosPorCola()["cola-0"]; !ok {
			t.Errorf("sin etiqueta el nombre cae al índice, para que dos líneas de log no salgan idénticas: %v",
				c.RescatadosPorCola())
		}
	})
}
