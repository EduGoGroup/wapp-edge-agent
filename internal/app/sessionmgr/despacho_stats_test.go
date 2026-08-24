package sessionmgr

// despacho_stats_test.go — LA REFERENCIA QUE FALTABA (Plan 051 Ola 4 · T4.0).
//
// El diagnóstico de esta tarea fue que no faltaba un tubo, faltaba una REFERENCIA: `despachador.New`
// devolvía una `d` que era variable local de `startDespachador` y nadie la guardaba, así que los ocho
// motivos de omisión y los cuatro contadores de atasco/sello existían y eran ilegibles desde fuera.
//
// Estos tests fijan las tres propiedades de la lectura, y ninguna es cosmética:
//   - la sesión RETIENE su despachador y el Manager lo alcanza;
//   - «no sé» (sin despachador / sesión no viva) NO se confunde con «todo a cero»;
//   - el agregado es de sesiones VIVAS y recorre la lista canónica, nunca la transcribe.

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/despachador"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// colaMuda satisface app.ColaDespachador sin hacer nada: los tests de abajo NUNCA arrancan el bucle
// (`Run`), sólo construyen el despachador para leer sus contadores. Una cola de verdad exigiría SQLite y no
// mediría nada más.
type colaMuda struct{}

func (colaMuda) CabezaDeSesion(context.Context, string) (*app.ColaCabeza, error) { return nil, nil }
func (colaMuda) MarcarDespachada(context.Context, int64) error                   { return nil }

// sinkMudo satisface app.InboundSink; tampoco se ejercita.
type sinkMudo struct{}

func (sinkMudo) Deliver(context.Context, domain.InboundEvent) error { return nil }

var (
	_ app.ColaDespachador = colaMuda{}
	_ app.InboundSink     = sinkMudo{}
)

// managerConDespachador arma un Manager con UNA sesión viva que ya retiene su despachador, saltándose el
// cableado real (cola + mux + sink). Reproduce el estado que `startDespachador` deja tras `setDespachador`.
func managerConDespachador(t *testing.T, sessionID string) (*Manager, *despachador.Despachador) {
	t.Helper()
	m := NewManager(NewLayout(filepath.Join(t.TempDir(), "edge-data")), nil, 5, testLogger())

	d, err := despachador.New(despachador.Deps{
		Cola:      colaMuda{},
		Sink:      sinkMudo{},
		SessionID: sessionID,
		Log:       testLogger(),
	})
	if err != nil {
		t.Fatalf("despachador.New: %v", err)
	}

	s := &liveSession{meta: domain.Session{SessionID: sessionID}, log: testLogger()}
	s.setDespachador(d)
	m.mu.Lock()
	m.live[sessionID] = s
	m.mu.Unlock()
	return m, d
}

// TestManager_DespachoStats_LeeElDespachadorDeLaSesion: con la referencia retenida, el Manager entrega el
// desglose completo. Es la prueba directa del arreglo.
func TestManager_DespachoStats_LeeElDespachadorDeLaSesion(t *testing.T) {
	m, _ := managerConDespachador(t, uuidA)

	st, ok := m.DespachoStats(uuidA)
	if !ok {
		t.Fatal("DespachoStats devolvió ok=false para una sesión viva CON despachador retenido: la " +
			"referencia se volvió a perder (era el bug entero de T4.0)")
	}
	// SE RECORRE la lista canónica; jamás se transcribe (INV-051.3).
	for _, motivo := range app.MotivosOmitido() {
		if _, presente := st.OmitidosPorMotivo[string(motivo)]; !presente {
			t.Errorf("falta el motivo %q en el desglose de la sesión; las OCHO claves van siempre", motivo)
		}
	}
	if got, want := len(st.OmitidosPorMotivo), len(app.MotivosOmitido()); got != want {
		t.Errorf("el desglose tiene %d claves, want %d", got, want)
	}
}

// TestManager_DespachoStats_AusenciaNoEsCero: una sesión que no existe, y una viva SIN despachador
// arrancado, devuelven ok=false — no un struct a cero.
//
// La diferencia importa en campo: «esta sesión no ha omitido nada» y «esta sesión no está drenando» se
// parecen mucho en un dashboard y significan lo contrario. Por eso el puerto devuelve dos valores.
func TestManager_DespachoStats_AusenciaNoEsCero(t *testing.T) {
	m, _ := managerConDespachador(t, uuidA)

	if _, ok := m.DespachoStats(uuidB); ok {
		t.Error("DespachoStats devolvió ok=true para una sesión que no está viva")
	}

	// Sesión viva pero sin despachador (sin cola cableada, o `New` falló): también ok=false.
	m.mu.Lock()
	m.live[uuidB] = &liveSession{meta: domain.Session{SessionID: uuidB}, log: testLogger()}
	m.mu.Unlock()
	if _, ok := m.DespachoStats(uuidB); ok {
		t.Error("DespachoStats devolvió ok=true para una sesión viva SIN despachador: esa sesión escucha " +
			"y no drena, que es justo lo que hay que poder distinguir")
	}
}

// TestManager_DespachoStatsVivas_SoloVivasYNuncaNil: el agregado suma las sesiones vivas, trae las ocho
// claves y DEJA DE CONTAR a la sesión que se retira del registro.
//
// El agregado NO es acumulativo, y está decidido así: los contadores del despachador son por proceso (la
// durabilidad la da la cola en disco), y un acumulador que sobreviva a la sesión mezclaría la de un cliente
// con la del siguiente y sólo crecería en memoria. La consecuencia —que este número pueda BAJAR— es el
// significado de «vivas», no un bug del contador.
func TestManager_DespachoStatsVivas_SoloVivasYNuncaNil(t *testing.T) {
	m, _ := managerConDespachador(t, uuidA)

	st := m.DespachoStatsVivas()
	if got, want := len(st.OmitidosPorMotivo), len(app.MotivosOmitido()); got != want {
		t.Errorf("el agregado tiene %d claves, want %d (las de app.MotivosOmitido())", got, want)
	}
	// Sin tráfico todo está a cero, pero el mapa NO es nil: el bloque local nunca sale con huecos.
	for motivo, n := range st.OmitidosPorMotivo {
		if n != 0 {
			t.Errorf("agregado[%q] = %d en un despachador recién construido; want 0", motivo, n)
		}
	}

	// Se retira la sesión del registro (lo que hace Unlink): el agregado deja de verla, sin panicar.
	m.mu.Lock()
	delete(m.live, uuidA)
	m.mu.Unlock()
	if st := m.DespachoStatsVivas(); len(st.OmitidosPorMotivo) != len(app.MotivosOmitido()) {
		t.Error("sin sesiones vivas el agregado debe seguir trayendo las ocho claves a 0, no un mapa nil")
	}
}

// TestManager_DespachoStatsVivas_ToleraSesionSinDespachador: una sesión viva sin despachador no aporta ni
// rompe el agregado (nil-safe), que es el estado de cualquier Manager de test y de una sesión cuyo
// despachador no arrancó.
func TestManager_DespachoStatsVivas_ToleraSesionSinDespachador(t *testing.T) {
	m, _ := managerConDespachador(t, uuidA)
	m.mu.Lock()
	m.live[uuidB] = &liveSession{meta: domain.Session{SessionID: uuidB}, log: testLogger()}
	m.mu.Unlock()

	st := m.DespachoStatsVivas()
	if st.CabezasAtascadas != 0 || st.FallosSelloDespacho != 0 || st.FallosSelloPresupuesto != 0 {
		t.Errorf("el agregado se ensució con una sesión sin despachador: %+v", st)
	}
	// ⚠️ `CabezasAtascadas` y `FallosSelloPresupuesto` se comprueban aquí a 0 porque el agregado los SIGUE
	// sumando (son campos de proto), aunque desde T1.6-5 ningún despachador pueda moverlos. Ver `statsDe`.
}

// ─────────────────────────────────────────────────────────────────────────────────────────────────────
// LA SUMA DE VERDAD — dos sesiones con contadores distintos y NO NULOS
// ─────────────────────────────────────────────────────────────────────────────────────────────────────
//
// 🔴 EL AGUJERO QUE ESTO CIERRA (revisión adversarial, 2026-08-17). Los tres tests de arriba miran
// despachadores RECIÉN CONSTRUIDOS, con todo a 0. Sobre esa foto, `DespachoStatsVivas` puede llevar un `=`
// donde debe haber un `+=`, o puede olvidarse un contador entero en el agregado, y los tres siguen en
// verde: 0 vale 0 + 0. Una suma sólo se prueba sumando números DISTINTOS y NO NULOS.
//
// CÓMO SE MUEVEN LOS CONTADORES SIN EXPORTAR NADA NUEVO. Son atómicos privados del despachador, y el único
// camino legítimo para tocarlos desde otro paquete es el que usa producción: su propio bucle. Así que a la
// cola se le da un GUION —una lista de cabezas, una por vuelta, escrita para que cada contador acabe en un
// número EXACTO— y, agotado el guion, la cola CANCELA el contexto del bucle. El número de vueltas lo fija
// el guion, no un reloj: no hay `time.Sleep` haciendo de sincronía en ninguna parte.
//
// (Y por eso el guion se verifica antes que la suma: un guion que no moviera nada dejaría al test sumando
// ceros otra vez, que es justo el fallo que viene a cerrar.)

// cuentasGuion describe un guion EN EL IDIOMA DE LOS CONTADORES, que es como se lee este test. Los valores
// son primos pequeños y distintos entre sí a propósito: si el agregado cruzara dos campos, con 1 y 1
// pasaría desapercibido y con 3 y 5 no.
//
// 🔴 ERAN CUATRO CAMPOS HASTA EL 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-5 · ADR-0045). Los dos que se
// fueron —`fallosSelloPresupuesto` y `pollsCabezaAtascada`— no se podían seguir guionando porque sus
// PRODUCTORES desaparecieron con el push: el primero contaba fallos de `DespacharSinIntent` (método
// retirado) y el segundo, polls con la cabeza atascada (situación disuelta: ver
// `TestCabezaEnEstadoImprevistoSaleTambien`). Sus campos siguen en el DTO y en el proto, clavados a 0.
//
// ⚠️ EL TEST NO SE DEBILITÓ POR PERDERLOS: la propiedad que prueba —`+=` y no `=`, y ningún cruce de
// campos— se sostiene con dos series distintas. Lo que sí hubo que reponer es la segunda dimensión que
// aportaba el par de sellos: por eso el desglose de omisiones se guiona ahora con DOS motivos distintos,
// que además ejercita el «nunca agregado entre motivos» de INV-051.3.
type cuentasGuion struct {
	// omitidosFastlane y omitidosBreaker mueven DOS entradas distintas del desglose. Se eligen esos dos
	// porque son el par que el ADR contrapone: `fastlane` era el camino SANO y `breaker` un FALLO que manda
	// a mirar Ollama. Si alguien los agregara, este test lo dice.
	omitidosFastlane    int
	omitidosBreaker     int
	fallosSelloDespacho int
}

// colaGuionada sirve el guion: una cabeza por cada `CabezaDeSesion`, y sella (o falla al sellar) según el
// id. Agotado el guion cancela el contexto, que es lo que para el bucle de forma determinista.
type colaGuionada struct {
	mu         sync.Mutex
	pasos      []*app.ColaCabeza
	i          int
	selloFalla map[int64]bool
	cancel     context.CancelFunc
}

var _ app.ColaDespachador = (*colaGuionada)(nil)

func (c *colaGuionada) CabezaDeSesion(context.Context, string) (*app.ColaCabeza, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.i >= len(c.pasos) {
		// Guion agotado. Se cancela ANTES de devolver «no hay nada»: Run comprueba el ctx justo después de
		// la vuelta, así que el bucle sale sin ejecutar ninguna más y los contadores quedan en su valor
		// exacto. Cancelar dos veces es inofensivo.
		c.cancel()
		return nil, nil
	}
	// COPIA, como el doble del paquete despachador: el bucle no debe poder mutar la fila «en disco».
	paso := *c.pasos[c.i]
	c.i++
	return &paso, nil
}

func (c *colaGuionada) MarcarDespachada(_ context.Context, id int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.selloFalla[id] {
		return errors.New("cola guionada: MarcarDespachada falla para esta fila")
	}
	return nil
}

// cabezaGuion arma una fila mínima: el despachador sólo mira id/seq/estado/sobre. `Meta` nil es legal
// (DecodeColaMeta devuelve el cero sin error), así que el evento sale sin remitente y nada se ensucia.
func cabezaGuion(id int64, estado, intentJSON string) *app.ColaCabeza {
	return &app.ColaCabeza{
		ID:          id,
		Seq:         id,
		Estado:      estado,
		WAMessageID: "wamid-guion",
		IntentJSON:  intentJSON,
		TieneIntent: intentJSON != "",
	}
}

// nuevaColaGuionada traduce las cuentas pedidas a la lista de cabezas que las produce. Las fases van en
// este orden y cada una explica QUÉ camino del bucle recorre.
//
// 🔴 TODAS LAS CABEZAS DEL GUION LLEVAN UN SOBRE ESCRITO, y eso las hace filas ANTIGUAS por definición
// (bajo pull nadie escribe `intent_json`). Es deliberado: es la única forma de mover el desglose por
// motivo, y de paso el guion documenta que ese desglose sólo puede moverse drenando colas viejas.
func nuevaColaGuionada(q cuentasGuion, cancel context.CancelFunc) *colaGuionada {
	c := &colaGuionada{selloFalla: map[int64]bool{}, cancel: cancel}
	var id int64

	// (1) OMISIONES POR MOTIVO — una vuelta por fila, dos motivos distintos. El bucle entrega la fila,
	// cuenta SU motivo y la sella bien.
	for _, f := range []struct {
		n      int
		motivo app.MotivoOmitido
	}{
		{q.omitidosFastlane, app.MotivoFastlane},
		{q.omitidosBreaker, app.MotivoBreaker},
	} {
		for i := 0; i < f.n; i++ {
			id++
			c.pasos = append(c.pasos, cabezaGuion(id, app.EstadoNuevo, app.SobreOmitido(f.motivo)))
		}
	}

	// (2) FALLOS DE SELLO DE DESPACHO — una vuelta por fila. Fila SIN sobre cuyo `MarcarDespachada` falla:
	// se entregó y no se pudo sellar ⇒ el contador de los DUPLICADOS. Se usa la fila sin sobre y no una con
	// motivo para no ensuciar el desglose de la fase (1).
	for i := 0; i < q.fallosSelloDespacho; i++ {
		id++
		c.pasos = append(c.pasos, cabezaGuion(id, app.EstadoNuevo, ""))
		c.selloFalla[id] = true
	}

	// 🔴 HABÍA DOS FASES MÁS y se fueron con el push (T1.6-5): (3) fallos de sello por PRESUPUESTO —una
	// fila `nuevo` servida 1+N veces con `DespacharSinIntent` fallando— y (4) la CABEZA ATASCADA —una fila
	// en un estado desconocido servida 2+N veces—. Las dos guionaban mecanismos que ya no existen; ver el
	// doc de `cuentasGuion`.
	return c
}

// arrancaDespachadorGuionado corre un despachador contra su guion HASTA AGOTARLO y lo devuelve con los
// contadores ya movidos.
func arrancaDespachadorGuionado(t *testing.T, sessionID string, q cuentasGuion) *despachador.Despachador {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := despachador.New(despachador.Deps{
		Cola:      nuevaColaGuionada(q, cancel),
		Sink:      sinkMudo{},
		SessionID: sessionID,
		Log:       testLogger(),
		// Poll MÍNIMO: aquí no se mide latencia, se cuentan vueltas. (Aquí iba también un `Presupuesto` de
		// 1 ns, que era lo que hacía vencer el reloj entre dos vueltas sobre la misma cabeza; el presupuesto
		// se retiró en T1.6-5 y con él las dos fases del guion que dependían de él.)
		Despertador: despachador.NewPollFijo(time.Millisecond),
	})
	if err != nil {
		t.Fatalf("despachador.New: %v", err)
	}

	hecho := make(chan error, 1)
	go func() { hecho <- d.Run(ctx) }()
	select {
	case err := <-hecho:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		// Guardia anti-cuelgue, no sincronía: si el guion no para el bucle, esto lo dice en 10 s en vez de
		// dejar el CI colgado.
		cancel()
		t.Fatal("el bucle no terminó al agotarse el guion")
	}
	return d
}

// comprobarGuion verifica que el guion movió lo que prometía. Va ANTES de la suma porque es lo que impide
// que este test degenere en el que vino a sustituir (uno que suma ceros).
func comprobarGuion(t *testing.T, etiqueta string, d *despachador.Despachador, q cuentasGuion) {
	t.Helper()
	if got, want := d.OmitidosPorMotivo()[app.MotivoFastlane], int64(q.omitidosFastlane); got != want {
		t.Errorf("sesión %s: omitidos[fastlane] = %d, el guion pedía %d", etiqueta, got, want)
	}
	if got, want := d.OmitidosPorMotivo()[app.MotivoBreaker], int64(q.omitidosBreaker); got != want {
		t.Errorf("sesión %s: omitidos[breaker] = %d, el guion pedía %d", etiqueta, got, want)
	}
	if got, want := d.FallosSelloDespacho(), int64(q.fallosSelloDespacho); got != want {
		t.Errorf("sesión %s: fallos de sello de DESPACHO = %d, el guion pedía %d", etiqueta, got, want)
	}
}

// TestManager_DespachoStatsVivas_SumaDeVerdad es la prueba de la SUMA: dos sesiones vivas con contadores
// distintos y no nulos, y el agregado que tiene que valer la suma de los dos, campo a campo.
//
// LAS MUTACIONES QUE CAZA, y ninguna la cazaban los tres tests de arriba:
//   - `total.X = st.X` en vez de `+=` ⇒ el agregado valdría el de UNA sesión (y, con el recorrido de un
//     mapa, cuál de las dos sería una lotería distinta en cada ejecución).
//
// ⚠️ SUS DOS SERIES SE REDUJERON A DOS MOTIVOS + UN SELLO en T1.6-5 (ver `cuentasGuion`): las otras dos
// perdieron su productor con el push. La propiedad probada es la misma.
//   - un contador que se olvide en el agregado ⇒ se queda en 0 mientras las dos sesiones traen valores.
//   - un cruce de campos (sumar un motivo en otro, o un sello donde no toca) ⇒ los primos no cuadran.
func TestManager_DespachoStatsVivas_SumaDeVerdad(t *testing.T) {
	qA := cuentasGuion{omitidosFastlane: 3, omitidosBreaker: 5, fallosSelloDespacho: 7}
	qB := cuentasGuion{omitidosFastlane: 11, omitidosBreaker: 2, fallosSelloDespacho: 13}

	dA := arrancaDespachadorGuionado(t, uuidA, qA)
	dB := arrancaDespachadorGuionado(t, uuidB, qB)
	comprobarGuion(t, "A", dA, qA)
	comprobarGuion(t, "B", dB, qB)

	m := NewManager(NewLayout(filepath.Join(t.TempDir(), "edge-data")), nil, 5, testLogger())
	for sid, d := range map[string]*despachador.Despachador{uuidA: dA, uuidB: dB} {
		s := &liveSession{meta: domain.Session{SessionID: sid}, log: testLogger()}
		s.setDespachador(d)
		m.mu.Lock()
		m.live[sid] = s
		m.mu.Unlock()
	}

	st := m.DespachoStatsVivas()

	if got, want := st.OmitidosPorMotivo[string(app.MotivoFastlane)], int64(qA.omitidosFastlane+qB.omitidosFastlane); got != want {
		t.Errorf("agregado[fastlane] = %d, want %d (%d de A + %d de B): el MISMO motivo sí se suma entre "+
			"sesiones, y esto es lo que un `=` en vez de un `+=` rompería en silencio",
			got, want, qA.omitidosFastlane, qB.omitidosFastlane)
	}
	if got, want := st.OmitidosPorMotivo[string(app.MotivoBreaker)], int64(qA.omitidosBreaker+qB.omitidosBreaker); got != want {
		t.Errorf("agregado[breaker] = %d, want %d", got, want)
	}
	// 🔴 Y NUNCA ENTRE MOTIVOS DISTINTOS (INV-051.3): `fastlane` era el camino SANO y `breaker` un FALLO que
	// manda a mirar Ollama. El guion los puso en números distintos para que agregarlos se note.
	if st.OmitidosPorMotivo[string(app.MotivoFastlane)] == st.OmitidosPorMotivo[string(app.MotivoBreaker)] {
		t.Errorf("fastlane y breaker salieron con el mismo valor (%d): el guion los puso distintos a "+
			"propósito, así que o se han cruzado o se han agregado",
			st.OmitidosPorMotivo[string(app.MotivoFastlane)])
	}
	if got, want := st.FallosSelloDespacho, int64(qA.fallosSelloDespacho+qB.fallosSelloDespacho); got != want {
		t.Errorf("agregado de fallos de sello de DESPACHO = %d, want %d", got, want)
	}
	// Los campos SIN productor vivo siguen sumándose (son de proto) y por tanto tienen que salir en 0: si
	// alguno trajera un número, es que un contador se está cruzando con otro.
	if st.FallosSelloPresupuesto != 0 || st.CabezasAtascadas != 0 || st.PollsCabezaAtascada != 0 {
		t.Errorf("un campo sin productor vivo trajo valor (sello_presupuesto=%d cabezas=%d polls=%d): "+
			"desde T1.6-5 nada puede moverlos, así que sólo pueden venir de un cruce",
			st.FallosSelloPresupuesto, st.CabezasAtascadas, st.PollsCabezaAtascada)
	}

	// Los otros SEIS motivos siguen a 0 y presentes: el guion sólo tocó dos, y ningún motivo se agrega con
	// otro. La lista se RECORRE, jamás se transcribe.
	for _, motivo := range app.MotivosOmitido() {
		if motivo == app.MotivoFastlane || motivo == app.MotivoBreaker {
			continue
		}
		n, presente := st.OmitidosPorMotivo[string(motivo)]
		if !presente {
			t.Errorf("falta el motivo %q en el agregado; las ocho claves van siempre", motivo)
			continue
		}
		if n != 0 {
			t.Errorf("agregado[%q] = %d y el guion no lo tocó: los motivos se están mezclando entre sí", motivo, n)
		}
	}
}
