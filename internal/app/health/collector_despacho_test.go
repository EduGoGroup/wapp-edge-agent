package health

// collector_despacho_test.go — LOS OCHO MOTIVOS, DISTINGUIBLES (Plan 051 Ola 4 · T4.0, INV-051.3).
//
// DOS INVARIANTES, y las dos han fallado antes en este repo:
//
//  1. LAS OCHO CLAVES SIEMPRE, Y RECORRIENDO `app.MotivosOmitido()`. La lista se ha quedado corta dos veces
//     por transcribirla a mano; por eso esa función devuelve una COPIA y por eso hay un guardarraíl AST en
//     `internal/app`. Estos tests NO enumeran motivos: los recorren. Si mañana hay un noveno, pasan solos.
//     (La única excepción son las tres constantes `app.MotivoFastlane/Presupuesto/Breaker` que la prueba de
//     no-agregación NOMBRA a propósito: ahí el punto es precisamente que esos tres NO se mezclan, y para
//     decirlo hay que señalarlos. No es una lista: es una aserción sobre tres miembros concretos.)
//
//  2. NADA SE AGREGA. `fastlane` es el camino SANO —el regex resolvió el intent en microsegundos— y
//     `presupuesto`/`breaker` son FALLOS que mandan a mirar Ollama. Un solo número «omitidos» no responde
//     ninguna pregunta. Igual con los dos sellos: sólo `failed_seal_dispatch` implica duplicados ya
//     publicados en la nube (T3.12).

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// despachoFijo satisface DespachoReader con un juego de contadores fijo por sesión.
type despachoFijo struct {
	porSesion map[string]DespachoStats
	vivas     DespachoStats
}

func (f despachoFijo) DespachoStats(sessionID string) (DespachoStats, bool) {
	st, ok := f.porSesion[sessionID]
	return st, ok
}

func (f despachoFijo) DespachoStatsVivas() DespachoStats { return f.vivas }

var _ DespachoReader = despachoFijo{}

// TestReport_OchoClavesAunqueTodasACero: sin lector cableado (el caso de todos los tests y wirings que no
// lo pasan) el desglose NO viene vacío ni nil: viene con las ocho claves canónicas a 0.
//
// «Un motivo a 0 es un dato, no un hueco»: dice que por ese camino no se está yendo nadie. Un mapa vacío
// obligaría a quien lo lee a distinguir «no se midió» de «no pasó», y esa duda es la que hace que un Edge
// que dejó de clasificar pase por un Edge tranquilo.
func TestReport_OchoClavesAunqueTodasACero(t *testing.T) {
	c := NewCollector(regConSesion("S"), nil, nil, "v", time.Now())

	r, ok := c.Collect(context.Background(), "S")
	if !ok {
		t.Fatal("Collect devolvió ok=false para una sesión con prueba de vida")
	}
	// SE RECORRE LA LISTA CANÓNICA. Jamás se transcribe: ese es el fallo que este test existe para cazar.
	for _, m := range app.MotivosOmitido() {
		n, presente := r.IntentOmittedByReason[string(m)]
		if !presente {
			t.Errorf("falta la clave %q en intent_omitted_by_reason; las OCHO van siempre, incluso a 0", m)
			continue
		}
		if n != 0 {
			t.Errorf("intent_omitted_by_reason[%q] = %d sin lector cableado; want 0", m, n)
		}
	}
	if got, want := len(r.IntentOmittedByReason), len(app.MotivosOmitido()); got != want {
		t.Errorf("intent_omitted_by_reason tiene %d claves, want %d (las de app.MotivosOmitido())", got, want)
	}
}

// TestReport_DesgloseLlegaDelDespachadorDeSuSesion: con lector cableado, cada sesión recibe SUS contadores
// (no los de la vecina) y el resto de claves sigue presente a 0.
func TestReport_DesgloseLlegaDelDespachadorDeSuSesion(t *testing.T) {
	reg := NewRegistry()
	reg.SetSocketState("A", SocketConnected, "")
	reg.SetSocketState("B", SocketConnected, "")

	lector := despachoFijo{porSesion: map[string]DespachoStats{
		"A": statsCon(map[app.MotivoOmitido]int64{app.MotivoFastlane: 11}),
		"B": statsCon(map[app.MotivoOmitido]int64{app.MotivoBreaker: 7}),
	}}

	c := NewCollector(reg, nil, nil, "v", time.Now(), WithDespachoReader(lector))
	reports := c.Reports(context.Background())

	if got := reports["A"].IntentOmittedByReason[string(app.MotivoFastlane)]; got != 11 {
		t.Errorf("A.fastlane = %d, want 11", got)
	}
	if got := reports["A"].IntentOmittedByReason[string(app.MotivoBreaker)]; got != 0 {
		t.Errorf("A.breaker = %d, want 0 (los contadores de B no se cuelan en A)", got)
	}
	if got := reports["B"].IntentOmittedByReason[string(app.MotivoBreaker)]; got != 7 {
		t.Errorf("B.breaker = %d, want 7", got)
	}
	// Y las ocho claves siguen ahí en las dos, con o sin tráfico.
	for _, id := range []string{"A", "B"} {
		if got, want := len(reports[id].IntentOmittedByReason), len(app.MotivosOmitido()); got != want {
			t.Errorf("%s: el desglose tiene %d claves, want %d", id, got, want)
		}
	}
}

// TestReport_FastlaneNoSeSumaConLosFallos es la prueba de NO-AGREGACIÓN (criterio literal de INV-051.3).
//
// Los tres valores son PRIMOS GRANDES Y DISTINTOS a propósito: así ninguna suma parcial puede coincidir por
// casualidad con otro contador del Report, y el barrido de abajo detecta cualquier «total» que alguien
// añada — un campo nuevo, un helper que agregue, un consumidor que sume «para simplificar el dashboard».
func TestReport_FastlaneNoSeSumaConLosFallos(t *testing.T) {
	const (
		nFastlane    int64 = 1000003
		nPresupuesto int64 = 2000029
		nBreaker     int64 = 3000017
	)
	lector := despachoFijo{porSesion: map[string]DespachoStats{"S": statsCon(map[app.MotivoOmitido]int64{
		app.MotivoFastlane:    nFastlane,
		app.MotivoPresupuesto: nPresupuesto,
		app.MotivoBreaker:     nBreaker,
	})}}

	c := NewCollector(regConSesion("S"), nil, nil, "v", time.Now(), WithDespachoReader(lector))
	r, _ := c.Collect(context.Background(), "S")

	// (1) Cada uno conserva su valor: nadie los fundió.
	casos := map[app.MotivoOmitido]int64{
		app.MotivoFastlane:    nFastlane,
		app.MotivoPresupuesto: nPresupuesto,
		app.MotivoBreaker:     nBreaker,
	}
	for motivo, want := range casos {
		if got := r.IntentOmittedByReason[string(motivo)]; got != want {
			t.Errorf("intent_omitted_by_reason[%q] = %d, want %d", motivo, got, want)
		}
	}

	// (2) NINGUNA entrada del desglose contiene una suma de dos o más de ellos. `fastlane` es el camino
	// sano y los otros dos son fallos: mezclarlos borra la única pregunta que este desglose responde.
	prohibidas := map[int64]string{
		nFastlane + nPresupuesto:            "fastlane+presupuesto",
		nFastlane + nBreaker:                "fastlane+breaker",
		nPresupuesto + nBreaker:             "presupuesto+breaker",
		nFastlane + nPresupuesto + nBreaker: "fastlane+presupuesto+breaker",
	}
	for clave, valor := range r.IntentOmittedByReason {
		if quienes, mal := prohibidas[valor]; mal {
			t.Errorf("intent_omitted_by_reason[%q] = %d, que es exactamente %s: alguien AGREGÓ motivos "+
				"(INV-051.3). `fastlane` es el camino SANO; `presupuesto`/`breaker` son fallos y uno de "+
				"ellos manda a mirar Ollama. No se suman en ningún punto de la cadena.", clave, valor, quienes)
		}
	}

	// (3) Y tampoco aparece la suma en ningún campo int64 del Report (un «total_omitidos» que alguien
	// añadiera «para el dashboard» caería aquí sin que haya que tocar este test).
	for campo, valor := range camposInt64(r) {
		if quienes, mal := prohibidas[valor]; mal {
			t.Errorf("Report.%s = %d, que es exactamente %s: no debe existir ningún campo que agregue "+
				"motivos de omisión (INV-051.3)", campo, valor, quienes)
		}
	}
}

// TestReport_LosDosSellosViajanSeparados: `failed_seal_dispatch` y `failed_seal_budget` llegan cada uno con
// su valor y NINGÚN campo del Report vale su suma.
//
// 🔴 POR QUÉ IMPORTA (T3.12): un fallo de `MarcarDespachada` es un mensaje YA PUBLICADO que se volverá a
// publicar —un duplicado en la nube—; un fallo de `DespacharSinIntent` es una fila que se reintenta sola en
// el poll siguiente y no tiene consecuencia aguas arriba. Sumarlos convierte ruido operativo en un
// incidente (o, peor, esconde el incidente dentro del ruido). `FallosSello()` del despachador ya existe y
// se queda DENTRO del despachador, a propósito: al Report no sube.
func TestReport_LosDosSellosViajanSeparados(t *testing.T) {
	const (
		nDespacho    int64 = 4000037
		nPresupuesto int64 = 5000011
	)
	st := statsCon(nil)
	st.FallosSelloDespacho = nDespacho
	st.FallosSelloPresupuesto = nPresupuesto

	lector := despachoFijo{porSesion: map[string]DespachoStats{"S": st}}
	c := NewCollector(regConSesion("S"), nil, nil, "v", time.Now(), WithDespachoReader(lector))
	r, _ := c.Collect(context.Background(), "S")

	if r.FailedSealDispatch != nDespacho {
		t.Errorf("failed_seal_dispatch = %d, want %d", r.FailedSealDispatch, nDespacho)
	}
	if r.FailedSealBudget != nPresupuesto {
		t.Errorf("failed_seal_budget = %d, want %d", r.FailedSealBudget, nPresupuesto)
	}
	for campo, valor := range camposInt64(r) {
		if valor == nDespacho+nPresupuesto {
			t.Errorf("Report.%s = %d, que es la SUMA de los dos sellos: sólo failed_seal_dispatch implica "+
				"duplicados en la nube, así que agregarlos deshace T3.12", campo, valor)
		}
	}
}

// TestReport_ContadoresDeAtascoLleganEnteros: cabezas atascadas y sus polls, cada uno por su lado (uno dice
// SI hubo atasco, el otro si es AHORA).
func TestReport_ContadoresDeAtascoLleganEnteros(t *testing.T) {
	st := statsCon(nil)
	st.CabezasAtascadas = 2
	st.PollsCabezaAtascada = 913

	lector := despachoFijo{porSesion: map[string]DespachoStats{"S": st}}
	c := NewCollector(regConSesion("S"), nil, nil, "v", time.Now(), WithDespachoReader(lector))
	r, _ := c.Collect(context.Background(), "S")

	if r.StuckHeads != 2 || r.StuckHeadPolls != 913 {
		t.Errorf("stuck_heads/stuck_head_polls = %d/%d, want 2/913", r.StuckHeads, r.StuckHeadPolls)
	}
}

// TestCollector_DespachoVivasSinLectorEsElCeroCanonico: el bloque de daemon del plano de control nunca sale
// con un mapa nil, ni siquiera antes de que el Manager se cablee (SetDespachoReader llega después).
func TestCollector_DespachoVivasSinLectorEsElCeroCanonico(t *testing.T) {
	c := NewCollector(NewRegistry(), nil, nil, "v", time.Now())
	st := c.DespachoVivas()
	if got, want := len(st.OmitidosPorMotivo), len(app.MotivosOmitido()); got != want {
		t.Errorf("DespachoVivas() sin lector dio %d claves, want %d", got, want)
	}
}

// TestCollector_SetDespachoReaderCablaTarde reproduce el orden REAL del wiring del daemon: el colector nace
// sin lector (el Manager todavía no existe) y lo recibe después. Si esa costura se rompiera, el desglose
// saldría a cero para siempre con todo lo demás en verde — el modo de fallo caro de T3.13.
func TestCollector_SetDespachoReaderCablaTarde(t *testing.T) {
	c := NewCollector(regConSesion("S"), nil, nil, "v", time.Now())
	if r, _ := c.Collect(context.Background(), "S"); r.IntentOmittedByReason[string(app.MotivoApagado)] != 0 {
		t.Fatal("sin lector el desglose debería estar a cero")
	}

	c.SetDespachoReader(despachoFijo{porSesion: map[string]DespachoStats{
		"S": statsCon(map[app.MotivoOmitido]int64{app.MotivoApagado: 42}),
	}})

	if r, _ := c.Collect(context.Background(), "S"); r.IntentOmittedByReason[string(app.MotivoApagado)] != 42 {
		t.Error("SetDespachoReader no surtió efecto: el desglose seguiría a cero en producción para siempre")
	}
}

// statsCon arma un DespachoStats con las ocho claves a 0 y los motivos indicados poblados. Parte de
// DespachoStatsCero() —que recorre app.MotivosOmitido()— para que tampoco los tests transcriban la lista.
func statsCon(motivos map[app.MotivoOmitido]int64) DespachoStats {
	st := DespachoStatsCero()
	for m, n := range motivos {
		st.OmitidosPorMotivo[string(m)] = n
	}
	return st
}

// camposInt64 devuelve todos los campos int64 del Report por nombre. Se usa por REFLEXIÓN, y no con una
// lista de campos, para que un campo agregador AÑADIDO EN EL FUTURO caiga en la red sin que nadie tenga que
// acordarse de actualizar estos tests — el mismo criterio que el guardarraíl AST del enum.
func camposInt64(r Report) map[string]int64 {
	v := reflect.ValueOf(r)
	tipo := v.Type()
	out := make(map[string]int64, tipo.NumField())
	for i := 0; i < tipo.NumField(); i++ {
		if v.Field(i).Kind() == reflect.Int64 {
			out[tipo.Field(i).Name] = v.Field(i).Int()
		}
	}
	return out
}
