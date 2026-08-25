package sessionmgr

// aviso_cableado_test.go — LOS DOS EXTREMOS DEL TIMBRE (Plan 044 · Ola 1.8 · T1.8-7).
//
// 🔴 EL AGUJERO QUE ESTE FICHERO CIERRA, que es el mismo que el del cronómetro y el del consultor de
// perfiles con otro nombre. El canal con el que el listener despierta al despachador de su sesión nace en
// `liveSession.arm` y tiene que llegar a DOS SITIOS DISTINTOS, por caminos que no se tocan:
//
//	arm → s.aviso ─┬─→ gateway.SetAviso  → whatsmeow.WithAviso  → Listener.aviso     (quien TOCA)
//	               └─→ despachador.Deps.Despertador (AvisoConRespaldo)               (quien ESCUCHA)
//
// Y AQUÍ EL MODO DE FALLO ES PEOR QUE EN SUS PRECEDENTES, aunque sus consecuencias sean más benignas:
// borrar CUALQUIERA de las dos líneas —o, peor, hacer que cada lado se fabrique su propio canal— no rompe
// absolutamente nada visible. El entrante se anota, se entrega y se acusa igual; sólo vuelve a esperar al
// poll. Es decir: los cuatro gates en verde, ningún log, ningún contador, y la tarea entera deshecha. Un
// cable que sólo se manifiesta como MEDIO SEGUNDO DE MÁS no lo sostiene nada que no sea un test.
//
// Los dos extremos se prueban de forma distinta a propósito:
//   - el del LISTENER, por IDENTIDAD DE CANAL (que el gateway lleve EXACTAMENTE el de la sesión), porque
//     ahí el fallo plausible no es el olvido sino el sustituto: un canal recién hecho deja el campo no-nil
//     y todo verde;
//   - el del DESPACHADOR, por CONDUCTA (que el bucle despierte al tocar el canal, y que NO despierte solo
//     mientras nadie lo toque), porque ahí no hay nada que inspeccionar: el despertador vive dentro del
//     `Despachador` y su tipo es un detalle privado.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo, y todas se ejecutaron.

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// ─────────────────────────────────────────────────────────────────────────────
// Extremo 1: el canal de la sesión llega al gateway (y de ahí al Listener)
// ─────────────────────────────────────────────────────────────────────────────

// TestWithWhatsmeowListen_ElTimbre_LLEGA_AlGatewayDeCadaSesion custodia
// `gateway.SetAviso(s.avisoCanal())` del factory de escucha (listen.go).
//
// Se invoca el factory REAL en vez de arrancar una sesión entera porque lo que se mide es el CABLE, no la
// escucha: `NewListenGatewayForDevice` sólo compone un cargador de device diferido, así que el gateway se
// construye sin tocar WhatsApp, sin red y sin device pareado. (Mismo molde que
// sesion_pasiva_cableado_test.go y latencia_cableado_test.go.)
//
// 🔴 SE AFIRMA POR IDENTIDAD DE CANAL Y NO POR «NO ES NIL», y la diferencia es todo el test: el «arreglo»
// plausible de quien no entienda para qué está la línea —`SetAviso(make(chan struct{}, 1))`— dejaría el
// campo perfectamente poblado, el listener tocando un timbre y el despachador escuchando otro. Sin nil que
// delate nada.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - borrar `gateway.SetAviso(s.avisoCanal())` del factory ⇒ el campo queda a nil y el listener no avisa;
//   - pasarle un canal recién construido ⇒ cae la comparación de identidad;
//   - quitar `s.aviso = make(chan struct{}, 1)` de `arm` ⇒ la sesión no tiene timbre que repartir y el
//     campo vuelve a quedar nil (que es, además, el fallo del ORDEN: si el canal naciera con el
//     despachador, el listener ya habría arrancado sin él).
func TestWithWhatsmeowListen_ElTimbre_LLEGA_AlGatewayDeCadaSesion(t *testing.T) {
	// El fixture de perfiles vale tal cual: lo único que hace falta es un Manager con la escucha REAL
	// cableada (WithWhatsmeowListen), que es el factory que este test interroga. El predicado que le pide
	// es irrelevante aquí.
	m := managerConPerfiles(t, func(string) bool { return false })

	s := &liveSession{
		meta: domain.Session{SessionID: "sess-timbre", JID: "56911112222:1@s.whatsapp.net"},
		log:  testLogger(),
	}
	// `arm` es quien crea el canal en producción, y va ANTES del factory igual que en startListener.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.arm(cancel)

	deLaSesion := s.avisoCanal()
	if deLaSesion == nil {
		t.Fatal("`arm` no dejó timbre en la sesión: sin canal no hay nada que cablear y el resto del test " +
			"no probaría nada")
	}

	runner, _, err := m.newListener(ctx, s)
	if err != nil {
		t.Fatalf("el factory de escucha falló al construir la sesión: %v", err)
	}

	enGateway := timbreDelGateway(t, runner)
	if enGateway == 0 {
		t.Fatal("el factory de WithWhatsmeowListen NO cableó el timbre en el ListenGateway.\n" +
			"    CONSECUENCIA: el Listener nace sin canal, así que tras cada `Enqueue` no avisa a nadie y el\n" +
			"    despachador de la sesión vuelve a enterarse por su poll — hasta medio segundo de espera por\n" +
			"    mensaje. Nada falla, nada se pierde y ningún gate lo ve.")
	}
	if quiero := reflect.ValueOf(deLaSesion).Pointer(); enGateway != quiero {
		t.Errorf("el gateway lleva un canal DISTINTO del de su sesión (%#x != %#x).\n"+
			"    CONSECUENCIA: el listener toca un timbre y el despachador escucha otro. Los dos campos están\n"+
			"    poblados, no hay nil, no hay error y el efecto es exactamente el de no haber hecho la tarea.",
			enGateway, quiero)
	}
}

// timbreDelGateway saca el canal `aviso` del ListenGateway que el factory dejó dentro del runner. Mismo
// molde —y mismas comprobaciones ruidosas— que `consultorDelGateway` en sesion_pasiva_cableado_test.go:
// cuando el parseo deja de encontrar lo que busca hay que fallar, no dejar de mirar en silencio.
func timbreDelGateway(t *testing.T, runner listenRunner) uintptr {
	t.Helper()

	v := reflect.ValueOf(runner)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		t.Fatalf("el factory devolvió un runner inesperado (%T): se esperaba un *app.Listen", runner)
	}
	campoGW := v.Elem().FieldByName("gateway")
	if !campoGW.IsValid() {
		t.Fatal("app.Listen ya no tiene el campo `gateway`: ¿se renombró? Este test mira ahí a propósito, " +
			"porque es donde el factory deja el ListenGateway que lleva el timbre")
	}
	gw := campoGW.Elem() // valor dinámico de la interfaz: el *whatsmeow.ListenGateway real
	if !gw.IsValid() || gw.Kind() != reflect.Pointer || gw.IsNil() {
		t.Fatalf("el runner no lleva un gateway utilizable (%v): el factory de producción tiene que haber "+
			"construido uno con NewListenGatewayForDevice", campoGW.Kind())
	}
	campo := gw.Elem().FieldByName("aviso")
	if !campo.IsValid() {
		t.Fatal("whatsmeow.ListenGateway ya no tiene el campo `aviso`: ¿se renombró? Es el campo que " +
			"SetAviso rellena y que listenerOpts() pasa al Listener con WithAviso")
	}
	return campo.Pointer()
}

// ─────────────────────────────────────────────────────────────────────────────
// Extremo 2: el despachador de la sesión ESCUCHA ese mismo canal
// ─────────────────────────────────────────────────────────────────────────────

// colaConGrifo es una `app.ColaDespachador` con una fila que sólo aparece cuando el test abre el grifo, y
// que además AVISA de cada mirada. Las dos cosas son necesarias:
//
//   - el grifo permite sembrar trabajo DESPUÉS de que el bucle se haya dormido, que es el único escenario
//     en el que se puede distinguir «despertó por el timbre» de «pasaba por aquí»;
//   - `miradas` cierra la carrera de arranque del propio test: abrir el grifo antes de que el bucle haya
//     hecho su primera vuelta haría que la fila saliera sin que nadie tocara nada, y el test daría un rojo
//     falso que costaría media tarde.
type colaConGrifo struct {
	mu         sync.Mutex
	abierto    bool
	despachada bool
	miradas    chan struct{}
}

func nuevaColaConGrifo() *colaConGrifo {
	return &colaConGrifo{miradas: make(chan struct{}, 256)}
}

func (c *colaConGrifo) CabezaDeSesion(_ context.Context, sessionID string) (*app.ColaCabeza, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case c.miradas <- struct{}{}:
	default:
	}
	if !c.abierto || c.despachada {
		return nil, nil
	}
	return &app.ColaCabeza{
		ID: 1, Seq: 1, SessionID: sessionID,
		ChatJID: "56911112222@s.whatsapp.net", WAMessageID: "WA-TIMBRE-1",
		TSWhatsApp: time.Now().Unix(), Estado: app.EstadoNuevo, Texto: "quiero dos empanadas",
	}, nil
}

func (c *colaConGrifo) MarcarDespachada(_ context.Context, _ int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.despachada = true
	return nil
}

func (c *colaConGrifo) abrir() {
	c.mu.Lock()
	c.abierto = true
	c.mu.Unlock()
}

var _ app.ColaDespachador = (*colaConGrifo)(nil)

// sinkConCanal publica el identificador de lo entregado (INV-051.1: el identificador, no el evento).
type sinkConCanal struct{ entregas chan string }

func (s sinkConCanal) Deliver(_ context.Context, e domain.InboundEvent) error {
	select {
	case s.entregas <- e.MessageID:
	default:
	}
	return nil
}

// muxDeEsteSink es el CloudLinkMux mínimo que startDespachador necesita, devolviendo UN sink concreto.
type muxDeEsteSink struct{ sink app.InboundSink }

func (m muxDeEsteSink) Register(string, string,
	func(ctx context.Context, commandID, to, text string) error,
	func(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) error,
	func() bool) {
}
func (m muxDeEsteSink) Unregister(string)                       {}
func (m muxDeEsteSink) SinkFor(string) app.InboundSink          { return m.sink }
func (m muxDeEsteSink) SendReceipt(string, domain.ReceiptEvent) {}
func (m muxDeEsteSink) SendLoggedOut(string)                    {}

var _ CloudLinkMux = muxDeEsteSink{}

// silencioSinTimbre es cuánto se observa al bucle SIN tocarle el timbre en la fase 1 del test de abajo.
//
// 🔴 EL NÚMERO NO ES ARBITRARIO Y NO ES LA CONSTANTE DE PRODUCCIÓN. Tiene que ser CÓMODAMENTE MAYOR que el
// poll de 500 ms —que es lo que gobierna a un despachador SIN despertador cableado, o sea, el estado al
// que devuelve la mutación que este test caza— y cómodamente MENOR que el respaldo del despertador por
// aviso, que es lo que gobierna al bucle cuando el cable está bien. Con 1,2 s hay más del doble de margen
// por abajo y cuatro veces por arriba.
//
// Se escribe como literal y no como `despachador.DefaultPollMS`/`DefaultRespaldoMS` a propósito: un test
// que se derive de las constantes que protege sigue pasando cuando alguien las mueve, que es justo cuando
// tendría que doler.
const silencioSinTimbre = 1200 * time.Millisecond

// TestStartDespachador_ElBucleDespiertaPorElTimbreDeSuSesion custodia el OTRO extremo: que
// `startDespachador` construya el despertador por aviso SOBRE EL CANAL DE LA SESIÓN.
//
// Es una PAREJA DE FASES, y esa es toda su fuerza:
//
//	FASE 1 — con la fila ya disponible y NADIE tocando el timbre, no se entrega nada durante 1,2 s. Esto es
//	         lo que caza la mutación importante: sin el cableado, `Deps.Despertador` queda a nil, manda el
//	         `PollFijo(500 ms)` de `despachador.New` y la fila saldría en medio segundo. Un test que sólo
//	         mirase la fase 2 pasaría IGUAL con el cable roto — el poll acabaría entregando— y no probaría
//	         nada en absoluto.
//	FASE 2 — se toca el timbre de la sesión y la entrega llega enseguida. Esto es lo que caza el sustituto:
//	         si el despachador escuchara un canal distinto del que devuelve `s.avisoCanal()`, la fase 1
//	         seguiría pasando y aquí se acabaría el plazo.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - borrar el bloque `if aviso := s.avisoCanal(); aviso != nil { deps.Despertador = … }` de
//     startDespachador ⇒ vuelve el poll de 500 ms y cae la FASE 1;
//   - construirlo sobre `make(chan struct{}, 1)` en vez de sobre el canal de la sesión ⇒ cae la FASE 2;
//   - quitar `s.aviso = make(chan struct{}, 1)` de `arm` ⇒ el canal es nil, el `if` no entra, vuelve el
//     poll y cae la FASE 1.
func TestStartDespachador_ElBucleDespiertaPorElTimbreDeSuSesion(t *testing.T) {
	cola := nuevaColaConGrifo()
	sink := sinkConCanal{entregas: make(chan string, 8)}
	m := NewManager(NewLayout(t.TempDir()), nil, 5, testLogger(),
		WithColaDespachador(cola),
		WithWhatsmeowListen(muxDeEsteSink{sink: sink}, "wApp"),
	)
	s := &liveSession{meta: domain.Session{SessionID: uuidA}, log: testLogger()}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.arm(cancel) // como startListener: el canal existe ANTES de que arranque nadie
	m.startDespachador(ctx, s)
	if s.getDespachador() == nil {
		t.Fatal("la sesión no estrenó despachador con cola+mux+sink cableados: el test no está probando lo " +
			"que cree")
	}

	// Se espera a que el bucle haya MIRADO la cola vacía al menos una vez antes de abrir el grifo. Sin
	// esto, la fila podría aparecer a tiempo para la primera vuelta —que ocurre sin esperar a nadie, por
	// diseño de Run— y la fase 1 daría un rojo falso.
	select {
	case <-cola.miradas:
	case <-time.After(3 * time.Second):
		t.Fatal("el bucle del despachador no llegó a mirar la cola: no arrancó")
	}
	cola.abrir()

	// FASE 1: el trabajo está ahí y nadie ha llamado. Aquí el `time.After` NO es una guardia, es la
	// aserción.
	select {
	case id := <-sink.entregas:
		t.Fatalf("se entregó %s sin que nadie tocara el timbre, en menos de %s.\n"+
			"    CONSECUENCIA: el despachador NO está escuchando el canal de su sesión — está sondeando. Es\n"+
			"    exactamente el estado anterior a T1.8-7 (`Deps.Despertador` a nil ⇒ PollFijo de 500 ms), y\n"+
			"    llega hasta aquí sin un solo error, sin un log y con los cuatro gates en verde.",
			id, silencioSinTimbre)
	case <-time.After(silencioSinTimbre):
	}

	// FASE 2: se toca el timbre de ESTA sesión, que es el que el listener tocaría en campo.
	select {
	case s.avisoCanal() <- struct{}{}:
	default:
		t.Fatal("el timbre de la sesión estaba lleno con el bucle dormido: alguien más está avisando")
	}
	select {
	case id := <-sink.entregas:
		if id != "WA-TIMBRE-1" {
			t.Fatalf("se entregó %q en vez de la fila sembrada: el circuito no es el que el test cree", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("tocar el timbre de la sesión NO despertó a su despachador.\n" +
			"    CONSECUENCIA: el canal que el listener toca y el que el despachador escucha no son el mismo,\n" +
			"    así que cada aviso se pierde y el entrante sigue esperando al respaldo (5 s en producción) —\n" +
			"    diez veces PEOR que el poll que esta tarea vino a jubilar, y en completo silencio.")
	}

	cancel()
	s.waitDespachadores()
	m.wg.Wait()
}
