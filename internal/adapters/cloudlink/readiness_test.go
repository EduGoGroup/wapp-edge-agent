package cloudlink

// readiness_test.go — la readiness de inferencia que el Edge AFIRMA y el latido fuera de cadencia que la
// retransmite (Plan 044 · Ola 1.8 · T1.8-5, criterios (b) segunda mitad y (c)).
//
// TODO CORRE CONTRA EL Adapter REAL hablando con el server-double de e2e_test.go (bufconn), reusando el
// arnés de inferencia_test.go: aquí no se prueba una función suelta, se prueba QUÉ LLEGA AL CABLE.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (las DOS se escribieron, se compilaron y se EJECUTARON el
// 2026-08-24; ninguna se dedujo). Son las del criterio (d) de T1.8-5, y están elegidas para que cada una
// caiga sobre UNA mitad distinta de (c):
//
//  1. RETIRAR EL LATIDO DE TRANSICIÓN — sustituir el `if cl := a.currentClient(); cl != nil {
//     a.heartbeatAll(cl) }` de MarcarInferenciaReadiness por un `_ = a.currentClient()`. El estado se
//     sigue guardando y la cadencia lo sigue publicando; lo único que desaparece es la INMEDIATEZ.
//     ⇒ ROJO en TestReadiness_TransicionAReady_LatidoInmediatoSinEsperarLaCadencia («no llegó el latido
//     FUERA DE CADENCIA de la transición a READY … en 100ms»), y también en los dos de la segunda fuente.
//     TestReadiness_LosLatidosDeCadenciaLlevanElCampo se queda VERDE, que es lo correcto: esa mitad no
//     depende del latido de transición.
//  2. QUITAR EL CAMPO DE LOS LATIDOS DE CADENCIA — en heartbeatLoop, envolver `a.heartbeatAll(cl)` entre
//     un `previo := a.infReadiness.Swap(readinessDesconocida)` y su `Store(previo)`. Es el error de leer
//     el campo como si fuera de TRANSICIÓN y no de ESTADO.
//     ⇒ ROJO SÓLO en TestReadiness_LosLatidosDeCadenciaLlevanElCampo («readiness del latido nº2 =
//     INFERENCE_READINESS_UNSPECIFIED, want READY»); la primera mitad sigue VERDE.
//
// 🔴 NINGÚN TEST DE ESTE FICHERO ESPERA A LA CADENCIA PARA DARSE POR BUENO, y esa es la propiedad que
// vigilan. Los que miden inmediatez ponen el intervalo de latido en UNA HORA: si dentro de 100 ms llega
// un Heartbeat, no puede venir del ticker. El plazo corto no mide velocidad —eso sería un test de reloj
// de pared—, AÍSLA LA FUENTE: con la cadencia a una hora sólo hay un camino que pueda producirlo.

import (
	"context"
	"strconv"
	"testing"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-shared/envelope"
)

// sesionReadiness es la sesión que se registra para que HAYA latidos: el campo viaja dentro de
// `Heartbeat`, que se emite uno por sesión, así que un Adapter sin sesiones no emite ninguno.
const sesionReadiness = "sess-readiness"

// proveedorConError implementa app.ServidorInferencia devolviendo SIEMPRE el error canónico que se le
// dio. Es el doble que hace falta aquí y que inferencia_test.go no tiene: aquel siempre acierta.
type proveedorConError struct{ err error }

func (p *proveedorConError) Inferir(context.Context, app.PeticionInferencia) (app.RespuestaInferencia, error) {
	return app.RespuestaInferencia{}, p.err
}

// registrarSesionReadiness da de alta una sesión mínima en el multiplex (sin emisor real: aquí no se
// envía nada, sólo se late).
func registrarSesionReadiness(h *arnesInferencia, sessionID string) {
	h.adapter.Register(sessionID, "",
		func(context.Context, string, string, string) error { return nil },
		nil,
		func() bool { return true })
}

// esperarLatido lee del server-double hasta el primer Heartbeat, o falla si no llega en `plazo`.
//
// DEVUELVE EL PLAZO COMO PARTE DEL CONTRATO DEL HELPER: los tests de inmediatez le pasan 100 ms y los de
// cadencia uno holgado, y quien lea la llamada ve de cuál de los dos se trata sin abrir esta función.
func esperarLatido(t *testing.T, srv *serverDouble, plazo time.Duration, que string) *cloudlinkv1.Heartbeat {
	t.Helper()
	limite := time.After(plazo)
	for {
		select {
		case msg := <-srv.received:
			if hb := msg.GetHeartbeat(); hb != nil {
				return hb
			}
			// Cualquier otro frame (un InferenceResult, p.ej.) no es lo que se busca: se descarta.
		case <-limite:
			t.Fatalf("no llegó %s en %v", que, plazo)
			return nil
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (c) primera mitad — EL LATIDO INMEDIATO DE LA TRANSICIÓN
// ─────────────────────────────────────────────────────────────────────────────

// TestReadiness_TransicionAReady_LatidoInmediatoSinEsperarLaCadencia: con la cadencia puesta en UNA HORA,
// marcar READY produce un Heartbeat con INFERENCE_READINESS_READY en menos de 100 ms.
//
// POR QUÉ ESTE TEST ES EL QUE IMPORTA DE TODA LA TAREA: sin el latido de transición, el campo existiría y
// el Cloud lo leería… en el siguiente latido de cadencia, o sea hasta 30 s después de que el cajero
// abriera su socket. El calentamiento de arranque volvería a llegar tarde, exactamente igual que con el
// `sleep 6` que esta ola vino a retirar, sólo que con un reloj distinto y más difícil de ver.
//
// De paso ancla que el estado inicial es UNSPECIFIED y NUNCA DOWN: el primer latido, el que ancla la
// sesión al conectar, se comprueba antes de tocar nada.
func TestReadiness_TransicionAReady_LatidoInmediatoSinEsperarLaCadencia(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Cadencia de una hora: lo que llegue en 100 ms no puede venir del ticker.
	h := nuevoArnesInferencia(t, nil, WithHeartbeatInterval(time.Hour))
	registrarSesionReadiness(h, sesionReadiness)
	h.arrancar(t, ctx)

	// El latido de ANCLA (runOnce → heartbeatAll). Nadie ha afirmado nada todavía.
	inicial := esperarLatido(t, h.srv, 5*time.Second, "el latido inicial de ancla")
	if got := inicial.GetInferenceReadiness(); got != cloudlinkv1.InferenceReadiness_INFERENCE_READINESS_UNSPECIFIED {
		t.Fatalf("readiness del latido inicial = %v, want UNSPECIFIED. 🔴 El cero del contrato significa "+
			"«este Edge no lo dice»: mandar DOWN sin que nadie lo haya comprobado haría que el Cloud dejara "+
			"de calentar un Edge sano, sin un solo error", got)
	}

	arranque := time.Now()
	if !h.adapter.MarcarInferenciaReadiness(true) {
		t.Fatal("MarcarInferenciaReadiness(true) dijo que NO hubo transición desde el estado inicial; " +
			"la transición es lo que dispara el latido")
	}

	hb := esperarLatido(t, h.srv, 100*time.Millisecond,
		"el latido FUERA DE CADENCIA de la transición a READY (la cadencia está en 1 h: si esto expira, "+
			"el latido de transición no se está emitiendo)")
	if got := hb.GetInferenceReadiness(); got != cloudlinkv1.InferenceReadiness_INFERENCE_READINESS_READY {
		t.Fatalf("readiness del latido de transición = %v, want READY", got)
	}
	if tardo := time.Since(arranque); tardo > 100*time.Millisecond {
		t.Errorf("el latido de transición tardó %v (> 100 ms)", tardo)
	}

	// IDEMPOTENCIA: repetir el mismo valor no es una transición y no debe producir OTRO latido. Sin esta
	// guarda, la segunda fuente de readiness (cada inferencia servida marca READY) emitiría un latido por
	// petición y por sesión.
	if h.adapter.MarcarInferenciaReadiness(true) {
		t.Error("MarcarInferenciaReadiness(true) repetido se declaró TRANSICIÓN; debe ser idempotente")
	}
	select {
	case msg := <-h.srv.received:
		if msg.GetHeartbeat() != nil {
			t.Errorf("llegó un latido extra tras repetir la MISMA readiness: la guarda de transición no está")
		}
	case <-time.After(150 * time.Millisecond):
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (c) segunda mitad — LOS LATIDOS DE CADENCIA LO SIGUEN LLEVANDO
// ─────────────────────────────────────────────────────────────────────────────

// TestReadiness_LosLatidosDeCadenciaLlevanElCampo: el campo es de ESTADO, no de transición, así que TODOS
// los latidos lo llevan — también los que emite el ticker mucho después del cambio.
//
// 🔴 LA READINESS SE MARCA **ANTES** DE ARRANCAR, y eso es deliberado: con el stream aún sin abrir no hay
// latido de transición que emitir (currentClient() es nil), así que lo que este test observa son
// EXCLUSIVAMENTE el latido de ancla y los del ticker. Si el campo se poblara sólo en el camino de la
// transición —el error natural de leer «inmediato» como «sólo entonces»—, este test se pondría rojo y el
// de arriba seguiría verde, que es justo la separación que pide el criterio (d).
func TestReadiness_LosLatidosDeCadenciaLlevanElCampo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Cadencia corta: aquí sí interesa que el ticker dispare varias veces dentro del test.
	h := nuevoArnesInferencia(t, nil, WithHeartbeatInterval(30*time.Millisecond))
	registrarSesionReadiness(h, sesionReadiness)

	if !h.adapter.MarcarInferenciaReadiness(true) {
		t.Fatal("MarcarInferenciaReadiness(true) no se declaró transición desde el estado inicial")
	}
	h.arrancar(t, ctx)

	// TRES latidos: el primero es el ancla de runOnce; con la cadencia en 30 ms, del segundo en adelante
	// los emite heartbeatLoop. Exigir tres es lo que garantiza que al menos DOS son de cadencia.
	const quiero = 3
	for i := 1; i <= quiero; i++ {
		hb := esperarLatido(t, h.srv, 5*time.Second, "el latido nº"+strconv.Itoa(i))
		if got := hb.GetInferenceReadiness(); got != cloudlinkv1.InferenceReadiness_INFERENCE_READINESS_READY {
			t.Fatalf("readiness del latido nº%d = %v, want READY. 🔴 El campo es de ESTADO: un Cloud que se "+
				"reconecta tiene que poder leerlo del PRIMER latido que vea, sin haber presenciado la "+
				"transición", i, got)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (b) segunda mitad — LA MUERTE POR SEÑAL SE APRENDE AL PEDIR LA SIGUIENTE INFERENCIA
// ─────────────────────────────────────────────────────────────────────────────

// TestReadiness_InferenciaConOllamaCaido_PasaAReadinessDown: un cajero que muere por SIGKILL no manda
// «caído» (no corre su `defer`). El núcleo lo aprende igual: la siguiente inferencia vuelve con
// app.ErrInferenciaOllamaCaido y eso pasa la readiness a DOWN, con su latido inmediato.
//
// ES LA MITAD QUE HACE QUE NO HAGA FALTA UN PROBE. Sin ella, la única fuente sería el aviso del cajero, y
// un aviso que no se manda deja al Cloud calentando para siempre un Edge que ya no puede servir.
func TestReadiness_InferenciaConOllamaCaido_PasaAReadinessDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub, _, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	prov := &proveedorConError{err: app.ErrInferenciaOllamaCaido}
	h := nuevoArnesInferencia(t, nil,
		WithHeartbeatInterval(time.Hour), WithCloudEncPubKey(pub), WithServidorInferencia(prov))
	registrarSesionReadiness(h, sesionReadiness)

	// Se parte de READY: es el estado en el que el aviso de arranque del cajero deja al núcleo. Sin él, el
	// paso a DOWN no sería una transición y no habría latido que observar.
	h.adapter.MarcarInferenciaReadiness(true)
	h.arrancar(t, ctx)
	if hb := esperarLatido(t, h.srv, 5*time.Second, "el latido de ancla"); hb.GetInferenceReadiness() !=
		cloudlinkv1.InferenceReadiness_INFERENCE_READINESS_READY {
		t.Fatalf("el latido de ancla debía salir READY; salió %v", hb.GetInferenceReadiness())
	}

	pushInferencia(t, h, peticion("cmd-ollama-caido", sesionReadiness))

	hb := esperarLatido(t, h.srv, 5*time.Second, "el latido de la transición a DOWN")
	if got := hb.GetInferenceReadiness(); got != cloudlinkv1.InferenceReadiness_INFERENCE_READINESS_DOWN {
		t.Fatalf("readiness tras un OLLAMA_DOWN = %v, want DOWN. Sin esta transición, un cajero muerto por "+
			"SIGKILL dejaría al Cloud calentando este Edge para siempre", got)
	}
}

// TestReadiness_UnaInferenciaServidaReponeReady: la cara positiva de la misma fuente. Una inferencia que
// sale bien es la prueba más dura de que el proveedor local está ahí, y repone la READY que se hubiera
// perdido (un aviso de arranque que no llegó porque el socket del núcleo aún no estaba).
//
// SIN ESTA MITAD, UN AVISO PERDIDO SERÍA PERMANENTE: el Edge se quedaría marcado DOWN mientras sirve
// inferencias sin un solo error, que es la peor de las dos formas de fallar.
func TestReadiness_UnaInferenciaServidaReponeReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub, _, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	h := nuevoArnesInferencia(t, nil,
		WithHeartbeatInterval(time.Hour), WithCloudEncPubKey(pub), WithServidorInferencia(nuevoProveedor()))
	registrarSesionReadiness(h, sesionReadiness)

	// Se parte de DOWN (p.ej. una inferencia anterior falló con el cajero parado).
	h.adapter.MarcarInferenciaReadiness(false)
	h.arrancar(t, ctx)
	if hb := esperarLatido(t, h.srv, 5*time.Second, "el latido de ancla"); hb.GetInferenceReadiness() !=
		cloudlinkv1.InferenceReadiness_INFERENCE_READINESS_DOWN {
		t.Fatalf("el latido de ancla debía salir DOWN; salió %v", hb.GetInferenceReadiness())
	}

	pushInferencia(t, h, peticion("cmd-servida", sesionReadiness))

	hb := esperarLatido(t, h.srv, 5*time.Second, "el latido de la transición de vuelta a READY")
	if got := hb.GetInferenceReadiness(); got != cloudlinkv1.InferenceReadiness_INFERENCE_READINESS_READY {
		t.Fatalf("readiness tras una inferencia SERVIDA = %v, want READY", got)
	}
}

// TestReadiness_LosOtrosCodigosDeErrorNoLaMueven: BREAKER_ABIERTO lo devuelve un cajero VIVO. La readiness
// habla de si el proveedor ESTÁ, no de si esta petición salió, así que no se toca.
//
// 🔴 CUSTODIA UNA DECISIÓN, NO UNA IMPLEMENTACIÓN. Tratar los cinco códigos por igual parece más simple y
// es peor: el breaker se cierra solo a los 60 s, así que la señal oscilaría —DOWN, READY, DOWN— sin que
// nada cambiara en la máquina, y el Cloud dejaría de calentar a rachas. Los otros tres (TIMEOUT,
// LEASE_INVALID, EDGE_SIN_CAPACIDAD) caen del mismo lado por el mismo argumento.
func TestReadiness_LosOtrosCodigosDeErrorNoLaMueven(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub, _, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	prov := &proveedorConError{err: app.ErrInferenciaBreakerAbierto}
	h := nuevoArnesInferencia(t, nil,
		WithHeartbeatInterval(time.Hour), WithCloudEncPubKey(pub), WithServidorInferencia(prov))
	registrarSesionReadiness(h, sesionReadiness)

	h.adapter.MarcarInferenciaReadiness(true)
	h.arrancar(t, ctx)
	esperarLatido(t, h.srv, 5*time.Second, "el latido de ancla")

	pushInferencia(t, h, peticion("cmd-breaker", sesionReadiness))

	// 🔴 NO SE USA esperarResultado AQUÍ, Y NO ES ESTILO: aquel helper va por recvKind, que DESCARTA todo
	// frame que no case — latidos incluidos. Con él, el latido espurio que este test existe para cazar se
	// tiraría a la basura antes de poder mirarlo y el test saldría verde pasara lo que pasara. Se lee el
	// canal a mano y se clasifica cada frame.
	limite := time.After(5 * time.Second)
	for visto := false; !visto; {
		select {
		case msg := <-h.srv.received:
			if hb := msg.GetHeartbeat(); hb != nil {
				t.Fatalf("un BREAKER_ABIERTO movió la readiness (latido con %v): con la cadencia en una hora, "+
					"un latido aquí sólo puede venir de una transición, y sólo OLLAMA_DOWN debe provocarla",
					hb.GetInferenceReadiness())
			}
			if res := msg.GetInferenceResult(); res != nil && res.GetCommandId() == "cmd-breaker" {
				if res.GetError() != cloudlinkv1.InferenceError_INFERENCE_ERROR_BREAKER_OPEN {
					t.Fatalf("el error devuelto = %v, want BREAKER_OPEN (el test no estaría midiendo lo que cree)",
						res.GetError())
				}
				visto = true
			}
		case <-limite:
			t.Fatal("no llegó el InferenceResult de cmd-breaker")
		}
	}
	if got := h.adapter.readinessProto(); got != cloudlinkv1.InferenceReadiness_INFERENCE_READINESS_READY {
		t.Errorf("readiness tras un BREAKER_ABIERTO = %v, want READY (intacta)", got)
	}
}
