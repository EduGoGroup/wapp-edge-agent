package cloudlink

// inferencia_test.go — EL CARRIL DE LA INFERENCIA, PROBADO POR EL CABLE (Plan 044 · Ola 1.6 · T1.6-2,
// ADR-0045 §2, REQ-34).
//
// Todo lo de aquí corre contra el Adapter REAL hablando con el server-double de e2e_test.go (bufconn, sin
// red ni TLS): se empuja un `CloudToEdge{inference_request}` de verdad y se lee el `EdgeToCloud{
// inference_result}` que vuelve. No se llama a `atender()` a mano en ningún test, y es deliberado: la
// mitad del valor de este carril está en el DESVÍO de `runOnce` —que la petición no pase por el
// dispatcher-por-sesión— y ese desvío sólo existe en el camino completo.
//
// LOS CINCO INVARIANTES QUE ESTE FICHERO SOSTIENE:
//
//  1. INV-051.3 · la traducción a proto cubre LOS CINCO errores canónicos y ninguno cae en el `default`
//     (TestAProtoInferenceError_…). Es el test que caza el sexto error añadido a `app` con el switch sin
//     tocar, ANTES de que el Cloud reciba un motivo que no sabe mapear.
//  2. El carril NO es el dispatcher: se sirve con `session_id` VACÍO y sin ninguna sesión registrada —
//     justo lo que el demux por sesión rechazaría con «comando para session_id desconocido, ignorado».
//  3. La salida sube SELLADA (envelope.SealFor hacia la pública de la nube) y correlacionada por el
//     `command_id` DEL REQUEST; y sin esa pública NO se llama al proveedor.
//  4. Idempotencia EN VUELO —no «visto para siempre»—: el duplicado simultáneo se ignora, el repetido
//     DESPUÉS de terminar se vuelve a servir (es un reintento legítimo del Cloud que perdió la respuesta).
//  5. El gate de lease del ADR-0007 con su gracia: desactivado sin Validator, LEASE_INVALID con el lease
//     revocado tras agotar la gracia, servido en modo sombra, y servido si el lease se vuelve operable
//     DENTRO de la ventana (la ventana medida de 0,5-1,1 s en que el Validator nace cerrado).
//
// 🔴 INV-051.1: ningún test escribe un prompt o una salida en un log; lo que se busca en el log son
// frases de DESENLACE. Los prompts de aquí son texto de atrezo, no PII.

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-cloudlink/lease"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-shared/envelope"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

const (
	// sesionInferencia es la sesión que registran los tests que necesitan UNA (los del gate de lease).
	// Los demás no registran ninguna a propósito: ver TestInferencia_SeSirveSinSesionYConSessionIDVacio.
	sesionInferencia = "sess-inferencia"
	// salidaDelModelo es lo que devuelve el proveedor doble. Es JSON porque el contrato dice `raw_json`,
	// pero el Edge no lo valida ni lo mira: sube tal cual.
	salidaDelModelo = `{"intent":"comprar","confidence":0.91}`
)

// ─────────────────────────────────────────────────────────────────────────────
// El doble del proveedor local de LLM
// ─────────────────────────────────────────────────────────────────────────────

// proveedorFake implementa app.ServidorInferencia. En producción del otro lado hay un socket unix, el
// proceso `agent cajero` y un Ollama; aquí sólo hace falta poder (a) CONTAR las llamadas, (b) ver con qué
// petición llegó y (c) QUEDARSE COLGADO a voluntad, que es lo que convierte «el carril está lleno» y «hay
// una inferencia en vuelo» en estados observables sin depender del reloj.
type proveedorFake struct {
	// llamadas es EL NÚMERO QUE MANDA en las aserciones. El canal `entradas` es sólo una señal de
	// sincronización y puede descartar capturas si se llenara; este contador, no.
	llamadas atomic.Int64
	entradas chan app.PeticionInferencia
	// bloquear, si no es nil, retiene al proveedor dentro de Inferir hasta que el test llame a liberar()
	// (o hasta que el ctx del carril muera, que es lo que pasa al apagar el Adapter: sin ese caso, el
	// shutdown del carril esperaría para siempre a un worker que nadie va a soltar).
	bloquear chan struct{}
	unaVez   sync.Once

	salida string
}

func nuevoProveedor() *proveedorFake {
	return &proveedorFake{entradas: make(chan app.PeticionInferencia, 32), salida: salidaDelModelo}
}

// nuevoProveedorColgado devuelve un proveedor que se queda DENTRO de Inferir hasta que el test lo libere.
func nuevoProveedorColgado() *proveedorFake {
	p := nuevoProveedor()
	p.bloquear = make(chan struct{})
	return p
}

func (p *proveedorFake) Inferir(ctx context.Context, pet app.PeticionInferencia) (app.RespuestaInferencia, error) {
	p.llamadas.Add(1)
	select {
	case p.entradas <- pet:
	default:
	}
	if p.bloquear != nil {
		select {
		case <-p.bloquear:
		case <-ctx.Done():
			// El carril se está apagando: se devuelve un error canónico para no salirse del vocabulario que
			// el puerto promete (el carril loguearía un «error fuera del vocabulario» y taparía la causa).
			return app.RespuestaInferencia{}, app.ErrInferenciaTimeout
		}
	}
	return app.RespuestaInferencia{RawJSON: p.salida}, nil
}

// liberar suelta a todos los que estén colgados. Idempotente: los tests la llaman con `defer` Y en medio,
// porque si el test muere antes de tiempo el `defer` es lo único que impide que el shutdown del carril
// espere a un worker retenido.
func (p *proveedorFake) liberar() {
	if p.bloquear == nil {
		return
	}
	p.unaVez.Do(func() { close(p.bloquear) })
}

// ─────────────────────────────────────────────────────────────────────────────
// Arnés
// ─────────────────────────────────────────────────────────────────────────────

// arnesInferencia es el rawHarness de dispatcher_test.go con dos cosas que aquí hacen falta y allí no: un
// ValidatorFactory inyectable (el gate de lease de la inferencia se mide contra Validators reales) y un
// log INSPECCIONABLE (varios invariantes de este carril sólo dejan rastro en el log — un duplicado
// ignorado, por definición, no produce ninguna respuesta que mirar).
type arnesInferencia struct {
	srv     *serverDouble
	stream  cloudlinkv1.CloudLink_ConnectServer
	adapter *Adapter
	log     *syncBuf // syncBuf vive en lease_log_test.go: el logger escribe desde varias goroutines
}

// nuevoArnesInferencia cablea el Adapter real contra el server-double, SIN arrancarlo: el llamante
// registra las sesiones que necesite y luego llama a arrancar (mismo orden que rawHarness).
func nuevoArnesInferencia(t *testing.T, newValidator ValidatorFactory, opts ...Option) *arnesInferencia {
	t.Helper()

	srv := newServerDouble()
	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	cloudlinkv1.RegisterCloudLinkServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	cc, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	registro := &syncBuf{}
	log := sharedlogger.New(sharedlogger.WithWriter(registro), sharedlogger.WithJSON(true))
	// Heartbeat de una hora: sólo interesa el latido inicial, si es que hay sesión que lo emita.
	adapter := NewAdapter(cc, log, newValidator, append([]Option{WithHeartbeatInterval(time.Hour)}, opts...)...)

	return &arnesInferencia{srv: srv, adapter: adapter, log: registro}
}

// arrancar lanza el loop del Adapter y espera el handshake del stream. NO espera ningún latido: varios
// tests no registran sesión alguna, así que no hay latido que esperar — y no hace falta, porque el frame
// que empuja el test se queda en el canal de recepción del cliente hasta que `runOnce` entra en su bucle,
// que es DESPUÉS de publicar el cliente (setClient). El descarte silencioso que sufría `Deliver` (ver
// awaitAdapterReady) no puede ocurrir en este camino.
func (h *arnesInferencia) arrancar(t *testing.T, ctx context.Context) {
	t.Helper()
	go func() { _ = h.adapter.Run(ctx) }()
	select {
	case h.stream = <-h.srv.streamCh:
	case <-ctx.Done():
		t.Fatalf("timeout esperando que el Adapter abra el stream: %v", ctx.Err())
	}
}

// peticion arma un InferenceRequest con los dos identificadores y un prompt de atrezo.
func peticion(cmdID, sessionID string) *cloudlinkv1.InferenceRequest {
	return &cloudlinkv1.InferenceRequest{
		CommandId: cmdID,
		SessionId: sessionID,
		Prompt:    "clasifica esto",
		Format:    "json",
	}
}

// pushInferencia empuja el frame cloud->edge. El `session_id` del SOBRE se pone igual que el del request:
// hoy el carril lee el del sobre y el `command_id` el del request (ver la nota en el informe de T1.6-2).
func pushInferencia(t *testing.T, h *arnesInferencia, req *cloudlinkv1.InferenceRequest) {
	t.Helper()
	pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
		CommandId: req.GetCommandId(),
		SessionId: req.GetSessionId(),
		Payload:   &cloudlinkv1.CloudToEdge_InferenceRequest{InferenceRequest: req},
	})
}

// esperarResultado lee del server-double hasta el InferenceResult correlacionado con cmdID.
func esperarResultado(t *testing.T, ctx context.Context, h *arnesInferencia, cmdID string) *cloudlinkv1.InferenceResult {
	t.Helper()
	msg := recvKind(t, ctx, h.srv, "InferenceResult de "+cmdID, func(m *cloudlinkv1.EdgeToCloud) bool {
		return m.GetInferenceResult() != nil && m.GetInferenceResult().GetCommandId() == cmdID
	})
	return msg.GetInferenceResult()
}

// esperarEntradaAlProveedor bloquea hasta que el proveedor doble haya ENTRADO en Inferir, y devuelve la
// petición con la que entró. Es la sincronización que sustituye a los sleeps en todo este fichero.
func esperarEntradaAlProveedor(t *testing.T, ctx context.Context, p *proveedorFake) app.PeticionInferencia {
	t.Helper()
	select {
	case pet := <-p.entradas:
		return pet
	case <-ctx.Done():
		t.Fatalf("timeout: el proveedor nunca fue invocado (llamadas=%d): %v", p.llamadas.Load(), ctx.Err())
		return app.PeticionInferencia{}
	}
}

// abrirSalida abre el `enc_output` con la privada de la nube y devuelve el raw_json que iba dentro.
func abrirSalida(t *testing.T, priv []byte, res *cloudlinkv1.InferenceResult) string {
	t.Helper()
	if len(res.GetEncOutput()) == 0 {
		t.Fatalf("el resultado no trae `enc_output` (error=%v): no hay sobre que abrir", res.GetError())
	}
	crudo, err := envelope.OpenWith(priv, res.GetEncOutput())
	if err != nil {
		t.Fatalf("OpenWith sobre el enc_output: %v", err)
	}
	var salida cloudlinkv1.InferenceOutput
	if err := proto.Unmarshal(crudo, &salida); err != nil {
		t.Fatalf("Unmarshal InferenceOutput: %v", err)
	}
	return salida.GetRawJson()
}

// esError afirma que el resultado vino por la rama de ERROR del oneof (no por `enc_output`) y con el
// motivo esperado. Las dos mitades importan: `GetError()` devuelve UNSPECIFIED tanto para «error sin
// causa nombrada» como para «esto ni siquiera es un error», así que sin mirar la rama, un test de
// UNSPECIFIED pasaría con un resultado exitoso.
func esError(t *testing.T, res *cloudlinkv1.InferenceResult, quiero cloudlinkv1.InferenceError) {
	t.Helper()
	if _, ok := res.GetResult().(*cloudlinkv1.InferenceResult_Error); !ok {
		t.Fatalf("el resultado NO vino por la rama de error del oneof (result=%T, enc_output=%d bytes)",
			res.GetResult(), len(res.GetEncOutput()))
	}
	if res.GetError() != quiero {
		t.Errorf("motivo: got %v want %v", res.GetError(), quiero)
	}
}

// pushSendText empuja un SendText por la sesión dada. Es la BARRERA de orden del test del kill-switch: el
// dispatcher es SERIAL dentro de cada session_id, así que el Ack de este envío sólo llega cuando el
// LeaseUpdate empujado antes por la misma sesión ya se aplicó. Sustituye a un sleep, que sería lo único
// que se puede escribir sin esta propiedad.
func pushSendText(t *testing.T, h *arnesInferencia, sessionID, cmdID string) {
	t.Helper()
	pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
		CommandId: cmdID,
		SessionId: sessionID,
		Payload:   &cloudlinkv1.CloudToEdge_SendText{SendText: &cloudlinkv1.SendText{To: "5491100000000", Text: "barrera de orden"}},
	})
}

// enviarNada es el sendFunc de las sesiones que estos tests registran sólo para tener un Validator: aquí
// nadie manda WhatsApp salvo las barreras del test del kill-switch, que usan su propio canal.
func enviarNada(context.Context, string, string, string) error { return nil }

// ─────────────────────────────────────────────────────────────────────────────
// 1) INV-051.3 · la traducción a proto cubre los cinco
// ─────────────────────────────────────────────────────────────────────────────

// TestAProtoInferenceError_TraduceLosCincoSinCaerEnElDefault es EL test que hace verdadero el comentario
// de aProtoInferenceError: recorre `app.ErroresInferencia` —la lista canónica, no una copia a mano— y
// exige que ninguno salga por el `default`.
//
// EL FALLO QUE CAZA, y por qué no lo caza nada más: añadir un sexto error a `app` compila sin tocar el
// switch. En campo eso se manifiesta como un Cloud que recibe INFERENCE_ERROR_UNSPECIFIED —un motivo que
// no sabe mapear— justo cuando más falta hace saber por qué se degradó. No hay ningún test de conducta
// que lo vea, porque el error nuevo no tiene todavía un camino que lo produzca.
//
// LA CORRESPONDENCIA POR NOMBRE NO ES UNA COPIA DEL SWITCH. El switch decide por IDENTIDAD DE PUNTERO; el
// test decide por el `codigo` del error, que es la etiqueta que viaja por el socket, y deriva de él el
// nombre que el enum tiene que llevar: "INFERENCE_ERROR_" + MAYÚSCULAS(codigo). Son dos afirmaciones
// independientes sobre el mismo hecho, así que un arm del switch cruzado (timeout→BREAKER_OPEN) sale rojo.
func TestAProtoInferenceError_TraduceLosCincoSinCaerEnElDefault(t *testing.T) {
	if len(app.ErroresInferencia) == 0 {
		t.Fatalf("app.ErroresInferencia está vacía: este test recorrería la nada y saldría verde")
	}

	porValor := make(map[cloudlinkv1.InferenceError]string, len(app.ErroresInferencia))
	for _, e := range app.ErroresInferencia {
		got := aProtoInferenceError(e)

		if got == cloudlinkv1.InferenceError_INFERENCE_ERROR_UNSPECIFIED {
			t.Errorf("aProtoInferenceError(%q) cayó en el DEFAULT (UNSPECIFIED).\n"+
				"    CONSECUENCIA: el Cloud recibe un motivo que no sabe mapear y degrada a ciegas.\n"+
				"    ARREGLO: añade el case en aProtoInferenceError (inferencia.go) y el valor en el enum del .proto.",
				e.Codigo())
			continue
		}
		if otro, repetido := porValor[got]; repetido {
			t.Errorf("aProtoInferenceError devuelve %v para DOS errores distintos (%q y %q): el Cloud no "+
				"podría distinguirlos y uno de los dos diagnósticos se pierde", got, otro, e.Codigo())
		}
		porValor[got] = e.Codigo()

		// Y cada uno cae en el valor que le toca POR NOMBRE.
		quieroNombre := "INFERENCE_ERROR_" + strings.ToUpper(e.Codigo())
		quiero, ok := cloudlinkv1.InferenceError_value[quieroNombre]
		if !ok {
			t.Errorf("el error %q espera el valor de enum %s y ese nombre NO existe en el contrato: o el "+
				"código canónico o el enum se renombraron y dejaron de corresponderse", e.Codigo(), quieroNombre)
			continue
		}
		if int32(got) != quiero {
			t.Errorf("aProtoInferenceError(%q) = %v, want %s (el switch tiene un case cruzado)",
				e.Codigo(), got, quieroNombre)
		}
	}

	// El `default` sigue siendo UNSPECIFIED, que es el único valor que no AFIRMA una causa falsa. Se
	// comprueba con un error del tipo canónico que no es ninguno de los cinco (el caso del sexto error
	// recién declarado y aún sin case) y con el nil defensivo.
	if got := aProtoInferenceError(&app.ErrorInferencia{}); got != cloudlinkv1.InferenceError_INFERENCE_ERROR_UNSPECIFIED {
		t.Errorf("el default devolvió %v: tiene que ser UNSPECIFIED — devolver OLLAMA_DOWN escondería el "+
			"olvido tras un diagnóstico plausible y FALSO sobre la máquina del cliente", got)
	}
	if got := aProtoInferenceError(nil); got != cloudlinkv1.InferenceError_INFERENCE_ERROR_UNSPECIFIED {
		t.Errorf("aProtoInferenceError(nil) = %v, want UNSPECIFIED", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 2) El desvío: el carril no es el dispatcher-por-sesión
// ─────────────────────────────────────────────────────────────────────────────

// TestInferencia_SeSirveSinSesionYConSessionIDVacio es el invariante que justifica el carril ENTERO: una
// inferencia se atiende aunque su `session_id` venga vacío —que es lo normal por contrato: el servicio de
// inferencia es del EDGE, no de una sesión— y aunque no haya NINGUNA sesión registrada.
//
// Es exactamente lo que el demux por sesión haría fracasar: `handleCommand` resuelve `a.entry(sid)`, no
// encuentra nada y escribe «comando para session_id desconocido (ignorado)» — el Cloud se quedaría
// esperando una respuesta que nunca sale. Por eso el test no se conforma con ver la respuesta: comprueba
// además que ESA frase NO aparece en el log, que es la huella de haber pasado por el camino equivocado.
func TestInferencia_SeSirveSinSesionYConSessionIDVacio(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub, _, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	prov := nuevoProveedor()
	h := nuevoArnesInferencia(t, nil, WithCloudEncPubKey(pub), WithServidorInferencia(prov))
	// NO se registra ninguna sesión: es la mitad del enunciado.
	h.arrancar(t, ctx)

	pushInferencia(t, h, peticion("cmd-sin-sesion", ""))

	res := esperarResultado(t, ctx, h, "cmd-sin-sesion")
	if len(res.GetEncOutput()) == 0 {
		t.Fatalf("la inferencia NO se sirvió con session_id vacío (motivo=%v); el carril está gateando por "+
			"sesión, que es justo lo que no debe hacer", res.GetError())
	}
	if prov.llamadas.Load() != 1 {
		t.Errorf("llamadas al proveedor: got %d want 1", prov.llamadas.Load())
	}
	if registro := h.log.String(); strings.Contains(registro, "session_id desconocido") {
		t.Errorf("el frame pasó por el demux por sesión (el log trae «session_id desconocido»): el desvío de "+
			"runOnce dejó de estar ANTES de disp.dispatch.\n    log=%s", registro)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 3) Sellado y correlación
// ─────────────────────────────────────────────────────────────────────────────

// TestInferencia_ElResultadoSubeSelladoYCorrelacionado: la respuesta es un EdgeToCloud_InferenceResult con
// el `command_id` DEL REQUEST, la rama `enc_output` poblada, y dentro del sobre —abierto con la privada de
// la nube— un InferenceOutput con el raw_json EXACTO.
//
// LAS TRES MITADES SON UNA SOLA COSA. Sin el command_id el Cloud no sabe a qué pregunta responde esto;
// sin el sobre, la salida del modelo —que puede llevar texto literal del cliente— viajaría en claro
// (ADR-0020 §5); y sin la exactitud del raw_json el Edge habría «arreglado» una salida, que es lo que
// haría invisible el fallo del prompt (lo único que en el Cloud se puede corregir).
//
// De paso ancla que el Edge NO INTERPRETA NADA: prompt, format, temperatura y plazo llegan al puerto tal
// y como venían en el frame.
func TestInferencia_ElResultadoSubeSelladoYCorrelacionado(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub, priv, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	prov := nuevoProveedor()
	h := nuevoArnesInferencia(t, nil, WithCloudEncPubKey(pub), WithServidorInferencia(prov))
	h.arrancar(t, ctx)

	temperatura := float32(0)
	req := &cloudlinkv1.InferenceRequest{
		CommandId:   "cmd-sellado",
		SessionId:   "",
		Prompt:      "clasifica: quiero dos empanadas",
		Format:      `{"type":"object"}`,
		Temperature: &temperatura,
		TimeoutMs:   4500,
	}
	pushInferencia(t, h, req)

	// (a) El puerto recibe el frame VERBATIM.
	pet := esperarEntradaAlProveedor(t, ctx, prov)
	if pet.CommandID != req.GetCommandId() {
		t.Errorf("CommandID: got %q want %q", pet.CommandID, req.GetCommandId())
	}
	if pet.Prompt != req.GetPrompt() {
		t.Errorf("el prompt llegó alterado: got %q want %q", pet.Prompt, req.GetPrompt())
	}
	if pet.Format != req.GetFormat() {
		t.Errorf("el format llegó alterado: got %q want %q (el Edge lo reenvía OPACO)", pet.Format, req.GetFormat())
	}
	if pet.Temperature == nil || *pet.Temperature != 0 {
		t.Errorf("la temperatura perdió su PRESENCIA: got %v want un puntero a 0 — sin presencia explícita, "+
			"«quiero 0» y «no dije nada» serían el mismo byte", pet.Temperature)
	}
	if pet.Timeout != 4500*time.Millisecond {
		t.Errorf("el plazo: got %v want 4.5s", pet.Timeout)
	}

	// (b) La respuesta: correlacionada, sellada y exacta.
	res := esperarResultado(t, ctx, h, "cmd-sellado")
	if _, ok := res.GetResult().(*cloudlinkv1.InferenceResult_EncOutput); !ok {
		t.Fatalf("la respuesta no vino por la rama enc_output (motivo=%v)", res.GetError())
	}
	if got := abrirSalida(t, priv, res); got != salidaDelModelo {
		t.Errorf("raw_json dentro del sobre: got %q want %q", got, salidaDelModelo)
	}
	if h.adapter.inferenciasServidas.Load() != 1 {
		t.Errorf("el contador de servidas (INV-051.3): got %d want 1", h.adapter.inferenciasServidas.Load())
	}
}

// TestInferencia_SinPublicaDeCifradoNoSeLlamaAlProveedor: sin `cloud_enc_pubkey` la salida no se podría
// sellar y el contrato no admite salida en claro para este campo, así que NO se gasta el LLM del cliente
// en producir algo que no se puede entregar. Se comprueban LAS DOS MITADES —el puerto mudo y la respuesta
// de error—: sólo con la segunda, un carril que llamara al proveedor y tirara el resultado pasaría igual.
//
// El motivo es UNSPECIFIED a propósito y no es un olvido: ninguno de los cinco dice «inferí pero no puedo
// sellarte la respuesta», y responder OLLAMA_DOWN sería mentir sobre la causa.
func TestInferencia_SinPublicaDeCifradoNoSeLlamaAlProveedor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prov := nuevoProveedor()
	// Sin WithCloudEncPubKey => a.cloudEncPub == nil.
	h := nuevoArnesInferencia(t, nil, WithServidorInferencia(prov))
	h.arrancar(t, ctx)

	pushInferencia(t, h, peticion("cmd-sin-pubkey", ""))

	res := esperarResultado(t, ctx, h, "cmd-sin-pubkey")
	esError(t, res, cloudlinkv1.InferenceError_INFERENCE_ERROR_UNSPECIFIED)
	if len(res.GetEncOutput()) != 0 {
		t.Errorf("vino enc_output (%d bytes) sin pública con la que sellar", len(res.GetEncOutput()))
	}
	// La otra mitad: el proveedor NO fue tocado. Al haber llegado ya la respuesta, si se hubiera llamado
	// el contador ya estaría en 1 (la llamada ocurriría ANTES de responder).
	if n := prov.llamadas.Load(); n != 0 {
		t.Errorf("el proveedor fue invocado %d veces sin pública de cifrado: se quemó el LLM del cliente "+
			"para producir una salida que no se puede entregar", n)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 4) Idempotencia EN VUELO (y sólo en vuelo)
// ─────────────────────────────────────────────────────────────────────────────

// TestInferencia_DuplicadoEnVueloSeIgnora: dos frames con el mismo `command_id` mientras el primero sigue
// DENTRO del proveedor ⇒ una sola llamada y una sola respuesta.
//
// LA BARRERA QUE HACE ESTO DETERMINISTA (nada de sleeps). Con `maxInflight=1` y el proveedor colgado, el
// único worker está ocupado, así que:
//
//   - si la idempotencia funciona, el duplicado muere en `marcarEnVuelo` y NO produce respuesta;
//   - si NO funcionara, caería en la rama de «carril lleno» y respondería EDGE_SIN_CAPACIDAD.
//
// Acto seguido se empuja una petición con OTRO command_id, que sí se rechaza por carril lleno. Los tres
// frames se procesan EN ORDEN y de forma SÍNCRONA en el hilo de `Recv` (despachar no espera a nadie), así
// que cuando llega el rechazo del tercero, la respuesta del duplicado —de existir— ya estaría delante en
// el stream. Ver que no está es una afirmación, no una espera.
func TestInferencia_DuplicadoEnVueloSeIgnora(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub, priv, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	prov := nuevoProveedorColgado()
	defer prov.liberar()

	h := nuevoArnesInferencia(t, nil,
		WithCloudEncPubKey(pub), WithServidorInferencia(prov), WithInferenciaMaxInflight(1))
	h.arrancar(t, ctx)

	const cmdID = "cmd-duplicado"
	pushInferencia(t, h, peticion(cmdID, ""))
	esperarEntradaAlProveedor(t, ctx, prov) // el único worker está ocupado y colgado

	pushInferencia(t, h, peticion(cmdID, ""))         // el DUPLICADO
	pushInferencia(t, h, peticion("cmd-barrera", "")) // la BARRERA

	var respuestasDelDuplicado int
barrido:
	for {
		select {
		case msg := <-h.srv.received:
			res := msg.GetInferenceResult()
			switch {
			case res == nil:
			case res.GetCommandId() == cmdID:
				respuestasDelDuplicado++
			case res.GetCommandId() == "cmd-barrera":
				esError(t, res, cloudlinkv1.InferenceError_INFERENCE_ERROR_EDGE_SIN_CAPACIDAD)
				break barrido
			}
		case <-ctx.Done():
			t.Fatalf("timeout esperando la barrera: %v", ctx.Err())
		}
	}

	if respuestasDelDuplicado != 0 {
		t.Errorf("el duplicado EN VUELO produjo %d respuesta(s) antes de la barrera: la idempotencia por "+
			"command_id no está mirando (habría dos respuestas correlacionadas al mismo id)", respuestasDelDuplicado)
	}
	if n := prov.llamadas.Load(); n != 1 {
		t.Errorf("llamadas al proveedor: got %d want 1 (el duplicado quemó CPU del cliente por segunda vez)", n)
	}
	if registro := h.log.String(); !strings.Contains(registro, "DUPLICADO en vuelo") {
		t.Errorf("el log no deja rastro del duplicado ignorado; un duplicado que no responde y no loguea es "+
			"indistinguible de un frame perdido.\n    log=%s", registro)
	}

	// Y la primera, la legítima, termina normal.
	prov.liberar()
	res := esperarResultado(t, ctx, h, cmdID)
	if got := abrirSalida(t, priv, res); got != salidaDelModelo {
		t.Errorf("raw_json de la petición original: got %q want %q", got, salidaDelModelo)
	}
	if n := prov.llamadas.Load(); n != 1 {
		t.Errorf("llamadas al proveedor al final: got %d want 1", n)
	}
}

// TestInferencia_MismoCommandIDTrasTerminarSeVuelveAServir es la OTRA MITAD, y es la que explica por qué
// el registro se llama `enVuelo` y no `visto`: repetir un `inference_request` YA RESPONDIDO es, casi
// siempre, que el Cloud no recibió la respuesta y la vuelve a pedir — y servirla otra vez es exactamente
// lo que hace falta. Una idempotencia «para siempre» (la de DiagnosticsRequest, a.diagSeen) dejaría al
// Cloud sin respuesta posible tras cualquier corte del stream.
func TestInferencia_MismoCommandIDTrasTerminarSeVuelveAServir(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub, priv, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	prov := nuevoProveedor()
	h := nuevoArnesInferencia(t, nil, WithCloudEncPubKey(pub), WithServidorInferencia(prov))
	h.arrancar(t, ctx)

	const cmdID = "cmd-reintento-del-cloud"

	pushInferencia(t, h, peticion(cmdID, ""))
	primera := esperarResultado(t, ctx, h, cmdID)
	if got := abrirSalida(t, priv, primera); got != salidaDelModelo {
		t.Fatalf("primera respuesta: got %q want %q", got, salidaDelModelo)
	}

	// El Cloud perdió la respuesta y la vuelve a pedir con el MISMO command_id.
	pushInferencia(t, h, peticion(cmdID, ""))
	segunda := esperarResultado(t, ctx, h, cmdID)
	if got := abrirSalida(t, priv, segunda); got != salidaDelModelo {
		t.Errorf("el reintento NO se sirvió: got %q want %q (¿la idempotencia pasó a ser «visto para "+
			"siempre»? entonces el Cloud no puede recuperarse de un corte)", got, salidaDelModelo)
	}
	if n := prov.llamadas.Load(); n != 2 {
		t.Errorf("llamadas al proveedor: got %d want 2 (una por petición: el reintento se sirve de verdad, "+
			"no se responde con una caché)", n)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 5) El carril lleno
// ─────────────────────────────────────────────────────────────────────────────

// TestInferencia_CarrilLlenoRechazaSinCapacidadDeInmediato: con `maxInflight=1` y el proveedor colgado, la
// segunda petición vuelve rechazada EN EL ACTO mientras la primera sigue dentro.
//
// EL TIEMPO ES LA MITAD DEL ENUNCIADO, no un adorno. El código correcto es el `select` con `default` de
// `despachar`: se rechaza sin esperar a nadie porque esa función corre EN EL HILO DE `Recv`, el único
// stream del Edge, y cualquier espera ahí es el head-of-line que los Planes 027 y 050 mataron. Un carril
// que encolara con buffer, o que esperase al worker, daría el MISMO código de error y pasaría un test que
// sólo mirase el código — mientras congela la recepción de todas las sesiones durante lo que dure una
// inferencia (hasta 120 s).
func TestInferencia_CarrilLlenoRechazaSinCapacidadDeInmediato(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub, _, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	prov := nuevoProveedorColgado()
	defer prov.liberar()

	h := nuevoArnesInferencia(t, nil,
		WithCloudEncPubKey(pub), WithServidorInferencia(prov), WithInferenciaMaxInflight(1))
	h.arrancar(t, ctx)

	pushInferencia(t, h, peticion("cmd-ocupa-la-plaza", ""))
	esperarEntradaAlProveedor(t, ctx, prov)

	inicio := time.Now()
	pushInferencia(t, h, peticion("cmd-rechazada", ""))
	res := esperarResultado(t, ctx, h, "cmd-rechazada")
	transcurrido := time.Since(inicio)

	esError(t, res, cloudlinkv1.InferenceError_INFERENCE_ERROR_EDGE_SIN_CAPACIDAD)

	// «De inmediato» = sin esperar a que se libere la plaza. La primera sigue colgada (nadie ha llamado a
	// liberar), así que cualquier espera al worker habría agotado el contexto de 10 s; el umbral de 1 s es
	// tres órdenes de magnitud por debajo de una inferencia real (2-36 s medidos) y muy por encima de lo
	// que tarda un bufconn, así que no es un test de reloj de pared disfrazado.
	if transcurrido > time.Second {
		t.Errorf("el rechazo tardó %v: `despachar` está ESPERANDO en el hilo del stream en vez de rechazar "+
			"con el `default` del select", transcurrido)
	}
	// Y la primera sigue dentro: el rechazo no la desalojó ni la contaminó.
	if n := prov.llamadas.Load(); n != 1 {
		t.Errorf("llamadas al proveedor: got %d want 1 (la rechazada no debe llegar al modelo)", n)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 6) El gate de lease (ADR-0007) y su gracia
// ─────────────────────────────────────────────────────────────────────────────

// TestInferenciaLease_SinValidator_ElGateEstaDesactivadoNoRevocado: con una sesión registrada pero sin
// ValidatorFactory, `algunaSesionOperable` devuelve hayGate=false y la inferencia SE SIRVE.
//
// Es deliberado y es la diferencia entre «no revocado» y «aún no existe»: un Edge recién arrancado, o uno
// que corre sin clave pública de lease, no está bajo el kill-switch — está arrancando. El mismo criterio
// que `validator == nil` en handleSendText.
func TestInferenciaLease_SinValidator_ElGateEstaDesactivadoNoRevocado(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub, _, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	prov := nuevoProveedor()
	h := nuevoArnesInferencia(t, nil, // nil = sin ValidatorFactory
		WithCloudEncPubKey(pub), WithServidorInferencia(prov),
		// Gracia minúscula: si el gate bloqueara, el test no se comería dos segundos por nada.
		WithInferenciaLeaseGracia(50*time.Millisecond))
	h.adapter.Register(sesionInferencia, "", enviarNada, nil, func() bool { return true })
	h.arrancar(t, ctx)

	pushInferencia(t, h, peticion("cmd-sin-validator", sesionInferencia))

	res := esperarResultado(t, ctx, h, "cmd-sin-validator")
	if len(res.GetEncOutput()) == 0 {
		t.Fatalf("sin Validator la inferencia debe servirse (gate desactivado), y volvió con motivo=%v",
			res.GetError())
	}
	if n := h.adapter.errLeaseInvalido.Load(); n != 0 {
		t.Errorf("el contador de LEASE_INVALID subió a %d sin ningún Validator en juego", n)
	}
}

// TestInferenciaLease_Revocado_RespondeLeaseInvalidTrasLaGracia es EL KILL-SWITCH DEL ADR-0007 aplicado a
// la inferencia: servir inferencia es OPERAR, así que un Edge con su lease revocado —un clon del disco—
// no puede seguir quemando el LLM del dueño legítimo.
//
// EL TEST ES UN A/B, porque sólo el contraste prueba que el gate mira: con el lease VIGENTE la misma
// petición se sirve; tras la revocación, la siguiente muere con LEASE_INVALID. Un test que sólo probara
// el segundo tramo pasaría también con un Validator recién nacido (que ya dice CanOperate=false por
// «nunca aplicado») y no habría demostrado nada sobre la revocación.
//
// LA SINCRONIZACIÓN NO ES UN SLEEP. El LeaseUpdate viaja por el dispatcher-por-sesión, que es SERIAL
// dentro de cada session_id; así que un SendText empujado DESPUÉS por la misma sesión sólo se atiende
// cuando el lease ya se aplicó, y su Ack es la prueba ordenada de que así fue.
func TestInferenciaLease_Revocado_RespondeLeaseInvalidTrasLaGracia(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pubLease, privLease, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	issuer, err := lease.NewIssuer(privLease)
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	pubEnc, _, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}

	const gracia = 150 * time.Millisecond
	prov := nuevoProveedor()
	h := nuevoArnesInferencia(t, func() *lease.Validator { return lease.NewValidator(pubLease) },
		WithCloudEncPubKey(pubEnc), WithServidorInferencia(prov), WithInferenciaLeaseGracia(gracia))

	enviados := make(chan sendCall, 8)
	h.adapter.Register(sesionInferencia, "", func(_ context.Context, commandID, to, text string) error {
		enviados <- sendCall{commandID: commandID, to: to, text: text}
		return nil
	}, nil, func() bool { return true })
	h.arrancar(t, ctx)

	// (a) Lease VIGENTE. La barrera confirma que se aplicó antes de seguir.
	luOK, err := issuer.Issue("edge-inferencia", "tenant-1", time.Hour, 1)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
		CommandId: "cmd-lease-ok", SessionId: sesionInferencia,
		Payload: &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luOK},
	})
	pushSendText(t, h, sesionInferencia, "cmd-barrera-vigente")
	if ack := recvAck(t, ctx, h.srv, "cmd-barrera-vigente"); !ack.GetOk() {
		t.Fatalf("la barrera dice que el lease NO quedó vigente (err=%q): el resto del test no probaría el "+
			"kill-switch, sino un Validator recién nacido", ack.GetError())
	}
	<-enviados

	pushInferencia(t, h, peticion("cmd-con-lease", sesionInferencia))
	if res := esperarResultado(t, ctx, h, "cmd-con-lease"); len(res.GetEncOutput()) == 0 {
		t.Fatalf("con el lease VIGENTE la inferencia debía servirse, y volvió con motivo=%v", res.GetError())
	}

	// (b) REVOCADO. Misma barrera: el Ack negativo prueba que la revocación ya se aplicó.
	luRevoke, err := issuer.Revoke("edge-inferencia", "tenant-1")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
		CommandId: "cmd-lease-revoke", SessionId: sesionInferencia,
		Payload: &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luRevoke},
	})
	pushSendText(t, h, sesionInferencia, "cmd-barrera-revocado")
	if ack := recvAck(t, ctx, h.srv, "cmd-barrera-revocado"); ack.GetOk() {
		t.Fatalf("la barrera dice que la revocación NO se aplicó: el test siguiente no probaría nada")
	}

	llamadasAntes := prov.llamadas.Load()
	inicio := time.Now()
	pushInferencia(t, h, peticion("cmd-revocado", sesionInferencia))
	res := esperarResultado(t, ctx, h, "cmd-revocado")
	transcurrido := time.Since(inicio)

	esError(t, res, cloudlinkv1.InferenceError_INFERENCE_ERROR_LEASE_INVALID)
	if transcurrido < gracia {
		t.Errorf("el rechazo llegó en %v, antes de agotar la gracia de %v: el gate dejó de dar la ventana en "+
			"la que el Validator nace cerrado, y toda inferencia del primer segundo tras arrancar moriría "+
			"con el error que dice «kill-switch»", transcurrido, gracia)
	}
	if n := prov.llamadas.Load(); n != llamadasAntes {
		t.Errorf("el proveedor fue invocado %d vez/veces con el lease revocado: el clon sigue quemando el "+
			"LLM del dueño legítimo", n-llamadasAntes)
	}
	if n := h.adapter.errLeaseInvalido.Load(); n != 1 {
		t.Errorf("el contador de LEASE_INVALID (INV-051.3: por separado): got %d want 1", n)
	}
}

// TestInferenciaLease_ModoSombra_SirveIgualYAvisa: con D-055.4 encendido el gate REGISTRA lo que habría
// bloqueado y deja pasar. Se honra aquí igual que en handleSendText/handleSendMedia — un gate que bloquea
// en un camino y no en otro no es un modo sombra, es una inconsistencia que invalidaría las 72 h de campo
// del Plan 055.
func TestInferenciaLease_ModoSombra_SirveIgualYAvisa(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pubLease, privLease, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	issuer, err := lease.NewIssuer(privLease)
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	pubEnc, _, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}

	prov := nuevoProveedor()
	h := nuevoArnesInferencia(t, func() *lease.Validator { return lease.NewValidator(pubLease) },
		WithCloudEncPubKey(pubEnc), WithServidorInferencia(prov),
		WithInferenciaLeaseGracia(50*time.Millisecond), WithLeaseShadowMode(true))
	h.adapter.Register(sesionInferencia, "", enviarNada, nil, func() bool { return true })
	h.arrancar(t, ctx)

	luRevoke, err := issuer.Revoke("edge-inferencia", "tenant-1")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
		CommandId: "cmd-lease-revoke", SessionId: sesionInferencia,
		Payload: &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luRevoke},
	})

	pushInferencia(t, h, peticion("cmd-sombra", sesionInferencia))
	res := esperarResultado(t, ctx, h, "cmd-sombra")
	if len(res.GetEncOutput()) == 0 {
		t.Fatalf("el modo SOMBRA debió servir la inferencia pese al lease no vigente; motivo=%v", res.GetError())
	}
	// 🔴 SE ANCLA LA FRASE EXACTA, Y LA FRASE ES LA MISMA QUE LA DE handleSendText/handleSendMedia
	// —«HABRÍA sido bloqueado», en masculino— aunque el sujeto de esta línea sea una inferencia. La
	// concordancia rara está puesta a propósito y este test es lo que la sostiene: las 72 h de campo del
	// modo sombra (D-055.4, Plan 055) se auditan GREPEANDO el log, así que un operador que busque la frase
	// de los envíos tiene que encontrar también las inferencias. Si alguien «arregla» la gramática, aquí
	// se entera de por qué no debía.
	const fraseCompartida = "HABRÍA sido bloqueado por lease no vigente — MODO SOMBRA"
	if registro := h.log.String(); !strings.Contains(registro, fraseCompartida) {
		t.Errorf("el modo sombra sirvió pero NO avisó: sin ese WARN, las 72 h de campo no medirían nada.\n"+
			"    log=%s", registro)
	}
}

// TestInferenciaLease_LaGraciaSirveAlLeaseQueLlegaTarde protege LA VENTANA MEDIDA de 0,5-1,1 s en la que
// el Validator nace CERRADO: `Register` lo construye y dice CanOperate=false hasta que llega el primer
// LeaseUpdate. Sin gracia, toda inferencia de ese primer segundo moriría con LEASE_INVALID — el error más
// alarmante del vocabulario (dice «kill-switch», no «espera un momento») y el que el Cloud degrada peor.
//
// CÓMO SE FUERZA EL ORDEN SIN MIRAR EL RELOJ: el gate llama a `hasDEK` en CADA sondeo (algunaSesionOperable
// lo evalúa para el CanOperate 2-de-2). Registrando una sesión cuyo hasDEK avisa por canal, el test sabe
// (1.ª llamada) que el gate ya comprobó y falló, y (2.ª llamada) que está dentro del bucle de gracia. Sólo
// ENTONCES se manda el lease bueno. Si la gracia no existiera, el rechazo ya habría salido.
func TestInferenciaLease_LaGraciaSirveAlLeaseQueLlegaTarde(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pubLease, privLease, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	issuer, err := lease.NewIssuer(privLease)
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	pubEnc, privEnc, err := envelope.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}

	prov := nuevoProveedor()
	// Gracia holgada: el test la corta él mismo mandando el lease, no la espera. Lo que se mide es que el
	// gate REINTENTE dentro de la ventana, no cuánto dura.
	h := nuevoArnesInferencia(t, func() *lease.Validator { return lease.NewValidator(pubLease) },
		WithCloudEncPubKey(pubEnc), WithServidorInferencia(prov), WithInferenciaLeaseGracia(10*time.Second))

	sondeos := make(chan struct{}, 256)
	h.adapter.Register(sesionInferencia, "", enviarNada, nil, func() bool {
		select {
		case sondeos <- struct{}{}:
		default:
		}
		return true
	})
	h.arrancar(t, ctx)

	pushInferencia(t, h, peticion("cmd-gracia", sesionInferencia))

	// 1.ª: el gate comprobó y falló (el Validator nace cerrado). 2.ª: está sondeando dentro de la gracia.
	for i := 1; i <= 2; i++ {
		select {
		case <-sondeos:
		case <-ctx.Done():
			t.Fatalf("timeout esperando el sondeo nº%d del gate de lease: %v", i, ctx.Err())
		}
	}

	// AHORA llega el lease, como llega en producción: medio segundo tarde.
	luOK, err := issuer.Issue("edge-inferencia", "tenant-1", time.Hour, 1)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	pushCloud(t, h.stream, &cloudlinkv1.CloudToEdge{
		CommandId: "cmd-lease-tardio", SessionId: sesionInferencia,
		Payload: &cloudlinkv1.CloudToEdge_LeaseUpdate{LeaseUpdate: luOK},
	})

	res := esperarResultado(t, ctx, h, "cmd-gracia")
	if len(res.GetEncOutput()) == 0 {
		t.Fatalf("la inferencia se rechazó (motivo=%v) pese a que el lease se volvió operable DENTRO de la "+
			"gracia: cada arranque del Edge tendría un segundo de LEASE_INVALID falsos", res.GetError())
	}
	if got := abrirSalida(t, privEnc, res); got != salidaDelModelo {
		t.Errorf("raw_json: got %q want %q", got, salidaDelModelo)
	}
	if n := h.adapter.errLeaseInvalido.Load(); n != 0 {
		t.Errorf("el contador de LEASE_INVALID subió a %d en un caso que acabó sirviéndose", n)
	}
	if registro := h.log.String(); !strings.Contains(registro, "se volvió operable dentro de la gracia") {
		t.Errorf("no hay rastro en el log de que la espera ocurriera; sin él, un operador no puede "+
			"distinguir esta ventana de un Edge lento.\n    log=%s", registro)
	}
}
