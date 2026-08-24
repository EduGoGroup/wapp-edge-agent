package cloudlink

// inferencia.go — EL CARRIL PROPIO DE LA INFERENCIA (Plan 044 · Ola 1.6 · T1.6-2, ADR-0045 §2, REQ-34).
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 POR QUÉ UN CARRIL APARTE Y NO EL DISPATCHER-POR-SESIÓN QUE YA HABÍA
// ─────────────────────────────────────────────────────────────────────────────
// `Recv` desvía los `inference_request` AQUÍ, ANTES de `disp.dispatch(c2e)`. Tres razones, y las dos
// primeras bastan por sí solas:
//
//  1. EL DEADLINE NO CABE. El dispatcher aplica `defaultCommandTimeout` = 30 s por operación (Plan 027
//     T1, H7). Una inferencia legítima de esta máquina llega a 36 s medidos, y el techo del Edge son
//     120 s: pasar por ahí abortaría las inferencias largas a los 30 s y el Cloud leería un TIMEOUT que
//     no es del modelo sino del transporte. Subir aquel deadline a 120 s no es opción: es el que impide
//     que un SendMedia colgado viva lo que vive el stream.
//  2. REINTRODUCIRÍA EL HEAD-OF-LINE QUE LOS PLANES 027 Y 050 MATARON. El dispatcher es SERIAL DENTRO DE
//     CADA session_id, y el `session_id` de un `inference_request` viene NORMALMENTE VACÍO por contrato
//     — o sea que TODAS las inferencias caerían en la MISMA cola (la de la sesión ""), en fila india, y
//     además compartirían esa cola con cualquier otro comando que llegara sin sesión. Una inferencia de
//     36 s bloquearía a las siguientes durante 36 s antes siquiera de pedir plaza.
//  3. LA CONCURRENCIA CORRECTA ES OTRA. Al dispatcher le importa el ORDEN por sesión; a la inferencia no
//     le importa el orden en absoluto (cada una se correlaciona por su `command_id`) y lo que sí le
//     importa es cuántas caben a la vez, que es una propiedad de la MÁQUINA. Son dos políticas
//     distintas sobre dos ejes distintos.
//
// EL FLUJO, y dónde ocurre cada paso:
//
//	Recv (adapter.go) ──inference_request──► despachar()          [hilo del stream: barato o nada]
//	   ├─ ① set de EN VUELO por command_id ─── duplicado ⇒ se ignora con log
//	   └─ ② envío NO BLOQUEANTE al carril ──── sin worker libre ⇒ EDGE_SIN_CAPACIDAD
//	                                             │
//	   worker (K goroutines) ────────────────────┘
//	   ├─ ③ gate de LEASE, con gracia ──────────────────────────► LEASE_INVALID
//	   ├─ ④ app.ServidorInferencia.Inferir() ── socket unix ────► los otros cuatro errores
//	   └─ ⑤ SealFor(cloud_enc_pubkey) y respuesta por currentClient()
//
// 🔴 INV-051.1: el prompt y la salida NO salen por ningún log de este fichero. Sólo `command_id`,
// tamaños y desenlace. INV-051.3: los desenlaces se cuentan por separado (ver carrilInferencia).

import (
	"context"
	"sync"
	"time"

	"github.com/EduGoGroup/wapp-cloudlink/client"
	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-shared/envelope"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// Defaults del carril.
const (
	// defaultInferenceMaxInflight son las inferencias que el Edge acepta tener EN VUELO a la vez, y a la
	// vez el número de workers del carril (WAPP_AGENT_INFERENCE_MAX_INFLIGHT).
	//
	// POR QUÉ 4 Y NO 1, teniendo el aforo del cajero UNA sola plaza: los dos números miden cosas
	// distintas y el segundo no debe copiar al primero. El aforo protege a Ollama (una inferencia a la
	// vez, cerrado por la O0); esto es cuántas peticiones el Edge acepta TENER ABIERTAS, incluidas las que
	// están haciendo cola en el aforo con su plazo corriendo. Con 1, la segunda petición de cualquier
	// ráfaga se rechazaría al instante aunque hubiera plazo de sobra para atenderla en cuanto la primera
	// terminara; con 4, tres pueden esperar su turno y sólo se van con EDGE_SIN_CAPACIDAD si su propio
	// plazo se agota — que es la señal honesta y la que el dueño del equipo necesita ver.
	//
	// El tope es DURO sobre lo que IMPORTA —cuántas peticiones están siendo atendidas—: el canal no tiene
	// buffer, así que nunca hay más de K dentro de un worker (ver despachar). El mapa `enVuelo` sí puede
	// tener un instante más entradas que K, porque se marca ANTES de intentar el envío y se desmarca en el
	// `default`; no es una fuga, es que el mapa mide idempotencia y el canal mide capacidad.
	defaultInferenceMaxInflight = 4

	// defaultInferenceLeaseGracia es cuánto se espera a que el lease de alguna sesión se vuelva operable
	// antes de rechazar con LEASE_INVALID (WAPP_AGENT_INFERENCE_LEASE_GRACIA_MS).
	//
	// 🔴 EXISTE POR UNA VENTANA MEDIDA: EL VALIDATOR NACE CERRADO. `Register` construye el Validator de
	// una sesión (adapter.go) y ese Validator dice `CanOperate == false` hasta que llega el primer
	// LeaseUpdate del Cloud — entre 0,5 y 1,1 s después de registrar la sesión. Sin gracia, TODA
	// inferencia que cayera en esa ventana moriría con LEASE_INVALID, que es el error más alarmante del
	// vocabulario (dice «kill-switch», no «espera un momento») y el que el Cloud degrada peor.
	//
	// 2000 ms es ~2× el peor extremo de esa ventana. Se paga SÓLO cuando el gate iba a rechazar: si hay
	// lease vigente, la primera comprobación acierta y no se espera nada.
	defaultInferenceLeaseGracia = 2000 * time.Millisecond

	// sondeoLease es cada cuánto se re-pregunta durante la gracia. 50 ms es dos órdenes de magnitud por
	// debajo de la ventana que cubre y tres por debajo de una inferencia: el coste de sondear es nulo
	// comparado con lo que viene después.
	sondeoLease = 50 * time.Millisecond
)

// carrilInferencia atiende los `inference_request` de UN stream. Lo crea runOnce junto al dispatcher y
// muere con él.
type carrilInferencia struct {
	a  *Adapter
	cl *client.Client // stream con el que se ANCLA; para RESPONDER se usa a.currentClient() (ver responder)

	// cola NO TIENE BUFFER, y esa es la forma de que `maxInflight` diga la verdad exacta. Con K workers
	// esperando en el `<-cola`, un envío no bloqueante entra si y sólo si hay un worker libre, así que
	// «en vuelo» nunca pasa de K. Con buffer, el tope observable sería K+buffer y el nombre del número
	// mentiría.
	//
	// El precio es una carrera benigna: un worker que acaba de terminar y aún no ha vuelto al select
	// rechaza una petición que en realidad cabía. Con el aforo en 1, esa petición habría hecho cola de
	// todas formas, así que lo que se pierde es despreciable — y el sesgo es el conservador.
	cola chan *cloudlinkv1.CloudToEdge

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// enVuelo son los command_id que se están atendiendo AHORA. Es la idempotencia de este frame.
	//
	// 🔴 «EN VUELO» Y NO «VISTO», al revés que la idempotencia de DiagnosticsRequest (a.diagSeen, que
	// recuerda para siempre dentro de una ventana acotada). La diferencia es qué significa un duplicado
	// en cada caso: repetir un DiagnosticsRequest ya atendido es un reenvío del transporte y rearmar el
	// bundle sería trabajo tirado; repetir un `inference_request` YA RESPONDIDO es, casi siempre, que el
	// Cloud no recibió la respuesta y la vuelve a pedir — y ahí servirla otra vez es exactamente lo que
	// hace falta. Lo que no tiene sentido es servir DOS VECES A LA VEZ el mismo command_id: quema el doble
	// de CPU para producir dos respuestas correlacionadas al mismo id, de las que el Cloud usará una.
	mu      sync.Mutex
	enVuelo map[string]struct{}
}

// nuevoCarrilInferencia construye el carril y arranca sus workers. `maxInflight <= 0` cae al default.
func nuevoCarrilInferencia(baseCtx context.Context, a *Adapter, cl *client.Client, maxInflight int) *carrilInferencia {
	if maxInflight <= 0 {
		maxInflight = defaultInferenceMaxInflight
	}
	ctx, cancel := context.WithCancel(baseCtx)
	c := &carrilInferencia{
		a:       a,
		cl:      cl,
		cola:    make(chan *cloudlinkv1.CloudToEdge),
		ctx:     ctx,
		cancel:  cancel,
		enVuelo: make(map[string]struct{}),
	}
	for i := 0; i < maxInflight; i++ {
		c.wg.Add(1)
		go c.worker()
	}
	return c
}

// despachar encola una petición. LO LLAMA EL LOOP DE `Recv`, así que todo lo que hay aquí tiene que ser
// barato o no estar: cualquier espera en esta función frena la recepción del ÚNICO stream del Edge, que
// es el head-of-line que los Planes 027 y 050 mataron.
//
// Por eso el gate de lease NO está aquí sino en el worker: su gracia puede costar hasta 2 s (ver
// defaultInferenceLeaseGracia), y 2 s parado en el hilo del stream son 2 s sin leer un solo comando de
// ninguna sesión. La consecuencia, dicha para que no sorprenda: una petición que llega con el carril
// LLENO se responde EDGE_SIN_CAPACIDAD aunque el lease tampoco fuera vigente. Es un caso de esquina
// (hace falta saturación y lease inválido a la vez) y el error que se da describe lo primero que impidió
// atenderla, que es información verdadera.
func (c *carrilInferencia) despachar(c2e *cloudlinkv1.CloudToEdge) {
	req := c2e.GetInferenceRequest()
	cmdID := req.GetCommandId()

	// ① Idempotencia por command_id EN VUELO.
	if !c.marcarEnVuelo(cmdID) {
		c.a.log.Info("CloudLink: inference_request DUPLICADO en vuelo (mismo command_id), ignorado",
			"command_id", cmdID, "session_id", c2e.GetSessionId())
		return
	}

	// ② Envío NO BLOQUEANTE: sin worker libre, se rechaza al instante en vez de frenar el stream.
	select {
	case c.cola <- c2e:
	default:
		c.desmarcar(cmdID)
		// 🔴 `max_inflight` SALE DEL CAMPO DEL ADAPTER Y NO DEL CANAL. El canal NO TIENE BUFFER a propósito
		// (ver el campo `cola`), así que `cap(c.cola)` es 0 SIEMPRE: un operador que recibiera
		// EDGE_SIN_CAPACIDAD y mirara esta clave para saber cuántas plazas tiene su Edge leería un cero que
		// no significa nada — y que además se lee como «no hay ninguna», que es una explicación plausible y
		// falsa de por qué le rechazan las peticiones.
		c.a.log.Warn("CloudLink: inference_request RECHAZADO, el carril está lleno "+
			"(el Edge ya atiende el máximo de inferencias simultáneas)",
			"command_id", cmdID, "max_inflight", c.a.inferenciaMaxInflight,
			"prompt_bytes", len(req.GetPrompt()))
		c.responderError(c2e, app.ErrInferenciaSinCapacidad)
	case <-c.ctx.Done():
		c.desmarcar(cmdID)
	}
}

// worker atiende peticiones hasta que el carril se cierra.
func (c *carrilInferencia) worker() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case c2e := <-c.cola:
			c.atender(c2e)
		}
	}
}

// atender sirve UNA petición de principio a fin.
func (c *carrilInferencia) atender(c2e *cloudlinkv1.CloudToEdge) {
	req := c2e.GetInferenceRequest()
	cmdID := req.GetCommandId()
	defer c.desmarcar(cmdID)

	// El recover() aísla un pánico para que no se lleve por delante a un worker del carril —y con él una
	// K-ésima parte de la capacidad de inferencia del Edge, para el resto de la vida del stream—. Es la
	// misma protección que el dispatcher aplica a los comandos por sesión.
	defer func() {
		if r := recover(); r != nil {
			c.a.log.Error("CloudLink: pánico sirviendo una inferencia (aislado)",
				"command_id", cmdID, "panic", r)
			c.responderError(c2e, app.ErrInferenciaOllamaCaido)
		}
	}()

	// ⑤-ANTES: SIN PÚBLICA DE CIFRADO NO SE PUEDE RESPONDER, ASÍ QUE NO SE GASTA CPU EN INTENTARLO.
	//
	// 🔴 ESTE CASO NO TIENE CÓDIGO EN EL CONTRATO, y conviene decirlo en vez de disimularlo. El oneof de
	// InferenceResult sólo admite `enc_output` (sellado) o un `InferenceError`, y los cinco errores
	// describen por qué el Edge no pudo INFERIR — ninguno dice «inferí, pero no puedo sellarte la
	// respuesta». Responder OLLAMA_DOWN o EDGE_SIN_CAPACIDAD sería mentir sobre la causa, que es
	// exactamente lo que el enum existe para impedir; no responder sería colgar al Cloud, que el contrato
	// prohíbe. Se responde UNSPECIFIED —el único valor que no afirma una causa falsa— y se grita en Error.
	//
	// Cuándo pasa: sólo si el enrolamiento no distribuyó `cloud_enc_pubkey` (dev, o un Edge a medio
	// enrolar). En producción la pública está siempre.
	if c.a.cloudEncPub == nil {
		c.a.log.Error("CloudLink: inference_request recibido SIN pública de cifrado de la nube; la salida no se " +
			"podría sellar y el contrato no admite salida en claro. Se responde error sin causa nombrada " +
			"(el enum no tiene un valor para esto) y NO se llama al proveedor")
		c.responder(c2e, &cloudlinkv1.InferenceResult{
			CommandId: cmdID,
			Result:    &cloudlinkv1.InferenceResult_Error{Error: cloudlinkv1.InferenceError_INFERENCE_ERROR_UNSPECIFIED},
		})
		return
	}

	// ③ EL GATE DE LEASE.
	if !c.leaseVigente(cmdID) {
		c.a.errLeaseInvalido.Add(1)
		c.responderError(c2e, app.ErrInferenciaLeaseInvalido)
		return
	}

	// ④ LA INFERENCIA, por el socket del cajero.
	if c.a.inferencia == nil {
		// Sin puerto cableado. No es un estado esperable en producción (el wiring lo construye siempre),
		// pero un nil aquí sería un pánico en el camino caliente.
		c.a.log.Error("CloudLink: inference_request recibido sin servidor de inferencia cableado",
			"command_id", cmdID)
		c.responderError(c2e, app.ErrInferenciaOllamaCaido)
		return
	}

	inicio := time.Now()
	resp, err := c.a.inferencia.Inferir(c.ctx, app.PeticionInferencia{
		CommandID:   cmdID,
		SessionID:   sessionIDDe(c2e),
		Prompt:      req.GetPrompt(),
		Format:      req.GetFormat(),
		Temperature: req.Temperature,
		Timeout:     time.Duration(req.GetTimeoutMs()) * time.Millisecond,
	})
	transcurrido := time.Since(inicio)

	if err != nil {
		canonico, ok := app.EsErrorInferencia(err)
		if !ok {
			c.a.log.Error("CloudLink: la inferencia devolvió un error fuera del vocabulario canónico "+
				"(bug: app.ServidorInferencia promete los cinco)", "command_id", cmdID, "error", err)
			canonico = app.ErrInferenciaOllamaCaido
		}
		// 🔴 INV-051.1: del error se dice que existe; jamás el prompt que lo provocó.
		c.a.log.Warn("CloudLink: inference_request NO servido; sube el error nombrado y el Cloud degrada",
			"command_id", cmdID, "codigo", canonico.Codigo(), "error", err,
			"latencia_ms", transcurrido.Milliseconds(), "prompt_bytes", len(req.GetPrompt()))
		c.responderError(c2e, canonico)
		return
	}

	// ⑤ SELLADO. La salida del modelo puede llevar texto literal del cliente, así que NO viaja en claro
	// (ADR-0020 §5, mismo trato que SensitivePayload en IncomingMessage). En ESTA dirección sí se puede
	// sellar: el Edge tiene la pública de cifrado de la nube.
	salida, err := proto.Marshal(&cloudlinkv1.InferenceOutput{RawJson: resp.RawJSON})
	if err != nil {
		c.a.log.Error("CloudLink: no se pudo serializar el InferenceOutput",
			"command_id", cmdID, "error", err, "salida_bytes", len(resp.RawJSON))
		c.responder(c2e, &cloudlinkv1.InferenceResult{
			CommandId: cmdID,
			Result:    &cloudlinkv1.InferenceResult_Error{Error: cloudlinkv1.InferenceError_INFERENCE_ERROR_UNSPECIFIED},
		})
		return
	}
	sellada, err := envelope.SealFor(c.a.cloudEncPub, salida)
	if err != nil {
		// 🔴 AQUÍ NO HAY FALLBACK CLARO, al revés que en sealSensitive (§10.H). Allí el campo en claro
		// EXISTE en el frame y dejarlo poblado es una degradación legítima; aquí el único campo de salida
		// es `enc_output`, que ES el sobre: mandar el marshal sin sellar produciría bytes que el Cloud
		// intentaría abrir con OpenWith y no podría, o sea un fallo más confuso que este error.
		c.a.log.Error("CloudLink: no se pudo SELLAR la salida de la inferencia; no hay camino en claro para "+
			"este campo, así que se responde error sin causa nombrada",
			"command_id", cmdID, "error", err, "salida_bytes", len(resp.RawJSON))
		c.responder(c2e, &cloudlinkv1.InferenceResult{
			CommandId: cmdID,
			Result:    &cloudlinkv1.InferenceResult_Error{Error: cloudlinkv1.InferenceError_INFERENCE_ERROR_UNSPECIFIED},
		})
		return
	}

	c.a.inferenciasServidas.Add(1)
	c.a.log.Info("CloudLink: inference_request SERVIDO",
		"command_id", cmdID, "latencia_ms", transcurrido.Milliseconds(),
		"prompt_bytes", len(req.GetPrompt()), "salida_bytes", len(resp.RawJSON),
		"sellada_bytes", len(sellada))
	c.responder(c2e, &cloudlinkv1.InferenceResult{
		CommandId: cmdID,
		Result:    &cloudlinkv1.InferenceResult_EncOutput{EncOutput: sellada},
	})
}

// leaseVigente aplica el gate de lease del ADR-0007 a una inferencia. Devuelve si se puede servir.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 SE GATEA CONTRA *CUALQUIER* SESIÓN OPERABLE, NO CONTRA LA DEL FRAME
// ─────────────────────────────────────────────────────────────────────────────
// El `session_id` de un `inference_request` VIENE NORMALMENTE VACÍO por contrato: el servicio de
// inferencia es del EDGE —un proceso, un Ollama—, no de una sesión de WhatsApp. Buscar el Validator de
// esa sesión daría nil el 99 % de las veces y el gate quedaría desactivado justo donde hace falta, o
// —peor— rechazaría todo por «sesión desconocida».
//
// El alcance correcto es el mismo que el de DiagnosticsRequest: DAEMON. Y la pregunta correcta es «¿este
// Edge tiene AUTORIZACIÓN PARA OPERAR ahora mismo?», que se responde con que AL MENOS UNA sesión tenga
// su lease vigente. Servir inferencia es OPERAR (ADR-0045: «respeta el lease como cualquier orden»), así
// que un Edge con TODOS sus leases revocados —el kill-switch anti-clon en acción— no puede servir
// ninguna: si pudiera, un clon del disco seguiría quemando el LLM del dueño legítimo.
//
// SIN NINGUNA SESIÓN REGISTRADA se sirve, y es deliberado: un Edge recién arrancado que todavía no ha
// restaurado sus sesiones no está revocado, está arrancando. El gate de lease bloquea lo REVOCADO, no lo
// que aún no existe — mismo criterio que `validator == nil` (gate desactivado) en handleSendText.
//
// 🔴 LA GRACIA. `Register` crea el Validator CERRADO: dice `CanOperate == false` hasta que llega el
// primer LeaseUpdate, 0,5–1,1 s después. Sin gracia, cada arranque tendría una ventana de un segundo en
// la que toda inferencia moriría con LEASE_INVALID — el error que dice «kill-switch», no «espera». Se
// sondea cada `sondeoLease` hasta `graciaLease` antes de rechazar.
func (c *carrilInferencia) leaseVigente(cmdID string) bool {
	inicio := time.Now()
	gracia := c.a.inferenciaLeaseGracia
	if gracia <= 0 {
		gracia = defaultInferenceLeaseGracia
	}

	for {
		hayGate, operable := c.a.algunaSesionOperable()
		if !hayGate || operable {
			if espera := time.Since(inicio); espera > 0 && hayGate {
				c.a.log.Info("CloudLink: el lease se volvió operable dentro de la gracia; se sirve la inferencia",
					"command_id", cmdID, "esperado_ms", espera.Milliseconds())
			}
			return true
		}
		if time.Since(inicio) >= gracia {
			break
		}
		select {
		case <-time.After(sondeoLease):
		case <-c.ctx.Done():
			return false
		}
	}

	// MODO SOMBRA (D-055.4, Plan 055): con él encendido el gate REGISTRA lo que habría bloqueado y deja
	// pasar. Se honra aquí igual que en handleSendText/handleSendMedia — un gate que bloquea en un camino
	// y no en otro no es un modo sombra, es una inconsistencia que invalidaría las 72 h de campo.
	if c.a.leaseShadowMode {
		// 🔴 LA FRASE ES «HABRÍA sido bloqueado» EN MASCULINO, IGUAL QUE EN handleSendText/handleSendMedia,
		// y la concordancia rara con «la inferencia» es deliberada. Las 72 h de campo del modo sombra
		// (D-055.4, Plan 055) se auditan GREPEANDO el log: si esta línea dijera «bloqueada», quien busque la
		// frase de los envíos no encontraría ni una sola inferencia y concluiría que el gate nunca las tocó.
		c.a.log.Warn("CloudLink: inference_request HABRÍA sido bloqueado por lease no vigente — MODO SOMBRA, se sirve",
			"command_id", cmdID, "gracia_ms", gracia.Milliseconds())
		return true
	}

	c.a.log.Warn("CloudLink: inference_request BLOQUEADO por lease no vigente en TODAS las sesiones (kill-switch); "+
		"servir inferencia es operar (ADR-0007)",
		"command_id", cmdID, "gracia_ms", gracia.Milliseconds())
	return false
}

// responderError sube un InferenceResult con el error nombrado, traduciendo el vocabulario de `app` al
// enum del contrato.
func (c *carrilInferencia) responderError(c2e *cloudlinkv1.CloudToEdge, e *app.ErrorInferencia) {
	c.responder(c2e, &cloudlinkv1.InferenceResult{
		CommandId: c2e.GetInferenceRequest().GetCommandId(),
		Result:    &cloudlinkv1.InferenceResult_Error{Error: aProtoInferenceError(e)},
	})
}

// aProtoInferenceError traduce un error canónico de `app` al enum del contrato.
//
// 🔴 EL `default` NO ES DECORATIVO Y NO PUEDE SER OTRO VALOR. Si alguien añade un sexto error a
// app.ErroresInferencia y olvida esta función, todos los casos nuevos saldrían por aquí. Devolver
// UNSPECIFIED hace ese olvido VISIBLE arriba (el Cloud recibe un motivo que no sabe mapear y lo dice);
// devolver OLLAMA_DOWN lo escondería como un diagnóstico plausible y falso. Además hay un test que
// recorre app.ErroresInferencia y exige que ninguno caiga en el default, así que el olvido se caza antes
// de llegar a campo.
func aProtoInferenceError(e *app.ErrorInferencia) cloudlinkv1.InferenceError {
	switch e {
	case app.ErrInferenciaOllamaCaido:
		return cloudlinkv1.InferenceError_INFERENCE_ERROR_OLLAMA_DOWN
	case app.ErrInferenciaBreakerAbierto:
		return cloudlinkv1.InferenceError_INFERENCE_ERROR_BREAKER_OPEN
	case app.ErrInferenciaTimeout:
		return cloudlinkv1.InferenceError_INFERENCE_ERROR_TIMEOUT
	case app.ErrInferenciaLeaseInvalido:
		return cloudlinkv1.InferenceError_INFERENCE_ERROR_LEASE_INVALID
	case app.ErrInferenciaSinCapacidad:
		return cloudlinkv1.InferenceError_INFERENCE_ERROR_EDGE_SIN_CAPACIDAD
	default:
		return cloudlinkv1.InferenceError_INFERENCE_ERROR_UNSPECIFIED
	}
}

// responder sube el InferenceResult por el stream.
//
// 🔴 USA a.currentClient() Y NO EL `cl` CON EL QUE NACIÓ EL CARRIL. Una inferencia puede tardar decenas
// de segundos, y en ese tiempo el stream puede haberse caído y reconectado: `cl` sería entonces un
// stream MUERTO y el Send fallaría en silencio (a.send sólo loguea un Warn), dejando al Cloud esperando
// una respuesta que ya se escribió en el vacío. `currentClient()` devuelve el stream VIVO, que es el
// único por el que la respuesta puede llegar.
//
// Sin stream vivo (reconectando) la respuesta se DESCARTA con un Warn, y no se encola en el outbox: una
// inferencia es válida sólo dentro de la ventana del Cloud que la pidió, así que entregarla al reconectar
// sería entregar algo caducado a un correlacionador que ya la dio por perdida. Es el mismo criterio de
// «la durabilidad aquí es un anti-feature» del canal con el cajero.
//
// 🔴 NO SE MANDA Ack. El InferenceResult ES la respuesta correlacionada por `command_id`, así que un Ack
// además sería un segundo acuse del mismo hecho — y, peor, el Cloud podría tomarlo por la confirmación y
// dejar de esperar el resultado. Es la decisión que el commit `db2fce3` del contrato dejó explícitamente
// a esta tarea al retirar la mención al Ack del comentario de `InferenceRequest.command_id`.
func (c *carrilInferencia) responder(c2e *cloudlinkv1.CloudToEdge, res *cloudlinkv1.InferenceResult) {
	cl := c.a.currentClient()
	if cl == nil {
		c.a.log.Warn("CloudLink: sin stream vivo para responder la inferencia; se descarta "+
			"(una inferencia caducada no se reenvía al reconectar)",
			"command_id", res.GetCommandId())
		return
	}
	c.a.send(cl, &cloudlinkv1.EdgeToCloud{
		CommandId: uuid.NewString(),
		SessionId: sessionIDDe(c2e),
		Payload:   &cloudlinkv1.EdgeToCloud_InferenceResult{InferenceResult: res},
	})
}

// sessionIDDe resuelve la sesión de trazabilidad de un inference_request.
//
// 🔴 HAY DOS CAMPOS CON ESE NOMBRE Y NO SON EL MISMO: el del SOBRE (`CloudToEdge.session_id`, el que usa
// el demux para enrutar) y el del REQUEST (`InferenceRequest.session_id`, el que el .proto documenta como
// «trazabilidad de qué conversación originó la pregunta cuando el Cloud lo sabe»). Este frame es de
// alcance DAEMON, así que el del sobre normalmente viene vacío y el útil es el de dentro.
//
// SE PREFIERE EL DEL REQUEST Y SE CAE AL DEL SOBRE. No es indecisión: el frame hermano de alcance daemon
// —DiagnosticsRequest— lee los DOS de su mensaje interno, y leer aquí sólo el del sobre haría que un
// Cloud que pueble el campo documentado viera su trazabilidad desaparecer en los logs del Edge y en la
// respuesta, sin un solo error. Aceptar los dos es lo único que no depende de adivinar cuál eligió el
// emisor. NADIE decide nada con este valor —el gate de lease es de alcance daemon y no lo mira (ver
// leaseVigente)—: es traza, y por eso el fallback es seguro.
func sessionIDDe(c2e *cloudlinkv1.CloudToEdge) string {
	if sid := c2e.GetInferenceRequest().GetSessionId(); sid != "" {
		return sid
	}
	return c2e.GetSessionId()
}

// marcarEnVuelo registra el command_id si no estaba ya. Devuelve false para un duplicado.
func (c *carrilInferencia) marcarEnVuelo(cmdID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.enVuelo[cmdID]; ok {
		return false
	}
	c.enVuelo[cmdID] = struct{}{}
	return true
}

// desmarcar libera un command_id.
func (c *carrilInferencia) desmarcar(cmdID string) {
	c.mu.Lock()
	delete(c.enVuelo, cmdID)
	c.mu.Unlock()
}

// shutdown cierra el carril: cancela su ctx (cortando las inferencias en vuelo, que así retornan pronto
// en vez de esperar su plazo completo) y espera a que TODOS los workers terminen. Sin goroutines
// fugadas. Idempotente (context.CancelFunc lo es).
func (c *carrilInferencia) shutdown() {
	c.cancel()
	c.wg.Wait()
}
