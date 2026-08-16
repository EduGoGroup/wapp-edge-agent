// Package whatsmeow — listener de RECEPCIÓN 24/7 (RF-5/RF-6, design §5). Código NUEVO (no existe en
// EduGo, que deshabilitó la escucha): se construye desde cero sobre client.AddEventHandler.
//
// El Listener registra UN handler en el cliente y ENRUTA por tipo de evento:
//   - *events.Message      -> construye un domain.InboundEvent y lo entrega al InboundSink.
//   - *events.Connected    -> marca estado conectado y RESETEA el backoff.
//   - *events.Disconnected -> marca estado desconectado y AVANZA el backoff (whatsmeow auto-reconecta).
//   - *events.LoggedOut    -> marca la sesión CAÍDA (no se re-empareja automáticamente).
//   - *events.OfflineSyncPreview/*events.OfflineSyncCompleted -> abren y cierran el corchete de la ráfaga
//     offline SOLO para contar y reconciliar (ADR-0037 §4). NO deciden descartes: quien decide qué entra
//     es la VENTANA TEMPORAL de onMessage, per-evento (ver inbound_window.go).
//
// La lógica de enrutado/mapeo vive en handleEvent(ctx, evt any), TESTEABLE con eventos sintéticos sin
// un *whatsmeow.Client real. Register() solo cablea handleEvent al AddEventHandler real (no se cubre
// en tests: requiere socket/red, por diseño).
package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-edge-intent/classifier"
	"github.com/EduGoGroup/wapp-shared/logger"
)

// ConnState es el estado de conexión observado por el Listener a partir de los eventos de whatsmeow.
type ConnState int

const (
	// StateDisconnected: el socket no está conectado (estado inicial y tras *events.Disconnected).
	StateDisconnected ConnState = iota
	// StateConnected: socket conectado y autenticado (tras *events.Connected).
	StateConnected
	// StateLoggedOut: la sesión fue cerrada por WhatsApp (tras *events.LoggedOut); requiere re-pairing.
	StateLoggedOut
)

// Listener enruta los eventos de whatsmeow hacia el dominio/sink y lleva el estado de conexión y la
// política de backoff. Es seguro para uso concurrente (whatsmeow invoca el handler desde sus
// goroutines): el estado se protege con mu.
type Listener struct {
	sink    app.InboundSink
	log     logger.Logger
	backoff *Backoff

	mu    sync.Mutex
	state ConnState

	// onDisconnect, si está definido, se invoca tras avanzar el backoff en cada *events.Disconnected
	// con el delay calculado. En el spike es nil (whatsmeow auto-reconecta); se inyecta en tests para
	// verificar el disparo de la política de reconexión, y queda como costura para una reconexión
	// manual en Fase 1.
	onDisconnect func(attempt int, delay time.Duration)

	// onConnect, si está definido, se invoca tras marcar el estado conectado en cada *events.Connected.
	// La escucha lo usa para anunciar presencia (SendPresence available, §10.D) sobre el cliente vivo,
	// de modo que WhatsApp propague los acuses de entrega/lectura. nil = no hace nada (tests que no lo
	// ejercitan). Se re-dispara en cada reconexión (re-anunciar presencia es lo correcto).
	onConnect func()

	// onReceipt, si está definido, recibe cada ACUSE (delivered/read) ya mapeado a domain.ReceiptEvent.
	// Es la costura de SALIDA del acuse (el análogo del InboundSink de los entrantes): el CloudLink la
	// cablea en T2 para subir el estado a la nube. nil = se ignora sin romper la escucha. No lleva PII.
	onReceipt func(domain.ReceiptEvent)

	// onLoggedOut, si está definido, se invoca al recibir *events.LoggedOut (WhatsApp cerró el device),
	// TRAS marcar el estado y loguear (Plan 020 T3). El CloudLink lo cablea para propagar el estado ZOMBIE
	// a la nube mientras el stream aún vive. nil = solo se loguea (comportamiento previo). Corre FUERA del
	// lock. No lleva PII. (Sufijo Hook para no colisionar con el método onLoggedOut.)
	onLoggedOutHook func()

	// reporter registra la prueba de vida del socket y la edad del último entrante en el registro de salud
	// por sesión (Plan 031 T6): connected en *events.Connected, connecting en *events.Disconnected (whatsmeow
	// reintenta), dead en *events.LoggedOut, y el instante de cada *events.Message. nil ⇒ no se reporta (el
	// registro es opcional; el factory del sessionmgr lo liga a la sesión). No lleva PII: solo estados/tiempos.
	reporter health.SessionReporter

	// brackets OBSERVA los corchetes de la ráfaga de esta sesión y lleva los contadores de la puerta.
	// Solo observabilidad y reconciliación (ADR-0037 §4): NO decide ni un descarte. Nunca nil.
	brackets *bracketObserver

	// connectSeal devuelve el INICIO DE LA CONEXIÓN vigente, leído FRESCO en cada llamada y jamás cacheado.
	// Lo cablea Register a partir del cliente vivo (Client.LastSuccessfulConnect). nil ⇒ no hay sello y
	// resolveThreshold cae a `now`, que es el respaldo estricto. En tests se inyecta un sello controlado.
	connectSeal func() time.Time

	// margin es el margen de la ventana temporal (ADR-0037): se descarta lo anterior a `sello − margin`.
	// Default defaultConnectMargin; configurable por Edge (WAPP_AGENT_INBOUND_MARGIN_SECONDS).
	margin time.Duration

	// cola es la COLA DURABLE de entrantes (Plan 051 Ola 1): el mesonero anota el mensaje en disco
	// (cifrado con la DEK de la sesión) ANTES de entregarlo al sink, para que el acuse de WhatsApp salga
	// con el mensaje ya durable. nil ⇒ COMPORTAMIENTO IDÉNTICO AL PREVIO (solo sink.Deliver): así el
	// cableado del adaptador puede hacerse en otra tarea sin romper nada de lo que hoy funciona.
	cola app.ColaEntrantes

	// sessionID etiqueta las filas de la cola. Va EN CLARO en disco a propósito (app/cola.go): es la
	// clave de enrutado y la que ELIGE la DEK con que el adaptador sella Texto y Meta. El Listener NO lo
	// conoce por sí mismo (el patrón vigente es que el sink por-sesión etiquete el evento aguas abajo,
	// sessionmgr/listen.go), así que se INYECTA con WithSessionID desde quien sí lo sabe. Vacío + cola
	// cableada = fila sin sesión: el adaptador no podría elegir DEK, por eso van juntas en el cableado y
	// por eso NewListener desactiva la cola (ruidosamente) si falta esta.
	sessionID string

	// fastLane decide si un texto se salta el LLM (carril rápido, µs). Default classifier.FastLane. Es
	// inyectable para poder probar el nacimiento de la fila en EstadoClasificado sin depender del léxico
	// concreto del clasificador. OJO SEMÁNTICO: true significa «entrega SIN intención», no «ya
	// clasificado con X» — por eso el IntentJSON que se persiste es una marca de omisión, no un intent.
	fastLane func(string) bool
}

// Las dos MARCAS DE OMISIÓN que puede llevar la columna intent al nacer la fila. Ninguna es una
// intención: ambas dicen «el cajero no debe reclamar esta fila (nace en EstadoClasificado)» y se
// diferencian en el PORQUÉ, que es justo lo que el desglose de INV-051.3 necesita distinguir:
//
//   - fastlaneIntentJSON: había texto y el CARRIL RÁPIDO (regex léxico, µs) lo resolvió sin LLM — p.ej.
//     "2", una opción de menú.
//   - sinTextoIntentJSON: NO HABÍA TEXTO que clasificar (imagen, audio, sticker, ubicación, …). El
//     fastlane devuelve true para la cadena vacía, así que sin esta distinción todo el tráfico no
//     textual se contabilizaría como "fastlane" y la métrica mentiría sobre cuánto LLM nos ahorra el
//     léxico. Se comprueba ANTES de llamar al fastlane.
const (
	fastlaneIntentJSON = `{"omitido":"fastlane"}`
	sinTextoIntentJSON = `{"omitido":"sin_texto"}`
)

// ListenerOption configura el Listener sin romper la firma de NewListener (variádica), igual que
// app.ListenOption hace con el caso de uso Listen. Sin opciones, los defaults son los de siempre.
type ListenerOption func(*Listener)

// WithCola cablea la cola durable de entrantes (Plan 051 Ola 1). nil se IGNORA: el listener se queda
// con el camino de siempre (solo sink.Deliver), que es el fallback explícito de esta ola.
func WithCola(c app.ColaEntrantes) ListenerOption {
	return func(l *Listener) {
		if c != nil {
			l.cola = c
		}
	}
}

// WithSessionID inyecta el identificador de sesión con que se etiquetan las filas de la cola (y con el
// que el adaptador elige la DEK). Vacío se ignora. Va SIEMPRE junto a WithCola.
func WithSessionID(id string) ListenerOption {
	return func(l *Listener) {
		if id != "" {
			l.sessionID = id
		}
	}
}

// WithFastLane sustituye el carril rápido por defecto (classifier.FastLane). nil se ignora. Existe
// para los tests: el default es el bueno en producción.
func WithFastLane(fn func(string) bool) ListenerOption {
	return func(l *Listener) {
		if fn != nil {
			l.fastLane = fn
		}
	}
}

// SetHealthReporter liga el listener al registro de salud de SU sesión (Plan 031 T6). Se llama ANTES de
// Register (al construir el ciclo de escucha). nil ⇒ no se reporta salud. No es secreto ni PII.
func (l *Listener) SetHealthReporter(r health.SessionReporter) { l.reporter = r }

// NewListener construye el listener con el sink y el logger dados, el backoff por defecto del spike y el
// observador de corchetes. El margen de la ventana arranca en su default; SetConnectMargin lo ajusta.
// Sin opts el comportamiento es el histórico: SIN cola durable, solo entrega al sink.
func NewListener(sink app.InboundSink, log logger.Logger, opts ...ListenerOption) *Listener {
	l := &Listener{
		sink:     sink,
		log:      log,
		backoff:  DefaultBackoff(),
		state:    StateDisconnected,
		brackets: newBracketObserver(log),
		margin:   defaultConnectMargin,
		fastLane: classifier.FastLane,
	}
	for _, o := range opts {
		o(l)
	}
	// CABLEADO INCOMPLETO: WithCola SIN WithSessionID. Sin sesión, cada fila se escribiría con
	// session_id="" y el adaptador pediría la DEK de la cadena vacía: o falla en cada mensaje, o —peor—
	// mezcla el material de todas las sesiones bajo una llave que no es de nadie. Ninguna de las dos se
	// puede descubrir en producción por casualidad, así que NO se falla en silencio: se GRITA en el log y
	// se DEGRADA al camino de siempre (cola nil ⇒ solo sink.Deliver), que es exactamente el
	// comportamiento previo al Plan 051. Tumbar el proceso tampoco es opción: un error de cableado no
	// puede dejar sin escucha a las sesiones que sí están bien.
	if l.cola != nil && l.sessionID == "" {
		l.log.Error("listener: cableado incompleto — WithCola sin WithSessionID; la cola durable queda DESACTIVADA para esta sesión (se entrega solo al sink). Corrige el arranque: ambas opciones van juntas")
		l.cola = nil
	}
	return l
}

// SetConnectMargin fija el margen de la ventana temporal (ADR-0037). Se llama ANTES de Register (al
// construir el ciclo de escucha). Un valor <=0 se ignora: el margen es lo que absorbe el desfase de reloj
// que no podemos medir, así que dejarlo en cero descartaría tráfico vivo.
func (l *Listener) SetConnectMargin(d time.Duration) {
	if d > 0 {
		l.margin = d
	}
}

// SetConnectSeal inyecta la fuente del inicio de conexión. En producción la cablea Register desde el
// cliente vivo; existe como costura para probar el criterio con un sello controlado.
func (l *Listener) SetConnectSeal(fn func() time.Time) { l.connectSeal = fn }

// InboundStats devuelve el acumulado de la puerta de entrada de esta sesión (ADR-0037 §4): corchetes
// reconciliados, descartados por la ventana, ecos propios, admitidos sin hora y las dos degradaciones del
// encolado durable (Plan 051 · INV-051.3). Son cardinalidades, sin PII. Es el material que la telemetría
// de flota (ADR-0023) debe publicar; publicarlo al heartbeat es tarea de la Ola 4.
func (l *Listener) InboundStats() InboundStats { return l.brackets.snapshot() }

// State devuelve el estado de conexión observado (para observabilidad/tests).
func (l *Listener) State() ConnState {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

// Register cablea handleEvent al AddEventHandler REAL del cliente whatsmeow. El ctx (vida de la
// sesión Listen) se propaga a cada entrega al sink. NO se cubre en tests (requiere un client real).
func (l *Listener) Register(ctx context.Context, client *wm.Client) uint32 {
	// Sello de la ventana temporal (ADR-0037): Client.LastSuccessfulConnect leído FRESCO en cada
	// evaluación, nunca cacheado. Se lee desde el handler de mensaje, que es exactamente donde la cadena
	// happens-before del handlerQueue ya lo ordena (ver la cabecera de inbound_window.go, y ahí también el
	// residual F2/F3 que esta lectura NO cubre y por el que resolveThreshold desconfía del valor).
	l.connectSeal = func() time.Time { return client.LastSuccessfulConnect }
	return client.AddEventHandler(func(evt any) {
		l.handleEvent(ctx, evt)
	})
}

// handleEvent es el ENRUTADOR PURO (testeable): recibe un evento de whatsmeow y reacciona según su
// tipo. No abre sockets ni depende de un client; en tests se le pasan eventos sintéticos.
func (l *Listener) handleEvent(ctx context.Context, evt any) {
	switch e := evt.(type) {
	case *events.Message:
		l.onMessage(ctx, e)
	case *events.Connected:
		l.onConnected()
	case *events.Disconnected:
		l.onDisconnected()
	case *events.LoggedOut:
		l.onLoggedOut(e)
	case *events.Receipt:
		l.onReceiptEvent(e)
	case *events.OfflineSyncPreview:
		// SOLO OBSERVABILIDAD (ADR-0037 §4): abre el corchete para poder reconciliar «el servidor anunció
		// N, la ventana descartó M». NO decide descartes — eso es cosa de la ventana temporal, per-evento.
		l.brackets.arm(e)
	case *events.OfflineSyncCompleted:
		l.brackets.close(closeCompleted)
	default:
		// Otros eventos (presencia, history sync, …) no son del alcance actual.
	}
}

// onMessage decide si el entrante se ADMITE y, si lo hace, lo mapea a domain.InboundEvent y lo entrega al
// sink. Un fallo de entrega se registra pero NO tumba la escucha (el socket sigue vivo).
//
// Los filtros de la puerta, en este orden:
//  1. ECO PROPIO (IsFromMe): lo mandamos nosotros; no es una conversación entrante y no tiene por qué subir.
//  2. SIN HORA UTILIZABLE: se ADMITE explícitamente (ver abajo).
//  3. VENTANA TEMPORAL (ADR-0037, criterio único): anterior a `inicioDeConexión − margen` ⇒ se descarta.
//
// Lo ADMITIDO sigue el orden del Plan 051 Ola 1: ventana → fastlane → INSERT cifrado en la cola durable →
// sink.Deliver. El INSERT va antes de la entrega para que el acuse salga con el mensaje ya en disco; la
// entrega al sink SE MANTIENE (escritura doble) hasta que exista el despachador de la Ola 3.
//
// El descarte es SILENCIOSO hacia el cliente final y CONTADO hacia nosotros: un descarte invisible es
// indistinguible de un bug. Se cuenta, no se transcribe (ADR-0034 nivel 3): ni texto ni teléfono al log.
func (l *Listener) onMessage(ctx context.Context, e *events.Message) {
	// Prueba de vida (Plan 031 T6): sella el instante del último entrante. Se marca ANTES de filtrar y para
	// TODO mensaje —incluidos los descartados— porque es prueba de vida del SOCKET, no señal de negocio:
	// una ráfaga descartada sigue demostrando que el socket recibe. Comportamiento idéntico al previo.
	if l.reporter != nil {
		l.reporter.MarkInbound(time.Now())
	}

	// 1 · Eco propio. Hoy este dato solo se usaba para no gastar el clasificador (intent/sink.go:143); el
	// evento subía igual a la nube. Descartarlo aquí es un CAMBIO DE COMPORTAMIENTO deliberado y aprobado.
	if e.Info.IsFromMe {
		l.brackets.countSelfDrop()
		return
	}

	// 2 · Sin hora utilizable ⇒ SE ADMITE, explícitamente y contado. No es teórico: GetUnixTime devuelve
	// un time.Time CERO y ok=true cuando el atributo `t` vale "0" (binary/attrs.go:116-123), sin registrar
	// error, así que el mensaje llega hasta aquí con Info.Timestamp en cero. (Si `t` falta del todo,
	// whatsmeow lo rechaza antes: UnixTime exige el atributo y parseMessageInfo corta en ag.OK().) Un cero
	// es anterior a CUALQUIER umbral, así que dejarlo caer por la comparación lo descartaría en silencio —
	// justo lo contrario de la asimetría del ADR: ante la duda, DEJAR PASAR.
	if e.Info.Timestamp.IsZero() {
		l.brackets.countNoTimestamp()
		l.log.Warn("listener: entrante SIN hora utilizable; se admite por precaución (la ventana no puede juzgarlo)")
		// Cola durable ANTES del sink, igual que en el camino normal (ver el comentario largo de abajo).
		// SELLO: aquí Info.Timestamp es CERO y NO se puede persistir tal cual — un 0 en la columna es epoch
		// 1970, y la poda por TTL de la cola se llevaría la fila en el primer barrido, o sea perder justo el
		// mensaje que acabamos de admitir por precaución. Se sella con la hora LOCAL de recepción, que es lo
		// único cierto que tenemos de él: no es su hora real, pero sitúa la fila en la ventana de retención.
		l.enqueueCola(ctx, e, time.Now().Unix())
		if err := l.sink.Deliver(ctx, toInboundEvent(e)); err != nil {
			l.log.Error("listener: no se pudo entregar el evento entrante al sink", "error", err)
		}
		return
	}

	// 3 · Ventana temporal (ADR-0037): el criterio ÚNICO. Info.Timestamp es el reloj del SERVIDOR del
	// mensaje original (message.go:216); el umbral sale del inicio de la conexión, NO de time.Now(), para
	// que nuestro propio atasco no cuente como antigüedad. Solo se loguean edad y umbral, nunca el mensaje.
	var seal time.Time
	if l.connectSeal != nil {
		seal = l.connectSeal() // lectura FRESCA, sin cachear (ver Register y la cabecera de inbound_window.go)
	}
	now := time.Now()
	threshold := resolveThreshold(seal, l.margin, now)
	if e.Info.Timestamp.Before(threshold) {
		age := now.Sub(e.Info.Timestamp)
		if age < 0 {
			age = 0
		}
		l.brackets.countWindowDrop(age)
		l.log.Warn("listener: entrante descartado por caer fuera de la ventana temporal (ADR-0037)",
			"edad", age.Round(time.Second).String(), "margen", l.margin.String())
		return
	}

	// 4 · COLA DURABLE (Plan 051 Ola 1), y va ANTES del sink a propósito.
	//
	// ⚠️ NO "LIMPIES" EL sink.Deliver DE ABAJO. En la Ola 1 esto es ESCRITURA DOBLE deliberada: primero
	// durabilidad (el INSERT cifrado), después la entrega de siempre. El gate «mensaje durable antes del
	// acuse» se cumple con que el INSERT ocurra ANTES del Deliver, no con quitar el Deliver. Quien sustituye
	// al camino inline es el DESPACHADOR de la Ola 3; hasta que exista y esté cableado, borrar esta entrega
	// deja los mensajes sin llegar a la nube — regresión de campo, no simplificación.
	//
	// Un descartado por la ventana (paso 3) ya ha vuelto: no genera fila (REQ-051.5).
	l.enqueueCola(ctx, e, e.Info.Timestamp.Unix())

	inbound := toInboundEvent(e)
	if err := l.sink.Deliver(ctx, inbound); err != nil {
		l.log.Error("listener: no se pudo entregar el evento entrante al sink",
			"error", err, "message_id", inbound.MessageID)
	}
}

// enqueueCola anota el entrante en la cola durable. tsWhatsApp va en epoch-SEGUNDOS (no milis): es el
// sello con el que la cola ordena y poda.
//
// REQ-051.8 — un fallo del Enqueue se REGISTRA y ya: no tumba el socket, no aborta el handler y no
// impide la entrega al sink. La cola es una mejora de durabilidad; que falle no puede ser peor que no
// tenerla. INV-051.1 — el texto del mensaje NUNCA sale por el log, ni entero ni truncado: solo
// identificadores y el error.
//
// cola == nil ⇒ no-op (fallback documentado: comportamiento idéntico al previo al Plan 051).
func (l *Listener) enqueueCola(ctx context.Context, e *events.Message, tsWhatsApp int64) {
	if l.cola == nil {
		return
	}
	// REQ-051.8 / T1.10 — RED DE SEGURIDAD. Un panic aquí dentro (driver de la BD, un crypterFor ajeno,
	// un nil inesperado) subiría al bucle de handlers de whatsmeow y TUMBARÍA LA SESIÓN entera: la cola
	// es una mejora de durabilidad, jamás puede ser peor que no tenerla.
	//
	// 🔴 INV-051.1: el valor recuperado NO se loguea. Un panic puede arrastrar en su mensaje el
	// argumento que lo provocó —y aquí ese argumento puede ser el TEXTO del mensaje o su meta—, así que
	// imprimirlo con %v filtraría contenido de negocio al log. Se anota solo el identificador del
	// mensaje, que es lo que permite correlacionar sin transcribir nada.
	defer func() {
		if r := recover(); r != nil {
			// INV-051.3: la degradación se CUENTA además de loguearse.
			l.brackets.countColaEnqueuePanic()
			l.log.Error("listener: panic al encolar el entrante; la escucha sigue (REQ-051.8)",
				"message_id", e.Info.ID)
		}
	}()
	text := messageText(e)
	item := app.ColaItem{
		SessionID:   l.sessionID,
		ChatJID:     e.Info.Chat.String(),
		WAMessageID: e.Info.ID,
		TSWhatsApp:  tsWhatsApp,
		Texto:       text,
		Meta:        l.colaMeta(e),
		Estado:      app.EstadoNuevo,
	}
	switch {
	case text == "":
		// SIN TEXTO (imagen, audio, sticker, ubicación, …): no hay nada que clasificar, así que la fila
		// nace resuelta igual que con el fastlane — pero con SU PROPIO motivo. Se comprueba ANTES del
		// carril rápido a propósito: classifier.FastLane("") devuelve true, y dejar caer estos mensajes
		// por esa rama los contaría como ahorro del léxico, que es falso (ver las constantes de arriba).
		item.Estado = app.EstadoClasificado
		item.IntentJSON = sinTextoIntentJSON
	case l.fastLane != nil && l.fastLane(text):
		// Carril rápido (µs, sin tocar el LLM ni el circuito): la fila NACE resuelta y el cajero no la
		// reclamará nunca. No se inventa una intención: se anota la marca de omisión.
		item.Estado = app.EstadoClasificado
		item.IntentJSON = fastlaneIntentJSON
	}
	if err := l.cola.Enqueue(ctx, item); err != nil {
		// INV-051.3: el fallo se CUENTA siempre, se grite o no (un log se pierde; el acumulado no).
		l.brackets.countColaEnqueueError()
		// EL THROTTLE DEL LOG NO VIVE AQUÍ: vive en el adaptador de la cola, que es quien conoce la sesión,
		// la causa y su ventana de enfriamiento — aquí solo se ve "un error por mensaje" y no hay forma de
		// saber si es el mismo fallo repetido. El adaptador marca con app.ErrColaFalloRepetido lo que YA
		// gritó, y aquí únicamente se baja el nivel. Sin esto, una sesión con la DEK ausente escribía un
		// Error por mensaje entrante, a ritmo de socket.
		if errors.Is(err, app.ErrColaFalloRepetido) {
			l.log.Debug("listener: la cola durable sigue rechazando los entrantes de esta sesión (fallo ya reportado); el evento se entrega igualmente al sink",
				"message_id", e.Info.ID)
			return
		}
		l.log.Error("listener: no se pudo anotar el entrante en la cola durable; la escucha sigue y el evento se entrega igualmente al sink (REQ-051.8)",
			"error", err, "message_id", e.Info.ID)
	}
}

// colaMetaPayload son los metadatos de negocio que acompañan a la fila. Es lo que el despachador (Ola 3)
// necesitará para reconstruir el domain.InboundEvent sin volver a ver el *events.Message, menos lo que ya
// son columnas propias (chat, id de mensaje, sello). Se persiste CIFRADO con la DEK de la sesión
// (INV-051.1): por eso puede llevar identidad del remitente, que en un log estaría prohibida.
type colaMetaPayload struct {
	Sender         string `json:"sender,omitempty"`
	SenderAlt      string `json:"sender_alt,omitempty"`
	AddressingMode string `json:"addressing_mode,omitempty"`
	PushName       string `json:"push_name,omitempty"`
	Type           string `json:"type,omitempty"`
	IsGroup        bool   `json:"is_group,omitempty"`
}

// colaMeta serializa los metadatos a JSON. Un fallo de serialización NO impide anotar la fila: se anota
// con Meta nil (columna NULL) y se registra, porque el texto durable vale más que su metadato.
func (l *Listener) colaMeta(e *events.Message) []byte {
	b, err := json.Marshal(colaMetaPayload{
		Sender:         e.Info.Sender.String(),
		SenderAlt:      e.Info.SenderAlt.String(),
		AddressingMode: string(e.Info.AddressingMode),
		PushName:       e.Info.PushName,
		Type:           e.Info.Type,
		IsGroup:        e.Info.IsGroup,
	})
	if err != nil {
		l.log.Error("listener: no se pudo serializar el metadato de la cola; la fila se anota sin meta",
			"error", err, "message_id", e.Info.ID)
		return nil
	}
	return b
}

// onConnected marca el estado conectado, resetea el backoff (reconexión exitosa) y dispara el hook
// onConnect si está cableado (anuncio de presencia, §10.D). El hook corre FUERA del lock.
func (l *Listener) onConnected() {
	l.mu.Lock()
	l.state = StateConnected
	l.backoff.Reset()
	hook := l.onConnect
	l.mu.Unlock()
	l.log.Info("listener: socket conectado (escucha 24/7 activa)")
	// Prueba de vida (Plan 031 T6): el socket de WhatsApp está VIVO (no solo "el cliente existe").
	if l.reporter != nil {
		l.reporter.SetSocketState(health.SocketConnected, "")
	}
	if hook != nil {
		hook()
	}
}

// onReceiptEvent mapea un *events.Receipt a domain.ReceiptEvent (acuse de entrega/lectura de un
// SALIENTE) y lo despacha por el hook onReceipt si está cableado. Los tipos de acuse fuera del ciclo
// {delivered, read} se IGNORAN sin romper (§10.A). El SessionID lo etiqueta el sink por-sesión aguas
// abajo (T2), como el InboundSink de entrantes (mux.SinkFor); aquí va vacío. No emite PII a los logs.
func (l *Listener) onReceiptEvent(e *events.Receipt) {
	status, ok := mapReceiptStatus(e.Type)
	if !ok {
		return // sender/retry/inactive/hist_sync/… no son del ciclo de vida de un saliente.
	}
	if l.onReceipt == nil {
		return
	}
	l.onReceipt(domain.ReceiptEvent{
		// Copia defensiva del slice de whatsmeow (types.MessageID es alias de string).
		MessageIDs: append([]string(nil), e.MessageIDs...),
		Status:     status,
		Timestamp:  e.Timestamp,
		// SessionID: lo rellena el sink por-sesión en T2 (patrón mux.SinkFor de los entrantes).
	})
}

// mapReceiptStatus traduce el types.ReceiptType de whatsmeow al estado de dominio (Plan 013 §10.A):
// Delivered→delivered (✓✓); Read/ReadSelf/Played→read (✓✓ azul); cualquier otro tipo se ignora
// (ok=false). NOTA DE CAMPO: las constantes reales llevan el infijo "Type"
// (types.ReceiptTypeDelivered/Read/ReadSelf/Played); el diseño §10.A las citaba sin él.
func mapReceiptStatus(t types.ReceiptType) (domain.ReceiptStatus, bool) {
	switch t {
	case types.ReceiptTypeDelivered:
		return domain.ReceiptDelivered, true
	case types.ReceiptTypeRead, types.ReceiptTypeReadSelf, types.ReceiptTypePlayed:
		return domain.ReceiptRead, true
	default:
		return "", false
	}
}

// onDisconnected marca el estado desconectado y AVANZA el backoff. whatsmeow auto-reconecta; aquí
// solo trazamos la cadencia y, si hay hook inyectado, lo disparamos con el delay calculado.
func (l *Listener) onDisconnected() {
	l.mu.Lock()
	l.state = StateDisconnected
	delay := l.backoff.Next()
	attempt := l.backoff.Attempt()
	hook := l.onDisconnect
	l.mu.Unlock()

	// Cierra el corchete abierto, si lo hay, para no perder su reconciliación. Es BEST-EFFORT y solo
	// observabilidad: este evento no es fiable (sale con `go` en client.go:512 y :580, y ni siquiera se
	// emite en una desconexión ESPERADA, client.go:576-580). Da igual: no decide ningún descarte.
	l.brackets.close(closeReconnect)

	l.log.Warn("listener: socket desconectado; whatsmeow reintentará (política de backoff)",
		"intento", attempt, "siguiente_delay", delay.String())
	// Prueba de vida (Plan 031 T6): corte transitorio — whatsmeow reintenta el dial (auto-reconnect); el
	// socket no está vivo pero tampoco caído para siempre ⇒ connecting, no degraded.
	if l.reporter != nil {
		l.reporter.SetSocketState(health.SocketConnecting, health.ReasonReconnecting)
	}
	if hook != nil {
		hook(attempt, delay)
	}
}

// onLoggedOut marca la sesión CAÍDA. NO se re-empareja automáticamente (requiere acción humana:
// escanear un QR nuevo). Se reporta para que el control/cloud lo sepa.
func (l *Listener) onLoggedOut(e *events.LoggedOut) {
	l.mu.Lock()
	l.state = StateLoggedOut
	hook := l.onLoggedOutHook
	l.mu.Unlock()
	// Mismo cierre best-effort del corchete que en la desconexión (ver onDisconnected).
	l.brackets.close(closeReconnect)

	l.log.Error("listener: sesión cerrada por WhatsApp (LoggedOut); requiere re-emparejar",
		"on_connect", e.OnConnect, "reason", e.Reason.String())
	// Prueba de vida (Plan 031 T6): WhatsApp cerró la sesión — dead, no se recupera solo (requiere re-QR).
	if l.reporter != nil {
		l.reporter.SetSocketState(health.SocketDead, health.ReasonLoggedOut)
	}
	// Plan 020 T3: propaga el cierre al cloud (estado ZOMBIE) mientras el stream aún vive, ANTES del
	// teardown local. El comportamiento local previo (logging/no re-pairing) no cambia. Corre fuera del lock.
	if hook != nil {
		hook()
	}
}

// toInboundEvent extrae de un *events.Message los campos útiles de dominio. El cuerpo de texto sale
// de Conversation o, si viene envuelto, de ExtendedTextMessage. No toca material cifrado.
func toInboundEvent(e *events.Message) domain.InboundEvent {
	return domain.InboundEvent{
		MessageID: e.Info.ID,
		Chat:      e.Info.Chat.String(),
		Sender:    e.Info.Sender.String(),
		// SenderAlt: la dirección alterna (número<->LID) que resuelve whatsmeow. Si el mapeo aún no se
		// conoce (JID vacío, "No LID found" en el primer contacto), .String() devuelve "" y aguas
		// abajo se sube solo lo conocido (tolerancia Plan 010 §10.H, sin llamar a GetPNForLID).
		SenderAlt:      e.Info.SenderAlt.String(),
		AddressingMode: string(e.Info.AddressingMode),
		PushName:       e.Info.PushName,
		Timestamp:      e.Info.Timestamp,
		Type:           e.Info.Type,
		Text:           messageText(e),
		IsFromMe:       e.Info.IsFromMe,
		IsGroup:        e.Info.IsGroup,
	}
}

// messageText devuelve el texto del mensaje: Conversation (mensaje simple) o el Text del
// ExtendedTextMessage (mensaje con contexto/enlace). Vacío si no es de texto.
func messageText(e *events.Message) string {
	if e.Message == nil {
		return ""
	}
	if c := e.Message.GetConversation(); c != "" {
		return c
	}
	return e.Message.GetExtendedTextMessage().GetText()
}
