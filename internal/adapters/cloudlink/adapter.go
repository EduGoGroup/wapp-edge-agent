package cloudlink

// Adapter es el conducto CloudLink REAL del Edge (pieza 02), sustituto productivo del LogSink del
// spike. Es UN SOLO multiplexor por Edge: abre UN único stream Connect (un mTLS por Edge) y MULTIPLEXA
// N sesiones por session_id (ADR-0008 §"un stream"). Cumple dos roles a la vez:
//
//   1. SALIDA (mux): SinkFor(session_id) devuelve un app.InboundSink cuyas entregas etiquetan el
//      EdgeToCloud con ESE session_id y lo escriben al ÚNICO stream. SOLO viaja CONTENIDO DE NEGOCIO
//      (remitente, texto, timestamp, id de WhatsApp): la DEK, el store cifrado y las llaves Signal
//      JAMÁS cruzan el cable (ADR-0007, zero-knowledge).
//
//   2. ENTRADA (demux): el loop de recepción lee cada CloudToEdge, mira su session_id y enruta el
//      comando (SendText/LeaseUpdate/Ping) a la sesión correspondiente del registro. Un comando para
//      un session_id desconocido se LOGUEA y se IGNORA (nunca paniquea).
//
// REGISTRO MULTI-SESIÓN: Register(session_id, sendFunc, hasDEK) da de alta una sesión (su emisor por
// cliente vivo + el booleano de presencia de la DEK); Unregister la quita. Cada sesión tiene SU PROPIO
// estado de lease (lease por sesión, ADR-0016 §5): el Adapter construye un lease.Validator FRESCO por
// sesión vía la ValidatorFactory (misma clave pública del Edge, estado independiente).
//
// GATE DE LEASE (ADR-0007, gate 2-de-2 a nivel de OPERACIÓN): si la sesión tiene Validator, antes de
// despachar un SendText se exige Validator.CanOperate(hasDEK). Si el lease de ESA sesión no está
// vigente (revocado, expirado o nunca aplicado), NO se invoca el sendFunc y se responde
// Ack{ok=false, error="lease no vigente"} — sin afectar a las demás sesiones.
//
// RECONEXIÓN: backoff exponencial + jitter (la política pura whatsmeow.Backoff es package-private de
// ese adaptador y sin jitter; aquí se usa una propia, decoupleada). Al reconectar se reabre el único
// stream y se re-ancla CADA sesión registrada con su Heartbeat inicial.
//
// DURABILIDAD: si la entrega a la nube falla (stream caído), se LOGUEA y se descarta (diagnóstico
// mínimo); el outbox durable de SQLite (ADR-0003) queda como FOLLOW-UP. Una entrega fallida nunca
// tumba el socket de WhatsApp.

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/EduGoGroup/wapp-cloudlink/client"
	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-cloudlink/lease"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-shared/envelope"
	"github.com/EduGoGroup/wapp-shared/logger"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// SendFunc es el callback de envío que el wiring conecta al despachador del Edge (cliente vivo de la
// sesión). El adaptador lo invoca al recibir un comando SendText para ESA sesión (tras pasar el gate),
// pasándole el command_id del comando para que el despachador CORRELACIONE el envío (Plan 013 §10.E):
// así el acuse (events.Receipt) posterior se puede etiquetar con ese command_id al subirlo.
type SendFunc = func(ctx context.Context, commandID, to, text string) error

// SendMediaFunc es el callback de envío de ARCHIVOS (Plan 017 §7), hermano de SendFunc para media: el
// wiring lo conecta al despachador de media del cliente vivo (SendMediaViaLiveClientTracked). El adaptador
// lo invoca al recibir un SendMedia para ESA sesión (tras pasar el gate de lease), pasándole el command_id
// (para la correlación del acuse) y los metadatos del archivo; la presigned URL la DESCARGA el despachador
// (GET sin credenciales), no viaja binario por gRPC.
type SendMediaFunc = func(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) error

// ValidatorFactory construye un lease.Validator FRESCO (estado independiente) para UNA sesión, o nil si
// el gate de lease está desactivado (sin clave pública). Todas las sesiones del Edge comparten la MISMA
// clave pública del servidor, pero cada una mantiene su PROPIO estado de lease (lease por sesión,
// ADR-0016 §5): por eso es un factory y no un Validator compartido.
type ValidatorFactory func() *lease.Validator

// ConfigApplier aplica una config empujada por la nube (frame ConfigUpdate, ADR-0021). Lo satisface
// edgeconfig.Service (interfaz estructural: el adapter de transporte NO importa edgeconfig). Devuelve error
// SOLO ante un fallo de persistencia (reintentable: el Cloud reempuja al reconectar); los demás desenlaces
// (kind desconocido, versión ya aplicada, blob inválido con last-known-good) los resuelve el applier con log
// y devuelven nil ⇒ el Edge siempre responde Ack. nil (no cableado) ⇒ el adapter Ack-ea tolerante sin persistir.
type ConfigApplier interface {
	Apply(ctx context.Context, kind, version string, payload []byte) error
}

// sessionEntry es el estado por sesión dentro del multiplex: su emisor por cliente vivo, su Validator de
// lease (propio) y su contador de heartbeat. El Adapter lo crea en Register y lo descarta en Unregister.
type sessionEntry struct {
	sendFunc      SendFunc
	sendMediaFunc SendMediaFunc    // emisor de ARCHIVOS por cliente vivo (Plan 017); nil => media no soportada
	validator     *lease.Validator // nil => sin gate de lease para esta sesión
	hasDEK        func() bool      // proveedor del booleano del gate 2-de-2 (p.ej. custody.Exists)
	leaseCtr      atomic.Int64     // contador de heartbeat de ESTA sesión (ancla de renovación)
	// selfJID/selfPN identifican el NÚMERO PROPIO de la sesión (Plan 020 T2, anti-self-loop): el JID crudo
	// del device propio y su número en E.164 SIN '+' (derivado del JID). No son secretos (van en claro por el
	// mTLS ya cifrado); permiten al Cloud saber a qué número pertenece la sesión al marcarla online. Vacíos si
	// la sesión aún no está emparejada (JID desconocido): el Cloud tolera vacío.
	selfJID string
	selfPN  string
}

// Adapter gestiona el único stream Connect contra la nube y multiplexa N sesiones por session_id.
type Adapter struct {
	cc           grpc.ClientConnInterface
	newValidator ValidatorFactory // construye el Validator por sesión (nil => sin gate)
	log          logger.Logger

	// cloudEncPub es la clave pública X25519 (32B) de CIFRADO de la nube (Plan 011 §6.3). Si está
	// presente, Deliver SELLA los campos sensibles (SensitivePayload) hacia esta pública con SealFor y
	// los envía en IncomingMessage.EncPayload; si es nil, va el fallback claro (§10.H): los campos
	// sensibles viajan planos y el mTLS sigue protegiendo el canal. Es ÚNICA por Edge (la nube tiene una
	// sola pública de cifrado); todas las sesiones sellan hacia la misma.
	cloudEncPub []byte

	hbInterval time.Duration
	baseDelay  time.Duration
	maxDelay   time.Duration

	// cmdTimeout es el deadline POR OPERACIÓN del demux (Plan 027 T1, cierra H7): cada handleCommand se
	// ejecuta bajo un context.WithTimeout de esta duración, de modo que un envío/descarga colgado no vive
	// lo que vive el stream. cmdQueueSize es el buffer por sesión del despacho concurrente (backpressure
	// acotado por session_id). Se fijan en NewAdapter y se ajustan con WithCommandTimeout/WithCommandQueueSize.
	cmdTimeout   time.Duration
	cmdQueueSize int

	// configApplier aplica los ConfigUpdate empujados por la nube (Plan 029 · T10 / ADR-0021): persiste el
	// blob (last-known-good), valida por kind y notifica al clasificador en caliente. nil (feature Intent off,
	// o camino de diagnóstico) ⇒ un ConfigUpdate se Ack-ea tolerante sin persistir (kinds futuros). Es un
	// enriquecimiento del demux, nunca gatea envíos/leases.
	configApplier ConfigApplier

	// outbox es la cola DURABLE de eventos edge->cloud (Plan 027 Ola 3 · T2, cierra H2 / ADR-0003). Si está
	// presente (WithOutbox), un entrante/acuse que no se pudo enviar (stream caído o Send con error) se
	// ENCOLA en vez de descartarse, y se drena en orden al reconectar. nil => comportamiento previo
	// (best-effort, se descarta): lo que usan los tests de transporte y el modo dev sin BD.
	outbox              app.Outbox
	outboxDrainInterval time.Duration // cadencia del re-drenaje mientras hay stream (además del drenaje al conectar)

	// pending guarda, EN MEMORIA, las sesiones con backlog en el outbox (Plan 027 T2): mientras una sesión
	// tiene eventos encolados, los nuevos también se encolan (no se envían en vivo) para PRESERVAR el orden
	// relativo por sesión al drenar. Se siembra de PendingSessions al arrancar y se recalcula tras cada
	// drenaje. Cross-sesión es independiente; por sesión, Deliver es serial (un listener por sesión).
	pendingMu sync.Mutex
	pending   map[string]bool

	mu       sync.Mutex
	cl       *client.Client           // stream activo; nil mientras está desconectado
	sessions map[string]*sessionEntry // registro multi-sesión por session_id
}

// Valores por defecto del despacho del demux (Plan 027 T1). defaultCommandTimeout es el deadline por
// operación (H7): 30s, igual que la descarga de media del gateway whatsmeow (sender.go), para que el
// deadline del demux nunca corte por debajo de una operación legítima. defaultCommandQueueSize es el
// buffer por sesión: holgura para absorber ráfagas por sesión sin frenar el loop del stream.
const (
	defaultCommandTimeout   = 30 * time.Second
	defaultCommandQueueSize = 64
)

// Parámetros del drenaje del outbox durable (Plan 027 T2). defaultOutboxDrainInterval es la cadencia del
// re-drenaje MIENTRAS hay stream (además del drenaje inmediato al conectar): cubre cualquier evento que
// caiga al outbox estando conectado (p.ej. un Send que falla justo antes de que el stream muera).
// outboxDrainBatch es el tamaño de lote por pasada de drenaje.
const (
	defaultOutboxDrainInterval = 5 * time.Second
	outboxDrainBatch           = 128
)

// Option configura aspectos opcionales del Adapter (cadencias de test, etc.).
type Option func(*Adapter)

// WithHeartbeatInterval fija la cadencia del Heartbeat. Por defecto 30s.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(a *Adapter) { a.hbInterval = d }
}

// WithBackoff fija la política de reconexión (base y tope). Por defecto 1s..60s.
func WithBackoff(base, max time.Duration) Option {
	return func(a *Adapter) { a.baseDelay, a.maxDelay = base, max }
}

// WithCloudEncPubKey fija la clave pública X25519 (32B) de cifrado de la nube para el sellado en
// tránsito (Plan 011 §6.3). Si no se fija (o pub==nil), Deliver usa el fallback claro (§10.H).
func WithCloudEncPubKey(pub []byte) Option {
	return func(a *Adapter) { a.cloudEncPub = pub }
}

// WithCommandTimeout fija el deadline POR OPERACIÓN del demux (Plan 027 T1, H7). Por defecto 30s. Un
// valor <=0 se ignora (conserva el default) para no dejar el demux sin deadline.
func WithCommandTimeout(d time.Duration) Option {
	return func(a *Adapter) {
		if d > 0 {
			a.cmdTimeout = d
		}
	}
}

// WithCommandQueueSize fija el buffer por sesión del despacho concurrente (Plan 027 T1). Sobre todo para
// tests deterministas; en producción el default es sensato. Un valor <=0 se ignora.
func WithCommandQueueSize(n int) Option {
	return func(a *Adapter) {
		if n > 0 {
			a.cmdQueueSize = n
		}
	}
}

// WithOutbox activa el outbox DURABLE (Plan 027 Ola 3 · T2, cierra H2 / ADR-0003): con él, los entrantes y
// acuses que no se pueden enviar (stream caído o Send con error) se ENCOLAN y se drenan en orden al
// reconectar, en vez de descartarse. Sin esta opción el Adapter conserva el comportamiento previo
// (best-effort). nil se ignora (queda desactivado).
func WithOutbox(o app.Outbox) Option {
	return func(a *Adapter) {
		if o != nil {
			a.outbox = o
		}
	}
}

// WithConfigApplier cablea el applier de config empujada por la nube (Plan 029 · T10, ConfigUpdate/ADR-0021):
// con él, un ConfigUpdate se persiste/valida/notifica; sin él, se Ack-ea tolerante sin persistir. nil se
// ignora (queda desactivado).
func WithConfigApplier(a ConfigApplier) Option {
	return func(ad *Adapter) {
		if a != nil {
			ad.configApplier = a
		}
	}
}

// WithOutboxDrainInterval ajusta la cadencia del re-drenaje del outbox mientras hay stream (Plan 027 T2).
// Sobre todo para tests deterministas (intervalos minúsculos); en producción el default (5s) es sensato.
// Un valor <=0 se ignora.
func WithOutboxDrainInterval(d time.Duration) Option {
	return func(a *Adapter) {
		if d > 0 {
			a.outboxDrainInterval = d
		}
	}
}

// NewAdapter construye el multiplexor CloudLink sobre una conexión gRPC ya establecida (cc).
//   - newValidator: factory del Validator de lease POR SESIÓN (nil => gate de lease desactivado).
//   - log: logger estructurado (nunca imprime DEK ni secretos).
//
// No abre el stream: Run lo abre y lo mantiene vivo. Las sesiones se dan de alta con Register.
func NewAdapter(cc grpc.ClientConnInterface, log logger.Logger, newValidator ValidatorFactory, opts ...Option) *Adapter {
	if log == nil {
		log = logger.Default()
	}
	a := &Adapter{
		cc:                  cc,
		newValidator:        newValidator,
		log:                 log,
		hbInterval:          30 * time.Second,
		baseDelay:           1 * time.Second,
		maxDelay:            60 * time.Second,
		cmdTimeout:          defaultCommandTimeout,
		cmdQueueSize:        defaultCommandQueueSize,
		outboxDrainInterval: defaultOutboxDrainInterval,
		pending:             make(map[string]bool),
		sessions:            make(map[string]*sessionEntry),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Register da de alta una sesión en el multiplex: selfJID es el JID crudo del device PROPIO de la sesión
// (Plan 020 T2; "" si aún sin emparejar) del que se deriva el número propio (E.164 sin '+') que viaja en
// cada Heartbeat; send es su emisor de TEXTO por cliente vivo y sendMedia su emisor de ARCHIVOS (Plan 017;
// nil => media no soportada por esa sesión), ambos conectados por el wiring al cliente VIVO de la escucha
// de esa sesión; hasDEK reporta la presencia de la DEK (gate 2-de-2). El Adapter construye un Validator de
// lease PROPIO para la sesión (estado independiente). Si el stream ya está vivo, ancla la sesión con su
// Heartbeat inicial. Idempotente: re-registrar reemplaza la entrada (reinicia su estado de lease/contador).
func (a *Adapter) Register(sessionID, selfJID string, send SendFunc, sendMedia SendMediaFunc, hasDEK func() bool) {
	if hasDEK == nil {
		hasDEK = func() bool { return true }
	}
	entry := &sessionEntry{
		sendFunc:      send,
		sendMediaFunc: sendMedia,
		hasDEK:        hasDEK,
		selfJID:       selfJID,                       // JID crudo del device propio (Plan 020 T2); "" si aún sin emparejar.
		selfPN:        domain.SelfPNFromJID(selfJID), // número propio E.164 sin '+'; "" si el JID no es un número.
	}
	if a.newValidator != nil {
		entry.validator = a.newValidator()
	}

	a.mu.Lock()
	a.sessions[sessionID] = entry
	cl := a.cl
	a.mu.Unlock()

	a.log.Info("CloudLink: sesión registrada en el multiplex",
		"session_id", sessionID, "lease_gate", entry.validator != nil)
	if cl != nil {
		// Stream ya vivo: ancla la sesión recién registrada con su latido inicial.
		a.sendHeartbeat(cl, sessionID, entry)
	}
}

// Unregister da de baja una sesión del multiplex (Unlink): sus comandos posteriores se ignoran limpio.
// Idempotente: no-op si la sesión no estaba registrada.
func (a *Adapter) Unregister(sessionID string) {
	a.mu.Lock()
	_, ok := a.sessions[sessionID]
	delete(a.sessions, sessionID)
	a.mu.Unlock()
	if ok {
		a.log.Info("CloudLink: sesión removida del multiplex", "session_id", sessionID)
	}
}

// SinkFor devuelve el sink de SALIDA (entrantes -> cloud) etiquetado con session_id. El sink escribe al
// ÚNICO stream del Adapter; varios sinks (uno por sesión) comparten el mismo conducto.
func (a *Adapter) SinkFor(sessionID string) app.InboundSink {
	return &sessionSink{a: a, sessionID: sessionID, cloudEncPub: a.cloudEncPub}
}

// entry resuelve el estado de una sesión bajo lock (nil si no está registrada).
func (a *Adapter) entry(sessionID string) *sessionEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sessions[sessionID]
}

// sessionSink implementa app.InboundSink para UNA sesión: etiqueta cada entrega con su session_id y la
// escribe al único stream del Adapter (mux de salida).
type sessionSink struct {
	a           *Adapter
	sessionID   string
	cloudEncPub []byte // pública de cifrado de la nube (snapshot del Adapter); nil => fallback claro
}

var _ app.InboundSink = (*sessionSink)(nil)

// Deliver mapea el InboundEvent a IncomingMessage etiquetado con session_id y lo reenvía por el stream.
// ZERO-KNOWLEDGE: solo viaja contenido de negocio; no hay campo alguno para la DEK ni el store. Si no hay
// stream vivo o el envío falla, con outbox durable configurado (Plan 027 T2) el evento se ENCOLA y se
// reenvía en orden al reconectar; sin outbox conserva el best-effort previo (se registra y descarta).
// Devuelve nil siempre (nunca tumba el socket de WhatsApp).
//
// CIFRADO EN TRÁNSITO (Plan 011 §6.3/§10.E): si hay pública de cifrado de la nube (s.cloudEncPub),
// los campos SENSIBLES (text/push_name/from_pn/from_lid) se serializan como un SensitivePayload proto,
// se sellan con SealFor (X25519 anónimo) y viajan en EncPayload; los planos se vacían. Si no hay
// pública o el sellado falla, se cae al FALLBACK CLARO (§10.H): los sensibles viajan planos y el mTLS
// sigue protegiendo el canal (nunca se falla el reenvío por esto). Los NO sensibles (from/ts_unix/
// wa_message_id/is_group/addressing_mode) van en claro SIEMPRE. El sellado ocurre ANTES de encolar, así
// que el outbox guarda ya los sensibles cifrados (durabilidad zero-knowledge).
func (s *sessionSink) Deliver(_ context.Context, evt domain.InboundEvent) error {
	fromPN, fromLID := deriveContactRefs(evt.Sender, evt.SenderAlt)
	in := &cloudlinkv1.IncomingMessage{
		From:           evt.Sender, // se mantiene (compat, ADR-0005)
		Text:           evt.Text,
		TsUnix:         evt.Timestamp.Unix(),
		WaMessageId:    evt.MessageID,
		IsGroup:        evt.IsGroup,
		PushName:       evt.PushName,
		FromPn:         fromPN,
		FromLid:        fromLID,
		AddressingMode: evt.AddressingMode,
		Intent:         classifiedIntentToProto(evt.Intent),
	}
	s.sealSensitive(in)
	msg := &cloudlinkv1.EdgeToCloud{
		CommandId: uuid.NewString(),
		SessionId: s.sessionID,
		Payload:   &cloudlinkv1.EdgeToCloud_Incoming{Incoming: in},
	}
	s.a.forward(msg, s.sessionID, app.OutboxKindIncoming, evt.MessageID)
	return nil
}

// sealSensitive sella los campos SENSIBLES del IncomingMessage hacia la pública de cifrado de la nube
// (Plan 011 §6.3/§10.E). Muta in EN SITIO: si el sellado tiene éxito, puebla in.EncPayload y vacía los
// planos sensibles (text/push_name/from_pn/from_lid). Si no hay pública (nil) o el sellado falla, deja
// los planos como están (FALLBACK CLARO §10.H) y NO propaga el error: el reenvío nunca se aborta por
// esto (el mTLS ya protege el canal). Los campos NO sensibles no se tocan.
func (s *sessionSink) sealSensitive(in *cloudlinkv1.IncomingMessage) {
	if s.cloudEncPub == nil {
		return // fallback claro (§10.H): sin pública de cifrado, los sensibles viajan planos
	}
	sp := &cloudlinkv1.SensitivePayload{
		Text:     in.GetText(),
		PushName: in.GetPushName(),
		FromPn:   in.GetFromPn(),
		FromLid:  in.GetFromLid(),
		// La intención LLM (Plan 029) es SENSIBLE: sus params pueden llevar texto literal del cliente. Con
		// sellado activo viaja DENTRO del sobre; el espejo claro (in.Intent) se vacía abajo.
		Intent: in.GetIntent(),
	}
	raw, err := proto.Marshal(sp)
	if err != nil {
		s.a.log.Warn("CloudLink: fallo al serializar SensitivePayload; fallback claro (§10.H)", "error", err)
		return
	}
	sealed, err := envelope.SealFor(s.cloudEncPub, raw)
	if err != nil {
		s.a.log.Warn("CloudLink: fallo al sellar SensitivePayload; fallback claro (§10.H)", "error", err)
		return
	}
	in.EncPayload = sealed
	in.Text, in.PushName, in.FromPn, in.FromLid = "", "", "", "" // no viajan en claro
	in.Intent = nil                                              // la intención va sellada, no en el espejo claro
}

// classifiedIntentToProto mapea la intención de dominio (Plan 029) al tipo del contrato CloudLink. Devuelve
// nil si no hay intención (el caso normal: la mayoría de mensajes no clasifican), de modo que el campo del
// proto queda vacío. Confidence pasa de float64 (dominio) a float32 (proto).
func classifiedIntentToProto(ci *domain.ClassifiedIntent) *cloudlinkv1.ClassifiedIntent {
	if ci == nil {
		return nil
	}
	return &cloudlinkv1.ClassifiedIntent{
		Intent:        ci.Name,
		Params:        ci.Params,
		Confidence:    float32(ci.Confidence),
		ConfigVersion: ci.ConfigVersion,
	}
}

// SendReceipt sube al cloud un ACUSE (delivered/read) de un mensaje SALIENTE por el ÚNICO stream del
// Edge, como EdgeToCloud{MessageReceipt} (Plan 013 §8). Es el análogo, para acuses, de sessionSink.
// Deliver (entrantes): lo cablea el wiring al SetReceiptHandler del gateway de cada sesión. El evt trae
// el session_id ya etiquetado (por-sesión, patrón mux.SinkFor) y el command_id llega correlacionado
// (vacío si no se correlacionó: sube como estado crudo por message_ids, no rompe). ZERO-KNOWLEDGE /
// §10.G: solo viajan message_ids/status/timestamp/session_id/command_id — nada de contenido/PII. Si no
// hay stream vivo o el envío falla, con outbox durable (Plan 027 T2) el acuse se ENCOLA y se drena en
// orden al reconectar; sin outbox conserva el best-effort previo (se descarta), sin tumbar la escucha.
func (a *Adapter) SendReceipt(commandID string, evt domain.ReceiptEvent) {
	r := &cloudlinkv1.MessageReceipt{
		SessionId:  evt.SessionID,
		MessageIds: evt.MessageIDs,
		Status:     receiptStatusToProto(evt.Status),
		Timestamp:  evt.Timestamp.Unix(),
		CommandId:  commandID,
	}
	msg := &cloudlinkv1.EdgeToCloud{
		CommandId: uuid.NewString(),
		SessionId: evt.SessionID,
		Payload:   &cloudlinkv1.EdgeToCloud_Receipt{Receipt: r},
	}
	a.forward(msg, evt.SessionID, app.OutboxKindReceipt, "")
}

// receiptStatusToProto mapea el estado de dominio del acuse al enum del contrato (Plan 013 §10.A):
// delivered→DELIVERED, read→READ; cualquier otro (no debería llegar: el enum de dominio es cerrado) cae
// a UNSPECIFIED por seguridad.
func receiptStatusToProto(s domain.ReceiptStatus) cloudlinkv1.ReceiptStatus {
	switch s {
	case domain.ReceiptDelivered:
		return cloudlinkv1.ReceiptStatus_RECEIPT_STATUS_DELIVERED
	case domain.ReceiptRead:
		return cloudlinkv1.ReceiptStatus_RECEIPT_STATUS_READ
	default:
		return cloudlinkv1.ReceiptStatus_RECEIPT_STATUS_UNSPECIFIED
	}
}

// Servidores JID de WhatsApp usados para clasificar un remitente como número o LID (Plan 010 §9).
// Se replican como constantes locales (no se importa go.mau.fi/whatsmeow en este adaptador de
// transporte) — los valores son los mismos que types.DefaultUserServer y types.HiddenUserServer.
const (
	serverPN  = "s.whatsapp.net" // número real → kind phone_e164
	serverLID = "lid"            // LID oculto  → kind wa_lid
)

// deriveContactRefs deriva el número (from_pn, E.164 sin '+') y el LID (from_lid) del remitente a
// partir de su JID principal (sender) y su alterno (senderAlt) — ambos en formato ".String()"
// (`user[.agent][:device]@server`). La clasificación número-vs-LID es por SERVER del JID
// (DefaultUserServer vs HiddenUserServer, Plan 010 §9), no por el orden: da igual cuál sea el
// principal. La normalización se hace tomando la user-part (se descarta server y device/agent).
//
// Tolerancia (Plan 010 §10.H): si senderAlt viene vacío (whatsmeow aún no aprendió el mapeo), solo
// se puebla el lado conocido; el otro queda "" y NO se falla. Un JID de server desconocido (grupo,
// broadcast, …) simplemente no puebla ninguno de los dos campos de identidad.
func deriveContactRefs(sender, senderAlt string) (fromPN, fromLID string) {
	for _, jid := range [2]string{sender, senderAlt} {
		if jid == "" {
			continue
		}
		user := jidUser(jid)
		switch jidServer(jid) {
		case serverPN:
			fromPN = user
		case serverLID:
			fromLID = user
		}
	}
	return fromPN, fromLID
}

// jidUser devuelve la user-part normalizada de un JID `user[.agent][:device]@server`: sin el server
// y sin el sufijo de dispositivo/agente. Para un número queda el E.164 sin '+'; para un LID, su id.
func jidUser(jid string) string {
	if i := strings.IndexByte(jid, '@'); i >= 0 {
		jid = jid[:i]
	}
	if i := strings.IndexAny(jid, ".:"); i >= 0 {
		jid = jid[:i]
	}
	return jid
}

// jidServer devuelve el server de un JID (lo que sigue al último '@'), o "" si no lo trae.
func jidServer(jid string) string {
	if i := strings.LastIndexByte(jid, '@'); i >= 0 {
		return jid[i+1:]
	}
	return ""
}

// Run mantiene el único stream Connect vivo: conecta, recibe comandos (demux por session_id), late, y
// reconecta con backoff + jitter ante cualquier caída. BLOQUEA hasta que ctx se cancele (devuelve nil
// al cancelar limpio).
func (a *Adapter) Run(ctx context.Context) error {
	// Siembra el guard de orden del outbox desde la BD (Plan 027 T2): si un reinicio dejó backlog, las
	// sesiones afectadas arrancan como "pendientes" para que un evento nuevo no adelante al backlog al
	// reconectar. No-op sin outbox.
	a.recomputePending(ctx)
	delay := a.baseDelay
	for {
		if ctx.Err() != nil {
			return nil
		}
		connected, err := a.runOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if connected {
			delay = a.baseDelay // reconexión exitosa: reinicia el retroceso
			a.log.Warn("CloudLink: stream caído, reconectando", "error", err)
		} else {
			a.log.Warn("CloudLink: conexión fallida, reintentando", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(withJitter(delay)):
		}
		delay = nextDelay(delay, a.maxDelay)
	}
}

// runOnce abre el stream, lo atiende hasta que cae o ctx se cancela. Devuelve connected=true si el
// stream llegó a establecerse (para que Run reinicie el backoff).
func (a *Adapter) runOnce(ctx context.Context) (bool, error) {
	cl, err := client.New(ctx, a.cc)
	if err != nil {
		return false, err
	}
	a.setClient(cl)
	defer a.setClient(nil)

	// Heartbeat inicial de CADA sesión registrada: ancla todas las sesiones en cuanto el stream revive.
	a.heartbeatAll(cl)

	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go a.heartbeatLoop(hbCtx, cl)

	// Drenaje del outbox durable (Plan 027 T2, H2): al conectar reenvía el backlog acumulado durante la
	// caída y sigue drenando periódicamente mientras el stream viva (hbCtx se cancela al caer). No-op sin
	// outbox configurado.
	go a.drainLoop(hbCtx, cl)

	// Despacho CONCURRENTE por session_id (Plan 027 T1, cierra H1/H7): en vez de ejecutar handleCommand
	// síncrono en este loop —lo que dejaba que una operación lenta de una sesión congelara la recepción de
	// TODAS— se encola en la cola de su sesión y un worker por session_id la procesa en paralelo con
	// deadline por operación. shutdown drena/cancela al caer el stream o apagarse el agente (sin fugas).
	disp := newCommandDispatcher(ctx, a, cl, a.cmdTimeout, a.cmdQueueSize)
	defer disp.shutdown()

	for {
		select {
		case <-ctx.Done():
			return true, nil
		case c2e, ok := <-cl.Received():
			if !ok {
				return true, errors.New("cloudlink: stream cerrado por el servidor")
			}
			disp.dispatch(c2e)
		}
	}
}

// handleCommand DEMULTIPLEXA un comando cloud->edge: resuelve la sesión por session_id y, si existe,
// despacha según su payload (oneof). Un comando para un session_id desconocido se loguea y se ignora.
func (a *Adapter) handleCommand(ctx context.Context, cl *client.Client, c2e *cloudlinkv1.CloudToEdge) {
	sid := c2e.GetSessionId()
	// ConfigUpdate (Plan 029 · T10 / ADR-0021) es config del EDGE por kind, NO de una sesión: se atiende
	// ANTES de resolver la sesión (aplica global) y SIEMPRE se Ack-ea, aunque el session_id que la etiqueta
	// no esté registrado. No pasa por el gate de lease (no es una operación de WhatsApp).
	if cu := c2e.GetConfigUpdate(); cu != nil {
		a.handleConfigUpdate(ctx, cl, c2e.GetCommandId(), cu)
		return
	}
	e := a.entry(sid)
	if e == nil {
		a.log.Warn("CloudLink: comando para session_id desconocido (ignorado)",
			"session_id", sid, "command_id", c2e.GetCommandId())
		return
	}
	switch {
	case c2e.GetSendText() != nil:
		a.handleSendText(ctx, cl, sid, e, c2e.GetCommandId(), c2e.GetSendText())
	case c2e.GetSendMedia() != nil:
		a.handleSendMedia(ctx, cl, sid, e, c2e.GetCommandId(), c2e.GetSendMedia())
	case c2e.GetLeaseUpdate() != nil:
		a.handleLeaseUpdate(sid, e, c2e.GetLeaseUpdate())
	case c2e.GetPing() != nil:
		a.send(cl, &cloudlinkv1.EdgeToCloud{
			CommandId: c2e.GetCommandId(),
			SessionId: sid,
			Payload:   &cloudlinkv1.EdgeToCloud_Pong{Pong: &cloudlinkv1.Pong{Nonce: c2e.GetPing().GetNonce()}},
		})
	default:
		a.log.Warn("CloudLink: comando sin payload soportado (ignorado)",
			"session_id", sid, "command_id", c2e.GetCommandId())
	}
}

// handleSendText aplica el gate de lease de ESA sesión y, si procede, despacha el texto vía su sendFunc,
// respondiendo con Ack. Si el lease de la sesión no está vigente, NO invoca el sendFunc (kill-switch) y
// responde Ack{ok=false} — sin afectar a las demás sesiones.
func (a *Adapter) handleSendText(ctx context.Context, cl *client.Client, sid string, e *sessionEntry, cmdID string, st *cloudlinkv1.SendText) {
	if e.validator != nil && !e.validator.CanOperate(e.hasDEK()) {
		a.log.Warn("CloudLink: SendText BLOQUEADO por lease no vigente (kill-switch)",
			"command_id", cmdID, "session_id", sid)
		a.ack(cl, sid, cmdID, false, "lease no vigente")
		return
	}
	if err := e.sendFunc(ctx, cmdID, st.GetTo(), st.GetText()); err != nil {
		a.log.Error("CloudLink: SendText falló al despachar", "command_id", cmdID, "session_id", sid, "error", err)
		a.ack(cl, sid, cmdID, false, err.Error())
		return
	}
	a.ack(cl, sid, cmdID, true, "")
}

// handleSendMedia aplica el gate de lease de ESA sesión (idéntico a handleSendText) y, si procede, despacha
// el ARCHIVO vía su sendMediaFunc (Plan 017 §7), respondiendo con Ack. Si el lease no está vigente NO invoca
// el emisor (kill-switch) y responde Ack{ok=false}. Si la sesión no tiene emisor de media (sendMediaFunc
// nil), responde Ack{ok=false} sin paniquear. El discriminador MediaKind del proto se mapea a "document"/
// "image"; la presigned URL la DESCARGA el despachador (GET sin credenciales), no viaja binario por gRPC.
func (a *Adapter) handleSendMedia(ctx context.Context, cl *client.Client, sid string, e *sessionEntry, cmdID string, sm *cloudlinkv1.SendMedia) {
	if e.validator != nil && !e.validator.CanOperate(e.hasDEK()) {
		a.log.Warn("CloudLink: SendMedia BLOQUEADO por lease no vigente (kill-switch)",
			"command_id", cmdID, "session_id", sid)
		a.ack(cl, sid, cmdID, false, "lease no vigente")
		return
	}
	if e.sendMediaFunc == nil {
		a.log.Warn("CloudLink: SendMedia sin emisor de media configurado para la sesión (ignorado)",
			"command_id", cmdID, "session_id", sid)
		a.ack(cl, sid, cmdID, false, "media no soportada por esta sesión")
		return
	}
	kind := mediaKindToString(sm.GetKind())
	if err := e.sendMediaFunc(ctx, cmdID, sm.GetTo(), sm.GetPresignedUrl(), sm.GetFilename(), sm.GetMime(), kind, sm.GetCaption()); err != nil {
		a.log.Error("CloudLink: SendMedia falló al despachar", "command_id", cmdID, "session_id", sid, "error", err)
		a.ack(cl, sid, cmdID, false, err.Error())
		return
	}
	a.ack(cl, sid, cmdID, true, "")
}

// mediaKindToString mapea el discriminador MediaKind del contrato al string del despachador del Edge
// (Plan 017 §6): IMAGE→"image"; DOCUMENT y UNSPECIFIED→"document" (caso por defecto: PDF). El mime no
// basta para elegir la rama Document vs Image, por eso el discriminador explícito.
func mediaKindToString(k cloudlinkv1.MediaKind) string {
	switch k {
	case cloudlinkv1.MediaKind_MEDIA_KIND_IMAGE:
		return "image"
	default:
		return "document"
	}
}

// handleConfigUpdate atiende un ConfigUpdate empujado por la nube (Plan 029 · T10 / ADR-0021): delega en el
// applier (persistencia last-known-good + validación por kind + notificación en caliente al clasificador) y
// responde Ack. Sin applier cableado (Intent off / diagnóstico), Ack-ea TOLERANTE sin persistir (kinds
// futuros). El applier resuelve kind-desconocido/versión-duplicada/blob-inválido con log devolviendo nil ⇒
// Ack{ok=true}; solo un fallo de PERSISTENCIA devuelve error ⇒ Ack{ok=false} (el Cloud reempuja al reconectar).
func (a *Adapter) handleConfigUpdate(ctx context.Context, cl *client.Client, cmdID string, cu *cloudlinkv1.ConfigUpdate) {
	sid := cu.GetSessionId()
	if a.configApplier == nil {
		a.log.Info("CloudLink: ConfigUpdate recibido sin applier (Intent off); Ack tolerante sin persistir",
			"kind", cu.GetKind(), "version", cu.GetVersion(), "session_id", sid, "command_id", cmdID)
		a.ack(cl, sid, cmdID, true, "")
		return
	}
	if err := a.configApplier.Apply(ctx, cu.GetKind(), cu.GetVersion(), cu.GetPayload()); err != nil {
		a.log.Error("CloudLink: ConfigUpdate no persistido (se reintentará al reconectar)",
			"kind", cu.GetKind(), "version", cu.GetVersion(), "session_id", sid, "command_id", cmdID, "error", err)
		a.ack(cl, sid, cmdID, false, err.Error())
		return
	}
	a.ack(cl, sid, cmdID, true, "")
}

// handleLeaseUpdate aplica un LeaseUpdate firmado al Validator de ESA sesión (verifica firma, expiración,
// counter). El estado de lease es por sesión: un LeaseUpdate de una sesión no toca el de las otras.
func (a *Adapter) handleLeaseUpdate(sid string, e *sessionEntry, lu *cloudlinkv1.LeaseUpdate) {
	if e.validator == nil {
		a.log.Warn("CloudLink: LeaseUpdate recibido sin Validator configurado (ignorado)", "session_id", sid)
		return
	}
	if err := e.validator.Apply(lu); err != nil {
		a.log.Warn("CloudLink: LeaseUpdate rechazado", "session_id", sid, "error", err)
		return
	}
	if lu.GetRevoked() {
		a.log.Warn("CloudLink: lease REVOCADO (kill-switch activo): envíos bloqueados", "session_id", sid)
	} else {
		a.log.Info("CloudLink: lease renovado/aplicado", "session_id", sid, "expires_unix", lu.GetExpiresUnix())
	}
}

func (a *Adapter) heartbeatLoop(ctx context.Context, cl *client.Client) {
	t := time.NewTicker(a.hbInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.heartbeatAll(cl)
		}
	}
}

// heartbeatAll emite un Heartbeat por CADA sesión registrada (cada uno etiquetado con su session_id y su
// contador propio). Snapshotea el registro bajo lock y emite fuera del lock.
func (a *Adapter) heartbeatAll(cl *client.Client) {
	type pending struct {
		id string
		e  *sessionEntry
	}
	a.mu.Lock()
	pend := make([]pending, 0, len(a.sessions))
	for id, e := range a.sessions {
		pend = append(pend, pending{id, e})
	}
	a.mu.Unlock()
	for _, p := range pend {
		a.sendHeartbeat(cl, p.id, p.e)
	}
}

// heartbeatStateFor es el ÚNICO punto de mapeo Edge→Cloud del ESTADO (Plan 022 T4, design §4/§10.D):
// traduce el estado de NEGOCIO/ciclo de vida del device (domain.SessionState) al estado de LÍNEA del
// Heartbeat (cloudlinkv1.SessionState) que el Cloud usa para DERIVAR fleet_sessions.state. La tabla
// canónica (con los estados que NO emiten latido) vive en el doc de domain.SessionState; aquí solo los
// que llegan a viajar por el wire:
//
//	domain.SessionState | wire Heartbeat.State        | Cloud fleet_sessions.state (derivado)
//	--------------------+-----------------------------+--------------------------------------
//	active              | SESSION_STATE_UNSPECIFIED   | online   (liveness: latido presente)
//	loggedout           | SESSION_STATE_LOGGED_OUT    | loggedout / zombie (WhatsApp cerró)
//	(pairing/suspended) | —no emiten latido—          | offline  (derivado por AUSENCIA)
//
// El proto (v0.6.0) solo modela dos valores de línea explícitos: `online`/`offline` NO son estados de
// línea, los deriva el Cloud por presencia/ausencia de latidos. Por eso todo estado distinto de loggedout
// que llegase a emitir un latido se reporta como UNSPECIFIED (liveness normal); pairing/suspended no
// emiten latido (sin socket / listener parado) y el Cloud los ve offline. El estado es metadato de
// NEGOCIO: JAMÁS material criptográfico (zero-knowledge intacto).
func heartbeatStateFor(s domain.SessionState) cloudlinkv1.SessionState {
	if s == domain.SessionStateLoggedOut {
		return cloudlinkv1.SessionState_SESSION_STATE_LOGGED_OUT
	}
	return cloudlinkv1.SessionState_SESSION_STATE_UNSPECIFIED
}

func (a *Adapter) sendHeartbeat(cl *client.Client, sessionID string, e *sessionEntry) {
	ctr := e.leaseCtr.Add(1)
	a.send(cl, &cloudlinkv1.EdgeToCloud{
		CommandId: uuid.NewString(),
		SessionId: sessionID,
		// Plan 020 T2: cada latido lleva el número/JID propio para que el Cloud sepa a qué número pertenece
		// la sesión al marcarla online (anti-self-loop). Un latido periódico corresponde a una sesión
		// `active` en el Edge → estado de línea UNSPECIFIED (liveness normal) vía el mapeo unificado (T4).
		Payload: &cloudlinkv1.EdgeToCloud_Heartbeat{Heartbeat: &cloudlinkv1.Heartbeat{
			LeaseCounter: ctr,
			SelfPn:       e.selfPN,
			SelfJid:      e.selfJID,
			State:        heartbeatStateFor(domain.SessionStateActive),
		}},
	})
}

// SendLoggedOut propaga al Cloud que WhatsApp CERRÓ la sesión (events.LoggedOut, Plan 020 T3): emite un
// Heartbeat con State=LOGGED_OUT por el ÚNICO stream del Edge para que el Cloud pueda marcar la sesión como
// ZOMBIE (distinto de un offline por caída de red). Se invoca desde el listener ANTES del teardown local,
// mientras el stream aún está vivo. Reusa el mecanismo de heartbeat (currentClient() + send); si no hay
// stream vivo lo registra y descarta (el Cloud lo detectará por ausencia de latidos). Incluye el número/JID
// propio si se conoce (útil para diagnóstico y correlación del zombie).
func (a *Adapter) SendLoggedOut(sessionID string) {
	cl := a.currentClient()
	if cl == nil {
		a.log.Warn("CloudLink desconectado: LoggedOut no propagado (el Cloud lo verá por ausencia de latidos)",
			"session_id", sessionID)
		return
	}
	var selfPN, selfJID string
	var ctr int64
	if e := a.entry(sessionID); e != nil {
		selfPN, selfJID = e.selfPN, e.selfJID
		ctr = e.leaseCtr.Add(1)
	}
	a.log.Warn("CloudLink: propagando LoggedOut al Cloud (sesión zombie)", "session_id", sessionID)
	a.send(cl, &cloudlinkv1.EdgeToCloud{
		CommandId: uuid.NewString(),
		SessionId: sessionID,
		Payload: &cloudlinkv1.EdgeToCloud_Heartbeat{Heartbeat: &cloudlinkv1.Heartbeat{
			LeaseCounter: ctr,
			SelfPn:       selfPN,
			SelfJid:      selfJID,
			// devices.state='loggedout' → estado de línea LOGGED_OUT (zombie) vía el mapeo unificado (T4).
			State: heartbeatStateFor(domain.SessionStateLoggedOut),
		}},
	})
}

func (a *Adapter) ack(cl *client.Client, sessionID, ackedCmdID string, ok bool, errMsg string) {
	a.send(cl, &cloudlinkv1.EdgeToCloud{
		CommandId: uuid.NewString(),
		SessionId: sessionID,
		Payload: &cloudlinkv1.EdgeToCloud_Ack{Ack: &cloudlinkv1.Ack{
			AckedCommandId: ackedCmdID,
			Ok:             ok,
			Error:          errMsg,
		}},
	})
}

func (a *Adapter) send(cl *client.Client, msg *cloudlinkv1.EdgeToCloud) {
	if err := cl.Send(msg); err != nil {
		a.log.Warn("CloudLink: fallo al enviar por el stream", "error", err)
	}
}

func (a *Adapter) setClient(cl *client.Client) {
	a.mu.Lock()
	a.cl = cl
	a.mu.Unlock()
}

func (a *Adapter) currentClient() *client.Client {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cl
}

// nextDelay duplica delay saturado en max (backoff exponencial).
func nextDelay(delay, max time.Duration) time.Duration {
	d := delay * 2
	if d > max {
		return max
	}
	return d
}

// withJitter añade hasta un 20% de jitter aleatorio para desincronizar reconexiones masivas.
func withJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d + time.Duration(rand.Int63n(int64(d)/5+1))
}
