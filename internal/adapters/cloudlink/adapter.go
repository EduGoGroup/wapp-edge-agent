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

// sessionEntry es el estado por sesión dentro del multiplex: su emisor por cliente vivo, su Validator de
// lease (propio) y su contador de heartbeat. El Adapter lo crea en Register y lo descarta en Unregister.
type sessionEntry struct {
	sendFunc      SendFunc
	sendMediaFunc SendMediaFunc    // emisor de ARCHIVOS por cliente vivo (Plan 017); nil => media no soportada
	validator     *lease.Validator // nil => sin gate de lease para esta sesión
	hasDEK        func() bool      // proveedor del booleano del gate 2-de-2 (p.ej. custody.Exists)
	leaseCtr      atomic.Int64     // contador de heartbeat de ESTA sesión (ancla de renovación)
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

	mu       sync.Mutex
	cl       *client.Client           // stream activo; nil mientras está desconectado
	sessions map[string]*sessionEntry // registro multi-sesión por session_id
}

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
		cc:           cc,
		newValidator: newValidator,
		log:          log,
		hbInterval:   30 * time.Second,
		baseDelay:    1 * time.Second,
		maxDelay:     60 * time.Second,
		sessions:     make(map[string]*sessionEntry),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Register da de alta una sesión en el multiplex: send es su emisor de TEXTO por cliente vivo y sendMedia
// su emisor de ARCHIVOS (Plan 017; nil => media no soportada por esa sesión), ambos conectados por el
// wiring al cliente VIVO de la escucha de esa sesión; hasDEK reporta la presencia de la DEK (gate
// 2-de-2). El Adapter construye un Validator de lease PROPIO para la sesión (estado independiente). Si
// el stream ya está vivo, ancla la sesión con su Heartbeat inicial. Idempotente: re-registrar reemplaza
// la entrada (reinicia su estado de lease/contador).
func (a *Adapter) Register(sessionID string, send SendFunc, sendMedia SendMediaFunc, hasDEK func() bool) {
	if hasDEK == nil {
		hasDEK = func() bool { return true }
	}
	entry := &sessionEntry{sendFunc: send, sendMediaFunc: sendMedia, hasDEK: hasDEK}
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
// ZERO-KNOWLEDGE: solo viaja contenido de negocio; no hay campo alguno para la DEK ni el store. Si no
// hay stream vivo, lo registra y devuelve nil (no tumba el socket; outbox durable = follow-up).
//
// CIFRADO EN TRÁNSITO (Plan 011 §6.3/§10.E): si hay pública de cifrado de la nube (s.cloudEncPub),
// los campos SENSIBLES (text/push_name/from_pn/from_lid) se serializan como un SensitivePayload proto,
// se sellan con SealFor (X25519 anónimo) y viajan en EncPayload; los planos se vacían. Si no hay
// pública o el sellado falla, se cae al FALLBACK CLARO (§10.H): los sensibles viajan planos y el mTLS
// sigue protegiendo el canal (nunca se falla el reenvío por esto). Los NO sensibles (from/ts_unix/
// wa_message_id/is_group/addressing_mode) van en claro SIEMPRE.
func (s *sessionSink) Deliver(_ context.Context, evt domain.InboundEvent) error {
	cl := s.a.currentClient()
	if cl == nil {
		s.a.log.Warn("CloudLink desconectado: InboundEvent no reenviado (follow-up: outbox durable)",
			"wa_message_id", evt.MessageID, "session_id", s.sessionID)
		return nil
	}
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
	}
	s.sealSensitive(in)
	msg := &cloudlinkv1.EdgeToCloud{
		CommandId: uuid.NewString(),
		SessionId: s.sessionID,
		Payload:   &cloudlinkv1.EdgeToCloud_Incoming{Incoming: in},
	}
	if err := cl.Send(msg); err != nil {
		s.a.log.Warn("CloudLink: fallo al reenviar InboundEvent (se descarta; follow-up: outbox)",
			"error", err, "wa_message_id", evt.MessageID, "session_id", s.sessionID)
		return nil
	}
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
}

// SendReceipt sube al cloud un ACUSE (delivered/read) de un mensaje SALIENTE por el ÚNICO stream del
// Edge, como EdgeToCloud{MessageReceipt} (Plan 013 §8). Es el análogo, para acuses, de sessionSink.
// Deliver (entrantes): lo cablea el wiring al SetReceiptHandler del gateway de cada sesión. El evt trae
// el session_id ya etiquetado (por-sesión, patrón mux.SinkFor) y el command_id llega correlacionado
// (vacío si no se correlacionó: sube como estado crudo por message_ids, no rompe). ZERO-KNOWLEDGE /
// §10.G: solo viajan message_ids/status/timestamp/session_id/command_id — nada de contenido/PII. Si no
// hay stream vivo, lo registra y descarta (outbox durable = follow-up), sin tumbar la escucha.
func (a *Adapter) SendReceipt(commandID string, evt domain.ReceiptEvent) {
	cl := a.currentClient()
	if cl == nil {
		a.log.Warn("CloudLink desconectado: MessageReceipt no reenviado (follow-up: outbox durable)",
			"session_id", evt.SessionID, "status", string(evt.Status), "acked", len(evt.MessageIDs))
		return
	}
	r := &cloudlinkv1.MessageReceipt{
		SessionId:  evt.SessionID,
		MessageIds: evt.MessageIDs,
		Status:     receiptStatusToProto(evt.Status),
		Timestamp:  evt.Timestamp.Unix(),
		CommandId:  commandID,
	}
	a.send(cl, &cloudlinkv1.EdgeToCloud{
		CommandId: uuid.NewString(),
		SessionId: evt.SessionID,
		Payload:   &cloudlinkv1.EdgeToCloud_Receipt{Receipt: r},
	})
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

	for {
		select {
		case <-ctx.Done():
			return true, nil
		case c2e, ok := <-cl.Received():
			if !ok {
				return true, errors.New("cloudlink: stream cerrado por el servidor")
			}
			a.handleCommand(ctx, cl, c2e)
		}
	}
}

// handleCommand DEMULTIPLEXA un comando cloud->edge: resuelve la sesión por session_id y, si existe,
// despacha según su payload (oneof). Un comando para un session_id desconocido se loguea y se ignora.
func (a *Adapter) handleCommand(ctx context.Context, cl *client.Client, c2e *cloudlinkv1.CloudToEdge) {
	sid := c2e.GetSessionId()
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

func (a *Adapter) sendHeartbeat(cl *client.Client, sessionID string, e *sessionEntry) {
	ctr := e.leaseCtr.Add(1)
	a.send(cl, &cloudlinkv1.EdgeToCloud{
		CommandId: uuid.NewString(),
		SessionId: sessionID,
		Payload:   &cloudlinkv1.EdgeToCloud_Heartbeat{Heartbeat: &cloudlinkv1.Heartbeat{LeaseCounter: ctr}},
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
