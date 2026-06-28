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
	"sync"
	"sync/atomic"
	"time"

	"github.com/EduGoGroup/wapp-cloudlink/client"
	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-cloudlink/lease"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-shared/logger"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// SendFunc es el callback de envío que el wiring conecta al despachador del Edge (cliente vivo de la
// sesión). El adaptador lo invoca al recibir un comando SendText para ESA sesión (tras pasar el gate).
type SendFunc = func(ctx context.Context, to, text string) error

// ValidatorFactory construye un lease.Validator FRESCO (estado independiente) para UNA sesión, o nil si
// el gate de lease está desactivado (sin clave pública). Todas las sesiones del Edge comparten la MISMA
// clave pública del servidor, pero cada una mantiene su PROPIO estado de lease (lease por sesión,
// ADR-0016 §5): por eso es un factory y no un Validator compartido.
type ValidatorFactory func() *lease.Validator

// sessionEntry es el estado por sesión dentro del multiplex: su emisor por cliente vivo, su Validator de
// lease (propio) y su contador de heartbeat. El Adapter lo crea en Register y lo descarta en Unregister.
type sessionEntry struct {
	sendFunc  SendFunc
	validator *lease.Validator // nil => sin gate de lease para esta sesión
	hasDEK    func() bool      // proveedor del booleano del gate 2-de-2 (p.ej. custody.Exists)
	leaseCtr  atomic.Int64     // contador de heartbeat de ESTA sesión (ancla de renovación)
}

// Adapter gestiona el único stream Connect contra la nube y multiplexa N sesiones por session_id.
type Adapter struct {
	cc           grpc.ClientConnInterface
	newValidator ValidatorFactory // construye el Validator por sesión (nil => sin gate)
	log          logger.Logger

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

// Register da de alta una sesión en el multiplex: send es su emisor por cliente vivo (lo conecta el
// wiring al cliente VIVO de la escucha de esa sesión), hasDEK reporta la presencia de la DEK (gate
// 2-de-2). El Adapter construye un Validator de lease PROPIO para la sesión (estado independiente). Si
// el stream ya está vivo, ancla la sesión con su Heartbeat inicial. Idempotente: re-registrar reemplaza
// la entrada (reinicia su estado de lease/contador).
func (a *Adapter) Register(sessionID string, send SendFunc, hasDEK func() bool) {
	if hasDEK == nil {
		hasDEK = func() bool { return true }
	}
	entry := &sessionEntry{sendFunc: send, hasDEK: hasDEK}
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
	return &sessionSink{a: a, sessionID: sessionID}
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
	a         *Adapter
	sessionID string
}

var _ app.InboundSink = (*sessionSink)(nil)

// Deliver mapea el InboundEvent a IncomingMessage etiquetado con session_id y lo reenvía por el stream.
// ZERO-KNOWLEDGE: solo viaja contenido de negocio; no hay campo alguno para la DEK ni el store. Si no
// hay stream vivo, lo registra y devuelve nil (no tumba el socket; outbox durable = follow-up).
func (s *sessionSink) Deliver(_ context.Context, evt domain.InboundEvent) error {
	cl := s.a.currentClient()
	if cl == nil {
		s.a.log.Warn("CloudLink desconectado: InboundEvent no reenviado (follow-up: outbox durable)",
			"wa_message_id", evt.MessageID, "session_id", s.sessionID)
		return nil
	}
	msg := &cloudlinkv1.EdgeToCloud{
		CommandId: uuid.NewString(),
		SessionId: s.sessionID,
		Payload: &cloudlinkv1.EdgeToCloud_Incoming{Incoming: &cloudlinkv1.IncomingMessage{
			From:        evt.Sender,
			Text:        evt.Text,
			TsUnix:      evt.Timestamp.Unix(),
			WaMessageId: evt.MessageID,
			IsGroup:     evt.IsGroup,
		}},
	}
	if err := cl.Send(msg); err != nil {
		s.a.log.Warn("CloudLink: fallo al reenviar InboundEvent (se descarta; follow-up: outbox)",
			"error", err, "wa_message_id", evt.MessageID, "session_id", s.sessionID)
		return nil
	}
	return nil
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
	if err := e.sendFunc(ctx, st.GetTo(), st.GetText()); err != nil {
		a.log.Error("CloudLink: SendText falló al despachar", "command_id", cmdID, "session_id", sid, "error", err)
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
