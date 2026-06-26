package cloudlink

// Adapter es el conducto CloudLink REAL del Edge (pieza 02), sustituto productivo del LogSink del
// spike. Cumple dos roles a la vez:
//
//   1. Puerto de SALIDA app.InboundSink: cada mensaje entrante ya descifrado (domain.InboundEvent)
//      se mapea a cloudlinkv1.IncomingMessage y se reenvía a la nube por el stream Connect
//      (edge->cloud). SOLO viaja CONTENIDO DE NEGOCIO (remitente, texto, timestamp, id de WhatsApp):
//      la DEK, el store cifrado y las llaves Signal JAMÁS cruzan el cable (ADR-0007, zero-knowledge).
//
//   2. Receptor de comandos cloud->edge: despacha SendText (vía el SendFunc inyectado, que el wiring
//      conecta a app.Send), aplica LeaseUpdate al *lease.Validator (kill-switch anti-clon) y responde
//      Ping con Pong. Emite Heartbeat periódico (lease_counter incremental) para anclar la renovación.
//
// GATE DE LEASE (ADR-0007, gate 2-de-2 a nivel de OPERACIÓN): si hay un Validator, antes de despachar
// un SendText se exige Validator.CanOperate(hasDEK). Si el lease no está vigente (revocado, expirado o
// nunca aplicado), NO se invoca el SendFunc y se responde Ack{ok=false, error="lease no vigente"}.
// FOLLOW-UP: la integración profunda con el desbloqueo del store (2-de-2 real en KeyCustody) queda
// pendiente; aquí el lease gatea a nivel de operación, no descifra/bloquea el .db.
//
// RECONEXIÓN: backoff exponencial + jitter (la política pura whatsmeow.Backoff es package-private de
// ese adaptador y sin jitter; aquí se usa una propia, decoupleada). Al reconectar se reabre el stream.
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

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-cloudlink/client"
	"github.com/EduGoGroup/wapp-cloudlink/lease"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-shared/logger"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// SendFunc es el callback de envío que el wiring conecta al caso de uso app.Send.Run. El adaptador lo
// invoca al recibir un comando SendText (tras pasar el gate de lease).
type SendFunc = func(ctx context.Context, to, text string) error

// Adapter implementa app.InboundSink y gestiona el stream Connect contra la nube.
type Adapter struct {
	cc        grpc.ClientConnInterface
	sessionID string
	sendFunc  SendFunc
	validator *lease.Validator // opcional (nil => sin gate de lease, p.ej. dev sin clave pública)
	hasDEK    func() bool       // proveedor del booleano del gate 2-de-2 (p.ej. custody.Exists)
	log       logger.Logger

	hbInterval time.Duration
	baseDelay  time.Duration
	maxDelay   time.Duration

	mu       sync.Mutex
	cl       *client.Client // stream activo; nil mientras está desconectado
	leaseCtr atomic.Int64
}

var _ app.InboundSink = (*Adapter)(nil)

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

// NewAdapter construye el adaptador CloudLink sobre una conexión gRPC ya establecida (cc).
//   - sessionID: identifica la sesión/teléfono dentro del Edge (multiplexado, ADR-0008).
//   - sendFunc: callback de envío (lo conecta el wiring a app.Send.Run).
//   - validator: opcional; si != nil se aplica el gate de lease antes de despachar SendText.
//   - hasDEK: proveedor del booleano del gate 2-de-2 (si nil, se asume true).
//   - log: logger estructurado (nunca imprime DEK ni secretos).
func NewAdapter(cc grpc.ClientConnInterface, sessionID string, sendFunc SendFunc, validator *lease.Validator, hasDEK func() bool, log logger.Logger, opts ...Option) *Adapter {
	if hasDEK == nil {
		hasDEK = func() bool { return true }
	}
	if log == nil {
		log = logger.Default()
	}
	a := &Adapter{
		cc:         cc,
		sessionID:  sessionID,
		sendFunc:   sendFunc,
		validator:  validator,
		hasDEK:     hasDEK,
		log:        log,
		hbInterval: 30 * time.Second,
		baseDelay:  1 * time.Second,
		maxDelay:   60 * time.Second,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Deliver implementa app.InboundSink: mapea el InboundEvent a IncomingMessage y lo reenvía por el
// stream. ZERO-KNOWLEDGE: solo viaja contenido de negocio; no hay campo alguno para la DEK ni el store.
// Si no hay stream vivo, lo registra y devuelve nil (no tumba el socket; outbox durable = follow-up).
func (a *Adapter) Deliver(_ context.Context, evt domain.InboundEvent) error {
	cl := a.currentClient()
	if cl == nil {
		a.log.Warn("CloudLink desconectado: InboundEvent no reenviado (follow-up: outbox durable)",
			"wa_message_id", evt.MessageID, "session_id", a.sessionID)
		return nil
	}
	msg := &cloudlinkv1.EdgeToCloud{
		CommandId: uuid.NewString(),
		SessionId: a.sessionID,
		Payload: &cloudlinkv1.EdgeToCloud_Incoming{Incoming: &cloudlinkv1.IncomingMessage{
			From:        evt.Sender,
			Text:        evt.Text,
			TsUnix:      evt.Timestamp.Unix(),
			WaMessageId: evt.MessageID,
			IsGroup:     evt.IsGroup,
		}},
	}
	if err := cl.Send(msg); err != nil {
		a.log.Warn("CloudLink: fallo al reenviar InboundEvent (se descarta; follow-up: outbox)",
			"error", err, "wa_message_id", evt.MessageID)
		return nil
	}
	return nil
}

// Run mantiene el stream Connect vivo: conecta, recibe comandos, late, y reconecta con backoff +
// jitter ante cualquier caída. BLOQUEA hasta que ctx se cancele (devuelve nil al cancelar limpio).
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

// runOnce abre un stream, lo atiende hasta que cae o ctx se cancela. Devuelve connected=true si el
// stream llegó a establecerse (para que Run reinicie el backoff).
func (a *Adapter) runOnce(ctx context.Context) (bool, error) {
	cl, err := client.New(ctx, a.cc)
	if err != nil {
		return false, err
	}
	a.setClient(cl)
	defer a.setClient(nil)

	// Heartbeat inicial: anuncia presencia en cuanto el stream está vivo (ancla de la sesión).
	a.sendHeartbeat(cl)

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

// handleCommand despacha un comando cloud->edge según su payload (oneof).
func (a *Adapter) handleCommand(ctx context.Context, cl *client.Client, c2e *cloudlinkv1.CloudToEdge) {
	switch {
	case c2e.GetSendText() != nil:
		a.handleSendText(ctx, cl, c2e.GetCommandId(), c2e.GetSendText())
	case c2e.GetLeaseUpdate() != nil:
		a.handleLeaseUpdate(c2e.GetLeaseUpdate())
	case c2e.GetPing() != nil:
		a.send(cl, &cloudlinkv1.EdgeToCloud{
			CommandId: c2e.GetCommandId(),
			SessionId: a.sessionID,
			Payload:   &cloudlinkv1.EdgeToCloud_Pong{Pong: &cloudlinkv1.Pong{Nonce: c2e.GetPing().GetNonce()}},
		})
	default:
		a.log.Warn("CloudLink: comando sin payload soportado (ignorado)", "command_id", c2e.GetCommandId())
	}
}

// handleSendText aplica el gate de lease y, si procede, despacha el texto vía el SendFunc, respondiendo
// con Ack. Si el lease no está vigente, NO invoca el SendFunc (kill-switch) y responde Ack{ok=false}.
func (a *Adapter) handleSendText(ctx context.Context, cl *client.Client, cmdID string, st *cloudlinkv1.SendText) {
	if a.validator != nil && !a.validator.CanOperate(a.hasDEK()) {
		a.log.Warn("CloudLink: SendText BLOQUEADO por lease no vigente (kill-switch)",
			"command_id", cmdID, "session_id", a.sessionID)
		a.ack(cl, cmdID, false, "lease no vigente")
		return
	}
	if err := a.sendFunc(ctx, st.GetTo(), st.GetText()); err != nil {
		a.log.Error("CloudLink: SendText falló al despachar", "command_id", cmdID, "error", err)
		a.ack(cl, cmdID, false, err.Error())
		return
	}
	a.ack(cl, cmdID, true, "")
}

// handleLeaseUpdate aplica un LeaseUpdate firmado al Validator (verifica firma, expiración, counter).
func (a *Adapter) handleLeaseUpdate(lu *cloudlinkv1.LeaseUpdate) {
	if a.validator == nil {
		a.log.Warn("CloudLink: LeaseUpdate recibido sin Validator configurado (ignorado)")
		return
	}
	if err := a.validator.Apply(lu); err != nil {
		a.log.Warn("CloudLink: LeaseUpdate rechazado", "error", err)
		return
	}
	if lu.GetRevoked() {
		a.log.Warn("CloudLink: lease REVOCADO (kill-switch activo): envíos bloqueados")
	} else {
		a.log.Info("CloudLink: lease renovado/aplicado", "expires_unix", lu.GetExpiresUnix())
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
			a.sendHeartbeat(cl)
		}
	}
}

func (a *Adapter) sendHeartbeat(cl *client.Client) {
	ctr := a.leaseCtr.Add(1)
	a.send(cl, &cloudlinkv1.EdgeToCloud{
		CommandId: uuid.NewString(),
		SessionId: a.sessionID,
		Payload:   &cloudlinkv1.EdgeToCloud_Heartbeat{Heartbeat: &cloudlinkv1.Heartbeat{LeaseCounter: ctr}},
	})
}

func (a *Adapter) ack(cl *client.Client, ackedCmdID string, ok bool, errMsg string) {
	a.send(cl, &cloudlinkv1.EdgeToCloud{
		CommandId: uuid.NewString(),
		SessionId: a.sessionID,
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
