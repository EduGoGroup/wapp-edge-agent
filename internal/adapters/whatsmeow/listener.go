// Package whatsmeow — listener de RECEPCIÓN 24/7 (RF-5/RF-6, design §5). Código NUEVO (no existe en
// EduGo, que deshabilitó la escucha): se construye desde cero sobre client.AddEventHandler.
//
// El Listener registra UN handler en el cliente y ENRUTA por tipo de evento:
//   - *events.Message      -> construye un domain.InboundEvent y lo entrega al InboundSink.
//   - *events.Connected    -> marca estado conectado y RESETEA el backoff.
//   - *events.Disconnected -> marca estado desconectado y AVANZA el backoff (whatsmeow auto-reconecta).
//   - *events.LoggedOut    -> marca la sesión CAÍDA (no se re-empareja automáticamente).
//
// La lógica de enrutado/mapeo vive en handleEvent(ctx, evt any), TESTEABLE con eventos sintéticos sin
// un *whatsmeow.Client real. Register() solo cablea handleEvent al AddEventHandler real (no se cubre
// en tests: requiere socket/red, por diseño).
package whatsmeow

import (
	"context"
	"sync"
	"time"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
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
}

// NewListener construye el listener con el sink y el logger dados y el backoff por defecto del spike.
func NewListener(sink app.InboundSink, log logger.Logger) *Listener {
	return &Listener{
		sink:    sink,
		log:     log,
		backoff: DefaultBackoff(),
		state:   StateDisconnected,
	}
}

// State devuelve el estado de conexión observado (para observabilidad/tests).
func (l *Listener) State() ConnState {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

// Register cablea handleEvent al AddEventHandler REAL del cliente whatsmeow. El ctx (vida de la
// sesión Listen) se propaga a cada entrega al sink. NO se cubre en tests (requiere un client real).
func (l *Listener) Register(ctx context.Context, client *wm.Client) uint32 {
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
	default:
		// Otros eventos (receipts, presencia, history sync, …) no son del alcance del spike.
	}
}

// onMessage mapea un *events.Message a domain.InboundEvent y lo entrega al sink. Un fallo de entrega
// se registra pero NO tumba la escucha (el socket sigue vivo).
func (l *Listener) onMessage(ctx context.Context, e *events.Message) {
	inbound := toInboundEvent(e)
	if err := l.sink.Deliver(ctx, inbound); err != nil {
		l.log.Error("listener: no se pudo entregar el evento entrante al sink",
			"error", err, "message_id", inbound.MessageID)
	}
}

// onConnected marca el estado conectado y resetea el backoff (reconexión exitosa).
func (l *Listener) onConnected() {
	l.mu.Lock()
	l.state = StateConnected
	l.backoff.Reset()
	l.mu.Unlock()
	l.log.Info("listener: socket conectado (escucha 24/7 activa)")
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

	l.log.Warn("listener: socket desconectado; whatsmeow reintentará (política de backoff)",
		"intento", attempt, "siguiente_delay", delay.String())
	if hook != nil {
		hook(attempt, delay)
	}
}

// onLoggedOut marca la sesión CAÍDA. NO se re-empareja automáticamente (requiere acción humana:
// escanear un QR nuevo). Se reporta para que el control/cloud lo sepa.
func (l *Listener) onLoggedOut(e *events.LoggedOut) {
	l.mu.Lock()
	l.state = StateLoggedOut
	l.mu.Unlock()
	l.log.Error("listener: sesión cerrada por WhatsApp (LoggedOut); requiere re-emparejar",
		"on_connect", e.OnConnect, "reason", e.Reason.String())
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
