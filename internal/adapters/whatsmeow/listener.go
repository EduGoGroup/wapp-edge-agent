// Package whatsmeow — listener de RECEPCIÓN 24/7 (RF-5/RF-6, design §5). Código NUEVO (no existe en
// EduGo, que deshabilitó la escucha): se construye desde cero sobre client.AddEventHandler.
//
// El Listener registra UN handler en el cliente y ENRUTA por tipo de evento:
//   - *events.Message      -> construye un domain.InboundEvent y lo entrega al InboundSink.
//   - *events.Connected    -> marca estado conectado y RESETEA el backoff.
//   - *events.Disconnected -> marca estado desconectado y AVANZA el backoff (whatsmeow auto-reconecta).
//   - *events.LoggedOut    -> marca la sesión CAÍDA (no se re-empareja automáticamente).
//   - *events.OfflineSyncPreview/*events.OfflineSyncCompleted -> abren y cierran el corchete de la
//     RÁFAGA OFFLINE (ADR-0037): mientras está abierto, los entrantes se descartan en la puerta.
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
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
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

	// gate es el estado de la RÁFAGA OFFLINE de ESTA sesión (ADR-0037 corte A + §6) y el contador de los
	// tres descartes de la puerta (gate, antigüedad, eco propio). Nunca nil: NewListener lo construye.
	// Vive POR SESIÓN, jamás como global de paquete (ADR-0037 §6, primer punto).
	gate *offlineGate

	// maxAge es el CINTURÓN por antigüedad (ADR-0037 corte B): un entrante cuya edad al procesarlo supere
	// este plazo se descarta. Cubre las dos fugas que el propio upstream documenta y por las que el corte A
	// no basta —cola llena (client.go:843) y nodo lento (client.go:874-886)—. Default defaultInboundMaxAge;
	// configurable por Edge (WAPP_AGENT_INBOUND_MAX_AGE_SECONDS). <=0 lo DESACTIVA (solo para tests que
	// ejercitan el gate aislado): en producción el guardarraíl de config nunca deja pasar un 0.
	maxAge time.Duration
}

// defaultInboundMaxAge es el umbral por defecto del cinturón B (ADR-0037 §B): 900 s / 15 min. No es a ojo.
// Suelo: el cinturón mide edad AL PROCESAR, no al llegar, y nuestro propio pipeline se atrasa (en el
// edge.log del 2026-08-06 hay 59 avisos "Node handling took…", los peores de ~37 s) más el desfase de reloj
// de una máquina recién despertada de suspensión; 60 s ya estaría dentro del ruido medido y mataría vivos.
// Techo: las caídas que motivan la ADR se miden en HORAS, y cualquier valor entre 5 y 30 min las corta
// igual. La asimetría decide el sentido del error: descartar un vivo es pérdida de negocio INVISIBLE;
// dejar pasar uno viejo llega deduplicado aguas abajo ⇒ el cinturón YERRA HACIA DEJAR PASAR.
const defaultInboundMaxAge = 15 * time.Minute

// SetHealthReporter liga el listener al registro de salud de SU sesión (Plan 031 T6). Se llama ANTES de
// Register (al construir el ciclo de escucha). nil ⇒ no se reporta salud. No es secreto ni PII.
func (l *Listener) SetHealthReporter(r health.SessionReporter) { l.reporter = r }

// NewListener construye el listener con el sink y el logger dados, el backoff por defecto del spike y el
// gate de ráfaga offline con los plazos por defecto (ADR-0037). SetInboundMaxAge ajusta el cinturón.
func NewListener(sink app.InboundSink, log logger.Logger) *Listener {
	return &Listener{
		sink:    sink,
		log:     log,
		backoff: DefaultBackoff(),
		state:   StateDisconnected,
		gate:    newOfflineGate(log, defaultOfflineWatchdog),
		maxAge:  defaultInboundMaxAge,
	}
}

// SetInboundMaxAge fija el umbral del cinturón B (ADR-0037). Se llama ANTES de Register (al construir el
// ciclo de escucha). Un valor <=0 DESACTIVA el cinturón y deja solo el corte A: es una costura de test, no
// una configuración de producción (config.Load aplica su propio guardarraíl al umbral).
func (l *Listener) SetInboundMaxAge(d time.Duration) { l.maxAge = d }

// OfflineStats devuelve el acumulado de descartes de la puerta de entrada de esta sesión (ADR-0037 §4):
// corchetes cerrados, descartados por el gate (A), por antigüedad (B) y ecos propios. Son cardinalidades,
// sin PII. Es el material que la telemetría de flota (ADR-0023) debe publicar.
func (l *Listener) OfflineStats() OfflineStats { return l.gate.snapshot() }

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
	case *events.Receipt:
		l.onReceiptEvent(e)
	case *events.OfflineSyncPreview:
		// Abre el corchete de la ráfaga (ADR-0037 corte A). Es la ÚNICA marca que el servidor nos da: no
		// hay marcador por mensaje (whatsmeow descarta el atributo `offline` del nodo).
		l.gate.arm(e)
	case *events.OfflineSyncCompleted:
		// Cierra el corchete: lo que llegue a partir de aquí vuelve a ser tráfico vivo.
		l.gate.close(closeCompleted)
	default:
		// Otros eventos (presencia, history sync, …) no son del alcance actual.
	}
}

// onMessage decide si el entrante se ADMITE y, si lo hace, lo mapea a domain.InboundEvent y lo entrega al
// sink. Un fallo de entrega se registra pero NO tumba la escucha (el socket sigue vivo).
//
// Los tres descartes de la puerta, en este orden (ADR-0037 §A/§B + filtro de eco propio):
//  1. ECO PROPIO (IsFromMe): lo mandamos nosotros; no es una conversación entrante y no tiene por qué subir.
//  2. GATE A: llegó dentro del corchete de la ráfaga que el servidor reencoló tras una caída.
//  3. CINTURÓN B: es demasiado viejo, venga de donde venga (cubre las fugas de orden de A).
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
		l.gate.countSelfDrop()
		return
	}

	now := time.Now()
	// 2 · Gate de la ráfaga offline. El resumen no nulo es un corchete que venció durante esta llamada
	// (watchdog perezoso): se emite FUERA del lock del gate.
	drop, closed := l.gate.dropInBurst(e.Info.Timestamp, now)
	l.gate.emit(closed)
	if drop {
		return
	}

	// 3 · Cinturón por antigüedad. Info.Timestamp es el reloj del SERVIDOR del mensaje original
	// (message.go:216, ag.UnixTime("t")). Solo se loguea la EDAD, nunca el mensaje.
	if l.maxAge > 0 {
		if age := now.Sub(e.Info.Timestamp); age > l.maxAge {
			l.gate.countAgeDrop()
			l.log.Warn("listener: entrante descartado por antigüedad (cinturón ADR-0037); si es habitual, el gate de ráfaga no está funcionando",
				"edad", age.Round(time.Second).String(), "umbral", l.maxAge.String())
			return
		}
	}

	inbound := toInboundEvent(e)
	if err := l.sink.Deliver(ctx, inbound); err != nil {
		l.log.Error("listener: no se pudo entregar el evento entrante al sink",
			"error", err, "message_id", inbound.MessageID)
	}
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

	// ADR-0037 §6: el socket murió — avanza la ÉPOCA y cierra el corchete si estaba abierto. Sin esto una
	// bandera armada sobreviviría a la reconexión y descartaría tráfico VIVO para siempre (el fallo más
	// grave que este mecanismo puede producir, y silencioso). NO se usa *events.Connected para esto: se
	// despacha desde una goroutine que antes hace I/O de red y puede llegar DESPUÉS del preview.
	l.gate.bumpEpoch()

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
	// ADR-0037 §6: mismo cierre de corchete que en la desconexión (ver onDisconnected).
	l.gate.bumpEpoch()

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
