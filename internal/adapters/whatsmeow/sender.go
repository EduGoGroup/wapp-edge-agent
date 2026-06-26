// Package whatsmeow — adaptador Sender (RF-4, design §3 fila 5): ejecuta el envío efímero a
// WhatsApp. Por CADA envío:
//
//  1. cryptostore.NewEncryptedContainer(ctx, db, dek)  -> store cifrado con la DEK custodiada (RAM).
//  2. cryptostore.FirstDeviceJID(ctx, db)              -> JID de la única sesión pareada del Edge.
//  3. cryptostore.LoadDevice(ctx, container, jid)       -> carga el device EXISTENTE de esa sesión.
//  4. whatsmeow.NewClient(device, Noop)                 -> cliente efímero (logger silencioso).
//  5. client.Connect() + client.WaitForConnection(t)    -> espera DETERMINISTA de la conexión.
//  6. client.SendMessage(texto).
//  7. Sleep(flush) -> Disconnect()                       -> flush del frame y cierre del socket.
//
// Copia-adaptación de edugo-api-messaging/internal/infra/whatsapp/sender.go (ADR-0004): renombrado
// al namespace wApp, puerto -> internal/app.Sender, cryptostore -> wApp. RECORTES respecto a EduGo:
//   - SOLO texto: se eliminó SendPDF / DocumentMessage / client.Upload (media, no necesario para el
//     spike RF-4; arrastraba el campo pdf* del outgoing y la rama de Upload en buildMessage).
//   - El JID de la sesión NO se recibe como parámetro (EduGo lo pasaba como waSessionID): el Edge
//     custodia UNA sesión, así que se resuelve internamente con cryptostore.FirstDeviceJID.
//
// La DEK (32B en claro) la pasa el caso de uso app.Send desde la custodia (RAM); este adaptador NO
// la retiene ni la loguea: solo la usa para construir el container. No se prueba E2E aquí (necesita
// un teléfono pareado): el ciclo real vive tras la costura `dispatch`, inyectable, para testear el
// cableado (DEK -> device -> mensaje) con fakes sin abrir un socket real.
package whatsmeow

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cryptostore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

const (
	// defaultConnectTimeout acota cuánto espera WaitForConnection a que el socket conecte y
	// autentique. Holgura para redes lentas.
	defaultConnectTimeout = 15 * time.Second
	// defaultFlushDelay deja a whatsmeow vaciar el frame saliente al socket antes de Disconnect.
	// Sin esto, un Disconnect inmediato puede cortar el envío.
	defaultFlushDelay = 1500 * time.Millisecond
	// waServer es el sufijo del JID de usuario de WhatsApp cuando `to` viene como dígitos crudos.
	waServer = "s.whatsapp.net"
)

// Sender implementa app.Sender sobre whatsmeow efímero.
//
// loadDevice construye el store cifrado con la DEK, resuelve el JID de la sesión pareada y carga el
// device EXISTENTE (en producción: NewEncryptedContainer + FirstDeviceJID + LoadDevice sobre la BD
// propia; inyectable para tests). dispatch ejecuta el ciclo real connect-send-disconnect sobre un
// cliente whatsmeow; se inyecta para testear el cableado (DEK -> device -> mensaje) sin socket real.
type Sender struct {
	loadDevice     loadDeviceFunc
	dispatch       dispatchFunc
	connectTimeout time.Duration
	flushDelay     time.Duration
}

// loadDeviceFunc construye el container cifrado con la DEK y carga el device de la sesión pareada.
// Devuelve error si la DEK no construye el container, no hay sesión pareada o el JID es inválido.
type loadDeviceFunc func(ctx context.Context, dek []byte) (*store.Device, error)

// dispatchFunc ejecuta el ciclo efímero sobre el device ya cargado: NewClient -> Connect ->
// WaitForConnection -> SendMessage -> Sleep(flush) -> Disconnect. Aislado tras esta costura porque
// depende de un teléfono pareado (no testeable sin E2E); en tests se inyecta un stub que verifica
// el cableado sin red.
type dispatchFunc func(ctx context.Context, device *store.Device, msg outgoing, connectTimeout, flushDelay time.Duration) error

var _ app.Sender = (*Sender)(nil)

// outgoing describe un mensaje de texto listo para enviar a un JID destino, ya conectado el cliente.
type outgoing struct {
	to   types.JID
	text string
}

// NewSender construye el adaptador real sobre la BD propia del Edge (store cifrado por envío). La BD
// debe estar YA migrada (tablas whatsmeow_* y msg_enc_*) y con una sesión pareada (T3).
func NewSender(db *sql.DB) *Sender {
	return &Sender{
		loadDevice:     realLoadDevice(db),
		dispatch:       realDispatch,
		connectTimeout: defaultConnectTimeout,
		flushDelay:     defaultFlushDelay,
	}
}

// newSenderWithDeps construye un Sender con dependencias inyectadas (tests): permite verificar el
// cableado DEK->device y la construcción del mensaje sin socket real.
func newSenderWithDeps(loadDevice loadDeviceFunc, dispatch dispatchFunc) *Sender {
	return &Sender{
		loadDevice:     loadDevice,
		dispatch:       dispatch,
		connectTimeout: defaultConnectTimeout,
		flushDelay:     defaultFlushDelay,
	}
}

// SendText envía un mensaje de texto a `to` (número crudo o JID), descifrando el store con `dek`.
// Ciclo efímero completo (ver doc del paquete).
func (s *Sender) SendText(ctx context.Context, dek []byte, to, text string) error {
	device, err := s.loadDevice(ctx, dek)
	if err != nil {
		return err
	}
	toJID, err := parseRecipient(to)
	if err != nil {
		return err
	}
	return s.dispatch(ctx, device, outgoing{to: toJID, text: text}, s.connectTimeout, s.flushDelay)
}

// realLoadDevice es el loader PRODUCTIVO: construye el container cifrado con la DEK, resuelve el JID
// de la sesión pareada y carga el device EXISTENTE. Devuelve error si la DEK no construye el
// container, no hay sesión pareada o el store quedó vacío. NO loguea la DEK ni el material del store.
func realLoadDevice(db *sql.DB) loadDeviceFunc {
	return func(ctx context.Context, dek []byte) (*store.Device, error) {
		container, err := cryptostore.NewEncryptedContainer(ctx, db, dek)
		if err != nil {
			return nil, fmt.Errorf("whatsapp: construir store cifrado: %w", err)
		}
		jid, err := cryptostore.FirstDeviceJID(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("whatsapp: resolver sesión pareada: %w", err)
		}
		device, err := cryptostore.LoadDevice(ctx, container, jid)
		if err != nil {
			return nil, fmt.Errorf("whatsapp: cargar device de la sesión: %w", err)
		}
		if device == nil {
			return nil, fmt.Errorf("whatsapp: no hay device pareado para la sesión")
		}
		return device, nil
	}
}

// parseRecipient normaliza el destino a un types.JID: limpia el formato (+, -, espacios), añade el
// sufijo de usuario si vienen dígitos crudos, y parsea. No registra el número (PII) en errores con
// material adicional: solo propaga el error de parseo.
func parseRecipient(to string) (types.JID, error) {
	cleaned := strings.TrimSpace(to)
	cleaned = strings.ReplaceAll(cleaned, "+", "")
	cleaned = strings.ReplaceAll(cleaned, "-", "")
	cleaned = strings.ReplaceAll(cleaned, " ", "")
	if cleaned == "" {
		return types.JID{}, fmt.Errorf("whatsapp: destino vacío")
	}
	if !strings.Contains(cleaned, "@") {
		cleaned = cleaned + "@" + waServer
	}
	jid, err := types.ParseJID(cleaned)
	if err != nil {
		return types.JID{}, fmt.Errorf("whatsapp: destino inválido: %w", err)
	}
	return jid, nil
}

// buildMessage arma el *waE2E.Message de TEXTO (Conversation). Recorte respecto a EduGo: sin rama
// de PDF/Upload (el spike solo envía texto, RF-4).
func buildMessage(msg outgoing) *waE2E.Message {
	return &waE2E.Message{Conversation: proto.String(msg.text)}
}

// realDispatch es el ciclo efímero PRODUCTIVO sobre whatsmeow. No se ejercita en los tests de este
// repo (necesita un teléfono pareado): calca el ciclo de EduGo con WaitForConnection determinista.
func realDispatch(ctx context.Context, device *store.Device, msg outgoing, connectTimeout, flushDelay time.Duration) error {
	// Logger silencioso: whatsmeow NO debe volcar material sensible (claves/store) a los logs.
	client := wm.NewClient(device, waLog.Noop)

	if err := client.Connect(); err != nil {
		return fmt.Errorf("whatsapp: conectar: %w", err)
	}
	// Cierre garantizado del socket pase lo que pase (éxito, error de envío o timeout).
	defer func() {
		time.Sleep(flushDelay) // deja a whatsmeow vaciar el frame antes de cerrar.
		client.Disconnect()
	}()

	// WaitForConnection bloquea hasta socket conectado + autenticado, o false en timeout/desconexión.
	if !client.WaitForConnection(connectTimeout) {
		return fmt.Errorf("whatsapp: la conexión efímera expiró tras %s", connectTimeout)
	}

	if _, err := client.SendMessage(ctx, msg.to, buildMessage(msg)); err != nil {
		return fmt.Errorf("whatsapp: enviar mensaje: %w", err)
	}
	return nil
}
