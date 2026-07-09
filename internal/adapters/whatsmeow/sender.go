// Package whatsmeow — adaptador Sender (RF-4, design §3 fila 5): ejecuta el envío efímero a
// WhatsApp. Por CADA envío:
//
//  1. cryptostore.NewEncryptedContainer(ctx, db, dialect, dek) -> store cifrado con la DEK custodiada (RAM).
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
	"io"
	"net/http"
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
	// defaultDownloadTimeout acota la descarga del binario desde la presigned URL (Plan 017 §7). El
	// Edge baja el archivo con net/http normal (SIN credenciales, SIN SDK S3): la URL prefirmada ES la
	// capability. Un timeout evita que una URL lenta/colgada bloquee el ciclo de envío.
	defaultDownloadTimeout = 30 * time.Second
	// mediaKindImage es el discriminador (string) que la nube envía para elegir la rama ImageMessage;
	// cualquier otro valor (incluido "document" y el UNSPECIFIED del proto) cae a DocumentMessage.
	mediaKindImage = "image"
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

// outgoing describe un mensaje listo para enviar a un JID destino, ya conectado el cliente. text y los
// campos media* son mutuamente excluyentes: si mediaData != nil es un envío de ARCHIVO (Document/Image
// según kind), si no, un envío de TEXTO (Conversation). El caption viaja EMBEBIDO en el mismo mensaje
// del archivo (Plan 017 §9.I), no como un segundo mensaje de texto.
type outgoing struct {
	to   types.JID
	text string

	// media (Plan 017 §7, re-portado de EduGo): binario YA descargado de la presigned URL por el Edge
	// (GET sin credenciales). nil => envío de texto.
	mediaData []byte
	filename  string // nombre visible en WhatsApp (DocumentMessage.Title/FileName).
	mime      string // "application/pdf", "image/png", …
	kind      string // "document" | "image" (mediaKindImage discrimina la rama).
	caption   string // texto descriptivo embebido (Document/Image Caption).
}

// mediaUploader abstrae client.Upload para poder testear buildMediaMessage con un fake sin socket real.
// *wm.Client lo satisface (Upload(ctx, plaintext, appInfo) (UploadResponse, error)).
type mediaUploader interface {
	Upload(ctx context.Context, plaintext []byte, appInfo wm.MediaType) (wm.UploadResponse, error)
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

// SendMedia envía un ARCHIVO (documento/imagen) a `to` por el ciclo efímero, descifrando el store con
// `dek`. Antes del envío DESCARGA el binario de la presigned URL con net/http normal (SIN credenciales,
// SIN SDK S3): la URL prefirmada ES la capability (Plan 017 §7, zero-knowledge). El caption viaja
// embebido en el mismo mensaje (§9.I). La DEK NO cifra el media (solo descifra el store); se conserva en
// la firma por simetría con SendText.
func (s *Sender) SendMedia(ctx context.Context, dek []byte, to, presignedURL, filename, mime, kind, caption string) error {
	device, err := s.loadDevice(ctx, dek)
	if err != nil {
		return err
	}
	toJID, err := parseRecipient(to)
	if err != nil {
		return err
	}
	data, err := downloadMedia(ctx, presignedURL)
	if err != nil {
		return err
	}
	return s.dispatch(ctx, device, outgoing{
		to: toJID, mediaData: data, filename: filename, mime: mime, kind: kind, caption: caption,
	}, s.connectTimeout, s.flushDelay)
}

// downloadMedia baja el binario de la presigned URL con un net/http normal, SIN credenciales ni SDK S3
// (Plan 017 §7): la URL prefirmada de corta vida transporta la autorización; el Edge NUNCA ve las claves
// R2. Acota con un timeout propio (no derriba el ciclo de escucha) y falla claro ante status != 200.
func downloadMedia(ctx context.Context, rawURL string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultDownloadTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("whatsapp: construir GET de media: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("whatsapp: descargar media: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("whatsapp: descargar media: status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("whatsapp: leer media: %w", err)
	}
	return data, nil
}

// realLoadDevice es el loader PRODUCTIVO: construye el container cifrado con la DEK, resuelve el JID
// de la sesión pareada y carga el device EXISTENTE. Devuelve error si la DEK no construye el
// container, no hay sesión pareada o el store quedó vacío. NO loguea la DEK ni el material del store.
func realLoadDevice(db *sql.DB) loadDeviceFunc {
	return func(ctx context.Context, dek []byte) (*store.Device, error) {
		// Store del Edge = SQLite embebido (ADR-0002): dialecto explícito (Plan 022 T0), fin del "sqlite"
		// hardcodeado dentro del cryptostore.
		container, err := cryptostore.NewEncryptedContainer(ctx, db, cryptostore.DialectSQLite, dek)
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

// buildMessage arma el *waE2E.Message de TEXTO (Conversation). La rama de media vive en
// buildMediaMessage (necesita el cliente para Upload); messageFor elige según outgoing.
func buildMessage(msg outgoing) *waE2E.Message {
	return &waE2E.Message{Conversation: proto.String(msg.text)}
}

// messageFor elige el *waE2E.Message según el outgoing: si trae media la sube (Upload) y arma
// Document/Image; si no, un Conversation de texto. Comparte la rama de media entre el ciclo efímero
// (realDispatch) y el envío por cliente vivo (ListenGateway.SendMediaViaLiveClientTracked).
func messageFor(ctx context.Context, up mediaUploader, msg outgoing) (*waE2E.Message, error) {
	if msg.mediaData != nil {
		return buildMediaMessage(ctx, up, msg)
	}
	return buildMessage(msg), nil
}

// mediaTypeForKind mapea el discriminador (string) al MediaType de whatsmeow para el Upload y devuelve
// si la rama es imagen. "image" → MediaImage; cualquier otro valor (incluido "document" y el
// UNSPECIFIED del proto) → MediaDocument (caso por defecto: PDF).
func mediaTypeForKind(kind string) (mt wm.MediaType, isImage bool) {
	if kind == mediaKindImage {
		return wm.MediaImage, true
	}
	return wm.MediaDocument, false
}

// buildMediaMessage sube el binario (client.Upload, MediaDocument/MediaImage) y arma el DocumentMessage
// (PDF) o ImageMessage con los campos del UploadResponse (URL/DirectPath/MediaKey/hashes/FileLength).
// Copia-adaptación de edugo-api-messaging (rama PDF); el Caption embebido (§9.I) y la rama ImageMessage
// son NUEVOS respecto a EduGo. whatsmeow cifra el binario con su MediaKey para el destinatario.
func buildMediaMessage(ctx context.Context, up mediaUploader, msg outgoing) (*waE2E.Message, error) {
	mt, isImage := mediaTypeForKind(msg.kind)
	upload, err := up.Upload(ctx, msg.mediaData, mt)
	if err != nil {
		return nil, fmt.Errorf("whatsapp: subir media: %w", err)
	}
	if isImage {
		return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
			URL:           proto.String(upload.URL),
			DirectPath:    proto.String(upload.DirectPath),
			MediaKey:      upload.MediaKey,
			FileEncSHA256: upload.FileEncSHA256,
			FileSHA256:    upload.FileSHA256,
			FileLength:    proto.Uint64(upload.FileLength),
			Mimetype:      proto.String(msg.mime),
			Caption:       proto.String(msg.caption),
		}}, nil
	}
	return &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
		Title:         proto.String(msg.filename),
		FileName:      proto.String(msg.filename),
		Mimetype:      proto.String(msg.mime),
		Caption:       proto.String(msg.caption),
		URL:           proto.String(upload.URL),
		DirectPath:    proto.String(upload.DirectPath),
		MediaKey:      upload.MediaKey,
		FileEncSHA256: upload.FileEncSHA256,
		FileSHA256:    upload.FileSHA256,
		FileLength:    proto.Uint64(upload.FileLength),
	}}, nil
}

// realDispatch es el ciclo efímero PRODUCTIVO sobre whatsmeow. No se ejercita en los tests de este
// repo (necesita un teléfono pareado): calca el ciclo de EduGo con WaitForConnection determinista.
//
// LEGADO / DEPRECADO para envío real (Plan 013 §10.C): el envío que espera ACUSES (delivered/read) debe
// ir por el CLIENTE VIVO de la escucha (ListenGateway.SendViaLiveClient/SendViaLiveClientTracked), NO
// por este cliente efímero. El Disconnect inmediato pierde los events.Receipt asíncronos (que llegan
// DESPUÉS) y este ciclo además descarta el SendResponse, con lo que se pierde el MessageID necesario
// para la correlación (§10.E). Se conserva como costura de tests/legacy; no se borra en este plan.
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

	waMsg, err := messageFor(ctx, client, msg)
	if err != nil {
		return err
	}
	if _, err := client.SendMessage(ctx, msg.to, waMsg); err != nil {
		return fmt.Errorf("whatsapp: enviar mensaje: %w", err)
	}
	return nil
}
