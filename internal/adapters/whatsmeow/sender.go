// Package whatsmeow — helpers de envío compartidos por el CLIENTE VIVO de la escucha
// (ListenGateway.SendViaLiveClient/SendViaLiveClientTracked/SendMediaViaLiveClientTracked,
// listen_gateway.go): parseo del destino, construcción del *waE2E.Message (texto y media, con
// Upload) y los dos loaders de device sobre la BD única del Edge.
//
// 🔴 Hasta el 2026-09-01 este fichero también definía un adaptador `Sender` que ejecutaba un ciclo
// EFÍMERO propio (connect -> send -> sleep -> disconnect) para envíos sin acuse. Ese adaptador tenía
// CERO llamantes en producción (deuda M-2, documentations/deuda.md) y se borró junto con
// `internal/app/send.go`. Lo que queda aquí —parseRecipient, buildMessage, downloadMedia,
// buildMediaMessage, mediaTypeForKind, realLoadDevice, realLoadDeviceByJID— SÍ tiene llamante real:
// listen_gateway.go, que construye el mensaje y descarga el media con las mismas funciones antes de
// enviarlo por el cliente vivo. No vuelvas a borrar este fichero entero sin comprobar eso primero.
//
// La DEK (32B en claro) la pasa el caso de uso al loader; estos helpers NO la retienen ni la loguean:
// solo la usan para construir el container.
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
	"google.golang.org/protobuf/proto"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cryptostore"
)

const (
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

// loadDeviceFunc construye el container cifrado con la DEK y carga el device de la sesión pareada.
// Devuelve error si la DEK no construye el container, no hay sesión pareada o el JID es inválido.
type loadDeviceFunc func(ctx context.Context, dek []byte) (*store.Device, error)

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

// realLoadDevice es el loader PRODUCTIVO single-sesión (legacy): sobre la BD ÚNICA compartida (Plan 022
// T3) construye el container per-device (OpenDeviceContainer con el dialecto de config, fin del "sqlite"
// hardcodeado), resuelve el ÚNICO JID pareado (FirstDeviceJID) y carga ese device. Devuelve error si la
// DEK no construye el container, no hay sesión pareada o el store quedó vacío. NO loguea la DEK ni el
// material del store. El listener MULTI-device usa realLoadDeviceByJID (carga por SU JID).
func realLoadDevice(db *sql.DB, dialect string) loadDeviceFunc {
	return func(ctx context.Context, dek []byte) (*store.Device, error) {
		container, err := cryptostore.OpenDeviceContainer(ctx, db, dialect, dek)
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

// realLoadDeviceByJID es el loader PRODUCTIVO per-device sobre la BD ÚNICA compartida (Plan 022 T3): a
// diferencia de realLoadDevice (que resuelve el ÚNICO device pareado con FirstDeviceJID), carga el
// device CONCRETO cuyo JID ya conoce el registro (devices.jid). Es el que usa el listener MULTI-device:
// N devices comparten la BD y cada uno se carga por SU JID con SU DEK (msg_enc_* aislado por JID; cruzar
// DEKs FALLA, T2). NO loguea la DEK ni el material del store.
func realLoadDeviceByJID(db *sql.DB, dialect, jidStr string) loadDeviceFunc {
	return func(ctx context.Context, dek []byte) (*store.Device, error) {
		if jidStr == "" {
			return nil, fmt.Errorf("whatsapp: sin JID de device para cargar (sesión sin emparejar)")
		}
		jid, err := types.ParseJID(jidStr)
		if err != nil {
			return nil, fmt.Errorf("whatsapp: JID de device inválido: %w", err)
		}
		container, err := cryptostore.OpenDeviceContainer(ctx, db, dialect, dek)
		if err != nil {
			return nil, fmt.Errorf("whatsapp: construir store cifrado: %w", err)
		}
		device, err := cryptostore.LoadDevice(ctx, container, jid)
		if err != nil {
			return nil, fmt.Errorf("whatsapp: cargar device de la sesión: %w", err)
		}
		if device == nil {
			return nil, fmt.Errorf("whatsapp: no hay device pareado para el JID de la sesión")
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
// buildMediaMessage (necesita el cliente para Upload); ListenGateway.sendViaLiveClient elige entre
// las dos según si el `outgoing` trae mediaData.
func buildMessage(msg outgoing) *waE2E.Message {
	return &waE2E.Message{Conversation: proto.String(msg.text)}
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
