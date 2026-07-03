package whatsmeow

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
)

// fakeUploader implementa mediaUploader sin socket real: captura lo subido y devuelve un UploadResponse
// determinista para verificar el cableado de buildMediaMessage.
type fakeUploader struct {
	gotData []byte
	gotType wm.MediaType
	resp    wm.UploadResponse
	err     error
}

func (f *fakeUploader) Upload(_ context.Context, plaintext []byte, appInfo wm.MediaType) (wm.UploadResponse, error) {
	f.gotData = plaintext
	f.gotType = appInfo
	return f.resp, f.err
}

// Estos tests cubren el CABLEADO del Sender (DEK -> loader -> dispatch + parseo del destino y
// construcción del *waE2E.Message de texto) SIN abrir un socket real: el ciclo whatsmeow vive tras
// la costura `dispatch`. El loader y el dispatch se inyectan con newSenderWithDeps.

// TestSender_SendText_WiresDEKAndMessage: SendText pasa la DEK al loader, parsea el destino a JID y
// entrega un outgoing de texto al dispatch.
func TestSender_SendText_WiresDEKAndMessage(t *testing.T) {
	var gotDEK []byte
	var gotMsg outgoing

	loader := func(_ context.Context, dek []byte) (*store.Device, error) {
		gotDEK = append([]byte(nil), dek...)
		return &store.Device{}, nil
	}
	dispatch := func(_ context.Context, _ *store.Device, msg outgoing, _, _ time.Duration) error {
		gotMsg = msg
		return nil
	}
	s := newSenderWithDeps(loader, dispatch)

	dek := []byte{7, 7, 7, 7}
	if err := s.SendText(context.Background(), dek, "+54 911-1234", "hola"); err != nil {
		t.Fatalf("SendText: %v", err)
	}
	if !bytes.Equal(gotDEK, []byte{7, 7, 7, 7}) {
		t.Fatalf("la DEK no llegó intacta al loader: %v", gotDEK)
	}
	if gotMsg.text != "hola" {
		t.Fatalf("outgoing de texto inesperado: %+v", gotMsg)
	}
	// El destino con formato (+ y -) se normalizó a un JID de usuario.
	if gotMsg.to.User != "549111234" || gotMsg.to.Server != "s.whatsapp.net" {
		t.Fatalf("destino mal parseado: user=%q server=%q", gotMsg.to.User, gotMsg.to.Server)
	}
}

// TestSender_LoaderError_Propagates: si el loader falla (DEK mala / sin device pareado), el envío
// devuelve el error y NO invoca al dispatch.
func TestSender_LoaderError_Propagates(t *testing.T) {
	dispatched := false
	loader := func(context.Context, []byte) (*store.Device, error) {
		return nil, errors.New("no hay device pareado para la sesión")
	}
	dispatch := func(context.Context, *store.Device, outgoing, time.Duration, time.Duration) error {
		dispatched = true
		return nil
	}
	s := newSenderWithDeps(loader, dispatch)

	if err := s.SendText(context.Background(), []byte{1}, "549111", "hola"); err == nil {
		t.Fatal("se esperaba error cuando el loader falla")
	}
	if dispatched {
		t.Fatal("el dispatch NO debía invocarse si el loader falló")
	}
}

// TestSender_EmptyRecipient_Error: un destino vacío falla en el parseo, sin invocar al dispatch.
func TestSender_EmptyRecipient_Error(t *testing.T) {
	dispatched := false
	loader := func(context.Context, []byte) (*store.Device, error) { return &store.Device{}, nil }
	dispatch := func(context.Context, *store.Device, outgoing, time.Duration, time.Duration) error {
		dispatched = true
		return nil
	}
	s := newSenderWithDeps(loader, dispatch)

	if err := s.SendText(context.Background(), []byte{1}, "   ", "hola"); err == nil {
		t.Fatal("un destino vacío debía fallar")
	}
	if dispatched {
		t.Fatal("el dispatch NO debía invocarse con destino vacío")
	}
}

// TestSender_DispatchError_Propagates: un fallo del dispatch (ciclo whatsmeow) se propaga.
func TestSender_DispatchError_Propagates(t *testing.T) {
	sentinel := errors.New("conexión efímera expiró")
	loader := func(context.Context, []byte) (*store.Device, error) { return &store.Device{}, nil }
	dispatch := func(context.Context, *store.Device, outgoing, time.Duration, time.Duration) error {
		return sentinel
	}
	s := newSenderWithDeps(loader, dispatch)

	if err := s.SendText(context.Background(), []byte{1}, "549111", "hola"); !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, quería envolver %v", err, sentinel)
	}
}

// TestBuildMessage_Text: buildMessage arma un Conversation con el texto dado.
func TestBuildMessage_Text(t *testing.T) {
	msg := buildMessage(outgoing{text: "buenas"})
	if msg.GetConversation() != "buenas" {
		t.Fatalf("Conversation = %q, quería %q", msg.GetConversation(), "buenas")
	}
	if msg.DocumentMessage != nil {
		t.Fatal("un mensaje de texto no debe llevar DocumentMessage (recorte de PDF)")
	}
}

// TestSender_SendMedia_DownloadsWithoutCredentialsAndWires: SendMedia DESCARGA el binario de la presigned
// URL con un GET SIN credenciales (ni Authorization ni cookies), parsea el destino y entrega un outgoing de
// media al dispatch con los bytes descargados + filename/mime/kind/caption. Es el corazón del zero-knowledge
// del Edge (Plan 017 §7): la URL prefirmada ES la capability; el Edge nunca ve claves R2 ni usa el SDK S3.
func TestSender_SendMedia_DownloadsWithoutCredentialsAndWires(t *testing.T) {
	const body = "%PDF-1.7 contenido de prueba"
	var sawAuth, sawCookie bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			sawAuth = true
		}
		if r.Header.Get("Cookie") != "" {
			sawCookie = true
		}
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	var gotMsg outgoing
	loader := func(context.Context, []byte) (*store.Device, error) { return &store.Device{}, nil }
	dispatch := func(_ context.Context, _ *store.Device, msg outgoing, _, _ time.Duration) error {
		gotMsg = msg
		return nil
	}
	s := newSenderWithDeps(loader, dispatch)

	err := s.SendMedia(context.Background(), []byte{9, 9}, "+54 911-1234",
		srv.URL, "Lista de precios.pdf", "application/pdf", "document", "acá va la lista")
	if err != nil {
		t.Fatalf("SendMedia: %v", err)
	}
	if sawAuth || sawCookie {
		t.Fatalf("el GET de media llevó credenciales (auth=%v cookie=%v): debe ir SIN credenciales", sawAuth, sawCookie)
	}
	if string(gotMsg.mediaData) != body {
		t.Fatalf("mediaData descargada = %q, quería %q", string(gotMsg.mediaData), body)
	}
	if gotMsg.filename != "Lista de precios.pdf" || gotMsg.mime != "application/pdf" || gotMsg.kind != "document" || gotMsg.caption != "acá va la lista" {
		t.Fatalf("outgoing de media mal cableado: %+v", gotMsg)
	}
	if gotMsg.to.User != "549111234" || gotMsg.to.Server != "s.whatsapp.net" {
		t.Fatalf("destino mal parseado: %+v", gotMsg.to)
	}
}

// TestSender_SendMedia_DownloadError_NoDispatch: si la descarga falla (status != 200), SendMedia devuelve
// error y NO invoca al dispatch (no se intenta subir nada a WhatsApp).
func TestSender_SendMedia_DownloadError_NoDispatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	dispatched := false
	loader := func(context.Context, []byte) (*store.Device, error) { return &store.Device{}, nil }
	dispatch := func(context.Context, *store.Device, outgoing, time.Duration, time.Duration) error {
		dispatched = true
		return nil
	}
	s := newSenderWithDeps(loader, dispatch)

	if err := s.SendMedia(context.Background(), []byte{1}, "549111", srv.URL, "x.pdf", "application/pdf", "document", ""); err == nil {
		t.Fatal("se esperaba error cuando la descarga falla")
	}
	if dispatched {
		t.Fatal("el dispatch NO debía invocarse si la descarga falló")
	}
}

// TestBuildMediaMessage_Document: kind "document" sube con MediaDocument y arma un DocumentMessage con
// Title/FileName/Mimetype/Caption y los campos del UploadResponse.
func TestBuildMediaMessage_Document(t *testing.T) {
	up := &fakeUploader{resp: wm.UploadResponse{
		URL: "https://wa/doc", DirectPath: "/dp", MediaKey: []byte("mk"),
		FileEncSHA256: []byte("enc"), FileSHA256: []byte("sha"), FileLength: 42,
	}}
	msg := outgoing{mediaData: []byte("bytes"), filename: "Lista.pdf", mime: "application/pdf", kind: "document", caption: "hola"}

	waMsg, err := buildMediaMessage(context.Background(), up, msg)
	if err != nil {
		t.Fatalf("buildMediaMessage: %v", err)
	}
	if up.gotType != wm.MediaDocument {
		t.Errorf("Upload con MediaType %q, quería MediaDocument", up.gotType)
	}
	doc := waMsg.GetDocumentMessage()
	if doc == nil {
		t.Fatal("un kind=document debe producir DocumentMessage")
	}
	if waMsg.GetImageMessage() != nil {
		t.Fatal("un documento no debe llevar ImageMessage")
	}
	if doc.GetFileName() != "Lista.pdf" || doc.GetTitle() != "Lista.pdf" {
		t.Errorf("FileName/Title: got %q/%q", doc.GetFileName(), doc.GetTitle())
	}
	if doc.GetMimetype() != "application/pdf" || doc.GetCaption() != "hola" {
		t.Errorf("Mimetype/Caption: got %q/%q", doc.GetMimetype(), doc.GetCaption())
	}
	if doc.GetURL() != "https://wa/doc" || doc.GetDirectPath() != "/dp" || doc.GetFileLength() != 42 {
		t.Errorf("campos del UploadResponse mal mapeados: %+v", doc)
	}
}

// TestBuildMediaMessage_Image: kind "image" sube con MediaImage y arma un ImageMessage con Mimetype/Caption
// y los campos del UploadResponse (rama NUEVA respecto a EduGo).
func TestBuildMediaMessage_Image(t *testing.T) {
	up := &fakeUploader{resp: wm.UploadResponse{
		URL: "https://wa/img", DirectPath: "/dpi", MediaKey: []byte("mk"),
		FileEncSHA256: []byte("enc"), FileSHA256: []byte("sha"), FileLength: 7,
	}}
	msg := outgoing{mediaData: []byte("png"), filename: "orden.png", mime: "image/png", kind: "image", caption: "mirá"}

	waMsg, err := buildMediaMessage(context.Background(), up, msg)
	if err != nil {
		t.Fatalf("buildMediaMessage: %v", err)
	}
	if up.gotType != wm.MediaImage {
		t.Errorf("Upload con MediaType %q, quería MediaImage", up.gotType)
	}
	img := waMsg.GetImageMessage()
	if img == nil {
		t.Fatal("un kind=image debe producir ImageMessage")
	}
	if waMsg.GetDocumentMessage() != nil {
		t.Fatal("una imagen no debe llevar DocumentMessage")
	}
	if img.GetMimetype() != "image/png" || img.GetCaption() != "mirá" {
		t.Errorf("Mimetype/Caption: got %q/%q", img.GetMimetype(), img.GetCaption())
	}
	if img.GetURL() != "https://wa/img" || img.GetFileLength() != 7 {
		t.Errorf("campos del UploadResponse mal mapeados: %+v", img)
	}
}

// TestBuildMediaMessage_UploadError: un fallo del Upload se propaga como error (no se arma mensaje).
func TestBuildMediaMessage_UploadError(t *testing.T) {
	up := &fakeUploader{err: errors.New("upload falló")}
	if _, err := buildMediaMessage(context.Background(), up, outgoing{mediaData: []byte("x"), kind: "document"}); err == nil {
		t.Fatal("se esperaba error cuando Upload falla")
	}
}

// TestParseRecipient_AlreadyJID: un destino que ya trae @server se respeta tal cual.
func TestParseRecipient_AlreadyJID(t *testing.T) {
	jid, err := parseRecipient("549111@s.whatsapp.net")
	if err != nil {
		t.Fatalf("parseRecipient: %v", err)
	}
	if jid.User != "549111" || jid.Server != "s.whatsapp.net" {
		t.Fatalf("JID inesperado: %+v", jid)
	}
}

// TestParseRecipient_Empty: un destino que queda vacío tras limpiar el formato falla.
func TestParseRecipient_Empty(t *testing.T) {
	if _, err := parseRecipient("  + - "); err == nil {
		t.Fatal("un destino vacío tras limpiar debía fallar")
	}
}
