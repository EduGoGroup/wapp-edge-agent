package whatsmeow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	wm "go.mau.fi/whatsmeow"
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

// TestDownloadMedia_NoCredentials: downloadMedia baja el binario con un GET SIN credenciales (ni
// Authorization ni cookies). Es el corazón del zero-knowledge del Edge (Plan 017 §7): la URL
// prefirmada ES la capability; el Edge nunca ve claves R2 ni usa el SDK S3.
func TestDownloadMedia_NoCredentials(t *testing.T) {
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

	data, err := downloadMedia(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("downloadMedia: %v", err)
	}
	if sawAuth || sawCookie {
		t.Fatalf("el GET de media llevó credenciales (auth=%v cookie=%v): debe ir SIN credenciales", sawAuth, sawCookie)
	}
	if string(data) != body {
		t.Fatalf("data descargada = %q, quería %q", string(data), body)
	}
}

// TestDownloadMedia_NonOKStatus_Error: un status distinto de 200 falla claro, sin datos.
func TestDownloadMedia_NonOKStatus_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	if _, err := downloadMedia(context.Background(), srv.URL); err == nil {
		t.Fatal("se esperaba error con status != 200")
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
