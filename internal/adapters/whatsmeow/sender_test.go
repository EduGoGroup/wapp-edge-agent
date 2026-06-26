package whatsmeow

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/store"
)

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
