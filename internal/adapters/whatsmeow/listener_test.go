package whatsmeow

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// --- fakes ---

// spySink captura los eventos entregados y permite forzar un error de entrega.
type spySink struct {
	got []domain.InboundEvent
	err error
}

func (s *spySink) Deliver(_ context.Context, evt domain.InboundEvent) error {
	s.got = append(s.got, evt)
	return s.err
}

// quietLogger devuelve un logger que escribe a un buffer (sin ruido en la salida del test).
func quietLogger() sharedlogger.Logger {
	return sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}))
}

func newJID(user, server string) types.JID {
	return types.JID{User: user, Server: server}
}

// --- tests ---

// TestHandleEvent_Message_Conversation: un *events.Message de texto simple se mapea a InboundEvent y
// se entrega al sink con los campos correctos.
func TestHandleEvent_Message_Conversation(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger())

	ts := time.Date(2026, 6, 25, 10, 0, 0, 0, time.UTC)
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     newJID("123", types.DefaultUserServer),
				Sender:   newJID("123", types.DefaultUserServer),
				IsFromMe: false,
				IsGroup:  false,
			},
			ID:        "MSGID1",
			PushName:  "Alice",
			Timestamp: ts,
			Type:      "text",
		},
		Message: &waE2E.Message{Conversation: proto.String("hola edge")},
	}

	l.handleEvent(context.Background(), evt)

	if len(sink.got) != 1 {
		t.Fatalf("se esperaba 1 evento entregado, hubo %d", len(sink.got))
	}
	in := sink.got[0]
	if in.MessageID != "MSGID1" || in.Text != "hola edge" || in.PushName != "Alice" {
		t.Fatalf("mapeo incorrecto: %+v", in)
	}
	if in.Sender != "123@s.whatsapp.net" || !in.Timestamp.Equal(ts) || in.Type != "text" {
		t.Fatalf("campos de Info incorrectos: %+v", in)
	}
}

// TestToInboundEvent_Identity: toInboundEvent copia la identidad alterna (SenderAlt) y el
// AddressingMode al InboundEvent — Sender número + SenderAlt LID (Plan 010 §9).
func TestToInboundEvent_Identity(t *testing.T) {
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender:         newJID("593999", types.DefaultUserServer),
				SenderAlt:      newJID("10001", types.HiddenUserServer),
				AddressingMode: types.AddressingModePN,
			},
			ID: "ID-PN",
		},
		Message: &waE2E.Message{Conversation: proto.String("x")},
	}
	in := toInboundEvent(evt)
	if in.Sender != "593999@s.whatsapp.net" {
		t.Fatalf("Sender = %q", in.Sender)
	}
	if in.SenderAlt != "10001@lid" {
		t.Fatalf("SenderAlt = %q, quería 10001@lid", in.SenderAlt)
	}
	if in.AddressingMode != "pn" {
		t.Fatalf("AddressingMode = %q, quería pn", in.AddressingMode)
	}
}

// TestToInboundEvent_Identity_NoAlt: si whatsmeow aún no conoce el alterno (SenderAlt vacío,
// "No LID found" del primer contacto), SenderAlt queda "" y NO se falla (tolerancia §10.H).
func TestToInboundEvent_Identity_NoAlt(t *testing.T) {
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender:         newJID("593999", types.DefaultUserServer),
				AddressingMode: types.AddressingModePN,
			},
			ID: "ID-NOALT",
		},
		Message: &waE2E.Message{Conversation: proto.String("x")},
	}
	in := toInboundEvent(evt)
	if in.SenderAlt != "" {
		t.Fatalf("SenderAlt debía venir vacío (mapeo no aprendido), fue %q", in.SenderAlt)
	}
	if in.Sender != "593999@s.whatsapp.net" || in.AddressingMode != "pn" {
		t.Fatalf("lo conocido debía subir igual: %+v", in)
	}
}

// TestHandleEvent_Message_ExtendedText: el texto se extrae del ExtendedTextMessage cuando no hay
// Conversation.
func TestHandleEvent_Message_ExtendedText(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger())

	evt := &events.Message{
		Info: types.MessageInfo{ID: "X2"},
		Message: &waE2E.Message{
			ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("con contexto")},
		},
	}
	l.handleEvent(context.Background(), evt)

	if len(sink.got) != 1 || sink.got[0].Text != "con contexto" {
		t.Fatalf("no se extrajo el texto extendido: %+v", sink.got)
	}
}

// TestHandleEvent_Message_DeliverError: un fallo de entrega NO entra en pánico ni tumba el listener
// (se registra y sigue).
func TestHandleEvent_Message_DeliverError(t *testing.T) {
	sink := &spySink{err: errors.New("sink caído")}
	l := NewListener(sink, quietLogger())
	l.handleEvent(context.Background(), &events.Message{
		Info:    types.MessageInfo{ID: "E1"},
		Message: &waE2E.Message{Conversation: proto.String("x")},
	})
	if len(sink.got) != 1 {
		t.Fatalf("el evento debía intentarse entregar pese al error: %+v", sink.got)
	}
}

// TestHandleEvent_Connected: marca StateConnected y resetea el backoff.
func TestHandleEvent_Connected(t *testing.T) {
	l := NewListener(&spySink{}, quietLogger())
	// Avanza el backoff para verificar el reset.
	l.backoff.Next()
	l.backoff.Next()

	l.handleEvent(context.Background(), &events.Connected{})

	if l.State() != StateConnected {
		t.Fatalf("estado = %v, quería StateConnected", l.State())
	}
	if l.backoff.Attempt() != 0 {
		t.Fatalf("el backoff no se reseteó tras Connected: attempt=%d", l.backoff.Attempt())
	}
}

// TestHandleEvent_Disconnected: marca StateDisconnected, avanza el backoff y dispara el hook con el
// delay calculado.
func TestHandleEvent_Disconnected(t *testing.T) {
	l := NewListener(&spySink{}, quietLogger())
	var gotAttempt int
	var gotDelay time.Duration
	l.onDisconnect = func(attempt int, delay time.Duration) {
		gotAttempt = attempt
		gotDelay = delay
	}

	l.handleEvent(context.Background(), &events.Disconnected{})

	if l.State() != StateDisconnected {
		t.Fatalf("estado = %v, quería StateDisconnected", l.State())
	}
	if gotAttempt != 1 || gotDelay != 1*time.Second {
		t.Fatalf("hook recibió attempt=%d delay=%s, quería 1 y 1s", gotAttempt, gotDelay)
	}

	// Una segunda desconexión avanza el backoff (2s).
	l.handleEvent(context.Background(), &events.Disconnected{})
	if gotDelay != 2*time.Second {
		t.Fatalf("segundo delay = %s, quería 2s", gotDelay)
	}
}

// TestHandleEvent_LoggedOut: marca StateLoggedOut (sesión caída; no re-empareja).
func TestHandleEvent_LoggedOut(t *testing.T) {
	l := NewListener(&spySink{}, quietLogger())
	l.handleEvent(context.Background(), &events.LoggedOut{OnConnect: true})
	if l.State() != StateLoggedOut {
		t.Fatalf("estado = %v, quería StateLoggedOut", l.State())
	}
}

// TestHandleEvent_Unknown: un evento no contemplado se ignora sin entregar nada ni entrar en pánico.
func TestHandleEvent_Unknown(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger())
	l.handleEvent(context.Background(), &events.PushNameSetting{})
	if len(sink.got) != 0 {
		t.Fatalf("no debía entregarse nada para un evento desconocido: %+v", sink.got)
	}
	if l.State() != StateDisconnected {
		t.Fatalf("estado inicial debía mantenerse en StateDisconnected, fue %v", l.State())
	}
}

// TestHandleEvent_Connected_DisparaPresenciaUnaVez: tras Connected se dispara el hook onConnect (anuncio
// de presencia, §10.D) UNA vez; otros eventos (Message) NO lo disparan.
func TestHandleEvent_Connected_DisparaPresenciaUnaVez(t *testing.T) {
	l := NewListener(&spySink{}, quietLogger())
	calls := 0
	l.onConnect = func() { calls++ }

	l.handleEvent(context.Background(), &events.Connected{})
	if calls != 1 {
		t.Fatalf("onConnect debía dispararse 1 vez tras Connected, fueron %d", calls)
	}

	l.handleEvent(context.Background(), &events.Message{
		Info:    types.MessageInfo{ID: "M"},
		Message: &waE2E.Message{Conversation: proto.String("hola")},
	})
	if calls != 1 {
		t.Fatalf("un Message NO debía disparar onConnect; total=%d", calls)
	}
}

// TestHandleEvent_Receipt_Delivered: un events.Receipt de entrega se mapea a domain.ReceiptEvent con
// estado delivered, sus MessageIDs y timestamp, y se despacha por el hook.
func TestHandleEvent_Receipt_Delivered(t *testing.T) {
	l := NewListener(&spySink{}, quietLogger())
	var got []domain.ReceiptEvent
	l.onReceipt = func(e domain.ReceiptEvent) { got = append(got, e) }

	ts := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	l.handleEvent(context.Background(), &events.Receipt{
		MessageIDs: []types.MessageID{"S1", "S2"},
		Timestamp:  ts,
		Type:       types.ReceiptTypeDelivered,
	})

	if len(got) != 1 {
		t.Fatalf("se esperaba 1 acuse, hubo %d", len(got))
	}
	ack := got[0]
	if ack.Status != domain.ReceiptDelivered {
		t.Fatalf("status = %q, quería delivered", ack.Status)
	}
	if len(ack.MessageIDs) != 2 || ack.MessageIDs[0] != "S1" || ack.MessageIDs[1] != "S2" {
		t.Fatalf("MessageIDs mal mapeados: %+v", ack.MessageIDs)
	}
	if !ack.Timestamp.Equal(ts) {
		t.Fatalf("timestamp = %v, quería %v", ack.Timestamp, ts)
	}
}

// TestHandleEvent_Receipt_ReadVariants: Read, ReadSelf y Played mapean todos a estado read (§10.A).
func TestHandleEvent_Receipt_ReadVariants(t *testing.T) {
	for _, rt := range []types.ReceiptType{
		types.ReceiptTypeRead, types.ReceiptTypeReadSelf, types.ReceiptTypePlayed,
	} {
		l := NewListener(&spySink{}, quietLogger())
		var got []domain.ReceiptEvent
		l.onReceipt = func(e domain.ReceiptEvent) { got = append(got, e) }

		l.handleEvent(context.Background(), &events.Receipt{
			MessageIDs: []types.MessageID{"S"},
			Type:       rt,
		})
		if len(got) != 1 || got[0].Status != domain.ReceiptRead {
			t.Fatalf("tipo %q debía mapear a read, dio %+v", rt, got)
		}
	}
}

// TestHandleEvent_Receipt_TipoIgnorado: un tipo de acuse fuera del ciclo saliente (p.ej. Sender) se
// IGNORA sin despachar nada ni romper (§10.A).
func TestHandleEvent_Receipt_TipoIgnorado(t *testing.T) {
	l := NewListener(&spySink{}, quietLogger())
	called := false
	l.onReceipt = func(domain.ReceiptEvent) { called = true }

	l.handleEvent(context.Background(), &events.Receipt{
		MessageIDs: []types.MessageID{"S"},
		Type:       types.ReceiptTypeSender,
	})
	if called {
		t.Fatal("un tipo de acuse ignorado no debía despachar ReceiptEvent")
	}
}

// TestHandleEvent_Receipt_HookNil: sin hook cableado (T0), un acuse no entra en pánico (se descarta).
func TestHandleEvent_Receipt_HookNil(t *testing.T) {
	l := NewListener(&spySink{}, quietLogger())
	l.handleEvent(context.Background(), &events.Receipt{
		MessageIDs: []types.MessageID{"S"},
		Type:       types.ReceiptTypeDelivered,
	})
	// No debe hacer panic; nada que aseverar más allá de sobrevivir.
}
