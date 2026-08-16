package whatsmeow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// --- fakes ---

// callLog es el REGISTRO DE LLAMADAS COMPARTIDO entre los dobles (sink y cola): sin él no se puede
// aseverar el ORDEN entre ambos, que es justo el gate del Plan 051 Ola 1 (durabilidad ANTES del acuse).
type callLog struct {
	mu    sync.Mutex
	order []string
}

func (c *callLog) record(name string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.order = append(c.order, name)
	c.mu.Unlock()
}

func (c *callLog) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.order...)
}

// spySink captura los eventos entregados y permite forzar un error de entrega. Si lleva calls, anota
// "deliver" en el registro compartido.
type spySink struct {
	got   []domain.InboundEvent
	err   error
	calls *callLog
}

func (s *spySink) Deliver(_ context.Context, evt domain.InboundEvent) error {
	s.calls.record("deliver")
	s.got = append(s.got, evt)
	return s.err
}

// spyCola es el doble de la cola durable: captura las filas anotadas, permite forzar un error (o un
// PANIC) de Enqueue y anota "enqueue" en el registro de llamadas compartido con el sink.
type spyCola struct {
	got   []app.ColaItem
	err   error
	calls *callLog
	// panicMsg != "" ⇒ Enqueue entra en pánico (simula un driver muerto o un crypterFor ajeno).
	panicMsg string
}

var _ app.ColaEntrantes = (*spyCola)(nil)

func (c *spyCola) Enqueue(_ context.Context, item app.ColaItem) error {
	c.calls.record("enqueue")
	if c.panicMsg != "" {
		panic(c.panicMsg)
	}
	c.got = append(c.got, item)
	return c.err
}

// colaWAIDs proyecta las filas capturadas a SOLO sus identificadores. Existe para que ningún mensaje de
// fallo imprima un app.ColaItem entero: con %+v saldría el TEXTO del mensaje a la salida de CI
// (INV-051.1 — la prohibición de transcribir contenido no se levanta por estar en un test).
func colaWAIDs(items []app.ColaItem) []string {
	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.SessionID+"/"+it.WAMessageID+"["+it.Estado+"]")
	}
	return ids
}

// liveMessage arma un *events.Message de texto EN VIVO (dentro de la ventana ADR-0037).
func liveMessage(id, text string) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:   newJID("123", types.DefaultUserServer),
				Sender: newJID("123", types.DefaultUserServer),
			},
			ID:        id,
			PushName:  "Alice",
			Timestamp: liveTS(),
			Type:      "text",
		},
		Message: &waE2E.Message{Conversation: proto.String(text)},
	}
}

// quietLogger devuelve un logger que escribe a un buffer (sin ruido en la salida del test).
func quietLogger() sharedlogger.Logger {
	return sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}))
}

func newJID(user, server string) types.JID {
	return types.JID{User: user, Server: server}
}

// liveTS es el sello de un mensaje EN VIVO. Desde el ADR-0037 el listener descarta los entrantes que caen
// fuera de la ventana temporal, así que los tests de MAPEO/ENRUTADO —que no van de eso— tienen que sellar
// sus mensajes con una marca reciente. Una fecha fija del pasado ya NO llega al sink, y es correcto que no
// llegue. (Un Timestamp CERO sí llega, pero por el camino de admisión explícita, que es otro test.)
func liveTS() time.Time { return time.Now() }

// --- tests ---

// TestHandleEvent_Message_Conversation: un *events.Message de texto simple se mapea a InboundEvent y
// se entrega al sink con los campos correctos.
func TestHandleEvent_Message_Conversation(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger())

	ts := liveTS()
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
		Info: types.MessageInfo{ID: "X2", Timestamp: liveTS()},
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
		Info:    types.MessageInfo{ID: "E1", Timestamp: liveTS()},
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

// TestHandleEvent_LoggedOut_HookDispara: si hay hook onLoggedOut cableado (Plan 020 T3), se invoca al
// recibir *events.LoggedOut (para propagar el estado ZOMBIE al cloud). Sin hook no rompe (test anterior).
func TestHandleEvent_LoggedOut_HookDispara(t *testing.T) {
	l := NewListener(&spySink{}, quietLogger())
	fired := 0
	l.onLoggedOutHook = func() { fired++ }
	l.handleEvent(context.Background(), &events.LoggedOut{OnConnect: false})
	if l.State() != StateLoggedOut {
		t.Fatalf("estado = %v, quería StateLoggedOut", l.State())
	}
	if fired != 1 {
		t.Fatalf("onLoggedOutHook invocado %d veces, quería 1", fired)
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

// --- cola durable de entrantes (Plan 051 Ola 1) ---

// TestOnMessage_Cola_EncolaAntesDeEntregar: el gate de la ola. Con la cola cableada, el INSERT ocurre
// ANTES del sink.Deliver (durabilidad antes del acuse) y la entrega SIGUE ocurriendo (escritura doble:
// el despachador que sustituye al camino inline es la Ola 3). Verifica además el mapeo de la fila.
func TestOnMessage_Cola_EncolaAntesDeEntregar(t *testing.T) {
	calls := &callLog{}
	sink := &spySink{calls: calls}
	cola := &spyCola{calls: calls}
	l := NewListener(sink, quietLogger(), WithCola(cola), WithSessionID("sess-1"))

	msg := liveMessage("MSG-COLA", "quiero dos empanadas")
	l.handleEvent(context.Background(), msg)

	if got := calls.snapshot(); len(got) != 2 || got[0] != "enqueue" || got[1] != "deliver" {
		t.Fatalf("orden de llamadas = %v, quería [enqueue deliver]", got)
	}
	if len(cola.got) != 1 {
		t.Fatalf("se esperaba 1 fila encolada, hubo %d", len(cola.got))
	}
	if len(sink.got) != 1 {
		t.Fatalf("la entrega al sink DEBE seguir ocurriendo en la Ola 1 (escritura doble), hubo %d", len(sink.got))
	}
	item := cola.got[0]
	// INV-051.1: los mensajes de fallo NO imprimen el ColaItem entero (%+v sacaría el TEXTO del mensaje a
	// la salida de CI, que es un log más). Solo identificadores.
	if item.SessionID != "sess-1" || item.WAMessageID != "MSG-COLA" {
		t.Fatalf("identificadores mal mapeados: session=%q wa_id=%q", item.SessionID, item.WAMessageID)
	}
	if item.ChatJID != "123@s.whatsapp.net" {
		t.Fatalf("chat mal mapeado: %q", item.ChatJID)
	}
	if item.Texto != "quiero dos empanadas" {
		t.Fatalf("texto mal mapeado (wa_id=%q): longitud %d", item.WAMessageID, len(item.Texto))
	}
	// El sello va en SEGUNDOS: con milis, el valor sería ~1000 veces mayor que el epoch actual.
	if item.TSWhatsApp != msg.Info.Timestamp.Unix() {
		t.Fatalf("TSWhatsApp = %d, quería epoch-segundos %d", item.TSWhatsApp, msg.Info.Timestamp.Unix())
	}
	if item.Estado != app.EstadoNuevo || item.IntentJSON != "" {
		t.Fatalf("un texto normal debía nacer nuevo y sin intent: estado=%q intent=%q", item.Estado, item.IntentJSON)
	}
	var meta colaMetaPayload
	if err := json.Unmarshal(item.Meta, &meta); err != nil {
		t.Fatalf("Meta no es JSON válido: %v", err)
	}
	if meta.PushName != "Alice" || meta.Sender != "123@s.whatsapp.net" {
		t.Fatalf("Meta mal poblada: %+v", meta)
	}
}

// TestOnMessage_Cola_ErrorNoImpideEntrega: REQ-051.8 — un Enqueue que falla se registra y ya: no entra
// en pánico, no aborta el handler y NO impide la entrega al sink.
func TestOnMessage_Cola_ErrorNoImpideEntrega(t *testing.T) {
	calls := &callLog{}
	sink := &spySink{calls: calls}
	cola := &spyCola{calls: calls, err: errors.New("disco lleno")}
	l := NewListener(sink, quietLogger(), WithCola(cola), WithSessionID("sess-1"))

	l.handleEvent(context.Background(), liveMessage("MSG-ERR", "hola"))

	if len(sink.got) != 1 {
		t.Fatalf("un fallo de la cola NO puede impedir la entrega al sink: %d entregas", len(sink.got))
	}
	if got := calls.snapshot(); len(got) != 2 || got[0] != "enqueue" || got[1] != "deliver" {
		t.Fatalf("orden de llamadas = %v, quería [enqueue deliver]", got)
	}
	// INV-051.3: la degradación no puede cerrarse con solo un log; queda contada y distinguible del panic.
	if s := l.InboundStats(); s.ColaEnqueueErrors != 1 || s.ColaEnqueuePanics != 0 {
		t.Fatalf("contadores = errores:%d panics:%d, quería 1 y 0", s.ColaEnqueueErrors, s.ColaEnqueuePanics)
	}
}

// TestOnMessage_Cola_PanicNoTumbaLaEscucha: REQ-051.8 / T1.10 — un PANIC dentro del Enqueue (driver,
// crypterFor ajeno) no puede subir al bucle de handlers de whatsmeow y tumbar la sesión. Se recupera, se
// registra SIN el valor recuperado (INV-051.1: podría arrastrar el texto) y la entrega al sink sigue.
func TestOnMessage_Cola_PanicNoTumbaLaEscucha(t *testing.T) {
	calls := &callLog{}
	sink := &spySink{calls: calls}
	cola := &spyCola{calls: calls, panicMsg: "driver muerto"}
	l := NewListener(sink, quietLogger(), WithCola(cola), WithSessionID("sess-1"))

	l.handleEvent(context.Background(), liveMessage("MSG-PANIC", "hola"))

	if len(sink.got) != 1 {
		t.Fatalf("un panic de la cola NO puede impedir la entrega al sink: %d entregas", len(sink.got))
	}
	if got := calls.snapshot(); len(got) != 2 || got[0] != "enqueue" || got[1] != "deliver" {
		t.Fatalf("orden de llamadas = %v, quería [enqueue deliver]", got)
	}
	// INV-051.3: un panic recuperado se cuenta APARTE del error de Enqueue — cualquier valor > 0 aquí es
	// un defecto, no una condición de campo, y confundirlos borraría esa diferencia.
	if s := l.InboundStats(); s.ColaEnqueuePanics != 1 || s.ColaEnqueueErrors != 0 {
		t.Fatalf("contadores = panics:%d errores:%d, quería 1 y 0", s.ColaEnqueuePanics, s.ColaEnqueueErrors)
	}
}

// TestNewListener_ColaSinSessionIDSeDegrada: F6 — WithCola sin WithSessionID es un ERROR DE CABLEADO. La
// fila se escribiría con session_id="" y el store elegiría la DEK de la cadena vacía. Se degrada al
// camino previo (cola nil ⇒ solo sink.Deliver), gritándolo en el log, en vez de fallar en silencio.
func TestNewListener_ColaSinSessionIDSeDegrada(t *testing.T) {
	calls := &callLog{}
	sink := &spySink{calls: calls}
	cola := &spyCola{calls: calls}
	l := NewListener(sink, quietLogger(), WithCola(cola)) // ¡sin WithSessionID!

	if l.cola != nil {
		t.Fatal("sin sessionID la cola debe quedar DESACTIVADA (nil), no escribir filas sin sesión")
	}
	l.handleEvent(context.Background(), liveMessage("MSG-SINSESS", "hola"))

	if len(cola.got) != 0 {
		t.Fatalf("no debía encolarse ninguna fila sin sesión, hubo %d", len(cola.got))
	}
	if got := calls.snapshot(); len(got) != 1 || got[0] != "deliver" {
		t.Fatalf("orden de llamadas = %v, quería solo [deliver] (camino previo al Plan 051)", got)
	}
}

// TestOnMessage_Cola_SinTextoNaceConMotivoPropio: F7 — un mensaje NO TEXTUAL (imagen, audio, sticker…)
// llega con Texto vacío. Nace clasificado (no hay nada que clasificar), pero con el motivo `sin_texto`,
// NO con `fastlane`: el fastlane devuelve true para la cadena vacía y contarlo como carril rápido
// falsearía el desglose de INV-051.3.
func TestOnMessage_Cola_SinTextoNaceConMotivoPropio(t *testing.T) {
	cola := &spyCola{}
	l := NewListener(&spySink{}, quietLogger(), WithCola(cola), WithSessionID("sess-1"))

	sinTexto := liveMessage("MSG-IMG", "")
	sinTexto.Message = nil // una imagen/sticker: no hay cuerpo de texto
	l.handleEvent(context.Background(), sinTexto)

	if len(cola.got) != 1 {
		t.Fatalf("se esperaba 1 fila encolada, hubo %d", len(cola.got))
	}
	item := cola.got[0]
	if item.Estado != app.EstadoClasificado {
		t.Fatalf("estado = %q, quería %q (el cajero no tiene nada que clasificar)", item.Estado, app.EstadoClasificado)
	}
	if item.IntentJSON != `{"omitido":"sin_texto"}` {
		t.Fatalf("intent = %q, quería la marca de omisión por falta de texto", item.IntentJSON)
	}
}

// TestOnMessage_Cola_DescartadoPorVentanaNoEncola: REQ-051.5 — lo que la ventana ADR-0037 descarta no
// genera fila (ni entrega).
func TestOnMessage_Cola_DescartadoPorVentanaNoEncola(t *testing.T) {
	calls := &callLog{}
	sink := &spySink{calls: calls}
	cola := &spyCola{calls: calls}
	l := NewListener(sink, quietLogger(), WithCola(cola), WithSessionID("sess-1"))
	// Sello = ahora ⇒ umbral = ahora − margen; un mensaje de hace 6 h cae fuera.
	l.SetConnectSeal(func() time.Time { return time.Now() })

	viejo := liveMessage("MSG-VIEJO", "mensaje de la ráfaga")
	viejo.Info.Timestamp = time.Now().Add(-6 * time.Hour)
	l.handleEvent(context.Background(), viejo)

	// INV-051.1: se imprimen CUENTAS e identificadores, nunca el ColaItem/InboundEvent enteros (llevan el
	// texto del mensaje, y la salida de CI es un log más).
	if len(cola.got) != 0 {
		t.Fatalf("un descartado por la ventana NO debe generar fila: %d filas (wa_ids=%v)",
			len(cola.got), colaWAIDs(cola.got))
	}
	if len(sink.got) != 0 {
		t.Fatalf("un descartado por la ventana tampoco se entrega: %d entregas", len(sink.got))
	}
	if got := calls.snapshot(); len(got) != 0 {
		t.Fatalf("no debía haber ninguna llamada, hubo %v", got)
	}
}

// TestOnMessage_Cola_EcoPropioNoEncola: el eco propio (IsFromMe) se descarta antes de todo; no encola.
func TestOnMessage_Cola_EcoPropioNoEncola(t *testing.T) {
	calls := &callLog{}
	sink := &spySink{calls: calls}
	cola := &spyCola{calls: calls}
	l := NewListener(sink, quietLogger(), WithCola(cola), WithSessionID("sess-1"))

	propio := liveMessage("MSG-MIO", "lo mandé yo")
	propio.Info.IsFromMe = true
	l.handleEvent(context.Background(), propio)

	if len(cola.got) != 0 || len(sink.got) != 0 {
		t.Fatalf("un eco propio no encola ni entrega: cola=%d filas (wa_ids=%v) sink=%d entregas",
			len(cola.got), colaWAIDs(cola.got), len(sink.got))
	}
}

// TestOnMessage_Cola_FastLaneNaceClasificado: un texto del carril rápido ("2", una opción de menú) nace
// en EstadoClasificado con la MARCA DE OMISIÓN — no con un intent inventado — para que el cajero no lo
// reclame nunca.
func TestOnMessage_Cola_FastLaneNaceClasificado(t *testing.T) {
	cola := &spyCola{}
	l := NewListener(&spySink{}, quietLogger(), WithCola(cola), WithSessionID("sess-1"))

	l.handleEvent(context.Background(), liveMessage("MSG-FAST", "2"))

	if len(cola.got) != 1 {
		t.Fatalf("se esperaba 1 fila encolada, hubo %d", len(cola.got))
	}
	item := cola.got[0]
	if item.Estado != app.EstadoClasificado {
		t.Fatalf("estado = %q, quería %q", item.Estado, app.EstadoClasificado)
	}
	if item.IntentJSON != `{"omitido":"fastlane"}` {
		t.Fatalf("intent = %q, quería la marca de omisión del fastlane", item.IntentJSON)
	}
}

// TestOnMessage_Cola_SinHoraUtilizableEncolaConSelloLocal: el camino de admisión explícita (Timestamp
// cero) TAMBIÉN encola, y con un sello NO CERO: un 0 sería epoch 1970 y la poda por TTL se llevaría la
// fila en el primer barrido.
func TestOnMessage_Cola_SinHoraUtilizableEncolaConSelloLocal(t *testing.T) {
	calls := &callLog{}
	sink := &spySink{calls: calls}
	cola := &spyCola{calls: calls}
	l := NewListener(sink, quietLogger(), WithCola(cola), WithSessionID("sess-1"))

	sinHora := liveMessage("MSG-SINHORA", "hola")
	sinHora.Info.Timestamp = time.Time{}
	antes := time.Now().Unix()
	l.handleEvent(context.Background(), sinHora)

	if len(cola.got) != 1 {
		t.Fatalf("el admitido sin hora también debe encolar, hubo %d filas", len(cola.got))
	}
	if ts := cola.got[0].TSWhatsApp; ts < antes {
		t.Fatalf("TSWhatsApp = %d; debía sellarse con la hora local de recepción (>= %d)", ts, antes)
	}
	if got := calls.snapshot(); len(got) != 2 || got[0] != "enqueue" || got[1] != "deliver" {
		t.Fatalf("orden de llamadas = %v, quería [enqueue deliver]", got)
	}
}

// TestOnMessage_ColaNil_ComportamientoPrevio: sin cola cableada (el fallback documentado), el listener
// se comporta EXACTAMENTE como antes del Plan 051: entrega al sink y no rompe nada.
func TestOnMessage_ColaNil_ComportamientoPrevio(t *testing.T) {
	calls := &callLog{}
	sink := &spySink{calls: calls}
	l := NewListener(sink, quietLogger()) // sin WithCola

	if l.cola != nil {
		t.Fatal("sin WithCola la cola debe quedar nil")
	}
	l.handleEvent(context.Background(), liveMessage("MSG-NILCOLA", "hola"))

	if len(sink.got) != 1 {
		t.Fatalf("se esperaba 1 entrega al sink, hubo %d", len(sink.got))
	}
	if got := calls.snapshot(); len(got) != 1 || got[0] != "deliver" {
		t.Fatalf("orden de llamadas = %v, quería solo [deliver]", got)
	}
}
