package whatsmeow

// listener_test.go — REESCRITO en el Plan 051 Ola 3 · T3.0.
//
// 🔴 QUÉ CAMBIÓ Y POR QUÉ TANTO. Hasta esta ola el LISTENER ENTREGABA: tenía un app.InboundSink y cada
// test comprobaba «llegó / no llegó al sink». Retirado el camino inline (INV-051.2), el listener no
// entrega; su ÚNICO efecto observable sobre un entrante admitido es la fila que anota en la COLA DURABLE.
// Por eso desapareció el doble `spySink` y todas las aserciones sobre entregas pasaron a ser aserciones
// sobre filas: no es un cambio cosmético de los tests, es que lo que antes probaban ya no existe.
//
// Los tests que comprobaban el ORDEN enqueue→deliver (el gate «durable antes del acuse» de la Ola 1) se
// quedaron sin segunda mitad: hoy el INSERT es lo último que pasa antes de que el handler retorne, así que
// la durabilidad previa al acuse es estructural y lo que se fija es que NO haya nada más.

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

// callLog es el REGISTRO DE LLAMADAS del doble de la cola. Sobrevive a T3.0 —aunque ya solo haya un actor
// que anotar— porque es la única forma de ver que el Enqueue SE INTENTÓ en los casos en que no deja rastro
// en `got`: el del panic (que aborta antes del append) y los de descarte (donde lo que se afirma es que la
// lista de llamadas está VACÍA, no solo que no hay filas).
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

// spyCola es el doble de la cola durable: captura las filas anotadas, permite forzar un error (o un
// PANIC) de Enqueue y anota "enqueue" en el registro de llamadas.
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

// listenerConCola es el molde por defecto de esta suite tras T3.0: un listener CON cola, que es el único
// cableado en el que un entrante admitido deja rastro. Antes el molde por defecto era «listener con sink»;
// hoy un listener sin cola no observa nada porque no hace nada con el mensaje.
func listenerConCola(cola *spyCola, opts ...ListenerOption) *Listener {
	base := []ListenerOption{WithCola(cola), WithSessionID("sess-1")}
	return NewListener(quietLogger(), append(base, opts...)...)
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
// sus mensajes con una marca reciente. Una fecha fija del pasado ya NO genera fila, y es correcto que no
// la genere. (Un Timestamp CERO sí entra, pero por el camino de admisión explícita, que es otro test.)
func liveTS() time.Time { return time.Now() }

// --- tests ---

// TestHandleEvent_Message_Conversation: un *events.Message de texto simple se enruta a onMessage y acaba
// como fila de la cola con sus campos. Antes de T3.0 este test miraba el domain.InboundEvent que salía por
// el sink; el mapeo a evento de dominio se sigue cubriendo aparte (TestToInboundEvent_*), que es donde vive
// ahora esa responsabilidad (la usa el despachador para la traducción inversa).
func TestHandleEvent_Message_Conversation(t *testing.T) {
	cola := &spyCola{}
	l := listenerConCola(cola)

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

	if len(cola.got) != 1 {
		t.Fatalf("se esperaba 1 fila encolada, hubo %d", len(cola.got))
	}
	item := cola.got[0]
	// INV-051.1: nunca se imprime el ColaItem entero (llevaría el texto a la salida de CI).
	if item.WAMessageID != "MSGID1" || item.Texto != "hola edge" {
		t.Fatalf("mapeo incorrecto: wa_id=%q longitud_texto=%d", item.WAMessageID, len(item.Texto))
	}
	if item.ChatJID != "123@s.whatsapp.net" || item.TSWhatsApp != ts.Unix() {
		t.Fatalf("campos de Info incorrectos: chat=%q ts=%d", item.ChatJID, item.TSWhatsApp)
	}
	var meta colaMetaPayload
	if err := json.Unmarshal(item.Meta, &meta); err != nil {
		t.Fatalf("Meta no es JSON válido: %v", err)
	}
	if meta.PushName != "Alice" || meta.Type != "text" {
		t.Fatalf("Meta mal poblada: %+v", meta)
	}
}

// TestToInboundEvent_Identity: toInboundEvent copia la identidad alterna (SenderAlt) y el
// AddressingMode al InboundEvent — Sender número + SenderAlt LID (Plan 010 §9).
//
// toInboundEvent ya no la llama el camino caliente (T3.0), pero sigue siendo la ESPECIFICACIÓN del mapeo
// que el despachador imita al reconstruir el evento desde la cola: por eso estos dos tests se conservan.
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
	cola := &spyCola{}
	l := listenerConCola(cola)

	evt := &events.Message{
		Info: types.MessageInfo{ID: "X2", Timestamp: liveTS()},
		Message: &waE2E.Message{
			ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("con contexto")},
		},
	}
	l.handleEvent(context.Background(), evt)

	if len(cola.got) != 1 || cola.got[0].Texto != "con contexto" {
		t.Fatalf("no se extrajo el texto extendido: %d filas (wa_ids=%v)", len(cola.got), colaWAIDs(cola.got))
	}
}

// TestHandleEvent_Connected: marca StateConnected y resetea el backoff.
func TestHandleEvent_Connected(t *testing.T) {
	l := NewListener(quietLogger())
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
	l := NewListener(quietLogger())
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
	l := NewListener(quietLogger())
	l.handleEvent(context.Background(), &events.LoggedOut{OnConnect: true})
	if l.State() != StateLoggedOut {
		t.Fatalf("estado = %v, quería StateLoggedOut", l.State())
	}
}

// TestHandleEvent_LoggedOut_HookDispara: si hay hook onLoggedOut cableado (Plan 020 T3), se invoca al
// recibir *events.LoggedOut (para propagar el estado ZOMBIE al cloud). Sin hook no rompe (test anterior).
func TestHandleEvent_LoggedOut_HookDispara(t *testing.T) {
	l := NewListener(quietLogger())
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

// TestHandleEvent_Unknown: un evento no contemplado se ignora sin anotar nada ni entrar en pánico.
func TestHandleEvent_Unknown(t *testing.T) {
	calls := &callLog{}
	cola := &spyCola{calls: calls}
	l := listenerConCola(cola)

	l.handleEvent(context.Background(), &events.PushNameSetting{})

	if got := calls.snapshot(); len(got) != 0 {
		t.Fatalf("un evento desconocido no debía provocar ninguna llamada, hubo %v", got)
	}
	if l.State() != StateDisconnected {
		t.Fatalf("estado inicial debía mantenerse en StateDisconnected, fue %v", l.State())
	}
}

// TestHandleEvent_Connected_DisparaPresenciaUnaVez: tras Connected se dispara el hook onConnect (anuncio
// de presencia, §10.D) UNA vez; otros eventos (Message) NO lo disparan.
func TestHandleEvent_Connected_DisparaPresenciaUnaVez(t *testing.T) {
	l := listenerConCola(&spyCola{})
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
//
// Los ACUSES son el único camino del listener que SÍ sigue empujando hacia fuera en línea (onReceipt),
// y es correcto: no llevan contenido, no pasan por la cola y su destino es el estado del saliente.
func TestHandleEvent_Receipt_Delivered(t *testing.T) {
	l := NewListener(quietLogger())
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
		l := NewListener(quietLogger())
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
	l := NewListener(quietLogger())
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
	l := NewListener(quietLogger())
	l.handleEvent(context.Background(), &events.Receipt{
		MessageIDs: []types.MessageID{"S"},
		Type:       types.ReceiptTypeDelivered,
	})
	// No debe hacer panic; nada que aseverar más allá de sobrevivir.
}

// --- cola durable de entrantes (Plan 051 Ola 1 · Ola 3) ---

// TestOnMessage_Cola_EncolaYNoHaceNadaMas es el gate de T3.0 (INV-051.2) y el heredero del antiguo
// TestOnMessage_Cola_EncolaAntesDeEntregar.
//
// Aquel test fijaba el ORDEN [enqueue, deliver] de la escritura doble de la Ola 1. Hoy fija que la lista de
// llamadas es EXACTAMENTE [enqueue]: el handler anota la fila y vuelve. La promesa «mensaje durable antes
// del acuse» dejó de necesitar un orden que vigilar —el INSERT es lo último que ocurre—, y lo que hay que
// vigilar es lo contrario: que no aparezca una segunda llamada a nada.
func TestOnMessage_Cola_EncolaYNoHaceNadaMas(t *testing.T) {
	calls := &callLog{}
	cola := &spyCola{calls: calls}
	l := listenerConCola(cola)

	msg := liveMessage("MSG-COLA", "quiero dos empanadas")
	l.handleEvent(context.Background(), msg)

	if got := calls.snapshot(); len(got) != 1 || got[0] != "enqueue" {
		t.Fatalf("llamadas = %v, quería exactamente [enqueue]: el listener ya no entrega (INV-051.2)", got)
	}
	if len(cola.got) != 1 {
		t.Fatalf("se esperaba 1 fila encolada, hubo %d", len(cola.got))
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

// TestOnMessage_Cola_ErrorNoTumbaLaEscucha: REQ-051.8 — un Enqueue que falla se registra y se CUENTA, pero
// no entra en pánico ni aborta el handler.
//
// ⚠️ LO QUE ESTE TEST NO AFIRMA: que el mensaje llegue igual. Antes de T3.0 lo rescataba el `sink.Deliver`
// y ya no existe. Quien lo rescata desde T1.13 es WHATSAPP: el entrante que no deja fila tampoco se acusa,
// así que se reofrece (ver listener_acuse_test.go, que es donde se custodia esa decisión). Aquí se sigue
// comprobando lo de siempre —que la escucha no se cae y que la degradación queda CONTADA—, porque el
// contador es lo único que dice cuánto está costando; el acuse es una decisión aparte.
func TestOnMessage_Cola_ErrorNoTumbaLaEscucha(t *testing.T) {
	calls := &callLog{}
	cola := &spyCola{calls: calls, err: errors.New("disco lleno")}
	l := listenerConCola(cola)

	l.handleEvent(context.Background(), liveMessage("MSG-ERR", "hola"))

	if got := calls.snapshot(); len(got) != 1 || got[0] != "enqueue" {
		t.Fatalf("llamadas = %v, quería [enqueue]", got)
	}
	// INV-051.3: la degradación no puede cerrarse con solo un log; queda contada y distinguible del panic.
	if s := l.InboundStats(); s.ColaEnqueueErrors != 1 || s.ColaEnqueuePanics != 0 {
		t.Fatalf("contadores = errores:%d panics:%d, quería 1 y 0", s.ColaEnqueueErrors, s.ColaEnqueuePanics)
	}
}

// TestOnMessage_Cola_PanicNoTumbaLaEscucha: REQ-051.8 / T1.10 — un PANIC dentro del Enqueue (driver,
// crypterFor ajeno) no puede subir al bucle de handlers de whatsmeow y tumbar la sesión. Se recupera y se
// registra SIN el valor recuperado (INV-051.1: podría arrastrar el texto).
func TestOnMessage_Cola_PanicNoTumbaLaEscucha(t *testing.T) {
	calls := &callLog{}
	cola := &spyCola{calls: calls, panicMsg: "driver muerto"}
	l := listenerConCola(cola)

	l.handleEvent(context.Background(), liveMessage("MSG-PANIC", "hola"))

	if got := calls.snapshot(); len(got) != 1 || got[0] != "enqueue" {
		t.Fatalf("llamadas = %v, quería [enqueue] (el intento se hizo y el pánico se recuperó)", got)
	}
	// INV-051.3: un panic recuperado se cuenta APARTE del error de Enqueue — cualquier valor > 0 aquí es
	// un defecto, no una condición de campo, y confundirlos borraría esa diferencia.
	if s := l.InboundStats(); s.ColaEnqueuePanics != 1 || s.ColaEnqueueErrors != 0 {
		t.Fatalf("contadores = panics:%d errores:%d, quería 1 y 0", s.ColaEnqueuePanics, s.ColaEnqueueErrors)
	}
}

// TestNewListener_ColaSinSessionIDSeDesactiva: F6 — WithCola sin WithSessionID es un ERROR DE CABLEADO. La
// fila se escribiría con session_id="" y el store elegiría la DEK de la cadena vacía. Se desactiva la cola
// gritándolo en el log, en vez de fallar en silencio.
//
// El precio de esa desactivación SUBIÓ en T3.0: antes quedaba el camino inline y solo se perdía la
// durabilidad; hoy la sesión queda sorda del todo. Se mantiene la decisión (no tumbar el proceso por un
// error de cableado de UNA sesión), y por eso el listener grita DOS veces al construirse en este caso.
func TestNewListener_ColaSinSessionIDSeDesactiva(t *testing.T) {
	calls := &callLog{}
	cola := &spyCola{calls: calls}
	l := NewListener(quietLogger(), WithCola(cola)) // ¡sin WithSessionID!

	if l.cola != nil {
		t.Fatal("sin sessionID la cola debe quedar DESACTIVADA (nil), no escribir filas sin sesión")
	}
	l.handleEvent(context.Background(), liveMessage("MSG-SINSESS", "hola"))

	if len(cola.got) != 0 {
		t.Fatalf("no debía encolarse ninguna fila sin sesión, hubo %d", len(cola.got))
	}
	if got := calls.snapshot(); len(got) != 0 {
		t.Fatalf("llamadas = %v, no debía haber ninguna", got)
	}
}

// TestOnMessage_Cola_SinTextoNaceConMotivoPropio: F7 — un mensaje NO TEXTUAL (imagen, audio, sticker…)
// llega con Texto vacío. Nace clasificado (no hay nada que clasificar), pero con el motivo `sin_texto`,
// NO con `fastlane`: el fastlane devuelve true para la cadena vacía y contarlo como carril rápido
// falsearía el desglose de INV-051.3.
func TestOnMessage_Cola_SinTextoNaceConMotivoPropio(t *testing.T) {
	cola := &spyCola{}
	l := listenerConCola(cola)

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
	// LITERAL A PROPÓSITO, no app.SobreOmitido(app.MotivoSinTexto): este test fija el FORMATO DE CABLE
	// que se persiste en la columna, y hacerlo contra la misma función que lo produce no probaría nada
	// (un cambio en el enum pasaría verde arrastrando el test consigo). El literal es el testigo
	// independiente; si el sobre cambia de forma, aquí tiene que doler.
	if item.IntentJSON != `{"omitido":"sin_texto"}` {
		t.Fatalf("intent = %q, quería la marca de omisión por falta de texto", item.IntentJSON)
	}
}

// TestOnMessage_Cola_DescartadoPorVentanaNoEncola: REQ-051.5 — lo que la ventana ADR-0037 descarta no
// genera fila. Y desde T3.0 «no genera fila» es lo MISMO que «no existe»: no hay segundo camino por el que
// pudiera colarse.
func TestOnMessage_Cola_DescartadoPorVentanaNoEncola(t *testing.T) {
	calls := &callLog{}
	cola := &spyCola{calls: calls}
	l := listenerConCola(cola)
	// Sello = ahora ⇒ umbral = ahora − margen; un mensaje de hace 6 h cae fuera.
	l.SetConnectSeal(func() time.Time { return time.Now() })

	viejo := liveMessage("MSG-VIEJO", "mensaje de la ráfaga")
	viejo.Info.Timestamp = time.Now().Add(-6 * time.Hour)
	l.handleEvent(context.Background(), viejo)

	// INV-051.1: se imprimen CUENTAS e identificadores, nunca el ColaItem entero (lleva el texto del
	// mensaje, y la salida de CI es un log más).
	if len(cola.got) != 0 {
		t.Fatalf("un descartado por la ventana NO debe generar fila: %d filas (wa_ids=%v)",
			len(cola.got), colaWAIDs(cola.got))
	}
	if got := calls.snapshot(); len(got) != 0 {
		t.Fatalf("no debía haber ninguna llamada, hubo %v", got)
	}
}

// TestOnMessage_Cola_EcoPropioNoEncola: el eco propio (IsFromMe) se descarta antes de todo; no encola.
func TestOnMessage_Cola_EcoPropioNoEncola(t *testing.T) {
	calls := &callLog{}
	cola := &spyCola{calls: calls}
	l := listenerConCola(cola)

	propio := liveMessage("MSG-MIO", "lo mandé yo")
	propio.Info.IsFromMe = true
	l.handleEvent(context.Background(), propio)

	if len(cola.got) != 0 || len(calls.snapshot()) != 0 {
		t.Fatalf("un eco propio no encola: %d filas (wa_ids=%v), llamadas=%v",
			len(cola.got), colaWAIDs(cola.got), calls.snapshot())
	}
}

// TestOnMessage_Cola_FastLaneNaceClasificado: un texto del carril rápido ("2", una opción de menú) nace
// en EstadoClasificado con la MARCA DE OMISIÓN — no con un intent inventado — para que el cajero no lo
// reclame nunca.
//
// EL FASTLANE SOBREVIVE A T3.0 y es legítimo que lo haga: es un regex léxico de microsegundos, no una
// llamada al LLM. Lo que se retiró del hilo de whatsmeow es la INFERENCIA, no el criterio de elegibilidad.
func TestOnMessage_Cola_FastLaneNaceClasificado(t *testing.T) {
	cola := &spyCola{}
	l := listenerConCola(cola)

	l.handleEvent(context.Background(), liveMessage("MSG-FAST", "2"))

	if len(cola.got) != 1 {
		t.Fatalf("se esperaba 1 fila encolada, hubo %d", len(cola.got))
	}
	item := cola.got[0]
	if item.Estado != app.EstadoClasificado {
		t.Fatalf("estado = %q, quería %q", item.Estado, app.EstadoClasificado)
	}
	// LITERAL A PROPÓSITO (mismo criterio que en el test de sin_texto, ver arriba): el test fija el
	// formato de cable con independencia de la constante que hoy lo produce.
	if item.IntentJSON != `{"omitido":"fastlane"}` {
		t.Fatalf("intent = %q, quería la marca de omisión del fastlane", item.IntentJSON)
	}
}

// TestOnMessage_Cola_SinHoraUtilizableEncolaConSelloLocal: el camino de admisión explícita (Timestamp
// cero) TAMBIÉN encola, y con un sello NO CERO: un 0 sería epoch 1970 y la poda por TTL se llevaría la
// fila en el primer barrido.
//
// Este es el camino (a′) de T3.0: tenía su PROPIO `sink.Deliver`, que se retiró junto con el otro. La
// aserción de que las llamadas son exactamente [enqueue] es lo que impide que vuelva solo aquí — un
// duplicado disparado por una condición rarísima (`t="0"`) sería imposible de reproducir en campo.
func TestOnMessage_Cola_SinHoraUtilizableEncolaConSelloLocal(t *testing.T) {
	calls := &callLog{}
	cola := &spyCola{calls: calls}
	l := listenerConCola(cola)

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
	if got := calls.snapshot(); len(got) != 1 || got[0] != "enqueue" {
		t.Fatalf("llamadas = %v, quería exactamente [enqueue]: el camino sin hora tampoco entrega (T3.0)", got)
	}
}

// --- puerta de elegibilidad en el listener (Plan 051 Ola 2 · T2.12) ---

// apagado es el predicado de un clasificador APAGADO, para WithClasificadorActivo. Existe con nombre
// propio porque aparece en casi todos los tests de esta sección y un `func() bool { return false }` suelto
// en cada uno esconde de qué va el caso.
func apagado() bool { return false }

// grupoMessage arma un entrante EN VIVO que viene de un GRUPO. Solo marca IsGroup: es el único campo que
// el listener consulta para juzgar la elegibilidad, así que fabricar un JID @g.us no probaría nada más y
// ataría el test a un detalle que el código no usa.
func grupoMessage(id, text string) *events.Message {
	msg := liveMessage(id, text)
	msg.Info.IsGroup = true
	return msg
}

// grupoSinTexto es una imagen/sticker recibida EN UN GRUPO: el caso que solapa las dos primeras ramas del
// switch y donde tiene que ganar `sin_texto` (ver TestOnMessage_Cola_OrdenDeLosMotivos).
func grupoSinTexto(id string) *events.Message {
	msg := grupoMessage(id, "")
	msg.Message = nil
	return msg
}

// TestOnMessage_Cola_GrupoNaceNoElegible: T2.12 — el defecto que aquella tarea cerró. Antes, un mensaje de
// GRUPO con texto nacía `nuevo`, el cajero lo reclamaba y lo mandaba a Ollama: carga que el Edge NUNCA ha
// clasificado (el decorador inline la filtraba desde el Plan 029) colándose por la puerta nueva de la cola,
// y justo sobre el recurso que el semáforo de 1 plaza existe para gobernar.
//
// Desde T3.0 esta rama es la ÚNICA puerta de elegibilidad que queda en el Edge: la copia del decorador
// murió con él. Si esto se rompe, ya no hay nadie detrás que lo tape.
func TestOnMessage_Cola_GrupoNaceNoElegible(t *testing.T) {
	cola := &spyCola{}
	l := listenerConCola(cola)

	l.handleEvent(context.Background(), grupoMessage("MSG-GRUPO", "quiero dos empanadas"))

	if len(cola.got) != 1 {
		t.Fatalf("se esperaba 1 fila encolada, hubo %d", len(cola.got))
	}
	item := cola.got[0]
	if item.Estado != app.EstadoClasificado {
		t.Fatalf("estado = %q, quería %q: un mensaje de grupo NO puede quedar reclamable por el cajero",
			item.Estado, app.EstadoClasificado)
	}
	// LITERAL A PROPÓSITO (mismo criterio que sin_texto/fastlane, ver arriba): el test fija el FORMATO DE
	// CABLE que se persiste en la columna, con independencia de la constante que hoy lo produce.
	if item.IntentJSON != `{"omitido":"no_elegible"}` {
		t.Fatalf("intent = %q, quería la marca de omisión por no elegible", item.IntentJSON)
	}
}

// TestOnMessage_Cola_ClasificadorApagadoNaceApagado: con la feature apagada, un mensaje que por lo demás
// sería perfectamente clasificable (directo, con texto, que el fastlane no atrapa) nace resuelto con
// `apagado`. Filtrarlo AQUÍ y no en el cajero es el punto de T2.12: en el cajero ya habría costado una
// plaza del semáforo y un claim.
func TestOnMessage_Cola_ClasificadorApagadoNaceApagado(t *testing.T) {
	cola := &spyCola{}
	l := listenerConCola(cola, WithClasificadorActivo(apagado))

	l.handleEvent(context.Background(), liveMessage("MSG-OFF", "quiero dos empanadas"))

	if len(cola.got) != 1 {
		t.Fatalf("se esperaba 1 fila encolada, hubo %d", len(cola.got))
	}
	item := cola.got[0]
	if item.Estado != app.EstadoClasificado {
		t.Fatalf("estado = %q, quería %q: con la feature apagada no hay a quién preguntar",
			item.Estado, app.EstadoClasificado)
	}
	if item.IntentJSON != `{"omitido":"apagado"}` {
		t.Fatalf("intent = %q, quería la marca de omisión por feature apagada", item.IntentJSON)
	}
}

// TestOnMessage_Cola_OrdenDeLosMotivos: el ORDEN de las ramas es contrato, no casualidad. Se comprueban los
// tres solapes que importan; en los tres los cinco caminos acabarían igual (mensaje SIN intención), así que
// lo único que se está fijando es QUÉ SE APRENDE al mirar la telemetría — que es de lo que va INV-051.3.
func TestOnMessage_Cola_OrdenDeLosMotivos(t *testing.T) {
	cases := []struct {
		nombre  string
		activo  func() bool // nil ⇒ opción no cableada (clasificador activo por default)
		mensaje func() *events.Message
		want    string
		porque  string
	}{
		{
			nombre:  "grupo sin texto gana sin_texto",
			mensaje: func() *events.Message { return grupoSinTexto("MSG-G-IMG") },
			want:    `{"omitido":"sin_texto"}`,
			porque:  "una imagen de grupo no tiene NADA que clasificar; contarla como no_elegible escondería el volumen de tráfico no textual",
		},
		{
			nombre:  "grupo con la feature apagada gana no_elegible",
			activo:  apagado,
			mensaje: func() *events.Message { return grupoMessage("MSG-G-OFF", "quiero dos empanadas") },
			want:    `{"omitido":"no_elegible"}`,
			porque:  "ser de grupo es una propiedad del MENSAJE y no cambia al encender la feature; el estado del sistema se juzga después",
		},
		{
			nombre:  "texto del fastlane con la feature apagada gana apagado",
			activo:  apagado,
			mensaje: func() *events.Message { return liveMessage("MSG-FAST-OFF", "2") },
			want:    `{"omitido":"apagado"}`,
			porque:  "si el fastlane fuera antes, con la feature apagada la telemetría mostraría una MEZCLA de fastlane y apagado en vez del 100 % de apagado que explica lo que pasa",
		},
	}
	for _, c := range cases {
		t.Run(c.nombre, func(t *testing.T) {
			cola := &spyCola{}
			var opts []ListenerOption
			if c.activo != nil {
				opts = append(opts, WithClasificadorActivo(c.activo))
			}
			l := listenerConCola(cola, opts...)

			l.handleEvent(context.Background(), c.mensaje())

			if len(cola.got) != 1 {
				t.Fatalf("se esperaba 1 fila encolada, hubo %d", len(cola.got))
			}
			item := cola.got[0]
			if item.Estado != app.EstadoClasificado {
				t.Fatalf("estado = %q, quería %q", item.Estado, app.EstadoClasificado)
			}
			if item.IntentJSON != c.want {
				t.Fatalf("intent = %q, quería %q — %s", item.IntentJSON, c.want, c.porque)
			}
		})
	}
}

// TestOnMessage_Cola_SinPredicadoNoSeFiltraPorApagado: el DEFAULT SEGURO. Sin WithClasificadorActivo (la
// opción no cableada, o cableada con nil) el filtro NO se activa por accidente: un mensaje normal sigue
// naciendo `nuevo` y llega al cajero. La asimetría es deliberada — un cableado incompleto debe clasificar
// de más (ruidoso, medible, sin pérdida de negocio), nunca dejar de clasificar en silencio.
func TestOnMessage_Cola_SinPredicadoNoSeFiltraPorApagado(t *testing.T) {
	for _, c := range []struct {
		nombre string
		opts   []ListenerOption
	}{
		{"opción no cableada", nil},
		{"opción cableada con nil", []ListenerOption{WithClasificadorActivo(nil)}},
	} {
		t.Run(c.nombre, func(t *testing.T) {
			cola := &spyCola{}
			l := listenerConCola(cola, c.opts...)

			l.handleEvent(context.Background(), liveMessage("MSG-DEFAULT", "quiero dos empanadas"))

			if len(cola.got) != 1 {
				t.Fatalf("se esperaba 1 fila encolada, hubo %d", len(cola.got))
			}
			item := cola.got[0]
			if item.Estado != app.EstadoNuevo || item.IntentJSON != "" {
				t.Fatalf("estado=%q intent=%q; sin predicado el clasificador se considera ACTIVO y la fila debe quedar reclamable",
					item.Estado, item.IntentJSON)
			}
		})
	}
}

// TestOnMessage_Cola_DirectoConClasificadorActivoNaceNuevo: la NO-REGRESIÓN del camino que SÍ debe llegar
// al cajero. Un mensaje directo, con texto, con el clasificador activo y que el fastlane no atrapa sigue
// naciendo `nuevo` y sin sobre — si esto se rompe, T2.12 habría apagado el clasificador entero en vez de
// filtrar lo que sobra.
func TestOnMessage_Cola_DirectoConClasificadorActivoNaceNuevo(t *testing.T) {
	cola := &spyCola{}
	l := listenerConCola(cola, WithClasificadorActivo(func() bool { return true }))

	l.handleEvent(context.Background(), liveMessage("MSG-VIVO", "quiero dos empanadas"))

	if len(cola.got) != 1 {
		t.Fatalf("se esperaba 1 fila encolada, hubo %d", len(cola.got))
	}
	item := cola.got[0]
	if item.Estado != app.EstadoNuevo || item.IntentJSON != "" {
		t.Fatalf("estado=%q intent=%q; el camino sano debe nacer nuevo y SIN sobre (lo reclama el cajero)",
			item.Estado, item.IntentJSON)
	}
}

// TestOnMessage_Cola_ColaNilNoConsultaElInterruptor: el predicado NO se llama cuando no hay cola — el
// interruptor solo gobierna el nacimiento de una fila, y enqueueCola vuelve antes de tocarlo.
func TestOnMessage_Cola_ColaNilNoConsultaElInterruptor(t *testing.T) {
	consultas := 0
	l := NewListener(quietLogger(), WithClasificadorActivo(func() bool {
		consultas++
		return false
	})) // sin WithCola

	l.handleEvent(context.Background(), liveMessage("MSG-SINCOLA", "quiero dos empanadas"))

	if consultas != 0 {
		t.Fatalf("el interruptor se consultó %d veces sin cola cableada; no debía consultarse ninguna", consultas)
	}
}

// TestOnMessage_ColaNil_ToleraElNilSinRomperNiContar prueba el COMPORTAMIENTO DEFENSIVO del listener ante
// una cola nil: no entra en pánico, no toca el estado de conexión y no inventa contadores.
//
// ⚠️ ESTE TEST FUE OTRA COSA HASTA EL 2026-08-17 y conviene saber qué, para no leerlo como más de lo que
// es. Se llamaba `TestOnMessage_ColaNil_ElMensajeSePierde` y documentaba —sin celebrarlo— un agujero de
// PRODUCCIÓN: T3.0 retiró el `sink.Deliver` inline, la apertura de `cola_entrantes.db` en el daemon seguía
// sin ser fatal, y con las dos cosas a la vez un listener sin cola no anotaba, no entregaba y no fallaba —
// el mensaje se evaporaba con el socket conectado, que es el modo de fallo más difícil de diagnosticar en
// campo («el cliente escribe y no pasa nada»).
//
// 🔴 ESE ESCENARIO YA NO DESCRIBE PRODUCCIÓN: el daemon NO ARRANCA si la cola no se puede abrir, migrar o
// construir (internal/infra/daemon). Lo que este test cubre ahora es lo único que sigue siendo cierto: que
// el camino nil-safe del listener AGUANTA, para los cableados de test y para el código defensivo. Que el
// arranque falle lo garantizan los `return fmt.Errorf(...)` del daemon, no un test de este paquete.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: quitar la guarda `if l.cola == nil { return }` de `enqueueCola` ⇒
// desreferencia de interfaz nil ⇒ pánico dentro del handler, es decir dentro del bucle de eventos de
// whatsmeow.
func TestOnMessage_ColaNil_ToleraElNilSinRomperNiContar(t *testing.T) {
	l := NewListener(quietLogger()) // sin WithCola

	if l.cola != nil {
		t.Fatal("sin WithCola la cola debe quedar nil")
	}
	// No entra en pánico y el estado de conexión no se toca.
	l.handleEvent(context.Background(), liveMessage("MSG-NILCOLA", "hola"))

	if s := l.InboundStats(); s.ColaEnqueueErrors != 0 || s.ColaEnqueuePanics != 0 {
		t.Fatalf("sin cola no hay intento que contar: errores=%d panics=%d", s.ColaEnqueueErrors, s.ColaEnqueuePanics)
	}
}
