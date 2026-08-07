package whatsmeow

// inbound_window_test.go — fija el criterio del ADR-0037 (ventana temporal contra el inicio de conexión),
// el filtro de eco propio y el corte C.

import (
	"context"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// msgAt construye un *events.Message de texto sellado en el instante dado.
func msgAt(id string, ts time.Time) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Sender: newJID("593999", types.DefaultUserServer)},
			ID:            id,
			Timestamp:     ts,
		},
		Message: &waE2E.Message{Conversation: proto.String("hola")},
	}
}

// listenerSealed devuelve un listener cuyo inicio de conexión es el instante dado (sello controlado).
func listenerSealed(sink *spySink, seal time.Time) *Listener {
	l := NewListener(sink, quietLogger())
	l.SetConnectSeal(func() time.Time { return seal })
	return l
}

// --- El criterio: la ventana temporal ---

// TestVentana_DescartaLoAnteriorAlUmbral: un mensaje enviado mucho antes de que el socket subiera cae.
func TestVentana_DescartaLoAnteriorAlUmbral(t *testing.T) {
	sink := &spySink{}
	seal := time.Now().Add(-time.Minute) // la conexión subió hace un minuto
	l := listenerSealed(sink, seal)

	l.handleEvent(context.Background(), msgAt("VIEJO", seal.Add(-6*time.Hour)))

	if len(sink.got) != 0 {
		t.Fatalf("un entrante de 6 h antes de conectar NO debe subir: %+v", sink.got)
	}
	if got := l.InboundStats().DroppedByWindow; got != 1 {
		t.Fatalf("DroppedByWindow = %d, quería 1", got)
	}
}

// TestVentana_NuestroAtascoNoCuentaComoAntiguedad es EL test del rediseño. Un mensaje enviado justo
// después de que el socket subió sigue siendo válido aunque lo procesemos MUCHO más tarde, porque el
// umbral cuelga del inicio de la conexión y no de time.Now(). Con un criterio basado en `now` este mismo
// mensaje se habría descartado por nuestra propia lentitud — que es lo que midió el incidente del
// 2026-08-06 (71 % de saturación sostenida, respiro máximo de 2,3 s).
func TestVentana_NuestroAtascoNoCuentaComoAntiguedad(t *testing.T) {
	sink := &spySink{}
	seal := time.Now().Add(-30 * time.Minute) // el socket lleva media hora arriba
	l := listenerSealed(sink, seal)

	// Mensaje enviado 1 s DESPUÉS de conectar, pero que llega a onMessage 30 min más tarde (pipeline
	// atascado). Su edad contra `now` sería de ~30 min: un criterio basado en `now` lo mataría.
	vivo := msgAt("VIVO-ATASCADO", seal.Add(time.Second))
	l.handleEvent(context.Background(), vivo)

	if len(sink.got) != 1 {
		t.Fatalf("un mensaje posterior al inicio de conexión debe subir aunque lo procesemos tarde: %+v", sink.got)
	}
	if got := l.InboundStats().DroppedByWindow; got != 0 {
		t.Fatalf("no debía descartarse nada: DroppedByWindow = %d", got)
	}
}

// TestVentana_MargenRescataLaMicrocaida: lo enviado dentro del margen ANTES de reconectar se trata como
// vivo. Es la ventana de rescate que el diseño anterior no tenía: una microcaída de 30 s ya no pierde el
// «1» que el cliente tecleó justo antes.
func TestVentana_MargenRescataLaMicrocaida(t *testing.T) {
	sink := &spySink{}
	seal := time.Now()
	l := listenerSealed(sink, seal)

	l.handleEvent(context.Background(), msgAt("MICROCAIDA", seal.Add(-30*time.Second)))

	if len(sink.got) != 1 {
		t.Fatalf("dentro del margen el entrante debe subir (rescate de la microcaída): %+v", sink.got)
	}
}

// TestVentana_MargenConfigurable: el margen manda sobre el borde del descarte.
func TestVentana_MargenConfigurable(t *testing.T) {
	seal := time.Now()

	// Con el default (5 min), un mensaje de 4 min antes de conectar entra.
	sink := &spySink{}
	l := listenerSealed(sink, seal)
	l.handleEvent(context.Background(), msgAt("4MIN", seal.Add(-4*time.Minute)))
	if len(sink.got) != 1 {
		t.Fatalf("con margen de 5 min, 4 min antes debe entrar: %+v", sink.got)
	}

	// Con un margen de 1 min, el mismo mensaje cae.
	sink2 := &spySink{}
	l2 := listenerSealed(sink2, seal)
	l2.SetConnectMargin(time.Minute)
	l2.handleEvent(context.Background(), msgAt("4MIN", seal.Add(-4*time.Minute)))
	if len(sink2.got) != 0 {
		t.Fatalf("con margen de 1 min, 4 min antes debe caer: %+v", sink2.got)
	}
}

// --- resolveThreshold: respaldo y saneamiento del sello ---

func TestResolveThreshold(t *testing.T) {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	margin := 5 * time.Minute

	t.Run("sello normal manda", func(t *testing.T) {
		seal := now.Add(-time.Hour)
		if got, want := resolveThreshold(seal, margin, now), seal.Add(-margin); !got.Equal(want) {
			t.Fatalf("umbral = %s, quería %s", got, want)
		}
	})

	t.Run("sello cero cae a now", func(t *testing.T) {
		if got, want := resolveThreshold(time.Time{}, margin, now), now.Add(-margin); !got.Equal(want) {
			t.Fatalf("umbral = %s, quería %s", got, want)
		}
	})

	// Un sello FUTURO es imposible (una conexión no empieza después de ahora): es la firma de una lectura
	// rota bajo F2/F3 o de un salto de reloj, y su modo de fallo es el CARO — un umbral futuro descartaría
	// tráfico vivo. Se trata como ausencia.
	t.Run("sello futuro se descarta y cae a now", func(t *testing.T) {
		if got, want := resolveThreshold(now.Add(time.Hour), margin, now), now.Add(-margin); !got.Equal(want) {
			t.Fatalf("umbral = %s, quería %s", got, want)
		}
	})
}

// TestVentana_SinSelloUsaNow: sin sello (arranque en frío) el respaldo es `now`, que es más estricto QUE EL
// SELLO. Ojo con leer de más: no garantiza que la ráfaga no escape — ambos umbrales son reloj local y la
// hora del mensaje es del servidor, así que un reloj local atrasado la deja pasar igual (ver
// resolveThreshold). Este test fija el respaldo, no una garantía absoluta.
func TestVentana_SinSelloUsaNow(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger()) // connectSeal nil
	ctx := context.Background()

	l.handleEvent(ctx, msgAt("VIEJO", time.Now().Add(-time.Hour)))
	if len(sink.got) != 0 {
		t.Fatalf("sin sello, un entrante de 1 h debe caer: %+v", sink.got)
	}
	l.handleEvent(ctx, msgAt("VIVO", time.Now()))
	if len(sink.got) != 1 {
		t.Fatalf("sin sello, un entrante recién enviado debe subir: %+v", sink.got)
	}
}

// --- El punto ciego: entrante sin hora utilizable ---

// TestVentana_SinHoraSeAdmite fija la asimetría del ADR: ante la duda, DEJAR PASAR. No es teórico —
// GetUnixTime devuelve time.Time{} con ok=true y SIN error cuando el atributo `t` vale "0"
// (binary/attrs.go:116-123), así que el mensaje llega con Timestamp cero. Cero es anterior a CUALQUIER
// umbral: dejarlo caer por la comparación lo descartaría en silencio.
func TestVentana_SinHoraSeAdmite(t *testing.T) {
	sink := &spySink{}
	seal := time.Now()
	l := listenerSealed(sink, seal)

	l.handleEvent(context.Background(), msgAt("SIN-HORA", time.Time{}))

	if len(sink.got) != 1 || sink.got[0].MessageID != "SIN-HORA" {
		t.Fatalf("un entrante sin hora utilizable debe ADMITIRSE, no descartarse: %+v", sink.got)
	}
	s := l.InboundStats()
	if s.AdmittedNoTimestamp != 1 {
		t.Fatalf("AdmittedNoTimestamp = %d, quería 1", s.AdmittedNoTimestamp)
	}
	if s.DroppedByWindow != 0 {
		t.Fatalf("no puede contarse como descarte de la ventana: %+v", s)
	}
}

// --- Eco propio ---

// TestOnMessage_IsFromMe_NoSubeALaNube fija el CAMBIO DE COMPORTAMIENTO aprobado: los mensajes que
// mandamos nosotros dejan de subir. Antes el dato solo servía para no gastar el clasificador
// (intent/sink.go:143) y el eco viajaba igual: en una sola corrida real subieron 403.
func TestOnMessage_IsFromMe_NoSubeALaNube(t *testing.T) {
	sink := &spySink{}
	l := listenerSealed(sink, time.Now())

	eco := msgAt("ECO-1", time.Now())
	eco.Info.IsFromMe = true
	l.handleEvent(context.Background(), eco)

	if len(sink.got) != 0 {
		t.Fatalf("el eco propio NO debe llegar al sink: %+v", sink.got)
	}
	if got := l.InboundStats().DroppedSelf; got != 1 {
		t.Fatalf("DroppedSelf = %d, quería 1", got)
	}
}

// TestOnMessage_NoPropio_SigueSubiendo: el filtro de eco propio no puede llevarse por delante el tráfico
// ajeno, que es el que sostiene el producto.
func TestOnMessage_NoPropio_SigueSubiendo(t *testing.T) {
	sink := &spySink{}
	l := listenerSealed(sink, time.Now())

	l.handleEvent(context.Background(), msgAt("AJENO", time.Now()))

	if len(sink.got) != 1 {
		t.Fatalf("el mensaje ajeno debía subir: %+v", sink.got)
	}
	if s := l.InboundStats(); s.DroppedSelf != 0 || s.DroppedByWindow != 0 {
		t.Fatalf("no debía descartarse nada: %+v", s)
	}
}

// --- Corchetes: SOLO observabilidad ---

// TestCorchetes_NoDecidenNada es la diferencia con el diseño anterior: con un corchete ABIERTO, un mensaje
// vivo sigue subiendo. Los corchetes ya no son un interruptor; el criterio es la hora del mensaje.
func TestCorchetes_NoDecidenNada(t *testing.T) {
	sink := &spySink{}
	seal := time.Now()
	l := listenerSealed(sink, seal)
	ctx := context.Background()

	l.handleEvent(ctx, &events.OfflineSyncPreview{Total: 5, Messages: 3, Receipts: 2})
	l.handleEvent(ctx, msgAt("VIVO-EN-CORCHETE", seal.Add(time.Second)))

	if len(sink.got) != 1 {
		t.Fatalf("un mensaje vivo debe subir aunque haya corchete abierto: %+v", sink.got)
	}
}

// TestCorchetes_Reconcilian: dentro del corchete se contabiliza lo que la VENTANA descartó, que es la
// pareja anunciado-vs-descartado — la única calibración posible de un descarte silencioso.
func TestCorchetes_Reconcilian(t *testing.T) {
	sink := &spySink{}
	seal := time.Now()
	l := listenerSealed(sink, seal)
	ctx := context.Background()

	l.handleEvent(ctx, &events.OfflineSyncPreview{Total: 3, Messages: 3})
	l.handleEvent(ctx, msgAt("R1", seal.Add(-2*time.Hour)))
	l.handleEvent(ctx, msgAt("R2", seal.Add(-45*time.Minute)))
	l.handleEvent(ctx, &events.OfflineSyncCompleted{Count: 3})

	s := l.InboundStats()
	if s.DroppedByWindow != 2 {
		t.Fatalf("DroppedByWindow = %d, quería 2", s.DroppedByWindow)
	}
	if s.Brackets != 1 {
		t.Fatalf("Brackets = %d, quería 1", s.Brackets)
	}
}

// TestCorchetes_Edades: la línea de reconciliación lleva la edad del más reciente y la del más viejo
// descartados. Es lo que distingue una ráfaga de segundos de una de horas.
func TestCorchetes_Edades(t *testing.T) {
	b := newBracketObserver(quietLogger())
	b.arm(&events.OfflineSyncPreview{Total: 3, Messages: 3})

	b.countWindowDrop(2 * time.Hour)
	b.countWindowDrop(30 * time.Second)
	b.countWindowDrop(45 * time.Minute)

	b.mu.Lock()
	s := b.closeLocked(closeCompleted)
	b.mu.Unlock()

	if s.dropped != 3 {
		t.Fatalf("dropped = %d, quería 3", s.dropped)
	}
	if s.newestAge != 30*time.Second {
		t.Fatalf("edad del más reciente = %s, quería 30s", s.newestAge)
	}
	if s.oldestAge != 2*time.Hour {
		t.Fatalf("edad del más viejo = %s, quería 2h", s.oldestAge)
	}
}

// --- Los acuses NO se filtran ---

// TestReceipt_NoSeFiltra fija la asimetría deliberada del ADR-0037 §5: Receipt.Timestamp es el instante
// del ACUSE, no el del mensaje original, así que un acuse legítimo de una entrega ocurrida durante la
// caída es viejo por definición. Filtrarlo borraría la prueba de entrega que queremos conservar.
func TestReceipt_NoSeFiltra(t *testing.T) {
	l := listenerSealed(&spySink{}, time.Now())
	var got []domain.ReceiptEvent
	l.onReceipt = func(r domain.ReceiptEvent) { got = append(got, r) }
	ctx := context.Background()

	l.handleEvent(ctx, &events.OfflineSyncPreview{Total: 3, Messages: 1, Receipts: 2})
	l.handleEvent(ctx, &events.Receipt{
		MessageIDs: []string{"M1"},
		Type:       types.ReceiptTypeDelivered,
		Timestamp:  time.Now().Add(-6 * time.Hour),
	})

	if len(got) != 1 {
		t.Fatalf("el acuse debía propagarse pese al corchete y a su antigüedad, hubo %d", len(got))
	}
}

// --- Corte C ---

// TestDisableHistorySync_PoneLaBanderaYConservaElAcuse: se corta la DESCARGA, pero se sigue acusando la
// notificación. Callar además el acuse dejaría al servidor reofreciendo el blob.
func TestDisableHistorySync_PoneLaBanderaYConservaElAcuse(t *testing.T) {
	c := &wm.Client{} // valor cero: disableHistorySync solo escribe banderas, no llama a whatsmeow.
	disableHistorySync(c)

	if !c.ManualHistorySyncDownload {
		t.Fatal("ManualHistorySyncDownload debía quedar en true (corte C)")
	}
	if c.DisableManualHistorySyncReceipt {
		t.Fatal("DisableManualHistorySyncReceipt debe quedarse en false: acusamos sin descargar")
	}
}
