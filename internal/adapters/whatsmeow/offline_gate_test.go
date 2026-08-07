package whatsmeow

// offline_gate_test.go — fija el comportamiento del ADR-0037 (cortes A/B/C + §6) y el filtro de ECO PROPIO.
// Todos los tests son de caja blanca sobre el paquete: el gate se inspecciona por OfflineStats() y, cuando
// hace falta ejercitar la época o el watchdog sin esperas reales, por sus campos.

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

// selfMsgAt construye un ECO PROPIO (IsFromMe) sellado en el instante dado.
func selfMsgAt(id string, ts time.Time) *events.Message {
	e := msgAt(id, ts)
	e.Info.IsFromMe = true
	return e
}

// --- Filtro de ECO PROPIO ---

// TestOnMessage_IsFromMe_NoSubeALaNube fija el CAMBIO DE COMPORTAMIENTO aprobado: los mensajes que mandamos
// nosotros mismos dejan de subir a la nube. Antes de este cambio el dato solo servía para no gastar el
// clasificador (intent/sink.go:143) y el eco viajaba igual: en una sola corrida real subieron 403.
func TestOnMessage_IsFromMe_NoSubeALaNube(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger())

	l.handleEvent(context.Background(), selfMsgAt("ECO-1", liveTS()))

	if len(sink.got) != 0 {
		t.Fatalf("el eco propio NO debe llegar al sink, llegaron %d: %+v", len(sink.got), sink.got)
	}
	if got := l.OfflineStats().DroppedSelf; got != 1 {
		t.Fatalf("DroppedSelf = %d, quería 1", got)
	}
}

// TestOnMessage_NoPropio_SigueSubiendo: el filtro de eco propio NO puede llevarse por delante el tráfico
// ajeno, que es el que sostiene todo el producto.
func TestOnMessage_NoPropio_SigueSubiendo(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger())

	l.handleEvent(context.Background(), msgAt("AJENO-1", liveTS()))

	if len(sink.got) != 1 {
		t.Fatalf("el mensaje ajeno debía subir, llegaron %d", len(sink.got))
	}
	if s := l.OfflineStats(); s.DroppedSelf != 0 || s.DroppedByGate != 0 || s.DroppedByAge != 0 {
		t.Fatalf("no debía descartarse nada: %+v", s)
	}
}

// --- Corte A: el gate por los corchetes del servidor ---

// TestGateA_CorcheteDescartaVivosYCompletedLoCierra: dentro del corchete se descarta hasta un mensaje
// RECIÉN sellado (lo que prueba que corta A y no el cinturón); tras el completed, el tráfico vuelve a pasar.
func TestGateA_CorcheteDescartaVivosYCompletedLoCierra(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger())
	ctx := context.Background()

	l.handleEvent(ctx, &events.OfflineSyncPreview{Total: 5, Messages: 3, Receipts: 2})
	l.handleEvent(ctx, msgAt("RAFAGA-1", liveTS()))
	l.handleEvent(ctx, msgAt("RAFAGA-2", liveTS()))

	if len(sink.got) != 0 {
		t.Fatalf("dentro del corchete no debe entregarse nada, entregó %d", len(sink.got))
	}
	s := l.OfflineStats()
	if s.DroppedByGate != 2 {
		t.Fatalf("DroppedByGate = %d, quería 2", s.DroppedByGate)
	}
	// Contadores SEPARADOS (ADR-0037 §4): un descarte de A jamás debe contarse como B.
	if s.DroppedByAge != 0 {
		t.Fatalf("un descarte del gate no puede contarse como antigüedad: %+v", s)
	}

	l.handleEvent(ctx, &events.OfflineSyncCompleted{Count: 5})
	l.handleEvent(ctx, msgAt("VIVO-1", liveTS()))

	if len(sink.got) != 1 || sink.got[0].MessageID != "VIVO-1" {
		t.Fatalf("tras el completed el tráfico vivo debe pasar: %+v", sink.got)
	}
	if b := l.OfflineStats().Brackets; b != 1 {
		t.Fatalf("Brackets = %d, quería 1 corchete cerrado", b)
	}
}

// TestGateA_SinPreviewNoDescartaNada: si no se perdió nada el servidor NO emite preview, y el gate debe
// quedarse quieto. Es el escenario más común y el que no puede romperse.
func TestGateA_SinPreviewNoDescartaNada(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger())

	l.handleEvent(context.Background(), msgAt("VIVO", liveTS()))

	if len(sink.got) != 1 {
		t.Fatalf("sin corchete abierto el mensaje debe pasar: %+v", sink.got)
	}
}

// --- Corte B: el cinturón por antigüedad ---

// TestCinturonB_DescartaViejoYDejaPasarVivo: sin corchete alguno (la fuga que A no cubre), un entrante
// viejo cae por antigüedad y uno reciente pasa.
func TestCinturonB_DescartaViejoYDejaPasarVivo(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger())
	ctx := context.Background()

	l.handleEvent(ctx, msgAt("VIEJO", time.Now().Add(-defaultInboundMaxAge-time.Minute)))
	if len(sink.got) != 0 {
		t.Fatalf("el entrante viejo NO debía entregarse: %+v", sink.got)
	}
	s := l.OfflineStats()
	if s.DroppedByAge != 1 {
		t.Fatalf("DroppedByAge = %d, quería 1", s.DroppedByAge)
	}
	if s.DroppedByGate != 0 {
		t.Fatalf("sin corchete abierto nada puede contarse como descarte del gate: %+v", s)
	}

	// El borde de dentro sí pasa: el cinturón yerra hacia DEJAR PASAR (ADR-0037 §B).
	l.handleEvent(ctx, msgAt("CASI", time.Now().Add(-defaultInboundMaxAge+time.Minute)))
	if len(sink.got) != 1 || sink.got[0].MessageID != "CASI" {
		t.Fatalf("un entrante dentro del umbral debe pasar: %+v", sink.got)
	}
}

// TestCinturonB_UmbralConfigurable: SetInboundMaxAge manda sobre el default.
func TestCinturonB_UmbralConfigurable(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger())
	l.SetInboundMaxAge(time.Minute)

	// Con el default (15 min) este mensaje pasaría; con el umbral de 1 min, no.
	l.handleEvent(context.Background(), msgAt("5MIN", time.Now().Add(-5*time.Minute)))

	if len(sink.got) != 0 {
		t.Fatalf("con umbral de 1 min un mensaje de 5 min debe caer: %+v", sink.got)
	}
	if got := l.OfflineStats().DroppedByAge; got != 1 {
		t.Fatalf("DroppedByAge = %d, quería 1", got)
	}
}

// --- §6: la bandera no puede quedarse pegada ---

// TestGate_ConnectedNoDesarmaElCorchete es la CORRECCIÓN al punto 6 del ADR. El documento dice que
// *events.Connected puede desarmar la bandera y que «es seguro»; no lo es: handleConnectSuccess despacha
// Connected desde una goroutine que antes hace I/O de red (connectionevents.go:183-200), mientras que el
// preview se despacha síncrono (connectionevents.go:80). Un Connected retrasado cortaría la ráfaga a la
// mitad. Este test fija que Connected NO toca el gate.
func TestGate_ConnectedNoDesarmaElCorchete(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger())
	ctx := context.Background()

	l.handleEvent(ctx, &events.OfflineSyncPreview{Total: 2, Messages: 2})
	l.handleEvent(ctx, &events.Connected{})
	l.handleEvent(ctx, msgAt("RAFAGA", liveTS()))

	if len(sink.got) != 0 {
		t.Fatalf("Connected NO debe desarmar el corchete; se entregó %+v", sink.got)
	}
	if got := l.OfflineStats().DroppedByGate; got != 1 {
		t.Fatalf("DroppedByGate = %d, quería 1 (el corchete seguía armado)", got)
	}
}

// TestGate_DisconnectedCierraElCorchete: el socket muere con el corchete abierto ⇒ se cierra (motivo
// reconexión) y el tráfico de la conexión siguiente pasa. Sin esto una bandera armada descartaría tráfico
// VIVO para siempre: el fallo más grave que este mecanismo puede producir, y silencioso.
func TestGate_DisconnectedCierraElCorchete(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger())
	ctx := context.Background()

	l.handleEvent(ctx, &events.OfflineSyncPreview{Total: 9, Messages: 9})
	l.handleEvent(ctx, &events.Disconnected{})
	l.handleEvent(ctx, msgAt("VIVO-TRAS-RECONEXION", liveTS()))

	if len(sink.got) != 1 {
		t.Fatalf("tras la desconexión el corchete debe estar cerrado: %+v", sink.got)
	}
	if b := l.OfflineStats().Brackets; b != 1 {
		t.Fatalf("Brackets = %d, quería 1", b)
	}
}

// TestGate_LoggedOutCierraElCorchete: mismo cierre por LoggedOut.
func TestGate_LoggedOutCierraElCorchete(t *testing.T) {
	sink := &spySink{}
	l := NewListener(sink, quietLogger())
	ctx := context.Background()

	l.handleEvent(ctx, &events.OfflineSyncPreview{Total: 1, Messages: 1})
	l.handleEvent(ctx, &events.LoggedOut{})
	l.handleEvent(ctx, msgAt("VIVO", liveTS()))

	if len(sink.got) != 1 {
		t.Fatalf("tras LoggedOut el corchete debe estar cerrado: %+v", sink.got)
	}
}

// TestGate_EpocaInvalidaCorcheteHeredado fija la validación de ÉPOCA en sí misma, aparte del cierre: un
// corchete armado en la época N no descarta nada una vez observada una desconexión (época N+1). Se fuerza
// la época a mano porque bumpEpoch además cierra; aquí se prueba la SEGUNDA línea de defensa, la que
// protege si el cierre no llegara a ocurrir.
func TestGate_EpocaInvalidaCorcheteHeredado(t *testing.T) {
	g := newOfflineGate(quietLogger(), defaultOfflineWatchdog)
	g.arm(&events.OfflineSyncPreview{Total: 3, Messages: 3})

	now := time.Now()
	if drop, _ := g.dropInBurst(now, now); !drop {
		t.Fatal("en su propia época el corchete debe descartar")
	}

	g.mu.Lock()
	g.epoch++ // desconexión observada SIN pasar por el cierre
	g.mu.Unlock()

	drop, closed := g.dropInBurst(now, now)
	if drop {
		t.Fatal("un corchete de una época anterior JAMÁS puede descartar tráfico de la siguiente")
	}
	if closed == nil {
		t.Fatal("el corchete heredado debía cerrarse por higiene")
	}
}

// TestGate_WatchdogDesarmaYDejaPasar: si el completed no llega nunca, el plazo de seguridad libera el
// corchete y el tráfico vuelve a pasar. Es el watchdog PEREZOSO (el que corre en la ruta del mensaje), la
// propiedad de seguridad que no depende de que el timer se despierte a tiempo.
func TestGate_WatchdogDesarmaYDejaPasar(t *testing.T) {
	g := newOfflineGate(quietLogger(), 50*time.Millisecond)
	g.arm(&events.OfflineSyncPreview{Total: 4, Messages: 4})

	base := time.Now()
	if drop, _ := g.dropInBurst(base, base); !drop {
		t.Fatal("dentro del plazo el corchete debe descartar")
	}

	vencido := base.Add(200 * time.Millisecond)
	drop, closed := g.dropInBurst(vencido, vencido)
	if drop {
		t.Fatal("pasado el watchdog el corchete NO puede seguir descartando")
	}
	if closed == nil || closed.reason != closeWatchdog {
		t.Fatalf("el cierre debía ser por watchdog: %+v", closed)
	}
}

// TestGate_WatchdogTimerCierraSinTrafico: aunque no vuelva a entrar ni un mensaje, el timer cierra el
// corchete y su cuenta no se pierde. Es lo que garantiza que la línea agregada SIEMPRE se emita.
func TestGate_WatchdogTimerCierraSinTrafico(t *testing.T) {
	g := newOfflineGate(quietLogger(), 20*time.Millisecond)
	g.arm(&events.OfflineSyncPreview{Total: 7, Messages: 7})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if g.snapshot().Brackets == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("el timer del watchdog debía cerrar el corchete sin necesidad de más tráfico")
}

// --- §4: las edades que deciden la ventana de gracia ---

// TestGate_EdadesDelCorchete: la línea agregada lleva la edad del entrante MÁS RECIENTE y la del MÁS VIEJO
// descartados. Es el dato que distingue una ráfaga de segundos de una de horas — exactamente lo que falta
// para decidir la ventana de gracia de la microcaída (ADR-0037 §Puntos abiertos).
func TestGate_EdadesDelCorchete(t *testing.T) {
	g := newOfflineGate(quietLogger(), defaultOfflineWatchdog)
	g.arm(&events.OfflineSyncPreview{Total: 3, Messages: 3})

	now := time.Now()
	g.dropInBurst(now.Add(-2*time.Hour), now)
	g.dropInBurst(now.Add(-30*time.Second), now)
	g.dropInBurst(now.Add(-45*time.Minute), now)

	g.mu.Lock()
	s := g.closeLocked(closeCompleted)
	g.mu.Unlock()

	if s.dropped != 3 {
		t.Fatalf("dropped = %d, quería 3", s.dropped)
	}
	if s.newestAge.Round(time.Second) != 30*time.Second {
		t.Fatalf("edad del más reciente = %s, quería 30s", s.newestAge)
	}
	if s.oldestAge.Round(time.Second) != 2*time.Hour {
		t.Fatalf("edad del más viejo = %s, quería 2h", s.oldestAge)
	}
}

// TestGate_EdadNegativaNoCuentaComoNegativa: un desfase de reloj puede sellar un mensaje "en el futuro";
// la edad se satura en cero en vez de contaminar la métrica con negativos.
func TestGate_EdadNegativaNoCuentaComoNegativa(t *testing.T) {
	g := newOfflineGate(quietLogger(), defaultOfflineWatchdog)
	g.arm(&events.OfflineSyncPreview{Total: 1, Messages: 1})

	now := time.Now()
	g.dropInBurst(now.Add(time.Hour), now) // mensaje del futuro

	g.mu.Lock()
	s := g.closeLocked(closeCompleted)
	g.mu.Unlock()

	if s.newestAge < 0 || s.oldestAge < 0 {
		t.Fatalf("las edades no pueden ser negativas: %+v", s)
	}
}

// --- §5: los acuses NO se filtran ---

// TestReceipt_NoSeFiltraPorAntiguedadNiPorCorchete fija la asimetría deliberada del ADR-0037 §5. Un acuse
// legítimo de una entrega ocurrida durante la caída es VIEJO POR DEFINICIÓN (Receipt.Timestamp es el
// instante del acuse, no el del mensaje original), así que filtrarlo por edad borraría exactamente la
// prueba de entrega que queremos conservar.
func TestReceipt_NoSeFiltraPorAntiguedadNiPorCorchete(t *testing.T) {
	l := NewListener(&spySink{}, quietLogger())
	var got []domain.ReceiptEvent
	l.onReceipt = func(r domain.ReceiptEvent) { got = append(got, r) }
	ctx := context.Background()

	// Corchete abierto Y acuse viejo: ni una cosa ni la otra puede detenerlo.
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
	// Cliente de valor cero: disableHistorySync solo escribe banderas, no llama a whatsmeow.
	c := &wm.Client{}
	disableHistorySync(c)

	if !c.ManualHistorySyncDownload {
		t.Fatal("ManualHistorySyncDownload debía quedar en true (corte C)")
	}
	if c.DisableManualHistorySyncReceipt {
		t.Fatal("DisableManualHistorySyncReceipt debe quedarse en false: acusamos sin descargar")
	}
}
