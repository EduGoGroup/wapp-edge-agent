package whatsmeow

// offline_gate.go — el GATE de la RÁFAGA OFFLINE (ADR-0037, cortes A y §6). Es el estado por sesión que
// decide si un *events.Message llegó por el buzón que WhatsApp reencola tras una caída, para descartarlo
// ANTES de convertirlo en evento de dominio.
//
// Por qué existe un gate y no un simple flag: whatsmeow NO expone un marcador por mensaje. El atributo
// `offline` viaja en el nodo pero parseMessageInfo lee solo siete atributos y `offline` no está entre ellos
// (ADR-0037 §Contexto). Lo único que el servidor nos da son los CORCHETES de la ráfaga:
// *events.OfflineSyncPreview la abre y *events.OfflineSyncCompleted la cierra (connectionevents.go:79-90).
//
// ÉPOCA DE CONEXIÓN (corrección al punto 6 del ADR, verificada contra el código de whatsmeow):
// el ADR dice que *events.Connected puede desarmar la bandera y que «es seguro». NO lo es.
// handleConnectSuccess despacha Connected DENTRO de una goroutine (connectionevents.go:183) que antes hace
// UploadedPreKeyCount (:184), getServerPreKeyCount (:186) y el IQ SetPassive (:196) —todo I/O de red— y
// solo entonces dispatchEvent(&events.Connected{}) (:200). El preview, en cambio, se despacha SÍNCRONO
// desde el handlerQueue (connectionevents.go:80, sin `go`). ⇒ el preview puede ADELANTAR al Connected y un
// desarme por Connected cortaría la ráfaga a la mitad. Por eso Connected NO toca el gate: en su lugar
// llevamos un contador de época que avanza en *events.Disconnected/*events.LoggedOut y se VALIDA al
// descartar (una bandera armada en la época N no descarta nada en la N+1).
//
// Lo que la época SÍ compra: que un corchete armado por una conexión anterior jamás descarte tráfico vivo
// de la siguiente. Lo que NO compra, y por eso el watchdog es obligatorio: whatsmeow NO emite
// *events.Disconnected en una desconexión ESPERADA (client.go:576-580, `if !cli.isExpectedDisconnect()`),
// así que hay reconexiones en las que la época no avanza. Ese hueco lo tapan el watchdog y el cinturón B.
//
// Dirección del error: ante la duda, el gate DEJA PASAR. Un descarte de tráfico vivo es una pérdida de
// negocio invisible; un entrante algo viejo que se cuela llega deduplicado aguas abajo (ADR-0037 §B).
//
// ZERO-KNOWLEDGE / ADR-0034: aquí se CUENTA, no se transcribe. Ni texto ni teléfono entran en este fichero;
// solo cardinalidades, duraciones y edades. La ráfaga es nivel 3 del ADR-0034 y loguearla sería guardar por
// la puerta de atrás justo lo que se decidió no guardar.

import (
	"sync"
	"time"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/EduGoGroup/wapp-shared/logger"
)

// disableHistorySync aplica el CORTE C del ADR-0037 a un cliente whatsmeow: no descargar siquiera el blob
// de history sync. Se llama en los TRES NewClient del Edge (escucha, emparejamiento y envío efímero).
//
// No es cosmético: HOY pagamos la descarga entera para nada. Nuestro `default:` del switch tira el
// events.HistorySync, pero para entonces whatsmeow ya descargó, descomprimió y PERSISTIÓ —
// DownloadHistorySync (message.go:747) lanza doStorage en una goroutine que escribe NCT salt, mapeos
// PN↔LID, push names históricos y message secrets. Ignorar el evento no evita ni una escritura. Con la
// bandera puesta, handleProtocolMessage ni empuja la notificación al canal (message.go:854-856).
//
// DisableManualHistorySyncReceipt se deja en su default (false) A PROPÓSITO: con él en false seguimos
// enviando el acuse de la notificación (message.go:860 — la condición es
// `!(ManualHistorySyncDownload && DisableManualHistorySyncReceipt)`), es decir ACUSAMOS SIN DESCARGAR.
// Callar además el acuse dejaría la notificación sin confirmar y el servidor podría reofrecer el blob:
// nos ahorraría un frame y nos costaría reintentos. Acusar es lo barato y lo estable.
func disableHistorySync(client *wm.Client) {
	client.ManualHistorySyncDownload = true
}

// Motivos de cierre de un corchete (ADR-0037 §4). Solo closeCompleted es rutina; los otros dos son
// ANOMALÍA y salen en WARN.
const (
	// closeCompleted: cierre normal, lo cerró *events.OfflineSyncCompleted.
	closeCompleted = "completed"
	// closeWatchdog: venció el plazo de seguridad sin que llegara el completed.
	closeWatchdog = "watchdog"
	// closeReconnect: el socket cayó (Disconnected/LoggedOut) con el corchete abierto.
	closeReconnect = "reconexion"
)

// defaultOfflineWatchdog es el plazo de seguridad del corchete (ADR-0037 §6). No es arbitrario: es EL MISMO
// plazo tras el cual whatsmeow renuncia a serializar un nodo (10 vueltas de un ticker de 30 s,
// client.go:874-886). Pasado ese punto la garantía de orden en la que se apoya el gate ya no existe, así que
// mantener la bandera armada no defiende nada — y una bandera pegada descartaría tráfico VIVO para siempre.
const defaultOfflineWatchdog = 5 * time.Minute

// OfflineStats es el acumulado POR SESIÓN de descartes en la puerta de entrada (ADR-0037 §4). Los
// contadores del gate (A) y del cinturón (B) van SEPARADOS a propósito: si en campo es B quien descarta de
// forma habitual, significa que A está roto, y mezclarlos borraría justo esa señal.
//
// No lleva PII: son cardinalidades. Es el material que la telemetría de flota (ADR-0023) debe exponer.
type OfflineStats struct {
	// Brackets es cuántos corchetes de ráfaga se han cerrado en esta sesión.
	Brackets uint64
	// DroppedByGate son los entrantes descartados por el corchete del servidor (corte A).
	DroppedByGate uint64
	// DroppedByAge son los entrantes descartados por antigüedad (cinturón B). Creciente y sostenido ⇒ A falla.
	DroppedByAge uint64
	// DroppedSelf son los ECOS PROPIOS descartados (IsFromMe). No es parte del ADR-0037: es el filtro
	// aprobado aparte para dejar de subir a la nube lo que mandamos nosotros mismos.
	DroppedSelf uint64
}

// bracketSummary es la foto de un corchete al cerrarse: lo que se loguea. Se construye BAJO el lock y se
// emite FUERA, para no sostener el mutex durante la escritura del log.
type bracketSummary struct {
	reason      string
	duration    time.Duration
	annTotal    int
	annMessages int
	annReceipts int
	dropped     int
	// newestAge/oldestAge son las edades del entrante MÁS RECIENTE y del MÁS VIEJO descartados en el
	// corchete. Es el dato que faltaba para decidir la ventana de gracia de la microcaída (ADR-0037
	// §Puntos abiertos): distinguir una ráfaga de segundos de una de horas. Solo válidas si dropped > 0.
	newestAge time.Duration
	oldestAge time.Duration
}

// offlineGate es el estado del corchete de UNA sesión. Vive en el Listener, NUNCA como global de paquete
// (ADR-0037 §6, primer punto). Seguro para uso concurrente: whatsmeow despacha desde varias goroutines.
type offlineGate struct {
	log      logger.Logger
	watchdog time.Duration

	mu sync.Mutex
	// epoch avanza en cada Disconnected/LoggedOut observado. Ver la nota de cabecera.
	epoch uint64
	// armed / armedEpoch / armedAt describen el corchete VIVO (si armed es false, los demás no valen).
	armed      bool
	armedEpoch uint64
	armedAt    time.Time
	// timer es el watchdog del corchete vivo; se para en todo cierre. nil fuera de un corchete.
	timer *time.Timer

	// annTotal/annMessages/annReceipts es lo que ANUNCIÓ el preview (no lo que llegó).
	annTotal, annMessages, annReceipts int

	// burst* acumula lo descartado DENTRO del corchete vivo; se resetea al armar.
	burstDropped   int
	burstNewestAge time.Duration
	burstOldestAge time.Duration

	stats OfflineStats
}

// newOfflineGate construye el gate de una sesión. Un watchdog <= 0 cae al default (guardarraíl, no
// invariante): el plazo es una defensa, nunca debe quedar desactivado por una config mal puesta.
func newOfflineGate(log logger.Logger, watchdog time.Duration) *offlineGate {
	if watchdog <= 0 {
		watchdog = defaultOfflineWatchdog
	}
	return &offlineGate{log: log, watchdog: watchdog}
}

// arm abre un corchete al llegar el preview del servidor. Si ya había uno abierto (preview repetido sin
// completed) se cierra el anterior como anomalía antes de abrir el nuevo: nunca se pierde su cuenta.
func (g *offlineGate) arm(e *events.OfflineSyncPreview) {
	g.mu.Lock()
	prev := g.closeLocked(closeWatchdog)
	g.armed = true
	g.armedEpoch = g.epoch
	g.armedAt = time.Now()
	g.annTotal, g.annMessages, g.annReceipts = e.Total, e.Messages, e.Receipts
	g.burstDropped = 0
	g.burstNewestAge, g.burstOldestAge = 0, 0
	// El watchdog es OBLIGATORIO (ADR-0037 §6): garantiza que el corchete se cierre —y se contabilice—
	// aunque el completed no llegue nunca y aunque no vuelva a entrar ni un solo mensaje.
	g.timer = time.AfterFunc(g.watchdog, func() { g.close(closeWatchdog) })
	g.mu.Unlock()

	g.emit(prev)
	// El preview NO lleva contenido: solo cardinalidades del buzón (ADR-0034 nivel 3 respetado).
	g.log.Info("listener: ráfaga offline detectada; se descartan los entrantes hasta cerrar el corchete (ADR-0037)",
		"anunciado_total", e.Total, "anunciado_mensajes", e.Messages,
		"anunciado_acuses", e.Receipts, "anunciado_notificaciones", e.Notifications)
}

// close cierra el corchete vivo por el motivo dado y emite su línea agregada. No-op si no había corchete.
func (g *offlineGate) close(reason string) {
	g.mu.Lock()
	s := g.closeLocked(reason)
	g.mu.Unlock()
	g.emit(s)
}

// bumpEpoch avanza la época (el socket murió) y cierra el corchete vivo si lo había. Lo invocan
// *events.Disconnected y *events.LoggedOut. Ver la nota de cabecera sobre lo que la época sí y no compra.
func (g *offlineGate) bumpEpoch() {
	g.mu.Lock()
	g.epoch++
	s := g.closeLocked(closeReconnect)
	g.mu.Unlock()
	g.emit(s)
}

// closeLocked cierra el corchete y devuelve su resumen, o nil si no había ninguno abierto. El llamante
// SOSTIENE el lock y debe emitir el resumen fuera de él.
func (g *offlineGate) closeLocked(reason string) *bracketSummary {
	if !g.armed {
		return nil
	}
	if g.timer != nil {
		g.timer.Stop()
		g.timer = nil
	}
	g.armed = false
	g.stats.Brackets++
	return &bracketSummary{
		reason:      reason,
		duration:    time.Since(g.armedAt),
		annTotal:    g.annTotal,
		annMessages: g.annMessages,
		annReceipts: g.annReceipts,
		dropped:     g.burstDropped,
		newestAge:   g.burstNewestAge,
		oldestAge:   g.burstOldestAge,
	}
}

// emit escribe la LÍNEA AGREGADA por corchete cerrado (ADR-0037 §4). INFO si cerró el servidor; WARN en
// cualquier otro motivo, porque un cierre que no es `completed` es una anomalía, no rutina. El session_id
// no se pasa a mano: el logger del listener ya lo arrastra (sessionmgr/listen.go:192).
func (g *offlineGate) emit(s *bracketSummary) {
	if s == nil {
		return
	}
	args := []any{
		"motivo_cierre", s.reason,
		"duracion", s.duration.Round(time.Millisecond).String(),
		"anunciado_total", s.annTotal,
		"anunciado_mensajes", s.annMessages,
		"anunciado_acuses", s.annReceipts,
		"descartados", s.dropped,
	}
	if s.dropped > 0 {
		// Las dos edades dicen si la ráfaga fue de segundos o de horas — el dato que decide la ventana de
		// gracia de la microcaída. Son duraciones, no contenido.
		args = append(args,
			"edad_mas_reciente", s.newestAge.Round(time.Second).String(),
			"edad_mas_vieja", s.oldestAge.Round(time.Second).String())
	}
	const msg = "listener: corchete de ráfaga offline cerrado (ADR-0037)"
	if s.reason == closeCompleted {
		g.log.Info(msg, args...)
		return
	}
	g.log.Warn(msg, args...)
}

// dropInBurst decide si un entrante cae por el corte A y, si cae, lo contabiliza. now y msgAt se pasan
// explícitos para que el test controle el reloj sin esperas reales.
//
// Devuelve además el resumen de un corchete cerrado POR VENCIMIENTO durante esta llamada: es el watchdog
// PEREZOSO, redundante con el timer y deliberado. Si el timer se retrasa (máquina suspendida, scheduler
// saturado — justo el escenario de una ráfaga), esta comprobación garantiza que un corchete vencido no
// descarte ni un mensaje más. Es la propiedad de seguridad que más importa: la bandera no puede pegarse.
func (g *offlineGate) dropInBurst(msgAt, now time.Time) (bool, *bracketSummary) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.armed {
		return false, nil
	}
	if g.armedEpoch != g.epoch {
		// Corchete de una conexión anterior: no descarta nada en esta. Se cierra por higiene.
		return false, g.closeLocked(closeReconnect)
	}
	if now.Sub(g.armedAt) > g.watchdog {
		return false, g.closeLocked(closeWatchdog)
	}
	age := now.Sub(msgAt)
	if age < 0 {
		age = 0 // desfase de reloj: un mensaje "del futuro" cuenta como edad cero, no como negativa.
	}
	if g.burstDropped == 0 || age < g.burstNewestAge {
		g.burstNewestAge = age
	}
	if age > g.burstOldestAge {
		g.burstOldestAge = age
	}
	g.burstDropped++
	g.stats.DroppedByGate++
	return true, nil
}

// countAgeDrop contabiliza un descarte del cinturón B (antigüedad).
func (g *offlineGate) countAgeDrop() {
	g.mu.Lock()
	g.stats.DroppedByAge++
	g.mu.Unlock()
}

// countSelfDrop contabiliza un eco propio descartado (IsFromMe).
func (g *offlineGate) countSelfDrop() {
	g.mu.Lock()
	g.stats.DroppedSelf++
	g.mu.Unlock()
}

// snapshot devuelve el acumulado de descartes de la sesión (copia, sin punteros vivos).
func (g *offlineGate) snapshot() OfflineStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stats
}
