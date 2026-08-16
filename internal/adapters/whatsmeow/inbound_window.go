package whatsmeow

// inbound_window.go — la VENTANA TEMPORAL de ingesta (ADR-0037) y el observador de corchetes.
//
// CRITERIO ÚNICO: se descarta todo *events.Message cuyo Info.Timestamp sea anterior a
// `inicioDeConexión − margen`. No hay bandera, ni época, ni watchdog: el criterio es per-evento y no
// depende de que ningún orden se respete.
//
// POR QUÉ NO CONTRA time.Now(): `now` mide cuándo lo procesamos NOSOTROS, y nuestro pipeline se atasca.
// En el incidente del 2026-08-06, dentro de una ventana muerta de 253,3 s hubo 179,1 s con algún nodo en
// vuelo —el 71 % del tiempo— con un respiro máximo de 2,3 s. Contra `now`, un margen de 5 min habría
// quedado al filo y el corte se habría comido tráfico vivo por nuestra propia lentitud. Contra el inicio
// de la conexión eso no puede pasar: un mensaje enviado después de que el socket subió sigue siendo
// posterior al umbral, lo procesemos cuando lo procesemos.
//
// DE DÓNDE SALE EL SELLO, y por qué NO de un evento de ciclo de vida: ninguno está serializado.
// *events.Connected se despacha dentro de un `go func()` que antes hace I/O de red
// (connectionevents.go:183-200) ⇒ puede llegar DESPUÉS de la ráfaga, y compararíamos contra el sello de
// la conexión anterior, dejándola pasar entera. *events.Disconnected sale con `go` en client.go:512 y
// :580 —y en :580 el dispatch y el autoReconnect son goroutines HERMANAS sin orden entre ellas—, y
// *events.LoggedOut en connectionevents.go:43 y :128. Un contador de época alimentado por esos eventos no
// arregla el problema: lo hereda. Y hay un cuarto agujero de lo mismo: whatsmeow NO emite
// *events.Disconnected en una desconexión ESPERADA (client.go:576-580, `if !cli.isExpectedDisconnect()`),
// así que hay reconexiones en las que una época simplemente no avanzaría.
//
// ⇒ El sello es `Client.LastSuccessfulConnect` (client.go:77), LEÍDO FRESCO en cada evaluación, nunca
// cacheado. Lo sella handleConnectSuccess de forma SÍNCRONA (connectionevents.go:160), antes del
// `go func()`. Leerlo fresco elimina el estado propio: no hay sello que envejezca ni bandera que se pegue.
//
// ⚠️ RESIDUAL DE CARRERA, dicho sin adornos. El campo NO es atómico. En el camino normal la lectura ESTÁ
// ordenada y no hay carrera: handlerQueueLoop corre cada nodeHandler en su goroutine y ESPERA su doneChan
// antes del siguiente (client.go:862-869), y "success" es un nodeHandler (client.go:293), así que
// escritura → close(doneChan) → recepción en el loop → `go` del handler siguiente → nuestra lectura es una
// cadena happens-before completa. La ventana solo se abre cuando el loop deja de esperar: F2
// (client.go:884, «Continuing handling… in background») y F3 (dos handlerQueueLoop sobre el mismo canal
// durante una reconexión; la cola se crea una vez en client.go:260 y el loop por conexión en client.go:562).
// NO se monta un mutex propio para esto: el escritor está en otro paquete y no lo tomaría, así que solo
// daría sensación de seguridad. Lo que sí se hace es NO FIARSE del valor leído: resolveThreshold descarta
// un sello imposible (ver ahí), porque una lectura rota de un time.Time (tres palabras: wall/ext/loc) NO
// tiene dirección de fallo garantizada y podría dar un umbral FUTURO, que es el modo caro.
//
// ZERO-KNOWLEDGE / ADR-0034: aquí se CUENTA, no se transcribe. Ni texto ni teléfono; solo cardinalidades,
// duraciones y edades.

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

// defaultConnectMargin es el margen por defecto de la ventana (ADR-0037): 300 s / 5 min.
//
// Lo que manda el número es el DESFASE DE RELOJ, y en una sola dirección: Info.Timestamp lo pone el reloj
// del SERVIDOR y el sello el reloj LOCAL, así que un reloj local ADELANTADO infla el umbral y descarta
// mensajes VIVOS — el fallo caro (al revés solo se cuela algo de ráfaga: barato). whatsmeow MIDE ese
// desfase (serverTimeOffset, connectionevents.go:165) pero NO lo expone: el campo es privado
// (client.go:193), así que no podemos corregirlo y el margen tiene que absorberlo. Eso lo pone en minutos.
//
// El margen es además la VENTANA DE RESCATE de la microcaída: todo lo enviado en los 5 min anteriores a
// reconectar se trata como vivo, así que un suspender/reanudar o una caída corta de wifi ya no pierde
// nada. Su coste está acotado: en una caída de 5 horas se ingieren, como mucho, los últimos 5 minutos.
const defaultConnectMargin = 5 * time.Minute

// InboundStats es el acumulado POR SESIÓN de la puerta de entrada (ADR-0037 §4). No lleva PII: son
// cardinalidades. Es el material que la telemetría de flota (ADR-0023) debe publicar.
type InboundStats struct {
	// Brackets es cuántos corchetes de ráfaga se han cerrado. Solo observabilidad: no deciden nada.
	Brackets uint64
	// DroppedByWindow son los entrantes descartados por caer fuera de la ventana temporal. Es EL contador
	// del criterio.
	DroppedByWindow uint64
	// DroppedSelf son los ECOS PROPIOS descartados (IsFromMe). No es del ADR-0037: es el filtro aprobado
	// aparte para dejar de subir a la nube lo que mandamos nosotros mismos.
	DroppedSelf uint64
	// AdmittedNoTimestamp son los entrantes ADMITIDOS por no traer hora utilizable (t="0"). Se cuentan
	// porque son el punto ciego del criterio: si crecen, la ventana está dejando pasar a ojos cerrados.
	AdmittedNoTimestamp uint64
	// ColaEnqueueErrors son los entrantes que NO pudieron anotarse en la cola durable porque Enqueue
	// devolvió error (Plan 051 · INV-051.3). El mensaje se entregó igual al sink (REQ-051.8), así que
	// nada se perdió HOY; lo que se pierde es la durabilidad, y por eso se cuenta aparte del panic:
	// distinguir "la cola dijo que no" de "la cola explotó" es la diferencia entre un disco lleno o una
	// DEK ausente y un bug del adaptador.
	ColaEnqueueErrors uint64
	// ColaEnqueuePanics son los pánicos RECUPERADOS dentro del encolado. Separado del anterior a
	// propósito: cualquier valor > 0 aquí es un defecto, no una condición de campo.
	ColaEnqueuePanics uint64
}

// resolveThreshold calcula el umbral de la ventana: `sello − margen`. Es una función PURA para poder
// probar el criterio sin reloj ni cliente.
//
// RESPALDO Y SANEAMIENTO del sello, en un solo sitio:
//   - sello CERO (arranque en frío, aún no hubo un `success`) ⇒ se usa `now`. Es más estricto QUE EL
//     SELLO —y solo eso—: como `now >= sello`, el umbral sube y lo descartado es un superconjunto. En
//     el instante de una conexión recién levantada el pipeline todavía no está atascado, que es cuando
//     `now` mentiría, así que el respaldo es razonable.
//     ⚠️ Pero NO garantiza que la ráfaga no escape, y esa frase estaría mal escrita aquí: los DOS
//     umbrales son reloj LOCAL y `Info.Timestamp` es reloj del SERVIDOR. Con el reloj local ATRASADO el
//     umbral se hunde y la ráfaga pasa, en proporción al desfase (un reloj 6 h atrasado deja pasar entera
//     una ráfaga de 5 h). Y el agravante es de manual: el respaldo se usa justo en ARRANQUE EN FRÍO —
//     máquina recién encendida o despierta de suspensión—, que es exactamente cuando el reloj puede ir
//     desfasado hasta que NTP re-sincronice, y cuando la ráfaga es mayor. El reloj local es el punto
//     ciego de este criterio; el margen lo absorbe hasta donde llega y no más.
//   - sello FUTURO ⇒ imposible: una conexión no puede haber empezado después de ahora. Es la firma de una
//     lectura rota (F2/F3) o de un salto de reloj, y es el modo CARO —un umbral futuro descartaría
//     tráfico vivo—, así que se trata igual que la ausencia: se cae a `now`. Esto NO sincroniza nada; es
//     no fiarse de un valor que no podemos leer con garantías.
//
// Un sello absurdamente VIEJO por lectura rota no se puede distinguir de uno legítimo, y su consecuencia
// es la barata (se cuela algo de ráfaga). Queda dicho, no tapado.
func resolveThreshold(seal time.Time, margin time.Duration, now time.Time) time.Time {
	if seal.IsZero() || seal.After(now) {
		seal = now
	}
	return seal.Add(-margin)
}

// bracketSummary es la foto de un corchete al cerrarse. Se construye BAJO el lock y se emite FUERA.
type bracketSummary struct {
	reason   string
	duration time.Duration
	// annTotal/annMessages/annReceipts es lo que ANUNCIÓ el servidor en el preview.
	annTotal, annMessages, annReceipts int
	// dropped es lo que la VENTANA descartó mientras el corchete estuvo abierto. La pareja
	// anunciado-vs-descartado es la RECONCILIACIÓN: la única calibración posible de un descarte que por
	// diseño es silencioso.
	dropped int
	// newestAge/oldestAge son las edades del entrante más reciente y del más viejo descartados. Es el dato
	// que distingue una ráfaga de segundos de una de horas. Solo válidas si dropped > 0.
	newestAge, oldestAge time.Duration
}

// Motivos de cierre de un corchete.
const (
	closeCompleted = "completed" // lo cerró *events.OfflineSyncCompleted (rutina).
	closeSuperOK   = "nuevo_preview"
	closeReconnect = "reconexion" // el socket cayó con el corchete abierto.
)

// bracketObserver observa los corchetes de la ráfaga de UNA sesión. Es SOLO OBSERVABILIDAD: cuenta y
// reconcilia, no decide ni un descarte. Por eso ya no necesita watchdog ni época — un corchete que se
// quedara abierto no puede hacer daño, a lo sumo pierde su línea de reconciliación (los contadores
// acumulados de la sesión siguen registrando todo).
type bracketObserver struct {
	log logger.Logger

	mu     sync.Mutex
	open   bool
	opened time.Time

	annTotal, annMessages, annReceipts int
	dropped                            int
	newestAge, oldestAge               time.Duration

	stats InboundStats
}

func newBracketObserver(log logger.Logger) *bracketObserver { return &bracketObserver{log: log} }

// arm abre un corchete al llegar el preview del servidor. Un preview con otro ya abierto cierra el
// anterior para no perder su cuenta.
func (b *bracketObserver) arm(e *events.OfflineSyncPreview) {
	b.mu.Lock()
	prev := b.closeLocked(closeSuperOK)
	b.open = true
	b.opened = time.Now()
	b.annTotal, b.annMessages, b.annReceipts = e.Total, e.Messages, e.Receipts
	b.dropped = 0
	b.newestAge, b.oldestAge = 0, 0
	b.mu.Unlock()

	b.emit(prev)
	// Solo cardinalidades del buzón: el preview no trae contenido.
	b.log.Info("listener: el servidor anuncia ráfaga offline; la ventana temporal decide qué entra (ADR-0037)",
		"anunciado_total", e.Total, "anunciado_mensajes", e.Messages,
		"anunciado_acuses", e.Receipts, "anunciado_notificaciones", e.Notifications)
}

// close cierra el corchete abierto por el motivo dado y emite su línea de reconciliación.
func (b *bracketObserver) close(reason string) {
	b.mu.Lock()
	s := b.closeLocked(reason)
	b.mu.Unlock()
	b.emit(s)
}

func (b *bracketObserver) closeLocked(reason string) *bracketSummary {
	if !b.open {
		return nil
	}
	b.open = false
	b.stats.Brackets++
	return &bracketSummary{
		reason: reason, duration: time.Since(b.opened),
		annTotal: b.annTotal, annMessages: b.annMessages, annReceipts: b.annReceipts,
		dropped: b.dropped, newestAge: b.newestAge, oldestAge: b.oldestAge,
	}
}

// emit escribe la línea de RECONCILIACIÓN del corchete: «el servidor anunció N, la ventana descartó M».
// INFO si lo cerró el servidor; WARN en cualquier otro motivo. El session_id no se pasa a mano: ya lo
// arrastra el logger del listener (sessionmgr/listen.go:192).
func (b *bracketObserver) emit(s *bracketSummary) {
	if s == nil {
		return
	}
	args := []any{
		"motivo_cierre", s.reason,
		"duracion", s.duration.Round(time.Millisecond).String(),
		"anunciado_total", s.annTotal,
		"anunciado_mensajes", s.annMessages,
		"anunciado_acuses", s.annReceipts,
		"descartados_por_ventana", s.dropped,
	}
	if s.dropped > 0 {
		args = append(args,
			"edad_mas_reciente", s.newestAge.Round(time.Second).String(),
			"edad_mas_vieja", s.oldestAge.Round(time.Second).String())
	}
	const msg = "listener: corchete de ráfaga offline cerrado — reconciliación anunciado/descartado (ADR-0037)"
	if s.reason == closeCompleted {
		b.log.Info(msg, args...)
		return
	}
	b.log.Warn(msg, args...)
}

// countWindowDrop contabiliza un descarte de la ventana y, si hay corchete abierto, lo imputa a su
// reconciliación con su edad. age ya viene saneada (>= 0) por el llamante.
func (b *bracketObserver) countWindowDrop(age time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stats.DroppedByWindow++
	if !b.open {
		return
	}
	if b.dropped == 0 || age < b.newestAge {
		b.newestAge = age
	}
	if age > b.oldestAge {
		b.oldestAge = age
	}
	b.dropped++
}

// countSelfDrop contabiliza un eco propio descartado (IsFromMe).
func (b *bracketObserver) countSelfDrop() {
	b.mu.Lock()
	b.stats.DroppedSelf++
	b.mu.Unlock()
}

// countNoTimestamp contabiliza un entrante ADMITIDO por no traer hora utilizable.
func (b *bracketObserver) countNoTimestamp() {
	b.mu.Lock()
	b.stats.AdmittedNoTimestamp++
	b.mu.Unlock()
}

// countColaEnqueueError contabiliza un entrante que la cola durable RECHAZÓ (Enqueue devolvió error).
func (b *bracketObserver) countColaEnqueueError() {
	b.mu.Lock()
	b.stats.ColaEnqueueErrors++
	b.mu.Unlock()
}

// countColaEnqueuePanic contabiliza un panic RECUPERADO dentro del encolado.
func (b *bracketObserver) countColaEnqueuePanic() {
	b.mu.Lock()
	b.stats.ColaEnqueuePanics++
	b.mu.Unlock()
}

// snapshot devuelve el acumulado de la sesión (copia, sin punteros vivos).
func (b *bracketObserver) snapshot() InboundStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stats
}
