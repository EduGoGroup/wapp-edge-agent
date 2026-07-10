package cloudlink

// dispatcher.go — Despacho CONCURRENTE del demux CloudLink (Plan 027 Ola 1 · T1, cierra H1/H7).
//
// PROBLEMA (H1, head-of-line blocking): el loop de recepción de runOnce llamaba a handleCommand de
// forma SÍNCRONA. Una operación lenta de UNA sesión (p.ej. un SendMedia que descarga la presigned URL
// con timeout de 30s) congelaba la lectura del ÚNICO stream y, con ella, los leases/pings/envíos de
// TODAS las demás sesiones multiplexadas.
//
// SOLUCIÓN: un commandDispatcher que ejecuta los comandos CONCURRENTEMENTE ENTRE sesiones pero SERIAL
// dentro de cada sesión — preserva el orden por session_id (invariante del demux) sin que una sesión
// bloquee a las otras. Cada session_id estrena, PEREZOSAMENTE al llegar su primer comando, una cola
// (channel bufferizado) + una goroutine worker que la consume FIFO. El worker envuelve CADA comando en
// un context.WithTimeout (H7: deadline por operación, ya no el ctx de vida del stream).
//
// CICLO DE VIDA: el dispatcher vive lo que vive UN stream (una invocación de runOnce). Se crea al abrir
// el stream y se cierra (shutdown) al caer o al apagarse el agente — cancela su contexto (propagándolo a
// las operaciones en vuelo, que así retornan pronto en vez de esperar su deadline completo) y espera con
// WaitGroup a que todos los workers terminen: cierre limpio, sin goroutines fugadas.

import (
	"context"
	"sync"
	"time"

	"github.com/EduGoGroup/wapp-cloudlink/client"
	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
)

// commandDispatcher reparte los comandos cloud->edge de UN stream por session_id: concurrente entre
// sesiones, serial (orden preservado) dentro de cada sesión. Lo crea runOnce por vida de stream.
type commandDispatcher struct {
	a         *Adapter
	cl        *client.Client // stream vivo al que responder (acks/pongs/despacho)
	timeout   time.Duration  // deadline por operación (H7)
	queueSize int            // buffer por sesión (backpressure acotado)

	ctx    context.Context    // hijo del ctx de runOnce; se cancela en shutdown
	cancel context.CancelFunc //

	mu     sync.Mutex // protege queues
	queues map[string]chan *cloudlinkv1.CloudToEdge
	wg     sync.WaitGroup // cuenta los workers vivos
}

// newCommandDispatcher construye el dispatcher para un stream (cl), derivando su ctx del de runOnce
// (baseCtx) para heredar la cancelación del apagado del agente.
func newCommandDispatcher(baseCtx context.Context, a *Adapter, cl *client.Client, timeout time.Duration, queueSize int) *commandDispatcher {
	ctx, cancel := context.WithCancel(baseCtx)
	return &commandDispatcher{
		a:         a,
		cl:        cl,
		timeout:   timeout,
		queueSize: queueSize,
		ctx:       ctx,
		cancel:    cancel,
		queues:    make(map[string]chan *cloudlinkv1.CloudToEdge),
	}
}

// dispatch encola un comando en la cola de SU session_id (creándola perezosamente). Lo llama SOLO el
// loop de runOnce (un único productor). El envío es bloqueante contra la cola de esa sesión: mientras
// haya hueco (buffer), no bloquea el loop, así que una sesión con un worker ocupado no frena a las
// otras (H1). Si la cola de esa sesión se satura de forma sostenida, aplica backpressure acotado (en
// vez de crecer sin límite o descartar comandos en silencio); el select con d.ctx.Done() evita quedar
// colgado si el dispatcher ya se está cerrando.
func (d *commandDispatcher) dispatch(c2e *cloudlinkv1.CloudToEdge) {
	q := d.queueFor(c2e.GetSessionId())
	select {
	case q <- c2e:
	case <-d.ctx.Done():
	}
}

// queueFor devuelve la cola de una sesión, arrancando su worker la primera vez. Bajo lock para proteger
// el mapa (aunque el productor sea único).
func (d *commandDispatcher) queueFor(sessionID string) chan *cloudlinkv1.CloudToEdge {
	d.mu.Lock()
	defer d.mu.Unlock()
	q, ok := d.queues[sessionID]
	if !ok {
		q = make(chan *cloudlinkv1.CloudToEdge, d.queueSize)
		d.queues[sessionID] = q
		d.wg.Add(1)
		go d.worker(q)
	}
	return q
}

// worker consume la cola de UNA sesión en orden FIFO (preserva el orden por session_id) hasta que el
// dispatcher se cancela. En cada comando aplica el deadline por operación.
func (d *commandDispatcher) worker(q chan *cloudlinkv1.CloudToEdge) {
	defer d.wg.Done()
	for {
		select {
		case <-d.ctx.Done():
			return
		case c2e := <-q:
			d.handle(c2e)
		}
	}
}

// handle procesa UN comando con su deadline por operación (H7): deriva un ctx con timeout del ctx del
// dispatcher, de modo que tanto el deadline como el apagado del agente cancelan la operación. El
// recover() aísla un pánico del handler para que no tumbe al worker de la sesión.
func (d *commandDispatcher) handle(c2e *cloudlinkv1.CloudToEdge) {
	defer func() {
		if r := recover(); r != nil {
			d.a.log.Error("CloudLink: pánico procesando comando (aislado)",
				"session_id", c2e.GetSessionId(), "command_id", c2e.GetCommandId(), "panic", r)
		}
	}()
	opCtx, cancel := context.WithTimeout(d.ctx, d.timeout)
	defer cancel()
	d.a.handleCommand(opCtx, d.cl, c2e)
}

// shutdown cierra el dispatcher: cancela su ctx (cortando las operaciones en vuelo y sacando a los
// workers de su bucle) y espera a que TODOS los workers terminen. Sin goroutines fugadas. Idempotente
// (context.CancelFunc lo es).
func (d *commandDispatcher) shutdown() {
	d.cancel()
	d.wg.Wait()
}
