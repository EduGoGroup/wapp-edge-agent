// Package logsink implementa el sumidero de logs en vivo del plano de control del Edge: un
// ring-buffer acotado en memoria + un broadcaster a los clientes conectados, y el handler HTTP
// GET /v1/logs que lo expone por SSE (text/event-stream).
//
// Diseño (decisión §10.C del Plan 007): el sink es un io.Writer. El logger del Edge se construye
// con io.MultiWriter(os.Stdout, sink) (ver internal/infra/logger.NewWithSink), de modo que CADA
// línea formateada por el handler slog real va, sin alterarse, tanto a stdout (comportamiento
// actual intacto) como al sink. Así el "tee" no duplica la lógica de formato/nivel de
// wapp-shared/logger (que no expone su slog.Handler): es el MISMO handler escribiendo a dos
// destinos. El sink trocea la entrada por '\n' (cada Handle de slog emite una línea completa
// terminada en '\n'), guarda las últimas N líneas y las difunde a los suscriptores en vivo.
//
// Zero-knowledge: el sink reusa las líneas que el slog ya emite; no añade verbosidad ni accede a
// material sensible (ADR-0007). Solo retransmite texto ya formateado.
package logsink

import (
	"bytes"
	"sync"
)

// DefaultCapacity es el tamaño por defecto del ring-buffer (líneas). ~2000 líneas: suficiente para
// que la UI muestre contexto reciente al conectar, sin crecer sin límite (decisión §10.C).
const DefaultCapacity = 2000

// subscriberBuffer es la capacidad del canal de cada suscriptor. Si un cliente lento lo llena, las
// líneas nuevas se DESCARTAN para ese suscriptor (no se bloquea al logger). 256 absorbe ráfagas
// normales; un cliente que no drena pierde líneas pero nunca frena la emisión de logs.
const subscriberBuffer = 256

// Sink es un ring-buffer concurrente y acotado de líneas de log ya formateadas, con un broadcaster
// a los suscriptores en vivo. Implementa io.Writer para teerse en la salida del logger vía
// io.MultiWriter(os.Stdout, sink). Es seguro para múltiples productores (escrituras del logger) y
// consumidores (clientes SSE) concurrentes.
type Sink struct {
	mu      sync.Mutex
	capByte int

	buf   []string // anillo de tamaño fijo capByte
	start int      // índice de la línea más antigua
	count int      // líneas vigentes (<= capByte)

	partial []byte // bytes recibidos aún sin '\n' de cierre (entre escrituras)

	subs   map[int]chan string
	nextID int
}

// New construye un Sink con la capacidad dada (líneas). Si capacity <= 0 usa DefaultCapacity.
func New(capacity int) *Sink {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	return &Sink{
		capByte: capacity,
		buf:     make([]string, capacity),
		subs:    make(map[int]chan string),
	}
}

// Write implementa io.Writer. Acumula los bytes recibidos y, por cada línea completa (delimitada por
// '\n'), la guarda en el ring-buffer y la difunde a los suscriptores. Nunca bloquea al llamante (el
// logger): los suscriptores con el canal lleno descartan la línea. Devuelve siempre len(p), nil
// (un sink en memoria no falla; devolver error rompería la cadena de escritura del logger/stdout).
func (s *Sink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.partial = append(s.partial, p...)
	for {
		i := bytes.IndexByte(s.partial, '\n')
		if i < 0 {
			break
		}
		line := string(s.partial[:i])
		s.partial = s.partial[i+1:]
		s.appendLocked(line)
		s.broadcastLocked(line)
	}
	if len(s.partial) == 0 {
		s.partial = nil // libera el array subyacente cuando no queda resto pendiente
	}
	return len(p), nil
}

// appendLocked inserta una línea en el ring-buffer, descartando la más antigua si está lleno. Debe
// llamarse con s.mu retenido.
func (s *Sink) appendLocked(line string) {
	if s.count < s.capByte {
		s.buf[(s.start+s.count)%s.capByte] = line
		s.count++
		return
	}
	s.buf[s.start] = line
	s.start = (s.start + 1) % s.capByte
}

// broadcastLocked envía la línea a cada suscriptor sin bloquear (descarta si el canal está lleno).
// Debe llamarse con s.mu retenido: así el envío y el cierre del canal (en cancel) quedan
// serializados y nunca se envía a un canal ya cerrado.
func (s *Sink) broadcastLocked(line string) {
	for _, ch := range s.subs {
		select {
		case ch <- line:
		default:
		}
	}
}

// snapshotLocked devuelve una copia de las líneas vigentes, de la más antigua a la más reciente.
// Debe llamarse con s.mu retenido.
func (s *Sink) snapshotLocked() []string {
	out := make([]string, s.count)
	for i := 0; i < s.count; i++ {
		out[i] = s.buf[(s.start+i)%s.capByte]
	}
	return out
}

// Subscribe registra un suscriptor en vivo. Devuelve, de forma ATÓMICA respecto al broadcaster, el
// snapshot del buffer reciente y un canal con las líneas POSTERIORES (sin solaparse ni perder
// líneas en el borde). El llamante DEBE invocar cancel al terminar (p.ej. defer) para liberar el
// suscriptor; tras cancel el canal queda cerrado. cancel es idempotente.
func (s *Sink) Subscribe() (snapshot []string, lines <-chan string, cancel func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := s.snapshotLocked()
	ch := make(chan string, subscriberBuffer)
	id := s.nextID
	s.nextID++
	s.subs[id] = ch

	var once sync.Once
	cancel = func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.subs, id)
			close(ch)
		})
	}
	return snap, ch, cancel
}

// subscriberCount devuelve el número de suscriptores activos. Uso interno/tests (verifica que la
// desconexión de un cliente SSE limpia su suscripción).
func (s *Sink) subscriberCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs)
}
