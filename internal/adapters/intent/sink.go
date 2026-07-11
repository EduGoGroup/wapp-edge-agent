// Package intent decora el sink de entrada del Edge con la CLASIFICACIÓN LLM local de intenciones (Plan
// 029, ADR-0020). Envuelve el app.InboundSink real: antes de reenviar un entrante a la nube, si el mensaje
// es texto libre elegible, pide al clasificador local (Ollama) una intención accionable {name, params,
// confidence} y la ANOTA en el evento. Todo lo demás (feature off, no elegible, carril rápido, timeout,
// error, circuito abierto, o "desconocido") entrega el mensaje SIN intención.
//
// INVARIANTES (ADR-0020 §Decisión.6):
//   - El decorador JAMÁS devuelve error por culpa del clasificador y JAMÁS bloquea/pierde el mensaje: la
//     clasificación es un ENRIQUECIMIENTO best-effort; el reenvío a la nube manda.
//   - La latencia de clasificación ocurre ANTES del outbox — aceptable (WhatsApp tolera segundos) y solo la
//     paga el texto libre; el carril rápido (números/sí-no/vacío) es 0 ms.
//   - Circuit breaker: si Ollama falla repetido, se deja de intentar (degradación a estático) sin castigar
//     cada mensaje con el timeout completo.
//
// El Decorator es COMPARTIDO por todas las sesiones del Edge (un solo Ollama, un solo circuito): Wrap
// produce un sink por sesión que comparte el clasificador y el estado del circuito. Seguro para uso
// concurrente (varios listeners entregan en paralelo).
package intent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-edge-intent/classifier"
	"github.com/EduGoGroup/wapp-shared/intents"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Parámetros del circuit breaker (ADR-0020): cbThreshold fallos CONSECUTIVOS abren el circuito por
// cbOpenFor; pasado ese tiempo entra en medio-abierto y deja pasar UN sondeo (éxito ⇒ cierra, fallo ⇒
// reabre). Un resultado "desconocido"/confianza baja NO es fallo (el clasificador respondió bien).
const (
	cbThreshold = 5
	cbOpenFor   = 60 * time.Second
)

// classifierPort es la dependencia del decorador hacia el clasificador. La cumple *classifier.Classifier;
// se declara como interfaz para inyectar un fake en los tests unitarios (sin Ollama).
type classifierPort interface {
	Classify(ctx context.Context, text string) (classifier.Classification, error)
	Reload(cfg *intents.Config)
}

// Decorator concentra el clasificador, la config en caliente y el circuit breaker COMPARTIDOS por todas las
// sesiones. Wrap produce el sink por sesión.
type Decorator struct {
	classifier classifierPort
	timeout    time.Duration
	log        sharedlogger.Logger
	now        func() time.Time

	mu sync.Mutex
	// ready indica que hay config de intenciones cargada (por push o por Bootstrap): sin ella el decorador
	// no clasifica (entrega tal cual) — el clasificador arranca sin prompt/schema útiles.
	ready     bool
	configVer string
	failures  int       // fallos consecutivos del clasificador (Ollama caído/timeout/pánico)
	openUntil time.Time // instante hasta el que el circuito está abierto
	probing   bool      // hay un sondeo de medio-abierto en curso (deja pasar solo uno)
}

// New construye el decorador sobre un clasificador ya creado. Arranca SIN config (ready=false) hasta que
// SetConfig la reciba (push del Cloud o Bootstrap de la config persistida). timeout<=0 cae a 3 s.
func New(cls classifierPort, timeout time.Duration, log sharedlogger.Logger) *Decorator {
	if log == nil {
		log = sharedlogger.Default()
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &Decorator{classifier: cls, timeout: timeout, log: log, now: time.Now}
}

// SetConfig recarga el clasificador en caliente con una config nueva y marca el decorador listo (ready).
// Es el suscriptor que el cableado registra en edgeconfig.Service para el kind 'intents': lo invoca tanto un
// push del Cloud como el Bootstrap de la config persistida. Regenera prompt/schema sin cortar
// clasificaciones en vuelo (el clasificador es concurrency-safe).
func (d *Decorator) SetConfig(cfg *intents.Config, version string) {
	d.classifier.Reload(cfg)
	d.mu.Lock()
	d.ready = true
	d.configVer = version
	d.mu.Unlock()
	d.log.Info("intent: config de intenciones cargada; clasificación ACTIVA", "config_version", version)
}

// Wrap envuelve un sink real con la clasificación de intenciones. El sink devuelto comparte el clasificador
// y el circuito del Decorator; su `next` es propio (una sesión = un next). Wrap es la costura de cableado
// (BuildSink y el camino multi-sesión de sessionmgr).
func (d *Decorator) Wrap(next app.InboundSink) app.InboundSink {
	return &sink{d: d, next: next}
}

// ConfigVersion devuelve la versión de config activa (o "" si aún no hay). Para GET /v1/intent/status.
func (d *Decorator) ConfigVersion() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.configVer
}

// Circuit devuelve el estado del circuito ("closed"/"open"/"half-open"). Para GET /v1/intent/status.
func (d *Decorator) Circuit() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failures < cbThreshold {
		return "closed"
	}
	if d.now().Before(d.openUntil) {
		return "open"
	}
	return "half-open"
}

// sink es el app.InboundSink por sesión: clasifica (si procede) y delega en next. Comparte d con las demás
// sesiones.
type sink struct {
	d    *Decorator
	next app.InboundSink
}

var _ app.InboundSink = (*sink)(nil)

// Deliver clasifica el entrante si es elegible y no lo atrapa el carril rápido, anota la intención en el
// evento y delega SIEMPRE en next. Nunca falla ni bloquea por culpa del clasificador.
func (s *sink) Deliver(ctx context.Context, evt domain.InboundEvent) error {
	d := s.d
	if !d.eligible(evt) || classifier.FastLane(evt.Text) {
		return s.next.Deliver(ctx, evt)
	}
	if ci := d.classify(ctx, evt.Text); ci != nil {
		evt.Intent = ci
	}
	return s.next.Deliver(ctx, evt)
}

// eligible reporta si el evento es candidato a clasificación: texto no vacío, no propio, no de grupo, y con
// config cargada. El carril rápido (números/sí-no) se evalúa aparte (0 ms, sin tocar el circuito).
func (d *Decorator) eligible(evt domain.InboundEvent) bool {
	if evt.Text == "" || evt.IsFromMe || evt.IsGroup {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ready
}

// classify pide una intención al clasificador respetando el circuito y el timeout. Devuelve la intención
// accionable o nil (cualquier no-éxito: circuito abierto, timeout, error, pánico, "desconocido" o confianza
// baja). Solo error/timeout/pánico castigan el circuito; un "desconocido" es un éxito sin intención.
func (d *Decorator) classify(ctx context.Context, text string) *domain.ClassifiedIntent {
	if !d.beginAttempt() {
		return nil // circuito abierto o sondeo de medio-abierto ya en curso
	}
	res, err := d.runClassify(ctx, text)
	if err != nil {
		d.recordFailure()
		d.log.Warn("intent: clasificación falló; se entrega el mensaje SIN intención", "error", err)
		return nil
	}
	d.recordSuccess()
	if res.Intent == "" || res.Intent == intents.ReservedUnknown {
		return nil // el clasificador respondió, pero sin intención accionable
	}
	return &domain.ClassifiedIntent{
		Name:          res.Intent,
		Params:        res.Params,
		Confidence:    res.Confidence,
		ConfigVersion: d.ConfigVersion(),
	}
}

// runClassify ejecuta el Classify bajo timeout propio (design §5, default 3 s) y RECUPERA un pánico del
// clasificador convirtiéndolo en error (aislamiento: un pánico nunca tumba el listener).
func (d *Decorator) runClassify(ctx context.Context, text string) (cls classifier.Classification, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pánico en el clasificador: %v", r)
		}
	}()
	cctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	return d.classifier.Classify(cctx, text)
}

// beginAttempt decide si se permite un intento de clasificación según el circuito: cerrado ⇒ sí; abierto y
// dentro de la ventana ⇒ no; ventana vencida ⇒ medio-abierto, deja pasar UN sondeo (marca probing para que
// las llamadas concurrentes no sondeen todas a la vez).
func (d *Decorator) beginAttempt() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failures < cbThreshold {
		return true // cerrado
	}
	if d.now().Before(d.openUntil) {
		return false // abierto
	}
	if d.probing {
		return false // medio-abierto, sondeo ya en curso
	}
	d.probing = true
	return true
}

// recordSuccess cierra el circuito (reinicia el contador de fallos y el sondeo).
func (d *Decorator) recordSuccess() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failures = 0
	d.probing = false
	d.openUntil = time.Time{}
}

// recordFailure suma un fallo consecutivo; al llegar al umbral abre el circuito por cbOpenFor. Un fallo del
// sondeo de medio-abierto (probing) reabre igual (failures ya >= umbral).
func (d *Decorator) recordFailure() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failures++
	d.probing = false
	if d.failures >= cbThreshold {
		d.openUntil = d.now().Add(cbOpenFor)
	}
}
