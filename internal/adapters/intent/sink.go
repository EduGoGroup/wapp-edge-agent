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
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/breaker"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-edge-intent/classifier"
	"github.com/EduGoGroup/wapp-shared/intents"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
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

	// cb es el CIRCUIT BREAKER, hoy en internal/app/breaker (Plan 051 Ola 2 · T2.4). Antes vivía aquí
	// dentro, en tres campos (failures/openUntil/probing) y tres métodos privados; se extrajo SIN tocar
	// un solo umbral porque el worker-cajero necesita exactamente la misma pieza y duplicarla habría
	// dejado dos calibraciones que divergen en silencio. El decorador conserva su `now` sustituible y se
	// lo PRESTA al breaker (WithClock) para que ambos lean el mismo reloj aunque el test lo cambie
	// después de construir.
	cb *breaker.Breaker

	mu sync.Mutex
	// ready indica que hay config de intenciones cargada (por push o por Bootstrap): sin ella el decorador
	// no clasifica (entrega tal cual) — el clasificador arranca sin prompt/schema útiles.
	ready     bool
	configVer string
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
	d := &Decorator{classifier: cls, timeout: timeout, log: log, now: time.Now}
	// El reloj se pasa como CLOSURE sobre el campo, no como valor: los tests sustituyen d.now DESPUÉS
	// de New, y si el breaker se quedara con la foto de time.Now el medio-abierto dejaría de poder
	// observarse sin esperar 60 s reales.
	d.cb = breaker.New(breaker.DefaultThreshold, breaker.DefaultOpenFor,
		breaker.WithClock(func() time.Time { return d.now() }))
	return d
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
// Las tres etiquetas son las MISMAS de siempre: viven ahora en breaker.State* y son contrato publicado
// (`intent_circuit`, ADR-0023), no detalle interno.
func (d *Decorator) Circuit() string { return d.cb.State() }

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
	if !d.cb.BeginAttempt() {
		return nil // circuito abierto o sondeo de medio-abierto ya en curso
	}
	res, err := d.runClassify(ctx, text)
	if err != nil {
		d.cb.RecordFailure()
		d.log.Warn("intent: clasificación falló; se entrega el mensaje SIN intención", "error", err)
		return nil
	}
	d.cb.RecordSuccess()
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
