// Package cajero es el WORKER-CAJERO del Edge (Plan 051 Ola 2 · T2.2/T2.3/T2.4/T2.6/T2.8, ADR-0038,
// design §4): el proceso que reclama lotes de la cola de entrantes, los clasifica contra el LLM local
// y escribe el intent de vuelta.
//
// POR QUÉ ES UN PROCESO APARTE, y no una goroutine más del daemon: el listener («mesonero») dejó de
// clasificar en línea en la Ola 1 para soltar el handler de whatsmeow en milisegundos; si la inferencia
// volviera a vivir dentro de `agent serve`, un Ollama lento seguiría comiéndose el mismo proceso que
// mantiene los sockets vivos. El cajero es el TERCER hijo de `wapp-ctl` (T2.2) y REQ-051.10 quiere que
// sea el único que habla con Ollama.
//
// ⚠️ LIMITACIÓN CONOCIDA DE ESTA OLA (REQ-051.10 aún NO se cumple): mientras dure la ESCRITURA DOBLE
// de la Ola 1, `agent serve` sigue clasificando inline con su decorador (internal/adapters/intent), de
// modo que HAY DOS clientes de Ollama en la máquina: el decorador y este worker. Eso no es un
// descuido de este paquete, es el orden del plan — la Ola 3 retira el decorador y el `grep` de
// llamadas a Ollama fuera del worker pasa a dar cero. Hasta entonces el semáforo de aquí acota SOLO
// las inferencias del cajero.
//
// EL CICLO, en una línea: reclamar el lote de la conversación más vieja → concatenar sus textos EN
// ORDEN DE seq → UNA sola inferencia → escribir el sobre en la última fila. Cinco fragmentos de un
// mismo turno son una inferencia, no cinco (design §4).
//
// 🔴 INV-051.1 — NI EL TEXTO DEL MENSAJE NI NINGÚN PARÁMETRO EXTRAÍDO SALEN POR EL LOG. El texto viaja
// en claro en memoria (hay que dárselo al modelo) y muere ahí. Lo que se loguea es la FORMA del
// trabajo: nombre de intención, confianza, métricas del modelo, cuántos mensajes y cuántas runas. Si
// añades una clave al log de métricas, pregúntate primero si un operador con acceso al fichero de log
// podría reconstruir con ella lo que un cliente escribió.
package cajero

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/breaker"
	"github.com/EduGoGroup/wapp-edge-intent/classifier"
	"github.com/EduGoGroup/wapp-shared/intents"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// DefaultMaxConcurrent es el número de inferencias SIMULTÁNEAS que el cajero permite
// (WAPP_WORKER_MAX_CONCURRENT). Es 1, y el número está CERRADO por la medición de la O0 en el VPS AMD
// real (ADR-0038 Enmienda 1 §(d); el design §4 y el ADR-0038 §3 decían «2» y quedan corregidos). No se
// re-discute aquí: con una sola instancia de Ollama, dos inferencias a la vez se pisan los hilos y la
// latencia p50 se dispara. El semáforo existe igualmente porque el día que haya dos instancias
// aisladas por `taskset` (ver T2.8) el número sube por config, no por parche.
const DefaultMaxConcurrent = 1

// ─────────────────────────────────────────────────────────────────────────────
// Los CUATRO números del modelo — reexportados del clasificador, con dueño único
// ─────────────────────────────────────────────────────────────────────────────
//
// Viven AQUÍ, en el paquete del worker, y no en internal/infra/config, por el mismo criterio que ya
// aplican los dos defaults del lado cajero de internal/app/cola.go: el número tiene un dueño (el
// módulo del clasificador, que los exporta EXPRESAMENTE para este consumidor) y el sitio donde se
// reexportan debe ser el paquete que los usa por derecho propio — el worker, que ya importa
// `classifier` porque su interfaz Clasificador habla en tipos de ese módulo. `config` sólo les pone
// nombre de variable de entorno y guardarraíl, y por eso pasa a REFERENCIARLOS desde aquí en vez de
// importar el clasificador para reexportar cuatro enteros (y arrastrar ollama→net/http al grafo de
// todo el que importe config, incluido wapp-ctl).
//
// El porqué de cada valor está en el doc comment de la constante de origen y NO se repite aquí.
const (
	// DefaultMaxRunes es el techo de runas de la entrada a clasificar (T2.5).
	DefaultMaxRunes = classifier.DefaultMaxRunes
	// DefaultNumThread son los hilos de inferencia que se le piden a Ollama por clasificación.
	DefaultNumThread = classifier.DefaultNumThread
	// DefaultNumPredict es el tope de tokens que el modelo puede GENERAR en una clasificación.
	DefaultNumPredict = classifier.DefaultNumPredict
	// DefaultNumCtx es la ventana de contexto (tokens) que se le pide a Ollama.
	DefaultNumCtx = classifier.DefaultNumCtx
)

// DefaultInferenceTimeoutMS es el plazo por defecto de UNA inferencia del worker (ms), el que gobierna
// Deps.Timeout — y NO tiene nada que ver con WAPP_AGENT_INTENT_WAIT_MS, el presupuesto de espera del
// despachador. Ver el doc comment de Deps.Timeout para el argumento completo: son dos presupuestos
// distintos, de dos caminos distintos.
const DefaultInferenceTimeoutMS = 15000

// DefaultMaxIntentos es cuántas veces se RECLAMA un lote antes de abandonarlo con
// app.MotivoFalloRepetido (WAPP_WORKER_MAX_INTENTOS). Es 3, y el número dice esto: dos reintentos
// gratis y a la tercera se cierra.
//
// EL PORQUÉ DEL 3, que no es un número redondo elegido a ojo. Los fallos de inferencia que se ven en
// campo son de dos clases y hay que tratarlas distinto:
//
//   - TRANSITORIOS: Ollama reiniciándose, un pico de carga que se come el plazo, una lectura de modelo
//     que llegó tarde. Se arreglan solos y merecen el reintento gratis que el diseño ya les daba (el
//     lote se queda en `tomado` y el barrido lo devuelve a `nuevo`). Dos reintentos —hasta 2·lease, o
//     sea ~2 minutos con el default— cubren de sobra un reinicio de Ollama.
//   - PERMANENTES: un texto que hace fallar al modelo siempre. Ese lote conserva el `seq` más bajo de la
//     cola, así que el siguiente claim SE LO VUELVE A LLEVAR EL PRIMERO, y con MAX_CONCURRENT=1 la cola
//     entera deja de avanzar. Para siempre, y sin más síntoma que un contador de fallos subiendo.
//
// Un tercer fallo consecutivo del MISMO lote ya no se explica por el azar: es un patrón, y a partir de
// ahí cada reintento es una inferencia pagada que no va a funcionar mientras bloquea a todos los demás
// mensajes. Bajarlo a 1 quitaría el reintento gratis (un Ollama reiniciándose perdería clasificaciones
// que hoy se recuperan); subirlo mucho alarga el bloqueo de la cola, que es el fallo que esto viene a
// cerrar.
const DefaultMaxIntentos = 3

// cierreTimeout es el plazo del UPDATE que cierra un lote. Es corto a propósito y va sobre un contexto
// DESLIGADO del ctx del proceso: ver cerrar().
//
// 🔴 EL NÚMERO VIVE POR DEBAJO DEL StopTimeout DEL SUPERVISOR, A PROPÓSITO. Cuando `wapp-ctl` para al
// cajero manda SIGTERM y, si el proceso no ha muerto en `StopTimeout` (internal/adapters/supervisor ·
// `defaultStopTimeout`, 10 s), manda SIGKILL. Con este plazo igual al del supervisor, el peor caso es
// un empate: el SIGKILL llega justo cuando el UPDATE que salva una inferencia YA PAGADA está a mitad,
// que es exactamente lo que el `context.WithoutCancel` de cerrar() venía a evitar. 5 s deja un margen
// holgado por delante del SIGKILL y sobra de largo para un UPDATE contra un SQLite local (la escritura
// es de milisegundos; los 5 s sólo cubren el caso patológico de un fichero bloqueado).
//
// Si alguien sube este número, tiene que subir ANTES el StopTimeout del hijo cajero. Al revés no.
const cierreTimeout = 5 * time.Second

// Clasificador es la dependencia del cajero hacia el LLM local. Es la interfaz MÍNIMA (un método) y
// vive aquí, no en el módulo del clasificador, para que los tests puedan meter un doble sin levantar
// Ollama. La cumple *classifier.Classifier.
type Clasificador interface {
	Classify(ctx context.Context, texto string) (classifier.Classification, error)
}

// Interruptor es la dependencia del cajero hacia el circuit breaker. La cumple *breaker.Breaker; se
// declara como interfaz para que un test pueda forzar el estado del circuito sin simular cinco fallos.
type Interruptor interface {
	BeginAttempt() bool
	RecordSuccess()
	// RecordFailure registra un fallo y devuelve si ESTA llamada abrió el circuito (el flanco
	// no-abierto → abierto). El bool viene del breaker y no se calcula aquí a propósito: sólo dentro de
	// su lock la respuesta es correcta bajo concurrencia (ver breaker.Breaker.RecordFailure).
	RecordFailure() bool
	State() string
}

// Deps son las dependencias del cajero. Todo son INTERFACES o funciones: el paquete no conoce SQLite,
// ni HTTP, ni el layout de ficheros — el cableado real vive en cmd/agent.
type Deps struct {
	// Cola es el puerto del lado cajero (Reclamar/MarcarClasificado/BarrerLeasesVencidos). Obligatoria.
	Cola app.ColaCajero
	// Clasificador es el LLM local. Obligatorio.
	Clasificador Clasificador
	// Breaker es el circuito compartido. nil ⇒ se construye uno con la calibración por defecto.
	Breaker Interruptor
	// Despertador es cómo se espera cuando la cola está vacía. nil ⇒ PollFijo(DefaultPollMS).
	Despertador Despertador
	// Log es el logger. nil ⇒ sharedlogger.Default() (nil-safe, como en todo el repo).
	Log sharedlogger.Logger
	// Ahora es el reloj inyectable. nil ⇒ time.Now. Lo usan la medición de latencia del camino de
	// fallo y el breaker por defecto, para que un test no dependa de esperas reales.
	Ahora func() time.Time
	// MaxFilas es el tope de filas por claim (WAPP_AGENT_COLA_CLAIM_MAX_FILAS). <=0 ⇒ el default del
	// puerto (app.DefaultColaClaimMaxFilas).
	MaxFilas int
	// MaxConcurrent son las plazas del semáforo de inferencias. <=0 ⇒ DefaultMaxConcurrent (1).
	MaxConcurrent int
	// MaxIntentos es cuántos RECLAMOS aguanta un lote antes de que el cajero lo abandone con
	// app.MotivoFalloRepetido (WAPP_WORKER_MAX_INTENTOS). <=0 ⇒ DefaultMaxIntentos (3).
	//
	// Es el freno del LOTE VENENOSO: sin él, un lote cuya inferencia siempre falla vuelve a `nuevo` por
	// el barrido conservando su `seq`, se lo lleva otra vez el claim siguiente (que elige por seq mínimo)
	// y con MaxConcurrent=1 congela la cola entera. Ver app.MotivoFalloRepetido.
	MaxIntentos int
	// Lease es el margen del claim (WAPP_AGENT_COLA_LEASE_SECONDS) y, a la vez, el PERIODO del barrido:
	// barrer con la misma cadencia que el lease acota la espera de rescate a [lease, 2·lease]. <=0 ⇒ el
	// default del puerto (app.DefaultColaLeaseSegundos).
	Lease time.Duration
	// Timeout acota UNA inferencia del WORKER (WAPP_WORKER_INFERENCE_TIMEOUT_MS, default
	// DefaultInferenceTimeoutMS = 15 s). <=0 ⇒ sin plazo propio (manda el ctx del proceso).
	//
	// 🔴 NO ES `WAPP_AGENT_INTENT_WAIT_MS` (4 s), Y CONFUNDIRLOS ROMPE EL WORKER EN SILENCIO. Son dos
	// presupuestos de dos caminos distintos y el design §5 avisa expresamente de no colapsarlos:
	//
	//   - WAPP_AGENT_INTENT_WAIT_MS (4 s, T3.1) es cuánto espera el DESPACHADOR a que aparezca un intent
	//     antes de entregar el mensaje sin él. Es corto porque su objetivo es NO RETENER LA ENTREGA:
	//     pasado el plazo el mensaje sale sin intent y la sesión sigue viva. (Sustituyó a la retirada
	//     WAPP_AGENT_INTENT_TIMEOUT_MS, que era el plazo del camino inline; ese camino muere en T3.0.)
	//   - Éste es «cuánto aguanto a Ollama», y el worker EXISTE JUSTO PARA PODER TARDAR: se sacó la
	//     inferencia a otro proceso precisamente para que un Ollama lento no coma el proceso de los
	//     sockets. Heredar aquí el presupuesto del despachador lo calibraría PARA FALLAR: la O0 midió
	//     p50 = 2.613 ms y p95 = 3.736 ms (docs/plans/051-worker-cajero-edge/O0-resultados-2026-08-09.md),
	//     así que cualquier plazo pegado a la p95 (los 3 s del inline retirado, los 4 s del despachador)
	//     aborta una fracción grande de las inferencias → cada aborto es un fallo → 5
	//     seguidos abren el breaker 60 s → las filas se quedan en `tomado` → el barrido las devuelve a
	//     `nuevo` → se re-reclaman y vuelven a expirar. Un bucle que no progresa hasta que el tope de la
	//     cola las descarta, y sin un solo error que lo delate.
	//
	// El default de 15 s es ≈4× la p95, la misma holgura con la que se eligió el lease de 60 s (≈16×).
	Timeout time.Duration
	// StatsEvery es cada cuánto el bucle emite el bloque COMPLETO de contadores en un Info
	// (WAPP_WORKER_STATS_EVERY_MS). <=0 ⇒ desactivado (sólo se emite el bloque final de Run).
	//
	// Existe porque el cajero NO tiene plano de control: sin esto los seis contadores sólo son legibles
	// cuando el proceso muere, que es justo cuando ya no sirven para nada. La publicación al heartbeat
	// —la que los saca de la máquina— es de la OLA 4; esto es el sustituto barato hasta entonces.
	StatsEvery time.Duration
	// Listo reporta si hay contrato de intenciones cargado. nil ⇒ siempre listo. Es el equivalente del
	// `ready` del decorador inline: sin contrato, el clasificador no tiene prompt ni schema útiles y
	// clasificar sería quemar CPU para devolver «desconocido».
	Listo func() bool
	// ConfigVersion es la versión del contrato vigente, que viaja en el sobre del cajero. nil ⇒ "".
	ConfigVersion func() string
	// OllamaURL es la URL del Ollama al que se apunta. Sólo se usa para la comprobación de afinidad de
	// CPU del arranque (T2.8): el cajero NO habla con Ollama por su cuenta, eso es del Clasificador.
	OllamaURL string
	// NumThread son los hilos de inferencia que el Clasificador le pide a Ollama (WAPP_WORKER_NUM_THREAD).
	// El cajero NO lo usa para inferir —no habla con Ollama—, sólo para el AVISO de T2.8: pedir más hilos
	// que CPUs tiene el proceso confinado es sobresuscripción, y con el `taskset` puesto es fácil que
	// pase sin que nadie lo note (el número se calibró en la O0 contra una máquina SIN confinar).
	// <=0 ⇒ DefaultNumThread, que es lo que el clasificador manda si nadie toca la variable.
	NumThread int
}

// Cajero es el bucle. Construir con New; el cero-valor no sirve.
type Cajero struct {
	cola          app.ColaCajero
	clasificador  Clasificador
	breaker       Interruptor
	despertador   Despertador
	log           sharedlogger.Logger
	ahora         func() time.Time
	maxFilas      int
	maxIntentos   int64
	lease         time.Duration
	timeout       time.Duration
	statsEvery    time.Duration
	listo         func() bool
	configVersion func() string
	ollamaURL     string
	numThread     int

	// sem es el SEMÁFORO de inferencias (T2.3): un canal con buffer de N plazas. Se toma ANTES de
	// reclamar y se suelta al cerrar el lote, así que acota a la vez las inferencias en vuelo y los
	// lotes con lease vivo. Con N=1 el bucle es estrictamente serial y no puede haber dos inferencias
	// solapadas — que es lo que el criterio de T2.3 mide en `ollama ps`.
	sem chan struct{}
	// enVuelo cuenta los lotes que se están procesando, para que Run no devuelva dejando goroutines
	// sueltas escribiendo en la BD.
	enVuelo sync.WaitGroup

	// NO HAY MUTEX DE BREAKER, y es deliberado: el FLANCO cerrado→abierto lo detecta el propio breaker
	// bajo SU lock y lo devuelve en el bool de RecordFailure. Envolverlo aquí en la secuencia
	// State()/RecordFailure()/State() con un mutex local era una carrera de verdad —el mutex del cajero
	// no excluye a nadie más que al cajero, y BeginAttempt/RecordSuccess se llamaban sin él—, así que
	// con MAX_CONCURRENT>1 el contador de aperturas podía sumar de más o de menos. Si vuelves a
	// necesitar un lock aquí, es señal de que la decisión debería vivir dentro del breaker.

	clasificados     atomic.Int64
	omitidos         atomic.Int64
	abandonados      atomic.Int64
	relevados        atomic.Int64
	fallos           atomic.Int64
	rescatados       atomic.Int64
	aperturasBreaker atomic.Int64
}

// chatJIDHashLen es cuántos caracteres HEX del hash del chat_jid se escriben en el log. 8, y el número
// está elegido contra el scrubber del bundle de diagnóstico (internal/app/diagnostics/builder.go), que
// redacta cualquier racha de 32+ hex: un hash entero saldría `[REDACTED]` justo donde hace falta.
const chatJIDHashLen = 8

// chatJIDHash devuelve un identificador ESTABLE y no legible de una conversación, apto para el log.
//
// El `chat_jid` es `593999XXXXXX@s.whatsapp.net` — el TELÉFONO del cliente. Escribirlo en un log local
// (que además acaba en el bundle de diagnóstico que se comparte con soporte) es escribir PII en claro, y
// sobra: quien diagnostica necesita poder decir «estas dos líneas hablan de la MISMA conversación», no
// saber de quién es el número. Un PREFIJO no valdría —los primeros dígitos de un JID son país + operadora,
// justo lo que no hay que filtrar—; el hash no conserva ninguna estructura del original.
//
// ⚠️ NO es anonimización criptográfica: un teléfono tiene ~10^10 candidatos y esto no lleva sal ni llave,
// así que es invertible por fuerza bruta. Lo que compra es que el número deje de estar EN CLARO —a la
// vista, indexable por un grep, copiable de un bundle— sin perder diagnóstico.
//
// 🔴 ESTÁ DUPLICADO A PROPÓSITO de colaentrantes.chatJIDHash, y la razón es la hexagonal: aquel vive en un
// ADAPTADOR (internal/adapters/colaentrantes) y este paquete es del núcleo, así que importarlo invertiría
// la dirección de las dependencias para reutilizar cinco líneas. La forma de unificarlos, el día que haya
// un tercer llamante, es subir la función a internal/app y que el adaptador la referencie — nunca al revés.
// Si cambias uno, cambia el otro: el valor tiene que emparejar entre los dos logs para que sirva de algo.
func chatJIDHash(jid string) string {
	if jid == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(jid))
	return hex.EncodeToString(sum[:])[:chatJIDHashLen]
}

// New valida las dependencias y aplica los defaults. Devuelve error sólo si falta algo sin lo que el
// bucle no puede existir (la cola y el clasificador): todo lo demás tiene un default seguro, porque un
// worker que se niega a arrancar es un worker que no clasifica nada.
func New(deps Deps) (*Cajero, error) {
	if deps.Cola == nil {
		return nil, errors.New("cajero: falta la cola (app.ColaCajero)")
	}
	if deps.Clasificador == nil {
		return nil, errors.New("cajero: falta el clasificador")
	}

	log := deps.Log
	if log == nil {
		log = sharedlogger.Default()
	}
	ahora := deps.Ahora
	if ahora == nil {
		ahora = time.Now
	}
	br := deps.Breaker
	if br == nil {
		br = breaker.New(breaker.DefaultThreshold, breaker.DefaultOpenFor, breaker.WithClock(ahora))
	}
	desp := deps.Despertador
	if desp == nil {
		desp = NewPollFijo(DefaultPollMS * time.Millisecond)
	}
	plazas := deps.MaxConcurrent
	if plazas <= 0 {
		plazas = DefaultMaxConcurrent
	}
	maxFilas := deps.MaxFilas
	if maxFilas <= 0 {
		maxFilas = app.DefaultColaClaimMaxFilas
	}
	maxIntentos := deps.MaxIntentos
	if maxIntentos <= 0 {
		maxIntentos = DefaultMaxIntentos
	}
	lease := deps.Lease
	if lease <= 0 {
		lease = app.DefaultColaLeaseSegundos * time.Second
	}
	numThread := deps.NumThread
	if numThread <= 0 {
		numThread = DefaultNumThread
	}

	return &Cajero{
		cola:          deps.Cola,
		clasificador:  deps.Clasificador,
		breaker:       br,
		despertador:   desp,
		log:           log,
		ahora:         ahora,
		maxFilas:      maxFilas,
		maxIntentos:   int64(maxIntentos),
		lease:         lease,
		timeout:       deps.Timeout,
		statsEvery:    deps.StatsEvery,
		listo:         deps.Listo,
		configVersion: deps.ConfigVersion,
		ollamaURL:     deps.OllamaURL,
		numThread:     numThread,
		sem:           make(chan struct{}, plazas),
	}, nil
}

// Run arranca el cajero y bloquea hasta que ctx se cancele. Devuelve nil en la parada ordenada: que el
// proceso termine porque le mandaron SIGTERM no es un fallo, y devolver error ahí haría que el
// supervisor lo tratara como caída.
//
// PARADA LIMPIA, y qué significa exactamente aquí: al cancelar el ctx el bucle deja de reclamar, se
// espera a que los lotes EN VUELO terminen (cada uno tiene una plaza del semáforo) y el barrido de
// leases se apaga con su ticker. Un lote cuya inferencia estaba a mitad se pierde como inferencia
// —el ctx la corta— pero NO se pierde como mensaje: sus filas se quedan en `tomado` y el barrido de
// leases del siguiente cajero las devuelve a `nuevo` en ≤ lease. Esa es, literalmente, la propiedad
// que el criterio de campo de T2.7 va a comprobar matando el worker a mitad de inferencia.
func (c *Cajero) Run(ctx context.Context) error {
	c.registrarAfinidad(ctx)
	c.log.Info("cajero: arrancando",
		"max_concurrent", cap(c.sem),
		"max_filas_claim", c.maxFilas,
		"max_intentos", c.maxIntentos,
		"lease_s", int(c.lease.Seconds()),
		"inferencia_timeout_ms", c.timeout.Milliseconds(),
		"stats_cada_ms", c.statsEvery.Milliseconds(),
	)

	var barrido sync.WaitGroup
	barrido.Add(1)
	go func() {
		defer barrido.Done()
		c.barrerLeases(ctx)
	}()

	c.bucle(ctx)

	// El bucle ya no reclama; se espera a que los lotes en vuelo cierren su UPDATE (que va sobre un
	// contexto desligado, así que la cancelación no lo aborta a medias) y a que el barrido pare.
	c.enVuelo.Wait()
	barrido.Wait()

	c.log.Info("cajero: detenido limpiamente", c.contadores()...)
	return nil
}

// contadores devuelve el bloque COMPLETO de contadores como pares clave/valor para el logger. Existe
// para que el bloque periódico de bucle() y el bloque final de Run() no puedan divergir: un contador
// nuevo se añade UNA vez y aparece en los dos sitios.
//
// 🔴 INV-051.1: aquí no entra nada derivado del contenido de un mensaje. Todo son cuentas y estado.
func (c *Cajero) contadores() []any {
	return []any{
		"clasificados", c.Clasificados(),
		"omitidos", c.Omitidos(),
		"abandonados", c.Abandonados(),
		"relevados", c.Relevados(),
		"fallos", c.Fallos(),
		"rescatados", c.Rescatados(),
		"aperturas_breaker", c.AperturasBreaker(),
		"circuito", c.Circuito(),
	}
}

// bucle es el ciclo de trabajo. Sale cuando ctx se cancela.
//
// ─────────────────────────────────────────────────────────────────────────────
// QUIÉN ESCRIBE app.MotivoBreaker — el hueco del diseño, resuelto y documentado
// ─────────────────────────────────────────────────────────────────────────────
// T2.4 manda dos cosas que, juntas, casi no dejan hueco: (1) con el circuito ABIERTO el cajero DEJA DE
// RECLAMAR —no reclama y luego marca omitido: no reclama en absoluto—, y (2) el sobre
// `{"omitido":"breaker"}` lo escribe el cajero. Si nunca se reclama con el circuito abierto, no hay
// lote en la mano sobre el que escribir ese sobre. La contradicción es real y se resuelve así:
//
//   - Se consulta el estado ANTES de reclamar. StateOpen ⇒ no se reclama nada. Las filas pendientes se
//     quedan en `nuevo` y degradan por el PRESUPUESTO del despachador (Ola 3), que las entrega sin
//     intent en 4 s. Ese es el camino del 99 % de los mensajes afectados por un Ollama caído, y su
//     motivo es `presupuesto`, NO `breaker`.
//   - `MotivoBreaker` lo escribe el único caso en que el cajero SÍ tiene un lote en la mano y SÍ sabe
//     que no va a llamar a Ollama: cuando BeginAttempt() dice que no DESPUÉS de haber reclamado. Eso
//     ocurre (a) si el circuito se abrió entre la consulta de estado y el intento —posible en cuanto
//     WAPP_WORKER_MAX_CONCURRENT > 1, porque otra inferencia en vuelo puede abrirlo—, y (b) si el
//     medio-abierto ya tiene un sondeo reservado por otro lote. Devolver ese lote a `nuevo` no es una
//     opción: sólo lo dejaría esperando al barrido para acabar igual, y mintiendo en la telemetría.
//   - Un fallo de inferencia (error/timeout/pánico) NO cierra el lote MIENTRAS LE QUEDEN INTENTOS. Se
//     deja en `tomado` y el barrido lo devuelve a `nuevo` dentro del lease: es un reintento gratis, y
//     marcarlo omitido a la primera convertiría un fallo transitorio en una pérdida definitiva de la
//     clasificación. Agotados los intentos (T2.19) sí se cierra, con app.MotivoFalloRepetido, porque a
//     partir de ahí el reintento gratis deja de ser gratis: ese lote conserva el `seq` más bajo y bloquea
//     a toda la cola detrás de él. Ver el corte en procesar().
//
// 🔴 CONSECUENCIA HONESTA: con el default WAPP_WORKER_MAX_CONCURRENT=1 el contador de `breaker` será
// CASI SIEMPRE CERO, porque con un solo lote en vuelo el circuito no puede abrirse entre la consulta y
// el intento. Eso NO es un bug de este bucle: es que el motivo `breaker` del ADR-0038 §(e) describe un
// estado que, con el semáforo en 1, el cajero esquiva por construcción. Quien lea la telemetría de la
// Ola 4 debe saberlo, o concluirá que el breaker no se abre nunca mirando el contador equivocado — el
// que hay que mirar es `aperturas_breaker` (AperturasBreaker), que sí cuenta las aperturas reales.
//
// ─────────────────────────────────────────────────────────────────────────────
// QUIÉN ESCRIBE app.MotivoSinTexto — SON DOS PRODUCTORES, no uno
// ─────────────────────────────────────────────────────────────────────────────
// El enum de internal/app/cola.go se lo atribuye SÓLO al listener («Lo escribe el LISTENER, al nacer
// la fila»), y eso describe el camino normal (T1.8) pero no la totalidad. El cajero es el SEGUNDO
// productor, como DEFENSA EN PROFUNDIDAD: si una fila llega aquí con el texto vacío —un bug del
// listener, o un descifrado que devolvió cadena vacía— no se quema una inferencia para que el modelo
// devuelva «desconocido»; se cierra con el motivo que describe el hecho, y se AVISA en Warn, porque
// que ese camino se dispare significa que algo aguas arriba está roto.
//
// ⚠️ Deuda anotada: el doc comment de app.MotivoSinTexto (internal/app/cola.go) sigue diciendo que lo
// escribe sólo el listener y le falta esta segunda mitad. Ese fichero tiene otro dueño en esta tanda.
func (c *Cajero) bucle(ctx context.Context) {
	// Latido de contadores (T2.6 · el hueco del «se construye y no se cablea»): el cajero no tiene
	// plano de control, así que sin esto los seis contadores sólo se leen cuando el proceso muere, que
	// es justo cuando ya no sirven. El select es NO BLOQUEANTE y va al principio de la vuelta: el bucle
	// itera al menos una vez por poll, así que la cadencia real nunca se desvía más de un poll de la
	// pedida. statsEvery <= 0 ⇒ canal nil, que en un select nunca está listo (y así no hace falta un
	// segundo camino de código para el caso «desactivado»).
	var latido <-chan time.Time
	if c.statsEvery > 0 {
		t := time.NewTicker(c.statsEvery)
		defer t.Stop()
		latido = t.C
	}

	for {
		if ctx.Err() != nil {
			return
		}

		select {
		case <-latido:
			c.log.Info("cajero: contadores", c.contadores()...)
		default:
		}

		// (1) Circuito abierto ⇒ no se reclama en absoluto (T2.4).
		if c.breaker.State() == breaker.StateOpen {
			if c.despertador.Esperar(ctx) != nil {
				return
			}
			continue
		}

		// (2) Sin contrato de intenciones cargado tampoco se reclama: reclamar sería marcar el lote
		// `tomado` para acabar clasificándolo con un prompt vacío. Espejo del `ready` del decorador.
		if c.listo != nil && !c.listo() {
			if c.despertador.Esperar(ctx) != nil {
				return
			}
			continue
		}

		// (3) Plaza del semáforo ANTES del claim: así el número de lotes con lease vivo nunca supera
		// al de inferencias permitidas, y no se marcan `tomado` filas que van a esperar su turno.
		select {
		case c.sem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		lote, err := c.cola.Reclamar(ctx, c.maxFilas)
		if err != nil {
			<-c.sem
			if ctx.Err() != nil {
				return // el error es la cancelación, no un fallo de la BD
			}
			c.fallos.Add(1)
			c.log.Error("cajero: no se pudo reclamar un lote de la cola", "error", err)
			if c.despertador.Esperar(ctx) != nil {
				return
			}
			continue
		}
		if lote == nil || len(lote.Mensajes) == 0 {
			// Cola vacía: es el estado NORMAL de un worker al día, no un error.
			<-c.sem
			if c.despertador.Esperar(ctx) != nil {
				return
			}
			continue
		}

		c.enVuelo.Add(1)
		go func(l *app.ColaLote) {
			defer func() {
				<-c.sem
				c.enVuelo.Done()
			}()
			c.procesar(ctx, l)
		}(lote)
	}
}

// procesar clasifica UN lote y lo cierra. Se ejecuta con una plaza del semáforo tomada.
func (c *Cajero) procesar(ctx context.Context, lote *app.ColaLote) {
	texto := concatenar(lote)
	runas := utf8.RuneCountInString(texto)

	// Defensa en profundidad: el listener ya marca `sin_texto` al nacer la fila (T1.8), así que un lote
	// entero sin texto no debería llegar aquí. Si llega, no se quema una inferencia para que el modelo
	// devuelva «desconocido»: se cierra con el motivo que describe el hecho.
	//
	// Y SE AVISA. Cerrar esto en silencio dejaba invisible justo el hecho que importa: una defensa en
	// profundidad que se dispara es una defensa que ha tenido que trabajar, o sea, un bug aguas arriba
	// (el listener no marcó `sin_texto` en T1.8, o el descifrado devolvió cadena vacía). Warn y no
	// Error porque el mensaje NO se pierde: sale igual, sin intent.
	//
	// 🔴 INV-051.1: aquí NO va el texto (aunque esté vacío en teoría, «vacío» aquí significa
	// «sólo espacios», y esos espacios podrían no ser todo lo que hay si el TrimSpace mintiera) NI el
	// chat_jid completo. Sólo la FORMA del trabajo: sesión y cuántos mensajes.
	if strings.TrimSpace(texto) == "" {
		c.log.Warn("cajero: lote SIN TEXTO reclamado; se cierra como `sin_texto` sin llamar a Ollama "+
			"(el listener debió marcarlo al nacer la fila, T1.8: revisa aguas arriba)",
			"session_id", lote.SessionID, "mensajes", len(lote.Mensajes))
		if c.cerrar(ctx, lote, app.SobreOmitido(app.MotivoSinTexto)) {
			c.omitidos.Add(1)
		}
		return
	}

	if !c.breaker.BeginAttempt() {
		// Ver el bloque «QUIÉN ESCRIBE app.MotivoBreaker» en el doc comment de bucle(): éste es el
		// único punto que lo escribe, y es exactamente la semántica del enum («el breaker está
		// abierto; ni siquiera se llamó a Ollama»).
		if c.cerrar(ctx, lote, app.SobreOmitido(app.MotivoBreaker)) {
			c.omitidos.Add(1)
		}
		return
	}

	inicio := c.ahora()
	res, err := c.clasificar(ctx, texto)
	if err != nil {
		// 🔴 EL APAGADO SE COMPRUEBA ANTES DE REGISTRAR NADA, y el orden es el arreglo, no un detalle
		// de estilo. Al revés —registrar el fallo y luego decir en el log «no es un fallo del
		// clasificador»— el código contradecía a su propio mensaje: `registrarFallo` suma a `fallos` Y
		// llama a `breaker.RecordFailure()`, así que cada SIGTERM con lotes en vuelo acercaba el
		// circuito a su umbral por un error que no es de Ollama. Con MAX_CONCURRENT alto y un reinicio
		// desafortunado, el cajero podía arrancar y encontrarse el breaker envenenado por su propia
		// muerte anterior. El breaker no debe aprender NADA de un apagado.
		//
		// Nota: se comprueba el ctx del PROCESO, no el del plazo de inferencia. Un timeout propio
		// (c.timeout) SÍ es un fallo del clasificador y debe contar — y ahí ctx.Err() es nil, porque
		// quien venció fue el contexto hijo que crea clasificar().
		//
		// El precio, dicho en voz alta: salir sin RecordSuccess ni RecordFailure deja reservado el flag
		// de sondeo del medio-abierto (ver el aviso de breaker.BeginAttempt). Es el caso que ese aviso
		// declara aceptable, y sólo porque ctx cancelado significa que el proceso ENTERO se está yendo:
		// el breaker muere con él y nace limpio en el siguiente arranque.
		if ctx.Err() != nil {
			// Apagado a mitad de inferencia: no es un fallo del clasificador, y el lote lo rescata el
			// barrido de leases del siguiente cajero.
			c.log.Info("cajero: inferencia cortada por el apagado; el lote vuelve a `nuevo` por el barrido de leases",
				"session_id", lote.SessionID, "mensajes", len(lote.Mensajes))
			return
		}
		c.registrarFallo()

		// 🔴 EL FRENO DEL LOTE VENENOSO (T2.19). El reintento gratis de abajo —dejar el lote en `tomado`
		// para que el barrido lo devuelva a `nuevo`— es lo correcto para un fallo TRANSITORIO y era una
		// TRAMPA para uno permanente: el barrido no toca el `seq`, el claim elige la conversación de `seq`
		// mínimo, así que un lote que siempre falla se lo vuelve a llevar el claim siguiente, y otra vez, y
		// otra. Con MAX_CONCURRENT=1 eso es la cola ENTERA parada para siempre, y el único síntoma sería
		// este contador de fallos subiendo. Ver app.MotivoFalloRepetido.
		//
		// El corte va DESPUÉS de registrarFallo (el fallo ocurrió: cuenta en `fallos` y castiga al breaker
		// igual que cualquier otro) y ANTES del `return` que deja el lote en `tomado`.
		//
		// `>=` y no `==`: si alguien baja WAPP_WORKER_MAX_INTENTOS en caliente, los lotes que ya pasaron el
		// nuevo tope tienen que cerrarse en su siguiente vuelta, no quedarse dando vueltas por encima de un
		// umbral que ya nunca van a igualar exactamente.
		if lote.Intentos >= c.maxIntentos {
			// 🔴 INV-051.1: `chat_jid_hash` y NUNCA el chat_jid completo (es el teléfono del cliente; ver
			// colaentrantes.chatJIDHash), y del error del clasificador se dice solo que existe —va en
			// `error`, que es lo que ya se loguea en el camino normal—, jamás el texto que lo provocó.
			//
			// Warn y no Error: el mensaje NO se pierde, sale sin intent, que es el fallo seguro. Lo que se
			// abandona es la clasificación. Pero se dice con todas las letras y con el número de intentos,
			// porque un lote abandonado es una conversación que el operador querrá mirar.
			c.log.Warn("cajero: lote ABANDONADO tras agotar sus intentos de clasificación; se cierra como "+
				"`fallo_repetido` para que la cola pueda AVANZAR (el mensaje sale sin intent, no se pierde)",
				"error", err,
				"session_id", lote.SessionID,
				"chat_jid_hash", chatJIDHash(lote.ChatJID),
				"intentos", lote.Intentos,
				"max_intentos", c.maxIntentos,
				"mensajes", len(lote.Mensajes),
				"runas", runas,
				"circuito", c.breaker.State(),
			)
			if c.cerrar(ctx, lote, app.SobreOmitido(app.MotivoFalloRepetido)) {
				c.omitidos.Add(1)
				c.abandonados.Add(1)
			}
			return
		}

		c.log.Warn("cajero: la inferencia falló; el lote queda en `tomado` y el barrido lo devolverá a `nuevo`",
			"error", err,
			"session_id", lote.SessionID,
			"mensajes", len(lote.Mensajes),
			"runas", runas,
			"intentos", lote.Intentos,
			"max_intentos", c.maxIntentos,
			"latencia_ms", c.ahora().Sub(inicio).Milliseconds(),
			"circuito", c.breaker.State(),
		)
		return
	}
	c.breaker.RecordSuccess()

	// T2.6 · el log de métricas. 🔴 INV-051.1: aquí NO va el texto clasificado ni ningún valor de
	// res.Params. `intent` es el NOMBRE de la intención del contrato (un identificador cerrado que la
	// nube ya conoce), no contenido del cliente.
	c.log.Info("cajero: lote clasificado",
		"intent", res.Intent,
		"confidence", res.Confidence,
		"total_ms", res.Metrics.TotalMs,
		"prompt_tokens", res.Metrics.PromptTokens,
		"output_tokens", res.Metrics.OutputTokens,
		"tokens_per_sec", res.Metrics.TokensPerSec,
		"mensajes", len(lote.Mensajes),
		"runas", runas,
		"truncado", res.Truncado,
	)

	// «desconocido» (o intent vacío) NO es un fallo —el modelo respondió bien, sin intención
	// accionable— y por eso ya se registró como éxito en el breaker. Lo que cambia es el sobre.
	if res.Intent == "" || res.Intent == intents.ReservedUnknown {
		if c.cerrar(ctx, lote, app.SobreOmitido(app.MotivoDesconocido)) {
			c.omitidos.Add(1)
		}
		return
	}

	sobre, err := c.sobre(res)
	if err != nil {
		// Serializar un struct de cuatro campos no falla salvo por un valor imposible (NaN en la
		// confianza). Se degrada a «desconocido» en vez de dejar el lote colgado: el mensaje sale sin
		// intent, que es el fallo seguro.
		c.log.Error("cajero: no se pudo serializar el sobre; el lote se cierra como `desconocido`", "error", err)
		if c.cerrar(ctx, lote, app.SobreOmitido(app.MotivoDesconocido)) {
			c.omitidos.Add(1)
		}
		return
	}
	if c.cerrar(ctx, lote, sobre) {
		c.clasificados.Add(1)
	}
}

// clasificar ejecuta la inferencia bajo el plazo propio del worker y RECUPERA un pánico del
// clasificador convirtiéndolo en error. Es la misma protección que el decorador inline lleva desde el
// Plan 029 (runClassify): un pánico del clasificador nunca puede tumbar el proceso que vacía la cola.
func (c *Cajero) clasificar(ctx context.Context, texto string) (cls classifier.Classification, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pánico en el clasificador: %v", r)
		}
	}()
	if c.timeout > 0 {
		cctx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		return c.clasificador.Classify(cctx, texto)
	}
	return c.clasificador.Classify(ctx, texto)
}

// cerrar escribe el sobre en la última fila del lote y devuelve si el cierre tuvo efecto.
//
// EL CONTEXTO VA DESLIGADO (context.WithoutCancel) A PROPÓSITO: si el proceso se está apagando justo
// cuando la inferencia acaba de terminar, cerrar con el ctx cancelado tiraría a la basura una
// inferencia YA PAGADA y dejaría el lote en `tomado` esperando 60 s al barrido. El trabajo ya está
// hecho; escribirlo cuesta un UPDATE. El plazo propio (cierreTimeout) impide que ese desligue se
// convierta en un cierre que nunca termina.
func (c *Cajero) cerrar(ctx context.Context, lote *app.ColaLote, intentJSON string) bool {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cierreTimeout)
	defer cancel()

	err := c.cola.MarcarClasificado(cctx, lote, intentJSON)
	switch {
	case err == nil:
		return true
	case errors.Is(err, app.ErrLoteRelevado):
		// NO es un fallo ni una corrupción: es la carrera del lease funcionando como debe. Se cuenta
		// (INV-051.3 exige contar las degradaciones, no sólo loguearlas), se registra en Info —nunca
		// Error— y NO se reintenta: reintentar sería pisar el intent del cajero que sí llegó a tiempo.
		// Si este contador sube, el número que hay que mirar es el LEASE, no este error: significa que
		// las inferencias están tardando más que WAPP_AGENT_COLA_LEASE_SECONDS.
		c.relevados.Add(1)
		c.log.Info("cajero: el lote fue relevado (lease vencido); el cierre tardío se descarta",
			"session_id", lote.SessionID, "mensajes", len(lote.Mensajes), "relevados", c.relevados.Load())
		return false
	default:
		c.fallos.Add(1)
		c.log.Error("cajero: no se pudo cerrar el lote", "error", err, "session_id", lote.SessionID)
		return false
	}
}

// registrarFallo contabiliza un fallo de inferencia en el breaker y cuenta la APERTURA cuando ese
// fallo es el que abre el circuito (no «instantes en que el circuito estaba abierto», que es otra cosa).
//
// EL FLANCO LO DECIDE EL BREAKER, no este método: RecordFailure devuelve si ESA llamada abrió el
// circuito, calculado dentro de su propio lock. La versión anterior lo deducía aquí con
// State()/RecordFailure()/State() bajo un mutex local, y ese mutex no servía: sólo excluía a otras
// llamadas a registrarFallo, mientras BeginAttempt y RecordSuccess entraban al breaker sin él. Con
// WAPP_WORKER_MAX_CONCURRENT > 1 —el escenario que el propio bucle invoca— el contador podía sumar dos
// aperturas de la misma o perder una. Ahora no hay nada que serializar aquí.
func (c *Cajero) registrarFallo() {
	c.fallos.Add(1)

	if c.breaker.RecordFailure() {
		n := c.aperturasBreaker.Add(1)
		c.log.Warn("cajero: el circuit breaker del clasificador se ABRIÓ; el cajero deja de reclamar lotes",
			"aperturas", n, "abierto_por_s", int(breaker.DefaultOpenFor.Seconds()))
	}
}

// barrerLeases devuelve a `nuevo` las filas que un cajero muerto dejó bloqueadas en `tomado`. Corre en
// su propia goroutine con periodo = lease: barrer más a menudo no rescata antes (una fila sólo es
// rescatable cuando su lease ya venció) y barrer más despacio alarga la espera sin ganar nada.
func (c *Cajero) barrerLeases(ctx context.Context) {
	t := time.NewTicker(c.lease)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := c.cola.BarrerLeasesVencidos(ctx, c.lease)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.log.Error("cajero: el barrido de leases falló", "error", err)
				continue
			}
			if n > 0 {
				// Warn y no Info: rescatar filas significa que ALGUIEN murió a mitad (este worker en su
				// vida anterior, o el otro cajero de una máquina multi-empresa). No es normal.
				total := c.rescatados.Add(n)
				c.log.Warn("cajero: leases vencidos rescatados (vuelven a `nuevo`)",
					"filas", n, "lease_s", int(c.lease.Seconds()), "rescatados_total", total)
			}
		}
	}
}

// sobreCajero es la forma del `intent_json` que escribe el cajero cuando SÍ hay intención accionable.
// Es la OTRA forma del sobre frente a `{"omitido":…}` (ver app.SobreOmitido).
type sobreCajero struct {
	Intent        string            `json:"intent"`
	Params        map[string]string `json:"params,omitempty"`
	Confidence    float64           `json:"confidence"`
	ConfigVersion string            `json:"config_version,omitempty"`
}

// sobre serializa la clasificación al JSON que se persiste en la columna `intent_json`.
func (c *Cajero) sobre(res classifier.Classification) (string, error) {
	version := ""
	if c.configVersion != nil {
		version = c.configVersion()
	}
	b, err := json.Marshal(sobreCajero{
		Intent:        res.Intent,
		Params:        res.Params,
		Confidence:    res.Confidence,
		ConfigVersion: version,
	})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// registrarAfinidad es T2.8: comprueba en el arranque el REPARTO de CPUs entre el Ollama al que se
// apunta y este mismo proceso, y lo deja EN EL LOG LOCAL. La publicación al heartbeat es de la Ola 4 y
// aquí no se hace.
//
// MIRA A LOS DOS LADOS, y no es un extra: la medición contra el Ollama real del VPS demostró que
// confinar sólo al vecino deja el 17,2 % de las clasificaciones por encima del timeout, mientras que dos
// conjuntos DISJUNTOS lo dejan en 0 % — con las mismas 2 vCPU para el vecino en ambos casos. Un chequeo
// que sólo mirase a Ollama daría por buena esa configuración. La tabla completa y el porqué están en el
// encabezado de afinidad.go.
//
// 🔴 NUNCA ES FATAL. Olvidar el `taskset` cuesta el 100 % de las clasificaciones (p50 47.991 ms contra
// un timeout de 15 s) y en silencio, pero un worker que se niega a arrancar porque no pudo leer /proc es
// un worker que no clasifica NADA. Se avisa y se sigue, y eso vale también cuando sólo se pudo leer una
// de las dos afinidades.
func (c *Cajero) registrarAfinidad(ctx context.Context) {
	c.registrarReparto(leerAfinidades(ctx, c.ollamaURL))
}

// registrarReparto es la MITAD DECIDIBLE de registrarAfinidad: recibe la lectura ya hecha y elige qué
// se loguea y con qué nivel. Está separada de la lectura porque leerAfinidades sólo funciona en Linux y
// los gates se corren en macOS: así los tres veredictos —y el hecho de que dos de ellos sean Warn y uno
// Info— quedan cubiertos por tests en cualquier plataforma.
func (c *Cajero) registrarReparto(lec lecturaAfinidad) {
	// El aviso de hilos va PRIMERO y por su cuenta: depende sólo de la afinidad propia —la que siempre se
	// puede leer— así que sigue sirviendo en el caso más común de fallo, el de no poder ver a Ollama.
	c.avisarHilosSobresuscritos(lec)

	if lec.ErrOllama != nil || lec.ErrCajero != nil {
		c.log.Warn("cajero: no se pudo comprobar el reparto de CPUs entre Ollama y el cajero (T2.8); el cajero arranca IGUAL",
			"error_ollama", lec.ErrOllama, "error_cajero", lec.ErrCajero,
			"cpus_ollama", lec.Ollama, "cpus_cajero", lec.Cajero, "ollama_url", c.ollamaURL)
		return
	}

	reparto, err := clasificarReparto(lec.Ollama, lec.Cajero, lec.Presentes)
	if err != nil {
		c.log.Warn("cajero: la afinidad de CPU se leyó pero no se pudo interpretar (T2.8); el cajero arranca IGUAL",
			"error", err, "cpus_ollama", lec.Ollama, "cpus_cajero", lec.Cajero, "ollama_url", c.ollamaURL)
		return
	}

	switch reparto.Veredicto {
	case afinidadDisjunta:
		c.log.Info("cajero: reparto de CPUs DISJUNTO entre Ollama y el cajero (T2.8) — es la configuración medida como buena",
			"cpus_ollama", reparto.Ollama.String(), "cpus_cajero", reparto.Cajero.String(), "ollama_url", c.ollamaURL)
	case afinidadCajeroSinConfinar:
		c.log.Warn("cajero: el cajero NO está confinado a ninguna CPU (T2.8) — puede subirse a todas, incluidas las de Ollama; "+
			recetaSolapamiento,
			"cpus_ollama", reparto.Ollama.String(), "cpus_cajero", reparto.Cajero.String(),
			"cpus_compartidas", reparto.Comunes.String(), "ollama_url", c.ollamaURL)
	default: // afinidadSolapada
		c.log.Warn("cajero: Ollama y el cajero SE PISAN en al menos una CPU (T2.8) — "+recetaSolapamiento,
			"cpus_ollama", reparto.Ollama.String(), "cpus_cajero", reparto.Cajero.String(),
			"cpus_compartidas", reparto.Comunes.String(), "ollama_url", c.ollamaURL)
	}
}

// avisarHilosSobresuscritos avisa cuando se le piden a Ollama más hilos de inferencia que CPUs tiene
// OLLAMA disponibles. No es el hallazgo de la medición, es su efecto secundario: el `num_thread` se
// calibró en la O0 contra una máquina SIN confinar, así que en cuanto alguien reparte las 6 vCPU el
// número puede quedar sobresuscrito y nadie se entera — no falla nada, sólo se pelean los hilos.
//
// 🔴 SE COMPARA CONTRA LAS CPUs DE OLLAMA, NO CONTRA LAS DEL CAJERO, y la distinción no es sutil:
// `num_thread` es una opción que este proceso PASA en la petición HTTP, pero los hilos los crea y los
// ejecuta el proceso de Ollama. El cajero, mientras tanto, está bloqueado esperando la respuesta y no
// usa CPU. Compararlo contra la afinidad propia daba las dos respuestas equivocadas a la vez: en el
// reparto real de campo —Ollama en 0-4 con sus 5 hilos, el Edge confinado a la vCPU 5— habría emitido
// un Warn PERMANENTE Y FALSO en cada arranque, y en cambio se habría callado en el caso que de verdad
// importa (Ollama estrangulado a menos CPUs que hilos), que es el que asfixia la inferencia.
//
// No se toca el número, sólo se dice: DefaultNumThread vive en el módulo del clasificador (otro repo) y
// está cerrado por la O0. Y si la afinidad de Ollama no se pudo leer, esto no hace nada: sin saber a
// cuántas CPUs está confinado, no hay comparación que hacer.
func (c *Cajero) avisarHilosSobresuscritos(lec lecturaAfinidad) {
	if lec.ErrOllama != nil {
		return
	}
	cpus, err := parsearListaCPUs(lec.Ollama)
	if err != nil || len(cpus) == 0 {
		return
	}
	if c.numThread <= len(cpus) {
		return
	}
	c.log.Warn("cajero: se le piden a Ollama MÁS hilos de inferencia que CPUs tiene OLLAMA (T2.8); "+
		"baja WAPP_WORKER_NUM_THREAD o amplía el `taskset` de Ollama",
		"num_thread", c.numThread, "cpus_ollama", cpus.String(), "cpus_ollama_total", len(cpus))
}

// concatenar une los textos del lote EN EL ORDEN EN QUE VIENEN, que el adaptador garantiza que es el
// orden ascendente de `seq` (y lo sostiene a mano, porque `UPDATE … RETURNING` no respeta el ORDER BY).
// El orden aquí es SEMÁNTICO: es el orden en que la persona escribió los fragmentos de su turno, y
// alterarlo cambia lo que el modelo entiende sin que nada falle.
//
// El separador es un salto de línea: un fragmento por línea reproduce cómo se escribieron en el chat.
func concatenar(lote *app.ColaLote) string {
	if lote == nil || len(lote.Mensajes) == 0 {
		return ""
	}
	if len(lote.Mensajes) == 1 {
		return lote.Mensajes[0].Texto
	}
	var b strings.Builder
	for i := range lote.Mensajes {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(lote.Mensajes[i].Texto)
	}
	return b.String()
}

// ─────────────────────────────────────────────────────────────────────────────
// Contadores locales (T2.6 · INV-051.3) — legibles SIN salir de la máquina
// ─────────────────────────────────────────────────────────────────────────────
//
// Son atómicos y monotónicos. Van SEPARADOS y nunca agregados: «se omitió» y «se omitió porque el
// breaker está abierto» son dos hechos operativos distintos, y sumarlos borra justo la información que
// sirve para diagnosticar.
//
// QUIÉN LOS LEE HOY, sin adornos: el LOG LOCAL, y nadie más. El cajero NO tiene plano de control (es
// deliberado, ver el doc comment de runCajero en cmd/agent) y por tanto no hay endpoint que
// consultarlos. Se emiten en dos sitios: el bloque final de Run() y el latido periódico de bucle()
// (WAPP_WORKER_STATS_EVERY_MS, 5 min por defecto, 0 = desactivado). Los métodos públicos de abajo
// existen para los TESTS y para el consumidor que viene: la publicación al HEARTBEAT —lo que saca
// estos números de la máquina y los pone delante de un operador— es de la OLA 4, y hasta entonces
// afirmar que «los lee el plano de control» sería describir una tubería que no existe.

// Clasificados son los lotes cerrados con una intención accionable.
func (c *Cajero) Clasificados() int64 { return c.clasificados.Load() }

// Omitidos son los lotes cerrados con un sobre de omisión (`desconocido`, `breaker`, `sin_texto`,
// `fallo_repetido`).
func (c *Cajero) Omitidos() int64 { return c.omitidos.Load() }

// Abandonados son los lotes que agotaron sus intentos y se cerraron con `fallo_repetido` (T2.19).
//
// ⚠️ ES UN SUBCONJUNTO DE Omitidos, NO UN CONTADOR PARALELO: un lote abandonado suma en los dos, porque
// también es un cierre con sobre de omisión. NO los sumes entre sí. Va aparte porque es el ÚNICO de los
// motivos que señala una conversación concreta atascada —el resto describen decisiones normales del
// cajero—, y es el número que hay que mirar cuando la cola parece ir lenta sin que nada falle: si sube,
// hay un texto que el modelo no puede clasificar y que estuvo bloqueando el turno de los demás.
func (c *Cajero) Abandonados() int64 { return c.abandonados.Load() }

// Relevados son los cierres que llegaron tarde porque otro cajero relevó el lote (app.ErrLoteRelevado).
// Trabajo de CPU perdido, nunca mensajes perdidos.
func (c *Cajero) Relevados() int64 { return c.relevados.Load() }

// Fallos son los errores reales: claim fallido, inferencia fallida y cierre fallido.
func (c *Cajero) Fallos() int64 { return c.fallos.Load() }

// Rescatados son las filas que el barrido devolvió de `tomado` a `nuevo`.
func (c *Cajero) Rescatados() int64 { return c.rescatados.Load() }

// AperturasBreaker son las veces que el circuito pasó de no-abierto a ABIERTO. Es el contador que hay
// que mirar para saber si Ollama se está cayendo — no el de omisiones por `breaker`, que con el
// semáforo en 1 es casi siempre cero (ver el doc comment de bucle()).
func (c *Cajero) AperturasBreaker() int64 { return c.aperturasBreaker.Load() }

// Circuito devuelve el estado del circuito, con las mismas etiquetas que `GET /v1/intent/status`.
func (c *Cajero) Circuito() string { return c.breaker.State() }
