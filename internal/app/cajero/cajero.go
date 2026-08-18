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

// ColaNombrada es UNA cola del cajero con la etiqueta con la que aparece en el log (Plan 051 Ola 4 ·
// T4.1). El nombre es, en el cableado real, el DATA_DIR de esa instalación.
//
// EL NOMBRE NO ES COSMÉTICO. Desde T4.1 el cajero atiende N instalaciones de la misma máquina y todos
// sus mensajes de log —el barrido que rescata filas, el claim que falla, el lote que se abandona— hablan
// de UNA de ellas. Sin la etiqueta, un operador con cinco instalaciones lee «el barrido de leases falló»
// y no tiene forma de saber cuál de los cinco ficheros SQLite está roto.
//
// 🔴 INV-051.1: el nombre viaja al log, así que es una RUTA DE DIRECTORIO y nunca nada derivado del
// contenido de un mensaje ni de un chat_jid.
type ColaNombrada struct {
	// Nombre es la etiqueta de la cola en el log (el data_dir). Vacío ⇒ se usa el índice.
	Nombre string
	// Cola es el puerto del lado cajero de ESA instalación. Obligatorio.
	Cola app.ColaCajero
	// Parte es el buzón por el que ESTA instalación recibe el parte de salud del cajero (Plan 051 Ola 4 ·
	// T4.5). nil ⇒ esta cola no recibe parte, y el daemon de esa instalación publicará `intent_circuit`
	// vacío. NO es obligatorio a propósito: el parte es TELEMETRÍA y su ausencia no puede impedir que se
	// clasifique (ver publicarParte).
	//
	// VIAJA PEGADO A LA COLA, y no en una lista paralela de Deps, porque el escritor y la cola son LA
	// MISMA BD (<data_dir>/cola_entrantes.db, tabla parte_worker): dos listas indexadas por posición se
	// desalinean el día que alguien reordene una, y el síntoma sería un parte escrito en el fichero de la
	// instalación vecina — donde su daemon lo leería como propio.
	Parte app.ParteWorkerEscritor
}

// Deps son las dependencias del cajero. Todo son INTERFACES o funciones: el paquete no conoce SQLite,
// ni HTTP, ni el layout de ficheros — el cableado real vive en cmd/agent.
type Deps struct {
	// Cola es el puerto del lado cajero (Reclamar/MarcarClasificado/BarrerLeasesVencidos) cuando hay UNA
	// SOLA cola. Es el atajo mono-cola y sigue siendo el caso normal (una instalación por máquina).
	//
	// Obligatoria SALVO que se pase Colas. Si se pasan las dos, MANDA Colas y ésta se ignora: elegir la
	// más rica evita la fusión silenciosa (una cola duplicada entre las dos vías sería exactamente el
	// round-robin que se reclama a sí mismo que el T4.1 prohíbe).
	Cola app.ColaCajero
	// Colas es la LISTA de colas que el cajero atiende en ROUND-ROBIN ESTRICTO (Plan 051 Ola 4 · T4.1),
	// una por instalación (un data_dir ⇒ un cola_entrantes.db). Tiene precedencia sobre Cola.
	//
	// 🔴 EL SEMÁFORO Y EL BREAKER SIGUEN SIENDO UNO POR PROCESO, no uno por cola, y eso NO es un descuido
	// de esta lista: los dos protegen a OLLAMA, que es uno por máquina. Un semáforo por cola con N=1 y
	// cinco instalaciones daría cinco inferencias simultáneas contra la misma instancia de Ollama —justo
	// el solapamiento que la O0 midió como la causa de que la latencia p50 se dispare—, y un breaker por
	// cola dejaría a cuatro colas martilleando un Ollama que la quinta ya sabe caído. Lo que rota es EL
	// CLAIM; el resto del bucle es de la máquina. Si alguna vez hace falta acotar por cola, el sitio es un
	// número nuevo, no partir estos dos.
	Colas []ColaNombrada
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

// colaMontada es una cola YA VALIDADA con su etiqueta y su contador propio de rescates. Es el elemento
// sobre el que gira el round-robin de T4.1.
//
// ES UN PUNTERO EN LA SLICE (`[]*colaMontada`) Y TIENE QUE SERLO: lleva dentro un atomic.Int64, y una
// slice de valores obligaría a indexar para sumar sin copiar jamás el elemento — una restricción que no
// se puede declarar y que el primer `for _, c := range colas` rompería en silencio (sumaría sobre la
// copia). Con punteros el rango es seguro por construcción.
type colaMontada struct {
	cola   app.ColaCajero
	nombre string
	// parte es el buzón del parte de salud de ESTA instalación (T4.5). Puede ser nil: el parte es
	// telemetría y una cola sin buzón sigue clasificando igual (ver ColaNombrada.Parte).
	parte app.ParteWorkerEscritor
	// rescatados son las filas que el barrido devolvió de `tomado` a `nuevo` EN ESTA cola. El agregado del
	// proceso sigue existiendo (Cajero.rescatados); éste es el desglose que dice CUÁL instalación es la
	// que tiene un cajero muriéndose a mitad.
	rescatados atomic.Int64
}

// Cajero es el bucle. Construir con New; el cero-valor no sirve.
type Cajero struct {
	// colas son las N colas del round-robin (Plan 051 Ola 4 · T4.1). Con una sola instalación tiene
	// exactamente un elemento y el bucle se comporta igual que antes de T4.1.
	colas []*colaMontada
	// cursor es la POSICIÓN del round-robin. Sólo lo toca bucle(), que es una única goroutine, así que no
	// necesita candado: las goroutines de procesar() no lo leen (cada una recibe SU cola por parámetro) y
	// el barrido las recorre todas sin cursor.
	cursor        int
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

	// veredictoTaskset es el resultado de la comprobación de afinidad de CPU del arranque (T2.8), RETENIDO
	// para que pueda viajar en el parte (T4.5). Hasta la Ola 4 se calculaba una vez, se logueaba y se
	// tiraba.
	//
	// ES UN atomic.Value Y NO UN string PELADO porque lo ESCRIBE Run (antes del bucle) y lo LEEN el
	// publicador del parte y los tests, potencialmente desde otra goroutine. Un string sin protección es
	// una carrera que `-race` caza. Cargarlo vacío —el cero del Value es nil, y el type assert falla
	// devolviendo ""— es exactamente la semántica que hace falta: vacío = «no se sabe» (ver Taskset).
	veredictoTaskset atomic.Value // string

	// inferencia es el histograma de latencia de la inferencia (T4.5), del que sale el p50 del parte. Va
	// por VALOR y no por puntero: es un array de atómicos, no necesita construcción, y el cero funciona.
	inferencia histogramaInferencia

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

// montarColas resuelve las DOS formas de pasar colas (Deps.Colas y el atajo Deps.Cola) en la lista única
// sobre la que gira el round-robin.
//
// FALLA ANTES DE ARRANCAR ante una cola nil DENTRO de la lista, y ese es el punto de esta función. El
// caso real que cubre: el cableado abre N data_dir's en un bucle y uno de ellos no pudo construir su
// Store. Sin esta comprobación el cajero arrancaría tan campante y la primera vez que el cursor cayera en
// esa posición —minutos después, en producción, con el arranque ya dado por bueno— el claim entraría en
// un puntero nil. Un pánico diferido es peor que un error de arranque, porque el error de arranque lo ve
// el operador que acaba de tocar la config.
func montarColas(deps Deps) ([]*colaMontada, error) {
	crudas := deps.Colas
	if len(crudas) == 0 {
		if deps.Cola == nil {
			return nil, errors.New("cajero: falta la cola (app.ColaCajero): pasa Deps.Cola o Deps.Colas")
		}
		crudas = []ColaNombrada{{Cola: deps.Cola}}
	}

	colas := make([]*colaMontada, 0, len(crudas))
	for i, cn := range crudas {
		if cn.Cola == nil {
			return nil, fmt.Errorf("cajero: la cola %d (%s) es nil: no se puede reclamar de ella",
				i, nombreDeCola(cn.Nombre, i))
		}
		colas = append(colas, &colaMontada{cola: cn.Cola, nombre: nombreDeCola(cn.Nombre, i), parte: cn.Parte})
	}
	return colas, nil
}

// nombreDeCola da a una cola sin etiqueta un nombre que al menos la distinga de sus hermanas. Un nombre
// vacío en el log sería peor que un número: dejaría dos líneas idénticas hablando de instalaciones
// distintas.
func nombreDeCola(nombre string, i int) string {
	if strings.TrimSpace(nombre) != "" {
		return nombre
	}
	return fmt.Sprintf("cola-%d", i)
}

// New valida las dependencias y aplica los defaults. Devuelve error sólo si falta algo sin lo que el
// bucle no puede existir (la cola y el clasificador): todo lo demás tiene un default seguro, porque un
// worker que se niega a arrancar es un worker que no clasifica nada.
func New(deps Deps) (*Cajero, error) {
	colas, err := montarColas(deps)
	if err != nil {
		return nil, err
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
		colas:         colas,
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
		// Las colas y el semáforo van JUNTOS en la misma línea a propósito: son los dos números que un
		// operador confunde. `colas` es cuántas instalaciones se turnan; `max_concurrent` sigue siendo
		// cuántas inferencias caben A LA VEZ EN LA MÁQUINA, no por cola (ver Deps.Colas).
		"colas", len(c.colas),
		"colas_nombres", strings.Join(c.nombresDeColas(), ","),
		"max_concurrent", cap(c.sem),
		"max_filas_claim", c.maxFilas,
		"max_intentos", c.maxIntentos,
		"lease_s", int(c.lease.Seconds()),
		"inferencia_timeout_ms", c.timeout.Milliseconds(),
		"stats_cada_ms", c.statsEvery.Milliseconds(),
	)

	// T4.5 · EL PRIMER PARTE VA AQUÍ, ANTES DEL BUCLE, y no se deja para el primer tick. Sin esto habría
	// un agujero de ParteCada (30 s) en cada arranque en el que el daemon leería «sin parte» con el cajero
	// perfectamente vivo — y en una máquina que se reinicia a menudo (la plataforma objetivo es un
	// portátil), ese hueco es una fracción nada despreciable del tiempo total.
	//
	// DESPUÉS de registrarAfinidad, para que el veredicto del taskset ya esté retenido y el primer parte
	// lo lleve. Al revés se publicaría un taskset vacío que sólo se corregiría 30 s más tarde.
	c.publicarParte(ctx)

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
		// Las tres señales del parte se emiten TAMBIÉN en el log local (T4.5). No es duplicar por
		// duplicar: el parte sólo se puede leer desde la nube, y el diagnóstico de campo —el bundle, el
		// `journalctl` de un operador— se hace sobre este fichero. Si algún día el tubo del parte se rompe,
		// estas tres claves son lo único que queda.
		"taskset", c.Taskset(),
		"p50_inferencia_ms", c.P50InferenciaMS(),
		"colas", len(c.colas),
	}
}

// nombresDeColas devuelve las etiquetas de las colas en el orden del round-robin. Sólo para el log.
func (c *Cajero) nombresDeColas() []string {
	nombres := make([]string, 0, len(c.colas))
	for _, cm := range c.colas {
		nombres = append(nombres, cm.nombre)
	}
	return nombres
}

// bucle es el ciclo de trabajo. Sale cuando ctx se cancela.
//
// ─────────────────────────────────────────────────────────────────────────────
// ROUND-ROBIN ESTRICTO ENTRE N COLAS (Plan 051 Ola 4 · T4.1)
// ─────────────────────────────────────────────────────────────────────────────
// Lo que rota es EL CLAIM y sólo el claim: el cursor avanza una posición por claim INTENTADO —tenga
// lote o no— y el bucle sólo duerme cuando ha encadenado TANTOS CLAIMS VACÍOS CONSECUTIVOS COMO COLAS
// HAY. Las dos mitades de esa frase son necesarias y por motivos opuestos:
//
//   - Si el cursor avanzara sólo cuando hay trabajo, la cola PARLANCHINA se llevaría todas las vueltas
//     y la callada no volvería a ser atendida hasta que aquélla se vaciara. Eso es inanición, y es lo
//     que el criterio de equidad de T4.1 mide (tras M rondas, ninguna cola espera más de N claims).
//   - Si se durmiera al primer vacío, el poll se pagaría N veces por vuelta y la última instalación de
//     la lista esperaría N·poll aun teniendo trabajo desde el principio.
//
// ⚠️ «N VACÍOS CONSECUTIVOS» NO ES LO MISMO QUE «LA VUELTA ENTERA EN BLANCO», y el comentario decía lo
// segundo mientras el código hacía lo primero. `vaciasSeguidas` es GLOBAL, no por vuelta: un claim con lote
// lo reinicia, así que la ventana de N vacíos puede quedar a caballo de dos vueltas. La conducta resultante
// es JUSTA igualmente —el cursor rota SIEMPRE, con lote o sin él, así que ninguna cola pasa hambre— y no
// hay espera activa: para no dormir hace falta un claim CON lote cada N intentos, es decir, trabajo real.
// Se deja como está a propósito: lo que estaba mal era la promesa escrita, no lo implementado.
//
// Con UNA sola cola —el caso normal, una instalación por máquina— el umbral es 1 y el bucle se comporta
// EXACTAMENTE igual que antes de T4.1: es la no-regresión que el plan exige.
//
// 🔴 EL SEMÁFORO Y EL BREAKER NO ROTAN: son UNO POR PROCESO porque protegen a Ollama, que es uno por
// máquina. El argumento completo está en Deps.Colas, y no se re-abre aquí.
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

	// Latido del PARTE (T4.5 · el tubo cajero→daemon). Va en el MISMO sitio que el de contadores —al
	// principio de la vuelta, en un select no bloqueante— pero con SU PROPIO ticker, y esa separación es
	// deliberada: `statsEvery` es un mando de LOG que el operador puede poner a 0 para callar la
	// telemetría local, y colgar de él la frescura del parte significaría que bajar la verbosidad del log
	// apaga `intent_circuit` en la nube. El argumento completo, con el cálculo del 3×, en app.ParteCada.
	tParte := time.NewTicker(app.ParteCada)
	defer tParte.Stop()

	// vaciasSeguidas cuenta CLAIMS VACÍOS CONSECUTIVOS —no «colas de esta vuelta»: el contador es global y
	// un claim con lote lo reinicia, así que la ventana puede quedar a caballo de dos vueltas—. Es lo que
	// decide cuándo dormir: sólo tras N vacíos seguidos, siendo N el número de colas. Dormir en cuanto una
	// cola está vacía —el comportamiento de antes de T4.1, trasladado sin pensar— pondría un poll de 500 ms
	// entre cola y cola: con cinco instalaciones, la quinta esperaría 2,5 s por vuelta aunque tuviera
	// trabajo desde el primer instante. Con UNA sola cola el umbral es 1 y el comportamiento es el de
	// siempre. Ver el bloque de arriba para por qué «consecutivos» basta y no se toca.
	vaciasSeguidas := 0

	for {
		if ctx.Err() != nil {
			return
		}

		// Dos latidos y un `default`: el select sigue siendo NO BLOQUEANTE. Si los dos canales estuvieran
		// listos a la vez, Go elige uno al azar y el tick del otro QUEDA PENDIENTE en su ticker, así que se
		// atiende en la vuelta siguiente —que llega, como mucho, un poll después—. No se pierde ninguno.
		//
		// 🔴 VA ANTES DE LA COMPROBACIÓN DEL BREAKER, y eso no es casualidad: con el circuito ABIERTO el
		// bucle no reclama y se va a dormir con un `continue`, pero vuelve por aquí arriba. Publicar
		// después habría dejado el parte congelado EXACTAMENTE en el escenario que el parte existe para
		// contar (Ollama caído ⇒ breaker abierto), y a los 90 s el daemon lo habría tirado por rancio: el
		// operador vería un hueco donde debía leer "open".
		select {
		case <-latido:
			c.log.Info("cajero: contadores", c.contadores()...)
		case <-tParte.C:
			c.publicarParte(ctx)
		default:
		}

		// (1) Circuito abierto ⇒ no se reclama en absoluto (T2.4). Vale para TODAS las colas: el breaker es
		// uno por proceso porque protege a Ollama, que es uno por máquina (ver Deps.Colas).
		if c.breaker.State() == breaker.StateOpen {
			vaciasSeguidas = 0 // la vuelta se interrumpió sin terminar; la siguiente empieza limpia
			if c.despertador.Esperar(ctx) != nil {
				return
			}
			continue
		}

		// (2) Sin contrato de intenciones cargado tampoco se reclama: reclamar sería marcar el lote
		// `tomado` para acabar clasificándolo con un prompt vacío. Espejo del `ready` del decorador.
		if c.listo != nil && !c.listo() {
			vaciasSeguidas = 0
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

		// (4) EL CLAIM ROTA — ROUND-ROBIN ESTRICTO (T4.1). El cursor avanza UNA POSICIÓN POR CLAIM
		// INTENTADO, haya lote o no, y ese «o no» es la mitad importante de la regla: si el cursor sólo
		// avanzara cuando hay trabajo, una cola PARLANCHINA (siempre con cola pendiente) se llevaría todas
		// las vueltas y la cola callada de al lado no volvería a ser atendida hasta que la primera se
		// vaciara. Eso es inanición, y es justo lo que el criterio de equidad de T4.1 mide.
		//
		// El cursor se lee y se avanza AQUÍ, en la única goroutine que lo toca. La cola elegida viaja por
		// PARÁMETRO al resto del camino (procesar → cerrar), y no en un campo del Cajero, porque el cierre
		// tiene que ir contra LA MISMA cola de la que se reclamó: cerrar un lote contra la cola del vecino
		// no encontraría sus filas, devolvería ErrLoteRelevado y el trabajo pagado se perdería en silencio.
		cm := c.colas[c.cursor]
		c.cursor = (c.cursor + 1) % len(c.colas)

		lote, err := cm.cola.Reclamar(ctx, c.maxFilas)
		if err != nil {
			<-c.sem
			if ctx.Err() != nil {
				return // el error es la cancelación, no un fallo de la BD
			}
			c.fallos.Add(1)
			// `cola` nombra la instalación: con N data_dir's, «no se pudo reclamar» sin decir de cuál deja
			// al operador con cinco ficheros que revisar y ninguna pista.
			c.log.Error("cajero: no se pudo reclamar un lote de la cola", "error", err, "cola", cm.nombre)
			// Se DUERME aunque no se haya completado la vuelta, y el contador se reinicia: un claim que
			// falla no es una cola vacía, y encadenar vueltas contra una BD rota sería un bucle caliente.
			vaciasSeguidas = 0
			if c.despertador.Esperar(ctx) != nil {
				return
			}
			continue
		}
		if lote == nil || len(lote.Mensajes) == 0 {
			// Cola vacía: es el estado NORMAL de un worker al día, no un error.
			<-c.sem
			vaciasSeguidas++
			if vaciasSeguidas < len(c.colas) {
				// Aún no se han encadenado N vacíos: quedan colas por probar antes de concluir que no hay
				// trabajo en ninguna parte, así que se sigue SIN dormir.
				continue
			}
			vaciasSeguidas = 0
			if c.despertador.Esperar(ctx) != nil {
				return
			}
			continue
		}
		vaciasSeguidas = 0

		c.enVuelo.Add(1)
		go func(cm *colaMontada, l *app.ColaLote) {
			defer func() {
				<-c.sem
				c.enVuelo.Done()
			}()
			c.procesar(ctx, cm, l)
		}(cm, lote)
	}
}

// procesar clasifica UN lote y lo cierra. Se ejecuta con una plaza del semáforo tomada.
//
// `cm` es LA COLA DE LA QUE SE RECLAMÓ, y viaja por parámetro hasta cerrar() porque el cierre lleva
// fencing por ClaimToken: contra otra cola no encontraría las filas, devolvería ErrLoteRelevado y una
// inferencia ya pagada se perdería con un log que diría «carrera del lease» sin que hubiera ninguna.
func (c *Cajero) procesar(ctx context.Context, cm *colaMontada, lote *app.ColaLote) {
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
		if c.cerrar(ctx, cm, lote, app.SobreOmitido(app.MotivoSinTexto)) {
			c.omitidos.Add(1)
		}
		return
	}

	if !c.breaker.BeginAttempt() {
		// Ver el bloque «QUIÉN ESCRIBE app.MotivoBreaker» en el doc comment de bucle(): éste es el
		// único punto que lo escribe, y es exactamente la semántica del enum («el breaker está
		// abierto; ni siquiera se llamó a Ollama»).
		if c.cerrar(ctx, cm, lote, app.SobreOmitido(app.MotivoBreaker)) {
			c.omitidos.Add(1)
		}
		return
	}

	inicio := c.ahora()
	res, err := c.clasificar(ctx, texto)
	// T4.5 · la latencia se mide UNA vez, con el reloj del CAJERO, y sirve a los dos caminos.
	//
	// 🔴 EL RELOJ DEL CAJERO Y NO `res.Metrics.TotalMs`, que es lo que el log de éxito ya publicaba. Son
	// dos números distintos: aquél es lo que Ollama dice que tardó por dentro, y éste es lo que el cajero
	// ESPERÓ de verdad (incluye el viaje HTTP, la serialización y la cola del propio Ollama). El segundo es
	// el que gobierna la plaza del semáforo y, por tanto, el que explica que la cola avance o no. Además es
	// el ÚNICO que existe en el camino de fallo —un timeout no trae Metrics—, así que medir con res.Metrics
	// obligaría a mezclar dos relojes en la misma serie.
	transcurrido := c.ahora().Sub(inicio)
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
		// 🔴 LOS FALLOS SÍ ENTRAN EN EL HISTOGRAMA, y va aquí —después del corte del apagado y antes de
		// todo lo demás— para que el único tiempo que NO se mide sea el que cortó el SIGTERM. Un timeout de
		// 15 s es latencia que el cajero pagó: ocupó la plaza del semáforo y toda la cola esperó detrás. El
		// argumento largo, en histogramaInferencia.observar.
		c.inferencia.observar(transcurrido)
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
				"cola", cm.nombre,
			)
			if c.cerrar(ctx, cm, lote, app.SobreOmitido(app.MotivoFalloRepetido)) {
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
			"latencia_ms", transcurrido.Milliseconds(),
			"circuito", c.breaker.State(),
			"cola", cm.nombre,
		)
		return
	}
	c.inferencia.observar(transcurrido) // el camino de éxito, con el MISMO reloj que el de fallo
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
		if c.cerrar(ctx, cm, lote, app.SobreOmitido(app.MotivoDesconocido)) {
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
		if c.cerrar(ctx, cm, lote, app.SobreOmitido(app.MotivoDesconocido)) {
			c.omitidos.Add(1)
		}
		return
	}
	if c.cerrar(ctx, cm, lote, sobre) {
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
func (c *Cajero) cerrar(ctx context.Context, cm *colaMontada, lote *app.ColaLote, intentJSON string) bool {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cierreTimeout)
	defer cancel()

	// SIEMPRE contra `cm`, la cola de la que se reclamó: ver el doc comment de procesar().
	err := cm.cola.MarcarClasificado(cctx, lote, intentJSON)
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
			"session_id", lote.SessionID, "mensajes", len(lote.Mensajes), "relevados", c.relevados.Load(),
			"cola", cm.nombre)
		return false
	default:
		c.fallos.Add(1)
		c.log.Error("cajero: no se pudo cerrar el lote", "error", err, "session_id", lote.SessionID,
			"cola", cm.nombre)
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

// parteTimeout es el plazo de UNA publicación del parte. Es corto a propósito y no es cosmético: el
// parte se publica DESDE EL BUCLE, en la goroutine que reclama, así que un UPSERT que se quedara
// esperando un fichero bloqueado dejaría de reclamar lotes en TODAS las colas mientras tanto. Dos
// segundos es un orden de magnitud más de lo que cuesta escribir cinco columnas en un SQLite local, y
// un orden de magnitud menos que el poll con el que el bucle ya vive.
//
// A DIFERENCIA DE cierreTimeout, ESTE CONTEXTO NO SE DESLIGA del ctx del proceso: el cierre de un lote
// salva una inferencia YA PAGADA y merece terminar aunque llegue el SIGTERM; un parte a medio escribir
// durante el apagado no vale nada —el proceso se está yendo, y el siguiente parte lo escribirá el
// cajero que arranque—. Colgarlo del ctx es además lo que garantiza que Run no se demora en la parada.
const parteTimeout = 2 * time.Second

// publicarParte deja el parte de salud del cajero escrito en TODAS sus colas (Plan 051 Ola 4 · T4.5).
//
// ─────────────────────────────────────────────────────────────────────────────
// POR QUÉ EN TODAS LAS COLAS Y NO EN UNA
// ─────────────────────────────────────────────────────────────────────────────
// Las tres señales del parte —circuito, taskset y p50— son POR PROCESO, no por cola: describen a
// OLLAMA y a ESTA máquina, que son uno solo aunque el cajero atienda cinco instalaciones (es el mismo
// argumento por el que el semáforo y el breaker no rotan, ver Deps.Colas). Pero el LECTOR no es uno:
// cada instalación tiene su propio `agent serve`, que construye su heartbeat leyendo SU
// <data_dir>/cola_entrantes.db y no sabe nada de las otras. Publicar en una sola cola dejaría a las N-1
// instalaciones restantes con `intent_circuit` vacío para siempre, sin ningún síntoma.
//
// Sí: el mismo valor se escribe N veces. Son N UPSERT de cinco columnas cada 30 s —con cinco
// instalaciones, una escritura cada seis segundos repartida entre cinco ficheros—, y el precio de la
// alternativa (un fichero compartido fuera de la cola, o un IPC nuevo) es una pieza que mantener.
//
// ─────────────────────────────────────────────────────────────────────────────
// UN FALLO AQUÍ NO PUEDE FRENAR NI TUMBAR EL BUCLE
// ─────────────────────────────────────────────────────────────────────────────
// El parte es TELEMETRÍA; la cola es EL TRABAJO. Un error se avisa en Warn —nunca Error, y nunca con un
// return que corte el recorrido— y se sigue con la cola siguiente: que el fichero de una instalación
// esté bloqueado no puede dejar sin parte a las otras cuatro, igual que no las deja sin barrido (ver
// barrerLeases). El modo de fallo resultante es el bueno: el daemon deja de ver partes frescos, los
// declara rancios a los 90 s (app.ParteRancio) y publica `intent_circuit` VACÍO — la ausencia honesta
// del dato, nunca un valor inventado.
func (c *Cajero) publicarParte(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	p := app.ParteWorker{
		TS:       c.ahora(),
		Circuito: c.Circuito(),
		Taskset:  c.Taskset(),
		P50ms:    c.inferencia.p50MS(),
	}
	for _, cm := range c.colas {
		if cm.parte == nil {
			continue // esta instalación no tiene buzón de parte: ver ColaNombrada.Parte
		}
		if ctx.Err() != nil {
			return
		}
		pctx, cancel := context.WithTimeout(ctx, parteTimeout)
		err := cm.parte.PublicarParte(pctx, p)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return // el error es la parada, no un fallo de la BD
			}
			// 🔴 INV-051.1: aquí sólo va el nombre de la cola (una ruta de directorio) y el error. El parte
			// no contiene nada de negocio, así que ni siquiera hay un campo del que haya que cuidarse.
			c.log.Warn("cajero: no se pudo publicar el parte de salud; el daemon de esa instalación "+
				"publicará `intent_circuit` VACÍO hasta el próximo parte (la clasificación NO se ve afectada)",
				"error", err, "cola", cm.nombre)
		}
	}
}

// barrerLeases devuelve a `nuevo` las filas que un cajero muerto dejó bloqueadas en `tomado`. Corre en
// su propia goroutine con periodo = lease: barrer más a menudo no rescata antes (una fila sólo es
// rescatable cuando su lease ya venció) y barrer más despacio alarga la espera sin ganar nada.
//
// 🔴 CON N COLAS SE BARREN TODAS DENTRO DEL MISMO TICK, y NO se lanza una goroutine por cola (T4.1). Una
// goroutine por cola significaría N tickers en vez de uno —N veces el trabajo de despertar al proceso, y
// N cadencias que pueden desfasarse— y, sobre todo, rompería la lectura del contador agregado: los N
// barridos escribirían en `c.rescatados` en instantes distintos y el número dejaría de corresponder a
// «lo rescatado en la última pasada». El barrido es un UPDATE contra un SQLite local; hacerlos en fila es
// milisegundos, no un problema de latencia que haya que paralelizar.
//
// EL BUCLE NO SE CORTA ANTE UN FALLO: si la cola 2 de 5 no se puede barrer, las otras cuatro se barren
// igual. Un `return`/`break` ahí dejaría a las instalaciones siguientes sin rescate por culpa de un
// vecino, y el barrido es justo lo que impide que las filas `tomado` de un worker muerto se queden
// bloqueadas para siempre.
func (c *Cajero) barrerLeases(ctx context.Context) {
	t := time.NewTicker(c.lease)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, cm := range c.colas {
				if ctx.Err() != nil {
					return
				}
				n, err := cm.cola.BarrerLeasesVencidos(ctx, c.lease)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					c.log.Error("cajero: el barrido de leases falló", "error", err, "cola", cm.nombre)
					continue
				}
				if n > 0 {
					// Warn y no Info: rescatar filas significa que ALGUIEN murió a mitad (este worker en su
					// vida anterior, o el otro cajero de una máquina multi-empresa). No es normal.
					//
					// Se emite el desglose de ESTA cola y el agregado del proceso: el primero dice QUÉ
					// instalación tiene el problema, el segundo sigue siendo el número que se compara con el
					// resto de contadores del bloque periódico.
					deLaCola := cm.rescatados.Add(n)
					total := c.rescatados.Add(n)
					c.log.Warn("cajero: leases vencidos rescatados (vuelven a `nuevo`)",
						"filas", n, "lease_s", int(c.lease.Seconds()), "rescatados_total", total,
						"cola", cm.nombre, "rescatados_cola", deLaCola)
				}
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
//
// 🔴 LÍMITE DECLARADO (T4.6, decisión del 2026-08-17): esto corre UNA SOLA VEZ, desde Run, y el veredicto
// que guarda se republica en el parte del worker cada 30 s SIN VOLVER A MIRAR. Un `taskset -pc` en
// caliente NO se refleja: se comprobó en campo (PC-12) cambiando la afinidad a `0-5` con el cajero vivo,
// y tanto el parte como la consola siguieron diciendo `disjunta`. La regla de rancidez NO puede cazarlo,
// porque el parte sí se refresca — el valor es OBSOLETO, no rancio, que es la única familia de avería
// que las defensas de la Ola 4 no cubren.
//
// Se eligió declararlo en vez de arreglarlo, y la declaración vive donde alguien la lee: el tooltip de
// los tres chips de CPU de la consola (`dashboard.html`), custodiado por
// `TestDashboardAvisaQueElRepartoDeCPUEsDelArranque`. Si algún día se arregla, la forma es llamar a esto
// desde la publicación del parte en vez de desde Run —es una lectura de /proc, no cuesta nada— y
// entonces hay que RETIRAR ese aviso de la consola: un tooltip que avisa de un límite que ya no existe
// es la misma avería al revés.
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

	// T4.5 · EL VEREDICTO SE RETIENE, no sólo se loguea. Hasta la Ola 4 este valor moría en el switch de
	// abajo, y con él la única pista que el operador tiene de por qué su clasificador va lento (la
	// medición: mismo vecino con 2 vCPU, 17,2 % de lotes por encima del timeout con los conjuntos
	// solapados contra 0 % con los disjuntos). Ahora viaja en el parte hasta el heartbeat.
	//
	// SE GUARDA AQUÍ Y NO EN LOS CAMINOS DE ERROR DE ARRIBA, a propósito: si la lectura falló (no-Linux,
	// /proc ilegible, Ollama de otro usuario) el veredicto se queda VACÍO, que es «no se sabe». Cualquier
	// otra cosa —un "disjunta" optimista por defecto— sería inventarse la señal que este chequeo existe
	// para no dar por buena (ver el encabezado de afinidad.go).
	c.veredictoTaskset.Store(string(reparto.Veredicto))

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

// Rescatados son las filas que el barrido devolvió de `tomado` a `nuevo`, SUMADAS SOBRE TODAS LAS COLAS.
// El desglose por instalación está en RescatadosPorCola.
func (c *Cajero) Rescatados() int64 { return c.rescatados.Load() }

// Colas es cuántas colas (instalaciones) atiende este cajero en round-robin (T4.1). Con el default es 1.
func (c *Cajero) Colas() int { return len(c.colas) }

// RescatadosPorCola es el desglose de Rescatados por instalación, indexado por el nombre de la cola (el
// data_dir en el cableado real).
//
// VA APARTE DEL AGREGADO, no en su lugar: el agregado responde «¿está muriéndose algún cajero?» y esto
// responde «¿cuál?». Con una sola cola el mapa tiene una entrada y su valor es igual al agregado.
func (c *Cajero) RescatadosPorCola() map[string]int64 {
	m := make(map[string]int64, len(c.colas))
	for _, cm := range c.colas {
		m[cm.nombre] = cm.rescatados.Load()
	}
	return m
}

// AperturasBreaker son las veces que el circuito pasó de no-abierto a ABIERTO. Es el contador que hay
// que mirar para saber si Ollama se está cayendo — no el de omisiones por `breaker`, que con el
// semáforo en 1 es casi siempre cero (ver el doc comment de bucle()).
func (c *Cajero) AperturasBreaker() int64 { return c.aperturasBreaker.Load() }

// Circuito devuelve el estado del circuito, con las mismas etiquetas que `GET /v1/intent/status`.
func (c *Cajero) Circuito() string { return c.breaker.State() }

// Taskset devuelve el veredicto del reparto de CPUs entre Ollama y el cajero (T2.8): "disjunta",
// "solapada" o "cajero_sin_confinar".
//
// 🔴 VACÍO SIGNIFICA «NO SE SABE», Y NUNCA SE SUSTITUYE POR UN DEFAULT. Se queda vacío en tres casos
// reales: fuera de Linux (taskset_other.go no puede leer la afinidad y devuelve error), cuando Ollama
// corre con otro usuario y su /proc no es legible, y cuando la lista de CPUs del kernel no se pudo
// interpretar. En los tres, lo cierto es que el reparto se desconoce — y un "disjunta" por defecto
// diría justo lo contrario de lo que la medición avisa (con los conjuntos solapados, el 17,2 % de las
// clasificaciones se pasó del timeout; ver el encabezado de afinidad.go).
func (c *Cajero) Taskset() string {
	v, _ := c.veredictoTaskset.Load().(string) // nil (nunca escrito) ⇒ "" ⇒ «no se sabe»
	return v
}

// P50InferenciaMS es el p50 de la latencia de inferencia en ms, o 0 si aún no hay muestras. Es una COTA
// SUPERIOR (el borde del bucket), y acumulado desde que arrancó el proceso: ver histogramaInferencia.
func (c *Cajero) P50InferenciaMS() int64 { return c.inferencia.p50MS() }
