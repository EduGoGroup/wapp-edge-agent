// Package cajero es el WORKER-CAJERO del Edge (Plan 051 Ola 2, ADR-0038): el proceso DUEÑO DEL LLM
// LOCAL de la máquina.
//
// 🔴 CAMBIÓ DE OFICIO EL 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-2, ADR-0045 §8). Hasta esa fecha era «el
// proceso que reclama lotes de la cola de entrantes, los clasifica contra el LLM local y escribe el
// intent de vuelta»: clasificaba POR INICIATIVA PROPIA. Hoy no lo hace. El Cloud es el único orquestador
// LLM (ADR-0045 §1) y baja el prompt ya construido en un frame `inference_request`; este proceso lo
// SIRVE. De push a pull.
//
// LA MEDICIÓN QUE LO DECIDIÓ: bajo push el cajero SIEMPRE PERDÍA LA CARRERA contra el despachador, que
// entrega el entrante al instante. De 430 inferencias medidas en campo el 2026-08-23, UNA llegó a tiempo
// a su ventana, y el intent no llegó JAMÁS a la nube. El argumento entero está en el bloque «EL BUCLE YA
// NO CLASIFICA» de bucle().
//
// POR QUÉ SIGUE SIENDO UN PROCESO APARTE, y no una goroutine más del daemon: REQ-051.10 («ningún otro
// proceso que el worker habla con Ollama»). Si la inferencia viviera dentro de `agent serve`, un Ollama
// lento se comería el mismo proceso que mantiene los sockets de WhatsApp vivos — que es exactamente el
// problema que la Ola 1 del Plan 051 resolvió sacándola de ahí. El gate de ese requisito es un grep de
// `ollama.New`, y su único resultado es cmd/agent/cajero.go.
//
// LAS TRES COSAS QUE HACE HOY:
//
//  1. SIRVE INFERENCIA al Cloud (servidor.go), con su aforo, su breaker y su histograma. Es su trabajo.
//  2. PUBLICA SU PARTE DE SALUD en la cola de cada instalación (publicarParte). Es el único tubo por el
//     que el circuito, el taskset y el p50 llegan al daemon, que los sube en su heartbeat.
//  3. BARRE LEASES VENCIDOS (barrerLeases), limpiando filas `tomado` que dejó un binario anterior.
//
// 🔴 INV-051.1 — NI EL PROMPT NI LA SALIDA DEL MODELO SALEN POR EL LOG, tampoco en debug. Los dos son
// contenido de negocio: el prompt lleva dentro el texto que el cliente escribió por WhatsApp. Viajan en
// claro en memoria (hay que dárselos al modelo) y mueren ahí. Lo que se loguea es la FORMA del trabajo:
// `command_id`, tamaños, tiempos y desenlace. Si añades una clave a un log de este paquete, pregúntate
// primero si un operador con acceso al fichero podría reconstruir con ella lo que un cliente escribió.
package cajero

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/breaker"
	"github.com/EduGoGroup/wapp-edge-intent/classifier"
	"github.com/EduGoGroup/wapp-edge-intent/ollama"
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
// Deps.Timeout. Desde el ADR-0045 se aplica cuando el Cloud NO fija plazo (`InferenceRequest.timeout_ms
// == 0`); cuando sí lo fija, manda el suyo acotado por DefaultMaxTimeoutMS.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 ERA 15.000 Y SE SUBE A 45.000 EL 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-2)
// ─────────────────────────────────────────────────────────────────────────────
//
// POR QUÉ EL 15.000 ESTABA MAL CALIBRADO. Se eligió como «≈4× la p95» con p95 = 3.736 ms, el número de
// la O0 del Plan 051 (docs/completed/051-worker-cajero-edge/O0-resultados-2026-08-09.md §4.1). Ese
// número tiene DOS correcciones publicadas en la propia documentación, las dos en
// docs/journal/2026-08-16.md (§correcciones a la O0):
//
//   - se midió CON EL VPS VACÍO (el stack de wApp apagado a propósito para no contaminar). Repetida la
//     batería con el stack VIVO, el p50 sube de 2.613 a 3.610 ms (+38 %);
//   - la p95 publicada está POST-PROCESADA: el crudo de la misma corrida es p95 5.323 ms con un máximo
//     de 26.397 ms.
//
// O sea que el «4× la p95» real era ~2,8× una p95 que ya estaba a 5,3 s, con máximos que doblaban el
// techo.
//
// 🔴 Y POR QUÉ NO SE PUEDE RECALIBRAR CONTRA LA MUESTRA DE CAMPO: ESTÁ CENSURADA POR ESTE MISMO TECHO.
// La medición del 2026-08-23 (430 inferencias en el VPS de UAT,
// docs/brainstorm/2026-08-23-tres-niveles-de-llm-y-doctrina-del-transporte.md §Anexo) da p50 8,1 s,
// p90 12,8 s y máximo 15,6 s — contra un techo de 15,0 s. Un máximo que coincide con el techo no es un
// máximo: es el punto donde el instrumento deja de medir. Las inferencias más largas se abortaron y no
// dejaron duración que promediar, así que esa muestra NO PUEDE decir cuánto tarda la cola derecha.
// Calibrar contra ella repetiría el error una vuelta más.
//
// DE DÓNDE SALE EL 45.000, ENTONCES: de la evidencia del VPS real SIN este techo encima. La tabla de
// contención del 2026-08-16 (docs/journal/2026-08-16.md §"Tabla B", 7 escenarios × 30 muestras, batería
// propia sin el plazo del cajero) mide los máximos que de verdad alcanza esta máquina:
//
//	escenario                          p50        p95        máx      > 15 s
//	M1 limpio (referencia)           4.282      6.752      7.015        0 %
//	B2b aislado 4/2 (taskset real)   7.008      9.932     12.098        0 %
//	G2 vecino con 2 vCPU            11.613     19.604     25.564     17,2 %
//	G3 vecino con 3 vCPU            18.953     38.494     45.629     79,3 %
//
// Y la medición manual del prompt de la Ola 1 (más largo: 1.469 tokens contra 1.022) llegó a 36,5 s
// (docs/plans/044-carrito-llm-2-presupuestos/tasks.md, T1.3(c), n=6: 2,3–36,5 s). 45.000 cubre los tres
// peores casos medidos —25,6 s, 36,5 s y 45,6 s— y no un cuarto inventado.
//
// 🔴 EL SEGUNDO MOTIVO, Y ES EL QUE CIERRA EL NÚMERO: RESTAURA LA CALIBRACIÓN DEL MP-09. FraccionLentitud
// deriva de este plazo el umbral por encima del cual un acierto castiga al breaker, y el MP-09 lo eligió
// para que quedara A UN FACTOR ~4,6 DEL p50 SANO (12 s contra los 2.613 ms de la O0), «separado de lo
// sano por un orden de magnitud». Con el p50 REAL de campo (8,1 s) ese umbral de 12 s está a 1,48× del
// p50 — y el p90 de campo (12,8 s) YA LO SUPERA. Es decir: con el techo viejo, más de una de cada diez
// inferencias PERFECTAMENTE SANAS contaba como fallo para el circuito, que es justo lo contrario de lo
// que el MP-09 quería. Con 45.000 el umbral pasa a 36 s = 4,4× el p50 de campo, casi exactamente la
// relación con la que el MP-09 se calibró.
//
// EL PRECIO, DICHO EN VOZ ALTA: una inferencia patológica ocupa la única plaza del aforo hasta 45 s.
// Eso NO deja al Cloud esperando, porque el plazo lo fija él (`timeout_ms`) y las peticiones que no
// consiguen plaza dentro del suyo se responden EDGE_SIN_CAPACIDAD sin colgarse. El default sólo gobierna
// el caso en que el Cloud no dice nada.
const DefaultInferenceTimeoutMS = 45_000

// FraccionLentitud es la fracción del presupuesto de inferencia (Deps.Timeout) por encima de la cual un
// ACIERTO deja de contar como señal de salud: el clasificador respondió, sí, pero tan cerca de agotar su
// plazo que lo siguiente que hará es agotarlo. Ese acierto NO cierra el circuito, castiga al breaker
// igual que un timeout — y el LOTE se cierra con su intent, que se clasificó bien y no se tira.
//
// ─────────────────────────────────────────────────────────────────────────────
// POR QUÉ EXISTE ESTA CONSTANTE (MP-09, medido en campo el 2026-08-20)
// ─────────────────────────────────────────────────────────────────────────────
// El breaker cuenta fallos CONSECUTIVOS y cualquier éxito pone el contador a cero (breaker.go:68-71).
// Contra un Ollama CAÍDO eso funciona: no acierta nunca, los cinco fallos llegan en ~2 s y el circuito
// abre. Contra un Ollama LENTO —vivo, respondiendo, e inútil a efectos prácticos— NO FUNCIONA, y la
// medición del VPS de UAT lo enseñó en crudo con un backlog de 240 entrantes:
//
//	T · OK(12190 ms) · OK(12626 ms) · T · T · OK(9460 ms) · OK(9803 ms) · T · OK(14878 ms) · OK(10795 ms)
//
// Racha máxima de timeouts consecutivos: 2. Umbral: 5. En la corrida entera, 95 timeouts y 316 lotes
// clasificados, y el circuito NO ABRIÓ NI UNA VEZ. Aproximadamente dos de cada tres intentos aciertan
// mientras el servicio tarda 9-15 s por lote.
//
// 🔴 EL DATO QUE DECIDE EL DISEÑO: uno de esos «aciertos» tardó 14.878 ms contra un plazo de 15.000. Se
// quedó a 122 ms de ser un timeout. La frontera entre «éxito» y «fallo» ahí no es una señal de salud, es
// un accidente de milisegundos — y el breaker estaba tomando su decisión más importante justo sobre esa
// frontera. Redefinir qué cuenta como éxito ataca eso; subir el peso de los timeouts o bajar el umbral
// no, porque el problema no es cuántos fallos hacen falta sino que un acierto casual los borra todos.
//
// POR QUÉ 0,8 Y NO OTRO NÚMERO. La fracción se eligió para que el umbral quedara a un factor ~4,6 del
// p50 sano, y esa RELACIÓN —no el valor absoluto— es lo que la constante protege:
//
//   - SEPARA de lo sano. La O0 midió p50 = 2.613 ms
//     (docs/completed/051-worker-cajero-edge/O0-resultados-2026-08-09.md): una inferencia normal no
//     rozaba los 12 s ni con el VPS cargado. Para abrir el circuito harían falta CINCO SEGUIDAS por
//     encima del umbral, así que un pico aislado —ni dos, ni tres— no lo abre. Esa era la calibración
//     que la opción de «bajar el umbral a 2 o 3» se cargaba.
//   - CAPTURA lo enfermo. Aplicada a la secuencia real de arriba, el contador ya no se reinicia:
//     T(1) · 12190(2) · 12626(3) · T(4) · T(5) ⇒ ABRE al quinto evento, ~62 s antes que hoy (que es
//     «nunca»).
//
// 🔧 ESA RELACIÓN SE HABÍA ROTO, Y LA ARREGLA EL PLAZO NUEVO (2026-08-24, Plan 044 · Ola 1.6 · T1.6-2).
// El p50 REAL de campo no es el de la O0 (medido con el VPS vacío) sino 8,1 s
// (docs/brainstorm/2026-08-23-tres-niveles-de-llm-y-doctrina-del-transporte.md §Anexo, 430 inferencias
// en UAT). Contra ese p50, el umbral de 12 s que salía del plazo de 15 s estaba a 1,48× — y el p90 de
// campo, 12,8 s, YA LO SUPERABA. O sea que el criterio de lentitud estaba marcando como enferma más de
// una de cada diez inferencias SANAS y castigando al breaker por ellas: exactamente al revés de lo que
// esta constante existe para hacer. Con el plazo nuevo (45 s) el umbral pasa a 36 s = 4,4× el p50 de
// campo, que es la relación con la que se calibró. La constante NO se ha tocado: lo que estaba mal era
// el número del que se deriva.
//
// LO QUE ESTA CONSTANTE NO ES: no es una perilla. Se deriva de un plazo, así que quien mueva el
// presupuesto mueve el umbral con él y la relación entre los dos números no se puede desincronizar a
// mano. Si algún día hace falta afinarla en campo sin recompilar, hacerla configurable es un cambio de
// una línea; hasta que la medición lo pida, una perilla más es una forma de romper esta calibración por
// descuido.
//
// 🔴 DE QUÉ PLAZO, Y ES LO QUE ARREGLÓ T1.7-2 (Plan 044 · Ola 1.7). Del plazo de CADA PETICIÓN, no del
// default del proceso. Todo lo de arriba se escribió cuando el Edge decidía el presupuesto y había UNO
// solo; desde el ADR-0045 lo fija el Cloud petición a petición, y desde el ADR-0044 conviven dos vías con
// plazos de un orden de magnitud de diferencia (interactiva y lote). Contra un umbral único, la relación
// «~4,6× el p50 sano» que esta constante protege sólo podía cumplirse para UNA de las dos: la otra
// quedaba o inmune al criterio (una interactiva de 9,9 s sobre 10 s contada como sana) o marcada como
// enferma yendo holgada (una de lote de 40 s sobre 90 s). Derivándolo del plazo de cada una, la relación
// se cumple en las dos a la vez y sin tocar este número.
//
// SIN PLAZO EFECTIVO NO HAY PRESUPUESTO del que derivar nada y el criterio se apaga: el breaker vuelve
// exactamente a la conducta de antes del MP-09. Es la única lectura honesta —sin plazo, «tardar
// demasiado» no está definido—. Hoy eso exige las dos cosas a la vez: que el Cloud no fije `timeout_ms` Y
// que el Edge tenga Deps.Timeout desactivado, que es como se mantiene intacta esa promesa.
const FraccionLentitud = 0.8

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
// 💀 HUÉRFANA DESDE EL 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-2, ADR-0045). El freno que describe protegía
// EL CLAIM, y el claim ya no existe: `Deps.MaxIntentos` se retiró con él y nada en este paquete lee esta
// constante. Sobrevive porque `config.DefaultWorkerMaxIntentos` la referencia y con ella toda la cadena
// hasta `WAPP_WORKER_MAX_INTENTOS`, que un operador puede seguir escribiendo en un EnvironmentFile sin
// que haga absolutamente nada. Retirar esa cadena entera es una decisión de config —hay `.env` en
// máquinas de clientes— y no es de esta tarea; queda anotado aquí para que quien la retome sepa que el
// consumidor murió primero.
const DefaultMaxIntentos = 3

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
	// Cola es el puerto del lado cajero cuando hay UNA SOLA cola. Es el atajo mono-cola y sigue siendo el
	// caso normal (una instalación por máquina).
	//
	// Obligatoria SALVO que se pase Colas. Si se pasan las dos, MANDA Colas y ésta se ignora: elegir la
	// más rica evita la fusión silenciosa (una cola duplicada entre las dos vías sería exactamente el
	// round-robin que se reclama a sí mismo que el T4.1 prohíbe).
	//
	// ⚠️ DESDE EL ADR-0045 EL CAJERO NO RECLAMA DE ELLA. De los tres métodos de app.ColaCajero sólo se usa
	// BarrerLeasesVencidos; Reclamar y MarcarClasificado quedaron sin llamante en este paquete. La cola
	// sigue haciendo falta por otra cosa: es donde vive el buzón del PARTE (ColaNombrada.Parte), el único
	// tubo por el que la salud del cajero llega al daemon de su instalación. Ver «EL BUCLE YA NO
	// CLASIFICA» en bucle().
	Cola app.ColaCajero
	// Colas es la LISTA de colas que el cajero atiende (Plan 051 Ola 4 · T4.1), una por instalación
	// (un data_dir ⇒ un cola_entrantes.db). Tiene precedencia sobre Cola.
	//
	// 🔴 EL AFORO Y EL BREAKER SIGUEN SIENDO UNO POR PROCESO, no uno por cola, y eso NO es un descuido
	// de esta lista: los dos protegen a OLLAMA, que es uno por máquina. Un aforo por cola con N=1 y
	// cinco instalaciones daría cinco inferencias simultáneas contra la misma instancia de Ollama —justo
	// el solapamiento que la O0 midió como la causa de que la latencia p50 se dispare—, y un breaker por
	// cola dejaría a cuatro colas martilleando un Ollama que la quinta ya sabe caído. Si alguna vez hace
	// falta acotar por cola, el sitio es un número nuevo, no partir estos dos.
	Colas []ColaNombrada
	// Ollama es el proveedor LOCAL de LLM. Obligatorio para poder SERVIR inferencia (ADR-0045): sin él,
	// ServidorInferencia() devuelve nil y este proceso queda reducido a barrer leases y publicar su parte.
	//
	// 🔴 SUSTITUYE A `Clasificador`, RETIRADO EL 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-2). Aquel era un
	// `Classify(ctx, texto) → {intent, params, confidence}`: el cajero construía el prompt desde el
	// contrato de intenciones y interpretaba la respuesta. Bajo el ADR-0045 el prompt lo construye EL
	// CLOUD y la salida sube CRUDA, así que la interfaz que hace falta es la del proveedor pelado
	// («prompt entra → JSON sale»), no la de un clasificador. Ver Chateador.
	Ollama Chateador
	// Modelo es el modelo que se le pide al proveedor (WAPP_AGENT_INTENT_MODEL). LO ELIGE EL EDGE y no
	// viaja en el frame: es propiedad de la máquina del cliente (qué cabe en su RAM, qué tolera su CPU).
	Modelo string
	// Temperatura es la que se aplica cuando el Cloud no manda ninguna. <=0 ⇒ DefaultTemperatura (0,1).
	//
	// ⚠️ El guardarraíl es `<=0` y eso significa que NO SE PUEDE PEDIR 0 COMO DEFAULT DEL EDGE — cae a
	// 0,1. Es deliberado y no cuesta nada: quien quiera determinismo exacto lo pide en el frame, donde el
	// campo SÍ distingue «cero» de «no dije nada» (es un `optional`, ver PeticionInferencia.Temperature).
	Temperatura float64
	// Opciones son las opciones de modelo que el Edge le pasa al proveedor (num_thread / num_ctx /
	// num_predict), calibradas en la O0 sobre la máquina real. nil ⇒ se mandan sólo las que el proveedor
	// tenga por defecto.
	//
	// ⚠️ `num_predict` DE AQUÍ ES UN DEFAULT, NO EL TECHO: desde T1.7-3 el Cloud puede fijarlo por petición
	// (InferenceRequest.max_output_tokens) y entonces manda el suyo. Ver el bloque de `num_predict` en
	// servidor.go · chat().
	Opciones map[string]any
	// KeepAlive son los SEGUNDOS que Ollama debe mantener el modelo cargado tras responder, y viaja en el
	// primer nivel de cada /api/chat (T1.7-4). Negativo = para siempre (ollama.KeepAliveForever).
	// nil ⇒ ollama.DefaultKeepAliveSeconds (hoy -1).
	//
	// 🔴 ES PUNTERO Y EL NIL ES EL LADO SEGURO, al revés que casi todos los guardarraíles de este struct
	// (que son `<=0 ⇒ default`). Aquí ese patrón sería un desastre silencioso: para Ollama el CERO
	// significa «descarga el modelo en cuanto respondas», o sea lo contrario de lo que queremos, y el cero
	// es a la vez el cero-valor de un `int`. Con `<=0 ⇒ default` no se podría pedir 0 nunca; con un `int`
	// desnudo, un Deps a medio poblar pediría descarga inmediata en cada petición. Con puntero, el
	// cero-valor del struct da el default (para siempre) y el 0 explícito sigue siendo expresable.
	//
	// POR QUÉ IMPORTA: cuando el runner de Ollama muere por silencio se lleva LA CACHÉ DE PREFIJOS con él,
	// y el siguiente mensaje paga carga del modelo (39 s medidos) más el prefill en frío del prompt
	// entero. En UAT eso lo tapa `OLLAMA_KEEP_ALIVE=-1` en el env de la unidad; en la máquina de un
	// cliente no hay quien lo ponga.
	KeepAlive *int
	// Breaker es el circuito compartido. nil ⇒ se construye uno con la calibración por defecto.
	Breaker Interruptor
	// Despertador es cómo se espera cuando la cola está vacía. nil ⇒ PollFijo(DefaultPollMS).
	Despertador Despertador
	// Log es el logger. nil ⇒ sharedlogger.Default() (nil-safe, como en todo el repo).
	Log sharedlogger.Logger
	// Ahora es el reloj inyectable. nil ⇒ time.Now. Lo usan la medición de latencia del camino de
	// fallo y el breaker por defecto, para que un test no dependa de esperas reales.
	Ahora func() time.Time
	// MaxConcurrent son las plazas del AFORO de inferencias. <=0 ⇒ DefaultMaxConcurrent (1).
	//
	// 🔴 ES UNO POR PROCESO Y LO COMPARTEN TODOS LOS SOCKETS de la máquina (uno por instalación, ver
	// cmd/agent/cajero.go): N aforos de una plaza serían N inferencias simultáneas contra el mismo
	// Ollama. El porqué de que no puedan ser dos semáforos está en Aforo.
	MaxConcurrent int
	// Lease es el margen del claim (WAPP_AGENT_COLA_LEASE_SECONDS) y, a la vez, el PERIODO del barrido:
	// barrer con la misma cadencia que el lease acota la espera de rescate a [lease, 2·lease]. <=0 ⇒ el
	// default del puerto (app.DefaultColaLeaseSegundos).
	Lease time.Duration
	// Timeout es el plazo por DEFECTO de UNA inferencia (WAPP_WORKER_INFERENCE_TIMEOUT_MS, default
	// DefaultInferenceTimeoutMS = 45 s). <=0 ⇒ sin plazo propio (manda el ctx de quien pide).
	//
	// 🔴 DESDE EL ADR-0045 ES UN DEFAULT, NO EL PLAZO. El presupuesto de cada inferencia lo fija el CLOUD
	// en `InferenceRequest.timeout_ms`, porque es quien conoce su ventana (los 45 s de agregación del
	// Nivel C, el turno acotado del Nivel B — ADR-0044). Éste se aplica sólo cuando el Cloud manda 0
	// («no lo fijé»). El TECHO de lo que el Cloud puede pedir es TimeoutMax.
	//
	// El worker EXISTE JUSTO PARA PODER TARDAR: se sacó la inferencia a otro proceso precisamente para
	// que un Ollama lento no coma el proceso que mantiene los sockets. El argumento completo del número
	// —y por qué el 15 s anterior estaba calibrado contra una muestra CENSURADA— está en
	// DefaultInferenceTimeoutMS.
	//
	// ⚠️ De él se DERIVA el umbral de lentitud del breaker (FraccionLentitud) SÓLO PARA LAS PETICIONES QUE
	// LLEGUEN SIN PLAZO. Desde T1.7-2 el umbral es de cada petición y sale del plazo de ella, así que mover
	// este número mueve el umbral de las que caen en el default y de ninguna más (ver registrarAcierto).
	Timeout time.Duration
	// TimeoutMax es el TECHO de lo que el Cloud puede pedir en `timeout_ms`. <=0 ⇒ DefaultMaxTimeoutMS.
	// Un plazo por encima se RECORTA y se avisa (nunca se rechaza la petición): ver Cajero.plazoDe.
	TimeoutMax time.Duration
	// StatsEvery es cada cuánto el bucle emite el bloque COMPLETO de contadores en un Info
	// (WAPP_WORKER_STATS_EVERY_MS). <=0 ⇒ desactivado (sólo se emite el bloque final de Run).
	//
	// Existe porque el cajero NO tiene plano de control: sin esto los seis contadores sólo son legibles
	// cuando el proceso muere, que es justo cuando ya no sirven para nada. La publicación al heartbeat
	// —la que los saca de la máquina— es de la OLA 4; esto es el sustituto barato hasta entonces.
	StatsEvery time.Duration
	// OllamaURL es la URL del Ollama al que se apunta. Sólo se usa para la comprobación de afinidad de
	// CPU del arranque (T2.8): la URL efectiva la lleva dentro el propio Ollama (Deps.Ollama).
	OllamaURL string
	// NumThread son los hilos de inferencia que se le piden a Ollama (WAPP_WORKER_NUM_THREAD).
	// Aquí se usa para el AVISO de T2.8: pedir más hilos
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
	colas       []*colaMontada
	ollama      Chateador
	modelo      string
	temperatura float64
	opciones    map[string]any
	// keepAlive son los segundos de `keep_alive` que van en CADA petición a Ollama (T1.7-4). Nunca es nil
	// después de New: el default se resuelve allí.
	keepAlive   *int
	breaker     Interruptor
	despertador Despertador
	log         sharedlogger.Logger
	ahora       func() time.Time
	lease       time.Duration
	timeout     time.Duration
	timeoutMax  time.Duration
	statsEvery  time.Duration
	ollamaURL   string
	numThread   int

	// aforo es el AFORO de inferencias (T2.3, extraído a su tipo en T1.6-2): N plazas contra el ÚNICO
	// Ollama de la máquina. Con N=1 no puede haber dos inferencias solapadas — que es lo que el criterio
	// de T2.3 mide en `ollama ps`.
	//
	// 🔴 LO COMPARTEN EL BUCLE Y EL SERVIDOR DE INFERENCIA, y por eso es un campo del Cajero y no una
	// variable local de ninguno de los dos: el servidor se obtiene con ServidorInferencia(), que lo toma
	// de aquí. Ver el bloque «UN SOLO AFORO» en aforo.go.
	aforo *Aforo

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

	// inferencia es el histograma de latencia TOTAL de la inferencia (T4.5), del que sale el p50 del
	// parte. Va por VALOR y no por puntero: es un array de atómicos, no necesita construcción, y el cero
	// funciona.
	inferencia histogramaInferencia

	// ─── T1.7-5 · las DOS FASES, por separado ────────────────────────────────
	//
	// 🔴 NO SUSTITUYEN A `inferencia`, LA COMPLETAN, y las tres tienen que convivir. El total es el único
	// que mide lo que el Cloud ESPERA (incluye el fallo, y el fallo también ocupó la plaza); estas dos
	// miden en qué se fue ese tiempo cuando hubo respuesta. Publicar sólo las dos fases dejaría sin
	// respuesta «¿cuánto tarda esto?», y publicar sólo el total es exactamente el número mezclado que dejó
	// dos p50 irreconciliables en el repo (ver el bloque de la ⑨ en servidor.go).
	prefill    histogramaInferencia
	generacion histogramaInferencia

	// porRegimen y porClase son los DOS REPARTOS acumulados del proceso (T1.7-5). Punteros porque el tipo
	// lleva un mapa que hay que construir (ver nuevoContador), al revés que los histogramas.
	//
	// ⚠️ VENTANAS DISTINTAS A LAS DE LOS HISTOGRAMAS, y está escrito también en el .proto: los cuantiles
	// son una foto de la ventana del emisor y estos son acumulados monótonos. Dividir uno por otro da un
	// número absurdo con muy buena pinta.
	porRegimen *contadorEtiquetado
	porClase   *contadorEtiquetado

	fallos           atomic.Int64
	rescatados       atomic.Int64
	aperturasBreaker atomic.Int64
	// lentas son las inferencias que ACERTARON pasando de umbralLento y por tanto castigaron al breaker
	// en vez de cerrarlo (MP-09). NO son fallos —la salida se sirvió al Cloud, y por eso no suma en
	// `fallos`— y son la única forma de distinguir en el log un circuito abierto por lentitud de uno
	// abierto por caída: con Ollama caído este contador es 0 y `fallos` sube; con Ollama lento suben los
	// dos.
	lentas atomic.Int64

	// ─────────────────────────────────────────────────────────────────────────
	// LOS CONTADORES DE LA INFERENCIA SERVIDA (T1.6-2 · INV-051.3)
	// ─────────────────────────────────────────────────────────────────────────
	//
	// 🔴 UNO POR DESENLACE Y NUNCA AGREGADOS. INV-051.3 lo exige, y el motivo es operativo: los cuatro
	// errores que este proceso puede producir piden intervenciones DISTINTAS —arrancar Ollama, esperar a
	// que el circuito cierre, mirar por qué el modelo tarda, o mirar el hardware— y un contador único de
	// «inferencias fallidas» no distingue ninguna de las cuatro.
	//
	// EL QUINTO ERROR DEL CONTRATO (LEASE_INVALID) NO ESTÁ AQUÍ, y su ausencia es correcta: lo produce el
	// DAEMON, que es quien tiene los Validator de lease, y lo cuenta allí (ver el carril,
	// internal/adapters/cloudlink/inferencia.go). Contarlo también aquí sería imposible —el cajero jamás
	// ve esas peticiones, se rechazan antes de llegar al socket— y tener el campo a cero para siempre
	// invitaría a leerlo como «nunca pasa».
	servidas atomic.Int64
	// abortadas son las inferencias que quien las pidió abandonó a mitad (SIGTERM del proceso, o el
	// daemon cerrando la conexión). NO es uno de los cinco del contrato y no sube por el cable: no hay a
	// quién responder. Existe porque sin él ese desenlace no dejaría rastro en NINGÚN contador — ni en
	// `servidas` ni en los cuatro errores— y un daemon que abortase sistemáticamente quemaría el LLM del
	// cliente de forma invisible.
	abortadas         atomic.Int64
	errOllamaCaido    atomic.Int64
	errBreakerAbierto atomic.Int64
	errTimeout        atomic.Int64
	errSinCapacidad   atomic.Int64
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
	// 🔴 EL PROVEEDOR NO ES OBLIGATORIO Y ANTES SÍ LO ERA (el `Clasificador`), y la inversión es la
	// consecuencia directa del ADR-0045: sin proveedor este proceso ya no se queda sin oficio. Sigue
	// barriendo leases vencidos y publicando su parte de salud —las dos cosas que el daemon de cada
	// instalación necesita de él—, y lo único que pierde es poder SERVIR inferencia, que es exactamente
	// lo que ServidorInferencia() reporta devolviendo nil. Negarse a arrancar aquí dejaría además sin
	// `intent_circuit` a todos los daemons de la máquina por una feature apagada.
	if deps.Ollama == nil {
		log := deps.Log
		if log == nil {
			log = sharedlogger.Default()
		}
		log.Warn("cajero: sin proveedor local de LLM (Deps.Ollama nil); este proceso NO servirá inferencia " +
			"(sigue barriendo leases y publicando su parte de salud)")
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
	temperatura := deps.Temperatura
	if temperatura <= 0 {
		temperatura = DefaultTemperatura
	}
	timeoutMax := deps.TimeoutMax
	if timeoutMax <= 0 {
		timeoutMax = DefaultMaxTimeoutMS * time.Millisecond
	}
	lease := deps.Lease
	if lease <= 0 {
		lease = app.DefaultColaLeaseSegundos * time.Second
	}
	numThread := deps.NumThread
	if numThread <= 0 {
		numThread = DefaultNumThread
	}

	// El default del keep_alive lo pone el MÓDULO DEL PROVEEDOR (ollama.DefaultKeepAliveSeconds) y no una
	// constante nuestra: quién decide cuánto vive el runner es una política del proveedor, y copiar aquí el
	// -1 lo dejaría caducar en silencio el día que esa recomendación cambie.
	keepAlive := deps.KeepAlive
	if keepAlive == nil {
		keepAlive = ollama.KeepAliveSeconds(ollama.DefaultKeepAliveSeconds)
	}

	return &Cajero{
		colas:       colas,
		ollama:      deps.Ollama,
		modelo:      deps.Modelo,
		temperatura: temperatura,
		opciones:    deps.Opciones,
		keepAlive:   keepAlive,
		porRegimen:  nuevoContador(RegimenesInferencia...),
		porClase:    nuevoContador(app.ClasesInferencia...),
		breaker:     br,
		despertador: desp,
		log:         log,
		ahora:       ahora,
		lease:       lease,
		timeout:     deps.Timeout,
		timeoutMax:  timeoutMax,
		statsEvery:  deps.StatsEvery,
		ollamaURL:   deps.OllamaURL,
		numThread:   numThread,
		aforo:       NuevoAforo(deps.MaxConcurrent),
	}, nil
}

// Run arranca el cajero y bloquea hasta que ctx se cancele. Devuelve nil en la parada ordenada: que el
// proceso termine porque le mandaron SIGTERM no es un fallo, y devolver error ahí haría que el
// supervisor lo tratara como caída.
//
// PARADA LIMPIA, y qué significa exactamente aquí: al cancelar el ctx el bucle sale de su espera y el
// barrido de leases se apaga con su ticker.
//
// 🔴 Run NO ESPERA A LAS INFERENCIAS EN VUELO, y desde T1.6-2 eso es correcto donde antes no lo era.
// Antes, un lote en vuelo tenía trabajo PENDIENTE en la BD (el UPDATE que cierra el lote), y perderlo
// significaba tirar una inferencia ya pagada; por eso había un WaitGroup. Hoy las inferencias las pide
// el Cloud y las sirve un servidor HTTP sobre el socket (ver servidor.go y el adaptador cajerosock): el
// dueño de esperar a que drenen es el `Shutdown` de ESE servidor, que es quien tiene las conexiones. Un
// WaitGroup aquí no vería ninguna de ellas y daría una falsa sensación de drenaje.
func (c *Cajero) Run(ctx context.Context) error {
	c.registrarAfinidad(ctx)
	c.log.Info("cajero: arrancando",
		// Las colas y el aforo van JUNTOS en la misma línea a propósito: son los dos números que un
		// operador confunde. `colas` es cuántas instalaciones atiende; `max_concurrent` es cuántas
		// inferencias caben A LA VEZ EN LA MÁQUINA, no por cola (ver Deps.Colas).
		"colas", len(c.colas),
		"colas_nombres", strings.Join(c.nombresDeColas(), ","),
		"max_concurrent", c.aforo.Plazas(),
		"modelo", c.modelo,
		"lease_s", int(c.lease.Seconds()),
		"inferencia_timeout_ms", c.timeout.Milliseconds(),
		"inferencia_timeout_max_ms", c.timeoutMax.Milliseconds(),
		// 🔴 «_default_» EN EL NOMBRE, Y NO ES COSMÉTICA (T1.7-2): desde esta tarea el umbral de lentitud NO
		// es del proceso sino DE CADA PETICIÓN —se deriva del plazo de ella, ver registrarAcierto—. Éste es
		// sólo el que le tocará a las que lleguen SIN plazo, y llamarlo `umbral_lento_ms` haría creer a quien
		// lea el arranque que todas se juzgan contra este número.
		"umbral_lento_default_ms", umbralLentoDe(c.timeout).Milliseconds(),
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
		// Los CINCO desenlaces de la inferencia servida, uno a uno (INV-051.3). Van los primeros porque
		// desde el ADR-0045 son el trabajo del proceso; lo de abajo es su salud.
		"servidas", c.Servidas(),
		"abortadas", c.Abortadas(),
		"err_ollama_caido", c.ErroresOllamaCaido(),
		"err_breaker_abierto", c.ErroresBreakerAbierto(),
		"err_timeout", c.ErroresTimeout(),
		"err_sin_capacidad", c.ErroresSinCapacidad(),
		"fallos", c.Fallos(),
		"lentas", c.Lentas(),
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

// bucle es el ciclo de vida del proceso. Sale cuando ctx se cancela.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 EL BUCLE YA NO CLASIFICA (Plan 044 · Ola 1.6 · T1.6-2 · ADR-0045 §8)
// ─────────────────────────────────────────────────────────────────────────────
// Hasta el 2026-08-24 esta función RECLAMABA: tomaba plaza del semáforo, se llevaba el lote de la
// conversación más vieja de la cola que tocara en el round-robin, lo clasificaba contra el LLM y
// escribía el intent de vuelta. El ADR-0045 §8 se lo quitó con una frase — el cajero «deja de clasificar
// por iniciativa propia» — y este bloque explica por qué esa frase no era una preferencia de diseño.
//
// LA MEDICIÓN QUE LO MATÓ: bajo el push, el cajero SIEMPRE PERDÍA LA CARRERA. El despachador entrega el
// entrante al instante y sella la fila como `despachado`; cuando el cajero terminaba su inferencia
// —2 a 36 s después— su fencing por ClaimToken ya no casaba y el cierre devolvía app.ErrLoteRelevado.
// Es decir: se pagaba la CPU de la inferencia entera, se ocupaba la única plaza de Ollama todo ese
// tiempo, y el resultado se tiraba. En la corrida de campo del 2026-08-23, de 430 inferencias UNA llegó
// a tiempo a su ventana; el intent no llegó JAMÁS a la nube. Clasificar por iniciativa propia no era
// «menos eficiente»: no producía absolutamente nada.
//
// LO QUE ESTE BUCLE CONSERVA, Y NO ES POCO — son las dos cosas que el ADR-0045 §8 le deja expresamente:
//
//   - EL PARTE DE SALUD (T4.5). Es el ÚNICO tubo por el que el circuito, el veredicto del taskset y el
//     p50 de inferencia llegan al daemon de cada instalación, que los publica en su heartbeat como
//     `intent_circuit`. Sin este latido, la nube deja de ver la salud del LLM de esa máquina — y ahora
//     importa MÁS que antes, porque es el mismo proceso que decide si una `inference_request` se sirve.
//   - EL BARRIDO DE LEASES (goroutine aparte, ver barrerLeases). Ya no rescata trabajo propio —nadie
//     reclama—, pero sí devuelve a `nuevo` las filas que quedaron en `tomado` en el disco de un cliente
//     que venía de un binario anterior. Es una limpieza de estado heredado y por eso se conserva:
//     retirarla es una decisión aparte, con su propia migración, y no es de esta tarea.
//
// LO QUE PIERDE, dicho para que nadie lo busque: el CURSOR del round-robin y `vaciasSeguidas`. Los dos
// existían para repartir EL CLAIM con equidad entre N instalaciones, y sin claim no hay nada que
// repartir — el parte se publica en TODAS las colas en cada tick (ver publicarParte) y el barrido las
// recorre todas dentro del mismo tick. La equidad de T4.1 sigue satisfecha por construcción, no por un
// cursor.
//
// QUIÉN CLASIFICA HOY: nadie por iniciativa propia. Cuando el Cloud necesita saber qué quiere un
// cliente, lo PIDE con un `inference_request` y este proceso lo SIRVE (ver servidor.go). Esa es la
// inversión entera del ADR-0045: de PUSH a PULL.
//
// 🔴 EL DESPERTADOR SIGUE SIENDO EL RELOJ DE ESTE BUCLE aunque ya no haya cola que sondear. Se conserva
// —y no se sustituye por un `time.Sleep`— porque es la costura inyectable con la que los tests avanzan
// el bucle sin esperas reales, y porque el poll marca la resolución con la que los dos latidos de abajo
// se atienden: un select no bloqueante necesita que ALGUIEN vuelva a pasar por él.
func (c *Cajero) bucle(ctx context.Context) {
	// Latido de contadores (T2.6): el cajero no tiene plano de control, así que sin esto los contadores
	// sólo se leen cuando el proceso muere, que es justo cuando ya no sirven. El select es NO BLOQUEANTE
	// y va al principio de la vuelta: el bucle itera al menos una vez por poll, así que la cadencia real
	// nunca se desvía más de un poll de la pedida. statsEvery <= 0 ⇒ canal nil, que en un select nunca
	// está listo (y así no hace falta un segundo camino de código para el caso «desactivado»).
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

	for {
		if ctx.Err() != nil {
			return
		}

		// Dos latidos y un `default`: el select sigue siendo NO BLOQUEANTE. Si los dos canales estuvieran
		// listos a la vez, Go elige uno al azar y el tick del otro QUEDA PENDIENTE en su ticker, así que se
		// atiende en la vuelta siguiente —que llega, como mucho, un poll después—. No se pierde ninguno.
		//
		// 🔴 EL PARTE SE PUBLICA CON EL CIRCUITO ABIERTO IGUAL QUE CON ÉL CERRADO, y eso es load-bearing:
		// el escenario que el parte existe para contar es precisamente «Ollama caído ⇒ breaker abierto».
		// Antes de T1.6-2 había aquí una guarda que interrumpía la vuelta con el circuito abierto y hubo
		// que ponerle el latido POR DELANTE para que el parte no se congelara justo entonces; ahora esa
		// guarda ya no existe (no hay claim que evitar) y el riesgo desaparece con ella.
		select {
		case <-latido:
			c.log.Info("cajero: contadores", c.contadores()...)
		case <-tParte.C:
			c.publicarParte(ctx)
		default:
		}

		if c.despertador.Esperar(ctx) != nil {
			return
		}
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
	c.anotarApertura(c.breaker.RecordFailure(), causaFallo)
}

// Las dos CAUSAS por las que el circuito puede abrirse. No son estados del breaker —que sigue teniendo
// tres y no se ha tocado— sino la enfermedad que lo abrió, y distinguirlas en el log es lo único que
// separa en campo un Ollama CAÍDO de uno LENTO: los dos dejan la misma línea de apertura y piden
// intervenciones distintas (arrancarlo vs. mirar quién le está comiendo la CPU).
const (
	// causaFallo: la inferencia no respondió (error de red, timeout propio o pánico del clasificador).
	causaFallo = "fallo"
	// causaLentitud: la inferencia SÍ respondió, por encima de umbralLento (MP-09).
	causaLentitud = "lentitud"
)

// registrarAcierto le cuenta al breaker una inferencia que RESPONDIÓ, y decide si esa respuesta es señal
// de salud o de enfermedad. Devuelve si fue LENTA, para que el llamante pueda decirlo en su log.
//
// 🔴 ES EL ARREGLO DEL MP-09, Y VIVE AQUÍ A PROPÓSITO. El paquete breaker declara en su cabecera que la
// semántica de «qué cuenta como fallo» SE SOSTIENE EN LOS LLAMANTES y no en él —ya hay un precedente
// vivo: un intent vacío o «desconocido» es un éxito porque el clasificador respondió bien—, así que
// añadir aquí una segunda excepción es extender un patrón que ya existía, no romper la migración
// intacta que exige REQ-051.12. El breaker no cambia ni una línea: sigue contando fallos consecutivos
// contra un umbral de 5 y abriendo 60 s. Lo que cambia es QUÉ le contamos.
//
// EL LOTE NO SE PIERDE NI SE DEGRADA. Una inferencia lenta se clasificó correctamente: su intent se
// escribe y el lote se cierra por el camino normal. Lo único que cambia es que el breaker aprende de
// ella. Por eso `lentas` es un contador aparte y NO suma en `fallos` —que gobierna el freno del lote
// venenoso (DefaultMaxIntentos)—: castigar los intentos de un lote por la lentitud de Ollama abandonaría
// mensajes ajenos al problema.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 EL UMBRAL ES DE LA PETICIÓN, NO DEL PROCESO (T1.7-2, Plan 044 · Ola 1.7)
// ─────────────────────────────────────────────────────────────────────────────
// `plazo` es el plazo EFECTIVO de ESTA inferencia —el que ya calculó `plazoDe` para ella, default y techo
// aplicados—, y el umbral se deriva de él en cada llamada. Antes se derivaba UNA vez en New, del default
// del proceso, y esa versión mentía en las dos direcciones desde que el ADR-0045 le dio el plazo al Cloud:
//
//   - MENTÍA POR ARRIBA, y es el defecto que cerró esta tarea: una petición interactiva con
//     `timeout_ms = 10 s` que tardaba 9,9 s se le contaba al circuito como ÉXITO —9,9 s < 36 s, el umbral
//     del default— cuando se había quedado a 100 ms de morir. El breaker no podía ver enferma la vía
//     estrecha por muy mal que fuese.
//   - MENTÍA POR ABAJO en la vía de lote: una petición con `timeout_ms = 90 s` que responde en 40 s va
//     holgadísima dentro de SU presupuesto, y el umbral del proceso la marcaba como lenta y castigaba al
//     circuito por ella.
//
// LA FÓRMULA NO CAMBIA (ADR-0042 §4: × FraccionLentitud). Cambia DE QUÉ PLAZO SE DERIVA, que es donde
// estaba el error: un umbral derivado hereda el error de su fuente sin dar ninguna señal, y con dos vías
// conviviendo (ADR-0044) no hay UN plazo del proceso que pueda representarlas a las dos.
func (c *Cajero) registrarAcierto(transcurrido, plazo time.Duration) bool {
	umbral := umbralLentoDe(plazo)
	if umbral <= 0 || transcurrido < umbral {
		c.breaker.RecordSuccess()
		return false
	}

	n := c.lentas.Add(1)
	abrio := c.breaker.RecordFailure()

	// 🔴 INV-051.1: sólo duraciones y cuentas. Ni texto, ni intención, ni chat.
	//
	// Warn y no Info: el lote salió bien, pero el clasificador está diciendo con todas las letras que va
	// camino de no salir. Es la línea que un operador quiere ver ANTES de la apertura, no después — y con
	// el semáforo en 1 aparece como mucho una vez cada `umbral_lento_ms`, así que no puede inundar el log.
	c.log.Warn("cajero: inferencia LENTA; respondió, pero se comió casi todo su plazo y cuenta como fallo "+
		"para el circuit breaker (el lote se cierra con su intent, no se pierde)",
		"latencia_ms", transcurrido.Milliseconds(),
		"umbral_lento_ms", umbral.Milliseconds(),
		// EL PLAZO DE ESTA PETICIÓN, no el default del proceso: es lo único que hace legible el `umbral_lento_ms`
		// de al lado. Con dos vías conviviendo (interactiva y lote, ADR-0044) el mismo `latencia_ms` es sano en
		// una y enfermo en la otra, y sin este número las dos líneas serían indistinguibles en el log.
		"plazo_ms", plazo.Milliseconds(),
		"lentas", n,
		"circuito", c.breaker.State(),
	)

	c.anotarApertura(abrio, causaLentitud)
	return true
}

// anotarApertura cuenta y anuncia la apertura del circuito cuando el fallo recién registrado fue el del
// FLANCO (no-abierto → abierto). El flanco lo decide el breaker dentro de su propio lock: aquí sólo se
// consume el bool, que es lo que hace que dos fallos concurrentes no puedan contar dos aperturas de la
// misma ni perderla.
func (c *Cajero) anotarApertura(abrio bool, causa string) {
	if !abrio {
		return
	}
	n := c.aperturasBreaker.Add(1)
	c.log.Warn("cajero: el circuit breaker del LLM local se ABRIÓ; las inferencias se rechazan con "+
		"BREAKER_OPEN sin llegar a intentarse",
		"causa", causa, "aperturas", n, "abierto_por_s", int(breaker.DefaultOpenFor.Seconds()))
}

// umbralLentoDe deriva el umbral de lentitud de UN plazo de inferencia (ver FraccionLentitud). Un plazo
// <= 0 devuelve 0, que APAGA el criterio: sin plazo no hay «demasiado tarde» que definir.
//
// 🔴 SIGUE SIENDO EL ÚNICO SITIO DONDE VIVE LA FÓRMULA, y por eso T1.7-2 no la movió: lo que cambió es
// QUIÉN la llama y CON QUÉ. Su llamante de producción es `registrarAcierto`, una vez por petición y con
// el plazo de ella; `Run` la usa además para poder anunciar en el arranque el umbral que le tocará a las
// peticiones que lleguen sin plazo (`umbral_lento_default_ms`), que es un dato informativo y no un
// criterio. Si algún día hace falta afinar la fracción, se afina aquí y las dos vías se mueven juntas.
//
// EL CERO SIGUE SIENDO ALCANZABLE, sólo que ahora exige las DOS cosas a la vez: que el Cloud no fije
// plazo Y que el Edge tenga Deps.Timeout desactivado (`plazoDe` devuelve el default cuando el pedido es
// 0). Es la lectura honesta: con un plazo real encima, «tardar demasiado» SÍ está definido, venga de
// donde venga el número.
func umbralLentoDe(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return 0
	}
	return time.Duration(float64(timeout) * FraccionLentitud)
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
		// ─── T1.7-5 · el reparto de la inferencia ──────────────────────────────
		//
		// 🔴 EL CUANTIL VIAJA ATADO A SU `n`, y no es cosmética: un p50 sobre n=3 es un MÁXIMO disfrazado, y
		// comparar cuantiles de n distinto ya fabricó aquí una conclusión falsa. El contrato lo impone en el
		// wire (InferenceLatency es UN mensaje con los dos campos, no dos campos sueltos) y este parte lo
		// respeta escribiendo siempre la pareja: el lector decide «no medible» mirando las muestras, jamás
		// el p50 —que vale 0 tanto sin muestras como con ellas si el reloj no avanzó—.
		PrefillP50ms:       c.prefill.p50MS(),
		PrefillMuestras:    int64(c.prefill.muestras()),
		GeneracionP50ms:    c.generacion.p50MS(),
		GeneracionMuestras: int64(c.generacion.muestras()),
		// Las FOTOS de los dos repartos. Se copian aquí (foto() devuelve copia) y el mismo mapa se escribe
		// en las N colas: es sólo lectura a partir de este punto, así que compartirlo entre los N UPSERT es
		// seguro y ahorra N copias de un mapa de tres claves.
		PorRegimen: c.porRegimen.foto(),
		PorClase:   c.porClase.foto(),
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

// Lentas son los ACIERTOS que se pasaron del umbral de lentitud y castigaron al breaker en vez de
// cerrarlo (MP-09). NO son fallos: sus lotes se clasificaron y se cerraron con su intent. Leído junto a
// Fallos distingue las dos enfermedades — Ollama caído sube `fallos` y deja esto en 0; Ollama lento sube
// los dos.
func (c *Cajero) Lentas() int64 { return c.lentas.Load() }

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

// ─────────────────────────────────────────────────────────────────────────────
// LOS CINCO DESENLACES DE LA INFERENCIA SERVIDA (T1.6-2 · INV-051.3)
// ─────────────────────────────────────────────────────────────────────────────
//
// Cinco accesores y NO uno que devuelva un struct, por el mismo criterio que el resto del bloque de
// contadores de este fichero: lo que los consume es el logger, que quiere pares clave/valor.

// Servidas son las inferencias que se sirvieron con salida (el camino feliz).
func (c *Cajero) Servidas() int64 { return c.servidas.Load() }

// Abortadas son las que quien las pidió abandonó a mitad. NO es uno de los cinco del contrato (ver el
// campo). Si este sube sin que suban los errores, el problema está AGUAS ARRIBA: el Edge estaba
// trabajando y nadie esperó el resultado.
func (c *Cajero) Abortadas() int64 { return c.abortadas.Load() }

// ErroresOllamaCaido son las inferencias que no se pudieron servir porque el proveedor local no
// respondió (INFERENCE_ERROR_OLLAMA_DOWN). Si este sube, el arreglo es arrancar Ollama.
func (c *Cajero) ErroresOllamaCaido() int64 { return c.errOllamaCaido.Load() }

// ErroresBreakerAbierto son las rechazadas SIN intentar por tener el circuito abierto
// (INFERENCE_ERROR_BREAKER_OPEN). Si este sube, mira `aperturas_breaker` y su `causa`: el arreglo
// depende de si el proveedor está caído o lento (ADR-0042).
func (c *Cajero) ErroresBreakerAbierto() int64 { return c.errBreakerAbierto.Load() }

// ErroresTimeout son las que agotaron su plazo CON EL PROVEEDOR TRABAJANDO (INFERENCE_ERROR_TIMEOUT).
// Si este sube, el modelo tarda más de lo que el Cloud está dispuesto a esperar.
func (c *Cajero) ErroresTimeout() int64 { return c.errTimeout.Load() }

// ErroresSinCapacidad son las que no consiguieron plaza del aforo dentro de su plazo
// (INFERENCE_ERROR_EDGE_SIN_CAPACIDAD). 🔴 NO ES UN TIMEOUT: nunca se llamó al modelo. Si este sube, el
// equipo va corto para el ritmo de peticiones que le está mandando el Cloud.
func (c *Cajero) ErroresSinCapacidad() int64 { return c.errSinCapacidad.Load() }

// Aforo devuelve el aforo del proceso. Existe para el log del arranque y los tests: no hay ningún camino
// de producción que deba tomar plaza fuera de servidor.go.
func (c *Cajero) Aforo() *Aforo { return c.aforo }
