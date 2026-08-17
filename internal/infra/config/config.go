// Package config define la configuracion del Edge Agent y su carga desde
// archivo YAML con overlay de variables de entorno (prefijo WAPP_AGENT_).
//
// Se apoya en github.com/EduGoGroup/wapp-shared/config para la lectura del YAML
// y el acceso tipado a variables de entorno.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/cajero"
	sharedconfig "github.com/EduGoGroup/wapp-shared/config"
)

// EnvPrefix es el prefijo aplicado a las variables de entorno del Edge Agent.
// Por ejemplo, la clave LOG_LEVEL se lee de la variable WAPP_AGENT_LOG_LEVEL.
const EnvPrefix = "WAPP_AGENT_"

// DefaultCloudLinkRuntimePort es el puerto por defecto del stream de runtime CloudLink (Connect, mTLS)
// con el que el enroll DERIVA el Endpoint tras enrolar (Plan 026 T3): topología de prod design §1
// (:8101 CloudLink / :8102 enroll). El proto de enroll no devuelve endpoint de runtime, así que se
// deriva del host del endpoint de enrolamiento con este puerto (configurable por RuntimePort).
const DefaultCloudLinkRuntimePort = "8101"

// DefaultCommandTimeoutSeconds es el deadline por operación por defecto del demux CloudLink (Plan 027 T1,
// H7): 30s, alineado con la descarga de media del gateway whatsmeow para no cortar por debajo de una
// operación legítima. Configurable por WAPP_AGENT_CLOUDLINK_COMMAND_TIMEOUT_SECONDS.
const DefaultCommandTimeoutSeconds = 30

// DefaultOutboxMaxEvents es el tope por defecto de eventos retenidos en el outbox durable del Edge (Plan
// 027 Ola 3 · T2, ADR-0003): al llenarse se aplica DROP-OLDEST (se descarta el más viejo, con log) en vez
// de crecer sin límite. 10 000 eventos es holgado para un nano-negocio y acota el tamaño del .db ante una
// caída larga de la nube. Configurable por WAPP_AGENT_OUTBOX_MAX_EVENTS.
const DefaultOutboxMaxEvents = 10000

// DefaultDiagLogLines es el número de líneas de log por defecto en el bundle de diagnóstico (Plan 031 T8).
// Configurable por WAPP_AGENT_DIAG_LOG_LINES. 500 da contexto reciente amplio sin acercarse al tope de tamaño.
const DefaultDiagLogLines = 500

// DefaultInboundMarginSeconds es el MARGEN por defecto de la ventana temporal de ingesta (ADR-0037):
// 300 s = 5 min. Se descarta todo entrante anterior a `inicioDeConexión − margen`. El número lo manda el
// DESFASE DE RELOJ, que whatsmeow mide (connectionevents.go:165) pero guarda privado (client.go:193): al
// no poder corregirlo, el margen tiene que absorberlo, y eso lo pone en minutos. Es además la ventana de
// rescate de la microcaída: lo enviado en los 5 min previos a reconectar se trata como vivo. Configurable
// por WAPP_AGENT_INBOUND_MARGIN_SECONDS; <=0 cae al default (guardarraíl: un margen cero descartaría
// tráfico vivo en cuanto el reloj local fuera un segundo por delante del servidor).
const DefaultInboundMarginSeconds = 300

// DefaultInboundStatsEveryMS es cada cuánto el daemon emite el bloque de LATENCIA DEL HANDLER DE ENTRANTES
// (Plan 051 Ola 3 · T3.13): el p50/p95/p99 del tiempo que onMessage pasa en el hilo de whatsmeow, que es
// lo que hace medible el criterio de cierre de la ola («handler < 50 ms p99», INV-051.2).
//
// 60 s, MÁS CORTO que los 5 min del cajero (DefaultWorkerStatsEveryMS), y no por gusto: la ventana de una
// prueba de campo se mide en minutos, y con 5 min una sesión de PC-11 entera cabría en dos líneas de log.
//
// Configurable por WAPP_AGENT_INBOUND_STATS_EVERY_MS — prefijo del AGENTE, no del worker: esto corre en
// `agent serve`, que es otro proceso con otro bloque de entorno.
//
// GUARDARRAÍL DISTINTO AL DE LOS DEMÁS NÚMEROS (calcado del cajero): sólo lo NEGATIVO cae a este default.
// El 0 es un valor legítimo y significa «cállate», no un dedazo. El bloque FINAL se emite igual.
const DefaultInboundStatsEveryMS = 60 * 1000

// DefaultOutboxTTLHours es el TTL por defecto (en horas) de un evento en el outbox: 0 = DESACTIVADO
// (durabilidad primero; el único recorte es el drop-oldest por tamaño). Un valor >0 poda los eventos más
// viejos que ese tiempo al encolar/drenar. Configurable por WAPP_AGENT_OUTBOX_TTL_HOURS.
const DefaultOutboxTTLHours = 0

// DefaultColaTTLHours es el TTL por defecto (en horas) de una fila en la COLA DE ENTRANTES (Plan 051,
// REQ-051.7). A diferencia del outbox, 0 aquí NO desactiva nada: la cola es un buzón de paso con poda
// agresiva (decisión cerrada del Plan 051) y un valor <=0 cae a este default. Configurable por
// WAPP_AGENT_COLA_TTL_HOURS.
const DefaultColaTTLHours = 24

// DefaultColaMaxRows es el tope por defecto de filas retenidas en la COLA DE ENTRANTES (Plan 051,
// REQ-051.7): al alcanzarlo se descartan las de menor `seq` (drop-oldest) en vez de crecer sin límite.
// Configurable por WAPP_AGENT_COLA_MAX_ROWS; un valor <=0 cae al default (guardarraíl).
const DefaultColaMaxRows = 50000

// Los dos defaults del lado CAJERO de la cola (Plan 051 Ola 2, T2.1/T2.7) se REEXPORTAN, no se declaran:
// el número vive UNA sola vez, en internal/app/cola.go, porque es parte del contrato de app.ColaCajero
// (lo que el puerto promete cuando el llamante pasa 0), no de la config ni del adaptador. Tenerlo
// tecleado también aquí era una segunda fuente de verdad sin nada que las atara: subir el tope en un
// sitio dejaba al otro mintiendo en silencio.
//
// Se mantienen los NOMBRES de config (…Seconds, no …Segundos) para no romper a quien ya los referencie;
// lo que cambia es de dónde sale el valor. La dirección del import es config → app y NO al revés:
// internal/app no importa nada de internal/infra (solo internal/domain e internal/infra/watchdog, que a
// su vez no importan nada interno), así que no hay ciclo — y no debe crearse: si algún día app necesitara
// leer configuración, se le pasa por parámetro.

// DefaultColaClaimMaxFilas es el TOPE DE FILAS que el worker-cajero se lleva en un solo claim (design §4):
// que una conversación gigante no monopolice al cajero. El porqué del 20, en app.DefaultColaClaimMaxFilas.
// Configurable por WAPP_AGENT_COLA_CLAIM_MAX_FILAS; un valor <=0 cae al default.
const DefaultColaClaimMaxFilas = app.DefaultColaClaimMaxFilas

// DefaultColaLeaseSeconds es el LEASE del claim en segundos: pasado ese tiempo, una fila `tomado` vuelve
// a `nuevo` porque se asume que el cajero que la reclamó murió a mitad. El margen es AMPLIO A PROPÓSITO y
// acortarlo empeora las cosas — el argumento completo (p95 de 3.736 ms, 16× por debajo) está en
// app.DefaultColaLeaseSegundos. Configurable por WAPP_AGENT_COLA_LEASE_SECONDS; <=0 cae al default.
const DefaultColaLeaseSeconds = app.DefaultColaLeaseSegundos

// ─────────────────────────────────────────────────────────────────────────────
// El WORKER-CAJERO (Plan 051 Ola 2) — prefijo WAPP_WORKER_, NO WAPP_AGENT_
// ─────────────────────────────────────────────────────────────────────────────
//
// WorkerEnvPrefix es el prefijo de las variables del worker-cajero. Es DISTINTO de EnvPrefix a
// propósito y no es un descuido: el design §4 y las tareas T2.3/T2.5 nombran literalmente
// `WAPP_WORKER_MAX_CONCURRENT`, `WAPP_WORKER_POLL_MS`, etc., porque el cajero es un PROCESO APARTE
// (el tercer hijo de `wapp-ctl`) y su unidad de systemd/LaunchAgent tiene su propio bloque de entorno.
// Meterlas bajo WAPP_AGENT_ las mezclaría con las del daemon, que es justo lo que la separación de
// procesos evita.
//
// Se leen con un SEGUNDO Loader (ver Load) en vez de con os.Getenv a pelo, para heredar el mismo
// comportamiento que el resto del fichero: un valor presente-pero-inválido no se traga en silencio,
// se avisa y se cae al default.
const WorkerEnvPrefix = "WAPP_WORKER_"

// DefaultWorkerMaxConcurrent son las inferencias SIMULTÁNEAS que el cajero permite. Es 1 y está
// CERRADO por la medición de la O0 en el VPS AMD real (ADR-0038 Enmienda 1 §(d)): con una sola
// instancia de Ollama, dos inferencias a la vez se pisan los hilos. El design §4 y el ADR-0038 §3
// decían «2» y quedan corregidos. Configurable por WAPP_WORKER_MAX_CONCURRENT; <=0 cae al default.
const DefaultWorkerMaxConcurrent = 1

// DefaultWorkerPollMS es cada cuánto (ms) el cajero vuelve a mirar la cola cuando está vacía. 500 ms
// es <15 % del coste de la inferencia que viene después (p95 medida: 3.736 ms) y deja el proceso a
// ~2 consultas/s contra un SQLite local. La ALTERNATIVA —`PRAGMA data_version`— sigue SIN DECIDIR y
// exige medición con el worker vivo delante (T2.7, segunda mitad). Configurable por
// WAPP_WORKER_POLL_MS; <=0 cae al default.
const DefaultWorkerPollMS = 500

// DefaultWorkerInferenceTimeoutMS es el plazo de UNA inferencia del worker-cajero.
//
// 🔴 ES UN NÚMERO DISTINTO DE DefaultIntentWaitMS (4000 ms) Y TIENE QUE SERLO. Aquel es el
// presupuesto del DESPACHADOR —cuánto espera a que aparezca el intent antes de entregar el mensaje sin
// él— y es corto porque su trabajo es NO RETENER la entrega: pasado el plazo, el mensaje sale sin
// intent y la sesión sigue viva. El worker existe justo para lo contrario: se sacó la inferencia a otro
// proceso PARA PODER TARDAR sin comerse el proceso que mantiene los sockets.
//
// Heredar aquí los 4 s del despachador lo calibraba PARA FALLAR. La O0 midió p50 = 2.613 ms y p95 = 3.736 ms sobre el
// VPS real (docs/plans/051-worker-cajero-edge/O0-resultados-2026-08-09.md), o sea que 3 s queda POR
// DEBAJO de la p95: ~20-40 % de las inferencias abortarían, cada aborto es un fallo, 5 seguidos abren
// el breaker 60 s, las filas se quedan en `tomado`, el barrido las devuelve a `nuevo`, se re-reclaman
// y vuelven a expirar. Un bucle que no progresa hasta que el tope de la cola las descarta — y sin un
// solo error que lo delate.
//
// 15 s son ≈4× la p95, la misma clase de holgura con la que se eligió el lease de 60 s (≈16×).
// Configurable por WAPP_WORKER_INFERENCE_TIMEOUT_MS; <=0 cae al default.
const DefaultWorkerInferenceTimeoutMS = cajero.DefaultInferenceTimeoutMS

// DefaultWorkerMaxIntentos es cuántas veces se RECLAMA un lote antes de que el cajero lo abandone con el
// sobre `{"omitido":"fallo_repetido"}` (T2.19). Es 3: dos reintentos gratis y a la tercera se cierra.
//
// 🔴 EXISTE PORQUE UN SOLO LOTE VENENOSO CONGELABA LA COLA ENTERA. Un fallo de inferencia no cierra el
// lote a propósito (reintento gratis: las filas se quedan en `tomado` y el barrido las devuelve a `nuevo`),
// pero el barrido NO toca el `seq` y el claim elige siempre la conversación de `seq` más bajo — así que un
// lote cuyo texto siempre falla se lo vuelve a llevar el claim siguiente, y el siguiente, para siempre. Con
// WAPP_WORKER_MAX_CONCURRENT=1 (el default) ningún otro mensaje se clasifica jamás, y el único síntoma es
// el contador de fallos subiendo.
//
// EL PORQUÉ DEL 3: dos reintentos —hasta ~2·lease, o sea unos 2 minutos con los defaults— cubren de sobra
// un Ollama que se reinicia o un pico de carga, que es la clase de fallo que se arregla sola y merece el
// reintento. Un tercer fallo consecutivo del MISMO lote ya no se explica por el azar: es un patrón, y a
// partir de ahí cada reintento es una inferencia pagada que no va a funcionar mientras bloquea a los demás.
// Configurable por WAPP_WORKER_MAX_INTENTOS; <=0 cae al default (un 0 significaría abandonar cada lote en
// su primer claim, sin llegar a clasificar nada).
const DefaultWorkerMaxIntentos = cajero.DefaultMaxIntentos

// DefaultWorkerStatsEveryMS es cada cuánto el bucle del cajero emite el bloque COMPLETO de contadores
// en un Info. El cajero NO tiene plano de control (deliberado: no se levanta un socket /v1 para
// exponer seis contadores), así que sin este latido los contadores sólo son legibles cuando el proceso
// muere. La publicación al HEARTBEAT —la que los saca de la máquina— es de la OLA 4.
//
// 🔴 SU GUARDARRAÍL ES DISTINTO DEL DE LOS DEMÁS: aquí el CERO ES VÁLIDO y significa DESACTIVADO (un
// operador con el log a nivel info y muchos tenants puede querer callarlo). Sólo un valor NEGATIVO cae
// al default. Configurable por WAPP_WORKER_STATS_EVERY_MS.
const DefaultWorkerStatsEveryMS = 5 * 60 * 1000

// Los CUATRO números del modelo (techo de entrada, hilos, contexto y tokens de salida) se REEXPORTAN,
// NO se teclean aquí: el número tiene un dueño y tenerlo escrito dos veces es la duplicación
// silenciosa de siempre — alguien sube el techo en un sitio y el otro sigue mintiendo.
//
// SE REEXPORTAN DESDE internal/app/cajero, NO desde el módulo del clasificador. El origen último sigue
// siendo `classifier` (que los exporta EXPRESAMENTE para este consumidor: «el caller (el worker cajero
// del Edge) las necesita para su config y NO debe duplicar los números a ciegas», classifier.go:22-23),
// pero el escalón intermedio es el paquete del WORKER, que es quien los usa por derecho propio.
// Es el mismo criterio que arriba con los dos defaults del lado cajero (internal/app/cola.go): el
// default de un puerto vive en el puerto, no en la infraestructura que lo configura. Importar
// `classifier` desde aquí sólo para reexportar cuatro enteros metía además ollama→net/http en el grafo
// de todo el que importa `config`, wapp-ctl incluido.
//
// Lo que config aporta encima es (a) el nombre de la variable de entorno, (b) el guardarraíl del <=0
// y (c) el punto donde el operador los ve juntos. El porqué de cada valor está en el doc comment de la
// constante de origen y no se repite aquí.

// DefaultWorkerMaxRunes es el TECHO de runas del texto que se manda a clasificar (T2.5): sin techo,
// pegar ~65 KB en un chat basta para que la inferencia tarde lo bastante como para contar fallos y
// ABRIR EL BREAKER COMPARTIDO, apagando la clasificación de todas las sesiones. Es la vía más barata
// que existe hoy para denegar el servicio del clasificador desde fuera. El porqué del número —hoy
// 1000 runas, bajado desde 4000 con medición contra Ollama real— vive en classifier.DefaultMaxRunes:
// NO se repite aquí precisamente para que no vuelva a envejecer, que ya pasó una vez. Configurable por WAPP_WORKER_MAX_RUNES; <=0 cae al default.
const DefaultWorkerMaxRunes = cajero.DefaultMaxRunes

// DefaultWorkerNumThread son los hilos de inferencia que se le piden a Ollama. Lo fijó la medición de
// la O0 sobre el VPS AMD real (el ADR-0038 §3 y el design §4 decían «2» y quedaron corregidos); no se
// re-discute sin medición. Configurable por WAPP_WORKER_NUM_THREAD; <=0 cae al default.
const DefaultWorkerNumThread = cajero.DefaultNumThread

// DefaultWorkerNumPredict es el tope de tokens que el modelo puede GENERAR en una clasificación: acota
// una generación desbocada (un modelo pequeño que entra en bucle ante una entrada degenerada no puede
// quemar tiempo sin límite). El porqué del número, en classifier.DefaultNumPredict. Configurable por
// WAPP_WORKER_NUM_PREDICT; <=0 cae al default.
const DefaultWorkerNumPredict = cajero.DefaultNumPredict

// DefaultWorkerNumCtx es la ventana de contexto (tokens) que se le pide a Ollama. Se manda EXPLÍCITA
// para no depender del default del build de Ollama ni del Modelfile del modelo, que cambian entre
// versiones, y hace de segundo techo si el de runas fallara. Subirla cuesta RAM permanente en la caja
// del cliente (la KV cache crece lineal con num_ctx), así que no se sube «por si acaso»: el argumento
// completo está en classifier.DefaultNumCtx. Configurable por WAPP_WORKER_NUM_CTX; <=0 cae al default.
const DefaultWorkerNumCtx = cajero.DefaultNumCtx

// WorkerConfig agrupa los parámetros del worker-cajero (Plan 051 Ola 2). Se leen del bloque `worker:`
// del YAML y del entorno con prefijo WAPP_WORKER_ (ver WorkerEnvPrefix).
type WorkerConfig struct {
	// MaxConcurrent son las plazas del semáforo de inferencias (T2.3). Default 1, CERRADO por la O0.
	MaxConcurrent int `yaml:"max_concurrent"`
	// PollMS es el intervalo del poll cuando la cola está vacía (T2.7, primera opción). Default 500.
	PollMS int `yaml:"poll_ms"`
	// MaxRunes es el techo de runas de la entrada a clasificar (T2.5). El default lo fija
	// classifier.DefaultMaxRunes (hoy 1000), no un literal de este fichero.
	MaxRunes int `yaml:"max_runes"`
	// NumThread son los hilos que se le piden a Ollama por inferencia (T2.5). Default 5.
	NumThread int `yaml:"num_thread"`
	// NumPredict es el tope de tokens generados (T2.5). Default DefaultWorkerNumPredict (256).
	NumPredict int `yaml:"num_predict"`
	// NumCtx es la ventana de contexto en tokens (T2.5). Default 4096.
	NumCtx int `yaml:"num_ctx"`
	// MaxIntentos es cuántos reclamos aguanta un lote antes de abandonarlo con `fallo_repetido` (T2.19).
	// Default DefaultWorkerMaxIntentos (3). Es el freno del lote venenoso: ver la constante.
	MaxIntentos int `yaml:"max_intentos"`
	// InferenceTimeoutMS acota UNA inferencia del worker. Default DefaultWorkerInferenceTimeoutMS
	// (15000). NO es Intent.WaitMS: ver el doc comment de la constante, el porqué está ahí entero.
	InferenceTimeoutMS int `yaml:"inference_timeout_ms"`
	// StatsEveryMS es la cadencia del latido de contadores en el log del cajero. Default
	// DefaultWorkerStatsEveryMS (300000 = 5 min). 0 lo DESACTIVA (guardarraíl distinto: sólo lo
	// negativo cae al default).
	StatsEveryMS int `yaml:"stats_every_ms"`
}

// DefaultPlatformAPIBaseURL es la URL base por defecto de la API PÚBLICA HTTP de la plataforma cloud
// (wapp-cloud-platform, puerto público 8103, rutas /api/v1/...): NO confundir con CloudLink (gRPC/mTLS,
// 8101/8102). La usa wapp-ctl para hablar DIRECTO con la nube en rutas que el núcleo no relaya (p.ej.
// POST /api/v1/signup, C-03/T3.5) — mismo default que WAPP_GUARDIAN_BFF usa para PUBLIC_API_BASE.
// Configurable por WAPP_AGENT_PLATFORM_API_BASE_URL.
const DefaultPlatformAPIBaseURL = "http://localhost:8103"

// runtimeEndpointStateFile es el nombre del archivo de estado bajo data_dir donde el enroll persiste el
// Endpoint de runtime derivado (Plan 026 T3, cierra follow-up 023) para que `serve` lo relea sin edición
// manual del config.yaml. Material PÚBLICO (host:puerto), nunca secretos.
const runtimeEndpointStateFile = "cloudlink-endpoint"

// RuntimeEndpointStatePath devuelve la ruta ESTABLE del archivo de estado del Endpoint de runtime dentro
// del data_dir (Plan 026 T3): <data_dir>/cloudlink-endpoint. El enroll lo ESCRIBE tras derivar el endpoint
// y config.Load lo LEE como fallback cuando el Endpoint no viene por YAML/env. Ambos (enroll vía wapp-ctl y
// el hijo `agent serve`) resuelven el MISMO data_dir (env compartido o defaultDataDir por SO), así que la
// ruta coincide entre escritura y lectura sin depender del CWD.
func RuntimeEndpointStatePath(dataDir string) string {
	return filepath.Join(dataDir, runtimeEndpointStateFile)
}

// Config agrupa los parametros minimos de arranque del Edge Agent.
type Config struct {
	// LogLevel es el nivel minimo de logging: debug, info, warn o error.
	LogLevel string `yaml:"log_level"`
	// LogJSON selecciona el formato JSON del logger cuando es true.
	LogJSON bool `yaml:"log_json"`
	// DBPath es la ruta del store SQLite cifrado del cryptostore.
	//
	// LEGACY (Plan 008): es la ruta PLANA single-sesión heredada. El modelo multi-sesión deriva el
	// store por sesión de DataDir (sessions/<id>/store.db, ADR-0016 §4); DBPath se conserva solo como
	// referencia del estado viejo para la migración clean-slate de arranque (edgemigrate).
	DBPath string `yaml:"db_path"`
	// DEKPath es la ruta del material relacionado con la DEK custodiada localmente.
	//
	// LEGACY (Plan 008): ruta PLANA single-sesión heredada. El modelo multi-sesión deriva la DEK por
	// sesión de DataDir (sessions/<id>/dek.key); se conserva solo para la migración clean-slate.
	DEKPath string `yaml:"dek_path"`
	// DBDialect selecciona el motor SQL de la BD del Edge (Plan 022 T0, design §5): "sqlite" (default,
	// embebido pure-Go, ADR-0002) o "postgres" (solo por cadena y solo si el binario se compiló con el
	// build-tag `postgres`; nunca bundleado en el Edge). Es la base del dialecto CONMUTABLE: T1 cablea
	// este valor a la apertura de la BD única. Se lee de WAPP_AGENT_DB_DIALECT.
	DBDialect string `yaml:"db_dialect"`
	// DBDSN es la cadena de conexión de la BD única cuando el dialecto la requiere (Postgres:
	// "postgres://user:pass@host:5432/db?sslmode=..."). En SQLite queda vacío: la ruta del fichero la
	// deriva el layout desde DataDir. Se lee de WAPP_AGENT_DB_DSN.
	DBDSN string `yaml:"db_dsn"`
	// DataDir es el directorio base del Edge (ADR-0016 §4): aloja el layout multi-sesión
	// (<data_dir>/sessions/<session_id>/{store.db,dek.key}), la BD de metadatos y el socket de control.
	// El Layout (internal/app/sessionmgr) deriva de aquí todas las rutas por sesión; nadie las arma a
	// mano.
	//
	// RUTA SAGRADA (MP-02, D1/D2): el default deja de ser "." (CWD) — que sembraba árboles sessions/
	// distintos según desde dónde se arrancara y forzaba re-emparejar. Ahora el default es una carpeta
	// de datos del usuario POR SO, SIEMPRE en el home y sin permisos de sistema (ver defaultDataDir), y
	// tras Load se ANCLA a ruta absoluta (filepath.Abs) una sola vez, venga del default, del YAML o del
	// override WAPP_AGENT_DATA_DIR. Así el store vive siempre en el mismo sitio, independiente del CWD.
	DataDir string `yaml:"data_dir"`
	// MaxSessions es el límite SUAVE de sesiones simultáneas (guardarraíl de RAM/sockets, design §10.G).
	// NO es un invariante de seguridad: un POST /pair por encima del límite responde error claro, no
	// crash. Se lee de WAPP_AGENT_MAX_SESSIONS (default 5).
	MaxSessions int `yaml:"max_sessions"`
	// MultiDevicePerAccount es el número de DISPOSITIVOS VIVOS que se permiten por CUENTA (número), base
	// del failover multi-dispositivo por número (Plan 022 T5, design §6/§10.F). Default 1 (comportamiento
	// actual: un device operativo por número). Con >1 el Manager permite N devices vivos del mismo número
	// (1 primary + standbys); el standby se promueve si el primary cae/expira (LoggedOut). Tope 4 (límite
	// de WhatsApp: 1 principal + 4 vinculados); se CLAMP a [1,4] al cargar.
	//
	// CAVEAT (requisito del plan): multi-device es RESILIENCIA, NO SIGILO. Más dispositivos NO reducen el
	// riesgo de baneo — al contrario, más companions = más huella. Por eso va OFF por defecto (1) y no se
	// debe incentivar agotar los 4 slots. Se lee de WAPP_AGENT_MULTIDEVICE_PER_ACCOUNT.
	MultiDevicePerAccount int `yaml:"multidevice_per_account"`
	// PushName es el nombre visible (push name) que se ANUNCIA en la presencia (SendPresence available,
	// Plan 013 §10.D) cuando el store restaurado aún no conoce el nombre REAL de la cuenta. whatsmeow
	// rechaza SendPresence sin PushName ("can't send presence without PushName set"), y sin presencia
	// available WhatsApp no propaga los acuses (delivered/read) al companion. FALLBACK, no override: el
	// gateway solo lo usa si Store.PushName viene vacío (store recién restaurado); si whatsmeow ya conoce
	// el nombre real de la cuenta (app-state), ese PREVALECE. No es secreto (no lleva PII/número). Se lee
	// de WAPP_AGENT_PUSH_NAME (default "wApp"). Para el e2e conviene fijar el nombre real de la cuenta.
	PushName string `yaml:"push_name"`
	// ControlSocketPath es la ruta del Unix domain socket donde el núcleo expone el contrato /v1 del
	// plano de control (ADR-0015): co-ubicado, SIN puerto de red. Default relativo al cwd, junto al
	// db_path (ver defaults). Override por WAPP_AGENT_CONTROL_SOCKET_PATH (mismo overlay que el resto).
	ControlSocketPath string `yaml:"control_socket_path"`
	// OutboxMaxEvents es el tope de eventos del outbox durable (Plan 027 Ola 3 · T2, ADR-0003): al llenarse
	// se descarta el más viejo (drop-oldest) con log, en vez de crecer sin límite. Default 10 000. Se lee de
	// WAPP_AGENT_OUTBOX_MAX_EVENTS; un valor <=0 cae al default (guardarraíl, no invariante).
	OutboxMaxEvents int `yaml:"outbox_max_events"`
	// OutboxTTLHours es el TTL (horas) de un evento del outbox: 0 = desactivado (solo recorta el drop-oldest
	// por tamaño). Con >0 se podan los eventos más viejos que ese tiempo. Se lee de WAPP_AGENT_OUTBOX_TTL_HOURS.
	OutboxTTLHours int `yaml:"outbox_ttl_hours"`
	// ColaTTLHours es el TTL (horas) de una fila de la COLA DE ENTRANTES (Plan 051, REQ-051.7). Default 24;
	// OJO: aquí 0 NO desactiva el TTL como en el outbox (la cola es un buzón de paso, no un archivo), sino
	// que cae al default. Se lee de WAPP_AGENT_COLA_TTL_HOURS.
	ColaTTLHours int `yaml:"cola_ttl_hours"`
	// ColaMaxRows es el tope de filas de la COLA DE ENTRANTES: al llenarse se descartan las más viejas
	// (drop-oldest). Default 50 000. Se lee de WAPP_AGENT_COLA_MAX_ROWS; un valor <=0 cae al default.
	ColaMaxRows int `yaml:"cola_max_rows"`
	// ColaClaimMaxFilas es el tope de filas que el worker-cajero se lleva en UN claim (Plan 051 Ola 2,
	// T2.1): que una conversación gigante no monopolice al cajero. Default 20. Se lee de
	// WAPP_AGENT_COLA_CLAIM_MAX_FILAS; un valor <=0 cae al default.
	//
	// CABLEADA desde T2.2: la consume `agent cajero` (cmd/agent/cajero.go) como cajero.Deps.MaxFilas.
	ColaClaimMaxFilas int `yaml:"cola_claim_max_filas"`
	// ColaLeaseSeconds es el lease del claim en segundos (Plan 051 Ola 2, T2.7): una fila `tomado` más
	// vieja que esto vuelve a `nuevo`. Default 60, margen amplio A PROPÓSITO (ver
	// DefaultColaLeaseSeconds). Se lee de WAPP_AGENT_COLA_LEASE_SECONDS; un valor <=0 cae al default.
	//
	// CABLEADA desde T2.2: `agent cajero` la usa a la vez como lease del claim y como PERIODO del
	// barrido de leases vencidos (cajero.Deps.Lease), de modo que la espera de rescate queda acotada a
	// [lease, 2·lease] sin un segundo número que mantener sincronizado.
	ColaLeaseSeconds int `yaml:"cola_lease_seconds"`
	// DiagLogLines es cuántas líneas del ring buffer de logs incluye el bundle de diagnóstico bajo demanda
	// (Plan 031 T8, ADR-0023). Default 500 (contexto reciente amplio sin acercarse al tope de 4 MiB del
	// frame). Se lee de WAPP_AGENT_DIAG_LOG_LINES; <=0 cae al default.
	DiagLogLines int `yaml:"diag_log_lines"`
	// InboundMarginSeconds es el margen de la ventana temporal de ingesta (ADR-0037): se descarta lo
	// anterior a `inicioDeConexión − margen`, y eso NO sube a la nube. Default 300 (5 min). Se lee de
	// WAPP_AGENT_INBOUND_MARGIN_SECONDS; <=0 cae al default (guardarraíl, no invariante).
	InboundMarginSeconds int `yaml:"inbound_margin_seconds"`
	// InboundStatsEveryMS es la cadencia del bloque de latencia del handler de entrantes en el log del
	// daemon (Plan 051 Ola 3 · T3.13). Default DefaultInboundStatsEveryMS (60000 = 1 min). Se lee de
	// WAPP_AGENT_INBOUND_STATS_EVERY_MS. 0 lo DESACTIVA (guardarraíl distinto: sólo lo negativo cae al
	// default); el bloque final del apagado se emite igual.
	//
	// Durante una sesión de PC-11 se baja a 10000 para no depender de que el tick caiga en el momento
	// bueno. NO se deja así: los logs del VPS van a un fichero y esa cadencia lo engorda.
	InboundStatsEveryMS int `yaml:"inbound_stats_every_ms"`
	// CloudLink configura el conducto edge<->cloud (pieza 02). Si Endpoint está vacío, el Edge usa
	// SOLO el LogSink (diagnóstico, sin red): no rompe los flujos pair/send/listen del spike.
	CloudLink CloudLinkConfig `yaml:"cloudlink"`
	// Intent configura el CLASIFICADOR LLM local de intenciones (Plan 029, ADR-0020). OFF por defecto: con
	// Enabled=false el cableado del sink es idéntico byte a byte al previo (sin decorador). Con Enabled=true
	// el Edge envuelve el sink de entrada con el clasificador (Ollama local) y persiste/aplica la config de
	// intenciones que empuja el Cloud.
	Intent IntentConfig `yaml:"intent"`
	// Worker configura el WORKER-CAJERO (Plan 051 Ola 2), el tercer hijo de `wapp-ctl`. OJO: sus
	// variables de entorno NO llevan el prefijo WAPP_AGENT_ sino WAPP_WORKER_ (ver WorkerEnvPrefix);
	// el bloque YAML sí es `worker:` dentro del mismo config.yaml, porque el fichero es compartido.
	Worker WorkerConfig `yaml:"worker"`
	// EnableAlphaTestAccounts activa el selector de usuarios de prueba (Alpha) en la UI de login. Default: false.
	EnableAlphaTestAccounts bool `yaml:"enable_alpha_test_accounts"`
	// PlatformAPIBaseURL es la URL base de la API PÚBLICA HTTP de la plataforma cloud (puerto 8103,
	// /api/v1/...), usada por wapp-ctl para llamadas directas que el núcleo no relaya por el socket local
	// (p.ej. el signup público, C-03/T3.5). Distinta de CloudLink.* (gRPC/mTLS 8101/8102). Se lee de
	// WAPP_AGENT_PLATFORM_API_BASE_URL.
	PlatformAPIBaseURL string `yaml:"platform_api_base_url"`
}

// DefaultIntentOllamaURL es la URL por defecto del Ollama local (loopback): el LLM corre en el MISMO equipo
// del Edge (zero-knowledge / sin dependencia de red externa para clasificar). Configurable por
// WAPP_AGENT_INTENT_OLLAMA_URL.
const DefaultIntentOllamaURL = "http://127.0.0.1:11434"

// DefaultIntentModel es el modelo por defecto del clasificador (pequeño, apto para CPU de escritorio).
// Configurable por WAPP_AGENT_INTENT_MODEL.
const DefaultIntentModel = "qwen3:1.7b"

// DefaultIntentWaitMS es el PRESUPUESTO DE ESPERA por defecto (ms) del DESPACHADOR (Plan 051 Ola 3):
// cuánto aguanta la entrega de un mensaje esperando a que el worker-cajero deje su intent en la cola
// antes de despacharlo SIN intención. Configurable por WAPP_AGENT_INTENT_WAIT_MS.
//
// 🔴 NO ES EL TIMEOUT DE INFERENCIA, y son dos números que NO se colapsan. El de inferencia es
// WAPP_WORKER_INFERENCE_TIMEOUT_MS (15000, ver DefaultWorkerInferenceTimeoutMS) y mide «cuánto aguanto a Ollama»;
// éste mide «cuánto retengo la entrega». Que el segundo sea MENOR que el primero es lo normal y no es
// una incoherencia: el despachador se rinde antes de que la inferencia termine, entrega sin intent, y
// la clasificación tardía sigue su curso en la cola sin retener a nadie.
const DefaultIntentWaitMS = 4000

// IntentConfig agrupa los parámetros del clasificador de intenciones (Plan 029). Todo OFF por defecto.
type IntentConfig struct {
	// Enabled activa el clasificador. false (default) ⇒ el sink no se decora (cableado idéntico al actual).
	Enabled bool `yaml:"enabled"`
	// OllamaURL es la URL del servidor Ollama local (default loopback :11434).
	OllamaURL string `yaml:"ollama_url"`
	// Model es el modelo de clasificación (default qwen3:1.7b).
	Model string `yaml:"model"`
	// WaitMS es el PRESUPUESTO DE ESPERA DEL DESPACHADOR en milisegundos (default 4000): cuánto retiene
	// la entrega de un mensaje aguardando a que aparezca su intent, antes de despacharlo sin él.
	// <=0 cae al default.
	//
	// 🔴 NO ES EL TIMEOUT DE INFERENCIA. El timeout de inferencia es WAPP_WORKER_INFERENCE_TIMEOUT_MS
	// (15000), que vive en Worker.InferenceTimeoutMS. SON DOS NÚMEROS DISTINTOS Y NO SE COLAPSAN: éste
	// acota lo que espera el que ENTREGA; aquél acota lo que tarda el que CLASIFICA.
	//
	// Sustituye a la retirada WAPP_AGENT_INTENT_TIMEOUT_MS (Plan 051 Ola 3, T3.1): ver VariablesRetiradas.
	WaitMS int `yaml:"wait_ms"`
}

// CloudLinkConfig agrupa los parámetros del conducto CloudLink. Todos OPCIONALES: con Endpoint vacío
// no se conecta a la nube (LogSink puro). El material cripto (cert/clave) vive fuera de git (.gitignore).
type CloudLinkConfig struct {
	// Endpoint es la dirección gRPC de la plataforma cloud (p.ej. "cloud.wapp.example:8101"). Vacío
	// desactiva el conducto real.
	Endpoint string `yaml:"endpoint"`
	// SessionID identifica la sesión/teléfono dentro del Edge (multiplexado, ADR-0008).
	SessionID string `yaml:"session_id"`
	// TLSCert/TLSKey/TLSCA son las rutas del cert de cliente del Edge y la CA (mTLS, ADR-0006). Si las
	// tres están presentes se usa mTLS; si no, el dial va insecure (solo dev; se loguea advertencia).
	TLSCert string `yaml:"tls_cert"`
	TLSKey  string `yaml:"tls_key"`
	TLSCA   string `yaml:"tls_ca"`
	// ServerName es el SAN esperado en el cert del servidor (mTLS). Por defecto se deriva del Endpoint.
	ServerName string `yaml:"server_name"`
	// LeasePubKeyPath es la ruta a la clave pública Ed25519 del emisor de leases (servidor). Si está
	// presente, se activa el gate de lease (kill-switch); si no, no se gatea (dev).
	LeasePubKeyPath string `yaml:"lease_pubkey_path"`
	// LeaseShadowMode activa el modo SOMBRA del gate de lease (D-055.4, Plan 055): con el validator
	// presente (LeasePubKeyPath configurado), un lease no vigente se REGISTRA (WARN) pero el envío se
	// despacha igual — no bloquea. Es una decisión DISTINTA de "¿hay validator?" (esa la gobierna
	// LeasePubKeyPath): esta decide qué hace el gate con el resultado de CanOperate una vez que SÍ hay
	// validator. Por defecto false (fail-closed real, el comportamiento de siempre); se enciende
	// mientras se corre el gate en campo sin haberlo visto bloquear nunca (README §8.4, 72h en las tres
	// máquinas). Se lee de WAPP_AGENT_CLOUDLINK_LEASE_SHADOW_MODE.
	LeaseShadowMode bool `yaml:"lease_shadow_mode"`
	// CloudEncPubKeyPath es la ruta a la clave pública X25519 (32B) de CIFRADO de la nube (Plan 011
	// §6.3/§6.4). Se puebla desde el enrolamiento (EnrollEdgeResponse.cloud_enc_pubkey). Si está
	// presente, el Edge SELLA los campos sensibles del entrante hacia esta pública (SealFor) antes de
	// reenviarlos; si no, va el fallback claro (§10.H). Persistida en base64 (una línea).
	CloudEncPubKeyPath string `yaml:"cloud_enc_pubkey_path"`
	// EnrollmentEndpoint es la dirección gRPC del servidor de enrolamiento del Gateway (subcomando
	// `enroll`). En dev suele ser un puerto distinto al de Connect (p.ej. "localhost:8102"). El dial de
	// enrolamiento usa TLS-de-servidor (NO mTLS): valida al Gateway con TLSCA. Vacío desactiva `enroll`.
	EnrollmentEndpoint string `yaml:"enrollment_endpoint"`
	// RuntimePort es el PUERTO del stream de runtime CloudLink (Connect, mTLS) con el que se DERIVA el
	// Endpoint tras enrolar (Plan 026 T3, cierra follow-up 023): host(EnrollmentEndpoint) + ":" +
	// RuntimePort. Por defecto "8101" (topología de prod, design §1: :8101 CloudLink / :8102 enroll). El
	// proto de enroll (EnrollEdgeResponse) NO devuelve un endpoint de runtime, así que se deriva del host
	// del endpoint de enrolamiento manteniendo el contrato intacto. Se lee de WAPP_AGENT_CLOUDLINK_RUNTIME_PORT.
	RuntimePort string `yaml:"runtime_port"`
	// ActivationCode es el código de activación emitido por el Gateway para autorizar el enrolamiento.
	// De un solo uso. Se puede pasar también como argumento: `agent enroll <codigo>`.
	ActivationCode string `yaml:"activation_code"`
	// EdgeID es la identidad del Edge que va al CommonName del CSR durante el enrolamiento. Si está
	// vacío se resuelve en tiempo de ejecución: SessionID si existe, si no el hostname del equipo.
	EdgeID string `yaml:"edge_id"`
	// CommandTimeoutSeconds es el DEADLINE POR OPERACIÓN del demux CloudLink (Plan 027 T1, cierra H7): cada
	// comando cloud->edge (SendText/SendMedia/…) se procesa bajo un context.WithTimeout de este tanto, de
	// forma que un envío o descarga de media colgado no vive lo que vive el stream ni frena a otras
	// sesiones. Default 30s (igual que la descarga de media del gateway, para no cortar por debajo de una
	// operación legítima). Se lee de WAPP_AGENT_CLOUDLINK_COMMAND_TIMEOUT_SECONDS; <=0 cae al default.
	CommandTimeoutSeconds int `yaml:"command_timeout_seconds"`
}

// defaultDataDir calcula la RUTA SAGRADA por defecto del store del Edge (MP-02, D1): SIEMPRE en el
// home del usuario y sin permisos de sistema (funciona para un usuario normal sin sudo). Nunca /var/lib
// ni rutas de sistema que exijan root.
//
// Base por SO vía os.UserConfigDir (macOS → ~/Library/Application Support; Linux → $XDG_CONFIG_HOME o
// ~/.config; Windows → %AppData%), a la que se añade wApp/edge. Si UserConfigDir falla, cae a
// ~/.wapp-edge (os.UserHomeDir). Último recurso: "." (nunca peor que el comportamiento previo). El valor
// devuelto es absoluto salvo en ese último fallback, que Load absolutiza igualmente.
func defaultDataDir() string {
	if base, err := os.UserConfigDir(); err == nil && base != "" {
		return filepath.Join(base, "wApp", "edge")
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".wapp-edge")
	}
	return "."
}

// defaults devuelve la configuracion con valores por defecto sensatos.
func defaults() Config {
	return Config{
		LogLevel:              "info",
		LogJSON:               false,
		DBPath:                "wapp-edge.db",
		DEKPath:               "dek.key",
		DBDialect:             "sqlite",
		DataDir:               defaultDataDir(),
		MaxSessions:           5,
		MultiDevicePerAccount: 1,
		PushName:              "wApp",
		ControlSocketPath:     "wapp-edge.sock",
		OutboxMaxEvents:       DefaultOutboxMaxEvents,
		OutboxTTLHours:        DefaultOutboxTTLHours,
		ColaTTLHours:          DefaultColaTTLHours,
		ColaMaxRows:           DefaultColaMaxRows,
		ColaClaimMaxFilas:     DefaultColaClaimMaxFilas,
		ColaLeaseSeconds:      DefaultColaLeaseSeconds,
		DiagLogLines:          DefaultDiagLogLines,
		InboundMarginSeconds:  DefaultInboundMarginSeconds,
		InboundStatsEveryMS:   DefaultInboundStatsEveryMS,
		CloudLink: CloudLinkConfig{
			RuntimePort:           DefaultCloudLinkRuntimePort,
			CommandTimeoutSeconds: DefaultCommandTimeoutSeconds,
		},
		Intent: IntentConfig{
			Enabled:   false,
			OllamaURL: DefaultIntentOllamaURL,
			Model:     DefaultIntentModel,
			WaitMS:    DefaultIntentWaitMS,
		},
		Worker: WorkerConfig{
			MaxConcurrent:      DefaultWorkerMaxConcurrent,
			PollMS:             DefaultWorkerPollMS,
			MaxRunes:           DefaultWorkerMaxRunes,
			NumThread:          DefaultWorkerNumThread,
			NumPredict:         DefaultWorkerNumPredict,
			NumCtx:             DefaultWorkerNumCtx,
			MaxIntentos:        DefaultWorkerMaxIntentos,
			InferenceTimeoutMS: DefaultWorkerInferenceTimeoutMS,
			StatsEveryMS:       DefaultWorkerStatsEveryMS,
		},
		EnableAlphaTestAccounts: false,
		PlatformAPIBaseURL:      DefaultPlatformAPIBaseURL,
	}
}

// Load construye la configuracion del Edge Agent.
//
// Orden de precedencia (de menor a mayor): valores por defecto, archivo YAML en
// path (opcional; si no existe se ignora) y variables de entorno con prefijo
// WAPP_AGENT_. Devuelve error solo si el YAML existe pero no puede leerse o
// parsearse.
func Load(path string) (Config, error) {
	cfg := defaults()

	loader := sharedconfig.New(
		sharedconfig.WithFile(path),
		sharedconfig.WithEnvPrefix(EnvPrefix),
	)

	if err := loader.Unmarshal(&cfg); err != nil {
		return Config{}, err
	}

	// Overlay de entorno: usa el valor actual (default o YAML) como fallback.
	cfg.LogLevel = loader.GetString("LOG_LEVEL", cfg.LogLevel)
	cfg.LogJSON = loader.GetBool("LOG_JSON", cfg.LogJSON)
	cfg.DBPath = loader.GetString("DB_PATH", cfg.DBPath)
	cfg.DEKPath = loader.GetString("DEK_PATH", cfg.DEKPath)
	cfg.DBDialect = loader.GetString("DB_DIALECT", cfg.DBDialect)
	cfg.DBDSN = loader.GetString("DB_DSN", cfg.DBDSN)
	cfg.DataDir = loader.GetString("DATA_DIR", cfg.DataDir)
	cfg.MaxSessions = loader.GetInt("MAX_SESSIONS", cfg.MaxSessions)
	cfg.MultiDevicePerAccount = loader.GetInt("MULTIDEVICE_PER_ACCOUNT", cfg.MultiDevicePerAccount)
	cfg.PushName = loader.GetString("PUSH_NAME", cfg.PushName)
	cfg.ControlSocketPath = loader.GetString("CONTROL_SOCKET_PATH", cfg.ControlSocketPath)
	cfg.OutboxMaxEvents = loader.GetInt("OUTBOX_MAX_EVENTS", cfg.OutboxMaxEvents)
	cfg.OutboxTTLHours = loader.GetInt("OUTBOX_TTL_HOURS", cfg.OutboxTTLHours)
	cfg.ColaTTLHours = loader.GetInt("COLA_TTL_HOURS", cfg.ColaTTLHours)
	cfg.ColaMaxRows = loader.GetInt("COLA_MAX_ROWS", cfg.ColaMaxRows)
	cfg.ColaClaimMaxFilas = loader.GetInt("COLA_CLAIM_MAX_FILAS", cfg.ColaClaimMaxFilas)
	cfg.ColaLeaseSeconds = loader.GetInt("COLA_LEASE_SECONDS", cfg.ColaLeaseSeconds)
	cfg.DiagLogLines = loader.GetInt("DIAG_LOG_LINES", cfg.DiagLogLines)
	cfg.InboundMarginSeconds = loader.GetInt("INBOUND_MARGIN_SECONDS", cfg.InboundMarginSeconds)
	cfg.InboundStatsEveryMS = loader.GetInt("INBOUND_STATS_EVERY_MS", cfg.InboundStatsEveryMS)
	cfg.CloudLink.Endpoint = loader.GetString("CLOUDLINK_ENDPOINT", cfg.CloudLink.Endpoint)
	cfg.CloudLink.SessionID = loader.GetString("CLOUDLINK_SESSION_ID", cfg.CloudLink.SessionID)
	cfg.CloudLink.TLSCert = loader.GetString("CLOUDLINK_TLS_CERT", cfg.CloudLink.TLSCert)
	cfg.CloudLink.TLSKey = loader.GetString("CLOUDLINK_TLS_KEY", cfg.CloudLink.TLSKey)
	cfg.CloudLink.TLSCA = loader.GetString("CLOUDLINK_TLS_CA", cfg.CloudLink.TLSCA)
	cfg.CloudLink.ServerName = loader.GetString("CLOUDLINK_SERVER_NAME", cfg.CloudLink.ServerName)
	cfg.CloudLink.LeasePubKeyPath = loader.GetString("CLOUDLINK_LEASE_PUBKEY_PATH", cfg.CloudLink.LeasePubKeyPath)
	cfg.CloudLink.LeaseShadowMode = loader.GetBool("CLOUDLINK_LEASE_SHADOW_MODE", cfg.CloudLink.LeaseShadowMode)
	cfg.CloudLink.CloudEncPubKeyPath = loader.GetString("CLOUDLINK_CLOUD_ENC_PUBKEY_PATH", cfg.CloudLink.CloudEncPubKeyPath)
	cfg.CloudLink.EnrollmentEndpoint = loader.GetString("CLOUDLINK_ENROLLMENT_ENDPOINT", cfg.CloudLink.EnrollmentEndpoint)
	cfg.CloudLink.ActivationCode = loader.GetString("CLOUDLINK_ACTIVATION_CODE", cfg.CloudLink.ActivationCode)
	cfg.CloudLink.EdgeID = loader.GetString("CLOUDLINK_EDGE_ID", cfg.CloudLink.EdgeID)
	cfg.CloudLink.RuntimePort = loader.GetString("CLOUDLINK_RUNTIME_PORT", cfg.CloudLink.RuntimePort)
	cfg.CloudLink.CommandTimeoutSeconds = loader.GetInt("CLOUDLINK_COMMAND_TIMEOUT_SECONDS", cfg.CloudLink.CommandTimeoutSeconds)
	cfg.Intent.Enabled = loader.GetBool("INTENT_ENABLED", cfg.Intent.Enabled)
	cfg.Intent.OllamaURL = loader.GetString("INTENT_OLLAMA_URL", cfg.Intent.OllamaURL)
	cfg.Intent.Model = loader.GetString("INTENT_MODEL", cfg.Intent.Model)
	cfg.Intent.WaitMS = loader.GetInt("INTENT_WAIT_MS", cfg.Intent.WaitMS)
	cfg.EnableAlphaTestAccounts = loader.GetBool("ALPHA_TEST_ACCOUNTS", loader.GetBool("ENABLE_ALPHA_LOGIN", cfg.EnableAlphaTestAccounts))
	cfg.PlatformAPIBaseURL = loader.GetString("PLATFORM_API_BASE_URL", cfg.PlatformAPIBaseURL)

	// El WORKER-CAJERO se lee con un SEGUNDO loader, con prefijo WAPP_WORKER_ y SIN fichero: el YAML
	// del bloque `worker:` ya lo pobló el Unmarshal de arriba (es el mismo config.yaml), así que aquí
	// sólo hace falta el overlay de entorno. La razón del prefijo distinto está en WorkerEnvPrefix: el
	// cajero es otro proceso, con su propio bloque de entorno en la unidad que lo lanza.
	workerLoader := sharedconfig.New(sharedconfig.WithEnvPrefix(WorkerEnvPrefix))
	cfg.Worker.MaxConcurrent = workerLoader.GetInt("MAX_CONCURRENT", cfg.Worker.MaxConcurrent)
	cfg.Worker.PollMS = workerLoader.GetInt("POLL_MS", cfg.Worker.PollMS)
	cfg.Worker.MaxRunes = workerLoader.GetInt("MAX_RUNES", cfg.Worker.MaxRunes)
	cfg.Worker.NumThread = workerLoader.GetInt("NUM_THREAD", cfg.Worker.NumThread)
	cfg.Worker.NumPredict = workerLoader.GetInt("NUM_PREDICT", cfg.Worker.NumPredict)
	cfg.Worker.NumCtx = workerLoader.GetInt("NUM_CTX", cfg.Worker.NumCtx)
	cfg.Worker.MaxIntentos = workerLoader.GetInt("MAX_INTENTOS", cfg.Worker.MaxIntentos)
	cfg.Worker.InferenceTimeoutMS = workerLoader.GetInt("INFERENCE_TIMEOUT_MS", cfg.Worker.InferenceTimeoutMS)
	cfg.Worker.StatsEveryMS = workerLoader.GetInt("STATS_EVERY_MS", cfg.Worker.StatsEveryMS)

	// Puerto de runtime CloudLink por defecto (Plan 026 T3): si nadie lo fijó (YAML/env), "8101"
	// (topología de prod, design §1). Con él el enroll deriva y persiste el Endpoint de runtime.
	if cfg.CloudLink.RuntimePort == "" {
		cfg.CloudLink.RuntimePort = DefaultCloudLinkRuntimePort
	}

	// Deadline por operación del demux (Plan 027 T1): un valor no positivo (sin fijar, o tecleado mal en
	// YAML/env) cae al default en vez de dejar el demux sin deadline efectivo.
	if cfg.CloudLink.CommandTimeoutSeconds <= 0 {
		cfg.CloudLink.CommandTimeoutSeconds = DefaultCommandTimeoutSeconds
	}

	// Outbox durable (Plan 027 Ola 3 · T2): tope no positivo cae al default (guardarraíl); TTL negativo se
	// satura en 0 (desactivado). No fatal: son límites operativos, no invariantes de seguridad.
	if cfg.OutboxMaxEvents <= 0 {
		cfg.OutboxMaxEvents = DefaultOutboxMaxEvents
	}
	if cfg.OutboxTTLHours < 0 {
		cfg.OutboxTTLHours = 0
	}

	// Cola de entrantes (Plan 051, REQ-051.7): tope y TTL no positivos caen al default. Aquí el TTL NO se
	// puede apagar poniendo 0 (a diferencia del outbox): la cola es un buzón de paso y una cola sin TTL
	// crecería con las filas que el worker nunca llegue a tomar (decisión cerrada del Plan 051).
	if cfg.ColaMaxRows <= 0 {
		cfg.ColaMaxRows = DefaultColaMaxRows
	}
	if cfg.ColaTTLHours <= 0 {
		cfg.ColaTTLHours = DefaultColaTTLHours
	}
	// Lado CAJERO de la cola (Plan 051 Ola 2, T2.1/T2.7): mismo guardarraíl. Un tope de claim a 0 dejaría
	// al cajero sin llevarse nada (cola parada en silencio) y un lease a 0 declararía vencidos TODOS los
	// leases al instante, re-clasificando cada lote una y otra vez. Ninguno de los dos es un invariante
	// de seguridad, pero los dos son formas fáciles de romper el worker tecleando un cero en el YAML.
	if cfg.ColaClaimMaxFilas <= 0 {
		cfg.ColaClaimMaxFilas = DefaultColaClaimMaxFilas
	}
	if cfg.ColaLeaseSeconds <= 0 {
		cfg.ColaLeaseSeconds = DefaultColaLeaseSeconds
	}
	// Bundle de diagnóstico (Plan 031 T8): nº de líneas de log no positivo cae al default.
	if cfg.DiagLogLines <= 0 {
		cfg.DiagLogLines = DefaultDiagLogLines
	}
	// Ventana temporal de ingesta (ADR-0037): un margen no positivo cae al default. El margen es lo que
	// absorbe el desfase de reloj que no podemos medir; a cero descartaría tráfico vivo.
	if cfg.InboundMarginSeconds <= 0 {
		cfg.InboundMarginSeconds = DefaultInboundMarginSeconds
	}
	// Latido de latencia del handler (T3.13): GUARDARRAÍL ASIMÉTRICO A PROPÓSITO, calcado del latido del
	// cajero. Sólo lo NEGATIVO cae al default; el 0 es una petición legítima («no emitas el bloque
	// periódico») y tragárselo dejaría al operador sin forma de callar un log que va a un fichero.
	if cfg.InboundStatsEveryMS < 0 {
		cfg.InboundStatsEveryMS = DefaultInboundStatsEveryMS
	}

	// Clasificador de intenciones (Plan 029): normaliza defaults cuando la feature está ON. Un valor
	// vacío/tecleado mal (YAML/env) cae al default en vez de dejar el clasificador sin URL/modelo/espera.
	// Con Enabled=false no se toca nada relevante (el decorador no se cablea).
	if cfg.Intent.OllamaURL == "" {
		cfg.Intent.OllamaURL = DefaultIntentOllamaURL
	}
	if cfg.Intent.Model == "" {
		cfg.Intent.Model = DefaultIntentModel
	}
	// Presupuesto de espera del despachador (Plan 051 Ola 3): un 0 significaría no esperar NUNCA —el
	// mensaje saldría siempre sin intent y el worker-cajero clasificaría para nadie—, y un negativo no
	// significa nada. Los dos caen al default.
	if cfg.Intent.WaitMS <= 0 {
		cfg.Intent.WaitMS = DefaultIntentWaitMS
	}

	// Worker-cajero (Plan 051 Ola 2): TODOS sus números caen al default si son <=0. Ninguno es un
	// invariante de seguridad, pero cada uno tiene su forma propia de romper el worker con un cero:
	// max_concurrent=0 dejaría un semáforo sin plazas (el bucle se bloquearía para siempre en el claim),
	// poll_ms=0 convertiría la espera en espera ACTIVA quemando un core con la cola vacía, max_runes=0
	// dejaría la entrada sin techo (la DoS de T2.5 de vuelta), y num_thread/num_predict/num_ctx a 0
	// viajarían a Ollama como opciones explícitas absurdas en vez de dejar que él aplique las suyas.
	if cfg.Worker.MaxConcurrent <= 0 {
		cfg.Worker.MaxConcurrent = DefaultWorkerMaxConcurrent
	}
	if cfg.Worker.PollMS <= 0 {
		cfg.Worker.PollMS = DefaultWorkerPollMS
	}
	if cfg.Worker.MaxRunes <= 0 {
		cfg.Worker.MaxRunes = DefaultWorkerMaxRunes
	}
	if cfg.Worker.NumThread <= 0 {
		cfg.Worker.NumThread = DefaultWorkerNumThread
	}
	if cfg.Worker.NumPredict <= 0 {
		cfg.Worker.NumPredict = DefaultWorkerNumPredict
	}
	if cfg.Worker.NumCtx <= 0 {
		cfg.Worker.NumCtx = DefaultWorkerNumCtx
	}
	// max_intentos=0 abandonaría TODOS los lotes en su primer claim, con el sobre `fallo_repetido` y sin
	// haber llamado a Ollama ni una vez: la clasificación entera apagada en silencio, que es peor que no
	// tener el freno. Y un negativo no significa nada. Los dos caen al default.
	if cfg.Worker.MaxIntentos <= 0 {
		cfg.Worker.MaxIntentos = DefaultWorkerMaxIntentos
	}
	// inference_timeout_ms=0 dejaría la inferencia SIN plazo propio (colgada del ctx del proceso, o sea
	// sin plazo en la práctica), que es peor que un plazo mal elegido: una inferencia patológica se
	// quedaría con la plaza del semáforo hasta que el lease la relevara.
	if cfg.Worker.InferenceTimeoutMS <= 0 {
		cfg.Worker.InferenceTimeoutMS = DefaultWorkerInferenceTimeoutMS
	}
	// 🔴 EL GUARDARRAÍL DE stats_every_ms ES DISTINTO Y A PROPÓSITO: el CERO es un valor legítimo que
	// significa DESACTIVADO, no un dedazo. Sólo lo NEGATIVO —que no significa nada— cae al default.
	if cfg.Worker.StatsEveryMS < 0 {
		cfg.Worker.StatsEveryMS = DefaultWorkerStatsEveryMS
	}

	// API pública de la plataforma (C-03/T3.5): un valor vacío (tecleado mal en YAML/env) cae al default en
	// vez de dejar el signup del Edge sin destino contra el que llamar.
	if cfg.PlatformAPIBaseURL == "" {
		cfg.PlatformAPIBaseURL = DefaultPlatformAPIBaseURL
	}

	// Dialecto de BD (Plan 022 T0): solo "sqlite" (default) o "postgres". Se valida aquí para fallar
	// pronto ante un valor tecleado mal (YAML/env) en vez de arrastrarlo hasta abrir la BD.
	switch cfg.DBDialect {
	case "sqlite", "postgres":
	default:
		return Config{}, fmt.Errorf("config: db_dialect no soportado %q (usa \"sqlite\" o \"postgres\")", cfg.DBDialect)
	}

	// D2 (MP-02): ancla data_dir a ruta ABSOLUTA una sola vez, venga del default sagrado, del YAML o del
	// override WAPP_AGENT_DATA_DIR. filepath.Abs es idempotente (una ruta ya absoluta se devuelve limpia)
	// y no toca el disco; el MkdirAll de la raíz lo hace el arranque (cmd/agent). Así el store nunca
	// depende del CWD desde el que se lance el daemon.
	absDataDir, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return Config{}, fmt.Errorf("config: no se pudo resolver data_dir %q a ruta absoluta: %w", cfg.DataDir, err)
	}
	cfg.DataDir = absDataDir

	// Endpoint de runtime PERSISTIDO por el enroll (Plan 026 T3, cierra follow-up 023): si nadie fijó el
	// Endpoint por YAML/env (lo normal en el kit: viene comentado), se lee del archivo de estado
	// <data_dir>/cloudlink-endpoint que el enroll escribió al derivarlo del enrollment_endpoint. Así
	// `serve` levanta el stream contra la nube SIN que un no-técnico edite el config.yaml. Es un FALLBACK
	// de menor prioridad: un Endpoint explícito (YAML o env WAPP_AGENT_CLOUDLINK_ENDPOINT) siempre gana.
	// Best-effort: si el archivo no existe (aún no se enroló) o no se puede leer, se queda vacío (el Edge
	// cae a LogSink/LogMux, igual que antes). Solo material PÚBLICO (host:puerto), nunca secretos.
	if cfg.CloudLink.Endpoint == "" {
		if data, readErr := os.ReadFile(RuntimeEndpointStatePath(cfg.DataDir)); readErr == nil {
			cfg.CloudLink.Endpoint = strings.TrimSpace(string(data))
		}
	}

	// Failover multi-dispositivo por número (Plan 022 T5, §10.F): CLAMP a [1,4]. 1 = off (un device vivo
	// por número, comportamiento actual); 4 = tope de WhatsApp (1 principal + 4 vinculados). Valores fuera
	// de rango (0, negativos, >4) se saturan en vez de fallar el arranque (guardarraíl, no invariante de
	// seguridad). RESILIENCIA, no sigilo: no se debe subir sin necesidad operativa (más huella).
	if cfg.MultiDevicePerAccount < 1 {
		cfg.MultiDevicePerAccount = 1
	}
	if cfg.MultiDevicePerAccount > 4 {
		cfg.MultiDevicePerAccount = 4
	}

	return cfg, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// VARIABLES RETIRADAS — el aviso que evita el fallo silencioso
// ─────────────────────────────────────────────────────────────────────────────
//
// AvisoRetirada describe UNA variable de entorno que ya no se lee y qué debe usarse en su lugar.
//
// Existe porque retirar una variable de entorno FALLA EN SILENCIO por definición: el operador la deja
// puesta en su unidad de systemd/LaunchAgent, el proceso arranca sin una queja, y el número que él cree
// haber fijado no gobierna nada. No hay error, no hay log, no hay forma de notarlo salvo midiendo el
// comportamiento. Un Warn en el arranque cuesta tres líneas y cierra ese agujero.
type AvisoRetirada struct {
	// Variable es el NOMBRE COMPLETO de la variable retirada (con prefijo), tal y como el operador la
	// tiene escrita en su entorno. Se nombra literal para que un grep en sus ficheros la encuentre.
	Variable string
	// Sustituta es el nombre completo de la variable VIGENTE que hace su trabajo (o "" si no hay
	// sustituta y la función simplemente desapareció).
	Sustituta string
	// Motivo explica en una frase por qué se retiró, para que el aviso sea accionable sin abrir el plan.
	Motivo string
}

// VariablesRetiradas devuelve el aviso a emitir por cada variable de entorno RETIRADA que siga presente
// en el entorno del proceso. Devuelve nil si no hay ninguna (el caso normal).
//
// Es una función PURA sobre el entorno —sólo os.LookupEnv, sin YAML, sin disco— y por eso NO vive dentro
// de Load: config no tiene logger y no debe tenerlo (Load se llama antes de construirlo). El que sí lo
// tiene es el arranque, que llama aquí y emite un Warn por cada elemento. Así el aviso es testeable con
// t.Setenv sin levantar nada.
//
// SE MIRA LA PRESENCIA, NO EL VALOR: una variable puesta a vacío también se avisa, porque el operador la
// escribió con una intención y esa intención ya no se cumple.
func VariablesRetiradas() []AvisoRetirada {
	var avisos []AvisoRetirada

	// WAPP_AGENT_INTENT_TIMEOUT_MS (Plan 051 Ola 3, T3.1): era el plazo del camino INLINE —el decorador
	// que clasificaba dentro del handler de whatsmeow— y ese camino se retira con la ola. Se nombran las
	// DOS variables vivas porque la confusión clásica es colapsarlas en una: la que acota lo que ESPERA
	// el despachador (WAPP_AGENT_INTENT_WAIT_MS) y la que acota lo que TARDA la inferencia del
	// worker-cajero (WAPP_WORKER_INFERENCE_TIMEOUT_MS). Quien tenía puesta la retirada casi siempre
	// quería la segunda, así que es la que se le señala como sustituta.
	//
	// ⚠️ LA SUSTITUTA SE NOMBRA COMO ESTÁ EN EL CÓDIGO, NO COMO ESTÁ EN LOS DOCS. El plan y el ADR-0038
	// la llaman `WAPP_WORKER_TIMEOUT_MS`, pero lo que Load lee de verdad es
	// `WAPP_WORKER_INFERENCE_TIMEOUT_MS` (ver el overlay del workerLoader). Mandar al operador a la
	// variable de los docs sería reproducir el mismo fallo silencioso que este aviso existe para evitar:
	// la escribiría, no gobernaría nada y nadie se enteraría.
	if _, presente := os.LookupEnv(EnvPrefix + "INTENT_TIMEOUT_MS"); presente {
		avisos = append(avisos, AvisoRetirada{
			Variable:  EnvPrefix + "INTENT_TIMEOUT_MS",
			Sustituta: WorkerEnvPrefix + "INFERENCE_TIMEOUT_MS",
			Motivo: "el camino inline de clasificación se retiró (Plan 051 Ola 3): el timeout de INFERENCIA " +
				"es ahora " + WorkerEnvPrefix + "INFERENCE_TIMEOUT_MS y el presupuesto de ESPERA del " +
				"despachador es " + EnvPrefix + "INTENT_WAIT_MS; son dos números distintos y esta variable " +
				"ya no gobierna ninguno",
		})
	}

	return avisos
}

// DefaultConfigPath devuelve la ruta ESTABLE del config.yaml del Edge dentro del data_dir sagrado
// (Plan 023 · T1): <data_dir>/config.yaml. Cierra el gotcha del CWD — MP-02 lo cerró para el data_dir
// (D2, absolutización); aquí se extiende al ARCHIVO de config, que hasta ahora se buscaba relativo al
// CWD ("config.yaml") y por tanto dependía de desde dónde se lanzara el proceso.
//
// Resuelve el data_dir igual que Load —override WAPP_AGENT_DATA_DIR, y si no el default por SO
// (defaultDataDir)— SIN depender del CWD, y lo absolutiza. El instalador y el LaunchAgent (T3/T4)
// además fijan WAPP_AGENT_CONFIG a este mismo valor; cuando esa variable está presente, el arranque
// (cmd/agent, cmd/wapp-ctl) la respeta y no llama aquí. El archivo puede no existir aún: Load lo trata
// como opcional (defaults + env), así que apuntar a una ruta inexistente en instalación limpia es seguro.
func DefaultConfigPath() string {
	dir := os.Getenv(EnvPrefix + "DATA_DIR")
	if dir == "" {
		dir = defaultDataDir()
	}
	if abs, err := filepath.Abs(dir); err == nil {
		dir = abs
	}
	return filepath.Join(dir, "config.yaml")
}
