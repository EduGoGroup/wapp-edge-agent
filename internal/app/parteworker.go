package app

import (
	"context"
	"time"
)

// parteworker.go — EL TUBO CAJERO→DAEMON (Plan 051 Ola 4 · T4.5).
//
// Aquí vive el ÚNICO canal por el que el proceso del cajero le cuenta al proceso del daemon cómo está.
// No es un IPC nuevo: es la MISMA BD de la cola (<data_dir>/cola_entrantes.db) que los dos ya abren,
// con una tabla de una sola fila. El mismo criterio con el que el cajero sondea el contrato de
// intenciones en edge.db (cmd/agent/cajero.go · contratoIntenciones): el disco es lo que comparten, y
// una tabla es más barata que un socket.

// ParteWorker es el parte de salud que el proceso del CAJERO deja escrito para que el DAEMON lo lea.
// Existe porque son DOS PROCESOS y el único canal que comparten es el disco (la BD de la cola) — la
// deuda declarada en internal/infra/daemon/daemon.go (el `nil` explícito de health.NewCollector) y en
// docs/funcionalidades/20. Sin este parte, `intent_circuit` viaja vacío.
//
// Frontera zero-knowledge (ADR-0007): aquí NO va ni una llave, ni una credencial, ni un JID, ni un
// número, ni contenido de mensaje. Solo señales operativas. Es la misma regla que INV-051.1 impone al
// log del cajero, aplicada a un canal que además SALE de la máquina (el heartbeat).
type ParteWorker struct {
	// TS es cuándo lo escribió el cajero. El lector DEBE compararlo con su reloj (ver ParteRancio).
	TS time.Time
	// Circuito es el estado del circuit breaker del clasificador. Vacío = no se sabe.
	//
	// ⚠️ POR EL TUBO VIAJAN LAS ETIQUETAS DEL BREAKER TAL CUAL —"closed" | "open" | "half-open"
	// (breaker.StateClosed/StateOpen/StateHalfOpen)—, CON GUION en el tercero. La forma que el contrato
	// del heartbeat publica es `half_open` (ADR-0023), y esa traducción la hace el LECTOR, no el
	// escritor. Se deja así a propósito: el cajero copia el valor de su breaker sin interpretarlo, que
	// es lo que impide que una etiqueta nueva del breaker se pierda en silencio por el camino.
	Circuito string
	// Taskset es el veredicto del reparto de CPUs entre Ollama y el cajero (T2.8): "disjunta" |
	// "solapada" | "cajero_sin_confinar". Vacío = no se sabe (no-Linux, o /proc ilegible).
	Taskset string
	// P50ms es el p50 de la INFERENCIA en ms; 0 = sin muestras.
	//
	// ⚠️ ES EL TOTAL, y desde T1.7-5 ya no es el único número de latencia que viaja: mezcla PREFILL y
	// GENERACIÓN, dos regímenes que se diferencian en un orden de magnitud. Se conserva porque es lo que
	// publica `SessionHealth.intent_p50_ms` (campo 10) y porque responde una pregunta que las fases no
	// responden —cuánto espera de verdad quien pidió la inferencia, fallos incluidos—, pero para saber
	// POR QUÉ tarda hay que mirar los cuatro campos de abajo.
	P50ms int64

	// ─── Plan 044 · Ola 1.7 · T1.7-5 · el reparto de la inferencia ───────────
	//
	// 🔴 EL CUANTIL Y SU `n` VIAJAN EN PAREJA Y SE LEEN EN PAREJA. Un p50 sobre una muestra pequeña es un
	// MÁXIMO DISFRAZADO, y comparar cuantiles de `n` distinto ya fabricó aquí una conclusión falsa. El
	// contrato del heartbeat lo impone en el wire (`InferenceLatency` es UN mensaje con los dos campos, y
	// su AUSENCIA significa «no medible»); este canal lo respeta con la única regla que un par de enteros
	// permite: EL QUE DECIDE ES `…Muestras`. Con `…Muestras == 0` el p50 NO SIGNIFICA NADA y el lector no
	// debe publicarlo — ni siquiera como cero, que se leería como «instantáneo».

	// PrefillP50ms es el p50 del PREFILL (digerir el prompt de entrada) en ms.
	PrefillP50ms int64
	// PrefillMuestras es cuántos prefills se midieron. 0 ⇒ no medible ⇒ PrefillP50ms no significa nada.
	PrefillMuestras int64
	// GeneracionP50ms es el p50 de la GENERACIÓN (producir los tokens de salida) en ms.
	GeneracionP50ms int64
	// GeneracionMuestras es cuántas generaciones se midieron. 0 ⇒ no medible.
	GeneracionMuestras int64

	// PorRegimen es el reparto ACUMULADO por calor del prefijo (cajero.RegimenesInferencia: frio /
	// templado / caliente). PorClase, el reparto por `class` (app.ClasesInferencia).
	//
	// 🔴 SON MAPAS Y NO UN CAMPO POR CATEGORÍA, y esa es toda la gracia: los umbrales que definen los
	// regímenes son POLÍTICA DEL EMISOR y se mueven con el hardware del cliente, así que una categoría
	// nueva tiene que poder aparecer sin tocar el contrato del heartbeat ni el esquema de este canal. Por
	// eso cruzan la BD como JSON en una sola columna y llegan al wire tal cual: en todo el recorrido
	// cajero→daemon→nube NADIE enumera las claves más que el cajero, que es quien las inventa.
	//
	// ⚠️ SON ACUMULADOS MONÓTONOS DEL PROCESO, mientras que los dos p50 de arriba son una foto. Las
	// ventanas NO coinciden y dividir un cuantil entre uno de estos contadores da un número absurdo con
	// buena pinta.
	//
	// nil ⇒ «este cajero no lo mide» (o el parte está rancio). Un mapa presente con una clave a 0 es un
	// DATO: «esta máquina no ha pagado ni un arranque en frío».
	PorRegimen map[string]int64
	PorClase   map[string]int64
}

// ParteWorkerEscritor lo usa el CAJERO.
type ParteWorkerEscritor interface {
	PublicarParte(ctx context.Context, p ParteWorker) error
}

// ParteWorkerLector lo usa el DAEMON. Si el cajero nunca escribió, devuelve
// (ParteWorker{}, false, nil): la ausencia de parte NO es un error. Es el mismo contrato que el
// `(nil, nil)` de ColaCajero.Reclamar con la cola vacía: el estado normal de una instalación en la que
// el cajero todavía no ha arrancado no puede llegarle al llamante como un fallo.
type ParteWorkerLector interface {
	LeerParte(ctx context.Context) (ParteWorker, bool, error)
}

// ParteCada es cada cuánto REESCRIBE el cajero su parte, y es la mitad escritora del contrato de
// frescura: ParteRancio se deriva de este número, así que los dos no pueden divergir.
//
// 🔴 POR QUÉ NO ES `WAPP_WORKER_STATS_EVERY_MS`, que es el latido que ya existía en el bucle del cajero
// y era el candidato obvio. Dos motivos, y cualquiera de los dos basta:
//
//   - SU DEFAULT SON 5 MINUTOS (config.DefaultWorkerStatsEveryMS). Colgar de él la frescura obligaría a
//     un ParteRancio de 15 minutos, y un `intent_circuit` que tarda un cuarto de hora en admitir que el
//     cajero murió no es una señal de salud, es un adorno.
//   - EL OPERADOR PUEDE PONERLO A CERO, y el cero lo DESACTIVA (es el guardarraíl explícito de
//     config.Load: un 0 no cae al default). Con esa configuración el parte se escribiría una sola vez,
//     al arrancar, y a los 90 s el daemon lo declararía rancio PARA SIEMPRE. Es decir: bajar la
//     verbosidad del log apagaría el heartbeat. Un mando de LOG no puede gobernar una señal de SALUD.
//
// 30 s es el compromiso: 20 escrituras por hora y por instalación contra un SQLite local (un UPSERT de
// once columnas desde T1.7-5, sin BLOB) es ruido comparado con el tráfico de la propia cola, y deja la detección
// de un cajero muerto por debajo del minuto y medio.
const ParteCada = 30 * time.Second

// ParteRancio es cuánto puede tener un parte antes de que el lector lo tire ENTERO.
//
// EL 3× ESTÁ CALCULADO, no elegido: 3 · ParteCada = 3 · 30 s = 90 s. Tres periodos es lo que hace falta
// para que un parte no se declare rancio por una tardanza normal del escritor, porque el bucle del
// cajero NO publica con un reloj de precisión: publica al principio de cada vuelta, y la vuelta puede
// quedarse bloqueada tomando la plaza del semáforo mientras una inferencia está en curso. El peor caso
// medible es una inferencia entera (Timeout, 15 s por defecto) más el cierre del lote (cierreTimeout,
// 5 s) ⇒ ~20 s de retraso sobre los 30 s del tick, o sea ~50 s < 90 s. Con dos periodos (60 s) ese peor
// caso rozaría el umbral y un Edge sano publicaría huecos.
//
// 🔴 REGLA DURA: un parte rancio ⇒ TODOS los campos a su cero ⇒ intent_circuit vacío.
// JAMÁS se publica un "closed" heredado de un cajero muerto: eso es una señal de salud INVENTADA, y es
// PEOR que la ausencia del dato — la nube vería un Edge sano mientras el clasificador lleva horas
// caído. Esta decisión está cerrada y no se reabre.
const ParteRancio = 3 * ParteCada
