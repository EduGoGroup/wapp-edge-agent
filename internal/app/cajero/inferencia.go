package cajero

// inferencia.go — EL p50 DE LA INFERENCIA (Plan 051 Ola 4 · T4.5).
//
// 🔴 POR QUÉ HAY QUE CONSTRUIRLO: hasta esta tarea el cajero NO tenía ningún agregado de latencia. Los
// únicos números que existían eran valores CRUDOS por lote, y sólo en el log (`latencia_ms` en el
// camino de fallo, `total_ms` de res.Metrics en el de éxito). Con eso, la pregunta que un operador hace
// de verdad —«¿va lento el clasificador?»— sólo se puede responder leyendo cientos de líneas a ojo. El
// parte del heartbeat necesita UN número, y ese número no existía.
//
// ─────────────────────────────────────────────────────────────────────────────
// POR QUÉ NO SE REUSA internal/app/latencia, QUE ES EL HISTOGRAMA QUE YA HABÍA
// ─────────────────────────────────────────────────────────────────────────────
// Se leyó entero antes de decidir, y su INGENIERÍA sí se reusa (buckets fijos + atómicos + monotónico +
// percentil por rango con ceil: este fichero es su gemelo pequeño). Lo que no encaja es su REJILLA, y
// no por un margen discutible:
//
//   - Su rejilla se calibró para el HANDLER DE ENTRANTES, cuyo criterio es «< 50 ms p99» (INV-051.2):
//     el borde más alto son 2,5 s y por encima está el bucket de desbordamiento. La inferencia vive DOS
//     órdenes de magnitud más arriba —el p50 medido en campo son 8,1 s y la Ola 1.6 vio lotes de 61 s—,
//     así que CASI TODAS las muestras caerían en el desbordamiento y Percentil devolvería CotaDesbordada
//     (-1). Un instrumento que devuelve «se salió de la escala» para el 100 % de las medidas no mide nada.
//   - Su enum `Camino` (Encolado/Descartado) es vocabulario del LISTENER. Meter aquí la inferencia
//     obligaría a etiquetarla como una de esas dos poblaciones, y el doc del paquete dice expresamente
//     que INV-051.2 se juzga contra `Encolado`: contaminar esa serie con muestras de segundos rompería
//     el p99 del handler, que es el criterio de cierre de la Ola 3.
//
// Ampliar la rejilla de aquel paquete para que sirviera a los dos sería peor: sus bordes de 40/50/75 ms
// son los que sostienen su criterio y no se pueden mover, así que habría que añadir una segunda rejilla
// y un parámetro para elegirla — o sea, este fichero, pero acoplado al camino caliente del socket.
//
// 🔴 INV-051.1 / ADR-0007: aquí sólo entran DURACIONES. Ni texto, ni intención, ni chat.

import (
	"math"
	"sync/atomic"
	"time"
)

// numBucketsInferencia es el tamaño de la rejilla. Dieciséis, como en internal/app/latencia y por el
// mismo motivo: cabe en una línea de caché y la búsqueda lineal es más rápida que una binaria.
const numBucketsInferencia = 16

// bordesInferenciaMS es la rejilla, en MILISEGUNDOS y por borde SUPERIOR INCLUSIVO: una muestra cae en
// el primer bucket cuyo borde sea >= a ella. Milisegundos y no microsegundos porque lo que se publica
// es un entero de ms (app.ParteWorker.P50ms) y porque a esta escala el microsegundo es ruido.
//
// LOS BORDES ESTÁN PUESTOS DONDE LA MEDICIÓN DICE, no repartidos en potencias de dos:
//
//   - El racimo 7500/10000/12500/15000 rodea la zona donde de verdad viven las medidas: el p50 REAL de
//     campo son 8,1 s y el p90 12,8 s (430 inferencias en UAT,
//     docs/brainstorm/2026-08-23-tres-niveles-de-llm-y-doctrina-del-transporte.md §Anexo). Ahí la
//     resolución es de 2,5 s, un ~30 % sobre el p50 — bastante para ver que el clasificador se degrada y
//     no tanto como para gastar buckets en precisión falsa.
//   - LOS TRES BORDES DE ARRIBA SE DERIVAN DE SUS CONSTANTES Y NO SE ESCRIBEN A MANO, que es lo que
//     arregló T1.7-2: el umbral de lentitud, el plazo por defecto y el techo son exactamente los números
//     contra los que se juzga una inferencia, y un borde copiado a mano CADUCA EN SILENCIO en cuanto
//     alguien mueve la constante. Ya pasó: esta rejilla llevaba un borde en 15.000 rotulado «EL TIMEOUT»
//     desde que la Ola 1.6 subió el plazo a 45.000, y con él la pregunta operativa que este histograma
//     existe para responder —«¿cuántas se comieron el plazo?»— no tenía respuesta: todo lo que pasaba de
//     20 s caía en un bucket de 20→60 s. Es el mismo truco del borde de 50 ms de internal/app/latencia
//     con INV-051.2, sólo que atado a la constante en vez de a una copia suya.
//   - 30000 flanquea por abajo al umbral de lentitud para poder distinguir «va justo» de «cuenta como
//     fallo para el breaker», que con un solo borde serían indistinguibles (el mismo par 40/50 ms del
//     gemelo).
//   - Los bordes bajos (250…5000) se conservan sin racimo: ya no es donde viven las medidas, pero es
//     donde vive una máquina SANA con la caché de prefijo caliente, y aplastarlas todas en un bucket
//     dejaría el p50 sin resolución justo el día que la cosa vaya bien.
//
// El último borde es el DESBORDAMIENTO: peor que el TECHO que el propio Edge impone. Sólo es alcanzable
// con el plazo desactivado (Deps.Timeout <= 0 y el Cloud sin fijar `timeout_ms`), porque cualquier otra
// inferencia se aborta al llegar a su plazo, que nunca pasa del techo.
//
// ⚠️ EL ORDEN ES UNA INVARIANTE Y SE COMPRUEBA: con tres bordes derivados de constantes, mover una podría
// romper la partición y dejar buckets muertos sin que nada fallara. Lo caza
// TestRejillaInferencia_EsEstrictamenteCrecienteYCadaBucketEsAlcanzable, gemelo del que ya protegía
// bordesUS en internal/app/latencia.
var bordesInferenciaMS = [numBucketsInferencia]int64{
	250,
	500,
	1_000,
	2_000,
	3_000,
	5_000,
	7_500,
	10_000,
	12_500, // ← el p90 de campo (12,8 s) cae en el bucket siguiente
	15_000,
	20_000,
	30_000,
	int64(float64(DefaultInferenceTimeoutMS) * FraccionLentitud), // 36 s ← EL UMBRAL DE LENTITUD (ADR-0042)
	DefaultInferenceTimeoutMS,                                    // 45 s ← EL PLAZO por defecto
	DefaultMaxTimeoutMS,                                          // 120 s ← EL TECHO de lo que el Cloud puede pedir
	math.MaxInt64,
}

// histogramaInferencia acumula las latencias de inferencia del proceso. UNA sola población: el breaker,
// el semáforo y este histograma son POR PROCESO porque los tres describen a Ollama, que es uno por
// máquina (ver Deps.Colas). Partirlo por cola daría N series de las que ninguna tendría muestras
// suficientes para un percentil, midiendo además el mismo Ollama N veces.
//
// MONOTÓNICO Y SIN RESET, igual que internal/app/latencia: los contadores sólo suben. El patrón «leer y
// poner a cero» tiene una ventana entre la lectura y el reset en la que las muestras se pierden, y
// mentiría a la baja justo cuando hay ráfaga. Consecuencia honesta que hay que saber leer: el p50 que
// viaja en el parte es el DE TODA LA VIDA DEL PROCESO, no el del último minuto — se mueve despacio, y
// un cajero que lleva días arriba no reflejará una degradación reciente hasta que acumule muestras. Si
// algún día hace falta el p50 del intervalo, el camino es restar dos fotos (como hace latencia.Delta),
// nunca resetear.
//
// El cero es utilizable y el NIL TAMBIÉN (todos los métodos son nil-safe), para que un Cajero construido
// sin este campo se comporte igual que antes de esta tarea.
type histogramaInferencia struct {
	buckets [numBucketsInferencia]atomic.Uint64
}

// observar anota una latencia de inferencia.
//
// 🔴 LOS FALLOS ENTRAN, Y ES UNA DECISIÓN, NO UNA OBVIEDAD. Un timeout es latencia que el cajero PAGÓ de
// verdad: la plaza del aforo estuvo ocupada TODO el plazo de esa petición y el resto de la cola esperó.
// Medir sólo los éxitos daría el número exactamente al revés de como hace falta: con Ollama medio
// caído, los pocos lotes que salen adelante son los cortos, así que el p50 MEJORARÍA a medida que el
// clasificador empeora, y el operador vería un número verde mientras la cola se atasca. Es el mismo
// argumento por el que internal/app/latencia se niega a medir sólo lo que encola.
//
// LO QUE NO ENTRA es la inferencia cortada por el APAGADO (ctx del proceso cancelado): ese tiempo no lo
// decidió Ollama sino el SIGTERM, y contarlo metería una muestra corta y arbitraria por cada lote en
// vuelo en cada parada. El corte lo hace el llamante (procesar), que es quien sabe distinguirlo.
//
// Sin locks, sin asignaciones: un `atomic.Add` y una búsqueda lineal de 16 comparaciones. Aun así corre
// en la goroutine de procesar() y no en el hilo de whatsmeow, así que el presupuesto es holgado.
func (h *histogramaInferencia) observar(d time.Duration) {
	if h == nil {
		return
	}
	h.buckets[bucketInferenciaDe(d.Milliseconds())].Add(1)
}

// bucketInferenciaDe localiza el bucket de una muestra en milisegundos. Una muestra <= 0 (reloj
// inyectado que no avanza, típico en tests) cae en el primer bucket, que es la lectura correcta: fue
// más rápida que la resolución de la rejilla.
func bucketInferenciaDe(ms int64) int {
	for i, borde := range bordesInferenciaMS {
		if ms <= borde {
			return i
		}
	}
	return numBucketsInferencia - 1
}

// p50MS devuelve el p50 en milisegundos: el borde SUPERIOR del bucket donde cae la mediana. Es una COTA
// SUPERIOR, nunca una interpolación — interpolar dentro del bucket inventaría una precisión que la
// rejilla no tiene.
//
// 🔴 SIN MUESTRAS DEVUELVE 0, que es lo que app.ParteWorker.P50ms define como «sin muestras». Es el
// único valor de retorno ambiguo del canal y se acepta a sabiendas: un p50 real de 0 ms es imposible
// (una inferencia contra Ollama no baja de decenas de ms), así que en la práctica 0 sólo significa
// «este cajero todavía no ha clasificado nada» — que es exactamente lo que el operador necesita
// entender cuando ve el hueco.
//
// EN EL DESBORDAMIENTO SE PUBLICA EL ÚLTIMO BORDE FINITO —el TECHO, DefaultMaxTimeoutMS— y no un
// centinela negativo, al revés que latencia.CotaDesbordada. El motivo es el consumidor: esto viaja en un
// entero del contrato del heartbeat, donde un -1 se leería como un valor medido o rompería una agregación
// aguas arriba. El número publicado entonces MIENTE A LA BAJA (el p50 real es peor que el techo), pero un
// p50 de dos minutos ya es una alarma tan estridente que no hay decisión que dependa de la diferencia. Y
// sólo es alcanzable con el plazo desactivado.
func (h *histogramaInferencia) p50MS() int64 {
	if h == nil {
		return 0
	}
	var total uint64
	var conteo [numBucketsInferencia]uint64
	for i := range h.buckets {
		v := h.buckets[i].Load()
		conteo[i] = v
		total += v
	}
	if total == 0 {
		return 0
	}

	// Rango por CEIL, igual que latencia.Percentil: con ceil, «p50 <= X» equivale exactamente a «al menos
	// la mitad de las inferencias cupo en X». Truncar movería el corte una posición hacia abajo y en
	// poblaciones pequeñas dejaría fuera justo la muestra que decide.
	rango := uint64(math.Ceil(0.5 * float64(total)))
	if rango == 0 {
		rango = 1
	}
	var acumulado uint64
	for i := range conteo {
		acumulado += conteo[i]
		if acumulado >= rango {
			if i >= numBucketsInferencia-1 {
				return bordesInferenciaMS[numBucketsInferencia-2]
			}
			return bordesInferenciaMS[i]
		}
	}
	// Inalcanzable (acumulado termina valiendo total >= rango). Devolver el último borde finito es mejor
	// respuesta que un 0, que significaría «sin muestras» habiéndolas.
	return bordesInferenciaMS[numBucketsInferencia-2]
}

// muestras es cuántas inferencias se han medido. Sólo lo usan los tests y el log: el parte publica el
// p50 y el consumidor no necesita la población para interpretarlo (0 ya significa «sin muestras»).
func (h *histogramaInferencia) muestras() uint64 {
	if h == nil {
		return 0
	}
	var total uint64
	for i := range h.buckets {
		total += h.buckets[i].Load()
	}
	return total
}
