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
//     el borde más alto son 2,5 s y por encima está el bucket de desbordamiento. La inferencia vive un
//     orden de magnitud más arriba — la O0 midió p50 = 2.613 ms y p95 = 3.736 ms contra un timeout de
//     15 s (docs/plans/051-worker-cajero-edge/O0-resultados-2026-08-09.md)—, así que CASI TODAS las
//     muestras caerían en el desbordamiento y Percentil devolvería CotaDesbordada (-1). Un instrumento
//     que devuelve «se salió de la escala» para el 100 % de las medidas no mide nada.
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
//   - El racimo 2000/2500/3000/3500/4000 rodea la zona donde de verdad viven las medidas (p50 2.613 ms,
//     p95 3.736 ms). Ahí la resolución es de 500 ms, que sobre un p50 de ~2,6 s es un 19 % — bastante
//     para ver que el clasificador se degrada y no tanto como para gastar buckets en precisión falsa.
//   - 15000 es EXACTAMENTE DefaultInferenceTimeoutMS, y ese borde es el que sostiene la pregunta
//     operativa que importa: «¿cuántas inferencias se comieron el plazo?». Es el mismo truco del borde
//     de 50 ms de internal/app/latencia con INV-051.2; sin él, la respuesta habría que estimarla.
//   - 20000/60000 existen para el caso en que alguien DESACTIVA el plazo propio (Deps.Timeout <= 0 ⇒
//     manda el ctx del proceso): sin ellos, un Ollama agonizante aplastaría todo en el último bucket.
//
// El último borde es el DESBORDAMIENTO. Con el timeout por defecto es inalcanzable —la inferencia se
// aborta a los 15 s—, así que sólo se puebla con el plazo desactivado.
var bordesInferenciaMS = [numBucketsInferencia]int64{
	250,
	500,
	1_000,
	1_500,
	2_000,
	2_500,
	3_000, // ← aquí cae el p50 medido por la O0 (2.613 ms)
	3_500,
	4_000, // ← y aquí el p95 (3.736 ms)
	5_000,
	7_500,
	10_000,
	15_000, // ← EL TIMEOUT (DefaultInferenceTimeoutMS)
	20_000,
	60_000,
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
// 🔴 LOS FALLOS ENTRAN, Y ES UNA DECISIÓN, NO UNA OBVIEDAD. Un timeout de 15 s es latencia que el
// cajero PAGÓ de verdad: la plaza del semáforo estuvo ocupada esos 15 s y el resto de la cola esperó.
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
// EN EL DESBORDAMIENTO SE PUBLICA EL ÚLTIMO BORDE FINITO (60.000) y no un centinela negativo, al revés
// que latencia.CotaDesbordada. El motivo es el consumidor: esto viaja en un entero del contrato del
// heartbeat, donde un -1 se leería como un valor medido o rompería una agregación aguas arriba. El
// número publicado entonces MIENTE A LA BAJA (el p50 real es peor que 60 s), pero un p50 de un minuto
// ya es una alarma tan estridente que no hay decisión que dependa de la diferencia. Y sólo es
// alcanzable con Deps.Timeout desactivado.
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
