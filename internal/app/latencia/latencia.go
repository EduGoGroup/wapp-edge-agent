// Package latencia mide CUÁNTO TARDA EL HANDLER DE ENTRANTES del listener (Plan 051 Ola 3 · T3.13).
//
// 🔴 POR QUÉ EXISTE. El criterio de cierre de la Ola 3 es «handler < 50 ms p99» (INV-051.2) y hasta esta
// tarea NO HABÍA CON QUÉ MEDIRLO: ni un `time.Since` en el listener fuera de tests, ni un campo de latencia
// en ningún log de `internal/`. El criterio existía sobre el papel y era incomprobable en campo.
//
// 🔴 INV-051.1 SIGUE INTACTO: aquí no entra NADA derivado del contenido de un mensaje. Este paquete solo
// conoce duraciones y cardinalidades; ni ve el texto, ni el teléfono, ni el chat.
//
// ─────────────────────────────────────────────────────────────────────────────
// POR QUÉ UN HISTOGRAMA DE BUCKETS FIJOS Y NO UN RESERVOIR NI UNA VENTANA
// ─────────────────────────────────────────────────────────────────────────────
// El cronómetro corre EN EL HILO DE WHATSMEOW, en el mismo camino caliente que INV-051.2 protege. Lo que
// se le puede pedir a ese sitio es muy poco:
//
//   - Un RESERVOIR (muestreo aleatorio) con k=1000 apoya el p99 en ~10 muestras: para un percentil de cola
//     eso es ruido, no medida.
//   - Una VENTANA CIRCULAR obliga a un `sort` en cada emisión y no deja acumulado.
//   - Un histograma de buckets fijos son 16 `atomic.Uint64` por serie: SIN locks, SIN asignaciones, SIN
//     sort en caliente, ~256 B por serie sea cual sea el volumen, y da acumulado y delta a la vez.
//
// EL COSTE, MEDIDO (no estimado). `BenchmarkObservar`, en coste_test.go, lo produce; el comando está
// escrito en su cabecera. En un Apple M1 Pro (darwin/arm64), con `-benchtime 2s`:
//
//   - `Observar` sola: 9,1 ns/op — la búsqueda lineal de 16 comparaciones más el `atomic.Add`.
//   - `Observar` con la muestra en el bucket de desbordamiento: 10,9 ns/op — el peor caso de la rejilla
//     apenas se distingue del mejor, así que un handler patológico no paga además un instrumento lento.
//   - EL NÚMERO DE CAMPO, con los dos relojes que el listener paga de verdad (`time.Now` a la entrada y
//     `time.Since` en el defer): 90,5 ns/op. Cero asignaciones en los tres.
//
// ⚠️ EL REPARTO NO ES EL QUE ESTA CABECERA DECÍA ANTES DE MEDIRLO, y conviene saberlo antes de optimizar
// nada: la parte cara son LOS RELOJES (~80 ns el par en arm64), no el histograma (9 ns). Aquí se decía
// «~25 ns» para el reloj y «~5 ns + 5-10 ns» para búsqueda y atómico; el total quedaba en el mismo orden
// por casualidad, pero quien fuera a recortar coste habría ido a por la mitad barata.
//
// Frente a un handler cuyo acto caro es un INSERT cifrado en SQLite (milisegundos), el efecto observador
// queda por debajo del 0,1 %. La CONDICIÓN bajo la que ese número dejaría de valer —varias sesiones
// midiendo a la vez en un Edge sin confinar a una CPU— está escrita en el sub-benchmark concurrente.
//
// ─────────────────────────────────────────────────────────────────────────────
// MONOTÓNICO Y SIN RESET (y por qué eso NO es un detalle de implementación)
// ─────────────────────────────────────────────────────────────────────────────
// Los contadores SOLO SUBEN. Quien quiera el dato del intervalo resta su lectura anterior (ver Delta). El
// patrón alternativo —leer y poner a cero— tiene una ventana entre la lectura y el reset en la que las
// muestras que caen se PIERDEN sin dejar rastro, y es justo el fallo que un instrumento de medición no se
// puede permitir: mentiría a la baja precisamente cuando hay ráfaga, que es cuando importa.
//
// ─────────────────────────────────────────────────────────────────────────────
// EL VALOR PUBLICADO ES UNA COTA SUPERIOR, NUNCA UNA INTERPOLACIÓN
// ─────────────────────────────────────────────────────────────────────────────
// La rejilla tiene un BORDE EXACTO EN 50 ms a propósito: con él, «el 99 % de los entrantes se atendió en
// 50 ms o menos» se responde de forma EXACTA, sin estimar nada. Lo que se publica es el borde superior del
// bucket donde cae el percentil; interpolar dentro del bucket inventaría una precisión que la rejilla no
// tiene, y sobre un criterio de umbral eso es peor que redondear hacia arriba.
//
// Consecuencia honesta que hay que leer en el journal: con esta rejilla, `p99_ms=50` significa «el 99 %
// cupo en 50 ms o menos» y NO permite afirmar «< 50 ms». La afirmación estricta del criterio solo se
// sostiene con `p99_ms <= 40`.
package latencia

import (
	"math"
	"sync/atomic"
	"time"
)

// Camino es la POBLACIÓN a la que pertenece una medición. Son DOS series separadas, y separarlas es una
// decisión de medida, no de gusto:
//
//   - Medir SOLO lo que encola daría un p99 optimista y falso: una ráfaga descartada por la ventana
//     (ADR-0037) son miles de eventos que sí atan el hilo del socket, y no contarlos esconde ese trabajo.
//   - MEZCLARLAS es peor todavía: los descartes cuestan microsegundos, así que en una ráfaga con 90 % de
//     descartes DILUYEN el p99 por debajo aunque los encolados estén empeorando — el número mejoraría
//     justo cuando el Edge va peor.
//
// 🔴 INV-051.2 SE JUZGA CONTRA `Encolado`. `Descartado` existe para que nadie pueda leer el número bueno
// sin ver de qué población salió.
type Camino uint8

const (
	// Encolado es el camino que termina en la cola durable: el normal, el del fastlane, el del entrante
	// sin hora utilizable y el del Enqueue fallido (ese tiempo el handler lo gastó de verdad).
	Encolado Camino = iota
	// Descartado es el camino que termina SIN fila: el eco propio (IsFromMe) y el que cae fuera de la
	// ventana temporal del ADR-0037.
	Descartado
	// numCaminos cierra el enum; no es un camino.
	numCaminos
)

// String da el nombre del camino para logs y mensajes de test.
func (c Camino) String() string {
	switch c {
	case Encolado:
		return "encolado"
	case Descartado:
		return "descartado"
	default:
		return "desconocido"
	}
}

// NumBuckets es el tamaño de la rejilla. Es público porque Muestra lo expone en su array.
const NumBuckets = 16

// bordesUS es la rejilla, en MICROSEGUNDOS y por borde SUPERIOR INCLUSIVO: una muestra cae en el primer
// bucket cuyo borde sea >= a ella.
//
// 🔴 EL BORDE DE 50 ms ES EL QUE SOSTIENE EL CRITERIO (INV-051.2) y no se puede tocar sin rehacer la
// pregunta que este paquete existe para responder. Los de 40 y 75 lo flanquean para poder distinguir «va
// justo» de «se pasó», que con un solo borde serían indistinguibles.
//
// El último borde es el DESBORDAMIENTO (todo lo peor que 2,5 s). Existe para que un handler patológico no
// pueda esconderse dentro del último bucket normal aplastando el percentil.
var bordesUS = [NumBuckets]int64{
	500,       // 0,5 ms
	1_000,     // 1 ms
	2_000,     // 2 ms
	5_000,     // 5 ms
	10_000,    // 10 ms
	20_000,    // 20 ms
	30_000,    // 30 ms
	40_000,    // 40 ms  ← el «va justo»
	50_000,    // 50 ms  ← EL CRITERIO (INV-051.2)
	75_000,    // 75 ms  ← el «se pasó»
	100_000,   // 100 ms
	250_000,   // 250 ms
	500_000,   // 500 ms
	1_000_000, // 1 s
	2_500_000, // 2,5 s
	math.MaxInt64,
}

// etiquetas nombra cada bucket tal como sale al log (campo `p99_bucket`). Hace EXPLÍCITA la resolución de
// la medida: sin ella, un `p99_ms=50` se lee como «tardó 50 ms» en vez de «cupo en el tramo 40-50».
var etiquetas = [NumBuckets]string{
	"<=0.5", "0.5-1", "1-2", "2-5", "5-10", "10-20", "20-30", "30-40",
	"40-50", "50-75", "75-100", "100-250", "250-500", "500-1000", "1000-2500", ">2500",
}

// CotaDesbordada es lo que Corte.CotaMS vale cuando el percentil cae en el bucket de desbordamiento
// (>2,5 s): ahí NO existe cota superior y publicar el borde inferior (2500) sería publicar un número que
// se lee como una medida siendo lo contrario.
//
// Se usa un NEGATIVO a propósito: una duración negativa es estructuralmente imposible, así que nadie lo
// puede confundir con un valor medido, y —a diferencia de +Inf— sobrevive intacto a un handler JSON. El
// campo `p99_bucket` que va al lado dice lo que pasó de verdad (">2500").
const CotaDesbordada = -1.0

// serie es UNA población: sus buckets y su máximo.
type serie struct {
	buckets [NumBuckets]atomic.Uint64
	// maxUS es el máximo EXACTO en microsegundos, por CAS. Es lo único que no pierde resolución con la
	// rejilla, y es lo que permite ver que hubo un pico aunque el percentil lo aplaste. Monotónico como
	// los buckets: es el máximo DE TODA LA VIDA DEL PROCESO, no el del intervalo (ver Delta).
	maxUS atomic.Int64
}

// Histograma acumula las dos series. El cero es utilizable, y el NIL TAMBIÉN: todos sus métodos son
// nil-safe para que el listener pueda llevar el campo sin cablear y comportarse exactamente igual que
// antes de esta tarea.
//
// 🔴 NO ES UNA INTERFAZ, Y ESO ES DELIBERADO. Un `interface{ Observar(...) }` colgando del Listener sería
// la puerta por la que mañana alguien cuelga un exportador remoto en el camino caliente — exactamente la
// clase de cosa que INV-051.2 prohíbe y que el barrido de listener_camino_caliente_test.go vigila. Un
// puntero a tipo concreto no puede recibir una implementación que hable por la red.
type Histograma struct {
	series [numCaminos]serie
	// puerta son los CONTADORES DE DEGRADACIÓN de la puerta de entrada (T1.13), compartidos por todas las
	// sesiones igual que las series de arriba. Viven AQUÍ y no en un objeto propio porque este es el único
	// instrumento que ya viaja hasta cada Listener y que el latido ya lee: hospedarlos aquí publicó T1.13
	// sin un puerto nuevo, sin una opción nueva y sin tocar el cableado del daemon. Ver puerta.go.
	//
	// Es un VALOR, no un puntero: así `&Histograma{}` —que la doc de Nuevo declara igual de válido— sale
	// con sus contadores listos y no hay ningún camino en el que Puerta() devuelva algo sin construir.
	puerta Puerta
}

// Puerta da acceso a los contadores de la puerta de entrada de ESTE instrumento (T1.13). nil-safe: un
// histograma nil devuelve una Puerta nil, cuyos métodos son a su vez no-ops.
func (h *Histograma) Puerta() *Puerta {
	if h == nil {
		return nil
	}
	return &h.puerta
}

// Nuevo construye un histograma vacío. Existe por simetría con el resto del código; `&Histograma{}` es
// igual de válido.
func Nuevo() *Histograma { return &Histograma{} }

// Observar anota una duración en la serie del camino dado. Es lo único que corre en el hilo de whatsmeow:
// sin locks, sin asignaciones y sin ramas caras.
//
// nil ⇒ no-op (histograma no cableado). Un camino fuera de rango se ignora en vez de entrar en pánico: un
// instrumento de medida no puede ser nunca la causa de que se caiga la escucha.
func (h *Histograma) Observar(c Camino, d time.Duration) {
	if h == nil || c >= numCaminos {
		return
	}
	us := d.Microseconds()
	s := &h.series[c]
	s.buckets[bucketDe(us)].Add(1)
	for {
		old := s.maxUS.Load()
		if us <= old || s.maxUS.CompareAndSwap(old, us) {
			return
		}
	}
}

// bucketDe localiza el bucket de una muestra en microsegundos. Búsqueda lineal de 16 comparaciones: a
// esta escala es más rápida que una binaria (cabe en una línea de caché y no ramifica de forma
// impredecible), y las duraciones reales se concentran en los primeros buckets.
//
// Una muestra <= 0 (reloj monotónico que no avanzó dentro de la resolución) cae en el primer bucket, que
// es la lectura correcta: fue más rápida que la granularidad de la rejilla.
func bucketDe(us int64) int {
	for i, borde := range bordesUS {
		if us <= borde {
			return i
		}
	}
	return NumBuckets - 1
}

// Muestra es una FOTO de una serie: el reparto por buckets, el total y el máximo exacto. Es un valor puro
// (sin atómicos), así que se puede guardar, restar y comparar sin carreras.
type Muestra struct {
	// Buckets es el reparto por la rejilla de bordesUS.
	Buckets [NumBuckets]uint64
	// N es la población. Se DERIVA de la suma de Buckets y no de un contador aparte: así Σbuckets == N es
	// cierto por construcción y no hay ventana en la que el total y el reparto discrepen.
	N uint64
	// MaxUS es el máximo exacto en microsegundos. En una Muestra de intervalo (Delta) es el máximo
	// ACUMULADO, no el del intervalo: ver el porqué en Delta.
	MaxUS int64
}

// Snapshot toma la foto de una serie. Es una lectura PURA: no resetea nada (ver la cabecera del paquete).
func (h *Histograma) Snapshot(c Camino) Muestra {
	var m Muestra
	if h == nil || c >= numCaminos {
		return m
	}
	s := &h.series[c]
	for i := range s.buckets {
		v := s.buckets[i].Load()
		m.Buckets[i] = v
		m.N += v
	}
	m.MaxUS = s.maxUS.Load()
	return m
}

// Delta devuelve lo ocurrido ENTRE dos fotos. Es la mitad del patrón «monotónico + resta en el emisor»:
// el intervalo se calcula aquí, en frío, y el camino caliente nunca tiene que poner nada a cero.
//
// La resta se protege contra un `antes` posterior a `ahora` (llamada con los argumentos cambiados, o dos
// fotos de histogramas distintos) dejando el bucket en 0 en vez de dar la vuelta al uint64: un contador
// gigantesco en el log sería un diagnóstico peor que un cero.
//
// 🔴 MaxUS NO SE RESTA: se arrastra el ACUMULADO. Un máximo por intervalo exigiría poner el máximo a cero
// al leerlo, y este paquete no resetea nada — es la invariante de la que cuelga todo lo demás. Quien lea
// el log debe saber que el máximo publicado es el de toda la vida del proceso (y por eso el campo se
// llama `max_ms_acum`, no `max_ms`).
func Delta(antes, ahora Muestra) Muestra {
	var d Muestra
	for i := range ahora.Buckets {
		if ahora.Buckets[i] > antes.Buckets[i] {
			d.Buckets[i] = ahora.Buckets[i] - antes.Buckets[i]
		}
		d.N += d.Buckets[i]
	}
	d.MaxUS = ahora.MaxUS
	return d
}

// Corte es un percentil resuelto: el borde superior del bucket donde cae, y la etiqueta de ese bucket.
type Corte struct {
	// CotaMS es el borde SUPERIOR del bucket, en milisegundos: el percentil real es <= a este valor.
	// Vale CotaDesbordada (-1) si cayó en el bucket de desbordamiento, donde no hay cota superior.
	CotaMS float64
	// Bucket es la etiqueta legible del tramo (p. ej. "40-50"). Hace explícita la resolución.
	Bucket string
}

// Percentil resuelve el percentil q (0..1) de una muestra por RANGO MÁS CERCANO: el valor del elemento
// que ocupa la posición ceil(q·N) de la población ordenada.
//
// 🔴 POR QUÉ `ceil` Y NO OTRA COSA. Con ceil, «p99 <= X» equivale exactamente a «al menos el 99 % de las
// muestras cupo en X», que es la frase del criterio INV-051.2 palabra por palabra. Truncar movería el
// corte una posición hacia abajo y, en poblaciones pequeñas, dejaría fuera de la cuenta justamente la
// cola que el p99 vino a mirar.
//
// Devuelve ok=false con la población vacía. NO se inventa un cero: un percentil sin población no es «0 ms»,
// es «no hay dato», y confundir las dos cosas es la forma más fácil de publicar un verde falso.
func Percentil(m Muestra, q float64) (Corte, bool) {
	if m.N == 0 {
		return Corte{}, false
	}
	rango := uint64(math.Ceil(q * float64(m.N)))
	if rango == 0 {
		rango = 1
	}
	if rango > m.N {
		rango = m.N
	}
	var acumulado uint64
	for i := range m.Buckets {
		acumulado += m.Buckets[i]
		if acumulado >= rango {
			return corteDe(i), true
		}
	}
	// Inalcanzable (acumulado termina valiendo N >= rango), pero un percentil que devuelve el último
	// bucket es una respuesta mejor que un cero silencioso si algún día deja de serlo.
	return corteDe(NumBuckets - 1), true
}

// corteDe traduce un índice de bucket a su Corte publicable.
func corteDe(i int) Corte {
	if i >= NumBuckets-1 {
		return Corte{CotaMS: CotaDesbordada, Bucket: etiquetas[NumBuckets-1]}
	}
	return Corte{CotaMS: float64(bordesUS[i]) / 1000, Bucket: etiquetas[i]}
}

// MaxMS devuelve el máximo exacto de la muestra en milisegundos (0 si no hubo ninguna).
func (m Muestra) MaxMS() float64 { return float64(m.MaxUS) / 1000 }
