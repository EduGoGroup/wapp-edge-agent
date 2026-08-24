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
	"sync"
	"sync/atomic"
	"time"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
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

// histogramaInferencia acumula latencias de inferencia del proceso. POR PROCESO y nunca por cola: el
// breaker, el semáforo y este histograma describen a Ollama, que es uno por máquina (ver Deps.Colas).
// Partirlo por cola daría N series de las que ninguna tendría muestras suficientes para un percentil,
// midiendo además el mismo Ollama N veces.
//
// ⚠️ DESDE T1.7-5 HAY TRES INSTANCIAS DE ESTE TIPO, NO UNA, Y NO SE MEZCLAN: el TOTAL (Cajero.inferencia,
// que es el que viaja como `intent_p50_ms` desde T4.5), el PREFILL y la GENERACIÓN. La rejilla les sirve
// a las tres —las tres viven en el mismo rango, de las décimas de segundo a los dos minutos— pero sus
// poblaciones son distintas: el total se alimenta también en el camino de FALLO y las otras dos sólo en
// el de éxito, así que sus `muestras()` NO tienen por qué cuadrar y restarlas no da «lo que fallaron».
// El porqué de esa asimetría está en Cajero.observarFases.
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
	h.observarMS(d.Milliseconds())
}

// observarMS es la misma anotación con la muestra YA en milisegundos. Existe porque desde T1.7-5 hay dos
// poblaciones cuyo origen es un número de Ollama, no un cronómetro de Go: el prefill y la generación
// llegan ya convertidos (ollama.Metrics.PromptMs y ChatResponse.EvalDuration), y envolverlos en un
// time.Duration para que este método volviera a dividirlos sería una ida y vuelta que sólo puede perder
// precisión.
func (h *histogramaInferencia) observarMS(ms int64) {
	if h == nil {
		return
	}
	h.buckets[bucketInferenciaDe(ms)].Add(1)
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

// ─────────────────────────────────────────────────────────────────────────────
// EL CALOR DEL PREFIJO (Plan 044 · Ola 1.7 · T1.7-5)
// ─────────────────────────────────────────────────────────────────────────────
//
// 🔴 LOS UMBRALES SON POLÍTICA DEL EMISOR Y NO VIAJAN EN EL PROTO. El contrato transporta el reparto YA
// HECHO (`SessionHealth.inference_by_regime`, un mapa) precisamente para que estos dos números se puedan
// mover con el hardware del cliente sin cortar una versión del contrato ni bumpear dos consumidores.
//
// 🔴 Y SON CONFIGURACIÓN, NO CONSTANTES (WAPP_WORKER_PREFILL_FRIO_MS / _CALIENTE_MS). El conteo del
// régimen `templado` existe justamente para decir «estos dos números ya no parten bien la población de
// ESTA máquina»; si recalibrarlos exigiera recompilar y redesplegar, la señal llegaría semanas después de
// la pregunta — que es lo que hizo que este problema tardara tanto en verse. Los valores de abajo son sólo
// el DEFAULT, y el efectivo se lee en la línea `cajero: arrancando`.

const (
	// DefaultPrefillFrioMS es el borde por encima del cual un prefill se considera FRÍO: la caché de
	// prefijos del runner no tenía nada aprovechable y hubo que digerir el prompt entero.
	//
	// 5 s SALE DE LA MEDICIÓN, no de un número redondo: con el prefijo frío el prefill cuesta ~21,6 ms por
	// token, así que 5 s son ~230 tokens — por debajo del prompt más pequeño que el Cloud construye. Un
	// prefill que pasa de ahí no puede haber tenido un acierto de caché apreciable.
	//
	// ⚠️ MEDIDO CONTRA UNA MÁQUINA (el VPS de UAT, agosto de 2026). En el equipo de un cliente —otra CPU,
	// otro modelo, otro tamaño de prompt— la frontera cae en otro sitio, y por eso es configurable.
	DefaultPrefillFrioMS = 5_000
	// DefaultPrefillCalienteMS es el borde por debajo del cual un prefill se considera CALIENTE: la caché
	// tenía el prefijo y sólo hubo que digerir la cola del prompt.
	//
	// 2 s ES EL TECHO MEDIDO DEL CASO CALIENTE (0,1-1,2 s el prompt entero), con margen: por debajo de ahí
	// no cabe un prefill frío de un prompt real.
	DefaultPrefillCalienteMS = 2_000
)

// umbralesRegimen es LA PAREJA de bordes con la que se clasifica el calor del prefijo.
//
// 🔴 LOS DOS EN UN TIPO Y NO COMO DOS CAMPOS SUELTOS DEL CAJERO, por la misma razón por la que el cuantil
// viaja pegado a su muestra: sólo significan algo JUNTOS. Sueltos, nada impide construir un cajero con
// `caliente = 5 s` y `frio = 2 s` —una configuración INVERTIDA que no da error y hace desaparecer el
// régimen `templado` entero, dejando el reparto mintiendo en silencio—. Con un tipo, la única forma de
// obtener una pareja es su constructor, y ahí vive la validación.
type umbralesRegimen struct {
	calienteMS int64
	frioMS     int64
}

// nuevosUmbralesRegimen resuelve los defaults y RECHAZA una pareja inconsistente.
//
// LOS DOS GUARDARRAÍLES, y el segundo es el que importa:
//
//   - `<=0 ⇒ default`, como el resto de los números de Deps. Un umbral de cero o negativo no es una
//     política, es un campo sin poblar.
//   - CALIENTE < FRÍO, o los DOS caen al default. No se «arregla» uno solo: con `caliente >= frio` la
//     franja del medio no existe y `regimenDe` clasificaría todo lo que pasa de `frio` como frío y el
//     resto como caliente — el reparto seguiría saliendo, con tres claves, y una de ellas clavada a 0
//     para siempre. Corregir sólo un borde dejaría una tercera configuración que nadie ha revisado; caer
//     los dos al default deja al operador con la que sí está medida, y el WARN le dice por qué.
func nuevosUmbralesRegimen(caliente, frio time.Duration, log sharedlogger.Logger) umbralesRegimen {
	calienteMS, frioMS := caliente.Milliseconds(), frio.Milliseconds()
	if calienteMS <= 0 {
		calienteMS = DefaultPrefillCalienteMS
	}
	if frioMS <= 0 {
		frioMS = DefaultPrefillFrioMS
	}
	if calienteMS >= frioMS {
		log.Warn("cajero: los umbrales de régimen del prefijo están INVERTIDOS o pegados "+
			"(caliente >= frio); se usan los DOS defaults. Con esa pareja el régimen `templado` no puede "+
			"darse nunca y el reparto del heartbeat publicaría una clave clavada a 0 sin decir por qué",
			"prefill_caliente_ms_pedido", calienteMS, "prefill_frio_ms_pedido", frioMS,
			"prefill_caliente_ms", DefaultPrefillCalienteMS, "prefill_frio_ms", DefaultPrefillFrioMS)
		return umbralesRegimen{calienteMS: DefaultPrefillCalienteMS, frioMS: DefaultPrefillFrioMS}
	}
	return umbralesRegimen{calienteMS: calienteMS, frioMS: frioMS}
}

const (
	// RegimenFrio: prefill > PrefillFrioMS. Se pagó el arranque en frío entero.
	RegimenFrio = "frio"
	// RegimenTemplado: prefill en [caliente, frio]. LA FRANJA QUE LOS UMBRALES DEL PLAN NO CUBRÍAN, y por
	// eso tiene nombre propio en vez de repartirse entre las otras dos.
	//
	// 🔴 SU CONTADOR ES LA SEÑAL DE QUE ESTOS UMBRALES PIDEN RECALIBRARSE, y es la razón de que exista.
	// Los dos bordes de arriba están calibrados contra UNA máquina (el VPS de UAT, agosto de 2026); en el
	// equipo de un cliente —otra CPU, otro modelo, otro tamaño de prompt— la frontera real entre «caché
	// caliente» y «caché fría» cae en otro sitio. Mientras `templado` sea residual, los bordes describen
	// bien esa máquina. En cuanto se lleve una fracción apreciable del tráfico, lo que hay que mover son
	// PrefillFrioMS y PrefillCalienteMS — no es que haya aparecido un tercer fenómeno físico, es que los
	// dos números dejaron de partir la población donde de verdad se parte — y por eso son CONFIGURABLES:
	// la respuesta a esa señal es mover los bordes en el `.env` de esa máquina, no recompilar.
	//
	// ⚠️ NO ES «no se sabe»: un prefill de 3 s ES un dato medido, y describe un acierto PARCIAL de caché
	// (el prefijo estaba, la cola del prompt no). Lo que no se sabe no se cuenta en ningún régimen — ver
	// Cajero.observarFases.
	RegimenTemplado = "templado"
	// RegimenCaliente: prefill < PrefillCalienteMS. La caché de prefijos tenía el prefijo del tenant.
	RegimenCaliente = "caliente"
)

// RegimenesInferencia es la lista de regímenes CONOCIDOS, del más caro al más barato. La usa el desglose
// del heartbeat para sembrar las tres claves a cero: un régimen a 0 es un DATO («esta hora no pagó ni un
// arranque en frío»), y un hueco en el mapa obligaría al consumidor a adivinar si es cero o si este Edge
// no lo mide.
var RegimenesInferencia = []string{RegimenFrio, RegimenTemplado, RegimenCaliente}

// regimenDe clasifica un prefill medido en ms. Los bordes son ESTRICTOS por los dos lados, así que el
// valor exacto de cualquiera de los dos umbrales cae en `templado`: la franja del medio es CERRADA a
// propósito, para que ningún valor pueda quedar sin régimen por un `>=` mal puesto.
//
// ⚠️ NO SE LLAMA CON UN PREFILL <= 0. Ese caso es «no medible» y se descarta antes (observarFases): darle
// aquí un régimen lo metería en `caliente`, que es la mentira más cómoda de creer — un Ollama que dejara
// de devolver `prompt_eval_duration` se vería como una máquina con la caché siempre caliente.
func (u umbralesRegimen) regimenDe(prefillMS int64) string {
	switch {
	case prefillMS > u.frioMS:
		return RegimenFrio
	case prefillMS < u.calienteMS:
		return RegimenCaliente
	default:
		return RegimenTemplado
	}
}

// observarFases anota las DOS fases de una inferencia que salió bien y devuelve el régimen con el que se
// clasificó el prefill, o "" si no era medible (para el log).
//
// 🔴 SÓLO SE LLAMA EN EL CAMINO DE ÉXITO, y es la asimetría con `histogramaInferencia.observar`, que se
// alimenta en los dos. No es una excepción de conveniencia: prefill y generación son NÚMEROS QUE DEVUELVE
// EL PROVEEDOR EN SU RESPUESTA, y en el camino de fallo no hay respuesta de la que leerlos. Un timeout no
// es «un prefill que tardó mucho»: es una petición de la que no sabemos NADA de su reparto interno.
// Inventar ahí una muestra —imputarle todo el plazo al prefill, por ejemplo— envenenaría justo el número
// que esta tarea existe para limpiar.
//
// 🔴 UN VALOR <= 0 NO ES UNA MUESTRA. `prompt_eval_duration` lo devuelve Ollama SIEMPRE, así que un cero
// significa que la respuesta no lo traía (versión del proveedor, respuesta recortada) y no que el prefill
// fuera instantáneo. Contarlo como 0 ms metería una muestra falsa en el bucket más bajo y, peor, sumaría
// al régimen `caliente`: una máquina que perdió el dato se vería como una máquina que va de maravilla.
// Descartándolo, la ausencia viaja como ausencia (`samples` sin crecer ⇒ el heartbeat publica «no
// medible»), que es la única lectura honesta.
//
// LOS CALENTAMIENTOS SÍ ENTRAN AQUÍ, al revés que en el breaker. La regla del repo es que un calentamiento
// se mide EXACTAMENTE IGUAL que todo lo demás y sólo se excluye de UNA cosa —la salud del proveedor—; una
// segunda exclusión no escrita en ninguna parte sería peor que la consecuencia que tiene. Y la
// consecuencia hay que saber leerla: un calentamiento paga prefill FRÍO POR DISEÑO, así que en una máquina
// ociosa con calentamientos periódicos el contador de `frio` sube sin que ningún cliente haya esperado.
func (c *Cajero) observarFases(prefillMS, generacionMS int64) string {
	if c == nil {
		return ""
	}
	var regimen string
	if prefillMS > 0 {
		c.prefill.observarMS(prefillMS)
		regimen = c.umbrales.regimenDe(prefillMS)
		c.porRegimen.contar(regimen)
	}
	// LA GENERACIÓN SE JUZGA POR SU CUENTA y no dentro del `if` de arriba: son dos números independientes
	// del proveedor y uno puede faltar sin el otro. Anidarlos haría que perder el prefill borrase también
	// la generación, que sí se midió.
	if generacionMS > 0 {
		c.generacion.observarMS(generacionMS)
	}
	return regimen
}

// ─────────────────────────────────────────────────────────────────────────────
// EL CONTADOR ETIQUETADO — un mapa acumulado y monótono
// ─────────────────────────────────────────────────────────────────────────────

// contadorEtiquetado cuenta sucesos por etiqueta. Es el gemelo abierto del histograma de arriba: donde
// aquél tiene una rejilla fija de 16 buckets, éste tiene un mapa que puede crecer.
//
// POR QUÉ UN MUTEX Y NO ATÓMICOS, que es lo que usa todo lo demás de este fichero: porque el conjunto de
// etiquetas tiene que poder CRECER sin recompilar a los consumidores —es la propiedad por la que el
// contrato transporta un `map<string,int64>` y no un contador por categoría—, y un mapa que crece no se
// puede leer sin candado. El precio es nulo aquí: se toca UNA VEZ POR INFERENCIA, y una inferencia son
// segundos. Esto no es el camino caliente del socket de entrantes (INV-051.2), donde el mismo candado
// habría sido inaceptable.
//
// MONÓTONO Y SIN RESET, igual que el histograma y por el mismo motivo: el patrón «leer y poner a cero»
// pierde las muestras que caen entre la lectura y el reset, y miente a la baja justo cuando hay ráfaga.
// Consecuencia honesta: lo que viaja en el parte es EL ACUMULADO DE TODA LA VIDA DEL PROCESO, y la
// ventana la hace el consumidor con un `rate()`.
//
// El cero-valor NO sirve (el mapa sería nil): construir con nuevoContador. El NIL sí es utilizable —todos
// los métodos son nil-safe—, para que un Cajero armado sin estos campos se comporte como antes de T1.7-5.
type contadorEtiquetado struct {
	mu sync.Mutex
	n  map[string]int64
}

// nuevoContador construye el contador con las etiquetas conocidas SEMBRADAS A CERO.
//
// 🔴 LA SIEMBRA ES EL PUNTO. Sin ella, «este proceso no ha visto ni un arranque en frío» y «este Edge no
// mide el régimen» llegarían al consumidor como la misma cosa: una clave ausente. Con ella, la ausencia
// de la clave significa una sola cosa —el emisor no conoce esa categoría—, y el cero significa lo que
// tiene que significar. Es el mismo criterio con el que el desglose por motivo publica sus ocho claves
// siempre (INV-051.3).
func nuevoContador(etiquetas ...string) *contadorEtiquetado {
	c := &contadorEtiquetado{n: make(map[string]int64, len(etiquetas))}
	for _, e := range etiquetas {
		c.n[e] = 0
	}
	return c
}

// contar suma uno a la etiqueta. Una etiqueta que no se sembró se crea: el mapa es abierto a propósito
// (ver el doc del tipo).
func (c *contadorEtiquetado) contar(etiqueta string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = make(map[string]int64, 1)
	}
	c.n[etiqueta]++
}

// foto devuelve una COPIA del acumulado. Copia y no el mapa interno: lo que sale de aquí viaja a otro
// proceso por el parte y se recorre mientras esta goroutine puede estar contando — devolver el mapa vivo
// sería una carrera que `-race` caza el día que dos inferencias terminen a la vez.
//
// Devuelve nil si no hay contador (nil-safe), que el escritor del parte traduce a «no medible».
func (c *contadorEtiquetado) foto() map[string]int64 {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		return nil
	}
	copia := make(map[string]int64, len(c.n))
	for k, v := range c.n {
		copia[k] = v
	}
	return copia
}

// numPredictPorDefecto es el tope de tokens de salida que el Edge aplica cuando el Cloud no manda
// `max_output_tokens`. Se lee del MAPA DE OPCIONES que de verdad viaja al proveedor y no de un campo
// aparte: un segundo sitio con el mismo número es un sitio donde el latido puede decir 256 mientras la
// petición manda otra cosa, que es justo la clase de divergencia que este latido existe para descartar.
//
// 0 = el Edge no fija ninguno (mapa sin la clave, o con un valor de un tipo que no es entero) y manda el
// default del proveedor. No es un valor alcanzable desde el cableado real.
func (c *Cajero) numPredictPorDefecto() int {
	if c == nil {
		return 0
	}
	v, ok := c.opciones["num_predict"]
	if !ok {
		return 0
	}
	n, ok := v.(int)
	if !ok {
		return 0
	}
	return n
}
