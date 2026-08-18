package latencia

// coste_test.go — LO QUE CUESTA MEDIR (Plan 051 Ola 3 · T3.13, cierre del hueco del presupuesto).
//
// 🔴 POR QUÉ EXISTE. La cabecera de latencia.go afirma un presupuesto —«~25 ns + ~5 ns + 5-10 ns», efecto
// observador por debajo del 0,1 %— y hasta este fichero ese número era PROSA: no había un solo Benchmark
// en el repo entero, así que la cifra que justifica meter un cronómetro en el hilo de whatsmeow no se
// había medido nunca. Un instrumento que se defiende con una estimación propia no se está defendiendo.
//
// El fichero tiene DOS piezas con papeles distintos, y conviene no confundirlas:
//
//   - los Benchmark DOCUMENTAN el coste. No aseveran: `go test` no los corre, y un benchmark que fallara
//     por pasarse de un umbral sería un test de rendimiento en una máquina compartida, es decir, un rojo
//     intermitente. El número se obtiene a mano (ver el comando de abajo) y se pega en el journal.
//   - el test de asignaciones SÍ ASEVERA. «Sin asignaciones» no es una cifra de rendimiento sino una
//     propiedad estructural del código: o el camino caliente asigna, o no. Si asignara, el coste dejaría
//     de ser el medido aquí y pasaría a depender del GC — que es justo lo que no se puede permitir un
//     handler que corre en el hilo del socket.
//
// ─────────────────────────────────────────────────────────────────────────────
// CÓMO SE OBTIENE EL NÚMERO (el comando, escrito para que nadie tenga que inventarlo)
// ─────────────────────────────────────────────────────────────────────────────
//
//	GOWORK=off go test ./internal/app/latencia/ -run '^$' -bench 'BenchmarkObservar' -benchmem -benchtime 2s
//
// `-run '^$'` deja fuera los tests (un benchmark no debe pagar el tiempo de la suite) y `-benchmem` es lo
// que hace visible el allocs/op que el test de abajo custodia. El presupuesto contra el que se lee es
// **150 ns/evento**: por debajo, el efecto observador sobre un handler cuyo acto caro es un INSERT
// cifrado en SQLite (milisegundos) es ruido.
//
// ⚠️ EL NÚMERO QUE IMPORTA EN CAMPO ES `camino_completo`, NO `Observar` a secas: el listener paga también
// el `time.Now()` de la entrada y el `time.Since` del defer, y en macOS esa pareja es la parte CARA de
// todo esto (el atómico y la búsqueda lineal son calderilla al lado). Publicar solo el de `Observar`
// sería publicar la mitad barata del gasto.

import (
	"testing"
	"time"
)

// durTipica es una muestra de 1,2 ms: la altura a la que vive un handler sano, y cae en un bucket BAJO
// (índice 2), que es donde la búsqueda lineal termina antes. Es el caso mayoritario real.
const durTipica = 1200 * time.Microsecond

// durDesbordada cae en el ÚLTIMO bucket (>2,5 s): es el peor caso de `bucketDe`, que recorre los 16
// bordes sin encontrar ninguno. Se mide aparte porque si el coste dependiera del bucket, el handler
// patológico —el que ya va mal— pagaría además el camino lento del instrumento.
const durDesbordada = 3 * time.Second

// BenchmarkObservar mide el camino caliente del cronómetro. Los sub-benchmarks no son variantes de gusto:
// cada uno responde una pregunta distinta sobre el presupuesto.
func BenchmarkObservar(b *testing.B) {
	// (a) La anotación sola, en el bucket bajo: el coste puro de la búsqueda lineal + el atomic.Add.
	b.Run("observar_bucket_bajo", func(b *testing.B) {
		h := Nuevo()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h.Observar(Encolado, durTipica)
		}
	})

	// (b) El bucket de DESBORDAMIENTO: las 16 comparaciones completas. Si esto se disparara respecto de
	// (a), el instrumento sería más caro precisamente cuando el handler ya va mal.
	b.Run("observar_desbordamiento", func(b *testing.B) {
		h := Nuevo()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h.Observar(Encolado, durDesbordada)
		}
	})

	// (c) EL NÚMERO DE CAMPO: lo que de verdad añade T3.13 a onMessage, relojes incluidos. Reproduce el
	// patrón del listener (`inicio := time.Now()` … `defer Observar(camino, time.Since(inicio))`) sin el
	// defer, que es coste del lenguaje y no del instrumento.
	b.Run("camino_completo", func(b *testing.B) {
		h := Nuevo()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			inicio := time.Now()
			h.Observar(Encolado, time.Since(inicio))
		}
	})

	// (d) CONCURRENTE, que es como se usa de verdad: N sesiones ⇒ N handlers de whatsmeow golpeando EL
	// MISMO histograma (uno por Edge, no uno por sesión). Aquí es donde se ve el rebote de la línea de
	// caché que la cabecera del paquete admite como riesgo (5-10 ns → 50-100 ns): todas las goroutines
	// escriben el MISMO bucket, que es el peor reparto posible y el que de verdad ocurre cuando el Edge
	// va bien (todas las muestras caen en el mismo tramo bajo).
	//
	// ⚠️ CÓMO SE LEE ESTE NÚMERO, PORQUE ENGAÑA. RunParallel reporta AGREGADO: ns/op = tiempo total ÷
	// operaciones totales, con todas las goroutines a la vez. La latencia que ve UNA goroutine es ese
	// número × GOMAXPROCS. En la medición de referencia (M1 Pro, GOMAXPROCS=8) el agregado da ~63 ns/op,
	// o sea ~505 ns por evento y goroutina: POR ENCIMA del presupuesto de 150 ns.
	//
	// 🔴 EL PRESUPUESTO SE CUMPLE, Y SE CUMPLE POR UNA CONDICIÓN DEL DESPLIEGUE — no porque este número
	// sea bueno. En el VPS de UAT los procesos del Edge están CONFINADOS A UNA SOLA CPU por un drop-in de
	// systemd (`/etc/systemd/system/wapp-edge.service.d/20-taskset.conf` → `CPUAffinity=5`, la otra mitad
	// del reparto 5/1 con Ollama en 0-4). Go respeta `sched_getaffinity`, así que allí GOMAXPROCS=1 y NO
	// PUEDE haber dos goroutines golpeando la misma línea de caché a la vez: el rebote que mide este
	// sub-benchmark es estructuralmente imposible en la topología desplegada, y el número que manda es el
	// de (c), ~90 ns. Se comprueba en el proceso VIVO, no en el fichero:
	//
	//	grep Cpus_allowed_list /proc/$(pgrep -f 'agent serve')/status
	//
	// (`systemctl show` dice lo que el fichero pide, no lo que el kernel concedió — la lección de T2.8.)
	//
	// 🔴 LA CONDICIÓN BAJO LA QUE ESTO DEJARÍA DE CUMPLIRSE, escrita para que se reconozca al llegar:
	// desplegar SIN el confinamiento (o ampliarlo a varias CPU) Y tener varias sesiones activas a la vez.
	// Ahí el histograma es COMPARTIDO por todo el Edge —uno por Edge, no uno por sesión—, así que N
	// handlers de whatsmeow escriben el mismo bucket en paralelo y el coste por evento sube al orden que
	// mide (d). Seguiría siendo despreciable frente al INSERT cifrado del handler, pero el presupuesto de
	// 150 ns/evento que esta tarea afirma ya no se sostendría, y habría que rehacerlo con el número de la
	// topología nueva antes de seguir apoyando INV-051.2 en él.
	//
	// Este sub-benchmark, por tanto, no mide el caso de campo: acota el techo teórico y deja escrito dónde
	// estaría el problema si el confinamiento desapareciera.
	b.Run("concurrente_mismo_bucket", func(b *testing.B) {
		h := Nuevo()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				h.Observar(Encolado, durTipica)
			}
		})
	})

	// (e) Concurrente con las DOS series repartidas: el contraste de (d). Dos buckets en líneas de caché
	// distintas dicen cuánto de (d) era contención y cuánto era trabajo.
	b.Run("concurrente_dos_series", func(b *testing.B) {
		h := Nuevo()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			c := Encolado
			for pb.Next() {
				h.Observar(c, durTipica)
				c ^= 1 // alterna Encolado/Descartado sin ramas
			}
		})
	})
}

// TestObservar_NoAsignaNADA custodia la cláusula «sin asignaciones» de la cabecera del paquete, que es la
// que sostiene todo lo demás: un `Observar` que asignara metería al GC en el hilo del socket, y entonces
// el coste dejaría de ser los ~100 ns del benchmark para pasar a ser «lo que tarde la próxima pausa».
// Sería además el peor sitio posible para descubrirlo: la asignación sería proporcional al VOLUMEN de
// entrantes, así que el instrumento empeoraría el número justo en la ráfaga que vino a medir.
//
// Se cubren las dos series y los dos extremos de la rejilla a propósito: la búsqueda lineal sale por un
// `return` distinto en el bucket de desbordamiento, y un boxing accidental ahí (p. ej. meter un `%v` en
// una traza de diagnóstico dentro del camino) solo se vería con esa muestra.
//
// `testing.AllocsPerRun` fija GOMAXPROCS a 1 mientras mide, así que este test no puede ser `t.Parallel()`
// ni fiarse de nada concurrente: mide la FORMA del código, no su comportamiento bajo carga.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas): cualquier cosa que escape al heap dentro de Observar —
// p. ej. cambiar la firma a `Observar(c Camino, d time.Duration, extra any)` y pasarle el bucket, o
// guardar la muestra en un slice de diagnóstico.
func TestObservar_NoAsignaNADA(t *testing.T) {
	h := Nuevo()

	casos := []struct {
		nombre string
		camino Camino
		d      time.Duration
	}{
		{"encolado_bucket_bajo", Encolado, durTipica},
		{"descartado_microsegundos", Descartado, 30 * time.Microsecond},
		{"encolado_desbordamiento", Encolado, durDesbordada},
		{"descartado_desbordamiento", Descartado, durDesbordada},
	}

	for _, cs := range casos {
		c, d := cs.camino, cs.d
		asignaciones := testing.AllocsPerRun(1000, func() { h.Observar(c, d) })
		if asignaciones != 0 {
			t.Errorf("%s: Observar asignó %.2f veces por llamada, se exige 0.\n"+
				"    CONSECUENCIA: el cronómetro corre EN EL HILO DE WHATSMEOW y una asignación por evento "+
				"mete presión de GC proporcional al volumen de entrantes — el instrumento pasaría a empeorar "+
				"el p99 que INV-051.2 mide, y lo haría en la ráfaga, que es cuando el número importa.\n"+
				"    SI EL CAMBIO ES DELIBERADO: no basta con subir el umbral; hay que rehacer el análisis de "+
				"coste de la cabecera de latencia.go y volver a medir con `-bench BenchmarkObservar -benchmem`.",
				cs.nombre, asignaciones)
		}
	}
}
