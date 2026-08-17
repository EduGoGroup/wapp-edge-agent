package latencia

// latencia_test.go — los tests del INSTRUMENTO (Plan 051 Ola 3 · T3.13).
//
// Regla de la casa: un test que custodia una invariante tiene que ponerse ROJO al romper a propósito lo
// que dice proteger. Cada test de aquí lleva escrita su mutación, y todas se ejecutaron de verdad.

import (
	"sync"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// LA REJILLA
// ─────────────────────────────────────────────────────────────────────────────

// TestRejilla_EsEstrictamenteCrecienteYCadaBucketEsAlcanzable custodia la FORMA de la rejilla, no sus
// valores. Es el único test que sigue vigilando aunque alguien reescriba entero el resto del fichero.
//
// 🔴 POR QUÉ HACE FALTA, Y CON UN CASO REAL. En la prueba por mutación de T3.13 se metieron tres bordes
// con el MISMO valor y el paquete se lo tragó EN SILENCIO: `bucketDe` devuelve el primero que cumple
// `us <= borde`, así que los duplicados quedan como BUCKETS MUERTOS —existen, salen en las etiquetas, y no
// pueden recibir una sola muestra jamás—. Nada falla; simplemente la resolución de la medida se degrada
// sin avisar, y un p99 se publica con la etiqueta de un tramo que no es el suyo.
//
// Las dos invariantes son las dos caras de lo mismo:
//
//   - ESTRICTAMENTE CRECIENTE (`<`, no `<=`) es la propiedad estructural: un borde repetido —o menor que
//     el anterior— rompe la partición.
//   - CADA BUCKET ALCANZABLE la ata a su CONSUMIDOR: `bucketDe(bordesUS[i])` tiene que devolver `i`, o ese
//     bucket es inalcanzable por construcción. Se comprueban las dos porque la primera sin la segunda deja
//     la propiedad como una afirmación sobre un array, y lo que importa es lo que hace la función.
//
// Las etiquetas van en el mismo test porque son parte de la misma rejilla: una etiqueta vacía o repetida
// se publica en `p99_bucket` y hace ILEGIBLE la única línea sobre la que se juzga INV-051.2.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas): duplicar un borde (p. ej. poner 40_000 en el hueco de
// 50_000) ⇒ rojo por las dos invariantes; vaciar o repetir una etiqueta ⇒ rojo por la tercera.
func TestRejilla_EsEstrictamenteCrecienteYCadaBucketEsAlcanzable(t *testing.T) {
	for i := 0; i+1 < len(bordesUS); i++ {
		if bordesUS[i] >= bordesUS[i+1] {
			t.Errorf("la rejilla no es estrictamente creciente en %d: bordesUS[%d]=%d >= bordesUS[%d]=%d — "+
				"un borde repetido o menor deja BUCKETS MUERTOS que no pueden recibir una muestra",
				i, i, bordesUS[i], i+1, bordesUS[i+1])
		}
	}

	for i, borde := range bordesUS {
		// El borde es SUPERIOR E INCLUSIVO: su propio valor tiene que caer en SU bucket. Si cae en otro, ese
		// tramo es inalcanzable y la etiqueta que se publique de él será mentira.
		if got := bucketDe(borde); got != i {
			t.Errorf("el bucket %d (%q, borde %d µs) es INALCANZABLE: bucketDe(%d) = %d",
				i, etiquetas[i], borde, borde, got)
		}
	}

	// La igualdad de tamaños hoy la garantiza el compilador (los dos son `[NumBuckets]`), y se afirma igual:
	// es la invariante que sostiene el indexado cruzado de `corteDe`, y si mañana alguno pasa a ser un slice
	// —para una rejilla configurable, p. ej.— este test es el que lo caza en el mismo commit.
	if len(etiquetas) != len(bordesUS) {
		t.Fatalf("hay %d etiquetas para %d bordes: `corteDe` indexa las dos con el mismo entero",
			len(etiquetas), len(bordesUS))
	}
	vistas := make(map[string]int, len(etiquetas))
	for i, e := range etiquetas {
		if e == "" {
			t.Errorf("la etiqueta del bucket %d está vacía: `p99_bucket` saldría en blanco justo en la línea "+
				"sobre la que se juzga el criterio de la ola", i)
		}
		if antes, dup := vistas[e]; dup {
			t.Errorf("la etiqueta %q está repetida (buckets %d y %d): dos tramos distintos publicarían el "+
				"mismo `p99_bucket` y la resolución de la medida dejaría de ser legible", e, antes, i)
		}
		vistas[e] = i
	}
}

// TestBucketDe_ElBordeDe50EsExactoYCerrado es EL TEST DE LA REJILLA, y el borde de 50 ms es el motivo
// entero por el que este paquete existe: sin un borde EXACTO ahí, «p99 < 50 ms» (INV-051.2) deja de tener
// respuesta y pasa a ser una estimación.
//
// Los bordes son SUPERIORES E INCLUSIVOS: 50,000 µs cae en el bucket "40-50", 50,001 µs ya cae en "50-75".
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - cambiar `us <= borde` por `us < borde` en bucketDe ⇒ los 50.000 µs exactos se van a "50-75".
//   - fusionar los buckets 40/50/75 en uno solo [40,100) ⇒ los cuatro casos del entorno del criterio
//     colapsan en el mismo tramo y el veredicto se vuelve inservible.
func TestBucketDe_ElBordeDe50EsExactoYCerrado(t *testing.T) {
	casos := []struct {
		us     int64
		bucket string
		cotaMS float64
		porQue string
	}{
		{us: -1, bucket: "<=0.5", cotaMS: 0.5, porQue: "una muestra no positiva fue más rápida que la rejilla, no un error"},
		{us: 0, bucket: "<=0.5", cotaMS: 0.5, porQue: "idem: el reloj monotónico no avanzó dentro de la resolución"},
		{us: 500, bucket: "<=0.5", cotaMS: 0.5, porQue: "el borde es INCLUSIVO"},
		{us: 501, bucket: "0.5-1", cotaMS: 1, porQue: "un microsegundo por encima ya es el bucket siguiente"},
		{us: 40_000, bucket: "30-40", cotaMS: 40, porQue: "40 ms exactos: el último tramo que permite AFIRMAR < 50"},
		{us: 40_001, bucket: "40-50", cotaMS: 50, porQue: "el tramo que roza el criterio"},
		{us: 50_000, bucket: "40-50", cotaMS: 50, porQue: "🔴 50 ms EXACTOS caen DENTRO del criterio, no fuera"},
		{us: 50_001, bucket: "50-75", cotaMS: 75, porQue: "🔴 un microsegundo por encima de 50 ms ya se pasó"},
		{us: 2_500_000, bucket: "1000-2500", cotaMS: 2500, porQue: "el último tramo con cota superior"},
		{us: 2_500_001, bucket: ">2500", cotaMS: CotaDesbordada, porQue: "desbordamiento: no hay cota superior que publicar"},
	}
	for _, c := range casos {
		i := bucketDe(c.us)
		got := corteDe(i)
		if got.Bucket != c.bucket || got.CotaMS != c.cotaMS {
			t.Errorf("bucketDe(%d µs) = %q/%v, se esperaba %q/%v — %s",
				c.us, got.Bucket, got.CotaMS, c.bucket, c.cotaMS, c.porQue)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// EL PERCENTIL
// ─────────────────────────────────────────────────────────────────────────────

// observarMS llena un histograma con `veces` muestras de `ms` milisegundos.
func observarMS(h *Histograma, c Camino, ms float64, veces int) {
	d := time.Duration(ms * float64(time.Millisecond))
	for i := 0; i < veces; i++ {
		h.Observar(c, d)
	}
}

// TestPercentil_SobrePoblacionConocida fija la DEFINICIÓN del percentil: rango más cercano con `ceil`, y
// el bucket es el primero cuyo acumulado ALCANZA ese rango (>=, no >).
//
// Las dos poblaciones están elegidas para que cada mutación caiga en una de ellas:
//
//   - «el borde justo» (n=1000, q·n=990 entero) distingue `>=` de `>`: con 990 muestras rápidas, el
//     acumulado IGUALA el rango, así que el p99 es el bucket rápido. Cambiar `>=` por `>` lo empuja a la
//     cola y devuelve 250 ms.
//   - «rango fraccionario» (n=150, q·n=148,5) distingue `ceil` de truncar: con ceil el rango es 149 y las
//     148 muestras rápidas NO llegan, así que el p99 es la cola; truncando a 148 sí llegarían y devolvería
//     el bucket rápido.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO: `math.Ceil` → truncado (o `math.Floor`) ⇒ falla el caso
// fraccionario; `acumulado >= rango` → `acumulado > rango` ⇒ falla el caso del borde justo.
func TestPercentil_SobrePoblacionConocida(t *testing.T) {
	casos := []struct {
		nombre    string
		rapidasMS float64
		rapidas   int
		lentasMS  float64
		lentas    int
		q         float64
		bucket    string
		cotaMS    float64
	}{
		{
			nombre: "el borde justo: 990 de 1000 rapidas y el p99 NO ve la cola",
			// 99 % exacto de la población cabe en 1 ms ⇒ el p99 es 1 ms. Que la intuición diga «pero hay un
			// 1 % lento» es justo el error que este caso fija: el p99 es el corte del 99 %, no el máximo.
			rapidasMS: 1, rapidas: 990, lentasMS: 200, lentas: 10,
			q: 0.99, bucket: "0.5-1", cotaMS: 1,
		},
		{
			nombre:    "rango fraccionario: 148 de 150 no llegan al corte y el p99 SI ve la cola",
			rapidasMS: 1, rapidas: 148, lentasMS: 200, lentas: 2,
			q: 0.99, bucket: "100-250", cotaMS: 250,
		},
		{
			nombre:    "la mediana no la mueve la cola",
			rapidasMS: 1, rapidas: 990, lentasMS: 200, lentas: 10,
			q: 0.50, bucket: "0.5-1", cotaMS: 1,
		},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			h := Nuevo()
			observarMS(h, Encolado, c.rapidasMS, c.rapidas)
			observarMS(h, Encolado, c.lentasMS, c.lentas)

			m := h.Snapshot(Encolado)
			if want := uint64(c.rapidas + c.lentas); m.N != want {
				t.Fatalf("N = %d, se esperaba %d: el total se DERIVA de la suma de buckets", m.N, want)
			}
			got, ok := Percentil(m, c.q)
			if !ok {
				t.Fatalf("Percentil devolvió ok=false con %d muestras", m.N)
			}
			if got.Bucket != c.bucket || got.CotaMS != c.cotaMS {
				t.Fatalf("p%v = %q/%v, se esperaba %q/%v", c.q*100, got.Bucket, got.CotaMS, c.bucket, c.cotaMS)
			}
		})
	}
}

// TestPercentil_SinPoblacionNoEsCero: un percentil sin muestras es «no hay dato», NUNCA «0 ms». Publicar
// un cero ahí es la forma más barata de colgar un verde falso en el journal.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: devolver `Corte{}, true` con N == 0.
func TestPercentil_SinPoblacionNoEsCero(t *testing.T) {
	h := Nuevo()
	if _, ok := Percentil(h.Snapshot(Encolado), 0.99); ok {
		t.Fatal("con la población vacía el percentil debe decir «no hay dato» (ok=false), no devolver 0 ms")
	}
}

// TestPercentil_VeredictoBinarioDelCriterio es el test que juzga LA PREGUNTA DE LA OLA: «¿el handler
// atendió el 99 % de los entrantes en menos de 50 ms?».
//
// Con la rejilla, la respuesta se lee así y solo así:
//   - cota <= 40  ⇒ SÍ se puede afirmar «< 50 ms».
//   - cota == 50  ⇒ el 99 % cupo en 50 ms o menos, pero NO se puede afirmar «< 50»: es el borde.
//   - cota >= 75  ⇒ NO cumple.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: fusionar los buckets 40/50/75 en uno solo [40,100) (es decir, quitar
// los bordes 40.000, 50.000 y 75.000 y dejar 100.000). Los dos primeros casos pasarían a devolver 100 y
// el veredicto quedaría ambiguo justo en la única franja donde hay que decidir.
func TestPercentil_VeredictoBinarioDelCriterio(t *testing.T) {
	casos := []struct {
		nombre    string
		rapidasMS float64
		lentasMS  float64
		cotaMS    float64
		bucket    string
		cumple    bool
	}{
		{nombre: "99 a 39 ms: se puede AFIRMAR que cumple", rapidasMS: 39, lentasMS: 200, cotaMS: 40, bucket: "30-40", cumple: true},
		{nombre: "99 a 49 ms: el borde, NO se puede afirmar", rapidasMS: 49, lentasMS: 200, cotaMS: 50, bucket: "40-50", cumple: false},
		{nombre: "99 a 51 ms: NO cumple", rapidasMS: 51, lentasMS: 200, cotaMS: 75, bucket: "50-75", cumple: false},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			h := Nuevo()
			observarMS(h, Encolado, c.rapidasMS, 99)
			observarMS(h, Encolado, c.lentasMS, 1)

			got, ok := Percentil(h.Snapshot(Encolado), 0.99)
			if !ok {
				t.Fatal("Percentil devolvió ok=false con 100 muestras")
			}
			if got.CotaMS != c.cotaMS || got.Bucket != c.bucket {
				t.Fatalf("p99 = %v/%q, se esperaba %v/%q", got.CotaMS, got.Bucket, c.cotaMS, c.bucket)
			}
			// La afirmación del criterio, escrita como tal: es la frase que se pega en el journal.
			if cumple := got.CotaMS > 0 && got.CotaMS < 50; cumple != c.cumple {
				t.Fatalf("veredicto «p99 < 50 ms» = %v, se esperaba %v (cota %v ms, tramo %q)",
					cumple, c.cumple, got.CotaMS, got.Bucket)
			}
		})
	}
}

// TestPercentil_DesbordamientoNoSeDisfrazaDeMedida: por encima de 2,5 s no hay cota superior, y publicar
// el borde inferior (2500) sería publicar un número que se lee como una medida siendo lo contrario.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: hacer que corteDe devuelva 2500 (o el borde de bordesUS) para el último
// bucket en vez del centinela.
func TestPercentil_DesbordamientoNoSeDisfrazaDeMedida(t *testing.T) {
	h := Nuevo()
	observarMS(h, Encolado, 4000, 10)

	got, ok := Percentil(h.Snapshot(Encolado), 0.99)
	if !ok {
		t.Fatal("Percentil devolvió ok=false con 10 muestras")
	}
	if got.CotaMS != CotaDesbordada {
		t.Fatalf("CotaMS = %v: en el desbordamiento debe ir el centinela %v, no un número que parezca medido",
			got.CotaMS, CotaDesbordada)
	}
	if got.Bucket != ">2500" {
		t.Fatalf("Bucket = %q, se esperaba \">2500\": la etiqueta es lo único que dice qué pasó", got.Bucket)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// LAS DOS SERIES
// ─────────────────────────────────────────────────────────────────────────────

// TestObservar_LasDosSeriesNoSeMezclan: mezclar encolados y descartes daría un p99 diluido —los descartes
// son de microsegundos— y el número mejoraría justo cuando el Edge va peor.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: ignorar el Camino en Observar/Snapshot (una sola serie compartida).
func TestObservar_LasDosSeriesNoSeMezclan(t *testing.T) {
	h := Nuevo()
	observarMS(h, Encolado, 100, 10)
	observarMS(h, Descartado, 0.1, 990)

	enc := h.Snapshot(Encolado)
	desc := h.Snapshot(Descartado)
	if enc.N != 10 || desc.N != 990 {
		t.Fatalf("poblaciones = encolado %d / descartado %d, se esperaba 10 / 990", enc.N, desc.N)
	}
	pEnc, _ := Percentil(enc, 0.99)
	if pEnc.CotaMS != 100 {
		t.Fatalf("p99 de los ENCOLADOS = %v ms: los 990 descartes de 0,1 ms lo están diluyendo, que es "+
			"exactamente lo que las dos series existen para impedir", pEnc.CotaMS)
	}
	pDesc, _ := Percentil(desc, 0.99)
	if pDesc.CotaMS != 0.5 {
		t.Fatalf("p99 de los DESCARTES = %v ms, se esperaba 0.5", pDesc.CotaMS)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CONCURRENCIA
// ─────────────────────────────────────────────────────────────────────────────

// TestObservar_Concurrente_NoPierdeNiUnaMuestra: el cronómetro corre en el hilo de whatsmeow, y hay UN
// listener por sesión ⇒ N escritores simultáneos sobre el mismo histograma. Si el acumulado perdiera
// cuentas, el p99 se calcularía sobre una población que no existió.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: sustituir `atomic.Uint64` por un `uint64` plano en serie.buckets
// (con -race salta el detector; sin -race se pierden incrementos y falla el recuento).
func TestObservar_Concurrente_NoPierdeNiUnaMuestra(t *testing.T) {
	const goroutines = 8
	const porGoroutine = 2000

	h := Nuevo()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// Duraciones repartidas por la rejilla para que el reparto por buckets también se ejercite.
			d := time.Duration(g+1) * time.Millisecond
			for i := 0; i < porGoroutine; i++ {
				h.Observar(Encolado, d)
				h.Observar(Descartado, time.Microsecond)
			}
		}(g)
	}
	// Un lector concurrente: el latido lee mientras los listeners escriben, y esa es la carrera real.
	var lector sync.WaitGroup
	fin := make(chan struct{})
	lector.Add(1)
	go func() {
		defer lector.Done()
		for {
			select {
			case <-fin:
				return
			default:
				_ = h.Snapshot(Encolado)
			}
		}
	}()

	wg.Wait()
	close(fin)
	lector.Wait()

	const total = goroutines * porGoroutine
	if got := h.Snapshot(Encolado); got.N != total {
		t.Fatalf("Σ buckets de ENCOLADO = %d, se esperaban %d muestras: el acumulado pierde cuentas", got.N, total)
	}
	if got := h.Snapshot(Descartado); got.N != total {
		t.Fatalf("Σ buckets de DESCARTADO = %d, se esperaban %d", got.N, total)
	}
	// El máximo por CAS tiene que quedarse con el más lento de todos (8 ms), no con el último que escribió.
	if got := h.Snapshot(Encolado); got.MaxUS != int64(goroutines)*1000 {
		t.Fatalf("MaxUS = %d µs, se esperaban %d: el CAS del máximo no está conservando el mayor",
			got.MaxUS, int64(goroutines)*1000)
	}
}

// TestHistogramaNil_EsInocuo: el campo del listener es nil-safe a propósito — sin WithLatencia el
// comportamiento tiene que ser IDÉNTICO al de antes de esta tarea, no un pánico en el hilo del socket.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: quitar la guarda `h == nil` de Observar o de Snapshot.
func TestHistogramaNil_EsInocuo(t *testing.T) {
	var h *Histograma
	h.Observar(Encolado, time.Second)
	if m := h.Snapshot(Encolado); m.N != 0 {
		t.Fatalf("el snapshot de un histograma nil debe ser la muestra vacía, got N=%d", m.N)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// EL DELTA (y el reset que NO existe)
// ─────────────────────────────────────────────────────────────────────────────

// TestDelta_LosContadoresSonMONOTONICOS_YSnapshotNoResetea es el test de la invariante central del
// paquete: leer NO pone a cero. El patrón «leo y limpio» tiene una ventana entre la lectura y el reset en
// la que las muestras se pierden sin dejar rastro, y ese fallo mentiría a la baja justo en la ráfaga, que
// es cuando el número importa.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: reintroducir un reset tras leer (p. ej. `s.buckets[i].Store(0)` dentro
// de Snapshot, o un `Swap(0)`) ⇒ la segunda foto deja de ser acumulada y las tres aserciones caen.
func TestDelta_LosContadoresSonMONOTONICOS_YSnapshotNoResetea(t *testing.T) {
	h := Nuevo()
	observarMS(h, Encolado, 1, 10)

	primera := h.Snapshot(Encolado)
	if primera.N != 10 {
		t.Fatalf("primera foto: N = %d, se esperaba 10", primera.N)
	}

	// (a) Releer SIN observar nada tiene que dar exactamente lo mismo. Con un reset daría cero.
	if repetida := h.Snapshot(Encolado); repetida.N != primera.N {
		t.Fatalf("releer sin observar nada dio N = %d (antes %d): Snapshot está RESETEANDO, y este paquete "+
			"no resetea nada (ver la cabecera)", repetida.N, primera.N)
	}

	observarMS(h, Encolado, 1, 5)
	segunda := h.Snapshot(Encolado)

	// (b) La segunda foto es ACUMULADA, no del intervalo.
	if segunda.N != 15 {
		t.Fatalf("segunda foto: N = %d, se esperaba 15 (10 + 5). Un %d significaría que la primera lectura "+
			"se llevó por delante las muestras que ya había", segunda.N, 5)
	}
	// (c) El intervalo lo calcula el EMISOR restando, y da exactamente lo nuevo.
	if d := Delta(primera, segunda); d.N != 5 {
		t.Fatalf("Delta(primera, segunda).N = %d, se esperaban las 5 muestras del intervalo", d.N)
	}
}

// TestDelta_NoDaLaVueltaAlContador: dos fotos en orden inverso (o de histogramas distintos) darían una
// resta negativa sobre uint64, y un contador astronómico en el log es un diagnóstico peor que un cero.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: restar sin comprobar (`d.Buckets[i] = ahora.Buckets[i] - antes.Buckets[i]`).
func TestDelta_NoDaLaVueltaAlContador(t *testing.T) {
	h := Nuevo()
	observarMS(h, Encolado, 1, 3)
	pocas := h.Snapshot(Encolado)
	observarMS(h, Encolado, 1, 7)
	muchas := h.Snapshot(Encolado)

	if d := Delta(muchas, pocas); d.N != 0 {
		t.Fatalf("Delta con las fotos al revés dio N = %d: el uint64 dio la vuelta en vez de quedarse en 0", d.N)
	}
}
