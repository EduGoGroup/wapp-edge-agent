package cajero

// inferencia_test.go — LA REJILLA DEL HISTOGRAMA DE INFERENCIA (T1.7-2, Plan 044 · Ola 1.7).

import (
	"math"
	"testing"
	"time"
)

// TestRejillaInferencia_EsEstrictamenteCrecienteYCadaBucketEsAlcanzable es el GEMELO de
// TestRejilla_EsEstrictamenteCrecienteYCadaBucketEsAlcanzable de internal/app/latencia, y existe porque
// no tenerlo era una divergencia silenciosa entre dos piezas que comparten toda la ingeniería (16
// buckets, borde superior inclusivo, monotónico y sin reset): una tenía protegida su rejilla y la otra
// no.
//
// 🔴 Y AQUÍ HACE MÁS FALTA QUE ALLÍ, porque desde T1.7-2 TRES de los bordes NO son literales sino
// expresiones sobre constantes (el umbral de lentitud, DefaultInferenceTimeoutMS y DefaultMaxTimeoutMS).
// Eso es lo que impide que caduquen en silencio —el defecto que esta tarea vino a arreglar: un borde
// rotulado «EL TIMEOUT» con 15.000 dentro cuando el plazo ya eran 45.000—, pero abre la puerta a otro:
// mover una constante puede DESORDENAR la rejilla. Un borde repetido o menor que el anterior no falla,
// simplemente deja BUCKETS MUERTOS que no pueden recibir una sola muestra, y la resolución de la medida
// se degrada sin avisar.
//
// Las dos invariantes son las dos caras de lo mismo, igual que en el gemelo: la primera es la propiedad
// estructural del array; la segunda la ata a `bucketInferenciaDe`, que es lo que de verdad se ejecuta.
//
// ⚠️ NO ES TAUTOLÓGICO: no compara ningún borde contra la constante de la que sale (eso pasaría con
// cualquier valor). Comprueba una PROPIEDAD —el orden, y que cada tramo sea alcanzable— que se rompe de
// verdad si alguien mueve un número al sitio equivocado.
func TestRejillaInferencia_EsEstrictamenteCrecienteYCadaBucketEsAlcanzable(t *testing.T) {
	for i := 0; i+1 < len(bordesInferenciaMS); i++ {
		if bordesInferenciaMS[i] >= bordesInferenciaMS[i+1] {
			t.Errorf("la rejilla no es estrictamente creciente en %d: bordesInferenciaMS[%d]=%d >= [%d]=%d — "+
				"un borde repetido o menor deja BUCKETS MUERTOS que no pueden recibir una muestra",
				i, i, bordesInferenciaMS[i], i+1, bordesInferenciaMS[i+1])
		}
	}

	for i, borde := range bordesInferenciaMS {
		// El borde es SUPERIOR E INCLUSIVO: su propio valor tiene que caer en SU bucket.
		if got := bucketInferenciaDe(borde); got != i {
			t.Errorf("el bucket %d (borde %d ms) es INALCANZABLE: bucketInferenciaDe(%d) = %d",
				i, borde, borde, got)
		}
	}

	if bordesInferenciaMS[numBucketsInferencia-1] != math.MaxInt64 {
		t.Errorf("el último borde es el DESBORDAMIENTO y tiene que ser MaxInt64, got %d",
			bordesInferenciaMS[numBucketsInferencia-1])
	}
}

// TestRejillaInferencia_CubreElPlazoREAL es el criterio (e) de T1.7-2, y la pregunta que responde es la
// operativa: «¿cuántas inferencias se comieron su plazo?».
//
// 🔴 EL DEFECTO QUE CIERRA, en un número: antes de esta tarea el borde más alto por debajo del
// desbordamiento eran 60 s, con el anterior en 20 s. Con el plazo por defecto en 45 s desde la Ola 1.6,
// TODO lo que pasara de 20 s —incluidas las que morían justo en el plazo, y las de 61 s que la propia
// Ola 1.6 midió en campo— caía en el mismo bucket de 20→60 s. El histograma tenía tres bordes calibrados
// contra un plazo de 15 s que ya no existía y ni uno donde se toman las decisiones.
//
// LO QUE SE COMPRUEBA SON LOS TRES CORTES, no los bordes. Un borde es SUPERIOR E INCLUSIVO, así que lo
// que la rejilla sabe separar son TRAMOS, y los tramos que hacen falta son estos cuatro:
//
//	≤ 36 s          sana (por debajo del umbral de lentitud, ADR-0042)
//	(36 s,  45 s]   LENTA: castiga al breaker, pero cupo en el plazo por defecto
//	(45 s, 120 s]   se pasó del plazo por defecto — sólo llega aquí quien pidió un `timeout_ms` mayor
//	> 120 s         desbordamiento: por encima del TECHO que el Edge impone al Cloud
//
// Se mide por CONDUCTA (dónde cae cada latencia) y no comparando los bordes con las constantes, que sería
// tautológico: lo que hace falta saber es que los cuatro casos se distinguen, no que dos números sean
// iguales.
func TestRejillaInferencia_CubreElPlazoREAL(t *testing.T) {
	plazo := int64(DefaultInferenceTimeoutMS)                              // 45 s
	umbral := int64(float64(DefaultInferenceTimeoutMS) * FraccionLentitud) // 36 s
	techo := int64(DefaultMaxTimeoutMS)                                    // 120 s

	casos := []struct {
		ms     int64
		porQue string
	}{
		{umbral, "la última que todavía NO castiga al breaker"},
		{umbral + 1, "la primera LENTA (cruzó el umbral de lentitud)"},
		{plazo + 1, "una que se pasó del plazo por defecto"},
		{techo + 1, "una por encima del TECHO: desbordamiento"},
	}

	visto := map[int]string{}
	for _, c := range casos {
		b := bucketInferenciaDe(c.ms)
		if otro, repetido := visto[b]; repetido {
			t.Errorf("%d ms (%s) comparte bucket %d con «%s»: la rejilla no distingue los dos casos",
				c.ms, c.porQue, b, otro)
		}
		visto[b] = c.porQue
	}

	// Sólo el último de los cuatro puede caer en el desbordamiento. Que los otros tres NO caigan es
	// literalmente lo que «cubre el plazo real» significa: con la rejilla vieja, el umbral y el plazo
	// compartían bucket con todo lo que pasara de 20 s.
	if got := bucketInferenciaDe(techo + 1); got != numBucketsInferencia-1 {
		t.Errorf("por encima del techo la muestra va al DESBORDAMIENTO: bucketInferenciaDe(%d) = %d, want %d",
			techo+1, got, numBucketsInferencia-1)
	}
	for _, ms := range []int64{umbral, umbral + 1, plazo, plazo + 1, techo} {
		if bucketInferenciaDe(ms) >= numBucketsInferencia-1 {
			t.Errorf("%d ms cae en el DESBORDAMIENTO: la rejilla no cubre el plazo real", ms)
		}
	}

	// Y el contrapunto que impide leer los cortes al revés: una que responde EN su plazo comparte tramo
	// con la primera lenta, porque las dos son «lenta pero servida». Eso es correcto y es la resolución
	// que la rejilla promete a esta escala; separar dentro de ese tramo sería precisión inventada.
	if bucketInferenciaDe(plazo) != bucketInferenciaDe(umbral+1) {
		t.Errorf("una respuesta de %d ms (justo en el plazo) y una de %d ms son las dos «lenta pero servida» "+
			"y comparten tramo: buckets %d y %d", plazo, umbral+1,
			bucketInferenciaDe(plazo), bucketInferenciaDe(umbral+1))
	}
}

// TestP50Inferencia_EnDesbordamientoPublicaElTecho ata el comentario de p50MS a su conducta: en el
// desbordamiento se publica el último borde FINITO —el techo— y nunca un centinela negativo, porque ese
// número viaja en un entero del contrato del heartbeat donde un -1 se leería como una medida.
func TestP50Inferencia_EnDesbordamientoPublicaElTecho(t *testing.T) {
	var h histogramaInferencia
	for range 3 {
		h.observar(time.Duration(DefaultMaxTimeoutMS+10_000) * time.Millisecond)
	}
	if got, want := h.p50MS(), int64(DefaultMaxTimeoutMS); got != want {
		t.Errorf("p50MS en desbordamiento: got %d want %d (el último borde finito, nunca un negativo)", got, want)
	}
}
