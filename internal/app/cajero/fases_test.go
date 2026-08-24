package cajero

// fases_test.go — PREFILL Y GENERACIÓN, POR SEPARADO (Plan 044 · Ola 1.7 · T1.7-5).
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 QUÉ PROBLEMA RESUELVE ESTO, en números
// ─────────────────────────────────────────────────────────────────────────────
// Hasta esta tarea la latencia se publicaba como UN SOLO NÚMERO que mezcla dos regímenes separados por un
// orden de magnitud: con el prefijo FRÍO el prefill cuesta ~21,6 ms por token; con el prefijo CALIENTE
// baja a 0,1-1,2 s el prompt entero. Ese número mezclado es el que dejó DOS p50 IRRECONCILIABLES en el
// repo —~20 s en el informe de diseño contra 8,1 s en campo— y no era un error de medición: medían
// poblaciones con distinto calor de prefijo.
//
// Lo que estos tests fijan es que esa distinción sobrevive al viaje: se mide, se clasifica, se cuenta y
// se publica ATADA A SU MUESTRA.

import (
	"context"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-intent/ollama"
)

// respuestaConFases arma una respuesta del proveedor con el prefill y la generación en NANOSEGUNDOS, que
// es como los devuelve Ollama.
func respuestaConFases(prefillMS, generacionMS int64) *ollama.ChatResponse {
	const nsPorMS = 1_000_000
	return &ollama.ChatResponse{
		Message:            ollama.Message{Role: "assistant", Content: `{"intent":"crear_pedido"}`},
		Done:               true,
		PromptEvalDuration: prefillMS * nsPorMS,
		EvalDuration:       generacionMS * nsPorMS,
	}
}

// servirConFases sirve UNA inferencia cuyo proveedor devuelve esas dos fases, y devuelve el Cajero.
func servirConFases(ctx context.Context, t *testing.T, prefillMS, generacionMS int64, clase string) *Cajero {
	t.Helper()
	c, s := servidorDe(t, Deps{
		Ollama:        &chateadorEspia{resp: respuestaConFases(prefillMS, generacionMS)},
		Opciones:      opcionesDelEdge(),
		MaxConcurrent: 1,
		Timeout:       timeoutDeLaMedicion,
	})
	p := peticionDe("dame un pedido", timeoutDeLaMedicion)
	p.Clase = clase
	if _, err := s.Inferir(ctx, p); err != nil {
		t.Fatalf("Inferir: %v", err)
	}
	return c
}

// ─────────────────────────────────────────────────────────────────────────────
// Los tres regímenes
// ─────────────────────────────────────────────────────────────────────────────

// TestRegimenDe_LosTresTramosYSusBordes fija la partición completa, BORDES INCLUIDOS.
//
// 🔴 LA FRANJA DEL MEDIO ES LA RAZÓN DE ESTE TEST. Los umbrales del plan sólo definían dos regímenes
// (> 5 s frío, < 2 s caliente) y dejaban [2 s, 5 s] SIN CUBRIR: una medida ahí no era ni una cosa ni la
// otra, y repartirla a cualquiera de los dos lados habría metido una mentira en el número que esta ola
// existe para limpiar. `templado` es esa franja, y los dos casos de borde exacto son los que garantizan
// que ningún valor puede quedarse sin régimen por un `>=` mal puesto.
func TestRegimenDe_LosTresTramosYSusBordes(t *testing.T) {
	u := nuevosUmbralesRegimen(0, 0, &logCaptura{}) // los defaults, resueltos por el constructor
	casos := []struct {
		prefillMS int64
		quiero    string
	}{
		{1, RegimenCaliente},
		{DefaultPrefillCalienteMS - 1, RegimenCaliente},
		{DefaultPrefillCalienteMS, RegimenTemplado}, // borde inferior: la franja es CERRADA
		{3_000, RegimenTemplado},                    // el centro de la franja que no estaba cubierta
		{DefaultPrefillFrioMS, RegimenTemplado},     // borde superior: también cerrado
		{DefaultPrefillFrioMS + 1, RegimenFrio},     //
		{50_000, RegimenFrio},                       // el prefill de un calentamiento en UAT
	}
	for _, c := range casos {
		if got := u.regimenDe(c.prefillMS); got != c.quiero {
			t.Errorf("regimenDe(%d ms) = %q, want %q", c.prefillMS, got, c.quiero)
		}
	}
}

// TestUmbralesRegimen_SonConfiguracionYNoConstante es el criterio operativo de la ola: los bordes se
// mueven SIN RECOMPILAR. El conteo de `templado` existe para decir «estos dos números ya no parten bien la
// población de esta máquina», y si la respuesta a esa señal exigiera un binario nuevo, llegaría semanas
// después de la pregunta.
//
// El caso es real: en una máquina más lenta, un prefill de 3 s puede ser perfectamente CALIENTE.
func TestUmbralesRegimen_SonConfiguracionYNoConstante(t *testing.T) {
	ctx := context.Background()

	c, s := servidorDe(t, Deps{
		Ollama:          &chateadorEspia{resp: respuestaConFases(3_000, 500)},
		Opciones:        opcionesDelEdge(),
		PrefillCaliente: 4 * time.Second, // con los defaults (2 s/5 s) esto sería `templado`
		PrefillFrio:     20 * time.Second,
		MaxConcurrent:   1,
		Timeout:         timeoutDeLaMedicion,
	})
	if _, err := s.Inferir(ctx, peticionDe("hola", timeoutDeLaMedicion)); err != nil {
		t.Fatalf("Inferir: %v", err)
	}

	if got := c.porRegimen.foto()[RegimenCaliente]; got != 1 {
		t.Errorf("con umbrales recalibrados (caliente < 4 s) un prefill de 3 s es CALIENTE; "+
			"porRegimen[caliente]=%d, foto=%v", got, c.porRegimen.foto())
	}
	if got := c.umbrales.calienteMS; got != 4_000 {
		t.Errorf("el umbral efectivo no es el configurado: got %d want 4000", got)
	}
}

// TestUmbralesRegimen_UnaParejaInvertidaCaeENTERAAlDefault es el guardarraíl que impide una configuración
// que MIENTE EN SILENCIO.
//
// 🔴 Con `caliente >= frio` la franja del medio no existe: `regimenDe` clasificaría como frío todo lo que
// pase de `frio` y como caliente todo lo demás, y el reparto del heartbeat seguiría saliendo con sus TRES
// claves, una de ellas clavada a 0 para siempre. Nadie leería eso como «la config está mal»; lo leería
// como «esta máquina nunca está templada», que es una conclusión falsa sobre el hardware del cliente.
//
// Y CAEN LOS DOS, no sólo el que sobra: corregir un borde dejaría una tercera pareja que nadie ha
// revisado. El operador se queda con la que sí está medida, y el WARN le dice qué pidió y qué se aplicó.
func TestUmbralesRegimen_UnaParejaInvertidaCaeENTERAAlDefault(t *testing.T) {
	log := &logCaptura{}

	u := nuevosUmbralesRegimen(9*time.Second, 3*time.Second, log)

	if u.calienteMS != DefaultPrefillCalienteMS || u.frioMS != DefaultPrefillFrioMS {
		t.Errorf("una pareja invertida debía caer ENTERA al default; got %+v", u)
	}
	e, ok := log.buscar("umbrales de régimen del prefijo están INVERTIDOS")
	if !ok {
		t.Fatal("una config rechazada tiene que dejar rastro: sin el WARN, el operador ve el default y no " +
			"sabe que lo suyo se descartó")
	}
	if e.nivel != "warn" {
		t.Errorf("nivel: got %q want warn", e.nivel)
	}
	// LOS DOS PARES EN LA MISMA LÍNEA: lo que se pidió y lo que se aplicó. Con sólo uno de ellos, el
	// operador tendría que ir a buscar el otro a otro sitio para entender qué pasó.
	claves := clavesDe(e)
	for _, k := range []string{"prefill_caliente_ms_pedido", "prefill_frio_ms_pedido",
		"prefill_caliente_ms", "prefill_frio_ms"} {
		if !claves[k] {
			t.Errorf("el WARN no lleva %q: %v", k, claves)
		}
	}
}

// TestObservarFases_UnPrefillNoMedibleNoEsUnaMuestra es el test que impide la mentira más cómoda de
// creer.
//
// 🔴 Ollama devuelve `prompt_eval_duration` SIEMPRE, así que un cero significa que la respuesta NO LO
// TRAÍA (otra versión del proveedor, una respuesta recortada), nunca que el prefill fuera instantáneo.
// Contándolo, esa muestra caería en el bucket más bajo Y sumaría al régimen `caliente`: una máquina que
// PERDIÓ el dato se vería como una máquina que va de maravilla, y el operador miraría un dashboard verde
// mientras el prefill se le dispara. Descartándola, la ausencia viaja como ausencia y el heartbeat
// publica «no medible», que es la única lectura honesta.
func TestObservarFases_UnPrefillNoMedibleNoEsUnaMuestra(t *testing.T) {
	ctx := context.Background()

	c := servirConFases(ctx, t, 0, 4_000, app.ClaseInteractivo)

	if n := c.prefill.muestras(); n != 0 {
		t.Errorf("prefill.muestras: got %d want 0 — un prefill de 0 ms NO es un prefill instantáneo, es un "+
			"prefill que el proveedor no reportó", n)
	}
	if got := c.porRegimen.foto()[RegimenCaliente]; got != 0 {
		t.Errorf("porRegimen[%q]: got %d want 0 — la muestra perdida se contó como si la caché estuviera "+
			"caliente", RegimenCaliente, got)
	}
	// La GENERACIÓN sí se midió y no puede caerse con la otra: son dos números independientes del
	// proveedor, y anidar sus guardas haría que perder uno borrase el otro.
	if n := c.generacion.muestras(); n != 1 {
		t.Errorf("generacion.muestras: got %d want 1 — perder el prefill no puede borrar una generación que "+
			"SÍ se midió", n)
	}
}

// TestFases_SeMidenPorSeparadoYSeClasificanKO recorre el camino entero de una inferencia real: dos fases
// medidas, dos histogramas distintos y un régimen contado.
func TestFases_SeMidenPorSeparadoYSeClasificanKO(t *testing.T) {
	ctx := context.Background()

	// Un caso de campo: prefill FRÍO de 50 s (lo que paga un P1 recién arrancado en UAT) y una generación
	// corta. Con un solo número los dos serían «55 s» y no habría forma de saber en qué se fue el tiempo.
	c := servirConFases(ctx, t, 50_000, 900, app.ClaseLote)

	if n := c.prefill.muestras(); n != 1 {
		t.Fatalf("prefill.muestras: got %d want 1", n)
	}
	if n := c.generacion.muestras(); n != 1 {
		t.Fatalf("generacion.muestras: got %d want 1", n)
	}
	// El p50 es una COTA SUPERIOR (el borde del bucket), nunca una interpolación: lo que se comprueba es
	// que cada fase cayó en SU escala, no un número exacto que la rejilla no promete.
	if p := c.prefill.p50MS(); p < 30_000 {
		t.Errorf("prefill p50: got %d ms, un prefill de 50 s no puede publicarse por debajo de 30 s", p)
	}
	if p := c.generacion.p50MS(); p > 2_000 {
		t.Errorf("generacion p50: got %d ms, una generación de 0,9 s no puede publicarse por encima de 2 s", p)
	}
	if got := c.porRegimen.foto()[RegimenFrio]; got != 1 {
		t.Errorf("porRegimen[%q]: got %d want 1", RegimenFrio, got)
	}
	if got := c.porClase.foto()[app.ClaseLote]; got != 1 {
		t.Errorf("porClase[%q]: got %d want 1", app.ClaseLote, got)
	}
}

// TestPorClase_EtiquetaDesconocidaCaeEnInteractivoYNoAbreClaveNueva fija las dos mitades de la
// normalización: dónde cae lo desconocido, y —lo que importa más— que NO se abre una clave nueva.
//
// Sin la normalización, el mapa del heartbeat crecería una clave por cada errata que alguien cometa
// escribiendo una constante en el Cloud, y un mapa que crece con las erratas deja de ser legible.
func TestPorClase_EtiquetaDesconocidaCaeEnInteractivoYNoAbreClaveNueva(t *testing.T) {
	ctx := context.Background()

	c := servirConFases(ctx, t, 1_000, 500, "LOTE_URGENTE_v2")

	foto := c.porClase.foto()
	if got := foto[app.ClaseInteractivo]; got != 1 {
		t.Errorf("porClase[%q]: got %d want 1 — lo desconocido cae en interactivo", app.ClaseInteractivo, got)
	}
	if _, existe := foto["LOTE_URGENTE_v2"]; existe {
		t.Errorf("una clase desconocida abrió una clave propia en el mapa: %v", foto)
	}
}

// TestContadorEtiquetado_SiembraLasClavesConocidas es el test de la distinción que sostiene todo lo
// demás: «este proceso no ha visto ni un arranque en frío» (clave a 0) y «este Edge no mide el régimen»
// (clave ausente) NO PUEDEN ser lo mismo. Sin la siembra lo serían, y el consumidor tendría que adivinar.
func TestContadorEtiquetado_SiembraLasClavesConocidas(t *testing.T) {
	c, err := New(Deps{Cola: &colaFake{}, Ollama: &chateadorEspia{}, Log: &logCaptura{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	regimenes := c.porRegimen.foto()
	for _, r := range RegimenesInferencia {
		v, ok := regimenes[r]
		if !ok {
			t.Errorf("el régimen %q no está sembrado: su ausencia se leerá como «este Edge no lo mide»", r)
			continue
		}
		if v != 0 {
			t.Errorf("el régimen %q nace con %d y debía nacer a 0", r, v)
		}
	}
	clases := c.porClase.foto()
	for _, cl := range app.ClasesInferencia {
		if _, ok := clases[cl]; !ok {
			t.Errorf("la clase %q no está sembrada", cl)
		}
	}
}

// TestContadorEtiquetado_FotoEsUnaCopia custodia la razón por la que `foto()` copia: lo que sale de ahí
// viaja a otro proceso por el parte y se recorre mientras el cajero puede estar contando. Devolver el
// mapa vivo sería una carrera que `-race` caza el día que dos inferencias terminen a la vez — o sea, en
// campo y no en CI.
func TestContadorEtiquetado_FotoEsUnaCopia(t *testing.T) {
	cont := nuevoContador(RegimenesInferencia...)
	cont.contar(RegimenFrio)

	foto := cont.foto()
	foto[RegimenFrio] = 999

	if got := cont.foto()[RegimenFrio]; got != 1 {
		t.Errorf("tocar la foto cambió el contador: got %d want 1 (foto() devolvió el mapa VIVO)", got)
	}
}

// TestParte_LlevaElRepartoDeLaInferencia cierra el camino: lo medido llega al tubo cajero→daemon, y el
// cuantil viaja ATADO A SU MUESTRA.
//
// 🔴 LA PAREJA ES EL PUNTO. Un p50 sobre una muestra pequeña es un MÁXIMO DISFRAZADO, y comparar
// cuantiles de `n` distinto ya fabricó aquí una conclusión falsa. Por eso el parte publica siempre los
// dos números y el lector decide «no medible» mirando LAS MUESTRAS — nunca el p50, que vale 0 tanto sin
// muestras como con ellas si todo fue rapidísimo.
func TestParte_LlevaElRepartoDeLaInferencia(t *testing.T) {
	ctx := context.Background()
	esc := &escritorParteFake{}

	c, s := servidorDe(t, Deps{
		Colas:         []ColaNombrada{{Nombre: "inst-a", Cola: &colaFake{}, Parte: esc}},
		Ollama:        &chateadorEspia{resp: respuestaConFases(6_000, 1_500)},
		Opciones:      opcionesDelEdge(),
		MaxConcurrent: 1,
		Timeout:       timeoutDeLaMedicion,
	})
	p := peticionDe("dame un pedido", timeoutDeLaMedicion)
	p.Clase = app.ClaseLote
	if _, err := s.Inferir(ctx, p); err != nil {
		t.Fatalf("Inferir: %v", err)
	}
	c.publicarParte(ctx)

	parte, hay := esc.ultimo()
	if !hay {
		t.Fatal("no se publicó ningún parte")
	}
	if parte.PrefillMuestras != 1 {
		t.Errorf("PrefillMuestras: got %d want 1 — sin la muestra, el p50 del prefill no significa nada",
			parte.PrefillMuestras)
	}
	if parte.GeneracionMuestras != 1 {
		t.Errorf("GeneracionMuestras: got %d want 1", parte.GeneracionMuestras)
	}
	if parte.PrefillP50ms <= 0 {
		t.Errorf("PrefillP50ms: got %d, se midió un prefill de 6 s", parte.PrefillP50ms)
	}
	if got := parte.PorRegimen[RegimenFrio]; got != 1 {
		t.Errorf("PorRegimen[%q]: got %d want 1 (6 s > %d ms)", RegimenFrio, got, DefaultPrefillFrioMS)
	}
	if got := parte.PorClase[app.ClaseLote]; got != 1 {
		t.Errorf("PorClase[%q]: got %d want 1", app.ClaseLote, got)
	}
}
