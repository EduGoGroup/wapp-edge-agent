package app

import (
	"testing"

	"github.com/EduGoGroup/wapp-shared/intents"
)

// cola_test.go — El enum de MOTIVOS del sobre `omitido` (Plan 051 Ola 2 · ADR-0038 §(e)).
//
// El comentario de cola.go promete que «un motivo que no esté en la lista canónica no tiene sobre y EL
// TEST LO CAZA». Este fichero es ese test. Sin él la promesa era una afirmación sin respaldo: el enum se
// escribió con siete constantes, una lista canónica aparte y un mapa de sobres precalculados — tres sitios
// que hay que mantener sincronizados a mano y ninguna red debajo.
//
// El paquete es `app` (no `app_test`), igual que send_test.go / pair_test.go / listen_test.go, y aquí
// hace falta: la exhaustividad se asoma a `motivosOmitido` y a `sobreOmitido`, que son privados.

// TestMotivosOmitidoEsExhaustiva es EL test que hace verdadero el comentario de cola.go: las ocho
// constantes del enum están todas en la lista canónica, la lista tiene exactamente ocho elementos y
// ninguno repetido.
//
// Las ocho se enumeran A MANO aquí, no se derivan de `motivosOmitido`: derivarlas convertiría el test en
// una tautología («la lista contiene lo que la lista contiene»). Añadir un noveno motivo al enum sin
// añadirlo a la lista canónica —el olvido exacto que rompe SobreOmitido en silencio, y con él la
// telemetría por motivo de la Ola 4— tiene que romper AQUÍ.
func TestMotivosOmitidoEsExhaustiva(t *testing.T) {
	esperados := []MotivoOmitido{
		MotivoFastlane,
		MotivoSinTexto,
		MotivoNoElegible,
		MotivoPresupuesto,
		MotivoBreaker,
		MotivoDesconocido,
		MotivoApagado,
		MotivoFalloRepetido,
	}

	lista := MotivosOmitido()
	if len(lista) != 8 {
		t.Fatalf("la lista canónica debe tener exactamente 8 motivos, tiene %d: %v", len(lista), lista)
	}

	vistos := make(map[MotivoOmitido]int, len(lista))
	for _, m := range lista {
		vistos[m]++
	}
	for m, n := range vistos {
		if n > 1 {
			t.Errorf("motivo %q repetido %d veces en la lista canónica", m, n)
		}
	}
	for _, m := range esperados {
		if vistos[m] == 0 {
			t.Errorf("la constante %q del enum NO está en la lista canónica (sin ella no tiene sobre)", m)
		}
	}

	// El mapa de sobres precalculados se DERIVA de la lista, así que un duplicado en la lista colapsaría
	// dos entradas en una y solo se vería aquí: los tamaños tienen que coincidir.
	if len(sobreOmitido) != len(motivosOmitido) {
		t.Fatalf("sobres precalculados: %d entradas para %d motivos canónicos (¿motivo repetido en la lista?)",
			len(sobreOmitido), len(motivosOmitido))
	}
}

// TestMotivosOmitidoDevuelveCopia comprueba que el llamante NO puede corromper la lista de todos: mutar
// el slice devuelto (reordenarlo para pintar un informe, por ejemplo) no debe verse en la llamada
// siguiente. Es el contrato que documenta MotivosOmitido, y devolver el slice interno lo rompería sin que
// nada más se enterase.
func TestMotivosOmitidoDevuelveCopia(t *testing.T) {
	primera := MotivosOmitido()
	if len(primera) == 0 {
		t.Fatal("la lista canónica no puede estar vacía")
	}
	original := primera[0]

	primera[0] = MotivoOmitido("saboteado")

	segunda := MotivosOmitido()
	if segunda[0] != original {
		t.Fatalf("mutar el slice devuelto contaminó la lista canónica: got %q, want %q", segunda[0], original)
	}
	// Y el mapa de sobres, que se deriva de la misma lista, sigue intacto.
	if SobreOmitido(original) == "" {
		t.Fatalf("el sobre de %q desapareció tras mutar la copia", original)
	}
}

// TestSobreOmitidoFormatoDeCable fija el FORMATO DE CABLE del sobre `omitido` para los ocho motivos.
//
// 🔴 Los JSON se escriben como LITERALES, no derivados de las constantes: si el esperado se construyera
// con `"{\"omitido\":\""+string(m)+"\"}"` el test pasaría igual aunque alguien renombrara un motivo o
// cambiara la clave del struct, porque esperado y obtenido saldrían de la misma fuente. Aquí el esperado
// es INDEPENDIENTE de quien lo produce, que es la única forma de que este test pruebe algo.
//
// Importa porque este string se PERSISTE en la columna `intent_json` de la cola: cambiarlo deja las filas
// ya escritas en disco ilegibles para EsOmitido, y el síntoma sería un mensaje que se despacha con un
// sobre de omisión colado como si fuera un intent.
func TestSobreOmitidoFormatoDeCable(t *testing.T) {
	casos := []struct {
		nombre string
		motivo MotivoOmitido
		want   string
	}{
		{"fastlane", MotivoFastlane, `{"omitido":"fastlane"}`},
		{"sin_texto", MotivoSinTexto, `{"omitido":"sin_texto"}`},
		{"no_elegible", MotivoNoElegible, `{"omitido":"no_elegible"}`},
		{"presupuesto", MotivoPresupuesto, `{"omitido":"presupuesto"}`},
		{"breaker", MotivoBreaker, `{"omitido":"breaker"}`},
		{"desconocido", MotivoDesconocido, `{"omitido":"desconocido"}`},
		{"apagado", MotivoApagado, `{"omitido":"apagado"}`},
		{"fallo_repetido", MotivoFalloRepetido, `{"omitido":"fallo_repetido"}`},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			if got := SobreOmitido(c.motivo); got != c.want {
				t.Fatalf("SobreOmitido(%q) = %q, quería %q", c.motivo, got, c.want)
			}
		})
	}
}

// TestSobreOmitidoFueraDeLaListaDevuelveVacio: un motivo que NO está en la lista canónica no tiene sobre,
// y "" es el fallo SEGURO — significa NULL en la columna, es decir «esta fila la reclama el cajero»: se
// clasifica de más, nunca se pierde el mensaje. (Ojo con el nombre: aquí «desconocido» es «ajeno al
// enum», no la constante MotivoDesconocido, que sí es canónica.)
func TestSobreOmitidoFueraDeLaListaDevuelveVacio(t *testing.T) {
	for _, m := range []MotivoOmitido{"", "inventado", "FASTLANE", "fastlane "} {
		t.Run("motivo="+string(m), func(t *testing.T) {
			if got := SobreOmitido(m); got != "" {
				t.Fatalf("SobreOmitido(%q) = %q, quería \"\" (fuera de la lista canónica no hay sobre)", m, got)
			}
		})
	}
}

// TestEsOmitidoReconoceLosOchoSobres: la puerta que usará el despachador (Ola 3) reconoce el sobre de
// omisión de cada uno de los ocho motivos, y devuelve el motivo exacto (el desglose de la telemetría
// nunca se agrega, INV-051.3).
func TestEsOmitidoReconoceLosOchoSobres(t *testing.T) {
	casos := []struct {
		sobre string
		want  MotivoOmitido
	}{
		{`{"omitido":"fastlane"}`, MotivoFastlane},
		{`{"omitido":"sin_texto"}`, MotivoSinTexto},
		{`{"omitido":"no_elegible"}`, MotivoNoElegible},
		{`{"omitido":"presupuesto"}`, MotivoPresupuesto},
		{`{"omitido":"breaker"}`, MotivoBreaker},
		{`{"omitido":"desconocido"}`, MotivoDesconocido},
		{`{"omitido":"apagado"}`, MotivoApagado},
		{`{"omitido":"fallo_repetido"}`, MotivoFalloRepetido},
	}
	for _, c := range casos {
		t.Run(string(c.want), func(t *testing.T) {
			got, ok := EsOmitido(c.sobre)
			if !ok {
				t.Fatalf("EsOmitido(%s) dijo que NO es omisión", c.sobre)
			}
			if got != c.want {
				t.Fatalf("EsOmitido(%s) = %q, quería %q", c.sobre, got, c.want)
			}
		})
	}
}

// TestEsOmitidoRechazaLoQueNoEsOmision: ante la duda, NO es omisión. El caso que de verdad importa es el
// segundo — el sobre del CAJERO, el que sí lleva intent: si EsOmitido lo tomara por una omisión, el
// despachador tiraría un intent válido y el mensaje llegaría a la nube sin clasificar, sin error ni log.
func TestEsOmitidoRechazaLoQueNoEsOmision(t *testing.T) {
	casos := []struct {
		nombre string
		json   string
	}{
		{"vacío (fila sin clasificar ⇒ NULL)", ""},
		{"sobre del cajero", `{"intent":"consultar_saldo","params":{},"confidence":0.9,"config_version":"v1"}`},
		{"JSON ilegible", "no soy json"},
		{"omitido vacío", `{"omitido":""}`},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			got, ok := EsOmitido(c.json)
			if ok {
				t.Fatalf("EsOmitido(%q) = (%q, true), quería (\"\", false)", c.json, got)
			}
			if got != "" {
				t.Fatalf("EsOmitido(%q) devolvió motivo %q con ok=false", c.json, got)
			}
		})
	}
}

// TestMotivoDesconocidoNoDiverjeDeWappShared es el ANTI-DRIFT del único motivo con dueño FUERA de este
// repo: `MotivoDesconocido` se REFERENCIA de intents.ReservedUnknown (wapp-shared) en vez de escribirse a
// mano. Se comprueban las dos mitades, y las dos hacen falta:
//
//   - que siga siendo IGUAL a intents.ReservedUnknown — caza el día que alguien "simplifique" cola.go
//     escribiendo "desconocido" literal y wapp-shared renombre después la reservada;
//   - que su valor sea "desconocido" — caza el caso contrario, que wapp-shared cambie la constante y el
//     Edge la siga a ciegas, escribiendo en `intent_json` un sobre que los datos ya persistidos y el
//     despachador de la Ola 3 no reconocen.
func TestMotivoDesconocidoNoDiverjeDeWappShared(t *testing.T) {
	if string(MotivoDesconocido) != intents.ReservedUnknown {
		t.Fatalf("MotivoDesconocido = %q, quería intents.ReservedUnknown = %q (divergencia con wapp-shared)",
			MotivoDesconocido, intents.ReservedUnknown)
	}
	if string(MotivoDesconocido) != "desconocido" {
		t.Fatalf("MotivoDesconocido = %q, quería \"desconocido\": wapp-shared cambió la reservada y el sobre "+
			"persistido en `intent_json` dejaría de ser reconocible", MotivoDesconocido)
	}
}

// TestSobreOmitidoRoundTrip cierra el círculo: lo que SobreOmitido escribe en `intent_json`, EsOmitido lo
// vuelve a leer como el MISMO motivo, para toda la lista canónica. Es la prueba de que productor y lector
// del sobre no pueden divergir — y recorre la lista (no una tabla fija) para que un motivo nuevo entre
// aquí solo, sin que nadie tenga que acordarse.
func TestSobreOmitidoRoundTrip(t *testing.T) {
	for _, m := range MotivosOmitido() {
		t.Run(string(m), func(t *testing.T) {
			sobre := SobreOmitido(m)
			if sobre == "" {
				t.Fatalf("el motivo canónico %q no tiene sobre", m)
			}
			got, ok := EsOmitido(sobre)
			if !ok {
				t.Fatalf("EsOmitido(%s) no reconoció el sobre que produjo SobreOmitido(%q)", sobre, m)
			}
			if got != m {
				t.Fatalf("round-trip de %q: EsOmitido devolvió %q", m, got)
			}
		})
	}
}

// TestColaLoteUltimo cubre DÓNDE SE ESCRIBE EL INTENT: la última fila del lote, la de mayor Seq (§4 del
// design). Los dos casos nil no son adorno — el cajero llama a Ultimo() sobre lo que devuelve Reclamar,
// y (nil, nil) es la respuesta NORMAL de una cola vacía: sin el guardarraíl, el worker al día haría
// pánico en su primer tick ocioso.
func TestColaLoteUltimo(t *testing.T) {
	t.Run("lote nil", func(t *testing.T) {
		var lote *ColaLote
		if got := lote.Ultimo(); got != nil {
			t.Fatalf("Ultimo() sobre un lote nil = %+v, quería nil", got)
		}
	})

	t.Run("lote vacío", func(t *testing.T) {
		lote := &ColaLote{SessionID: "A", ChatJID: "chat@s"}
		if got := lote.Ultimo(); got != nil {
			t.Fatalf("Ultimo() sobre un lote sin mensajes = %+v, quería nil", got)
		}
	})

	t.Run("lote poblado devuelve el de mayor Seq", func(t *testing.T) {
		lote := &ColaLote{
			SessionID: "A",
			ChatJID:   "chat@s",
			Mensajes: []ColaMensaje{
				{ID: 1, Seq: 10, WAMessageID: "wa-1", Texto: "quiero"},
				{ID: 2, Seq: 20, WAMessageID: "wa-2", Texto: "dos"},
				{ID: 3, Seq: 30, WAMessageID: "wa-3", Texto: "empanadas"},
			},
		}
		ultimo := lote.Ultimo()
		if ultimo == nil {
			t.Fatal("Ultimo() sobre un lote poblado devolvió nil")
		}
		if ultimo.Seq != 30 || ultimo.WAMessageID != "wa-3" {
			t.Fatalf("Ultimo() = seq %d (%s), quería seq 30 (wa-3)", ultimo.Seq, ultimo.WAMessageID)
		}
		// Es un PUNTERO a la fila del slice, no una copia: el cajero escribe el intent a través de él.
		if ultimo != &lote.Mensajes[2] {
			t.Fatal("Ultimo() debe apuntar a la fila del lote, no a una copia")
		}
	})
}
