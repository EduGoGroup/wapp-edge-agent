package cajerosock

// simetria_test.go — EL CABLE NO PUEDE TIRAR UN CAMPO EN SILENCIO (Plan 044 · Ola 1.7).
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 ESTE FICHERO NACE DE UN FALLO REAL, NO DE UNA PRECAUCIÓN
// ─────────────────────────────────────────────────────────────────────────────
// T1.7-2 añadió `app.PeticionInferencia.Calentamiento` y su punto de consulta en el cajero, con tests de
// conducta que lo cubrían. Estaba bien construido y no servía para nada: `PeticionWire` no nombraba el
// campo, así que la marca MORÍA AL CRUZAR EL SOCKET. Sin error, sin log, sin un test rojo — el cajero
// recibía la petición con el campo a su cero y se comportaba como si el Cloud no hubiera dicho nada. El
// fallo sólo se vio leyendo el fichero del cable a mano, semanas después.
//
// Ese es exactamente el modo de fallo de una traducción escrita a mano entre dos structs: AÑADIR es
// silencioso. Nadie escribe un test para el campo que se le olvidó.
//
// LO QUE ESTE TEST NO PUEDE HACER, dicho para que nadie se confíe: no comprueba que el valor se COPIE
// bien (eso lo hacen los tests de conducta), sólo que el cable TIENE dónde ponerlo. Es el guardián
// barato que caza la clase de error más probable, no todos.

import (
	"reflect"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// equivalenciaDeNombres son los campos cuyo nombre CAMBIA al cruzar el cable, con el porqué de cada uno.
// Todo lo demás tiene que llamarse igual a los dos lados.
//
// LA TABLA ES DELIBERADAMENTE INCÓMODA DE AMPLIAR: cada entrada es una excepción que alguien tuvo que
// justificar, y cuantas menos haya, menos sitios donde el cable y el puerto puedan divergir sin que se
// note.
var equivalenciaDeNombres = map[string]string{
	// El puerto lleva una time.Duration; el cable, milisegundos. JSON no tiene tipo duración y mandarla
	// como el int64 de nanosegundos de Go acoplaría el contrato a una decisión del lenguaje.
	"Timeout": "TimeoutMS",
	// El puerto está en español, como todo el dominio de este repo; el cable espeja el nombre del FRAME
	// (`class`), que es lo que un operador ve en un dump de CloudLink.
	"Clase": "Class",
	// Mismo caso: `warmup` es el nombre del campo 10 del contrato.
	"Calentamiento": "Warmup",
}

// TestPeticionWireEspejaElPuertoDeInferencia exige que CADA campo de app.PeticionInferencia tenga sitio
// en el cable, y viceversa.
//
// LAS DOS DIRECCIONES, y la segunda no es simetría por gusto: un campo que sólo existe en el cable es un
// campo que el servidor lee y no puede entregarle a nadie — o sea, un dato que el daemon paga por
// serializar y que se pierde igual, sólo que un paso más tarde.
func TestPeticionWireEspejaElPuertoDeInferencia(t *testing.T) {
	puerto := reflect.TypeOf(app.PeticionInferencia{})
	cable := reflect.TypeOf(PeticionWire{})

	enElCable := make(map[string]bool, cable.NumField())
	for i := range cable.NumField() {
		enElCable[cable.Field(i).Name] = true
	}

	esperadosEnElCable := make(map[string]bool, puerto.NumField())
	for i := range puerto.NumField() {
		campo := puerto.Field(i).Name
		quiero := campo
		if alias, ok := equivalenciaDeNombres[campo]; ok {
			quiero = alias
		}
		esperadosEnElCable[quiero] = true
		if !enElCable[quiero] {
			t.Errorf("app.PeticionInferencia.%s NO tiene campo en PeticionWire (se esperaba %q).\n"+
				"    Un campo que el cable no nombra MUERE AL CRUZAR EL SOCKET, sin error y sin log: el "+
				"cajero lo recibe a su cero y se comporta como si el Cloud no hubiera dicho nada.\n"+
				"    Añádelo a PeticionWire, al Marshal del cliente (inferenciacliente) y al mapeo del "+
				"servidor — o, si de verdad no debe viajar, dilo en equivalenciaDeNombres con su porqué.",
				campo, quiero)
		}
	}

	for i := range cable.NumField() {
		campo := cable.Field(i).Name
		if !esperadosEnElCable[campo] {
			t.Errorf("PeticionWire.%s no corresponde a ningún campo de app.PeticionInferencia: el daemon "+
				"paga por serializarlo y el servidor no tiene dónde entregarlo", campo)
		}
	}
}

// TestRespuestaWireEspejaLaRespuestaDelPuerto es la mitad de vuelta. Hoy es un solo campo, y justo por eso
// se escribe: el día que la respuesta gane algo (tokens consumidos, el modelo que respondió), el olvido
// sería igual de silencioso que el de la ida.
func TestRespuestaWireEspejaLaRespuestaDelPuerto(t *testing.T) {
	puerto := reflect.TypeOf(app.RespuestaInferencia{})
	cable := reflect.TypeOf(RespuestaWire{})

	enElCable := make(map[string]bool, cable.NumField())
	for i := range cable.NumField() {
		enElCable[cable.Field(i).Name] = true
	}
	for i := range puerto.NumField() {
		campo := puerto.Field(i).Name
		if !enElCable[campo] {
			t.Errorf("app.RespuestaInferencia.%s NO tiene campo en RespuestaWire: la salida del modelo se "+
				"perdería al volver del cajero al daemon", campo)
		}
	}
	// `Error` es del cable y NO del puerto a propósito: en el puerto el fallo viaja como `error` de Go, no
	// como un campo. No se comprueba la dirección de vuelta por eso.
	if !enElCable["Error"] {
		t.Error("RespuestaWire perdió el campo Error: sin él, un fallo del proveedor llegaría al daemon " +
			"como una respuesta vacía y no como uno de los cinco errores canónicos")
	}
}
