package domain

import (
	"reflect"
	"strings"
	"testing"
)

// inbound_sin_intent_test.go — EL GUARDARRAÍL DEL RETIRO DEL PUSH (Plan 044 · Ola 1.6 · T1.6-5 ·
// ADR-0045 · D-044.31 · REQ-35).
//
// QUÉ CUIDA, Y POR QUÉ NO BASTA CON QUE HOY NO COMPILE. El 2026-08-24 se retiró `InboundEvent.Intent`
// (y con él `ClassifiedIntent`) porque el Edge dejó de clasificar por iniciativa propia: la señal la
// PIDE el Cloud por el frame `inference_request`. Hoy nada lo puebla porque el campo no existe y el
// proto tampoco tiene dónde ponerlo. Pero el modo de fallo que se quiere impedir no es «alguien rellena
// el campo»: es **alguien vuelve a AÑADIRLO** —para un caso puntual, para una prueba, «solo local»— y
// nadie se entera, porque añadir un campo a un struct no rompe absolutamente nada.
//
// Por eso el test mira el TIPO y no un camino: un camino concreto se puede rodear, un campo no. Es el
// mismo criterio con el que este repo prefiere un invariante estructural a N tests de conducta.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): añadir `Intent *struct{}` —o cualquier campo cuyo nombre
// contenga "intent"— a `InboundEvent` ⇒ este test falla nombrando el campo.
//
// 🔴 SI ALGÚN DÍA HAY QUE VOLVER A ADJUNTAR UNA INTENCIÓN AL ENTRANTE, este test NO es el obstáculo: es
// la conversación. Borrarlo exige decir en qué ADR se revierte el 0045, y eso es exactamente lo que se
// quiere que cueste.
func TestInboundEvent_NoTieneCampoDeIntencion(t *testing.T) {
	tipo := reflect.TypeOf(InboundEvent{})
	for i := 0; i < tipo.NumField(); i++ {
		nombre := tipo.Field(i).Name
		if strings.Contains(strings.ToLower(nombre), "intent") {
			t.Errorf("InboundEvent volvió a tener un campo de intención (%q): el ADR-0045 pasó la "+
				"clasificación a PULL y el Edge NO adjunta señal al entrante. Si esto es deliberado, "+
				"hace falta un ADR que revierta el 0045 — no un campo más", nombre)
		}
	}
}

// TestInboundEvent_NoDeclaraTipoDeIntencion: el gemelo del anterior sobre el PAQUETE. El campo se podría
// reintroducir con otro nombre (`Clasificacion`, `Senal`, …) y el test de arriba no lo vería; lo que no
// se puede disimular es el TIPO que habría que declarar para llevarlo.
//
// Se comprueba con el único instrumento que tiene un test dentro de su propio paquete sin leer AST: que
// el símbolo no exista es indemostrable por reflexión, así que se asevera lo que sí es demostrable — que
// ningún campo de InboundEvent es un puntero a un struct con la FORMA de una clasificación
// (Name/Params/Confidence/ConfigVersion). Esa forma es la que el proto retirado transportaba.
func TestInboundEvent_NingunCampoTieneLaFormaDeUnaClasificacion(t *testing.T) {
	// La forma que se persigue: los cuatro nombres juntos. Tres de ellos sueltos son inocentes; los
	// cuatro en el mismo struct son la señal del Plan 029 con otro nombre encima.
	forma := []string{"Name", "Params", "Confidence", "ConfigVersion"}

	tipo := reflect.TypeOf(InboundEvent{})
	for i := 0; i < tipo.NumField(); i++ {
		campo := tipo.Field(i).Type
		for campo.Kind() == reflect.Pointer {
			campo = campo.Elem()
		}
		if campo.Kind() != reflect.Struct {
			continue
		}
		encontrados := 0
		for _, n := range forma {
			if _, ok := campo.FieldByName(n); ok {
				encontrados++
			}
		}
		if encontrados == len(forma) {
			t.Errorf("el campo %q lleva un struct con la forma exacta de una clasificación LLM "+
				"(%v): eso es el `ClassifiedIntent` del push volviendo con otro nombre (ADR-0045)",
				tipo.Field(i).Name, forma)
		}
	}
}
