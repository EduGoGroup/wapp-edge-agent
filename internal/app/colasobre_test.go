package app

// colasobre_test.go — EL FORMATO DE CABLE DEL SOBRE `meta_enc` (MP-10 Parte A).
//
// POR QUÉ NACE ESTE FICHERO AHORA. ColaMeta llevaba desde la Ola 3 sin test propio y se defendía sola: sus
// seis campos los escribe el listener y los lee el despachador, y ambos lados se comparan en los tests de
// integración de la cola. MP-10 le añade un SÉPTIMO campo (`Sintetico`) que rompe esa simetría —lo escribe
// solo el camino del inyector— y con él una promesa nueva que ningún test de integración puede hacer valer:
// que el JSON de las filas REALES no cambie ni un byte. Un `bool` sin `omitempty` habría metido
// `"sintetico":false` en el sobre de TODAS las filas del Edge, incluidas las millones que no tienen nada
// que ver con la medición.
//
// El paquete es `app` (no `app_test`), igual que cola_test.go / send_test.go / listen_test.go.
//
// ⚠️ ESTOS TESTS NO SE HAN EJECUTADO: el entorno en el que se escribieron no tiene toolchain de Go.

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestColaMetaSinSinteticoNoEmiteLaClave fija el JSON EXACTO de un sobre del camino real: los campos que
// valen su cero no aparecen, y `sintetico` es uno de ellos.
//
// 🔴 EL ESPERADO SE ESCRIBE COMO LITERAL, no se deriva del struct (el mismo criterio que
// TestSobreOmitidoFormatoDeCable): derivarlo lo volvería una tautología que pasaría igual aunque alguien
// renombrara una etiqueta o quitara un `omitempty`, porque esperado y obtenido saldrían de la misma fuente.
// Aquí el esperado es INDEPENDIENTE de quien lo produce, que es la única forma de que este test pruebe algo
// sobre bytes que se PERSISTEN cifrados en disco y se releen meses después.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO:
//   - quitar el `,omitempty` de `Sintetico` en colasobre.go ⇒ el JSON pasa a llevar `"sintetico":false` y
//     falla tanto la comparación literal como la comprobación de la clave. Es EL fallo que este test existe
//     para cazar: cambiaría el sobre de todas las filas reales por una marca que solo importa al inyector.
//   - mover `Sintetico` por encima de otro campo en el struct ⇒ cambia el ORDEN de las claves (encoding/json
//     serializa en orden de declaración) y la comparación literal se rompe, que es correcto: el orden forma
//     parte de lo que se comparó byte a byte alguna vez.
//   - renombrar cualquier etiqueta (`push_name` → `pushname`) ⇒ rojo aquí, en vez de silencio y un campo
//     vacío en el despachador.
func TestColaMetaSinSinteticoNoEmiteLaClave(t *testing.T) {
	meta := ColaMeta{
		Sender:         "5215550001111@s.whatsapp.net",
		SenderAlt:      "5215550001111@lid",
		AddressingMode: "pn",
		PushName:       "Ana",
		Type:           "text",
		// IsGroup y Sintetico se dejan en su cero A PROPÓSITO: son los dos bools con omitempty.
	}

	b, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("json.Marshal(ColaMeta): %v", err)
	}

	const quiere = `{"sender":"5215550001111@s.whatsapp.net","sender_alt":"5215550001111@lid",` +
		`"addressing_mode":"pn","push_name":"Ana","type":"text"}`
	if got := string(b); got != quiere {
		t.Errorf("el sobre de una fila REAL cambió de forma:\n  got  %s\n  want %s", got, quiere)
	}
	if strings.Contains(string(b), "sintetico") {
		t.Error("una fila que NO es sintética lleva la clave `sintetico` en su sobre: sin omitempty, la " +
			"marca del inyector se cuela en el JSON persistido de todas las filas del Edge, que es " +
			"justamente lo que MP-10 no puede permitirse cambiar")
	}
}

// TestColaMetaSinteticoRoundTrip cierra el círculo del séptimo campo: cuando SÍ vale true, viaja en el JSON
// y DecodeColaMeta lo vuelve a leer. Sin esto, el `omitempty` de arriba podría estar "bien" a base de no
// emitir nunca la clave.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO:
//   - cambiar la etiqueta a `json:"synthetic,omitempty"` (o cualquier otra) ⇒ el escritor y el lector dejan
//     de hablar el mismo idioma sin que falle ningún Unmarshal: la marca se perdería EN SILENCIO, que es el
//     modo de fallo que la cabecera de colasobre.go describe para todo este sobre.
//   - hacer que DecodeColaMeta devuelva el cero cuando el JSON trae claves desconocidas ⇒ rojo.
func TestColaMetaSinteticoRoundTrip(t *testing.T) {
	b, err := json.Marshal(ColaMeta{
		Sender:    "5215550001111@s.whatsapp.net",
		Sintetico: true,
	})
	if err != nil {
		t.Fatalf("json.Marshal(ColaMeta): %v", err)
	}

	const quiere = `{"sender":"5215550001111@s.whatsapp.net","sintetico":true}`
	if got := string(b); got != quiere {
		t.Errorf("la marca local del inyector no sale con la forma esperada:\n  got  %s\n  want %s", got, quiere)
	}

	leida, err := DecodeColaMeta(b)
	if err != nil {
		t.Fatalf("DecodeColaMeta: %v", err)
	}
	if !leida.Sintetico {
		t.Error("el sobre se escribió con `sintetico:true` y se releyó como false: la marca LOCAL del " +
			"inyector se pierde entre el que escribe la fila y el despachador que la abre")
	}
}
