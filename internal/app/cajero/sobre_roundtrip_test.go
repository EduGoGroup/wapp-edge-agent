package cajero

// sobre_roundtrip_test.go — EL CONTRATO DE CLAVES ENTRE EL CAJERO Y EL DESPACHADOR
// (Plan 051 Ola 3 · T3.5, verificación cruzada).
//
// 🔴 EL FALLO CARO DE LA OLA 3 ES SILENCIOSO, y por eso necesita un test propio. El `intent_json` lo
// ESCRIBE este paquete (`sobreCajero`, cajero.go) y lo LEE otro proceso, el despachador, con un tipo
// DISTINTO (`app.SobreClasificado`, app/colasobre.go). Lo único que ata a los dos son las CUATRO ETIQUETAS
// JSON: en disco no hay un struct, hay bytes. Si una clave diverge —"name" en vez de "intent", "version"
// en vez de "config_version"— no se rompe nada: el `Unmarshal` no falla (los campos que no casan se
// ignoran), no se escribe una sola línea de log, y el campo llega VACÍO. El síntoma aparece en el Cloud,
// que resuelve el flujo contra una intención sin nombre, a kilómetros de la causa.
//
// Los tests que ya existían cubren cada mitad por separado: `TestCiclo_UnaInferenciaPorLote_EnOrdenDeSeq`
// (este paquete) deserializa el sobre con `sobreCajero` —el MISMO tipo que lo escribió, así que no puede
// detectar una divergencia—, y `TestSobreDelCajeroSeLeeConSusClaves` (despachador) parte de un literal
// TRANSCRITO A MANO — que es exactamente lo que se desincroniza. Este es el único que cierra el círculo:
// serializa con el productor real y lee con el consumidor real.
//
// La deuda de fondo (que el cajero pase a usar `app.SobreClasificado` y borre `sobreCajero`) sigue
// anotada en app/colasobre.go. Mientras exista, este test es lo que impide que las dos copias diverjan.

import (
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-intent/classifier"
)

// TestSobreDelCajeroLoLeeElDespachadorSinPerderNingunaClave es el round-trip real: el cajero serializa, el
// despachador lee, y las cuatro claves llegan enteras.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (cualquiera de las dos direcciones de la divergencia):
//   - renombrar una etiqueta de `sobreCajero` (p.ej. `json:"intent"` → `json:"name"`) ⇒ FALLA el literal Y
//     falla la lectura del campo correspondiente;
//   - renombrar una etiqueta de `app.SobreClasificado` ⇒ el literal sigue bien (lo produce el cajero) y
//     FALLA la lectura: ese es el caso que hoy nadie cazaría;
//   - quitar `omitempty` de `params`/`config_version`, o añadirlo a `confidence` ⇒ FALLA el literal (la
//     forma que queda escrita en la columna cambia, y con ella lo que verá un binario viejo tras un
//     rollback).
func TestSobreDelCajeroLoLeeElDespachadorSinPerderNingunaClave(t *testing.T) {
	// Se construye el Cajero a pelo y no con New: `sobre` sólo consulta `c.configVersion`, y pasar por el
	// constructor obligaría a inventar una cola, un clasificador y un breaker para probar una serialización.
	c := &Cajero{configVersion: func() string { return "v7" }}

	res := classifier.Classification{
		Intent:     "crear_pedido",
		Params:     map[string]string{"producto": "pan", "cantidad": "2"},
		Confidence: 0.87,
	}
	raw, err := c.sobre(res)
	if err != nil {
		t.Fatalf("el cajero no pudo serializar su sobre: %v", err)
	}

	// (1) EL LITERAL — la forma EXACTA que queda escrita en la columna `intent_json`.
	//
	// Se fija a propósito, aunque el round-trip de abajo ya pruebe la compatibilidad: la columna es un
	// FORMATO DE DISCO que sobrevive a los despliegues (la cola es durable), así que un cambio de forma
	// tiene que ser una decisión, no un efecto colateral. `encoding/json` ordena las claves de un mapa
	// alfabéticamente, de ahí `cantidad` antes que `producto`.
	const esperado = `{"intent":"crear_pedido","params":{"cantidad":"2","producto":"pan"},"confidence":0.87,"config_version":"v7"}`
	if raw != esperado {
		t.Fatalf("el sobre del cajero cambió de forma.\n got: %s\nwant: %s", raw, esperado)
	}

	// (2) EL ROUND-TRIP — lo lee el consumidor real, con SU tipo.
	sobre, ok := app.LeerSobreClasificado(raw)
	if !ok {
		t.Fatalf("el despachador NO reconoció como sobre de clasificación lo que el cajero acaba de escribir: %s", raw)
	}
	if sobre.Intent != "crear_pedido" {
		t.Fatalf("clave `intent` perdida en el viaje: %q", sobre.Intent)
	}
	if sobre.Confidence != 0.87 {
		t.Fatalf("clave `confidence` perdida en el viaje: %v", sobre.Confidence)
	}
	if sobre.ConfigVersion != "v7" {
		t.Fatalf("clave `config_version` perdida en el viaje: %q", sobre.ConfigVersion)
	}
	if len(sobre.Params) != 2 || sobre.Params["producto"] != "pan" || sobre.Params["cantidad"] != "2" {
		t.Fatalf("clave `params` perdida en el viaje: %d claves leídas", len(sobre.Params))
	}

	// (3) EL ORDEN DE LAS DOS PUERTAS — un sobre del cajero NO es un sobre de omisión. Si `EsOmitido`
	// respondiera true aquí, el despachador tiraría una intención perfectamente calculada.
	if motivo, esOmitido := app.EsOmitido(raw); esOmitido {
		t.Fatalf("el sobre del cajero se leyó como una omisión (motivo %q): la intención se tiraría", motivo)
	}
}

// TestSobreDelCajeroSinConfigVersionNiParams fija la otra forma que la columna puede tomar: la del sobre
// mínimo. Los dos `omitempty` hacen que `params` y `config_version` DESAPAREZCAN del JSON, no que salgan
// vacíos, y eso también es formato de disco.
//
// Importa porque `configVersion` es nil hasta que llega la config del Cloud (el cajero arranca sin ella):
// los sobres escritos en esa ventana tienen esta forma, y el despachador tiene que saber leerlos.
func TestSobreDelCajeroSinConfigVersionNiParams(t *testing.T) {
	c := &Cajero{} // configVersion nil: el cajero aún no recibió la config del Cloud

	raw, err := c.sobre(classifier.Classification{Intent: "consultar_estado", Confidence: 0.5})
	if err != nil {
		t.Fatalf("sobre: %v", err)
	}
	const esperado = `{"intent":"consultar_estado","confidence":0.5}`
	if raw != esperado {
		t.Fatalf("el sobre mínimo cambió de forma.\n got: %s\nwant: %s", raw, esperado)
	}
	sobre, ok := app.LeerSobreClasificado(raw)
	if !ok || sobre.Intent != "consultar_estado" || sobre.Confidence != 0.5 {
		t.Fatalf("el despachador no leyó el sobre mínimo: ok=%t intent=%q confidence=%v", ok, sobre.Intent, sobre.Confidence)
	}
	if sobre.ConfigVersion != "" || len(sobre.Params) != 0 {
		t.Fatalf("el sobre mínimo llegó con campos inventados: config_version=%q params=%d", sobre.ConfigVersion, len(sobre.Params))
	}
}

// TestUnSobreConLaClaveEquivocadaNoSeCuelaComoIntencion es el CONTRAEJEMPLO: la demostración de que el fallo
// que este fichero previene es real y silencioso.
//
// El JSON de abajo es el mismo sobre con `"name"` en vez de `"intent"` — la divergencia típica de un
// renombrado a medias. No falla ningún Unmarshal y no produce ningún log; lo único que impide que se cuele
// como una intención SIN NOMBRE es el `Intent == ""` de `app.LeerSobreClasificado`.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: quitar ese `if s.Intent == ""` de `LeerSobreClasificado` ⇒ este sobre
// pasaría por bueno y el despachador entregaría un `domain.ClassifiedIntent` con `Name: ""`, que en el
// Cloud es un flujo resuelto contra la nada.
func TestUnSobreConLaClaveEquivocadaNoSeCuelaComoIntencion(t *testing.T) {
	const divergente = `{"name":"crear_pedido","params":{"producto":"pan"},"confidence":0.87,"config_version":"v7"}`

	sobre, ok := app.LeerSobreClasificado(divergente)
	if ok {
		t.Fatalf("un sobre con la clave equivocada pasó por bueno: se entregaría una intención sin nombre "+
			"(intent=%q, confidence=%v). Ese es el fallo caro de la Ola 3, y es invisible en el log",
			sobre.Intent, sobre.Confidence)
	}
}
