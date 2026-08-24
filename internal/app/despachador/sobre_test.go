package despachador

// sobre_test.go — LA REGLA DEL SOBRE, hasta sus bordes (Plan 051 Ola 3 · T3.5, bloque (d)).
//
// `despachador_test.go` ya recorre los OCHO motivos canónicos y comprueba que las filas que los traen se
// entregan y se cuentan. Aquí se cierran las tres cosas que aquello no cubría, y que son justamente por
// donde el sobre se filtraría o se contaría mal:
//
//  1. QUE EL JSON DEL SOBRE NO APAREZCA EN *NADA* DE LO ENTREGADO. Hasta el 2026-08-24 el argumento era
//     «no basta con `Intent == nil`, porque alguien podría rescatar el sobre metiéndolo en `Text`»; el
//     campo `Intent` ya no existe (T1.6-5, ADR-0045) y por eso este barrido es HOY LA ÚNICA aserción del
//     contrato: ADR-0038 §(e) prohíbe que el sobre salga del Edge, no que salga por un campo concreto.
//  2. EL MOTIVO QUE ESTA VERSIÓN NO CONOCE — el «noveno motivo» escrito por un binario más nuevo (un
//     rollback a medias). Es el único caso en que el desglose puede mentir, y tiene contador propio.
//  3. LOS DOS SOBRES QUE NO SE PUEDEN LEER — el `intent_json` corrupto y el `meta_enc` corrupto. En los dos
//     el criterio es el mismo y es el que hay que blindar: SE ENTREGA IGUAL. Perder una clasificación o un
//     remitente es malo; retener el mensaje es peor.
//
// Reutiliza el arnés de `despachador_test.go` (colaFake, sinkFake, despertadorManual, relojFalso, arrancar,
// filaNueva/filaVieja): mismo paquete a propósito, para que no nazcan dos moldes del mismo bucle.
//
// ⚠️ TODOS LOS CASOS DE ESTE FICHERO USAN `filaVieja`, y es información, no un detalle: bajo pull ningún
// productor del Edge escribe en `intent_json`, así que un sobre —bueno, ilegible o del futuro— sólo puede
// venir de una cola escrita por un binario anterior. Lo que se prueba aquí es que esas colas se drenan
// bien mientras se vacían.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// exigirSobreNoViaja comprueba que NI UN BYTE del sobre de omisión aparece en el evento entregado.
//
// 🔴 POR QUÉ SE BARRE EL EVENTO ENTERO Y NO UN CAMPO: el contrato de ADR-0038 §(e) es que el sobre MUERE
// EN EL EDGE, no que un campo concreto esté a nil. Bastaría con que alguien lo concatenara al `Text` «para
// no perder la traza». (Hasta T1.6-5 este helper empezaba comprobando `evt.Intent == nil`, que era la
// aserción del test de los ocho motivos; ese campo ya no existe y el barrido se quedó solo.)
//
// INV-051.1: si algo casa, el mensaje de fallo dice QUÉ aguja apareció y en qué mensaje, pero NO vuelca el
// evento — llevaría el texto y el push_name a la salida de CI, que es un log más.
func exigirSobreNoViaja(t *testing.T, evt domain.InboundEvent, sobre string, motivo app.MotivoOmitido) {
	t.Helper()
	// El evento entero, serializado con %+v: incluye todos los campos exportados. Es una cadena LOCAL que
	// no se imprime salvo que haya hallazgo.
	pajar := fmt.Sprintf("%+v", evt)
	for _, aguja := range []string{sobre, `"omitido"`, "omitido", string(motivo)} {
		if aguja == "" {
			continue
		}
		if strings.Contains(pajar, aguja) {
			t.Fatalf("el sobre de omisión se coló en el evento entregado (%s): apareció %q. "+
				"El sobre MUERE EN EL EDGE (ADR-0038 §(e)); lo que sale es el CONTADOR por motivo, no el sobre",
				evt.MessageID, aguja)
		}
	}
}

// TestSobreDeOmisionNoSeCuelaPorNingunCampoDelEvento es el complemento del recorrido de los ocho motivos:
// allí se comprueba que el sobre no llega como `Intent`; aquí, que no llega POR NINGÚN SITIO.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): en `Despachador.evento`, hacer que la rama de
// `app.EsOmitido` deje rastro del sobre en el evento — p.ej. `evt.Text = evt.Text + c.IntentJSON`.
func TestSobreDeOmisionNoSeCuelaPorNingunCampoDelEvento(t *testing.T) {
	motivos := app.MotivosOmitido()
	filas := make([]*app.ColaCabeza, 0, len(motivos))
	for i, m := range motivos {
		id := int64(i + 1)
		filas = append(filas, filaVieja(id, id*10, app.SobreOmitido(m)))
	}
	cola := nuevaColaFake(filas...)
	a := arrancar(t, cola)

	for _, m := range motivos {
		evt := a.esperarEntrega(t)
		exigirSobreNoViaja(t, evt, app.SobreOmitido(m), m)
		// Y el mensaje sí sale entero: lo que muere es el sobre, no el contenido.
		if evt.Text == "" {
			t.Fatalf("el mensaje salió sin texto (%s): se estaba omitiendo la clasificación, no el mensaje", evt.MessageID)
		}
	}
}

// TestSobreConMotivoDelFuturoSaleSinIntentYSeCuentaAparte cubre el caso que el recorrido de la lista
// canónica NO puede cubrir por construcción: un motivo que ESTA versión no conoce.
//
// EL ESCENARIO ES REAL Y TIENE NOMBRE: un rollback a medias. Un binario nuevo escribió en la cola un sobre
// con un motivo que su enum ya tenía; se revierte el Edge a la versión anterior y ese binario viejo se
// encuentra la fila en disco. Como la cola es DURABLE, el sobre le sobrevive al despliegue.
//
// LAS TRES COSAS QUE TIENEN QUE PASAR, y ninguna es obvia:
//   - el mensaje SE ENTREGA igual, sin intención (jamás se retiene un mensaje por no entender su sobre);
//   - NO se inventa una entrada en el mapa del desglose — ese mapa se declara inmutable en `New` y se lee
//     concurrentemente, así que escribirlo aquí sería una escritura concurrente sobre un mapa;
//   - se cuenta en `OmitidosFueraDeLista`, que es la serie que delata el rollback a medias.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - en `contarOmitido`, sustituir el contador aparte por `d.omitidos[motivo] = new(atomic.Int64)` (o por
//     un simple `return`) ⇒ FALLA en la aserción de `OmitidosFueraDeLista`;
//   - hacer que el desglose crezca con el motivo desconocido ⇒ FALLA en la aserción de tamaño del desglose;
//   - retener el mensaje por no reconocer el motivo ⇒ FALLA en la entrega.
func TestSobreConMotivoDelFuturoSaleSinIntentYSeCuentaAparte(t *testing.T) {
	// LITERAL, no derivado: `app.SobreOmitido` de un motivo desconocido devuelve "", precisamente porque no
	// está en la lista. Lo que hay en disco es este JSON, escrito por una versión que sí lo conocía.
	const sobreDelFuturo = `{"omitido":"motivo_que_esta_version_no_conoce"}`
	// Se comprueba que de verdad es «del futuro» para esta versión: si algún día entra en la lista canónica,
	// este test tiene que avisar en vez de seguir probando otra cosa.
	for _, m := range app.MotivosOmitido() {
		if string(m) == "motivo_que_esta_version_no_conoce" {
			t.Fatal("el motivo elegido como 'del futuro' entró en la lista canónica; elige otro para este test")
		}
	}

	cola := nuevaColaFake(filaVieja(1, 10, sobreDelFuturo))
	a := arrancar(t, cola)

	evt := a.esperarEntrega(t)
	if evt.Text != "hola" {
		t.Fatalf("el mensaje se entregó mutilado (%s): un motivo que no entendemos no puede costar el mensaje", evt.MessageID)
	}

	a.sincronizar(t)
	if got := a.d.OmitidosFueraDeLista(); got != 1 {
		t.Fatalf("omitidos_fuera_de_lista = %d, se esperaba 1: es la ÚNICA serie que delata un rollback a medias", got)
	}
	if got := a.d.Omitidos(); got != 1 {
		t.Fatalf("omitidos (total) = %d, se esperaba 1: el total incluye los de fuera de lista", got)
	}
	// El desglose NO crece: sigue teniendo exactamente las entradas de la lista canónica, y todas a 0.
	desglose := a.d.OmitidosPorMotivo()
	if len(desglose) != len(app.MotivosOmitido()) {
		t.Fatalf("el desglose creció a %d entradas: un motivo desconocido no puede escribir en un mapa declarado inmutable",
			len(desglose))
	}
	for m, n := range desglose {
		if n != 0 {
			t.Fatalf("el motivo canónico %q se contó %d veces por un sobre que no era suyo", m, n)
		}
	}
	if got := a.d.IntentsDescartados(); got != 0 {
		t.Fatalf("intents_descartados = %d, se esperaba 0: era un sobre de OMISIÓN, no una clasificación", got)
	}
	if got := a.d.Despachados(); got != 1 {
		t.Fatalf("despachados = %d, se esperaba 1: el mensaje salió", got)
	}
}

// TestSobreIlegibleSeEntregaIgualYSeCuenta: el `intent_json` está, pero no es ninguna de las dos formas
// conocidas (ni `{"omitido":…}` ni el sobre del cajero). Un JSON corrupto, un formato viejo sin migrar.
//
// EL CRITERIO ES EL FALLO SEGURO: se entrega SIN intención y se cuenta en `SobresIlegibles`, cuyo valor
// sano es CERO. Retener el mensaje sería convertir un byte corrupto en una conversación perdida.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): hacer que la rama `!ok` de `app.LeerSobreClasificado`
// devuelva `veredicto{}` (el del camino normal bajo pull) en vez de `veredicto{ilegible: true}` ⇒
// `sobres_ilegibles` se queda en 0, que es el mismo valor que tiene cuando todo va bien: la degradación se
// vuelve invisible. 🔴 Y hoy esa mutación es MÁS FÁCIL DE COLAR que antes, porque `veredicto{}` pasó de
// ser el caso raro a ser el caso dominante — de ahí que este test importe más, no menos.
func TestSobreIlegibleSeEntregaIgualYSeCuenta(t *testing.T) {
	casos := []struct {
		nombre string
		sobre  string
	}{
		// JSON roto: ni siquiera deserializa.
		{"json truncado", `{"intent":"crear_ped`},
		// JSON válido pero sin nombre de intención: `LeerSobreClasificado` devuelve ok=false a propósito,
		// porque una intención sin nombre haría que el Cloud resolviera el flujo contra la nada.
		{"sobre sin nombre de intencion", `{"params":{"producto":"pan"},"confidence":0.9}`},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			cola := nuevaColaFake(filaVieja(1, 10, c.sobre))
			a := arrancar(t, cola)

			evt := a.esperarEntrega(t)
			if evt.Text != "hola" {
				t.Fatalf("el mensaje se entregó mutilado (%s): un sobre corrupto no puede costar el mensaje", evt.MessageID)
			}
			a.sincronizar(t)
			if got := a.d.SobresIlegibles(); got != 1 {
				t.Fatalf("sobres_ilegibles = %d, se esperaba 1 (CERO es el valor sano, y por eso no puede confundirse con este caso)", got)
			}
			if got := a.d.IntentsDescartados(); got != 0 {
				t.Fatalf("intents_descartados = %d: un sobre que no se pudo LEER no es una clasificación descartada; "+
					"son dos hechos distintos y sólo uno manda a mirar la integridad del disco", got)
			}
			if got := a.d.Despachados(); got != 1 {
				t.Fatalf("despachados = %d, se esperaba 1: el mensaje salió", got)
			}
		})
	}
}

// TestMetaIlegibleEntregaElMensajeSinRemitente: el `meta_enc` descifró bien pero no era JSON válido.
//
// El criterio vuelve a ser el mismo y por eso está aquí: EL MENSAJE SALE IGUAL, con los campos de identidad
// vacíos, y la degradación se CUENTA. Perder el remitente es malo; perder el mensaje es peor.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: hacer que `Despachador.evento` retorne (o retenga la fila) cuando
// `app.DecodeColaMeta` falla ⇒ el mensaje no se entrega y FALLA la entrega. Quitar el `metasIlegibles.Add(1)`
// ⇒ FALLA el contador y la ceguera vuelve a ser total (el evento no se distingue de uno sin push_name).
func TestMetaIlegibleEntregaElMensajeSinRemitente(t *testing.T) {
	fila := filaVieja(1, 10, `{"intent":"crear_pedido","confidence":0.9}`)
	fila.Meta = []byte(`{esto no es json`)
	cola := nuevaColaFake(fila)
	a := arrancar(t, cola)

	evt := a.esperarEntrega(t)
	if evt.Text != "hola" {
		t.Fatalf("el mensaje no salió por un metadato ilegible (%s)", evt.MessageID)
	}
	if evt.Sender != "" || evt.PushName != "" || evt.Type != "" {
		t.Fatalf("meta ilegible produjo campos de identidad poblados (%s): sender_vacio=%t push_vacio=%t type=%q",
			evt.MessageID, evt.Sender == "", evt.PushName == "", evt.Type)
	}
	// El identificador de trazabilidad SÍ sobrevive: viene de columnas propias, no del metadato.
	if evt.MessageID == "" || evt.Chat == "" {
		t.Fatal("se perdieron los identificadores de enrutado, que no viven en el metadato sino en columnas propias")
	}
	// Y el sobre de la fila vieja se procesó igual: un metadato roto no puede alterar el veredicto sobre el
	// `intent_json`, que vive en otra columna.
	a.sincronizar(t)
	if got := a.d.IntentsDescartados(); got != 1 {
		t.Fatalf("intents_descartados = %d, se esperaba 1: el metadato y el sobre son columnas distintas "+
			"y un fallo en una no puede cambiar la lectura de la otra", got)
	}
	a.sincronizar(t)
	if got := a.d.MetasIlegibles(); got != 1 {
		t.Fatalf("metas_ilegibles = %d, se esperaba 1 (CERO es el valor sano; si crece, los mensajes salen sin remitente)", got)
	}
	if got := a.d.Despachados(); got != 1 {
		t.Fatalf("despachados = %d, se esperaba 1", got)
	}
}
