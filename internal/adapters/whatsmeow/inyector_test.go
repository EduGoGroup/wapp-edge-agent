package whatsmeow

// inyector_test.go — EL INYECTOR DE ENTRANTES SINTÉTICOS (MP-10 Parte A).
//
// Lo que se custodia aquí no es «que el fabricante rellene campos»: es que el sintético RECORRA EL MISMO
// CAMINO que un entrante real. Un sintético que se cuele por una rama distinta —la del mensaje sin hora,
// la del grupo, la del texto vacío— produce una medida MÁS CORTA que la de campo, y publicar eso como el
// p99 del handler es peor que no medir nada: es el error que MP-10 existe para dejar de cometer.
//
// Reutiliza los dobles del paquete (spyCola, callLog, listenerConCola, liveMessage, quietLogger,
// gatewayDePrueba, cuerpoDelMetodo, nombreDeLoLlamado): mismo paquete a propósito.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// inyeccionValida es el molde de una inyección bien formada: lo mínimo que hace falta para que el
// fabricante devuelva un evento. Los tests que prueban un rechazo parten de aquí y rompen UNA cosa, de
// modo que lo que falle sea la que rompieron y no un descuido del molde.
func inyeccionValida() app.InyeccionEntrante {
	return app.InyeccionEntrante{
		Texto:  "quiero dos empanadas",
		Lote:   "mp10-t1",
		Indice: 1,
	}
}

// --- El fabricante: las cuatro guardas de la puerta ---

// TestFabricarEntranteSintetico_PasaLasCuatroGuardasDeLaPuerta es EL test de este fichero. Las cuatro
// guardas de `onMessage` no descartan todas —dos desvían a otra rama—, y las cuatro arruinan la medida:
//
//	IsFromMe (listener.go:511)          — descarta en la puerta: ni fila ni camino.
//	Timestamp cero (listener.go:525)    — NO descarta: toma la rama que SALTA la ventana. Handler más corto.
//	Ventana ADR-0037 (listener.go:553)  — descarta: ni fila ni camino.
//	IsGroup (listener.go:716)           — NO descarta: la fila nace `clasificado`/`no_elegible`.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - `IsFromMe: true` en FabricarEntranteSintetico ⇒ falla la primera aserción (y en el test de conducta
//     de abajo desaparece la fila entera).
//   - `Timestamp: time.Time{}` ⇒ falla la aserción de «no es cero»: el sintético entraría por la rama del
//     entrante sin hora, que sella con la hora local y NO evalúa la ventana. Mide otro handler.
//   - `Timestamp: time.Now().Add(-24 * time.Hour)` ⇒ falla la aserción del umbral: la ventana lo
//     descartaría y el histograma se llenaría en la serie DESCARTADO (microsegundos) en vez de en
//     ENCOLADO, o sea el p99 saldría espectacularmente bueno midiendo lo que no es.
//   - `IsGroup: true` ⇒ falla la última aserción.
func TestFabricarEntranteSintetico_PasaLasCuatroGuardasDeLaPuerta(t *testing.T) {
	evt, err := FabricarEntranteSintetico(inyeccionValida())
	if err != nil {
		t.Fatalf("el fabricante rechazó una inyección válida: %v", err)
	}

	if evt.Info.IsFromMe {
		t.Errorf("IsFromMe = true: el eco propio se descarta en la puerta (listener.go:511) sin dejar fila " +
			"y sin llegar siquiera al carril rápido. El inyectado no se mediría nunca")
	}
	if evt.Info.Timestamp.IsZero() {
		t.Errorf("Timestamp es CERO: eso NO descarta el mensaje, lo desvía a la rama del entrante sin hora " +
			"(listener.go:525), que SALTA la guarda de ventana y sella con la hora local. Es un camino " +
			"legítimo del listener y NO es el que MP-10 quiere medir")
	}
	// El umbral se calcula con el respaldo MÁS ESTRICTO posible: sello cero ⇒ resolveThreshold cae a `now`
	// (inbound_window.go:139-144), así que el umbral es `now − margen`. Si el sintético lo supera con ese
	// respaldo, lo supera con cualquier sello real (que solo puede ser anterior o igual a `now`).
	umbral := resolveThreshold(time.Time{}, defaultConnectMargin, time.Now())
	if !evt.Info.Timestamp.After(umbral) {
		t.Errorf("el sello del sintético (%s) NO supera el umbral de la ventana (%s): el entrante se "+
			"descartaría por el ADR-0037 (listener.go:553) y su tiempo caería en la serie DESCARTADO, que "+
			"cuesta microsegundos. El p99 publicado sería el de un descarte, no el del handler",
			evt.Info.Timestamp.Format(time.RFC3339Nano), umbral.Format(time.RFC3339Nano))
	}
	if evt.Info.IsGroup {
		t.Errorf("IsGroup = true: la fila nacería ya `clasificado` con la marca `no_elegible` " +
			"(listener.go:716) y el cajero no la reclamaría jamás; se mediría un camino que en campo solo " +
			"recorre el tráfico de grupos")
	}
	// La quinta condición, que no es una guarda sino la validación de entrada: sin texto la fila nace
	// `clasificado`/`sin_texto` (listener.go:713). El fabricante lo rechaza; aquí se comprueba que el
	// evento que SÍ produce lleva el texto donde `messageText` lo busca (GetConversation, listener.go:936).
	if got := messageText(evt); got == "" {
		t.Errorf("messageText() devolvió vacío: el texto no está donde el listener lo busca " +
			"(Message.Conversation), así que la fila nacería con la marca `sin_texto`")
	}
}

// TestFabricarEntranteSintetico_LaMarcaPortanteYSuUnicidad custodia el guardarraíl 🔴 «los sintéticos
// deben ser distinguibles de los reales aguas abajo» y, de paso, la propiedad que hace que la medida no
// mienta: dos índices distintos son dos filas distintas.
//
// 🔴 POR QUÉ LA UNICIDAD ES CRÍTICA Y NO COSMÉTICA: la cola tiene índice único (session_id, wa_message_id)
// y el store trata el choque como DUPLICADO devolviendo nil (listener.go:618), es decir SIN error. Dos
// inyectados con el mismo ID no dan dos filas y un fallo: dan UNA fila y ningún síntoma. El histograma
// saldría corto y nadie sabría por qué.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - quitar `PrefijoSintetico` de idSintetico (o vaciar la constante) ⇒ el sintético sube a la nube
//     disfrazado de mensaje real.
//   - dejar de meter el índice en el ID (p.ej. `"%s%s"` en vez de `"%s%s-%04d"`) ⇒ los dos IDs coinciden.
//   - dejar de meter el LOTE en el ID ⇒ los IDs coinciden entre corridas distintas (no lo caza este
//     subtest, sí el de abajo que compara dos lotes).
func TestFabricarEntranteSintetico_LaMarcaPortanteYSuUnicidad(t *testing.T) {
	p := inyeccionValida()

	uno, err := FabricarEntranteSintetico(p)
	if err != nil {
		t.Fatalf("inyección válida rechazada: %v", err)
	}
	if !strings.HasPrefix(uno.Info.ID, PrefijoSintetico) {
		t.Fatalf("el wa_message_id %q NO empieza por %q.\n"+
			"    CONSECUENCIA: ésta es la ÚNICA marca que sale del Edge — la del meta viaja cifrada con la\n"+
			"    DEK y la nube no la ve nunca. Sin el prefijo, la plataforma no puede separar el tráfico de\n"+
			"    prueba del real y los sintéticos acaban en un informe de negocio como pedidos que nadie hizo.",
			uno.Info.ID, PrefijoSintetico)
	}

	p.Indice = 2
	dos, err := FabricarEntranteSintetico(p)
	if err != nil {
		t.Fatalf("inyección válida rechazada: %v", err)
	}
	if uno.Info.ID == dos.Info.ID {
		t.Errorf("dos índices distintos dieron el MISMO wa_message_id (%q): el índice único "+
			"(session_id, wa_message_id) absorbe el segundo como duplicado y el store devuelve nil, así que "+
			"la fila se evapora SIN error y el histograma sale corto", uno.Info.ID)
	}

	p.Lote, p.Indice = "mp10-otra-corrida", 1
	otroLote, err := FabricarEntranteSintetico(p)
	if err != nil {
		t.Fatalf("inyección válida rechazada: %v", err)
	}
	if otroLote.Info.ID == uno.Info.ID {
		t.Errorf("dos LOTES distintos con el mismo índice dieron el mismo wa_message_id (%q): la segunda "+
			"corrida entera se evaporaría contra el índice único", uno.Info.ID)
	}
}

// TestFabricarEntranteSintetico_RechazaLoQueNoSirveParaMedir fija las dos validaciones de entrada. Las dos
// producen un sintético que NO mide lo que dice medir, y ninguna de las dos falla de forma visible en
// campo si se deja pasar: por eso se cortan aquí, con un error explicativo.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: quitar cualquiera de los dos `if` de FabricarEntranteSintetico ⇒ el
// fabricante devuelve un evento y `err` viene nil.
func TestFabricarEntranteSintetico_RechazaLoQueNoSirveParaMedir(t *testing.T) {
	casos := []struct {
		nombre string
		romper func(*app.InyeccionEntrante)
		porque string
	}{
		{
			nombre: "sin texto",
			romper: func(p *app.InyeccionEntrante) { p.Texto = "" },
			porque: "la fila nacería `clasificado` con la marca `sin_texto` (listener.go:713) y NO recorrería " +
				"el camino que MP-10 mide",
		},
		{
			nombre: "solo espacios en el texto",
			romper: func(p *app.InyeccionEntrante) { p.Texto = "   \t\n " },
			porque: "un texto en blanco no es texto: el fastlane lo resolvería sin tocar el LLM y la fila " +
				"nacería resuelta igual",
		},
		{
			nombre: "sin lote",
			romper: func(p *app.InyeccionEntrante) { p.Lote = "" },
			porque: "dos corridas repetirían el wa_message_id y la segunda la absorbería el índice único SIN error",
		},
	}

	for _, cs := range casos {
		t.Run(cs.nombre, func(t *testing.T) {
			p := inyeccionValida()
			cs.romper(&p)
			evt, err := FabricarEntranteSintetico(p)
			if err == nil {
				t.Fatalf("se fabricó un sintético que no sirve para medir (%s): %s", cs.nombre, cs.porque)
			}
			if evt != nil {
				t.Errorf("con error se devolvió además un evento no nil: el llamante que ignore el error " +
					"inyectaría basura por el camino real")
			}
		})
	}
}

// TestFabricarEntranteSintetico_ElJIDPorDefectoEsConstanteYReconocible fija la decisión del JID de chat
// cuando el llamante no da ninguno: uno solo, constante, con letras (ningún número de WhatsApp las tiene,
// así que no puede colisionar con un chat real).
//
// El segundo subtest es el que importa de verdad: un ChatJID CON GUIONES tiene que llegar INTACTO. El
// camino de envío del repo (`parseRecipient`, sender.go:259) limpia los guiones porque su entrada es un
// teléfono tecleado por un humano; reusarlo aquí mutilaría los JID de lote (`carga-lote7-0003@…`) en
// silencio y las filas dejarían de casar con el LIKE que las borra.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - sustituir `jidSintetico` por `parseRecipient` ⇒ el segundo subtest devuelve "cargalote70003@…".
//   - hacer que el JID por defecto lleve el lote o el índice ⇒ el primero deja de ser constante.
func TestFabricarEntranteSintetico_ElJIDPorDefectoEsConstanteYReconocible(t *testing.T) {
	t.Run("sin ChatJID: el mismo JID sintetico siempre", func(t *testing.T) {
		p := inyeccionValida()
		uno, err := FabricarEntranteSintetico(p)
		if err != nil {
			t.Fatalf("inyección válida rechazada: %v", err)
		}
		p.Lote, p.Indice = "otro-lote", 77
		dos, err := FabricarEntranteSintetico(p)
		if err != nil {
			t.Fatalf("inyección válida rechazada: %v", err)
		}
		if uno.Info.Chat.String() != dos.Info.Chat.String() {
			t.Errorf("el JID por defecto cambió entre inyecciones (%q vs %q): se pidió CONSTANTE para que sea "+
				"reconocible de un vistazo en la cola", uno.Info.Chat.String(), dos.Info.Chat.String())
		}
		if !strings.HasPrefix(uno.Info.Chat.String(), usuarioChatSintetico+"@") {
			t.Errorf("el JID por defecto es %q y debía empezar por %q@: un JID sintético con forma de número "+
				"real es indistinguible de un chat de verdad en la cola",
				uno.Info.Chat.String(), usuarioChatSintetico)
		}
		// En un 1:1 el remitente ES el chat (mismo criterio que cmd/colaseed).
		if uno.Info.Sender.String() != uno.Info.Chat.String() {
			t.Errorf("Sender (%q) != Chat (%q): en un chat 1:1 coinciden, y el meta de la fila publica el "+
				"Sender", uno.Info.Sender.String(), uno.Info.Chat.String())
		}
	})

	t.Run("con ChatJID de lote: los guiones sobreviven", func(t *testing.T) {
		const jid = "carga-lote7-0003@s.whatsapp.net"
		p := inyeccionValida()
		p.ChatJID = jid

		evt, err := FabricarEntranteSintetico(p)
		if err != nil {
			t.Fatalf("un chat_jid de lote fue rechazado: %v (si types.ParseJID no acepta esta forma, hay que "+
				"dejar de parsear y construir el types.JID a mano, como en el camino por defecto)", err)
		}
		if got := evt.Info.Chat.String(); got != jid {
			t.Errorf("el chat_jid llegó como %q y se pasó %q.\n"+
				"    CONSECUENCIA: los JID de lote llevan guiones por diseño (colaseed siembra\n"+
				"    `colaseed-<lote>-0001@…`). Si se mutilan, las filas sintéticas dejan de casar con el LIKE\n"+
				"    que las borra y una corrida abandonada se queda en la cola para siempre.", got, jid)
		}
	})
}

// --- La puerta: InyectarEntrante ---

// TestInyectarEntrante_SinSesionEscuchando_ErrorExplicativoYNoPanic. El Listener solo existe mientras
// serve() mantiene el socket vivo; fuera de eso el puntero es nil. Una inyección ahí NO puede reventar el
// plano de control (es un daemon 24/7): tiene que devolver un error que diga qué falta.
//
// Y NO BASTA CON QUE FALLE: tiene que fallar con el CENTINELA. Este es el ÚNICO test del repo que ata el
// `ErrSinEscuchaViva` a la función que lo produce — el del sessionmgr fabrica el error con una closure, así
// que un `fmt.Errorf` plano aquí se le escaparía entero. Y un error plano aquí no es un detalle de estilo:
// `sessionmgr` deja de poder traducirlo a `ErrInyectorNoCableado`, el borde no lo reconoce, la tanda NO
// aborta y el operador recibe un 200 con `inyectados: 0, errores: 500`. El estado que este error nombra dura
// hasta 60 s cada vez que una sesión se reconecta, así que no es una rama teórica.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - quitar el `if l == nil` de InyectarEntrante ⇒ nil pointer dereference al llamar a l.handleEvent, y el
//     pánico sube al handler HTTP del plano de control.
//   - volver a un `fmt.Errorf` plano (o cambiar el `%w` por `%v`) ⇒ el centinela no viaja y errors.Is falla:
//     es la regresión que deja el 409 inalcanzable.
func TestInyectarEntrante_SinSesionEscuchando_ErrorExplicativoYNoPanic(t *testing.T) {
	g := gatewayDePrueba() // sin setLiveListener: no hay sesión escuchando

	acusar, err := g.InyectarEntrante(context.Background(), inyeccionValida())
	if err == nil {
		t.Fatalf("sin Listener vivo la inyección devolvió err = nil (acusar = %v): el llamante creería que "+
			"midió algo", acusar)
	}
	if !errors.Is(err, ErrSinEscuchaViva) {
		t.Errorf("el error no lleva el centinela ErrSinEscuchaViva (%v): sin él sessionmgr no puede traducirlo "+
			"a ErrInyectorNoCableado, el borde no lo reconoce y una tanda lanzada durante el backoff de "+
			"reconexión contesta 200 con inyectados=0 en vez de 409", err)
	}
	if acusar {
		t.Errorf("con error se devolvió acusar = true: el bool solo vale como señal de diagnóstico y un true " +
			"sin fila es exactamente la señal contraria a la real")
	}
}

// TestInyectarEntrante_SoltarElListenerAlSalirDeServe fija la razón del `defer g.setLiveListener(nil)`: el
// gateway SOBREVIVE a serve(), así que sin la limpieza una inyección tardía entraría por el handler de una
// sesión que ya no escucha.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: borrar el `defer g.setLiveListener(nil)` de serve() no rompe ESTE test
// (serve no es ejercitable), pero sí el AST de abajo; lo que este test custodia es que la limpieza
// FUNCIONE, o sea que setLiveListener(nil) devuelva el gateway al estado «sin sesión».
func TestInyectarEntrante_SoltarElListenerAlSalirDeServe(t *testing.T) {
	cola := &spyCola{calls: &callLog{}}
	g := gatewayDePrueba()
	g.setLiveListener(listenerConCola(cola))

	if _, err := g.InyectarEntrante(context.Background(), inyeccionValida()); err != nil {
		t.Fatalf("con Listener vivo la inyección falló: %v", err)
	}
	g.setLiveListener(nil)
	if _, err := g.InyectarEntrante(context.Background(), inyeccionValida()); err == nil {
		t.Errorf("tras soltar el Listener la inyección siguió funcionando: el puntero apunta a una sesión " +
			"muerta y lo que se mida ahí no es el handler que en campo se ejecuta")
	}
}

// TestInyectarEntrante_DejaFilaConLasDosMarcas es el test de CONDUCTA de extremo a extremo del frente: se
// inyecta por la puerta pública y se mira la fila que llega a la cola.
//
// Se afirman las DOS marcas, porque son dos contratos distintos:
//   - PORTANTE: el prefijo en el `wa_message_id`, que es columna propia de la cola, lo copia el despachador
//     al domain.InboundEvent y acaba en el proto de CloudLink. Es lo único que la NUBE ve.
//   - LOCAL: `sintetico: true` en el meta, que viaja cifrado con la DEK y muere en el Edge.
//
// Y se afirma que la fila nace `nuevo`: si naciera `clasificado` con cualquier marca de omisión, el
// sintético no ejercitaría el tramo que el cajero y el despachador recorren, que es lo que MP-10 mide.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - quitar el campo `Sintetico` de colaMetaPayload (o su relleno en colaMeta) ⇒ falla la marca local.
//   - cambiar la etiqueta JSON `sintetico` por otra ⇒ falla igual (lo que ata a los dos extremos son las
//     CLAVES, no el tipo Go — ver la cabecera de internal/app/colasobre.go).
//   - entrar por `onMessage` en vez de por `handleEvent` ⇒ NO lo caza este test (la fila sale idéntica);
//     lo caza el AST de abajo, que es la razón de que exista.
func TestInyectarEntrante_DejaFilaConLasDosMarcas(t *testing.T) {
	cola := &spyCola{calls: &callLog{}}
	// fastLane determinista: el default es el léxico real del clasificador, y que la fila nazca `nuevo` no
	// puede depender de qué frase se eligió en el molde.
	l := listenerConCola(cola, WithFastLane(func(string) bool { return false }))
	g := gatewayDePrueba()
	g.setLiveListener(l)

	acusar, err := g.InyectarEntrante(context.Background(), inyeccionValida())
	if err != nil {
		t.Fatalf("la inyección falló: %v", err)
	}
	if !acusar {
		t.Errorf("acusar = false: significa que la fila NO llegó a disco. El bool no viaja a WhatsApp " +
			"(nadie se lo devuelve a whatsmeow), pero como señal de diagnóstico dice que el INSERT falló")
	}
	if len(cola.got) != 1 {
		t.Fatalf("filas anotadas = %d, se esperaba 1 (%v)", len(cola.got), colaWAIDs(cola.got))
	}
	fila := cola.got[0]

	if !strings.HasPrefix(fila.WAMessageID, PrefijoSintetico) {
		t.Errorf("el wa_message_id de la fila (%q) no lleva la marca PORTANTE %q: es la única que sale del "+
			"Edge", fila.WAMessageID, PrefijoSintetico)
	}
	if fila.Estado != app.EstadoNuevo {
		t.Errorf("la fila sintética nació en estado %q y debía nacer %q: una fila que nace resuelta no la "+
			"reclama el cajero, así que el inyectado no ejercitaría el tramo que MP-10 mide",
			fila.Estado, app.EstadoNuevo)
	}
	var meta colaMetaPayload
	if err := json.Unmarshal(fila.Meta, &meta); err != nil {
		t.Fatalf("el meta de la fila no es JSON válido: %v", err)
	}
	if !meta.Sintetico {
		t.Errorf("el meta de la fila NO trae `sintetico: true`: la marca local se deriva del prefijo del " +
			"wa_message_id en colaMeta (listener.go); sin ella no se puede auditar la cola sin re-derivar " +
			"el prefijo a mano")
	}
}

// TestColaMeta_UnEntranteREAL_NoSeMarcaComoSintetico es la otra mitad del test de arriba, y sin ella
// aquél no probaría nada: un `Sintetico: true` constante lo pasaría igual.
//
// Se comprueba además que la CLAVE no aparezca en el JSON (el `omitempty` de la etiqueta): lo que se
// persiste en `meta_enc` de todas las filas reales del Edge no puede engordar por una marca que solo tiene
// sentido en una de cada millón.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: rellenar `Sintetico` con `true` fijo (o con una condición que no sea el
// prefijo) ⇒ todas las filas reales del Edge se marcan como sintéticas y el guardarraíl se invierte.
func TestColaMeta_UnEntranteREAL_NoSeMarcaComoSintetico(t *testing.T) {
	cola := &spyCola{calls: &callLog{}}
	l := listenerConCola(cola)

	l.handleEvent(context.Background(), liveMessage("MSGID-REAL", "quiero dos empanadas"))

	if len(cola.got) != 1 {
		t.Fatalf("filas anotadas = %d, se esperaba 1 (%v)", len(cola.got), colaWAIDs(cola.got))
	}
	var meta colaMetaPayload
	if err := json.Unmarshal(cola.got[0].Meta, &meta); err != nil {
		t.Fatalf("el meta de la fila no es JSON válido: %v", err)
	}
	if meta.Sintetico {
		t.Errorf("un entrante REAL (wa_message_id %q) se marcó como sintético: el guardarraíl queda del "+
			"revés y la nube dejaría de poder separar el tráfico de prueba del de negocio",
			cola.got[0].WAMessageID)
	}
	// El JSON del meta NO se imprime nunca (INV-051.1: lleva el push_name y el JID del remitente).
	if bytes.Contains(cola.got[0].Meta, []byte(`"sintetico"`)) {
		t.Errorf("la clave `sintetico` aparece en el meta de una fila real: falta el `omitempty` en la " +
			"etiqueta, y esa clave se persiste cifrada en TODAS las filas del Edge para no decir nada")
	}
}

// --- La forma del cableado: lo que ningún test de conducta puede ver ---

// TestInyectarEntrante_EntraPorHandleEvent_YServePublicaSuListener es el test de FORMA del frente, y
// existe porque las dos propiedades que fija son INVISIBLES a un test de conducta.
//
// 🔴 (a) LA PUERTA ES `handleEvent`, NO `onMessage`. Las dos producen la MISMA fila, así que ningún
// espía de la cola las distingue. Y sin embargo la diferencia es la razón entera de este micro-plan:
//   - `handleEvent` es literalmente lo que Register cablea en AddEventHandlerWithSuccessStatus
//     (listener.go:369-371), o sea el punto por el que entra un entrante de VERDAD;
//   - incluye el `recover` que sostiene la garantía del acuse (listener.go:408-416) y el `switch` de tipo;
//   - deja el tramo cronometrado ÍNTEGRO por dentro (el cronómetro vive en onMessage, listener.go:494-496).
//
// Entrar por `onMessage` mediría un trozo MÁS CORTO que el camino real y publicaría como p99 del handler
// algo que no es el handler.
//
// 🔴 (b) serve() PUBLICA Y SUELTA SU LISTENER. serve() no es ejercitable (exige device pareado y socket
// vivo contra WhatsApp), así que el par `setLiveListener(listener)` / `defer setLiveListener(nil)` no lo
// mira nadie: borrar el primero deja el inyector muerto para siempre con todos los gates en verde, y
// borrar el segundo deja el puntero apuntando a una sesión difunta.
//
// Molde y disciplina de listen_gateway_cableado_test.go (T3.16): fallar RUIDOSAMENTE cuando el parseo deja
// de encontrar lo que buscaba, en vez de dejar de mirar en silencio.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - cambiar `l.handleEvent(ctx, evt)` por `l.onMessage(ctx, evt)` en InyectarEntrante;
//   - borrar `g.setLiveListener(listener)` de serve() (o su `defer … (nil)`).
func TestInyectarEntrante_EntraPorHandleEvent_YServePublicaSuListener(t *testing.T) {
	t.Run("la puerta del inyector es handleEvent", func(t *testing.T) {
		const (
			fichero = "inyector.go"
			metodo  = "InyectarEntrante"
			puerta  = "handleEvent"
			atajo   = "onMessage"
		)
		cuerpo := cuerpoDelMetodoDeFichero(t, fichero, metodo)

		var porLaPuerta, porElAtajo int
		ast.Inspect(cuerpo, func(n ast.Node) bool {
			llamada, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch nombreDeLoLlamado(llamada.Fun) {
			case puerta:
				porLaPuerta++
			case atajo:
				porElAtajo++
			}
			return true
		})

		if porElAtajo > 0 {
			t.Errorf("%s() llama a %s().\n"+
				"    CONSECUENCIA: se mediría un tramo MÁS CORTO que el camino real — se saltarían el recover\n"+
				"    del handler (listener.go:408-416) y el switch de tipo (listener.go:417), que es por donde\n"+
				"    entra un entrante de verdad. Publicar eso como el p99 del handler es exactamente el error\n"+
				"    que MP-10 existe para dejar de cometer.", metodo, atajo)
		}
		if porLaPuerta == 0 {
			t.Errorf("%s() ya no llama a %s(): el inyectado no entra por el punto que Register cablea en "+
				"AddEventHandlerWithSuccessStatus (listener.go:369-371). Si la puerta se movió, hay que mover "+
				"esta custodia con ella, no borrarla", metodo, puerta)
		}
	})

	t.Run("serve publica y suelta su Listener", func(t *testing.T) {
		const (
			fichero = "listen_gateway.go"
			metodo  = "serve"
			setter  = "setLiveListener"
		)
		cuerpo := cuerpoDelMetodoDeFichero(t, fichero, metodo)

		llamadas, diferidas := 0, 0
		ast.Inspect(cuerpo, func(n ast.Node) bool {
			switch nodo := n.(type) {
			case *ast.DeferStmt:
				if nombreDeLoLlamado(nodo.Call.Fun) == setter {
					diferidas++
				}
			case *ast.CallExpr:
				if nombreDeLoLlamado(nodo.Fun) == setter {
					llamadas++
				}
			}
			return true
		})

		// El `defer` también es un CallExpr, así que `llamadas` cuenta las dos: la publicación y la limpieza.
		if llamadas < 2 {
			t.Errorf("%s() invoca %s() %d vez/veces y se esperaban 2 (publicar + limpiar).\n"+
				"    CONSECUENCIA de perder la PUBLICACIÓN: el inyector queda muerto para siempre —siempre\n"+
				"    «no hay sesión escuchando»— con los cuatro gates en VERDE, y MP-10 vuelve a no tener\n"+
				"    instrumento.\n"+
				"    CONSECUENCIA de perder la LIMPIEZA: el gateway sobrevive a serve(), así que el puntero\n"+
				"    quedaría apuntando a un Listener sin socket detrás y lo que se midiera ahí no sería el\n"+
				"    handler que en campo se ejecuta.", metodo, setter, llamadas)
		}
		if diferidas != 1 {
			t.Errorf("%s() tiene %d `defer %s(nil)` y se esperaba exactamente 1: la limpieza tiene que correr "+
				"en TODAS las salidas de serve() (cancelación del ctx, error de Connect, LoggedOut), no solo "+
				"en la buena", metodo, diferidas, setter)
		}
	})
}

// cuerpoDelMetodoDeFichero parsea un fichero del paquete y devuelve el cuerpo del método pedido, fallando
// ruidosamente si algo se renombró. Envuelve a cuerpoDelMetodo (listen_gateway_cableado_test.go) para no
// repetir el parseo en cada subtest.
func cuerpoDelMetodoDeFichero(t *testing.T, fichero, metodo string) *ast.BlockStmt {
	t.Helper()
	fset := token.NewFileSet()
	arbol, err := parser.ParseFile(fset, fichero, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("no se pudo parsear %s: %v (si el fichero se renombró, actualiza este test: es el que "+
			"vigila por dónde entra el inyectado)", fichero, err)
	}
	cuerpo := cuerpoDelMetodo(arbol, metodo)
	if cuerpo == nil {
		t.Fatalf("%s ya no tiene %s(): ¿se renombró? Este test mira ahí a propósito", fichero, metodo)
	}
	return cuerpo
}
