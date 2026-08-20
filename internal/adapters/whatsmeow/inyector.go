package whatsmeow

// inyector.go — EL INYECTOR DE ENTRANTES SINTÉTICOS (MP-10 Parte A).
//
// 🔴 POR QUÉ EXISTE. INV-051.2 exige que el handler de entrantes salga en menos de 50 ms p99, y esa
// afirmación lleva meses sin poder comprobarse en campo: el único modo de llenar el histograma era mandar
// mensajes REALES por WhatsApp, y cien mensajes seguidos contra un número de producción es exactamente
// como se consigue que WhatsApp lo bloquee. El p99 del VPS sale con `n = 0` por eso, no por un fallo del
// cronómetro. Este fichero fabrica el entrante y lo mete por el MISMO punto por el que entra uno de
// verdad, de modo que lo que se mida sea el camino real y no un trozo suyo.
//
// 🔴 SUPERFICIE DE PRODUCCIÓN EN UN DAEMON 24/7, y se trata como tal. Todo lo que se inyecta queda
// MARCADO, y la marca viaja en dos planos distintos a propósito:
//
//   - PORTANTE (aguas abajo, hasta la nube): el prefijo `PrefijoSintetico` en el `WAMessageID`. El
//     `wa_message_id` es columna de la cola, lo copia el despachador al `domain.InboundEvent` y acaba en
//     el proto de CloudLink, así que un mensaje sintético es distinguible de uno real EN LA NUBE, sin
//     que nadie tenga que abrir la fila cifrada del Edge. Es la marca que sostiene el guardarraíl.
//   - LOCAL (solo en el Edge): el campo `sintetico` del meta de la fila (ver `colaMetaPayload` en
//     listener.go). Viaja CIFRADO con la DEK de la sesión y no sale de la máquina; sirve para auditar la
//     cola sin re-derivar el prefijo a mano.
//
// La marca portante es la que NO se puede quitar sin romper el guardarraíl. La local es comodidad.
//
// 🔴 QUÉ NO HACE. No genera carga sola (no hay ticker, no hay goroutine, no hay ráfaga programada aquí):
// una llamada, un mensaje. Quien decide cuántos y a qué ritmo es el plano de control, río arriba, detrás
// de la palanca `InyectorEntrantes` (apagada por defecto, internal/infra/config). Este fichero no consulta
// esa palanca: no es su capa, y duplicar el interruptor aquí sería dos guardianes que producen el mismo
// síntoma y que ningún test puede custodiar por separado.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// PrefijoSintetico es LA MARCA PORTANTE del guardarraíl «los sintéticos deben ser distinguibles de los
// reales aguas abajo»: se antepone al `Info.ID` del evento fabricado, que es el `wa_message_id` de la fila
// de la cola, el `MessageID` del `domain.InboundEvent` que reconstruye el despachador y, al final, el
// campo del proto de CloudLink que sube a la nube.
//
// 🔴 ES LA ÚNICA MARCA QUE SALE DEL EDGE. La otra —`sintetico` en el meta de la fila— viaja cifrada con la
// DEK de la sesión y muere aquí: la nube no la ve nunca. Por eso quitar este prefijo NO es un cambio
// cosmético: deja a la plataforma sin forma de separar el tráfico de prueba del real, y el primer sitio
// donde eso se nota es un informe de negocio con pedidos que nadie hizo.
//
// El formato imita el de `cmd/colaseed` (`COLASEED-<lote>-C%04d-M%04d`) por la misma razón por la que
// aquél lo eligió: es reconocible de un vistazo y BORRABLE con un solo `LIKE` si una corrida se abandona.
const PrefijoSintetico = "SINTETICO-"

// ErrSinEscuchaViva es el centinela de «este gateway NO tiene Listener publicado». Existe porque sin él el
// estado que nombra sale de aquí como un `fmt.Errorf` plano, el borde del plano de control no lo reconoce y
// la tanda entera contesta 200 con `inyectados: 0, errores: 500` — o sea, «he medido» a quien no midió nada,
// que es EXACTAMENTE el modo de fallo que MP-10 existe para eliminar (`colaseed` medía otra cosa y devolvía
// n = 0 en silencio).
//
// 🔴 QUÉ ESTADO NOMBRA, Y CUÁL NO. NO dice «el daemon no corre» ni «la sesión no existe»: cuando este error
// sale, el daemon 24/7 está en pie y la sesión está en el registro vivo. Dice que su ciclo de escucha no
// tiene AHORA MISMO un Listener publicado, y en campo eso ocurre en dos ventanas concretas:
//
//   - ENTRE INTENTOS: el factory del sessionmgr publica el cable del inyector (listen.go:169) ANTES de que
//     `serve()` corra, y `serve()` solo publica el Listener DESPUÉS de `Register` (listen_gateway.go:323).
//     El hueco entre las dos es corto pero real.
//   - EN BACKOFF DE RECONEXIÓN: `serve()` ya salió y su `defer` limpió el puntero, pero el cable del ciclo
//     anterior sigue publicado porque nadie lo pone a nil. `runListener` espera su backoff exponencial
//     acotado (listen.go:298-328, `backoffMax` = 60 s por defecto) antes de reconstruir el gateway. Esta es
//     la ventana LARGA, y es la que de verdad se pisa cuando se mide contra un Edge que acaba de perder red.
//
// Por eso es TRANSITORIO y REINTENTABLE, y el mensaje lo dice: quien lo reciba tiene que ESPERAR unos
// segundos y volver a lanzar la tanda, no ir a comprobar si el daemon está levantado.
var ErrSinEscuchaViva = errors.New("whatsapp: la sesión no tiene Listener publicado ahora mismo " +
	"(el daemon corre y la sesión existe: el ciclo de escucha está entre Register y su publicación, o " +
	"cayó y espera el backoff de reconexión —hasta 60 s—); es TRANSITORIO y REINTENTABLE: espera unos " +
	"segundos y repite la tanda")

const (
	// usuarioChatSintetico es la user-part del JID de chat que se usa cuando el llamante no da ninguno.
	// Es CONSTANTE (no lleva lote ni índice) a propósito: el JID sintético por defecto tiene que ser
	// UNO, reconocible a simple vista en la cola y en un log, y con una forma que ningún número real
	// puede tener —un número de WhatsApp es E.164, o sea solo dígitos, así que una user-part con letras
	// no colisiona jamás con un chat de verdad—.
	//
	// ⚠️ CONSECUENCIA QUE EL LLAMANTE DEBE CONOCER: sin `ChatJID` TODOS los inyectados caen en la MISMA
	// conversación, y la cola ordena y despacha por conversación. Para medir el reparto entre chats hay
	// que pasar un `ChatJID` distinto por conversación (que es lo que hace `colaseed` con su `-NNNN`).
	//
	// Precedente de que un JID con letras no rompe nada aguas abajo: `cmd/colaseed` lleva desde la Ola 1
	// sembrando `colaseed-<lote>-0001@s.whatsapp.net` y nadie parsea el `chat_jid` (el despachador lo
	// copia tal cual a `domain.InboundEvent.Chat`).
	usuarioChatSintetico = "sintetico"

	// tipoMensajeSintetico es el `Info.Type` del evento fabricado. whatsmeow rellena ese campo con
	// "text" para un mensaje de texto (es el valor que usan los tests de este paquete y el que el
	// despachador espera encontrar en el meta), y el sintético tiene que parecerse a lo que imita.
	tipoMensajeSintetico = "text"

	// pushNameSintetico es el nombre visible del remitente fabricado. NO es un nombre plausible a
	// propósito: si una fila sintética acabara donde no debe, quien la mire tiene que verlo sin
	// necesidad de decodificar el `wa_message_id`.
	pushNameSintetico = "SINTETICO (inyector MP-10)"
)

// FabricarEntranteSintetico construye el `*events.Message` que el inyector entrega al handler REAL.
//
// 🔴 CADA CAMPO ESTÁ ELEGIDO PARA ESQUIVAR UNA GUARDA CONCRETA DE `onMessage`, y por eso ninguno es
// arbitrario. Las cuatro que importan, con su línea:
//
//	listener.go:511 · `if e.Info.IsFromMe` ⇒ ECO PROPIO: se descarta en la puerta, sin fila y sin llegar
//	                  siquiera al carril rápido. Por eso IsFromMe = false.
//	listener.go:525 · `if e.Info.Timestamp.IsZero()` ⇒ el CERO no descarta, pero toma una rama DISTINTA
//	                  que SALTA la guarda de ventana y sella la fila con la hora local. Es un camino
//	                  legítimo del listener y NO es el que este MP quiere medir: mediría un handler más
//	                  corto que el de un mensaje normal. Por eso el sello es `time.Now()`, no cero.
//	listener.go:553 · `if e.Info.Timestamp.Before(threshold)` ⇒ VENTANA TEMPORAL (ADR-0037). El umbral es
//	                  `sello_de_conexión − margen`, y `resolveThreshold` (inbound_window.go:139-144) cae a
//	                  `now` cuando el sello es cero o futuro. Con `Timestamp = time.Now()` el sintético
//	                  está SIEMPRE por encima del umbral, haya sello o no: `now − margen <= now`. Un sello
//	                  antiguo lo dejaría pasar igual. Por eso NO se acepta una hora del llamante.
//	                  ⚠️ ESA DESIGUALDAD SE APOYA EN `margin > 0`, Y ESA GARANTÍA ES PRESTADA, NO PROPIA.
//	                  El `now` con el que `resolveThreshold` respalda un sello cero se toma DESPUÉS de que
//	                  este fichero fabrique el `Timestamp`, así que con `margin == 0` el umbral quedaría
//	                  por delante del sello del sintético y `Before` lo mandaría a `Descartado` — el
//	                  inyectado no llegaría a la cola y la medición saldría con población cero, sin un
//	                  solo error. Hoy no puede pasar, y quien lo impide son DOS terceros: el default
//	                  `defaultConnectMargin = 5 * time.Minute` (inbound_window.go:85) y el guardarraíl de
//	                  `config.Load`, que devuelve al default cualquier `InboundMarginSeconds <= 0`
//	                  (internal/infra/config/config.go:787-788) — más `SetInboundMargin`, que el factory
//	                  solo invoca `if m.inboundMargin > 0` (sessionmgr/listen.go:120-122). Si algún día se
//	                  admite un margen cero, este fichero deja de ser correcto: hay que cambiar aquí el
//	                  sello, no allí el guardarraíl.
//	listener.go:716 · `case e.Info.IsGroup` ⇒ un grupo no descarta, pero hace nacer la fila ya
//	                  `clasificado` con la marca `no_elegible`: el cajero no la reclama nunca. Por eso
//	                  IsGroup = false.
//
// Y la quinta, que es la validación de entrada: `listener.go:713` (`case text == ""`) hace nacer la fila
// `clasificado` con la marca `sin_texto`. Un inyectado sin texto recorrería el handler pero NO el camino
// que se quiere medir, así que se rechaza aquí, ruidosamente, en vez de producir una medida que miente.
//
// Devuelve error —y no un evento degradado— en los dos casos en que el sintético no serviría para medir:
// sin texto y sin lote.
func FabricarEntranteSintetico(p app.InyeccionEntrante) (*events.Message, error) {
	texto := p.Texto
	if strings.TrimSpace(texto) == "" {
		return nil, fmt.Errorf("inyector: el entrante sintético necesita texto; sin él la fila nace `clasificado` " +
			"con la marca `sin_texto` (listener.go:713) y NO recorre el camino que MP-10 mide")
	}
	// 🔴 EL LOTE ES OBLIGATORIO, y no es burocracia: el `wa_message_id` sale de él. La cola tiene índice
	// único (session_id, wa_message_id) y el store trata el choque como DUPLICADO devolviendo nil
	// (listener.go:618), o sea SIN error visible. Con el lote vacío, dos corridas seguidas producen los
	// mismos IDs y la segunda se evapora en silencio: el histograma saldría corto y nadie sabría por qué —
	// exactamente el modo de fallo que este MP existe para eliminar. Mismo criterio que `cmd/colaseed`,
	// que también rechaza el lote vacío.
	lote := strings.TrimSpace(p.Lote)
	if lote == "" {
		return nil, fmt.Errorf("inyector: el entrante sintético necesita un lote; sin él dos corridas repiten el " +
			"wa_message_id y la segunda la absorbe el índice único (session_id, wa_message_id) SIN error")
	}

	chat, err := jidSintetico(p.ChatJID)
	if err != nil {
		return nil, err
	}

	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat: chat,
				// El remitente ES el chat: en un 1:1 de WhatsApp coinciden, y así lo siembra también
				// `colaseed` («En un chat 1:1 el remitente ES el chat»).
				Sender: chat,
				// listener.go:511 — un true aquí descartaría el sintético en la puerta.
				IsFromMe: false,
				// listener.go:716 — un true aquí haría nacer la fila `clasificado`/`no_elegible`.
				IsGroup: false,
				// AddressingMode "pn": el sintético imita un chat por NÚMERO, que es el 100 % del tráfico
				// que hoy mide INV-051.2. Se copia al meta de la fila tal cual (listener.go:784).
				AddressingMode: types.AddressingModePN,
			},
			ID:       idSintetico(lote, p.Indice),
			PushName: pushNameSintetico,
			// listener.go:525 y :553 — ver el bloque de arriba. `time.Now()` es el ÚNICO valor que pasa
			// las dos guardas a la vez, y por eso no se acepta una hora del llamante.
			Timestamp: time.Now(),
			Type:      tipoMensajeSintetico,
		},
		// `messageText` (listener.go:932-940) lee primero `GetConversation()`; el `ExtendedTextMessage` es
		// la rama del mensaje con contexto/enlace, que no es lo que se quiere imitar.
		Message: &waE2E.Message{Conversation: proto.String(texto)},
	}, nil
}

// idSintetico compone el `wa_message_id` del inyectado: prefijo portante + lote + índice a cuatro cifras.
// El `%04d` se copia de `cmd/colaseed` (`COLASEED-<lote>-C%04d-M%04d`) para que los IDs de una corrida
// ordenen lexicográficamente igual que numéricamente hasta 9999.
func idSintetico(lote string, indice int) string {
	return fmt.Sprintf("%s%s-%04d", PrefijoSintetico, lote, indice)
}

// jidSintetico resuelve el JID de chat del inyectado: el que pida el llamante o, si no pide ninguno, el
// sintético por defecto.
//
// ⚠️ NO REUSA `parseRecipient` (sender.go:259), y la razón es concreta: aquél LIMPIA los guiones del
// destino (`strings.ReplaceAll(cleaned, "-", "")`) porque su entrada es un teléfono tecleado por un
// humano. Un JID sintético lleva guiones por diseño (`colaseed-<lote>-0001@…`), así que pasarlo por ahí lo
// mutilaría en silencio y las filas dejarían de casar con el `LIKE` que las borra.
func jidSintetico(chatJID string) (types.JID, error) {
	limpio := strings.TrimSpace(chatJID)
	if limpio == "" {
		// Se construye a mano en vez de parsear: es un valor constante nuestro, no una entrada, y así el
		// camino por defecto no tiene ninguna rama de error que pueda fallar en campo.
		return types.JID{User: usuarioChatSintetico, Server: waServer}, nil
	}
	if !strings.Contains(limpio, "@") {
		limpio = limpio + "@" + waServer
	}
	jid, err := types.ParseJID(limpio)
	if err != nil {
		// El JID sintético NO es PII (lo fabrica quien inyecta), así que aquí sí se puede citar: es la
		// única forma de diagnosticar una llamada mal formada desde el plano de control.
		return types.JID{}, fmt.Errorf("inyector: chat_jid sintético inválido (%q): %w", limpio, err)
	}
	return jid, nil
}

// InyectarEntrante mete un entrante SINTÉTICO por el camino REAL del handler de entrantes de la sesión
// que está escuchando ahora mismo.
//
// 🔴 ENTRA POR `handleEvent`, NO POR `onMessage`, Y ESA ES LA DECISIÓN ENTERA DE ESTE MÉTODO.
// `handleEvent` es literalmente lo que `Register` cablea en `AddEventHandlerWithSuccessStatus`
// (listener.go:369-371): es el punto por el que entra un entrante de VERDAD. Incluye el `recover` que
// sostiene la garantía del acuse (listener.go:408-416) y el `switch` de tipo (listener.go:417), y deja el
// tramo cronometrado ÍNTEGRO por dentro —el cronómetro de T3.13 vive dentro de `onMessage`
// (listener.go:494-496)—. Entrar por `onMessage` mediría un trozo MÁS CORTO que el camino real, y publicar
// como p99 del handler algo que no es el handler es justo el error que MP-10 existe para dejar de cometer.
//
// 🔴 EL `bool` QUE DEVUELVE ES EL PERMISO DE ACUSE A WHATSAPP (T1.13), no «se encoló». Aquí NO viaja a la
// red por una razón simple: quien lo convierte en un acuse es whatsmeow, y whatsmeow solo mira el valor
// que devuelve el handler que ÉL invocó. Esta llamada no viene de whatsmeow, así que el bool muere en el
// llamante y vale únicamente como SEÑAL DE DIAGNÓSTICO —un `false` dice que la fila no llegó a disco—.
// Un sintético jamás provoca un reenvío de WhatsApp, y tampoco lo provocaría un `false`.
//
// 🔴 MIENTRAS EL INYECTOR CORRE, `last_inbound` NO PRUEBA NADA DEL SOCKET DE WHATSAPP. El handler sella la
// prueba de vida (`l.reporter.MarkInbound`, listener.go:506) ANTES de filtrar y para TODO mensaje, y un
// sintético entra por ahí igual que uno real — que es justo el punto del diseño, no un descuido: si el
// sintético esquivara ese sello ya no recorrería el camino real. La consecuencia hay que conocerla: durante
// una tanda, `GET /v1/sessions` reporta la sesión como RECIBIENDO TRÁFICO aunque el socket esté sordo, así
// que la señal de salud queda contaminada por el instrumento de medida y no sirve para diagnosticar la
// sesión hasta que la tanda termina. Es el precio aceptado de entrar por el camino real.
//
// Devuelve error si no hay sesión escuchando (envuelto sobre ErrSinEscuchaViva: transitorio y reintentable,
// ver su doc) o si el evento no se pudo fabricar. Nunca entra en pánico: el `recover` de `handleEvent` cubre
// el camino de abajo.
//
// VIVE AQUÍ Y NO EN listen_gateway.go a propósito: todo lo que MP-10 añade a la superficie de producción
// —la marca portante, el fabricante y la puerta— se lee, se audita y, si un día sobra, se BORRA como una
// sola unidad. Lo único que este frente deja fuera de este fichero es lo que no puede estar dentro: el
// campo del struct y su pareja de setter/getter, que van donde están sus hermanos.
func (g *ListenGateway) InyectarEntrante(ctx context.Context, p app.InyeccionEntrante) (acusar bool, err error) {
	l := g.liveListener()
	if l == nil {
		// Envuelto sobre el CENTINELA, no un fmt.Errorf plano: el borde del plano de control tiene que poder
		// distinguir este estado con errors.Is para abortar la tanda con un 409 en la PRIMERA inyección. Con
		// un error plano el 409 es literalmente inalcanzable y su hueco cae en un 200 mentiroso.
		return false, fmt.Errorf("%w: no hay Listener al que entregar el sintético", ErrSinEscuchaViva)
	}
	evt, err := FabricarEntranteSintetico(p)
	if err != nil {
		return false, err
	}
	return l.handleEvent(ctx, evt), nil
}
