package whatsmeow

// listener_acuse_test.go — EL PERMISO DE ACUSE (Plan 051 · T1.13).
//
// 🔴 EL DEFECTO QUE ESTE FICHERO CIERRA, y por qué era el peor que quedaba abierto. whatsmeow acusa el
// mensaje a WhatsApp DESPUÉS de correr los handlers (message.go:431 los invoca, :467 acusa) y se salta el
// acuse si alguno devuelve false (message.go:437). El listener se registraba con `client.AddEventHandler`,
// cuyo envoltorio devuelve true SIEMPRE (client.go:763-768), y además el retorno de `enqueueCola` no se
// consultaba. Con las dos cosas juntas, un INSERT fallido se acusaba igual: WhatsApp daba el mensaje por
// ENTREGADO, no lo reenviaba nunca y desaparecía EN SILENCIO — en un plan cuya promesa entera es «ni un
// mensaje perdido», y con los cuatro gates en verde.
//
// 🔴 LO QUE SE CUSTODIA AQUÍ NO ES «QUE DEVUELVA FALSE»: es LA FRONTERA. El valor de retorno tiene un
// precio caro en las DOS direcciones y por eso cada camino necesita su propio test:
//
//   - un true de más ⇒ el mensaje se pierde para siempre (el defecto original);
//   - un false de más ⇒ le pedimos a WhatsApp que reenvíe algo que volveremos a descartar por el mismo
//     motivo, y como lo reenviado llega IDÉNTICO, eso es un bucle que se repite en cada reconexión.
//
// Por eso los descartes deliberados (eco propio, fuera de ventana) tienen aquí tanta custodia como el
// fallo de escritura: convertir uno de ellos en `false` sería cambiar una pérdida silenciosa por una
// tormenta de reenvíos, que no es mejor.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo, y todas se ejecutaron.

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/latencia"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// --- LO QUE NO DEJA FILA POR UN FALLO NUESTRO: NO SE ACUSA ---

// TestOnMessage_ElINSERTQueFalla_NoSeAcusa es EL test de T1.13: el disco lleno, la DEK ausente, la BD
// bloqueada. El mensaje no está en ningún sitio, así que no se puede dar por recibido.
//
// 🔴 SE PRUEBAN LOS DOS CAMINOS QUE ADMITEN, y no por completismo: onMessage propaga el veredicto del
// INSERT desde DOS `return` distintos —el normal y el del entrante SIN HORA UTILIZABLE (t="0")—, así que
// son dos sitios donde alguien puede tirar el valor por la borda. Con un solo caso, cambiar el del
// entrante sin hora por `l.enqueueCola(...); return true` dejaba este fichero ENTERO en verde (mutación
// M12, comprobada: se quedó verde hasta que se añadió el segundo caso). El camino sin hora es además el
// que menos se mira —lo dispara una condición rarísima— y el que peor se diagnostica en campo.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - `return false` → `return true` en la rama del error de Enqueue (listener.go) ⇒ vuelve el defecto
//     entero: se acusa lo que no se escribió.
//   - dejar de propagar el retorno en CUALQUIERA de los dos `return l.enqueueCola(...)` de onMessage.
func TestOnMessage_ElINSERTQueFalla_NoSeAcusa(t *testing.T) {
	sinHora := liveMessage("MSG-ERR-SINHORA", "quiero dos empanadas")
	sinHora.Info.Timestamp = time.Time{}

	casos := []struct {
		nombre  string
		mensaje *events.Message
	}{
		{"entrante normal", liveMessage("MSG-ERR", "quiero dos empanadas")},
		{"entrante SIN hora utilizable (t=0)", sinHora},
	}

	for _, cs := range casos {
		t.Run(cs.nombre, func(t *testing.T) {
			cola := &spyCola{calls: &callLog{}, err: errors.New("disco lleno")}
			l := listenerConCola(cola)

			if l.handleEvent(context.Background(), cs.mensaje) {
				t.Error("el handler autorizó el ACUSE de un entrante que NO llegó a la cola.\n" +
					"    CONSECUENCIA: whatsmeow acusa a WhatsApp, WhatsApp da el mensaje por ENTREGADO y NO LO\n" +
					"    REENVÍA JAMÁS. El mensaje del cliente se pierde para siempre, sin error visible para él\n" +
					"    (ve el doble check) y sin nada roto para nosotros: los cuatro gates seguirían en verde.\n" +
					"    Es exactamente el defecto que T1.13 vino a cerrar.")
			}
			// El contador de T1.10 no se toca: sigue midiendo lo mismo, aunque lo que significa haya cambiado
			// (de «mensajes perdidos» a «mensajes que WhatsApp tendrá que reofrecer»).
			if s := l.InboundStats(); s.ColaEnqueueErrors != 1 || s.ColaEnqueuePanics != 0 {
				t.Errorf("contadores = errores:%d panics:%d, quería 1 y 0: el acuse es una decisión NUEVA, no un "+
					"reemplazo de la telemetría de INV-051.3", s.ColaEnqueueErrors, s.ColaEnqueuePanics)
			}
		})
	}
}

// TestOnMessage_ElFalloRepetido_TampocoSeAcusa cubre la OTRA salida del mismo error, la del throttle de
// log (app.ErrColaFalloRepetido): una sesión con la DEK ausente falla en cada mensaje y el adaptador marca
// lo que ya gritó para que aquí solo se baje el nivel del log.
//
// Tiene test propio porque es un `return` DISTINTO: bajar el nivel del log no puede cambiar la decisión
// del acuse, y un fallo repetido es justo el que más mensajes se llevaría por delante.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): `return false` → `return true` en la rama de
// ErrColaFalloRepetido (la del log en Debug) ⇒ el test de arriba sigue VERDE y este cae.
func TestOnMessage_ElFalloRepetido_TampocoSeAcusa(t *testing.T) {
	cola := &spyCola{calls: &callLog{}, err: fmt.Errorf("cola: %w", app.ErrColaFalloRepetido)}
	l := listenerConCola(cola)

	if l.handleEvent(context.Background(), liveMessage("MSG-REPE", "quiero dos empanadas")) {
		t.Error("se acusó un entrante rechazado por un fallo YA REPORTADO de la cola.\n" +
			"    CONSECUENCIA: es el caso de la sesión con la DEK ausente, que falla en TODOS sus mensajes.\n" +
			"    Acusarlos los pierde todos y en silencio, que es la razón por la que el fallo se marca como\n" +
			"    repetido: para bajar el ruido del LOG, nunca para relajar la garantía de durabilidad.")
	}
}

// TestOnMessage_ElPanicAlEncolar_NoSeAcusa fija la decisión sobre el pánico recuperado (driver muerto,
// crypterFor ajeno): cuenta como «no dejó fila». Un pánico a mitad del camino de escritura no deja ninguna
// prueba de que la fila llegara al disco, y ante la duda la asimetría del plan es clara — reenviar de más
// es barato (el índice único (session_id, wa_message_id) absorbe el duplicado), acusar de más es perder.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): asignar `dejoFila = true` dentro del recover de
// enqueueCola ⇒ el pánico pasaría a acusar. (Nótese que NO hay una asignación a `false` que borrar: el
// valor cero del resultado ES la respuesta, precisamente para no tener dos defensas diciendo lo mismo.)
func TestOnMessage_ElPanicAlEncolar_NoSeAcusa(t *testing.T) {
	cola := &spyCola{calls: &callLog{}, panicMsg: "driver muerto"}
	l := listenerConCola(cola)

	if l.handleEvent(context.Background(), liveMessage("MSG-PANIC", "quiero dos empanadas")) {
		t.Error("se acusó un entrante cuyo encolado entró en PÁNICO.\n" +
			"    CONSECUENCIA: el pánico se recupera para no tumbar la escucha (REQ-051.8), pero eso no dice\n" +
			"    nada sobre si la fila llegó al disco. Acusar aquí es dar por entregado un mensaje del que no\n" +
			"    tenemos ni una prueba de escritura.")
	}
	if s := l.InboundStats(); s.ColaEnqueuePanics != 1 {
		t.Errorf("ColaEnqueuePanics = %d, quería 1: el pánico se sigue contando aparte del error (INV-051.3)",
			s.ColaEnqueuePanics)
	}
}

// TestOnMessage_SinColaCableada_NoSeAcusa es la decisión más discutible del encargo, y por eso se fija con
// un test y con su razón escrita. Aquí no ha fallado ninguna escritura: es que no hay dónde escribir (un
// cableado hecho por fuera de `agent serve`; el daemon no arranca sin cola desde el Plan 051 O3).
//
// SE ELIGE NO ACUSAR. El precio conocido es que, mientras dure el cableado roto, WhatsApp reofrezca los
// mismos mensajes en cada reconexión. Se prefiere por dos razones: los mensajes SOBREVIVEN —en cuanto el
// proceso arranque bien cableado entran todos—, y el fallo se vuelve VISIBLE donde nadie puede ignorarlo:
// quien escribe al negocio ve su mensaje SIN confirmar, en vez de verlo confirmado y sin respuesta, que es
// el síntoma imposible de diagnosticar («el cliente escribe y no pasa nada»).
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): `return false` → `return true` en la guarda
// `if l.cola == nil` de enqueueCola.
func TestOnMessage_SinColaCableada_NoSeAcusa(t *testing.T) {
	l := NewListener(quietLogger()) // sin WithCola: sesión SORDA

	if l.handleEvent(context.Background(), liveMessage("MSG-NILCOLA", "quiero dos empanadas")) {
		t.Error("una sesión SIN cola cableada autorizó el acuse.\n" +
			"    CONSECUENCIA: esa sesión no anota, no entrega y no falla; acusar convierte eso en una\n" +
			"    pérdida TOTAL y silenciosa de todo lo que le escriban. No acusando, los mensajes se quedan\n" +
			"    en WhatsApp hasta que el proceso arranque bien cableado, y el remitente ve que su mensaje\n" +
			"    no está confirmado — el único síntoma que apunta a la causa.")
	}
}

// --- LO QUE NO DEJA FILA POR DECISIÓN NUESTRA: SÍ SE ACUSA ---

// TestOnMessage_LosDescartesDeliberados_SIAcusan es la mitad que impide que este arreglo se convierta en
// una tormenta de reenvíos. Son los dos caminos que salen SIN fila y con razón, y en los dos el reenvío
// sería un bucle: lo reenviado llega idéntico (mismo IsFromMe, mismo Info.Timestamp, que es el reloj del
// SERVIDOR del mensaje original) y se volvería a descartar por el mismo criterio.
//
// La ráfaga del ADR-0037 son MILES de eventos. Negarles el acuse no rescata ni uno: los reofrece todos en
// cada reconexión, para siempre.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas, una por caso): `return true` → `return false` en la
// rama de IsFromMe y en la del descarte por ventana de onMessage.
func TestOnMessage_LosDescartesDeliberados_SIAcusan(t *testing.T) {
	seal := time.Now()

	casos := []struct {
		nombre  string
		mensaje *events.Message
		porque  string
	}{
		{
			nombre:  "eco propio (IsFromMe)",
			mensaje: ecoPropio("MSG-ECO"),
			porque: "es un mensaje que mandamos NOSOTROS. No hay nada que persistir, y no acusarlo sería " +
				"pedirle a WhatsApp que nos devuelva nuestro propio eco una y otra vez",
		},
		{
			nombre:  "fuera de la ventana temporal (ADR-0037)",
			mensaje: msgAt("MSG-VIEJO", seal.Add(-6*time.Hour)),
			porque: "el descarte es determinista sobre Info.Timestamp, que es inmutable: el reenvío llegaría " +
				"con el mismo sello y volvería a caer fuera. Sería un bucle a ritmo de reconexión, y sobre " +
				"la ráfaga entera del ADR-0037",
		},
	}

	for _, cs := range casos {
		t.Run(cs.nombre, func(t *testing.T) {
			cola := &spyCola{calls: &callLog{}}
			l := listenerSealed(cola, seal)

			if !l.handleEvent(context.Background(), cs.mensaje) {
				t.Errorf("este camino NEGÓ el acuse, y debe concederlo: %s.\n"+
					"    CONSECUENCIA: WhatsApp reenvía lo que hemos decidido no procesar, nosotros lo volvemos\n"+
					"    a descartar y volvemos a negar el acuse. El Edge se queda masticando el mismo tráfico\n"+
					"    en cada reconexión, y el socket que debía atender lo vivo se lo gasta en lo muerto.", cs.porque)
			}
			// Testigo de que el escenario es el que el test cree: estos caminos no dejan fila.
			if len(cola.got) != 0 {
				t.Fatalf("filas anotadas = %v; este caso debía salir SIN fila (si ahora encola, el test ya "+
					"no está midiendo un descarte)", colaWAIDs(cola.got))
			}
		})
	}
}

// TestOnMessage_LoQueDejaFila_SIEMPREAcusa recorre TODOS los caminos que terminan con el mensaje en disco
// y exige el acuse en los seis.
//
// ⚠️ LOS CUATRO MOTIVOS DE OMISIÓN NO SON DESCARTES, y es el malentendido más fácil de este código: sin
// texto, grupo, clasificador apagado y fastlane DEJAN FILA (nacen en EstadoClasificado con su marca). La
// puerta de elegibilidad no decide si el mensaje entra, decide si el CAJERO lo reclama. Acusan porque el
// mensaje está en disco — no por indulgencia.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): `return true` → `return false` al final de enqueueCola ⇒
// caen los seis casos a la vez.
func TestOnMessage_LoQueDejaFila_SIEMPREAcusa(t *testing.T) {
	sinTexto := liveMessage("MSG-IMG", "")
	sinTexto.Info.Type = "image"
	sinHora := liveMessage("MSG-SINHORA", "quiero dos empanadas")
	sinHora.Info.Timestamp = time.Time{}

	casos := []struct {
		nombre  string
		mensaje *events.Message
		opts    []ListenerOption
	}{
		{"texto directo (nace nuevo)", liveMessage("MSG-OK", "quiero dos empanadas"), nil},
		{"sin texto (imagen, audio…)", sinTexto, nil},
		{"de un grupo (no elegible)", grupoMessage("MSG-GRUPO", "quiero dos empanadas"), nil},
		{"clasificador apagado", liveMessage("MSG-OFF", "quiero dos empanadas"), []ListenerOption{WithClasificadorActivo(apagado)}},
		{"carril rápido (fastlane)", liveMessage("MSG-FAST", "2"), nil},
		{"sin hora utilizable (t=0)", sinHora, nil},
	}

	for _, cs := range casos {
		t.Run(cs.nombre, func(t *testing.T) {
			cola := &spyCola{calls: &callLog{}}
			l := listenerConCola(cola, cs.opts...)

			acusar := l.handleEvent(context.Background(), cs.mensaje)

			// Primero el testigo: si no hay fila, el caso no es el que el test cree y el acuse no prueba nada.
			if len(cola.got) != 1 {
				t.Fatalf("filas anotadas = %d, se esperaba 1: este caso DEBE dejar fila (los motivos de "+
					"omisión no son descartes), así que el escenario no es el que el test cree", len(cola.got))
			}
			if !acusar {
				t.Error("se NEGÓ el acuse de un mensaje que SÍ quedó escrito en la cola.\n" +
					"    CONSECUENCIA: WhatsApp lo reenvía, el segundo intento choca contra el índice único\n" +
					"    (session_id, wa_message_id) y se absorbe como duplicado… y se vuelve a negar el acuse.\n" +
					"    El mensaje ya está a salvo y aun así se pide su reenvío en cada reconexión, para\n" +
					"    siempre: tráfico y ciclos del socket gastados en algo que ya está hecho.")
			}
		})
	}
}

// TestHandleEvent_LoQueNoEsUnEntrante_SIEMPREAcusa fija que el permiso solo lo puede negar el camino del
// mensaje. No es simetría estética: un false CORTA la cadena de handlers en seco (dispatchEvent retorna en
// el primero que falla, client.go:929), así que negarlo en un *events.Receipt o un *events.Connected
// dejaría ciegos a los demás handlers registrados en el cliente sin ganar nada — esos eventos no llevan
// acuse por esta vía.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): `return true` → `return false` al final de handleEvent.
func TestHandleEvent_LoQueNoEsUnEntrante_SIEMPREAcusa(t *testing.T) {
	casos := []struct {
		nombre string
		evt    any
	}{
		{"Connected", &events.Connected{}},
		{"Disconnected", &events.Disconnected{}},
		{"LoggedOut", &events.LoggedOut{}},
		{"Receipt", &events.Receipt{Type: types.ReceiptTypeDelivered}},
		{"OfflineSyncPreview", &events.OfflineSyncPreview{Messages: 3}},
		{"OfflineSyncCompleted", &events.OfflineSyncCompleted{Count: 3}},
		{"evento fuera del alcance", &events.PushNameSetting{}},
	}

	for _, cs := range casos {
		t.Run(cs.nombre, func(t *testing.T) {
			l := listenerConCola(&spyCola{calls: &callLog{}})
			if !l.handleEvent(context.Background(), cs.evt) {
				t.Error("un evento que no es un entrante negó el acuse.\n" +
					"    CONSECUENCIA: whatsmeow deja de recorrer la cadena de handlers en cuanto uno devuelve\n" +
					"    false, así que este listener dejaría CIEGO a cualquier otro handler del mismo cliente\n" +
					"    para ese evento. Y no compra nada: estos eventos no se acusan por esta vía.")
			}
		})
	}
}

// TestHandleEvent_UnPanicoFueraDelEncolado_NoSeAcusa cierra el agujero HERMANO del de arriba: hasta el
// 2026-08-18 la garantía «no se acusa lo que no se escribió» dependía de que el recover de enqueueCola
// siguiera cubriendo todo el camino de escritura. Un pánico un centímetro más arriba —el reporter de
// salud, la lectura del sello, un hook de sesión— se escapaba del handler, lo recogía whatsmeow, y su
// dispatchEvent devolvía `handlerFailed` en su valor CERO (false, client.go:918-933): SE ACUSABA. El
// defecto entero, por la puerta de atrás, y sin que ningún test de los de arriba se enterase.
//
// 🔴 LA SEGUNDA ASERCIÓN ES LA QUE IMPIDE LA DEFENSA DUPLICADA. Aquí se exige que ColaEnqueuePanics siga
// en CERO: prueba que quien recogió este pánico fue la red de handleEvent y NO la de enqueueCola, es
// decir que las dos tienen responsabilidades distintas y no una copia de la otra. La de abajo SABE dónde
// pasó y por eso cuenta; ésta no sabe nada y solo garantiza. Por eso quitar cualquiera de las dos pone
// algo en rojo, y por separado.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - quitar el `defer recover()` de handleEvent ⇒ el pánico sube y revienta el test (que es, en campo,
//     el pánico llegando a whatsmeow y el mensaje acusándose);
//   - asignar `acusar = true` dentro de ese recover ⇒ se acusa el pánico.
//
// Y la mutación complementaria, que cae en el OTRO test: quitar el recover de enqueueCola deja esta
// garantía intacta pero ColaEnqueuePanics a cero ⇒ rojo en TestOnMessage_ElPanicAlEncolar_NoSeAcusa.
func TestHandleEvent_UnPanicoFueraDelEncolado_NoSeAcusa(t *testing.T) {
	t.Run("en el camino del entrante, antes del INSERT", func(t *testing.T) {
		cola := &spyCola{calls: &callLog{}}
		l := listenerConCola(cola)
		// El reporter de salud (Plan 031 T6) es lo PRIMERO que toca onMessage, mucho antes de la cola: un
		// pánico aquí es exactamente «un centímetro más arriba del recover que ya existía».
		l.SetHealthReporter(reporterQuePanica{})

		acusar := l.handleEvent(context.Background(), liveMessage("MSG-PANIC-ARRIBA", "quiero dos empanadas"))

		if acusar {
			t.Error("se acusó un entrante cuyo handler entró en PÁNICO antes de llegar al INSERT.\n" +
				"    CONSECUENCIA: el mensaje no se escribió en ningún sitio y WhatsApp lo da por entregado.\n" +
				"    Es el mismo defecto que cerró T1.13, alcanzado por un camino que ninguno de los otros\n" +
				"    tests de este fichero mira: basta con que alguien estreche el recover de enqueueCola o\n" +
				"    con que reviente cualquier cosa del handler que no sea el propio INSERT.")
		}
		if len(cola.got) != 0 {
			t.Fatalf("filas anotadas = %d, se esperaba 0: el pánico debía cortar ANTES del INSERT, así que "+
				"el escenario no es el que el test cree", len(cola.got))
		}
		if s := l.InboundStats(); s.ColaEnqueuePanics != 0 {
			t.Errorf("ColaEnqueuePanics = %d, se esperaba 0.\n"+
				"    Esto NO es un contador mal llevado: significa que quien recogió el pánico fue la red de\n"+
				"    enqueueCola y no la de handleEvent, o sea que las dos hacen lo mismo. Dos defensas con el\n"+
				"    mismo síntoma observable son una que ningún test puede custodiar — y esa es justo la\n"+
				"    trampa que este fichero está evitando.", s.ColaEnqueuePanics)
		}
	})

	t.Run("en un evento que no es un entrante", func(t *testing.T) {
		l := listenerConCola(&spyCola{calls: &callLog{}})
		// El consumidor de acuses lo cablea el CloudLink (SetReceiptHandler): un pánico suyo corre dentro
		// de nuestro handler y hoy se llevaría por delante la cadena entera de whatsmeow, con volcado de
		// pila y valor recuperado impreso con %v en NUESTRO log (INV-051.1).
		l.onReceipt = func(domain.ReceiptEvent) { panic("consumidor de acuses roto") }

		acusar := l.handleEvent(context.Background(), &events.Receipt{
			MessageIDs: []types.MessageID{"S1"},
			Timestamp:  time.Now(),
			Type:       types.ReceiptTypeDelivered,
		})

		// Lo que se defiende aquí es que el pánico NO SALGA del handler (si sale, este test revienta). El
		// false es la consecuencia, y es la correcta: whatsmeow ya cortaba la cadena al recuperar el
		// pánico, así que no se pierde nada que hoy funcione — y se gana no imprimir la pila.
		if acusar {
			t.Error("un pánico en un hook de sesión salió del recover concediendo el acuse: el permiso solo " +
				"puede concederse cuando el handler ha terminado su trabajo, y este no lo terminó")
		}
	})
}

// reporterQuePanica es un health.SessionReporter que revienta al sellar la prueba de vida. No es un caso
// rebuscado: el reporter lo inyecta el factory del sessionmgr y es lo primero que toca onMessage, así que
// es el punto más limpio para provocar un pánico FUERA del camino de escritura sin tocar la cola.
type reporterQuePanica struct{}

func (reporterQuePanica) SetSocketState(health.SocketState, string) {}
func (reporterQuePanica) SetDEKLoadDuration(time.Duration)          {}
func (reporterQuePanica) MarkInbound(time.Time)                     { panic("registro de salud roto") }

// CountPassiveDrop (Plan 046 · T2.3) NO revienta: el pánico de este doble se provoca en MarkInbound, que es
// lo primero que toca onMessage. Si además reventara aquí, el test del recover no distinguiría cuál de los
// dos hooks falló.
func (reporterQuePanica) CountPassiveDrop() {}

// --- lo que hace VISIBLE el cambio de garantía (T1.13) ---

// TestListenerOpts_LaDegradacionLLEGA_AlAcumuladoDelEdge custodia el otro extremo del cable de T1.13: no
// basta con que el Edge deje de acusar lo que no escribe — hay que poder VER que está pasando.
//
// 🔴 EL AGUJERO QUE CIERRA. Los contadores por sesión del listener (`InboundStats`) existían desde la Ola 1
// y al barrer el repo tenían ONCE llamantes, los once en `_test.go`: se incrementaban en memoria y morían
// con el proceso. Un Edge con la cola rota reofrecía entrantes en bucle y la única huella era una línea de
// log POR MENSAJE, con las repetidas en Debug. Desde T1.13 el acumulado del EDGE viaja dentro del mismo
// instrumento compartido que el cronómetro y sale en el bloque del latido.
//
// Se prueba sobre el Listener que nace de `listenerOpts()` —el mismo que construye serve()— porque lo que
// se custodia es el CABLE, no el contador.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - borrar `l.brackets.puerta = h.Puerta()` de WithLatencia ⇒ el acumulado del Edge no se llena nunca y
//     el bloque sale con ceros mientras el Edge se degrada;
//   - borrar `b.puerta.AnotaEnqueueError()` (o su gemelo del panic) del observador de corchetes ⇒ lo mismo,
//     por el otro extremo.
func TestListenerOpts_LaDegradacionLLEGA_AlAcumuladoDelEdge(t *testing.T) {
	casos := []struct {
		nombre string
		cola   *spyCola
		mirar  func(latencia.PuertaStats) uint64
		otro   func(latencia.PuertaStats) uint64
	}{
		{
			nombre: "el INSERT que falla",
			cola:   &spyCola{calls: &callLog{}, err: errors.New("disco lleno")},
			mirar:  func(p latencia.PuertaStats) uint64 { return p.EnqueueErrors },
			otro:   func(p latencia.PuertaStats) uint64 { return p.EnqueuePanics },
		},
		{
			nombre: "el encolado que entra en pánico",
			cola:   &spyCola{calls: &callLog{}, panicMsg: "driver muerto"},
			mirar:  func(p latencia.PuertaStats) uint64 { return p.EnqueuePanics },
			otro:   func(p latencia.PuertaStats) uint64 { return p.EnqueueErrors },
		},
	}

	for _, cs := range casos {
		t.Run(cs.nombre, func(t *testing.T) {
			h := latencia.Nuevo()
			g := gatewayDePrueba()
			g.SetCola(cs.cola, "sess-1")
			g.SetLatencia(h)

			l := NewListener(g.log, g.listenerOpts()...)
			l.handleEvent(context.Background(), liveMessage("MSG-DEGRADA", "quiero dos empanadas"))

			p := h.Puerta().Snapshot()
			if got := cs.mirar(p); got != 1 {
				t.Errorf("el acumulado del EDGE quedó en %d y se esperaba 1.\n"+
					"    CONSECUENCIA: la degradación se cuenta solo en el acumulado POR SESIÓN, que no publica\n"+
					"    nadie (once llamantes, los once tests). El bloque del latido sale con ceros mientras el\n"+
					"    Edge reofrece entrantes en bucle, y la única huella queda en una línea por mensaje —las\n"+
					"    repetidas, en Debug—. Es cambiar una garantía a ciegas.", got)
			}
			if got := cs.otro(p); got != 0 {
				t.Errorf("el otro contador se movió (%d): error y pánico van SEPARADOS a propósito — uno es una "+
					"condición de campo y el otro es un defecto, y confundirlos borra esa diferencia", got)
			}
			// Y el acumulado por sesión sigue existiendo igual: esto AÑADE una vista del Edge, no sustituye
			// la de la sesión (que es la que usa el resto de la suite).
			if s := l.InboundStats(); s.ColaEnqueueErrors+s.ColaEnqueuePanics != 1 {
				t.Errorf("el acumulado POR SESIÓN dejó de contar (errores=%d panics=%d): el contador compartido "+
					"se añadió al mismo sitio, no en lugar de", s.ColaEnqueueErrors, s.ColaEnqueuePanics)
			}
		})
	}
}

// --- el último tramo del cable: cómo se registra el handler ---

// TestRegister_SeRegistraConElHandlerQueGobiernaElAcuse cierra la vía de escape de todo lo de arriba. Todo
// este fichero prueba que handleEvent DEVUELVE el veredicto correcto; nada de eso vale si el valor no
// llega a whatsmeow, y ese último tramo vive en Register(), que no es ejercitable (necesita un
// *whatsmeow.Client con socket vivo).
//
// 🔴 LAS DOS MUTACIONES QUE LOS TESTS DE CONDUCTA NO PUEDEN VER, y por eso este test mira la FORMA:
//
//   - volver a `client.AddEventHandler(func(evt any) { l.handleEvent(ctx, evt) })` ⇒ compila, el handler
//     sigue corriendo, y el envoltorio de whatsmeow devuelve true SIEMPRE (client.go:763-768). El defecto
//     entero vuelve y los cuatro gates se quedan en verde;
//   - conservar AddEventHandlerWithSuccessStatus pero cerrar con `l.handleEvent(ctx, evt); return true` ⇒
//     lo mismo, sin cambiar ni el nombre de la función registrada.
//
// Molde: internal/infra/db/cola_cableado_ast_test.go (T3.16) y listen_gateway_cableado_test.go, con su
// misma disciplina — fallar RUIDOSAMENTE cuando el parseo deja de encontrar lo que buscaba, en vez de
// dejar de mirar en silencio.
func TestRegister_SeRegistraConElHandlerQueGobiernaElAcuse(t *testing.T) {
	const (
		fichero    = "listener.go"
		metodo     = "Register"
		conAcuse   = "AddEventHandlerWithSuccessStatus"
		sinAcuse   = "AddEventHandler"
		enrutador  = "handleEvent"
		porQueDuel = "el valor de retorno del handler es lo ÚNICO que impide que whatsmeow acuse a WhatsApp " +
			"un mensaje que no llegamos a escribir"
	)

	fset := token.NewFileSet()
	arbol, err := parser.ParseFile(fset, fichero, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("no se pudo parsear %s: %v (si el fichero se renombró, actualiza este test: es el que "+
			"vigila cómo se registra el handler, y %s)", fichero, err, porQueDuel)
	}
	cuerpo := cuerpoDelMetodo(arbol, metodo)
	if cuerpo == nil {
		t.Fatalf("%s ya no tiene el método %s: ¿se renombró? Ese es el único sitio donde el listener se "+
			"engancha al cliente real, y %s", fichero, metodo, porQueDuel)
	}

	registros, conRetorno := 0, 0
	usaElPelado := false
	ast.Inspect(cuerpo, func(n ast.Node) bool {
		llamada, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch nombreDeLoLlamado(llamada.Fun) {
		case sinAcuse:
			usaElPelado = true
		case conAcuse:
			registros++
			for _, arg := range llamada.Args {
				if lit, ok := arg.(*ast.FuncLit); ok && devuelveLaLlamadaA(lit.Body, enrutador) {
					conRetorno++
				}
			}
		}
		return true
	})

	if usaElPelado {
		t.Errorf("%s() se registra con %s.\n"+
			"    CONSECUENCIA: ese envoltorio devuelve true SIEMPRE (client.go:763-768), así que whatsmeow\n"+
			"    ACUSA el mensaje pase lo que pase — incluido el INSERT que falló. WhatsApp lo da por\n"+
			"    entregado y no lo reenvía nunca: el mensaje se pierde en silencio. Es el defecto que cerró\n"+
			"    T1.13, y vuelve entero sin poner en rojo ningún otro test.\n"+
			"    SI EL CAMBIO ES DELIBERADO: hay que retirar también la garantía «durable antes del acuse»\n"+
			"    del plan, no solo esta llamada.", metodo, sinAcuse)
	}
	if registros == 0 {
		t.Fatalf("%s() ya no registra ningún handler con %s: si el enganche se movió, ese sitio es el que "+
			"hay que custodiar ahora", metodo, conAcuse)
	}
	if conRetorno != registros {
		t.Errorf("%s() registra %d handler(es) con %s y solo %d DEVUELVE lo que dice %s().\n"+
			"    CONSECUENCIA: un handler que llama al enrutador y cierra con `return true` deja el veredicto\n"+
			"    del acuse en el suelo. Todo lo que este fichero prueba sobre qué devuelve %s pasa a ser la\n"+
			"    conducta de una función cuyo resultado nadie lee, y el mensaje que no se pudo escribir se\n"+
			"    acusa igual.", metodo, registros, conAcuse, conRetorno, enrutador, enrutador)
	}
}

// devuelveLaLlamadaA dice si el cuerpo tiene un `return <nombre>(...)`: no basta con que se INVOQUE al
// enrutador, tiene que devolverse lo que responde. Es justo la diferencia entre el arreglo y el defecto.
func devuelveLaLlamadaA(cuerpo *ast.BlockStmt, nombre string) bool {
	encontrado := false
	ast.Inspect(cuerpo, func(n ast.Node) bool {
		ret, ok := n.(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 1 {
			return true
		}
		if llamada, ok := ret.Results[0].(*ast.CallExpr); ok && nombreDeLoLlamado(llamada.Fun) == nombre {
			encontrado = true
		}
		return true
	})
	return encontrado
}

// ecoPropio es un entrante marcado como NUESTRO (IsFromMe). Se sella en vivo para que el caso lo decida el
// eco y no la ventana temporal.
func ecoPropio(id string) *events.Message {
	m := liveMessage(id, "esto lo mandamos nosotros")
	m.Info.IsFromMe = true
	return m
}
