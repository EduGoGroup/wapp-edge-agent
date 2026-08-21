package whatsmeow

// listener_perfil_test.go — EL CORTE DE LA SESIÓN PASIVA (Plan 046 · Ola 2 · T2.2, REQ-07/ADR-0027).
//
// QUÉ SE CUSTODIA AQUÍ. Una sesión marcada como PASIVA por la nube (kind:"filters") no recibe: su entrante
// se descarta EN LA PUERTA, sin encolarse, sin persistirse y sin entregarse. Eso son dos afirmaciones muy
// distintas y las dos necesitan test propio:
//
//   - LO QUE NO PASA — no hay fila, no hay llamada a NADIE, no queda rastro local (REQ-07 dice «nada
//     local», no «casi nada»);
//   - LO QUE SÍ PASA — se ACUSA a WhatsApp igual (si no, el tráfico de la pasiva se convierte en una
//     ráfaga perpetua de reofrecimientos), se CUENTA, y se mide en la serie de DESCARTES.
//
// 🔴 POR QUÉ EL CORTE ESTÁ EN EL PASO 1.5 Y NO EN EL «3.5» QUE PEDÍA EL PLAN. `enqueueCola` se alcanza
// desde DOS `return` de onMessage: el normal y el del entrante SIN HORA UTILIZABLE (`t="0"`), que encola
// ANTES de que la ventana del ADR-0037 se evalúe. Un filtro colocado tras la ventana dejaría escribirse en
// disco los entrantes de una pasiva que llegaran con `t="0"`. Hay un test por cada uno de los dos caminos,
// y el segundo es exactamente la prueba de que la desviación del plan hacía falta.
//
// Reutiliza los dobles de listener_test.go (callLog, spyCola, listenerConCola, liveMessage, quietLogger):
// mismo paquete a propósito.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo.

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/outbox"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/latencia"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
)

// outboxReal abre un outbox de verdad sobre una BD única recién migrada. Existe para poder afirmar sobre LA
// SEGUNDA TABLA del criterio (a) de T2.2 —«nada local» son DOS tablas, no una— con el mismo código que corre
// en campo en vez de con un doble que solo probaría lo que el test ya cree.
//
// 🔴 POR QUÉ HACE FALTA UN OUTBOX DE VERDAD PARA PROBAR QUE NO SE TOCA. El listener no tiene ninguna
// referencia al outbox, así que «no escribe ahí» es cierto POR CONSTRUCCIÓN hoy — y ese es exactamente el
// tipo de verdad que deja de serlo sin que nadie lo note. Un `Depth == 0` medido sobre la tabla real es lo
// que convierte esa construcción en una aserción falsable el día que alguien cablee algo aquí.
func outboxReal(t *testing.T) *outbox.Store {
	t.Helper()
	ctx := context.Background()
	database, err := wappdb.OpenAndMigrate(ctx, filepath.Join(t.TempDir(), "edge.db"))
	if err != nil {
		t.Fatalf("abrir/migrar la BD única: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	ob, err := outbox.New(ctx, database, 0, 0, quietLogger())
	if err != nil {
		t.Fatalf("construir el outbox: %v", err)
	}
	return ob
}

// pasiva es el consultor de perfiles de una sesión CALLADA. Existe con nombre —en vez de una closure
// anónima repetida— para que los tests se lean como el escenario que describen, igual que `apagado` en
// listener_test.go. Afirma además que se pregunta por la sesión CORRECTA: devolver true a ciegas dejaría
// pasar el bug de consultar el mapa con otro session_id.
func pasiva(t *testing.T, esperado string) func(string) bool {
	t.Helper()
	return func(id string) bool {
		if id != esperado {
			t.Errorf("el listener consultó el perfil de la sesión %q y la suya es %q.\n"+
				"    CONSECUENCIA: el mapa de perfiles se indexa por session_id. Preguntando por otro, el Edge "+
				"nunca encontraría a sus propias pasivas y todas seguirían recibiendo — sin un solo error.", id, esperado)
		}
		return id == esperado
	}
}

// activa es su complemento: el consultor que dice que esta sesión NO está callada (el caso mayoritario y
// el que tiene que comportarse EXACTAMENTE como antes del Plan 046).
func activa(string) bool { return false }

// TestOnMessage_SesionPasiva_NoDejaNadaLocalYSeAcusa es EL test de T2.2: los criterios (a) y (b) juntos,
// porque son la misma decisión vista por sus dos caras.
//
// (a) NADA LOCAL, Y SON DOS TABLAS. El enunciado exige aserción sobre las DOS que el Edge escribe:
//
//   - `cola_entrantes`: se afirma sobre las LLAMADAS y no solo sobre las filas capturadas, y la diferencia
//     importa — `len(cola.got) == 0` también sería cierto si el Enqueue se hubiera intentado y hubiera
//     fallado. Cero llamadas es lo que prueba que no se intentó escribir;
//   - `outbox`: se abre uno REAL sobre una BD migrada y se mide su profundidad. Que el listener no tenga
//     referencia al outbox hace que esto sea cierto por construcción HOY, y por eso mismo hay que medirlo:
//     una verdad estructural sin aserción es la que se rompe sin que nadie se entere. (Que además no tenga
//     más colaboradores que la cola lo custodia listener_camino_caliente_test.go.)
//
// (b) SE ACUSA. Verificado por MUTACIÓN, según pide el criterio de cierre.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - `return true` → `return false` en la rama del perfil pasivo (listener.go, paso 1.5) ⇒ cae la
//     aserción del acuse. CONSECUENCIA EN CAMPO de esa mutación: whatsmeow no acusa, WhatsApp reofrece el
//     MISMO mensaje en cada reconexión, se vuelve a descartar por el mismo motivo (la config no cambia) y
//     el número pasivo entra en una tormenta de reenvíos perpetua.
//   - borrar el bloque del filtro entero (o moverlo detrás del `return l.enqueueCola(...)`) ⇒ aparecen la
//     llamada y la fila: la sesión pasiva vuelve a escribir en disco y REQ-07 deja de cumplirse.
//   - quitar `l.brackets.countPassiveDrop()` ⇒ el contador se queda a 0 y el filtro se vuelve invisible.
//   - quitar `camino = latencia.Descartado` ⇒ el descarte se cuenta como ENCOLADO y el p99 del handler
//     mejora justo cuando el Edge más filtra (INV-051.2 se juzga contra la serie `Encolado`).
func TestOnMessage_SesionPasiva_NoDejaNadaLocalYSeAcusa(t *testing.T) {
	ctx := context.Background()
	h := latencia.Nuevo()
	calls := &callLog{}
	cola := &spyCola{calls: calls}
	ob := outboxReal(t)
	l := listenerConCola(cola, WithSesionPasiva(pasiva(t, "sess-1")), WithLatencia(h))

	acusar := l.handleEvent(ctx, liveMessage("MSG-PASIVA", "quiero dos empanadas"))

	// TABLA 2 — el outbox. Se mide ANTES que nada porque es la que nadie miraba.
	if depth, err := ob.Depth(ctx, "sess-1"); err != nil || depth != 0 {
		t.Errorf("outbox: profundidad = %d (err=%v), se esperaba 0.\n"+
			"    CONSECUENCIA: REQ-07 dice «nada local» y eso incluye la SEGUNDA tabla. Un entrante de una sesión\n"+
			"    pasiva que dejara rastro en el outbox saldría además hacia la nube en el próximo drenaje, que es\n"+
			"    exactamente lo que el cliente marca la sesión como pasiva para impedir.", depth, err)
	}

	if got := calls.snapshot(); len(got) != 0 {
		t.Errorf("llamadas = %v, no debía haber ninguna.\n"+
			"    CONSECUENCIA: el entrante de una sesión PASIVA quedó escrito en la cola durable. Está cifrado con\n"+
			"    la DEK de la sesión y se poda a las 24 h, pero está EN DISCO — y REQ-07 dice «nada local»: ni en\n"+
			"    outbox ni en tabla alguna.", got)
	}
	if len(cola.got) != 0 {
		t.Errorf("filas anotadas = %d, se esperaban 0 (%v)", len(cola.got), colaWAIDs(cola.got))
	}
	if !acusar {
		t.Error("el handler NEGÓ el acuse de un entrante descartado por perfil pasivo.\n" +
			"    CONSECUENCIA: whatsmeow retorna antes del acuse y WhatsApp REENVÍA. Como el reenvío llega\n" +
			"    idéntico y la config no ha cambiado, se vuelve a descartar y se vuelve a pedir otro reenvío:\n" +
			"    una ráfaga PERPETUA sobre el número que precisamente queríamos tener callado. Es el mismo\n" +
			"    razonamiento del descarte por ventana (ADR-0037), escrito en listener.go:566-570.")
	}
	if s := l.InboundStats(); s.DroppedByPassiveProfile != 1 {
		t.Errorf("DroppedByPassiveProfile = %d, se esperaba 1.\n"+
			"    CONSECUENCIA: un descarte que no deja fila, no deja log (va en Debug) y no cambia el acuse es\n"+
			"    INDISTINGUIBLE de «esa sesión no recibió nada». Sin este contador, un filtro que corta de más\n"+
			"    —o que no corta— no se ve por ninguna parte.", s.DroppedByPassiveProfile)
	}
	// El descarte NO puede contaminar los contadores de sus vecinos: cada uno cuenta lo suyo.
	if s := l.InboundStats(); s.DroppedSelf != 0 || s.DroppedByWindow != 0 || s.AdmittedNoTimestamp != 0 {
		t.Errorf("el descarte por perfil se imputó a otro contador: self=%d ventana=%d sin_hora=%d",
			s.DroppedSelf, s.DroppedByWindow, s.AdmittedNoTimestamp)
	}
	if got := h.Snapshot(latencia.Descartado).N; got != 1 {
		t.Errorf("serie DESCARTADO: N = %d, se esperaba 1", got)
	}
	if got := h.Snapshot(latencia.Encolado).N; got != 0 {
		t.Errorf("serie ENCOLADO: N = %d, se esperaba 0: contar un descarte como encolado diluye el p99 con "+
			"muestras de microsegundos y MEJORA el número justo cuando el Edge filtra más", got)
	}
}

// TestOnMessage_SesionPasiva_TampocoElEntranteSinHoraUtilizable es LA PRUEBA DE QUE EL CORTE NO PODÍA IR
// DONDE EL PLAN LO MANDABA.
//
// El plan prescribía un «paso 3.5», entre la ventana del ADR-0037 y el `enqueueCola`. Pero `enqueueCola`
// tiene DOS llamantes, y el otro es el paso 2: el entrante cuyo `Info.Timestamp` es CERO —lo que ocurre de
// verdad cuando el atributo `t` vale "0"— se ADMITE por precaución y encola con la hora local, ANTES de que
// la ventana llegue a evaluarse. Con el filtro en el 3.5, ESTE mensaje de una sesión pasiva acabaría escrito
// en `cola_entrantes.db`. Es raro; REQ-07 no admite «raros».
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (es la del plan original): mover el bloque del filtro a después de la
// ventana temporal ⇒ este test cae y el de arriba sigue VERDE. Ese contraste es el test.
func TestOnMessage_SesionPasiva_TampocoElEntranteSinHoraUtilizable(t *testing.T) {
	calls := &callLog{}
	cola := &spyCola{calls: calls}
	l := listenerConCola(cola, WithSesionPasiva(pasiva(t, "sess-1")))

	sinHora := liveMessage("MSG-PASIVA-T0", "quiero dos empanadas")
	sinHora.Info.Timestamp = time.Time{} // GetUnixTime devuelve el cero con ok=true cuando el atributo t="0"

	acusar := l.handleEvent(context.Background(), sinHora)

	if got := calls.snapshot(); len(got) != 0 || len(cola.got) != 0 {
		t.Errorf("llamadas = %v, filas = %d: el entrante SIN HORA UTILIZABLE de una sesión pasiva dejó rastro "+
			"local. Es el camino que ADMITE por precaución y encola ANTES de la ventana; por eso el filtro va "+
			"en el paso 1.5 y no en el 3.5 que decía el plan", got, len(cola.got))
	}
	if !acusar {
		t.Error("se negó el acuse: el descarte por perfil acusa por los dos caminos, no solo por el normal")
	}
	if s := l.InboundStats(); s.DroppedByPassiveProfile != 1 || s.AdmittedNoTimestamp != 0 {
		t.Errorf("contadores = pasivo:%d sin_hora:%d, se esperaba 1 y 0: el mensaje se descarta ANTES de que la "+
			"rama del timestamp lo cuente como admitido", s.DroppedByPassiveProfile, s.AdmittedNoTimestamp)
	}
}

// TestOnMessage_SesionActiva_DejaLaFilaIdenticaAHoy es el criterio (c): con el consultor cableado y la
// sesión ACTIVA, la fila tiene que ser la MISMA que sin Plan 046 — no «parecida»: la misma.
//
// Se compara contra un listener SIN la opción en vez de contra una lista de campos escrita a mano, y es
// deliberado: una comparación campo a campo envejece en cuanto alguien añade uno a app.ColaItem, y lo haría
// en silencio. El mismo *events.Message alimenta a los dos, así que todo lo derivado de él es determinista.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: que el filtro toque el item (marcarlo, cambiar su estado, alterar el
// sello) en vez de limitarse a dejar pasar. El corte es binario: o no hay mensaje, o no hay diferencia.
func TestOnMessage_SesionActiva_DejaLaFilaIdenticaAHoy(t *testing.T) {
	msg := liveMessage("MSG-ACTIVA", "quiero dos empanadas")

	conFiltro := &spyCola{calls: &callLog{}}
	comoHoy := &spyCola{calls: &callLog{}}

	lFiltro := listenerConCola(conFiltro, WithSesionPasiva(activa))
	lHoy := listenerConCola(comoHoy) // sin la opción: el Edge de antes del Plan 046

	if !lFiltro.handleEvent(context.Background(), msg) {
		t.Fatal("un entrante de sesión ACTIVA que dejó fila debe acusarse")
	}
	lHoy.handleEvent(context.Background(), msg)

	if len(conFiltro.got) != 1 {
		t.Fatalf("filas con el filtro cableado = %d, se esperaba 1: la sesión ACTIVA no se corta (fail-open, "+
			"D-046.2)", len(conFiltro.got))
	}
	if len(comoHoy.got) != 1 {
		t.Fatalf("filas sin el filtro = %d, se esperaba 1: el escenario no es el que el test cree", len(comoHoy.got))
	}
	if !reflect.DeepEqual(conFiltro.got[0], comoHoy.got[0]) {
		// No se imprimen los items: llevan el TEXTO del mensaje (INV-051.1 no se levanta por estar en un test).
		t.Errorf("la fila de la sesión ACTIVA no es idéntica a la que dejaba el Edge antes del Plan 046: %v vs %v",
			colaWAIDs(conFiltro.got), colaWAIDs(comoHoy.got))
	}
	if s := lFiltro.InboundStats(); s.DroppedByPassiveProfile != 0 {
		t.Errorf("DroppedByPassiveProfile = %d en una sesión ACTIVA, se esperaba 0", s.DroppedByPassiveProfile)
	}
}

// TestOnMessage_SinConfigDeFiltros_DejaFila es el criterio (d): el FAIL-OPEN de D-046.2 en sus tres formas
// alcanzables. Un Edge nunca pierde tráfico por una config ausente o por un cableado a medias.
//
// 🔴 LA ASIMETRÍA NO ES ESTÉTICA. De los dos fallos posibles, caer hacia «pasiva» deja al Edge SORDO —el
// cliente escribe y no pasa nada, sin un solo error en el log, y el síntoma no apunta a su causa—; caer
// hacia «activa» solo sube tráfico que la nube ya sabe ignorar (`reactiveBlocked`, D-046.7). Se falla hacia
// el lado que no pierde mensajes.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: invertir el default de `sesionEsPasiva` (devolver true sin predicado
// cableado, o que WithSesionPasiva acepte el nil y lo guarde) ⇒ TODAS las sesiones de TODOS los Edge sin
// config se quedan mudas de golpe.
func TestOnMessage_SinConfigDeFiltros_DejaFila(t *testing.T) {
	casos := []struct {
		nombre string
		opts   []ListenerOption
	}{
		{"sin la opción (el Edge de antes del 046)", nil},
		{"opción cableada con nil", []ListenerOption{WithSesionPasiva(nil)}},
		{"consultor sin config aplicada (responde 'no pasiva')", []ListenerOption{WithSesionPasiva(activa)}},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			cola := &spyCola{calls: &callLog{}}
			l := listenerConCola(cola, c.opts...)

			if !l.handleEvent(context.Background(), liveMessage("MSG-FAILOPEN", "quiero dos empanadas")) {
				t.Fatal("el entrante dejó fila: tiene que acusarse")
			}
			if len(cola.got) != 1 {
				t.Fatalf("filas = %d, se esperaba 1.\n"+
					"    CONSECUENCIA: sin config de filtros el Edge se quedó SORDO. El cliente escribe y no pasa\n"+
					"    nada, no hay error en ningún log y el síntoma no apunta a su causa por ninguna parte.",
					len(cola.got))
			}
		})
	}
}

// TestPerfilPasivo_NoTocaLosAcusesDeSalientes fija D-046.3 por su primera mitad: los `Receipt` de los
// mensajes que la sesión pasiva ENVÍA siguen subiendo y contándose como métrica de negocio. La privacidad
// que este plan compra es la del CONTENIDO ENTRANTE, no la de la operación de la flota.
//
// Hoy es cierto POR CONSTRUCCIÓN —un *events.Receipt no pasa por onMessage, se enruta en handleEvent— y el
// test existe justamente para eso: para que SIGA siendo cierto el día que alguien mueva el corte de sitio.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: subir el filtro de perfil a `handleEvent` (antes del switch), que es el
// sitio donde «parece» que hay que ponerlo si uno lee «la sesión pasiva no habla con la nube» sin el matiz
// de D-046.3 ⇒ el acuse deja de propagarse y la nube pierde la prueba de entrega de sus PROPIOS envíos.
func TestPerfilPasivo_NoTocaLosAcusesDeSalientes(t *testing.T) {
	l := listenerConCola(&spyCola{calls: &callLog{}}, WithSesionPasiva(pasiva(t, "sess-1")))
	var got []domain.ReceiptEvent
	l.onReceipt = func(r domain.ReceiptEvent) { got = append(got, r) }

	acusar := l.handleEvent(context.Background(), &events.Receipt{
		MessageIDs: []string{"M1"},
		Type:       types.ReceiptTypeDelivered,
		Timestamp:  time.Now(),
	})

	if len(got) != 1 {
		t.Fatalf("acuses propagados = %d, se esperaba 1: el Receipt de un SALIENTE de la sesión pasiva SÍ sube "+
			"(D-046.3). Lo que el 046 corta es el contenido ENTRANTE, no la operación de la flota", len(got))
	}
	if !acusar {
		t.Error("un *events.Receipt no lleva acuse por esta vía: negarlo cortaría la cadena de handlers de " +
			"whatsmeow en seco sin ganar nada")
	}
}

// TestPerfilPasivo_ElSessionHealthDeLaSesionPasivaSIGUE_SUBIENDO fija D-046.3 por su segunda mitad: el
// Heartbeat y el `SessionHealth` de una sesión pasiva SIEMPRE suben. La privacidad que este plan compra es la
// del CONTENIDO ENTRANTE, no la de la operación de la flota: un Edge que dejara de reportar la salud de sus
// sesiones calladas sería, desde la nube, un Edge caído.
//
// 🔴 QUÉ SE AFIRMA AQUÍ Y QUÉ SE AFIRMABA ANTES. La versión anterior de este test comprobaba `MarkInbound`,
// que es el MECANISMO —la prueba de vida del socket— y no el parte. Un `MarkInbound` correcto con un
// `Collect` que devolviera ok=false seguiría siendo un Edge mudo para la nube, y el test habría pasado. Se
// entra por el `health.Collector` REAL, que es literalmente lo que el adapter CloudLink mapea a
// `SessionHealth` en cada latido (ADR-0023), y se comprueba que el parte SALE y que sale con datos usables.
//
// 🔴 POR QUÉ `last_inbound_event_age_s` ES EL CAMPO DELICADO. Es lo que distingue «socket vivo y callado» de
// «socket muerto y nadie se ha enterado». Si el descarte por perfil saltara el MarkInbound, cada sesión
// pasiva de la flota aparecería con una edad creciente e infinita: indistinguible de una sesión rota, y la
// señal que existe para detectar Edges caídos se volvería ruido. Por eso se afirma que la edad es CERO
// (entrante recién sellado) y no simplemente que el parte exista.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - mover el bloque del filtro POR ENCIMA del `l.reporter.MarkInbound(inicio)` —el «optimicemos, si vamos
//     a descartar no hagamos nada»— ⇒ la edad del último entrante crece sin techo en todas las pasivas;
//   - hacer que el corte devuelva antes de tocar el reporter (o retirar `SetHealthReporter` del cableado del
//     ciclo de escucha) ⇒ `Collect` devuelve ok=false y la sesión DESAPARECE del heartbeat y de /v1/health,
//     que es la forma más cara de «filtrar de más»: la nube deja de saber si ese teléfono sigue conectado.
func TestPerfilPasivo_ElSessionHealthDeLaSesionPasivaSigueSubiendo(t *testing.T) {
	ctx := context.Background()
	reg := health.NewRegistry()
	l := listenerConCola(&spyCola{calls: &callLog{}}, WithSesionPasiva(pasiva(t, "sess-1")))
	l.SetHealthReporter(reg.For("sess-1"))
	// El socket está VIVO: es el estado que el listener deja en *events.Connected y el que hace que la
	// pregunta «¿por qué no llega nada de este número?» tenga una respuesta distinta de «está caído».
	reg.SetSocketState("sess-1", health.SocketConnected, "")

	l.handleEvent(ctx, liveMessage("MSG-SALUD", "quiero dos empanadas"))

	// El PARTE, que es lo que viaja: mismo colector que arma el heartbeat y que responde GET /v1/health.
	//
	// Reloj REAL a propósito, y no uno inyectado con una fecha fija: el sello del entrante lo pone `onMessage`
	// con `time.Now()` y no hay costura para cambiárselo, así que un reloj de test congelado en cualquier
	// instante mediría la distancia entre DOS relojes distintos y la edad saldría en horas. Con el real, la
	// edad es de microsegundos y trunca a 0, que es justo lo que se quiere afirmar.
	col := health.NewCollector(reg, nil, nil, "test", time.Now().Add(-time.Hour))
	r, ok := col.Collect(ctx, "sess-1")
	if !ok {
		t.Fatal("la sesión PASIVA no produce SessionHealth: el colector no la reconoce.\n" +
			"    CONSECUENCIA: desaparece del heartbeat y de GET /v1/health. Desde la nube, una sesión que no\n" +
			"    reporta salud es una sesión CAÍDA — así que marcar un número como pasivo lo convertiría en una\n" +
			"    alarma de operación permanente. D-046.3 dice lo contrario: el Heartbeat y el SessionHealth de\n" +
			"    una sesión pasiva SIEMPRE suben; lo que se corta es el contenido entrante, no la telemetría.")
	}
	if r.SocketState != string(health.SocketConnected) {
		t.Errorf("SessionHealth.socket_state = %q, want %q: el corte por perfil no puede tocar la prueba de vida "+
			"del socket", r.SocketState, health.SocketConnected)
	}
	if r.LastInboundAgeS != 0 {
		t.Errorf("SessionHealth.last_inbound_event_age_s = %d, want 0.\n"+
			"    CONSECUENCIA: el entrante descartado por perfil pasivo NO selló la prueba de vida del socket, así\n"+
			"    que la edad crece sin techo en TODAS las sesiones pasivas de la flota y pasan a ser\n"+
			"    indistinguibles de sesiones rotas. MarkInbound es salud del SOCKET, no señal de negocio: se marca\n"+
			"    para TODO mensaje, incluidos los descartados.", r.LastInboundAgeS)
	}
	// Y el contador del filtro viaja en el MISMO parte: es lo que explica por qué esa sesión, con el socket
	// conectado y recibiendo, no entrega nada.
	if r.DroppedByPassiveProfile != 1 {
		t.Errorf("SessionHealth.dropped_passive = %d, want 1: sin este número, un socket conectado que no\n"+
			"    entrega nada se lee como una avería y no como la configuración funcionando",
			r.DroppedByPassiveProfile)
	}
}

// TestListenerOpts_ElConsultorDePerfiles_LLEGA_AlListener custodia el último tramo del cable, que es donde
// este plan tiene su fallo silencioso más caro: el `append` de `WithSesionPasiva(g.sesionPasiva)` en
// listenerOpts(). Borrarlo deja `go build`, `go vet` y los cuatro gates en VERDE, con el mapa de perfiles
// perfectamente construido, perfectamente empujado por la nube, perfectamente aplicado… y un Listener que
// no lo consulta nunca: TODAS las sesiones pasivas del Edge subiendo su tráfico, y REQ-07 incumplido sin un
// solo síntoma.
//
// Se comprueba por CONDUCTA (¿desapareció la fila?) y no por inspección de la lista de opciones: una opción
// presente pero inerte pasaría una comprobación estructural.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - borrar el `append` de listenerOpts() ⇒ el Listener nace sin consultor y la fila aparece.
//   - vaciar el cuerpo de SetSesionPasiva (o hacerlo escribir en otro campo) ⇒ el gateway no guarda nada.
//   - meter el `append` DENTRO del bloque `if g.cola != nil && g.sessionID != ""` ⇒ este test seguiría
//     verde (aquí sí hay cola); lo caza el subtest de abajo.
func TestListenerOpts_ElConsultorDePerfiles_LLEGA_AlListener(t *testing.T) {
	const sid = "sess-42"
	cola := &spyCola{calls: &callLog{}}

	g := gatewayDePrueba()
	g.SetCola(cola, sid)
	g.SetSesionPasiva(pasiva(t, sid))

	l := NewListener(g.log, g.listenerOpts()...)
	l.handleEvent(context.Background(), liveMessage("MSG-CABLE-PERFIL", "quiero dos empanadas"))

	if len(cola.got) != 0 {
		t.Fatalf("filas anotadas = %d, se esperaban 0.\n"+
			"    CONSECUENCIA: el consultor de perfiles que el daemon construyó y el Manager inyectó NO llega al\n"+
			"    Listener. Todas las sesiones pasivas del Edge siguen encolando, persistiendo y entregando su\n"+
			"    tráfico entrante: REQ-07 incumplido con los cuatro gates en VERDE.\n"+
			"    SI EL CAMBIO ES DELIBERADO (se retira el filtro): hay que retirar también el contador y la ficha\n"+
			"    de la funcionalidad, no solo este append.", len(cola.got))
	}
	if s := l.InboundStats(); s.DroppedByPassiveProfile != 1 {
		t.Errorf("DroppedByPassiveProfile = %d, se esperaba 1", s.DroppedByPassiveProfile)
	}
}

// TestListenerOpts_UnaSesionSinCola_TambienConsultaElPerfil fija la decisión de que el consultor viaje
// FUERA del bloque de la cola, al contrario que el interruptor del clasificador. El motivo es de
// naturaleza, no de estilo: aquél solo decide en qué ESTADO nace una fila (sin cola no tiene nada que
// decidir), mientras que éste decide si el mensaje ENTRA — y eso es anterior e independiente de que haya
// dónde anotarlo. Sin este test, «ordenar» el método metiendo el append dentro del `if` sería un cambio
// invisible que apagaría el filtro justo en la sesión peor cableada, y de paso su contador.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: mover el `append` del consultor dentro del `if g.cola != nil && …`.
func TestListenerOpts_UnaSesionSinCola_TambienConsultaElPerfil(t *testing.T) {
	const sid = "sess-77"
	consultado := false

	g := gatewayDePrueba() // sin SetCola: la sesión queda SORDA (T3.0), pero el handler sigue corriendo
	g.SetSesionPasiva(func(id string) bool {
		consultado = consultado || id == sid
		return true
	})

	// El session_id se inyecta SUELTO porque este gateway nunca pasó por `SetCola` (no hay cola NI sesión que
	// registrar), y el Listener necesita saber cuál es la suya para poder preguntar por ella. El caso hermano
	// —gateway que SÍ pasó por SetCola con la cola a nil, que es lo que hace el factory del sessionmgr cuando
	// no hay cola inyectada— lo cubre TestListenerOpts_SinCola_ElFiltroDePerfilesSigueCortando, y ahí el id
	// viaja por `listenerOpts()` sin ayuda. Lo que aquí se prueba es que el filtro no depende de la cola.
	l := NewListener(g.log, append(g.listenerOpts(), WithSessionID(sid))...)
	l.handleEvent(context.Background(), liveMessage("MSG-SIN-COLA-PERFIL", "quiero dos empanadas"))

	if !consultado {
		t.Error("una sesión SIN cola no consultó su perfil: el corte decide si el mensaje ENTRA, no en qué " +
			"estado nace su fila, así que no puede depender de que haya cola")
	}
	if s := l.InboundStats(); s.DroppedByPassiveProfile != 1 {
		t.Errorf("DroppedByPassiveProfile = %d, se esperaba 1: el descarte se cuenta también sin cola",
			s.DroppedByPassiveProfile)
	}
}
