package whatsmeow

// listener_perfil_publicacion_test.go — LA PUBLICACIÓN DEL CONTADOR DE DESCARTES POR PERFIL PASIVO
// (Plan 046 · Ola 2 · T2.3, REQ-11).
//
// 🔑 LA LECCIÓN QUE ESTE FICHERO EXISTE PARA NO REPETIR: un contador que nadie lee es medio arreglo.
// `Listener.InboundStats()` tuvo ONCE llamantes durante una ola entera y los once eran `_test.go`: los
// números vivían en memoria por sesión y morían con el proceso, mientras en campo un Edge con la cola rota
// reofrecía entrantes en bucle sin dejar más huella que un log en Debug (Plan 051 · T1.13). Aquí se custodia
// que el contador del filtro de privacidad NACE PUBLICADO, por las tres bocas y desde UN SOLO SITIO
// (`bracketObserver.countPassiveDrop`):
//
//	stats  → `Listener.InboundStats()`, la vista por sesión que usan los tests del corte (T2.2);
//	puerta → el acumulado del EDGE, que sale en el bloque del latido como `descartes_perfil_pasivo`;
//	salud  → el registro por sesión, que sale en `GET /v1/health` como `dropped_passive`.
//
// El tramo que sigue a `salud` —Registry → Report → JSON— se custodia en
// `internal/app/health/perfil_pasivo_test.go` y en
// `internal/adapters/control/server/health_perfil_pasivo_test.go`: `control/server` depende (vía
// `sessionmgr`) de este paquete, así que un test de aquí no puede importar el servidor sin un ciclo.
//
// Reutiliza los dobles de listener_test.go y listener_perfil_test.go (spyCola, callLog, pasiva, newJID).

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/latencia"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Datos del remitente del escenario de INV-6. Son constantes con nombre —y no literales sueltos— porque el
// test los usa DOS veces cada uno: una para fabricar el mensaje y otra para buscarlos en el log. Un literal
// duplicado que alguien "corrigiera" en un sitio y no en el otro convertiría este test en uno que no busca
// nada y siempre pasa.
const (
	numeroRemitente = "34600111222"
	nombreRemitente = "Alicia Perez Fabricada"
	textoRemitente  = "quiero dos empanadas de carne"
	// El JID de la PROPIA sesión (el número del NEGOCIO, no el del remitente). Es el atributo que
	// `sessionmgr/listen.go` mete en el logger de cada sesión y que por tanto arrastra TODA línea que el
	// listener escribe en producción. Se elige un número que no comparte ni un dígito con el del remitente:
	// si compartieran prefijo, un `strings.Count` no podría distinguir cuál de los dos se filtró.
	jidPropioDeLaSesion = "56988877766:1@s.whatsapp.net"
)

// mensajeDelRemitente arma un entrante VIVO con identidad y contenido DISTINGUIBLES: un número largo que no
// puede aparecer por casualidad en un sello de tiempo, un push name propio y un texto con palabras que no
// están en ningún mensaje de log del repo. `liveMessage` no sirve aquí: su remitente es "123" y su nombre
// "Alice", y buscar "123" en un log con timestamps daría falsos positivos constantes.
func mensajeDelRemitente(id string) *events.Message {
	jid := newJID(numeroRemitente, types.DefaultUserServer)
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: jid, Sender: jid},
			ID:            id,
			PushName:      nombreRemitente,
			Timestamp:     time.Now(),
			Type:          "text",
		},
		Message: &waE2E.Message{Conversation: proto.String(textoRemitente)},
	}
}

// logCapturado devuelve el LOGGER DE SESIÓN tal como lo construye producción, sobre un buffer propio.
//
// 🔴 LLEVA EL `jid` DE LA PROPIA SESIÓN, Y ESO ES LA MITAD DEL TEST DE INV-6. En campo, el logger de cada
// listener nace de `m.log.With("session_id", …, "jid", meta.JID)` (sessionmgr/listen.go), así que TODAS sus
// líneas arrastran ese atributo — incluidas las dos del filtro. La versión anterior de este helper construía
// un logger PELADO, de modo que el test de PII no reproducía la condición de producción: buscaba fugas en un
// log que, por construcción, tenía menos datos de los que tiene el de verdad. Un test que no reproduce el
// escenario pasa por casualidad.
//
// 🔴 Y EL NIVEL IMPORTA IGUAL: la línea por-mensaje que escribe el corte (T2.2) va en Debug, así que con un
// logger a Info el test estaría buscando PII en un log donde esa línea ni siquiera aparece.
func logCapturado() (sharedlogger.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	base := sharedlogger.New(sharedlogger.WithWriter(&buf), sharedlogger.WithLevel(slog.LevelDebug))
	return base.With("session_id", "sess-1", "jid", jidPropioDeLaSesion), &buf
}

// listenerPasivoConLog arma el listener del escenario: cola espía, sesión "sess-1" marcada PASIVA, cronómetro
// compartido y registro de salud reales, y el logger de sesión capturado.
func listenerPasivoConLog(t *testing.T) (*Listener, *spyCola, *latencia.Histograma, *health.Registry, *bytes.Buffer) {
	t.Helper()
	log, buf := logCapturado()
	h := latencia.Nuevo()
	reg := health.NewRegistry()
	cola := &spyCola{calls: &callLog{}}
	l := NewListener(log,
		WithCola(cola), WithSessionID("sess-1"),
		WithSesionPasiva(pasiva(t, "sess-1")),
		WithLatencia(h))
	l.SetHealthReporter(reg.For("sess-1"))
	return l, cola, h, reg, buf
}

// TestOnMessage_SesionPasiva_ElContadorSalePorLasTresBocas es EL test de T2.3 en el lado del listener: un
// solo descarte tiene que aparecer, con el mismo valor, en los tres acumulados.
//
// 🔴 POR QUÉ SE AFIRMAN LOS TRES A LA VEZ Y NO UNO POR TEST. El modo de fallo que importa no es «no cuenta»
// —eso lo caza cualquiera de los tres— sino «cuenta en dos de los tres», que es lo que pasa en cuanto los
// incrementos se reparten entre el llamante y el contador. Y ese fallo es especialmente caro en
// observabilidad: dos cifras discrepantes no delatan cuál miente, así que el operador deja de fiarse de las
// dos. Afirmarlos juntos es lo que convierte «se escriben desde el mismo sitio» en una propiedad probada.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - quitar `b.puerta.AnotaDescartePasivo()` de countPassiveDrop ⇒ el latido publica 0 para siempre;
//   - quitar `b.salud.CountPassiveDrop()` ⇒ `GET /v1/health` publica 0 para siempre (y con él el bundle);
//   - quitar `l.brackets.setSalud(r)` de SetHealthReporter ⇒ el contador se queda sin la boca de la salud
//     aunque el registro esté perfectamente cableado: el fallo silencioso más caro de esta tarea;
//   - cablear en `SetHealthReporter` un reporter distinto del de `l.reporter` ⇒ el contador de una sesión
//     acabaría reportándose en la fila de otra.
func TestOnMessage_SesionPasiva_ElContadorSalePorLasTresBocas(t *testing.T) {
	l, cola, h, reg, _ := listenerPasivoConLog(t)

	for i := 0; i < 2; i++ {
		if !l.handleEvent(context.Background(), mensajeDelRemitente("MSG-3BOCAS")) {
			t.Fatal("el descarte por perfil pasivo debe ACUSAR")
		}
	}

	if len(cola.got) != 0 {
		t.Fatalf("el escenario no es el que el test cree: quedaron %d filas", len(cola.got))
	}
	if got := l.InboundStats().DroppedByPassiveProfile; got != 2 {
		t.Errorf("boca 1 (por sesión, InboundStats) = %d, want 2", got)
	}
	if got := h.Puerta().Snapshot().DescartesPasivos; got != 2 {
		t.Errorf("boca 2 (acumulado del EDGE, el del latido) = %d, want 2.\n"+
			"    CONSECUENCIA: `descartes_perfil_pasivo` sale a 0 en el bloque del latido pase lo que pase. Es la\n"+
			"    ÚNICA huella de un filtro que no deja fila, no sube al cable y acusa igual que si hubiera\n"+
			"    entregado: sin ella, un filtro roto y una sesión sin tráfico son la misma línea.", got)
	}
	snap, ok := reg.Snapshot("sess-1")
	if !ok {
		t.Fatal("la sesión no tiene entrada en el registro de salud")
	}
	if snap.DroppedByPassiveProfile != 2 {
		t.Errorf("boca 3 (salud por sesión, la de GET /v1/health) = %d, want 2.\n"+
			"    CONSECUENCIA: `dropped_passive` sale a 0 en el plano de control y en el bundle de diagnóstico.\n"+
			"    Es la única de las tres que dice QUÉ sesión está callada: el bloque del latido es del Edge\n"+
			"    entero y no puede responder eso.", snap.DroppedByPassiveProfile)
	}
}

// TestOnMessage_SesionPasiva_SinRegistroDeSaludNoRompeNada: el contador es un instrumento de medida y jamás
// puede ser la causa de que se caiga la escucha. Sin reporter cableado (tests, cableados que no vienen del
// daemon) el corte tiene que seguir funcionando igual, sin pánico y sin perder el acuse.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: quitar la guarda `if b.salud != nil` de countPassiveDrop ⇒ nil pointer
// dereference sobre una interfaz nil, recogido por el `recover` de handleEvent, que devolvería FALSE ⇒
// WhatsApp reofrecería el mensaje en bucle. Nótese la cadena: un descuido en un contador termina en una
// tormenta de reenvíos.
func TestOnMessage_SesionPasiva_SinRegistroDeSaludNoRompeNada(t *testing.T) {
	cola := &spyCola{calls: &callLog{}}
	l := listenerConCola(cola, WithSesionPasiva(pasiva(t, "sess-1"))) // sin SetHealthReporter

	if !l.handleEvent(context.Background(), mensajeDelRemitente("MSG-SIN-SALUD")) {
		t.Fatal("sin registro de salud cableado el descarte perdió el acuse: el contador reventó y el recover " +
			"de handleEvent devolvió false")
	}
	if got := l.InboundStats().DroppedByPassiveProfile; got != 1 {
		t.Errorf("DroppedByPassiveProfile = %d, want 1", got)
	}
}

// TestOnMessage_SesionPasiva_ElLogNoLLeva_NiNumeroNiNombreNiTexto es el criterio (c) de T2.3: INV-6
// verificado POR GREP SOBRE LA SALIDA CAPTURADA, no por lectura del código.
//
// 🔴 POR QUÉ SE BUSCA SOBRE EL LOG COMPLETO Y NO SOBRE «la línea del filtro». Un test que aislara la línea
// del filtro y la inspeccionara daría por buenas las fugas que ocurren en cualquier OTRA línea del mismo
// camino —y el camino del descarte pasivo escribe dos: la de T2.2 (Debug, por mensaje) y la de resumen
// (Info, throttled)—. Se busca en TODO lo que el handler escribió, que es lo que de verdad acaba en
// `/root/source/wApp/logs/edge.log`.
//
// 🔴 Y POR QUÉ IMPORTA MÁS AQUÍ QUE EN NINGÚN OTRO SITIO. El Plan 046 va de PRIVACIDAD: el cliente marca una
// sesión como pasiva precisamente para que su tráfico no salga de su equipo. Un número de teléfono impreso en
// el log del filtro que lo protege sería el peor sitio posible para una fuga — y el log NO está cifrado con
// la DEK, a diferencia de la fila de la cola.
//
// ─────────────────────────────────────────────────────────────────────────────
// 📌 EL CRITERIO, ENMENDADO (decisión del 2026-08-21). Léelo antes de "arreglar" nada.
// ─────────────────────────────────────────────────────────────────────────────
// El enunciado original de T2.3 decía «en el log solo puede aparecer `session_id`», y ESE CRITERIO ERA
// INCUMPLIBLE tal cual: la línea Debug del corte lleva `message_id`, y en producción el logger de sesión
// arrastra el `jid` de la PROPIA sesión en todas sus líneas (sessionmgr/listen.go). Cumplirlo al pie de la
// letra habría exigido escribir las líneas del filtro con un logger distinto del de la sesión — es decir,
// perder la correlación por sesión justo en las líneas donde más falta hace.
//
// LO QUE INV-6 PROTEGE ES EL CONTENIDO Y LA IDENTIDAD DEL **REMITENTE**. Con eso, el criterio queda así:
//
//	SE ADMITEN → `session_id`; `message_id` (identificador OPACO, y es lo único que permite correlacionar
//	             un descarte concreto); el `jid` PROPIO de la sesión (el número del NEGOCIO, que ya aparece
//	             en cada línea del Edge desde el Plan 022 y que el cliente conoce porque es suyo).
//	NUNCA      → el número o el JID del REMITENTE, su `push_name`, ni una runa del texto.
//
// Y por eso este test construye el listener con el LOGGER REAL DE SESIÓN, con su atributo `jid` puesto: la
// versión anterior usaba un logger pelado y por tanto no reproducía la condición de producción — pasaba por
// casualidad, no por cumplimiento.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - añadir `"sender", e.Info.Sender.String()` o `"chat_jid", …` a cualquiera de las dos líneas;
//   - añadir `"push_name", e.Info.PushName` (es el nombre que el remitente eligió: PII de manual);
//   - loguear el texto, entero o truncado, "para diagnosticar";
//   - quitar el `.With("jid", …)` de `logCapturado` ⇒ no pone el test en rojo, pero lo devuelve al estado
//     inútil del que salió; por eso hay una aserción POSITIVA de que el `jid` propio está presente.
func TestOnMessage_SesionPasiva_ElLogNoLLeva_NiNumeroNiNombreNiTexto(t *testing.T) {
	l, _, _, _, buf := listenerPasivoConLog(t)

	l.handleEvent(context.Background(), mensajeDelRemitente("MSG-PII"))

	out := buf.String()
	if out == "" {
		t.Fatal("el camino del descarte pasivo no escribió NADA en el log: el test estaría buscando PII en un " +
			"buffer vacío y pasaría siempre. Revisa el nivel del logger (la línea de T2.2 va en Debug)")
	}
	// GUARDIÁN DEL ESCENARIO. Sin esto, alguien que "limpiara" `logCapturado` devolvería el test a buscar
	// fugas en un log más pobre que el de producción, y seguiría verde. La condición que se reproduce es
	// exactamente ésta: el logger de sesión arrastra el jid PROPIO en todas sus líneas.
	if !strings.Contains(out, jidPropioDeLaSesion) {
		t.Fatalf("el log capturado NO lleva el `jid` propio de la sesión (%q), así que este test no está\n"+
			"    reproduciendo la condición de producción: en campo TODAS las líneas del listener lo arrastran\n"+
			"    (sessionmgr/listen.go). Con un logger pelado, buscar PII aquí pasa por casualidad.\n"+
			"    LOG: %s", jidPropioDeLaSesion, out)
	}
	prohibido := []struct{ que, valor, porque string }{
		{"el NÚMERO del remitente", numeroRemitente,
			"es el dato que el cliente marca la sesión como pasiva para no exponer, y el log NO está cifrado " +
				"con la DEK (a diferencia de la fila de la cola)"},
		{"el JID del remitente", newJID(numeroRemitente, types.DefaultUserServer).String(),
			"es el número con dominio: la misma fuga con otra forma"},
		{"el PUSH NAME del remitente", nombreRemitente,
			"es el nombre que la persona eligió mostrar; identifica igual que el número"},
		{"el TEXTO del mensaje", textoRemitente,
			"INV-051.1 / ADR-0034 nivel 3: el contenido no sale por el log ni entero ni truncado, y este es " +
				"justo el mensaje que REQ-07 promete que no queda en ningún sitio"},
	}
	for _, p := range prohibido {
		if n := strings.Count(out, p.valor); n != 0 {
			t.Errorf("%s aparece %d veces en el log del filtro.\n    CONSECUENCIA: %s\n    LOG: %s",
				p.que, n, p.porque, out)
		}
	}

	// La otra mitad del criterio: la línea de resumen SÍ está. Un filtro que no dijera nada en Info sería
	// invisible en campo (el Edge corre a Info), que es el estado del que T2.3 saca al contador.
	if !strings.Contains(out, msgPasivaResumen) {
		t.Errorf("no salió la línea de resumen del filtro (%q).\n"+
			"    CONSECUENCIA: en campo el Edge corre a INFO, así que la línea por-mensaje de T2.2 (Debug) no se\n"+
			"    ve. Sin esta, el operador que mira el log no tiene forma de saber que una sesión está callada\n"+
			"    A PROPÓSITO.\n    LOG: %s", msgPasivaResumen, out)
	}
	if !strings.Contains(out, "descartados=1") {
		t.Errorf("la línea de resumen no lleva el acumulado (`descartados`): %s", out)
	}
}

// TestOnMessage_SesionPasiva_LaLineaDeResumen_VaThrottled fija que la línea de Info sale UNA VEZ y luego
// espera su ventana de enfriamiento.
//
// 🔴 POR QUÉ HAY THROTTLE Y NO UNA LÍNEA POR MENSAJE. Una sesión pasiva no descarta «de vez en cuando»:
// descarta TODO su tráfico, para siempre. Una línea por descarte en Info sería una línea por mensaje a ritmo
// de socket en el fichero que el operador lee con `grep … | tail -3`, y ahogaría el bloque del latido —que
// es el otro sitio donde este mismo contador se publica—. Ese fue exactamente el motivo por el que T2.2 puso
// su línea en Debug; el throttle es lo que permite tener las dos cosas.
//
// 🔴 Y POR QUÉ LA PRIMERA SALE EN EL ACTO: es la que dice «el filtro se ha activado en esta sesión», y esa
// información no admite cinco minutos de retraso cuando alguien está mirando por qué un número no responde.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - quitar el throttle (loguear siempre) ⇒ salen 3 líneas donde se esperan 2;
//   - arrancar `ultimoGritoPasivo` a `time.Now()` en vez de al cero ⇒ la PRIMERA línea no sale y el filtro
//     tarda cinco minutos en delatarse;
//   - mover la emisión DENTRO del candado ⇒ no lo caza este test, lo caza `-race` bajo carga (queda dicho).
func TestOnMessage_SesionPasiva_LaLineaDeResumenVaThrottled(t *testing.T) {
	l, _, _, _, buf := listenerPasivoConLog(t)
	ctx := context.Background()

	l.handleEvent(ctx, mensajeDelRemitente("MSG-T1"))
	l.handleEvent(ctx, mensajeDelRemitente("MSG-T2"))
	if n := strings.Count(buf.String(), msgPasivaResumen); n != 1 {
		t.Fatalf("líneas de resumen tras dos descartes seguidos = %d, want 1: la ventana de enfriamiento (%s) "+
			"no se está respetando y una sesión pasiva con tráfico escribiría una línea por mensaje",
			n, passiveDropLogCooldown)
	}

	// Se envejece el sello a mano en vez de dormir la ventana entera: es un test, no una espera de 5 minutos.
	// Se puede porque el observador es del mismo paquete; la alternativa (inyectar un reloj) añadiría una
	// costura de producción que solo existiría para esto.
	l.brackets.mu.Lock()
	l.brackets.ultimoGritoPasivo = time.Now().Add(-2 * passiveDropLogCooldown)
	l.brackets.mu.Unlock()

	l.handleEvent(ctx, mensajeDelRemitente("MSG-T3"))
	out := buf.String()
	if n := strings.Count(out, msgPasivaResumen); n != 2 {
		t.Errorf("líneas de resumen tras vencer el enfriamiento = %d, want 2: pasada la ventana la línea "+
			"tiene que volver a salir, o una sesión que lleva días callada deja de aparecer en el log", n)
	}
	// Y lleva el ACUMULADO, no el del intervalo: es lo que la hace comparable con `dropped_passive` de
	// /v1/health y con `descartes_perfil_pasivo` del latido, que también son acumulados del proceso.
	if !strings.Contains(out, "descartados=3") {
		t.Errorf("la segunda línea de resumen no lleva el acumulado de 3 descartes; publicar el delta la haría "+
			"incomparable con las otras dos vías, que son acumulados: %s", out)
	}
}
