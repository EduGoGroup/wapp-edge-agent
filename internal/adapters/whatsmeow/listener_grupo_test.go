package whatsmeow

// listener_grupo_test.go — EL FILTRO DE GRUPO DE LA PUERTA (Plan 044 · Ola 1.5 · T1.5-3, REQ-36/D-044.30).
//
// 🔴 QUÉ CONDUCTA SE DEROGÓ, porque sin esto los tests de abajo parecen decir lo contrario de lo que dicen
// los que había antes. Hasta T1.5-3 un entrante de GRUPO SÍ entraba: se encolaba, la fila nacía
// `clasificado` con la marca `no_elegible` y el despachador la subía a la nube sin intención. Se pagaba el
// INSERT cifrado, el disco, el cable y el almacenamiento en la nube por un mensaje que el Edge no atiende.
// REQ-36 lo cambia: el grupo se descarta EN LA PUERTA (paso 5 de onMessage), y no queda NADA — ni fila, ni
// entrega, ni un byte local.
//
// 🔴 LOS TRES DETALLES DE D-044.30 QUE ESTOS TESTS CUSTODIAN, y ninguno es opcional:
//
//  1. SE ACUSA SIEMPRE. Negar el acuse sería un reenvío ETERNO: venir de un grupo es una decisión
//     determinista sobre el JID, así que lo reofrecido llegaría idéntico y se volvería a descartar.
//  2. EL ORDEN DE LOS FILTROS ES POR COSTE CRECIENTE, y el paso 5 es la excepción consciente (ver el
//     docstring de onMessage): por coste puro iría antes de la ventana.
//  3. FAIL-OPEN ante cableado incompleto. Aquí no hay predicado que cablear, y el cero de `IsGroup` es
//     `false`: por omisión el mensaje SIGUE, nunca se corta de más.
//
// Y el detalle que no está en la lista pero es el que más caro sale si se rompe: el filtro tiene que
// dominar LOS DOS caminos que admiten, incluido el del entrante sin hora utilizable (`t="0"`). Ver
// TestOnMessage_Grupo_SinHoraUtilizable_TampocoDejaFila.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo, y la mutación COMPILA.

import (
	"context"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/latencia"
)

// listenerConGrupoMedido arma un listener con cola espía y con el instrumento del EDGE cableado, que es el
// que publica `descartes_grupo` en el bloque del latido. Devuelve los tres para poder afirmar la conducta
// y las DOS bocas del contador en el mismo test.
func listenerConGrupoMedido(t *testing.T) (*Listener, *spyCola, *latencia.Histograma) {
	t.Helper()
	h := latencia.Nuevo()
	cola := &spyCola{calls: &callLog{}}
	l := NewListener(quietLogger(), WithCola(cola), WithSessionID("sess-1"), WithLatencia(h))
	return l, cola, h
}

// TestOnMessage_Grupo_SeDescartaEnLaPuerta es EL test de T1.5-3, y afirma las tres mitades del criterio a
// la vez porque separarlas dejaría pasar el modo de fallo que importa: «descarta pero no acusa» (tormenta
// de reenvíos) y «descarta y no cuenta» (filtro invisible) son tan malos como «no descarta».
//
// El entrante DIRECTO va en el mismo test como testigo: sin él, un filtro que cortara TODO —no solo los
// grupos— pasaría en verde, y ese es el fallo caro en la otra dirección (el Edge se queda mudo).
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (compilan todas):
//   - borrar el bloque `if e.Info.IsGroup { … }` entero de onMessage ⇒ el grupo vuelve a dejar fila;
//   - `return true` → `return false` dentro de ese bloque ⇒ cae el acuse (y con él, en campo, WhatsApp
//     reofrece cada mensaje de cada grupo en cada reconexión, para siempre);
//   - quitar `b.puerta.AnotaDescarteGrupo()` de countGroupDrop ⇒ `descartes_grupo` sale 0 para siempre;
//   - cambiar la condición por `!e.Info.IsGroup` ⇒ el testigo del directo cae en el acto.
func TestOnMessage_Grupo_SeDescartaEnLaPuerta(t *testing.T) {
	l, cola, h := listenerConGrupoMedido(t)

	if !l.handleEvent(context.Background(), grupoMessage("MSG-GRUPO", "quiero dos empanadas")) {
		t.Error("el descarte de un entrante de GRUPO NEGÓ el acuse.\n" +
			"    CONSECUENCIA: venir de un grupo es una decisión DETERMINISTA sobre el JID, así que lo que\n" +
			"    WhatsApp reofrezca llegará idéntico y lo volveremos a descartar. Es un bucle a ritmo de\n" +
			"    reconexión sobre TODO el tráfico de TODOS los grupos (D-044.30, detalle 1).")
	}
	if len(cola.got) != 0 {
		t.Errorf("filas anotadas = %v; REQ-36 exige CERO.\n"+
			"    CONSECUENCIA: vuelve la conducta derogada — el grupo se guarda cifrado en disco y sube a la\n"+
			"    nube marcado `no_elegible`. Se paga INSERT, disco, cable y almacenamiento por un mensaje\n"+
			"    que el Edge no va a atender jamás.", colaWAIDs(cola.got))
	}
	if got := l.InboundStats().DroppedByGroup; got != 1 {
		t.Errorf("boca 1 (por sesión, InboundStats.DroppedByGroup) = %d, want 1", got)
	}
	if got := h.Puerta().Snapshot().DescartesGrupo; got != 1 {
		t.Errorf("boca 2 (acumulado del EDGE, el de `descartes_grupo` en el latido) = %d, want 1.\n"+
			"    CONSECUENCIA: el descarte no deja fila, no sube al cable y ACUSA igual que si hubiera\n"+
			"    entregado, así que sin este contador «el filtro está cortando» y «a esos grupos no les\n"+
			"    escribe nadie» son exactamente la misma línea de log.", got)
	}

	// EL TESTIGO: el entrante DIRECTO tiene que salir INTACTO del cambio.
	if !l.handleEvent(context.Background(), liveMessage("MSG-DIRECTO", "quiero dos empanadas")) {
		t.Fatal("el entrante directo perdió el acuse: el filtro de grupo está cortando lo que no es suyo")
	}
	if len(cola.got) != 1 {
		t.Fatalf("filas anotadas = %d, se esperaba 1: el entrante DIRECTO debe seguir encolando", len(cola.got))
	}
	if estado := cola.got[0].Estado; estado != app.EstadoNuevo {
		t.Errorf("estado = %q, quería %q: el directo con texto y el clasificador vivo sigue naciendo "+
			"reclamable por el cajero", estado, app.EstadoNuevo)
	}
	if got := l.InboundStats().DroppedByGroup; got != 1 {
		t.Errorf("DroppedByGroup = %d tras un entrante directo, quería seguir en 1: el contador está "+
			"midiendo algo que no es «venir de un grupo»", got)
	}
}

// TestOnMessage_Grupo_SinHoraUtilizable_TampocoDejaFila es el test del agujero que la colocación en el
// paso 5 abría, y por el que onMessage dejó de tener DOS `return l.enqueueCola(...)`.
//
// 🔴 EL AGUJERO, dicho entero: el camino del entrante SIN HORA UTILIZABLE (`t="0"`, atributo que whatsmeow
// admite sin error) ADMITE por precaución y encolaba ANTES de que la ventana llegara a evaluarse. Un filtro
// puesto DESPUÉS de la ventana no lo dominaba. Y con el caso `grupo` ya retirado de la puerta de
// elegibilidad, ese entrante no habría nacido `no_elegible` sino `nuevo`: tráfico de grupo reclamado por el
// cajero y mandado a Ollama — peor que la conducta que T1.5-3 vino a derogar. Es la MISMA lección que
// aprendió el filtro de perfil pasivo cuando el Plan 046 lo bajó del «paso 3.5» al 1.5.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (compilan las dos):
//   - devolver `return l.enqueueCola(ctx, e, time.Now().Unix())` dentro de la rama `Timestamp.IsZero()`
//     (en vez de fijar `tsCola` y seguir) ⇒ este test cae y el de arriba sigue VERDE;
//   - mover el bloque `if e.Info.IsGroup { … }` DENTRO de la rama `else` de la ventana, detrás de
//     `tsCola = e.Info.Timestamp.Unix()` —o sea, devolverlo a la colocación que tenía antes de T1.5-3—
//     ⇒ el filtro sigue siendo ALCANZABLE (nada de código muerto) y el grupo CON hora se sigue cortando,
//     así que el test de arriba queda VERDE; pero el entrante sin hora ya no pasa por esa rama y llega al
//     encolado, y este test cae. Es el agujero que se describe arriba, reproducido a propósito.
//
// ⛔ LA MUTACIÓN QUE NO VALE, y por qué se descartó: «mover ese bloque detrás del
// `return l.enqueueCola(ctx, e, tsCola)` final» COMPILA, pero deja código INALCANZABLE y `go vet` lo caza
// como *unreachable code*. La cazaría el gate de LINT antes de que llegara a correr un test, así que no
// demuestra que este fichero proteja nada: una mutación solo vale si pone el test en rojo por la vía del
// TEST.
func TestOnMessage_Grupo_SinHoraUtilizable_TampocoDejaFila(t *testing.T) {
	l, cola, _ := listenerConGrupoMedido(t)

	sinHora := grupoMessage("MSG-G-SINHORA", "quiero dos empanadas")
	sinHora.Info.Timestamp = time.Time{}

	if !l.handleEvent(context.Background(), sinHora) {
		t.Error("el grupo sin hora utilizable NEGÓ el acuse: el descarte sigue siendo determinista, " +
			"y el reenvío traería el mismo IsGroup")
	}
	if len(cola.got) != 0 {
		t.Errorf("filas anotadas = %v; REQ-36 dice CERO filas para un entrante de grupo, y no hace ninguna "+
			"excepción con el que llega sin hora.\n"+
			"    CONSECUENCIA: con el caso `grupo` ya retirado de la puerta de elegibilidad, esa fila nace\n"+
			"    `nuevo` — o sea que el cajero la reclama y la manda al LLM. Es PEOR que la conducta vieja,\n"+
			"    que al menos la marcaba `no_elegible` y no gastaba una plaza del semáforo.", colaWAIDs(cola.got))
	}
	if got := l.InboundStats().DroppedByGroup; got != 1 {
		t.Errorf("DroppedByGroup = %d, want 1: el descarte tiene que contarse venga con hora o sin ella", got)
	}
	if got := l.InboundStats().AdmittedNoTimestamp; got != 1 {
		t.Errorf("AdmittedNoTimestamp = %d, want 1: el filtro de grupo NO puede saltarse el paso 3 — ese "+
			"contador es el punto ciego del criterio de la ventana y tiene que seguir viéndolo todo", got)
	}
}

// TestOnMessage_Grupo_SinTexto_SeDescartaComoGrupo fija un SOLAPE QUE CAMBIÓ DE VEREDICTO, que es de las
// cosas que más despistan al leer la telemetría vieja.
//
// Una imagen recibida en un grupo la reclamaban DOS ramas de la puerta de elegibilidad (`sin_texto` y
// `no_elegible`), y ganaba `sin_texto` — así lo fijaba TestOnMessage_Cola_OrdenDeLosMotivos. Desde T1.5-3
// ese mensaje NO LLEGA a la puerta de elegibilidad: el filtro de grupo lo corta antes y se cuenta como
// descarte de GRUPO. Consecuencia para quien lee las series: `sin_texto` baja, y no porque haya menos
// tráfico no textual sino porque el de los grupos ya no se guarda.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (compila): mover el bloque `if e.Info.IsGroup { … }` de onMessage al
// switch de enqueueCola (es decir, deshacer la mudanza) ⇒ vuelve a haber fila y este test cae.
func TestOnMessage_Grupo_SinTexto_SeDescartaComoGrupo(t *testing.T) {
	l, cola, h := listenerConGrupoMedido(t)

	if !l.handleEvent(context.Background(), grupoSinTexto("MSG-G-IMG")) {
		t.Error("la imagen de grupo negó el acuse")
	}
	if len(cola.got) != 0 {
		t.Errorf("filas anotadas = %v; una imagen recibida EN UN GRUPO se descarta por ser de grupo, no se "+
			"guarda con la marca `sin_texto`", colaWAIDs(cola.got))
	}
	if got := h.Puerta().Snapshot().DescartesGrupo; got != 1 {
		t.Errorf("descartes_grupo = %d, want 1: el descarte se está imputando a otra serie", got)
	}
}

// TestOnMessage_Grupo_SinInstrumentoCableado_NoRompeNada — FAIL-OPEN del instrumento (D-044.30, detalle 3,
// aplicado a la medida): un contador es un instrumento y JAMÁS puede ser la causa de que se caiga la
// escucha. Sin `WithLatencia` el `*latencia.Puerta` es nil, y el corte tiene que seguir funcionando igual:
// sin pánico, sin fila y con acuse.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (compila): quitar la guarda `if p == nil { return }` de
// AnotaDescarteGrupo ⇒ pánico en el hilo de whatsmeow, que es la peor consecuencia posible de una métrica.
func TestOnMessage_Grupo_SinInstrumentoCableado_NoRompeNada(t *testing.T) {
	cola := &spyCola{calls: &callLog{}}
	l := listenerConCola(cola) // SIN WithLatencia: el instrumento del Edge no está cableado

	if !l.handleEvent(context.Background(), grupoMessage("MSG-G-SINHIST", "quiero dos empanadas")) {
		t.Error("sin instrumento cableado se perdió el acuse")
	}
	if len(cola.got) != 0 {
		t.Errorf("filas anotadas = %v; el corte no puede depender de que haya histograma", colaWAIDs(cola.got))
	}
	if got := l.InboundStats().DroppedByGroup; got != 1 {
		t.Errorf("DroppedByGroup = %d, want 1: el acumulado por sesión no depende del instrumento del Edge", got)
	}
}
