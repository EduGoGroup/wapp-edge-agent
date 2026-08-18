package whatsmeow

// listen_gateway_margen_test.go — CUSTODIA DEL MARGEN DE LA VENTANA DE INGESTA (ADR-0037 · REQ-051.5).
//
// 🔴 EL AGUJERO QUE ESTE FICHERO CIERRA, y es el hermano del que cerró listen_gateway_cableado_test.go.
// `SetInboundMargin` deja el margen configurado en el gateway (WAPP_AGENT_INBOUND_MARGIN_SECONDS, que el
// Manager le pasa por sesión), pero hasta el 2026-08-18 ese valor se aplicaba al Listener DENTRO de
// serve(), que exige un *store.Device pareado y un socket vivo contra WhatsApp. Resultado: el cable no lo
// ejercitaba ningún test y borrarlo dejaba los cuatro gates en VERDE.
//
// Y lo que se pierde al borrarlo no es cosmético: el margen es lo ÚNICO que absorbe el desfase entre el
// reloj del SERVIDOR (Info.Timestamp) y el reloj LOCAL (el sello de conexión), y es la ventana de rescate
// de la microcaída — todo lo enviado en los 5 min anteriores a reconectar. Sin él llegando, un Edge con el
// reloj adelantado empieza a descartar tráfico VIVO como si fuera ráfaga vieja: el cliente escribe, el
// mensaje se descarta con un Warn y nadie relaciona el síntoma con una opción de configuración que sí está
// puesta en el `.env`, sí llega al gateway y simplemente no viaja el último metro.
//
// El margen se ensambla ahora en listenerOpts(), que es interrogable. Regla de la casa: cada test lleva
// escrita la mutación que lo pone en rojo, y todas se ejecutaron.

import (
	"context"
	"testing"
	"time"
)

// TestListenerOpts_ElMargenDeIngesta_LLEGA_AlListener custodia el cable entero: SetInboundMargin →
// g.inboundMargin → listenerOpts() → el Listener que nace de ahí.
//
// Se prueba por CONSECUENCIA —qué entra y qué no por la puerta— y no comprobando que el campo `margin`
// tenga el valor: lo que hay que garantizar no es que el número llegue, es que llegue A TIEMPO de gobernar
// la ventana, que es lo único para lo que existe.
//
// Las dos mitades se necesitan mutuamente. La segunda fija que SIN cablear el margen manda el default (5
// min) y el mismo mensaje CAE; sin ella, un `WithConnectMargin` que ignorara su argumento y pusiera un
// margen enorme pasaría la primera mitad tan campante.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - borrar `listenerOpts = append(listenerOpts, WithConnectMargin(g.inboundMargin))` de listenerOpts()
//     ⇒ manda el default y el entrante de 10 min antes se descarta;
//   - vaciar el cuerpo de SetInboundMargin (el gateway nunca guarda el valor) ⇒ lo mismo.
func TestListenerOpts_ElMargenDeIngesta_LLEGA_AlListener(t *testing.T) {
	seal := time.Now()
	// 10 min antes de que subiera el socket: FUERA del default (5 min) y DENTRO del margen configurado.
	// Es el caso de un Edge con el reloj desfasado, que es para lo que el margen es configurable.
	enviado := seal.Add(-10 * time.Minute)

	// (a) CON el margen configurado: el entrante entra y deja fila.
	amplia := &spyCola{calls: &callLog{}}
	g := gatewayDePrueba()
	g.SetCola(amplia, "sess-1")
	g.SetInboundMargin(30 * time.Minute)

	l := NewListener(g.log, g.listenerOpts()...)
	l.SetConnectSeal(func() time.Time { return seal })
	l.handleEvent(context.Background(), msgAt("MSG-MARGEN", enviado))

	if len(amplia.got) != 1 {
		t.Errorf("filas anotadas = %d, se esperaba 1.\n"+
			"    CONSECUENCIA: el margen configurado en el Edge NO llega al Listener y manda el default de 5\n"+
			"    min. En un equipo con el reloj adelantado —el fallo que este número existe para absorber, y\n"+
			"    el caro, porque Info.Timestamp es reloj del SERVIDOR y el sello es reloj LOCAL— el Edge\n"+
			"    empieza a DESCARTAR TRÁFICO VIVO como si fuera ráfaga vieja. El síntoma en campo es «el\n"+
			"    cliente escribe y no pasa nada» con la opción bien puesta en el .env y bien leída: nadie la\n"+
			"    va a mirar. Y los cuatro gates seguirían en VERDE.\n"+
			"    SI EL CAMBIO ES DELIBERADO: retirar el margen configurable es tocar el ADR-0037, no un append.",
			len(amplia.got))
	}

	// (b) SIN cablearlo: manda el default del Listener y el MISMO entrante cae. Es lo que hace que (a)
	// pruebe el cable y no una constante.
	estrecha := &spyCola{calls: &callLog{}}
	sinMargen := gatewayDePrueba()
	sinMargen.SetCola(estrecha, "sess-1")

	l2 := NewListener(sinMargen.log, sinMargen.listenerOpts()...)
	l2.SetConnectSeal(func() time.Time { return seal })
	l2.handleEvent(context.Background(), msgAt("MSG-DEFAULT", enviado))

	if len(estrecha.got) != 0 {
		t.Errorf("sin margen configurado el entrante de 10 min antes debía CAER (default 5 min) y dejó %d "+
			"fila(s): si entra igual, la mitad (a) de este test no está probando que el margen viaje — "+
			"estaría pasando con cualquier valor", len(estrecha.got))
	}
}

// TestListenerOpts_MargenNoConfigurado_NoAPLASTAElDefault custodia el guardián del valor no positivo, que
// desde el 2026-08-18 es load-bearing en producción: listenerOpts() añade WithConnectMargin SIEMPRE, así
// que en el caso normal —Edge sin la variable puesta— lo que viaja es un CERO, y lo único que impide que
// ese cero se coma el default de 5 min es el `if d > 0` de SetConnectMargin.
//
// 🔴 POR QUÉ ESTE TEST NO EXISTÍA ANTES Y AHORA ES OBLIGATORIO. Hasta ese día había DOS guardianes para lo
// mismo —el `if g.inboundMargin > 0` de serve() y el `if d > 0` de SetConnectMargin—, y con dos defensas
// que desembocan en la misma conducta ningún test podía custodiar ninguna: borrabas una y la otra tapaba
// el síntoma. Al mover el ensamblaje se RETIRÓ la de arriba en vez de moverla, y la que queda es
// interrogable. Esta es la conducta que protege: la ventana de rescate de la microcaída.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): quitar el `if d > 0` de Listener.SetConnectMargin ⇒ el
// margen queda en 0, el umbral pasa a ser el sello exacto y todo lo enviado ANTES de reconectar se
// descarta.
func TestListenerOpts_MargenNoConfigurado_NoAplastaElDefault(t *testing.T) {
	seal := time.Now()
	cola := &spyCola{calls: &callLog{}}

	g := gatewayDePrueba() // sin SetInboundMargin: g.inboundMargin es CERO
	g.SetCola(cola, "sess-1")

	l := NewListener(g.log, g.listenerOpts()...)
	l.SetConnectSeal(func() time.Time { return seal })
	// 30 s antes de reconectar: la microcaída típica (un suspender/reanudar, un wifi que parpadea).
	l.handleEvent(context.Background(), msgAt("MSG-MICROCAIDA", seal.Add(-30*time.Second)))

	if len(cola.got) != 1 {
		t.Errorf("filas anotadas = %d, se esperaba 1.\n"+
			"    CONSECUENCIA: un margen sin configurar (cero) ha ARRASADO el default de 5 min, así que la\n"+
			"    ventana de rescate de la microcaída desaparece: todo lo que el cliente escribió mientras el\n"+
			"    socket estaba abajo se descarta al reconectar. Y es el caso NORMAL —la mayoría de Edges no\n"+
			"    tocan WAPP_AGENT_INBOUND_MARGIN_SECONDS—, no un caso raro.\n"+
			"    SI EL CAMBIO ES DELIBERADO: el sitio donde se juzga el valor no positivo es\n"+
			"    Listener.SetConnectMargin, y es el ÚNICO que hay a propósito.", len(cola.got))
	}
	if got := l.margin; got != defaultConnectMargin {
		t.Errorf("margin = %s, quería el default %s: la conducta de arriba podría cumplirse por casualidad "+
			"con otro valor amplio, y lo que se defiende es el default concreto del ADR-0037", got, defaultConnectMargin)
	}
}
