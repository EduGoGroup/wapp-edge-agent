package whatsmeow

// listen_gateway_cableado_test.go — EL ÚLTIMO TRAMO DEL CABLE DEL CRONÓMETRO (Plan 051 Ola 3 · T3.13).
//
// 🔴 EL AGUJERO QUE ESTE FICHERO CIERRA. listener_latencia_test.go prueba que un Listener CON
// WithLatencia mide todos sus caminos, y internal/app/sessionmgr/latencia_cableado_test.go prueba que el
// histograma llega hasta el ListenGateway. Entre esos dos hay un salto que no miraba nadie: el `append`
// que mete `WithLatencia(g.latencia)` en las opciones con que nace el Listener. Borrarlo dejaba `go
// build`, `go vet`, `go test ./... -p 1` y `make ci-docker` en VERDE, con el histograma perfectamente
// construido, perfectamente inyectado en el gateway… y un Listener que no lo recibe nunca. En campo eso
// es el bloque de latencia del latido con `n=0` para siempre y el criterio INV-051.2 («handler < 50 ms
// p99») otra vez sin instrumento.
//
// 🔴 Y NO ERA UN CABLE, ERAN TRES. Por el mismo sitio inauditable pasan los otros dos ensamblajes del
// plan, con el mismo modo de fallo silencioso y consecuencias PEORES que la del cronómetro:
//
//   - `WithCola` + `WithSessionID` (Ola 1): sin ellos LA COLA DEJA DE LLENARSE. El entrante no se
//     persiste, el cajero no tiene qué clasificar y el despachador no tiene qué entregar — el plan
//     entero se queda sin materia prima, y el Edge sigue conectado a WhatsApp como si nada.
//   - `WithClasificadorActivo` (Ola 2): sin él el interruptor no llega y manda el default ACTIVO, así que
//     con la feature APAGADA las filas nacen `nuevo`, el cajero las reclama, gasta su plaza del semáforo
//     y llama a Ollama para tráfico que este Edge no debía clasificar.
//
// Los tres se prueban igual: ejerciendo `listenerOpts()` y comprobando que lo que dejaron los setters
// LLEGA hasta la conducta del Listener.
//
// POR QUÉ SE PUDO PROBAR AHORA Y ANTES NO: el ensamblaje vivía dentro de serve(), que exige un
// *store.Device pareado y un socket vivo contra WhatsApp. Está extraído a `listenerOpts()` justamente
// para esto (ver su doc); el comportamiento es idéntico y el cable pasó de inauditable a ejercitable.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo, y todas se ejecutaron.

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/latencia"
)

// gatewayDePrueba construye un ListenGateway como el que arma el Manager, pero sin cargador de device: el
// cargador solo se usa dentro de Listen (que aquí no se llama), así que un nil basta para interrogar los
// setters y el ensamblaje de opciones.
func gatewayDePrueba() *ListenGateway {
	return newListenGateway(nil, quietLogger())
}

// TestListenerOpts_ElCronometroLLEGA_AlListenerQueNaceDeAqui es EL test de este fichero: el histograma que
// SetLatencia dejó en el gateway tiene que estar en las opciones con que nace su Listener, y se comprueba
// por CONDUCTA —se hace pasar un mensaje y se mira si el instrumento se llenó— y no por inspección de la
// lista de opciones: una opción presente pero inerte pasaría una comprobación estructural y seguiría sin
// medir nada.
//
// El Listener se construye EXACTAMENTE como lo construye serve() (`NewListener(g.log, g.listenerOpts()...)`),
// que es lo que hace que esto valga como prueba del cable y no como otro test del Listener.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - borrar `listenerOpts = append(listenerOpts, WithLatencia(g.latencia))` de listenerOpts() ⇒ el
//     Listener nace sin cronómetro y N = 0, con todos los gates en verde.
//   - `WithLatencia(latencia.Nuevo())` en ese append ⇒ el Listener mide sobre un instrumento gemelo que
//     nadie publica: el mismo agujero sin ningún nil que lo delate.
//   - vaciar el cuerpo de SetLatencia (o hacerlo escribir en otro campo) ⇒ el gateway nunca guarda nada.
func TestListenerOpts_ElCronometroLLEGA_AlListenerQueNaceDeAqui(t *testing.T) {
	h := latencia.Nuevo()
	cola := &spyCola{calls: &callLog{}}

	g := gatewayDePrueba()
	g.SetCola(cola, "sess-1")
	g.SetLatencia(h)

	l := NewListener(g.log, g.listenerOpts()...)
	l.handleEvent(context.Background(), liveMessage("MSG-CABLE", "quiero dos empanadas"))

	if got := h.Snapshot(latencia.Encolado).N; got != 1 {
		t.Errorf("serie ENCOLADO: N = %d, se esperaba 1.\n"+
			"    CONSECUENCIA: el cronómetro que el daemon construyó y el Manager inyectó NO llega al Listener,\n"+
			"    así que DEJA DE LLENARSE: el bloque de latencia del latido sale con n=0 y sin percentiles PARA\n"+
			"    SIEMPRE, con el Edge atendiendo mensajes con normalidad y los cuatro gates en verde. INV-051.2\n"+
			"    vuelve a ser incomprobable en campo.\n"+
			"    SI EL CAMBIO ES DELIBERADO (se retira el cronómetro): hay que retirar también el bloque de\n"+
			"    latencia del latido y el criterio que cuelga de él, no solo este append.", got)
	}
	// Testigo independiente de que el mensaje recorrió lo que el test cree: la cola sí recibió su fila, así
	// que un N=0 de arriba sería del cronómetro y no de un escenario mal montado.
	if len(cola.got) != 1 {
		t.Fatalf("filas anotadas = %d, se esperaba 1: el escenario no es el que el test cree (%v)",
			len(cola.got), colaWAIDs(cola.got))
	}
}

// TestListenerOpts_UnaSesionSINCola_TambienMide fija la decisión de que el cronómetro viaje FUERA del
// bloque de la cola, al contrario que el interruptor del clasificador. El motivo es de medida: una sesión
// sin cola cableada sigue gastando tiempo del hilo de whatsmeow en cada entrante, y ese tiempo cuenta para
// INV-051.2. Sin este test, «ordenar» el método metiendo el append dentro del `if` sería un cambio
// invisible que apagaría la medición justo en la sesión que peor está cableada.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: mover el `append` del cronómetro dentro del `if g.cola != nil && …`.
func TestListenerOpts_UnaSesionSinCola_TambienMide(t *testing.T) {
	h := latencia.Nuevo()

	g := gatewayDePrueba() // sin SetCola: la sesión queda SORDA (T3.0), pero el handler sigue corriendo
	g.SetLatencia(h)

	l := NewListener(g.log, g.listenerOpts()...)
	l.handleEvent(context.Background(), liveMessage("MSG-SIN-COLA", "quiero dos empanadas"))

	if got := h.Snapshot(latencia.Encolado).N; got != 1 {
		t.Errorf("serie ENCOLADO: N = %d, se esperaba 1: una sesión SIN cola gasta tiempo del hilo de "+
			"whatsmeow igual, y ese tiempo cuenta para INV-051.2. Si el cronómetro solo viaja cuando hay "+
			"cola, la sesión peor cableada es justo la que deja de medirse", got)
	}
}

// TestListenerOpts_LaColaYSuSessionID_LLEGAN_AlListener custodia el cable de la Ola 1, que es el que
// sostiene el plan entero: sin él no hay fila, y sin fila no hay cajero, ni despachador, ni entrega.
//
// Se afirman las DOS cosas que viajan juntas, y la segunda no es decorativa: el `session_id` es la clave
// con la que el adaptador de la cola elige la DEK con que sella la fila (por eso el Listener no lo puede
// deducir solo y hay que inyectárselo). Una fila con el session_id equivocado no es una fila mal
// etiquetada: es una fila que nadie puede descifrar.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - borrar `WithCola(g.cola)` del append de listenerOpts() ⇒ el Listener nace sordo y no anota nada.
//   - borrar `WithSessionID(g.sessionID)` ⇒ NewListener detecta el cableado a medias y DESACTIVA la cola
//     (ver listener.go), así que tampoco se anota: el mismo silencio por el otro extremo.
func TestListenerOpts_LaColaYSuSessionID_LLEGAN_AlListener(t *testing.T) {
	const sid = "sess-42"
	cola := &spyCola{calls: &callLog{}}

	g := gatewayDePrueba()
	g.SetCola(cola, sid)

	l := NewListener(g.log, g.listenerOpts()...)
	l.handleEvent(context.Background(), liveMessage("MSG-COLA", "quiero dos empanadas"))

	if len(cola.got) != 1 {
		t.Fatalf("filas anotadas = %d, se esperaba 1.\n"+
			"    CONSECUENCIA: LA COLA DEJA DE LLENARSE. El entrante no se persiste, el cajero no tiene qué\n"+
			"    clasificar y el despachador no tiene qué entregar — el Edge sigue conectado a WhatsApp y\n"+
			"    recibiendo mensajes, y no queda ni rastro de ellos. Los cuatro gates seguirían en VERDE.\n"+
			"    SI EL CAMBIO ES DELIBERADO: la sesión sin cola es un fallo de arranque desde T3.0, no un\n"+
			"    modo de funcionamiento; hay que tocar la política de arranque del daemon, no este append.",
			len(cola.got))
	}
	if got := cola.got[0].SessionID; got != sid {
		t.Errorf("la fila se anotó con session_id %q y se esperaba %q.\n"+
			"    CONSECUENCIA: el session_id es la clave con la que el adaptador elige la DEK que sella la\n"+
			"    fila. Con el valor equivocado la fila no queda mal etiquetada: queda ILEGIBLE para quien\n"+
			"    tenga que descifrarla después.", got, sid)
	}
}

// TestListenerOpts_LaColaNoViajaSinSessionID_PeroElSessionIDSiViajaSolo convierte en test la relación REAL
// entre las dos opciones, que NO es simétrica — y hasta el 2026-08-21 este fichero afirmaba que sí lo era.
//
// LA REGLA, en sus dos mitades:
//
//   - LA COLA NO VIAJA SIN session_id. Sigue igual y sigue siendo lo importante: el session_id es la clave
//     con la que el adaptador elige la DEK que sella la fila, así que una cola sin él no escribe filas mal
//     etiquetadas, escribe filas que NADIE PUEDE DESCIFRAR.
//   - EL session_id SÍ VIAJA SOLO. Esto es lo que cambió, y era un BUG con la forma de un contrato. El
//     session_id no es una opción «de la cola»: es la IDENTIDAD de la sesión, y tiene un segundo consumidor
//     que no depende de ella — `Listener.sesionEsPasiva()` (Plan 046), que corta en seco si el id está
//     vacío. Con la regla vieja, un listener sin cola perdía su identidad y el FILTRO DE PRIVACIDAD se
//     apagaba ENTERO, en silencio y sin contar un solo descarte, mientras el comentario de producción
//     afirmaba lo contrario en tres líneas seguidas. Hoy no muerde en campo (sin cola el daemon no arranca
//     desde el Plan 051 O3), pero una promesa escrita que el código no cumple es una trampa con fecha.
//
// 🔴 POR QUÉ SE MIRAN LAS OPCIONES Y NO LA CONDUCTA, que es lo contrario de lo que hace el resto del
// fichero. La defensa está DUPLICADA a propósito: el `if` de listenerOpts() impide que la cola salga sin
// identidad, y NewListener —si aun así le llegara rota— la desactiva y lo grita (listener.go). Las dos
// desembocan en la misma conducta observable («no se anota nada»), así que un test por conducta se
// quedaría VERDE al romper el `if`, tapado por la red de seguridad de abajo. Aplicando las opciones sobre
// un Listener DESNUDO se salta esa red y se interroga solo al punto de ensamblaje.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - relajar el guardián a `if g.cola != nil` ⇒ la cola viaja con el session_id vacío;
//   - devolver `WithSessionID(g.sessionID)` DENTRO del bloque de la cola —que es de donde salió— ⇒ cae la
//     segunda mitad, y con ella el filtro de perfiles de cualquier listener sin cola.
func TestListenerOpts_LaColaNoViajaSinSessionID_PeroElSessionIDSiViajaSolo(t *testing.T) {
	casos := []struct {
		nombre       string
		cola         app.ColaEntrantes
		sessionID    string
		quiereCola   bool
		quiereSesion string
	}{
		{"cola sin session_id: no viaja ninguna de las dos",
			&spyCola{calls: &callLog{}}, "", false, ""},
		{"session_id sin cola: la identidad viaja igual (el filtro de perfiles la necesita)",
			nil, "sess-huerfano", false, "sess-huerfano"},
		{"las dos presentes: el caso de campo",
			&spyCola{calls: &callLog{}}, "sess-42", true, "sess-42"},
	}

	for _, cs := range casos {
		t.Run(cs.nombre, func(t *testing.T) {
			g := gatewayDePrueba()
			// Se escriben los campos a mano: lo que aquí se interroga es el ENSAMBLAJE, que es la segunda
			// oportunidad de romper la pareja después de los setters.
			g.cola, g.sessionID = cs.cola, cs.sessionID

			// Listener DESNUDO: sin NewListener, para que su red de seguridad no tape el fallo.
			desnudo := &Listener{}
			for _, opt := range g.listenerOpts() {
				opt(desnudo)
			}

			if hayCola := desnudo.cola != nil; hayCola != cs.quiereCola {
				t.Errorf("cola cableada = %v, se esperaba %v.\n"+
					"    CONSECUENCIA (si viajó de más): la cola llega al Listener sin la clave que elige la DEK.\n"+
					"    Hoy lo rescata NewListener desactivándola con un log de Error, pero eso es una RED, no el\n"+
					"    contrato: si alguien la quita, la sesión escribe filas que nadie podrá descifrar.",
					hayCola, cs.quiereCola)
			}
			if desnudo.sessionID != cs.quiereSesion {
				t.Errorf("session_id cableado = %q, se esperaba %q.\n"+
					"    CONSECUENCIA (si falta): `Listener.sesionEsPasiva()` corta en seco con el id vacío, así\n"+
					"    que el consultor de perfiles llegaría perfectamente cableado y el filtro de privacidad\n"+
					"    estaría APAGADO igualmente — sin log, sin contador y sin un solo descarte anotado.",
					desnudo.sessionID, cs.quiereSesion)
			}
		})
	}
}

// TestListenerOpts_SinCola_ElFiltroDePerfilesSIGUE_CORTANDO es la mitad de CONDUCTA de lo anterior, y es la
// que de verdad importa: no basta con que la opción viaje, tiene que producir el corte.
//
// El escenario es el que la regla vieja rompía en silencio: un gateway con identidad y SIN cola. Antes, el
// `WithSessionID` se quedaba dentro del bloque de la cola, el Listener nacía sin saber quién era y
// `sesionEsPasiva()` devolvía false por su primera guarda — el filtro entero desactivado, con el predicado
// cableado y el mapa perfectamente aplicado.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: devolver `WithSessionID(g.sessionID)` al `if g.cola != nil && …` de
// listenerOpts() ⇒ `DroppedByPassiveProfile` se queda en 0 y el entrante se trata como el de una sesión
// activa. (Nótese que este test NO puede afirmar sobre filas: sin cola no hay dónde anotarlas. El contador
// es la única huella, que es exactamente el argumento de T2.3.)
func TestListenerOpts_SinCola_ElFiltroDePerfilesSigueCortando(t *testing.T) {
	const sid = "sess-sin-cola"

	g := gatewayDePrueba()
	// SetCola con la cola a nil: en producción es lo que hace el factory del sessionmgr cuando `m.cola` no
	// está inyectada. La identidad SÍ tiene que quedar guardada (ver SetCola).
	g.SetCola(nil, sid)
	g.SetSesionPasiva(pasiva(t, sid))

	l := NewListener(g.log, g.listenerOpts()...)
	if !l.handleEvent(context.Background(), liveMessage("MSG-SIN-COLA-PASIVA", "quiero dos empanadas")) {
		t.Fatal("el descarte por perfil pasivo debe ACUSAR también sin cola")
	}
	if s := l.InboundStats(); s.DroppedByPassiveProfile != 1 {
		t.Errorf("DroppedByPassiveProfile = %d, se esperaba 1.\n"+
			"    CONSECUENCIA: el listener sin cola perdió su identidad, así que `sesionEsPasiva()` cortó en su\n"+
			"    primera guarda (`l.sessionID == \"\"`) y el filtro de privacidad quedó APAGADO ENTERO — con el\n"+
			"    consultor cableado, el mapa aplicado y ni un solo síntoma.", s.DroppedByPassiveProfile)
	}
}

// TestListenerOpts_ElInterruptorDelClasificador_LLEGA_AlListener custodia el cable de la Ola 2 (T2.12): el
// predicado que decide si una fila NACE reclamable por el cajero o ya resuelta con la marca `apagado`.
//
// Se prueba por CONSECUENCIA —el estado y el motivo con que nace la fila— y no comprobando que el campo
// esté puesto: lo que importa no es que el predicado llegue, sino que llegue A TIEMPO de gobernar la
// puerta de elegibilidad, que es lo único que hace ese cable.
//
// Las dos mitades del test se necesitan mutuamente: la segunda fija que el default sin cablear es ACTIVO
// (la asimetría deliberada de la Ola 2 — clasificar de más antes que callar), y es lo que hace que la
// primera signifique algo. Sin ella, un `clasificadorActivo` que devolviera siempre false pasaría.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): borrar `WithClasificadorActivo(g.clasificadorActivo)` del
// bloque de la cola en listenerOpts().
func TestListenerOpts_ElInterruptorDelClasificador_LLEGA_AlListener(t *testing.T) {
	// (a) Clasificador APAGADO: la fila tiene que nacer YA RESUELTA con la marca `apagado`.
	apagada := &spyCola{calls: &callLog{}}
	g := gatewayDePrueba()
	g.SetCola(apagada, "sess-1")
	g.SetClasificadorActivo(func() bool { return false })

	l := NewListener(g.log, g.listenerOpts()...)
	l.handleEvent(context.Background(), liveMessage("MSG-APAGADO", "quiero dos empanadas"))

	if len(apagada.got) != 1 {
		t.Fatalf("filas anotadas = %d, se esperaba 1: el escenario no es el que el test cree", len(apagada.got))
	}
	if got := apagada.got[0].Estado; got != app.EstadoClasificado {
		t.Errorf("con el clasificador APAGADO la fila nació en estado %q y debía nacer %q.\n"+
			"    CONSECUENCIA: el interruptor no llega al Listener y manda su default ACTIVO, así que la fila\n"+
			"    nace reclamable: el cajero la toma, GASTA UNA PLAZA del semáforo de un hueco y llama a Ollama\n"+
			"    para tráfico que este Edge no debía clasificar. A ~45 msg/min por caja, esa plaza es el\n"+
			"    recurso escaso que el Plan 051 entero existe para gobernar.\n"+
			"    SI EL CAMBIO ES DELIBERADO: el filtro se puso en el listener porque aquí es GRATIS; moverlo\n"+
			"    al cajero es rehacer T2.12, no borrar un append.",
			got, app.EstadoClasificado)
	}
	if got := string(apagada.got[0].IntentJSON); got != string(app.SobreOmitido(app.MotivoApagado)) {
		t.Errorf("la fila nació con el sobre %q y se esperaba el de %q: el motivo es lo que permite\n"+
			"    distinguir «apagado» de «fastlane» en el desglose de INV-051.3, y con el motivo equivocado la\n"+
			"    telemetría explicaría mal lo que está pasando", got, app.MotivoApagado)
	}

	// (b) SIN cablear el interruptor: manda el default SEGURO (ACTIVO) y la fila nace reclamable. Es lo que
	// hace que (a) pruebe el cable y no una constante.
	viva := &spyCola{calls: &callLog{}}
	sinInterruptor := gatewayDePrueba()
	sinInterruptor.SetCola(viva, "sess-1")

	l2 := NewListener(sinInterruptor.log, sinInterruptor.listenerOpts()...)
	l2.handleEvent(context.Background(), liveMessage("MSG-DEFAULT", "quiero dos empanadas"))

	if len(viva.got) != 1 {
		t.Fatalf("filas anotadas = %d, se esperaba 1", len(viva.got))
	}
	if got := viva.got[0].Estado; got != app.EstadoNuevo {
		t.Errorf("sin interruptor cableado la fila nació %q y debía nacer %q: el default de la Ola 2 es "+
			"ACTIVO a propósito (clasificar de más antes que callar), y un cableado a medias no puede "+
			"apagar el clasificador en silencio", got, app.EstadoNuevo)
	}
}

// TestServeConstruyeSuListenerConListenerOpts cierra la vía de escape que abre la extracción: el test de
// arriba prueba que `listenerOpts()` cablea bien, pero no que serve() —el único llamante de producción—
// siga usándolo. Si alguien volviera a armar opciones a mano dentro de serve(), el cable real quedaría sin
// custodia otra vez y toda la conducta probada arriba pasaría a ser la de una función muerta.
//
// serve() no es ejercitable (necesita device pareado y socket vivo), así que lo que se comprueba es la
// FORMA del código, con el molde de internal/infra/db/cola_cableado_ast_test.go (T3.16) y su misma
// disciplina: fallar ruidosamente cuando el parseo deja de encontrar lo que buscaba, en vez de dejar de
// mirar en silencio.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas): devolver el ensamblaje inline a serve() (o pasarle a
// NewListener cualquier otra cosa que no salga de listenerOpts()).
func TestServeConstruyeSuListenerConListenerOpts(t *testing.T) {
	const (
		fichero     = "listen_gateway.go"
		metodo      = "serve"
		constructor = "NewListener"
		fuenteOpts  = "listenerOpts"
	)

	fset := token.NewFileSet()
	arbol, err := parser.ParseFile(fset, fichero, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("no se pudo parsear %s: %v (si el fichero se renombró, actualiza este test: es el que "+
			"vigila que serve() siga tomando sus opciones de %s())", fichero, err, fuenteOpts)
	}

	cuerpo := cuerpoDelMetodo(arbol, metodo)
	if cuerpo == nil {
		t.Fatalf("%s ya no tiene el método %s: ¿se renombró? Este test mira ahí a propósito, porque es el "+
			"único sitio de producción que construye el Listener de una sesión", fichero, metodo)
	}

	construcciones, bienCableadas := 0, 0
	ast.Inspect(cuerpo, func(n ast.Node) bool {
		llamada, ok := n.(*ast.CallExpr)
		if !ok || nombreDeLoLlamado(llamada.Fun) != constructor {
			return true
		}
		construcciones++
		for _, arg := range llamada.Args {
			if interior, ok := arg.(*ast.CallExpr); ok && nombreDeLoLlamado(interior.Fun) == fuenteOpts {
				bienCableadas++
			}
		}
		return true
	})

	if construcciones == 0 {
		t.Fatalf("%s() ya no construye ningún %s: ¿se movió la escucha a otro sitio? Ese sitio es el que "+
			"hay que custodiar ahora", metodo, constructor)
	}
	if bienCableadas != construcciones {
		t.Errorf("%s() construye %d %s y solo %d toma sus opciones de %s().\n"+
			"    CONSECUENCIA: las opciones del Listener volverían a armarse en un sitio que ningún test puede\n"+
			"    ejercitar (serve necesita device pareado y socket vivo), y perder ahí el `append` del\n"+
			"    cronómetro —o el de la cola— deja los cuatro gates en VERDE mientras en campo el bloque de\n"+
			"    latencia sale con n=0 para siempre.\n"+
			"    SI EL CAMBIO ES DELIBERADO: lo que hay que mover es la custodia, no borrarla — el ensamblaje\n"+
			"    de opciones tiene que vivir en un método interrogable desde un test.",
			metodo, construcciones, constructor, bienCableadas, fuenteOpts)
	}
}

// cuerpoDelMetodo devuelve el cuerpo del método (o función) con ese nombre, o nil si no está.
func cuerpoDelMetodo(arbol *ast.File, nombre string) *ast.BlockStmt {
	for _, decl := range arbol.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name != nil && fn.Name.Name == nombre {
			return fn.Body
		}
	}
	return nil
}

// nombreDeLoLlamado da el nombre de lo invocado SIN su receptor ni su paquete (`g.listenerOpts()` →
// "listenerOpts"): lo que se quiere identificar es la función, y el receptor solo añadiría formas que
// escribir en la comparación.
func nombreDeLoLlamado(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return ""
}
