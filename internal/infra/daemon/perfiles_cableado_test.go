package daemon

// perfiles_cableado_test.go — EL PRIMER TRAMO DEL FILTRO DE PERFILES (Plan 046 · Ola 2 · T2.2).
//
// 🔴 QUÉ SE CUSTODIA AQUÍ. La cadena del consultor de perfiles tiene cinco tramos —
//
//	daemon.opcionPerfilesSesion → sessionmgr.WithSesionPasiva → Manager.sesionPasiva
//	    → gateway.SetSesionPasiva → whatsmeow.WithSesionPasiva → Listener.sesionPasiva
//
// — y este fichero mira el PRIMERO, que es el que ningún test podía mirar: la lista de opciones de `Run` no
// la ejercita nadie (sus únicos importadores son `cmd/agent`, cuyos tests no la llaman). Medido, no supuesto:
// borrar `sessionmgr.WithSesionPasiva(perfiles.PasivaFunc())` de esa lista dejaba `go build`, `go vet`,
// `go test ./... -p 1` y los cuatro gates COMPLETAMENTE VERDES, y en campo apagaba el filtro de privacidad en
// toda la flota — sin fila, sin log y sin contador, porque el corte por perfil pasivo es silencioso por
// diseño y acusa a WhatsApp igual que si hubiera entregado. Es exactamente el mismo agujero que destapó
// `buildLatencia`, y por eso la línea se extrajo a una función con el mismo molde.
//
// El de en medio (Manager → gateway) lo cierra `internal/app/sessionmgr/sesion_pasiva_cableado_test.go`; el
// último (gateway → Listener → corte), `internal/adapters/whatsmeow/listener_perfil_test.go`.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/wiring"
)

// consultorEnElManager devuelve la DIRECCIÓN DEL CÓDIGO del predicado que la opción dejó en un Manager REAL
// (0 si no dejó ninguno).
//
// Se construye un Manager de verdad y no se da por buena la opción, por el mismo motivo que en
// `TestBuildLatencia_*`: «devuelve una Option» no prueba que la Option guarde nada — `WithSesionPasiva`
// IGNORA los nil por diseño, así que una opción perfectamente formada puede no cablear absolutamente nada.
//
// El store nil no se usa en la construcción (NewManager solo le hace interface-upgrade) y el Layout necesita
// un directorio, no una sesión: aquí no se arranca ningún listener.
func consultorEnElManager(t *testing.T, opt sessionmgr.Option) uintptr {
	t.Helper()
	mgr := sessionmgr.NewManager(sessionmgr.NewLayout(t.TempDir()), nil, 1, logMudo(), opt)

	// Lectura por reflexión del campo NO EXPORTADO donde WithSesionPasiva deja el predicado. `Pointer()` sí
	// está permitido sobre un campo no exportado (a diferencia de `Interface()` o `Call()`).
	//
	// ⚠️ SI ESTE `Fatal` SALTA, no es este test el que está roto: alguien renombró `Manager.sesionPasiva` y
	// hay que actualizar el nombre aquí. Se prefiere ese rojo ruidoso a un test que deje de mirar en silencio.
	campo := reflect.ValueOf(mgr).Elem().FieldByName("sesionPasiva")
	if !campo.IsValid() {
		t.Fatal("sessionmgr.Manager ya no tiene el campo `sesionPasiva`: ¿se renombró? Este test mira ese campo " +
			"a propósito, porque es donde acaba el consultor que cada listener interroga en la puerta")
	}
	return campo.Pointer()
}

// TestOpcionPerfilesSesion_ElConsultorLLEGA_AlManager es EL test de este fichero: el predicado de la vista de
// perfiles que el daemon construye tiene que acabar dentro del Manager.
//
// Se afirma por IDENTIDAD DE CÓDIGO y no solo por «no es nil», y la diferencia importa: el modo de fallo que
// se busca no es únicamente el olvido, es el SUSTITUTO —un `WithSesionPasiva(func(string) bool { return
// false })— que dejaría el campo poblado, los gates en verde y el filtro apagado en toda la flota.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - borrar `opcionPerfilesSesion(perfiles)` de la lista de opciones de `Run` ⇒ no lo caza ESTE test (Run no
//     se ejercita), lo caza el hecho de que la función quede sin llamantes de producción y el linter `unused`
//     lo diga. Es la razón de extraerla: convierte un borrado invisible en un símbolo huérfano;
//   - devolver `sessionmgr.WithSesionPasiva(nil)` desde opcionPerfilesSesion ⇒ el campo queda a 0;
//   - devolver cualquier otro predicado ⇒ cae la comparación de punteros.
func TestOpcionPerfilesSesion_ElConsultorLlega_AlManager(t *testing.T) {
	// La vista REAL, construida como en `Run`. Sin `edgeconfig.Service` queda vacía y fail-open (avisa con un
	// Warn), que es justo lo que hace falta aquí: lo que se mide es el CABLE, no el contenido del mapa.
	perfiles := wiring.RegisterFilters(nil, logMudo())

	enManager := consultorEnElManager(t, opcionPerfilesSesion(perfiles))
	if enManager == 0 {
		t.Fatal("opcionPerfilesSesion NO cableó el consultor de perfiles en el Manager.\n" +
			"    CONSECUENCIA: ningún listener del Edge consulta el mapa. TODAS las sesiones que la nube marcó\n" +
			"    PASIVAS siguen encolando, persistiendo y entregando su tráfico entrante, y REQ-07 queda\n" +
			"    incumplido sin un solo síntoma — el corte no deja fila, no sube al cable y acusa a WhatsApp\n" +
			"    igual que si hubiera entregado, así que desde fuera «filtra» y «no filtra» son la misma foto.")
	}
	if quiero := reflect.ValueOf(perfiles.PasivaFunc()).Pointer(); enManager != quiero {
		t.Errorf("el Manager recibió un predicado DISTINTO del de la vista de perfiles (%#x != %#x): el mapa que "+
			"la nube mantiene al día y el que se pregunta en la puerta no son el mismo", enManager, quiero)
	}
}

// TestOpcionPerfilesSesion_SinVista_EsFailOpen fija la degradación honesta: sin vista de perfiles la opción
// tiene que dejar el Manager EXACTAMENTE como estaba antes del Plan 046 (nadie es pasiva), sin pánico.
//
// 🔴 LA ASIMETRÍA NO ES ESTÉTICA (D-046.2). De los dos fallos posibles, caer hacia «pasiva» deja al Edge
// SORDO —el cliente escribe y no pasa nada, sin un solo error en el log— mientras que caer hacia «activa»
// solo sube tráfico que la nube ya sabe ignorar (`reactiveBlocked`, D-046.7).
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: que `opcionPerfilesSesion` fabrique un predicado propio cuando `p` es nil
// (un `func(string) bool { return true }` «por si acaso»), o que entre en pánico al desreferenciarlo.
func TestOpcionPerfilesSesion_SinVista_EsFailOpen(t *testing.T) {
	if got := consultorEnElManager(t, opcionPerfilesSesion(nil)); got != 0 {
		t.Errorf("con la vista de perfiles a nil el Manager quedó con un predicado (%#x) que nadie construyó: "+
			"tiene que quedar vacío para que mande el default FAIL-OPEN del Listener", got)
	}
}

// TestCableado_FiltersVersion_SePasaElMetodoNoSuResultado cierra el ÚNICO tramo de la Ola 2 que quedó
// declarado como hueco a la vista en el propio código: la llamada `health.WithFiltersVersion(perfiles.Version)`
// vive dentro de `Run`, que ningún test ejercita, así que tocarla deja los cuatro gates en VERDE y solo se
// nota en campo — publicando una mentira plausible.
//
// 🔴 POR QUÉ SOBRE EL AST Y NO SOBRE LA CONDUCTA. `Run` monta medio wiring (socket, BD, Keychain): no se
// puede arrancar en un test unitario, y el colector se construye con siete argumentos que dependen de él, que
// es justo por lo que esta línea NO se extrajo a un helper como `opcionPerfilesSesion`. Pero el invariante
// tampoco es de conducta: es ESTRUCTURAL —«este argumento es el método, no su resultado»—, así que se
// comprueba donde vive, en la sintaxis. Emparenta con la regla de los cinco tramos de la cabecera.
//
// ⚠️ EL MODO DE FALLO QUE CAZA ES UNO SOLO, y conviene decirlo con precisión porque el comentario de
// daemon.go sugiere dos. Verificado POR MUTACIÓN el 2026-08-21:
//
//   - la llamada DESAPARECE de la lista ⇒ este test se pone ROJO. Es el modo real: `go build`, `go vet` y
//     `go test ./...` siguen VERDES, y en campo `filters_version` publica 0, que se lee como «este Edge no
//     tiene mapa». Nadie más lo nota.
//   - se pasa `perfiles.Version()` en vez del método ⇒ NO HACE FALTA ESTE TEST: no compila. `WithFiltersVersion`
//     recibe un `func() int64`, así que el sistema de tipos ya rechaza el resultado `int64`. El riesgo de
//     «congelar la versión del arranque» que teme daemon.go está cerrado por la FIRMA, no por vigilancia.
//     La comprobación del selector que hay abajo es, por tanto, cinturón sobre tirantes: se deja porque es
//     gratis y porque documenta la intención, no porque tape un hueco.
//
// Mutación que lo pone en rojo: borrar `health.WithFiltersVersion(perfiles.Version)` de la lista de opciones.
func TestCableado_FiltersVersion_SePasaElMetodoNoSuResultado(t *testing.T) {
	fset := token.NewFileSet()
	archivo, err := parser.ParseFile(fset, "daemon.go", nil, 0)
	if err != nil {
		t.Fatalf("parseando daemon.go: %v", err)
	}

	encontradas := 0
	ast.Inspect(archivo, func(n ast.Node) bool {
		llamada, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := llamada.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "WithFiltersVersion" {
			return true
		}
		encontradas++
		if len(llamada.Args) != 1 {
			t.Fatalf("health.WithFiltersVersion con %d argumentos, quiero 1", len(llamada.Args))
		}
		arg, ok := llamada.Args[0].(*ast.SelectorExpr)
		if !ok {
			t.Fatalf("el argumento de WithFiltersVersion es %T, y tiene que ser el SELECTOR `perfiles.Version` "+
				"—el método, SIN paréntesis—: pasar su resultado congela la versión del arranque para siempre",
				llamada.Args[0])
		}
		if arg.Sel.Name != "Version" {
			t.Fatalf("WithFiltersVersion recibe %q, quiero `Version`", arg.Sel.Name)
		}
		return true
	})

	if encontradas != 1 {
		t.Fatalf("health.WithFiltersVersion aparece %d veces en daemon.go, quiero exactamente 1. Si se borró, "+
			"`filters_version` publica 0 en toda la flota y ningún otro test lo nota", encontradas)
	}
}
