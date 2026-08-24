package cajero

// clase_ast_test.go — `class` DESCRIBE, NO DECIDE (Plan 044 · Ola 1.7 · T1.7-3).
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 POR QUÉ ESTO ES UN TEST ESTRUCTURAL Y NO SEIS TESTS DE CONDUCTA
// ─────────────────────────────────────────────────────────────────────────────
// El invariante que hay que sostener es NEGATIVO y está repartido: «ninguna decisión de este paquete lee
// la clase». Escrito como conducta habría que enumerar las decisiones —el breaker abre igual con `lote`
// que con `interactivo`, el aforo no adelanta, el plazo no cambia, el umbral de lentitud no se mueve, el
// techo no se recorta…— y esa lista SE QUEDA CORTA SOLA: el día que alguien añada una séptima decisión y
// la haga mirar la clase, los seis tests siguen verdes. Un test sobre el AST no enumera decisiones,
// enumera POSICIONES SINTÁCTICAS LEGALES, y una decisión nueva que lea la clase cae fuera de todas ellas
// sin que nadie tenga que acordarse de nada. Es el mismo criterio con el que este repo custodia otros
// invariantes que se repiten en N sitios.
//
// ─────────────────────────────────────────────────────────────────────────────
// POR QUÉ IMPORTA, que no es purismo
// ─────────────────────────────────────────────────────────────────────────────
// Con `class` gobernando algo, el breaker acabaría teniendo DOS UMBRALES FIJOS en vez de uno — y seguiría
// contando como SANA una petición con `timeout_ms = 10 s` que tardó 9,9 s, que es exactamente el fallo
// que el breaker existe para detectar. El mecanismo bueno ya está construido y es otro: el umbral POR
// PETICIÓN, derivado del plazo de cada una (ADR-0042, T1.7-2 · registrarAcierto). Dejar entrar la clase
// en una decisión no añade una perilla: RESUCITA UN BUG YA ARREGLADO.
//
// ⚠️ ALCANCE: este fichero mira el paquete `cajero`, que es donde viven el breaker, el aforo, el plazo y
// el umbral — o sea, TODAS las decisiones. El adaptador de CloudLink y el del socket sólo COPIAN el campo
// de un struct a otro, y ahí no hay nada que decidir.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// nombresDeLaClase son los identificadores por los que la clase circula en este paquete: el campo del
// puerto (`p.Clase`) y la variable local que lo normaliza (`clase`).
//
// LOS DOS, Y NO SÓLO EL CAMPO: `clase := app.NormalizarClase(p.Clase)` es justo el punto donde el rastro
// del campo se pierde. Vigilando sólo `.Clase`, un `if clase == app.ClaseLote` tres líneas más abajo
// pasaría inadvertido — que es la forma MÁS PROBABLE de romper esto, no la menos.
var nombresDeLaClase = map[string]bool{"Clase": true, "clase": true}

// TestLaClaseNoGobiernaNingunaDecision recorre el AST del paquete y exige que cada aparición de la clase
// esté en una POSICIÓN QUE NO DECIDE.
//
// LA LISTA BLANCA ES CORTA A PROPÓSITO (tres posiciones), porque su valor está en lo que deja fuera:
//
//   - ARGUMENTO DE UNA LLAMADA — `log.Info("class", clase)`, `porClase.contar(clase)`,
//     `app.NormalizarClase(p.Clase)`. Es todo lo que una etiqueta necesita: viajar a un log o a un
//     contador.
//   - LADO DE UNA ASIGNACIÓN — `clase := app.NormalizarClase(p.Clase)`. Normalizar una vez y reusar.
//   - VALOR DE UN CAMPO EN UN LITERAL — copiar el campo de un struct a otro.
//
// Y lo que deja fuera es exactamente la forma de decidir: un `if`, un `switch`, un `case`, una
// comparación (`==`, `!=`), una negación, un índice de mapa. Cualquiera de esas hace fallar este test con
// el fichero y la línea.
func TestLaClaseNoGobiernaNingunaDecision(t *testing.T) {
	fset := token.NewFileSet()
	// Se parsea FICHERO A FICHERO y no con ParseDir (deprecada desde Go 1.25, y el linter la rechaza). El
	// directorio es "." porque `go test` corre con el CWD en el del paquete: no hay ruta que mantener
	// sincronizada con nada.
	fuentes := fuentesDeProduccion(t, ".")
	if len(fuentes) == 0 {
		t.Fatal("no se encontró ni un .go de producción en el paquete: este test no está mirando nada")
	}

	// PREMISA DEL TEST, Y NO ES CEREMONIA: si el campo dejara de leerse (un refactor que lo borra, un
	// fichero renombrado que el filtro ya no captura), el recorrido de abajo no encontraría nada y el test
	// pasaría en VERDE sin haber mirado nada. Es el modo de fallo clásico de un test estructural, y esta
	// cuenta es lo único que lo distingue de un verde honesto.
	var vistas int

	for _, ruta := range fuentes {
		fichero, err := parser.ParseFile(fset, ruta, nil, 0)
		if err != nil {
			t.Fatalf("parseando %s: %v", ruta, err)
		}
		padres := padresDe(fichero)
		ast.Inspect(fichero, func(n ast.Node) bool {
			if !esLaClase(n) {
				return true
			}
			vistas++
			padre := padres[n]
			if posicionQueNoDecide(padre, n) {
				return true
			}
			t.Errorf("%s: la CLASE aparece en una posición que DECIDE (%T), y `class` es SOLO telemetría.\n"+
				"    Posiciones legales: argumento de una llamada, lado de una asignación, o valor de un "+
				"campo en un literal.\n"+
				"    Si hace falta distinguir peticiones por su coste, el mecanismo es el umbral POR "+
				"PETICIÓN derivado del plazo (ADR-0042), no esta etiqueta.",
				fset.Position(n.Pos()), padre)
			return true
		})
	}

	if vistas == 0 {
		t.Fatal("el recorrido no encontró NI UNA aparición de la clase en el paquete: o el campo dejó de " +
			"usarse (y entonces `class` no llega ni al log ni al heartbeat, que es el otro fallo posible), " +
			"o este test ha dejado de mirar donde debía")
	}
}

// fuentesDeProduccion lista los `.go` del directorio EXCLUYENDO los `_test.go`. Un test SÍ puede comparar
// la clase —de hecho hace falta para probar que se normaliza— y meterlos en el recorrido convertiría este
// guardián en ruido perpetuo.
func fuentesDeProduccion(t *testing.T, dir string) []string {
	t.Helper()
	entradas, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("listando %s: %v", dir, err)
	}
	var out []string
	for _, e := range entradas {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		out = append(out, filepath.Join(dir, n))
	}
	return out
}

// esLaClase dice si el nodo es una lectura de la clase: el campo (`algo.Clase`) o la variable local.
func esLaClase(n ast.Node) bool {
	switch v := n.(type) {
	case *ast.SelectorExpr:
		return nombresDeLaClase[v.Sel.Name]
	case *ast.Ident:
		return v.Name == "clase"
	}
	return false
}

// posicionQueNoDecide aplica la lista blanca. El nodo hijo se pasa para poder exigir, en la llamada, que
// la clase sea un ARGUMENTO y no la función que se invoca: `clase(...)` sería una decisión disfrazada.
func posicionQueNoDecide(padre ast.Node, hijo ast.Node) bool {
	switch p := padre.(type) {
	case *ast.CallExpr:
		for _, arg := range p.Args {
			if arg == hijo {
				return true
			}
		}
		return false
	case *ast.AssignStmt:
		return true
	case *ast.KeyValueExpr:
		return true
	case *ast.SelectorExpr:
		// `p.Clase` visto como hijo de sí mismo al inspeccionar el Ident interno: el SelectorExpr ya se
		// juzga por su cuenta, así que aquí no hay nada que decidir.
		return true
	case *ast.Field, *ast.ValueSpec:
		// La DECLARACIÓN del campo o de una variable. No es una lectura.
		return true
	}
	return false
}

// padresDe construye el mapa hijo→padre del fichero. go/ast no lo trae, y sin él no se puede saber en qué
// posición sintáctica aparece un nodo — que es justo lo que este test juzga.
func padresDe(fichero *ast.File) map[ast.Node]ast.Node {
	padres := make(map[ast.Node]ast.Node)
	var pila []ast.Node
	ast.Inspect(fichero, func(n ast.Node) bool {
		if n == nil {
			if len(pila) > 0 {
				pila = pila[:len(pila)-1]
			}
			return true
		}
		if len(pila) > 0 {
			padres[n] = pila[len(pila)-1]
		}
		pila = append(pila, n)
		return true
	})
	return padres
}
