package db

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// cola_cableado_ast_test.go — EL GUARDARRAÍL DE QUE LA COLA SE ABRE POR DONDE DEBE (Plan 051 · T3.16).
//
// EL FALLO QUE ESTE FICHERO EXISTE PARA CAZAR. El perfil de escritura de la cola (colaTuning:
// synchronous=NORMAL + wal_autocheckpoint=4000) lo aplica OpenCola AL ABRIR, y los dos pragmas son
// POR-CONEXIÓN. La cola la abren DOS PROCESOS DISTINTOS —`agent serve` (internal/infra/daemon/daemon.go),
// que hace el Enqueue del handler, y `agent cajero` (cmd/agent/cajero.go), que hace los UPDATE de lote—,
// así que el perfil solo sirve si lo aplican LOS DOS: al que se quede en el perfil conservador no le
// afecta el del otro, seguirá haciendo fsync en cada commit y disparando checkpoints cada 4 MiB en mitad
// del tráfico ajeno. Ese es el mecanismo medido detrás de los picos de 250-471 ms en el p99 del handler
// (PC-11), y es exactamente lo que T3.15 fue a arreglar.
//
// POR QUÉ NO BASTABA EL TEST QUE YA HABÍA. db_tuning_test.go verifica los pragmas EFECTIVOS de una
// conexión abierta con OpenCola: es correcto y sigue haciendo falta, pero mira la PIEZA, no el CABLE. Con
// él en verde, revertir cualquiera de las dos líneas de producción a db.Open() dejaba la suite entera
// verde y el pragma sin aplicar en campo. Es el mismo patrón que T3.13 (el histograma bien construido que
// nadie vigilaba que siguiera enchufado).
//
// LA DEFENSA PRINCIPAL NO ES ESTE TEST, ES EL TIPO. db.ColaDBPath hace que `db.Open(ctx, d,
// layout.ColaDB())` NO COMPILE: el error ya no se puede teclear por descuido. Este fichero cubre lo que un
// tipo no puede impedir —las VÍAS DE ESCAPE, que existen y son escribibles—:
//
//	(B) pasar la ruta de la cola a un constructor que aplica el perfil conservador (incluida la variante
//	    `Open(ctx, d, string(layout.ColaDB()))`, que sí compila);
//	(C) convertir la ruta a string en cualquier punto de producción, que es el primer paso de (B) y el
//	    único motivo por el que alguien la escribiría;
//	(D) rearmar la ruta a mano con el literal "cola_entrantes.db" en otro sitio, esquivando el layout;
//	(E) fabricar un ColaDBPath fuera del layout, que es el otro extremo del mismo agujero.
//
// Y (A), la comprobación del título: que las DOS aperturas de producción sigan existiendo y sigan pasando
// por OpenCola con la ruta del layout. Borrar una de las dos líneas no es una vía de escape, es la
// mutación obvia, y es la que este test pone en rojo con nombre y apellidos.
//
// (Molde de la casa: internal/app/cola_enum_ast_test.go censa las constantes del enum parseando el
// fuente. Mismo motivo — probar la FORMA del código, no solo su conducta — y misma disciplina de fallar
// ruidosamente cuando el parseo deja de encontrar lo que buscaba.)

const (
	// raizDelRepo es la raíz del módulo desde el directorio de ESTE paquete (internal/infra/db): `go test`
	// sitúa el working directory en el del paquete.
	raizDelRepo = "../../.."
	// metodoDeLaRuta es el ÚNICO productor legítimo de la ruta de la cola (sessionmgr.Layout.ColaDB).
	metodoDeLaRuta = "ColaDB"
	// constructorDeLaCola es la única apertura que aplica colaTuning.
	constructorDeLaCola = "OpenCola"
	// tipoDeLaRuta es el tipo opaco de la ruta; convertir hacia él fuera del layout es fabricarla a mano.
	tipoDeLaRuta = "ColaDBPath"
	// ficheroDelLayout es el dueño del layout en disco: el único sitio donde pueden aparecer el nombre del
	// fichero de la cola y la conversión al tipo de su ruta.
	ficheroDelLayout = "internal/app/sessionmgr/layout.go"
	// nombreFicheroDeLaCola es el nombre del fichero en disco (constante colaDBName del layout).
	nombreFicheroDeLaCola = "cola_entrantes.db"
)

// llamantesDeProduccion son los ficheros que ABREN la cola en campo. Son dos porque son dos procesos, y
// esa es la razón de ser de todo esto: el perfil es por-conexión y no se hereda de un proceso a otro.
// Si algún día hay un tercero, va a esta lista (y el resto de reglas ya lo vigilan aunque se olvide).
var llamantesDeProduccion = []string{
	"internal/infra/daemon/daemon.go", // proceso `agent serve`: el Enqueue del handler
	"cmd/agent/cajero.go",             // proceso `agent cajero`: los UPDATE de cierre de lote
}

// aperturasQueNoAplicanElPerfil son los constructores que dejan la conexión en el perfil CONSERVADOR
// (defaultTuning) o sin pragmas ninguno. Se comparan solo por NOMBRE, sin el paquete, a propósito: así la
// lista cubre a la vez `Open` de este paquete, `db.Open` desde fuera y `sql.Open` pelado, que son las tres
// formas en que alguien acabaría abriendo la cola por el sitio equivocado.
var aperturasQueNoAplicanElPerfil = map[string]bool{
	"Open":               true,
	"OpenAndMigrate":     true,
	"OpenSessionStore":   true,
	"OpenAndMigrateMeta": true,
	"openSQLite":         true,
}

// TestColaSeAbreSiemprePorOpenCola es el guardarraíl: en TODO el fuente de PRODUCCIÓN, la BD de la cola
// solo se abre con OpenCola y su ruta solo la fabrica el layout — y los dos procesos que la abren en campo
// siguen haciéndolo.
func TestColaSeAbreSiemprePorOpenCola(t *testing.T) {
	fuentes := fuentesDeProduccion(t)

	// abrenLaCola registra qué ficheros contienen una apertura BIEN CABLEADA: OpenCola(..., X.ColaDB()).
	abrenLaCola := make(map[string]int)
	vioElLayout := false

	for _, f := range fuentes {
		if f.ruta == ficheroDelLayout {
			vioElLayout = true
		}

		ast.Inspect(f.arbol, func(n ast.Node) bool {
			switch nodo := n.(type) {
			case *ast.CallExpr:
				nombre := nombreDeLoLlamado(nodo.Fun)
				switch {
				// (A) La apertura buena. Cuenta como cableada solo si la ruta viene del layout: un
				// OpenCola con una ruta fabricada a mano no prueba que se abra EL fichero de la cola.
				case nombre == constructorDeLaCola:
					if algunArgUsaLaRuta(nodo.Args) {
						abrenLaCola[f.ruta]++
					}

				// (B) La apertura mala. Cubre `Open(ctx, d, layout.ColaDB())` —que hoy no compila, pero
				// dejaría de ser imposible si alguien quitara el tipo— y también la variante que SÍ
				// compila: `Open(ctx, d, string(layout.ColaDB()))`.
				case aperturasQueNoAplicanElPerfil[nombre] && algunArgUsaLaRuta(nodo.Args):
					t.Errorf("%s:%d — %s() recibe la ruta de la cola (%s()).\n"+
						"    CONSECUENCIA: esa conexión se queda en el perfil CONSERVADOR (synchronous=FULL, "+
						"wal_autocheckpoint=1000). Los pragmas son POR-CONEXIÓN, así que el perfil que aplique "+
						"el otro proceso NO le vale: seguirá haciendo fsync en cada commit y un checkpoint cada "+
						"4 MiB en mitad del tráfico del otro, que es la causa medida de los picos de 250-471 ms "+
						"en el p99 del handler (PC-11).\n"+
						"    ARREGLO: %s(ctx, layout.%s()).",
						f.ruta, f.linea(nodo.Pos()), nombre, metodoDeLaRuta, constructorDeLaCola, metodoDeLaRuta)

				// (C) La conversión a string: el primer paso de (B) y la única grieta que el tipo deja
				// abierta. En producción no hay ningún motivo legítimo para escribirla —formatear la ruta
				// en un fmt.Errorf o en un par clave/valor del logger NO necesita conversión—, así que se
				// prohíbe entera en vez de intentar adivinar a dónde va a parar el string resultante.
				case nombre == "string" && len(nodo.Args) == 1 && usaLaRuta(nodo.Args[0]):
					t.Errorf("%s:%d — se convierte a string la ruta de la cola (%s()).\n"+
						"    POR QUÉ ESTORBA: db.%s existe para que la ruta NO encaje en db.Open ni en ningún "+
						"otro constructor sin el perfil de la cola. Un string(...) desarma esa barrera y deja "+
						"la apertura equivocada a un paso, fuera del alcance del compilador.\n"+
						"    ARREGLO: pasa la ruta tal cual (%%s y el logger la formatean sin conversión); si "+
						"de verdad hace falta el string, di aquí por qué.",
						f.ruta, f.linea(nodo.Pos()), metodoDeLaRuta, tipoDeLaRuta)

				// (E) Fabricar la ruta fuera de su único productor. Es el otro extremo del mismo agujero:
				// con un ColaDBPath("...") a mano, OpenCola abriría un fichero que el layout no conoce.
				case nombre == tipoDeLaRuta && f.ruta != ficheroDelLayout:
					t.Errorf("%s:%d — se fabrica un db.%s fuera de %s.\n"+
						"    POR QUÉ IMPORTA: el layout es la ÚNICA fuente de verdad de las rutas en disco "+
						"(ADR-0016 §4) y el único productor legítimo de este tipo. Fabricarlo aquí es armar la "+
						"ruta a mano con otro nombre.\n"+
						"    ARREGLO: usa layout.%s().",
						f.ruta, f.linea(nodo.Pos()), tipoDeLaRuta, ficheroDelLayout, metodoDeLaRuta)
				}

			// (D) El nombre del fichero, rearmado a mano en otro sitio. Esquiva el layout entero y con él
			// todo lo anterior.
			case *ast.BasicLit:
				if nodo.Kind == token.STRING && f.ruta != ficheroDelLayout {
					if s, err := strconv.Unquote(nodo.Value); err == nil && s == nombreFicheroDeLaCola {
						t.Errorf("%s:%d — el literal %q aparece fuera de %s.\n"+
							"    POR QUÉ IMPORTA: rearmar la ruta a mano esquiva a la vez el layout y el tipo "+
							"que obliga a abrirla con %s.\n"+
							"    ARREGLO: usa layout.%s().",
							f.ruta, f.linea(nodo.Pos()), nombreFicheroDeLaCola, ficheroDelLayout,
							constructorDeLaCola, metodoDeLaRuta)
					}
				}
			}
			return true
		})
	}

	// (A) LA COMPROBACIÓN DEL TÍTULO: los dos procesos siguen abriendo la cola, y por el constructor bueno.
	for _, llamante := range llamantesDeProduccion {
		if abrenLaCola[llamante] == 0 {
			t.Errorf("%s ya NO abre la cola con %s(ctx, layout.%s()).\n"+
				"    CONSECUENCIA si se cambió por otro constructor: esa conexión pierde el perfil de "+
				"escritura de la cola (synchronous=NORMAL + WAL de 16 MiB) y, como los pragmas son "+
				"POR-CONEXIÓN, el perfil del OTRO proceso no la cubre: vuelve el fsync por commit y el "+
				"checkpoint en mitad de la ráfaga (PC-11, los picos de 250-471 ms del p99 del handler).\n"+
				"    CONSECUENCIA si la apertura se borró: ese proceso ya no tiene cola — el daemon perdería "+
				"cada entrante con el socket conectado, el cajero no tendría nada que reclamar.\n"+
				"    Si el cambio es deliberado (la cola se abre desde otro sitio), mueve el fichero en "+
				"`llamantesDeProduccion` — no borres la comprobación.",
				llamante, constructorDeLaCola, metodoDeLaRuta)
		}
	}

	// Las redes contra el falso verde: un test de esta clase se queda verde de la peor manera cuando deja
	// de mirar. Si el walk no parseó fuentes, o no vio el layout, nada de lo de arriba comprobó nada.
	if len(fuentes) == 0 {
		t.Fatalf("el barrido no encontró NINGÚN fuente de producción bajo %s: este guardarraíl no miró nada "+
			"(¿cambió el layout del repo, o la ruta relativa desde este paquete?)", raizDelRepo)
	}
	if !vioElLayout {
		t.Fatalf("el barrido no encontró %s, que es el único productor legítimo de la ruta: sin él las reglas "+
			"(D) y (E) no comprueban nada y su lista blanca apunta a un fichero que ya no existe "+
			"(¿se movió el layout?)", ficheroDelLayout)
	}
}

// fuenteParseada es un .go de producción ya parseado, con lo justo para señalar líneas en los fallos.
type fuenteParseada struct {
	// ruta es relativa a la raíz del repo y con separadores '/', para que el mensaje de fallo se pueda
	// pegar en un editor igual en cualquier SO.
	ruta  string
	arbol *ast.File
	fset  *token.FileSet
}

// linea traduce una posición del AST a número de línea.
func (f fuenteParseada) linea(p token.Pos) int { return f.fset.Position(p).Line }

// fuentesDeProduccion parsea todos los .go del repo EXCLUYENDO los _test.go: lo que se custodia aquí es
// cómo se abre la cola en CAMPO, y un test puede fabricar rutas y convertir tipos con toda legitimidad
// (lo hace, por ejemplo, db_tuning_test.go, que abre colas en t.TempDir()).
func fuentesDeProduccion(t *testing.T) []fuenteParseada {
	t.Helper()

	var fuentes []fuenteParseada
	fset := token.NewFileSet()

	err := filepath.WalkDir(raizDelRepo, func(ruta string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Los directorios ocultos (.git, .github) no llevan fuente del módulo y .git es enorme. La
			// comparación excluye la propia raíz del barrido: su nombre es ".." (raizDelRepo es relativo
			// desde este paquete) y saltarla dejaría el guardarraíl sin mirar nada.
			if ruta != raizDelRepo && strings.HasPrefix(d.Name(), ".") {
				return fs.SkipDir
			}
			return nil
		}
		nombre := d.Name()
		if !strings.HasSuffix(nombre, ".go") || strings.HasSuffix(nombre, "_test.go") {
			return nil
		}

		arbol, errParse := parser.ParseFile(fset, ruta, nil, 0)
		if errParse != nil {
			// Un fuente que no parsea no se puede auditar: se dice y se sigue, en vez de abortar el
			// barrido entero y dejar sin mirar todo lo demás.
			t.Errorf("no se pudo parsear %s: %v", ruta, errParse)
			return nil
		}
		rel, errRel := filepath.Rel(raizDelRepo, ruta)
		if errRel != nil {
			rel = ruta
		}
		fuentes = append(fuentes, fuenteParseada{ruta: filepath.ToSlash(rel), arbol: arbol, fset: fset})
		return nil
	})
	if err != nil {
		t.Fatalf("barrido de fuentes desde %s: %v", raizDelRepo, err)
	}
	return fuentes
}

// nombreDeLoLlamado devuelve el nombre de la función/conversión invocada, SIN el paquete: "OpenCola" tanto
// para `OpenCola(...)` como para `db.OpenCola(...)`, y "Open" tanto para `db.Open(...)` como para
// `sql.Open(...)`. Ignorar el paquete es deliberado (ver aperturasQueNoAplicanElPerfil): el alias del
// import cambia de fichero en fichero y lo que importa es la forma de la llamada. Devuelve "" para lo que
// no sea un identificador o un selector simple (una llamada a un campo de struct, por ejemplo).
func nombreDeLoLlamado(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	default:
		return ""
	}
}

// usaLaRuta indica si la expresión contiene, a cualquier profundidad, una llamada al método que produce la
// ruta de la cola (`layout.ColaDB()`). Mira EN PROFUNDIDAD para cazar los envoltorios: `string(l.ColaDB())`
// y `filepath.Join(l.ColaDB(), …)` cuentan igual que la llamada pelada.
func usaLaRuta(e ast.Expr) bool {
	encontrada := false
	ast.Inspect(e, func(n ast.Node) bool {
		if encontrada {
			return false
		}
		if llamada, ok := n.(*ast.CallExpr); ok && nombreDeLoLlamado(llamada.Fun) == metodoDeLaRuta {
			encontrada = true
			return false
		}
		return true
	})
	return encontrada
}

// algunArgUsaLaRuta indica si alguno de los argumentos de una llamada usa la ruta de la cola.
func algunArgUsaLaRuta(args []ast.Expr) bool {
	for _, a := range args {
		if usaLaRuta(a) {
			return true
		}
	}
	return false
}
