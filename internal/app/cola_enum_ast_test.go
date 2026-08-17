package app

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

// cola_enum_ast_test.go — EL GUARDARRAÍL DEL ENUM `MotivoOmitido` (Plan 051 Ola 3 · T3.9).
//
// EL FALLO QUE ESTE FICHERO EXISTE PARA CAZAR, y por qué el resto de tests no lo caza. `SobreOmitido` es
// un lookup de mapa (cola.go): un motivo que no esté en `motivosOmitido` devuelve "" EN SILENCIO. Los
// escritores del camino caliente —el listener (adapters/whatsmeow/listener.go) y el cajero
// (app/cajero/cajero.go)— lo llaman sin comprobar nada, así que ese "" se persistiría como `intent_json`
// NULL, y NULL en esa columna significa exactamente lo CONTRARIO del hecho registrado: «esta fila aún no
// pasó por el cajero» (INV-051.3). Peor todavía, una fila `clasificado` sin sobre tiene en disco la MISMA
// FORMA que un fragmento intermedio de lote, de modo que el despachador la entregaría contándola en
// `fragmentos_de_lote` —tráfico sano— en vez de en su motivo de degradación. El porqué se perdería sin
// rastro en la única serie que el operador mira.
//
// 🔴 NO SE VALIDA EN EL CAMINO CALIENTE, Y ES DELIBERADO. Ese camino corre UNA VEZ POR MENSAJE ENTRANTE y
// hoy todos sus llamantes pasan CONSTANTES LITERALES: no existe ninguna ruta viva por la que un motivo
// inválido llegue a disco. El riesgo no es de runtime, es de MANTENIMIENTO —alguien declara una constante
// `Motivo…` nueva y olvida añadirla al slice—, y un fallo de mantenimiento se caza en el gate, no pagando
// una comprobación por mensaje para siempre. (`DespacharSinIntent` sí valida, pero es el único: ahí el
// motivo entra por parámetro desde el despachador y devuelve ErrMotivoOmitidoDesconocido.)
//
// POR QUÉ SE PARSEA EL FUENTE Y NO SE ENUMERAN A MANO. Ya existe `TestMotivosOmitidoEsExhaustiva`
// (cola_test.go), y enumera las ocho constantes a mano precisamente para no ser una tautología. Pero esa
// lista a mano tiene el MISMO punto ciego que la canónica: quien declara la novena constante y olvida el
// slice, olvida también el test — y el test sigue verde con ocho de nueve. Go no permite enumerar por
// reflexión las constantes de un tipo (no sobreviven a la compilación), así que la única fuente que NO
// depende de que alguien se acuerde es el propio fichero .go. Este test lo lee con go/ast: declarar una
// constante de tipo MotivoOmitido fuera de `motivosOmitido` lo pone en ROJO sin tocar nada más.
//
// (Es el mismo molde que listener_camino_caliente_test.go, que interroga por reflexión los campos del
// Listener para que nadie le vuelva a colgar un sink: probar la FORMA del código, no solo su conducta.)

const (
	// ficheroDelEnum es el fuente que se parsea. Se lee por ruta RELATIVA porque `go test` sitúa el
	// working directory en el directorio del paquete; si alguien renombra el fichero, este test falla
	// ruidosamente en vez de quedarse verde sin mirar nada (ver la comprobación de `len(declaradas) == 0`).
	ficheroDelEnum = "cola.go"
	// tipoDelEnum es el tipo cuyas constantes hay que censar.
	tipoDelEnum = "MotivoOmitido"
	// listaCanonica es el nombre del slice que debe contenerlas todas.
	listaCanonica = "motivosOmitido"
)

// constanteDeclarada es una constante del enum tal y como aparece EN EL FUENTE (no en el binario).
type constanteDeclarada struct {
	// nombre es el identificador Go: "MotivoFastlane".
	nombre string
	// valor es el literal de cadena del que se inicializa ("fastlane"), o "" cuando el valor NO es un
	// literal. Hoy pasa exactamente una vez: `MotivoDesconocido = intents.ReservedUnknown`, que se
	// REFERENCIA a wapp-shared a propósito (ver su comentario en cola.go). Resolver ese valor exigiría
	// type-checking del paquete entero; no hace falta, porque la pertenencia a la lista se comprueba por
	// NOMBRE y el sobre no vacío se comprueba sobre los valores que devuelve MotivosOmitido() en runtime.
	valor string
	// linea es dónde se declaró, para que el mensaje de fallo apunte al sitio exacto.
	linea int
}

// TestEnumMotivoOmitidoNoTieneHuerfanas es el guardarraíl: TODA constante de tipo MotivoOmitido declarada
// en cola.go tiene que estar en el slice `motivosOmitido`, aparecer en MotivosOmitido() y tener un sobre
// no vacío. Añadir una constante y olvidar el slice pone esto en rojo.
func TestEnumMotivoOmitidoNoTieneHuerfanas(t *testing.T) {
	declaradas, enLaLista := leerEnumDelFuente(t)

	// (1) Ninguna constante huérfana: lo que se declara, se registra. ES LA COMPROBACIÓN DEL TÍTULO.
	for _, c := range declaradas {
		if !enLaLista[c.nombre] {
			t.Errorf("%s:%d — la constante %s es de tipo %s pero NO está en el slice `%s`.\n"+
				"    CONSECUENCIA: SobreOmitido(%s) devuelve \"\" en silencio y la fila se persiste con "+
				"`intent_json` NULL, que en disco significa lo contrario de lo que pasó («esta fila no pasó "+
				"por el cajero») y además la vuelve indistinguible de un fragmento intermedio de lote.\n"+
				"    ARREGLO: añade %s a `%s` en %s (y documenta el motivo, como los demás).",
				ficheroDelEnum, c.linea, c.nombre, tipoDelEnum, listaCanonica,
				c.nombre, c.nombre, listaCanonica, ficheroDelEnum)
		}
	}

	// (2) Ninguna entrada fantasma en la lista: todo lo que el slice nombra tiene que ser una constante del
	// enum. Hoy el compilador ya lo impediría casi siempre, pero no si alguien cambia el TIPO de una
	// constante a un string suelto: seguiría compilando dentro del slice y dejaría de contar como motivo.
	nombresDeclarados := make(map[string]bool, len(declaradas))
	for _, c := range declaradas {
		nombresDeclarados[c.nombre] = true
	}
	for nombre := range enLaLista {
		if !nombresDeclarados[nombre] {
			t.Errorf("el slice `%s` nombra a %s, que NO es una constante de tipo %s declarada en %s "+
				"(¿le cambiaron el tipo, o se declaró en otro fichero?)",
				listaCanonica, nombre, tipoDelEnum, ficheroDelEnum)
		}
	}

	// (3) El censo del fuente y el que ve el runtime tienen que cuadrar en número. Cierra el hueco de una
	// constante declarada FUERA de cola.go: no la vería el parseo, pero sí engordaría MotivosOmitido().
	enRuntime := MotivosOmitido()
	if len(enRuntime) != len(declaradas) {
		t.Errorf("MotivosOmitido() devuelve %d motivos y %s declara %d constantes de tipo %s: "+
			"o hay una constante del enum en OTRO fichero, o la lista canónica tiene un valor que no "+
			"corresponde a ninguna constante",
			len(enRuntime), ficheroDelEnum, len(declaradas), tipoDelEnum)
	}

	// (4) Todo motivo de la lista canónica tiene sobre. Es la promesa que el camino caliente NO comprueba,
	// así que si se rompe aquí no se rompe en ningún sitio hasta que aparezca un NULL en disco.
	for _, m := range enRuntime {
		if SobreOmitido(m) == "" {
			t.Errorf("el motivo %q está en la lista canónica pero SobreOmitido(%q) devuelve \"\" "+
				"(¿se rompió la precomputación de `sobreOmitido`?)", m, m)
		}
	}

	// (5) Y el puente entre ambos censos: el VALOR literal de cada constante declarada tiene que estar en
	// MotivosOmitido() con sobre propio. Es lo que ata el nombre que vio el parser con la cadena que se
	// persiste en `intent_json`; sin esto, (1) y (4) podrían pasar con el slice apuntando a otra cosa.
	valoresEnRuntime := make(map[MotivoOmitido]bool, len(enRuntime))
	for _, m := range enRuntime {
		valoresEnRuntime[m] = true
	}
	for _, c := range declaradas {
		if c.valor == "" {
			continue // valor no literal (MotivoDesconocido = intents.ReservedUnknown); ver el campo `valor`.
		}
		if !valoresEnRuntime[MotivoOmitido(c.valor)] {
			t.Errorf("%s:%d — %s vale %q, pero ese valor NO aparece en MotivosOmitido()",
				ficheroDelEnum, c.linea, c.nombre, c.valor)
		}
		if SobreOmitido(MotivoOmitido(c.valor)) == "" {
			t.Errorf("%s:%d — %s vale %q y NO tiene sobre. ARREGLO: añade %s a `%s` en %s.",
				ficheroDelEnum, c.linea, c.nombre, c.valor, c.nombre, listaCanonica, ficheroDelEnum)
		}
	}
}

// leerEnumDelFuente parsea cola.go y devuelve (a) todas las constantes declaradas de tipo MotivoOmitido y
// (b) el conjunto de NOMBRES que el slice `motivosOmitido` enumera. Falla el test si el fichero no se
// puede parsear o si alguna de las dos búsquedas vuelve vacía: un parseo que no encuentra nada dejaría el
// guardarraíl verde sin haber mirado, que es el peor desenlace posible para un test de esta clase.
func leerEnumDelFuente(t *testing.T) (declaradas []constanteDeclarada, enLaLista map[string]bool) {
	t.Helper()

	fset := token.NewFileSet()
	fichero, err := parser.ParseFile(fset, ficheroDelEnum, nil, 0)
	if err != nil {
		t.Fatalf("no se pudo parsear %s (¿se renombró el fichero?): %v", ficheroDelEnum, err)
	}
	enLaLista = make(map[string]bool)

	for _, decl := range fichero.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}

		switch gen.Tok {
		case token.CONST:
			// tipoVigente arrastra el tipo declarado hacia abajo dentro del MISMO bloque `const`: en Go, un
			// spec sin tipo ni valores hereda los del anterior (el molde de `iota`). Un spec CON valores pero
			// SIN tipo, en cambio, es una constante nueva sin tipo declarado, así que corta la herencia.
			tipoVigente := ""
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				switch {
				case vs.Type != nil:
					if ident, ok := vs.Type.(*ast.Ident); ok {
						tipoVigente = ident.Name
					} else {
						tipoVigente = ""
					}
				case len(vs.Values) > 0:
					tipoVigente = ""
				}
				if tipoVigente != tipoDelEnum {
					continue
				}
				for i, nombre := range vs.Names {
					if nombre.Name == "_" {
						continue
					}
					declaradas = append(declaradas, constanteDeclarada{
						nombre: nombre.Name,
						valor:  literalDeCadena(vs.Values, i),
						linea:  fset.Position(nombre.Pos()).Line,
					})
				}
			}

		case token.VAR:
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok || len(vs.Names) != 1 || vs.Names[0].Name != listaCanonica || len(vs.Values) != 1 {
					continue
				}
				lit, ok := vs.Values[0].(*ast.CompositeLit)
				if !ok {
					t.Fatalf("`%s` ya no se inicializa con un literal de slice: este test lee sus elementos "+
						"del fuente y hay que actualizarlo", listaCanonica)
				}
				for _, elemento := range lit.Elts {
					ident, ok := elemento.(*ast.Ident)
					if !ok {
						t.Fatalf("`%s` contiene un elemento que no es un identificador (%T): el test espera "+
							"que la lista canónica se escriba con las constantes del enum, no con literales",
							listaCanonica, elemento)
					}
					enLaLista[ident.Name] = true
				}
			}
		}
	}

	// Las dos redes contra el falso verde: si el parseo no vio constantes, o no vio la lista, el resto del
	// test compararía dos conjuntos vacíos y pasaría sin haber comprobado nada.
	if len(declaradas) == 0 {
		t.Fatalf("el parseo de %s no encontró NINGUNA constante de tipo %s: cambió la forma de la "+
			"declaración y este guardarraíl dejó de mirar (arréglalo, no lo borres)",
			ficheroDelEnum, tipoDelEnum)
	}
	if len(enLaLista) == 0 {
		t.Fatalf("el parseo de %s no encontró el slice `%s` (¿se renombró?): sin él este guardarraíl "+
			"no compara nada", ficheroDelEnum, listaCanonica)
	}
	return declaradas, enLaLista
}

// literalDeCadena devuelve el valor del i-ésimo inicializador si es un literal de cadena, o "" si no lo es
// (una referencia a otro paquete, una expresión, o un spec sin valores). Es la lectura DELIBERADAMENTE
// parcial que documenta constanteDeclarada.valor: lo que no sea literal se comprueba por nombre.
func literalDeCadena(valores []ast.Expr, i int) string {
	if i >= len(valores) {
		return ""
	}
	lit, ok := valores[i].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return ""
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return ""
	}
	return s
}
