package app

// inferencia_test.go — LOS DOS INVARIANTES DE `app/inferencia.go` QUE NO SE VEN DESDE FUERA
// (Plan 044 · Ola 1.6 · T1.6-2, ADR-0045 §2, REQ-34).
//
// ─────────────────────────────────────────────────────────────────────────────
// 1) `NormalizarFormato` — EL BUG QUE NO DA ERROR AQUÍ, SINO EN LA MÁQUINA DEL CLIENTE
// ─────────────────────────────────────────────────────────────────────────────
// El campo `format` del proveedor es un VALOR JSON CRUDO (ollama.ChatRequest.Format es un
// json.RawMessage y se serializa SIN comillas ni escapes). El contrato admite dos formas para ese campo
// —«"json" a secas, o un JSON Schema serializado»— y en el cable NO son la misma cosa: el schema ya es un
// valor JSON, la palabra `json` no lo es. Copiada verbatim produce `"format":json` en el cuerpo, sintaxis
// inválida, 400 del proveedor… y ese 400 se traduce a OLLAMA_DOWN, o sea que CULPARÍAMOS A LA MÁQUINA DEL
// CLIENTE de un error de serialización NUESTRO. El dueño del equipo miraría un Ollama perfectamente sano.
//
// 🔴 POR ESO EL TEST CENTRAL NO COMPARA CADENAS, SERIALIZA UN CUERPO DE VERDAD. Comprobar que
// NormalizarFormato("json") devuelve `"json"` es comprobar la letra; lo que hay que comprobar es la
// CONSECUENCIA: que un cuerpo que lleva ese valor como json.RawMessage se serializa y vuelve a parsear
// ENTERO. Es la única aserción que sigue mirando el día que alguien cambie la forma de citar.
//
// ─────────────────────────────────────────────────────────────────────────────
// 2) LA LISTA CANÓNICA DE LOS CINCO ERRORES (INV-051.3)
// ─────────────────────────────────────────────────────────────────────────────
// `ErroresInferencia` existe para que «los cinco» sea algo que se puede RECORRER y no una frase de un
// comentario: de ella cuelgan la reconstrucción del error desde su código (el socket del cajero) y la
// auditoría del switch que traduce a proto (en internal/adapters/cloudlink). Un error declarado y
// olvidado en la lista no rompe ninguna compilación: viaja por el socket como código desconocido y
// aterriza en un `false` que el llamante tiene prohibido tapar. Por eso hay aquí un guardarraíl de AST
// —mismo molde que cola_enum_ast_test.go— además de los tests de conducta.
//
// El paquete es `app` (no `app_test`), igual que el resto de tests de este directorio.

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// NormalizarFormato
// ─────────────────────────────────────────────────────────────────────────────

// cuerpoProveedor reproduce la FORMA REAL del cuerpo que sale hacia el proveedor: `format` es un VALOR
// JSON crudo (json.RawMessage), NO un string de Go. La diferencia es exactamente el bug: en un `string`
// el codificador pondría las comillas por su cuenta y no habría nada que normalizar; en un RawMessage lo
// que se escribe es lo que hay, y un valor mal formado se lleva por delante el cuerpo ENTERO — no sólo su
// campo. Ver internal/app/cajero/servidor.go, que construye este mismo cuerpo con `[]byte(f)`.
type cuerpoProveedor struct {
	Model  string          `json:"model"`
	Format json.RawMessage `json:"format,omitempty"`
}

// schemaDeEjemplo es un JSON Schema serializado como el que el Cloud manda cuando quiere forzar la forma
// de la salida. Empieza por '{', que es la marca por la que NormalizarFormato lo deja pasar verbatim.
const schemaDeEjemplo = `{"type":"object","properties":{"intent":{"type":"string"}},"required":["intent"]}`

// TestNormalizarFormato_ElCuerpoEnteroParseaConLasDosFormas es EL TEST de esta función: no mira cómo
// quedó el `format`, mira si el CUERPO que lo lleva dentro sobrevive. Cubre las dos formas del contrato
// (schema y palabra suelta) y los caracteres que romperían una concatenación de comillas a mano.
//
// SE PONE ROJO SI: NormalizarFormato vuelve al verbatim (json.Marshal rechaza un RawMessage inválido y el
// Marshal falla), si deja de escapar comillas/barras/control (el cuerpo deja de parsear o el valor vuelve
// distinto), o si empieza a «arreglar» el schema (el round-trip deja de coincidir).
func TestNormalizarFormato_ElCuerpoEnteroParseaConLasDosFormas(t *testing.T) {
	casos := []struct {
		nombre string
		format string
		// quiero es el valor JSON que el proveedor DEBE ver en el campo `format` una vez decodificado. Para
		// un escalar es la cadena original (citarla no puede cambiarla); para un schema, el objeto.
		quiero any
	}{
		{
			nombre: "la palabra json a secas — el caso común, y el que producía `\"format\":json`",
			format: "json",
			quiero: "json",
		},
		{
			nombre: "un JSON Schema serializado — la otra forma del contrato",
			format: schemaDeEjemplo,
			quiero: map[string]any{
				"type":       "object",
				"properties": map[string]any{"intent": map[string]any{"type": "string"}},
				"required":   []any{"intent"},
			},
		},
		{
			nombre: "un escalar cualquiera que no sea `json`",
			format: "text",
			quiero: "text",
		},
		{
			nombre: "un format con COMILLAS dentro — rompería una concatenación a mano",
			format: `el modelo dijo "hola"`,
			quiero: `el modelo dijo "hola"`,
		},
		{
			nombre: "un format con BARRA INVERTIDA dentro",
			format: `C:\ruta\formato`,
			quiero: `C:\ruta\formato`,
		},
		{
			nombre: "un format con salto de línea y un byte de control",
			format: "linea1\nlinea2\ttab\x01fin",
			quiero: "linea1\nlinea2\ttab\x01fin",
		},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			normalizado := NormalizarFormato(c.format)
			if normalizado == "" {
				t.Fatalf("NormalizarFormato(%q) devolvió vacío: sólo el format VACÍO puede normalizar a vacío "+
					"(vacío aguas abajo significa «sin restricción de formato», que NO es lo que el Cloud pidió)", c.format)
			}

			// EL CUERPO DE VERDAD. json.Marshal valida el RawMessage antes de escribirlo, así que un valor
			// mal formado NO produce un cuerpo raro: produce un error aquí mismo, que es justo el fallo que
			// en producción se manifestaría como un 400 del proveedor traducido a OLLAMA_DOWN.
			cuerpo, err := json.Marshal(cuerpoProveedor{Model: "qwen3:1.7b", Format: json.RawMessage(normalizado)})
			if err != nil {
				t.Fatalf("el cuerpo NO se pudo serializar con format=%q normalizado a %q: %v\n"+
					"    ESO ES EL BUG QUE ESTA FUNCIÓN EXISTE PARA NO TENER: el proveedor responde 400, el 400 "+
					"se traduce a OLLAMA_DOWN y el dueño del equipo mira un Ollama sano.", c.format, normalizado, err)
			}

			// Y el cuerpo entero vuelve a parsear (no sólo su campo).
			var vuelta map[string]json.RawMessage
			if err := json.Unmarshal(cuerpo, &vuelta); err != nil {
				t.Fatalf("el cuerpo serializado NO parsea: %v\n    cuerpo=%s", err, cuerpo)
			}
			bruto, ok := vuelta["format"]
			if !ok {
				t.Fatalf("el cuerpo perdió el campo `format` por el camino: %s", cuerpo)
			}

			// Round-trip del VALOR: lo que el proveedor lee tiene que ser lo que el Cloud pidió, ni
			// recortado ni reinterpretado.
			var valor any
			if err := json.Unmarshal(bruto, &valor); err != nil {
				t.Fatalf("el valor de `format` no parsea por separado: %v (bruto=%s)", err, bruto)
			}
			if !reflect.DeepEqual(valor, c.quiero) {
				t.Errorf("el proveedor vería format=%#v, y el Cloud pidió %#v (normalizado=%q)", valor, c.quiero, normalizado)
			}
		})
	}
}

// TestNormalizarFormato_SchemaVerbatim_PalabraCitada_VacioVacio ancla las TRES decisiones de forma, que el
// test del cuerpo no distingue: un schema sale IDÉNTICO byte a byte (no se reformatea ni se valida —
// validarlo sería interpretarlo, ADR-0045 §1), una palabra sale CITADA, y el vacío se queda vacío
// («sin restricción de formato»; el campo se omite aguas abajo por el `omitempty`).
func TestNormalizarFormato_SchemaVerbatim_PalabraCitada_VacioVacio(t *testing.T) {
	if got := NormalizarFormato(schemaDeEjemplo); got != schemaDeEjemplo {
		t.Errorf("un JSON Schema debe salir VERBATIM.\n  got:  %s\n  want: %s", got, schemaDeEjemplo)
	}

	// El caso que da nombre a la función. Se comprueban las dos mitades por separado para que el mensaje de
	// fallo diga cuál se rompió: que ya no vale el verbatim, y que el citado es el correcto.
	got := NormalizarFormato("json")
	if got == "json" {
		t.Fatalf("NormalizarFormato(\"json\") devolvió la palabra SIN CITAR: eso produce `\"format\":json` " +
			"en el cuerpo (sintaxis inválida), 400 del proveedor y OLLAMA_DOWN sobre una máquina sana")
	}
	if got != `"json"` {
		t.Errorf("NormalizarFormato(\"json\") = %q, want %q", got, `"json"`)
	}

	if got := NormalizarFormato(""); got != "" {
		t.Errorf("NormalizarFormato(\"\") = %q, want \"\" (vacío = sin restricción de formato)", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// La lista canónica de los cinco errores
// ─────────────────────────────────────────────────────────────────────────────

// TestErroresInferencia_CodigosNoVaciosYDistintos: el código es la LLAVE con la que el error cruza el
// socket del cajero y con la que el carril de CloudLink lo traduce al enum. Dos errores con el mismo
// código harían que el cliente reconstruyera el equivocado —y el diagnóstico saldría invertido sin que
// nada fallara—; un código vacío haría irreconstruible al suyo.
func TestErroresInferencia_CodigosNoVaciosYDistintos(t *testing.T) {
	if len(ErroresInferencia) == 0 {
		t.Fatalf("ErroresInferencia está VACÍA: el resto de este fichero recorrería la nada y saldría verde")
	}
	vistos := make(map[string]*ErrorInferencia, len(ErroresInferencia))
	for _, e := range ErroresInferencia {
		if e == nil {
			t.Fatalf("ErroresInferencia contiene un nil")
		}
		if e.Codigo() == "" {
			t.Errorf("el error %q no tiene código: no se puede reconstruir al otro lado del socket", e.Error())
			continue
		}
		if e.Error() == "" {
			t.Errorf("el error de código %q no tiene razón: los logs del camino lo escriben tal cual", e.Codigo())
		}
		if otro, repetido := vistos[e.Codigo()]; repetido {
			t.Errorf("código DUPLICADO %q: lo comparten %q y %q, así que ErrorInferenciaDe devolvería "+
				"siempre el primero y el diagnóstico saldría cambiado", e.Codigo(), otro.Error(), e.Error())
		}
		vistos[e.Codigo()] = e
	}
}

// TestErrorInferenciaDe_ResuelveLosCanonicosYRechazaLoDesconocido: la resolución código→error tiene que
// cubrir la lista entera (si no, un desenlace real del cajero llegaría al daemon como «desconocido») y
// tiene que decir NO —no «algo parecido»— ante un código que no reconoce. Ese `false` significa que los
// dos extremos del socket llevan binarios de distinta versión, y taparlo con un OLLAMA_DOWN convertiría
// un problema de despliegue en un diagnóstico falso sobre la máquina del cliente.
func TestErrorInferenciaDe_ResuelveLosCanonicosYRechazaLoDesconocido(t *testing.T) {
	for _, e := range ErroresInferencia {
		got, ok := ErrorInferenciaDe(e.Codigo())
		if !ok {
			t.Errorf("ErrorInferenciaDe(%q) devolvió ok=false para un error de la LISTA CANÓNICA", e.Codigo())
			continue
		}
		if got != e {
			t.Errorf("ErrorInferenciaDe(%q) devolvió %q, want %q (la resolución cruzó dos entradas)",
				e.Codigo(), got.Error(), e.Error())
		}
	}

	for _, desconocido := range []string{"", "ollama_dowm", "OLLAMA_DOWN", "sexto_error_del_futuro"} {
		if got, ok := ErrorInferenciaDe(desconocido); ok {
			t.Errorf("ErrorInferenciaDe(%q) devolvió ok=true (%q): un código que no está en la lista tiene "+
				"que fallar, no elegir el parecido", desconocido, got.Error())
		}
	}
}

// TestEsErrorInferencia_LoExtraeAtravesDeUnWrap: los llamantes NO devuelven el error canónico pelado —lo
// enriquecen por el camino (`errors.Join`, `fmt.Errorf("%w")`) para que el log diga qué se estaba
// haciendo—. Si la extracción sólo funcionara con el error desnudo, el carril de CloudLink caería en su
// rama de «error fuera del vocabulario» y respondería OLLAMA_DOWN a un timeout o a un breaker abierto.
func TestEsErrorInferencia_LoExtraeAtravesDeUnWrap(t *testing.T) {
	for _, e := range ErroresInferencia {
		envuelto := fmt.Errorf("cajero: sirviendo la inferencia cmd-42: %w", e)

		got, ok := EsErrorInferencia(envuelto)
		if !ok {
			t.Errorf("EsErrorInferencia no encontró %q bajo un fmt.Errorf(\"%%w\")", e.Codigo())
			continue
		}
		if got != e {
			t.Errorf("EsErrorInferencia devolvió %q envolviendo %q", got.Codigo(), e.Codigo())
		}
		// Y el mismo hecho por la puerta estándar: `errors.Is` tiene que verlo igual, porque hay llamantes
		// (los del puerto) que comparan así en vez de extraer.
		if !errors.Is(envuelto, e) {
			t.Errorf("errors.Is no reconoce %q bajo el wrap", e.Codigo())
		}
	}

	if got, ok := EsErrorInferencia(errors.New("un error cualquiera del transporte")); ok {
		t.Errorf("EsErrorInferencia inventó un canónico (%q) para un error ajeno al vocabulario", got.Codigo())
	}
	if got, ok := EsErrorInferencia(nil); ok {
		t.Errorf("EsErrorInferencia(nil) devolvió %q", got.Codigo())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// El guardarraíl de AST: ningún error declarado puede quedarse fuera de la lista
// ─────────────────────────────────────────────────────────────────────────────

const (
	// ficheroDeLosErrores es el fuente que se parsea. Ruta RELATIVA porque `go test` sitúa el working
	// directory en el directorio del paquete; si alguien lo renombra, este test falla RUIDOSAMENTE en vez
	// de quedarse verde sin mirar nada (ver las dos redes contra el falso verde al final del helper).
	ficheroDeLosErrores = "inferencia.go"
	// tipoDelError es el tipo cuyos valores hay que censar.
	tipoDelError = "ErrorInferencia"
	// listaDeErrores es el nombre del slice que debe contenerlos todos.
	listaDeErrores = "ErroresInferencia"
)

// errorDeclarado es un error canónico tal y como aparece EN EL FUENTE (no en el binario).
type errorDeclarado struct {
	nombre string // identificador Go: "ErrInferenciaTimeout"
	codigo string // el literal del campo `codigo:` ("timeout"), o "" si no es un literal de cadena
	linea  int    // dónde se declaró, para que el fallo apunte al sitio exacto
}

// TestErroresInferencia_NoTieneHuerfanos es el guardarraíl que hace verdadero el comentario de
// inferencia.go («añadir un error nuevo va al final de esta lista, del enum y del switch; si falta
// cualquiera de los tres, el test estructural lo caza»). Aquí se cubre el PRIMERO de los tres.
//
// POR QUÉ SE PARSEA EL FUENTE Y NO SE ENUMERAN A MANO. Enumerar los cinco en el test tiene el MISMO punto
// ciego que la lista canónica: quien declare el sexto y olvide el slice, olvidará también el test, y el
// test seguirá verde con cinco de seis. Go no permite enumerar por reflexión los `var` de un paquete, así
// que la única fuente que NO depende de que alguien se acuerde es el propio .go.
//
// CONSECUENCIA DE UN HUÉRFANO, para que el fallo se entienda sin releer el diseño: el cajero devuelve ese
// error, lo escribe en el cuerpo por su `codigo`, y `ErrorInferenciaDe` del otro lado del socket responde
// `false` — el daemon lo trata como desalineación de binarios, y un desenlace REAL de la inferencia se
// contabiliza como un fallo de despliegue.
func TestErroresInferencia_NoTieneHuerfanos(t *testing.T) {
	declarados, enLaLista := leerErroresDelFuente(t)

	// (1) Lo que se declara, se registra. ES LA COMPROBACIÓN DEL TÍTULO.
	nombresDeclarados := make(map[string]bool, len(declarados))
	for _, d := range declarados {
		nombresDeclarados[d.nombre] = true
		if !enLaLista[d.nombre] {
			t.Errorf("%s:%d — %s es un *%s declarado pero NO está en `%s`.\n"+
				"    CONSECUENCIA: viaja por el socket del cajero como un código que `ErrorInferenciaDe` no "+
				"resuelve, y el daemon lo lee como binarios desalineados en vez de como el desenlace que es.\n"+
				"    ARREGLO: añádelo a `%s` (y al enum del .proto y al switch de aProtoInferenceError).",
				ficheroDeLosErrores, d.linea, d.nombre, tipoDelError, listaDeErrores, listaDeErrores)
		}
	}

	// (2) Ninguna entrada fantasma: todo lo que el slice nombra tiene que ser uno de los errores declarados
	// en este fichero. Hoy el compilador lo impediría casi siempre, pero no si alguien declara el error en
	// OTRO fichero del paquete — donde el parseo no lo vería y el guardarraíl dejaría de cubrirlo.
	for nombre := range enLaLista {
		if !nombresDeclarados[nombre] {
			t.Errorf("`%s` nombra a %s, que no es un *%s declarado en %s (¿se declaró en otro fichero?)",
				listaDeErrores, nombre, tipoDelError, ficheroDeLosErrores)
		}
	}

	// (3) El censo del fuente y el que ve el runtime tienen que cuadrar en número. Cierra el hueco de un
	// error declarado fuera de este fichero: no lo vería el parseo, pero sí engordaría la lista.
	if len(ErroresInferencia) != len(declarados) {
		t.Errorf("%s declara %d errores canónicos y `%s` tiene %d elementos en runtime: o hay uno declarado "+
			"en otro fichero, o la lista repite un valor", ficheroDeLosErrores, len(declarados), listaDeErrores, len(ErroresInferencia))
	}

	// (4) El puente entre los dos censos: el literal `codigo:` que vio el parser tiene que ser resoluble en
	// runtime. Sin esto, (1) podría pasar con el slice apuntando a otra cosa.
	for _, d := range declarados {
		if d.codigo == "" {
			t.Errorf("%s:%d — %s no se inicializa con un `codigo:` literal; este guardarraíl lo lee del "+
				"fuente y hay que actualizarlo (o devolver el literal)", ficheroDeLosErrores, d.linea, d.nombre)
			continue
		}
		if _, ok := ErrorInferenciaDe(d.codigo); !ok {
			t.Errorf("%s:%d — %s vale %q y ErrorInferenciaDe(%q) no lo resuelve",
				ficheroDeLosErrores, d.linea, d.nombre, d.codigo, d.codigo)
		}
	}
}

// leerErroresDelFuente parsea inferencia.go y devuelve (a) todos los `var` de nivel de paquete
// inicializados con `&ErrorInferencia{…}` y (b) el conjunto de NOMBRES que el slice `ErroresInferencia`
// enumera. Falla el test si alguna de las dos búsquedas vuelve vacía: un parseo que no encuentra nada
// compararía dos conjuntos vacíos y pasaría sin haber comprobado nada.
func leerErroresDelFuente(t *testing.T) (declarados []errorDeclarado, enLaLista map[string]bool) {
	t.Helper()

	fset := token.NewFileSet()
	fichero, err := parser.ParseFile(fset, ficheroDeLosErrores, nil, 0)
	if err != nil {
		t.Fatalf("no se pudo parsear %s (¿se renombró?): %v", ficheroDeLosErrores, err)
	}
	enLaLista = make(map[string]bool)

	for _, decl := range fichero.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, nombre := range vs.Names {
				if i >= len(vs.Values) {
					continue
				}
				if nombre.Name == listaDeErrores {
					leerElementosDeLaLista(t, vs.Values[i], enLaLista)
					continue
				}
				if codigo, es := errorCanonicoDeclarado(vs.Values[i]); es {
					declarados = append(declarados, errorDeclarado{
						nombre: nombre.Name,
						codigo: codigo,
						linea:  fset.Position(nombre.Pos()).Line,
					})
				}
			}
		}
	}

	// Las dos redes contra el falso verde.
	if len(declarados) == 0 {
		t.Fatalf("el parseo de %s no encontró NINGÚN `&%s{…}`: cambió la forma de la declaración y este "+
			"guardarraíl dejó de mirar (arréglalo, no lo borres)", ficheroDeLosErrores, tipoDelError)
	}
	if len(enLaLista) == 0 {
		t.Fatalf("el parseo de %s no encontró el slice `%s` (¿se renombró?): sin él este guardarraíl no "+
			"compara nada", ficheroDeLosErrores, listaDeErrores)
	}
	return declarados, enLaLista
}

// errorCanonicoDeclarado reconoce el inicializador `&ErrorInferencia{codigo: "…", razon: "…"}` y devuelve
// su código literal. El segundo valor dice si la expresión ERA uno de esos errores (un código no literal
// devolvería ("", true), que el test reporta como algo que hay que actualizar, no como un no-error).
func errorCanonicoDeclarado(valor ast.Expr) (codigo string, es bool) {
	unario, ok := valor.(*ast.UnaryExpr)
	if !ok || unario.Op != token.AND {
		return "", false
	}
	lit, ok := unario.X.(*ast.CompositeLit)
	if !ok {
		return "", false
	}
	ident, ok := lit.Type.(*ast.Ident)
	if !ok || ident.Name != tipoDelError {
		return "", false
	}
	for _, elemento := range lit.Elts {
		kv, ok := elemento.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		clave, ok := kv.Key.(*ast.Ident)
		if !ok || clave.Name != "codigo" {
			continue
		}
		basico, ok := kv.Value.(*ast.BasicLit)
		if !ok || basico.Kind != token.STRING {
			continue
		}
		if s, err := strconv.Unquote(basico.Value); err == nil {
			return s, true
		}
	}
	return "", true
}

// leerElementosDeLaLista vuelca en `destino` los identificadores que enumera el literal de slice de
// `ErroresInferencia`. Falla ruidosamente si la lista deja de escribirse con identificadores: el test
// lee sus elementos del fuente y un literal anónimo lo dejaría mirando a otro sitio.
func leerElementosDeLaLista(t *testing.T, valor ast.Expr, destino map[string]bool) {
	t.Helper()
	lit, ok := valor.(*ast.CompositeLit)
	if !ok {
		t.Fatalf("`%s` ya no se inicializa con un literal de slice: este test lee sus elementos del fuente "+
			"y hay que actualizarlo", listaDeErrores)
	}
	for _, elemento := range lit.Elts {
		ident, ok := elemento.(*ast.Ident)
		if !ok {
			t.Fatalf("`%s` contiene un elemento que no es un identificador (%T): el test espera que la lista "+
				"se escriba con los `var` del vocabulario, no con literales", listaDeErrores, elemento)
		}
		destino[ident.Name] = true
	}
}
