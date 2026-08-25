package main

// arranque_encadenado_ast_test.go — EL ORDEN DE ARRANQUE DE LOS DOS HIJOS, custodiado sobre el AST
// (DEUDA-044.9, Plan 044).
//
// 🔴 POR QUÉ UN TEST DE AST Y NO UNO DE CONDUCTA. Lo que se protege es una decisión que vive en `main()`:
// el cajero se arranca DENTRO de la goroutine del núcleo, después de que `sup.Start` devuelva. Un test de
// conducta exigiría levantar los dos procesos de verdad y medir una carrera de milisegundos — y esa
// carrera es justo la que NO se puede reproducir a voluntad: los 119 ms medidos en campo el 2026-08-25
// eran el caso benigno, y la ventana crece con el número de sesiones a restaurar. Un test así sería verde
// en CI (una máquina vacía restaura rápido) y no diría nada del caso que duele.
//
// LO QUE ESTA REGLA IMPIDE, en una frase: que alguien devuelva `cajeroSup.Start` a su propia goroutine
// «para que arranque antes». Eso es lo que había, y hacía que el aviso «listo» del cajero fallara SIEMPRE
// —10 arranques, 10 fallos— porque el núcleo aún no había abierto su socket del plano de control.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestArranqueDelCajero_VaEncadenadoAlNucleo_NoEnGoroutinePropia(t *testing.T) {
	fset := token.NewFileSet()
	archivo, err := parser.ParseFile(fset, "main.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("no se pudo parsear main.go: %v", err)
	}

	// Se recorre cada `go func(){...}()` y se anota qué Start contiene. La invariante es simple de
	// enunciar y por eso se puede custodiar: NINGUNA goroutine puede llevar `cajeroSup.Start` sin llevar
	// también `sup.Start` — o sea, el cajero no puede arrancar en un hilo que no haya esperado al núcleo.
	var infractoras int
	var conLasDos int
	ast.Inspect(archivo, func(n ast.Node) bool {
		gostmt, ok := n.(*ast.GoStmt)
		if !ok {
			return true
		}
		var tieneNucleo, tieneCajero bool
		ast.Inspect(gostmt, func(m ast.Node) bool {
			sel, ok := m.(*ast.SelectorExpr)
			if !ok || sel.Sel == nil || sel.Sel.Name != "Start" {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			switch ident.Name {
			case "sup":
				tieneNucleo = true
			case "cajeroSup":
				tieneCajero = true
			}
			return true
		})
		if tieneCajero && !tieneNucleo {
			infractoras++
		}
		if tieneCajero && tieneNucleo {
			conLasDos++
		}
		return true
	})

	if infractoras > 0 {
		t.Errorf("hay %d goroutine(s) que arrancan el CAJERO sin esperar al núcleo. Ése era el bug de "+
			"DEUDA-044.9: el cajero anuncia «listo» por el socket del plano de control y el núcleo lo abre "+
			"DESPUÉS de mgr.Restore(ctx) ⇒ el aviso falla siempre y el Cloud calienta tarde. "+
			"`sup.Start` ya BLOQUEA sondeando readiness: encadena, no paralelices", infractoras)
	}
	if conLasDos == 0 {
		t.Error("no se encontró ninguna goroutine que arranque el núcleo Y el cajero. Si el arranque " +
			"encadenado se movió de sitio, mueve también esta regla — pero no la borres: es lo único que " +
			"impide volver al arranque en paralelo")
	}
}
