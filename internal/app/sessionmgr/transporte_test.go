package sessionmgr

import (
	"database/sql"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// El adaptador real tiene que caber en la frontera tal cual: si un día deja de cumplirla, se rompe aquí, en
// compilación, y no en el arranque del daemon.
var _ Escucha = (*whatsmeow.ListenGateway)(nil)

// transporteWhatsmeowDeTest es el transporte REAL para los tests de cableado de este paquete, que afirman
// cosas sobre el *whatsmeow.ListenGateway que el factory construye. Es el calco de
// wiring.TransporteWhatsmeow, que estos tests no pueden importar (wiring importa sessionmgr: sería un ciclo).
func transporteWhatsmeowDeTest() Transporte {
	return Transporte{
		NuevoConector: func(storeDB *sql.DB, dialect string, log sharedlogger.Logger) app.Connector {
			return whatsmeow.NewConnector(storeDB, dialect, log)
		},
		NuevaEscucha: func(db *sql.DB, dialect, jid string, log sharedlogger.Logger) Escucha {
			return whatsmeow.NewListenGatewayForDevice(db, dialect, jid, log)
		},
	}
}

// rutaAdaptadorWhatsmeow es el paquete del adaptador real, el que el gestor ya no puede construir.
const rutaAdaptadorWhatsmeow = "github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"

// usosPermitidosDelAdaptadorReal es TODO lo que el código de producción de este paquete puede nombrar del
// adaptador de whatsmeow. 🔴 Esta lista SOLO ENCOGE: `Backoff` es un tipo puro (solo importa `time`) que
// quedó ahí como deuda (documentations/deuda.md); el día que se mude, la lista queda vacía y el import
// desaparece. Añadir una entrada es volver a atar el gestor a un transporte concreto: no se hace.
var usosPermitidosDelAdaptadorReal = map[string]bool{
	"Backoff": true,
}

// TestFronteraDelTransporte_SessionmgrNoConstruyeElAdaptadorReal es el candado de la frontera (ADR-0049,
// RW.1.c). Recorre el AST de TODOS los ficheros de producción del paquete y afirma que del adaptador de
// whatsmeow no se nombra nada fuera de la lista de arriba: ni un constructor, ni un tipo, ni un centinela.
// Va sobre el AST y no con un grep porque un grep cuenta comentarios, y este paquete nombra `whatsmeow` en
// decenas de ellos.
func TestFronteraDelTransporte_SessionmgrNoConstruyeElAdaptadorReal(t *testing.T) {
	ficheros, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listar el paquete: %v", err)
	}
	fset := token.NewFileSet()
	var revisados int
	var fuera []string
	for _, f := range ficheros {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("leer %s: %v", f, err)
		}
		file, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			t.Fatalf("parsear %s: %v", f, err)
		}
		revisados++
		alias := ""
		for _, imp := range file.Imports {
			if strings.Trim(imp.Path.Value, `"`) != rutaAdaptadorWhatsmeow {
				continue
			}
			alias = "whatsmeow"
			if imp.Name != nil {
				alias = imp.Name.Name
			}
		}
		if alias == "" {
			continue
		}
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != alias || pkg.Obj != nil {
				return true
			}
			if !usosPermitidosDelAdaptadorReal[sel.Sel.Name] {
				fuera = append(fuera, fset.Position(sel.Pos()).String()+": "+alias+"."+sel.Sel.Name)
			}
			return true
		})
	}
	// Un candado que no mira nada pasa siempre: si el glob no encontró el paquete, es un fallo, no un verde.
	if revisados < 5 {
		t.Fatalf("solo se revisaron %d ficheros de producción: el candado no está mirando el paquete", revisados)
	}
	sort.Strings(fuera)
	if len(fuera) > 0 {
		t.Fatalf("el gestor de sesiones nombra del adaptador de whatsmeow algo que no está permitido "+
			"(el transporte se recibe por sessionmgr.Transporte, no se construye aquí):\n  %s",
			strings.Join(fuera, "\n  "))
	}
}
