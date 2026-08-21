// aviso_test.go — guardas del literal AVISO_SESION_PASIVA_V1 (Plan 046 · T3.2 mitad (a)).
//
// Tres guardas, cada una caza una mutación distinta:
//  1. Golden: cualquier byte cambiado en la constante AvisoSesionPasiva sin tocar el golden.
//  2. Runbook: la constante divergió del §4 de docs/runbooks/perfiles-de-sesion.md (la fuente
//     del contrato). Se salta si el fichero no existe: wapp-edge-agent es un repo git separado
//     y docs/ no viaja con él (en CI este test SIEMPRE se salta; en el árbol wApp completo, no).
//  3. Cableado de la pantalla: index.html/app.js dejan de pedir/pintar el literal, o alguien
//     lo teclea a mano en un asset (segunda copia = divergencia futura).
package webui

import (
	"os"
	"strings"
	"testing"
)

// TestAvisoSesionPasivaGolden fija los bytes exactos del literal. El golden lleva un '\n' final
// (convención de ficheros de texto) que la constante no tiene; se recorta exactamente uno.
// Mutación que lo pone rojo: cambiar CUALQUIER carácter de AvisoSesionPasiva (una tilde, una
// mayúscula del énfasis, un salto de línea) sin regenerar el golden a conciencia.
func TestAvisoSesionPasivaGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/AVISO_SESION_PASIVA_V1.golden")
	if err != nil {
		t.Fatalf("no se pudo leer el golden: %v", err)
	}
	want := strings.TrimSuffix(string(raw), "\n")
	if AvisoSesionPasiva != want {
		t.Fatalf("AvisoSesionPasiva no coincide con el golden.\n--- constante ---\n%q\n--- golden ---\n%q", AvisoSesionPasiva, want)
	}
	if AvisoSesionPasivaID != "AVISO_SESION_PASIVA_V1" {
		t.Fatalf("AvisoSesionPasivaID = %q; el golden es de la V1 — si el literal subió de versión, regenera golden y test juntos", AvisoSesionPasivaID)
	}
}

// TestAvisoSesionPasivaCoincideConRunbook compara la constante contra la fuente del contrato:
// el bloque ```text del §4 de docs/runbooks/perfiles-de-sesion.md, en la raíz wApp (cuatro
// niveles arriba de este paquete). Hay dos copias del literal en el ecosistema —una por repo,
// Edge y nube— y este test (y su gemelo en la nube) es lo que impide que diverjan.
// Mutación que lo pone rojo: editar el runbook sin propagar aquí, o editar la constante sin
// tocar el runbook (solo se ve en el árbol wApp completo; en el CI del repo suelto se salta).
func TestAvisoSesionPasivaCoincideConRunbook(t *testing.T) {
	const runbook = "../../../../docs/runbooks/perfiles-de-sesion.md"
	raw, err := os.ReadFile(runbook)
	if err != nil {
		t.Skipf("runbook no disponible (%v): este repo viaja sin docs/ — el test solo corre en el árbol wApp completo", err)
	}
	doc := string(raw)

	idx := strings.Index(doc, AvisoSesionPasivaID)
	if idx < 0 {
		t.Fatalf("el runbook no contiene el ID %q: ¿subió de versión sin propagar?", AvisoSesionPasivaID)
	}
	rest := doc[idx:]
	open := strings.Index(rest, "```text\n")
	if open < 0 {
		t.Fatalf("no hay bloque ```text tras el ID %q en %s", AvisoSesionPasivaID, runbook)
	}
	rest = rest[open+len("```text\n"):]
	close_ := strings.Index(rest, "\n```")
	if close_ < 0 {
		t.Fatalf("el bloque ```text del §4 no cierra en %s", runbook)
	}
	canon := rest[:close_]

	if AvisoSesionPasiva != canon {
		t.Fatalf("la constante divergió del §4 del runbook (la fuente única).\n--- constante ---\n%q\n--- runbook ---\n%q", AvisoSesionPasiva, canon)
	}
}

// TestPantallaEmparejamientoCableaElAviso verifica, sobre los assets EMBEBIDOS (los mismos bytes
// que sirve wapp-ctl), que la pantalla de éxito está cableada al literal: index.html tiene el
// contenedor, app.js pide el endpoint y lo pinta en él, y NINGÚN asset teclea el texto a mano
// (el literal debe existir una sola vez en el repo: la constante Go).
// Mutaciones que lo ponen rojo: quitar el <p id="pair-passive-notice"> del HTML; que app.js deje
// de llamar al endpoint o de usar el contenedor; o pegar el texto del aviso dentro de un asset.
func TestPantallaEmparejamientoCableaElAviso(t *testing.T) {
	read := func(name string) string {
		t.Helper()
		b, err := assets.ReadFile(name)
		if err != nil {
			t.Fatalf("asset embebido %s: %v", name, err)
		}
		return string(b)
	}
	html := read("index.html")
	js := read("app.js")

	if !strings.Contains(html, `id="pair-passive-notice"`) {
		t.Fatal(`index.html perdió el contenedor id="pair-passive-notice" de la pantalla de éxito`)
	}
	for _, needle := range []string{"/v1/ui/aviso-sesion-pasiva", "pairPassiveNotice", "showPassiveNotice"} {
		if !strings.Contains(js, needle) {
			t.Fatalf("app.js perdió el cableado del aviso: falta %q", needle)
		}
	}
	// Anti-segunda-copia: una frase distintiva del literal no puede aparecer tecleada en assets.
	const distintiva = "nació en perfil PASIVA"
	for _, name := range []string{"index.html", "app.js", "login.html", "styles.css"} {
		if strings.Contains(read(name), distintiva) {
			t.Fatalf("%s contiene el literal tecleado a mano: la fuente única es webui.AvisoSesionPasiva", name)
		}
	}
}
