package macos

import (
	"encoding/xml"
	"errors"
	"io"
	"strings"
	"testing"
)

func sampleParams() Params {
	base := "/Users/u/Library/Application Support/wApp"
	return Params{
		CtlBin:     base + "/bin/wapp-ctl",
		AgentBin:   base + "/bin/agent",
		DataDir:    base + "/edge",
		ConfigPath: base + "/edge/config.yaml",
		StdoutLog:  base + "/edge/logs/edge.out.log",
		StderrLog:  base + "/edge/logs/edge.err.log",
	}
}

// TestRenderPlist_XMLBienFormado: el plist renderizado se tokeniza completo sin error (XML válido, incluye
// DOCTYPE y comentarios). Es la primera línea de defensa contra un plist roto que launchd rechazaría.
func TestRenderPlist_XMLBienFormado(t *testing.T) {
	dec := xml.NewDecoder(strings.NewReader(RenderPlist(sampleParams())))
	for {
		_, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("plist no es XML bien formado: %v", err)
		}
	}
}

// TestRenderPlist_SinTokensSinSustituir: no debe quedar ningún @@TOKEN@@ (fuerza a RenderPlist a cubrir
// todos los tokens de la plantilla; si alguien añade uno nuevo, este test lo caza).
func TestRenderPlist_SinTokensSinSustituir(t *testing.T) {
	if out := RenderPlist(sampleParams()); strings.Contains(out, "@@") {
		t.Fatalf("quedaron tokens sin sustituir en el plist:\n%s", out)
	}
}

// TestRenderPlist_ClavesDelContrato: el plist cumple el contrato del LaunchAgent (Plan 023 · T3): lanza
// wapp-ctl con --no-open y --autostart, RunAtLoad+KeepAlive, env estables (config/data_dir/agent_bin),
// WorkingDirectory y logs a archivo.
func TestRenderPlist_ClavesDelContrato(t *testing.T) {
	p := sampleParams()
	out := RenderPlist(p)
	musts := []string{
		"<string>" + Label + "</string>",
		"<key>RunAtLoad</key>",
		"<key>KeepAlive</key>",
		"<key>ProgramArguments</key>",
		"<string>" + p.CtlBin + "</string>",
		"<string>--no-open</string>",
		"<string>--autostart</string>",
		"<key>WAPP_AGENT_CONFIG</key>", "<string>" + p.ConfigPath + "</string>",
		"<key>WAPP_AGENT_DATA_DIR</key>", "<string>" + p.DataDir + "</string>",
		"<key>WAPP_CTL_AGENT_BIN</key>", "<string>" + p.AgentBin + "</string>",
		"<key>WorkingDirectory</key>",
		"<key>StandardOutPath</key>", "<string>" + p.StdoutLog + "</string>",
		"<key>StandardErrorPath</key>", "<string>" + p.StderrLog + "</string>",
	}
	for _, m := range musts {
		if !strings.Contains(out, m) {
			t.Errorf("el plist no contiene la clave/valor requerido: %s", m)
		}
	}
}

// TestRenderPlist_LaunchAgentNoDaemon: barrera del PUNTO DE PARADA de T3 — jamás LaunchDaemon/root. La DEK
// vive en el Keychain del usuario (T2); un daemon de sistema no lo vería.
func TestRenderPlist_LaunchAgentNoDaemon(t *testing.T) {
	out := RenderPlist(sampleParams())
	for _, forbidden := range []string{"LaunchDaemons", "<key>UserName</key>", "<key>GroupName</key>"} {
		if strings.Contains(out, forbidden) {
			t.Errorf("el plist NO debe contener %q (debe ser LaunchAgent por-usuario, sin root)", forbidden)
		}
	}
}
