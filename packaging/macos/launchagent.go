// Package macos contiene los artefactos de empaque/instalación del Edge en macOS (Plan 023): la plantilla
// del LaunchAgent por-usuario y su renderizado. RenderPlist es la fuente ÚNICA que también consumen los
// scripts install/uninstall (sustituyen los MISMOS tokens @@...@@), de modo que el test valida el plist
// que se instala de verdad.
//
// Es un LaunchAgent POR-USUARIO (NUNCA LaunchDaemon): la DEK vive en el Keychain del usuario (T2),
// invisible para un daemon de sistema (root).
package macos

import (
	_ "embed"
	"strings"
)

// Label es el Label del LaunchAgent y el nombre base del plist (com.wapp.edge.plist).
const Label = "com.wapp.edge"

//go:embed com.wapp.edge.plist.template
var plistTemplate string

// Params son las rutas ABSOLUTAS que el instalador sustituye en la plantilla (launchd no expande ~/$HOME).
type Params struct {
	CtlBin     string // ruta del wapp-ctl instalado (ProgramArguments)
	AgentBin   string // ruta del agent hermano (WAPP_CTL_AGENT_BIN; robustez del layout hermano bajo launchd)
	DataDir    string // <data_dir> (WorkingDirectory + WAPP_AGENT_DATA_DIR)
	ConfigPath string // <data_dir>/config.yaml (WAPP_AGENT_CONFIG; ruta estable de T1)
	StdoutLog  string // <data_dir>/logs/edge.out.log (StandardOutPath)
	StderrLog  string // <data_dir>/logs/edge.err.log (StandardErrorPath)
}

// RenderPlist sustituye los tokens @@...@@ de la plantilla embebida con los valores de p. La MISMA
// plantilla y el MISMO juego de tokens los usa install-launchagent.sh (sed): un token nuevo en la
// plantilla que RenderPlist no cubra deja un "@@" residual que el test caza.
func RenderPlist(p Params) string {
	return strings.NewReplacer(
		"@@WAPP_CTL_BIN@@", p.CtlBin,
		"@@AGENT_BIN@@", p.AgentBin,
		"@@DATA_DIR@@", p.DataDir,
		"@@CONFIG_PATH@@", p.ConfigPath,
		"@@STDOUT_LOG@@", p.StdoutLog,
		"@@STDERR_LOG@@", p.StderrLog,
	).Replace(plistTemplate)
}
