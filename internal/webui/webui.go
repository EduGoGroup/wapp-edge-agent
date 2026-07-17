// Package webui sirve la web UI del plano de control, EMBEBIDA con embed.FS (sin build step; decisión
// §10.I). El supervisor (cmd/wapp-ctl) la monta en "/" del mismo origen loopback que el proxy /v1/* →
// single-origin, sin CORS.
//
// UI REAL (T5): index.html + app.js + styles.css (HTML/CSS/JS vanilla, sin build step; decisión §10.I).
// Los tres assets se embeben con //go:embed y se sirven con un http.FileServer sobre el embed.FS, mismo
// origen loopback que el proxy /v1/*. El FileServer entrega index.html en "/" y resuelve el content-type
// por extensión (.html/.js/.css) automáticamente.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
)

// login.html se embebe junto a los assets del dashboard: es la pantalla de inicio de sesión del operador
// (Plan 033 · Ola 3 · Paso B, ADR-0025). Es un documento AUTOCONTENIDO (script inline) que reutiliza
// styles.css; wapp-ctl la sirve en GET /login (público) y protege index.html tras la cookie de sesión.
//
//go:embed index.html app.js styles.css login.html
var assets embed.FS

// Handler devuelve el http.Handler que sirve los assets embebidos. http.FileServer sirve index.html en
// "/" automáticamente y aplica el Content-Type correcto por extensión a app.js y styles.css.
func Handler() http.Handler {
	return http.FileServer(http.FS(assets))
}

// FS expone el sistema de archivos embebido para que wapp-ctl sirva documentos concretos con control de
// acceso propio (p.ej. login.html en GET /login, público, vs. index.html protegido tras la sesión).
func FS() fs.FS {
	return assets
}
