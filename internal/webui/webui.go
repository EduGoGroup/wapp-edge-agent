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
	"net/http"
)

//go:embed index.html app.js styles.css
var assets embed.FS

// Handler devuelve el http.Handler que sirve los assets embebidos. http.FileServer sirve index.html en
// "/" automáticamente y aplica el Content-Type correcto por extensión a app.js y styles.css.
func Handler() http.Handler {
	return http.FileServer(http.FS(assets))
}
