// Package webui sirve la web UI del plano de control, EMBEBIDA con embed.FS (sin build step; decisión
// §10.I). El supervisor (cmd/wapp-ctl) la monta en "/" del mismo origen loopback que el proxy /v1/* →
// single-origin, sin CORS.
//
// IMPORTANTE (T4): el contenido de index.html es un PLACEHOLDER mínimo para que T4 compile y se pueda
// probar el proxy/control. La UI REAL (estado, emparejar con QR, logs SSE) la implementa T5, que
// reemplaza el HTML/JS/CSS de este paquete SIN tocar el cableado del supervisor.
package webui

import (
	"embed"
	"net/http"
)

//go:embed index.html
var assets embed.FS

// Handler devuelve el http.Handler que sirve los assets embebidos. http.FileServer sirve index.html en
// "/" automáticamente. En T5 basta añadir app.js/styles.css al //go:embed y a este FS.
func Handler() http.Handler {
	return http.FileServer(http.FS(assets))
}
