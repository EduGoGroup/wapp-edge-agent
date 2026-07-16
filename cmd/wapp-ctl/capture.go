package main

// capture.go es un http.ResponseWriter que BUFERIZA la respuesta del reverse-proxy en memoria, para poder
// decidir (según el status) si hay que refrescar el token y REINTENTAR el request antes de escribir al
// navegador. Solo se usa en peticiones NO-streaming (el SSE se transmite en directo, sin capturar).

import (
	"bytes"
	"net/http"
)

type captureWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
	wrote  bool
}

func newCapture() *captureWriter {
	return &captureWriter{header: make(http.Header), status: http.StatusOK}
}

func (c *captureWriter) Header() http.Header { return c.header }

func (c *captureWriter) WriteHeader(status int) {
	if c.wrote {
		return
	}
	c.status = status
	c.wrote = true
}

func (c *captureWriter) Write(b []byte) (int, error) {
	if !c.wrote {
		c.WriteHeader(http.StatusOK)
	}
	return c.body.Write(b)
}

// Flush satisface http.Flusher (el ReverseProxy lo invoca); es un no-op porque ya bufferizamos.
func (c *captureWriter) Flush() {}

// flush vuelca la respuesta capturada al ResponseWriter real (cabeceras + status + cuerpo).
func (c *captureWriter) flush(w http.ResponseWriter) {
	dst := w.Header()
	for k, vs := range c.header {
		dst[k] = vs
	}
	w.WriteHeader(c.status)
	_, _ = w.Write(c.body.Bytes())
}
