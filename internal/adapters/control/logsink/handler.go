package logsink

import (
	"fmt"
	"net/http"
	"strings"
)

// Handler devuelve el http.HandlerFunc de GET /v1/logs: emite los logs en vivo por SSE
// (text/event-stream). Al conectar entrega el buffer reciente y luego transmite cada línea nueva.
// T3 lo registra con srv.Handle(http.MethodGet, "/v1/logs", logsink.Handler(sink)); el Server del
// núcleo (T0) delega directo al handler sin bufferizar la respuesta (ver server.go ServeHTTP), por
// lo que el streaming + flush funciona de extremo a extremo.
//
// Limpieza: respeta r.Context().Done() (cliente desconectado o Shutdown del servidor) para cerrar
// la suscripción y no fugar goroutines ni canales.
func Handler(s *Sink) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			// Sin flush no hay streaming real; es un error de programación del transporte, no del
			// cliente (cualquier ResponseWriter de net/http sobre una conexión soporta Flusher).
			http.Error(w, "streaming no soportado por el transporte", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		snapshot, lines, cancel := s.Subscribe()
		defer cancel()

		for _, line := range snapshot {
			writeSSE(w, line)
		}
		flusher.Flush()

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case line, ok := <-lines:
				if !ok {
					return
				}
				writeSSE(w, line)
				flusher.Flush()
			}
		}
	}
}

// writeSSE escribe una línea como un evento SSE (campo data). Cada línea del sink ya viene sin '\n'
// (el sink trocea por salto de línea), pero se sanea cualquier '\r' residual para no romper el
// framing del protocolo SSE (que delimita eventos con líneas en blanco).
func writeSSE(w http.ResponseWriter, line string) {
	line = strings.ReplaceAll(line, "\r", "")
	_, _ = fmt.Fprintf(w, "data: %s\n\n", line)
}
