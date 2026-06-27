package main

import (
	"encoding/json"
	"net/http"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/supervisor"
)

// El supervisor REPLICA el envelope de error del contrato /v1 (decisión §10.J: {"error":{code,message}})
// en vez de importar el helper del núcleo (internal/.../server.writeError es no-exportado y acoplaría el
// supervisor al paquete del servidor). Es un helper mínimo, idéntico en forma, documentado aquí.

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Códigos estables del supervisor (snake_case), para que la UI (T5) discrimine sin parsear el mensaje.
const (
	// codeDaemonDown lo emite el reverse-proxy cuando el núcleo no responde por el socket (503): la UI
	// lo interpreta como "daemon detenido" (solo el botón Arrancar activo). Es el contrato clave de T4.
	codeDaemonDown = "daemon_down"
	// codeStartFailed: el arranque del núcleo falló (no llegó a ready, etc.).
	codeStartFailed = "start_failed"
	// codeStopFailed: la parada del núcleo falló.
	codeStopFailed = "stop_failed"
	// codeMethodNotAllowed: verbo HTTP no permitido para una ruta /v1/daemon/*.
	codeMethodNotAllowed = "method_not_allowed"
)

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message}})
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeError(w, http.StatusMethodNotAllowed, codeMethodNotAllowed, "método no permitido")
}

// daemonStatusResponse es el cuerpo de /v1/daemon/{start,stop,status}: estado del proceso núcleo.
type daemonStatusResponse struct {
	State   string `json:"state"`         // "running" | "stopped"
	PID     int    `json:"pid,omitempty"` // pid del núcleo si corre
	Healthy bool   `json:"healthy"`       // GET /v1/health respondió 200
}

func toDaemonStatus(s supervisor.Status) daemonStatusResponse {
	return daemonStatusResponse{State: s.State, PID: s.PID, Healthy: s.Healthy}
}
