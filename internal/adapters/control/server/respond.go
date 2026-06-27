package server

import (
	"encoding/json"
	"net/http"
)

// writeJSON serializa v como JSON y lo escribe con el status dado. Centraliza el Content-Type para
// que todos los handlers /v1 respondan de forma homogénea. Es reusable por los tramos siguientes
// (T1 /v1/logs metadatos, T2 /v1/sessions/pair) para no repetir el encoder en cada handler.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	// El error de Encode solo ocurriría con un tipo no serializable (bug de programación) o un cliente
	// que cortó la conexión; en ambos casos no hay nada útil que devolver al cliente.
	_ = json.NewEncoder(w).Encode(v)
}

// errorBody es el envelope de error del contrato /v1 (decisión §10.J): {"error":{"code,message"}}.
type errorBody struct {
	Error errorDetail `json:"error"`
}

// errorDetail es el cuerpo del error: un código estable (snake_case, para que el cliente discrimine
// sin parsear el mensaje) y un mensaje legible. El status HTTP viaja en la línea de estado (códigos
// estándar 400/404/405/500/502/504), no se duplica en el cuerpo.
type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Códigos de error estables del contrato /v1 (snake_case, alineado con el envelope §10.J).
const (
	codeNotFound         = "not_found"
	codeMethodNotAllowed = "method_not_allowed"
	codeInternal         = "internal"
)

// writeError responde con el envelope de error y el status HTTP indicado. Reusable por todos los
// tramos del plano de control.
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message}})
}
