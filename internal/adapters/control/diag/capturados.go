package diag

// capturados.go — LA LECTURA DE LO QUE EL WHATSAPP FALSO «ENVIÓ» (ADR-0049).
//
// Es la otra mitad del transporte falso: el inyector (inyector.go) mete lo que entra, y esta ruta deja
// leer desde fuera lo que el Edge quiso enviar. Mismo molde y mismas reglas: la ruta SOLO se registra con
// su palanca echada (WAPP_AGENT_WHATSAPP_FALSO; ver registrarCapturados en internal/infra/daemon) —con ella
// baja no existe, no es que conteste 403— y pasa por el mismo guardia de autorización que el resto de /v1.

import (
	"net/http"
	"strings"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsappfalso"
)

// RutaCapturados es la ruta de lectura. Vive junto a la del inyector, bajo /v1/diag.
const RutaCapturados = "/v1/diag/outbound/captured"

// RecursoCapturados es el grant que se evalúa. Es EL MISMO del inyector, a propósito: quien puede meter
// entrantes falsos en una sesión puede leer lo que esa sesión contestó, y un recurso nuevo exigiría darlo
// de alta en la nube para una ruta que solo existe con una palanca de pruebas.
const RecursoCapturados = RecursoInyectar

// CapturadosDeps son las dependencias de la ruta. Leer devuelve los envíos de una sesión y cuántos expulsó
// el tope del registro; nil ⇒ la ruta contesta 503 (cableado incompleto), no un panic.
type CapturadosDeps struct {
	Leer func(sessionID string) (envios []whatsappfalso.Envio, descartados int)
}

// respuestaCapturados es el cuerpo de la lectura. `descartados` > 0 avisa de que el registro se llenó y
// soltó envíos viejos: lo que se ve no es todo.
type respuestaCapturados struct {
	SessionID   string                `json:"session_id"`
	Capturados  []whatsappfalso.Envio `json:"capturados"`
	Total       int                   `json:"total"`
	Descartados int                   `json:"descartados"`
}

// CapturadosHandler sirve GET /v1/diag/outbound/captured?session_id=<id>.
func CapturadosHandler(deps CapturadosDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if deps.Leer == nil {
			responderError(w, http.StatusServiceUnavailable, "captured_unavailable",
				"el registro de envíos capturados no está cableado")
			return
		}
		sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
		if sessionID == "" {
			responderError(w, http.StatusBadRequest, "session_id_required",
				"falta el parámetro session_id: lo capturado se lee por sesión")
			return
		}
		envios, descartados := deps.Leer(sessionID)
		if envios == nil {
			envios = []whatsappfalso.Envio{}
		}
		responderJSON(w, http.StatusOK, respuestaCapturados{
			SessionID:   sessionID,
			Capturados:  envios,
			Total:       len(envios),
			Descartados: descartados,
		})
	}
}
