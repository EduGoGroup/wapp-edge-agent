package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// enroller es el puerto ESTRECHO del onboarding sin terminal (Plan 023 · T1). El servidor de control
// NO conoce config ni data_dir; este puerto se inyecta desde cmd/agent (mismo patrón que
// RegisterPairing/RegisterUnlink) y envuelve el enroll REAL (internal/infra/enroll) SIN reimplementarlo.
//
// Invariante ZK (ADR-0015/0014): el enroll persiste el par mTLS + cloud_enc_pubkey (credenciales de
// transporte y la llave pública de la nube), NUNCA la DEK. El plano de control sigue sin tocar la DEK:
// solo dispara y observa. Un doble lo implementa en los tests SIN red ni Gateway real.
type enroller interface {
	// Enrolled reporta si el Edge YA tiene credencial mTLS (par cert/clave presente en disco). Es la
	// señal de "primera ejecución": ausente ⇒ la web muestra la pantalla enrolar en vez del dashboard.
	Enrolled() bool
	// Enroll ejecuta el enrolamiento con el activation code dado: dial validando la TLSCA pre-provista,
	// genera y persiste el par mTLS y puebla cloud_enc_pubkey. Devuelve error si falla (dial/CA/código).
	Enroll(ctx context.Context, activationCode string) error
}

// enrollHandler cuelga los endpoints de onboarding sobre el puerto enroller.
type enrollHandler struct {
	enroller enroller
	// log es el subconjunto Info/Error del logger; nil ⇒ sin trazas (igual que pairManager).
	log logger
}

// RegisterEnroll cuelga el onboarding sin terminal en el contrato /v1 (Plan 023 · T1):
//
//	GET  /v1/enroll/status → {"enrolled":bool}         (primera ejecución: la web elige pantalla)
//	POST /v1/enroll        → ejecuta el enroll con {"activation_code":"..."} del cuerpo
//
// Igual que RegisterPairing/RegisterUnlink: se llama ANTES de Serve y NO cambia la firma de New. El
// reverse-proxy de wapp-ctl enruta /v1/enroll* al núcleo por especificidad del ServeMux (cae en "/v1/"),
// sin tocar el supervisor. El núcleo sirve /v1 SIN credencial mTLS (solo la conexión a la nube la exige),
// así que el enroll puede correr en primera ejecución.
func (s *Server) RegisterEnroll(e enroller) {
	h := &enrollHandler{enroller: e}
	if s.log != nil {
		h.log = s.log
	}
	s.Handle(http.MethodGet, "/v1/enroll/status", h.handleStatus)
	s.Handle(http.MethodPost, "/v1/enroll", h.handleEnroll)
}

// enrollStatusResponse es el cuerpo de GET /v1/enroll/status.
type enrollStatusResponse struct {
	Enrolled bool `json:"enrolled"`
}

// enrollRequest es el cuerpo de POST /v1/enroll.
type enrollRequest struct {
	ActivationCode string `json:"activation_code"`
}

// enrollResponse es el cuerpo de éxito de POST /v1/enroll: Status "enrolled", Enrolled true.
type enrollResponse struct {
	Status   string `json:"status"`
	Enrolled bool   `json:"enrolled"`
}

// handleStatus responde si el Edge ya tiene credencial mTLS. Barato e idempotente: la web lo sondea para
// elegir entre la pantalla "enrolar" y el dashboard.
func (h *enrollHandler) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, enrollStatusResponse{Enrolled: h.enroller.Enrolled()})
}

// handleEnroll ejecuta el enrolamiento con el activation code del cuerpo. Si el Edge ya está enrolado
// responde 409 (no se re-enrola encima de una credencial viva); un cuerpo inválido o sin código → 400;
// un fallo del enroll (dial/Gateway/CA) → 502. En éxito, la credencial mTLS queda persistida por el
// puerto (que reusa internal/infra/enroll) y la web puede pasar a emparejar.
func (h *enrollHandler) handleEnroll(w http.ResponseWriter, r *http.Request) {
	if h.enroller.Enrolled() {
		writeError(w, http.StatusConflict, codeConflict,
			"el Edge ya está enrolado (credencial mTLS presente); desvincula antes de re-enrolar")
		return
	}

	var req enrollRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "cuerpo JSON inválido")
		return
	}
	code := strings.TrimSpace(req.ActivationCode)
	if code == "" {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "activation_code requerido")
		return
	}

	if err := h.enroller.Enroll(r.Context(), code); err != nil {
		if h.log != nil {
			h.log.Error("plano de control: el enrolamiento web falló", "error", err)
		}
		writeError(w, http.StatusBadGateway, codeEnrollFailed, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, enrollResponse{Status: "enrolled", Enrolled: true})
}
