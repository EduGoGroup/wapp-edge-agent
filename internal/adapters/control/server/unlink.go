package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// Unlinker es el puerto ESTRECHO que el plano de control necesita para desvincular una sesión, SIN
// conocer cryptostore/custody/whatsmeow. Es el CONTRATO para el wire-up: la implementación viva es
// *app.UnlinkSession (cableada en cmd/agent runServe con el registry/eraser/custody/logout reales); un
// doble lo implementa en los tests.
//
// Run desvincula la sesión `jid`: logout remoto best-effort + borrado del device + del registro + de la
// DEK. Devuelve app.ErrSessionNotFound si el JID no existe (→ 404); cualquier otro error es un fallo de
// limpieza (→ 500). NUNCA transporta la DEK (ADR-0007/0014).
type Unlinker interface {
	Run(ctx context.Context, jid string) (app.UnlinkResult, error)
}

// unlinkHandler sirve DELETE /v1/sessions/{id} sobre el Unlinker dado.
type unlinkHandler struct {
	unlinker Unlinker
	log      logger // subconjunto de sharedlogger.Logger; puede ser nil
}

// RegisterUnlink cuelga DELETE /v1/sessions/{id} sobre el Unlinker dado. Se llama ANTES de Serve, igual
// que RegisterPairing. runServe lo invoca con un *app.UnlinkSession; los tests con un doble. La firma de
// New (T0) NO cambia.
func (s *Server) RegisterUnlink(u Unlinker) {
	h := &unlinkHandler{unlinker: u}
	if s.log != nil {
		h.log = s.log
	}
	s.Handle(http.MethodDelete, "/v1/sessions/{id}", h.handle)
}

// unlinkResponse es el cuerpo de un DELETE /v1/sessions/{id} exitoso: confirma la desvinculación, el
// estado de negocio PREVIO (lo que había) y el desenlace del logout remoto best-effort. NO transporta
// material cripto.
type unlinkResponse struct {
	JID           string `json:"jid"`
	Unlinked      bool   `json:"unlinked"`
	PreviousState string `json:"previous_state,omitempty"`
	RemoteLogout  string `json:"remote_logout"` // "ok" | "skipped" | "failed"
}

// handle desvincula la sesión {id}. 404 si el JID no existe; 200 con el resultado en éxito; 500 con
// envelope ante un fallo de limpieza.
func (h *unlinkHandler) handle(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	res, err := h.unlinker.Run(r.Context(), id)
	if errors.Is(err, app.ErrSessionNotFound) {
		writeError(w, http.StatusNotFound, codeNotFound, "sesión no encontrada: "+id)
		return
	}
	if err != nil {
		if h.log != nil {
			h.log.Error("plano de control: no se pudo desvincular la sesión", "jid", id, "error", err)
		}
		writeError(w, http.StatusInternalServerError, codeInternal, "no se pudo desvincular la sesión")
		return
	}
	if h.log != nil {
		h.log.Info("plano de control: sesión desvinculada y estado local limpiado",
			"jid", id, "remote_logout", string(res.RemoteLogout))
	}
	writeJSON(w, http.StatusOK, unlinkResponse{
		JID:           res.JID,
		Unlinked:      true,
		PreviousState: string(res.Previous.State),
		RemoteLogout:  string(res.RemoteLogout),
	})
}
