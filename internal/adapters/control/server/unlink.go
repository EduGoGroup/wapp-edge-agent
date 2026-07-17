package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
)

// sessionUnlinker es el puerto ESTRECHO que el plano de control necesita para desvincular una sesión,
// RE-LLAVEADO a session_id (integración Plan 008): lo cumple *sessionmgr.Manager.Unlink. SIN conocer
// cryptostore/custody/whatsmeow, el control dispara el BORRADO QUIRÚRGICO de la sesión (cancela su
// listener, limpia SU DEK, borra SU fila y SU directorio; design §7). Un doble lo implementa en los tests.
//
// Unlink desvincula la sesión `id` (= session_id). Devuelve sessionmgr.ErrSessionNotFound si el
// session_id no existe (→ 404); cualquier otro error es un fallo de limpieza (→ 500). NUNCA transporta
// la DEK (ADR-0007/0014/0015).
type sessionUnlinker interface {
	Unlink(ctx context.Context, id string) error
}

// unlinkHandler sirve DELETE /v1/sessions/{id} sobre el sessionUnlinker dado.
type unlinkHandler struct {
	unlinker sessionUnlinker
	log      logger // subconjunto de sharedlogger.Logger; puede ser nil
}

// RegisterUnlink cuelga DELETE /v1/sessions/{id} sobre el sessionUnlinker dado. Se llama ANTES de Serve,
// igual que RegisterPairing. runServe lo invoca con el *sessionmgr.Manager; los tests con un doble. La
// firma de New NO cambia.
func (s *Server) RegisterUnlink(u sessionUnlinker) {
	h := &unlinkHandler{unlinker: u}
	if s.log != nil {
		h.log = s.log
	}
	// Escritura destructiva (edge.sessions.logout): exige refresh vivo incluso en modo degradado (ADR-0025).
	s.Handle(http.MethodDelete, "/v1/sessions/{id}", s.guard(resourceSessionsLogout, true, h.handle))
}

// unlinkResponse es el cuerpo de un DELETE /v1/sessions/{id} exitoso: confirma el borrado quirúrgico de
// la sesión identificada por session_id. NO transporta material cripto.
type unlinkResponse struct {
	SessionID string `json:"session_id"`
	Unlinked  bool   `json:"unlinked"`
}

// handle desvincula la sesión {id} (= session_id). 404 si el session_id no existe; 200 con el resultado
// en éxito; 500 con envelope ante un fallo de limpieza.
func (h *unlinkHandler) handle(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := h.unlinker.Unlink(r.Context(), id)
	if errors.Is(err, sessionmgr.ErrSessionNotFound) {
		writeError(w, http.StatusNotFound, codeNotFound, "sesión no encontrada: "+id)
		return
	}
	if err != nil {
		if h.log != nil {
			h.log.Error("plano de control: no se pudo desvincular la sesión", "session_id", id, "error", err)
		}
		writeError(w, http.StatusInternalServerError, codeInternal, "no se pudo desvincular la sesión")
		return
	}
	if h.log != nil {
		h.log.Info("plano de control: sesión desvinculada y estado local limpiado (borrado quirúrgico)", "session_id", id)
	}
	writeJSON(w, http.StatusOK, unlinkResponse{SessionID: id, Unlinked: true})
}
