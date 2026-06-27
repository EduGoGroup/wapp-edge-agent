package server

import (
	"context"
	"net/http"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// SessionLister es el puerto de lectura que consume GET /v1/sessions. Su implementación REAL es
// *sessionstore.Store (internal/adapters/sessionstore: List sobre la tabla `sessions`, en claro);
// satisface esta interfaz sin adaptadores. El wire-up sobre la BD viva del agente se hace en T3
// (`agent serve`); en T0 y en los tests se inyecta un doble. NO se hardcodean sesiones falsas: los
// datos salen siempre del lister inyectado (sessionstore en producción).
type SessionLister interface {
	List(ctx context.Context) ([]domain.Session, error)
}

// healthResponse es el cuerpo de GET /v1/health (decisión §10: {status, version}).
type healthResponse struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}

// handleHealth responde 200 con {status:"ok", version}. La versión es la build del núcleo
// (cmd/agent/main.go const Version), inyectada por Config.Version. Es la base del "daemon up/down"
// que el supervisor (T4) consultará por el socket.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok", Version: s.cfg.Version})
}

// sessionDTO es la proyección JSON de domain.Session para el contrato /v1 (nombres snake_case
// explícitos, desacoplados de los campos del dominio). NO incluye material criptográfico (el dominio
// tampoco lo tiene): solo metadatos de negocio (jid, estado, timestamps).
type sessionDTO struct {
	JID       string `json:"jid"`
	State     string `json:"state"`
	PairedAt  string `json:"paired_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

// sessionsResponse envuelve la lista en un objeto (no un array desnudo) para poder extenderlo
// (paginación, sesión activa, etc.) sin romper el contrato. Sessions nunca es null: lista vacía = [].
type sessionsResponse struct {
	Sessions []sessionDTO `json:"sessions"`
}

// handleSessions responde 200 con la lista de sesiones del agente (estado tipado). En el MVP el
// modelo soporta varias pero se valida con una sola (decisión §10.H). Si el lister falla, 500 con
// envelope. Si no hay lister inyectado (constructor sin dependencia), devuelve lista vacía bien
// tipada en vez de fallar.
func (s *Server) handleSessions(w http.ResponseWriter, r *http.Request) {
	var sessions []domain.Session
	if s.sessions != nil {
		var err error
		sessions, err = s.sessions.List(r.Context())
		if err != nil {
			if s.log != nil {
				s.log.Error("plano de control: no se pudieron listar las sesiones", "error", err)
			}
			writeError(w, http.StatusInternalServerError, codeInternal, "no se pudieron listar las sesiones")
			return
		}
	}

	out := sessionsResponse{Sessions: make([]sessionDTO, 0, len(sessions))}
	for _, sess := range sessions {
		out.Sessions = append(out.Sessions, toSessionDTO(sess))
	}
	writeJSON(w, http.StatusOK, out)
}

// toSessionDTO mapea el dominio a la proyección del contrato. Los timestamps cero (sesión sin
// emparejar/actualizar) se omiten (omitempty) en vez de emitir una fecha época-cero engañosa.
func toSessionDTO(s domain.Session) sessionDTO {
	dto := sessionDTO{JID: s.JID, State: string(s.State)}
	if !s.PairedAt.IsZero() {
		dto.PairedAt = s.PairedAt.UTC().Format(time.RFC3339)
	}
	if !s.UpdatedAt.IsZero() {
		dto.UpdatedAt = s.UpdatedAt.UTC().Format(time.RFC3339)
	}
	return dto
}
