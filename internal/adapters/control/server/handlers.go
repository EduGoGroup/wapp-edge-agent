package server

import (
	"context"
	"net/http"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// HealthReporter provee la salud de runtime ENRIQUECIDA para GET /v1/health (Plan 031 T7): el snapshot por
// sesión (del Registry T6, prueba de vida real) + los datos del daemon (uptime, versión). Lo satisface
// *health.Collector. Puede ser nil (constructor/tests sin colector): /v1/health responde solo {status,
// version} como antes (retrocompatible con el supervisor, que solo mira esos dos campos).
type HealthReporter interface {
	Reports(ctx context.Context) map[string]health.Report
	DaemonUptimeS() int64
}

// SessionLister es el puerto de lectura que consume GET /v1/sessions, RE-LLAVEADO a session_id
// (integración Plan 008): combina el inventario PERSISTIDO de N sesiones (Persisted, incluye 'pairing'
// aún no viva) con la SALUD de runtime por sesión (Health → etiqueta listening/degraded/…). Su
// implementación REAL la provee *sessionmgr.Manager (vía un adaptador en cmd/agent); en los tests se
// inyecta un doble. NO se hardcodean sesiones falsas: los datos salen siempre del inventario inyectado.
type SessionLister interface {
	// Persisted devuelve TODAS las sesiones registradas (session_id + jid + estado + timestamps).
	Persisted(ctx context.Context) ([]domain.Session, error)
	// Health devuelve la etiqueta de salud de runtime de una sesión VIVA (ok=false si no está viva).
	Health(id string) (string, bool)
}

// healthResponse es el cuerpo de GET /v1/health. Base histórica {status, version} (lo que el supervisor
// consulta para el up/down), ENRIQUECIDA en el Plan 031 T7 con el uptime del daemon y la salud por sesión
// (del Registry T6: prueba de vida real del socket). Los campos nuevos son omitempty: sin colector cableado
// el cuerpo es idéntico al previo (retrocompatible). ZERO-KNOWLEDGE: solo metadatos, nunca DEK/credenciales.
type healthResponse struct {
	Status   string                       `json:"status"`
	Version  string                       `json:"version"`
	UptimeS  int64                        `json:"uptime_s,omitempty"`
	Sessions map[string]sessionHealthView `json:"sessions,omitempty"`
}

// sessionHealthView es la proyección JSON de la salud de runtime de UNA sesión en GET /v1/health (misma
// lista cerrada de campos que el SessionHealth del heartbeat, ADR-0023). snake_case explícito.
type sessionHealthView struct {
	SocketState       string `json:"socket_state"`
	DegradedReason    string `json:"degraded_reason,omitempty"`
	LastInboundAgeS   int64  `json:"last_inbound_age_s"`
	DEKLoadDurationMs int64  `json:"dek_load_duration_ms"`
	IntentCircuit     string `json:"intent_circuit,omitempty"`
	OutboxDepth       int64  `json:"outbox_depth"`
	BinaryVersion     string `json:"binary_version"`
	DaemonUptimeS     int64  `json:"daemon_uptime_s"`
}

// handleHealth responde 200 con {status:"ok", version} y, si hay colector de salud cableado (Plan 031 T7),
// el uptime del daemon y la salud por sesión leída del Registry T6. La versión es la build del núcleo
// (cmd/agent/main.go const Version), inyectada por Config.Version. Es la base del "daemon up/down" que el
// supervisor consulta; los campos de salud son un enriquecimiento aditivo.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	resp := healthResponse{Status: "ok", Version: s.cfg.Version}
	if s.health != nil {
		resp.UptimeS = s.health.DaemonUptimeS()
		reports := s.health.Reports(r.Context())
		if len(reports) > 0 {
			resp.Sessions = make(map[string]sessionHealthView, len(reports))
			for id, hr := range reports {
				resp.Sessions[id] = sessionHealthView{
					SocketState:       hr.SocketState,
					DegradedReason:    hr.DegradedReason,
					LastInboundAgeS:   hr.LastInboundAgeS,
					DEKLoadDurationMs: hr.DEKLoadDurationMs,
					IntentCircuit:     hr.IntentCircuit,
					OutboxDepth:       hr.OutboxDepth,
					BinaryVersion:     hr.BinaryVersion,
					DaemonUptimeS:     hr.DaemonUptimeS,
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// sessionDTO es la proyección JSON de domain.Session para el contrato /v1 (nombres snake_case
// explícitos, desacoplados de los campos del dominio). RE-LLAVEADO a session_id (integración Plan 008):
// la identidad es session_id; el jid es opcional (vacío mientras 'pairing'); health refleja la salud de
// runtime del listener (vacío si la sesión no está viva). NO incluye material criptográfico.
type sessionDTO struct {
	SessionID string `json:"session_id"`
	JID       string `json:"jid,omitempty"`
	State     string `json:"state"`
	Health    string `json:"health,omitempty"`
	PairedAt  string `json:"paired_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

// sessionsResponse envuelve la lista en un objeto (no un array desnudo) para poder extenderlo
// (paginación, sesión activa, etc.) sin romper el contrato. Sessions nunca es null: lista vacía = [].
type sessionsResponse struct {
	Sessions []sessionDTO `json:"sessions"`
}

// handleSessions responde 200 con la lista de N sesiones del agente (session_id + estado de negocio +
// salud de runtime). Las sesiones salen del inventario PERSISTIDO (todas, incluida 'pairing'); la salud
// se enriquece consultando Health por session_id (vivas). Si el inventario falla, 500 con envelope. Si
// no hay inventario inyectado (constructor sin dependencia), devuelve lista vacía bien tipada.
func (s *Server) handleSessions(w http.ResponseWriter, r *http.Request) {
	if s.sessions == nil {
		writeJSON(w, http.StatusOK, sessionsResponse{Sessions: []sessionDTO{}})
		return
	}

	sessions, err := s.sessions.Persisted(r.Context())
	if err != nil {
		if s.log != nil {
			s.log.Error("plano de control: no se pudieron listar las sesiones", "error", err)
		}
		writeError(w, http.StatusInternalServerError, codeInternal, "no se pudieron listar las sesiones")
		return
	}

	out := sessionsResponse{Sessions: make([]sessionDTO, 0, len(sessions))}
	for _, sess := range sessions {
		dto := toSessionDTO(sess)
		if health, ok := s.sessions.Health(sess.SessionID); ok {
			dto.Health = health
		}
		out.Sessions = append(out.Sessions, dto)
	}
	writeJSON(w, http.StatusOK, out)
}

// toSessionDTO mapea el dominio a la proyección del contrato (sin la salud de runtime, que la añade el
// handler consultando Health). Los timestamps cero (sesión sin emparejar/actualizar) se omiten
// (omitempty) en vez de emitir una fecha época-cero engañosa.
func toSessionDTO(s domain.Session) sessionDTO {
	dto := sessionDTO{SessionID: s.SessionID, JID: s.JID, State: string(s.State)}
	if !s.PairedAt.IsZero() {
		dto.PairedAt = s.PairedAt.UTC().Format(time.RFC3339)
	}
	if !s.UpdatedAt.IsZero() {
		dto.UpdatedAt = s.UpdatedAt.UTC().Format(time.RFC3339)
	}
	return dto
}
