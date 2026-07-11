package intent

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// HealthProber sondea si el LLM local responde (GET /api/tags de Ollama con timeout corto). La cumple
// *ollama.Client (Health). Interfaz para desacoplar y poder inyectar un fake en los tests del handler.
type HealthProber interface {
	Health(ctx context.Context) error
}

// defaultProbeTimeout acota el sondeo de Ollama del endpoint de estado: corto para que la consola de
// onboarding (Plan 030) no se cuelgue si el LLM está caído o cargando en frío.
const defaultProbeTimeout = 2 * time.Second

// StatusDeps son las dependencias del endpoint GET /v1/intent/status. Todas tolerantes a nil para el caso
// feature OFF (el endpoint responde igual, con enabled=false).
type StatusDeps struct {
	// Enabled refleja cfg.Intent.Enabled.
	Enabled bool
	// Model es el modelo configurado (cfg.Intent.Model); informativo.
	Model string
	// Prober sondea Ollama. nil ⇒ ollama_ok=false sin sondear (feature off o sin cliente).
	Prober HealthProber
	// ConfigVersion devuelve la versión de la config 'intents' vigente (persistida/cargada) o "". nil ⇒ "".
	ConfigVersion func() string
	// Circuit devuelve el estado del circuito del clasificador ("closed"/"open"/"half-open"). nil ⇒ "closed".
	Circuit func() string
	// ProbeTimeout acota el sondeo de Ollama. <=0 ⇒ defaultProbeTimeout.
	ProbeTimeout time.Duration
}

// statusResponse es el cuerpo JSON de GET /v1/intent/status (lo consumirá la web de onboarding en el Plan 030).
type statusResponse struct {
	Enabled       bool   `json:"enabled"`
	OllamaOK      bool   `json:"ollama_ok"`
	Model         string `json:"model"`
	ConfigVersion string `json:"config_version"`
	Circuit       string `json:"circuit"`
}

// StatusHandler construye el handler de GET /v1/intent/status: reporta el estado del clasificador local sin
// bloquear (el sondeo de Ollama tiene timeout corto). Se registra SIEMPRE (aun con la feature off, donde
// responde enabled=false), vía Server.Handle en el arranque de `serve`.
func StatusHandler(deps StatusDeps) http.HandlerFunc {
	probeTimeout := deps.ProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = defaultProbeTimeout
	}
	return func(w http.ResponseWriter, r *http.Request) {
		resp := statusResponse{
			Enabled: deps.Enabled,
			Model:   deps.Model,
			Circuit: "closed",
		}
		if deps.Circuit != nil {
			resp.Circuit = deps.Circuit()
		}
		if deps.ConfigVersion != nil {
			resp.ConfigVersion = deps.ConfigVersion()
		}
		if deps.Prober != nil {
			ctx, cancel := context.WithTimeout(r.Context(), probeTimeout)
			resp.OllamaOK = deps.Prober.Health(ctx) == nil
			cancel()
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}
