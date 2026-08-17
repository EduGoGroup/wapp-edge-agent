package intent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Estos tests se reescribieron en el Plan 051 Ola 3 · T3.0. Los anteriores (TestStatusHandler_Enabled_OllamaOK,
// _Disabled_Defaults, _OllamaCaido) verificaban `ollama_ok` y `circuit`, los dos campos que el daemon dejó de
// poder conocer al retirarse el camino inline. No se "adaptaron": se sustituyeron, porque lo que probaban ya
// no es una propiedad de este proceso. El razonamiento completo está en el doc comment de statusResponse.

func doStatus(t *testing.T, deps StatusDeps) statusResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/intent/status", nil)
	StatusHandler(deps)(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status code: got %d", rec.Code)
	}
	var out statusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

func TestStatusHandler_Habilitado_ReportaContratoPersistido(t *testing.T) {
	out := doStatus(t, StatusDeps{
		Enabled:       true,
		Model:         "qwen3:1.7b",
		ConfigVersion: func() string { return "v-abc" },
	})
	if !out.Enabled || out.Model != "qwen3:1.7b" || out.ConfigVersion != "v-abc" {
		t.Errorf("respuesta inesperada: %+v", out)
	}
}

func TestStatusHandler_Deshabilitado_SinVersion(t *testing.T) {
	// Feature off: sin getter de versión ⇒ config_version="". El endpoint responde igual (200), que es el
	// contrato de siempre: la consola de onboarding necesita distinguir "apagado" de "no hay endpoint".
	out := doStatus(t, StatusDeps{Enabled: false, Model: "qwen3:1.7b"})
	if out.Enabled || out.ConfigVersion != "" {
		t.Errorf("respuesta con feature off inesperada: %+v", out)
	}
}

// TestStatusHandler_SeñalaAlCajero es el test que defiende la DECISIÓN de T3.0: este endpoint no debe
// volver a hablar del clasificador como si viviera aquí. Si alguien reintroduce `circuit`/`ollama_ok`
// leyendo algo local, tendrá que pasar antes por estas dos aserciones y por el doc comment que las explica.
func TestStatusHandler_SeñalaAlCajero(t *testing.T) {
	out := doStatus(t, StatusDeps{Enabled: true})
	if out.ClasificaEn != "worker-cajero" {
		t.Errorf("clasifica_en: got %q want worker-cajero", out.ClasificaEn)
	}
	if out.WorkerStatusURL != cajeroStatusPath {
		t.Errorf("worker_status_url: got %q want %q", out.WorkerStatusURL, cajeroStatusPath)
	}
}

// TestStatusHandler_NoExponeCircuitoNiOllama comprueba por el CUERPO CRUDO que las dos claves retiradas no
// han vuelto a colarse (p.ej. por un campo añadido con el mismo json tag en otra rama). Se mira el JSON y no
// el struct a propósito: el contrato con el operador es el JSON.
func TestStatusHandler_NoExponeCircuitoNiOllama(t *testing.T) {
	rec := httptest.NewRecorder()
	StatusHandler(StatusDeps{Enabled: true})(rec, httptest.NewRequest(http.MethodGet, "/v1/intent/status", nil))

	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, prohibida := range []string{"circuit", "ollama_ok"} {
		if _, hay := raw[prohibida]; hay {
			t.Errorf("el endpoint no puede reportar %q: ese dato vive en el proceso del cajero y aquí sería una mentira operativa", prohibida)
		}
	}
}
