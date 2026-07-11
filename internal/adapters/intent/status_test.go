package intent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeProber struct{ err error }

func (f fakeProber) Health(context.Context) error { return f.err }

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

func TestStatusHandler_Enabled_OllamaOK(t *testing.T) {
	out := doStatus(t, StatusDeps{
		Enabled:       true,
		Model:         "qwen3:1.7b",
		Prober:        fakeProber{err: nil},
		ConfigVersion: func() string { return "v-abc" },
		Circuit:       func() string { return "closed" },
	})
	if !out.Enabled || !out.OllamaOK || out.Model != "qwen3:1.7b" || out.ConfigVersion != "v-abc" || out.Circuit != "closed" {
		t.Errorf("respuesta inesperada: %+v", out)
	}
}

func TestStatusHandler_Disabled_Defaults(t *testing.T) {
	// Feature off: sin prober ni getters ⇒ ollama_ok=false, config_version="", circuit="closed".
	out := doStatus(t, StatusDeps{Enabled: false, Model: "qwen3:1.7b"})
	if out.Enabled || out.OllamaOK || out.ConfigVersion != "" || out.Circuit != "closed" {
		t.Errorf("respuesta con feature off inesperada: %+v", out)
	}
}

func TestStatusHandler_OllamaCaido(t *testing.T) {
	out := doStatus(t, StatusDeps{
		Enabled: true,
		Prober:  fakeProber{err: errors.New("connection refused")},
		Circuit: func() string { return "open" },
	})
	if out.OllamaOK {
		t.Errorf("ollama_ok debe ser false si el sondeo falla")
	}
	if out.Circuit != "open" {
		t.Errorf("circuit: got %q want open", out.Circuit)
	}
}
