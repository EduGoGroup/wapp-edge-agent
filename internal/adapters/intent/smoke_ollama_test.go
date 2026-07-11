//go:build ollama

// smoke_ollama_test.go — Plan 029 · T11: smoke del decorador contra Ollama REAL (build tag `ollama`).
//
// Arma el decorador con un clasificador real (Ollama local) y una config de intenciones de fixture, y
// ejercita el camino completo (Deliver → clasificación → anotación) con un sink fake:
//   (a) "quiero 2 hamburguesas" ⇒ el evento llega CON intent crear_pedido y params.
//   (b) "hola" / "2"            ⇒ sin intent (carril rápido / sin intención accionable).
//   (c) Ollama caído (puerto muerto) ⇒ sin intent, sin error, y el circuito abre tras 5 fallos.
//
// Se salta solo si Ollama no responde (CI sin modelo). Overrides por env: WAPP_INTENT_TEST_URL
// (default http://127.0.0.1:11434) y WAPP_INTENT_TEST_MODEL (default qwen3:1.7b).
//
// Correr:  go test -tags ollama -run TestSmokeOllama ./internal/adapters/intent/ -v

package intent

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-intent/classifier"
	"github.com/EduGoGroup/wapp-edge-intent/ollama"
	"github.com/EduGoGroup/wapp-shared/intents"
)

const fixtureIntents = `{
  "version": "smoke-1",
  "umbral_confianza": 0.5,
  "intents": [
    {
      "name": "crear_pedido",
      "descripcion": "El cliente quiere pedir uno o varios productos",
      "params": ["producto", "cantidad"],
      "ejemplos": [
        {"mensaje": "quiero 3 pizzas", "params": {"producto": "pizzas", "cantidad": "3"}},
        {"mensaje": "me das dos hamburguesas", "params": {"producto": "hamburguesas", "cantidad": "2"}},
        {"mensaje": "una coca cola por favor", "params": {"producto": "coca cola", "cantidad": "1"}}
      ]
    },
    {
      "name": "consultar_horario",
      "descripcion": "El cliente pregunta el horario de atención",
      "ejemplos": [
        {"mensaje": "a qué hora abren"},
        {"mensaje": "están abiertos ahora"}
      ]
    }
  ],
  "vocabulario": ["pizza", "hamburguesa", "coca cola"]
}`

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func realDecorator(t *testing.T) *Decorator {
	t.Helper()
	url := envOr("WAPP_INTENT_TEST_URL", "http://127.0.0.1:11434")
	model := envOr("WAPP_INTENT_TEST_MODEL", "qwen3:1.7b")

	client := ollama.New(url)
	hctx, hcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer hcancel()
	if err := client.Health(hctx); err != nil {
		t.Skipf("Ollama no disponible en %s (%v); se salta el smoke", url, err)
	}

	cfg, err := intents.ParseAndValidate([]byte(fixtureIntents))
	if err != nil {
		t.Fatalf("fixture inválido: %v", err)
	}
	cls := classifier.New(client, model, cfg)
	// Timeout generoso: la carga en frío del modelo puede tardar varios segundos.
	d := New(cls, 15*time.Second, testLogger())
	d.SetConfig(cfg, cfg.Version)
	return d
}

func TestSmokeOllama_ClasificaPedido(t *testing.T) {
	d := realDecorator(t)
	cap := &captureSink{}

	start := time.Now()
	if err := d.Wrap(cap).Deliver(context.Background(), textEvent("quiero 2 hamburguesas")); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	elapsed := time.Since(start)

	ci := cap.lastEvent().Intent
	if ci == nil {
		t.Fatalf("(a) se esperaba intención accionable, llegó SIN intent (latencia %s)", elapsed)
	}
	t.Logf("(a) 'quiero 2 hamburguesas' ⇒ intent=%q params=%v confidence=%.2f latencia=%s",
		ci.Name, ci.Params, ci.Confidence, elapsed)
	if ci.Name != "crear_pedido" {
		t.Errorf("(a) intent: got %q want crear_pedido", ci.Name)
	}
	if ci.Params["cantidad"] == "" || ci.Params["producto"] == "" {
		t.Errorf("(a) params incompletos: %v", ci.Params)
	}
}

func TestSmokeOllama_FastlaneYNoAccionable(t *testing.T) {
	d := realDecorator(t)
	cap := &captureSink{}

	for _, text := range []string{"2", "hola"} {
		start := time.Now()
		if err := d.Wrap(cap).Deliver(context.Background(), textEvent(text)); err != nil {
			t.Fatalf("Deliver(%q): %v", text, err)
		}
		elapsed := time.Since(start)
		got := cap.lastEvent().Intent
		t.Logf("(b) %-6q ⇒ intent=%v latencia=%s", text, got, elapsed)
		if got != nil {
			t.Errorf("(b) %q no debe producir intención accionable, got %+v", text, got)
		}
	}
}

func TestSmokeOllama_CaidoAbreCircuito(t *testing.T) {
	// Apunta a un puerto MUERTO (simula Ollama caído): conexión rechazada inmediata.
	client := ollama.New("http://127.0.0.1:1")
	cfg, err := intents.ParseAndValidate([]byte(fixtureIntents))
	if err != nil {
		t.Fatalf("fixture inválido: %v", err)
	}
	cls := classifier.New(client, "qwen3:1.7b", cfg)
	d := New(cls, 500*time.Millisecond, testLogger())
	d.SetConfig(cfg, cfg.Version)
	cap := &captureSink{}

	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := d.Wrap(cap).Deliver(context.Background(), textEvent("quiero una pizza grande")); err != nil {
			t.Fatalf("Deliver nunca debe devolver error por culpa del clasificador: %v", err)
		}
		if cap.lastEvent().Intent != nil {
			t.Errorf("(c) con Ollama caído el evento debe llegar SIN intent")
		}
	}
	t.Logf("(c) 5 entregas con Ollama caído en %s; circuito=%q", time.Since(start), d.Circuit())
	if d.Circuit() != "open" {
		t.Errorf("(c) el circuito debe abrir tras 5 fallos, got %q", d.Circuit())
	}
}
