package intent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-edge-intent/classifier"
	"github.com/EduGoGroup/wapp-shared/intents"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

func testLogger() sharedlogger.Logger {
	return sharedlogger.New(sharedlogger.WithWriter(discardWriter{}), sharedlogger.WithJSON(true))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// fakeClassifier controla el resultado de Classify por test (y cuenta invocaciones).
type fakeClassifier struct {
	mu     sync.Mutex
	res    classifier.Classification
	err    error
	calls  int
	reload int
}

func (f *fakeClassifier) Classify(_ context.Context, _ string) (classifier.Classification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.res, f.err
}

func (f *fakeClassifier) Reload(*intents.Config) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reload++
}

func (f *fakeClassifier) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// captureSink captura el último evento entregado.
type captureSink struct {
	mu   sync.Mutex
	last domain.InboundEvent
	n    int
}

func (c *captureSink) Deliver(_ context.Context, evt domain.InboundEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.last = evt
	c.n++
	return nil
}

func (c *captureSink) lastEvent() domain.InboundEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

// readyDecorator arma un decorador con config cargada (ready) y un fake classifier.
func readyDecorator(t *testing.T, fc *fakeClassifier) *Decorator {
	t.Helper()
	d := New(fc, 100*time.Millisecond, testLogger())
	d.SetConfig(&intents.Config{Version: "v1"}, "v1")
	return d
}

func textEvent(text string) domain.InboundEvent {
	return domain.InboundEvent{MessageID: "m1", Text: text, Sender: "593@s.whatsapp.net"}
}

func TestDeliver_NoConfig_NoClasifica(t *testing.T) {
	fc := &fakeClassifier{res: classifier.Classification{Intent: "crear_pedido"}}
	d := New(fc, 100*time.Millisecond, testLogger()) // sin SetConfig => ready=false
	cap := &captureSink{}
	if err := d.Wrap(cap).Deliver(context.Background(), textEvent("quiero pizza margarita")); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if fc.callCount() != 0 {
		t.Errorf("sin config no debe clasificar")
	}
	if cap.lastEvent().Intent != nil {
		t.Errorf("sin config el evento no debe llevar intención")
	}
}

func TestDeliver_ExitoAnotaIntencion(t *testing.T) {
	fc := &fakeClassifier{res: classifier.Classification{
		Intent: "crear_pedido", Params: map[string]string{"producto": "pizza"}, Confidence: 0.9,
	}}
	d := readyDecorator(t, fc)
	cap := &captureSink{}
	if err := d.Wrap(cap).Deliver(context.Background(), textEvent("quiero una pizza grande")); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	ci := cap.lastEvent().Intent
	if ci == nil || ci.Name != "crear_pedido" || ci.Params["producto"] != "pizza" || ci.ConfigVersion != "v1" {
		t.Fatalf("intención mal anotada: %+v", ci)
	}
}

func TestDeliver_Desconocido_SinIntencion_NoAbreCircuito(t *testing.T) {
	fc := &fakeClassifier{res: classifier.Classification{Intent: intents.ReservedUnknown}}
	d := readyDecorator(t, fc)
	cap := &captureSink{}
	for range 10 {
		_ = d.Wrap(cap).Deliver(context.Background(), textEvent("mensaje ambiguo larguito"))
	}
	if cap.lastEvent().Intent != nil {
		t.Errorf("'desconocido' no debe anotar intención")
	}
	if d.Circuit() != "closed" {
		t.Errorf("'desconocido' es un éxito: el circuito debe seguir cerrado, got %q", d.Circuit())
	}
}

func TestDeliver_NoElegible_Fastlane(t *testing.T) {
	fc := &fakeClassifier{res: classifier.Classification{Intent: "crear_pedido"}}
	d := readyDecorator(t, fc)
	cap := &captureSink{}

	// Grupo, propio, vacío: no elegibles. "2": carril rápido (número corto).
	cases := []domain.InboundEvent{
		{Text: "quiero pizza", IsGroup: true},
		{Text: "quiero pizza", IsFromMe: true},
		{Text: ""},
		{Text: "2"},
	}
	for _, ev := range cases {
		_ = d.Wrap(cap).Deliver(context.Background(), ev)
	}
	if fc.callCount() != 0 {
		t.Errorf("no elegibles / fastlane no deben clasificar (calls=%d)", fc.callCount())
	}
}

func TestCircuitBreaker_AbreTras5Fallos_YMedioAbierto(t *testing.T) {
	fc := &fakeClassifier{err: errors.New("ollama caído")}
	d := readyDecorator(t, fc)
	// Reloj controlado para no depender de esperas reales.
	now := time.Unix(1_700_000_000, 0)
	d.now = func() time.Time { return now }
	cap := &captureSink{}

	// 5 fallos consecutivos ⇒ circuito abierto.
	for range 5 {
		_ = d.Wrap(cap).Deliver(context.Background(), textEvent("quiero algo de comer"))
	}
	if d.Circuit() != "open" {
		t.Fatalf("tras 5 fallos el circuito debe estar abierto, got %q", d.Circuit())
	}
	callsAfterOpen := fc.callCount()

	// Con el circuito abierto, un nuevo mensaje NO llama al clasificador (degrada sin castigar timeout).
	_ = d.Wrap(cap).Deliver(context.Background(), textEvent("otro mensaje de prueba"))
	if fc.callCount() != callsAfterOpen {
		t.Errorf("circuito abierto no debe clasificar")
	}
	if cap.lastEvent().Intent != nil {
		t.Errorf("circuito abierto debe entregar sin intención")
	}

	// Pasada la ventana (60 s) ⇒ medio-abierto: deja pasar UN sondeo. Si vuelve a fallar, reabre.
	now = now.Add(61 * time.Second)
	if d.Circuit() != "half-open" {
		t.Fatalf("tras la ventana el circuito debe estar medio-abierto, got %q", d.Circuit())
	}
	_ = d.Wrap(cap).Deliver(context.Background(), textEvent("sondeo de medio abierto"))
	if fc.callCount() != callsAfterOpen+1 {
		t.Errorf("medio-abierto debe permitir un sondeo (calls=%d, esperado %d)", fc.callCount(), callsAfterOpen+1)
	}

	// Éxito tras recuperar Ollama ⇒ cierra.
	fc.mu.Lock()
	fc.err = nil
	fc.res = classifier.Classification{Intent: "crear_pedido", Confidence: 0.9}
	fc.mu.Unlock()
	now = now.Add(61 * time.Second) // vuelve a medio-abierto para permitir el sondeo de recuperación
	_ = d.Wrap(cap).Deliver(context.Background(), textEvent("recuperado ya funciona"))
	if d.Circuit() != "closed" {
		t.Errorf("un éxito en medio-abierto debe cerrar el circuito, got %q", d.Circuit())
	}
}

func TestSetConfig_RecargaClasificadorYMarcaReady(t *testing.T) {
	fc := &fakeClassifier{}
	d := New(fc, 100*time.Millisecond, testLogger())
	if d.ConfigVersion() != "" {
		t.Errorf("versión inicial no vacía")
	}
	d.SetConfig(&intents.Config{Version: "v7"}, "v7")
	if fc.reload != 1 {
		t.Errorf("SetConfig debe recargar el clasificador una vez (reload=%d)", fc.reload)
	}
	if d.ConfigVersion() != "v7" {
		t.Errorf("ConfigVersion: got %q want v7", d.ConfigVersion())
	}
}
