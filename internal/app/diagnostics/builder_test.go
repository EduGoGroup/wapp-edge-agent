package diagnostics

// builder_test.go — GATE ZERO-KNOWLEDGE VERIFICABLE (Plan 031 T8, ADR-0007/ADR-0023): la prueba que
// convierte la política "logs/bundles sin secretos" en una verificación automática. Siembra material
// sensible CONOCIDO (DEK hex, blob sellado base64, token) tanto en el ring buffer de logs como en el dump
// de goroutines, arma el bundle y ESCANEA que nada de eso aparezca en log_tail, goroutine_dump ni
// subsystems_json. Si el escaneo detecta una fuga, el scrubbing (Scrub) debe haberla redactado; si aun así
// aparece, el test FALLA (regresión de zero-knowledge). Cubre además el scrubbing unitario y el truncado.

import (
	"context"
	"strings"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
)

// Material sensible SEMBRADO: exactamente las formas que el bundle jamás debe filtrar (ADR-0007). El gate
// verifica que ninguno sobrevive al Scrub.
const (
	seededDEKHex    = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90" // 64 hex = 32 bytes
	seededSealedB64 = "c2VhbGVkLWRlay1ibG9iLXRoYXQtbXVzdC1uZXZlci1sZWFrLXBheWxvYWQ="     // base64 largo
	seededHexShort  = "deadbeefcafebabedeadbeefcafebabe"                                 // 32 hex = 16 bytes
)

// fakeLogs satisface LogTailer con líneas fijas.
type fakeLogs struct{ lines []string }

func (f fakeLogs) Tail(n int) []string {
	if n <= 0 || n > len(f.lines) {
		n = len(f.lines)
	}
	return f.lines[len(f.lines)-n:]
}

// fakeReporter satisface Reporter con reports fijos (metadatos, sin secretos).
type fakeReporter struct{ reports map[string]health.Report }

func (f fakeReporter) Reports(context.Context) map[string]health.Report { return f.reports }
func (f fakeReporter) DaemonUptimeS() int64                             { return 123 }
func (f fakeReporter) Version() string                                  { return "1.0.0-test" }

// TestZKGate_NoSeededSecretInBundle es el GATE: con material sensible sembrado en logs y dump, el bundle
// generado NO puede contener ninguno de esos valores en ninguno de sus tres campos.
func TestZKGate_NoSeededSecretInBundle(t *testing.T) {
	logs := fakeLogs{lines: []string{
		"INFO arranque ok",
		"DEBUG cargando DEK dek=" + seededDEKHex + " (no debería loguearse)",
		"DEBUG sobre sellado enc_payload=" + seededSealedB64,
		"WARN indexKey=" + seededHexShort,
		"INFO sesión lista",
	}}
	reporter := fakeReporter{reports: map[string]health.Report{
		"sess-1": {SocketState: "connected", BinaryVersion: "1.0.0-test", OutboxDepth: 0},
	}}
	b := NewBuilder(logs, reporter, 100)
	// Inyecta un dump de goroutines que TAMBIÉN filtra los secretos (defensa en profundidad).
	b.stack = func() string {
		return "goroutine 7 [running]:\n\tkeycustody.load(dek=" + seededDEKHex + ")\n\tsealed=" + seededSealedB64
	}

	bundle := b.Build(context.Background(), "full")

	secrets := []string{seededDEKHex, seededSealedB64, seededHexShort}
	fields := map[string]string{
		"log_tail":        bundle.LogTail,
		"goroutine_dump":  bundle.GoroutineDump,
		"subsystems_json": bundle.SubsystemsJSON,
	}
	for fname, fval := range fields {
		for _, sec := range secrets {
			if strings.Contains(fval, sec) {
				t.Errorf("FUGA ZERO-KNOWLEDGE: el campo %s del bundle contiene material sensible sembrado (%.12s…)", fname, sec)
			}
		}
	}
	// El scrubbing debe haber dejado su marca donde había secretos (prueba de que corrió, no de que el
	// input estaba limpio por casualidad).
	if !strings.Contains(bundle.LogTail, redacted) {
		t.Errorf("el log_tail no muestra la marca de redacción %q: el scrubbing no se aplicó", redacted)
	}
	if !strings.Contains(bundle.GoroutineDump, redacted) {
		t.Errorf("el goroutine_dump no muestra la marca de redacción %q", redacted)
	}
	// Contenido operativo legítimo SÍ sobrevive (el scrub no arrasa todo).
	if !strings.Contains(bundle.LogTail, "arranque ok") {
		t.Errorf("el scrubbing borró contenido legítimo del log_tail")
	}
}

// TestScrub_RedactsCryptoMaterial: unidad del scrubber sobre hex/base64 largos y respeto del texto normal.
func TestScrub_RedactsCryptoMaterial(t *testing.T) {
	cases := []struct {
		name string
		in   string
		leak string // subcadena que NO debe quedar
		keep string // subcadena que SÍ debe quedar
	}{
		{"dek hex", "dek=" + seededDEKHex, seededDEKHex, "dek="},
		{"sealed base64", "enc=" + seededSealedB64, seededSealedB64, "enc="},
		{"hex corto 16B", "idx=" + seededHexShort, seededHexShort, "idx="},
		{"texto normal", "sesión S conectada, outbox=3", "", "conectada"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Scrub(c.in)
			if c.leak != "" && strings.Contains(got, c.leak) {
				t.Errorf("Scrub(%q) filtró %q", c.in, c.leak)
			}
			if !strings.Contains(got, c.keep) {
				t.Errorf("Scrub(%q) borró lo legítimo %q: %q", c.in, c.keep, got)
			}
		})
	}
}

// TestBuild_TruncatesInOrigin: un log/dump enorme se recorta bajo el tope y muestra la marca de truncado.
// El contenido lleva espacios/saltos a propósito para que el Scrub NO lo colapse (una tira alfanumérica
// larga sí encajaría en el patrón base64): aquí se prueba el TRUNCADO, no el scrubbing.
func TestBuild_TruncatesInOrigin(t *testing.T) {
	// Muchas líneas cortas y legibles ⇒ el total supera el tope sin disparar el scrub.
	nLines := (maxLogTailBytes*2)/len("linea de log operativa numero ") + 1
	lines := make([]string, nLines)
	for i := range lines {
		lines[i] = "linea de log operativa numero"
	}
	b := NewBuilder(fakeLogs{lines: lines}, nil, nLines)
	b.stack = func() string { return strings.Repeat("goroutine 1 [running] en algun punto\n", maxGoroutineBytes/20) }

	bundle := b.Build(context.Background(), "full")
	if len(bundle.LogTail) > maxLogTailBytes {
		t.Errorf("log_tail = %d bytes, supera el tope %d", len(bundle.LogTail), maxLogTailBytes)
	}
	if len(bundle.GoroutineDump) > maxGoroutineBytes {
		t.Errorf("goroutine_dump = %d bytes, supera el tope %d", len(bundle.GoroutineDump), maxGoroutineBytes)
	}
	if !strings.Contains(bundle.LogTail, "truncado en origen") {
		t.Errorf("log_tail truncado sin marca de truncado")
	}
}

// TestSubsystemsJSON_IncludesSessions: el snapshot de subsistemas serializa el daemon y las sesiones.
func TestSubsystemsJSON_IncludesSessions(t *testing.T) {
	reporter := fakeReporter{reports: map[string]health.Report{
		"sess-abc": {SocketState: "degraded", DegradedReason: "dek_load_timeout", OutboxDepth: 5, BinaryVersion: "1.0.0-test"},
	}}
	b := NewBuilder(nil, reporter, 1)
	js := b.Build(context.Background(), "subsystems").SubsystemsJSON
	for _, want := range []string{"sess-abc", "degraded", "dek_load_timeout", "1.0.0-test", `"uptime_s":123`} {
		if !strings.Contains(js, want) {
			t.Errorf("subsystems_json no contiene %q: %s", want, js)
		}
	}
}
