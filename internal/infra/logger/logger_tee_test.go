package logger

import (
	"bytes"
	"strings"
	"testing"

	edgeconfig "github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
)

// TestNewWithWritersTeesWithoutLosingBase verifica el "tee": la salida base (stdout en producción)
// recibe la línea formateada Y el sink recibe EXACTAMENTE lo mismo. Se usa newWithWriters con una
// base inyectada para no tocar el stdout real del proceso.
func TestNewWithWritersTeesWithoutLosingBase(t *testing.T) {
	var base, sink bytes.Buffer
	cfg := edgeconfig.Config{LogLevel: "info", LogJSON: false}

	log := newWithWriters(cfg, &base, &sink)
	log.Info("hola tee", "k", "v")

	if base.Len() == 0 {
		t.Fatal("la salida base (stdout) no recibió nada: el tee perdió stdout")
	}
	if base.String() != sink.String() {
		t.Fatalf("tee divergente:\n base = %q\n sink = %q", base.String(), sink.String())
	}
	if !strings.Contains(base.String(), "hola tee") {
		t.Fatalf("la línea no contiene el mensaje: %q", base.String())
	}
}

// TestNewWithWritersNilSinkKeepsBaseOnly confirma que sin sink la base recibe la salida igual que
// New (comportamiento actual intacto).
func TestNewWithWritersNilSinkKeepsBaseOnly(t *testing.T) {
	var base bytes.Buffer
	cfg := edgeconfig.Config{LogLevel: "info", LogJSON: false}

	log := newWithWriters(cfg, &base, nil)
	log.Info("solo base")

	if !strings.Contains(base.String(), "solo base") {
		t.Fatalf("la base no recibió la línea: %q", base.String())
	}
}

// TestNewWithWritersRespectsLevel confirma que el filtrado por nivel ocurre antes del tee: una
// línea por debajo del nivel no llega ni a base ni a sink.
func TestNewWithWritersRespectsLevel(t *testing.T) {
	var base, sink bytes.Buffer
	cfg := edgeconfig.Config{LogLevel: "warn", LogJSON: false}

	log := newWithWriters(cfg, &base, &sink)
	log.Info("debajo del nivel")

	if base.Len() != 0 || sink.Len() != 0 {
		t.Fatalf("una línea por debajo del nivel no debe emitirse; base=%q sink=%q", base.String(), sink.String())
	}
}
