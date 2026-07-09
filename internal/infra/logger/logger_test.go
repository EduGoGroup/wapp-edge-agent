package logger

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	edgeconfig "github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":    slog.LevelDebug,
		"DEBUG":    slog.LevelDebug,
		"info":     slog.LevelInfo,
		"":         slog.LevelInfo,
		"loquesea": slog.LevelInfo,
		"warn":     slog.LevelWarn,
		"warning":  slog.LevelWarn,
		" Error ":  slog.LevelError,
	}

	for in, want := range cases {
		if got := ParseLevel(in); got != want {
			t.Errorf("ParseLevel(%q): got %v, want %v", in, got, want)
		}
	}
}

func TestNew_ReturnsUsableLogger(t *testing.T) {
	cfg := edgeconfig.Config{LogLevel: "debug", LogJSON: true}

	log := New(cfg)
	if log == nil {
		t.Fatal("New devolvio un Logger nil")
	}

	// No debe panicar al emitir ni al derivar un hijo con With.
	log.Info("smoke", "k", "v")
	if child := log.With("scope", "test"); child == nil {
		t.Fatal("With devolvio nil")
	}
}

// TestNew_WritesToLogFile verifica el enganche de WAPP_LOG_FILE (Plan 024 · T1): con la env
// apuntando a un archivo, una línea logueada aparece en ese archivo (además de stdout). Cubre
// también la creación del dir padre (0700) que hacen los lanzadores Win/Linux.
func TestNew_WritesToLogFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "logs", "edge.log")
	t.Setenv("WAPP_LOG_FILE", path)

	log := New(edgeconfig.Config{LogLevel: "info"})
	log.Info("hola-archivo", "k", "v")

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("leyendo el log file %q: %v", path, err)
	}
	if !strings.Contains(string(data), "hola-archivo") {
		t.Fatalf("el log file no contiene la linea esperada; contenido: %q", string(data))
	}
}

// TestNew_LogFileUnset comprueba que sin WAPP_LOG_FILE el constructor sigue funcionando (no crea
// archivo alguno) — la ruta macOS, donde el LaunchAgent redirige por su cuenta.
func TestNew_LogFileUnset(t *testing.T) {
	t.Setenv("WAPP_LOG_FILE", "")
	log := New(edgeconfig.Config{})
	if log == nil {
		t.Fatal("New devolvio nil sin WAPP_LOG_FILE")
	}
	log.Info("sin-archivo")
}

func TestNew_TextFormatDefaultLevel(t *testing.T) {
	// Construccion con una config minima (nivel desconocido -> info, texto).
	log := New(edgeconfig.Config{})
	if log == nil {
		t.Fatal("New devolvio un Logger nil")
	}
	log.Warn("aviso")
}
