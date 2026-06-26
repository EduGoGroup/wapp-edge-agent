package logger

import (
	"log/slog"
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

func TestNew_TextFormatDefaultLevel(t *testing.T) {
	// Construccion con una config minima (nivel desconocido -> info, texto).
	log := New(edgeconfig.Config{})
	if log == nil {
		t.Fatal("New devolvio un Logger nil")
	}
	log.Warn("aviso")
}
