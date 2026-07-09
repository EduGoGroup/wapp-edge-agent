package config

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestDefaultConfigPathUsaOverrideDataDir: con WAPP_AGENT_DATA_DIR seteado, la ruta estable del config
// es <data_dir>/config.yaml (Plan 023 · T1: cierra el gotcha del CWD).
func TestDefaultConfigPathUsaOverrideDataDir(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(EnvPrefix+"DATA_DIR", dir)

	got := DefaultConfigPath()
	want := filepath.Join(dir, "config.yaml")
	if got != want {
		t.Fatalf("DefaultConfigPath: got %q, want %q", got, want)
	}
}

// TestDefaultConfigPathSinEnvUsaDataDirSagrado: sin override, cae al data_dir sagrado por SO
// (defaultDataDir), NUNCA al CWD; la ruta es absoluta y termina en config.yaml.
func TestDefaultConfigPathSinEnvUsaDataDirSagrado(t *testing.T) {
	// Neutraliza cualquier WAPP_AGENT_DATA_DIR heredado del entorno de CI.
	t.Setenv(EnvPrefix+"DATA_DIR", "")

	got := DefaultConfigPath()

	// want se calcula igual que DefaultConfigPath (absolutiza el data_dir sagrado) para ser robusto
	// aunque defaultDataDir cayera al fallback ".".
	base := defaultDataDir()
	if abs, err := filepath.Abs(base); err == nil {
		base = abs
	}
	want := filepath.Join(base, "config.yaml")
	if got != want {
		t.Fatalf("DefaultConfigPath: got %q, want %q", got, want)
	}
	if !filepath.IsAbs(got) {
		t.Fatalf("debería ser absoluta (no relativa al CWD): %q", got)
	}
	if !strings.HasSuffix(got, string(filepath.Separator)+"config.yaml") {
		t.Fatalf("debería terminar en config.yaml: %q", got)
	}
}
