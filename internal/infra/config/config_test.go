package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("escribiendo YAML temporal: %v", err)
	}
	return path
}

func TestLoad_Defaults(t *testing.T) {
	// Sin archivo y sin entorno: deben quedar los valores por defecto.
	cfg, err := Load(filepath.Join(t.TempDir(), "no-existe.yaml"))
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}

	want := defaults()
	if cfg != want {
		t.Fatalf("defaults: got %+v, want %+v", cfg, want)
	}
}

func TestLoad_FromYAML(t *testing.T) {
	path := writeTempYAML(t, `
log_level: debug
log_json: true
db_path: /var/lib/wapp/edge.db
dek_path: /etc/wapp/dek.key
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}

	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel: got %q, want %q", cfg.LogLevel, "debug")
	}
	if !cfg.LogJSON {
		t.Errorf("LogJSON: got false, want true")
	}
	if cfg.DBPath != "/var/lib/wapp/edge.db" {
		t.Errorf("DBPath: got %q", cfg.DBPath)
	}
	if cfg.DEKPath != "/etc/wapp/dek.key" {
		t.Errorf("DEKPath: got %q", cfg.DEKPath)
	}
}

func TestLoad_EnvOverridesYAML(t *testing.T) {
	path := writeTempYAML(t, `
log_level: debug
log_json: false
db_path: /from/yaml.db
`)

	t.Setenv(EnvPrefix+"LOG_LEVEL", "error")
	t.Setenv(EnvPrefix+"LOG_JSON", "true")
	t.Setenv(EnvPrefix+"DB_PATH", "/from/env.db")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}

	if cfg.LogLevel != "error" {
		t.Errorf("env override LogLevel: got %q, want %q", cfg.LogLevel, "error")
	}
	if !cfg.LogJSON {
		t.Errorf("env override LogJSON: got false, want true")
	}
	if cfg.DBPath != "/from/env.db" {
		t.Errorf("env override DBPath: got %q, want %q", cfg.DBPath, "/from/env.db")
	}
	// dek_path no estaba ni en YAML ni en env: debe quedar el default.
	if cfg.DEKPath != defaults().DEKPath {
		t.Errorf("DEKPath default: got %q, want %q", cfg.DEKPath, defaults().DEKPath)
	}
}

func TestLoad_EnvOnlyOverDefaults(t *testing.T) {
	// Sin archivo: el entorno debe sobreescribir los defaults.
	t.Setenv(EnvPrefix+"DEK_PATH", "/only/env/dek.key")

	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}

	if cfg.DEKPath != "/only/env/dek.key" {
		t.Errorf("DEKPath: got %q, want %q", cfg.DEKPath, "/only/env/dek.key")
	}
	if cfg.LogLevel != defaults().LogLevel {
		t.Errorf("LogLevel default: got %q", cfg.LogLevel)
	}
}

func TestLoad_BadYAML(t *testing.T) {
	path := writeTempYAML(t, "log_level: [unbalanced")

	if _, err := Load(path); err == nil {
		t.Fatal("Load deberia fallar con YAML invalido, pero devolvio nil")
	}
}

func TestLoad_CloudLinkEnrollFromYAML(t *testing.T) {
	// Los campos de enrolamiento (T6) se leen del YAML bajo cloudlink.
	path := writeTempYAML(t, `
cloudlink:
  enrollment_endpoint: localhost:8444
  activation_code: code-yaml
  edge_id: edge-yaml
  tls_ca: /etc/wapp/ca.pem
  tls_cert: /etc/wapp/edge.crt
  tls_key: /etc/wapp/edge.key
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}

	if cfg.CloudLink.EnrollmentEndpoint != "localhost:8444" {
		t.Errorf("EnrollmentEndpoint: got %q", cfg.CloudLink.EnrollmentEndpoint)
	}
	if cfg.CloudLink.ActivationCode != "code-yaml" {
		t.Errorf("ActivationCode: got %q", cfg.CloudLink.ActivationCode)
	}
	if cfg.CloudLink.EdgeID != "edge-yaml" {
		t.Errorf("EdgeID: got %q", cfg.CloudLink.EdgeID)
	}
	if cfg.CloudLink.TLSCA != "/etc/wapp/ca.pem" {
		t.Errorf("TLSCA: got %q", cfg.CloudLink.TLSCA)
	}
}

func TestLoad_CloudLinkEnrollEnvOverridesYAML(t *testing.T) {
	// El entorno con prefijo WAPP_AGENT_CLOUDLINK_* sobreescribe los campos de enrolamiento.
	path := writeTempYAML(t, `
cloudlink:
  enrollment_endpoint: localhost:8444
  activation_code: code-yaml
  edge_id: edge-yaml
`)

	t.Setenv(EnvPrefix+"CLOUDLINK_ENROLLMENT_ENDPOINT", "gw.dev:9444")
	t.Setenv(EnvPrefix+"CLOUDLINK_ACTIVATION_CODE", "code-env")
	t.Setenv(EnvPrefix+"CLOUDLINK_EDGE_ID", "edge-env")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}

	if cfg.CloudLink.EnrollmentEndpoint != "gw.dev:9444" {
		t.Errorf("env override EnrollmentEndpoint: got %q", cfg.CloudLink.EnrollmentEndpoint)
	}
	if cfg.CloudLink.ActivationCode != "code-env" {
		t.Errorf("env override ActivationCode: got %q", cfg.CloudLink.ActivationCode)
	}
	if cfg.CloudLink.EdgeID != "edge-env" {
		t.Errorf("env override EdgeID: got %q", cfg.CloudLink.EdgeID)
	}
}
