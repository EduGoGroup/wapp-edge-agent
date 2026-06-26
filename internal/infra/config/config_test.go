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

func TestLoad_CloudLinkTLSFromYAML(t *testing.T) {
	// Los campos TLS/lease se leen del YAML bajo cloudlink.
	path := writeTempYAML(t, `
cloudlink:
  tls_cert: /etc/wapp/edge.crt
  tls_key: /etc/wapp/edge.key
  tls_ca: /etc/wapp/ca.pem
  server_name: cloud.wapp.example
  lease_pubkey_path: /etc/wapp/lease.pub
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}

	if cfg.CloudLink.TLSCert != "/etc/wapp/edge.crt" {
		t.Errorf("TLSCert: got %q", cfg.CloudLink.TLSCert)
	}
	if cfg.CloudLink.TLSKey != "/etc/wapp/edge.key" {
		t.Errorf("TLSKey: got %q", cfg.CloudLink.TLSKey)
	}
	if cfg.CloudLink.TLSCA != "/etc/wapp/ca.pem" {
		t.Errorf("TLSCA: got %q", cfg.CloudLink.TLSCA)
	}
	if cfg.CloudLink.ServerName != "cloud.wapp.example" {
		t.Errorf("ServerName: got %q", cfg.CloudLink.ServerName)
	}
	if cfg.CloudLink.LeasePubKeyPath != "/etc/wapp/lease.pub" {
		t.Errorf("LeasePubKeyPath: got %q", cfg.CloudLink.LeasePubKeyPath)
	}
}

func TestLoad_CloudLinkTLSEnvOverridesYAML(t *testing.T) {
	// El entorno con prefijo WAPP_AGENT_CLOUDLINK_* sobreescribe los campos TLS/lease.
	path := writeTempYAML(t, `
cloudlink:
  tls_cert: /from/yaml/edge.crt
  tls_key: /from/yaml/edge.key
  tls_ca: /from/yaml/ca.pem
  server_name: yaml.wapp.example
  lease_pubkey_path: /from/yaml/lease.pub
`)

	t.Setenv(EnvPrefix+"CLOUDLINK_TLS_CERT", "/from/env/edge.crt")
	t.Setenv(EnvPrefix+"CLOUDLINK_TLS_KEY", "/from/env/edge.key")
	t.Setenv(EnvPrefix+"CLOUDLINK_TLS_CA", "/from/env/ca.pem")
	t.Setenv(EnvPrefix+"CLOUDLINK_SERVER_NAME", "env.wapp.example")
	t.Setenv(EnvPrefix+"CLOUDLINK_LEASE_PUBKEY_PATH", "/from/env/lease.pub")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}

	if cfg.CloudLink.TLSCert != "/from/env/edge.crt" {
		t.Errorf("env override TLSCert: got %q", cfg.CloudLink.TLSCert)
	}
	if cfg.CloudLink.TLSKey != "/from/env/edge.key" {
		t.Errorf("env override TLSKey: got %q", cfg.CloudLink.TLSKey)
	}
	if cfg.CloudLink.TLSCA != "/from/env/ca.pem" {
		t.Errorf("env override TLSCA: got %q", cfg.CloudLink.TLSCA)
	}
	if cfg.CloudLink.ServerName != "env.wapp.example" {
		t.Errorf("env override ServerName: got %q", cfg.CloudLink.ServerName)
	}
	if cfg.CloudLink.LeasePubKeyPath != "/from/env/lease.pub" {
		t.Errorf("env override LeasePubKeyPath: got %q", cfg.CloudLink.LeasePubKeyPath)
	}
}

func TestLoad_CloudLinkTLSEnvOnlyOverDefaults(t *testing.T) {
	// Sin estos campos en YAML ni env: deben quedar vacíos (sin default).
	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}

	if cfg.CloudLink.TLSCert != "" {
		t.Errorf("TLSCert default vacío: got %q", cfg.CloudLink.TLSCert)
	}
	if cfg.CloudLink.ServerName != "" {
		t.Errorf("ServerName default vacío: got %q", cfg.CloudLink.ServerName)
	}
	if cfg.CloudLink.LeasePubKeyPath != "" {
		t.Errorf("LeasePubKeyPath default vacío: got %q", cfg.CloudLink.LeasePubKeyPath)
	}
}
