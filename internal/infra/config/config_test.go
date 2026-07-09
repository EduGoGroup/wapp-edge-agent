package config

import (
	"os"
	"path/filepath"
	"strings"
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
	// Sin archivo y sin entorno: deben quedar los valores por defecto. El data_dir sale del default
	// sagrado (defaultDataDir) y Load lo absolutiza; como defaultDataDir ya es absoluto y filepath.Abs
	// es idempotente, la estructura cargada coincide con defaults().
	cfg, err := Load(filepath.Join(t.TempDir(), "no-existe.yaml"))
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}

	want := defaults()
	if cfg != want {
		t.Fatalf("defaults: got %+v, want %+v", cfg, want)
	}
	// El default de data_dir NO es "." y ES una ruta absoluta (MP-02, D1/D2): el store no depende del CWD.
	if cfg.DataDir == "." {
		t.Fatalf("data_dir por defecto no debe ser \".\" (ruta sagrada MP-02): got %q", cfg.DataDir)
	}
	if !filepath.IsAbs(cfg.DataDir) {
		t.Fatalf("data_dir por defecto debe ser absoluto: got %q", cfg.DataDir)
	}
}

// TestDefaultDataDir_AbsoluteInHome: la ruta sagrada por defecto es absoluta, vive en el home del
// usuario (no en rutas de sistema como /var/lib que exigirían root) y no es "." (MP-02, D1).
func TestDefaultDataDir_AbsoluteInHome(t *testing.T) {
	got := defaultDataDir()
	if got == "." {
		t.Fatalf("defaultDataDir no debe ser \".\": got %q", got)
	}
	if !filepath.IsAbs(got) {
		t.Fatalf("defaultDataDir debe ser absoluto: got %q", got)
	}
	if strings.HasPrefix(got, "/var/lib") || strings.HasPrefix(got, "/etc") {
		t.Fatalf("defaultDataDir no debe caer en rutas de sistema con permisos root: got %q", got)
	}
	// Debe colgar del home / carpeta de config del usuario.
	home, herr := os.UserHomeDir()
	cfgBase, cerr := os.UserConfigDir()
	inHome := (herr == nil && strings.HasPrefix(got, home)) || (cerr == nil && strings.HasPrefix(got, cfgBase))
	if !inHome {
		t.Fatalf("defaultDataDir debe vivir en el home del usuario: got %q (home=%q cfg=%q)", got, home, cfgBase)
	}
}

// TestLoad_DataDirRelativeIsAbsolutized: un data_dir RELATIVO (por env) se normaliza a absoluto
// respecto al CWD tras Load, y la operación es idempotente (MP-02, D2).
func TestLoad_DataDirRelativeIsAbsolutized(t *testing.T) {
	tmp := t.TempDir()
	t.Chdir(tmp)
	t.Setenv(EnvPrefix+"DATA_DIR", "rel/store")

	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}

	if !filepath.IsAbs(cfg.DataDir) {
		t.Fatalf("data_dir relativo debe absolutizarse: got %q", cfg.DataDir)
	}
	want := filepath.Join(tmp, "rel", "store")
	if cfg.DataDir != want {
		t.Fatalf("data_dir absolutizado: got %q, want %q", cfg.DataDir, want)
	}
	// Idempotencia: Abs de una ruta ya absoluta no la cambia.
	if again, _ := filepath.Abs(cfg.DataDir); again != cfg.DataDir {
		t.Fatalf("filepath.Abs no es idempotente sobre %q: got %q", cfg.DataDir, again)
	}
}

// TestLoad_DataDirEnvOverrideAbsoluteRespected: un override absoluto por WAPP_AGENT_DATA_DIR se
// respeta tal cual (MP-02, D1/D2).
func TestLoad_DataDirEnvOverrideAbsoluteRespected(t *testing.T) {
	abs := filepath.Join(t.TempDir(), "sagrado")
	t.Setenv(EnvPrefix+"DATA_DIR", abs)

	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}
	if cfg.DataDir != abs {
		t.Fatalf("override absoluto de data_dir: got %q, want %q", cfg.DataDir, abs)
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

// TestLoad_MultiDevicePerAccount_Clamp (Plan 022 T5, §10.F): la opción es off por defecto (1) y se CLAMP a
// [1,4] — un valor por debajo sube a 1 y uno por encima del tope de WhatsApp baja a 4 (guardarraíl, no error).
func TestLoad_MultiDevicePerAccount_Clamp(t *testing.T) {
	// Default: off (1).
	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load default: %v", err)
	}
	if cfg.MultiDevicePerAccount != 1 {
		t.Fatalf("default MultiDevicePerAccount debería ser 1 (off), got %d", cfg.MultiDevicePerAccount)
	}

	cases := map[string]int{"0": 1, "-3": 1, "1": 1, "3": 3, "4": 4, "9": 4}
	for env, want := range cases {
		t.Run("env="+env, func(t *testing.T) {
			t.Setenv(EnvPrefix+"MULTIDEVICE_PER_ACCOUNT", env)
			cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
			if err != nil {
				t.Fatalf("Load(%s): %v", env, err)
			}
			if cfg.MultiDevicePerAccount != want {
				t.Fatalf("MULTIDEVICE_PER_ACCOUNT=%s → got %d, want %d (clamp [1,4])", env, cfg.MultiDevicePerAccount, want)
			}
		})
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

// TestLoad_PushName cubre el nuevo campo push_name (fallback de presencia, Plan 013 §10.D): default
// no vacío, lectura del YAML y override por WAPP_AGENT_PUSH_NAME.
func TestLoad_PushName(t *testing.T) {
	// Default no vacío: SendPresence necesita un PushName; sin config debe haber un fallback razonable.
	if defaults().PushName == "" {
		t.Fatalf("push_name por defecto no debe ser vacío (fallback de presencia)")
	}

	path := writeTempYAML(t, "push_name: Cuenta Real\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}
	if cfg.PushName != "Cuenta Real" {
		t.Errorf("push_name desde YAML: got %q, want %q", cfg.PushName, "Cuenta Real")
	}

	t.Setenv(EnvPrefix+"PUSH_NAME", "Desde Env")
	cfg, err = Load(path)
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}
	if cfg.PushName != "Desde Env" {
		t.Errorf("env override push_name: got %q, want %q", cfg.PushName, "Desde Env")
	}
}

// TestLoad_DBDialectAndDSN cubre el dialecto conmutable (Plan 022 T0): default "sqlite" y DSN vacío,
// lectura desde YAML y override por WAPP_AGENT_DB_DIALECT / WAPP_AGENT_DB_DSN.
func TestLoad_DBDialectAndDSN(t *testing.T) {
	// Default: sqlite embebido, sin DSN.
	if defaults().DBDialect != "sqlite" {
		t.Fatalf("db_dialect por defecto: got %q, want \"sqlite\"", defaults().DBDialect)
	}
	if defaults().DBDSN != "" {
		t.Fatalf("db_dsn por defecto debe ser vacío: got %q", defaults().DBDSN)
	}

	path := writeTempYAML(t, "db_dialect: postgres\ndb_dsn: postgres://u:p@h:5432/d\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}
	if cfg.DBDialect != "postgres" {
		t.Errorf("db_dialect desde YAML: got %q, want \"postgres\"", cfg.DBDialect)
	}
	if cfg.DBDSN != "postgres://u:p@h:5432/d" {
		t.Errorf("db_dsn desde YAML: got %q", cfg.DBDSN)
	}

	// Env override sobre el YAML.
	t.Setenv(EnvPrefix+"DB_DIALECT", "sqlite")
	t.Setenv(EnvPrefix+"DB_DSN", "/from/env.db")
	cfg, err = Load(path)
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}
	if cfg.DBDialect != "sqlite" {
		t.Errorf("env override db_dialect: got %q, want \"sqlite\"", cfg.DBDialect)
	}
	if cfg.DBDSN != "/from/env.db" {
		t.Errorf("env override db_dsn: got %q", cfg.DBDSN)
	}
}

// TestLoad_DBDialectInvalid: un dialecto no soportado (YAML/env) falla en Load, no se arrastra a abrir
// la BD.
func TestLoad_DBDialectInvalid(t *testing.T) {
	path := writeTempYAML(t, "db_dialect: mysql\n")
	if _, err := Load(path); err == nil {
		t.Fatal("Load debía fallar con un db_dialect no soportado")
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
