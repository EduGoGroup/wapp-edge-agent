package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app/cajero"
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
	// Sin archivo y sin entorno: deben quedar los valores por defecto. Se aísla el data_dir a un temp dir
	// vacío (WAPP_AGENT_DATA_DIR) para que la prueba sea HERMÉTICA: Load lee, si existe, el estado del
	// endpoint de runtime bajo data_dir (Plan 026 T3), y un enroll real en el home por defecto lo haría
	// no-determinista. El comportamiento del default sagrado se cubre en TestDefaultDataDir_AbsoluteInHome.
	dataDir := t.TempDir()
	t.Setenv(EnvPrefix+"DATA_DIR", dataDir)

	cfg, err := Load(filepath.Join(t.TempDir(), "no-existe.yaml"))
	if err != nil {
		t.Fatalf("Load devolvio error inesperado: %v", err)
	}

	want := defaults()
	want.DataDir = dataDir // el override de entorno reemplaza el default sagrado
	// La lista de colas del round-robin del cajero (Plan 051 O4 · T4.1) NO tiene default en defaults():
	// se DERIVA en Load, después de anclar data_dir, y su default es exactamente la lista de un elemento
	// con ese data_dir. Es la no-regresión de las instalaciones de una sola cola, aseverada aquí.
	want.Worker.DataDirs = []string{dataDir}
	// reflect.DeepEqual y no `!=`: Config dejó de ser comparable en T4.1 (WorkerConfig lleva un []string).
	if !reflect.DeepEqual(cfg, want) {
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

// TestLoad_RuntimePortDefaultAndOverride cubre el puerto de runtime (Plan 026 T3): default 8101 y
// override por WAPP_AGENT_CLOUDLINK_RUNTIME_PORT.
func TestLoad_RuntimePortDefaultAndOverride(t *testing.T) {
	// Default sin YAML ni env.
	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.CloudLink.RuntimePort != DefaultCloudLinkRuntimePort {
		t.Fatalf("RuntimePort default: got %q, want %q", cfg.CloudLink.RuntimePort, DefaultCloudLinkRuntimePort)
	}

	// Override por entorno.
	t.Setenv(EnvPrefix+"CLOUDLINK_RUNTIME_PORT", "9443")
	cfg, err = Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.CloudLink.RuntimePort != "9443" {
		t.Fatalf("RuntimePort override: got %q, want %q", cfg.CloudLink.RuntimePort, "9443")
	}
}

// TestLoad_LeaseShadowMode_DefaultsFailClosed ancla el default fail-closed del gate de lease (D-055.4,
// Plan 055): sin WAPP_AGENT_CLOUDLINK_LEASE_SHADOW_MODE en entorno ni YAML, LeaseShadowMode debe quedar en
// false (ENFORCE real). Invertir el default en Load (p.ej. loader.GetBool(..., true)) rompe esta prueba sin
// tocar el adapter — es la capa que faltaba cubrir (el adapter ya se prueba en lease_shadow_test.go).
func TestLoad_LeaseShadowMode_DefaultsFailClosed(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.CloudLink.LeaseShadowMode {
		t.Fatalf("default LeaseShadowMode debe ser false (fail-closed/ENFORCE), got true")
	}

	cases := map[string]bool{"1": true, "true": true, "0": false, "false": false}
	for env, want := range cases {
		t.Run("env="+env, func(t *testing.T) {
			t.Setenv(EnvPrefix+"CLOUDLINK_LEASE_SHADOW_MODE", env)
			cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
			if err != nil {
				t.Fatalf("Load(%s): %v", env, err)
			}
			if cfg.CloudLink.LeaseShadowMode != want {
				t.Fatalf("CLOUDLINK_LEASE_SHADOW_MODE=%s → got %v, want %v", env, cfg.CloudLink.LeaseShadowMode, want)
			}
		})
	}
}

// TestLoad_ColaLimits cubre los límites de la cola de entrantes (Plan 051, REQ-051.7): defaults 24 h /
// 50 000 filas, override por WAPP_AGENT_COLA_TTL_HOURS y WAPP_AGENT_COLA_MAX_ROWS, y el guardarraíl de que
// un valor no positivo cae al default — OJO: a diferencia del outbox, aquí 0 NO desactiva el TTL.
func TestLoad_ColaLimits(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.ColaTTLHours != DefaultColaTTLHours || cfg.ColaMaxRows != DefaultColaMaxRows {
		t.Fatalf("defaults de la cola: got ttl=%d max=%d, want ttl=%d max=%d",
			cfg.ColaTTLHours, cfg.ColaMaxRows, DefaultColaTTLHours, DefaultColaMaxRows)
	}

	t.Setenv(EnvPrefix+"COLA_TTL_HOURS", "6")
	t.Setenv(EnvPrefix+"COLA_MAX_ROWS", "1234")
	cfg, err = Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.ColaTTLHours != 6 || cfg.ColaMaxRows != 1234 {
		t.Fatalf("override de la cola: got ttl=%d max=%d, want ttl=6 max=1234", cfg.ColaTTLHours, cfg.ColaMaxRows)
	}

	// 0 no apaga el TTL de la cola (buzón de paso): cae al default, igual que un tope no positivo.
	t.Setenv(EnvPrefix+"COLA_TTL_HOURS", "0")
	t.Setenv(EnvPrefix+"COLA_MAX_ROWS", "-1")
	cfg, err = Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.ColaTTLHours != DefaultColaTTLHours || cfg.ColaMaxRows != DefaultColaMaxRows {
		t.Fatalf("guardarraíl de la cola: got ttl=%d max=%d, want ttl=%d max=%d",
			cfg.ColaTTLHours, cfg.ColaMaxRows, DefaultColaTTLHours, DefaultColaMaxRows)
	}
}

// TestLoad_ColaClaimMaxFilas cubre el TOPE DE FILAS POR CLAIM del worker-cajero (Plan 051 Ola 2, T2.1):
// default 20, override por WAPP_AGENT_COLA_CLAIM_MAX_FILAS, y el guardarraíl de que un valor no positivo
// cae al default.
//
// Por qué existe este test y no bastaba con el comentario del código: `ColaClaimMaxFilas` aún NO se cablea
// (lo consumirá el worker de otra tanda), así que el gate de config es HOY el único sitio donde este
// parámetro se puede romper sin que nada más se entere. Un 0 colándose hasta el claim significa un cajero
// que reclama cero filas: la cola deja de drenar EN SILENCIO, sin error y sin log.
func TestLoad_ColaClaimMaxFilas(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.ColaClaimMaxFilas != DefaultColaClaimMaxFilas {
		t.Fatalf("default del claim: got %d, want %d", cfg.ColaClaimMaxFilas, DefaultColaClaimMaxFilas)
	}

	t.Setenv(EnvPrefix+"COLA_CLAIM_MAX_FILAS", "7")
	cfg, err = Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.ColaClaimMaxFilas != 7 {
		t.Fatalf("override del claim: got %d, want 7", cfg.ColaClaimMaxFilas)
	}

	// Guardarraíl: 0 (y cualquier valor <=0) NO significa «sin tope» ni «cero filas»; cae al default.
	for _, crudo := range []string{"0", "-5"} {
		t.Run("no_positivo="+crudo, func(t *testing.T) {
			t.Setenv(EnvPrefix+"COLA_CLAIM_MAX_FILAS", crudo)
			cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
			if err != nil {
				t.Fatalf("Load(%s): %v", crudo, err)
			}
			if cfg.ColaClaimMaxFilas != DefaultColaClaimMaxFilas {
				t.Fatalf("guardarraíl del claim con %s: got %d, want %d",
					crudo, cfg.ColaClaimMaxFilas, DefaultColaClaimMaxFilas)
			}
		})
	}
}

// TestLoad_ColaLeaseSeconds cubre el LEASE del claim (Plan 051 Ola 2, T2.7): default 60 s, override por
// WAPP_AGENT_COLA_LEASE_SECONDS, y el guardarraíl de que un valor no positivo cae al default.
//
// El caso que de verdad importa aquí es el <=0: un lease de 0 s vencería INSTANTÁNEAMENTE y el barrido
// devolvería a `nuevo` lotes que un cajero vivo aún está clasificando — se pagaría una segunda inferencia
// por el mismo texto, en bucle. El guardarraíl es lo único que separa esa configuración del disco.
func TestLoad_ColaLeaseSeconds(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.ColaLeaseSeconds != DefaultColaLeaseSeconds {
		t.Fatalf("default del lease: got %d, want %d", cfg.ColaLeaseSeconds, DefaultColaLeaseSeconds)
	}

	t.Setenv(EnvPrefix+"COLA_LEASE_SECONDS", "15")
	cfg, err = Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.ColaLeaseSeconds != 15 {
		t.Fatalf("override del lease: got %d, want 15", cfg.ColaLeaseSeconds)
	}

	for _, crudo := range []string{"0", "-1"} {
		t.Run("no_positivo="+crudo, func(t *testing.T) {
			t.Setenv(EnvPrefix+"COLA_LEASE_SECONDS", crudo)
			cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
			if err != nil {
				t.Fatalf("Load(%s): %v", crudo, err)
			}
			if cfg.ColaLeaseSeconds != DefaultColaLeaseSeconds {
				t.Fatalf("guardarraíl del lease con %s: got %d, want %d",
					crudo, cfg.ColaLeaseSeconds, DefaultColaLeaseSeconds)
			}
		})
	}
}

// TestLoad_RuntimeEndpointStateFallback verifica que `serve` (config.Load) RELEE el endpoint de runtime
// persistido por el enroll en <data_dir>/cloudlink-endpoint cuando no viene por YAML/env (Plan 026 T3,
// cierra follow-up 023): así el stream se levanta sin edición manual del config.yaml.
func TestLoad_RuntimeEndpointStateFallback(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv(EnvPrefix+"DATA_DIR", dataDir)
	if err := os.WriteFile(RuntimeEndpointStatePath(dataDir), []byte("gateway.tudominio.com:8101\n"), 0o644); err != nil {
		t.Fatalf("escribir estado del endpoint: %v", err)
	}

	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.CloudLink.Endpoint != "gateway.tudominio.com:8101" {
		t.Fatalf("endpoint releído del estado: got %q, want %q", cfg.CloudLink.Endpoint, "gateway.tudominio.com:8101")
	}
}

// TestLoad_ExplicitEndpointWinsOverState verifica la PRECEDENCIA: un Endpoint explícito (YAML o env) gana
// sobre el archivo de estado persistido (el fallback solo aplica cuando el endpoint está vacío).
func TestLoad_ExplicitEndpointWinsOverState(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv(EnvPrefix+"DATA_DIR", dataDir)
	if err := os.WriteFile(RuntimeEndpointStatePath(dataDir), []byte("persistido:8101\n"), 0o644); err != nil {
		t.Fatalf("escribir estado del endpoint: %v", err)
	}

	// YAML explícito gana.
	yamlPath := writeTempYAML(t, "cloudlink:\n  endpoint: \"desde-yaml:7000\"\n")
	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.CloudLink.Endpoint != "desde-yaml:7000" {
		t.Fatalf("endpoint YAML debe ganar al estado: got %q", cfg.CloudLink.Endpoint)
	}

	// Env explícito gana.
	t.Setenv(EnvPrefix+"CLOUDLINK_ENDPOINT", "desde-env:7001")
	cfg, err = Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.CloudLink.Endpoint != "desde-env:7001" {
		t.Fatalf("endpoint env debe ganar al estado: got %q", cfg.CloudLink.Endpoint)
	}
}

// TestLoad_Worker_DefaultsYPrefijoPropio cubre el bloque del WORKER-CAJERO (Plan 051 Ola 2).
//
// Lo que de verdad vigila este test es EL PREFIJO: las variables del worker son WAPP_WORKER_*, NO
// WAPP_AGENT_*, porque el cajero es otro proceso con su propio bloque de entorno (design §4, T2.3).
// Un refactor que las meta bajo el loader general del Edge las renombraría en silencio: el operador
// exportaría WAPP_WORKER_MAX_CONCURRENT, nadie lo leería, y el semáforo se quedaría en el default sin
// que nada fallara. Por eso se comprueban las DOS direcciones (la buena aplica, la mala no).
func TestLoad_Worker_DefaultsYPrefijoPropio(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.Worker.MaxConcurrent != DefaultWorkerMaxConcurrent || DefaultWorkerMaxConcurrent != 1 {
		t.Fatalf("el semáforo por defecto es 1 (cerrado por la medición de la O0): got %d", cfg.Worker.MaxConcurrent)
	}
	if cfg.Worker.PollMS != DefaultWorkerPollMS {
		t.Fatalf("poll por defecto: got %d, want %d", cfg.Worker.PollMS, DefaultWorkerPollMS)
	}
	if cfg.Worker.MaxRunes != DefaultWorkerMaxRunes {
		t.Fatalf("techo de runas por defecto: got %d, want %d", cfg.Worker.MaxRunes, DefaultWorkerMaxRunes)
	}
	if cfg.Worker.NumThread != DefaultWorkerNumThread || cfg.Worker.NumPredict != DefaultWorkerNumPredict ||
		cfg.Worker.NumCtx != DefaultWorkerNumCtx {
		t.Fatalf("opciones de modelo por defecto: %+v", cfg.Worker)
	}

	if cfg.Worker.MaxIntentos != DefaultWorkerMaxIntentos || DefaultWorkerMaxIntentos != 3 {
		t.Fatalf("intentos por defecto: got %d, want 3 (dos reintentos gratis y a la tercera se abandona)",
			cfg.Worker.MaxIntentos)
	}
	if cfg.Worker.InferenceTimeoutMS != DefaultWorkerInferenceTimeoutMS {
		t.Fatalf("plazo de inferencia por defecto: got %d, want %d",
			cfg.Worker.InferenceTimeoutMS, DefaultWorkerInferenceTimeoutMS)
	}
	if cfg.Worker.StatsEveryMS != DefaultWorkerStatsEveryMS {
		t.Fatalf("latido de contadores por defecto: got %d, want %d",
			cfg.Worker.StatsEveryMS, DefaultWorkerStatsEveryMS)
	}

	// El prefijo BUENO aplica.
	t.Setenv(WorkerEnvPrefix+"MAX_CONCURRENT", "2")
	t.Setenv(WorkerEnvPrefix+"POLL_MS", "250")
	t.Setenv(WorkerEnvPrefix+"MAX_RUNES", "1500")
	t.Setenv(WorkerEnvPrefix+"NUM_THREAD", "3")
	t.Setenv(WorkerEnvPrefix+"NUM_PREDICT", "64")
	t.Setenv(WorkerEnvPrefix+"NUM_CTX", "2048")
	t.Setenv(WorkerEnvPrefix+"MAX_INTENTOS", "5")
	t.Setenv(WorkerEnvPrefix+"INFERENCE_TIMEOUT_MS", "9000")
	t.Setenv(WorkerEnvPrefix+"STATS_EVERY_MS", "60000")
	cfg, err = Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	esperado := WorkerConfig{
		MaxConcurrent: 2, PollMS: 250, MaxRunes: 1500, NumThread: 3, NumPredict: 64, NumCtx: 2048,
		MaxIntentos: 5, InferenceTimeoutMS: 9000, StatsEveryMS: 60000,
		// La lista de colas del round-robin (T4.1) no se toca en este test, así que vale su default: el
		// data_dir único de siempre, ya absolutizado.
		DataDirs: []string{cfg.DataDir},
	}
	// reflect.DeepEqual y NO `!=`: desde T4.1 WorkerConfig lleva un []string (DataDirs) y una struct con
	// slice NO ES COMPARABLE — `cfg.Worker != esperado` ya no compila. Comparar campo a campo dejaría el
	// test ciego al siguiente campo que se añada, que es justo la clase de agujero que este bloque existe
	// para tapar (vigila que el PREFIJO propio gobierne el struct ENTERO).
	if !reflect.DeepEqual(cfg.Worker, esperado) {
		t.Fatalf("override con el prefijo propio: got %+v, want %+v", cfg.Worker, esperado)
	}
}

// dirSinComas crea un directorio temporal cuya ruta NO contiene comas, y lo borra al terminar.
//
// Existe porque `t.TempDir()` deriva el nombre del directorio del NOMBRE DEL TEST, y los casos de
// TestLoad_WorkerDataDirs se llaman con comas a propósito ("lista por comas, con espacios y comas de
// más"). Esas comas acaban DENTRO de la ruta, `WAPP_WORKER_DATA_DIRS` se separa por comas, y la ruta
// se parte por las suyas propias: los dos temporales se truncan al mismo prefijo y el caso muere con
// un "apuntan al mismo directorio" que no tiene nada que ver con lo que estaba probando. Costó un
// rojo entender que el test se saboteaba con su propio nombre.
//
// Sólo hace falta en los casos que meten la ruta DENTRO de la lista; para `WAPP_AGENT_DATA_DIR`, que
// se lee como cadena entera, `t.TempDir()` sirve igual.
func dirSinComas(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "wapp-cola")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// TestLoad_WorkerDataDirs cubre la lista de colas del ROUND-ROBIN del cajero (Plan 051 Ola 4 · T4.1).
//
// Lo que vigila, por orden de gravedad si se rompiera:
//
//   - EL DEFAULT. Sin la variable, la lista es exactamente `[cfg.DataDir]`. Es la no-regresión del 99 %
//     de las instalaciones: una máquina con un solo data_dir no debe notar que el cajero sabe rotar.
//   - LA ABSOLUTIZACIÓN, y que ocurre DESPUÉS de anclar Config.DataDir. Una entrada relativa que quedara
//     sin anclar apuntaría al CWD del supervisor —no al del operador— y el cajero abriría la cola de un
//     directorio que nadie escribió, sin error y sin síntoma.
//   - LOS DUPLICADOS, que son ERROR y no deduplicación silenciosa: dos colas sobre el mismo fichero
//     SQLite son un round-robin que se turna consigo mismo.
//   - EL PREFIJO. Igual que el resto del bloque worker, es WAPP_WORKER_ y no WAPP_AGENT_.
func TestLoad_WorkerDataDirs(t *testing.T) {
	t.Run("sin la variable, la lista es el data_dir único de siempre", func(t *testing.T) {
		dataDir := t.TempDir()
		t.Setenv(EnvPrefix+"DATA_DIR", dataDir)

		cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !reflect.DeepEqual(cfg.Worker.DataDirs, []string{cfg.DataDir}) {
			t.Fatalf("el default es la lista de un elemento con el data_dir: got %v, want %v",
				cfg.Worker.DataDirs, []string{cfg.DataDir})
		}
	})

	t.Run("lista por comas, con espacios y comas de más", func(t *testing.T) {
		a, b := dirSinComas(t), dirSinComas(t)
		t.Setenv(EnvPrefix+"DATA_DIR", t.TempDir())
		// Espacios alrededor, una entrada vacía en medio y una coma colgando: es cómo queda una lista
		// tecleada a mano en una unidad de systemd, y no debe fallar por eso.
		t.Setenv(WorkerEnvPrefix+"DATA_DIRS", "  "+a+" , , "+b+" ,")

		cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !reflect.DeepEqual(cfg.Worker.DataDirs, []string{a, b}) {
			t.Fatalf("lista trimeada y sin vacíos: got %v, want %v", cfg.Worker.DataDirs, []string{a, b})
		}
	})

	t.Run("una lista entera vacía cae al default", func(t *testing.T) {
		dataDir := t.TempDir()
		t.Setenv(EnvPrefix+"DATA_DIR", dataDir)
		t.Setenv(WorkerEnvPrefix+"DATA_DIRS", " , , ")

		cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		// Una lista vacía NO puede significar «ninguna cola»: el cajero se quedaría vivo sin reclamar nada.
		if !reflect.DeepEqual(cfg.Worker.DataDirs, []string{cfg.DataDir}) {
			t.Fatalf("una lista vacía cae al default: got %v", cfg.Worker.DataDirs)
		}
	})

	t.Run("cada entrada se absolutiza", func(t *testing.T) {
		t.Setenv(EnvPrefix+"DATA_DIR", t.TempDir())
		t.Setenv(WorkerEnvPrefix+"DATA_DIRS", "relativo-a,otro-relativo")

		cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if len(cfg.Worker.DataDirs) != 2 {
			t.Fatalf("se esperaban 2 entradas: got %v", cfg.Worker.DataDirs)
		}
		for _, dir := range cfg.Worker.DataDirs {
			if !filepath.IsAbs(dir) {
				t.Errorf("cada entrada de la lista se ancla a ruta absoluta (el CWD del supervisor no es el "+
					"del operador): %q", dir)
			}
		}
	})

	t.Run("los duplicados son ERROR, no deduplicación silenciosa", func(t *testing.T) {
		dir := dirSinComas(t)
		t.Setenv(EnvPrefix+"DATA_DIR", t.TempDir())
		t.Setenv(WorkerEnvPrefix+"DATA_DIRS", dir+","+dir)

		_, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
		if err == nil {
			t.Fatal("dos entradas al mismo directorio deben fallar el arranque")
		}
		if !strings.Contains(err.Error(), dir) {
			t.Errorf("el error debe nombrar el directorio repetido: %v", err)
		}
	})

	t.Run("WAPP_AGENT_ no gobierna la lista del worker", func(t *testing.T) {
		dataDir := t.TempDir()
		t.Setenv(EnvPrefix+"DATA_DIR", dataDir)
		t.Setenv(EnvPrefix+"DATA_DIRS", t.TempDir()+","+t.TempDir())

		cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !reflect.DeepEqual(cfg.Worker.DataDirs, []string{cfg.DataDir}) {
			t.Fatalf("la lista es WAPP_WORKER_DATA_DIRS, no WAPP_AGENT_DATA_DIRS: got %v", cfg.Worker.DataDirs)
		}
	})
}

// TestLoad_InferenceTimeout_SigueVivoTrasElRetiroDelPush: el plazo de UNA inferencia
// (WAPP_WORKER_INFERENCE_TIMEOUT_MS, 15 s) NO se fue con el push.
//
// Su gemelo —el presupuesto de espera del despachador, WAPP_AGENT_INTENT_WAIT_MS— se retiró en T1.6-5
// con el ADR-0045, y el riesgo al retirarlo era arrastrar a éste por parecido de nombre. Son dos
// caminos distintos y sólo uno murió: bajo pull el Edge deja de RETENER mensajes, pero sigue habiendo
// una inferencia que acotar — la que el Edge SIRVE al Cloud (ADR-0045 §Decisión.2). El 15000 es ≈4× la
// p95 medida en la O0 (3.736 ms) y se clava aquí porque bajarlo aborta inferencias y abre el breaker.
func TestLoad_InferenceTimeout_SigueVivoTrasElRetiroDelPush(t *testing.T) {
	// 🔧 ERA 15000 HASTA EL 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-2). Aquel número se eligió como «≈4× la
	// p95 de la O0», y la O0 se midió CON EL VPS VACÍO; la muestra de campo que lo habría corregido está
	// CENSURADA por el propio techo (máximo 15,6 s contra un techo de 15,0 s). El argumento entero, con la
	// tabla de contención del VPS real que fija el 45.000, está en cajero.DefaultInferenceTimeoutMS.
	if DefaultWorkerInferenceTimeoutMS != 45000 {
		t.Fatalf("el default del worker es 45000 ms (cubre los máximos MEDIDOS en el VPS: 25,6 s / 36,5 s / "+
			"45,6 s): got %d", DefaultWorkerInferenceTimeoutMS)
	}
	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.Worker.InferenceTimeoutMS != DefaultWorkerInferenceTimeoutMS {
		t.Fatalf("plazo de inferencia por defecto: got %d, want %d",
			cfg.Worker.InferenceTimeoutMS, DefaultWorkerInferenceTimeoutMS)
	}

	// ─────────────────────────────────────────────────────────────────────────
	// 🔴 LA RELACIÓN QUE EL NÚMERO TIENE QUE SOSTENER, Y QUE ES POR LO QUE SE MOVIÓ
	// ─────────────────────────────────────────────────────────────────────────
	// El plazo no es un número suelto: de él DERIVA el umbral de lentitud del breaker
	// (cajero.FraccionLentitud, ADR-0042 · MP-09), y el MP-09 lo calibró para que ese umbral quedara a un
	// factor ~4,6 del p50 SANO. Con el techo viejo (15 s ⇒ umbral 12 s) y el p50 REAL de campo (8,1 s) ese
	// factor había caído a 1,48, y el p90 de campo —12,8 s— YA SUPERABA el umbral: más de una de cada diez
	// inferencias sanas castigaba al breaker, justo lo contrario de lo que el MP-09 quería.
	//
	// Esto NO es tautológico: no compara una constante consigo misma, sino DOS constantes independientes
	// (el plazo y la fracción) contra un TERCER número que no está en el código —el p50 medido en campo—.
	// Si alguien mueve el plazo sin mirar, este test dice cuál es el criterio que rompió.
	const p50DeCampoMS = 8100 // 430 inferencias en el VPS de UAT, 2026-08-23
	umbralLentoMS := int(float64(DefaultWorkerInferenceTimeoutMS) * cajero.FraccionLentitud)
	if factor := float64(umbralLentoMS) / p50DeCampoMS; factor < 4.0 {
		t.Errorf("el umbral de lentitud (%d ms) queda a %.2fx del p50 de campo (%d ms); el MP-09 lo calibró "+
			"a ~4,6x, y por debajo de 4x el criterio empieza a marcar como enfermo el tráfico SANO",
			umbralLentoMS, factor, p50DeCampoMS)
	}
}

// TestLoad_IntentWaitMS_YaNoSeLee vigila que la variable retirada NO gobierne nada (T1.6-5, ADR-0045).
//
// 🔴 POR QUÉ ESTE TEST NO ES REDUNDANTE CON EL AVISO DE VariablesRetiradas: aquél comprueba que el
// operador se ENTERA; éste comprueba que la variable no COLARÍA un valor por ninguna puerta. Son dos
// fallos distintos y el segundo es el silencioso — un `loader.GetInt` olvidado en Load seguiría leyendo
// la variable hacia un campo que ya nadie usa, y nada se pondría rojo.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: devolver el campo `WaitMS` a `IntentConfig` y su
// `cfg.Intent.WaitMS = loader.GetInt("INTENT_WAIT_MS", …)` a Load ⇒ el tipo vuelve a tener el campo y
// este test deja de compilar, que es la forma más ruidosa de fallar.
func TestLoad_IntentWaitMS_YaNoSeLee(t *testing.T) {
	t.Setenv(EnvPrefix+"DATA_DIR", t.TempDir()) // hermético: Load lee estado bajo data_dir
	t.Setenv(EnvPrefix+"INTENT_WAIT_MS", "1234")

	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	// El tipo ya no tiene dónde guardarlo: se comprueba lo ÚNICO que se puede comprobar sin campo, que es
	// que el resto de la config del clasificador sigue en pie y que Load no falla por la variable presente.
	if cfg.Intent.Model != DefaultIntentModel {
		t.Fatalf("con la variable retirada presente, el resto del bloque intent debe cargar normal: got %q", cfg.Intent.Model)
	}
	// Y que NO se ha colado hacia el plazo del worker, que es el sitio al que un copiar-pegar la mandaría.
	if cfg.Worker.InferenceTimeoutMS != DefaultWorkerInferenceTimeoutMS {
		t.Fatalf("WAPP_AGENT_INTENT_WAIT_MS no debe gobernar NADA, y menos el plazo del worker: got %d",
			cfg.Worker.InferenceTimeoutMS)
	}
}

// TestVariablesRetiradas vigila los avisos que evitan el fallo silencioso de retirar una variable.
//
// Retirar una variable de entorno no rompe nada visible: el operador la deja puesta en su unidad, el
// proceso arranca sin quejarse y el número que él cree haber fijado no gobierna nada. Este test asevera
// las dos mitades del contrato —hay aviso cuando está puesta, NO hay ruido cuando no lo está— para las
// DOS variables retiradas de hoy, y que el texto nombra literalmente lo que el operador tiene escrito.
//
// 🔴 LA SEGUNDA (WAPP_AGENT_INTENT_WAIT_MS, T1.6-5) NO TIENE SUSTITUTA, Y ESO SE ASEVERA. Es el caso que
// el tipo AvisoRetirada contempla con `Sustituta == ""`, y el único de los dos. Ofrecerle al operador
// una variable «parecida» le haría girar una palanca desconectada, que es exactamente el fallo que estos
// avisos existen para cerrar.
func TestVariablesRetiradas(t *testing.T) {
	// sinRetiradas deja el entorno SIN NINGUNA de las dos variables retiradas. t.Setenv + Unsetenv
	// restaura al salir del test aunque quien ejecute la suite las tuviera puestas de verdad en su shell.
	sinRetiradas := func(t *testing.T) {
		t.Helper()
		for _, v := range []string{EnvPrefix + "INTENT_TIMEOUT_MS", EnvPrefix + "INTENT_WAIT_MS"} {
			t.Setenv(v, "")
			if err := os.Unsetenv(v); err != nil {
				t.Fatalf("Unsetenv %s: %v", v, err)
			}
		}
	}

	t.Run("sin ninguna variable retirada no hay aviso", func(t *testing.T) {
		sinRetiradas(t)
		if avisos := VariablesRetiradas(); len(avisos) != 0 {
			t.Fatalf("sin variables retiradas puestas no debe haber ningún aviso: got %+v", avisos)
		}
	})

	t.Run("INTENT_TIMEOUT_MS avisa y manda al timeout de inferencia", func(t *testing.T) {
		sinRetiradas(t)
		t.Setenv(EnvPrefix+"INTENT_TIMEOUT_MS", "3000")
		avisos := VariablesRetiradas()
		if len(avisos) != 1 {
			t.Fatalf("se esperaba exactamente 1 aviso: got %d (%+v)", len(avisos), avisos)
		}
		aviso := avisos[0]
		if aviso.Variable != EnvPrefix+"INTENT_TIMEOUT_MS" {
			t.Fatalf("el aviso debe nombrar LITERALMENTE la variable retirada: got %q", aviso.Variable)
		}
		// La sustituta es la que el código LEE de verdad (WAPP_WORKER_INFERENCE_TIMEOUT_MS), no la que
		// nombran los docs del plan (WAPP_WORKER_TIMEOUT_MS): mandar al operador a una variable que nadie
		// lee reproduciría el mismo fallo silencioso que este aviso existe para cerrar.
		if aviso.Sustituta != WorkerEnvPrefix+"INFERENCE_TIMEOUT_MS" {
			t.Fatalf("la sustituta debe ser la variable que Load lee de verdad: got %q", aviso.Sustituta)
		}
		if !strings.Contains(aviso.Motivo, WorkerEnvPrefix+"INFERENCE_TIMEOUT_MS") {
			t.Fatalf("el motivo debe nombrar el timeout de INFERENCIA vigente: got %q", aviso.Motivo)
		}
	})

	t.Run("INTENT_WAIT_MS avisa SIN sustituta y explica el pull", func(t *testing.T) {
		sinRetiradas(t)
		t.Setenv(EnvPrefix+"INTENT_WAIT_MS", "4000")
		avisos := VariablesRetiradas()
		if len(avisos) != 1 {
			t.Fatalf("se esperaba exactamente 1 aviso: got %d (%+v)", len(avisos), avisos)
		}
		aviso := avisos[0]
		if aviso.Variable != EnvPrefix+"INTENT_WAIT_MS" {
			t.Fatalf("el aviso debe nombrar LITERALMENTE la variable retirada: got %q", aviso.Variable)
		}
		// 🔴 LA MITAD QUE IMPORTA: sin sustituta. Un nombre aquí sería una palanca desconectada.
		if aviso.Sustituta != "" {
			t.Fatalf("WAPP_AGENT_INTENT_WAIT_MS NO tiene sustituta (la espera se disolvió, no se mudó): got %q", aviso.Sustituta)
		}
		if !strings.Contains(aviso.Motivo, "PULL") {
			t.Fatalf("el motivo debe decir por qué desapareció (el paso a pull): got %q", aviso.Motivo)
		}
	})

	t.Run("las dos a la vez dan DOS avisos", func(t *testing.T) {
		sinRetiradas(t)
		t.Setenv(EnvPrefix+"INTENT_TIMEOUT_MS", "3000")
		t.Setenv(EnvPrefix+"INTENT_WAIT_MS", "4000")
		if avisos := VariablesRetiradas(); len(avisos) != 2 {
			t.Fatalf("cada variable retirada presente tiene su propio aviso: got %d (%+v)", len(avisos), avisos)
		}
	})

	t.Run("puesta a vacío también avisa", func(t *testing.T) {
		// Se mira la PRESENCIA, no el valor: quien la escribió tenía una intención y ya no se cumple.
		sinRetiradas(t)
		t.Setenv(EnvPrefix+"INTENT_TIMEOUT_MS", "")
		if avisos := VariablesRetiradas(); len(avisos) != 1 {
			t.Fatalf("una variable retirada presente-pero-vacía también se avisa: got %d", len(avisos))
		}
	})
}

// TestLoad_StatsEveryMS_ElCeroDesactiva: el latido de contadores es el ÚNICO número del worker cuyo
// cero es un valor legítimo («cállate»), no un dedazo. Sólo lo negativo cae al default.
func TestLoad_StatsEveryMS_ElCeroDesactiva(t *testing.T) {
	t.Run("cero desactiva", func(t *testing.T) {
		t.Setenv(WorkerEnvPrefix+"STATS_EVERY_MS", "0")
		cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.Worker.StatsEveryMS != 0 {
			t.Fatalf("un 0 explícito DESACTIVA el latido, no cae al default: got %d", cfg.Worker.StatsEveryMS)
		}
	})
	t.Run("negativo cae al default", func(t *testing.T) {
		t.Setenv(WorkerEnvPrefix+"STATS_EVERY_MS", "-1")
		cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.Worker.StatsEveryMS != DefaultWorkerStatsEveryMS {
			t.Fatalf("un negativo no significa nada y cae al default: got %d", cfg.Worker.StatsEveryMS)
		}
	})
}

// TestLoad_Worker_PrefijoDelAgenteNoAplica es la otra mitad del contrato del prefijo: WAPP_AGENT_ NO
// gobierna al worker. Si algún día alguien "unifica" los prefijos, este test lo caza.
func TestLoad_Worker_PrefijoDelAgenteNoAplica(t *testing.T) {
	t.Setenv(EnvPrefix+"MAX_CONCURRENT", "9")
	t.Setenv(EnvPrefix+"POLL_MS", "9999")
	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.Worker.MaxConcurrent != DefaultWorkerMaxConcurrent || cfg.Worker.PollMS != DefaultWorkerPollMS {
		t.Fatalf("WAPP_AGENT_* no debe gobernar al worker: got %+v", cfg.Worker)
	}
}

// TestLoad_Worker_GuardarrailNoPositivo: cada cero tiene su forma propia de romper el worker (semáforo
// sin plazas ⇒ bucle bloqueado; poll 0 ⇒ espera activa que quema un core; runas 0 ⇒ la DoS de T2.5 de
// vuelta), así que ninguno se traga: todos caen al default.
func TestLoad_Worker_GuardarrailNoPositivo(t *testing.T) {
	claves := []struct {
		env  string
		leer func(Config) int
		def  int
	}{
		{"MAX_CONCURRENT", func(c Config) int { return c.Worker.MaxConcurrent }, DefaultWorkerMaxConcurrent},
		{"POLL_MS", func(c Config) int { return c.Worker.PollMS }, DefaultWorkerPollMS},
		{"MAX_RUNES", func(c Config) int { return c.Worker.MaxRunes }, DefaultWorkerMaxRunes},
		{"NUM_THREAD", func(c Config) int { return c.Worker.NumThread }, DefaultWorkerNumThread},
		{"NUM_PREDICT", func(c Config) int { return c.Worker.NumPredict }, DefaultWorkerNumPredict},
		{"NUM_CTX", func(c Config) int { return c.Worker.NumCtx }, DefaultWorkerNumCtx},
		{"INFERENCE_TIMEOUT_MS", func(c Config) int { return c.Worker.InferenceTimeoutMS }, DefaultWorkerInferenceTimeoutMS},
		// STATS_EVERY_MS NO va en esta lista: su cero es válido (desactiva) y tiene su propio test.
	}
	for _, k := range claves {
		for _, crudo := range []string{"0", "-1"} {
			t.Run(k.env+"="+crudo, func(t *testing.T) {
				t.Setenv(WorkerEnvPrefix+k.env, crudo)
				cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
				if err != nil {
					t.Fatalf("Load(%s=%s): %v", k.env, crudo, err)
				}
				if got := k.leer(cfg); got != k.def {
					t.Fatalf("guardarraíl de %s con %s: got %d, want %d", k.env, crudo, got, k.def)
				}
			})
		}
	}
}

// TestLoad_Worker_DesdeYAML: el bloque `worker:` va en el MISMO config.yaml (el fichero es compartido
// entre `agent serve` y `agent cajero`), aunque las variables de entorno tengan prefijos distintos.
func TestLoad_Worker_DesdeYAML(t *testing.T) {
	yamlPath := writeTempYAML(t, "worker:\n  max_concurrent: 2\n  poll_ms: 750\n  max_runes: 2500\n")
	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("Load devolvió error inesperado: %v", err)
	}
	if cfg.Worker.MaxConcurrent != 2 || cfg.Worker.PollMS != 750 || cfg.Worker.MaxRunes != 2500 {
		t.Fatalf("el bloque worker del YAML no se aplicó: %+v", cfg.Worker)
	}
	// Lo que el YAML no nombra conserva su default (no se pone a cero por unmarshal parcial).
	if cfg.Worker.NumThread != DefaultWorkerNumThread || cfg.Worker.NumCtx != DefaultWorkerNumCtx {
		t.Fatalf("las claves ausentes del YAML deben conservar su default: %+v", cfg.Worker)
	}
}

// TestLoad_InboundStatsEveryMS_ElCeroDesactiva: el latido de latencia del handler (T3.13) es el segundo
// número del proyecto cuyo cero es un valor legítimo («cállate»), y por el mismo motivo que el del cajero:
// los logs del VPS van a un FICHERO, así que un operador tiene que poder callar un bloque que se emite
// cada minuto sin tener que apagar el daemon.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: escribir el guardarraíl como `<= 0` (que es como están TODOS los demás
// números del agente, y por eso es el error natural) ⇒ el 0 explícito cae al default y el bloque sigue
// saliendo. La otra mitad —que un negativo SÍ caiga— la cubre el segundo subtest.
func TestLoad_InboundStatsEveryMS_ElCeroDesactiva(t *testing.T) {
	t.Run("default cuando no se toca", func(t *testing.T) {
		cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.InboundStatsEveryMS != DefaultInboundStatsEveryMS {
			t.Fatalf("sin variable, la cadencia es el default (%d ms): got %d",
				DefaultInboundStatsEveryMS, cfg.InboundStatsEveryMS)
		}
	})
	t.Run("cero desactiva", func(t *testing.T) {
		t.Setenv(EnvPrefix+"INBOUND_STATS_EVERY_MS", "0")
		cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.InboundStatsEveryMS != 0 {
			t.Fatalf("un 0 explícito DESACTIVA el latido periódico, no cae al default: got %d", cfg.InboundStatsEveryMS)
		}
	})
	t.Run("negativo cae al default", func(t *testing.T) {
		t.Setenv(EnvPrefix+"INBOUND_STATS_EVERY_MS", "-1")
		cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.InboundStatsEveryMS != DefaultInboundStatsEveryMS {
			t.Fatalf("un negativo no significa nada y cae al default: got %d", cfg.InboundStatsEveryMS)
		}
	})
	t.Run("un valor de sesion de campo se respeta", func(t *testing.T) {
		// 10 s es lo que se pone durante PC-11 para no depender de que el tick caiga en el momento bueno.
		t.Setenv(EnvPrefix+"INBOUND_STATS_EVERY_MS", "10000")
		cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.InboundStatsEveryMS != 10000 {
			t.Fatalf("la cadencia de la sesión de campo se respeta tal cual: got %d", cfg.InboundStatsEveryMS)
		}
	})
}

// TestLoad_InboundStatsEveryMS_EsDelAGENTE_NoDelWorker: el prefijo importa porque son DOS PROCESOS con dos
// bloques de entorno. El cronómetro del handler vive en `agent serve`; el latido de contadores del cajero,
// en `agent cajero`. Cruzar los prefijos dejaría una de las dos telemetrías muda en campo, y el síntoma
// —«no sale el bloque»— no apunta a su causa.
func TestLoad_InboundStatsEveryMS_EsDelAgenteNoDelWorker(t *testing.T) {
	t.Setenv(WorkerEnvPrefix+"INBOUND_STATS_EVERY_MS", "1234")
	cfg, err := Load(filepath.Join(t.TempDir(), "ausente.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.InboundStatsEveryMS != DefaultInboundStatsEveryMS {
		t.Fatalf("WAPP_WORKER_* no debe gobernar la cadencia del AGENTE: got %d", cfg.InboundStatsEveryMS)
	}
}
