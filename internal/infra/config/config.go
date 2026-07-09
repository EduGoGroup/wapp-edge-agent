// Package config define la configuracion del Edge Agent y su carga desde
// archivo YAML con overlay de variables de entorno (prefijo WAPP_AGENT_).
//
// Se apoya en github.com/EduGoGroup/wapp-shared/config para la lectura del YAML
// y el acceso tipado a variables de entorno.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	sharedconfig "github.com/EduGoGroup/wapp-shared/config"
)

// EnvPrefix es el prefijo aplicado a las variables de entorno del Edge Agent.
// Por ejemplo, la clave LOG_LEVEL se lee de la variable WAPP_AGENT_LOG_LEVEL.
const EnvPrefix = "WAPP_AGENT_"

// DefaultCloudLinkRuntimePort es el puerto por defecto del stream de runtime CloudLink (Connect, mTLS)
// con el que el enroll DERIVA el Endpoint tras enrolar (Plan 026 T3): topología de prod design §1
// (:8101 CloudLink / :8102 enroll). El proto de enroll no devuelve endpoint de runtime, así que se
// deriva del host del endpoint de enrolamiento con este puerto (configurable por RuntimePort).
const DefaultCloudLinkRuntimePort = "8101"

// runtimeEndpointStateFile es el nombre del archivo de estado bajo data_dir donde el enroll persiste el
// Endpoint de runtime derivado (Plan 026 T3, cierra follow-up 023) para que `serve` lo relea sin edición
// manual del config.yaml. Material PÚBLICO (host:puerto), nunca secretos.
const runtimeEndpointStateFile = "cloudlink-endpoint"

// RuntimeEndpointStatePath devuelve la ruta ESTABLE del archivo de estado del Endpoint de runtime dentro
// del data_dir (Plan 026 T3): <data_dir>/cloudlink-endpoint. El enroll lo ESCRIBE tras derivar el endpoint
// y config.Load lo LEE como fallback cuando el Endpoint no viene por YAML/env. Ambos (enroll vía wapp-ctl y
// el hijo `agent serve`) resuelven el MISMO data_dir (env compartido o defaultDataDir por SO), así que la
// ruta coincide entre escritura y lectura sin depender del CWD.
func RuntimeEndpointStatePath(dataDir string) string {
	return filepath.Join(dataDir, runtimeEndpointStateFile)
}

// Config agrupa los parametros minimos de arranque del Edge Agent.
type Config struct {
	// LogLevel es el nivel minimo de logging: debug, info, warn o error.
	LogLevel string `yaml:"log_level"`
	// LogJSON selecciona el formato JSON del logger cuando es true.
	LogJSON bool `yaml:"log_json"`
	// DBPath es la ruta del store SQLite cifrado del cryptostore.
	//
	// LEGACY (Plan 008): es la ruta PLANA single-sesión heredada. El modelo multi-sesión deriva el
	// store por sesión de DataDir (sessions/<id>/store.db, ADR-0016 §4); DBPath se conserva solo como
	// referencia del estado viejo para la migración clean-slate de arranque (edgemigrate).
	DBPath string `yaml:"db_path"`
	// DEKPath es la ruta del material relacionado con la DEK custodiada localmente.
	//
	// LEGACY (Plan 008): ruta PLANA single-sesión heredada. El modelo multi-sesión deriva la DEK por
	// sesión de DataDir (sessions/<id>/dek.key); se conserva solo para la migración clean-slate.
	DEKPath string `yaml:"dek_path"`
	// DBDialect selecciona el motor SQL de la BD del Edge (Plan 022 T0, design §5): "sqlite" (default,
	// embebido pure-Go, ADR-0002) o "postgres" (solo por cadena y solo si el binario se compiló con el
	// build-tag `postgres`; nunca bundleado en el Edge). Es la base del dialecto CONMUTABLE: T1 cablea
	// este valor a la apertura de la BD única. Se lee de WAPP_AGENT_DB_DIALECT.
	DBDialect string `yaml:"db_dialect"`
	// DBDSN es la cadena de conexión de la BD única cuando el dialecto la requiere (Postgres:
	// "postgres://user:pass@host:5432/db?sslmode=..."). En SQLite queda vacío: la ruta del fichero la
	// deriva el layout desde DataDir. Se lee de WAPP_AGENT_DB_DSN.
	DBDSN string `yaml:"db_dsn"`
	// DataDir es el directorio base del Edge (ADR-0016 §4): aloja el layout multi-sesión
	// (<data_dir>/sessions/<session_id>/{store.db,dek.key}), la BD de metadatos y el socket de control.
	// El Layout (internal/app/sessionmgr) deriva de aquí todas las rutas por sesión; nadie las arma a
	// mano.
	//
	// RUTA SAGRADA (MP-02, D1/D2): el default deja de ser "." (CWD) — que sembraba árboles sessions/
	// distintos según desde dónde se arrancara y forzaba re-emparejar. Ahora el default es una carpeta
	// de datos del usuario POR SO, SIEMPRE en el home y sin permisos de sistema (ver defaultDataDir), y
	// tras Load se ANCLA a ruta absoluta (filepath.Abs) una sola vez, venga del default, del YAML o del
	// override WAPP_AGENT_DATA_DIR. Así el store vive siempre en el mismo sitio, independiente del CWD.
	DataDir string `yaml:"data_dir"`
	// MaxSessions es el límite SUAVE de sesiones simultáneas (guardarraíl de RAM/sockets, design §10.G).
	// NO es un invariante de seguridad: un POST /pair por encima del límite responde error claro, no
	// crash. Se lee de WAPP_AGENT_MAX_SESSIONS (default 5).
	MaxSessions int `yaml:"max_sessions"`
	// MultiDevicePerAccount es el número de DISPOSITIVOS VIVOS que se permiten por CUENTA (número), base
	// del failover multi-dispositivo por número (Plan 022 T5, design §6/§10.F). Default 1 (comportamiento
	// actual: un device operativo por número). Con >1 el Manager permite N devices vivos del mismo número
	// (1 primary + standbys); el standby se promueve si el primary cae/expira (LoggedOut). Tope 4 (límite
	// de WhatsApp: 1 principal + 4 vinculados); se CLAMP a [1,4] al cargar.
	//
	// CAVEAT (requisito del plan): multi-device es RESILIENCIA, NO SIGILO. Más dispositivos NO reducen el
	// riesgo de baneo — al contrario, más companions = más huella. Por eso va OFF por defecto (1) y no se
	// debe incentivar agotar los 4 slots. Se lee de WAPP_AGENT_MULTIDEVICE_PER_ACCOUNT.
	MultiDevicePerAccount int `yaml:"multidevice_per_account"`
	// PushName es el nombre visible (push name) que se ANUNCIA en la presencia (SendPresence available,
	// Plan 013 §10.D) cuando el store restaurado aún no conoce el nombre REAL de la cuenta. whatsmeow
	// rechaza SendPresence sin PushName ("can't send presence without PushName set"), y sin presencia
	// available WhatsApp no propaga los acuses (delivered/read) al companion. FALLBACK, no override: el
	// gateway solo lo usa si Store.PushName viene vacío (store recién restaurado); si whatsmeow ya conoce
	// el nombre real de la cuenta (app-state), ese PREVALECE. No es secreto (no lleva PII/número). Se lee
	// de WAPP_AGENT_PUSH_NAME (default "wApp"). Para el e2e conviene fijar el nombre real de la cuenta.
	PushName string `yaml:"push_name"`
	// ControlSocketPath es la ruta del Unix domain socket donde el núcleo expone el contrato /v1 del
	// plano de control (ADR-0015): co-ubicado, SIN puerto de red. Default relativo al cwd, junto al
	// db_path (ver defaults). Override por WAPP_AGENT_CONTROL_SOCKET_PATH (mismo overlay que el resto).
	ControlSocketPath string `yaml:"control_socket_path"`
	// CloudLink configura el conducto edge<->cloud (pieza 02). Si Endpoint está vacío, el Edge usa
	// SOLO el LogSink (diagnóstico, sin red): no rompe los flujos pair/send/listen del spike.
	CloudLink CloudLinkConfig `yaml:"cloudlink"`
}

// CloudLinkConfig agrupa los parámetros del conducto CloudLink. Todos OPCIONALES: con Endpoint vacío
// no se conecta a la nube (LogSink puro). El material cripto (cert/clave) vive fuera de git (.gitignore).
type CloudLinkConfig struct {
	// Endpoint es la dirección gRPC de la plataforma cloud (p.ej. "cloud.wapp.example:8101"). Vacío
	// desactiva el conducto real.
	Endpoint string `yaml:"endpoint"`
	// SessionID identifica la sesión/teléfono dentro del Edge (multiplexado, ADR-0008).
	SessionID string `yaml:"session_id"`
	// TLSCert/TLSKey/TLSCA son las rutas del cert de cliente del Edge y la CA (mTLS, ADR-0006). Si las
	// tres están presentes se usa mTLS; si no, el dial va insecure (solo dev; se loguea advertencia).
	TLSCert string `yaml:"tls_cert"`
	TLSKey  string `yaml:"tls_key"`
	TLSCA   string `yaml:"tls_ca"`
	// ServerName es el SAN esperado en el cert del servidor (mTLS). Por defecto se deriva del Endpoint.
	ServerName string `yaml:"server_name"`
	// LeasePubKeyPath es la ruta a la clave pública Ed25519 del emisor de leases (servidor). Si está
	// presente, se activa el gate de lease (kill-switch); si no, no se gatea (dev).
	LeasePubKeyPath string `yaml:"lease_pubkey_path"`
	// CloudEncPubKeyPath es la ruta a la clave pública X25519 (32B) de CIFRADO de la nube (Plan 011
	// §6.3/§6.4). Se puebla desde el enrolamiento (EnrollEdgeResponse.cloud_enc_pubkey). Si está
	// presente, el Edge SELLA los campos sensibles del entrante hacia esta pública (SealFor) antes de
	// reenviarlos; si no, va el fallback claro (§10.H). Persistida en base64 (una línea).
	CloudEncPubKeyPath string `yaml:"cloud_enc_pubkey_path"`
	// EnrollmentEndpoint es la dirección gRPC del servidor de enrolamiento del Gateway (subcomando
	// `enroll`). En dev suele ser un puerto distinto al de Connect (p.ej. "localhost:8102"). El dial de
	// enrolamiento usa TLS-de-servidor (NO mTLS): valida al Gateway con TLSCA. Vacío desactiva `enroll`.
	EnrollmentEndpoint string `yaml:"enrollment_endpoint"`
	// RuntimePort es el PUERTO del stream de runtime CloudLink (Connect, mTLS) con el que se DERIVA el
	// Endpoint tras enrolar (Plan 026 T3, cierra follow-up 023): host(EnrollmentEndpoint) + ":" +
	// RuntimePort. Por defecto "8101" (topología de prod, design §1: :8101 CloudLink / :8102 enroll). El
	// proto de enroll (EnrollEdgeResponse) NO devuelve un endpoint de runtime, así que se deriva del host
	// del endpoint de enrolamiento manteniendo el contrato intacto. Se lee de WAPP_AGENT_CLOUDLINK_RUNTIME_PORT.
	RuntimePort string `yaml:"runtime_port"`
	// ActivationCode es el código de activación emitido por el Gateway para autorizar el enrolamiento.
	// De un solo uso. Se puede pasar también como argumento: `agent enroll <codigo>`.
	ActivationCode string `yaml:"activation_code"`
	// EdgeID es la identidad del Edge que va al CommonName del CSR durante el enrolamiento. Si está
	// vacío se resuelve en tiempo de ejecución: SessionID si existe, si no el hostname del equipo.
	EdgeID string `yaml:"edge_id"`
}

// defaultDataDir calcula la RUTA SAGRADA por defecto del store del Edge (MP-02, D1): SIEMPRE en el
// home del usuario y sin permisos de sistema (funciona para un usuario normal sin sudo). Nunca /var/lib
// ni rutas de sistema que exijan root.
//
// Base por SO vía os.UserConfigDir (macOS → ~/Library/Application Support; Linux → $XDG_CONFIG_HOME o
// ~/.config; Windows → %AppData%), a la que se añade wApp/edge. Si UserConfigDir falla, cae a
// ~/.wapp-edge (os.UserHomeDir). Último recurso: "." (nunca peor que el comportamiento previo). El valor
// devuelto es absoluto salvo en ese último fallback, que Load absolutiza igualmente.
func defaultDataDir() string {
	if base, err := os.UserConfigDir(); err == nil && base != "" {
		return filepath.Join(base, "wApp", "edge")
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".wapp-edge")
	}
	return "."
}

// defaults devuelve la configuracion con valores por defecto sensatos.
func defaults() Config {
	return Config{
		LogLevel:              "info",
		LogJSON:               false,
		DBPath:                "wapp-edge.db",
		DEKPath:               "dek.key",
		DBDialect:             "sqlite",
		DataDir:               defaultDataDir(),
		MaxSessions:           5,
		MultiDevicePerAccount: 1,
		PushName:              "wApp",
		ControlSocketPath:     "wapp-edge.sock",
		CloudLink:             CloudLinkConfig{RuntimePort: DefaultCloudLinkRuntimePort},
	}
}

// Load construye la configuracion del Edge Agent.
//
// Orden de precedencia (de menor a mayor): valores por defecto, archivo YAML en
// path (opcional; si no existe se ignora) y variables de entorno con prefijo
// WAPP_AGENT_. Devuelve error solo si el YAML existe pero no puede leerse o
// parsearse.
func Load(path string) (Config, error) {
	cfg := defaults()

	loader := sharedconfig.New(
		sharedconfig.WithFile(path),
		sharedconfig.WithEnvPrefix(EnvPrefix),
	)

	if err := loader.Unmarshal(&cfg); err != nil {
		return Config{}, err
	}

	// Overlay de entorno: usa el valor actual (default o YAML) como fallback.
	cfg.LogLevel = loader.GetString("LOG_LEVEL", cfg.LogLevel)
	cfg.LogJSON = loader.GetBool("LOG_JSON", cfg.LogJSON)
	cfg.DBPath = loader.GetString("DB_PATH", cfg.DBPath)
	cfg.DEKPath = loader.GetString("DEK_PATH", cfg.DEKPath)
	cfg.DBDialect = loader.GetString("DB_DIALECT", cfg.DBDialect)
	cfg.DBDSN = loader.GetString("DB_DSN", cfg.DBDSN)
	cfg.DataDir = loader.GetString("DATA_DIR", cfg.DataDir)
	cfg.MaxSessions = loader.GetInt("MAX_SESSIONS", cfg.MaxSessions)
	cfg.MultiDevicePerAccount = loader.GetInt("MULTIDEVICE_PER_ACCOUNT", cfg.MultiDevicePerAccount)
	cfg.PushName = loader.GetString("PUSH_NAME", cfg.PushName)
	cfg.ControlSocketPath = loader.GetString("CONTROL_SOCKET_PATH", cfg.ControlSocketPath)
	cfg.CloudLink.Endpoint = loader.GetString("CLOUDLINK_ENDPOINT", cfg.CloudLink.Endpoint)
	cfg.CloudLink.SessionID = loader.GetString("CLOUDLINK_SESSION_ID", cfg.CloudLink.SessionID)
	cfg.CloudLink.TLSCert = loader.GetString("CLOUDLINK_TLS_CERT", cfg.CloudLink.TLSCert)
	cfg.CloudLink.TLSKey = loader.GetString("CLOUDLINK_TLS_KEY", cfg.CloudLink.TLSKey)
	cfg.CloudLink.TLSCA = loader.GetString("CLOUDLINK_TLS_CA", cfg.CloudLink.TLSCA)
	cfg.CloudLink.ServerName = loader.GetString("CLOUDLINK_SERVER_NAME", cfg.CloudLink.ServerName)
	cfg.CloudLink.LeasePubKeyPath = loader.GetString("CLOUDLINK_LEASE_PUBKEY_PATH", cfg.CloudLink.LeasePubKeyPath)
	cfg.CloudLink.CloudEncPubKeyPath = loader.GetString("CLOUDLINK_CLOUD_ENC_PUBKEY_PATH", cfg.CloudLink.CloudEncPubKeyPath)
	cfg.CloudLink.EnrollmentEndpoint = loader.GetString("CLOUDLINK_ENROLLMENT_ENDPOINT", cfg.CloudLink.EnrollmentEndpoint)
	cfg.CloudLink.ActivationCode = loader.GetString("CLOUDLINK_ACTIVATION_CODE", cfg.CloudLink.ActivationCode)
	cfg.CloudLink.EdgeID = loader.GetString("CLOUDLINK_EDGE_ID", cfg.CloudLink.EdgeID)
	cfg.CloudLink.RuntimePort = loader.GetString("CLOUDLINK_RUNTIME_PORT", cfg.CloudLink.RuntimePort)

	// Puerto de runtime CloudLink por defecto (Plan 026 T3): si nadie lo fijó (YAML/env), "8101"
	// (topología de prod, design §1). Con él el enroll deriva y persiste el Endpoint de runtime.
	if cfg.CloudLink.RuntimePort == "" {
		cfg.CloudLink.RuntimePort = DefaultCloudLinkRuntimePort
	}

	// Dialecto de BD (Plan 022 T0): solo "sqlite" (default) o "postgres". Se valida aquí para fallar
	// pronto ante un valor tecleado mal (YAML/env) en vez de arrastrarlo hasta abrir la BD.
	switch cfg.DBDialect {
	case "sqlite", "postgres":
	default:
		return Config{}, fmt.Errorf("config: db_dialect no soportado %q (usa \"sqlite\" o \"postgres\")", cfg.DBDialect)
	}

	// D2 (MP-02): ancla data_dir a ruta ABSOLUTA una sola vez, venga del default sagrado, del YAML o del
	// override WAPP_AGENT_DATA_DIR. filepath.Abs es idempotente (una ruta ya absoluta se devuelve limpia)
	// y no toca el disco; el MkdirAll de la raíz lo hace el arranque (cmd/agent). Así el store nunca
	// depende del CWD desde el que se lance el daemon.
	absDataDir, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return Config{}, fmt.Errorf("config: no se pudo resolver data_dir %q a ruta absoluta: %w", cfg.DataDir, err)
	}
	cfg.DataDir = absDataDir

	// Endpoint de runtime PERSISTIDO por el enroll (Plan 026 T3, cierra follow-up 023): si nadie fijó el
	// Endpoint por YAML/env (lo normal en el kit: viene comentado), se lee del archivo de estado
	// <data_dir>/cloudlink-endpoint que el enroll escribió al derivarlo del enrollment_endpoint. Así
	// `serve` levanta el stream contra la nube SIN que un no-técnico edite el config.yaml. Es un FALLBACK
	// de menor prioridad: un Endpoint explícito (YAML o env WAPP_AGENT_CLOUDLINK_ENDPOINT) siempre gana.
	// Best-effort: si el archivo no existe (aún no se enroló) o no se puede leer, se queda vacío (el Edge
	// cae a LogSink/LogMux, igual que antes). Solo material PÚBLICO (host:puerto), nunca secretos.
	if cfg.CloudLink.Endpoint == "" {
		if data, readErr := os.ReadFile(RuntimeEndpointStatePath(cfg.DataDir)); readErr == nil {
			cfg.CloudLink.Endpoint = strings.TrimSpace(string(data))
		}
	}

	// Failover multi-dispositivo por número (Plan 022 T5, §10.F): CLAMP a [1,4]. 1 = off (un device vivo
	// por número, comportamiento actual); 4 = tope de WhatsApp (1 principal + 4 vinculados). Valores fuera
	// de rango (0, negativos, >4) se saturan en vez de fallar el arranque (guardarraíl, no invariante de
	// seguridad). RESILIENCIA, no sigilo: no se debe subir sin necesidad operativa (más huella).
	if cfg.MultiDevicePerAccount < 1 {
		cfg.MultiDevicePerAccount = 1
	}
	if cfg.MultiDevicePerAccount > 4 {
		cfg.MultiDevicePerAccount = 4
	}

	return cfg, nil
}

// DefaultConfigPath devuelve la ruta ESTABLE del config.yaml del Edge dentro del data_dir sagrado
// (Plan 023 · T1): <data_dir>/config.yaml. Cierra el gotcha del CWD — MP-02 lo cerró para el data_dir
// (D2, absolutización); aquí se extiende al ARCHIVO de config, que hasta ahora se buscaba relativo al
// CWD ("config.yaml") y por tanto dependía de desde dónde se lanzara el proceso.
//
// Resuelve el data_dir igual que Load —override WAPP_AGENT_DATA_DIR, y si no el default por SO
// (defaultDataDir)— SIN depender del CWD, y lo absolutiza. El instalador y el LaunchAgent (T3/T4)
// además fijan WAPP_AGENT_CONFIG a este mismo valor; cuando esa variable está presente, el arranque
// (cmd/agent, cmd/wapp-ctl) la respeta y no llama aquí. El archivo puede no existir aún: Load lo trata
// como opcional (defaults + env), así que apuntar a una ruta inexistente en instalación limpia es seguro.
func DefaultConfigPath() string {
	dir := os.Getenv(EnvPrefix + "DATA_DIR")
	if dir == "" {
		dir = defaultDataDir()
	}
	if abs, err := filepath.Abs(dir); err == nil {
		dir = abs
	}
	return filepath.Join(dir, "config.yaml")
}
