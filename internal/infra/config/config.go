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

	sharedconfig "github.com/EduGoGroup/wapp-shared/config"
)

// EnvPrefix es el prefijo aplicado a las variables de entorno del Edge Agent.
// Por ejemplo, la clave LOG_LEVEL se lee de la variable WAPP_AGENT_LOG_LEVEL.
const EnvPrefix = "WAPP_AGENT_"

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
	// Endpoint es la dirección gRPC de la plataforma cloud (p.ej. "cloud.wapp.example:8443"). Vacío
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
	// `enroll`). En dev suele ser un puerto distinto al de Connect (p.ej. "localhost:8444"). El dial de
	// enrolamiento usa TLS-de-servidor (NO mTLS): valida al Gateway con TLSCA. Vacío desactiva `enroll`.
	EnrollmentEndpoint string `yaml:"enrollment_endpoint"`
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
		LogLevel:          "info",
		LogJSON:           false,
		DBPath:            "wapp-edge.db",
		DEKPath:           "dek.key",
		DataDir:           defaultDataDir(),
		MaxSessions:       5,
		ControlSocketPath: "wapp-edge.sock",
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
	cfg.DataDir = loader.GetString("DATA_DIR", cfg.DataDir)
	cfg.MaxSessions = loader.GetInt("MAX_SESSIONS", cfg.MaxSessions)
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

	// D2 (MP-02): ancla data_dir a ruta ABSOLUTA una sola vez, venga del default sagrado, del YAML o del
	// override WAPP_AGENT_DATA_DIR. filepath.Abs es idempotente (una ruta ya absoluta se devuelve limpia)
	// y no toca el disco; el MkdirAll de la raíz lo hace el arranque (cmd/agent). Así el store nunca
	// depende del CWD desde el que se lance el daemon.
	absDataDir, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return Config{}, fmt.Errorf("config: no se pudo resolver data_dir %q a ruta absoluta: %w", cfg.DataDir, err)
	}
	cfg.DataDir = absDataDir

	return cfg, nil
}
