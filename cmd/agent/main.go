// Command agent es el daemon del Edge Agent de wApp.
//
// Bootstrap minimo (T0, Plan 002): carga configuracion, construye el logger y
// registra el arranque. El subcomando `pair` (T3.4) ejecuta el emparejamiento por
// QR local con los adaptadores REALES (store SQLite cifrado + whatsmeow + control
// en terminal + custodia de la DEK en archivo). El subcomando `send` (T4.3) despacha
// un texto a un destino usando la sesion ya pareada. El subcomando `listen` (T5.5)
// mantiene el socket VIVO 24/7 (always-on), reenviando cada mensaje entrante al LogSink
// (stub CloudLink del spike) hasta Ctrl-C / SIGINT. La logica restante (CloudLink real,
// systray) se incorpora en chunks posteriores.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/logsink"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/daemon"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/edgemigrate"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/enroll"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/logger"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Version identifica la build del Edge Agent. Se inyecta en release vía
// -ldflags "-X main.Version=$(git describe --tags --always --dirty)" (ver
// Makefile, Plan 023 · T0). DEBE seguir siendo `var` (no `const`): ldflags -X
// solo sobre-escribe variables de string. El literal de abajo es el fallback de
// dev cuando se compila sin ldflags (go run/build directos, CI). La versión
// resultante viaja a /v1/health (server.Config.Version) y a los logs de arranque.
var Version = "0.1.0-bootstrap"

// processStartedAt marca el arranque del PROCESO (uptime del daemon, Plan 031 T7). Se fija al cargar el
// paquete (antes de cualquier subcomando), así el uptime que viaja en el heartbeat de salud y en GET
// /v1/health mide la vida real del daemon, no el instante en que se cableó el colector.
var processStartedAt = time.Now()

func main() {
	path := os.Getenv("WAPP_AGENT_CONFIG")
	if path == "" {
		// Ruta estable <data_dir>/config.yaml (Plan 023 · T1): cierra el gotcha del CWD (antes se buscaba
		// "config.yaml" relativo al directorio desde el que se lanzara el proceso). El instalador/LaunchAgent
		// además fijan WAPP_AGENT_CONFIG a este mismo valor.
		path = config.DefaultConfigPath()
	}

	cfg, err := config.Load(path)
	if err != nil {
		sharedlogger.Default().Error("no se pudo cargar la configuracion",
			"error", err, "path", path)
		os.Exit(1)
	}

	log := logger.New(cfg)

	// RUTA SAGRADA (MP-02, D2): cfg.DataDir ya viene ABSOLUTO desde config.Load (independiente del CWD).
	// Aseguramos la raíz del store con permisos restrictivos (0700) UNA sola vez aquí, antes de cualquier
	// subcomando: es el directorio base del layout multi-sesión (ADR-0016 §4) y todo cuelga de él. Si no
	// se puede crear, nada del daemon funcionaría, así que es fatal. NO se loguea ningún secreto: solo la
	// ruta del directorio (nunca la DEK).
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		log.Error("no se pudo asegurar el directorio de datos (data_dir)", "error", err, "data_dir", cfg.DataDir)
		os.Exit(1)
	}

	// Migración de ARRANQUE clean-slate al layout multi-sesión (ADR-0016 / Plan 008 §10.C): archiva el
	// store/DEK PLANOS heredados (DBPath/DEKPath) bajo <data_dir>/_archived-pre-008/ y crea el layout
	// <data_dir>/sessions/ vacío que el Manager poblará. Es IDEMPOTENTE (no-op si ya migró) y NO fatal:
	// un fallo de E/S aquí no debe impedir arrancar el daemon (se loguea y se continúa).
	if err := edgemigrate.ArchiveLegacySingleSession(cfg.DataDir, cfg.DBPath, cfg.DEKPath, log); err != nil {
		log.Error("migración clean-slate de arranque falló (continuo de todas formas)",
			"error", err, "data_dir", cfg.DataDir)
	}

	// Migración clean-slate hacia la BD ÚNICA (Plan 022 T1, ADR-0018 §8, fase 1): archiva el layout
	// multi-sesión POR-DIRECTORIO (sessions/<id>/) bajo <data_dir>/_archived-pre-022/ y deja sessions/
	// vacío. NO borra el árbol viejo (T6.5 lo lee para restaurar las sesiones ACTIVAS sin re-escanear).
	// Idempotente (no-op si ya migró) y NO fatal: un fallo de E/S no impide arrancar (se loguea y sigue).
	if err := edgemigrate.ArchiveLegacyPerSessionLayout(cfg.DataDir, log); err != nil {
		log.Error("migración clean-slate a BD única de arranque falló (continuo de todas formas)",
			"error", err, "data_dir", cfg.DataDir)
	}

	// Despacho de subcomandos. Sin argumento o `serve`: daemon multi-sesión.
	if len(os.Args) > 1 && os.Args[1] == "enroll" {
		if err := runEnroll(context.Background(), cfg, log); err != nil {
			log.Error("enrolamiento fallido", "error", err)
			os.Exit(1)
		}
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "serve" {
		sink := logsink.New(0)
		serveLog := logger.NewWithSink(cfg, sink)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := runServe(ctx, cfg, serveLog, sink); err != nil {
			serveLog.Error("daemon multi-sesión fallido", "error", err)
			os.Exit(1)
		}
		return
	}

	log.Info("wapp-edge-agent arrancando",
		"version", Version,
		"log_level", cfg.LogLevel,
		"log_json", cfg.LogJSON,
		"data_dir", cfg.DataDir,
		"db_path", cfg.DBPath,
		"config_path", path,
	)
}

// runServe es el daemon MULTI-SESIÓN UNIFICADO (integración Plan 008 + plano de control Plan 007): en UN
// SOLO proceso (decisión §10.E Plan 007 + ADR-0014/0015) levanta el Session Manager —restaura TODAS las
// sesiones activas y mantiene un listener por sesión 24/7 (concurrencia Go sin broker, ADR-0003)— Y el
// servidor /v1 del plano de control sobre el Unix socket co-ubicado (health, sessions, logs SSE, pairing
// async y unlink quirúrgico), con shutdown unificado bajo el mismo ctx (SIGINT/SIGTERM o cancelación del
// caller en los tests).
//
// RE-LLAVEADO A session_id (integración 008): el contrato /v1 ya NO llavea por JID. El Manager es la
// fuente única: GET /v1/sessions lista N por session_id+estado+salud; POST /v1/sessions/pair dispara
// Manager.Pair (genera su propio session_id/dir/DEK, async, devuelve SOLO QR/estado — la DEK nunca cruza,
// ADR-0007/0015); DELETE /v1/sessions/{id} hace Manager.Unlink(session_id) (borrado quirúrgico, §7).
//
// El servidor /v1 SIGUE arriba aunque no haya sesiones que restaurar (primer arranque antes de emparejar):
// así se puede emparejar el primer teléfono por POST /v1/sessions/pair sin reiniciar el daemon.
func runServe(ctx context.Context, cfg config.Config, log sharedlogger.Logger, sink *logsink.Sink) error {
	return daemon.New(cfg, log, sink, Version, processStartedAt).Run(ctx)
}

// runEnroll cablea el subcomando `enroll`: lee el código de activación de cfg o de os.Args
// (`agent enroll <codigo>`), valida precondiciones (endpoint de enrolamiento, TLSCA pre-provista y
// código presentes) y delega al paquete enroll, que genera el par mTLS y lo persiste en TLSCert/TLSKey.
// No toca pair/send/listen. La TLSCA DEBE estar pre-provista antes de enrolar (valida al Gateway).
func runEnroll(ctx context.Context, cfg config.Config, log sharedlogger.Logger) error {
	// Override opcional del código por argumento posicional: `agent enroll <codigo>`.
	if len(os.Args) > 2 && os.Args[2] != "" {
		cfg.CloudLink.ActivationCode = os.Args[2]
	}

	if cfg.CloudLink.EnrollmentEndpoint == "" {
		return fmt.Errorf("falta enrollment_endpoint (configura cloudlink.enrollment_endpoint o WAPP_AGENT_CLOUDLINK_ENROLLMENT_ENDPOINT)")
	}
	if cfg.CloudLink.TLSCA == "" {
		return fmt.Errorf("falta tls_ca: la CA que valida al Gateway debe estar pre-provista antes de enrolar")
	}
	if cfg.CloudLink.ActivationCode == "" {
		return fmt.Errorf("falta el código de activación (usa `agent enroll <codigo>` o WAPP_AGENT_CLOUDLINK_ACTIVATION_CODE)")
	}

	log.Info("enrolando el Edge contra el Gateway",
		"endpoint", cfg.CloudLink.EnrollmentEndpoint, "tls_cert", cfg.CloudLink.TLSCert, "tls_key", cfg.CloudLink.TLSKey)

	return enroll.Run(ctx, cfg, log)
}
