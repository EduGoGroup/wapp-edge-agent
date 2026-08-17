// Command agent es el daemon del Edge Agent de wApp.
//
// TRES subcomandos, y no hay mas:
//
//   - `enroll`: enrola el Edge contra la nube (mTLS por codigo de enrolamiento).
//   - `serve`:  el daemon 24/7 — restaura las sesiones activas, mantiene un listener
//     por sesion sobre el socket VIVO de whatsmeow, drena el outbox contra CloudLink
//     y expone el plano de control /v1 sobre el Unix socket co-ubicado.
//   - `cajero`: el worker-cajero de la cola de entrantes (Plan 051 Ola 2 · T2.2) — reclama
//     lotes por conversacion, clasifica contra el Ollama local y escribe el intent de
//     vuelta. MISMO BINARIO, papel distinto: es el tercer hijo de `wapp-ctl`, sin plano
//     de control propio (su readiness es «el proceso esta vivo»).
//
// Emparejar es `POST /v1/sessions/pair` contra ese plano de control (por terminal
// ASCII o por la web local de wapp-ctl); enviar y escuchar los gobierna `serve`.
// Sin subcomando, el binario registra el arranque y termina.
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

	// subcomando es el verbo con el que se invocó el binario ("", "enroll", "serve", "cajero"). Se
	// calcula ANTES de las migraciones porque una de ellas depende de él (ver justo debajo).
	subcomando := ""
	if len(os.Args) > 1 {
		subcomando = os.Args[1]
	}

	// Migración clean-slate hacia la BD ÚNICA (Plan 022 T1, ADR-0018 §8, fase 1): archiva el layout
	// multi-sesión POR-DIRECTORIO (sessions/<id>/) bajo <data_dir>/_archived-pre-022/ y deja sessions/
	// vacío. NO borra el árbol viejo (T6.5 lo lee para restaurar las sesiones ACTIVAS sin re-escanear).
	// Idempotente (no-op si ya migró) y NO fatal: un fallo de E/S no impide arrancar (se loguea y sigue).
	//
	// 🔴 EL CAJERO NO LA CORRE. Desde el Plan 051 Ola 2 hay DOS procesos de este binario vivos a la vez
	// (`agent serve` y `agent cajero`, ambos hijos de wapp-ctl) y el supervisor puede arrancarlos casi
	// en paralelo: dos procesos MOVIENDO el mismo árbol `sessions/` sin coordinación no tiene por qué
	// romper nada (es idempotente y no fatal), pero tampoco tiene por qué pasar. El dueño del layout es
	// el daemon; el cajero sólo LEE de edge.db y de la BD de la cola, y no necesita nada de esto.
	if subcomando != "cajero" {
		if err := edgemigrate.ArchiveLegacyPerSessionLayout(cfg.DataDir, log); err != nil {
			log.Error("migración clean-slate a BD única de arranque falló (continuo de todas formas)",
				"error", err, "data_dir", cfg.DataDir)
		}
	}

	// Despacho de subcomandos. Sin argumento o `serve`: daemon multi-sesión.
	if subcomando == "enroll" {
		if err := runEnroll(context.Background(), cfg, log); err != nil {
			log.Error("enrolamiento fallido", "error", err)
			os.Exit(1)
		}
		return
	}

	if subcomando == "serve" {
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

	// Worker-cajero (Plan 051 Ola 2 · T2.2). Mismo molde que `serve` —misma config, mismo logger, mismo
	// Layout— y el mismo ctx atado a SIGINT/SIGTERM para que el supervisor lo pueda parar limpio. NO
	// usa logsink: no expone plano de control, así que no hay quien lea el ring buffer de logs.
	if subcomando == "cajero" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := runCajero(ctx, cfg, log); err != nil {
			log.Error("worker-cajero fallido", "error", err)
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
