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
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/logsink"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/server"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/intent"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	waconn "github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/edgemigrate"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/enroll"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/logger"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/wiring"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"github.com/google/uuid"
)

// Version identifica la build del Edge Agent. Se inyecta en release vía
// -ldflags "-X main.Version=$(git describe --tags --always --dirty)" (ver
// Makefile, Plan 023 · T0). DEBE seguir siendo `var` (no `const`): ldflags -X
// solo sobre-escribe variables de string. El literal de abajo es el fallback de
// dev cuando se compila sin ldflags (go run/build directos, CI). La versión
// resultante viaja a /v1/health (server.Config.Version) y a los logs de arranque.
var Version = "0.1.0-bootstrap"

// singleDBFileName es el nombre de la BD ÚNICA del Edge (Plan 022 T3) bajo data_dir cuando el dialecto es
// SQLite y no se pasó un DSN explícito. Aloja metadatos (accounts/devices) + whatsmeow_* + msg_enc_* en un
// solo fichero (retira el modelo de sessions.db meta + store.db por sesión).
const singleDBFileName = "edge.db"

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

	// Despacho de subcomandos. Sin argumento: bootstrap (arranque informativo).
	if len(os.Args) > 1 && os.Args[1] == "pair" {
		if err := runPair(context.Background(), cfg, log); err != nil {
			log.Error("emparejamiento fallido", "error", err)
			os.Exit(1)
		}
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "send" {
		if len(os.Args) < 4 {
			log.Error("uso: agent send <destino> <texto>")
			os.Exit(1)
		}
		if err := runSend(context.Background(), cfg, log, os.Args[2], os.Args[3]); err != nil {
			log.Error("envio fallido", "error", err)
			os.Exit(1)
		}
		return
	}

	// `enroll` ejecuta el enrolamiento real contra el Gateway: genera el par mTLS del Edge a partir de
	// un código de activación y persiste cert+clave en TLSCert/TLSKey para que `listen` use mTLS.
	if len(os.Args) > 1 && os.Args[1] == "enroll" {
		if err := runEnroll(context.Background(), cfg, log); err != nil {
			log.Error("enrolamiento fallido", "error", err)
			os.Exit(1)
		}
		return
	}

	// `listen` y `restore` comparten flujo: restaurar la sesión persistida (T6.2) y mantener el
	// socket vivo 24/7. Se mantienen ambos nombres por claridad del hito T6.3.
	if len(os.Args) > 1 && (os.Args[1] == "listen" || os.Args[1] == "restore") {
		if err := runRestore(cfg, log); err != nil {
			log.Error("restauración/escucha fallida", "error", err)
			os.Exit(1)
		}
		return
	}

	// `serve` es el daemon MULTI-SESIÓN unificado (Plan 008 + plano de control Plan 007): en UN SOLO
	// proceso restaura TODAS las sesiones activas del registro y mantiene un listener por sesión 24/7 vía
	// el Session Manager (concurrencia Go, ADR-0003) Y levanta el contrato /v1 sobre el Unix socket
	// co-ubicado (health/sessions/logs/pairing/unlink), con apagado ordenado bajo el mismo ctx
	// (SIGINT/SIGTERM, §10.I). El logger se construye con tee al ring-buffer (logsink) para alimentar
	// GET /v1/logs sin perder stdout. CloudLink real por-sesión (session_id + lease) llega en T7.
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

// runPair ejecuta el caso de uso app.Pair con los adaptadores REALES: abre/migra el store SQLite
// cifrado, construye el conector whatsmeow real, pinta el QR en la terminal (os.Stdout) y sella la
// DEK en la custodia de archivo. Es interactivo: requiere escanear el QR con un telefono real.
func runPair(ctx context.Context, cfg config.Config, log sharedlogger.Logger) error {
	database, err := db.OpenAndMigrate(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()

	connector := waconn.NewConnector(database, db.DialectSQLite)
	qrSink := control.NewTerminalQRSink(os.Stdout)
	custody := keycustody.NewFileCustody(cfg.DEKPath)

	// Loguea la RUTA ABSOLUTA del store (MP-02, D2): el operador siempre debe ver dónde vive el store,
	// nunca depender del CWD ni de dónde se lanzó el comando. Sin secretos (jamás la DEK).
	log.Info("emparejamiento: escanea el QR con WhatsApp (Dispositivos vinculados)",
		"data_dir", cfg.DataDir, "db_path", cfg.DBPath, "dek_path", cfg.DEKPath)

	pairer := app.NewPair(connector, qrSink, custody)
	res, err := pairer.Run(ctx)
	if err != nil {
		return err
	}

	// Registra los METADATOS DE NEGOCIO de la sesión recién pareada (RF-7) para que RestoreSessions
	// la reanude al reiniciar. En claro: session_id + jid + estado + store_dir + timestamps (sin
	// material cripto).
	//
	// PUENTE T0 (Plan 008): el modelo multi-sesión llava por session_id (ADR-0016 §3), así que aquí se
	// genera el UUID de la sesión y su store_dir relativo. La GENERALIZACIÓN real (el Manager genera el
	// session_id ANTES del pairing, mkdir del dir por sesión, Upsert(pairing)->Upsert(active)) llega en
	// T3; este `agent pair` single-sesión es legacy y se reescribe entonces.
	now := time.Now()
	sessionID := uuid.NewString()
	sess := domain.Session{
		SessionID: sessionID,
		JID:       res.WaJID,
		// SelfPN + Role alineados con el camino multi-sesión (Plan 027 T5, cierra H8): el número propio se
		// deriva del JID recién pareado (implementación única domain.SelfPNFromJID) y el device arranca como
		// primary. Antes runPair legacy dejaba ambos vacíos, dejando la fila inconsistente con las que crea
		// el Manager (assignRoleLocked/self_pn).
		SelfPN:    domain.SelfPNFromJID(res.WaJID),
		Role:      domain.DeviceRolePrimary,
		State:     domain.SessionStateActive,
		StoreDir:  "sessions/" + sessionID,
		PairedAt:  now,
		UpdatedAt: now,
	}
	if err := sessionstore.New(database).Upsert(ctx, sess); err != nil {
		return err
	}

	log.Info("emparejamiento completado (PairSuccess): DEK sellada, store cifrado creado y sesión registrada",
		"wa_jid", res.WaJID, "db_path", cfg.DBPath, "dek_path", cfg.DEKPath)
	return nil
}

// runSend ejecuta el caso de uso app.Send con los adaptadores REALES: abre/migra el store SQLite
// cifrado, carga la DEK custodiada en archivo, construye el sender whatsmeow real (que resuelve la
// sesion pareada, conecta un cliente efimero y despacha el texto) y envia. Requiere una sesion ya
// emparejada (subcomando `pair`); envia por red de verdad (es el hito interactivo T4.3).
func runSend(ctx context.Context, cfg config.Config, log sharedlogger.Logger, to, text string) error {
	database, err := db.OpenAndMigrate(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()

	custody := keycustody.NewFileCustody(cfg.DEKPath)
	sender := waconn.NewSender(database, db.DialectSQLite)

	log.Info("envio: despachando texto a WhatsApp",
		"to", to, "db_path", cfg.DBPath, "dek_path", cfg.DEKPath)

	if err := app.NewSend(custody, sender).Run(ctx, to, text); err != nil {
		return err
	}

	log.Info("envio completado: texto despachado a WhatsApp", "to", to)
	return nil
}

// runRestore ejecuta el caso de uso app.RestoreSessions con los adaptadores REALES (T6.2/T6.3): abre/
// migra el store SQLite cifrado (aplica 0001 + 0002), resuelve la sesion a restaurar desde la tabla
// `sessions` (o la backfillea desde el store cifrado si la BD se pareo antes de existir esa tabla),
// marca la sesion activa y DELEGA la escucha always-on a app.Listen (que carga la DEK custodiada,
// reconstruye el device pareado, conecta el cliente y registra el Listener). Reenvia cada mensaje
// entrante al LogSink (stub CloudLink del spike) y mantiene el socket VIVO hasta Ctrl-C / SIGINT.
// Requiere una sesion ya emparejada (subcomando `pair`); reanuda SIN re-emparejar (es el hito T6.3).
func runRestore(cfg config.Config, log sharedlogger.Logger) error {
	// ctx cancelado por SIGINT (Ctrl-C) o SIGTERM: dispara el cierre limpio del socket always-on.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	database, err := db.OpenAndMigrate(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()

	custody := keycustody.NewFileCustody(cfg.DEKPath)
	gateway := waconn.NewListenGateway(database, log)
	// Nombre visible de FALLBACK para anunciar presencia (Plan 013 §10.D): solo se usa si el store
	// restaurado no trae ya el nombre real de la cuenta (ver ListenGateway.SetPushName).
	gateway.SetPushName(cfg.PushName)
	restore := newEscucha(ctx, cfg, log, database, custody, gateway)

	log.Info("restaurando sesion persistida y manteniendo el socket vivo (envia un WhatsApp al numero para ver el InboundEvent; Ctrl-C para detener)",
		"db_path", cfg.DBPath, "dek_path", cfg.DEKPath)

	if err := restore.Run(ctx); err != nil {
		return err
	}

	log.Info("restauracion/escucha finalizada: socket cerrado limpiamente")
	return nil
}

// newEscucha cablea la escucha always-on (RestoreSessions -> app.Listen) sobre la BD viva y el sink de
// reenvío (CloudLink real con tee a LogSink si hay endpoint; LogSink puro si no). Comparte el cableado
// del núcleo entre `runRestore` (subcomando listen/restore) y `runServe` (subcomando serve) para no
// DUPLICAR la construcción: misma BD, misma custodia, mismo gateway de escucha y mismo sink.
//
// Cliente vivo (lección Plan 006): el envío reutiliza el cliente VIVO de la escucha (buildSink detecta
// app.LiveSender en el gateway y enruta SendViaLiveClient), no un cliente efímero que dejaría sorda la
// escucha. El pairing es aparte (subcomando serve): usa un cliente efímero de identidad NUEVA, ver runServe.
func newEscucha(ctx context.Context, cfg config.Config, log sharedlogger.Logger, database *sql.DB, custody app.KeyCustody, gateway *waconn.ListenGateway) *app.RestoreSessions {
	sessions := sessionstore.New(database)
	locator := sessionstore.NewLocator(database)

	// Clasificador de intenciones (Plan 029): stack compartido (decorador + applier de config empujada).
	// Feature off ⇒ stack vacío (sink sin decorar, cableado idéntico). Bootstrap recarga la config
	// persistida (last-known-good) para arrancar clasificando sin esperar un push del Cloud.
	intentStack := wiring.BuildIntent(cfg, database, log)
	if intentStack.Service != nil {
		intentStack.Service.Bootstrap(ctx)
	}

	// Sink: conducto CloudLink REAL si hay endpoint configurado (con LogSink en tee para diagnóstico);
	// si no, LogSink puro (no rompe el flujo del spike). El adaptador corre su loop de conexión en
	// segundo plano, ligado al mismo ctx (se cierra con SIGINT junto al socket).
	sink := wiring.BuildSink(ctx, cfg, log, custody, database, gateway, intentStack)

	// app.Listen hace el restore CRIPTOGRAFICO + socket always-on; RestoreSessions le antepone el
	// registro de negocio (resolver/backfillear/marcar activa la sesion). Sin duplicar la conexion.
	listener := app.NewListen(custody, gateway, sink)
	return app.NewRestoreSessions(sessions, locator, listener)
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
	// Contexto hijo: apaga el Manager y la escucha al salir por CUALQUIER vía (señal o caída del /v1).
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// BD ÚNICA del Edge (Plan 022 T3, decisión §10.A): una sola *sql.DB con TODO — metadatos de negocio
	// (accounts/devices), el Container whatsmeow COMPARTIDO (whatsmeow_*) y el store cifrado per-device
	// (msg_enc_*). Dialecto/DSN CONMUTABLES por config (T0): SQLite embebido por defecto (fichero bajo
	// data_dir), Postgres solo por cadena. Migra AMBOS sets (store + meta) sobre la MISMA BD. La DEK sigue
	// custodiada POR DISPOSITIVO bajo <data_dir>/keys/<id>.key (DESACOPLADA de la BD, §3/§10.C).
	dbDSN := cfg.DBDSN
	if cfg.DBDialect == db.DialectSQLite && dbDSN == "" {
		dbDSN = filepath.Join(cfg.DataDir, singleDBFileName)
	}
	database, err := db.Open(ctx, cfg.DBDialect, dbDSN)
	if err != nil {
		return fmt.Errorf("serve: abrir la BD única (dialecto %q): %w", cfg.DBDialect, err)
	}
	defer func() { _ = database.Close() }()
	if err := db.Migrate(ctx, database); err != nil {
		return fmt.Errorf("serve: migrar la BD única: %w", err)
	}

	// Migración FASE 2 hacia la BD ÚNICA (Plan 022 T6.5, ADR-0018 §8, §10.K): restaura las sesiones
	// ACTIVAS que la fase 1 archivó en _archived-pre-022/ (JID+DEK+msg_enc_*) SIN re-escanear, con la
	// MISMA DEK per-device (keys/<id>.key) y mismo JID. Corre DESPUÉS de migrar el esquema (tablas listas)
	// y ANTES de Restore (que arranca un listener por device activo ya presente). NO fatal: un fallo se
	// loguea y se continúa; los devices caducados/fallidos caen al re-escaneo sin tumbar a los demás.
	if err := edgemigrate.RestoreArchivedActiveSessions(ctx, cfg.DataDir, database, cfg.DBDialect, log); err != nil {
		log.Error("serve: restauración de sesiones activas archivadas (T6.5) falló (continuo de todas formas)",
			"error", err, "data_dir", cfg.DataDir)
	}

	sessions := sessionstore.New(database)
	layout := sessionmgr.NewLayout(cfg.DataDir)

	// Multiplexor CloudLink REAL (T7): UN solo stream Connect por Edge que multiplexa N sesiones por
	// session_id (ADR-0008) con lease POR sesión (ADR-0016 §5). Su loop de stream corre en goroutine
	// ligada a ctx. Sin endpoint configurado cae a LogMux (diagnóstico por sesión, sin red). El Manager
	// registra cada sesión (live-sender + presencia de DEK) al arrancar su listener y la quita en Unlink.
	// Outbox durable (Plan 027 Ola 3 · T2, H2) sobre la BD ÚNICA: los entrantes/acuses con el stream
	// CloudLink caído se encolan y drenan en orden al reconectar en vez de descartarse (ADR-0003).
	outbox := wiring.BuildOutbox(ctx, cfg, database, log)

	// Clasificador de intenciones (Plan 029): stack COMPARTIDO por todas las sesiones (un solo Ollama, un
	// solo circuito). Feature off ⇒ stack vacío (applier/decorador nil ⇒ cableado idéntico al previo).
	// Bootstrap recarga la config persistida (last-known-good) antes de arrancar los listeners, para que la
	// clasificación arranque activa sin esperar un nuevo push del Cloud.
	intentStack := wiring.BuildIntent(cfg, database, log)
	if intentStack.Service != nil {
		intentStack.Service.Bootstrap(ctx)
	}
	mux := wiring.BuildMux(ctx, cfg, log, outbox, intentStack)

	// Manager con la BD ÚNICA compartida (WithSharedDB): escucha real per-device (un listener por device
	// que carga por SU JID sobre la BD compartida) y pairing real (Container per-device sobre la MISMA BD;
	// el QRSink lo inyecta el plano de control POR emparejamiento para el polling async del QR).
	mgr := sessionmgr.NewManager(layout, sessions, cfg.MaxSessions, log,
		sessionmgr.WithSharedDB(database, cfg.DBDialect),
		sessionmgr.WithWhatsmeowListen(mux, cfg.PushName),
		sessionmgr.WithWhatsmeowPairing(app.DefaultPairTimeout),
		// Failover multi-dispositivo por número (Plan 022 T5, §10.F): off por defecto (1). RESILIENCIA, no sigilo.
		sessionmgr.WithMultiDevicePerAccount(cfg.MultiDevicePerAccount),
		// Clasificador de intenciones (Plan 029 · T11): con la feature ON cada listener envuelve su sink con el
		// decorador compartido; off ⇒ DecoratorWrap devuelve nil ⇒ WithInboundDecorator no altera el cableado.
		sessionmgr.WithInboundDecorator(intentStack.DecoratorWrap()),
	)

	if err := mgr.Restore(ctx); err != nil {
		return fmt.Errorf("serve: restaurar sesiones activas: %w", err)
	}

	// Servidor /v1 sobre el Unix socket co-ubicado, LLAVEADO POR session_id contra el Manager (inventario
	// + salud por sesión). Endpoints colgados antes de Serve: logs SSE (Plan 007), pairing async y unlink.
	srv := server.New(
		server.Config{SocketPath: cfg.ControlSocketPath, Version: Version},
		log, managerInventory{mgr},
	)
	srv.Handle(http.MethodGet, "/v1/logs", logsink.Handler(sink))
	// GET /v1/intent/status (Plan 029 · T13): estado del clasificador local (la web de onboarding lo
	// consumirá en el Plan 030). Se registra SIEMPRE; con la feature off reporta enabled=false. El sondeo de
	// Ollama tiene timeout corto (no bloquea el endpoint).
	srv.Handle(http.MethodGet, "/v1/intent/status", intent.StatusHandler(intent.StatusDeps{
		Enabled:       intentStack.Enabled,
		Model:         intentStack.Model,
		Prober:        intentStack.Prober,
		ConfigVersion: intentStack.ConfigVersion,
		Circuit:       intentStack.CircuitFunc(),
	}))
	srv.RegisterPairing(mgr) // POST /v1/sessions/pair → Manager.Pair (async; QR por polling)
	srv.RegisterUnlink(mgr)  // DELETE /v1/sessions/{id} → Manager.Unlink(session_id)
	// GET /v1/enroll/status + POST /v1/enroll (onboarding sin terminal, Plan 023 · T1).
	srv.RegisterEnroll(enrollPort{cfg: cfg, log: log})

	ln, err := srv.Listen()
	if err != nil {
		return fmt.Errorf("serve: abrir socket /v1: %w", err)
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	log.Info("agent serve: daemon multi-sesión + plano de control /v1 en un solo proceso",
		"socket", cfg.ControlSocketPath, "version", Version, "data_dir", cfg.DataDir, "max_sesiones", cfg.MaxSessions)

	// Cierre unificado: señal/cancelación (ctx) o caída del servidor /v1.
	select {
	case <-ctx.Done():
		log.Info("agent serve: señal de cierre recibida, apagando")
	case err := <-serveErr:
		if err != nil {
			log.Error("agent serve: el servidor /v1 falló, apagando", "error", err)
		}
	}

	// Apaga el servidor /v1 (drena conexiones y elimina el socket file: sin socket huérfano) y detiene el
	// Manager (cancela cada listener, espera el WaitGroup y cierra cada store.db — apagado ordenado §10.I).
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("agent serve: cierre del servidor /v1 con error", "error", err)
	}
	mgr.Stop()

	log.Info("agent serve: detenido limpiamente (socket /v1 cerrado, listeners apagados)")
	return nil
}

// managerInventory adapta *sessionmgr.Manager al puerto de LECTURA del plano de control (server.
// SessionLister): GET /v1/sessions combina el inventario persistido (Persisted) con la salud de runtime
// por sesión (Health → etiqueta). Mantiene el paquete server desacoplado del tipo SessionHealth.
type managerInventory struct{ mgr *sessionmgr.Manager }

// Persisted devuelve TODAS las sesiones registradas (incluye 'pairing' aún no viva).
func (m managerInventory) Persisted(ctx context.Context) ([]domain.Session, error) {
	return m.mgr.Persisted(ctx)
}

// Health devuelve la etiqueta de salud de runtime de una sesión viva (ok=false si no está viva).
func (m managerInventory) Health(id string) (string, bool) {
	h, ok := m.mgr.Health(id)
	return h.String(), ok
}

// enrollPort adapta el enroll REAL (internal/infra/enroll) al puerto del plano de control
// (server.RegisterEnroll, Plan 023 · T1): ejecuta el mismo enrolamiento que `agent enroll <codigo>`
// REUSANDO enroll.Run —no lo reimplementa— y reporta si el par mTLS ya vive en disco. Guarda la config
// del núcleo para poblar EnrollmentEndpoint/TLSCA/rutas destino; por HTTP solo llega el activation code.
// Zero-knowledge: enroll.Run persiste el par mTLS + cloud_enc_pubkey, NUNCA la DEK.
type enrollPort struct {
	cfg config.Config
	log sharedlogger.Logger
}

// Enrolled reporta si el par mTLS (cert + clave) ya está presente en disco: la señal de "primera
// ejecución" que la web usa para elegir entre la pantalla enrolar y el dashboard.
func (p enrollPort) Enrolled() bool {
	return mtlsFileExists(p.cfg.CloudLink.TLSCert) && mtlsFileExists(p.cfg.CloudLink.TLSKey)
}

// Enroll valida las precondiciones de bootstrap (endpoint + TLSCA pre-provistos y rutas destino) y
// delega en enroll.Run con el activation code recibido. Mismas validaciones que runEnroll (subcomando
// CLI), para dar un mensaje claro en la web en vez de un fallo opaco del dial.
func (p enrollPort) Enroll(ctx context.Context, activationCode string) error {
	cfg := p.cfg
	cfg.CloudLink.ActivationCode = strings.TrimSpace(activationCode)

	if cfg.CloudLink.EnrollmentEndpoint == "" {
		return fmt.Errorf("falta enrollment_endpoint (bootstrap del paquete): configura cloudlink.enrollment_endpoint")
	}
	if cfg.CloudLink.TLSCA == "" {
		return fmt.Errorf("falta tls_ca: la CA que valida al Gateway debe estar pre-provista antes de enrolar")
	}
	if cfg.CloudLink.TLSCert == "" || cfg.CloudLink.TLSKey == "" {
		return fmt.Errorf("faltan rutas destino tls_cert/tls_key donde persistir la credencial mTLS")
	}
	if cfg.CloudLink.ActivationCode == "" {
		return fmt.Errorf("falta el código de activación")
	}

	p.log.Info("enroll web: enrolando el Edge contra el Gateway",
		"endpoint", cfg.CloudLink.EnrollmentEndpoint, "tls_cert", cfg.CloudLink.TLSCert)
	return enroll.Run(ctx, cfg, p.log)
}

// mtlsFileExists indica si path apunta a un archivo REGULAR existente (no directorio, no vacío en ruta).
func mtlsFileExists(path string) bool {
	if path == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
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
