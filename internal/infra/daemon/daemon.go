// Package daemon orquesta el arranque unificado del Edge Agent (Plan 008 + 007 + 022 + 027 + 029 + 031 + 033):
// BD única, outbox, clasificador de intenciones, auth de operador, session manager, servidor /v1 y
// shutdown ordenado.
package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/enrolladapter"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/inventory"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/logsink"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/server"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/intent"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/diagnostics"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/edgemigrate"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/wiring"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// SingleDBFileName es el nombre de la BD ÚNICA del Edge (Plan 022 T3) bajo data_dir.
//
// ESTÁ EXPORTADA porque el daemon ya NO es el único proceso que abre ese fichero: el worker-cajero
// (`agent cajero`, Plan 051 Ola 2) lee de ahí el contrato de intenciones. Tenía el nombre duplicado en
// un literal de cmd/agent y ese duplicado fallaba EN SILENCIO: si el nombre cambiara aquí, el cajero
// abriría un `edge.db` vacío, `Listo()` devolvería false para siempre y el worker no reclamaría nada,
// sin un solo error. Un símbolo compartido cuesta menos que ese modo de fallo.
//
// Sigue siendo el daemon el DUEÑO del fichero: es quien lo migra. El cajero sólo lo abre para leer.
const SingleDBFileName = "edge.db"

// Daemon encapsula el ciclo de vida y la orquestación de componentes del Edge Agent multi-sesión.
type Daemon struct {
	cfg       config.Config
	log       sharedlogger.Logger
	sink      *logsink.Sink
	version   string
	startedAt time.Time
}

// New construye una instancia del Daemon.
func New(cfg config.Config, log sharedlogger.Logger, sink *logsink.Sink, version string, startedAt time.Time) *Daemon {
	return &Daemon{
		cfg:       cfg,
		log:       log,
		sink:      sink,
		version:   version,
		startedAt: startedAt,
	}
}

// Run ejecuta el daemon multi-sesión unificado y bloquea hasta la cancelación del contexto o fallo del servidor.
func (d *Daemon) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg := d.cfg
	log := d.log

	dbDSN := cfg.DBDSN
	if cfg.DBDialect == db.DialectSQLite && dbDSN == "" {
		dbDSN = filepath.Join(cfg.DataDir, SingleDBFileName)
	}
	database, err := db.Open(ctx, cfg.DBDialect, dbDSN)
	if err != nil {
		return fmt.Errorf("serve: abrir la BD única (dialecto %q): %w", cfg.DBDialect, err)
	}
	defer func() { _ = database.Close() }()
	if err := db.Migrate(ctx, database); err != nil {
		return fmt.Errorf("serve: migrar la BD única: %w", err)
	}

	if err := edgemigrate.RestoreArchivedActiveSessions(ctx, cfg.DataDir, database, cfg.DBDialect, log); err != nil {
		log.Error("serve: restauración de sesiones activas archivadas (T6.5) falló (continuo de todas formas)",
			"error", err, "data_dir", cfg.DataDir)
	}

	sessions := sessionstore.New(database)
	layout := sessionmgr.NewLayout(cfg.DataDir)

	// Custodia de la DEK por sesión: ÚNICO punto de verdad sobre cómo se custodia (Plan 035 · DIP). Lo
	// comparten el session manager (WithKeyCustodyFactory) y la cola de entrantes (CrypterFor), para que no
	// existan dos criterios distintos de dónde vive la DEK.
	custodyFor := func(p string) app.KeyCustody { return keycustody.NewFileCustody(p) }

	// Cola durable de entrantes (Plan 051 Ola 1): vive en su PROPIA BD SQLite (<data_dir>/cola_entrantes.db),
	// NUNCA dentro de edge.db — por eso se abre aparte y se migra con MigrateCola (set "cola"), no con
	// Migrate. NO es fatal en ningún paso: si el fichero no se puede abrir o migrar, se loguea y el daemon
	// sigue con los listeners SIN cola (comportamiento idéntico al previo). El handle se cierra igual que el
	// de edge.db (defer, apagado ordenado §10.I).
	var colaDB *sql.DB
	if opened, colaErr := db.Open(ctx, db.DialectSQLite, layout.ColaDB()); colaErr != nil {
		log.Error("cola de entrantes: no se pudo abrir su BD; el Edge sigue SIN cola durable",
			"error", colaErr, "path", layout.ColaDB())
	} else {
		defer func() { _ = opened.Close() }()
		if err := db.MigrateCola(ctx, opened); err != nil {
			log.Error("cola de entrantes: no se pudo migrar su BD; el Edge sigue SIN cola durable",
				"error", err, "path", layout.ColaDB())
		} else {
			colaDB = opened
		}
	}
	cola := wiring.BuildCola(ctx, cfg, colaDB, layout, custodyFor, log)

	outbox := wiring.BuildOutbox(ctx, cfg, database, log)
	intentStack := wiring.BuildIntent(cfg, database, log)

	edgeCfgSvc := wiring.SharedEdgeConfigService(intentStack, log)
	authKeyStore := wiring.RegisterJWKS(edgeCfgSvc, log)
	intentStack.Applier = edgeCfgSvc
	edgeCfgSvc.Bootstrap(ctx)

	healthReg := health.NewRegistry()
	healthCollector := health.NewCollector(healthReg, outbox, intentStack.CircuitFunc(), d.version, d.startedAt)
	diagBuilder := diagnostics.NewBuilder(d.sink, healthCollector, cfg.DiagLogLines)

	mux, authRelay := wiring.BuildMux(ctx, cfg, log, outbox, intentStack, healthCollector, diagBuilder)

	authMgr := wiring.BuildAuthManager(cfg, log, authKeyStore, authRelay)
	authMgr.StartProactiveRefresh(ctx)

	mgr := sessionmgr.NewManager(layout, sessions, cfg.MaxSessions, log,
		sessionmgr.WithSharedDB(database, cfg.DBDialect),
		sessionmgr.WithHealthRegistry(healthReg),
		sessionmgr.WithWhatsmeowListen(mux, cfg.PushName),
		sessionmgr.WithWhatsmeowPairing(app.DefaultPairTimeout),
		sessionmgr.WithMultiDevicePerAccount(cfg.MultiDevicePerAccount),
		// Ventana temporal de ingesta (ADR-0037): lo que llegó del buzón que WhatsApp reencola tras una
		// caída no se ingiere. config.Load ya garantiza un margen > 0.
		sessionmgr.WithInboundMargin(time.Duration(cfg.InboundMarginSeconds)*time.Second),
		sessionmgr.WithInboundDecorator(intentStack.DecoratorWrap()),
		sessionmgr.WithKeyCustodyFactory(custodyFor),
		// Cola durable de entrantes (Plan 051 Ola 1): cada listener anota el entrante en disco (cifrado con
		// la DEK de SU sesión) antes de entregarlo al sink. nil (cola no disponible) ⇒ la opción no cambia
		// nada y el cableado queda idéntico al previo.
		sessionmgr.WithColaEntrantes(cola),
		// Interruptor del clasificador (Plan 051 Ola 2, T2.12): con la feature apagada, el entrante nace en
		// la cola YA resuelto (marca `apagado`) en vez de que el cajero gaste una plaza del semáforo para
		// descartarlo igual. Sale del MISMO stack que ya alimenta el decorador inline (línea de arriba), de
		// modo que ambos caminos leen un solo interruptor mientras dure la escritura doble de la Ola 1.
		sessionmgr.WithClasificadorActivo(intentStack.ClasificadorActivoFunc()),
	)

	if err := mgr.Restore(ctx); err != nil {
		return fmt.Errorf("serve: restaurar sesiones activas: %w", err)
	}

	srv := server.New(
		server.Config{SocketPath: cfg.ControlSocketPath, Version: d.version},
		log, inventory.New(mgr),
	)
	srv.SetHealthProvider(healthCollector)
	srv.SetAuthorizer(authMgr)
	srv.RegisterAuth(authMgr)
	srv.HandleAuthorized(http.MethodGet, "/v1/logs", "edge.status.read", false, logsink.Handler(d.sink))
	srv.HandleAuthorized(http.MethodGet, "/v1/intent/status", "edge.status.read", false, intent.StatusHandler(intent.StatusDeps{
		Enabled:       intentStack.Enabled,
		Model:         intentStack.Model,
		Prober:        intentStack.Prober,
		ConfigVersion: intentStack.ConfigVersion,
		Circuit:       intentStack.CircuitFunc(),
	}))
	srv.RegisterPairing(mgr)
	srv.RegisterUnlink(mgr)
	srv.RegisterEnroll(enrolladapter.New(cfg, log))

	ln, err := srv.Listen()
	if err != nil {
		return fmt.Errorf("serve: abrir socket /v1: %w", err)
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	log.Info("agent serve: daemon multi-sesión + plano de control /v1 en un solo proceso",
		"socket", cfg.ControlSocketPath, "version", d.version, "data_dir", cfg.DataDir, "max_sesiones", cfg.MaxSessions)

	select {
	case <-ctx.Done():
		log.Info("agent serve: señal de cierre recibida, apagando")
	case err := <-serveErr:
		if err != nil {
			log.Error("agent serve: el servidor /v1 falló, apagando", "error", err)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("agent serve: cierre del servidor /v1 con error", "error", err)
	}
	mgr.Stop()

	log.Info("agent serve: detenido limpiamente (socket /v1 cerrado, listeners apagados)")
	return nil
}
