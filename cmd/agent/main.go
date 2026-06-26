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
	"os"
	"os/signal"
	"syscall"

	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cloudlink"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	waconn "github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/logger"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Version identifica la build del Edge Agent.
const Version = "0.1.0-bootstrap"

func main() {
	path := os.Getenv("WAPP_AGENT_CONFIG")
	if path == "" {
		path = "config.yaml"
	}

	cfg, err := config.Load(path)
	if err != nil {
		sharedlogger.Default().Error("no se pudo cargar la configuracion",
			"error", err, "path", path)
		os.Exit(1)
	}

	log := logger.New(cfg)

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

	// `listen` y `restore` comparten flujo: restaurar la sesión persistida (T6.2) y mantener el
	// socket vivo 24/7. Se mantienen ambos nombres por claridad del hito T6.3.
	if len(os.Args) > 1 && (os.Args[1] == "listen" || os.Args[1] == "restore") {
		if err := runRestore(cfg, log); err != nil {
			log.Error("restauración/escucha fallida", "error", err)
			os.Exit(1)
		}
		return
	}

	log.Info("wapp-edge-agent arrancando",
		"version", Version,
		"log_level", cfg.LogLevel,
		"log_json", cfg.LogJSON,
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

	connector := waconn.NewConnector(database)
	qrSink := control.NewTerminalQRSink(os.Stdout)
	custody := keycustody.NewFileCustody(cfg.DEKPath)

	log.Info("emparejamiento: escanea el QR con WhatsApp (Dispositivos vinculados)",
		"db_path", cfg.DBPath, "dek_path", cfg.DEKPath)

	pairer := app.NewPair(connector, qrSink, custody)
	res, err := pairer.Run(ctx)
	if err != nil {
		return err
	}

	// Registra los METADATOS DE NEGOCIO de la sesión recién pareada (T6.1) para que RestoreSessions
	// la reanude al reiniciar (RF-7). En claro: jid + estado + timestamps (sin material cripto).
	now := time.Now()
	sess := domain.Session{
		JID:       res.WaJID,
		State:     domain.SessionStateActive,
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
	sender := waconn.NewSender(database)

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
	sink := cloudlink.NewLogSink(log)
	sessions := sessionstore.New(database)
	locator := sessionstore.NewLocator(database)

	// app.Listen hace el restore CRIPTOGRAFICO + socket always-on; RestoreSessions le antepone el
	// registro de negocio (resolver/backfillear/marcar activa la sesion). Sin duplicar la conexion.
	listener := app.NewListen(custody, gateway, sink)
	restore := app.NewRestoreSessions(sessions, locator, listener)

	log.Info("restaurando sesion persistida y manteniendo el socket vivo (envia un WhatsApp al numero para ver el InboundEvent; Ctrl-C para detener)",
		"db_path", cfg.DBPath, "dek_path", cfg.DEKPath)

	if err := restore.Run(ctx); err != nil {
		return err
	}

	log.Info("restauracion/escucha finalizada: socket cerrado limpiamente")
	return nil
}
