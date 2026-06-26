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

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cloudlink"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	waconn "github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
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

	if len(os.Args) > 1 && os.Args[1] == "listen" {
		if err := runListen(cfg, log); err != nil {
			log.Error("escucha fallida", "error", err)
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

	log.Info("emparejamiento completado (PairSuccess): DEK sellada y store cifrado creado",
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

// runListen ejecuta el caso de uso app.Listen con los adaptadores REALES: abre/migra el store SQLite
// cifrado, carga la DEK custodiada en archivo, construye el ListenGateway whatsmeow real (always-on:
// resuelve la sesion pareada, conecta el cliente, registra el Listener) y reenvia cada mensaje
// entrante al LogSink (stub CloudLink del spike). Mantiene el socket VIVO hasta Ctrl-C / SIGINT,
// momento en que cancela el ctx para un cierre limpio. Requiere una sesion ya emparejada (subcomando
// `pair`); recibe por red de verdad (es el hito interactivo T5.5).
func runListen(cfg config.Config, log sharedlogger.Logger) error {
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

	log.Info("escucha 24/7: manteniendo el socket vivo (envia un WhatsApp al numero para ver el InboundEvent; Ctrl-C para detener)",
		"db_path", cfg.DBPath, "dek_path", cfg.DEKPath)

	if err := app.NewListen(custody, gateway, sink).Run(ctx); err != nil {
		return err
	}

	log.Info("escucha 24/7 finalizada: socket cerrado limpiamente")
	return nil
}
