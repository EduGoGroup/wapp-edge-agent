// Command agent es el daemon del Edge Agent de wApp.
//
// Bootstrap minimo (T0, Plan 002): carga configuracion, construye el logger y
// registra el arranque. El subcomando `pair` (T3.4) ejecuta el emparejamiento por
// QR local con los adaptadores REALES (store SQLite cifrado + whatsmeow + control
// en terminal + custodia de la DEK en archivo). La logica restante (CloudLink,
// listener 24/7, systray) se incorpora en chunks posteriores.
package main

import (
	"context"
	"os"

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
