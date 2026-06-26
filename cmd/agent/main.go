// Command agent es el daemon del Edge Agent de wApp.
//
// Bootstrap minimo (T0, Plan 002): carga configuracion, construye el logger y
// registra el arranque. La logica real (cryptostore, whatsmeow, CloudLink,
// systray) se incorpora en chunks posteriores.
package main

import (
	"os"

	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
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
	log.Info("wapp-edge-agent arrancando",
		"version", Version,
		"log_level", cfg.LogLevel,
		"log_json", cfg.LogJSON,
		"db_path", cfg.DBPath,
		"config_path", path,
	)
}
