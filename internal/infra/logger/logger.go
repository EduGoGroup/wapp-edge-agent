// Package logger adapta el logger estructurado de wapp-shared a la configuracion
// del Edge Agent (nivel y formato JSON tomados de config.Config).
package logger

import (
	"log/slog"
	"strings"

	edgeconfig "github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// New construye el Logger del Edge Agent a partir de la configuracion dada,
// aplicando nivel y formato (texto/JSON) segun cfg.
func New(cfg edgeconfig.Config) sharedlogger.Logger {
	return sharedlogger.New(
		sharedlogger.WithLevel(ParseLevel(cfg.LogLevel)),
		sharedlogger.WithJSON(cfg.LogJSON),
	)
}

// ParseLevel traduce un nivel textual (debug, info, warn/warning, error) al
// slog.Level correspondiente. Cualquier valor desconocido o vacio devuelve
// slog.LevelInfo.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
