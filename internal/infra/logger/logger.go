// Package logger adapta el logger estructurado de wapp-shared a la configuracion
// del Edge Agent (nivel y formato JSON tomados de config.Config).
package logger

import (
	"io"
	"log/slog"
	"os"
	"strings"

	edgeconfig "github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// New construye el Logger del Edge Agent a partir de la configuracion dada,
// aplicando nivel y formato (texto/JSON) segun cfg.
func New(cfg edgeconfig.Config) sharedlogger.Logger {
	return newWithWriters(cfg, os.Stdout, nil)
}

// NewWithSink construye el Logger del Edge "teeando" su salida a stdout Y al sink dado (el
// ring-buffer de internal/adapters/control/logsink, que alimenta GET /v1/logs). El comportamiento
// en stdout es IDENTICO al de New: mismo nivel, mismo formato; el sink recibe exactamente las
// mismas lineas ya formateadas, porque ambos destinos comparten el mismo slog.Handler vía
// io.MultiWriter (wapp-shared/logger no expone su Handler, pero si acepta el io.Writer destino).
// Si sink es nil, equivale a New. Este es el PUNTO DE ENGANCHE para T3 (agent serve): construir el
// sink y pasar logger.NewWithSink(cfg, sink) en lugar de logger.New(cfg).
func NewWithSink(cfg edgeconfig.Config, sink io.Writer) sharedlogger.Logger {
	return newWithWriters(cfg, os.Stdout, sink)
}

// newWithWriters centraliza la construccion del Logger sobre wapp-shared, dirigiendo la salida a
// base (stdout en produccion) y, si sink != nil, tambien a sink mediante io.MultiWriter. Es
// unexportada y parametriza base para poder testear el tee sin tocar el stdout real del proceso.
func newWithWriters(cfg edgeconfig.Config, base, sink io.Writer) sharedlogger.Logger {
	w := base
	if sink != nil {
		w = io.MultiWriter(base, sink)
	}
	return sharedlogger.New(
		sharedlogger.WithLevel(ParseLevel(cfg.LogLevel)),
		sharedlogger.WithJSON(cfg.LogJSON),
		sharedlogger.WithWriter(w),
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
