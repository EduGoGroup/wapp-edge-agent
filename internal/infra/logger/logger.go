// Package logger adapta el logger estructurado de wapp-shared a la configuracion
// del Edge Agent (nivel y formato JSON tomados de config.Config).
package logger

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
	writers := []io.Writer{base}
	if sink != nil {
		writers = append(writers, sink)
	}
	if fw := fileWriter(); fw != nil {
		writers = append(writers, fw)
	}
	w := base
	if len(writers) > 1 {
		w = io.MultiWriter(writers...)
	}
	return sharedlogger.New(
		sharedlogger.WithLevel(ParseLevel(cfg.LogLevel)),
		sharedlogger.WithJSON(cfg.LogJSON),
		sharedlogger.WithWriter(w),
	)
}

// fileWriter abre, cuando la env WAPP_LOG_FILE está seteada, el archivo de log del Edge para
// escribir ADEMÁS de stdout (y del ring-buffer de /v1/logs). Lo fijan los lanzadores de
// autoarranque de Windows/Linux (Plan 024 · T1) con WAPP_LOG_FILE=<data_dir>/logs/edge.log; en
// macOS NO se setea (el LaunchAgent ya redirige stdout/stderr vía StandardOutPath), evitando el
// doble log. Tanto wapp-ctl como su hijo `agent serve` heredan la env (os.Environ) y abren el
// MISMO archivo en O_APPEND: sus líneas se intercalan sin truncarse — aceptable para el kit de
// test delegado. Si el path no se puede crear/abrir, degrada a solo-stdout con un aviso a stderr
// (NO rompe el arranque). El handle vive lo que dura el proceso (daemon 24/7); no se cierra.
func fileWriter() io.Writer {
	path := strings.TrimSpace(os.Getenv("WAPP_LOG_FILE"))
	if path == "" {
		return nil
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			fmt.Fprintf(os.Stderr, "logger: no se pudo crear el dir de WAPP_LOG_FILE %q: %v (log solo a stdout)\n", path, err)
			return nil
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger: no se pudo abrir WAPP_LOG_FILE %q: %v (log solo a stdout)\n", path, err)
		return nil
	}
	return f
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
