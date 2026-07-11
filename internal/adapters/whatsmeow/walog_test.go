package whatsmeow

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/EduGoGroup/wapp-shared/logger"
)

// TestWALogBridge_NilLogger: sin logger el puente degrada a Noop (comportamiento previo, nil-safe).
func TestWALogBridge_NilLogger(t *testing.T) {
	if got := newWALog(nil); got != waLog.Noop {
		t.Fatalf("newWALog(nil) = %T, quería waLog.Noop", got)
	}
}

// TestWALogBridge_MapeoNivelesYModulo: cada nivel de waLog sale por su nivel slog homólogo, con el
// printf-style resuelto, el prefijo "whatsmeow:" y el sub-módulo (Sub anidado) como atributo module=.
func TestWALogBridge_MapeoNivelesYModulo(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New(logger.WithWriter(&buf), logger.WithLevel(slog.LevelDebug))

	wl := newWALog(log).Sub("Client").Sub("Socket")
	wl.Errorf("caída %d", 1)
	wl.Warnf("aviso %s", "x")
	wl.Infof("conectado")
	wl.Debugf("frame")

	out := buf.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 {
		t.Fatalf("esperaba 4 líneas, hay %d:\n%s", len(lines), out)
	}
	for i, want := range []struct{ level, msg string }{
		{"ERROR", "whatsmeow: caída 1"},
		{"WARN", "whatsmeow: aviso x"},
		{"INFO", "whatsmeow: conectado"},
		{"DEBUG", "whatsmeow: frame"},
	} {
		if !strings.Contains(lines[i], "level="+want.level) {
			t.Errorf("línea %d sin level=%s: %s", i, want.level, lines[i])
		}
		if !strings.Contains(lines[i], want.msg) {
			t.Errorf("línea %d sin mensaje %q: %s", i, want.msg, lines[i])
		}
		if !strings.Contains(lines[i], "module=Client/Socket") {
			t.Errorf("línea %d sin module=Client/Socket: %s", i, lines[i])
		}
	}
}

// TestWALogBridge_RespetaNivelDelAgente: con el agente a INFO, los Debug de whatsmeow (ruidosos) NO
// salen; Info/Warn/Error sí.
func TestWALogBridge_RespetaNivelDelAgente(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New(logger.WithWriter(&buf), logger.WithLevel(slog.LevelInfo))

	wl := newWALog(log)
	wl.Debugf("ruido de protocolo")
	wl.Infof("conectado")

	out := buf.String()
	if strings.Contains(out, "ruido de protocolo") {
		t.Errorf("el Debug de whatsmeow salió con el agente a INFO:\n%s", out)
	}
	if !strings.Contains(out, "whatsmeow: conectado") {
		t.Errorf("el Info de whatsmeow no salió:\n%s", out)
	}
}
