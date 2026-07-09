package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/logsink"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/logger"
)

// TestRunServe_ArranqueYCierre verifica el ENSAMBLAJE de `agent serve` SIN WhatsApp real: con una BD
// vacía recién migrada, la escucha 24/7 resuelve "no hay sesión que restaurar" (app.ErrNoSessions) y
// por tanto NO conecta ningún cliente whatsmeow. Eso permite ejercitar el plano de control REAL (levanta
// el Unix socket /v1, responde GET /v1/health 200 mientras "escucha") y el shutdown unificado (al
// cancelar el contexto cierra limpio, SIN dejar el socket huérfano).
//
// Lo que cubre: el wire-up de cmd/agent (server.New + Handle(/v1/logs) + RegisterPairing + Listen/Serve
// + escucha bajo el mismo ctx + Shutdown). Lo que NO cubre (queda para el e2e T6, con teléfono real):
// el emparejamiento real por QR y la escucha 24/7 conectada de verdad al socket whatsmeow.
func TestRunServe_ArranqueYCierre(t *testing.T) {
	// Rutas CORTAS bajo /tmp: el sun_path de un Unix socket no admite las rutas largas de t.TempDir()
	// en macOS (/var/folders/...). Mismo criterio que internal/adapters/control/server/server_test.go.
	dir, err := os.MkdirTemp("/tmp", "wapp-serve-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	socket := filepath.Join(dir, "edge.sock")
	cfg := config.Config{
		LogLevel: "error", // silencioso: el test no inspecciona logs
		// Plan 022 T3: runServe abre la BD ÚNICA (<data_dir>/edge.db) con dialecto conmutable; en producción
		// config.Load defaultea DBDialect="sqlite", pero este test construye Config a mano, así que hay que
		// fijar DataDir + DBDialect explícitamente (antes runServe usaba DataDir para sessions.db meta).
		DataDir:           dir,
		DBDialect:         db.DialectSQLite,
		DBPath:            filepath.Join(dir, "edge.db"),
		DEKPath:           filepath.Join(dir, "dek.key"),
		ControlSocketPath: socket,
	}

	sink := logsink.New(0)
	log := logger.NewWithSink(cfg, sink)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runServe(ctx, cfg, log, sink) }()

	client := newUnixClient(socket)

	// El socket /v1 debe levantar y health responder 200 mientras la escucha está "corriendo".
	waitHealthy(t, client)

	// Cancela el contexto: dispara el shutdown unificado (cierra el /v1 y apaga la escucha).
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runServe devolvió error en el cierre: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runServe no terminó tras cancelar el contexto (posible fuga / cierre colgado)")
	}

	// Tras el cierre limpio, el socket file NO debe quedar huérfano (Shutdown lo elimina).
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Fatalf("el socket /v1 quedó huérfano tras el cierre: stat err=%v", err)
	}
}

// newUnixClient devuelve un http.Client que marca por el Unix socket dado (el navegador real no habla
// Unix socket: en producción lo hace el reverse-proxy del supervisor, T4).
func newUnixClient(socket string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}
}

// waitHealthy reintenta GET /v1/health hasta recibir 200 o agotar el plazo (el socket se crea de forma
// asíncrona respecto al arranque de runServe en su goroutine).
func waitHealthy(t *testing.T, client *http.Client) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://unix/v1/health", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			status := resp.StatusCode
			_ = resp.Body.Close()
			if status == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("GET /v1/health no respondió 200 dentro del plazo")
}
