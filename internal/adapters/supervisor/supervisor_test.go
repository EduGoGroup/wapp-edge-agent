package supervisor

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// TestMain implementa el patrón "helper process": cuando el binario de test se re-ejecuta a sí mismo con
// SUPERVISOR_FAKE_AGENT=1 actúa como un NÚCLEO FALSO (sirve GET /v1/health por el Unix socket y cierra
// limpio con SIGTERM), sin whatsmeow ni DB. Así el supervisor lanza un "agent" real (este mismo binario)
// y se ejercita exec + readiness + SIGTERM de verdad, pero el hijo es trivial.
func TestMain(m *testing.M) {
	if os.Getenv("SUPERVISOR_FAKE_AGENT") == "1" {
		fakeAgentMain()
		return
	}
	os.Exit(m.Run())
}

// fakeAgentMain es el núcleo falso. Lee el socket de WAPP_AGENT_CONTROL_SOCKET_PATH (el mismo override
// que soporta config.Load en el núcleo real) y el modo de SUPERVISOR_FAKE_MODE:
//   - "" (normal): escucha y responde 200 a /v1/health; SIGTERM ⇒ shutdown limpio.
//   - "noready":   escucha pero /v1/health responde 503 ⇒ el supervisor nunca lo ve ready.
//   - "crash":     sale de inmediato (no llega a escuchar) ⇒ readiness falla por muerte temprana.
func fakeAgentMain() {
	switch os.Getenv("SUPERVISOR_FAKE_MODE") {
	case "crash":
		os.Exit(1)
	}

	sock := os.Getenv("WAPP_AGENT_CONTROL_SOCKET_PATH")
	noReady := os.Getenv("SUPERVISOR_FAKE_MODE") == "noready"

	ln, err := net.Listen("unix", sock)
	if err != nil {
		os.Exit(2)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		if noReady {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","version":"fake"}`))
	})
	// Ruta extra para ejercitar el reverse-proxy del supervisor en cmd/wapp-ctl (no se usa aquí).
	mux.HandleFunc("GET /v1/ping", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	})

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() { _ = srv.Serve(ln) }()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = os.Remove(sock)
	os.Exit(0)
}

// fakeCfg arma una Config del supervisor que lanza ESTE binario de test como núcleo falso. socket y
// pidfile viven en un dir temporal por test. mode selecciona el comportamiento del fake.
func fakeCfg(t *testing.T, mode string) Config {
	t.Helper()
	dir := t.TempDir()
	sock := filepath.Join(dir, "edge.sock")
	env := append(os.Environ(),
		"SUPERVISOR_FAKE_AGENT=1",
		"WAPP_AGENT_CONTROL_SOCKET_PATH="+sock,
	)
	if mode != "" {
		env = append(env, "SUPERVISOR_FAKE_MODE="+mode)
	}
	return Config{
		AgentBin:     os.Args[0], // el propio binario de test
		SocketPath:   sock,
		PIDFile:      filepath.Join(dir, "edge.pid"),
		Env:          env,
		ReadyTimeout: 5 * time.Second,
		StopTimeout:  3 * time.Second,
	}
}

// TestStartStopStatus: arranque real (exec del fake) → ready; Status running+healthy; Stop hace SIGTERM,
// el fake cierra limpio, el PID file se borra y Status queda stopped.
func TestStartStopStatus(t *testing.T) {
	sup := New(fakeCfg(t, ""), nil)

	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	st := sup.Status(context.Background())
	if st.State != StateRunning || !st.Healthy || st.PID <= 0 {
		t.Fatalf("Status tras Start = %+v; quería running+healthy+pid>0", st)
	}
	if _, err := os.Stat(sup.cfg.PIDFile); err != nil {
		t.Fatalf("PID file ausente tras Start: %v", err)
	}

	if err := sup.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if st := sup.Status(context.Background()); st.State != StateStopped {
		t.Fatalf("Status tras Stop = %+v; quería stopped", st)
	}
	if _, err := os.Stat(sup.cfg.PIDFile); !os.IsNotExist(err) {
		t.Fatalf("PID file debería estar borrado tras Stop, err=%v", err)
	}
}

// TestStartIdempotent: un segundo Start NO lanza un segundo proceso (mismo pid; idempotente).
func TestStartIdempotent(t *testing.T) {
	sup := New(fakeCfg(t, ""), nil)
	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start#1: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })

	pid1 := sup.Status(context.Background()).PID
	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start#2: %v", err)
	}
	pid2 := sup.Status(context.Background()).PID
	if pid1 != pid2 {
		t.Fatalf("Start no fue idempotente: pid1=%d pid2=%d", pid1, pid2)
	}
}

// TestStartNotReady: el fake escucha pero /v1/health responde 503 ⇒ readiness vence ⇒ Start error, el
// hijo se mata y el PID file se limpia.
func TestStartNotReady(t *testing.T) {
	cfg := fakeCfg(t, "noready")
	cfg.ReadyTimeout = 700 * time.Millisecond
	sup := New(cfg, nil)

	err := sup.Start(context.Background())
	if err == nil {
		_ = sup.Stop(context.Background())
		t.Fatal("Start debía fallar por readiness no alcanzada")
	}
	if st := sup.Status(context.Background()); st.State != StateStopped {
		t.Fatalf("tras fallo de readiness Status = %+v; quería stopped", st)
	}
	if _, statErr := os.Stat(cfg.PIDFile); !os.IsNotExist(statErr) {
		t.Fatalf("PID file debería estar limpio tras fallo de readiness, err=%v", statErr)
	}
}

// TestStartCrashEarly: el fake sale de inmediato (no escucha) ⇒ Start detecta muerte temprana y falla.
func TestStartCrashEarly(t *testing.T) {
	sup := New(fakeCfg(t, "crash"), nil)
	if err := sup.Start(context.Background()); err == nil {
		_ = sup.Stop(context.Background())
		t.Fatal("Start debía fallar porque el núcleo murió antes de estar ready")
	}
}

// TestStatusStalePID: un PID file con un proceso YA MUERTO (sin handle en memoria) ⇒ Status lo detecta
// stale, lo limpia y reporta stopped.
func TestStatusStalePID(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "edge.pid")

	// Un proceso trivial ya terminado: su pid queda muerto (reapeado por Run).
	dead := exec.Command("true")
	if err := dead.Run(); err != nil {
		t.Fatalf("no se pudo correr proceso trivial: %v", err)
	}
	deadPID := dead.Process.Pid
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(deadPID)), 0o600); err != nil {
		t.Fatalf("write pid file: %v", err)
	}

	sup := New(Config{SocketPath: filepath.Join(dir, "edge.sock"), PIDFile: pidFile}, nil)
	if st := sup.Status(context.Background()); st.State != StateStopped {
		t.Fatalf("Status con PID stale = %+v; quería stopped", st)
	}
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Fatalf("PID file stale debería haberse limpiado, err=%v", err)
	}
}

// TestStopIdempotent: Stop sin nada corriendo no es error.
func TestStopIdempotent(t *testing.T) {
	sup := New(fakeCfg(t, ""), nil)
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatalf("Stop sin proceso debía ser nil, got %v", err)
	}
}
