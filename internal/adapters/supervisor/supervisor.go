// Package supervisor gestiona el CICLO DE VIDA del proceso núcleo (`agent serve`) desde el supervisor
// liviano cmd/wapp-ctl (Plan 007, T4, decisión §10.D). El núcleo NO se relanza a sí mismo: un proceso
// SIEMPRE VIVO (el supervisor) lo arranca como HIJO (exec.Command), lo detiene con SIGTERM (el núcleo ya
// cierra limpio por signal.NotifyContext, cmd/agent/main.go) y reporta su estado.
//
// Anti-duplicado (§10.D): se escribe un PID/lock file; un segundo Start es idempotente (no lanza un
// segundo proceso). Readiness: tras lanzar, se sondea GET /v1/health POR EL UNIX SOCKET co-ubicado
// (ADR-0015) hasta que responde 200 o vence el timeout. El caso "lo arrancó el SO" queda FUERA del MVP
// (Fase 5); aun así Status/Stop degradan con gracia ante un PID file de un arranque previo.
package supervisor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Config agrupa los parámetros del supervisor. SocketPath es la ÚNICA fuente de verdad de la ruta del
// socket: el supervisor la toma de cfg.ControlSocketPath (mismo overlay WAPP_AGENT_*) y el hijo `agent
// serve` la lee de su PROPIA config (mismo WAPP_AGENT_CONFIG / cwd) → ambos coinciden sin inventar otra
// fuente. En los tests se fuerza la coincidencia pasando Env con WAPP_AGENT_CONTROL_SOCKET_PATH.
type Config struct {
	// AgentBin es la ruta (o nombre en PATH) del binario núcleo a lanzar.
	AgentBin string
	// SocketPath es la ruta del Unix socket /v1 del núcleo (cfg.ControlSocketPath). Compartida con el hijo.
	SocketPath string
	// PIDFile es la ruta del lock/PID file anti-duplicado. Vacío ⇒ default SocketPath+".pid".
	PIDFile string
	// Args son los argumentos del subcomando. Vacío ⇒ ["serve"].
	Args []string
	// Env es el entorno del hijo. Vacío ⇒ os.Environ() (el hijo lee su config con el mismo overlay).
	Env []string
	// ReadyTimeout acota la espera de readiness (sondeo de /v1/health). Cero ⇒ 15s.
	ReadyTimeout time.Duration
	// StopTimeout acota la espera tras SIGTERM antes de SIGKILL. Cero ⇒ 10s.
	StopTimeout time.Duration
}

// Status es la foto del proceso núcleo que consume cmd/wapp-ctl para GET /v1/daemon/status.
type Status struct {
	// State es "running" o "stopped".
	State string
	// PID es el pid del núcleo si corre (0 si detenido).
	PID int
	// Healthy refleja si GET /v1/health respondió 200 (corriendo pero aún no-ready ⇒ running+!healthy).
	Healthy bool
}

const (
	// StateRunning/StateStopped son los valores de Status.State.
	StateRunning = "running"
	StateStopped = "stopped"

	defaultReadyTimeout = 15 * time.Second
	defaultStopTimeout  = 10 * time.Second
	healthPollInterval  = 100 * time.Millisecond
	healthProbeTimeout  = 2 * time.Second
)

// proc envuelve el hijo en ejecución y su reaping. Un único waiter llama a cmd.Wait() y cierra done;
// Stop/Status leen done por select (no llaman Wait dos veces). waitErr solo se lee tras <-done.
type proc struct {
	cmd     *exec.Cmd
	pid     int
	done    chan struct{}
	waitErr error
}

// Supervisor es seguro para uso concurrente (lo invocan handlers HTTP del supervisor).
type Supervisor struct {
	cfg Config
	log sharedlogger.Logger

	mu sync.Mutex
	p  *proc // hijo lanzado por ESTE supervisor; nil si no hay
}

// New construye el supervisor aplicando defaults. log puede ser nil (se omiten trazas).
func New(cfg Config, log sharedlogger.Logger) *Supervisor {
	if len(cfg.Args) == 0 {
		cfg.Args = []string{"serve"}
	}
	if cfg.Env == nil {
		cfg.Env = os.Environ()
	}
	if cfg.PIDFile == "" {
		cfg.PIDFile = cfg.SocketPath + ".pid"
	}
	if cfg.ReadyTimeout == 0 {
		cfg.ReadyTimeout = defaultReadyTimeout
	}
	if cfg.StopTimeout == 0 {
		cfg.StopTimeout = defaultStopTimeout
	}
	return &Supervisor{cfg: cfg, log: log}
}

// Start arranca el núcleo como hijo si no corre ya (idempotente). Lanza `agentBin serve`, escribe el PID
// file y SONDEA GET /v1/health hasta readiness o ReadyTimeout. Si no llega a ready, mata al hijo, limpia
// el PID file y devuelve un error claro. Si ya corría (this supervisor o PID file vivo) devuelve nil.
func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if pid, running := s.runningLocked(); running {
		if s.log != nil {
			s.log.Info("supervisor: el núcleo ya está corriendo (Start idempotente)", "pid", pid)
		}
		return nil
	}

	if s.cfg.AgentBin == "" {
		return errors.New("supervisor: AgentBin vacío (ruta del binario núcleo)")
	}

	cmd := exec.Command(s.cfg.AgentBin, s.cfg.Args...) //nolint:gosec // ruta del binario núcleo, controlada por config/flag (dev), no input de red.
	cmd.Env = s.cfg.Env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("supervisor: no se pudo lanzar %q: %w", s.cfg.AgentBin, err)
	}

	p := &proc{cmd: cmd, pid: cmd.Process.Pid, done: make(chan struct{})}
	go func() { p.waitErr = cmd.Wait(); close(p.done) }()

	if err := s.writePIDFile(p.pid); err != nil && s.log != nil {
		s.log.Warn("supervisor: no se pudo escribir el PID file", "error", err, "path", s.cfg.PIDFile)
	}

	if err := s.awaitReady(ctx, p); err != nil {
		s.terminateLocked(p)
		_ = os.Remove(s.cfg.PIDFile)
		return err
	}

	s.p = p
	if s.log != nil {
		s.log.Info("supervisor: núcleo arrancado y ready", "pid", p.pid, "socket", s.cfg.SocketPath)
	}
	return nil
}

// Stop detiene el núcleo con SIGTERM y espera su salida (SIGKILL si excede StopTimeout); limpia el PID
// file. Idempotente: si no corre nada devuelve nil. También degrada el caso "PID file de un arranque
// previo sin handle en memoria" (fuera del MVP, §10.D): SIGTERM al pid y limpieza best-effort.
func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.p != nil {
		s.terminateLocked(s.p)
		s.p = nil
		_ = os.Remove(s.cfg.PIDFile)
		if s.log != nil {
			s.log.Info("supervisor: núcleo detenido (SIGTERM)")
		}
		return nil
	}

	// Sin handle en memoria: ¿hay un PID file vivo de un arranque previo? Best-effort.
	pid := s.readPIDFile()
	if pid > 0 && processAlive(pid) {
		_ = signalPID(pid, syscall.SIGTERM)
		s.waitPIDExit(ctx, pid)
	}
	_ = os.Remove(s.cfg.PIDFile)
	return nil
}

// Status reporta running/stopped + pid + healthy. Limpia el PID file si está stale (proceso muerto).
func (s *Supervisor) Status(_ context.Context) Status {
	s.mu.Lock()
	defer s.mu.Unlock()

	pid, running := s.runningLocked()
	if !running {
		return Status{State: StateStopped}
	}
	return Status{State: StateRunning, PID: pid, Healthy: s.probeHealth() == nil}
}

// runningLocked decide si el núcleo corre y devuelve su pid. Prioriza el handle en memoria (hijo de este
// supervisor); si no, cae al PID file (caso adoptado / arranque previo). LIMPIA estado stale: hijo que
// salió por su cuenta (PID stale interno) o PID file con proceso muerto. Debe llamarse con s.mu tomado.
func (s *Supervisor) runningLocked() (int, bool) {
	if s.p != nil {
		select {
		case <-s.p.done:
			// El hijo murió fuera del Stop (crash): PID stale. Se limpia y se cae al chequeo por archivo.
			if s.log != nil {
				s.log.Warn("supervisor: el núcleo terminó fuera del supervisor (PID stale)", "pid", s.p.pid, "wait_err", s.p.waitErr)
			}
			s.p = nil
			_ = os.Remove(s.cfg.PIDFile)
		default:
			return s.p.pid, true
		}
	}

	pid := s.readPIDFile()
	if pid > 0 && processAlive(pid) {
		return pid, true
	}
	if pid > 0 {
		// PID file stale (proceso ya no existe): limpiar.
		_ = os.Remove(s.cfg.PIDFile)
	}
	return 0, false
}

// terminateLocked envía SIGTERM al hijo y espera su salida; SIGKILL si excede StopTimeout. No vuelve a
// señalizar si ya salió. Debe llamarse con s.mu tomado.
func (s *Supervisor) terminateLocked(p *proc) {
	select {
	case <-p.done:
		return // ya salió
	default:
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-p.done:
	case <-time.After(s.cfg.StopTimeout):
		if s.log != nil {
			s.log.Warn("supervisor: el núcleo no salió tras SIGTERM; enviando SIGKILL", "pid", p.pid)
		}
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

// awaitReady sondea GET /v1/health por el socket hasta 200, o falla por ReadyTimeout / muerte temprana
// del hijo / cancelación del ctx del caller.
func (s *Supervisor) awaitReady(ctx context.Context, p *proc) error {
	deadline := time.After(s.cfg.ReadyTimeout)
	ticker := time.NewTicker(healthPollInterval)
	defer ticker.Stop()

	for {
		if s.probeHealth() == nil {
			return nil
		}
		select {
		case <-p.done:
			return fmt.Errorf("supervisor: el núcleo (pid %d) salió antes de estar ready: %v", p.pid, p.waitErr)
		case <-deadline:
			return fmt.Errorf("supervisor: el núcleo no quedó ready en %s (GET /v1/health por %s)", s.cfg.ReadyTimeout, s.cfg.SocketPath)
		case <-ctx.Done():
			return fmt.Errorf("supervisor: arranque cancelado: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// probeHealth hace GET /v1/health por el Unix socket. nil ⇒ 200 (ready). Cualquier otro caso ⇒ error.
func (s *Supervisor) probeHealth() error {
	client := unixHTTPClient(s.cfg.SocketPath, healthProbeTimeout)
	resp, err := client.Get("http://unix/v1/health")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health status %d", resp.StatusCode)
	}
	return nil
}

// waitPIDExit espera (polling) a que un pid externo muera, hasta StopTimeout o cancelación.
func (s *Supervisor) waitPIDExit(ctx context.Context, pid int) {
	deadline := time.After(s.cfg.StopTimeout)
	ticker := time.NewTicker(healthPollInterval)
	defer ticker.Stop()
	for {
		if !processAlive(pid) {
			return
		}
		select {
		case <-deadline:
			_ = signalPID(pid, syscall.SIGKILL)
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Supervisor) writePIDFile(pid int) error {
	return os.WriteFile(s.cfg.PIDFile, []byte(strconv.Itoa(pid)), 0o600)
}

func (s *Supervisor) readPIDFile() int {
	raw, err := os.ReadFile(s.cfg.PIDFile)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0
	}
	return pid
}

// unixHTTPClient construye un http.Client que marca SIEMPRE el Unix socket dado (el host de la URL es un
// placeholder: "unix"). Reusado por el sondeo de readiness/health del supervisor y por el reverse-proxy.
func unixHTTPClient(socketPath string, timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socketPath)
			},
		},
	}
}

// processAlive comprueba si un pid existe (signal 0). En unix os.FindProcess no falla; la señal 0 no
// envía nada, solo verifica existencia/permiso. MVP: no protege contra reuso de pid (Fase 5).
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return p.Signal(syscall.Signal(0)) == nil
}

func signalPID(pid int, sig syscall.Signal) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return p.Signal(sig)
}
