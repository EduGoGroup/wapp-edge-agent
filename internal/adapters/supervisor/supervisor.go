// Package supervisor gestiona el CICLO DE VIDA del proceso núcleo (`agent serve`) desde el supervisor
// liviano cmd/wapp-ctl (Plan 007, T4, decisión §10.D). El núcleo NO se relanza a sí mismo: un proceso
// SIEMPRE VIVO (el supervisor) lo arranca como HIJO (exec.Command), lo detiene con SIGTERM (el núcleo ya
// cierra limpio por signal.NotifyContext, cmd/agent/main.go) y reporta su estado.
//
// Anti-duplicado (§10.D): se escribe un PID/lock file; un segundo Start es idempotente (no lanza un
// segundo proceso). Readiness: tras lanzar, se sondea GET /v1/health POR EL UNIX SOCKET co-ubicado
// (ADR-0015) hasta que responde 200 o vence el timeout. El caso "lo arrancó el SO" queda FUERA del MVP
// (Fase 5); aun así Status/Stop degradan con gracia ante un PID file de un arranque previo.
//
// GENERALIZACIÓN (Plan 051 · T2.2): el mismo supervisor gobierna AHORA cualquier hijo del ecosistema, no
// solo `agent serve`. Dos costuras, ambas ADITIVAS y con VALOR CERO = comportamiento histórico:
//
//   - Config.ReadyProbe sustituye el sondeo HTTP de /v1/health por el criterio de readiness que el hijo
//     admita (el cajero no tiene plano HTTP propio: ver ProbeProcesoVivo, restart.go).
//   - Config.Restart activa el RELANZADO AUTOMÁTICO con backoff exponencial (restart.go). Con el valor
//     cero el supervisor sigue SIN relanzar: el núcleo se rearranca a mano por /v1/daemon/start.
//
// Cada supervisor debe tener su PROPIO PIDFile: el default derivado del socket (SocketPath+".pid") solo
// vale para UN hijo por socket; el segundo (cajero) lo pasa explícito.
//
// HIJO ADOPTADO (code review de la Ola 2, hallazgo 2): el hijo sobrevive al supervisor (parar wapp-ctl no
// para el daemon 24/7), así que al volver wapp-ctl se encuentra un proceso vivo que NO es hijo suyo: no
// hay cmd.Wait() que lo llore y, por tanto, no hay reaper que dispare el relanzado. Con Restart.Enabled,
// Start arma para ese caso un VIGÍA POR PID (vigilarAdoptado, restart.go) que sondea processAlive y, al
// verlo caer, entra al MISMO camino de relanzado. Sin él, REQ-051.10 quedaba incumplida —en silencio—
// justo después de cada reinicio del supervisor.
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
	// ReadyProbe decide cuándo el hijo está listo. nil ⇒ el probe HTTP de GET /v1/health por el Unix
	// socket (comportamiento histórico). Se llama en bucle (cada healthPollInterval) hasta que devuelve
	// nil o vence ReadyTimeout. Ver ProbeProcesoVivo para el hijo sin plano HTTP.
	//
	// CONTRATO (no es un consejo): DEBE devolver en cuanto se cancele su ctx. El supervisor lo cancela al
	// abandonar la espera (timeout, muerte del hijo o Stop) pero NO puede esperar a la goroutine que lo
	// ejecuta —eso colgaría a Stop—: un probe que ignore la cancelación deja esa goroutine viva hasta que
	// él decida volver. El canal de resultado va con buffer 1, así que no se bloquea al escribir, pero el
	// tiempo que tarde en atender el ctx es tiempo que su goroutine sigue en pie. Ver
	// TestReadyProbeQueIgnoraElCtxNoCuelgaAlSupervisor.
	ReadyProbe func(ctx context.Context) error
	// Restart gobierna el relanzado automático del hijo que muere solo. Valor cero ⇒ SIN relanzado
	// (comportamiento histórico: el núcleo se rearranca a mano por /v1/daemon/start).
	Restart RestartPolicy
}

// Status es la foto del proceso núcleo que consume cmd/wapp-ctl para GET /v1/daemon/status.
type Status struct {
	// State es "running" o "stopped".
	State string
	// PID es el pid del núcleo si corre (0 si detenido).
	PID int
	// Healthy refleja si GET /v1/health respondió 200 (corriendo pero aún no-ready ⇒ running+!healthy).
	// Con Config.ReadyProbe propio no hay plano HTTP que sondear barato en cada Status: ahí Healthy es una
	// señal MÁS DÉBIL (ver healthyLocked) y Probe dice de dónde viene, para que nadie la confunda con la
	// del núcleo.
	Healthy bool
	// Probe identifica QUÉ criterio respalda a Healthy: ProbeTipoHTTP (sondeo real de /v1/health) o
	// ProbeTipoProcesoVivo (el hijo sin plano HTTP: solo se puede afirmar que el proceso está arriba).
	// Campo NUEVO y aditivo: /v1/daemon/status no lo publica (su cuerpo no cambia de forma, hay
	// consumidores); /v1/cajero/status sí, que es donde la distinción importa.
	Probe string
}

const (
	// StateRunning/StateStopped son los valores de Status.State.
	StateRunning = "running"
	StateStopped = "stopped"

	// ProbeTipoHTTP/ProbeTipoProcesoVivo son los valores de Status.Probe. Cualquier ReadyProbe propio se
	// reporta como ProbeTipoProcesoVivo: es la familia DÉBIL (no hay plano HTTP que sondear en cada
	// Status), y quien lea la respuesta debe tratar Healthy como "el proceso está arriba", no como
	// "el hijo funciona".
	ProbeTipoHTTP        = "http"
	ProbeTipoProcesoVivo = "proceso-vivo"

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
	// startedAt marca el lanzamiento; el relanzado lo usa para decidir si el hijo vivió lo bastante como
	// para resetear el backoff (RestartPolicy.ResetAfter). Se escribe ANTES de arrancar el reaper.
	startedAt time.Time
	// restartable dice si ESTE hijo, al morir solo, da derecho a un relanzado automático. Se pone en
	// adoptLocked (solo el hijo que llegó a ready y quedó adoptado) y el reaper lo consume (lo pone a
	// false) para relanzar UNA vez. Se lee y escribe SIEMPRE con s.mu tomado.
	restartable bool
}

// Supervisor es seguro para uso concurrente (lo invocan handlers HTTP del supervisor).
//
// ORDEN DE ADQUISICIÓN (importante, hay una goroutine reaper por hijo): s.mu es el ÚNICO lock del
// paquete y NADIE lo toma esperando a que otra goroutine termine. El reaper cierra p.done ANTES de pedir
// s.mu; quien espera al hijo (Stop→terminar, awaitReady) espera a p.done —nunca al reaper— así que un
// lock-holder jamás bloquea a la espera de una goroutine que quiere el lock. Lo mismo vale para el vigía
// del hijo adoptado: nadie lo espera, se le cierra la puerta y él se retira.
//
// QUÉ SE HACE CON EL LOCK SUELTO en el relanzado automático (restart.go), y por qué importa: la espera
// del backoff, la readiness del hijo nuevo y su terminación si no llegó a ready. Las tres son esperas
// LARGAS (hasta ReadyTimeout + StopTimeout ≈ 35 s con los valores del cajero) y retenerlas colgaría
// Stop/Start/Status —o sea, POST /v1/cajero/stop y GET /v1/cajero/status— durante todo ese rato. Mientras
// dura ese tramo, s.relanzando queda en true y el lock file ya apunta al hijo nuevo.
type Supervisor struct {
	cfg Config
	log sharedlogger.Logger

	mu sync.Mutex
	p  *proc // hijo lanzado por ESTE supervisor; nil si no hay

	// stopping marca que la parada fue PEDIDA (Stop): el reaper NO debe relanzar. Lo limpia el siguiente
	// Start explícito. Protegido por mu.
	stopping bool
	// stopCh es la "puerta" de la generación viva: Start la crea, Stop (y el propio Start) la cierran
	// para abortar una espera de backoff en curso sin time.Sleep pelados. nil ⇒ no hay generación viva.
	// Protegido por mu; el reaper captura SU canal al nacer y compara identidad antes de relanzar.
	stopCh chan struct{}
	// restartBackoff/restartAttempts son el estado del backoff exponencial entre relanzados. Protegidos
	// por mu; se resetean en cada Start explícito y cuando el hijo vivió más de Restart.ResetAfter.
	restartBackoff  time.Duration
	restartAttempts int
	// relanzando marca el tramo del relanzado que corre CON EL LOCK SUELTO (lanzar + readiness + cierre
	// del hijo que no llegó a ready). Durante ese tramo el lock file ya apunta a un hijo que TODAVÍA no es
	// de fiar: es lo que impide que Healthy diga "sí" en mitad de un crash-loop. Protegido por mu.
	relanzando bool
	// vigiaAdoptado dice si ya hay un vigía por PID en pie para el hijo ADOPTADO (el que sobrevivió al
	// supervisor anterior). Evita que dos Start seguidos dejen dos vigías sobre el mismo pid. Protegido
	// por mu; lo limpia el propio vigía al retirarse.
	vigiaAdoptado bool
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
	cfg.Restart = cfg.Restart.conDefaults()
	return &Supervisor{cfg: cfg, log: log}
}

// Start arranca el hijo si no corre ya (idempotente). Lanza `AgentBin Args...` (por defecto
// `agent serve`), escribe el PID file y espera readiness según ReadyProbe (por defecto GET /v1/health)
// hasta ReadyTimeout. Si no llega a ready, mata al hijo, limpia el PID file y devuelve un error claro. Si
// ya corría (este supervisor o PID file vivo) devuelve nil.
//
// Un Start explícito CANCELA cualquier relanzado automático pendiente y resetea el backoff: es el
// operador retomando el mando.
//
// En el camino IDEMPOTENTE (ya corría) hay una cosa más que hacer y que antes no se hacía: si lo que corre
// es un hijo ADOPTADO —vivo por el lock file, pero no lanzado por este proceso—, Start le arma el vigía
// por PID (vigilarAdoptadoLocked). Sin eso, el arranque de wapp-ctl sobre un cajero ya en marcha devolvía
// "ok" y dejaba al worker sin nadie que lo relanzara.
func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if pid, running := s.runningLocked(); running {
		if s.p == nil {
			// Corre, pero NO es hijo de este proceso (lock file de un supervisor anterior): sin cmd.Wait()
			// no hay reaper. Se le pone un vigía por PID para que el relanzado automático siga vivo.
			s.vigilarAdoptadoLocked(pid)
		}
		if s.log != nil {
			s.log.Info("supervisor: el núcleo ya está corriendo (Start idempotente)", "pid", pid, "adoptado", s.p == nil)
		}
		return nil
	}

	if s.cfg.AgentBin == "" {
		return errors.New("supervisor: AgentBin vacío (ruta del binario núcleo)")
	}

	// Un Start EXPLÍCITO abre una generación nueva: cancela la espera de backoff que hubiera en curso
	// (el reaper de la generación anterior verá su puerta cerrada y se retirará), reabre el relanzado
	// automático tras un Stop previo y devuelve el backoff al mínimo.
	s.cancelPendingRestartLocked()
	s.stopping = false
	s.stopCh = make(chan struct{})
	s.restartBackoff = 0
	s.restartAttempts = 0

	p, err := s.launchLocked()
	if err != nil {
		return err
	}

	// A diferencia del relanzado automático, un Start EXPLÍCITO sí retiene s.mu durante la readiness: es
	// una operación que el operador pidió y que debe excluir a otro Start simultáneo (dos launch a la vez
	// sobre el mismo lock file darían dos hijos). Lo acota ReadyTimeout y lo corta el ctx del caller (el
	// del request HTTP), que es quien manda aquí.
	if err := s.awaitReady(ctx, p); err != nil {
		// NO adoptado ⇒ p.restartable sigue false ⇒ su reaper no relanzará nada: un arranque fallido lo
		// decide el caller (que recibe el error), no el relanzado automático.
		s.terminar(p)
		_ = os.Remove(s.cfg.PIDFile)
		return err
	}

	s.adoptLocked(p)
	if s.log != nil {
		s.log.Info("supervisor: núcleo arrancado y ready", "pid", p.pid, "socket", s.cfg.SocketPath)
	}
	return nil
}

// launchLocked hace el exec del hijo, escribe el PID file y arranca SU goroutine reaper (un único
// cmd.Wait por hijo). No espera readiness ni adopta el hijo: eso lo deciden Start y el relanzado.
// Debe llamarse con s.mu tomado.
func (s *Supervisor) launchLocked() (*proc, error) {
	cmd := exec.Command(s.cfg.AgentBin, s.cfg.Args...) //nolint:gosec // ruta del binario núcleo, controlada por config/flag (dev), no input de red.
	cmd.Env = s.cfg.Env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("supervisor: no se pudo lanzar %q: %w", s.cfg.AgentBin, err)
	}

	p := &proc{cmd: cmd, pid: cmd.Process.Pid, done: make(chan struct{}), startedAt: time.Now()}
	gate := s.stopCh // la puerta de ESTA generación; el reaper compara identidad antes de relanzar.
	go func() {
		p.waitErr = cmd.Wait()
		close(p.done) // cerrar ANTES de pedir s.mu: es lo que hace imposible el deadlock (ver Supervisor).
		s.trasSalida(p, gate)
	}()

	if err := s.writePIDFile(p.pid); err != nil && s.log != nil {
		s.log.Warn("supervisor: no se pudo escribir el PID file", "error", err, "path", s.cfg.PIDFile)
	}
	return p, nil
}

// adoptLocked fija p como el hijo vivo de este supervisor y le concede (si la política lo permite) el
// derecho a un relanzado automático cuando muera solo. Debe llamarse con s.mu tomado.
func (s *Supervisor) adoptLocked(p *proc) {
	p.restartable = s.cfg.Restart.Enabled
	s.p = p
}

// Stop detiene el hijo con SIGTERM y espera su salida (SIGKILL si excede StopTimeout); limpia el PID
// file. Idempotente: si no corre nada devuelve nil. También degrada el caso "PID file de un arranque
// previo sin handle en memoria" (fuera del MVP, §10.D): SIGTERM al pid y limpieza best-effort.
//
// Con relanzado automático activo, Stop es la ÚNICA forma de que el hijo se quede abajo: cierra la puerta
// del relanzado antes de matarlo y con eso (a) despierta, sin relanzar, a un reaper dormido en su backoff
// y (b) ABORTA la readiness de un hijo recién relanzado (su ctx cuelga de esa misma puerta). No espera a
// esa goroutine —solo cierra su canal— así que no puede colgarse por ella. Si pilla el relanzado en su
// tramo sin lock, el hijo a medio arrancar aún no está adoptado (s.p == nil) y Stop lo cierra por el
// camino del lock file, que para eso se escribe ANTES de soltar el lock.
func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Parada PEDIDA: cierra la puerta al relanzado automático ANTES de matar al hijo. stopping corta al
	// reaper que aún no haya llegado; cerrar stopCh despierta al que ya esté esperando su backoff.
	s.stopping = true
	s.cancelPendingRestartLocked()

	if s.p != nil {
		s.p.restartable = false
		s.terminar(s.p)
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
		return Status{State: StateStopped, Probe: s.tipoProbe()}
	}
	return Status{State: StateRunning, PID: pid, Healthy: s.healthyLocked(), Probe: s.tipoProbe()}
}

// tipoProbe identifica la familia del probe de readiness de ESTE supervisor, para que quien lea Healthy
// sepa cuánto vale. No depende del estado: es config.
func (s *Supervisor) tipoProbe() string {
	if s.cfg.ReadyProbe != nil {
		return ProbeTipoProcesoVivo
	}
	return ProbeTipoHTTP
}

// healthyLocked resuelve el campo Healthy. Con el probe histórico es GET /v1/health por el socket (el
// núcleo sí tiene plano HTTP y el sondeo es barato). Debe llamarse con s.mu tomado y solo tiene sentido
// cuando runningLocked ya dijo que sí.
//
// Con un ReadyProbe propio NO se sondea nada aquí: el probe puede ser caro o bloqueante por diseño (una
// gracia de segundos, p.ej.) y Status lo llama la UI en bucle. Pero devolver `true` a secas —como hacía
// esta función— convertía Healthy en una constante para el ÚNICO hijo que no tiene otra señal: durante un
// crash-loop el lock file apunta al hijo recién lanzado y aún no ready, así que "running" ya era true y
// "healthy" también. Lo que se afirma ahora es lo más fuerte que este supervisor puede sostener sin plano
// HTTP: el proceso está arriba Y no estamos en mitad de un relanzado ni de una parada. Sigue siendo una
// señal DÉBIL —no detecta un hijo vivo pero inútil—: por eso Status.Probe la etiqueta.
func (s *Supervisor) healthyLocked() bool {
	if s.cfg.ReadyProbe != nil {
		return !s.relanzando && !s.stopping
	}
	return s.probeHealth() == nil
}

// runningLocked decide si el núcleo corre y devuelve su pid. Prioriza el handle en memoria (hijo de este
// supervisor); si no, cae al PID file (caso adoptado / arranque previo). LIMPIA estado stale: hijo que
// salió por su cuenta (PID stale interno) o PID file con proceso muerto. Debe llamarse con s.mu tomado.
//
// LÍMITE CONOCIDO (heredado de processAlive, agravado por el relanzado del Plan 051): si el pid del lock
// file fue REUSADO por otro proceso del sistema, esto dice running=true y el hijo real está muerto. Antes
// solo mentía en Status; ahora además hace que el relanzado se retire para siempre (ver trasSalida) y el
// worker se quede caído. La defensa de verdad (guardar también el arranque del proceso y compararlo) es
// de Fase 5; entretanto el vigía del hijo adoptado tiene la misma ceguera.
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

// terminar envía SIGTERM al hijo y espera su salida; SIGKILL si excede StopTimeout. No vuelve a
// señalizar si ya salió.
//
// NO necesita s.mu (y por eso no se llama "…Locked"): solo toca p —cuyo cmd y done son inmutables tras
// launchLocked— y campos de s que no cambian después de New (cfg, log). Puede llamarse con el lock tomado
// (Start, Stop: ahí la exclusión la quiere el caller) o SIN él (el relanzado automático, que no puede
// permitirse retener el lock hasta StopTimeout: ver restart.go).
func (s *Supervisor) terminar(p *proc) {
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

// awaitReady espera a que el hijo esté listo según Config.ReadyProbe (por defecto: GET /v1/health por el
// socket hasta 200), o falla por ReadyTimeout / muerte temprana del hijo / cancelación del ctx del caller.
//
// El probe corre en UNA goroutine aparte y no en línea: un ReadyProbe puede bloquear por diseño (p.ej.
// ProbeProcesoVivo espera una gracia entera), y si lo llamáramos en línea no veríamos morir al hijo
// durante esa espera —justo el caso que el probe pretende detectar—. El canal de resultado va con buffer
// 1 para que la goroutine no se quede colgada si abandonamos antes.
func (s *Supervisor) awaitReady(ctx context.Context, p *proc) error {
	probe := s.cfg.ReadyProbe
	if probe == nil {
		probe = func(context.Context) error { return s.probeHealth() }
	}

	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	ready := make(chan struct{}, 1)
	go func() {
		for {
			if err := probe(probeCtx); err == nil {
				ready <- struct{}{}
				return
			}
			select {
			case <-probeCtx.Done():
				return
			case <-time.After(healthPollInterval):
			}
		}
	}()

	deadline := time.NewTimer(s.cfg.ReadyTimeout)
	defer deadline.Stop()

	select {
	case <-ready:
		// Desempate explícito: si el hijo YA murió, no lo damos por ready aunque el probe dijera que sí
		// (select elige al azar entre casos listos, y un probe débil como ProbeProcesoVivo no lo sabe).
		select {
		case <-p.done:
			return fmt.Errorf("supervisor: el hijo (pid %d) salió antes de estar ready: %v", p.pid, p.waitErr)
		default:
			return nil
		}
	case <-p.done:
		return fmt.Errorf("supervisor: el hijo (pid %d) salió antes de estar ready: %v", p.pid, p.waitErr)
	case <-deadline.C:
		return fmt.Errorf("supervisor: el hijo no quedó ready en %s (bin %q, socket %s)", s.cfg.ReadyTimeout, s.cfg.AgentBin, s.cfg.SocketPath)
	case <-ctx.Done():
		return fmt.Errorf("supervisor: arranque cancelado: %w", ctx.Err())
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

// borrarPIDFileSiEsDe borra el lock file SOLO si sigue apuntando al pid dado. Lo necesita el relanzado
// automático: allí el lock se suelta durante la readiness, y en ese hueco otra generación (un Start del
// operador) puede haber escrito SU pid. Borrarlo a ciegas dejaría a ese hijo sin lock ⇒ el siguiente
// arranque no lo vería y lanzaría un SEGUNDO worker sobre la misma cola.
func (s *Supervisor) borrarPIDFileSiEsDe(pid int) {
	if s.readPIDFile() == pid {
		_ = os.Remove(s.cfg.PIDFile)
	}
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
