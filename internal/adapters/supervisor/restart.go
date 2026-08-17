package supervisor

import (
	"context"
	"errors"
	"os"
	"time"
)

// Defaults del relanzado automático (Plan 051 · T2.2). Solo aplican cuando RestartPolicy.Enabled: con el
// valor cero de la política el supervisor NO relanza nada (comportamiento histórico del núcleo).
const (
	defaultRestartMinBackoff = 1 * time.Second
	defaultRestartMaxBackoff = 60 * time.Second
	defaultRestartResetAfter = 5 * time.Minute
)

// RestartPolicy describe el relanzado automático de un hijo que muere solo. El valor cero (Enabled false)
// es el comportamiento histórico: el hijo que muere se reporta como detenido y se rearranca A MANO.
//
// El backoff es exponencial (duplica desde MinBackoff hasta MaxBackoff) y NO tiene tope de intentos: un
// hijo 24/7 que no arranca debe seguir intentándolo, no rendirse a mitad de la madrugada. La parada
// PEDIDA (Stop) siempre gana: ni relanza tras Stop ni deja colgada la espera del backoff.
type RestartPolicy struct {
	// Enabled activa el relanzado automático.
	Enabled bool
	// MinBackoff es la espera del primer relanzado. 0 ⇒ defaultRestartMinBackoff (1s).
	MinBackoff time.Duration
	// MaxBackoff es el techo de la espera. 0 ⇒ defaultRestartMaxBackoff (60s).
	MaxBackoff time.Duration
	// ResetAfter: si el hijo vivió más que esto, el backoff vuelve al mínimo (un proceso que aguantó
	// horas y cae una vez no arrastra el castigo del crash-loop anterior). 0 ⇒ defaultRestartResetAfter
	// (5m).
	ResetAfter time.Duration
}

// conDefaults devuelve la política con los ceros resueltos. Se aplica siempre en New (es inocua cuando
// Enabled es false) para que el resto del paquete no vuelva a razonar sobre ceros.
func (r RestartPolicy) conDefaults() RestartPolicy {
	if r.MinBackoff <= 0 {
		r.MinBackoff = defaultRestartMinBackoff
	}
	if r.MaxBackoff <= 0 {
		r.MaxBackoff = defaultRestartMaxBackoff
	}
	if r.MaxBackoff < r.MinBackoff {
		r.MaxBackoff = r.MinBackoff
	}
	if r.ResetAfter <= 0 {
		r.ResetAfter = defaultRestartResetAfter
	}
	return r
}

// ProbeProcesoVivo devuelve un ReadyProbe que solo exige que el proceso siga arriba tras una gracia: se
// limita a esperar `gracia` y decir «listo». Es el probe de un hijo SIN PLANO HTTP propio (el cajero del
// Plan 051, que no escucha en ningún socket: solo reclama trabajo de la cola y clasifica).
//
// Es DELIBERADAMENTE más débil que el probe HTTP y conviene saber qué NO garantiza:
//   - No garantiza que el hijo haya terminado de inicializarse (abrir la base cifrada, resolver la DEK,
//     alcanzar a Ollama). Solo que no se murió en los primeros `gracia`.
//   - No detecta un hijo VIVO PERO INÚTIL (bloqueado, en bucle de reintentos, sin dependencias).
//   - No comprueba el proceso por sí mismo —no tiene su pid—: la muerte temprana la detecta awaitReady,
//     que espera en paralelo a p.done y GANA a este probe (ver el desempate de awaitReady).
//
// Quien pueda ofrecer un chequeo real (un endpoint, un fichero de latido) debe preferirlo. Respeta la
// cancelación del ctx: si lo cancelan durante la gracia devuelve ctx.Err() sin esperar al reloj.
func ProbeProcesoVivo(gracia time.Duration) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if gracia <= 0 {
			return nil
		}
		t := time.NewTimer(gracia)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return nil
		}
	}
}

// trasSalida es la segunda mitad del reaper: corre en la goroutine del hijo, DESPUÉS de cmd.Wait() y de
// close(p.done), y SIEMPRE fuera del lock (por eso puede pedirlo sin invertir el orden de adquisición
// documentado en Supervisor). Decide si toca relanzar y, si toca, se convierte en el bucle de relanzado.
// El vigía del hijo adoptado (vigilarAdoptado) entra por aquí también, con un proc sintético.
//
// gate es la puerta de la generación en la que nació el hijo: Stop (y un Start explícito) la cierran, y
// eso es lo que interrumpe la espera del backoff Y la readiness del hijo nuevo. Comparar su IDENTIDAD con
// s.stopCh evita que un reaper rezagado relance sobre una generación que ya no es la suya.
//
// Invariante del bucle: se entra en cada iteración CON s.mu tomado y se sale de la función con s.mu
// liberado. Se hacen SIN el lock las tres esperas largas: el backoff, la readiness del hijo nuevo y el
// cierre del hijo que no llegó a ready. Retenerlo (como hacía la primera versión) colgaba Stop, Start y
// Status hasta ReadyTimeout+StopTimeout por vuelta —o sea, POST /v1/cajero/stop tardando medio minuto y
// GET /v1/cajero/status congelado—, y además contradecía la promesa de Supervisor de que un lock-holder
// nunca espera a otra goroutine. Durante ese tramo s.relanzando queda en true: es la marca que (a) hace
// que Healthy no mienta y (b) explica por qué el lock file ya apunta a un hijo que aún no es de fiar.
//
// El precio de soltar el lock es que otra generación puede tomar el mando en ese hueco, así que al
// recuperarlo se vuelve a comprobar TODO (stopping + identidad de la puerta) antes de tocar nada
// compartido, y el lock file solo se borra si sigue siendo el nuestro (borrarPIDFileSiEsDe).
//
// LÍMITE CONOCIDO: la comprobación de "¿ya está arriba?" es runningLocked, que ante un lock file stale con
// el pid REUSADO por otro proceso dice que sí. En ese caso el relanzado se retira para siempre y el worker
// se queda caído hasta que alguien lo arranque a mano. Es ceguera heredada de processAlive (MVP; la
// defensa real es de Fase 5), pero desde el Plan 051 ya no es solo un Status equivocado: es una cola que
// deja de vaciarse.
func (s *Supervisor) trasSalida(p *proc, gate chan struct{}) {
	s.mu.Lock()

	if !p.restartable || s.stopping || !s.cfg.Restart.Enabled {
		// Sin derecho a relanzado (arranque fallido, parada pedida o política desactivada). No se toca
		// s.p: si quedó apuntando a este hijo muerto, runningLocked lo detectará stale y lo limpiará,
		// que es EXACTAMENTE el comportamiento histórico.
		s.mu.Unlock()
		return
	}

	p.restartable = false // este relevo se toma una sola vez
	if s.p == p {
		s.p = nil
		_ = os.Remove(s.cfg.PIDFile)
	}
	if time.Since(p.startedAt) >= s.cfg.Restart.ResetAfter {
		s.restartBackoff = 0
		s.restartAttempts = 0
	}
	motivo := motivoSalida(p.waitErr)

	for {
		espera := s.siguienteBackoffLocked()
		intento := s.restartAttempts
		if s.log != nil {
			s.log.Warn("supervisor: el hijo murió solo; relanzando tras backoff",
				"intento", intento, "backoff_ms", espera.Milliseconds(), "motivo", motivo,
				"bin", s.cfg.AgentBin, "args", s.cfg.Args)
		}
		s.mu.Unlock()

		t := time.NewTimer(espera)
		select {
		case <-gate:
			// Stop (o un Start explícito) cerró la generación durante la espera: NO se relanza.
			t.Stop()
			return
		case <-t.C:
		}

		s.mu.Lock()
		if s.stopping || s.stopCh != gate {
			s.mu.Unlock()
			return
		}
		if pid, running := s.runningLocked(); running {
			// Alguien lo arrancó a mano mientras esperábamos (Start es idempotente): nos retiramos.
			if s.log != nil {
				s.log.Info("supervisor: el hijo ya estaba arriba al vencer el backoff; relanzado cancelado", "pid", pid)
			}
			s.mu.Unlock()
			return
		}

		np, err := s.launchLocked()
		if err != nil {
			motivo = err.Error()
			continue // sigue con s.mu tomado: es el invariante de entrada del bucle
		}

		// ── Tramo SIN LOCK: lanzar ya está hecho (el lock file apunta a np); falta esperar readiness y,
		// si falla, cerrarlo. Ambas cosas son esperas largas y ninguna toca estado compartido: awaitReady
		// solo lee cfg y np, y terminar() no necesita el lock (ver su doc).
		s.relanzando = true
		s.mu.Unlock()

		// El ctx de la readiness cuelga de la PUERTA: cerrarla (Stop, o un Start explícito) aborta el
		// sondeo en el acto en vez de dejar que Stop se coma hasta ReadyTimeout esperando su turno.
		readyCtx, cancelReady := ctxDePuerta(gate)
		errReady := s.awaitReady(readyCtx, np)
		cancelReady()
		if errReady != nil {
			s.terminar(np)
		}

		s.mu.Lock()
		s.relanzando = false

		if errReady != nil {
			if s.stopping || s.stopCh != gate {
				// Mientras esperábamos, otro tomó el mando. np ya está muerto; el lock file puede ser
				// suyo, así que solo se borra si sigue siendo el de np.
				s.borrarPIDFileSiEsDe(np.pid)
				s.mu.Unlock()
				return
			}
			s.borrarPIDFileSiEsDe(np.pid)
			motivo = errReady.Error()
			continue // invariante de entrada: se sigue con s.mu tomado
		}

		// np quedó ready. Solo se adopta si la generación SIGUE siendo la nuestra y nadie más tiene hijo:
		// adoptar bajo una generación ajena le dejaría una puerta ya cerrada (su reaper no relanzaría
		// nunca), y adoptar sobre un s.p vivo dejaría al otro hijo huérfano. En la práctica es un caso de
		// laboratorio —un Start durante la readiness ve a np vivo por el lock file y se retira por
		// idempotencia, sin tocar la puerta—, pero si ocurre se cierra np en vez de mentir.
		if s.stopping || s.stopCh != gate || s.p != nil {
			s.mu.Unlock()
			s.terminar(np)
			s.mu.Lock()
			s.borrarPIDFileSiEsDe(np.pid)
			s.mu.Unlock()
			if s.log != nil {
				s.log.Warn("supervisor: el hijo relanzado quedó ready pero la generación ya no es suya; cerrado", "pid", np.pid)
			}
			return
		}

		s.adoptLocked(np)
		if s.log != nil {
			s.log.Info("supervisor: hijo relanzado y ready", "pid", np.pid, "intento", intento, "bin", s.cfg.AgentBin)
		}
		s.mu.Unlock()
		return
	}
}

// ctxDePuerta devuelve un ctx que se cancela cuando se CIERRA gate (la puerta de la generación) o cuando
// se llama al cancel devuelto. Llamar SIEMPRE al cancel: es lo que libera la goroutine vigilante.
func ctxDePuerta(gate chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-gate:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// intervaloVigiaAdoptado es cada cuánto sondea el vigía del hijo adoptado. Es una var (no const) para que
// los tests puedan encogerlo; en producción no se toca. 2s es holgado: solo cubre el hueco entre que el
// hijo adoptado muere y el relanzado arranca, y processAlive es una señal, no un syscall caro.
var intervaloVigiaAdoptado = 2 * time.Second

// errHijoAdoptadoDesaparecido es el "motivo de salida" del hijo adoptado: no hay cmd.Wait() ni código de
// salida que contar, solo que su pid dejó de existir.
var errHijoAdoptadoDesaparecido = errors.New("el hijo adoptado (lock file de un supervisor anterior) desapareció")

// vigilarAdoptadoLocked arma el vigía por PID del hijo ADOPTADO: el que ya estaba vivo cuando este
// supervisor arrancó (wapp-ctl se reinició y el worker siguió su curso, como está previsto). Ese hijo no
// tiene reaper —no es hijo de este proceso, nadie llama a cmd.Wait sobre él—, así que sin vigía el
// relanzado automático quedaba muerto justo después de cada reinicio del supervisor, en silencio, y
// /v1/cajero/status pasaría a "stopped" para siempre en cuanto muriera (incumpliendo REQ-051.10).
//
// Solo aplica con Restart.Enabled (el núcleo no lo lleva: lo rearranca el operador). Debe llamarse con
// s.mu tomado y solo cuando s.p == nil (si hay hijo propio, su reaper ya cubre el caso).
func (s *Supervisor) vigilarAdoptadoLocked(pid int) {
	if !s.cfg.Restart.Enabled || s.vigiaAdoptado {
		return
	}
	// Un Start explícito es el operador diciendo "quiero esto arriba": reabre el relanzado si un Stop
	// previo lo había cerrado, y garantiza que exista una puerta que Stop pueda cerrar para retirar al
	// vigía (la generación puede no existir todavía: este supervisor no ha lanzado nada).
	s.stopping = false
	if s.stopCh == nil {
		s.stopCh = make(chan struct{})
	}
	s.vigiaAdoptado = true
	gate := s.stopCh
	desde := time.Now()
	if s.log != nil {
		s.log.Info("supervisor: hijo adoptado de un arranque previo; vigilando su pid para el relanzado automático",
			"pid", pid, "intervalo_ms", intervaloVigiaAdoptado.Milliseconds())
	}
	go s.vigilarAdoptado(pid, gate, desde)
}

// vigilarAdoptado sondea processAlive(pid) hasta que el hijo adoptado desaparece y entonces entra al MISMO
// camino de relanzado que el reaper (trasSalida, con un proc SINTÉTICO: no hay cmd ni waitErr que pasarle,
// y trasSalida no los usa —solo restartable, startedAt, waitErr y la comparación con s.p—).
//
// Se retira limpiamente en tres casos, y en los tres deja s.vigiaAdoptado en false para que un Start
// posterior pueda volver a armarlo: (a) se cierra la puerta —Stop, o un Start explícito que abre otra
// generación—, (b) al morir el adoptado ya hay otra generación al mando, (c) ya hay un hijo propio (su
// reaper manda). No se le espera desde ningún sitio: nadie retiene s.mu esperando a esta goroutine.
//
// Hereda la ceguera de processAlive ante el REUSO de pid (ver runningLocked): si el pid se reasigna, el
// vigía creerá que el hijo sigue vivo. MVP; la defensa real es de Fase 5.
func (s *Supervisor) vigilarAdoptado(pid int, gate chan struct{}, desde time.Time) {
	t := time.NewTicker(intervaloVigiaAdoptado)
	defer t.Stop()

	for {
		select {
		case <-gate:
			s.retirarVigia()
			return
		case <-t.C:
		}
		if processAlive(pid) {
			continue
		}

		s.mu.Lock()
		s.vigiaAdoptado = false
		cedeElPaso := s.stopping || s.stopCh != gate || s.p != nil
		s.mu.Unlock()
		if cedeElPaso {
			return
		}
		if s.log != nil {
			s.log.Warn("supervisor: el hijo adoptado desapareció; entrando al relanzado automático", "pid", pid)
		}

		// trasSalida pide el lock por su cuenta (y vuelve a comprobarlo todo), así que se llama SIN él.
		muerto := make(chan struct{})
		close(muerto)
		s.trasSalida(&proc{
			pid:         pid,
			done:        muerto,
			startedAt:   desde,
			waitErr:     errHijoAdoptadoDesaparecido,
			restartable: true,
		}, gate)
		return
	}
}

// retirarVigia limpia la marca del vigía del hijo adoptado.
func (s *Supervisor) retirarVigia() {
	s.mu.Lock()
	s.vigiaAdoptado = false
	s.mu.Unlock()
}

// siguienteBackoffLocked avanza el backoff exponencial (min → ×2 → … → max) y cuenta el intento. Debe
// llamarse con s.mu tomado.
func (s *Supervisor) siguienteBackoffLocked() time.Duration {
	if s.restartBackoff <= 0 {
		s.restartBackoff = s.cfg.Restart.MinBackoff
	} else {
		s.restartBackoff *= 2
		if s.restartBackoff > s.cfg.Restart.MaxBackoff {
			s.restartBackoff = s.cfg.Restart.MaxBackoff
		}
	}
	s.restartAttempts++
	return s.restartBackoff
}

// cancelPendingRestartLocked cierra la puerta de la generación viva (si la hay) para despertar a un
// reaper que esté esperando su backoff, y la deja a nil. Debe llamarse con s.mu tomado.
func (s *Supervisor) cancelPendingRestartLocked() {
	if s.stopCh != nil {
		close(s.stopCh)
		s.stopCh = nil
	}
}

// motivoSalida traduce el error de cmd.Wait a texto de log (nil ⇒ salida con código 0, que para un
// daemon 24/7 sigue siendo una muerte inesperada).
func motivoSalida(err error) string {
	if err == nil {
		return "salida con código 0"
	}
	return err.Error()
}
