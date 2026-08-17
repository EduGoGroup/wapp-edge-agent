package supervisor

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// cfgHijoQueMuereSolo arma la Config de un hijo SIN plano HTTP (modo "diesoon" del fake): queda ready por
// ProbeProcesoVivo y se muere solo tras vidaMS milisegundos. Es el molde del cajero (Plan 051 · T2.2).
func cfgHijoQueMuereSolo(t *testing.T, vidaMS int) Config {
	t.Helper()
	cfg := fakeCfg(t, "diesoon")
	cfg.Env = append(cfg.Env, "SUPERVISOR_FAKE_LIFETIME_MS="+strconv.Itoa(vidaMS))
	cfg.ReadyProbe = ProbeProcesoVivo(5 * time.Millisecond)
	cfg.ReadyTimeout = 2 * time.Second
	cfg.StopTimeout = 2 * time.Second
	return cfg
}

// esperarHasta hace polling de cond hasta que se cumple o vence el límite. Devuelve si se cumplió.
func esperarHasta(limite time.Duration, cond func() bool) bool {
	fin := time.Now().Add(limite)
	for time.Now().Before(fin) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// intentosRestart / backoffActual leen el estado del relanzado CON EL LOCK TOMADO: el reaper los escribe
// desde su propia goroutine y sin esto el -race del gate cazaría la lectura.
func (s *Supervisor) intentosRestart() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restartAttempts
}

func (s *Supervisor) backoffActual() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restartBackoff
}

// relanzandoAhora dice si el relanzado está en su tramo SIN LOCK (lanzado + readiness del hijo nuevo).
func (s *Supervisor) relanzandoAhora() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.relanzando
}

// vigiaActiva dice si hay un vigía por PID en pie para el hijo adoptado.
func (s *Supervisor) vigiaActiva() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.vigiaAdoptado
}

// TestRestartPolicyCeroNoRelanza es la REGRESIÓN del comportamiento histórico: con el valor cero de
// RestartPolicy un hijo que muere solo se queda muerto (el núcleo se rearranca a mano por
// POST /v1/daemon/start). Si alguien activa el relanzado "por defecto", este test lo caza.
func TestRestartPolicyCeroNoRelanza(t *testing.T) {
	cfg := cfgHijoQueMuereSolo(t, 80) // valor cero de cfg.Restart: sin relanzado
	sup := New(cfg, nil)
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })

	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !esperarHasta(2*time.Second, func() bool { return sup.Status(context.Background()).State == StateStopped }) {
		t.Fatal("el hijo debía morir solo y quedar reportado como stopped")
	}

	// Margen holgado por si hubiera un relanzado (el default sería 1s).
	time.Sleep(1500 * time.Millisecond)
	if st := sup.Status(context.Background()); st.State != StateStopped {
		t.Fatalf("con RestartPolicy en cero NO debe relanzarse; Status = %+v", st)
	}
	if n := sup.intentosRestart(); n != 0 {
		t.Fatalf("intentos de relanzado = %d; quería 0", n)
	}
}

// TestRestartRelanzaYElBackoffCrece: con Enabled el hijo que muere solo vuelve a arrancar, y como muere
// una y otra vez (crash-loop) el backoff DUPLICA en cada intento en vez de reintentar a rueda libre.
func TestRestartRelanzaYElBackoffCrece(t *testing.T) {
	cfg := cfgHijoQueMuereSolo(t, 60)
	cfg.Restart = RestartPolicy{
		Enabled:    true,
		MinBackoff: 20 * time.Millisecond,
		MaxBackoff: 400 * time.Millisecond,
		ResetAfter: time.Hour, // el hijo vive 60ms ⇒ nunca resetea: el backoff debe crecer de verdad
	}
	sup := New(cfg, nil)
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })

	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	pidInicial := sup.Status(context.Background()).PID
	if pidInicial <= 0 {
		t.Fatalf("Status tras Start sin pid: %d", pidInicial)
	}

	if !esperarHasta(6*time.Second, func() bool { return sup.intentosRestart() >= 3 }) {
		t.Fatalf("esperaba >=3 relanzados; hubo %d", sup.intentosRestart())
	}
	if bo := sup.backoffActual(); bo <= cfg.Restart.MinBackoff {
		t.Fatalf("el backoff no creció: %s (mínimo %s)", bo, cfg.Restart.MinBackoff)
	}

	// Y el hijo vuelve a estar arriba con OTRO pid (no es el mismo proceso resucitado).
	if !esperarHasta(2*time.Second, func() bool {
		st := sup.Status(context.Background())
		return st.State == StateRunning && st.PID != pidInicial
	}) {
		t.Fatal("tras el relanzado esperaba un hijo vivo con un pid distinto")
	}
}

// TestStopDuranteBackoffNoRelanza: la parada PEDIDA gana. Con el reaper dormido en su backoff, Stop debe
// (a) devolver enseguida —sin quedarse esperando a la goroutine ni al reloj— y (b) NO relanzar después.
func TestStopDuranteBackoffNoRelanza(t *testing.T) {
	cfg := cfgHijoQueMuereSolo(t, 60)
	cfg.Restart = RestartPolicy{
		Enabled:    true,
		MinBackoff: 800 * time.Millisecond, // suficiente para pillar al reaper esperando
		MaxBackoff: 2 * time.Second,
		ResetAfter: time.Hour,
	}
	sup := New(cfg, nil)

	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// El hijo muere solo y el reaper entra en su primera espera de backoff.
	if !esperarHasta(3*time.Second, func() bool { return sup.intentosRestart() >= 1 }) {
		t.Fatal("el reaper no llegó a programar el relanzado")
	}

	inicio := time.Now()
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if tardanza := time.Since(inicio); tardanza > 400*time.Millisecond {
		t.Fatalf("Stop tardó %s: se quedó colgado esperando el backoff", tardanza)
	}

	// Más allá del backoff programado: nada debe haber revivido.
	time.Sleep(1200 * time.Millisecond)
	if st := sup.Status(context.Background()); st.State != StateStopped {
		t.Fatalf("tras Stop durante el backoff NO debe relanzarse; Status = %+v", st)
	}
	if n := sup.intentosRestart(); n != 1 {
		t.Fatalf("intentos de relanzado = %d; quería 1 (el programado, nunca ejecutado)", n)
	}
}

// TestStopDuranteLaReadinessDeUnRelanzadoDevuelveRapido: el caso que colgaba a la consola. Con el reaper
// ya PASADO el backoff —lanzando el hijo nuevo y esperando su readiness— un Stop tenía que esperar a que
// terminara toda esa secuencia (ReadyTimeout + StopTimeout) porque el bucle retenía el lock. Ahora el
// tramo corre sin lock y el ctx de la readiness cuelga de la puerta que Stop cierra: Stop devuelve rápido
// y nada se relanza después.
func TestStopDuranteLaReadinessDeUnRelanzadoDevuelveRapido(t *testing.T) {
	// El hijo vive 2s (tiempo de sobra para quedar ready y morir SOLO) y la readiness pide 1s de gracia:
	// esa gracia es la ventana en la que queremos pillar al relanzado.
	cfg := cfgHijoQueMuereSolo(t, 2000)
	cfg.ReadyProbe = ProbeProcesoVivo(1 * time.Second)
	cfg.ReadyTimeout = 5 * time.Second
	cfg.StopTimeout = 2 * time.Second
	cfg.Restart = RestartPolicy{
		Enabled:    true,
		MinBackoff: 20 * time.Millisecond,
		MaxBackoff: 100 * time.Millisecond,
		ResetAfter: time.Hour,
	}
	sup := New(cfg, nil)
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })

	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// El hijo muere solo, el reaper cumple su backoff y entra en el tramo lanzado+readiness.
	if !esperarHasta(8*time.Second, sup.relanzandoAhora) {
		t.Fatal("el relanzado no llegó a su tramo de readiness")
	}

	inicio := time.Now()
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Con el lock retenido esto tardaba ReadyTimeout (o la gracia entera) + lo que costara matar al hijo.
	if tardanza := time.Since(inicio); tardanza > 1200*time.Millisecond {
		t.Fatalf("Stop tardó %s durante la readiness de un relanzado: se quedó esperando al bucle", tardanza)
	}

	intentosTrasStop := sup.intentosRestart()
	time.Sleep(1500 * time.Millisecond) // más que la gracia de readiness y que el backoff
	if st := sup.Status(context.Background()); st.State != StateStopped {
		t.Fatalf("tras Stop durante la readiness NO debe quedar nada arriba; Status = %+v", st)
	}
	if n := sup.intentosRestart(); n != intentosTrasStop {
		t.Fatalf("hubo relanzados después del Stop: %d → %d", intentosTrasStop, n)
	}
}

// TestHealthyDelHijoSinPlanoHTTPNoEsUnaConstante: con ReadyProbe propio, Healthy devolvía true SIEMPRE, así
// que /v1/cajero/status decía healthy:true también en mitad de un crash-loop. Ahora refleja el estado real
// que este supervisor puede sostener, y Status.Probe dice de qué probe viene la señal.
func TestHealthyDelHijoSinPlanoHTTPNoEsUnaConstante(t *testing.T) {
	dir := t.TempDir()
	sup := New(Config{
		SocketPath: filepath.Join(dir, "edge.sock"),
		PIDFile:    filepath.Join(dir, "edge.cajero.pid"),
		ReadyProbe: ProbeProcesoVivo(0),
	}, nil)

	sup.mu.Lock()
	sano := sup.healthyLocked()
	sup.relanzando = true
	duranteRelanzado := sup.healthyLocked()
	sup.relanzando = false
	sup.stopping = true
	duranteParada := sup.healthyLocked()
	sup.stopping = false
	sup.mu.Unlock()

	if !sano {
		t.Fatal("con el hijo arriba y en reposo, Healthy debía ser true")
	}
	if duranteRelanzado {
		t.Fatal("en mitad de un relanzado (hijo aún no ready) Healthy NO puede decir true")
	}
	if duranteParada {
		t.Fatal("con una parada pedida en curso Healthy NO puede decir true")
	}

	if st := sup.Status(context.Background()); st.Probe != ProbeTipoProcesoVivo {
		t.Fatalf("Status.Probe = %q; quería %q (hijo sin plano HTTP)", st.Probe, ProbeTipoProcesoVivo)
	}
	// Y el supervisor histórico (sin ReadyProbe) sigue etiquetando su probe fuerte.
	nucleo := New(Config{SocketPath: filepath.Join(dir, "nucleo.sock"), PIDFile: filepath.Join(dir, "nucleo.pid")}, nil)
	if st := nucleo.Status(context.Background()); st.Probe != ProbeTipoHTTP {
		t.Fatalf("Status.Probe del núcleo = %q; quería %q", st.Probe, ProbeTipoHTTP)
	}
}

// TestVigiaDelHijoAdoptadoRelanza: wapp-ctl se reinicia y el worker sigue vivo (es lo previsto: el hijo
// sobrevive al supervisor). Start es idempotente por lock file, pero ese hijo NO es hijo de este proceso:
// no hay cmd.Wait() ni reaper. Sin vigía, el relanzado automático (REQ-051.10) quedaba muerto justo tras
// cada reinicio del supervisor. Aquí se simula el adoptado y se comprueba que al morir SÍ se relanza.
func TestVigiaDelHijoAdoptadoRelanza(t *testing.T) {
	anterior := intervaloVigiaAdoptado
	intervaloVigiaAdoptado = 20 * time.Millisecond
	t.Cleanup(func() { intervaloVigiaAdoptado = anterior })

	cfg := cfgHijoQueMuereSolo(t, 400)
	cfg.Restart = RestartPolicy{
		Enabled:    true,
		MinBackoff: 20 * time.Millisecond,
		MaxBackoff: 100 * time.Millisecond,
		ResetAfter: time.Hour,
	}

	// El hijo "de un supervisor anterior": mismo binario fake, lanzado FUERA del supervisor, con su pid en
	// el lock file. Se le hace Wait en otra goroutine para que no quede zombi (un zombi sigue existiendo
	// para processAlive y el vigía no lo vería morir nunca).
	adoptado := exec.Command(cfg.AgentBin) //nolint:gosec // el propio binario de test
	adoptado.Env = cfg.Env
	if err := adoptado.Start(); err != nil {
		t.Fatalf("no se pudo lanzar el hijo adoptado: %v", err)
	}
	go func() { _ = adoptado.Wait() }()
	pidAdoptado := adoptado.Process.Pid
	if err := os.WriteFile(cfg.PIDFile, []byte(strconv.Itoa(pidAdoptado)), 0o600); err != nil {
		t.Fatalf("write pid file: %v", err)
	}

	sup := New(cfg, nil)
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })

	// Start ve el lock file vivo ⇒ idempotente (no lanza nada) pero DEBE armar el vigía.
	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start sobre un hijo adoptado debía ser idempotente: %v", err)
	}
	if st := sup.Status(context.Background()); st.State != StateRunning || st.PID != pidAdoptado {
		t.Fatalf("Status con hijo adoptado = %+v; quería running con pid %d", st, pidAdoptado)
	}

	// El adoptado muere solo (~400ms). El vigía debe verlo y entrar al camino de relanzado.
	if !esperarHasta(5*time.Second, func() bool { return sup.intentosRestart() >= 1 }) {
		t.Fatal("el hijo adoptado murió y NADIE lo relanzó (vigía por PID ausente)")
	}
	if !esperarHasta(5*time.Second, func() bool {
		st := sup.Status(context.Background())
		return st.State == StateRunning && st.PID != pidAdoptado
	}) {
		t.Fatal("tras el relanzado del adoptado esperaba un hijo vivo con otro pid")
	}
}

// TestVigiaDelHijoAdoptadoSeRetiraConStop: el vigía no puede quedarse dando vueltas ni resucitar nada
// después de una parada PEDIDA.
func TestVigiaDelHijoAdoptadoSeRetiraConStop(t *testing.T) {
	anterior := intervaloVigiaAdoptado
	intervaloVigiaAdoptado = 20 * time.Millisecond
	t.Cleanup(func() { intervaloVigiaAdoptado = anterior })

	cfg := cfgHijoQueMuereSolo(t, 3000) // vive de sobra: lo mata el Stop, no el reloj
	cfg.Restart = RestartPolicy{Enabled: true, MinBackoff: 20 * time.Millisecond, ResetAfter: time.Hour}

	adoptado := exec.Command(cfg.AgentBin) //nolint:gosec // el propio binario de test
	adoptado.Env = cfg.Env
	if err := adoptado.Start(); err != nil {
		t.Fatalf("no se pudo lanzar el hijo adoptado: %v", err)
	}
	go func() { _ = adoptado.Wait() }()
	if err := os.WriteFile(cfg.PIDFile, []byte(strconv.Itoa(adoptado.Process.Pid)), 0o600); err != nil {
		t.Fatalf("write pid file: %v", err)
	}

	sup := New(cfg, nil)
	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if !esperarHasta(2*time.Second, func() bool { return !sup.vigiaActiva() }) {
		t.Fatal("tras Stop el vigía del adoptado debía haberse retirado")
	}

	time.Sleep(500 * time.Millisecond)
	if st := sup.Status(context.Background()); st.State != StateStopped {
		t.Fatalf("tras Stop nada debe revivir; Status = %+v", st)
	}
	if n := sup.intentosRestart(); n != 0 {
		t.Fatalf("intentos de relanzado = %d; quería 0 (la parada fue PEDIDA)", n)
	}
}

// TestReadyProbeQueIgnoraElCtxNoCuelgaAlSupervisor fija el contrato de Config.ReadyProbe: el supervisor no
// espera NUNCA a la goroutine del probe. Un probe de terceros que ignore la cancelación se queda dando
// vueltas por su cuenta, pero Start devuelve al vencer ReadyTimeout y el hijo se cierra igual.
func TestReadyProbeQueIgnoraElCtxNoCuelgaAlSupervisor(t *testing.T) {
	cfg := fakeCfg(t, "") // el fake normal: vive hasta que le llegue el SIGTERM
	cfg.ReadyTimeout = 300 * time.Millisecond
	cfg.StopTimeout = 2 * time.Second

	probeVolvio := make(chan struct{})
	var unaVez sync.Once
	cfg.ReadyProbe = func(_ context.Context) error {
		// Deliberadamente MALEDUCADO: ni mira el ctx.
		time.Sleep(1500 * time.Millisecond)
		unaVez.Do(func() { close(probeVolvio) })
		return errors.New("probe que nunca da por listo al hijo")
	}

	sup := New(cfg, nil)
	inicio := time.Now()
	err := sup.Start(context.Background())
	tardanza := time.Since(inicio)
	if err == nil {
		_ = sup.Stop(context.Background())
		t.Fatal("Start debía fallar: el probe nunca da por listo al hijo")
	}

	// 🔴 LA ASERCIÓN QUE DECIDE ES ÉSTA, Y NO ES DE RELOJ: si `Start` ha vuelto y el probe TODAVÍA no,
	// entonces no lo esperó. Punto. Antes esto se medía comparando la tardanza contra un umbral de pared
	// (1200 ms) que había que colocar entre el caso bueno (~300 ms, el ReadyTimeout) y el malo (1500 ms,
	// lo que duerme el probe). Ese umbral era ambiguo por construcción: bastaba con que el contenedor del
	// CI fuera un segundo más lento para que un supervisor CORRECTO diera 1,31 s y el test lo llamara
	// colgado — que es exactamente lo que pasó en `make ci-docker`, con `go test` local en verde.
	// El canal ya existía (se usaba abajo para no ensuciar los tests siguientes); sólo faltaba leerlo aquí.
	select {
	case <-probeVolvio:
		t.Fatalf("Start esperó a la goroutine del probe: volvió DESPUÉS que él (tardanza %s, ReadyTimeout %s)",
			tardanza, cfg.ReadyTimeout)
	default:
	}

	// Red secundaria, deliberadamente HOLGADA: sólo caza una espera desbocada (un supervisor que se cuelgue
	// de verdad), no la lentitud de la máquina. El contrato fino lo fija el `select` de arriba.
	if tardanza > 3*time.Second {
		t.Fatalf("Start tardó %s, muy por encima de su ReadyTimeout de %s", tardanza, cfg.ReadyTimeout)
	}
	if st := sup.Status(context.Background()); st.State != StateStopped {
		t.Fatalf("tras el fallo de readiness Status = %+v; quería stopped", st)
	}

	// Se espera a que el probe maleducado vuelva ANTES de terminar el test: no deja fuga real (el canal de
	// awaitReady tiene buffer 1), pero dejarlo corriendo ensuciaría a los tests siguientes.
	select {
	case <-probeVolvio:
	case <-time.After(3 * time.Second):
		t.Fatal("el probe maleducado no volvió nunca")
	}
}

// TestProbeProcesoVivoRespetaCancelacion: el probe débil no puede convertirse en una espera inmortal.
func TestProbeProcesoVivoRespetaCancelacion(t *testing.T) {
	probe := ProbeProcesoVivo(10 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(20*time.Millisecond, cancel)

	inicio := time.Now()
	err := probe(ctx)
	if err == nil {
		t.Fatal("con el ctx cancelado el probe debía devolver error, no dar por listo al hijo")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v; quería context.Canceled", err)
	}
	if tardanza := time.Since(inicio); tardanza > 2*time.Second {
		t.Fatalf("el probe tardó %s en atender la cancelación (gracia de 10s)", tardanza)
	}

	// Con el ctx ya cancelado ni siquiera arma el temporizador.
	if err := probe(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("con ctx ya cancelado error = %v; quería context.Canceled", err)
	}

	// Camino feliz: cumplida la gracia, listo.
	if err := ProbeProcesoVivo(5 * time.Millisecond)(context.Background()); err != nil {
		t.Fatalf("gracia cumplida debía dar listo: %v", err)
	}
	// Gracia cero ⇒ listo de inmediato (sin bloquear).
	if err := ProbeProcesoVivo(0)(context.Background()); err != nil {
		t.Fatalf("gracia cero debía dar listo: %v", err)
	}
}
