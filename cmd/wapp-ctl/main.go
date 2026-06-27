// Command wapp-ctl es el SUPERVISOR liviano del Edge (Plan 007, T4). Proceso SIEMPRE VIVO que:
//
//  1. Sirve la web UI embebida (internal/webui) en loopback 127.0.0.1:8765 (configurable; §10.G).
//  2. Reverse-proxy de /v1/* (rutas que NO son /v1/daemon/*) al Unix socket co-ubicado del núcleo
//     (ADR-0015) → single-origin, sin CORS, el socket nunca se expone a red (decisión §10.A).
//  3. Arranca/detiene el núcleo (`agent serve`) como proceso HIJO vía internal/adapters/supervisor
//     (exec + PID file + SIGTERM; §10.D), expuesto en POST /v1/daemon/start|stop y GET /v1/daemon/status.
//
// La UI y /v1/daemon/* SIGUEN respondiendo aunque el núcleo esté caído (ese es el punto: poder
// arrancarlo). Solo el proxy /v1/* depende de que el núcleo viva; si el socket no responde se traduce a
// una respuesta CLARA "daemon down" (no un 502 crudo) para que la UI (T5) lo distinga. Sin TLS ni auth:
// loopback + permisos del socket bastan en el equipo del cliente (decisión §10.K).
package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/supervisor"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/logger"
	"github.com/EduGoGroup/wapp-edge-agent/internal/webui"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Version identifica la build del supervisor.
const Version = "0.1.0-ctl"

func main() {
	addr := flag.String("addr", envOr("WAPP_CTL_ADDR", "127.0.0.1:8765"), "dirección loopback donde sirve el supervisor (host:puerto)")
	agentBin := flag.String("agent-bin", envOr("WAPP_CTL_AGENT_BIN", defaultAgentBin()), "ruta del binario núcleo `agent` a lanzar (default: hermano de wapp-ctl, si no PATH)")
	socketFlag := flag.String("socket", "", "ruta del Unix socket /v1 del núcleo (default: cfg.ControlSocketPath del config)")
	pidFile := flag.String("pid-file", "", "ruta del PID/lock file anti-duplicado (default: <socket>.pid)")
	noOpen := flag.Bool("no-open", false, "no abrir el navegador automáticamente al arrancar")
	flag.Parse()

	// Config del Edge: MISMA fuente y overlay que el núcleo (WAPP_AGENT_CONFIG / config.yaml + WAPP_AGENT_*).
	// De ahí sale la ruta del socket, para no inventar otra fuente (el hijo `agent serve` la lee igual).
	cfgPath := os.Getenv("WAPP_AGENT_CONFIG")
	if cfgPath == "" {
		cfgPath = "config.yaml"
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		sharedlogger.Default().Error("wapp-ctl: no se pudo cargar la configuración", "error", err, "path", cfgPath)
		os.Exit(1)
	}
	log := logger.New(cfg)

	socketPath := cfg.ControlSocketPath
	if *socketFlag != "" {
		socketPath = *socketFlag
	}

	sup := supervisor.New(supervisor.Config{
		AgentBin:   *agentBin,
		SocketPath: socketPath,
		PIDFile:    *pidFile, // vacío ⇒ el supervisor usa <socket>.pid
	}, log)

	router := newRouter(sup, socketPath, log)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()

	log.Info("wapp-ctl: supervisor arriba",
		"addr", *addr, "socket", socketPath, "agent_bin", *agentBin, "version", Version)

	if !*noOpen {
		openBrowser("http://"+*addr, log)
	}

	select {
	case <-ctx.Done():
		log.Info("wapp-ctl: señal de cierre recibida, apagando")
	case err := <-serveErr:
		if err != nil && err != http.ErrServerClosed {
			log.Error("wapp-ctl: el servidor loopback falló", "error", err)
		}
	}

	// Cierre ordenado del loopback. NOTA: NO se detiene el núcleo aquí (el supervisor controla su ciclo
	// por /v1/daemon/*; parar el supervisor no implica parar el daemon 24/7).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// newRouter construye el mux del supervisor: control de proceso (/v1/daemon/*), reverse-proxy del resto
// de /v1/* al socket del núcleo, y la web UI embebida en "/". Factorizado para los tests.
func newRouter(sup *supervisor.Supervisor, socketPath string, log sharedlogger.Logger) *http.ServeMux {
	mux := http.NewServeMux()

	// Control de proceso: lo atiende el supervisor (no se proxya). Sin método en el patrón para devolver
	// un 405 con envelope ante el verbo equivocado (en vez de proxyar la ruta /v1/daemon/* al núcleo).
	mux.HandleFunc("/v1/daemon/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		if err := sup.Start(r.Context()); err != nil {
			writeError(w, http.StatusInternalServerError, codeStartFailed, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, toDaemonStatus(sup.Status(r.Context())))
	})

	mux.HandleFunc("/v1/daemon/stop", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		if err := sup.Stop(r.Context()); err != nil {
			writeError(w, http.StatusInternalServerError, codeStopFailed, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, toDaemonStatus(sup.Status(r.Context())))
	})

	mux.HandleFunc("/v1/daemon/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		writeJSON(w, http.StatusOK, toDaemonStatus(sup.Status(r.Context())))
	})

	// Reverse-proxy del resto de /v1/* al Unix socket del núcleo. /v1/daemon/* gana por especificidad
	// del ServeMux (patrón más largo), así que aquí solo cae health/sessions/logs/pair.
	mux.Handle("/v1/", newCoreProxy(socketPath, log))

	// Web UI embebida (placeholder T4; UI real T5), mismo origen loopback.
	mux.Handle("/", webui.Handler())
	return mux
}

// newCoreProxy construye el reverse-proxy a /v1/* del núcleo por el Unix socket. El Transport marca
// SIEMPRE el socket (DialContext a "unix"); el host de la URL es un placeholder. Si el socket no
// responde (núcleo caído), el ErrorHandler TRADUCE el fallo a "daemon down" (503 + envelope), nunca un
// 502 crudo ni una página rota → contrato estable para la UI (T5): status 503 + code "daemon_down".
func newCoreProxy(socketPath string, log sharedlogger.Logger) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			r.URL.Scheme = "http"
			r.URL.Host = "unix" // placeholder; el DialContext ignora el host y marca el socket
		},
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socketPath)
			},
			ResponseHeaderTimeout: 30 * time.Second,
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if log != nil {
				log.Warn("wapp-ctl: núcleo no responde por el socket (daemon down)", "path", r.URL.Path, "error", err)
			}
			writeError(w, http.StatusServiceUnavailable, codeDaemonDown,
				"el núcleo no responde por el socket: arranca el daemon (POST /v1/daemon/start)")
		},
	}
}

// defaultAgentBin resuelve la ruta del binario núcleo: primero el hermano "agent" junto al ejecutable de
// wapp-ctl (caso dev: `go build ./cmd/...` deja ambos juntos); si no existe, "agent" a secas (PATH). El
// flag --agent-bin / env WAPP_CTL_AGENT_BIN lo sobreescribe.
func defaultAgentBin() string {
	if exe, err := os.Executable(); err == nil {
		cand := filepath.Join(filepath.Dir(exe), "agent")
		if fi, statErr := os.Stat(cand); statErr == nil && !fi.IsDir() {
			return cand
		}
	}
	return "agent"
}

// openBrowser abre la URL en el navegador del SO. Best-effort, NO bloqueante, NO fatal (si no puede,
// solo loguea): el usuario siempre puede abrir la URL a mano.
func openBrowser(url string, log sharedlogger.Logger) {
	var name string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		name, args = "open", []string{url}
	case "windows":
		name, args = "rundll32", []string{"url.dll,FileProtocolHandler", url}
	default:
		name, args = "xdg-open", []string{url}
	}
	if err := exec.Command(name, args...).Start(); err != nil && log != nil { //nolint:gosec // comando fijo del SO, url loopback propia.
		log.Warn("wapp-ctl: no se pudo abrir el navegador (best-effort)", "url", url, "error", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
