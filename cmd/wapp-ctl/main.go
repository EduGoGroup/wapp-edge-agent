// Command wapp-ctl es el SUPERVISOR liviano del Edge (Plan 007, T4). Proceso SIEMPRE VIVO que:
//
//  1. Sirve la web UI embebida (internal/webui) en loopback 127.0.0.1:8105 (configurable; §10.G).
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
	"net/http"
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

// Version identifica la build del supervisor. Se inyecta en release vía
// -ldflags "-X main.Version=$(git describe --tags --always --dirty)" (ver
// Makefile, Plan 023 · T0). DEBE seguir siendo `var` (no `const`): ldflags -X
// solo sobre-escribe variables de string. El literal es el fallback de dev
// cuando se compila sin ldflags. La versión aparece en el log de arranque.
var Version = "0.1.0-ctl"

func main() {
	addr := flag.String("addr", envOr("WAPP_CTL_ADDR", "127.0.0.1:8105"), "dirección loopback donde sirve el supervisor (host:puerto)")
	agentBin := flag.String("agent-bin", envOr("WAPP_CTL_AGENT_BIN", defaultAgentBin()), "ruta del binario núcleo `agent` a lanzar (default: hermano de wapp-ctl, si no PATH)")
	socketFlag := flag.String("socket", "", "ruta del Unix socket /v1 del núcleo (default: cfg.ControlSocketPath del config)")
	pidFile := flag.String("pid-file", "", "ruta del PID/lock file anti-duplicado (default: <socket>.pid)")
	noOpen := flag.Bool("no-open", false, "no abrir el navegador automáticamente al arrancar")
	autostart := flag.Bool("autostart", false, "arrancar el núcleo (agent serve) automáticamente al iniciar (lo usa el LaunchAgent, Plan 023 · T3); por defecto el núcleo se arranca bajo demanda por POST /v1/daemon/start")
	flag.Parse()

	// Config del Edge: MISMA fuente y overlay que el núcleo (WAPP_AGENT_CONFIG / config.yaml + WAPP_AGENT_*).
	// De ahí sale la ruta del socket, para no inventar otra fuente (el hijo `agent serve` la lee igual).
	cfgPath := os.Getenv("WAPP_AGENT_CONFIG")
	if cfgPath == "" {
		// Misma ruta estable que el núcleo (Plan 023 · T1): <data_dir>/config.yaml, no relativa al CWD.
		cfgPath = config.DefaultConfigPath()
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

	// Autoarranque del núcleo (Plan 023 · T3): bajo el LaunchAgent por-usuario queremos recepción 24/7 y que
	// el Restore del Plan 022 corra al iniciar sesión. Start es idempotente (si el núcleo ya corre no hace
	// nada) y BLOQUEA sondeando readiness, así que va en goroutine para no retrasar el select de cierre.
	// Corre TRAS el login (LaunchAgent), con el Keychain del usuario ya disponible para la DEK (T2).
	if *autostart {
		go func() {
			if err := sup.Start(ctx); err != nil {
				log.Error("wapp-ctl: no se pudo autoarrancar el núcleo; arráncalo por POST /v1/daemon/start", "error", err)
				return
			}
			log.Info("wapp-ctl: núcleo autoarrancado (agent serve) — recepción 24/7 y Restore del Plan 022 en curso")
		}()
	}

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

	// Borde de sesión del operador (Plan 033 · Ola 3 · Paso B): sesión en cookie HttpOnly + CSRF, con el
	// access token custodiado server-side y el refresh SIEMPRE en el núcleo.
	store := newSessionStore()
	socketClient := newSocketClient(socketPath)
	auth := newAuthBorder(store, socketClient, log)

	// Control de proceso: lo atiende el supervisor (no se proxya). Sin método en el patrón para devolver
	// un 405 con envelope ante el verbo equivocado (en vez de proxyar la ruta /v1/daemon/* al núcleo).
	// Las mutadoras (start/stop) exigen CSRF SI hay sesión (bootstrap pre-login sigue funcionando).
	mux.HandleFunc("/v1/daemon/start", requireCSRFIfSession(store, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		if err := sup.Start(r.Context()); err != nil {
			writeError(w, http.StatusInternalServerError, codeStartFailed, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, toDaemonStatus(sup.Status(r.Context())))
	}))

	mux.HandleFunc("/v1/daemon/stop", requireCSRFIfSession(store, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		if err := sup.Stop(r.Context()); err != nil {
			writeError(w, http.StatusInternalServerError, codeStopFailed, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, toDaemonStatus(sup.Status(r.Context())))
	}))

	mux.HandleFunc("/v1/daemon/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		writeJSON(w, http.StatusOK, toDaemonStatus(sup.Status(r.Context())))
	})

	// Reverse-proxy endurecido del resto de /v1/* al Unix socket del núcleo (Bearer de la cookie + CSRF +
	// retry-on-401 con refresh single-flight). /v1/daemon/* gana por especificidad del ServeMux.
	mux.Handle("/v1/", newCoreProxy(socketPath, auth, store, log))

	// Borde de autenticación (rutas propias de wapp-ctl, NO proxy).
	mux.HandleFunc("POST /login", auth.handleLoginPost)
	mux.HandleFunc("GET /login", auth.handleLoginGet)
	mux.HandleFunc("POST /logout", auth.handleLogout)
	mux.HandleFunc("GET /session", auth.handleSession)

	// Web UI embebida, mismo origen loopback. El documento raíz (/) está PROTEGIDO: sin sesión válida
	// redirige a /login. Los assets estáticos (app.js, styles.css, …) se sirven sin sesión (no llevan
	// secretos y los necesita también la pantalla de login).
	mux.Handle("/", rootGate(store))
	return mux
}

// rootGate protege el documento raíz de la webui: "/" (index.html) exige sesión válida — sin ella redirige
// a /login. El resto de rutas (assets estáticos) se delega al FileServer embebido sin restricción.
func rootGate(store *sessionStore) http.Handler {
	fs := webui.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/index.html" {
			if store.fromRequest(r) == nil {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
				return
			}
		}
		fs.ServeHTTP(w, r)
	})
}

// requireCSRFIfSession envuelve un handler mutador propio de wapp-ctl (daemon start/stop): si hay sesión de
// operador, exige X-CSRF-Token válido; sin sesión (bootstrap de primera ejecución) lo deja pasar.
func requireCSRFIfSession(store *sessionStore, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if sess := store.fromRequest(r); sess != nil && !csrfValid(r, sess) {
			writeError(w, http.StatusForbidden, "csrf_invalid", "Token CSRF ausente o inválido.")
			return
		}
		next(w, r)
	}
}

// defaultAgentBin resuelve la ruta del binario núcleo: primero el hermano "agent" junto al ejecutable de
// wapp-ctl (caso dev: `go build ./cmd/...` deja ambos juntos); si no existe, "agent" a secas (PATH). El
// flag --agent-bin / env WAPP_CTL_AGENT_BIN lo sobreescribe.
//
// En Windows el binario núcleo se compila como "agent.exe" (ver Makefile, sufijo .exe del build_target),
// así que el hermano a resolver es "agent.exe"; sin este ajuste el layout hermano no resolvía en Windows
// (Plan 024 · T0). El fallback de PATH usa el mismo nombre por-SO.
func defaultAgentBin() string {
	name := "agent"
	if runtime.GOOS == "windows" {
		name = "agent.exe"
	}
	if exe, err := os.Executable(); err == nil {
		cand := filepath.Join(filepath.Dir(exe), name)
		if fi, statErr := os.Stat(cand); statErr == nil && !fi.IsDir() {
			return cand
		}
	}
	return name
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
