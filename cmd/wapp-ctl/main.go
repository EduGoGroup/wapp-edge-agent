// Command wapp-ctl es el SUPERVISOR liviano del Edge (Plan 007, T4). Proceso SIEMPRE VIVO que:
//
//  1. Sirve la web UI embebida (internal/webui) en loopback 127.0.0.1:8105 (configurable; §10.G).
//  2. Reverse-proxy de /v1/* (rutas que NO son /v1/daemon/*) al Unix socket co-ubicado del núcleo
//     (ADR-0015) → single-origin, sin CORS, el socket nunca se expone a red (decisión §10.A).
//  3. Arranca/detiene el núcleo (`agent serve`) como proceso HIJO vía internal/adapters/supervisor
//     (exec + PID file + SIGTERM; §10.D), expuesto en POST /v1/daemon/start|stop y GET /v1/daemon/status.
//  4. Arranca/detiene el WORKER CAJERO (`agent cajero`, Plan 051 · T2.2) como SEGUNDO hijo, con su propio
//     PID file, readiness de proceso vivo y RELANZADO AUTOMÁTICO; expuesto en POST /v1/cajero/start|stop
//     y GET /v1/cajero/status. Se apaga con -cajero-enabled=false / WAPP_CTL_CAJERO_ENABLED=false.
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
	"strconv"
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
	platformURLFlag := flag.String("platform-api-base-url", "", "URL base de la API pública HTTP de la plataforma cloud, puerto 8103 (default: cfg.PlatformAPIBaseURL del config; C-03/T3.5)")
	pidFile := flag.String("pid-file", "", "ruta del PID/lock file anti-duplicado DEL NÚCLEO (default: <socket>.pid); NO afecta al cajero, cuyo lock cuelga siempre del socket (ver cajeroPIDFile)")
	// Sin comillas invertidas en el uso: en un flag booleano flag.UnquoteUsage las tomaría por el NOMBRE
	// del valor y las imprimiría como si el flag llevara argumento (ver -agent-bin, que sí lo lleva).
	cajeroEnabled := flag.Bool("cajero-enabled", envBoolOr("WAPP_CTL_CAJERO_ENABLED", true), "supervisar también el worker cajero (agent cajero, Plan 051 · T2.2) como SEGUNDO hijo; false ⇒ wapp-ctl se comporta como antes del Plan 051")
	noOpen := flag.Bool("no-open", false, "no abrir el navegador automáticamente al arrancar")
	autostart := flag.Bool("autostart", false, "arrancar el núcleo (agent serve) —y el cajero, si está habilitado— automáticamente al iniciar (lo usa el LaunchAgent, Plan 023 · T3); por defecto se arrancan bajo demanda por POST /v1/daemon/start y /v1/cajero/start")
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

	// Trabajo 1 (code review 056 · T11): PlatformAPIBaseURL NO se deriva del enrolamiento (a diferencia de
	// CloudLink.Endpoint) — si nadie la fijó, el signup del Edge llamará SIEMPRE al propio localhost del
	// equipo. Un log mudo es justo lo que hizo que este defecto pasara desapercibido fuera de la máquina de
	// desarrollo; avisa en cada arranque mientras siga en el default.
	if platformAPIBaseURLLeftAtDevDefault(cfg) {
		log.Warn("wapp-ctl: PlatformAPIBaseURL sigue en el default de desarrollo — el alta de usuarios (signup) apuntará a localhost y fallará fuera de esta máquina; fija WAPP_AGENT_PLATFORM_API_BASE_URL (ver README §Variables de entorno)",
			"platform_api_base_url", cfg.PlatformAPIBaseURL)
	}

	socketPath := cfg.ControlSocketPath
	if *socketFlag != "" {
		socketPath = *socketFlag
	}

	platformBaseURL := cfg.PlatformAPIBaseURL
	if *platformURLFlag != "" {
		platformBaseURL = *platformURLFlag
	}

	sup := supervisor.New(configNucleo(*agentBin, socketPath, *pidFile), log)

	// SEGUNDO hijo (Plan 051 · T2.2): el worker cajero. MISMO binario del ecosistema, papel distinto
	// (`agent cajero`), como el manager del Plan 048. Diferencias con el núcleo, todas deliberadas:
	//
	//   - PIDFile PROPIO Y EXPLÍCITO: el default del supervisor se deriva del socket (<socket>.pid) y dos
	//     supervisores sobre el mismo socket colisionarían en el mismo lock file, matándose entre ellos.
	//   - ReadyProbe de proceso vivo: el cajero NO tiene plano HTTP que sondear (no escucha en ningún
	//     socket; reclama trabajo de la cola). Ver ProbeProcesoVivo y sus límites.
	//   - Relanzado automático: si el worker se cae, la cola deja de vaciarse en silencio. El núcleo NO
	//     lo lleva (lo rearranca el operador por /v1/daemon/start); el cajero sí (REQ-051.10).
	//   - StopTimeout de 20s en vez de los 10s por defecto: al recibir el SIGTERM el cajero intenta CERRAR
	//     EL LOTE EN VUELO (marcar en la base local la inferencia que ya pagó) antes de irse. Ese plazo
	//     interno empataba con los 10s del supervisor, y un empate aquí lo gana el SIGKILL: se tiraría
	//     trabajo ya hecho y el lote volvería a la cola para pagarse dos veces. Los 20s existen para que el
	//     cierre del lote gane esa carrera con margen. No se toca el del núcleo (su cierre limpio es otro).
	var cajeroSup *supervisor.Supervisor
	if *cajeroEnabled {
		cajeroSup = supervisor.New(supervisor.Config{
			AgentBin:    *agentBin,
			SocketPath:  socketPath,
			PIDFile:     cajeroPIDFile(socketPath),
			Args:        []string{"cajero"},
			ReadyProbe:  supervisor.ProbeProcesoVivo(2 * time.Second),
			StopTimeout: 20 * time.Second,
			Restart:     supervisor.RestartPolicy{Enabled: true},
		}, log)
	}

	router := newRouterConCajero(sup, cajeroSup, socketPath, platformBaseURL, log)

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
		"addr", *addr, "socket", socketPath, "agent_bin", *agentBin, "platform_api_base_url", platformBaseURL,
		"cajero_enabled", *cajeroEnabled, "version", Version)

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

		// El cajero va en SU PROPIA goroutine y su fallo NO puede impedir que el núcleo arranque ni tumbar
		// wapp-ctl: el núcleo manda (sin él no hay recepción 24/7; sin cajero solo se deja de clasificar, y
		// la cola conserva lo entrante hasta que vuelva). Por eso Error + seguir, nunca os.Exit.
		if cajeroSup != nil {
			go func() {
				if err := cajeroSup.Start(ctx); err != nil {
					log.Error("wapp-ctl: no se pudo autoarrancar el cajero; el núcleo sigue su curso, arráncalo por POST /v1/cajero/start", "error", err)
					return
				}
				log.Info("wapp-ctl: cajero autoarrancado (agent cajero) — clasificación de la cola en curso")
			}()
		}
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
	// por /v1/daemon/*; parar el supervisor no implica parar el daemon 24/7). LO MISMO VALE PARA EL
	// CAJERO (Plan 051 · T2.2): sobrevive a wapp-ctl y se para por POST /v1/cajero/stop; al volver
	// wapp-ctl, su Start es idempotente contra el PID file que dejó vivo.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// newRouter construye el mux del supervisor SIN cajero (el ecosistema tal como era antes del Plan 051).
// Firma intacta a propósito: la usan los tests del paquete.
func newRouter(sup *supervisor.Supervisor, socketPath, platformBaseURL string, log sharedlogger.Logger) *http.ServeMux {
	return newRouterConCajero(sup, nil, socketPath, platformBaseURL, log)
}

// newRouterConCajero construye el mux del supervisor: control de proceso (/v1/daemon/*), control del
// worker cajero (/v1/cajero/*, solo si cajeroSup != nil), reverse-proxy del resto de /v1/* al socket del
// núcleo, y la web UI embebida en "/". Factorizado para los tests.
//
// platformBaseURL es la URL base de la API pública HTTP de la plataforma cloud (puerto 8103), que
// authBorder usa para el signup (C-03/T3.5) — llamada DIRECTA por red, distinta del socketClient (que
// solo habla con el núcleo local por Unix socket).
//
// cajeroSup nil ⇒ NO se monta ninguna ruta /v1/cajero/* (con -cajero-enabled=false wapp-ctl responde
// exactamente lo que respondía antes: esas rutas caen en el proxy genérico de /v1/*).
func newRouterConCajero(sup, cajeroSup *supervisor.Supervisor, socketPath, platformBaseURL string, log sharedlogger.Logger) *http.ServeMux {
	mux := http.NewServeMux()

	// Borde de sesión del operador (Plan 033 · Ola 3 · Paso B): sesión en cookie HttpOnly + CSRF, con el
	// access token custodiado server-side y el refresh SIEMPRE en el núcleo.
	store := newSessionStore()
	socketClient := newSocketClient(socketPath)
	platformClient := newPlatformClient()
	auth := newAuthBorder(store, socketClient, platformClient, platformBaseURL, log)

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

	// Control del worker cajero (Plan 051 · T2.2), simétrico al del núcleo y con EL MISMO
	// requireCSRFIfSession en las mutadoras. Va APARTE: la respuesta de /v1/daemon/status no cambia de
	// forma (hay consumidores) — quien quiera el estado del worker pregunta por /v1/cajero/status, que
	// reutiliza el cuerpo {state,pid,healthy} del núcleo MÁS un campo "probe" propio (ver
	// cajeroStatusResponse): el healthy del cajero es una señal más débil y hay que decirlo.
	if cajeroSup != nil {
		mux.HandleFunc("/v1/cajero/start", requireCSRFIfSession(store, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				methodNotAllowed(w, http.MethodPost)
				return
			}
			if err := cajeroSup.Start(r.Context()); err != nil {
				writeError(w, http.StatusInternalServerError, codeStartFailed, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, toCajeroStatus(cajeroSup.Status(r.Context())))
		}))

		mux.HandleFunc("/v1/cajero/stop", requireCSRFIfSession(store, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				methodNotAllowed(w, http.MethodPost)
				return
			}
			if err := cajeroSup.Stop(r.Context()); err != nil {
				writeError(w, http.StatusInternalServerError, codeStopFailed, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, toCajeroStatus(cajeroSup.Status(r.Context())))
		}))

		mux.HandleFunc("/v1/cajero/status", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				methodNotAllowed(w, http.MethodGet)
				return
			}
			writeJSON(w, http.StatusOK, toCajeroStatus(cajeroSup.Status(r.Context())))
		})
	}

	// Reverse-proxy endurecido del resto de /v1/* al Unix socket del núcleo (Bearer de la cookie + CSRF +
	// retry-on-401 con refresh single-flight). /v1/daemon/* gana por especificidad del ServeMux.
	mux.Handle("/v1/", newCoreProxy(socketPath, auth, store, log))

	// Borde de autenticación (rutas propias de wapp-ctl, NO proxy).
	mux.HandleFunc("POST /login", auth.handleLoginPost)
	mux.HandleFunc("GET /login", auth.handleLoginGet)
	mux.HandleFunc("POST /signup", auth.handleSignupPost)
	mux.HandleFunc("GET /signup", auth.handleLoginGet)
	mux.HandleFunc("POST /logout", auth.handleLogout)
	mux.HandleFunc("GET /session", auth.handleSession)

	// Web UI embebida, mismo origen loopback. El documento raíz (/) está PROTEGIDO: sin sesión válida
	// redirige a /login. Los assets estáticos (app.js, styles.css, …) se sirven sin sesión (no llevan
	// secretos y los necesita también la pantalla de login).
	mux.Handle("/", rootGate(store))
	return mux
}

// cajeroStatusResponse es el cuerpo de /v1/cajero/{start,stop,status}. Es el MISMO cuerpo del núcleo
// (embebido: state, pid, healthy — un solo contrato) más un campo propio, "probe", que dice de qué
// criterio viene ese healthy:
//
//   - "http": el healthy fuerte del núcleo (GET /v1/health respondió 200).
//   - "proceso-vivo": el cajero NO tiene plano HTTP; lo más que se puede afirmar es que su proceso está
//     arriba y no está en mitad de un relanzado. Un worker vivo pero bloqueado o sin Ollama seguirá
//     diciendo healthy:true, y quien pinte esta respuesta debe saberlo.
//
// El campo va SOLO aquí: la respuesta de /v1/daemon/status no cambia de forma (tiene consumidores).
type cajeroStatusResponse struct {
	daemonStatusResponse
	Probe string `json:"probe"`
}

func toCajeroStatus(s supervisor.Status) cajeroStatusResponse {
	return cajeroStatusResponse{daemonStatusResponse: toDaemonStatus(s), Probe: s.Probe}
}

// rootGate protege el documento raíz de la webui: "/" (index.html) exige sesión válida CON TENANT ASIGNADO
// — sin sesión, o con sesión pero sin tenant ("en revisión", M-01 code review 056 del Plan 056), redirige a
// /login. La decisión se toma AQUÍ, en servidor: escribir "/index.html" a mano no sirve de nada, a
// diferencia del check anterior (solo en el JS del cliente) que sí se podía saltar. El resto de rutas
// (assets estáticos) se delega al FileServer embebido sin restricción.
func rootGate(store *sessionStore) http.Handler {
	fs := webui.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/index.html" {
			sess := store.fromRequest(r)
			if sess == nil {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
				return
			}
			if _, tenant, _ := sess.meta(); tenant == "" {
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

// platformAPIBaseURLLeftAtDevDefault reporta si PlatformAPIBaseURL se quedó en el default de desarrollo
// (config.DefaultPlatformAPIBaseURL, "http://localhost:8103"): fuera de la máquina de desarrollo eso
// significa que el signup del Edge llama a un puerto local vacío y falla siempre (Trabajo 1, code review
// 056 · T11). Extraída de main() para poder probar la decisión sin arrancar el supervisor completo.
func platformAPIBaseURLLeftAtDevDefault(cfg config.Config) bool {
	return cfg.PlatformAPIBaseURL == config.DefaultPlatformAPIBaseURL
}

// configNucleo arma la Config del supervisor DEL NÚCLEO (`agent serve`). Extraída de main() por el mismo
// motivo que platformAPIBaseURLLeftAtDevDefault: main() no lo mira ningún test, y lo que decide esta
// función ya costó cinco minutos de recepción caída en el VPS.
//
// 🔴 RELANZADO AUTOMÁTICO ACTIVADO (Plan 051 Ola 5 · T5.4). Hasta aquí esta Config NO llevaba campo
// `Restart`, así que la política quedaba en su valor cero (Enabled false) y el núcleo que moría solo se
// quedaba muerto hasta que alguien llamaba a POST /v1/daemon/start. El hallazgo de campo de PC-13 fue el
// caro: la unidad systemd vigila a `wapp-ctl` (su ExecStart) y el núcleo es HIJO suyo, así que con el
// núcleo muerto y el portero vivo `systemctl is-active wapp-edge` sigue diciendo `active`. Tras un
// `kill -9` el cajero —que sí llevaba la política— volvió solo y el núcleo no: el VPS pasó ~5 minutos sin
// poder recibir WhatsApp con el indicador en verde.
//
// Se arregla AQUÍ y no en la unidad systemd a propósito: el criterio de T5.4 admite las dos salidas («que
// `is-active` no diga active» O «que el conjunto se reinicie solo en menos de 60 s») y esta no obliga a
// redesplegar ninguna unidad en el VPS, que es donde este proyecto ya se pegó un susto.
//
// QUÉ NO CAMBIA, y conviene tenerlo escrito porque es lo que asusta de este cambio:
//
//   - EL ARRANQUE NO SE VUELVE AUTOMÁTICO. `Restart` solo gobierna al hijo que YA arrancó y se murió: el
//     derecho a relanzarse se concede en adoptLocked, tras un Start que llegó a ready. El núcleo se sigue
//     pidiendo por POST /v1/daemon/start (el `ExecStartPost` de la unidad) o con -autostart.
//   - LA PARADA PEDIDA SIGUE PARANDO. Stop cierra la puerta del relanzado ANTES de matar al hijo
//     (supervisor.Stop), así que POST /v1/daemon/stop deja el núcleo abajo y abajo se queda.
//
// El backoff es el de la casa (1 s → ×2 → 60 s, sin tope de intentos): un núcleo que no arranca debe
// seguir intentándolo. Aquí eso es menos ciego que en el cajero, porque el núcleo SÍ tiene plano HTTP: su
// Status.Healthy sale de un GET /v1/health real, de modo que un crash-loop se ve como running=false o
// healthy=false en /v1/daemon/status, y no como un "todo bien" constante.
func configNucleo(agentBin, socketPath, pidFile string) supervisor.Config {
	return supervisor.Config{
		AgentBin:   agentBin,
		SocketPath: socketPath,
		PIDFile:    pidFile, // vacío ⇒ el supervisor usa <socket>.pid
		Restart:    supervisor.RestartPolicy{Enabled: true},
	}
}

// cajeroPIDFile deriva el PID/lock file del cajero SIEMPRE del socket: <socket>.cajero.pid. Que sea
// EXPLÍCITO y DISTINTO del núcleo no es cosmético (los dos supervisores comparten SocketPath y con el
// default derivado escribirían el mismo lock file, cada uno leyendo el pid del otro).
//
// Y que NO dependa de -pid-file tampoco lo es: la identidad de un lock tiene que ser estable entre
// arranques o deja de ser un lock. Antes, con -pid-file el del cajero era "X.cajero" y sin él
// "<socket>.cajero.pid": un operador que añadiera o quitara ese flag entre dos arranques dejaba huérfano
// el lock anterior, el supervisor nuevo no veía al cajero vivo y lanzaba un SEGUNDO worker sobre la misma
// cola — dos clientes de Ollama reclamando los mismos mensajes. El socket, en cambio, es la identidad
// real del Edge (misma fuente que usa el hijo para hablar con el núcleo), así que el lock cuelga de él.
// Si algún día hace falta moverlo, que sea con un flag PROPIO (-cajero-pid-file), no reaprovechando el
// del núcleo.
func cajeroPIDFile(socketPath string) string {
	return socketPath + ".cajero.pid"
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envBoolOr es el molde de envOr para flags booleanos (WAPP_CTL_CAJERO_ENABLED). Acepta lo que acepta
// strconv.ParseBool ("1/0", "true/false", "t/f", "T/F", "TRUE/FALSE"…); un valor ilegible NO tumba el
// arranque: se queda en el default y el flag de línea de comandos siempre puede sobreescribirlo.
func envBoolOr(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
