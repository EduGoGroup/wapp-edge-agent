// Package server implementa el servidor HTTP del plano de control del Edge: expone el contrato /v1
// (ADR-0015) sobre un Unix domain socket CO-UBICADO (sin puerto de red; la invariante "núcleo = Unix
// socket" del ADR-0015). El navegador no habla Unix socket: el supervisor (cmd/wapp-ctl, T4) hace
// reverse-proxy desde loopback, de modo que el socket nunca se expone a la red.
//
// Alcance T0: esqueleto + dos endpoints de LECTURA (GET /v1/health, GET /v1/sessions) + helpers de
// respuesta/errores reusables (decisión §10.J). Los tramos siguientes cuelgan más rutas SIN
// reescribir el servidor mediante (*Server).Handle: T1 → GET /v1/logs (SSE), T2 → POST/GET
// /v1/sessions/pair. El wire-up al daemon vivo (`agent serve`) es T3.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Config agrupa los parámetros de arranque del servidor /v1.
type Config struct {
	// SocketPath es la ruta del Unix domain socket donde escucha el servidor (cfg.ControlSocketPath).
	SocketPath string
	// Version es la build del núcleo que reporta GET /v1/health (cmd/agent/main.go const Version).
	Version string
}

// Server sirve el contrato /v1 del núcleo. Implementa http.Handler: enruta por (*http.ServeMux) y
// además traduce los casos sin match (ruta desconocida → 404, método no permitido → 405) al envelope
// de error JSON, que el ServeMux estándar no produce por sí mismo.
type Server struct {
	cfg      Config
	log      sharedlogger.Logger
	sessions SessionLister

	mux     *http.ServeMux
	routes  []routeTemplate
	httpSrv *http.Server
}

// routeTemplate registra una ruta declarada (método + segmentos de la plantilla) para poder
// distinguir 404 (ninguna plantilla casa la ruta) de 405 (la ruta casa pero el método no) y calcular
// la cabecera Allow. Soporta segmentos literales y comodines {param} / {param...} de Go 1.22.
type routeTemplate struct {
	method string
	segs   []string
}

// New construye el servidor /v1 y registra las rutas de LECTURA de T0 (health, sessions). El logger
// puede ser nil (se omiten las trazas). El SessionLister puede ser nil (sessions devuelve []). Las
// dependencias se inyectan por construcción: ese es el contrato para T3 (le pasa el lister real
// sobre la BD viva y un logger).
func New(cfg Config, log sharedlogger.Logger, sessions SessionLister) *Server {
	s := &Server{
		cfg:      cfg,
		log:      log,
		sessions: sessions,
		mux:      http.NewServeMux(),
	}
	// http.Server se construye aquí (no en Serve) para que Shutdown sea seguro aunque se invoque antes
	// de que la goroutine de Serve haya arrancado.
	s.httpSrv = &http.Server{
		Handler:           s,
		ReadHeaderTimeout: 5 * time.Second,
	}

	s.Handle(http.MethodGet, "/v1/health", s.handleHealth)
	s.Handle(http.MethodGet, "/v1/sessions", s.handleSessions)
	return s
}

// Handle registra una ruta /v1 adicional. PUNTO DE EXTENSIÓN para los tramos siguientes (T1 /v1/logs,
// T2 /v1/sessions/pair): se llama ANTES de Serve. pattern es la plantilla de ruta estilo ServeMux de
// Go 1.22 (p.ej. "/v1/logs" o "/v1/sessions/{id}/pair"); method es un verbo HTTP (http.MethodGet…).
// Registrar varias veces la misma ruta con métodos distintos es válido (alimenta la cabecera Allow).
func (s *Server) Handle(method, pattern string, h http.HandlerFunc) {
	s.mux.HandleFunc(method+" "+pattern, h)
	s.routes = append(s.routes, routeTemplate{method: method, segs: splitPath(pattern)})
}

// ServeHTTP enruta la petición. Si el ServeMux encuentra un handler para (método, ruta) lo ejecuta
// (esto deja pasar streaming como el SSE de T1, sin buffering). Si no, decide entre 405 y 404 con el
// envelope de error: 405 si algún plantilla casa la ruta con otro método (añade Allow), 404 si
// ninguna casa.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// mux.Handler solo DECIDE si hay match (y nos da la plantilla para el 404/405); el despacho real va
	// por mux.ServeHTTP para que pueble r.PathValue de los comodines {param} (mux.Handler NO los
	// pobla). Sin esto, un handler con {id} recibiría r.PathValue("id") vacío. El envelope custom de
	// 404/405 solo corre cuando NO hay match (pattern == ""), así que mux.ServeHTTP nunca toca ese caso.
	if _, pattern := s.mux.Handler(r); pattern != "" {
		s.mux.ServeHTTP(w, r)
		return
	}

	pathSegs := splitPath(r.URL.Path)
	if allowed := s.allowedMethods(pathSegs); len(allowed) > 0 {
		w.Header().Set("Allow", strings.Join(allowed, ", "))
		writeError(w, http.StatusMethodNotAllowed, codeMethodNotAllowed,
			fmt.Sprintf("método %s no permitido para %s", r.Method, r.URL.Path))
		return
	}
	writeError(w, http.StatusNotFound, codeNotFound,
		fmt.Sprintf("ruta no encontrada: %s", r.URL.Path))
}

// Listen crea el Unix domain socket en cfg.SocketPath con permisos restrictivos (0600) y devuelve el
// listener listo para Serve. Elimina un socket HUÉRFANO de un arranque previo, pero se niega a borrar
// un archivo regular ajeno en esa ruta (evita destruir datos por una mala configuración). El listener
// queda creado de forma SÍNCRONA: tras volver, el socket ya acepta conexiones.
func (s *Server) Listen() (net.Listener, error) {
	if s.cfg.SocketPath == "" {
		return nil, errors.New("control/server: ruta de socket vacía (cfg.SocketPath)")
	}

	if info, err := os.Stat(s.cfg.SocketPath); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("control/server: %q ya existe y no es un socket; abortando para no borrar datos", s.cfg.SocketPath)
		}
		if err := os.Remove(s.cfg.SocketPath); err != nil {
			return nil, fmt.Errorf("control/server: no se pudo eliminar el socket previo %q: %w", s.cfg.SocketPath, err)
		}
	}

	ln, err := net.Listen("unix", s.cfg.SocketPath)
	if err != nil {
		return nil, fmt.Errorf("control/server: no se pudo escuchar en %q: %w", s.cfg.SocketPath, err)
	}
	if err := os.Chmod(s.cfg.SocketPath, 0o600); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("control/server: no se pudo aplicar permisos 0600 a %q: %w", s.cfg.SocketPath, err)
	}
	return ln, nil
}

// Serve atiende peticiones en ln. BLOQUEA hasta que Shutdown cierre el servidor (o haya un error de
// red). Devuelve nil en cierre limpio. Patrón habitual: ln, _ := s.Listen(); go s.Serve(ln); …;
// s.Shutdown(ctx). El cierre del listener unix elimina el socket file.
func (s *Server) Serve(ln net.Listener) error {
	if s.log != nil {
		s.log.Info("plano de control: sirviendo /v1 sobre Unix socket", "socket", s.cfg.SocketPath)
	}
	if err := s.httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown cierra el servidor de forma ordenada (drena conexiones en curso hasta que ctx expire) y
// hace best-effort por eliminar el socket file. Idempotente frente a un servidor no arrancado.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.httpSrv.Shutdown(ctx)
	_ = os.Remove(s.cfg.SocketPath)
	return err
}

// allowedMethods devuelve, ordenados, los métodos declarados para una ruta cuyo path casa alguna
// plantilla (para la cabecera Allow del 405). Lista vacía ⇒ ninguna plantilla casa ⇒ es un 404.
func (s *Server) allowedMethods(pathSegs []string) []string {
	var methods []string
	seen := make(map[string]bool)
	for _, rt := range s.routes {
		if segsMatch(rt.segs, pathSegs) && !seen[rt.method] {
			seen[rt.method] = true
			methods = append(methods, rt.method)
		}
	}
	sort.Strings(methods)
	return methods
}

// splitPath parte una ruta o plantilla en segmentos sin vacíos (la raíz "/" → nil).
func splitPath(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

// segsMatch indica si la plantilla tmpl casa la ruta path (ignorando el método). Soporta el comodín
// final {x...} (multi-segmento) de Go 1.22; los segmentos {x} casan exactamente uno.
func segsMatch(tmpl, path []string) bool {
	if n := len(tmpl); n > 0 && isMultiWildcard(tmpl[n-1]) {
		if len(path) < n-1 {
			return false
		}
		for i := 0; i < n-1; i++ {
			if !segMatch(tmpl[i], path[i]) {
				return false
			}
		}
		return true
	}
	if len(tmpl) != len(path) {
		return false
	}
	for i := range tmpl {
		if !segMatch(tmpl[i], path[i]) {
			return false
		}
	}
	return true
}

// segMatch casa un segmento de plantilla contra uno de ruta: un comodín {param} casa cualquier valor;
// un literal casa por igualdad.
func segMatch(tmplSeg, pathSeg string) bool {
	if strings.HasPrefix(tmplSeg, "{") && strings.HasSuffix(tmplSeg, "}") {
		return true
	}
	return tmplSeg == pathSeg
}

// isMultiWildcard detecta el comodín multi-segmento {nombre...} de Go 1.22.
func isMultiWildcard(seg string) bool {
	return strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "...}")
}
