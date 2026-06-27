package sessionmgr

import (
	"context"
	"sync"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// SessionHealth es la salud de RUNTIME del listener de una sesión (design §10.H): un estado vivo,
// distinto del estado de NEGOCIO persistido (domain.SessionState: pairing/active/loggedout). El
// plano de control (GET /v1/sessions, T6) lo expone para que el operador vea una sesión 'degraded'
// (su socket cayó y está reintentando) sin que eso tumbe el proceso ni las otras sesiones.
type SessionHealth int

const (
	// HealthStarting: la goroutine listener se arrancó pero aún no reporta socket vivo (estado inicial).
	HealthStarting SessionHealth = iota
	// HealthListening: el listener está escuchando (su runner corre sin haber caído).
	HealthListening
	// HealthDegraded: el listener cayó (error o pánico) y está reintentando con backoff (§10.H). Aislado:
	// no afecta a las demás sesiones.
	HealthDegraded
	// HealthStopped: el listener terminó por apagado ordenado (Stop canceló su context, §10.I).
	HealthStopped
)

// String da una etiqueta legible de la salud (logs / plano de control).
func (h SessionHealth) String() string {
	switch h {
	case HealthStarting:
		return "starting"
	case HealthListening:
		return "listening"
	case HealthDegraded:
		return "degraded"
	case HealthStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// liveSession es el estado VIVO de una sesión que el Manager posee (design §1). Reúne el metadato de
// negocio, la custodia DEK resuelta para ESA sesión, su logger etiquetado y, a partir de T4, el cancel
// de su goroutine listener y su salud de runtime.
//
// La conexión (store/cliente whatsmeow) NO se materializa como campo: el listener la abre y la cierra
// DENTRO de su goroutine (design §6/§10.I), de modo que cada intento de reconexión obtiene un handle
// fresco y el apagado ordenado cierra el *sql.DB vía defer al cancelarse el context. Así el Manager no
// arrastra un puntero compartido a recursos de red que habría que proteger entre goroutines.
type liveSession struct {
	// meta es el metadato de negocio persistido (session_id, jid, estado, store_dir, timestamps). Es
	// inmutable tras el registro: el listener NO lo muta (la salud de runtime va en `health`), así que
	// List() puede leerlo bajo el lock del Manager sin tocar mu.
	meta domain.Session
	// custody es la custodia DEK de ESTA sesión (NewFileCustody(layout.DEKPath(id))); inyectada, no global.
	custody app.KeyCustody
	// log arrastra session_id/jid en cada línea (design §10.J); hijo del logger del Manager.
	log sharedlogger.Logger

	// mu protege cancel/done/health/lastErr: los escribe la goroutine listener y los leen Stop()/Health().
	mu sync.Mutex
	// cancel detiene la goroutine listener de la sesión (apagado ordenado, design §10.I). nil hasta que
	// startListener arranca un listener real.
	cancel context.CancelFunc
	// done se CIERRA cuando la goroutine listener de la sesión retorna (tras cancel, ya cerró su *sql.DB
	// vía defer). Permite a Unlink esperar SOLO a ESTA goroutine —el borrado quirúrgico de una sesión
	// sin tocar a las demás (design §7)— sin usar el WaitGroup GLOBAL del Manager (que une a todas). nil
	// si no se arrancó listener (sesión registrada sin escucha): waitDone es entonces un no-op.
	done chan struct{}
	// health es la salud de runtime observada por la goroutine listener (starting→listening→degraded→stopped).
	health SessionHealth
	// lastErr es la última causa de caída del listener (para diagnóstico/plano de control); nil si sano.
	lastErr error
}

// arm prepara la sesión para su goroutine listener bajo lock: guarda su cancel (apagado ordenado /
// borrado quirúrgico) y abre el canal done que esa goroutine cerrará al retornar. Lo invoca
// startListener justo antes de lanzar la goroutine; Stop usa cancel y Unlink espera done.
func (s *liveSession) arm(cancel context.CancelFunc) {
	s.mu.Lock()
	s.cancel = cancel
	s.done = make(chan struct{})
	s.mu.Unlock()
}

// signalDone cierra el canal done (la goroutine listener ya retornó). Idempotente solo dentro de una
// goroutine (cada listener lo cierra una vez al salir, vía defer). Seguro sin lock: la referencia a
// done quedó publicada por arm antes del `go` (happens-before), y el cierre no compite con escrituras.
func (s *liveSession) signalDone() {
	if s.done != nil {
		close(s.done)
	}
}

// waitDone bloquea hasta que la goroutine listener de ESTA sesión haya retornado (done cerrado). Si la
// sesión se registró SIN escucha (done nil), retorna de inmediato. Lee la referencia bajo lock para no
// competir con arm. Es la pieza que permite a Unlink unir SOLO esta goroutine, no el WaitGroup global.
func (s *liveSession) waitDone() {
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done != nil {
		<-done
	}
}

// stop cancela la goroutine listener si está arrancada (idempotente). No espera: el WaitGroup del
// Manager hace el join. Marca la sesión como deteniéndose para reflejarlo en Health() de inmediato.
func (s *liveSession) stop() {
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// mark fija la salud de runtime (y opcionalmente la causa de caída) bajo lock.
func (s *liveSession) mark(h SessionHealth, cause error) {
	s.mu.Lock()
	s.health = h
	if h == HealthDegraded {
		s.lastErr = cause
	}
	if h == HealthListening {
		s.lastErr = nil
	}
	s.mu.Unlock()
}

// snapshot devuelve la salud y la última causa de caída bajo lock (lectura para Health()/tests).
func (s *liveSession) snapshot() (SessionHealth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.health, s.lastErr
}
