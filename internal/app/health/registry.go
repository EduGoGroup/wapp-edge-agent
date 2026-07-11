// Package health mantiene el SNAPSHOT DE SALUD DE RUNTIME por sesión del Edge (Plan 031 T6). Es el
// CONTRATO que T7 lee para armar el mensaje SessionHealth del heartbeat y que el plano de control local
// expone en GET /v1/health: prueba de vida REAL del socket de WhatsApp (no "el cliente existe"), motivo
// de degradación, duración de la última carga de la DEK y edad del último evento entrante.
//
// Frontera zero-knowledge (ADR-0007): aquí SOLO viven METADATOS de salud (estados, motivos, duraciones,
// timestamps). NUNCA la DEK, credenciales ni contenido de mensajes. El motivo (DegradedReason) es una
// etiqueta corta y estable (dek_load_timeout, reconnecting, logged_out…), no un texto libre con PII.
//
// T6 lo PUEBLA (desde el ciclo de escucha: sessionmgr, app.Listen y el listener whatsmeow); T7 lo
// CONSUME. La API es deliberadamente mínima: setters thread-safe por session_id + lectura de snapshot.
package health

import (
	"sync"
	"time"
)

// SocketState es la salud observada del socket de WhatsApp de una sesión (prueba de vida). Son las cuatro
// etiquetas del contrato del heartbeat (Plan 031 T1): el receptor viejo las ignora, el emisor las manda
// como string.
type SocketState string

const (
	// SocketConnecting: aún no hay socket vivo — arrancando, cargando la DEK, o whatsmeow reintentando el
	// dial tras un corte transitorio (auto-reconnect). Estado inicial.
	SocketConnecting SocketState = "connecting"
	// SocketConnected: socket conectado y autenticado (tras *events.Connected). La sesión recibe/envía.
	SocketConnected SocketState = "connected"
	// SocketDegraded: el listener cayó (error/timeout) y está reintentando con backoff, o la carga de la DEK
	// venció su plazo (dek_load_timeout). Con DegradedReason poblado. Aislado: no tumba a las demás sesiones.
	SocketDegraded SocketState = "degraded"
	// SocketDead: WhatsApp cerró la sesión (*events.LoggedOut); requiere re-emparejar. No se recupera solo.
	SocketDead SocketState = "dead"
)

// Motivos de degradación (DegradedReason): etiquetas cortas y ESTABLES (las consume el Cloud en T3/T4).
// No son texto libre ni llevan PII.
const (
	// ReasonDEKLoadTimeout: la carga de la DEK excedió su plazo (el caso del incidente 2026-07-11: cgo del
	// Keychain bloqueado esperando el diálogo de permiso). El camino NO queda colgado: se reintenta con backoff.
	ReasonDEKLoadTimeout = "dek_load_timeout"
	// ReasonReconnecting: corte transitorio del socket; whatsmeow reintenta el dial (auto-reconnect).
	ReasonReconnecting = "reconnecting"
	// ReasonLoggedOut: WhatsApp cerró la sesión (LoggedOut) — estado dead, requiere re-emparejar.
	ReasonLoggedOut = "logged_out"
	// ReasonListenerDown: el ciclo de escucha cayó por un error no clasificado; reintento con backoff.
	ReasonListenerDown = "listener_down"
)

// Snapshot es la foto inmutable de la salud de runtime de UNA sesión. T7 la lee y deriva los campos del
// heartbeat (p. ej. last_inbound_event_age_s = now - LastInboundAt; dek_load_duration_ms = DEKLoadDuration).
type Snapshot struct {
	// SocketState es la prueba de vida del socket de WhatsApp de la sesión.
	SocketState SocketState
	// DegradedReason es la etiqueta del motivo cuando SocketState es degraded/dead; "" en connected/connecting.
	DegradedReason string
	// DEKLoadDuration es cuánto tardó la ÚLTIMA carga de la DEK en completarse (0 si aún no completó ninguna).
	// En el caso de timeout abandonado, se rellena TARDE cuando la carga cgo por fin retorna (watchdog.onLate).
	DEKLoadDuration time.Duration
	// LastInboundAt es el instante del último evento entrante entregado por el listener; cero si aún ninguno.
	// T7 deriva la EDAD (now - LastInboundAt) al armar el heartbeat: una edad creciente con socket "connected"
	// es la firma del arranque mudo (§1 del runbook).
	LastInboundAt time.Time
}

// sessionHealth es el estado mutable por sesión, protegido por el mutex del Registry.
type sessionHealth struct {
	state       SocketState
	reason      string
	dekDuration time.Duration
	lastInbound time.Time
}

// Registry es el registro vivo de salud por session_id (Plan 031 T6). Thread-safe: lo pueblan varias
// goroutines (el listener whatsmeow, la carga de DEK, el runner del sessionmgr) y lo lee T7/el plano de
// control. Un solo Registry por daemon, compartido por todas las sesiones.
type Registry struct {
	mu       sync.RWMutex
	sessions map[string]*sessionHealth
}

// NewRegistry construye un Registry vacío listo para poblar.
func NewRegistry() *Registry {
	return &Registry{sessions: make(map[string]*sessionHealth)}
}

// entry devuelve (creándola si hace falta) la fila mutable de la sesión id, bajo lock de escritura.
func (r *Registry) entry(id string) *sessionHealth {
	sh, ok := r.sessions[id]
	if !ok {
		sh = &sessionHealth{}
		r.sessions[id] = sh
	}
	return sh
}

// SetSocketState fija la prueba de vida del socket de la sesión id y su motivo (etiqueta corta). Para los
// estados sanos (connected/connecting) pasa reason "" — SetSocketState lo limpia igual por seguridad. Es
// nil-safe: un *Registry nil (tests sin registro cableado) hace no-op.
func (r *Registry) SetSocketState(id string, state SocketState, reason string) {
	if r == nil {
		return
	}
	if state == SocketConnected || state == SocketConnecting {
		reason = "" // los estados sanos no llevan motivo; no arrastrar uno viejo.
	}
	r.mu.Lock()
	sh := r.entry(id)
	sh.state = state
	sh.reason = reason
	r.mu.Unlock()
}

// SetDEKLoadDuration registra cuánto tardó la última carga de la DEK de la sesión id (éxito o retorno
// tardío de una carga abandonada). Nil-safe.
func (r *Registry) SetDEKLoadDuration(id string, d time.Duration) {
	if r == nil {
		return
	}
	r.mu.Lock()
	sh := r.entry(id)
	sh.dekDuration = d
	r.mu.Unlock()
}

// MarkInbound sella el instante del último evento entrante de la sesión id (lo llama el listener al
// entregar un mensaje). T7 deriva la edad al leer. Nil-safe.
func (r *Registry) MarkInbound(id string, at time.Time) {
	if r == nil {
		return
	}
	r.mu.Lock()
	sh := r.entry(id)
	sh.lastInbound = at
	r.mu.Unlock()
}

// Snapshot devuelve la foto de salud de la sesión id (ok=false si no hay entrada). Nil-safe. Lo consume T7
// para armar el heartbeat y el plano de control para GET /v1/health.
func (r *Registry) Snapshot(id string) (Snapshot, bool) {
	if r == nil {
		return Snapshot{}, false
	}
	r.mu.RLock()
	sh, ok := r.sessions[id]
	if !ok {
		r.mu.RUnlock()
		return Snapshot{}, false
	}
	snap := Snapshot{
		SocketState:     sh.state,
		DegradedReason:  sh.reason,
		DEKLoadDuration: sh.dekDuration,
		LastInboundAt:   sh.lastInbound,
	}
	r.mu.RUnlock()
	return snap, true
}

// Remove borra la entrada de la sesión id (al desvincularla): su salud deja de reportarse. Idempotente y
// nil-safe.
func (r *Registry) Remove(id string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	delete(r.sessions, id)
	r.mu.Unlock()
}

// SessionReporter es la vista POR SESIÓN del Registry que se entrega al stack de escucha (el listener
// whatsmeow y la carga de la DEK) para que reporten salud SIN conocer su session_id: el factory del
// sessionmgr lo liga a la sesión con For(id). Mantiene el acoplamiento mínimo (esos adaptadores no ven el
// Registry entero ni pueden leer otras sesiones).
type SessionReporter interface {
	// SetSocketState reporta la prueba de vida del socket de esta sesión.
	SetSocketState(state SocketState, reason string)
	// SetDEKLoadDuration reporta la duración de la última carga de la DEK de esta sesión.
	SetDEKLoadDuration(d time.Duration)
	// MarkInbound sella el instante del último evento entrante de esta sesión.
	MarkInbound(at time.Time)
}

// For liga el Registry a una sesión concreta y devuelve su SessionReporter. Nil-safe: un *Registry nil
// devuelve un reporter no-op, así los caminos/tests que no cablean registro operan sin ramificaciones.
func (r *Registry) For(id string) SessionReporter {
	return boundReporter{reg: r, id: id}
}

// boundReporter adapta el Registry a SessionReporter fijando el session_id. reg puede ser nil (los setters
// del Registry son nil-safe), de modo que el reporter ligado también es no-op sin registro.
type boundReporter struct {
	reg *Registry
	id  string
}

func (b boundReporter) SetSocketState(state SocketState, reason string) {
	b.reg.SetSocketState(b.id, state, reason)
}
func (b boundReporter) SetDEKLoadDuration(d time.Duration) { b.reg.SetDEKLoadDuration(b.id, d) }
func (b boundReporter) MarkInbound(at time.Time)           { b.reg.MarkInbound(b.id, at) }
