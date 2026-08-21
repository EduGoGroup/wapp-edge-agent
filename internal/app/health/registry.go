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
	"sort"
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

	// DroppedByPassiveProfile es cuántos entrantes ha descartado LA PUERTA de esta sesión por tener PERFIL
	// PASIVO (Plan 046 · Ola 2 · T2.3, REQ-07/REQ-11). Es una cardinalidad, sin PII: no dice de quién ni
	// qué, solo cuántos.
	//
	// 🔴 ES LA ÚNICA PRUEBA DE QUE EL FILTRO EXISTE. Un descarte que no deja fila, no sube al cable y no
	// deja acuse distinguible es indistinguible de «esa sesión no recibió nada»: sin este número, un filtro
	// roto —o un filtro que corta de más— no se ve por ninguna parte. Por eso el contador viaja hasta aquí
	// en vez de quedarse en el acumulado por sesión del listener (`whatsmeow.InboundStats`), que hasta el
	// Plan 051 · T1.13 no tenía un solo llamante de producción.
	//
	// ⚠️ VIVE EN MEMORIA Y SE REINICIA CON EL PROCESO, y desde T5.4 del Plan 051 el núcleo SE RELANZA SOLO.
	// Un 0 después de un reinicio NO significa «no descartó nada»: significa «este proceso acaba de nacer».
	// Se lee como una serie que sube, no como un total histórico.
	//
	// ⚠️ Y SE VA CON LA SESIÓN: `Remove` borra la entrada al desvincular, así que el contador vuelve a 0 si
	// la sesión se re-empareja. Es coherente con el resto del Snapshot (es salud de una sesión VIVA), pero
	// hay que saberlo antes de restarle dos lecturas.
	DroppedByPassiveProfile uint64
}

// sessionHealth es el estado mutable por sesión, protegido por el mutex del Registry.
type sessionHealth struct {
	state       SocketState
	reason      string
	dekDuration time.Duration
	lastInbound time.Time
	// droppedPassive es el acumulado de descartes por PERFIL PASIVO de esta sesión (Plan 046 · T2.3). Va
	// bajo el MISMO mutex que el resto y no como atómico suelto: quien lo escribe es el hilo de whatsmeow —
	// una vez por entrante descartado, que en una sesión pasiva con tráfico es a ritmo de socket— y quien lo
	// lee es el colector, una vez por latido. La contención real es la del `entry()` que ya se paga, y un
	// atómico aquí obligaría a sacar el campo del Snapshot coherente que este tipo promete.
	droppedPassive uint64
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

// CountPassiveDrop suma UN entrante descartado en la puerta por PERFIL PASIVO de la sesión id (Plan 046 ·
// Ola 2 · T2.3). Lo llama el listener, en el hilo de whatsmeow, desde el MISMO sitio donde incrementa su
// acumulado por sesión (whatsmeow.bracketObserver.countPassiveDrop): dos puntos de incremento separados
// serían dos cosas que alguien tiene que acordarse de tocar a la vez, y una de las dos se quedaría atrás.
//
// Crea la entrada si no existe, igual que el resto de setters. En el camino real ya existe —onMessage llama
// a MarkInbound ANTES de filtrar, para TODO mensaje— así que esta rama no cambia qué sesiones enumera
// SessionIDs(); se documenta porque un cableado que llamara aquí sin haber reportado nunca salud haría
// aparecer la sesión en GET /v1/health con el resto de campos a cero.
//
// Nil-safe (los tests sin registro cableado operan sin ramificaciones). No lleva PII: es una cardinalidad.
func (r *Registry) CountPassiveDrop(id string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	sh := r.entry(id)
	sh.droppedPassive++
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
		SocketState:             sh.state,
		DegradedReason:          sh.reason,
		DEKLoadDuration:         sh.dekDuration,
		LastInboundAt:           sh.lastInbound,
		DroppedByPassiveProfile: sh.droppedPassive,
	}
	r.mu.RUnlock()
	return snap, true
}

// SessionIDs devuelve, ordenados, los session_id con entrada de salud (los que han reportado algo del ciclo
// de escucha). Lo consume el colector (T7) para enumerar las sesiones vivas al armar GET /v1/health y el
// snapshot de subsistemas del bundle de diagnóstico (T8). Nil-safe (devuelve nil sin registro).
func (r *Registry) SessionIDs() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	ids := make([]string, 0, len(r.sessions))
	for id := range r.sessions {
		ids = append(ids, id)
	}
	r.mu.RUnlock()
	sort.Strings(ids)
	return ids
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
	// CountPassiveDrop suma un entrante descartado en la puerta por PERFIL PASIVO (Plan 046 · T2.3).
	//
	// Está en ESTA interfaz —y no en un puerto nuevo— porque es exactamente el mismo tipo de dato que
	// MarkInbound: una señal por-sesión que nace en el hilo de whatsmeow, no lleva PII y sale por las mismas
	// dos bocas (el heartbeat y GET /v1/health). Un puerto propio habría duplicado el cableado del factory
	// del sessionmgr para transportar un entero.
	CountPassiveDrop()
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
func (b boundReporter) CountPassiveDrop()                  { b.reg.CountPassiveDrop(b.id) }
