// collector.go — COLECTOR DE SALUD del Edge (Plan 031 T7). Arma un Report por sesión combinando el
// snapshot de runtime del Registry (T6: prueba de vida del socket, motivo de degradación, duración de la
// última carga de la DEK, edad del último entrante) con las señales de ALCANCE DAEMON que T6 no puebla:
// profundidad del outbox (ADR-0003), estado del circuito del clasificador (Plan 029), versión del binario
// (ldflags) y uptime del proceso. El Report es NEUTRAL (sin proto): el adapter CloudLink lo mapea a
// SessionHealth para el heartbeat y el plano de control lo serializa a JSON en GET /v1/health.
//
// Frontera zero-knowledge (ADR-0007): el Report SOLO lleva METADATOS operativos (estados, edades,
// duraciones, profundidades, versiones). JAMÁS la DEK, credenciales ni contenido de mensajes.
package health

import (
	"context"
	"strings"
	"time"
)

// Report es la foto de salud DERIVADA de una sesión, lista para el wire (edades/duraciones ya calculadas).
// Un único origen de los campos que exige el contrato SessionHealth (Plan 031 T1); dos serializadores
// (proto en el heartbeat, JSON en /v1/health) lo consumen.
type Report struct {
	// SocketState es la etiqueta de prueba de vida del socket ("connected"/"connecting"/"degraded"/"dead"/"").
	SocketState string
	// DegradedReason es el motivo corto cuando degradado/muerto; "" si sano.
	DegradedReason string
	// LastInboundAgeS es la edad en segundos del último evento entrante (0 si aún ninguno).
	LastInboundAgeS int64
	// DEKLoadDurationMs es la duración en ms de la última carga de la DEK (0 si ninguna completó).
	DEKLoadDurationMs int64
	// IntentCircuit es el estado del circuito del clasificador ("closed"/"open"/"half_open"); "" si 029 off.
	IntentCircuit string
	// OutboxDepth es la profundidad del outbox de la sesión (eventos pendientes de drenar).
	OutboxDepth int64
	// BinaryVersion es la build del Edge (ldflags); traza de flota y base del auto-update (Plan 032).
	BinaryVersion string
	// DaemonUptimeS es el uptime del daemon en segundos (mismo valor para todas las sesiones del proceso).
	DaemonUptimeS int64
}

// OutboxDepther es el puerto MÍNIMO que el colector necesita del outbox: la profundidad por sesión. Se
// declara aquí (no se importa app.Outbox) para que el paquete health no dependa de la capa de aplicación
// ni del adapter. Lo satisface *outbox.Store. nil ⇒ el colector reporta profundidad 0.
type OutboxDepther interface {
	Depth(ctx context.Context, sessionID string) (int64, error)
}

// Collector arma Reports de salud. Uno por daemon, compartido: lee el Registry (T6) y las señales daemon.
// Todas sus dependencias externas son tolerantes a nil (outbox/circuit ausentes ⇒ campos en su cero).
type Collector struct {
	reg       *Registry
	outbox    OutboxDepther
	circuit   func() string // estado del circuito del clasificador; nil ⇒ "" (029 off)
	version   string
	startedAt time.Time
	now       func() time.Time
}

// CollectorOption ajusta el colector (reloj de test).
type CollectorOption func(*Collector)

// WithClock inyecta el reloj (tests deterministas de edades/uptime).
func WithClock(now func() time.Time) CollectorOption {
	return func(c *Collector) {
		if now != nil {
			c.now = now
		}
	}
}

// NewCollector construye el colector. reg puede ser nil (Collect devolverá ok=false para toda sesión);
// outbox nil ⇒ profundidad 0; circuit nil ⇒ IntentCircuit "". version es la build del binario (ldflags) y
// startedAt marca el arranque del proceso (base del uptime).
func NewCollector(reg *Registry, outbox OutboxDepther, circuit func() string, version string, startedAt time.Time, opts ...CollectorOption) *Collector {
	c := &Collector{
		reg:       reg,
		outbox:    outbox,
		circuit:   circuit,
		version:   version,
		startedAt: startedAt,
		now:       time.Now,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Collect arma el Report de la sesión sessionID. ok=false si la sesión no tiene entrada de salud en el
// Registry (sin prueba de vida real ⇒ no se reporta salud, por diseño T6/T7). Deriva la edad del último
// entrante y la duración de la DEK del snapshot; puebla las señales daemon (outbox/circuito/versión/uptime)
// SIEMPRE que la sesión exista.
func (c *Collector) Collect(ctx context.Context, sessionID string) (Report, bool) {
	if c == nil {
		return Report{}, false
	}
	snap, ok := c.reg.Snapshot(sessionID)
	if !ok {
		return Report{}, false
	}
	now := c.now()
	r := Report{
		SocketState:       string(snap.SocketState),
		DegradedReason:    snap.DegradedReason,
		DEKLoadDurationMs: snap.DEKLoadDuration.Milliseconds(),
		IntentCircuit:     c.intentCircuit(),
		BinaryVersion:     c.version,
		DaemonUptimeS:     c.uptimeS(now),
	}
	if !snap.LastInboundAt.IsZero() {
		if age := now.Sub(snap.LastInboundAt); age > 0 {
			r.LastInboundAgeS = int64(age.Seconds())
		}
	}
	if c.outbox != nil {
		if depth, err := c.outbox.Depth(ctx, sessionID); err == nil {
			r.OutboxDepth = depth
		}
	}
	return r, true
}

// Reports arma el Report de TODAS las sesiones vivas (las que tienen entrada en el Registry). Lo consume
// GET /v1/health y el snapshot de subsistemas del bundle de diagnóstico (T8). Nunca es nil (mapa vacío si
// no hay sesiones).
func (c *Collector) Reports(ctx context.Context) map[string]Report {
	out := make(map[string]Report)
	if c == nil {
		return out
	}
	for _, id := range c.reg.SessionIDs() {
		if r, ok := c.Collect(ctx, id); ok {
			out[id] = r
		}
	}
	return out
}

// DaemonUptimeS devuelve el uptime del daemon en segundos (para el bloque daemon de /v1/health).
func (c *Collector) DaemonUptimeS() int64 {
	if c == nil {
		return 0
	}
	return c.uptimeS(c.now())
}

// Version devuelve la build del binario (para el bloque daemon de /v1/health).
func (c *Collector) Version() string {
	if c == nil {
		return ""
	}
	return c.version
}

// intentCircuit lee el estado del circuito y lo NORMALIZA al contrato del wire ("half-open" → "half_open").
// El endpoint /v1/intent/status conserva su forma con guion; el heartbeat/bundle usan la forma con guion
// bajo del contrato SessionHealth (ADR-0023). "" si 029 no está activo.
func (c *Collector) intentCircuit() string {
	if c.circuit == nil {
		return ""
	}
	return strings.ReplaceAll(c.circuit(), "-", "_")
}

// uptimeS calcula el uptime en segundos (0 si startedAt es cero o el reloj retrocedió).
func (c *Collector) uptimeS(now time.Time) int64 {
	if c.startedAt.IsZero() {
		return 0
	}
	if d := now.Sub(c.startedAt); d > 0 {
		return int64(d.Seconds())
	}
	return 0
}
