package whatsmeow

import (
	"sync"
	"time"
)

const (
	// defaultCorrelatorMax acota cuántos envíos sin acusar se retienen a la vez (tope de memoria):
	// llegado el tope, se evicta el más antiguo (FIFO). Holgado para picos de envío.
	defaultCorrelatorMax = 4096
	// defaultCorrelatorTTL acota cuánto se retiene un envío esperando su acuse: los receipts llegan en
	// segundos/minutos, no indefinidamente (§10.E). Vencido el TTL, el acuse subirá como estado crudo.
	defaultCorrelatorTTL = 30 * time.Minute
)

// correlationEntry guarda el MessageID (SendResponse.ID) del envío y cuándo se registró (para el TTL).
type correlationEntry struct {
	msgID     string    // SendResponse.ID: la clave que traerá events.Receipt.MessageIDs.
	at        time.Time // instante de registro (para el vencimiento por TTL).
	timestamp time.Time // SendResponse.Timestamp del envío (informativo, no se usa para el TTL).
}

// Correlator ata el command_id de un envío con el MessageID que WhatsApp devuelve en el SendResponse,
// para que —al llegar un events.Receipt con esos MessageIDs— el acuse se pueda etiquetar con el
// command_id ORIGINAL del envío (Plan 013 §8/§10.E). Es ACOTADO (tope de tamaño + expiración por TTL)
// y seguro para uso concurrente. Hay UNO por ListenGateway, es decir uno por sesión: la correlación es
// naturalmente POR sesión (ADR-0008).
//
// NOTA DE CAMPO (§10.E): el diseño habla de "ServerID", pero en la versión pineada de whatsmeow
// SendResponse.ServerID (types.MessageServerID = int) SOLO viene para newsletters; para un mensaje
// normal el identificador es SendResponse.ID (types.MessageID = string), que es EXACTAMENTE lo que
// trae events.Receipt.MessageIDs. Por eso correlacionamos por MessageID, no por ServerID.
type Correlator struct {
	mu    sync.Mutex
	max   int
	ttl   time.Duration
	now   func() time.Time            // inyectable en tests (reloj falso).
	byCmd map[string]correlationEntry // command_id -> entry.
	byMsg map[string]string           // msgID -> command_id (índice inverso para Lookup O(1)).
	order []string                    // command_ids en orden de inserción (evicción FIFO por tope).
}

// NewCorrelator construye un Correlator con el tope y el TTL dados; valores <= 0 caen a los defaults.
func NewCorrelator(max int, ttl time.Duration) *Correlator {
	if max <= 0 {
		max = defaultCorrelatorMax
	}
	if ttl <= 0 {
		ttl = defaultCorrelatorTTL
	}
	return &Correlator{
		max:   max,
		ttl:   ttl,
		now:   time.Now,
		byCmd: make(map[string]correlationEntry),
		byMsg: make(map[string]string),
	}
}

// Remember registra un envío: command_id -> {msgID, timestamp}. Ignora command_id o msgID vacíos (el
// camino de envío sin command_id no correlaciona). Antes de insertar poda las entradas vencidas y, si
// se supera el tope, evicta la más antigua.
func (c *Correlator) Remember(commandID, msgID string, timestamp time.Time) {
	if commandID == "" || msgID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneExpiredLocked()
	if old, ok := c.byCmd[commandID]; ok {
		// Re-registro del mismo command_id: limpia el índice inverso previo (conserva su posición en
		// order; pruneExpired escanea por completo, así que la posición no afecta al TTL).
		delete(c.byMsg, old.msgID)
	} else {
		c.order = append(c.order, commandID)
	}
	c.byCmd[commandID] = correlationEntry{msgID: msgID, at: c.now(), timestamp: timestamp}
	c.byMsg[msgID] = commandID
	c.evictOverflowLocked()
}

// Lookup devuelve el command_id del PRIMER messageID conocido (no vencido) del slice. ok=false si
// ninguno correlaciona (vencido o desconocido): el acuse igual sube como estado crudo por message_ids
// (§10.E). Un events.Receipt puede acusar varios IDs; basta con que uno matchee.
func (c *Correlator) Lookup(messageIDs []string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneExpiredLocked()
	for _, id := range messageIDs {
		if cmd, ok := c.byMsg[id]; ok {
			return cmd, true
		}
	}
	return "", false
}

// Len devuelve cuántos envíos se retienen (para observabilidad/tests). Poda lo vencido antes de contar.
func (c *Correlator) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneExpiredLocked()
	return len(c.byCmd)
}

// pruneExpiredLocked elimina las entradas cuya edad supera el TTL y compacta `order` (quitando
// referencias muertas y duplicados). Escanea order completo (acotado por el tope), así que el orden de
// las entradas NO afecta a la corrección del vencimiento. Debe llamarse con el lock tomado.
func (c *Correlator) pruneExpiredLocked() {
	if c.ttl <= 0 || len(c.order) == 0 {
		return
	}
	cutoff := c.now().Add(-c.ttl)
	kept := c.order[:0] // filtrado in-place: solo se escribe en posiciones ya leídas.
	seen := make(map[string]bool, len(c.order))
	for _, cmd := range c.order {
		if seen[cmd] {
			continue // duplicado obsoleto en order.
		}
		e, ok := c.byCmd[cmd]
		if !ok {
			continue // ya borrado por evicción/re-registro.
		}
		if !e.at.After(cutoff) { // at <= cutoff => vencido.
			delete(c.byMsg, e.msgID)
			delete(c.byCmd, cmd)
			continue
		}
		seen[cmd] = true
		kept = append(kept, cmd)
	}
	c.order = kept
}

// evictOverflowLocked aplica el tope de tamaño evictando por el frente (FIFO, más antiguo primero)
// hasta volver bajo el máximo. Debe llamarse con el lock tomado.
func (c *Correlator) evictOverflowLocked() {
	for len(c.byCmd) > c.max && len(c.order) > 0 {
		cmd := c.order[0]
		c.order = c.order[1:]
		if e, ok := c.byCmd[cmd]; ok {
			delete(c.byMsg, e.msgID)
			delete(c.byCmd, cmd)
		}
	}
}
