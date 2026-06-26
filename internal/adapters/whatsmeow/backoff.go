package whatsmeow

import "time"

// Backoff es una política de reconexión con retroceso EXPONENCIAL y tope (RF-6, design §5). Es PURA
// (sin red ni tiempo real: solo calcula el siguiente delay dado el intento) para poder probarla con
// tablas. La usa el Listener al observar *events.Disconnected.
//
// DECISIÓN DE RECONEXIÓN (T5.3): el *whatsmeow.Client YA hace auto-reconnect propio
// (EnableAutoReconnect=true por defecto, con su propio retroceso interno), así que en el spike NO
// reimplementamos el bucle de reconexión: dejamos que whatsmeow reconecte solo (lo más simple y
// correcto). Esta política vive igualmente porque (a) el diseño exige una política de backoff PROPIA
// y testeada, y (b) es la base lista para conducir una reconexión MANUAL si en Fase 1 desactivamos
// el auto-reconnect (p.ej. para coordinar con el lease de CloudLink). El Listener la avanza en cada
// Disconnected y la resetea en cada Connected, dejando trazada la cadencia aunque el socket lo
// reconecte whatsmeow.
type Backoff struct {
	// Base es el delay del primer reintento (intento 0). Debe ser > 0.
	Base time.Duration
	// Max es el tope: el delay nunca lo supera.
	Max time.Duration

	// attempt es el número de reintentos ya contabilizados (0 = aún ninguno).
	attempt int
}

// DefaultBackoff devuelve la política por defecto del spike: 1s, 2s, 4s, …, tope 60s.
func DefaultBackoff() *Backoff {
	return &Backoff{Base: 1 * time.Second, Max: 60 * time.Second}
}

// Next devuelve el delay del reintento ACTUAL y avanza el contador. La secuencia es
// Base*2^0, Base*2^1, … saturada en Max. Es monótona no decreciente y nunca supera Max.
func (b *Backoff) Next() time.Duration {
	d := b.delayFor(b.attempt)
	b.attempt++
	return d
}

// Reset vuelve la política al estado inicial (tras una reconexión exitosa: *events.Connected).
func (b *Backoff) Reset() {
	b.attempt = 0
}

// Attempt expone cuántos reintentos se han contabilizado (para logs/observabilidad).
func (b *Backoff) Attempt() int {
	return b.attempt
}

// delayFor calcula Base*2^n saturado en Max, evitando overflow al duplicar: en cuanto el valor
// alcanza o supera Max, devuelve Max.
func (b *Backoff) delayFor(n int) time.Duration {
	d := b.Base
	for i := 0; i < n; i++ {
		if d >= b.Max {
			return b.Max
		}
		d *= 2
	}
	if d > b.Max {
		return b.Max
	}
	return d
}
