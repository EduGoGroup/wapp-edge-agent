package app

import "context"

// outbox.go — Puerto del OUTBOX DURABLE del Edge (Plan 027 Ola 3 · T2, cierra H2 / ADR-0003).
//
// Cuando el stream CloudLink está caído, los eventos del Edge hacia la nube (mensajes ENTRANTES y ACUSES)
// se PERSISTEN en vez de descartarse, y se reenvían al reconectar. El puerto vive en app (hexagonal); la
// implementación real (internal/adapters/outbox) lo respalda con la BD única SQLite del Edge (ADR-0018),
// y un doble en memoria lo simula en los tests del transporte. Sin broker (ADR-0003): cola en SQLite.

// OutboxItem es UN evento pendiente de reenvío a la nube. Payload es el EdgeToCloud serializado tal cual
// se enviará (los campos sensibles de un entrante ya viajan SELLADOS a la pública de cifrado de la nube
// cuando el Edge está enrolado, Plan 011 §6.3): el outbox nunca ve credenciales ni llaves privadas.
type OutboxItem struct {
	// DedupeKey es la clave de idempotencia LOCAL: el EdgeToCloud.command_id (UUID ya presente en el
	// payload). El encolado es idempotente por ella; el reenvío usa los MISMOS bytes (mismo command_id)
	// para que la nube deduplique. Nunca se genera un command_id nuevo en el reintento.
	DedupeKey string
	// SessionID es el discriminador de sesión (outbox única con discriminador): el drenaje preserva el
	// orden relativo POR SESIÓN.
	SessionID string
	// Kind clasifica el evento ("incoming" | "receipt"): diagnóstico/métrica, no altera el reenvío.
	Kind string
	// Payload es el EdgeToCloud serializado (proto). El transporte lo deserializa para reenviarlo.
	Payload []byte
}

// Kinds de OutboxItem (etiquetas estables para diagnóstico/métrica).
const (
	OutboxKindIncoming = "incoming"
	OutboxKindReceipt  = "receipt"
)

// Outbox es la cola durable de eventos edge->cloud. Todas las operaciones son seguras para uso concurrente
// (el Edge encola desde los listeners por sesión y drena desde el loop del stream).
type Outbox interface {
	// Enqueue persiste un evento pendiente. Es IDEMPOTENTE por DedupeKey (encolar dos veces el mismo
	// command_id no duplica). Aplica la política de tamaño (drop-oldest al llegar al tope) y el TTL.
	Enqueue(ctx context.Context, item OutboxItem) error
	// Drain devuelve hasta max eventos pendientes en ORDEN de encolado (FIFO global y por sesión), sin
	// borrarlos: el llamante los reenvía y confirma con Delete uno a uno.
	Drain(ctx context.Context, max int) ([]OutboxItem, error)
	// Delete quita un evento ya reenviado con éxito (por DedupeKey). Idempotente.
	Delete(ctx context.Context, dedupeKey string) error
	// Fail marca un intento de reenvío fallido (incrementa el contador de intentos; diagnóstico). No borra.
	Fail(ctx context.Context, dedupeKey string) error
	// PendingSessions devuelve los session_id que tienen al menos un evento pendiente. Sirve para SEMBRAR
	// el guard de orden del transporte al arrancar (un evento nuevo no debe adelantar a un backlog previo).
	PendingSessions(ctx context.Context) ([]string, error)
}
