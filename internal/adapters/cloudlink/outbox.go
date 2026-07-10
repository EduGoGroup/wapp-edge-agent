package cloudlink

// outbox.go — Integración del OUTBOX DURABLE en el Adapter CloudLink (Plan 027 Ola 3 · T2, cierra H2 /
// ADR-0003). El puerto app.Outbox lo respalda internal/adapters/outbox (SQLite sobre la BD única). Aquí se
// conecta al camino de SALIDA (entrantes/acuses):
//
//   - forward: intenta enviar por el stream vivo; si no se puede (sin stream o Send con error), ENCOLA en
//     vez de descartar. Preserva el orden POR SESIÓN vía un guard en memoria (una sesión con backlog encola
//     también sus eventos nuevos, en vez de mandarlos en vivo por delante del backlog).
//   - drainLoop: al conectar (y con un ticker mientras hay stream) drena el outbox en orden de seq,
//     reenviando cada evento por el stream y borrándolo al confirmarse. La nube deduplica por
//     command_id/wa_message_id (el proto NO cambia).
//
// El retry cadence de FONDO lo da la reconexión de Run (backoff + jitter): si el stream muere a mitad de
// un drenaje, este se detiene y el siguiente Connect lo reanuda desde donde quedó (los eventos siguen en la
// BD). Los latidos/loggedout NO usan el outbox (son liveness, no eventos de negocio).

import (
	"context"

	"github.com/EduGoGroup/wapp-cloudlink/client"
	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"google.golang.org/protobuf/proto"
	"time"
)

// forward envía msg por el stream vivo o, con outbox durable configurado, lo ENCOLA si no se puede (H2).
// Sin outbox conserva el best-effort previo (se descarta si no hay stream o el envío falla). diagID es un
// identificador de diagnóstico (p.ej. wa_message_id) para los logs; puede ir vacío.
func (a *Adapter) forward(msg *cloudlinkv1.EdgeToCloud, sessionID, kind, diagID string) {
	if a.outbox == nil {
		if cl := a.currentClient(); cl != nil {
			if err := cl.Send(msg); err != nil {
				a.log.Warn("CloudLink: fallo al reenviar evento (sin outbox, se descarta)",
					"kind", kind, "session_id", sessionID, "error", err)
			}
			return
		}
		a.log.Warn("CloudLink desconectado: evento no reenviado (sin outbox, se descarta)",
			"kind", kind, "session_id", sessionID)
		return
	}

	// Con outbox: si la sesión ya tiene backlog, encola para NO adelantar al backlog (orden por sesión).
	if a.isPending(sessionID) {
		a.enqueue(msg, sessionID, kind, diagID)
		return
	}
	if cl := a.currentClient(); cl != nil {
		if err := cl.Send(msg); err == nil {
			return // enviado en vivo
		}
	}
	// Sin stream o envío fallido: marca la sesión pendiente y encola (se drena al reconectar).
	a.markPending(sessionID)
	a.enqueue(msg, sessionID, kind, diagID)
}

// enqueue serializa msg y lo persiste en el outbox por su command_id (idempotente). Nunca genera un
// command_id nuevo: reusa el del msg para que la nube deduplique el reenvío.
func (a *Adapter) enqueue(msg *cloudlinkv1.EdgeToCloud, sessionID, kind, diagID string) {
	raw, err := proto.Marshal(msg)
	if err != nil {
		a.log.Error("outbox: no se pudo serializar el evento (se pierde)",
			"kind", kind, "session_id", sessionID, "wa_message_id", diagID, "error", err)
		return
	}
	if err := a.outbox.Enqueue(context.Background(), app.OutboxItem{
		DedupeKey: msg.GetCommandId(),
		SessionID: sessionID,
		Kind:      kind,
		Payload:   raw,
	}); err != nil {
		a.log.Error("outbox: no se pudo encolar el evento (se pierde)",
			"kind", kind, "session_id", sessionID, "wa_message_id", diagID, "error", err)
		return
	}
	a.log.Info("outbox: evento encolado (stream no disponible); se reenviará al reconectar",
		"kind", kind, "session_id", sessionID, "wa_message_id", diagID)
}

// isPending indica si la sesión tiene backlog en el outbox (guard de orden en memoria).
func (a *Adapter) isPending(sessionID string) bool {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	return a.pending[sessionID]
}

// markPending marca una sesión como con-backlog (sus eventos nuevos irán al outbox hasta drenarse).
func (a *Adapter) markPending(sessionID string) {
	a.pendingMu.Lock()
	a.pending[sessionID] = true
	a.pendingMu.Unlock()
}

// recomputePending REEMPLAZA el guard en memoria por la verdad de la BD (sesiones con eventos pendientes).
// Se llama al arrancar (sembrado tras un reinicio con backlog) y tras cada pasada de drenaje. No-op sin
// outbox.
func (a *Adapter) recomputePending(ctx context.Context) {
	if a.outbox == nil {
		return
	}
	ids, err := a.outbox.PendingSessions(ctx)
	if err != nil {
		a.log.Warn("outbox: no se pudo recalcular las sesiones pendientes", "error", err)
		return
	}
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	a.pendingMu.Lock()
	a.pending = set
	a.pendingMu.Unlock()
}

// drainLoop drena el outbox al conectar y luego periódicamente mientras el stream siga vivo (ctx ligado al
// stream). No-op sin outbox.
func (a *Adapter) drainLoop(ctx context.Context, cl *client.Client) {
	if a.outbox == nil {
		return
	}
	a.drainOnce(ctx, cl) // inmediato: reenvía el backlog acumulado durante la caída
	t := time.NewTicker(a.outboxDrainInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.drainOnce(ctx, cl)
		}
	}
}

// drainOnce reenvía TODO el outbox pendiente en orden de seq por lotes, borrando cada evento al confirmarse
// el envío. Se detiene ante un envío fallido (el stream está cayendo: Run reconectará y reanudará) o un
// borrado fallido (para no reenviar en bucle). Al terminar recalcula el guard de orden.
func (a *Adapter) drainOnce(ctx context.Context, cl *client.Client) {
	if a.outbox == nil {
		return
	}
	for {
		if ctx.Err() != nil {
			return
		}
		items, err := a.outbox.Drain(ctx, outboxDrainBatch)
		if err != nil {
			a.log.Warn("outbox: no se pudo leer pendientes para drenar", "error", err)
			return
		}
		if len(items) == 0 {
			break
		}
		for _, it := range items {
			if ctx.Err() != nil {
				return
			}
			var msg cloudlinkv1.EdgeToCloud
			if err := proto.Unmarshal(it.Payload, &msg); err != nil {
				a.log.Error("outbox: payload ilegible; descartando para no atascar el drenaje",
					"session_id", it.SessionID, "error", err)
				_ = a.outbox.Delete(ctx, it.DedupeKey)
				continue
			}
			if err := cl.Send(&msg); err != nil {
				a.log.Warn("outbox: reenvío falló; se reintentará al reconectar",
					"session_id", it.SessionID, "kind", it.Kind, "error", err)
				_ = a.outbox.Fail(ctx, it.DedupeKey)
				a.recomputePending(ctx)
				return
			}
			if err := a.outbox.Delete(ctx, it.DedupeKey); err != nil {
				a.log.Error("outbox: no se pudo borrar el evento reenviado; detengo el drenaje para no reenviar en bucle",
					"session_id", it.SessionID, "error", err)
				return
			}
		}
	}
	a.recomputePending(ctx)
}
