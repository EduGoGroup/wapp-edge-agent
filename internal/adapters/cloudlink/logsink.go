// Package cloudlink contiene el adaptador del puerto app.InboundSink hacia la nube (CloudLink).
//
// En la Fase 1 será el cliente gRPC bidi-stream con mTLS (pieza 02). En el SPIKE es un STUB de LOG
// (design §8): cada mensaje entrante se escribe como log estructurado, lo justo para VER que la
// recepción 24/7 funciona end-to-end (T5.5) sin tocar la nube. Sin red, sin broker (ADR-0003).
package cloudlink

import (
	"context"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-shared/logger"
)

// LogSink implementa app.InboundSink escribiendo cada evento entrante en el logger estructurado del
// ecosistema (wapp-shared/logger). Es el stub del spike: no reenvía a ninguna nube.
type LogSink struct {
	log logger.Logger
}

var _ app.InboundSink = (*LogSink)(nil)

// NewLogSink construye el sink de log sobre el logger dado.
func NewLogSink(log logger.Logger) *LogSink {
	return &LogSink{log: log}
}

// Deliver registra el InboundEvent como log estructurado. Devuelve siempre nil (escribir un log no
// falla en este stub). Los campos permiten verificar la recepción al enviar un WhatsApp al número.
func (s *LogSink) Deliver(_ context.Context, evt domain.InboundEvent) error {
	s.log.Info("mensaje entrante recibido (InboundEvent)",
		"message_id", evt.MessageID,
		"chat", evt.Chat,
		"sender", evt.Sender,
		"push_name", evt.PushName,
		"type", evt.Type,
		"timestamp", evt.Timestamp,
		"is_from_me", evt.IsFromMe,
		"is_group", evt.IsGroup,
		"text", evt.Text,
	)
	return nil
}

// LogMux es el multiplexor CloudLink de DIAGNÓSTICO: la variante sin red del Adapter para cuando no hay
// endpoint configurado (dev / primer arranque). Satisface el mismo contrato que el Adapter real frente
// al Session Manager (Register/Unregister/SinkFor) pero NO envía a ninguna nube: cada sesión obtiene un
// LogSink etiquetado con su session_id y Register/Unregister son no-ops. Mantiene el daemon multi-sesión
// funcionando (listeners arriba, entrantes a log) sin CloudLink, igual que el LogSink puro hacía en el
// camino single-sesión del spike.
type LogMux struct {
	log logger.Logger
}

// NewLogMux construye el multiplexor de diagnóstico sobre el logger dado.
func NewLogMux(log logger.Logger) *LogMux {
	return &LogMux{log: log}
}

// Register es un no-op: el LogMux no gatea lease ni mantiene estado por sesión (solo loguea).
func (m *LogMux) Register(sessionID string, _ func(ctx context.Context, to, text string) error, _ func() bool) {
	m.log.Info("CloudLink (LogMux): sesión registrada para diagnóstico (sin reenvío a la nube)", "session_id", sessionID)
}

// Unregister es un no-op simétrico a Register.
func (m *LogMux) Unregister(sessionID string) {
	m.log.Info("CloudLink (LogMux): sesión removida del diagnóstico", "session_id", sessionID)
}

// SinkFor devuelve un LogSink que arrastra el session_id en cada línea (diagnóstico por sesión).
func (m *LogMux) SinkFor(sessionID string) app.InboundSink {
	return NewLogSink(m.log.With("session_id", sessionID))
}
