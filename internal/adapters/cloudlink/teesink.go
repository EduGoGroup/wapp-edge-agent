package cloudlink

import (
	"context"
	"errors"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// TeeSink entrega cada InboundEvent a VARIOS sinks en orden (patrón tee). Permite que el conducto
// primario (Adapter CloudLink) y un sink de diagnóstico (LogSink) reciban el mismo evento sin tocar la
// firma de app.Listen, que sigue tomando un único InboundSink.
//
// Política de error: intenta TODOS los sinks aunque alguno falle (un fallo de reenvío a la nube no
// debe impedir el log de diagnóstico) y agrega los errores con errors.Join. app.Listen registra el
// error y sigue escuchando: una entrega fallida nunca tumba el socket.
type TeeSink struct {
	sinks []app.InboundSink
}

var _ app.InboundSink = (*TeeSink)(nil)

// NewTeeSink construye un tee sobre los sinks dados, en orden de entrega.
func NewTeeSink(sinks ...app.InboundSink) *TeeSink {
	return &TeeSink{sinks: sinks}
}

// Deliver entrega el evento a cada sink, agregando los errores.
func (t *TeeSink) Deliver(ctx context.Context, evt domain.InboundEvent) error {
	var errs []error
	for _, s := range t.sinks {
		if err := s.Deliver(ctx, evt); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
