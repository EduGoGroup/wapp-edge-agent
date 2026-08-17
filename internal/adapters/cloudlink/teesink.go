package cloudlink

import (
	"context"
	"errors"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// TeeSink entrega cada InboundEvent a VARIOS sinks en orden (patrón tee). Permite que el conducto
// primario (Adapter CloudLink) y un sink de diagnóstico (LogSink) reciban el mismo evento allí donde solo
// cabe un único app.InboundSink.
//
// ⚠️ HOY NO TIENE NINGÚN LLAMANTE (constatado en el Plan 051 Ola 3 · T3.8). Se escribió para el camino
// inline del listener, que T3.0 retiró; el despachador de la Ola 3 recibe UN sink, el crudo de la sesión.
// Se conserva porque el punto donde encajaría sigue existiendo (despachador.Deps.Sink), pero no confundas
// «está aquí» con «está en uso»: si lo cableas, hazlo a sabiendas.
//
// Política de error: intenta TODOS los sinks aunque alguno falle (un fallo de reenvío a la nube no
// debe impedir el log de diagnóstico) y agrega los errores con errors.Join. El despachador registra el
// error y NO sella la fila: la reintenta en el poll siguiente en vez de darla por entregada.
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
