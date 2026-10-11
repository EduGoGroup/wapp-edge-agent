package whatsmeow

// sin_transporte.go — LA PUERTA DE ENTRANTES, SIN CLIENTE Y SIN SOCKET (ADR-0049).
//
// QUÉ ES. El transporte falso de WhatsApp (internal/adapters/whatsappfalso) no abre socket, no carga device
// y no toca el almacén cifrado, pero lo que entra por él tiene que recorrer EL MISMO código que un entrante
// real: las guardas de la puerta, el meta, la doble marca, el cronómetro y el encolado. Ese código es el
// `Listener` de este paquete, que no necesita cliente para existir (solo `Register` lo pide). Este fichero
// es lo ÚNICO que el adaptador real añade para el falso: tres métodos que publican ese mismo `Listener` sin
// `Connect()`, y nada que reescriba una guarda.
//
// POR QUÉ VIVE AQUÍ Y NO EN EL PAQUETE DEL FALSO. `handleEvent`, `listenerOpts` y el puntero del Listener
// vivo son privados, y deben seguir siéndolo. Exportarlos para que otro paquete monte la puerta sería
// ensanchar el adaptador real por un instrumento. Lo que se borra junto se escribe junto.
//
// 🔴 NADA DE ESTO SE ALCANZA CON LA PALANCA BAJA. El único que llama a `NewListenGatewaySinTransporte` es
// el transporte falso, y el arranque solo lo elige con WAPP_AGENT_WHATSAPP_FALSO=true.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/EduGoGroup/wapp-shared/logger"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// errSinTransporte es lo que devuelve `Listen` sobre un gateway sin transporte: no hay device que cargar.
// Quien lo vea llamó al método del transporte real sobre la puerta del falso; es un error de cableado.
var errSinTransporte = errors.New("whatsapp: este gateway no tiene transporte (lo construyó el WhatsApp falso): " +
	"se escucha con EscucharSinTransporte, no con Listen")

// NewListenGatewaySinTransporte construye un gateway con sus setters, su Correlator y su inyector de
// siempre, pero SIN forma de cargar un device: no hay socket que abrir. Solo sirve con
// EscucharSinTransporte.
func NewListenGatewaySinTransporte(log logger.Logger) *ListenGateway {
	return newListenGateway(func(context.Context, []byte) (*store.Device, error) {
		return nil, errSinTransporte
	}, log)
}

// EscucharSinTransporte publica el Listener de la puerta —construido EXACTAMENTE como en serve(), con las
// mismas opciones y los mismos manejadores— y BLOQUEA hasta que el ctx se cancele. No hay cliente, ni
// `Register`, ni `Connect()`, ni presencia: la única diferencia con serve() es el transporte.
//
// El `events.Connected` se entrega a mano por la puerta porque en el camino real lo emite el socket al
// conectar, y de él cuelgan el estado «conectado» de la sesión y su prueba de vida en el registro de salud.
func (g *ListenGateway) EscucharSinTransporte(ctx context.Context) error {
	listener := NewListener(g.log, g.listenerOpts()...)
	listener.SetHealthReporter(g.reporter)
	listener.onReceipt = g.onReceipt
	listener.onLoggedOutHook = g.onLoggedOut

	g.setLiveListener(listener)
	defer g.setLiveListener(nil)

	listener.handleEvent(ctx, &events.Connected{})
	g.log.Warn("escucha SIN TRANSPORTE iniciada: esta sesión NO habla con WhatsApp (WhatsApp falso, ADR-0049)")

	<-ctx.Done()
	g.log.Info("escucha sin transporte detenida: cancelación recibida")
	return nil
}

// HayEscuchaSinTransporte dice si la puerta está publicada ahora mismo. El falso lo consulta antes de
// «enviar», igual que el envío real exige un cliente vivo.
func (g *ListenGateway) HayEscuchaSinTransporte() bool { return g.liveListener() != nil }

// RecordarEnvioSinTransporte anota la correlación command_id ↔ identificador del mensaje de un envío del
// falso, en el MISMO Correlator que usa el envío real, para que su acuse suba etiquetado.
func (g *ListenGateway) RecordarEnvioSinTransporte(commandID, messageID string, at time.Time) {
	if commandID != "" && g.correlator != nil {
		g.correlator.Remember(commandID, messageID, at)
	}
}

// AcusarEntregaSinTransporte entrega por la puerta un acuse `delivered` del mensaje dado, como lo haría el
// socket al recibir un events.Receipt. Recorre el mapeo y el manejador de acuses de siempre. Sin escucha
// publicada devuelve un error que envuelve ErrSinEscuchaViva.
func (g *ListenGateway) AcusarEntregaSinTransporte(ctx context.Context, messageID string, at time.Time) error {
	l := g.liveListener()
	if l == nil {
		return fmt.Errorf("%w: no hay Listener al que entregar el acuse", ErrSinEscuchaViva)
	}
	l.handleEvent(ctx, &events.Receipt{
		MessageIDs: []types.MessageID{messageID},
		Type:       types.ReceiptTypeDelivered,
		Timestamp:  at,
	})
	return nil
}
