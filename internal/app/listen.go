package app

// Listen es el caso de uso de ESCUCHA 24/7 (always-on) del Edge (RF-5/RF-6, design §5).
//
// A diferencia de Send (ciclo efímero connect-send-disconnect), Listen mantiene el socket de
// WhatsApp VIVO de forma continua: arranca la conexión, registra el listener que enruta los mensajes
// entrantes hacia el sink y BLOQUEA hasta que el ctx se cancele (Ctrl-C / SIGINT en el daemon).
//
// Orquesta el flujo DESACOPLADO por interfaces para ser testeable sin teléfono ni red:
//   - recupera la DEK del puerto KeyCustody (la misma sellada en el pairing, zero-knowledge);
//   - delega la conexión+escucha al puerto ListenGateway, que en producción descifra el store con la
//     DEK, carga el device pareado, conecta el cliente whatsmeow always-on y registra el listener.
//
// Invariante de seguridad (ADR-0007): la DEK en claro vive SOLO en RAM y, en modo always-on,
// MIENTRAS el socket viva. ListenGateway.Listen bloquea hasta cancelación, así que la DEK permanece
// en scope durante toda la sesión y se borra (zero) al salir, pase lo que pase. NUNCA se loguea.

import (
	"context"
	"fmt"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// InboundSink es el puerto de SALIDA de los mensajes entrantes ya descifrados (domain.InboundEvent).
// En la Fase 1 lo implementa el cliente CloudLink (reenvío a la nube por gRPC bidi-stream); en el
// spike lo implementa un adaptador de LOG (CloudLink stub, design §8). Abstrae el destino para que
// ni el caso de uso ni el listener conozcan a dónde va el evento.
type InboundSink interface {
	// Deliver entrega un evento entrante al destino. Devuelve error si no pudo entregarlo (el
	// listener lo registra y sigue escuchando: una entrega fallida no debe tumbar el socket).
	Deliver(ctx context.Context, evt domain.InboundEvent) error
}

// ListenGateway ABSTRAE la conexión always-on + escucha sobre whatsmeow para el caso de uso. La
// implementación real (internal/adapters/whatsmeow) construye el store CIFRADO con la DEK, carga el
// device pareado, conecta el cliente, registra el listener que enruta los *events.Message hacia el
// sink y mantiene el socket VIVO hasta que el ctx se cancele. Un fake en los tests emite eventos al
// sink y respeta la cancelación sin red.
//
// Contrato de seguridad: la DEK (32 bytes) entra SOLO para descifrar el store y NUNCA se persiste ni
// se loguea.
type ListenGateway interface {
	// Listen conecta, registra el listener (eventos -> sink) y BLOQUEA manteniendo el socket vivo
	// hasta que ctx se cancele. Devuelve nil al cancelarse limpio, o error si la conexión inicial
	// (o la carga del device) falla.
	Listen(ctx context.Context, dek []byte, sink InboundSink) error
}

// Listen es el caso de uso. Sus dependencias son puertos (interfaces) para inyectar fakes en tests.
type Listen struct {
	custody KeyCustody
	gateway ListenGateway
	sink    InboundSink
}

// NewListen construye el caso de uso con los puertos dados.
func NewListen(custody KeyCustody, gateway ListenGateway, sink InboundSink) *Listen {
	return &Listen{custody: custody, gateway: gateway, sink: sink}
}

// Run recupera la DEK de custodia y arranca la escucha always-on, bloqueando hasta que el ctx se
// cancele (o falle la conexión). Garantiza que la DEK en claro se borra de RAM al salir (defer zero):
// como Listen bloquea toda la sesión, la DEK permanece viva mientras el socket viva (ADR-0007).
func (l *Listen) Run(ctx context.Context) error {
	dek, err := l.custody.Load()
	if err != nil {
		return fmt.Errorf("listen: cargar DEK de custodia: %w", err)
	}
	defer zeroBytes(dek)

	if err := l.gateway.Listen(ctx, dek, l.sink); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	return nil
}
