package app

// Send es el caso de uso de envío de un mensaje de texto a un destino (RF-4, design §3 fila 5).
//
// Orquesta el flujo DESACOPLADO por interfaces para ser testeable sin teléfono ni red:
//   - valida las entradas (destino y texto no vacíos);
//   - recupera la DEK del puerto KeyCustody (la misma sellada en el pairing, zero-knowledge);
//   - delega el envío al puerto Sender, que en producción descifra el store con la DEK, carga el
//     device pareado, conecta el cliente whatsmeow y despacha el texto.
//
// Invariante de seguridad: la DEK en claro vive SOLO en RAM durante el envío; se borra del slice
// (zero) al salir, pase lo que pase. NUNCA se loguea. La nube nunca la ve.
//
// El Edge es DESPACHADOR (ADR-0005): este caso de uso solo despacha el texto que se le pide; no
// arma payloads ni consulta endpoints de negocio.

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Sender ABSTRAE el envío real por whatsmeow para el caso de uso. La implementación real
// (internal/adapters/whatsmeow) construye el store CIFRADO con la DEK dada, carga el device
// pareado, conecta un cliente efímero y despacha el texto; un fake en los tests verifica el
// cableado sin red.
//
// Contrato de seguridad: la DEK (32 bytes) entra SOLO para descifrar el store y NUNCA se persiste
// ni se loguea. El destino se normaliza a JID en el adaptador.
type Sender interface {
	// SendText envía text al destino to (número crudo o JID), descifrando el store con dek.
	SendText(ctx context.Context, dek []byte, to, text string) error
}

// Errores de validación del caso de uso (sin material sensible).
var (
	// ErrEmptyRecipient: el destino llegó vacío.
	ErrEmptyRecipient = errors.New("send: destino vacío")
	// ErrEmptyText: el texto a enviar llegó vacío.
	ErrEmptyText = errors.New("send: texto vacío")
)

// Send es el caso de uso. Sus dependencias son puertos (interfaces) para inyectar fakes en tests.
type Send struct {
	custody KeyCustody
	sender  Sender
}

// NewSend construye el caso de uso con los puertos dados.
func NewSend(custody KeyCustody, sender Sender) *Send {
	return &Send{custody: custody, sender: sender}
}

// Run valida las entradas, recupera la DEK de custodia y despacha el texto al destino. Garantiza
// que la DEK en claro se borra de RAM al salir (defer zero). Valida ANTES de tocar la custodia
// para no recuperar material de llave si la orden es inválida.
func (s *Send) Run(ctx context.Context, to, text string) error {
	if strings.TrimSpace(to) == "" {
		return ErrEmptyRecipient
	}
	if strings.TrimSpace(text) == "" {
		return ErrEmptyText
	}

	dek, err := s.custody.Load()
	if err != nil {
		return fmt.Errorf("send: cargar DEK de custodia: %w", err)
	}
	defer zeroBytes(dek)

	if err := s.sender.SendText(ctx, dek, to, text); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	return nil
}
