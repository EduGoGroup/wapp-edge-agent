package app

// Pair es el caso de uso de emparejamiento por QR local (RF-1/RF-2, design §6/§7).
//
// Orquesta el flujo end-to-end DESACOPLADO por interfaces para ser testeable sin teléfono:
//   - genera la DEK (32 B, AES-256) en el lado del cliente (zero-knowledge, ADR-0007);
//   - arranca la conexión vía el puerto Connector (el adaptador whatsmeow real construye el store
//     CIFRADO con esa DEK, abre el canal de QR y conecta);
//   - cada QR que llega se entrega al puerto QRSink (el control lo pinta en terminal);
//   - al recibir el Connected post-escaneo, sella la DEK en el puerto KeyCustody.
//
// Invariante de seguridad: la DEK en claro vive SOLO en RAM durante el pairing; se borra del slice
// (zero) al salir, pase lo que pase. NUNCA se loguea. La nube nunca la ve (no hay relay).

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"
)

// DEKSize es el tamaño en bytes de la DEK (AES-256). Coincide con envelope.DEKSize y
// keycustody.KeySize; se define local para no acoplar el caso de uso a esos paquetes.
const DEKSize = 32

// DefaultPairTimeout acota cuánto espera un pairing antes de declararse timeout. Debe cubrir TODO
// el flujo manual: lectura humana del QR + abrir WhatsApp + escanear + la reconexión post-pairing
// de whatsmeow que dispara el Connected (el que sella la DEK). 90s da margen cómodo.
const DefaultPairTimeout = 90 * time.Second

// PairingSignalType clasifica las señales que el Connector emite durante un pairing. Es la
// abstracción MÍNIMA sobre whatsmeow: el caso de uso reacciona a estas señales sin conocer
// whatsmeow, lo que lo hace testeable con un fake (sin conexión real).
type PairingSignalType string

const (
	// PairingSignalQR transporta un nuevo código QR. El campo QR lleva el string CRUDO del código
	// (no un PNG): el control lo renderiza en terminal con qrterminal.
	PairingSignalQR PairingSignalType = "qr"
	// PairingSignalConnected indica que el teléfono escaneó y la sesión quedó autenticada
	// (whatsmeow *events.Connected con Store.ID != nil). Lleva el JID en el campo WaJID.
	PairingSignalConnected PairingSignalType = "connected"
	// PairingSignalError indica un fallo irrecuperable del pairing (campo Err).
	PairingSignalError PairingSignalType = "error"
)

// PairingSignal es una señal del Connector hacia el caso de uso. El canal devuelto por
// StartConnection las emite en orden y se cierra al terminar (éxito, error o cancelación).
type PairingSignal struct {
	Type PairingSignalType
	// QR es el string crudo del código QR (solo en PairingSignalQR).
	QR string
	// WaJID es el JID de la sesión recién emparejada (solo en PairingSignalConnected).
	WaJID string
	// Err es el error del pairing (solo en PairingSignalError).
	Err error
}

// Connector ABSTRAE whatsmeow para el caso de uso de pairing. La implementación real
// (internal/adapters/whatsmeow) construye el store CIFRADO con la DEK dada, crea el cliente, abre
// el canal de QR y conecta; un fake en los tests emite señales sin red.
//
// Contrato de seguridad: la DEK (32 bytes) entra SOLO aquí (para cifrar el store recién creado) y
// NUNCA se persiste ni se loguea. La implementación NO registra handlers de mensajes entrantes ni
// Listen: solo lo necesario para el pairing (QR + Connected).
type Connector interface {
	// StartConnection arranca un pairing nuevo y devuelve un canal por el que fluyen las señales
	// (QR…, luego Connected | Error) y que se cierra al terminar. El ctx controla la cancelación.
	StartConnection(ctx context.Context, dek []byte) (<-chan PairingSignal, error)
}

// QRSink recibe el código QR para mostrarlo al usuario (en el spike: terminal). Abstrae el control
// para que el caso de uso no dependa de cómo se pinta el QR.
type QRSink interface {
	// ShowQR muestra el código QR crudo. Devuelve error si no pudo renderizarlo.
	ShowQR(code string) error
}

// Errores del caso de uso (sin material sensible).
var (
	// ErrPairTimeout: el teléfono no completó el escaneo dentro del timeout.
	ErrPairTimeout = errors.New("pairing: tiempo de espera agotado sin emparejar")
	// ErrPairClosed: el conector cerró el canal sin Connected ni Error explícito.
	ErrPairClosed = errors.New("pairing: la conexión se cerró sin completar el emparejamiento")
)

// PairResult resume el desenlace de un pairing exitoso.
type PairResult struct {
	// WaJID es el JID de la sesión emparejada.
	WaJID string
}

// Pair es el caso de uso. Sus dependencias son puertos (interfaces) para inyectar fakes en tests.
type Pair struct {
	connector Connector
	qr        QRSink
	custody   KeyCustody
	timeout   time.Duration
	newDEK    func() ([]byte, error)
}

// PairOption configura un Pair en su construcción.
type PairOption func(*Pair)

// WithTimeout fija el timeout total del pairing (ignora valores <= 0).
func WithTimeout(d time.Duration) PairOption {
	return func(p *Pair) {
		if d > 0 {
			p.timeout = d
		}
	}
}

// withDEKSource inyecta la fuente de la DEK (tests: DEK determinista o fallo de generación).
func withDEKSource(f func() ([]byte, error)) PairOption {
	return func(p *Pair) { p.newDEK = f }
}

// NewPair construye el caso de uso con los puertos dados y opciones.
func NewPair(connector Connector, qr QRSink, custody KeyCustody, opts ...PairOption) *Pair {
	p := &Pair{
		connector: connector,
		qr:        qr,
		custody:   custody,
		timeout:   DefaultPairTimeout,
		newDEK:    generateDEK,
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Run ejecuta un pairing completo y bloquea hasta éxito, error o timeout. Garantiza que la DEK en
// claro se borra de RAM al salir (defer zero) y que el ctx del conector se cancela (libera el
// socket efímero).
func (p *Pair) Run(ctx context.Context) (PairResult, error) {
	dek, err := p.newDEK()
	if err != nil {
		return PairResult{}, fmt.Errorf("pairing: generar DEK: %w", err)
	}
	defer zeroBytes(dek)

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	signals, err := p.connector.StartConnection(ctx, dek)
	if err != nil {
		return PairResult{}, fmt.Errorf("pairing: iniciar conexión: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return PairResult{}, ErrPairTimeout
			}
			return PairResult{}, ctx.Err()
		case sig, ok := <-signals:
			if !ok {
				return PairResult{}, ErrPairClosed
			}
			switch sig.Type {
			case PairingSignalQR:
				if err := p.qr.ShowQR(sig.QR); err != nil {
					return PairResult{}, fmt.Errorf("pairing: mostrar QR: %w", err)
				}
			case PairingSignalConnected:
				// Pairing OK: sellar la DEK en custodia ANTES de retornar (la DEK la borra el defer).
				if err := p.custody.Store(dek); err != nil {
					return PairResult{}, fmt.Errorf("pairing: sellar DEK en custodia: %w", err)
				}
				return PairResult{WaJID: sig.WaJID}, nil
			case PairingSignalError:
				if sig.Err != nil {
					return PairResult{}, fmt.Errorf("pairing: %w", sig.Err)
				}
				return PairResult{}, errors.New("pairing: fallo del conector")
			}
		}
	}
}

// generateDEK produce una DEK de 32 bytes con CSPRNG (crypto/rand).
func generateDEK() ([]byte, error) {
	dek := make([]byte, DEKSize)
	if _, err := rand.Read(dek); err != nil {
		return nil, err
	}
	return dek, nil
}

// zeroBytes pone a cero un slice (borrado de material sensible de RAM).
func zeroBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
