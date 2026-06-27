package control

import (
	"context"
	"errors"
	"sync"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// PairStatus es el estado observable de un emparejamiento desde el plano de control.
type PairStatus string

const (
	// PairPending: el pairing está en curso; hay (o habrá) un QR vigente para escanear.
	PairPending PairStatus = "pending"
	// PairSuccess: el teléfono escaneó y la sesión quedó sellada (la DEK ya se custodió DENTRO del
	// núcleo; jamás cruza el contrato /v1).
	PairSuccess PairStatus = "success"
	// PairError: el pairing falló o expiró (Snapshot.Err lleva el mensaje, sin material sensible).
	PairError PairStatus = "error"
)

// MemoryQRSink implementa app.QRSink guardando en memoria el QR VIGENTE y el estado del pairing, en
// vez de pintarlo en terminal (es el análogo de TerminalQRSink para el plano de control). El caso de
// uso app.Pair llama ShowQR en cada rotación del QR (connector.go: GetQRChannel emite varios QR antes
// del success/error); el handler de poll lee el QR vigente vía Snapshot y lo renderiza a PNG.
//
// Es concurrente-seguro: ShowQR corre en la goroutine de pairing mientras el handler HTTP lee por
// Snapshot. Finish fija el estado TERMINAL (success/error) cuando app.Pair.Run retorna.
//
// Invariante (ADR-0014/0007): este sink solo ve el QR (artefacto público de whatsmeow) y un mensaje
// de error saneado. NUNCA la DEK ni el store; el sellado de la DEK ocurre dentro de app.Pair.
type MemoryQRSink struct {
	mu     sync.RWMutex
	qr     string
	status PairStatus
	errMsg string

	firstOnce sync.Once
	firstCh   chan struct{} // se cierra al llegar el PRIMER QR (o al Finish prematuro).
}

var _ app.QRSink = (*MemoryQRSink)(nil)

// NewMemoryQRSink crea un sink en estado pending sin QR aún.
func NewMemoryQRSink() *MemoryQRSink {
	return &MemoryQRSink{status: PairPending, firstCh: make(chan struct{})}
}

// ShowQR registra el QR crudo vigente (rotación). Devuelve error si el código viene vacío. Mantiene
// el estado pending: solo Finish lo lleva a terminal.
func (s *MemoryQRSink) ShowQR(code string) error {
	if code == "" {
		return errors.New("control: código QR vacío")
	}
	s.mu.Lock()
	s.qr = code
	s.mu.Unlock()
	s.firstOnce.Do(func() { close(s.firstCh) })
	return nil
}

// Finish fija el estado TERMINAL del pairing: success si err es nil, error con el mensaje de err en
// caso contrario. Idempotente en la práctica (lo invoca una sola vez el gestor al terminar Run).
// También libera a quien espere el primer QR (WaitFirstQR) si el pairing falló antes de emitir uno.
func (s *MemoryQRSink) Finish(err error) {
	s.mu.Lock()
	if err != nil {
		s.status = PairError
		s.errMsg = err.Error()
	} else {
		s.status = PairSuccess
		s.errMsg = ""
	}
	s.mu.Unlock()
	s.firstOnce.Do(func() { close(s.firstCh) })
}

// WaitFirstQR bloquea hasta que llegue el primer QR (o el pairing termine, o ctx se cancele). Lo usa
// el handler de POST /pair para devolver el primer QR si está listo en un margen breve, o pending si
// la rotación aún no produjo ninguno (evita devolver un qr vacío de forma sistemática por la carrera
// entre arrancar la goroutine y el primer ShowQR).
func (s *MemoryQRSink) WaitFirstQR(ctx context.Context) {
	select {
	case <-s.firstCh:
	case <-ctx.Done():
	}
}

// PairSnapshot es una foto consistente del sink para construir la respuesta del contrato.
type PairSnapshot struct {
	Status PairStatus
	QR     string // QR crudo vigente ("" si aún no llegó ninguno).
	Err    string // mensaje de error (solo en PairError).
}

// Snapshot devuelve una copia atómica del estado actual (QR vigente + estado + error).
func (s *MemoryQRSink) Snapshot() PairSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return PairSnapshot{Status: s.status, QR: s.qr, Err: s.errMsg}
}
