// Package control contiene el MÍNIMO del plano de control del Edge para el spike (design §7): NO
// es el contrato completo de ADR-0015 (systray + web local con todo /v1, fuera de alcance). Aquí
// solo vive el sink que pinta el QR de emparejamiento en una terminal.
package control

import (
	"errors"
	"io"

	qrterminal "github.com/mdp/qrterminal/v3"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// TerminalQRSink implementa app.QRSink renderizando el código QR como arte ASCII en un io.Writer
// (por defecto os.Stdout en el binario). El writer se inyecta por construcción para testear con un
// buffer en memoria sin tocar la terminal real.
type TerminalQRSink struct {
	w io.Writer
}

var _ app.QRSink = (*TerminalQRSink)(nil)

// NewTerminalQRSink construye un sink que escribe el QR en w.
func NewTerminalQRSink(w io.Writer) *TerminalQRSink {
	return &TerminalQRSink{w: w}
}

// ShowQR pinta el código QR crudo en el writer. Devuelve error si el código viene vacío (qrterminal
// no puede codificar una cadena vacía). Usa nivel de redundancia M y bloques completos (escaneable
// por un teléfono); evita la detección de sixel/terminal para ser determinista en tests.
func (s *TerminalQRSink) ShowQR(code string) error {
	if code == "" {
		return errors.New("control: código QR vacío")
	}
	qrterminal.GenerateWithConfig(code, qrterminal.Config{
		Level:     qrterminal.M,
		Writer:    s.w,
		BlackChar: qrterminal.BLACK,
		WhiteChar: qrterminal.WHITE,
		QuietZone: 2,
	})
	return nil
}
