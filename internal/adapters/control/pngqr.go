package control

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	qrcode "github.com/skip2/go-qrcode"
)

// qrPNGSize es el lado (px) del PNG del QR. ~256px da una imagen nítida y escaneable por un
// teléfono dentro de un <img> de la web UI sin inflar el data-URL.
const qrPNGSize = 256

// PNGDataURL renderiza el string CRUDO del QR de whatsmeow a un data-URL PNG
// (`data:image/png;base64,...`) listo para `<img src>` en la web UI (decisión §10.B). El render es
// server-side con skip2/go-qrcode (Go puro, SIN CGO; mantiene el binario estático de la Pieza 01).
// Nivel de recuperación medio (M): tolera ~15% de daño sin agrandar de más el código.
//
// Contrato de seguridad: solo transforma el artefacto público de whatsmeow (el QR). NUNCA toca la
// DEK ni el store; el data-URL resultante no transporta material sensible (ADR-0014/0007).
func PNGDataURL(code string) (string, error) {
	if code == "" {
		return "", errors.New("control: código QR vacío")
	}
	png, err := qrcode.Encode(code, qrcode.Medium, qrPNGSize)
	if err != nil {
		return "", fmt.Errorf("control: render del QR a PNG: %w", err)
	}
	var b strings.Builder
	b.WriteString("data:image/png;base64,")
	b.WriteString(base64.StdEncoding.EncodeToString(png))
	return b.String(), nil
}
