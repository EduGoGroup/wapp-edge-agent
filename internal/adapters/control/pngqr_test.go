package control

import (
	"encoding/base64"
	"image/png"
	"strings"
	"testing"
)

const dataURLPrefix = "data:image/png;base64,"

// TestPNGDataURL_ValidPNG: un QR crudo produce un data-URL con el prefijo correcto cuyo payload
// base64 decodifica a un PNG VÁLIDO (parseable por image/png). Es la garantía de que la web UI puede
// pintarlo en un <img src>.
func TestPNGDataURL_ValidPNG(t *testing.T) {
	url, err := PNGDataURL("2@abc,def,ghi==")
	if err != nil {
		t.Fatalf("PNGDataURL: %v", err)
	}
	if !strings.HasPrefix(url, dataURLPrefix) {
		t.Fatalf("data-URL: prefijo inesperado: %q", url[:min(len(url), 40)])
	}

	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(url, dataURLPrefix))
	if err != nil {
		t.Fatalf("base64 inválido: %v", err)
	}
	img, err := png.Decode(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("el payload no es un PNG válido: %v", err)
	}
	if img.Bounds().Dx() == 0 || img.Bounds().Dy() == 0 {
		t.Fatalf("PNG con dimensiones nulas: %v", img.Bounds())
	}
}

// TestPNGDataURL_Empty: un código vacío devuelve error y no un data-URL.
func TestPNGDataURL_Empty(t *testing.T) {
	if _, err := PNGDataURL(""); err == nil {
		t.Fatal("se esperaba error con código QR vacío")
	}
}

// TestPNGDataURL_DistintosCodigos: códigos distintos producen data-URLs distintos (el PNG refleja el
// contenido, no se ignora el input).
func TestPNGDataURL_DistintosCodigos(t *testing.T) {
	a, err := PNGDataURL("2@uno")
	if err != nil {
		t.Fatalf("PNGDataURL(uno): %v", err)
	}
	b, err := PNGDataURL("2@dos-distinto-mas-largo")
	if err != nil {
		t.Fatalf("PNGDataURL(dos): %v", err)
	}
	if a == b {
		t.Fatal("dos códigos distintos produjeron el mismo data-URL")
	}
}
