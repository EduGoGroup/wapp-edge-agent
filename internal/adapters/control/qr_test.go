package control

import (
	"bytes"
	"strings"
	"testing"
)

// TestTerminalQRSink_WritesNonEmpty: dado un string de QR y un buffer, ShowQR escribe algo no vacío
// y no entra en pánico. No se valida el arte ASCII, solo que produjo salida.
func TestTerminalQRSink_WritesNonEmpty(t *testing.T) {
	var buf bytes.Buffer
	sink := NewTerminalQRSink(&buf)
	if err := sink.ShowQR("2@abc,def,ghi=="); err != nil {
		t.Fatalf("ShowQR: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("ShowQR no escribió nada en el writer")
	}
}

// TestTerminalQRSink_EmptyCode: un código vacío devuelve error y no escribe nada (qrterminal no
// puede codificar la cadena vacía).
func TestTerminalQRSink_EmptyCode(t *testing.T) {
	var buf bytes.Buffer
	sink := NewTerminalQRSink(&buf)
	if err := sink.ShowQR(""); err == nil {
		t.Fatal("se esperaba error con código QR vacío")
	}
	if buf.Len() != 0 {
		t.Fatal("no debía escribir nada con código vacío")
	}
}

// TestTerminalQRSink_DistintosCodigos: códigos distintos producen salidas distintas (el QR refleja
// el contenido), confirmando que no se ignora el input.
func TestTerminalQRSink_DistintosCodigos(t *testing.T) {
	render := func(code string) string {
		var b strings.Builder
		if err := NewTerminalQRSink(&b).ShowQR(code); err != nil {
			t.Fatalf("ShowQR(%q): %v", code, err)
		}
		return b.String()
	}
	if render("2@uno") == render("2@dos-mucho-mas-largo") {
		t.Fatal("dos códigos distintos produjeron el mismo render")
	}
}
