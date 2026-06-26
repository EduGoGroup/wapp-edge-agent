package app

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// --- fakes (sin red ni teléfono) ---

// fakeSender registra lo que recibió SendText y permite forzar un error.
type fakeSender struct {
	called  bool
	gotDEK  []byte
	gotTo   string
	gotText string
	sendErr error
}

func (f *fakeSender) SendText(_ context.Context, dek []byte, to, text string) error {
	f.called = true
	// Copia defensiva: el caso de uso borra (zero) la DEK al salir; sin copia veríamos ceros.
	f.gotDEK = append([]byte(nil), dek...)
	f.gotTo = to
	f.gotText = text
	return f.sendErr
}

// custodyWith construye un fakeCustody (definido en pair_test.go) ya sembrado con una DEK.
func custodyWith(dek []byte) *fakeCustody {
	return &fakeCustody{stored: append([]byte(nil), dek...)}
}

// --- tests ---

// TestSend_Success: con DEK custodiada, Run carga la DEK y la pasa intacta al Sender junto al
// destino y el texto.
func TestSend_Success(t *testing.T) {
	dek := bytes.Repeat([]byte{0xAB}, DEKSize)
	cust := custodyWith(dek)
	snd := &fakeSender{}

	if err := NewSend(cust, snd).Run(context.Background(), "+54 911-1234", "hola mundo"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !snd.called {
		t.Fatal("el Sender no fue invocado")
	}
	if !bytes.Equal(snd.gotDEK, dek) {
		t.Fatalf("la DEK no llegó intacta al Sender: %v", snd.gotDEK)
	}
	if snd.gotTo != "+54 911-1234" || snd.gotText != "hola mundo" {
		t.Fatalf("destino/texto inesperados: to=%q text=%q", snd.gotTo, snd.gotText)
	}
}

// TestSend_NoDEK: si la custodia no tiene DEK, Run falla y NO invoca al Sender.
func TestSend_NoDEK(t *testing.T) {
	snd := &fakeSender{}
	err := NewSend(&fakeCustody{}, snd).Run(context.Background(), "549111", "hola")
	if err == nil {
		t.Fatal("se esperaba error al no haber DEK custodiada")
	}
	if snd.called {
		t.Fatal("el Sender NO debía invocarse sin DEK")
	}
}

// TestSend_EmptyRecipient: un destino vacío falla con ErrEmptyRecipient, antes de tocar la custodia.
func TestSend_EmptyRecipient(t *testing.T) {
	cust := custodyWith(bytes.Repeat([]byte{1}, DEKSize))
	snd := &fakeSender{}
	err := NewSend(cust, snd).Run(context.Background(), "   ", "hola")
	if !errors.Is(err, ErrEmptyRecipient) {
		t.Fatalf("error = %v, quería ErrEmptyRecipient", err)
	}
	if snd.called {
		t.Fatal("el Sender NO debía invocarse con destino vacío")
	}
}

// TestSend_EmptyText: un texto vacío falla con ErrEmptyText, sin invocar al Sender.
func TestSend_EmptyText(t *testing.T) {
	cust := custodyWith(bytes.Repeat([]byte{1}, DEKSize))
	snd := &fakeSender{}
	err := NewSend(cust, snd).Run(context.Background(), "549111", "  \t ")
	if !errors.Is(err, ErrEmptyText) {
		t.Fatalf("error = %v, quería ErrEmptyText", err)
	}
	if snd.called {
		t.Fatal("el Sender NO debía invocarse con texto vacío")
	}
}

// TestSend_SenderError: un fallo del Sender se propaga envuelto.
func TestSend_SenderError(t *testing.T) {
	sentinel := errors.New("socket caído")
	cust := custodyWith(bytes.Repeat([]byte{1}, DEKSize))
	snd := &fakeSender{sendErr: sentinel}
	err := NewSend(cust, snd).Run(context.Background(), "549111", "hola")
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, quería envolver %v", err, sentinel)
	}
}
