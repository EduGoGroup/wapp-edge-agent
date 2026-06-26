package app

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// --- fakes (sin red ni teléfono) ---

type fakeConnector struct {
	gotDEK []byte
	start  func(ctx context.Context, dek []byte) (<-chan PairingSignal, error)
}

func (f *fakeConnector) StartConnection(ctx context.Context, dek []byte) (<-chan PairingSignal, error) {
	f.gotDEK = append([]byte(nil), dek...)
	return f.start(ctx, dek)
}

type fakeQRSink struct {
	codes []string
	err   error
}

func (f *fakeQRSink) ShowQR(code string) error {
	f.codes = append(f.codes, code)
	return f.err
}

type fakeCustody struct {
	stored   []byte
	storeErr error
}

func (f *fakeCustody) Store(dek []byte) error {
	if f.storeErr != nil {
		return f.storeErr
	}
	f.stored = append([]byte(nil), dek...)
	return nil
}

func (f *fakeCustody) Load() ([]byte, error) {
	if f.stored == nil {
		return nil, errors.New("sin DEK")
	}
	return append([]byte(nil), f.stored...), nil
}

func (f *fakeCustody) Exists() bool { return f.stored != nil }

// chanOf construye un canal de señales ya pobladas y CERRADO (fin de pairing).
func chanOf(sigs ...PairingSignal) <-chan PairingSignal {
	ch := make(chan PairingSignal, len(sigs))
	for _, s := range sigs {
		ch <- s
	}
	close(ch)
	return ch
}

// --- tests ---

// TestPair_Success: el conector emite QR y luego Connected. Verifica que el QR llega al QRSink, que
// se genera una DEK de 32 B y que al Connected esa MISMA DEK queda sellada en la custodia (round-trip).
func TestPair_Success(t *testing.T) {
	const wantJID = "123456@s.whatsapp.net"
	fc := &fakeConnector{start: func(_ context.Context, _ []byte) (<-chan PairingSignal, error) {
		return chanOf(
			PairingSignal{Type: PairingSignalQR, QR: "2@code-uno"},
			PairingSignal{Type: PairingSignalConnected, WaJID: wantJID},
		), nil
	}}
	qr := &fakeQRSink{}
	cust := &fakeCustody{}

	res, err := NewPair(fc, qr, cust).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.WaJID != wantJID {
		t.Fatalf("WaJID = %q, quería %q", res.WaJID, wantJID)
	}
	if len(qr.codes) != 1 || qr.codes[0] != "2@code-uno" {
		t.Fatalf("el QR no llegó al QRSink: %v", qr.codes)
	}
	if len(cust.stored) != DEKSize {
		t.Fatalf("DEK sellada de %d bytes, esperaba %d", len(cust.stored), DEKSize)
	}
	if !bytes.Equal(cust.stored, fc.gotDEK) {
		t.Fatal("la DEK sellada en custodia difiere de la entregada al conector")
	}
	loaded, err := cust.Load()
	if err != nil || len(loaded) != DEKSize {
		t.Fatalf("round-trip de custodia falló: err=%v len=%d", err, len(loaded))
	}
}

// TestPair_MultipleQR: si llegan varios QR (refresh) antes del Connected, todos pasan por el sink.
func TestPair_MultipleQR(t *testing.T) {
	fc := &fakeConnector{start: func(_ context.Context, _ []byte) (<-chan PairingSignal, error) {
		return chanOf(
			PairingSignal{Type: PairingSignalQR, QR: "a"},
			PairingSignal{Type: PairingSignalQR, QR: "b"},
			PairingSignal{Type: PairingSignalConnected, WaJID: "x@s"},
		), nil
	}}
	qr := &fakeQRSink{}
	if _, err := NewPair(fc, qr, &fakeCustody{}).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(qr.codes) != 2 {
		t.Fatalf("se esperaban 2 QR en el sink, hubo %d", len(qr.codes))
	}
}

// TestPair_ConnectorStartError: si StartConnection falla, Run propaga el error sin sellar nada.
func TestPair_ConnectorStartError(t *testing.T) {
	fc := &fakeConnector{start: func(_ context.Context, _ []byte) (<-chan PairingSignal, error) {
		return nil, errors.New("boom conexión")
	}}
	cust := &fakeCustody{}
	_, err := NewPair(fc, &fakeQRSink{}, cust).Run(context.Background())
	if err == nil {
		t.Fatal("se esperaba error de StartConnection")
	}
	if cust.Exists() {
		t.Fatal("no debía sellarse la DEK ante un fallo de conexión")
	}
}

// TestPair_ChannelClosedWithoutConnected: el conector cierra el canal sin Connected ni Error.
func TestPair_ChannelClosedWithoutConnected(t *testing.T) {
	fc := &fakeConnector{start: func(_ context.Context, _ []byte) (<-chan PairingSignal, error) {
		return chanOf(PairingSignal{Type: PairingSignalQR, QR: "a"}), nil
	}}
	cust := &fakeCustody{}
	_, err := NewPair(fc, &fakeQRSink{}, cust).Run(context.Background())
	if !errors.Is(err, ErrPairClosed) {
		t.Fatalf("error = %v, quería ErrPairClosed", err)
	}
	if cust.Exists() {
		t.Fatal("no debía sellarse la DEK si el canal se cerró sin Connected")
	}
}

// TestPair_SignalError: una señal de error del conector aborta el pairing y se envuelve el motivo.
func TestPair_SignalError(t *testing.T) {
	sentinel := errors.New("pairing rechazado")
	fc := &fakeConnector{start: func(_ context.Context, _ []byte) (<-chan PairingSignal, error) {
		return chanOf(PairingSignal{Type: PairingSignalError, Err: sentinel}), nil
	}}
	_, err := NewPair(fc, &fakeQRSink{}, &fakeCustody{}).Run(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, quería envolver %v", err, sentinel)
	}
}

// TestPair_SignalErrorNil: error del conector sin detalle -> error genérico, sin pánico.
func TestPair_SignalErrorNil(t *testing.T) {
	fc := &fakeConnector{start: func(_ context.Context, _ []byte) (<-chan PairingSignal, error) {
		return chanOf(PairingSignal{Type: PairingSignalError}), nil
	}}
	if _, err := NewPair(fc, &fakeQRSink{}, &fakeCustody{}).Run(context.Background()); err == nil {
		t.Fatal("se esperaba error genérico del conector")
	}
}

// TestPair_QRSinkError: si mostrar el QR falla, Run aborta y no sella la DEK.
func TestPair_QRSinkError(t *testing.T) {
	fc := &fakeConnector{start: func(_ context.Context, _ []byte) (<-chan PairingSignal, error) {
		return chanOf(PairingSignal{Type: PairingSignalQR, QR: "a"}), nil
	}}
	cust := &fakeCustody{}
	_, err := NewPair(fc, &fakeQRSink{err: errors.New("terminal rota")}, cust).Run(context.Background())
	if err == nil {
		t.Fatal("se esperaba error del QRSink")
	}
	if cust.Exists() {
		t.Fatal("no debía sellarse la DEK si el QR no pudo mostrarse")
	}
}

// TestPair_CustodyStoreError: si el sellado de la DEK falla, Run devuelve error.
func TestPair_CustodyStoreError(t *testing.T) {
	fc := &fakeConnector{start: func(_ context.Context, _ []byte) (<-chan PairingSignal, error) {
		return chanOf(PairingSignal{Type: PairingSignalConnected, WaJID: "x@s"}), nil
	}}
	_, err := NewPair(fc, &fakeQRSink{}, &fakeCustody{storeErr: errors.New("disco lleno")}).Run(context.Background())
	if err == nil {
		t.Fatal("se esperaba error de custodia")
	}
}

// TestPair_Timeout: si nunca llega Connected (canal abierto), Run vence con ErrPairTimeout.
func TestPair_Timeout(t *testing.T) {
	fc := &fakeConnector{start: func(_ context.Context, _ []byte) (<-chan PairingSignal, error) {
		ch := make(chan PairingSignal, 1)
		ch <- PairingSignal{Type: PairingSignalQR, QR: "a"} // un QR y luego silencio (sin cerrar).
		return ch, nil
	}}
	cust := &fakeCustody{}
	_, err := NewPair(fc, &fakeQRSink{}, cust, WithTimeout(30*time.Millisecond)).Run(context.Background())
	if !errors.Is(err, ErrPairTimeout) {
		t.Fatalf("error = %v, quería ErrPairTimeout", err)
	}
	if cust.Exists() {
		t.Fatal("un timeout no debe sellar la DEK")
	}
}

// TestPair_DEKGenError: un fallo al generar la DEK aborta antes de conectar.
func TestPair_DEKGenError(t *testing.T) {
	called := false
	fc := &fakeConnector{start: func(_ context.Context, _ []byte) (<-chan PairingSignal, error) {
		called = true
		return chanOf(), nil
	}}
	p := NewPair(fc, &fakeQRSink{}, &fakeCustody{},
		withDEKSource(func() ([]byte, error) { return nil, errors.New("sin entropía") }))
	if _, err := p.Run(context.Background()); err == nil {
		t.Fatal("se esperaba error de generación de DEK")
	}
	if called {
		t.Fatal("no debía intentarse conectar si la DEK no se generó")
	}
}

// TestPair_ParentCtxCancelled: cancelar el ctx padre corta el pairing sin timeout-deadline.
func TestPair_ParentCtxCancelled(t *testing.T) {
	fc := &fakeConnector{start: func(_ context.Context, _ []byte) (<-chan PairingSignal, error) {
		return make(chan PairingSignal), nil // nunca emite ni cierra.
	}}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	_, err := NewPair(fc, &fakeQRSink{}, &fakeCustody{}, WithTimeout(5*time.Second)).Run(ctx)
	if err == nil || errors.Is(err, ErrPairTimeout) {
		t.Fatalf("error = %v, quería cancelación (no timeout)", err)
	}
}

// TestWithTimeout_IgnoraNoPositivo: WithTimeout(<=0) conserva el default.
func TestWithTimeout_IgnoraNoPositivo(t *testing.T) {
	p := NewPair(&fakeConnector{}, &fakeQRSink{}, &fakeCustody{}, WithTimeout(0))
	if p.timeout != DefaultPairTimeout {
		t.Fatalf("timeout = %v, quería el default %v", p.timeout, DefaultPairTimeout)
	}
}
