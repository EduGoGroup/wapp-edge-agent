package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestMemoryQRSink_ShowQRUpdatesCurrent: ShowQR fija el QR vigente y mantiene pending; una segunda
// rotación lo reemplaza.
func TestMemoryQRSink_ShowQRUpdatesCurrent(t *testing.T) {
	s := NewMemoryQRSink()
	if snap := s.Snapshot(); snap.Status != PairPending || snap.QR != "" {
		t.Fatalf("estado inicial inesperado: %+v", snap)
	}
	if err := s.ShowQR("qr-1"); err != nil {
		t.Fatalf("ShowQR: %v", err)
	}
	if snap := s.Snapshot(); snap.Status != PairPending || snap.QR != "qr-1" {
		t.Fatalf("tras ShowQR(qr-1): %+v", snap)
	}
	if err := s.ShowQR("qr-2"); err != nil {
		t.Fatalf("ShowQR: %v", err)
	}
	if snap := s.Snapshot(); snap.QR != "qr-2" {
		t.Fatalf("la rotación no reemplazó el QR vigente: %+v", snap)
	}
}

// TestMemoryQRSink_EmptyCode: un código vacío devuelve error y no cambia el estado.
func TestMemoryQRSink_EmptyCode(t *testing.T) {
	s := NewMemoryQRSink()
	if err := s.ShowQR(""); err == nil {
		t.Fatal("se esperaba error con código vacío")
	}
	if snap := s.Snapshot(); snap.QR != "" {
		t.Fatalf("no debió fijar QR: %+v", snap)
	}
}

// TestMemoryQRSink_FinishSuccess: Finish(nil) lleva a success y limpia el error.
func TestMemoryQRSink_FinishSuccess(t *testing.T) {
	s := NewMemoryQRSink()
	_ = s.ShowQR("qr-1")
	s.Finish(nil)
	if snap := s.Snapshot(); snap.Status != PairSuccess || snap.Err != "" {
		t.Fatalf("tras Finish(nil): %+v", snap)
	}
}

// TestMemoryQRSink_FinishError: Finish(err) lleva a error con el mensaje del err.
func TestMemoryQRSink_FinishError(t *testing.T) {
	s := NewMemoryQRSink()
	s.Finish(errors.New("boom"))
	snap := s.Snapshot()
	if snap.Status != PairError || snap.Err != "boom" {
		t.Fatalf("tras Finish(err): %+v", snap)
	}
}

// TestMemoryQRSink_WaitFirstQR: WaitFirstQR desbloquea al llegar el primer QR.
func TestMemoryQRSink_WaitFirstQR(t *testing.T) {
	s := NewMemoryQRSink()
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = s.ShowQR("qr-1")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.WaitFirstQR(ctx)
	if snap := s.Snapshot(); snap.QR != "qr-1" {
		t.Fatalf("WaitFirstQR retornó sin QR vigente: %+v", snap)
	}
}

// TestMemoryQRSink_WaitFirstQRUnblocksOnFinish: si el pairing falla antes de emitir QR, Finish
// también libera a WaitFirstQR (no se cuelga hasta el timeout del ctx).
func TestMemoryQRSink_WaitFirstQRUnblocksOnFinish(t *testing.T) {
	s := NewMemoryQRSink()
	go func() {
		time.Sleep(20 * time.Millisecond)
		s.Finish(errors.New("falló antes del QR"))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.WaitFirstQR(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("WaitFirstQR no se liberó tras Finish")
	}
}

// TestMemoryQRSink_Race: ShowQR/Finish/Snapshot concurrentes son seguros bajo -race.
func TestMemoryQRSink_Race(t *testing.T) {
	s := NewMemoryQRSink()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _ = s.ShowQR("qr") }()
		go func() { defer wg.Done(); _ = s.Snapshot() }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); s.Finish(nil) }()
	wg.Wait()
}
