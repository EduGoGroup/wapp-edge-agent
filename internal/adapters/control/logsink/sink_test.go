package logsink

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// writeLine alimenta una línea completa (con '\n') al sink, como haría un slog.Handler.
func writeLine(t *testing.T, s *Sink, line string) {
	t.Helper()
	if _, err := s.Write([]byte(line + "\n")); err != nil {
		t.Fatalf("Write devolvió error inesperado: %v", err)
	}
}

func TestWriteSplitsLinesAndKeepsPartial(t *testing.T) {
	s := New(10)

	// Una escritura con dos líneas completas + un resto sin '\n': el resto no debe aflorecer aún.
	if _, err := s.Write([]byte("a\nb\nparcial")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	snap, _, cancel := s.Subscribe()
	cancel()
	if got, want := len(snap), 2; got != want {
		t.Fatalf("líneas vigentes = %d, want %d (el resto sin '\\n' no debe contarse)", got, want)
	}
	if snap[0] != "a" || snap[1] != "b" {
		t.Fatalf("snapshot = %v, want [a b]", snap)
	}

	// Al cerrar la línea pendiente, debe completarse uniendo el resto previo.
	if _, err := s.Write([]byte(" cerrada\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	snap2, _, cancel2 := s.Subscribe()
	cancel2()
	if got, want := len(snap2), 3; got != want {
		t.Fatalf("líneas = %d, want %d", got, want)
	}
	if snap2[2] != "parcial cerrada" {
		t.Fatalf("línea reconstruida = %q, want %q", snap2[2], "parcial cerrada")
	}
}

func TestRingDiscardsOldest(t *testing.T) {
	const capacity = 5
	s := New(capacity)
	for i := 0; i < 12; i++ {
		writeLine(t, s, fmt.Sprintf("line-%02d", i))
	}
	snap, _, cancel := s.Subscribe()
	cancel()
	if got := len(snap); got != capacity {
		t.Fatalf("snapshot len = %d, want %d (ring acotado)", got, capacity)
	}
	// Deben quedar las 5 más recientes (07..11), en orden.
	for i, line := range snap {
		want := fmt.Sprintf("line-%02d", 7+i)
		if line != want {
			t.Fatalf("snap[%d] = %q, want %q", i, line, want)
		}
	}
}

func TestSubscribeDeliversSnapshotThenNewLines(t *testing.T) {
	s := New(100)
	writeLine(t, s, "old-1")
	writeLine(t, s, "old-2")

	snap, lines, cancel := s.Subscribe()
	defer cancel()

	if len(snap) != 2 || snap[0] != "old-1" || snap[1] != "old-2" {
		t.Fatalf("snapshot = %v, want [old-1 old-2]", snap)
	}

	// Las nuevas líneas llegan por el canal, no en el snapshot.
	writeLine(t, s, "new-1")
	writeLine(t, s, "new-2")

	for _, want := range []string{"new-1", "new-2"} {
		select {
		case got := <-lines:
			if got != want {
				t.Fatalf("línea recibida = %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout esperando %q", want)
		}
	}
}

func TestCancelClosesChannelAndRemovesSubscriber(t *testing.T) {
	s := New(10)
	_, lines, cancel := s.Subscribe()
	if got := s.subscriberCount(); got != 1 {
		t.Fatalf("suscriptores = %d, want 1", got)
	}

	cancel()
	cancel() // idempotente: no debe entrar en pánico ni cerrar dos veces.

	if got := s.subscriberCount(); got != 0 {
		t.Fatalf("suscriptores tras cancel = %d, want 0", got)
	}
	if _, ok := <-lines; ok {
		t.Fatalf("el canal debe quedar cerrado tras cancel")
	}
}

// TestSlowSubscriberDoesNotBlockWriter llena el canal de un suscriptor lento y verifica que las
// escrituras del logger siguen retornando (drop en vez de bloqueo).
func TestSlowSubscriberDoesNotBlockWriter(t *testing.T) {
	s := New(10)
	_, _, cancel := s.Subscribe() // nunca drena su canal
	defer cancel()

	done := make(chan struct{})
	go func() {
		for i := 0; i < subscriberBuffer*4; i++ {
			writeLine(t, s, fmt.Sprintf("flood-%d", i))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Write se bloqueó por un suscriptor lento (debería descartar)")
	}
}

// TestConcurrentProducersConsumers ejercita el sink con múltiples productores y consumidores para
// detectar carreras bajo -race.
func TestConcurrentProducersConsumers(t *testing.T) {
	s := New(256)

	const producers = 8
	const consumers = 8
	const perProducer = 500

	var wg sync.WaitGroup

	stop := make(chan struct{})
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, lines, cancel := s.Subscribe()
			defer cancel()
			for {
				select {
				case <-stop:
					return
				case <-lines:
				}
			}
		}()
	}

	var pwg sync.WaitGroup
	for p := 0; p < producers; p++ {
		pwg.Add(1)
		go func(id int) {
			defer pwg.Done()
			for i := 0; i < perProducer; i++ {
				writeLine(t, s, fmt.Sprintf("p%d-%d", id, i))
			}
		}(p)
	}
	pwg.Wait()

	close(stop)
	wg.Wait()

	// El buffer nunca excede su capacidad.
	snap, _, cancel := s.Subscribe()
	cancel()
	if len(snap) > 256 {
		t.Fatalf("snapshot len = %d excede la capacidad 256", len(snap))
	}
}
