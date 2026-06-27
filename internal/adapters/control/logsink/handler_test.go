package logsink

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestHandlerStreamsSnapshotAndLiveLines arranca un servidor de prueba con el handler SSE, conecta
// un cliente real, y verifica que recibe el buffer reciente al conectar y luego las líneas nuevas
// como eventos SSE (data: ...).
func TestHandlerStreamsSnapshotAndLiveLines(t *testing.T) {
	s := New(100)
	writeLine(t, s, "antes-1")

	srv := httptest.NewServer(Handler(s))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("Cache-Control = %q, want no-cache", cc)
	}

	dataCh := make(chan string, 16)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if after, ok := strings.CutPrefix(line, "data: "); ok {
				dataCh <- after
			}
		}
	}()

	// El snapshot debe llegar primero.
	expectData(t, dataCh, "antes-1")

	// Esperar a que el suscriptor quede registrado antes de emitir líneas en vivo (evita carrera
	// entre la conexión y la primera escritura).
	waitSubscribers(t, s, 1)

	writeLine(t, s, "vivo-1")
	writeLine(t, s, "vivo-2")
	expectData(t, dataCh, "vivo-1")
	expectData(t, dataCh, "vivo-2")

	// Al cancelar el contexto del cliente, el servidor debe limpiar la suscripción.
	cancel()
	waitSubscribers(t, s, 0)
}

func expectData(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("evento SSE = %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout esperando evento SSE %q", want)
	}
}

func waitSubscribers(t *testing.T, s *Sink, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if s.subscriberCount() == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("suscriptores = %d, want %d (timeout)", s.subscriberCount(), want)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
