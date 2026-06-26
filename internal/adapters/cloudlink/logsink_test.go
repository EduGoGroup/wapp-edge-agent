package cloudlink

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// TestLogSink_Deliver_WritesEvent: Deliver escribe una línea de log NO vacía con campos del
// InboundEvent, sin pánico ni error.
func TestLogSink_Deliver_WritesEvent(t *testing.T) {
	var buf bytes.Buffer
	log := sharedlogger.New(sharedlogger.WithWriter(&buf), sharedlogger.WithJSON(true))
	sink := NewLogSink(log)

	evt := domain.InboundEvent{
		MessageID: "MID-9",
		Chat:      "123@s.whatsapp.net",
		Sender:    "123@s.whatsapp.net",
		PushName:  "Bob",
		Timestamp: time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC),
		Type:      "text",
		Text:      "ping",
	}

	if err := sink.Deliver(context.Background(), evt); err != nil {
		t.Fatalf("Deliver devolvió error: %v", err)
	}

	out := buf.String()
	if strings.TrimSpace(out) == "" {
		t.Fatal("el LogSink no escribió nada")
	}
	for _, want := range []string{"MID-9", "Bob", "ping"} {
		if !strings.Contains(out, want) {
			t.Fatalf("la salida del log no contiene %q: %s", want, out)
		}
	}
}
