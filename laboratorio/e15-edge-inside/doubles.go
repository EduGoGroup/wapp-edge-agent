//go:build laboratorio

package main

// doubles.go — LOS DOS DOBLES DE E15: el multiplexor de CloudLink (el «sumidero» de lo que el Edge sube a
// la nube) y la custodia de la DEK en un fichero (en lugar del Keychain de macOS).
//
// Todo lo demás del camino es el código de producción: el transporte falso, la puerta de entrantes, la
// cola cifrada, el despachador, el gestor de sesiones y el plano de control.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// deliveredEvent es un entrante que el despachador entregó al sumidero, con la hora a la que llegó.
type deliveredEvent struct {
	SessionID string
	Event     domain.InboundEvent
	At        time.Time
}

// receivedReceipt es un acuse (delivered/read) que el Edge habría subido a la nube.
type receivedReceipt struct {
	CommandID string
	Event     domain.ReceiptEvent
	At        time.Time
}

// sendFunc es el emisor de texto que cada sesión registra en el multiplexor: lo que CloudLink llama
// cuando la nube manda un `SendCommand`.
type sendFunc func(ctx context.Context, commandID, to, text string) error

// memoryMux es el DOBLE de CloudLink: cumple sessionmgr.CloudLinkMux como el adaptador real
// (internal/adapters/cloudlink) o el LogMux de diagnóstico, pero guarda en memoria lo que recibe.
//
// 🔴 QUÉ SE PIERDE AL DOBLARLO, y hay que tenerlo presente al leer E15: el adaptador real es quien aplica
// el gate 2-de-2 (lease vigente + DEK presente) ANTES de llamar al emisor, quien sella el texto para el
// tránsito y quien cae al outbox durable si el stream está caído. Aquí no hay lease, ni sellado de
// tránsito, ni outbox: el emisor se llama a pelo. Eso es lo que estudia E17, no E15.
type memoryMux struct {
	mu        sync.Mutex
	senders   map[string]sendFunc
	delivered []deliveredEvent
	receipts  []receivedReceipt
	// rejectNext es cuántas entregas va a RECHAZAR el sumidero antes de aceptar (bandera -sink-rejects):
	// sirve para ver que el despachador reintenta la MISMA cabeza y no adelanta a la siguiente.
	rejectNext int
	rejected   int
}

var _ sessionmgr.CloudLinkMux = (*memoryMux)(nil)

func newMemoryMux(rejectFirst int) *memoryMux {
	return &memoryMux{senders: map[string]sendFunc{}, rejectNext: rejectFirst}
}

func (m *memoryMux) Register(sessionID, _ string, send func(ctx context.Context, commandID, to, text string) error,
	_ func(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) error,
	_ func() bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.senders[sessionID] = send
}

func (m *memoryMux) Unregister(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.senders, sessionID)
}

// SinkFor devuelve el sumidero de la sesión: lo que el despachador llama con cada entrante.
func (m *memoryMux) SinkFor(sessionID string) app.InboundSink {
	return sinkFunc(func(_ context.Context, evt domain.InboundEvent) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.rejectNext > 0 {
			m.rejectNext--
			m.rejected++
			return errors.New("laboratorio: el sumidero rechaza esta entrega a propósito (-sink-rejects)")
		}
		m.delivered = append(m.delivered, deliveredEvent{SessionID: sessionID, Event: evt, At: time.Now()})
		return nil
	})
}

func (m *memoryMux) SendReceipt(commandID string, evt domain.ReceiptEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.receipts = append(m.receipts, receivedReceipt{CommandID: commandID, Event: evt, At: time.Now()})
}

func (m *memoryMux) SendLoggedOut(string) {}

func (m *memoryMux) sender(sessionID string) sendFunc {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.senders[sessionID]
}

func (m *memoryMux) deliveredCopy() []deliveredEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]deliveredEvent(nil), m.delivered...)
}

func (m *memoryMux) receiptsCopy() []receivedReceipt {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]receivedReceipt(nil), m.receipts...)
}

func (m *memoryMux) rejectedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rejected
}

type sinkFunc func(ctx context.Context, evt domain.InboundEvent) error

func (f sinkFunc) Deliver(ctx context.Context, evt domain.InboundEvent) error { return f(ctx, evt) }

// fileCustody es el DOBLE de la custodia de la DEK: la guarda en un fichero 0600 bajo un directorio 0700,
// en la misma ruta que el layout le asigna (`<data_dir>/keys/<session_id>.key`).
//
// 🔴 POR QUÉ ES UN DOBLE Y NO EL CÓDIGO DEL PRODUCTO. En darwin, `keycustody.NewFileCustody` NO escribe un
// fichero: es el Keychain (keychain_darwin.go), que pide permiso al usuario y no se deja borrar sin
// diálogo. El fichero plano es el suelo de Linux/Windows sin llavero, y el ADR-0049 (D-FW-7) RECHAZÓ
// usarlo en macOS dentro del binario. Este doble no contradice eso: vive en el laboratorio, fuera del
// binario, bajo `//go:build laboratorio`, y su llave es efímera (nace al emparejar y muere al borrar el
// directorio). La DEK NO se imprime nunca: solo su tamaño y su ruta.
type fileCustody struct{ path string }

var _ app.KeyCustody = fileCustody{}

func (c fileCustody) Store(dek []byte) error {
	if len(dek) != app.DEKSize {
		return fmt.Errorf("laboratorio: la DEK mide %d bytes y debe medir %d", len(dek), app.DEKSize)
	}
	if err := os.MkdirAll(filepath.Dir(c.path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(c.path, dek, 0o600)
}

func (c fileCustody) Load() ([]byte, error) { return os.ReadFile(c.path) }

func (c fileCustody) Exists() bool {
	_, err := os.Stat(c.path)
	return err == nil
}

// Clear la borra: es lo que llama el desvinculado (sessionmgr/unlink.go, `clearDEK`).
func (c fileCustody) Clear() error {
	if err := os.Remove(c.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// syncBuffer es un bytes.Buffer con candado: el log del Edge se escribe desde varias goroutines y el
// programa lo lee para comprobar qué gritó y qué NO dejó escapar (la DEK).
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
