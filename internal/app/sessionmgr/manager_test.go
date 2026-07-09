package sessionmgr

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// clearer es el subconjunto de la custodia que expone el borrado quirúrgico por sesión (R5). La
// custodia concreta (FileCustody) lo implementa; el puerto app.KeyCustody no, así que se type-asserta.
type clearer interface{ Clear() error }

func testLogger() sharedlogger.Logger {
	// Logger a un buffer descartado: silencioso pero real (no nil) para ejercitar los campos log.
	return sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}))
}

// TestManager_CustodyPerSession demuestra la custodia DEK MULTI-ENTRADA (R1, DoD T1): dos sesiones
// distintas obtienen custodias independientes; Store/Load no se pisan y Clear() de una no toca la otra.
func TestManager_CustodyPerSession(t *testing.T) {
	base := filepath.Join(t.TempDir(), "edge-data")
	m := NewManager(NewLayout(base), nil, 5, testLogger())
	m.newCustody = newMemCustodyFactory() // doble en memoria: no tocar el Keychain real (Plan 023 T2)

	custA, err := m.custodyFor(uuidA)
	if err != nil {
		t.Fatalf("custodyFor(A) error: %v", err)
	}
	custB, err := m.custodyFor(uuidB)
	if err != nil {
		t.Fatalf("custodyFor(B) error: %v", err)
	}

	// DEKs distintas (32 bytes cada una).
	dekA := bytes.Repeat([]byte{0xAA}, 32)
	dekB := bytes.Repeat([]byte{0xBB}, 32)

	// Se enrutan a través de liveSession para ejercitar también la estructura (campo custody).
	sessA := &liveSession{meta: domain.Session{SessionID: uuidA}, custody: custA, log: testLogger()}
	sessB := &liveSession{meta: domain.Session{SessionID: uuidB}, custody: custB, log: testLogger()}

	if err := sessA.custody.Store(dekA); err != nil {
		t.Fatalf("Store(A) error: %v", err)
	}
	if err := sessB.custody.Store(dekB); err != nil {
		t.Fatalf("Store(B) error: %v", err)
	}

	gotA, err := sessA.custody.Load()
	if err != nil {
		t.Fatalf("Load(A) error: %v", err)
	}
	gotB, err := sessB.custody.Load()
	if err != nil {
		t.Fatalf("Load(B) error: %v", err)
	}
	if !bytes.Equal(gotA, dekA) || !bytes.Equal(gotB, dekB) {
		t.Fatalf("las DEKs por sesión se cruzaron: A=%x B=%x", gotA, gotB)
	}
	if sessA.meta.SessionID == sessB.meta.SessionID {
		t.Fatalf("meta.SessionID no debería coincidir entre sesiones")
	}

	// Clear() de A: idempotente y aislado. B permanece intacta.
	cl, ok := custA.(clearer)
	if !ok {
		t.Fatalf("la custodia no expone Clear()")
	}
	if err := cl.Clear(); err != nil {
		t.Fatalf("Clear(A) error: %v", err)
	}
	if custA.Exists() {
		t.Fatalf("tras Clear(A) la DEK de A no debería existir")
	}
	if err := cl.Clear(); err != nil {
		t.Fatalf("Clear(A) repetido debería ser idempotente, dio: %v", err)
	}
	if !custB.Exists() {
		t.Fatalf("Clear(A) afectó a la DEK de B (no debería)")
	}
	if got, err := custB.Load(); err != nil || !bytes.Equal(got, dekB) {
		t.Fatalf("la DEK de B cambió tras Clear(A): got %x err %v", got, err)
	}
}

// TestManager_ListEmpty_StopNoPanic cubre el esqueleto: List() vacío al arranque y Stop() sin sesiones
// no panica (apagado ordenado sobre un map vacío).
func TestManager_ListEmpty_StopNoPanic(t *testing.T) {
	m := NewManager(NewLayout(t.TempDir()), nil, 5, testLogger())

	if got := m.List(); len(got) != 0 {
		t.Fatalf("List() al inicio debería ser vacío, got %d", len(got))
	}
	if got := m.Capacity(); got != 5 {
		t.Fatalf("Capacity(): got %d, want 5", got)
	}

	// No debe panicar sin listeners arrancados.
	m.Stop()
}
