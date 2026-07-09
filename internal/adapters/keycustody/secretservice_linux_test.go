//go:build linux

package keycustody

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	keyring "github.com/zalando/go-keyring"
)

// secretservice_linux_test.go usa el backend MOCK de go-keyring (keyring.MockInit): NO toca el Secret
// Service real, así que corre HEADLESS en CI-linux. NO corre en darwin (build tag linux) — ahí la custodia
// es el Keychain. Cubre el camino "keyring disponible" (round-trip + migración archivo→keyring). El
// FALLBACK al archivo 0600 cuando NO hay Secret Service NO es forzable con el mock (MockInit siempre deja
// el backend "disponible"); esa rama se verifica EN VIVO en el test delegado (Plan 024 · T4) sobre un
// Linux con escritorio (keyring) y sobre uno headless (fallback a archivo). newTestDEK viene de
// migrate_test.go (mismo paquete).

// TestSecretService_RoundTripConMock: Store → Exists → Load → Clear contra el keyring mock.
func TestSecretService_RoundTripConMock(t *testing.T) {
	keyring.MockInit()
	path := filepath.Join(t.TempDir(), "keys", "a1b2c3d4-1111-2222-3333-444455556666.key")
	c := NewFileCustody(path)
	if c.fallback != nil {
		t.Fatal("con MockInit el Secret Service debe estar disponible (sin fallback a archivo)")
	}
	dek := newTestDEK(t)
	if err := c.Store(dek); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if !c.Exists() {
		t.Fatal("Exists debe ser true tras Store")
	}
	got, err := c.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatalf("round-trip difiere: got %x, want %x", got, dek)
	}
	if err := c.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if c.Exists() {
		t.Fatal("Exists debe ser false tras Clear")
	}
}

// TestSecretService_MigraArchivoYBorra: con la DEK en un archivo plano legacy, Load la MIGRA al keyring y
// borra el archivo SOLO tras verificar la relectura; una segunda Load la sirve ya desde el keyring y no
// queda la DEK en disco plano (aislamiento, ADR-0007 / R6).
func TestSecretService_MigraArchivoYBorra(t *testing.T) {
	keyring.MockInit()
	dir := t.TempDir()
	path := filepath.Join(dir, "keys", "a1b2c3d4-9999-8888-7777-666655554444.key")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	dek := newTestDEK(t)
	if err := os.WriteFile(path, dek, 0o600); err != nil {
		t.Fatal(err)
	}

	c := NewFileCustody(path)
	t.Cleanup(func() { _ = c.Clear() })

	got, err := c.Load()
	if err != nil {
		t.Fatalf("Load (migración): %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatal("la DEK migrada no coincide")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatal("el archivo plano debería borrarse tras verificar la lectura desde el keyring")
	}
	// Idempotente: la segunda Load ya viene del keyring.
	got2, err := c.Load()
	if err != nil || !bytes.Equal(got2, dek) {
		t.Fatalf("segunda Load (desde keyring): err=%v", err)
	}
	// Aislamiento: la DEK (bytes conocidos) no queda en ningún archivo del dir tras migrar.
	entries, err := os.ReadDir(filepath.Dir(path))
	if err == nil {
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			raw, rErr := os.ReadFile(filepath.Join(filepath.Dir(path), e.Name()))
			if rErr == nil && bytes.Contains(raw, dek) {
				t.Errorf("el archivo %s contiene la DEK en claro tras migrar al keyring", e.Name())
			}
		}
	}
}
