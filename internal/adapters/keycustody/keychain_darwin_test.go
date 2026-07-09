//go:build darwin

package keycustody

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// KeychainCustody debe satisfacer el puerto app.KeyCustody (verificación en compilación).
var _ app.KeyCustody = (*KeychainCustody)(nil)

// Estos tests tocan el Keychain REAL de macOS. Corren SOLO en darwin y se SALTAN con t.Skip si el entorno
// no da acceso al Keychain (CI headless/sandbox), para no fallar el gate; en la Mac del usuario verifican
// el camino real (round-trip + migración archivo→Keychain + aislamiento). Usan un account UUID único por
// test y limpian con Clear.

// TestKeychain_RoundTrip: Store → Exists → Load → Clear sobre el Keychain real.
func TestKeychain_RoundTrip(t *testing.T) {
	const id = "a1b2c3d4-1111-2222-3333-444455556666"
	path := filepath.Join(t.TempDir(), "keys", id+".key")
	c := NewFileCustody(path)
	t.Cleanup(func() { _ = c.Clear() })

	dek := newTestDEK(t)
	if err := c.Store(dek); err != nil {
		t.Skipf("Keychain no disponible en este entorno (Store falló): %v", err)
	}
	if !c.Exists() {
		t.Fatal("Exists debería ser true tras Store")
	}
	got, err := c.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatal("Load no coincide con lo guardado")
	}
	if err := c.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if c.Exists() {
		t.Fatal("Exists debería ser false tras Clear")
	}
}

// TestKeychain_MigraArchivoYBorra: con la DEK en un archivo plano legacy, Load la MIGRA al Keychain y borra
// el archivo SOLO tras verificar la relectura; una segunda Load la sirve ya desde el Keychain.
func TestKeychain_MigraArchivoYBorra(t *testing.T) {
	const id = "a1b2c3d4-9999-8888-7777-666655554444"
	dir := t.TempDir()
	path := filepath.Join(dir, "keys", id+".key")
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
		t.Skipf("Keychain no disponible en este entorno (Load/migración falló): %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatal("la DEK migrada no coincide")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatal("el archivo plano debería borrarse tras verificar la lectura desde Keychain")
	}
	// Idempotente: la segunda Load ya viene del Keychain.
	got2, err := c.Load()
	if err != nil || !bytes.Equal(got2, dek) {
		t.Fatalf("segunda Load (desde Keychain): err=%v", err)
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
				t.Errorf("el archivo %s contiene la DEK en claro tras migrar al Keychain", e.Name())
			}
		}
	}
}
