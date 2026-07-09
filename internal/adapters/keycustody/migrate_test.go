package keycustody

import (
	"bytes"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// memCustody es un DOBLE en memoria del puerto app.KeyCustody para CI HEADLESS (el CI no tiene Keychain).
// Guarda la DEK en RAM, nunca en disco: sirve de destino de migración y para el barrido de aislamiento.
type memCustody struct {
	dek          []byte
	loadOverride []byte // si != nil, Load lo devuelve (para simular una verificación fallida)
	storeErr     error  // si != nil, Store falla (para simular Keychain caído)
	storeCalls   int
}

// memCustody debe satisfacer el puerto app.KeyCustody (verificación en compilación).
var _ app.KeyCustody = (*memCustody)(nil)

func (m *memCustody) Store(dek []byte) error {
	m.storeCalls++
	if m.storeErr != nil {
		return m.storeErr
	}
	m.dek = append([]byte(nil), dek...)
	return nil
}

func (m *memCustody) Load() ([]byte, error) {
	if m.loadOverride != nil {
		return m.loadOverride, nil
	}
	if m.dek == nil {
		return nil, ErrNoKey
	}
	return append([]byte(nil), m.dek...), nil
}

func (m *memCustody) Exists() bool { return m.dek != nil }

// newTestDEK genera una DEK de KeySize bytes con CSPRNG (bytes conocidos que no aparecen por azar en
// disco). Compartida por los tests de migración y por el test real de Keychain (darwin).
func newTestDEK(t *testing.T) []byte {
	t.Helper()
	dek := make([]byte, KeySize)
	if _, err := rand.Read(dek); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return dek
}

// TestMigrate_ImportaYBorraTrasVerificar: happy path — importa la DEK del archivo a la custodia y BORRA el
// archivo SOLO tras verificar la relectura.
func TestMigrate_ImportaYBorraTrasVerificar(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "3f2504e0-4f89-41d3-9a0c-0305e82c3301.key")
	dek := newTestDEK(t)
	if err := os.WriteFile(path, dek, 0o600); err != nil {
		t.Fatal(err)
	}
	mem := &memCustody{}

	got, migrated, err := migrateFileToCustody(path, mem)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if !migrated {
		t.Fatal("migrated debería ser true")
	}
	if !bytes.Equal(got, dek) {
		t.Fatal("la DEK devuelta no coincide")
	}
	if !bytes.Equal(mem.dek, dek) {
		t.Fatal("la custodia no recibió la DEK")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatal("el archivo plano debería haberse borrado tras verificar la relectura")
	}
}

// TestMigrate_Idempotente: sin archivo no hace nada; con archivo migra una vez; re-ejecutar no re-almacena.
func TestMigrate_Idempotente(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dek.key")
	mem := &memCustody{}

	if _, migrated, err := migrateFileToCustody(path, mem); err != nil || migrated {
		t.Fatalf("sin archivo: got migrated=%v err=%v; want false,nil", migrated, err)
	}
	if mem.storeCalls != 0 {
		t.Fatalf("no debería haber llamado Store; llamadas=%d", mem.storeCalls)
	}

	dek := newTestDEK(t)
	if err := os.WriteFile(path, dek, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, migrated, err := migrateFileToCustody(path, mem); err != nil || !migrated {
		t.Fatalf("primera migración: got migrated=%v err=%v; want true,nil", migrated, err)
	}
	// Segunda pasada: el archivo ya no está → nada que migrar, sin error.
	if _, migrated, err := migrateFileToCustody(path, mem); err != nil || migrated {
		t.Fatalf("re-ejecución: got migrated=%v err=%v; want false,nil", migrated, err)
	}
	if mem.storeCalls != 1 {
		t.Fatalf("Store debería haberse llamado 1 vez, no %d", mem.storeCalls)
	}
}

// TestMigrate_ConservaArchivoSiVerificacionFalla: si la relectura desde la custodia no coincide, NO se
// borra el archivo (fallback seguro del design §10).
func TestMigrate_ConservaArchivoSiVerificacionFalla(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dek.key")
	dek := newTestDEK(t)
	if err := os.WriteFile(path, dek, 0o600); err != nil {
		t.Fatal(err)
	}
	// La custodia "miente" al releer: devuelve otros bytes.
	mem := &memCustody{loadOverride: make([]byte, KeySize)}

	if _, migrated, err := migrateFileToCustody(path, mem); err == nil || migrated {
		t.Fatalf("verificación fallida: got migrated=%v err=%v; want false + error", migrated, err)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatal("el archivo debería CONSERVARSE si la verificación falla")
	}
}

// TestMigrate_ConservaArchivoSiStoreFalla: si la custodia (p. ej. Keychain) falla al importar, el archivo
// se conserva y se propaga el error.
func TestMigrate_ConservaArchivoSiStoreFalla(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dek.key")
	if err := os.WriteFile(path, newTestDEK(t), 0o600); err != nil {
		t.Fatal(err)
	}
	mem := &memCustody{storeErr: errors.New("keychain caído")}

	if _, migrated, err := migrateFileToCustody(path, mem); err == nil || migrated {
		t.Fatalf("Store fallido: got migrated=%v err=%v; want false + error", migrated, err)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatal("el archivo debería conservarse si Store falla")
	}
}

// TestMigrate_RechazaTamanoInvalido: un archivo que no mide KeySize se rechaza sin tocar la custodia.
func TestMigrate_RechazaTamanoInvalido(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dek.key")
	if err := os.WriteFile(path, []byte("corta"), 0o600); err != nil {
		t.Fatal(err)
	}
	mem := &memCustody{}
	if _, migrated, err := migrateFileToCustody(path, mem); err == nil || migrated {
		t.Fatalf("tamaño inválido: got migrated=%v err=%v; want false + error", migrated, err)
	}
	if mem.storeCalls != 0 {
		t.Fatal("no debería haber tocado la custodia")
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatal("el archivo debería conservarse")
	}
}

// TestMigrate_DEKNoQuedaEnDiscoPlano: tras migrar a una custodia en memoria, la DEK (bytes conocidos) no
// aparece en NINGÚN archivo del dir (patrón isolation_test del Plan 022 / ADR-0007). Es el DoD R3/R6.
func TestMigrate_DEKNoQuedaEnDiscoPlano(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "b3f1c2d4-5e6a-4b7c-8d9e-0f1a2b3c4d5e.key")
	dek := newTestDEK(t)
	if err := os.WriteFile(path, dek, 0o600); err != nil {
		t.Fatal(err)
	}
	mem := &memCustody{}
	if _, migrated, err := migrateFileToCustody(path, mem); err != nil || !migrated {
		t.Fatalf("migrate: migrated=%v err=%v", migrated, err)
	}

	// El archivo de la DEK debe haberse borrado.
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatal("el archivo de la DEK sigue en disco tras migrar a la custodia")
	}
	// Barrido: ningún archivo del dir contiene los 32 bytes de la DEK.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		raw, rErr := os.ReadFile(filepath.Join(dir, e.Name()))
		if rErr != nil {
			t.Fatal(rErr)
		}
		if bytes.Contains(raw, dek) {
			t.Errorf("el archivo %s contiene la DEK en claro ⇒ no debería tras migrar a la custodia", e.Name())
		}
	}
}
