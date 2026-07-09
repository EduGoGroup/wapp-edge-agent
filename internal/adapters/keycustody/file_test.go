//go:build !darwin

package keycustody_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-shared/envelope"
)

// FileCustody debe satisfacer el puerto app.KeyCustody (verificación en compilación).
var _ app.KeyCustody = (*keycustody.FileCustody)(nil)

// newDEK genera una DEK aleatoria de 32 bytes para los tests.
func newDEK(t *testing.T) []byte {
	t.Helper()
	dek := make([]byte, keycustody.KeySize)
	if _, err := rand.Read(dek); err != nil {
		t.Fatalf("no se pudo generar DEK de prueba: %v", err)
	}
	return dek
}

func TestFileCustody_StoreLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "dek.key")
	c := keycustody.NewFileCustody(path)
	dek := newDEK(t)

	if err := c.Store(dek); err != nil {
		t.Fatalf("Store: %v", err)
	}
	got, err := c.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatalf("round-trip difiere: got %x, want %x", got, dek)
	}
}

func TestFileCustody_ExistsBeforeAndAfter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dek.key")
	c := keycustody.NewFileCustody(path)

	if c.Exists() {
		t.Fatal("Exists debe ser false antes de Store")
	}
	if err := c.Store(newDEK(t)); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if !c.Exists() {
		t.Fatal("Exists debe ser true tras Store")
	}
}

func TestFileCustody_ExistsFalseOnDirectory(t *testing.T) {
	// Una ruta que apunta a un directorio no es una DEK regular.
	dir := t.TempDir()
	c := keycustody.NewFileCustody(dir)
	if c.Exists() {
		t.Fatal("Exists debe ser false cuando la ruta es un directorio")
	}
}

func TestFileCustody_FilePermsAre0600(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "dek.key")
	c := keycustody.NewFileCustody(path)
	if err := c.Store(newDEK(t)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat archivo: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("permiso del archivo = %o, want 0600", perm)
	}

	di, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("Stat directorio: %v", err)
	}
	if perm := di.Mode().Perm(); perm != 0o700 {
		t.Fatalf("permiso del directorio = %o, want 0700", perm)
	}
}

func TestFileCustody_StoreForces0600OnPreexistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dek.key")
	// Archivo preexistente con permisos laxos (0644).
	if err := os.WriteFile(path, []byte("viejo"), 0o644); err != nil {
		t.Fatalf("preparar archivo previo: %v", err)
	}

	c := keycustody.NewFileCustody(path)
	dek := newDEK(t)
	if err := c.Store(dek); err != nil {
		t.Fatalf("Store: %v", err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("Store debe forzar 0600 sobre archivo preexistente, got %o", perm)
	}
	got, err := c.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatal("Store debe sobrescribir el contenido previo")
	}
}

func TestFileCustody_LoadMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ausente.key")
	c := keycustody.NewFileCustody(path)

	_, err := c.Load()
	if err == nil {
		t.Fatal("Load sobre archivo ausente debe devolver error")
	}
	if !errors.Is(err, keycustody.ErrNoKey) {
		t.Fatalf("error esperado ErrNoKey, got %v", err)
	}
}

func TestFileCustody_LoadCorruptSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dek.key")
	// Contenido del tamaño incorrecto (no 32 bytes).
	if err := os.WriteFile(path, []byte("corto"), 0o600); err != nil {
		t.Fatalf("preparar archivo corrupto: %v", err)
	}
	c := keycustody.NewFileCustody(path)

	if _, err := c.Load(); err == nil {
		t.Fatal("Load sobre DEK de tamaño incorrecto debe devolver error")
	}
}

func TestFileCustody_StoreRejectsWrongSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dek.key")
	c := keycustody.NewFileCustody(path)

	cases := map[string][]byte{
		"vacía": {},
		"corta": make([]byte, keycustody.KeySize-1),
		"larga": make([]byte, keycustody.KeySize+1),
		"nil":   nil,
	}
	for name, dek := range cases {
		t.Run(name, func(t *testing.T) {
			if err := c.Store(dek); !errors.Is(err, keycustody.ErrKeySize) {
				t.Fatalf("Store(%s) error = %v, want ErrKeySize", name, err)
			}
			if c.Exists() {
				t.Fatalf("Store(%s) inválido no debe crear archivo", name)
			}
		})
	}
}

func TestFileCustody_StoreErrorWhenDirNotCreatable(t *testing.T) {
	// El directorio padre es en realidad un archivo: MkdirAll debe fallar.
	dir := t.TempDir()
	blocker := filepath.Join(dir, "soy-un-archivo")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("preparar blocker: %v", err)
	}
	path := filepath.Join(blocker, "dek.key")
	c := keycustody.NewFileCustody(path)

	if err := c.Store(newDEK(t)); err == nil {
		t.Fatal("Store debe fallar si no puede crear el directorio contenedor")
	}
}

func TestFileCustody_StoreErrorWhenPathIsDirectory(t *testing.T) {
	// La ruta destino es un directorio existente: MkdirAll(dir-padre) tiene éxito
	// pero WriteFile sobre un directorio falla.
	path := t.TempDir()
	c := keycustody.NewFileCustody(path)

	if err := c.Store(newDEK(t)); err == nil {
		t.Fatal("Store debe fallar cuando la ruta destino es un directorio")
	}
}

func TestFileCustody_LoadErrorWhenPathIsDirectory(t *testing.T) {
	// Leer un directorio devuelve un error distinto a os.ErrNotExist: no es ErrNoKey.
	path := t.TempDir()
	c := keycustody.NewFileCustody(path)

	_, err := c.Load()
	if err == nil {
		t.Fatal("Load debe fallar cuando la ruta es un directorio")
	}
	if errors.Is(err, keycustody.ErrNoKey) {
		t.Fatalf("error de directorio no debe ser ErrNoKey, got %v", err)
	}
}

// TestFileCustody_IntegratesWithEnvelope prueba que la DEK custodiada encaja con
// el contrato de envelope: se sella algo con la DEK cargada y se vuelve a abrir.
// El adaptador NO depende de envelope; el acoplamiento ocurre solo aquí, en el test.
func TestFileCustody_IntegratesWithEnvelope(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dek.key")
	c := keycustody.NewFileCustody(path)

	dek := newDEK(t)
	if err := c.Store(dek); err != nil {
		t.Fatalf("Store: %v", err)
	}
	loaded, err := c.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	env, err := envelope.NewEnvelope(loaded)
	if err != nil {
		t.Fatalf("NewEnvelope con DEK custodiada: %v", err)
	}
	plaintext := []byte("mensaje de negocio del Edge")
	sealed, err := env.Seal(plaintext)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	opened, err := env.Open(sealed)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatalf("envelope round-trip con DEK custodiada difiere: got %q", opened)
	}
}
