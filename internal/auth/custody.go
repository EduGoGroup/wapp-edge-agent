package auth

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// SecretCustody custodia UN secreto opaco de LONGITUD VARIABLE (el refresh token
// del operador). Es el patrón mono-secreto de app.KeyCustody (Store/Load/Exists/
// Clear) PERO sin la restricción de 32 bytes de la DEK: el refresh token del IAM
// es un opaco base64 (~44 chars), así que NO cabe en keycustody.NewFileCustody
// (que exige KeySize exacto). Por eso es un puerto propio, sin extender
// app.KeyCustody (ADR-0025: "custodia adicional junto a la DEK").
//
// DEUDA EXPLÍCITA (follow-up Paso B / Ola 4): la impl por defecto es un archivo
// 0600 (el "suelo de seguridad", igual que el fallback de la DEK). El respaldo en
// el keystore del SO (Keychain/DPAPI/Secret Service) para el refresh token queda
// pendiente: los adaptadores actuales de keycustody codifican el contrato de 32
// bytes de la DEK y no admiten un opaco de longitud variable sin tocar ese
// paquete. El refresh token NUNCA sube a la nube (zero-knowledge).
type SecretCustody interface {
	// Store persiste el secreto, sobrescribiendo cualquiera previo.
	Store(secret []byte) error
	// Load recupera el secreto custodiado. Devuelve ErrNoSecret si no hay ninguno.
	Load() ([]byte, error)
	// Exists indica si hay un secreto custodiado.
	Exists() bool
	// Clear elimina el secreto de forma idempotente (ausente ⇒ no es error).
	Clear() error
}

// ErrNoSecret indica que se intentó Load un secreto que aún no ha sido custodiado.
var ErrNoSecret = errors.New("auth: no hay secreto custodiado")

const (
	secretDirPerm  os.FileMode = 0o700
	secretFilePerm os.FileMode = 0o600
)

// FileSecretCustody persiste el secreto en un archivo local 0600 bajo un
// directorio 0700. Es el suelo de seguridad (ver SecretCustody). La ruta se
// inyecta por construcción; nunca se hardcodea.
type FileSecretCustody struct {
	path string
}

var _ SecretCustody = (*FileSecretCustody)(nil)

// NewFileSecretCustody construye la custodia del refresh token del operador en
// path (p. ej. "<data_dir>/auth/operator.refresh"). El directorio contenedor se
// crea 0700 en el primer Store.
func NewFileSecretCustody(path string) *FileSecretCustody {
	return &FileSecretCustody{path: path}
}

func (c *FileSecretCustody) Store(secret []byte) error {
	if len(secret) == 0 {
		return fmt.Errorf("auth: refresh token vacío")
	}
	if err := os.MkdirAll(filepath.Dir(c.path), secretDirPerm); err != nil {
		return fmt.Errorf("auth: no se pudo crear el directorio de custodia del refresh: %w", err)
	}
	if err := os.WriteFile(c.path, secret, secretFilePerm); err != nil {
		return fmt.Errorf("auth: no se pudo escribir el refresh token: %w", err)
	}
	// WriteFile aplica el permiso solo al CREAR: forzamos 0600 aun sobre un archivo preexistente.
	if err := os.Chmod(c.path, secretFilePerm); err != nil {
		return fmt.Errorf("auth: no se pudo fijar el permiso 0600 del refresh token: %w", err)
	}
	return nil
}

func (c *FileSecretCustody) Load() ([]byte, error) {
	b, err := os.ReadFile(c.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNoSecret, c.path)
		}
		return nil, fmt.Errorf("auth: no se pudo leer el refresh token: %w", err)
	}
	return b, nil
}

func (c *FileSecretCustody) Exists() bool {
	info, err := os.Stat(c.path)
	if err != nil {
		return false
	}
	return info.Mode().IsRegular()
}

func (c *FileSecretCustody) Clear() error {
	if err := os.Remove(c.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("auth: no se pudo borrar el refresh token: %w", err)
	}
	return nil
}

// MemorySecretCustody es una custodia EN MEMORIA para tests (sin disco). Segura
// para uso concurrente.
type MemorySecretCustody struct {
	mu     sync.Mutex
	secret []byte
}

var _ SecretCustody = (*MemorySecretCustody)(nil)

func (c *MemorySecretCustody) Store(secret []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.secret = append([]byte(nil), secret...)
	return nil
}

func (c *MemorySecretCustody) Load() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.secret == nil {
		return nil, ErrNoSecret
	}
	return append([]byte(nil), c.secret...), nil
}

func (c *MemorySecretCustody) Exists() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.secret != nil
}

func (c *MemorySecretCustody) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.secret = nil
	return nil
}
