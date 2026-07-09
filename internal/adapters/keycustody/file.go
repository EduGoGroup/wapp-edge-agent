//go:build !darwin

package keycustody

// file.go es la custodia PROVISIONAL para las plataformas SIN Keychain (Windows/Linux): persiste la DEK
// en un archivo local en texto plano con permisos 0600 bajo un directorio 0700. En darwin la sustituye
// el Keychain (keychain_darwin.go, Plan 023 · T2); este archivo queda bajo //go:build !darwin.
//
// ⚠️ DEUDA EXPLÍCITA (ADR-0007 / ADR-0013): NO es la decisión v1 para Windows/Linux — el Plan 024 la
// sustituirá por DPAPI (Windows) y Secret Service (Linux). Como el cambio es solo de adaptador (todo
// depende del puerto app.KeyCustody), la deuda queda acotada a este archivo. El archivo se guarda SIN
// cifrar a propósito: cifrar la DEK requeriría custodiar la llave que la cifra — justo lo que el keystore
// del SO resuelve en v1. Documentado como provisional para no dar falsa sensación de seguridad.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// dirPerm y filePerm son los permisos del directorio contenedor y del archivo de la DEK: solo el dueño
// puede entrar al directorio (0700) y leer/escribir el archivo (0600).
const (
	dirPerm  os.FileMode = 0o700
	filePerm os.FileMode = 0o600
)

// FileCustody implementa app.KeyCustody persistiendo la DEK en un archivo local. La ruta del archivo se
// inyecta por construcción: nunca se hardcodea.
type FileCustody struct {
	path string
}

// NewFileCustody construye un FileCustody que custodia la DEK en path (p. ej. ".../keys/<id>.key"); su
// directorio contenedor se crea con permisos 0700 en el primer Store si no existe.
func NewFileCustody(path string) *FileCustody {
	return &FileCustody{path: path}
}

// Store persiste la DEK en el archivo con permisos 0600, creando el directorio contenedor (0700) si hace
// falta. Devuelve ErrKeySize si la DEK no mide KeySize bytes. Sobrescribe cualquier DEK previa.
func (c *FileCustody) Store(dek []byte) error {
	if len(dek) != KeySize {
		return ErrKeySize
	}
	if err := os.MkdirAll(filepath.Dir(c.path), dirPerm); err != nil {
		return fmt.Errorf("no se pudo crear el directorio de custodia: %w", err)
	}
	// WriteFile aplica filePerm solo al CREAR; si el archivo ya existía con otros permisos no los cambia.
	// Forzamos 0600 explícitamente tras escribir para que la garantía se cumpla incluso sobre un archivo
	// preexistente.
	if err := os.WriteFile(c.path, dek, filePerm); err != nil {
		return fmt.Errorf("no se pudo escribir la DEK: %w", err)
	}
	if err := os.Chmod(c.path, filePerm); err != nil {
		return fmt.Errorf("no se pudo fijar el permiso 0600 de la DEK: %w", err)
	}
	return nil
}

// Load recupera la DEK custodiada. Devuelve ErrNoKey (envuelto) si el archivo no existe todavía, o un
// error claro si el contenido no mide KeySize bytes (archivo corrupto/manipulado).
func (c *FileCustody) Load() ([]byte, error) {
	dek, err := os.ReadFile(c.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNoKey, c.path)
		}
		return nil, fmt.Errorf("no se pudo leer la DEK: %w", err)
	}
	if len(dek) != KeySize {
		return nil, fmt.Errorf("DEK custodiada corrupta: mide %d bytes, se esperaban %d", len(dek), KeySize)
	}
	return dek, nil
}

// Exists indica si hay una DEK custodiada (el archivo existe y es regular). Cualquier error de acceso
// distinto a "no existe" se trata como ausencia: el consumidor confirma con Load, que sí devuelve el
// error detallado.
func (c *FileCustody) Exists() bool {
	info, err := os.Stat(c.path)
	if err != nil {
		return false
	}
	return info.Mode().IsRegular()
}

// Clear elimina la DEK custodiada (borrado quirúrgico por sesión, ADR-0016 §3 / Plan 008 R5). Es
// IDEMPOTENTE: si el archivo ya no existe, no es un error. Opera solo sobre c.path (la entrada de ESTA
// sesión): no toca el store ni la custodia de ninguna otra. La usa el Manager al desvincular una sesión,
// vía el puerto de limpieza por sesión (sin afectar a las demás entradas, design §3).
func (c *FileCustody) Clear() error {
	if err := os.Remove(c.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("no se pudo borrar la DEK custodiada: %w", err)
	}
	return nil
}
