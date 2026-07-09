//go:build !darwin

package keycustody

// file.go define FileCustody: la custodia en un archivo local en texto plano con permisos 0600 bajo un
// directorio 0700. Queda bajo //go:build !darwin (en darwin la sustituye el Keychain, keychain_darwin.go).
//
// Rol tras el Plan 024 · T2: FileCustody + su constructor NO exportado newFileCustody son el FALLBACK
// pure-Go que reusan los keystores del SO cuando NO están disponibles:
//   - windows → dpapi_windows.go define el NewFileCustody exportado (DPAPI); degrada a newFileCustody.
//   - linux   → secretservice_linux.go define el NewFileCustody exportado (Secret Service); degrada a
//     newFileCustody si no hay D-Bus/escritorio (headless).
//   - resto   → file_fallback.go expone NewFileCustody = newFileCustody directamente.
//
// ⚠️ DEUDA EXPLÍCITA (ADR-0007 / ADR-0013): el archivo plano NO es la custodia v1 — es el suelo de
// seguridad. Se guarda SIN cifrar a propósito: cifrar la DEK requeriría custodiar la llave que la cifra —
// justo lo que el keystore del SO (DPAPI/Secret Service) resuelve. Como todo depende del puerto
// app.KeyCustody, el cambio de backend es solo de adaptador.

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

// newFileCustody construye un FileCustody que custodia la DEK en path (p. ej. ".../keys/<id>.key"); su
// directorio contenedor se crea con permisos 0700 en el primer Store si no existe. NO exportado: el
// NewFileCustody exportado lo define cada plataforma (DPAPI/Secret Service/fallback) y delega aquí para el
// suelo pure-Go. Firma idéntica a la de darwin para no cambiar el wiring.
func newFileCustody(path string) *FileCustody {
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
