// Package keycustody contiene adaptadores del puerto app.KeyCustody.
//
// FileCustody es la custodia PROVISIONAL del spike: persiste la DEK en un
// archivo local en texto plano con permisos 0600.
//
// ⚠️ DEUDA EXPLÍCITA (ADR-0007 / ADR-0013): este adaptador NO es la decisión v1.
// Antes de cualquier release debe sustituirse por el keystore del SO (DPAPI en
// Windows, Keychain en macOS, Secret Service en Linux). El custodio headless del
// appliance Linux queda como punto abierto (ADR-0014). Como el cambio es solo de
// adaptador (el resto de la app depende del puerto app.KeyCustody), esta deuda
// está acotada a este paquete.
//
// El archivo se guarda SIN cifrar a propósito: en el spike la protección es solo
// el permiso 0600 (lectura/escritura únicamente para el dueño) sobre un
// directorio 0700. No se cifra porque cifrar la DEK requeriría a su vez custodiar
// la llave que la cifra — exactamente el problema que el keystore del SO resuelve
// en v1. Documentado como provisional para no dar falsa sensación de seguridad.
package keycustody

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// KeySize es el tamaño exacto en bytes de la DEK (AES-256). Coincide con
// envelope.DEKSize; se replica aquí como constante local para no acoplar el
// adaptador al paquete envelope.
const KeySize = 32

// dirPerm y filePerm son los permisos del directorio contenedor y del archivo
// de la DEK: solo el dueño puede entrar al directorio (0700) y leer/escribir el
// archivo (0600).
const (
	dirPerm  os.FileMode = 0o700
	filePerm os.FileMode = 0o600
)

// ErrKeySize indica que la DEK pasada a Store no mide exactamente KeySize bytes.
var ErrKeySize = fmt.Errorf("la DEK debe medir exactamente %d bytes (AES-256)", KeySize)

// ErrNoKey indica que se intentó Load una DEK que aún no ha sido custodiada.
var ErrNoKey = errors.New("no hay DEK custodiada en la ruta indicada")

// FileCustody implementa app.KeyCustody persistiendo la DEK en un archivo local.
//
// Es la custodia provisional del spike (ver el doc del paquete). La ruta del
// archivo se inyecta por construcción: nunca se hardcodea.
type FileCustody struct {
	path string
}

// NewFileCustody construye un FileCustody que custodia la DEK en path.
// La ruta debe ser un archivo (p. ej. ".../dek.key"); su directorio contenedor
// se crea con permisos 0700 en el primer Store si no existe.
func NewFileCustody(path string) *FileCustody {
	return &FileCustody{path: path}
}

// Store persiste la DEK en el archivo con permisos 0600, creando el directorio
// contenedor (0700) si hace falta. Devuelve ErrKeySize si la DEK no mide
// KeySize bytes. Sobrescribe cualquier DEK previa.
func (c *FileCustody) Store(dek []byte) error {
	if len(dek) != KeySize {
		return ErrKeySize
	}
	if err := os.MkdirAll(filepath.Dir(c.path), dirPerm); err != nil {
		return fmt.Errorf("no se pudo crear el directorio de custodia: %w", err)
	}
	// WriteFile aplica filePerm solo al CREAR; si el archivo ya existía con otros
	// permisos no los cambia. Forzamos 0600 explícitamente tras escribir para que
	// la garantía se cumpla incluso sobre un archivo preexistente.
	if err := os.WriteFile(c.path, dek, filePerm); err != nil {
		return fmt.Errorf("no se pudo escribir la DEK: %w", err)
	}
	if err := os.Chmod(c.path, filePerm); err != nil {
		return fmt.Errorf("no se pudo fijar el permiso 0600 de la DEK: %w", err)
	}
	return nil
}

// Load recupera la DEK custodiada. Devuelve ErrNoKey (envuelto) si el archivo no
// existe todavía, o un error claro si el contenido no mide KeySize bytes
// (archivo corrupto/manipulado).
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

// Exists indica si hay una DEK custodiada (el archivo existe y es regular).
// Cualquier error de acceso distinto a "no existe" se trata como ausencia: el
// consumidor confirma con Load, que sí devuelve el error detallado.
func (c *FileCustody) Exists() bool {
	info, err := os.Stat(c.path)
	if err != nil {
		return false
	}
	return info.Mode().IsRegular()
}
