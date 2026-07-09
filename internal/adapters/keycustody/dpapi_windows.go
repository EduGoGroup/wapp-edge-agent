//go:build windows

package keycustody

// dpapi_windows.go es la custodia de la DEK en Windows vía DPAPI (Data Protection API): cifra la DEK con
// CryptProtectData (ámbito USUARIO actual, CRYPTPROTECT_UI_FORBIDDEN) y guarda el blob cifrado resultante
// en <path>.dpapi (directorio 0700, archivo 0600). El blob SOLO lo puede descifrar (CryptUnprotectData) el
// MISMO usuario Windows en la MISMA máquina: ni otro usuario local, ni la nube, ni el plano de control
// pueden (zero-knowledge, ADR-0007; la DEK jamás sube ni cruza CloudLink).
//
// Pure-Go: usa golang.org/x/sys/windows (syscalls a crypt32.dll), SIN CGO — no rompe el cross-compile del
// resto (ADR-0002). Reusa la pieza pure-Go migrateFileToCustody (migrate.go) para importar la DEK desde el
// archivo plano legacy y verificar la relectura ANTES de borrarlo. Si DPAPI no respondiera en el entorno,
// degrada con gracia al archivo 0600 (newFileCustody) para no bloquear el arranque (Plan 024 · D2).

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

// dpapiSuffix es la extensión del archivo que guarda la DEK cifrada por DPAPI, junto a la ruta legacy.
const dpapiSuffix = ".dpapi"

// dpapiCustody implementa app.KeyCustody (Store/Load/Exists) + Clear() respaldándose en DPAPI. blobPath
// (<legacyPath>.dpapi) es el ALMACÉN real (blob cifrado); legacyPath (archivo plano heredado de file.go) es
// solo el ORIGEN de la migración one-shot. Si DPAPI no está disponible, fallback != nil y todo se delega al
// archivo 0600 (misma semántica que FileCustody).
type dpapiCustody struct {
	blobPath   string
	legacyPath string
	fallback   *FileCustody
}

// NewFileCustody construye la custodia de la DEK en Windows. MISMA firma que la impl pure-Go (file.go) para
// que el wiring (cmd/agent, sessionmgr, edgemigrate) no cambie. path es la ruta legacy de la DEK; el blob
// cifrado va a path+".dpapi". Si DPAPI no responde (entorno atípico), degrada al archivo 0600.
func NewFileCustody(path string) *dpapiCustody {
	c := &dpapiCustody{blobPath: path + dpapiSuffix, legacyPath: path}
	if !dpapiAvailable() {
		c.fallback = newFileCustody(path)
	}
	return c
}

// dpapiAvailable prueba un round-trip protect→unprotect de un valor trivial: si DPAPI está operativo
// devuelve true; si CryptProtectData/CryptUnprotectData fallan, false ⇒ se degrada al archivo 0600.
func dpapiAvailable() bool {
	enc, err := dpapiProtect([]byte("wapp-dpapi-probe"))
	if err != nil {
		return false
	}
	_, err = dpapiUnprotect(enc)
	return err == nil
}

// Store cifra la DEK con DPAPI y persiste el blob en <path>.dpapi (0600). Devuelve ErrKeySize si no mide
// KeySize bytes. NO toca el archivo legacy: la migración/limpieza la hacen Load/Clear.
func (c *dpapiCustody) Store(dek []byte) error {
	if len(dek) != KeySize {
		return ErrKeySize
	}
	if c.fallback != nil {
		return c.fallback.Store(dek)
	}
	enc, err := dpapiProtect(dek)
	if err != nil {
		return fmt.Errorf("keycustody: DPAPI no pudo cifrar la DEK: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(c.blobPath), dirPerm); err != nil {
		return fmt.Errorf("no se pudo crear el directorio de custodia: %w", err)
	}
	if err := os.WriteFile(c.blobPath, enc, filePerm); err != nil {
		return fmt.Errorf("no se pudo escribir el blob DPAPI: %w", err)
	}
	if err := os.Chmod(c.blobPath, filePerm); err != nil {
		return fmt.Errorf("no se pudo fijar el permiso 0600 del blob DPAPI: %w", err)
	}
	return nil
}

// Load descifra la DEK del blob DPAPI si existe (y retira cualquier archivo plano legacy que quedara). Si
// no existe pero sí el archivo legacy, MIGRA (importa, verifica la relectura y borra el archivo). Devuelve
// ErrNoKey si no hay ni blob ni archivo.
func (c *dpapiCustody) Load() ([]byte, error) {
	if c.fallback != nil {
		return c.fallback.Load()
	}
	enc, err := os.ReadFile(c.blobPath)
	if err == nil {
		dek, derr := dpapiUnprotect(enc)
		if derr != nil {
			return nil, fmt.Errorf("keycustody: DPAPI no pudo descifrar la DEK (%s): %w", c.blobPath, derr)
		}
		if len(dek) != KeySize {
			return nil, fmt.Errorf("keycustody: la DEK DPAPI mide %d bytes, se esperaban %d", len(dek), KeySize)
		}
		c.removeLegacy() // el keystore manda: no dejar la DEK también en disco plano
		return dek, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("no se pudo leer el blob DPAPI: %w", err)
	}

	// Miss: migración one-shot desde el archivo plano legacy (verifica la relectura ANTES de borrar).
	dek, migrated, mErr := migrateFileToCustody(c.legacyPath, dpapiSink{c})
	if mErr != nil {
		return nil, mErr
	}
	if migrated {
		return dek, nil
	}
	return nil, fmt.Errorf("%w: %s", ErrNoKey, c.blobPath)
}

// Exists indica si hay DEK disponible: el blob DPAPI, o el archivo legacy pendiente de migrar.
func (c *dpapiCustody) Exists() bool {
	if c.fallback != nil {
		return c.fallback.Exists()
	}
	if info, err := os.Stat(c.blobPath); err == nil && info.Mode().IsRegular() {
		return true
	}
	return c.legacyExists()
}

// Clear borra el blob DPAPI Y cualquier archivo plano legacy (borrado quirúrgico por sesión, ADR-0016 §3).
// Idempotente: borrar algo ausente no es error.
func (c *dpapiCustody) Clear() error {
	if c.fallback != nil {
		return c.fallback.Clear()
	}
	var firstErr error
	if err := os.Remove(c.blobPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		firstErr = fmt.Errorf("no se pudo borrar el blob DPAPI: %w", err)
	}
	if err := c.removeLegacyErr(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// dpapiSink adapta la custodia DPAPI al puerto dekSink de la migración con operaciones DIRECTAS sobre el
// blob (sin re-entrar en Load), evitando recursión durante la verificación de la relectura.
type dpapiSink struct{ c *dpapiCustody }

func (s dpapiSink) Store(dek []byte) error { return s.c.Store(dek) }

func (s dpapiSink) Load() ([]byte, error) {
	enc, err := os.ReadFile(s.c.blobPath)
	if err != nil {
		return nil, err
	}
	return dpapiUnprotect(enc)
}

// legacyExists indica si el archivo plano legacy existe y es regular.
func (c *dpapiCustody) legacyExists() bool {
	if c.legacyPath == "" {
		return false
	}
	info, err := os.Stat(c.legacyPath)
	return err == nil && info.Mode().IsRegular()
}

// removeLegacy borra el archivo plano legacy si existe, ignorando el error (best-effort).
func (c *dpapiCustody) removeLegacy() { _ = c.removeLegacyErr() }

// removeLegacyErr borra el archivo plano legacy de forma idempotente (ausente ⇒ no es error).
func (c *dpapiCustody) removeLegacyErr() error {
	if c.legacyPath == "" {
		return nil
	}
	if err := os.Remove(c.legacyPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("no se pudo borrar el archivo plano legacy %s: %w", c.legacyPath, err)
	}
	return nil
}

// dpapiProtect cifra data con DPAPI (ámbito usuario, sin UI). Devuelve el blob cifrado (copiado a memoria
// de Go; el buffer que asigna Windows se libera con LocalFree).
func dpapiProtect(data []byte) ([]byte, error) {
	in := newBlob(data)
	var out windows.DataBlob
	if err := windows.CryptProtectData(in, nil, nil, 0, nil, windows.CRYPTPROTECT_UI_FORBIDDEN, &out); err != nil {
		return nil, err
	}
	defer localFree(&out)
	return blobBytes(&out), nil
}

// dpapiUnprotect descifra un blob DPAPI producido por dpapiProtect (mismo usuario/máquina).
func dpapiUnprotect(data []byte) ([]byte, error) {
	in := newBlob(data)
	var out windows.DataBlob
	if err := windows.CryptUnprotectData(in, nil, nil, 0, nil, windows.CRYPTPROTECT_UI_FORBIDDEN, &out); err != nil {
		return nil, err
	}
	defer localFree(&out)
	return blobBytes(&out), nil
}

// newBlob envuelve un []byte de Go en un windows.DataBlob para pasarlo a la API DPAPI.
func newBlob(d []byte) *windows.DataBlob {
	if len(d) == 0 {
		return &windows.DataBlob{}
	}
	return &windows.DataBlob{Size: uint32(len(d)), Data: &d[0]}
}

// blobBytes copia el contenido de un windows.DataBlob (asignado por Windows) a un []byte de Go.
func blobBytes(b *windows.DataBlob) []byte {
	if b.Size == 0 || b.Data == nil {
		return nil
	}
	out := make([]byte, b.Size)
	copy(out, unsafe.Slice(b.Data, b.Size))
	return out
}

// localFree libera el buffer que Windows asignó para el DataBlob de salida (LocalFree).
func localFree(b *windows.DataBlob) {
	if b.Data != nil {
		_, _ = windows.LocalFree(windows.Handle(uintptr(unsafe.Pointer(b.Data))))
	}
}
