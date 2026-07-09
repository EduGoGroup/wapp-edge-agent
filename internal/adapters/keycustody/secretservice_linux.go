//go:build linux

package keycustody

// secretservice_linux.go es la custodia de la DEK en Linux vía Secret Service (D-Bus,
// org.freedesktop.secrets: GNOME Keyring / KWallet) usando github.com/zalando/go-keyring. La DEK se guarda
// como un secreto del keyring: service="com.wapp.edge", user=<session_id> (derivado del basename de la
// ruta, coherente con la custodia multi-entrada del Plan 022), valor = DEK en base64. Solo la sesión de
// escritorio del MISMO usuario puede leerla; jamás sube a la nube ni cruza el plano de control
// (zero-knowledge, ADR-0007).
//
// Pure-Go: go-keyring habla D-Bus con godbus, SIN CGO (el CGO solo lo requieren freebsd/dragonfly, no
// linux) — no rompe el cross-compile (ADR-0002). Reusa la pieza pure-Go migrateFileToCustody (migrate.go)
// para importar la DEK desde el archivo plano legacy verificando la relectura ANTES de borrarlo.
//
// FALLBACK CRÍTICO (design §6; riesgo headless): si el entorno NO tiene Secret Service (servidor headless,
// sin escritorio/D-Bus), NewFileCustody degrada con gracia al archivo 0600 (newFileCustody) para que el
// test delegado (Plan 024 · T4) no se rompa por falta de keyring. La disponibilidad se detecta UNA vez en
// el constructor (probe) y se recuerda.

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	keyring "github.com/zalando/go-keyring"
)

// secretServiceName es el service (colección) de todos los secretos de DEK del Edge en el Secret Service
// (coherente con el service del Keychain en darwin; el user discrimina el session_id).
const secretServiceName = "com.wapp.edge"

// secretServiceProbeUser es un user de sondeo para detectar si el Secret Service responde (nunca se crea).
const secretServiceProbeUser = "__wapp_probe__"

// secretServiceCustody implementa app.KeyCustody (Store/Load/Exists) + Clear() respaldándose en el Secret
// Service. legacyPath (archivo plano heredado de file.go) es solo el ORIGEN de la migración one-shot. Si el
// keyring no está disponible, fallback != nil y todo se delega al archivo 0600 (misma semántica que
// FileCustody).
type secretServiceCustody struct {
	account    string
	legacyPath string
	fallback   *FileCustody
}

// NewFileCustody construye la custodia de la DEK en Linux. MISMA firma que la impl pure-Go (file.go) para
// que el wiring (cmd/agent, sessionmgr, edgemigrate) no cambie. Del basename de path deriva el user del
// keyring (el session_id). Si el Secret Service no responde (headless/sin D-Bus), degrada al archivo 0600.
func NewFileCustody(path string) *secretServiceCustody {
	c := &secretServiceCustody{account: accountFromPath(path), legacyPath: path}
	if !secretServiceAvailable() {
		c.fallback = newFileCustody(path)
	}
	return c
}

// secretServiceAvailable prueba un Get sobre un user de sondeo: si el backend responde (aunque sea con
// ErrNotFound) el Secret Service está disponible; cualquier otro error (sin D-Bus/daemon/colección) ⇒ no
// disponible, y NewFileCustody degrada al archivo 0600.
func secretServiceAvailable() bool {
	_, err := keyring.Get(secretServiceName, secretServiceProbeUser)
	return err == nil || errors.Is(err, keyring.ErrNotFound)
}

// accountFromPath deriva el user del keyring del basename de la ruta, sin extensión (keys/<session_id>.key
// → <session_id>; dek.key → "dek"). Nunca vacío: cae a "default". Réplica de la de darwin (tag-aislada).
func accountFromPath(path string) string {
	base := filepath.Base(path)
	base = strings.TrimSuffix(base, filepath.Ext(base))
	if base == "" || base == "." || base == string(filepath.Separator) {
		return "default"
	}
	return base
}

// Store persiste la DEK (base64) como secreto del keyring (sobrescribe la previa de la misma cuenta).
// Devuelve ErrKeySize si no mide KeySize bytes. NO toca el archivo legacy: eso lo hacen Load/Clear.
func (c *secretServiceCustody) Store(dek []byte) error {
	if len(dek) != KeySize {
		return ErrKeySize
	}
	if c.fallback != nil {
		return c.fallback.Store(dek)
	}
	enc := base64.StdEncoding.EncodeToString(dek)
	if err := keyring.Set(secretServiceName, c.account, enc); err != nil {
		return fmt.Errorf("keycustody: el Secret Service no pudo guardar la DEK (cuenta %q): %w", c.account, err)
	}
	return nil
}

// Load recupera la DEK del keyring si la tiene (y retira cualquier archivo plano legacy). Si no la tiene
// pero existe el archivo legacy, MIGRA (importa, verifica la relectura y borra el archivo). Devuelve
// ErrNoKey si no hay ni secreto ni archivo.
func (c *secretServiceCustody) Load() ([]byte, error) {
	if c.fallback != nil {
		return c.fallback.Load()
	}
	dek, err := c.keyringLoad()
	if err == nil {
		c.removeLegacy() // el keystore manda: no dejar la DEK también en disco plano
		return dek, nil
	}
	if !errors.Is(err, keyring.ErrNotFound) {
		return nil, err
	}

	// Miss: migración one-shot desde el archivo plano legacy (verifica la relectura ANTES de borrar).
	migrated, mErr := c.migrateFromLegacy()
	if mErr != nil {
		return nil, mErr
	}
	if migrated != nil {
		return migrated, nil
	}
	return nil, fmt.Errorf("%w: Secret Service cuenta %q", ErrNoKey, c.account)
}

// Exists indica si hay DEK disponible: el secreto del keyring, o el archivo legacy pendiente de migrar.
func (c *secretServiceCustody) Exists() bool {
	if c.fallback != nil {
		return c.fallback.Exists()
	}
	if _, err := keyring.Get(secretServiceName, c.account); err == nil {
		return true
	}
	return c.legacyExists()
}

// Clear borra el secreto del keyring Y cualquier archivo plano legacy (borrado quirúrgico por sesión,
// ADR-0016 §3). Idempotente: borrar algo ausente no es error.
func (c *secretServiceCustody) Clear() error {
	if c.fallback != nil {
		return c.fallback.Clear()
	}
	err := keyring.Delete(secretServiceName, c.account)
	rmErr := c.removeLegacyErr()
	if err != nil && !errors.Is(err, keyring.ErrNotFound) {
		return fmt.Errorf("keycustody: el Secret Service no pudo borrar la DEK (cuenta %q): %w", c.account, err)
	}
	return rmErr
}

// keyringLoad lee la DEK DIRECTAMENTE del keyring (sin migración). Devuelve keyring.ErrNotFound si no
// existe. Usado por Load y por la verificación de la migración (sink), para no recursar.
func (c *secretServiceCustody) keyringLoad() ([]byte, error) {
	enc, err := keyring.Get(secretServiceName, c.account)
	if err != nil {
		return nil, err // incluye keyring.ErrNotFound
	}
	dek, derr := base64.StdEncoding.DecodeString(enc)
	if derr != nil {
		return nil, fmt.Errorf("keycustody: la DEK del Secret Service está corrupta (base64, cuenta %q): %w", c.account, derr)
	}
	if len(dek) != KeySize {
		return nil, fmt.Errorf("keycustody: la DEK del Secret Service mide %d bytes, se esperaban %d (cuenta %q)", len(dek), KeySize, c.account)
	}
	return dek, nil
}

// migrateFromLegacy ejecuta la migración archivo→keyring reusando migrateFileToCustody. secretServiceSink
// expone Store/keyringLoad DIRECTOS para que la verificación no vuelva a entrar en Load (recursión).
func (c *secretServiceCustody) migrateFromLegacy() ([]byte, error) {
	dek, migrated, err := migrateFileToCustody(c.legacyPath, secretServiceSink{c})
	if err != nil {
		return nil, err
	}
	if !migrated {
		return nil, nil
	}
	return dek, nil
}

// secretServiceSink adapta el keyring al puerto dekSink de la migración con operaciones DIRECTAS (sin la
// lógica de migración de Load), evitando recursión durante la verificación.
type secretServiceSink struct{ c *secretServiceCustody }

func (s secretServiceSink) Store(dek []byte) error { return s.c.Store(dek) }
func (s secretServiceSink) Load() ([]byte, error)  { return s.c.keyringLoad() }

// legacyExists indica si el archivo plano legacy existe y es regular.
func (c *secretServiceCustody) legacyExists() bool {
	if c.legacyPath == "" {
		return false
	}
	info, err := os.Stat(c.legacyPath)
	return err == nil && info.Mode().IsRegular()
}

// removeLegacy borra el archivo plano legacy si existe, ignorando el error (best-effort).
func (c *secretServiceCustody) removeLegacy() { _ = c.removeLegacyErr() }

// removeLegacyErr borra el archivo plano legacy de forma idempotente (ausente ⇒ no es error).
func (c *secretServiceCustody) removeLegacyErr() error {
	if c.legacyPath == "" {
		return nil
	}
	if err := os.Remove(c.legacyPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("no se pudo borrar el archivo plano legacy %s: %w", c.legacyPath, err)
	}
	return nil
}
