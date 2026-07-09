package edgemigrate

import (
	"sync"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// custody_fake_test.go — doble EN MEMORIA del puerto app.KeyCustody para los tests de restauración
// (Plan 023 · T2). RestoreArchivedActiveSessions re-ubica la DEK con keycustody.NewFileCustody, que en
// darwin escribe al Keychain REAL de la máquina. Si los tests usaran ese backend, contaminarían el
// Keychain con las cuentas de fixture (tid1/tid2/…) en cada corrida. Se inyecta con
// WithCustodyFactory(newMemCustodyFactory()) para quedarse en memoria: headless y sin efectos globales.

// memCustody es una entrada (una DEK) del doble, indexada por el path de la DEK.
type memCustody struct {
	backing *custodyBacking
	key     string
}

// custodyBacking es el almacén compartido por las memCustody creadas por un mismo factory (mutex para
// -race).
type custodyBacking struct {
	mu   sync.Mutex
	deks map[string][]byte
}

// newMemCustodyFactory crea un factory para RestoreArchivedActiveSessions (WithCustodyFactory) con un
// backing propio: cada Store queda en memoria, nunca en el Keychain real.
func newMemCustodyFactory() func(path string) app.KeyCustody {
	b := &custodyBacking{deks: make(map[string][]byte)}
	return func(path string) app.KeyCustody {
		return memCustody{backing: b, key: path}
	}
}

// Store persiste una copia de la DEK (32 bytes exactos, como el puerto real).
func (c memCustody) Store(dek []byte) error {
	if len(dek) != keycustody.KeySize {
		return keycustody.ErrKeySize
	}
	c.backing.mu.Lock()
	defer c.backing.mu.Unlock()
	cp := make([]byte, len(dek))
	copy(cp, dek)
	c.backing.deks[c.key] = cp
	return nil
}

// Load devuelve una copia de la DEK custodiada, o keycustody.ErrNoKey si no hay ninguna.
func (c memCustody) Load() ([]byte, error) {
	c.backing.mu.Lock()
	defer c.backing.mu.Unlock()
	dek, ok := c.backing.deks[c.key]
	if !ok {
		return nil, keycustody.ErrNoKey
	}
	cp := make([]byte, len(dek))
	copy(cp, dek)
	return cp, nil
}

// Exists indica si hay DEK custodiada para este slot.
func (c memCustody) Exists() bool {
	c.backing.mu.Lock()
	defer c.backing.mu.Unlock()
	_, ok := c.backing.deks[c.key]
	return ok
}
