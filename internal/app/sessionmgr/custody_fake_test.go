package sessionmgr

import (
	"sync"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// custody_fake_test.go — doble EN MEMORIA del puerto app.KeyCustody para los tests de sessionmgr.
//
// Por qué existe (Plan 023 · T2): en darwin, keycustody.NewFileCustody devuelve la custodia sobre el
// Keychain REAL de la máquina. Si los tests usaran ese backend (vía Manager.custodyFor), escribirían al
// Keychain global compartido con UUIDs de fixture fijos → colisiones (errSecDuplicateItem bajo -race) y
// contaminación de la máquina/CI. El Manager inyecta la custodia por factory (Manager.newCustody); estos
// tests le pasan newMemCustodyFactory() para quedarse en memoria: headless, determinista y sin estado
// global entre corridas. NO debilita las aserciones: se sigue verificando Store/Load/Exists/Clear y el
// aislamiento por sesión; solo cambia el backend por uno de prueba (la verificación de que la DEK real no
// queda en disco plano vive en el paquete keycustody, donde corre contra el Keychain real).

// memCustody es una entrada (una sesión) del doble. Reproduce la semántica de persistencia del backend
// real: las custodias del MISMO path comparten slot en el backing, así que Store en una y Load en otra
// (p. ej. dos custodyFor(id) del mismo id) ven la misma DEK.
type memCustody struct {
	backing *custodyBacking
	key     string // path de la DEK (Layout.DEKPath(id)); discrimina la sesión
}

// custodyBacking es el almacén COMPARTIDO por todas las memCustody de UN Manager de test, indexado por
// path de DEK. El mutex lo hace seguro para los listeners/pairings concurrentes (y para -race).
type custodyBacking struct {
	mu   sync.Mutex
	deks map[string][]byte
}

// newMemCustodyFactory crea un factory para Manager.newCustody con un backing PROPIO (aislado por test):
// cada path devuelve una memCustody sobre el mismo almacén, de modo que dos custodyFor(id) comparten DEK.
func newMemCustodyFactory() func(path string) app.KeyCustody {
	b := &custodyBacking{deks: make(map[string][]byte)}
	return func(path string) app.KeyCustody {
		return memCustody{backing: b, key: path}
	}
}

// Store persiste una COPIA de la DEK (32 bytes exactos, como el puerto real). Sobrescribe la previa.
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

// Load devuelve una COPIA de la DEK custodiada, o keycustody.ErrNoKey si no hay ninguna (como el real).
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

// Clear borra la DEK de este slot. Idempotente (borrar algo ausente no es error), como el puerto real;
// satisface la interfaz `clearer` que el código type-asserta en el borrado quirúrgico (pairing/unlink).
func (c memCustody) Clear() error {
	c.backing.mu.Lock()
	defer c.backing.mu.Unlock()
	delete(c.backing.deks, c.key)
	return nil
}
