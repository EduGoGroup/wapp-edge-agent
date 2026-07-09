package cryptostore

// upgrade_test.go cubre la CARRERA del upgrade de esquema de whatsmeow sobre la BD ÚNICA COMPARTIDA
// (Plan 022 T3): N Containers construidos CONCURRENTEMENTE sobre una sola *sql.DB no deben chocar al crear
// la tabla de versión de whatsmeow. Reproduce el arranque en frío multi-sesión (Manager.Restore lanza una
// goroutine por sesión, cada una construye su Container → sqlstore.Upgrade sobre la MISMA db).

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestConcurrentUpgrade_SharedDB_NoRace: arranca N constructores de Container EN PARALELO sobre una única
// *sql.DB compartida (cada uno con SU DEK, como el modelo per-device) y verifica que NINGUNO falla con
// "already exists" al crear el esquema whatsmeow.
//
// SIN el fix (upgrade.go serializando por *sql.DB), con -race y N alto esto reproduce el bug del e2e vivo:
// dos sqlstore.Upgrade concurrentes corren el CREATE TABLE de whatsmeow_version a la vez y el 2º revienta
// con "failed to create version table: table whatsmeow_version already exists". Con el fix, el primer
// upgrade crea el esquema bajo el lock y los demás lo ven creado (no-op): los N tienen éxito.
//
// Ejecutar con `go test -race`: además del fallo funcional, el acceso concurrente sin serializar dispara
// el detector de carreras del runtime SQL.
func TestConcurrentUpgrade_SharedDB_NoRace(t *testing.T) {
	ctx := context.Background()
	db := openAt(t, filepath.Join(t.TempDir(), "shared.db"))
	defer func() { _ = db.Close() }()

	const n = 16 // suficiente para forzar el solape del CREATE TABLE inicial sin el candado.

	// Barrera de arranque: todas las goroutines esperan a que se libere para MAXIMIZAR la simultaneidad
	// del primer Upgrade (el instante exacto de la carrera), en vez de escalonarse por el coste de spawn.
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(n)
	done.Add(n)
	errs := make([]error, n)

	for i := 0; i < n; i++ {
		go func(idx int) {
			defer done.Done()
			dek := newDEK(t)
			ready.Done()
			<-start
			// OpenDeviceContainer → NewEncryptedContainer → newCryptoContainer → upgradeWhatsmeowSchema:
			// el mismo camino que recorre cada listener al restaurar sobre la BD única.
			_, errs[idx] = OpenDeviceContainer(ctx, db, DialectSQLite, dek)
		}(i)
	}

	ready.Wait() // todas listas y bloqueadas en <-start.
	close(start) // ¡arranque simultáneo!
	done.Wait()

	for i, err := range errs {
		if err != nil {
			// Falla explícita si reaparece la carrera del CREATE TABLE (o cualquier otro error de upgrade).
			if strings.Contains(err.Error(), "already exists") {
				t.Fatalf("container %d: reapareció la carrera del upgrade (CREATE TABLE concurrente): %v", i, err)
			}
			t.Fatalf("container %d: OpenDeviceContainer falló: %v", i, err)
		}
	}
}
