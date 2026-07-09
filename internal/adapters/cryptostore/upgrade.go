package cryptostore

// upgrade.go SERIALIZA el upgrade de esquema de whatsmeow (sqlstore.Container.Upgrade) por *sql.DB, para
// eliminar una CARRERA introducida por la BD ÚNICA COMPARTIDA (Plan 022 T3).
//
// EL PORQUÉ (bug del e2e vivo, arranque en frío con 2+ sesiones): sobre la BD única, el Manager arranca
// los listeners CONCURRENTEMENTE (sessionmgr.Manager.Restore → una goroutine runListener por sesión), y
// cada listener construye SU Container cifrado (OpenDeviceContainer → newCryptoContainer), que llama a
// sqlstore.Upgrade sobre la MISMA *sql.DB. El upgrade de whatsmeow crea primero su tabla de versión
// (whatsmeow_version) con un CREATE TABLE NO idempotente: dos upgrades concurrentes chocan y el 2º revienta
//
//	"failed to create version table: table whatsmeow_version already exists"
//
// El error se autorrecupera en el retry (el 2º upgrade ya ve la tabla y omite), pero deja un WARN espurio
// de "sesión degradada" + 1s de backoff en CADA arranque en frío multi-sesión. En el modelo viejo (.db por
// sesión) no ocurría: cada upgrade corría sobre SU archivo/handle. Es un defecto de concurrencia propio de
// compartir la *sql.DB, no de la cripto.
//
// LA SOLUCIÓN (mínima, dialecto-agnóstica): un mutex POR *sql.DB alrededor SOLO del Upgrade. El upgrade de
// whatsmeow es idempotente UNA VEZ que existe la tabla de versión; la carrera es exclusivamente el
// CREATE TABLE inicial. Serializar por DB hace que el primer Upgrade cree el esquema y los demás lo vean ya
// creado (no-op), sin WARNs ni backoff. Se elige mutex (no sync.Once) para que un fallo transitorio del
// upgrade se pueda REINTENTAR (Once cachearía el error de por vida). Y se llavea por *sql.DB (no un lock
// global) para no serializar arranques de BDs independientes (p. ej. varios stores en un test). Vale igual
// para SQLite y Postgres: el lock envuelve la llamada, no asume nada del motor.

import (
	"context"
	"database/sql"
	"sync"

	"go.mau.fi/whatsmeow/store/sqlstore"
)

// Registro de mutex por *sql.DB. upgradeRegistryMu protege el mapa; cada entrada serializa los Upgrade de
// ESA db. El mapa crece con cada *sql.DB distinta y no se purga: en producción hay una sola BD (una entrada)
// y en tests las *sql.DB son efímeras y pocas, así que el "leak" es acotado e irrelevante frente a la
// simplicidad de no rastrear cierres de db aquí (el ciclo de vida de la db lo posee el Manager, no esta capa).
var (
	upgradeRegistryMu sync.Mutex
	upgradeLocks      = map[*sql.DB]*sync.Mutex{}
)

// upgradeLockFor devuelve (creando si hace falta) el mutex que serializa los Upgrade de db.
func upgradeLockFor(db *sql.DB) *sync.Mutex {
	upgradeRegistryMu.Lock()
	defer upgradeRegistryMu.Unlock()
	mu, ok := upgradeLocks[db]
	if !ok {
		mu = &sync.Mutex{}
		upgradeLocks[db] = mu
	}
	return mu
}

// upgradeWhatsmeowSchema ejecuta inner.Upgrade(ctx) sosteniendo el lock POR db durante SOLO esa llamada, de
// modo que N constructores de Container concurrentes sobre la MISMA db no corran el CREATE TABLE inicial en
// paralelo. Fuera del upgrade no se retiene lock alguno: el resto del arranque sigue concurrente.
func upgradeWhatsmeowSchema(ctx context.Context, inner *sqlstore.Container, db *sql.DB) error {
	mu := upgradeLockFor(db)
	mu.Lock()
	defer mu.Unlock()
	return inner.Upgrade(ctx)
}
