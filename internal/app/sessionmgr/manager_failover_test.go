package sessionmgr

// manager_failover_test.go ejercita el FAILOVER MULTI-DISPOSITIVO POR NÚMERO (Plan 022 T5, design §6/
// §10.F/§10.G) sobre la BD ÚNICA REAL: el cupo por cuenta (off por defecto), la asignación de rol
// primary/standby con >1, la promoción del standby al caer el primary (events.LoggedOut) y la
// persistencia local del estado 'loggedout' con re-escaneo del MISMO número a la MISMA cuenta.
//
// Reusa el andamiaje de los otros tests del paquete (fakePairer/discardQR de manager_pair_test.go,
// fakeFabric/mustLive de manager_listen_test.go, countAccount de manager_bd_unica_test.go): aquí solo
// se añade el factory de "mismo número" y el constructor con cupo configurable.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
)

// sameNumberFactory emula app.Pair para el MISMO número en cada llamada: sella una DEK distinta y devuelve
// un JID del mismo `number` con sufijo de device creciente (number:1, number:2, …). Así todas las sesiones
// comparten self_pn (misma cuenta por resolveAccount) pero son DISPOSITIVOS distintos (jid único por device),
// que es justo el escenario del failover por número y del re-escaneo.
func sameNumberFactory(number string) pairFactory {
	calls := 0
	return func(custody app.KeyCustody, _ *sql.DB, _ app.QRSink) pairRunner {
		calls++
		return &fakePairer{
			custody: custody,
			jid:     fmt.Sprintf("%s:%d@s.whatsapp.net", number, calls),
			dek:     bytes.Repeat([]byte{byte(calls)}, 32),
			sealDEK: true,
		}
	}
}

// newFailoverManager arma un Manager sobre la BD ÚNICA REAL con el cupo multi-dispositivo `limit`, el factory
// de pairing dado y un fakeFabric como escucha (sin whatsmeow, sin mux: onLoggedOut opera solo metadatos).
func newFailoverManager(t *testing.T, limit int, factory pairFactory) (*Manager, *sessionstore.Store, *sql.DB) {
	t.Helper()
	base := filepath.Join(t.TempDir(), "edge-data")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatalf("crear data_dir: %v", err)
	}
	database, err := wappdb.OpenAndMigrate(context.Background(), filepath.Join(base, "edge.db"))
	if err != nil {
		t.Fatalf("abrir/migrar la BD única: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store := sessionstore.New(database)
	m := NewManager(NewLayout(base), store, 5, testLogger(),
		WithSharedDB(database, wappdb.DialectSQLite),
		WithMultiDevicePerAccount(limit))
	m.newPairer = factory
	m.newListener = newFakeFabric().factory
	return m, store, database
}

// TestManager_MultiDeviceOff_RejectsSecondSameNumber (DoD T5): con la opción OFF (default 1), parear un
// SEGUNDO device del MISMO número se rechaza (ErrAccountAtCapacity) sin dejar restos; queda 1 device primary.
func TestManager_MultiDeviceOff_RejectsSecondSameNumber(t *testing.T) {
	ctx := context.Background()
	m, store, _ := newFailoverManager(t, 1, sameNumberFactory("56911110000"))
	defer m.Stop()

	if _, err := m.Pair(ctx, discardQR{}); err != nil {
		t.Fatalf("Pair #1: %v", err)
	}
	if _, err := m.Pair(ctx, discardQR{}); !errors.Is(err, ErrAccountAtCapacity) {
		t.Fatalf("el 2.º device del mismo número con cupo 1 debería dar ErrAccountAtCapacity, got %v", err)
	}

	rows, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("debería quedar 1 device tras el rechazo (sin restos), hay %d", len(rows))
	}
	if rows[0].Role != domain.DeviceRolePrimary {
		t.Fatalf("el único device debería ser primary, got %q", rows[0].Role)
	}
	if got := len(m.List()); got != 1 {
		t.Fatalf("el registro vivo debería tener 1 device, got %d", got)
	}
}

// TestManager_MultiDeviceOn_TwoLivePlusPromotion (DoD T5): con la opción ON (cupo 2), dos devices del MISMO
// número quedan vivos (primary + standby); al recibir LoggedOut el primary, se persiste 'loggedout' local y
// el standby se promueve a primary (failover). RESILIENCIA, no sigilo.
func TestManager_MultiDeviceOn_TwoLivePlusPromotion(t *testing.T) {
	ctx := context.Background()
	m, store, _ := newFailoverManager(t, 2, sameNumberFactory("56922220000"))
	defer m.Stop()

	res1, err := m.Pair(ctx, discardQR{})
	if err != nil {
		t.Fatalf("Pair #1: %v", err)
	}
	res2, err := m.Pair(ctx, discardQR{})
	if err != nil {
		t.Fatalf("Pair #2 (mismo número, cupo 2): %v", err)
	}
	if got := len(m.List()); got != 2 {
		t.Fatalf("se esperaban 2 devices vivos del mismo número, got %d", got)
	}

	dev1, _ := store.Get(ctx, res1.SessionID)
	dev2, _ := store.Get(ctx, res2.SessionID)
	if dev1.AccountID == "" || dev1.AccountID != dev2.AccountID {
		t.Fatalf("el mismo número debería dar la MISMA cuenta: a1=%q a2=%q", dev1.AccountID, dev2.AccountID)
	}
	if dev1.Role != domain.DeviceRolePrimary {
		t.Fatalf("el 1.º device debería ser primary, got %q", dev1.Role)
	}
	if dev2.Role != domain.DeviceRoleStandby {
		t.Fatalf("el 2.º device del mismo número debería ser standby, got %q", dev2.Role)
	}

	// El primary recibe LoggedOut: se persiste 'loggedout' local y el standby toma el relevo.
	m.onLoggedOut(mustLive(t, m, res1.SessionID))

	dev1b, _ := store.Get(ctx, res1.SessionID)
	if dev1b.State != domain.SessionStateLoggedOut {
		t.Fatalf("el primary caído debería quedar 'loggedout' persistido, got %q", dev1b.State)
	}
	dev2b, _ := store.Get(ctx, res2.SessionID)
	if dev2b.Role != domain.DeviceRolePrimary {
		t.Fatalf("el standby debería promoverse a primary al caer el primary, got %q", dev2b.Role)
	}
}

// TestManager_LoggedOut_PersistsAndReScanSameAccount (DoD T5): LoggedOut persiste 'loggedout' local; el
// RE-ESCANEO del mismo número (con cupo 1) vincula a la cuenta EXISTENTE (no crea cuenta nueva) porque el
// zombie no ocupa cupo (reusa sessionstore.resolveAccount por self_pn).
func TestManager_LoggedOut_PersistsAndReScanSameAccount(t *testing.T) {
	ctx := context.Background()
	m, store, database := newFailoverManager(t, 1, sameNumberFactory("56933330000"))
	defer m.Stop()

	res1, err := m.Pair(ctx, discardQR{})
	if err != nil {
		t.Fatalf("Pair #1: %v", err)
	}
	dev1, _ := store.Get(ctx, res1.SessionID)
	acc1 := dev1.AccountID

	// LoggedOut: persiste el estado local (lo que T4 dejó pendiente).
	m.onLoggedOut(mustLive(t, m, res1.SessionID))
	dev1b, _ := store.Get(ctx, res1.SessionID)
	if dev1b.State != domain.SessionStateLoggedOut {
		t.Fatalf("LoggedOut debería persistir 'loggedout' local, got %q", dev1b.State)
	}

	// Re-escaneo del MISMO número: nuevo device, MISMA cuenta, nuevo primary (el zombie no ocupa cupo).
	res2, err := m.Pair(ctx, discardQR{})
	if err != nil {
		t.Fatalf("re-escaneo tras LoggedOut (cupo 1) no debería rechazarse: %v", err)
	}
	dev2, _ := store.Get(ctx, res2.SessionID)
	if dev2.AccountID != acc1 {
		t.Fatalf("el re-escaneo debería colgar de la MISMA cuenta: acc1=%q acc2=%q", acc1, dev2.AccountID)
	}
	if dev2.Role != domain.DeviceRolePrimary {
		t.Fatalf("el re-escaneo debería ser el nuevo primary del número, got %q", dev2.Role)
	}
	if n := countAccount(t, database, acc1); n != 1 {
		t.Fatalf("el número debería tener UNA sola cuenta (sin silo nuevo), got %d", n)
	}
}
