package sessionmgr

// manager_bd_unica_test.go ejercita el ciclo de vida y el BORRADO QUIRÚRGICO del Manager sobre la BD
// ÚNICA REAL (Plan 022 T3, decisión §10.A/I): a diferencia de manager_unlink_test.go (fakes en memoria),
// aquí hay un edge.db real con metadatos (accounts/devices) + material cifrado (msg_enc_*) + whatsmeow_*,
// para PROBAR que el borrado no deja HUÉRFANOS en ninguna tabla y que un reinicio restaura todos los
// devices activos. No usa whatsmeow (la escucha se cablea con el fakeFabric).

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"go.mau.fi/whatsmeow/types"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
)

// canonJID devuelve la forma canónica del JID (types.ParseJID(raw).String()) para que el JID almacenado en
// devices/msg_enc_* coincida EXACTO con el que cryptostore.DeleteDevice usa al borrar (evita depender de la
// normalización de whatsmeow en las aserciones de purga).
func canonJID(t *testing.T, raw string) string {
	t.Helper()
	jid, err := types.ParseJID(raw)
	if err != nil {
		t.Fatalf("ParseJID(%q): %v", raw, err)
	}
	return jid.String()
}

// newBDUnicaManager arma un Manager sobre la BD ÚNICA REAL (un edge.db con metadatos + msg_enc_* +
// whatsmeow_*) compartida vía WithSharedDB, con un sessionstore real. Devuelve el Manager, el store y la
// *sql.DB para inspeccionar el borrado sin huérfanos.
func newBDUnicaManager(t *testing.T) (*Manager, *sessionstore.Store, *sql.DB) {
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
		WithSharedDB(database, wappdb.DialectSQLite))
	m.newCustody = newMemCustodyFactory() // doble en memoria: no tocar el Keychain real (Plan 023 T2)
	return m, store, database
}

// seedActiveDevice da de alta un device ACTIVO en la BD única: fila devices/accounts (por self_pn), su DEK
// custodiada y filas de material CIFRADO (msg_enc_device + msg_enc_identities) llaveadas por su JID, para
// que el borrado quirúrgico tenga qué purgar. Devuelve el account_id resuelto.
func seedActiveDevice(t *testing.T, m *Manager, database *sql.DB, sessionID, selfPN, jid string) string {
	t.Helper()
	ctx := context.Background()
	if err := m.sessions.Upsert(ctx, domain.Session{
		SessionID: sessionID, JID: jid, SelfPN: selfPN, State: domain.SessionStateActive,
	}); err != nil {
		t.Fatalf("Upsert(%s): %v", sessionID, err)
	}
	custody, err := m.custodyFor(sessionID)
	if err != nil {
		t.Fatalf("custodyFor(%s): %v", sessionID, err)
	}
	if err := custody.Store(bytes.Repeat([]byte{0x22}, 32)); err != nil {
		t.Fatalf("Store DEK %s: %v", sessionID, err)
	}
	insertEncDevice(t, database, jid)
	got, err := m.sessions.Get(ctx, sessionID)
	if err != nil {
		t.Fatalf("Get(%s): %v", sessionID, err)
	}
	return got.AccountID
}

// insertEncDevice inserta filas de material CIFRADO de relleno para el JID (msg_enc_device +
// msg_enc_identities), como las que produciría un pairing real; el borrado (cryptostore.DeleteDevice) debe
// purgarlas por jid/our_jid. Los BLOB van con relleno x'00' (no se descifran en el borrado).
func insertEncDevice(t *testing.T, database *sql.DB, jid string) {
	t.Helper()
	ctx := context.Background()
	if _, err := database.ExecContext(ctx,
		`INSERT INTO msg_enc_device (jid, registration_id, signed_pre_key_id, noise_priv,
		   identity_priv, signed_pre_key_priv, signed_pre_key_sig, adv_secret_key, adv_details,
		   adv_account_sig, adv_account_sig_key, adv_device_sig)
		 VALUES (?, 1, 1, x'00', x'00', x'00', x'00', x'00', x'00', x'00', x'00', x'00')`, jid); err != nil {
		t.Fatalf("insert msg_enc_device(%s): %v", jid, err)
	}
	if _, err := database.ExecContext(ctx,
		`INSERT INTO msg_enc_identities (our_jid, their_id, identity) VALUES (?, 'peer@s.whatsapp.net', x'00')`,
		jid); err != nil {
		t.Fatalf("insert msg_enc_identities(%s): %v", jid, err)
	}
}

// countEnc cuenta las filas de msg_enc_device + msg_enc_identities del JID (0 ⇒ material purgado sin huérfanos).
func countEnc(t *testing.T, database *sql.DB, jid string) int {
	t.Helper()
	ctx := context.Background()
	var nDev, nID int
	if err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM msg_enc_device WHERE jid=?`, jid).Scan(&nDev); err != nil {
		t.Fatalf("count msg_enc_device: %v", err)
	}
	if err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM msg_enc_identities WHERE our_jid=?`, jid).Scan(&nID); err != nil {
		t.Fatalf("count msg_enc_identities: %v", err)
	}
	return nDev + nID
}

// countAccount cuenta las filas de accounts con ese account_id (0 ⇒ cuenta purgada).
func countAccount(t *testing.T, database *sql.DB, accountID string) int {
	t.Helper()
	var n int
	if err := database.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM accounts WHERE account_id=?`, accountID).Scan(&n); err != nil {
		t.Fatalf("count accounts: %v", err)
	}
	return n
}

// TestManager_Unlink_BDUnica_NoOrphans (DoD T3): borrar el device A sobre la BD única lo elimina de TODAS
// las tablas (devices, cuenta vacía, msg_enc_*) + su DEK, y deja a B intacto (device, cuenta, material,
// DEK). Cero huérfanos y sin daño colateral (decisión §10.I).
func TestManager_Unlink_BDUnica_NoOrphans(t *testing.T) {
	ctx := context.Background()
	m, store, database := newBDUnicaManager(t)

	jidA := canonJID(t, "111111111:1@s.whatsapp.net")
	jidB := canonJID(t, "222222222:1@s.whatsapp.net")
	accA := seedActiveDevice(t, m, database, uuidA, "111111111", jidA)
	accB := seedActiveDevice(t, m, database, uuidB, "222222222", jidB)
	if accA == accB {
		t.Fatalf("dos números distintos deberían dar cuentas distintas: %q", accA)
	}

	// Borrado quirúrgico de A (persistido, sin listener vivo).
	if err := m.Unlink(ctx, uuidA); err != nil {
		t.Fatalf("Unlink(A): %v", err)
	}

	// --- A desaparece de TODAS las tablas + su DEK ---
	if _, err := store.Get(ctx, uuidA); !errors.Is(err, app.ErrSessionNotFound) {
		t.Fatalf("device A debería estar borrado, Get=%v", err)
	}
	if n := countAccount(t, database, accA); n != 0 {
		t.Fatalf("la cuenta de A debería estar purgada (quedó vacía), quedan %d", n)
	}
	if n := countEnc(t, database, jidA); n != 0 {
		t.Fatalf("el material cifrado de A debería estar purgado, quedan %d filas", n)
	}
	custA, _ := m.custodyFor(uuidA)
	if custA.Exists() {
		t.Fatal("la DEK de A debería estar borrada")
	}

	// --- B intacto: device, cuenta, material cifrado y DEK ---
	if _, err := store.Get(ctx, uuidB); err != nil {
		t.Fatalf("device B debería seguir vivo: %v", err)
	}
	if n := countAccount(t, database, accB); n != 1 {
		t.Fatalf("la cuenta de B no debería tocarse, count=%d", n)
	}
	if n := countEnc(t, database, jidB); n != 2 {
		t.Fatalf("el material cifrado de B no debería tocarse, count=%d", n)
	}
	custB, _ := m.custodyFor(uuidB)
	if !custB.Exists() {
		t.Fatal("la DEK de B no debería borrarse")
	}
}

// TestManager_UnlinkAccount_BDUnica (DoD T3): dos devices del MISMO número cuelgan de la MISMA cuenta;
// borrar la CUENTA elimina AMBOS devices, la cuenta y TODO su material cifrado + DEKs. Cero huérfanos.
func TestManager_UnlinkAccount_BDUnica(t *testing.T) {
	ctx := context.Background()
	m, store, database := newBDUnicaManager(t)

	const selfPN = "56911112222"
	jidA := canonJID(t, "56911112222:1@s.whatsapp.net")
	jidB := canonJID(t, "56911112222:2@s.whatsapp.net")
	accA := seedActiveDevice(t, m, database, uuidA, selfPN, jidA)
	accB := seedActiveDevice(t, m, database, uuidB, selfPN, jidB)
	if accA != accB {
		t.Fatalf("el mismo número debería dar la MISMA cuenta: A=%q B=%q", accA, accB)
	}

	if err := m.UnlinkAccount(ctx, accA); err != nil {
		t.Fatalf("UnlinkAccount: %v", err)
	}

	// Ambos devices, la cuenta y todo el material cifrado + DEKs desaparecen; sin huérfanos.
	for _, id := range []string{uuidA, uuidB} {
		if _, err := store.Get(ctx, id); !errors.Is(err, app.ErrSessionNotFound) {
			t.Fatalf("device %s debería estar borrado, Get=%v", id, err)
		}
	}
	if n := countAccount(t, database, accA); n != 0 {
		t.Fatalf("la cuenta debería estar purgada, quedan %d", n)
	}
	if n := countEnc(t, database, jidA) + countEnc(t, database, jidB); n != 0 {
		t.Fatalf("el material cifrado de ambos devices debería estar purgado, quedan %d filas", n)
	}
	custA, _ := m.custodyFor(uuidA)
	custB, _ := m.custodyFor(uuidB)
	if custA.Exists() || custB.Exists() {
		t.Fatal("las DEK de ambos devices deberían estar borradas")
	}
}

// TestManager_UnlinkAccount_NotFound: borrar una cuenta sin dispositivos devuelve ErrSessionNotFound (→404).
func TestManager_UnlinkAccount_NotFound(t *testing.T) {
	m, _, _ := newBDUnicaManager(t)
	if err := m.UnlinkAccount(context.Background(), "cuenta-inexistente"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("UnlinkAccount de cuenta vacía debería dar ErrSessionNotFound, got %v", err)
	}
}

// TestManager_Restore_BDUnica_AllActive (DoD T3): un reinicio (Restore) sobre la BD única arranca un
// listener por CADA device ACTIVO. Usa el fakeFabric (sin whatsmeow) para observar los listeners.
func TestManager_Restore_BDUnica_AllActive(t *testing.T) {
	m, _, database := newBDUnicaManager(t)

	seedActiveDevice(t, m, database, uuidA, "111111111", "111111111:1@s.whatsapp.net")
	seedActiveDevice(t, m, database, uuidB, "222222222", "222222222:1@s.whatsapp.net")

	fab := newFakeFabric()
	m.newListener = fab.factory

	if err := m.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	waitForHealth(t, m, uuidA, HealthListening)
	waitForHealth(t, m, uuidB, HealthListening)
	if got := len(m.List()); got != 2 {
		t.Fatalf("Restore debería dejar 2 devices vivos, got %d", got)
	}
	m.Stop()
}
