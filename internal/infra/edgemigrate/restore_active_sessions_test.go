package edgemigrate

// Tests de la FASE 2 (Plan 022 T6.5): restaurar sesiones ACTIVAS del árbol archivado a la BD única SIN
// re-escanear. Fixtures SINTÉTICAS (store.db + dek.key fabricados en un tmpdir con una DEK conocida); NO se
// usan teléfonos ni datos reales. Verifican: migración con la MISMA DEK per-device y mismo JID; ≥2 devices
// de 2 números; fallback por device caducado/DEK cruzada sin tumbar a los demás; idempotencia (2×); que
// cruzar DEKs sigue FALLANDO (no regresa T2); y que la limpieza ocurre SOLO tras verificar.

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"go.mau.fi/whatsmeow/types"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-shared/envelope"
)

// ids UUID válidos para nombrar los directorios de sesión archivados (uuidPattern los exige).
const (
	tid1 = "11111111-1111-4111-8111-111111111111"
	tid2 = "22222222-2222-4222-8222-222222222222"
	tid3 = "33333333-3333-4333-8333-333333333333"
)

// newDEK genera una DEK aleatoria de 32B (AES-256) para las fixtures.
func newDEK(t *testing.T) []byte {
	t.Helper()
	dek := make([]byte, envelope.DEKSize)
	if _, err := rand.Read(dek); err != nil {
		t.Fatalf("generar DEK: %v", err)
	}
	return dek
}

// canon devuelve la forma canónica del JID (como se persiste en msg_enc_device.jid y devices.jid).
func canon(t *testing.T, raw string) string {
	t.Helper()
	jid, err := types.ParseJID(raw)
	if err != nil {
		t.Fatalf("ParseJID(%q): %v", raw, err)
	}
	return jid.String()
}

// seedArchivedSession fabrica un store.db por-sesión SINTÉTICO bajo <dataDir>/_archived-pre-022/sessions/<id>/
// con material CIFRADO (msg_enc_device + msg_enc_identities) sellado con sealDEK, y escribe dek.key con
// dekFileBytes (que puede DIFERIR de sealDEK para simular una sesión caducada / DEK cruzada). Es el árbol
// exacto que deja la fase 1.
func seedArchivedSession(t *testing.T, dataDir, id, jidRaw string, sealDEK, dekFileBytes []byte) {
	t.Helper()
	ctx := context.Background()
	dir := filepath.Join(dataDir, archivePre022DirName, sessionsDirName, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir archivo %s: %v", id, err)
	}
	storePath := filepath.Join(dir, legacyStoreDBName)

	// OpenSessionStore crea el store.db con el esquema msg_enc_* (0001), como un pairing real.
	store, err := wappdb.OpenSessionStore(ctx, storePath)
	if err != nil {
		t.Fatalf("OpenSessionStore(%s): %v", id, err)
	}
	env, err := envelope.NewEnvelope(sealDEK)
	if err != nil {
		t.Fatalf("envelope(sealDEK): %v", err)
	}
	seal := func(pt []byte) []byte {
		ct, sErr := env.Seal(pt)
		if sErr != nil {
			t.Fatalf("Seal: %v", sErr)
		}
		return ct
	}
	jid := canon(t, jidRaw)
	// noise_priv/identity_priv/signed_pre_key_priv = 32B; signed_pre_key_sig = 64B; el resto, relleno sellado.
	b32 := func() []byte { b := make([]byte, 32); _, _ = rand.Read(b); return b }
	b64 := func() []byte { b := make([]byte, 64); _, _ = rand.Read(b); return b }
	if _, err := store.ExecContext(ctx,
		`INSERT INTO msg_enc_device (jid, registration_id, signed_pre_key_id, noise_priv, identity_priv,
		   signed_pre_key_priv, signed_pre_key_sig, adv_secret_key, adv_details, adv_account_sig,
		   adv_account_sig_key, adv_device_sig)
		 VALUES (?, 42, 7, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		jid, seal(b32()), seal(b32()), seal(b32()), seal(b64()), seal(b32()),
		seal([]byte("details")), seal([]byte("acct-sig")), seal([]byte("acct-sig-key")), seal([]byte("dev-sig"))); err != nil {
		t.Fatalf("insert msg_enc_device(%s): %v", id, err)
	}
	if _, err := store.ExecContext(ctx,
		`INSERT INTO msg_enc_identities (our_jid, their_id, identity) VALUES (?, 'peer@s.whatsapp.net', ?)`,
		jid, seal(b32())); err != nil {
		t.Fatalf("insert msg_enc_identities(%s): %v", id, err)
	}
	// Fold el WAL al fichero principal para que el store archivado sea autocontenido antes de cerrarlo.
	if _, err := store.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("wal_checkpoint(%s): %v", id, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store(%s): %v", id, err)
	}

	if err := os.WriteFile(filepath.Join(dir, legacyDEKName), dekFileBytes, 0o600); err != nil {
		t.Fatalf("escribir dek.key(%s): %v", id, err)
	}
}

// openUnifiedDB abre una BD única fresca (edge.db) ya migrada (accounts/devices + msg_enc_* + whatsmeow_*).
func openUnifiedDB(t *testing.T, dataDir string) *sql.DB {
	t.Helper()
	database, err := wappdb.OpenAndMigrate(context.Background(), filepath.Join(dataDir, "edge.db"))
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

// archivedSessionDir devuelve la ruta del subdirectorio archivado de una sesión (para aserciones de purga).
func archivedSessionDir(dataDir, id string) string {
	return filepath.Join(dataDir, archivePre022DirName, sessionsDirName, id)
}

// dirExistsT indica si el directorio existe (helper de aserción de purga).
func dirExistsT(t *testing.T, path string) bool {
	t.Helper()
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.IsDir()
}

// countEncDevice cuenta filas de msg_enc_device por jid en la BD única.
func countEncDevice(t *testing.T, database *sql.DB, jid string) int {
	t.Helper()
	var n int
	if err := database.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM msg_enc_device WHERE jid=?`, jid).Scan(&n); err != nil {
		t.Fatalf("count msg_enc_device: %v", err)
	}
	return n
}

// TestRestore_TwoDevicesTwoNumbers (DoD T6.5): dos devices de DOS números viajan a la BD única con SU DEK y
// mismo JID; quedan 'active'; la DEK re-ubicada (keys/<id>.key) descifra el material copiado (sesión viva);
// el árbol archivado se purga tras verificar.
func TestRestore_TwoDevicesTwoNumbers(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	dek1, dek2 := newDEK(t), newDEK(t)
	jid1, jid2 := canon(t, "111111111:1@s.whatsapp.net"), canon(t, "222222222:1@s.whatsapp.net")
	seedArchivedSession(t, dataDir, tid1, "111111111:1@s.whatsapp.net", dek1, dek1)
	seedArchivedSession(t, dataDir, tid2, "222222222:1@s.whatsapp.net", dek2, dek2)

	database := openUnifiedDB(t, dataDir)
	var buf bytes.Buffer
	if err := RestoreArchivedActiveSessions(ctx, dataDir, database, wappdb.DialectSQLite, newLogger(&buf)); err != nil {
		t.Fatalf("RestoreArchivedActiveSessions: %v", err)
	}

	store := sessionstore.New(database)
	for _, tc := range []struct {
		id, jid, selfPN string
		dek             []byte
	}{
		{tid1, jid1, "111111111", dek1},
		{tid2, jid2, "222222222", dek2},
	} {
		sess, err := store.Get(ctx, tc.id)
		if err != nil {
			t.Fatalf("Get(%s): %v", tc.id, err)
		}
		if sess.State != domain.SessionStateActive {
			t.Fatalf("device %s debería quedar active, got %q", tc.id, sess.State)
		}
		if sess.JID != tc.jid {
			t.Fatalf("device %s JID esperado %q, got %q", tc.id, tc.jid, sess.JID)
		}
		if sess.SelfPN != tc.selfPN {
			t.Fatalf("device %s self_pn esperado %q, got %q", tc.id, tc.selfPN, sess.SelfPN)
		}
		if n := countEncDevice(t, database, tc.jid); n != 1 {
			t.Fatalf("device %s debería tener su msg_enc_device en la BD única, got %d", tc.id, n)
		}
		// La DEK re-ubicada DESCIFRA el ciphertext copiado (sesión viva sin re-escanear, sin re-cifrar).
		assertDEKOpensDevice(t, dataDir, database, tc.id, tc.jid, tc.dek, true)
		// El árbol archivado de un device migrado y verificado se purgó.
		if dirExistsT(t, archivedSessionDir(dataDir, tc.id)) {
			t.Fatalf("el archivo de %s debería estar purgado tras verificar", tc.id)
		}
	}
}

// assertDEKOpensDevice lee la DEK re-ubicada en keys/<id>.key (o usa dekOverride si !nil) y comprueba que
// Open() del noise_priv COPIADO en la BD única funciona (want=true) o FALLA (want=false, cruce de DEKs).
func assertDEKOpensDevice(t *testing.T, dataDir string, database *sql.DB, id, jid string, dekOverride []byte, want bool) {
	t.Helper()
	dek := dekOverride
	if dek == nil {
		keyPath, err := dekPathFor(dataDir, id)
		if err != nil {
			t.Fatalf("dekPathFor(%s): %v", id, err)
		}
		dek, err = os.ReadFile(keyPath)
		if err != nil {
			t.Fatalf("leer DEK re-ubicada %s: %v", id, err)
		}
	}
	var noiseCT []byte
	if err := database.QueryRowContext(context.Background(),
		`SELECT noise_priv FROM msg_enc_device WHERE jid=?`, jid).Scan(&noiseCT); err != nil {
		t.Fatalf("leer noise_priv copiado(%s): %v", id, err)
	}
	env, err := envelope.NewEnvelope(dek)
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	_, openErr := env.Open(noiseCT)
	if want && openErr != nil {
		t.Fatalf("la DEK per-device debería descifrar el material copiado de %s: %v", id, openErr)
	}
	if !want && openErr == nil {
		t.Fatalf("una DEK CRUZADA NO debería descifrar el material de %s (regresión T2)", id)
	}
}

// TestRestore_CrossDEKStillFails (DoD T6.5): tras migrar, la DEK de OTRO device NO descifra el material —
// el aislamiento per-device de T2 se conserva (no se reintroduce una DEK global).
func TestRestore_CrossDEKStillFails(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	dek1, dek2 := newDEK(t), newDEK(t)
	jid1 := canon(t, "111111111:1@s.whatsapp.net")
	seedArchivedSession(t, dataDir, tid1, "111111111:1@s.whatsapp.net", dek1, dek1)
	seedArchivedSession(t, dataDir, tid2, "222222222:1@s.whatsapp.net", dek2, dek2)

	database := openUnifiedDB(t, dataDir)
	var buf bytes.Buffer
	if err := RestoreArchivedActiveSessions(ctx, dataDir, database, wappdb.DialectSQLite, newLogger(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	// La DEK del device 2 NO debe abrir el material del device 1 (cruce de DEKs = fallo, no regresa T2).
	assertDEKOpensDevice(t, dataDir, database, tid1, jid1, dek2, false)
}

// TestRestore_ExpiredDeviceFallback (DoD T6.5): un device con la DEK CRUZADA (store sellado con otra llave,
// simula caducidad ~14 días / DEK que no corresponde) cae al fallback 'loggedout' sin copiar material y SIN
// tumbar al device bueno; su archivo se CONSERVA (no se purga sin verificar).
func TestRestore_ExpiredDeviceFallback(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	good, sealBad, dekMismatch := newDEK(t), newDEK(t), newDEK(t)
	jidGood := canon(t, "111111111:1@s.whatsapp.net")
	jidBad := canon(t, "999999999:1@s.whatsapp.net")
	seedArchivedSession(t, dataDir, tid1, "111111111:1@s.whatsapp.net", good, good)           // bueno
	seedArchivedSession(t, dataDir, tid2, "999999999:1@s.whatsapp.net", sealBad, dekMismatch) // dek.key ≠ sella

	database := openUnifiedDB(t, dataDir)
	var buf bytes.Buffer
	if err := RestoreArchivedActiveSessions(ctx, dataDir, database, wappdb.DialectSQLite, newLogger(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	store := sessionstore.New(database)

	// El bueno migró (active) y se purgó.
	goodSess, err := store.Get(ctx, tid1)
	if err != nil || goodSess.State != domain.SessionStateActive {
		t.Fatalf("device bueno debería quedar active: sess=%+v err=%v", goodSess, err)
	}
	if dirExistsT(t, archivedSessionDir(dataDir, tid1)) {
		t.Fatal("el archivo del device bueno debería estar purgado")
	}

	// El caducado cae a fallback loggedout, SIN material cifrado copiado, y su archivo se CONSERVA.
	badSess, err := store.Get(ctx, tid2)
	if err != nil {
		t.Fatalf("el device caducado debería registrarse loggedout, Get: %v", err)
	}
	if badSess.State != domain.SessionStateLoggedOut {
		t.Fatalf("device caducado debería quedar loggedout, got %q", badSess.State)
	}
	if n := countEncDevice(t, database, jidBad); n != 0 {
		t.Fatalf("el device caducado NO debe tener material cifrado copiado, got %d", n)
	}
	if !dirExistsT(t, archivedSessionDir(dataDir, tid2)) {
		t.Fatal("el archivo del device caducado debería CONSERVARSE (no purgar sin verificar)")
	}
	// El bueno sigue descifrable con su DEK (el fallo aislado no lo afectó).
	assertDEKOpensDevice(t, dataDir, database, tid1, jidGood, good, true)
}

// TestRestore_Idempotent (DoD T6.5): correr la migración DOS veces deja el mismo estado (no duplica, no
// falla); la 2.ª corrida es no-op sobre el device ya migrado (archivo purgado).
func TestRestore_Idempotent(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	dek := newDEK(t)
	jid := canon(t, "111111111:1@s.whatsapp.net")
	seedArchivedSession(t, dataDir, tid1, "111111111:1@s.whatsapp.net", dek, dek)

	database := openUnifiedDB(t, dataDir)
	var buf bytes.Buffer
	log := newLogger(&buf)
	if err := RestoreArchivedActiveSessions(ctx, dataDir, database, wappdb.DialectSQLite, log); err != nil {
		t.Fatalf("Restore #1: %v", err)
	}
	if err := RestoreArchivedActiveSessions(ctx, dataDir, database, wappdb.DialectSQLite, log); err != nil {
		t.Fatalf("Restore #2 (idempotente): %v", err)
	}

	if n := countEncDevice(t, database, jid); n != 1 {
		t.Fatalf("la 2.ª corrida NO debe duplicar msg_enc_device, got %d", n)
	}
	all, err := sessionstore.New(database).List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("debería haber exactamente 1 device tras 2 corridas, got %d", len(all))
	}
	if dirExistsT(t, archivedSessionDir(dataDir, tid1)) {
		t.Fatal("el archivo debería estar purgado tras la migración")
	}
}

// TestRestore_NoArchive_NoOp: sin árbol archivado, la migración es no-op (instalación limpia).
func TestRestore_NoArchive_NoOp(t *testing.T) {
	dataDir := t.TempDir()
	database := openUnifiedDB(t, dataDir)
	var buf bytes.Buffer
	if err := RestoreArchivedActiveSessions(context.Background(), dataDir, database, wappdb.DialectSQLite, newLogger(&buf)); err != nil {
		t.Fatalf("Restore sin archivo debería ser no-op, got: %v", err)
	}
	all, err := sessionstore.New(database).List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 0 {
		t.Fatalf("sin archivo no debería registrar devices, got %d", len(all))
	}
}

// TestRestore_PostgresDialect_NoOp: con dialecto Postgres el tramo no soporta el traslado (SQLite→PG) y sale
// limpio sin tocar la BD (re-escaneo). No abre Postgres real: solo comprueba el guard.
func TestRestore_PostgresDialect_NoOp(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	dek := newDEK(t)
	seedArchivedSession(t, dataDir, tid3, "333333333:1@s.whatsapp.net", dek, dek)
	database := openUnifiedDB(t, dataDir)
	var buf bytes.Buffer
	if err := RestoreArchivedActiveSessions(ctx, dataDir, database, wappdb.DialectPostgres, newLogger(&buf)); err != nil {
		t.Fatalf("Restore(Postgres) debería ser no-op sin error: %v", err)
	}
	// No migró nada (guard) y CONSERVÓ el archivo.
	if n := countEncDevice(t, database, canon(t, "333333333:1@s.whatsapp.net")); n != 0 {
		t.Fatalf("con Postgres no debe copiar material, got %d", n)
	}
	if !dirExistsT(t, archivedSessionDir(dataDir, tid3)) {
		t.Fatal("con Postgres el archivo debe conservarse")
	}
}
