package cryptostore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"path/filepath"
	"testing"

	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-shared/envelope"
	"go.mau.fi/whatsmeow"
)

// openAt abre (y migra, idempotente) un handle nuevo al .db en path. Reabrir handles distintos
// sobre el mismo fichero demuestra que el round-trip va contra DISCO, no contra memoria.
func openAt(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := wappdb.OpenAndMigrate(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenAndMigrate(%s): %v", path, err)
	}
	return db
}

// TestRoundTrip_EncryptedStore: persistir un device + stores de sesión cifrados, comprobar que en
// disco hay CIPHERTEXT, reabrir con la MISMA DEK y verificar que todo el material sobrevive el
// round-trip y que whatsmeow.NewClient acepta el device rehidratado. Por último, una DEK
// equivocada NO descifra (auth tag GCM). Cubre RF-3 (cifrado) y el rechazo de DEK incorrecta.
func TestRoundTrip_EncryptedStore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")

	dek := newDEK(t)
	env, err := envelope.NewEnvelope(dek)
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	t.Logf("GCM overhead por valor = %d bytes (nonce 12 + tag 16)", env.Overhead())

	// --- FASE 1: persistir cifrado ---
	db1 := openAt(t, path)
	cont1, err := newCryptoContainer(ctx, db1, DialectSQLite, env)
	if err != nil {
		t.Fatalf("newCryptoContainer: %v", err)
	}

	dev, srcAdvSecret := syntheticDevice(t)
	srcNoise := *dev.NoiseKey.Priv
	srcIdentity := *dev.IdentityKey.Priv
	srcSPK := *dev.SignedPreKey.Priv
	srcSPKSig := *dev.SignedPreKey.Signature

	if err := cont1.PutDevice(ctx, dev); err != nil {
		t.Fatalf("PutDevice: %v", err)
	}

	// Datos sintéticos de sesión / identidad / senderkey / prekeys.
	srcSession := []byte("sesion-signal-sintetica-de-longitud-variable-XXXXXXXXXXXX")
	if err := dev.Sessions.PutSession(ctx, "15559990000.0:0", srcSession); err != nil {
		t.Fatalf("PutSession: %v", err)
	}
	var srcIdent [32]byte
	copy(srcIdent[:], bytes.Repeat([]byte{0xAB}, 32))
	if err := dev.Identities.PutIdentity(ctx, "15559990000.0:0", srcIdent); err != nil {
		t.Fatalf("PutIdentity: %v", err)
	}
	srcSenderKey := []byte("sender-key-de-grupo-sintetica-1234567890")
	if err := dev.SenderKeys.PutSenderKey(ctx, "grupo@g.us", "15559990000.0:0", srcSenderKey); err != nil {
		t.Fatalf("PutSenderKey: %v", err)
	}
	genPreKeys, err := dev.PreKeys.GetOrGenPreKeys(ctx, 3)
	if err != nil {
		t.Fatalf("GetOrGenPreKeys: %v", err)
	}
	if len(genPreKeys) != 3 {
		t.Fatalf("esperaba 3 prekeys, hubo %d", len(genPreKeys))
	}
	srcPreKeyPriv := *genPreKeys[0].Priv
	srcPreKeyID := genPreKeys[0].KeyID
	_ = db1.Close()

	// --- EVIDENCIA: el contenido en disco es CIPHERTEXT (lectura SQL cruda) ---
	dbCheck := openAt(t, path)
	assertCiphertextOnDisk(t, dbCheck, dev.ID.String(), srcNoise[:], srcSession)
	_ = dbCheck.Close()

	// --- FASE 2: reabrir con la MISMA DEK y verificar round-trip ---
	db2 := openAt(t, path)
	cont2, err := newCryptoContainer(ctx, db2, DialectSQLite, env)
	if err != nil {
		t.Fatalf("newCryptoContainer (reabrir): %v", err)
	}
	got, err := cont2.GetDevice(ctx, *dev.ID)
	if err != nil {
		t.Fatalf("GetDevice: %v", err)
	}
	if got == nil {
		t.Fatal("GetDevice devolvió nil tras persistir")
	}

	// Round-trip de los campos directos del Device.
	eqBytes(t, "NoiseKey.Priv", got.NoiseKey.Priv[:], srcNoise[:])
	eqBytes(t, "IdentityKey.Priv", got.IdentityKey.Priv[:], srcIdentity[:])
	eqBytes(t, "SignedPreKey.Priv", got.SignedPreKey.Priv[:], srcSPK[:])
	eqBytes(t, "SignedPreKey.Signature", got.SignedPreKey.Signature[:], srcSPKSig[:])
	eqBytes(t, "AdvSecretKey", got.AdvSecretKey, srcAdvSecret)
	if got.RegistrationID != dev.RegistrationID {
		t.Errorf("RegistrationID: got %d want %d", got.RegistrationID, dev.RegistrationID)
	}
	if got.SignedPreKey.KeyID != dev.SignedPreKey.KeyID {
		t.Errorf("SignedPreKey.KeyID: got %d want %d", got.SignedPreKey.KeyID, dev.SignedPreKey.KeyID)
	}

	// Round-trip de los stores de sesión (descifrados vía el Device rehidratado).
	gotSession, err := got.Sessions.GetSession(ctx, "15559990000.0:0")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	eqBytes(t, "Session", gotSession, srcSession)

	trusted, err := got.Identities.IsTrustedIdentity(ctx, "15559990000.0:0", srcIdent)
	if err != nil {
		t.Fatalf("IsTrustedIdentity: %v", err)
	}
	if !trusted {
		t.Error("la identidad descifrada no coincidió con la sembrada")
	}

	gotSenderKey, err := got.SenderKeys.GetSenderKey(ctx, "grupo@g.us", "15559990000.0:0")
	if err != nil {
		t.Fatalf("GetSenderKey: %v", err)
	}
	eqBytes(t, "SenderKey", gotSenderKey, srcSenderKey)

	gotPreKey, err := got.PreKeys.GetPreKey(ctx, srcPreKeyID)
	if err != nil {
		t.Fatalf("GetPreKey: %v", err)
	}
	if gotPreKey == nil {
		t.Fatal("GetPreKey devolvió nil")
	}
	eqBytes(t, "PreKey.Priv", gotPreKey.Priv[:], srcPreKeyPriv[:])

	// El device rehidratado debe ser aceptado por whatsmeow.NewClient SIN conectar.
	client := whatsmeow.NewClient(got, nil)
	if client == nil {
		t.Fatal("whatsmeow.NewClient devolvió nil con el device rehidratado")
	}
	if client.Store == nil || client.Store.ID == nil {
		t.Fatal("el cliente no quedó ligado al device rehidratado")
	}
	t.Logf("whatsmeow.NewClient aceptó el device rehidratado (JID=%s) sin conectar", client.Store.ID)
	_ = db2.Close()

	// --- FASE 3: DEK equivocada => el descifrado DEBE fallar (GCM auth tag) ---
	badDEK := append([]byte(nil), dek...)
	badDEK[0] ^= 0xFF // un bit distinto basta
	badEnv, err := envelope.NewEnvelope(badDEK)
	if err != nil {
		t.Fatal(err)
	}
	db3 := openAt(t, path)
	defer func() { _ = db3.Close() }()
	contBad, err := newCryptoContainer(ctx, db3, DialectSQLite, badEnv)
	if err != nil {
		t.Fatalf("newCryptoContainer (bad dek): %v", err)
	}
	if _, err := contBad.GetDevice(ctx, *dev.ID); err == nil {
		t.Fatal("GetDevice con DEK equivocada DEBÍA fallar y no falló (GCM no autenticó)")
	} else {
		t.Logf("OK: GetDevice con DEK equivocada falló como se esperaba: %v", err)
	}
}

// assertCiphertextOnDisk confirma (lectura SQL cruda) que el contenido persistido NO es el
// plaintext original y mide plaintext+overhead(28).
func assertCiphertextOnDisk(t *testing.T, db *sql.DB, jid string, plainNoise, plainSession []byte) {
	t.Helper()
	var diskNoise []byte
	if err := db.QueryRow(
		`SELECT noise_priv FROM msg_enc_device WHERE jid=?`, jid).Scan(&diskNoise); err != nil {
		t.Fatalf("leer noise_priv en disco: %v", err)
	}
	if bytes.Equal(diskNoise, plainNoise) {
		t.Fatal("noise_priv en disco coincide con el plaintext => NO está cifrado")
	}
	if len(diskNoise) != len(plainNoise)+envelope.Overhead {
		t.Errorf("noise_priv en disco mide %d, esperaba plaintext(%d)+overhead(%d)=%d",
			len(diskNoise), len(plainNoise), envelope.Overhead, len(plainNoise)+envelope.Overhead)
	}
	t.Logf("EVIDENCIA noise_priv en disco: len=%d, sha256=%x (≠ plaintext sha256=%x)",
		len(diskNoise), sha256.Sum256(diskNoise), sha256.Sum256(plainNoise))

	var diskSession []byte
	if err := db.QueryRow(
		`SELECT session FROM msg_enc_sessions WHERE our_jid=? LIMIT 1`, jid).Scan(&diskSession); err != nil {
		t.Fatalf("leer session en disco: %v", err)
	}
	if bytes.Contains(diskSession, plainSession) {
		t.Fatal("la sesión en disco contiene el plaintext => NO está cifrada")
	}
	t.Logf("EVIDENCIA session en disco: len=%d (≠ plaintext)", len(diskSession))
}

func eqBytes(t *testing.T, name string, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Errorf("%s NO sobrevivió el round-trip: got %x want %x", name, got, want)
	}
}
