package cryptostore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"path/filepath"
	"testing"

	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
)

// openTestDB abre un store SQLite efímero (t.TempDir) ya migrado (tablas msg_enc_*). El
// driver es modernc.org/sqlite (CGO-free), sin testcontainers ni Postgres: el spike corre
// el round-trip de cifrado contra un fichero .db real en disco.
func openTestDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.db")
	db, err := wappdb.OpenAndMigrate(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

// newDEK genera una DEK de 32 bytes con CSPRNG.
func newDEK(t *testing.T) []byte {
	t.Helper()
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		t.Fatal(err)
	}
	return dek
}

// newSyntheticAccount fabrica un ADVSignedDeviceIdentity con bytes sintéticos.
// Las firmas (account/device) miden 64B y la clave 32B como en un pairing real, pero su
// contenido es aleatorio: el spike NO valida firmas, solo round-trip de bytes cifrados.
func newSyntheticAccount(t *testing.T) *waAdv.ADVSignedDeviceIdentity {
	t.Helper()
	rnd := func(n int) []byte {
		b := make([]byte, n)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		return b
	}
	return &waAdv.ADVSignedDeviceIdentity{
		Details:             rnd(48),
		AccountSignature:    rnd(64),
		AccountSignatureKey: rnd(32),
		DeviceSignature:     rnd(64),
	}
}

// syntheticDevice fabrica un *store.Device con material criptográfico SINTÉTICO (sin red/QR).
// Devuelve también el AdvSecretKey "fuente" para comparar el round-trip.
func syntheticDevice(t *testing.T) (*store.Device, []byte) {
	t.Helper()
	advSecret := make([]byte, 32)
	if _, err := rand.Read(advSecret); err != nil {
		t.Fatal(err)
	}
	idKey := keys.NewKeyPair()
	dev := &store.Device{
		RegistrationID: 123456,
		NoiseKey:       keys.NewKeyPair(),
		IdentityKey:    idKey,
		SignedPreKey:   idKey.CreateSignedPreKey(7),
		AdvSecretKey:   advSecret,
		Account:        newSyntheticAccount(t),
	}
	jid := types.NewJID("15551230000", types.DefaultUserServer)
	jid.Device = 0
	dev.ID = &jid
	return dev, advSecret
}
