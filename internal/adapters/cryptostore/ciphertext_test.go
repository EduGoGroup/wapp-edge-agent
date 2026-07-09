package cryptostore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-shared/envelope"
)

// TestDBFileIsCiphertext es la prueba dura de RF-3/RNF-3: tras persistir material con un PLAINTEXT
// CONOCIDO (un "needle" reconocible) a través del cryptostore, los BYTES CRUDOS del fichero .db (y de
// sus sidecars -wal/-shm) NO contienen ese plaintext en ningún lado. Demuestra que el .db queda en
// ciphertext porque el cifrado es a nivel de campo con la DEK, sin SQLCipher.
func TestDBFileIsCiphertext(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "store.db")

	// Needles: plaintext reconocibles que NO deben aparecer en el fichero si todo va cifrado.
	sessionNeedle := []byte("SUPER-SECRET-SESSION-NEEDLE-9e8f7a6b5c4d3e2f")
	senderNeedle := []byte("SUPER-SECRET-SENDERKEY-NEEDLE-0011223344556677")

	db := openAt(t, path)
	env, err := envelope.NewEnvelope(newDEK(t))
	if err != nil {
		t.Fatal(err)
	}
	cont, err := newCryptoContainer(ctx, db, DialectSQLite, env)
	if err != nil {
		t.Fatalf("newCryptoContainer: %v", err)
	}

	dev, _ := syntheticDevice(t)
	noiseNeedle := append([]byte(nil), dev.NoiseKey.Priv[:]...) // material directo del device
	if err := cont.PutDevice(ctx, dev); err != nil {
		t.Fatalf("PutDevice: %v", err)
	}
	if err := dev.Sessions.PutSession(ctx, "15559990000.0:0", sessionNeedle); err != nil {
		t.Fatalf("PutSession: %v", err)
	}
	if err := dev.SenderKeys.PutSenderKey(ctx, "grupo@g.us", "15559990000.0:0", senderNeedle); err != nil {
		t.Fatalf("PutSenderKey: %v", err)
	}

	// Forzar el volcado del WAL al fichero principal y cerrar para que no quede nada solo en caché.
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("wal_checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Inspeccionar TODOS los ficheros del store (.db, -wal, -shm): ninguno debe contener un needle.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	needles := map[string][]byte{
		"session":   sessionNeedle,
		"senderkey": senderNeedle,
		"noise":     noiseNeedle,
	}
	scanned := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		scanned++
		for name, needle := range needles {
			if bytes.Contains(raw, needle) {
				t.Errorf("el fichero %s contiene el plaintext %q => el .db NO está en ciphertext",
					e.Name(), name)
			}
		}
		t.Logf("inspeccionado %s (%d bytes): sin plaintext", e.Name(), len(raw))
	}
	if scanned == 0 {
		t.Fatal("no se inspeccionó ningún fichero del store")
	}
}

// TestDBFileHasOwnerOnlyPerms verifica que el .db se crea con permisos 0600 (solo el dueño).
func TestDBFileHasOwnerOnlyPerms(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	db, err := wappdb.Open(context.Background(), wappdb.DialectSQLite, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("permisos del .db = %o, esperaba 600", perm)
	}
}
