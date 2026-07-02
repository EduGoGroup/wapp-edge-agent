package cryptostore

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-shared/envelope"
	"go.mau.fi/whatsmeow/types"
)

// TestDeviceMetadata_RoundTrip: PutDevice con PushName/BusinessName/LID poblados debe restaurarlos por
// GetDevice (cifrado/descifrado con la MISMA DEK), y en disco deben quedar CIFRADOS (no plaintext). Es
// el "gafete" real que sobrevive al reinicio (follow-up Plan 013), en vez de degradar al fallback config.
func TestDeviceMetadata_RoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")

	dek := newDEK(t)
	env, err := envelope.NewEnvelope(dek)
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}

	db1 := openAt(t, path)
	cont1, err := newCryptoContainer(ctx, db1, env)
	if err != nil {
		t.Fatalf("newCryptoContainer: %v", err)
	}

	dev, _ := syntheticDevice(t)
	const wantPush = "Tienda Doña Ana"
	const wantBiz = "Ana's Bakery LLC"
	wantLID := types.NewJID("998877665544332", types.HiddenUserServer)
	dev.PushName = wantPush
	dev.BusinessName = wantBiz
	dev.LID = wantLID

	if err := cont1.PutDevice(ctx, dev); err != nil {
		t.Fatalf("PutDevice: %v", err)
	}
	_ = db1.Close()

	// Evidencia: push_name en disco es CIPHERTEXT (≠ plaintext, y mide plaintext+overhead).
	dbCheck := openAt(t, path)
	var diskPush []byte
	if err := dbCheck.QueryRow(
		`SELECT push_name FROM msg_enc_device WHERE jid=?`, dev.ID.String()).Scan(&diskPush); err != nil {
		t.Fatalf("leer push_name en disco: %v", err)
	}
	if bytes.Equal(diskPush, []byte(wantPush)) {
		t.Fatal("push_name en disco coincide con el plaintext => NO está cifrado")
	}
	if len(diskPush) != len(wantPush)+envelope.Overhead {
		t.Errorf("push_name en disco mide %d, esperaba plaintext(%d)+overhead(%d)",
			len(diskPush), len(wantPush), envelope.Overhead)
	}
	_ = dbCheck.Close()

	// Reabrir con la MISMA DEK y verificar el round-trip.
	db2 := openAt(t, path)
	defer func() { _ = db2.Close() }()
	cont2, err := newCryptoContainer(ctx, db2, env)
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
	if got.PushName != wantPush {
		t.Errorf("PushName: got %q want %q", got.PushName, wantPush)
	}
	if got.BusinessName != wantBiz {
		t.Errorf("BusinessName: got %q want %q", got.BusinessName, wantBiz)
	}
	if got.LID.String() != wantLID.String() {
		t.Errorf("LID: got %q want %q", got.LID.String(), wantLID.String())
	}
}

// TestDeviceMetadata_EmptyDegrades: un device SIN metadata (PushName/BusinessName/LID en cero, como el
// primer arranque o un store viejo) no debe romper GetDevice: los campos vuelven vacíos y el LID queda
// zero (IsEmpty), degradando al fallback config sin error.
func TestDeviceMetadata_EmptyDegrades(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")

	dek := newDEK(t)
	env, err := envelope.NewEnvelope(dek)
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}

	db1 := openAt(t, path)
	cont1, err := newCryptoContainer(ctx, db1, env)
	if err != nil {
		t.Fatalf("newCryptoContainer: %v", err)
	}
	dev, _ := syntheticDevice(t) // sin PushName/BusinessName/LID
	if err := cont1.PutDevice(ctx, dev); err != nil {
		t.Fatalf("PutDevice: %v", err)
	}
	_ = db1.Close()

	db2 := openAt(t, path)
	defer func() { _ = db2.Close() }()
	cont2, err := newCryptoContainer(ctx, db2, env)
	if err != nil {
		t.Fatalf("newCryptoContainer (reabrir): %v", err)
	}
	got, err := cont2.GetDevice(ctx, *dev.ID)
	if err != nil {
		t.Fatalf("GetDevice: %v", err)
	}
	if got.PushName != "" {
		t.Errorf("PushName debía ser vacío, got %q", got.PushName)
	}
	if got.BusinessName != "" {
		t.Errorf("BusinessName debía ser vacío, got %q", got.BusinessName)
	}
	if !got.LID.IsEmpty() {
		t.Errorf("LID debía ser zero, got %q", got.LID.String())
	}
}

// TestMigrateStore_IdempotentMetadataColumns: abrir y migrar el MISMO store dos veces (simula 2º
// arranque) NO debe fallar por "duplicate column". El runner re-ejecuta todos los .sql en cada arranque
// sin tabla de versión, así que la migración de columnas nuevas (ensureDeviceMetadataColumns) está
// guardada por PRAGMA table_info. Verifica también que las 3 columnas existen tras migrar.
func TestMigrateStore_IdempotentMetadataColumns(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")

	// 1er arranque.
	db1, err := wappdb.OpenSessionStore(ctx, path)
	if err != nil {
		t.Fatalf("OpenSessionStore (1º): %v", err)
	}
	// Re-migrar sobre el MISMO handle (idempotencia dentro del proceso).
	if err := wappdb.MigrateStore(ctx, db1); err != nil {
		t.Fatalf("MigrateStore re-aplicada: %v", err)
	}
	_ = db1.Close()

	// 2º arranque: reabrir y re-migrar el fichero existente. Aquí es donde un ALTER pelado reventaría.
	db2, err := wappdb.OpenSessionStore(ctx, path)
	if err != nil {
		t.Fatalf("OpenSessionStore (2º arranque): %v", err)
	}
	defer func() { _ = db2.Close() }()

	cols := map[string]bool{}
	rows, err := db2.QueryContext(ctx, `PRAGMA table_info(msg_enc_device)`)
	if err != nil {
		t.Fatalf("PRAGMA table_info: %v", err)
	}
	for rows.Next() {
		var (
			cid       int
			name      string
			colType   string
			notNull   int
			dfltValue *string
			pk        int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			t.Fatalf("scan table_info: %v", err)
		}
		cols[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	_ = rows.Close()
	for _, c := range []string{"push_name", "business_name", "lid"} {
		if !cols[c] {
			t.Errorf("columna %q ausente tras migrar", c)
		}
	}
}

// TestMigrateStore_LegacyStoreGetsColumns: un store YA EMPAREJADO creado ANTES de esta mejora (tabla
// msg_enc_device sin las 3 columnas de metadata) debe recibirlas vía el ALTER guardado, SIN
// re-emparejar y sin fallar en un 2º arranque. Reproduce el store legacy creando la tabla con el
// esquema viejo y aplicando MigrateStore dos veces.
func TestMigrateStore_LegacyStoreGetsColumns(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")

	raw, err := wappdb.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Esquema LEGACY de msg_enc_device (sin push_name/business_name/lid): simula un store viejo.
	if _, err := raw.ExecContext(ctx, `CREATE TABLE msg_enc_device (
		jid TEXT PRIMARY KEY, registration_id INTEGER NOT NULL, signed_pre_key_id INTEGER NOT NULL,
		noise_priv BLOB NOT NULL, identity_priv BLOB NOT NULL, signed_pre_key_priv BLOB NOT NULL,
		signed_pre_key_sig BLOB NOT NULL, adv_secret_key BLOB NOT NULL, adv_details BLOB NOT NULL,
		adv_account_sig BLOB NOT NULL, adv_account_sig_key BLOB NOT NULL, adv_device_sig BLOB NOT NULL)`); err != nil {
		t.Fatalf("crear tabla legacy: %v", err)
	}

	// 1er arranque tras el upgrade: el ALTER guardado debe añadir las columnas.
	if err := wappdb.MigrateStore(ctx, raw); err != nil {
		t.Fatalf("MigrateStore sobre store legacy: %v", err)
	}
	// 2º arranque: re-migrar NO debe fallar por "duplicate column".
	if err := wappdb.MigrateStore(ctx, raw); err != nil {
		t.Fatalf("MigrateStore 2º arranque sobre store legacy: %v", err)
	}
	defer func() { _ = raw.Close() }()

	rows, err := raw.QueryContext(ctx, `PRAGMA table_info(msg_enc_device)`)
	if err != nil {
		t.Fatalf("PRAGMA table_info: %v", err)
	}
	cols := map[string]bool{}
	for rows.Next() {
		var (
			cid       int
			name      string
			colType   string
			notNull   int
			dfltValue *string
			pk        int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			t.Fatalf("scan table_info: %v", err)
		}
		cols[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	_ = rows.Close()
	for _, c := range []string{"push_name", "business_name", "lid"} {
		if !cols[c] {
			t.Errorf("columna %q no fue añadida al store legacy", c)
		}
	}
}
