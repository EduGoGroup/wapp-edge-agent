package cryptostore

import (
	"context"
	"strings"
	"testing"

	"github.com/EduGoGroup/wapp-shared/envelope"
)

// TestUpstreamSchemaRejectsCiphertext DEMUESTRA (no asume) que la columna whatsmeow_device.noise_key
// del esquema REAL de whatsmeow (también en SQLite: CHECK length(noise_key)=32) rechaza un ciphertext
// GCM de 60B (32 + overhead 28). Es lo que JUSTIFICA el esquema propio msg_enc_* con BLOB libre en
// vez de envolver el SQLStore real para esas columnas (ADR-0002).
func TestUpstreamSchemaRejectsCiphertext(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)

	// newCryptoContainer ejecuta sqlstore.Upgrade -> crea whatsmeow_device con sus CHECK.
	env, err := envelope.NewEnvelope(newDEK(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newCryptoContainer(ctx, db, DialectSQLite, env); err != nil {
		t.Fatalf("newCryptoContainer: %v", err)
	}

	ciphertext60 := make([]byte, 60) // tamaño de un ciphertext GCM de un valor de 32B
	ok32 := make([]byte, 32)
	sig64 := make([]byte, 64)

	_, err = db.ExecContext(ctx, `
		INSERT INTO whatsmeow_device
			(jid, registration_id, noise_key, identity_key,
			 signed_pre_key, signed_pre_key_id, signed_pre_key_sig,
			 adv_key, adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig)
		VALUES ('test@s.whatsapp.net', 1, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)`,
		ciphertext60, ok32, ok32, sig64, ok32, ok32, sig64, ok32, sig64)
	if err == nil {
		t.Fatal("el INSERT de 60B en noise_key DEBÍA violar el CHECK length=32 y no lo hizo")
	}
	low := strings.ToLower(err.Error())
	if !strings.Contains(low, "check") && !strings.Contains(low, "constraint") {
		t.Fatalf("se esperaba violación de CHECK/constraint; error fue: %v", err)
	}
	t.Logf("DEMOSTRADO: el esquema upstream rechaza el ciphertext en noise_key -> %v", err)

	// Sanity: el mismo INSERT con 32B exactos SÍ pasa (la columna acepta el plaintext).
	_, err = db.ExecContext(ctx, `
		INSERT INTO whatsmeow_device
			(jid, registration_id, noise_key, identity_key,
			 signed_pre_key, signed_pre_key_id, signed_pre_key_sig,
			 adv_key, adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig)
		VALUES ('ok@s.whatsapp.net', 1, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)`,
		ok32, ok32, ok32, sig64, ok32, ok32, sig64, ok32, sig64)
	if err != nil {
		t.Fatalf("el INSERT con 32B exactos debía pasar y falló: %v", err)
	}
	t.Log("control OK: 32B plaintext entra en noise_key sin problema")
}
