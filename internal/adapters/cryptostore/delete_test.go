package cryptostore

import (
	"context"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

// TestDeleteDevice_RemovesAllPerJIDRows verifica que DeleteDevice borra TODAS las filas msg_enc_* de la
// sesión (device + identities + sessions + prekeys + sender_keys) y que es IDEMPOTENTE (un segundo
// borrado, o el de una sesión inexistente, NO falla). No requiere DEK: solo borra ciphertext.
func TestDeleteDevice_RemovesAllPerJIDRows(t *testing.T) {
	db, _ := openTestDB(t)
	ctx := context.Background()
	jid := "56123@s.whatsapp.net"

	// Sembramos una fila en cada tabla por JID con ciphertext arbitrario (no se descifra al borrar).
	ct := []byte("ciphertext")
	seed := []struct {
		stmt string
		args []any
	}{
		{`INSERT INTO msg_enc_device (jid, registration_id, signed_pre_key_id, noise_priv, identity_priv,
			signed_pre_key_priv, signed_pre_key_sig, adv_secret_key, adv_details, adv_account_sig,
			adv_account_sig_key, adv_device_sig) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
			[]any{jid, 1, 1, ct, ct, ct, ct, ct, ct, ct, ct, ct}},
		{`INSERT INTO msg_enc_identities (our_jid, their_id, identity) VALUES (?,?,?)`, []any{jid, "peer", ct}},
		{`INSERT INTO msg_enc_sessions (our_jid, their_id, session) VALUES (?,?,?)`, []any{jid, "peer", ct}},
		{`INSERT INTO msg_enc_prekeys (jid, key_id, priv, uploaded) VALUES (?,?,?,?)`, []any{jid, 1, ct, 0}},
		{`INSERT INTO msg_enc_sender_keys (our_jid, chat_id, sender_id, sender_key) VALUES (?,?,?,?)`, []any{jid, "g", "u", ct}},
	}
	for _, s := range seed {
		if _, err := db.ExecContext(ctx, s.stmt, s.args...); err != nil {
			t.Fatalf("sembrar fila: %v", err)
		}
	}

	parsed, err := types.ParseJID(jid)
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	if err := DeleteDevice(ctx, db, DialectSQLite, parsed); err != nil {
		t.Fatalf("DeleteDevice: %v", err)
	}

	for _, tbl := range []string{"msg_enc_device", "msg_enc_identities", "msg_enc_sessions", "msg_enc_prekeys", "msg_enc_sender_keys"} {
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&n); err != nil {
			t.Fatalf("contar %s: %v", tbl, err)
		}
		if n != 0 {
			t.Errorf("%s: quedaron %d filas tras DeleteDevice", tbl, n)
		}
	}

	// Idempotente: borrar de nuevo (ya ausente) no es error.
	if err := DeleteDevice(ctx, db, DialectSQLite, parsed); err != nil {
		t.Fatalf("DeleteDevice idempotente: %v", err)
	}
}

// TestDeleteDevice_EmptyDB: borrar en una BD recién migrada sin device pareado no falla (idempotencia
// sobre tablas vacías + creación idempotente del esquema whatsmeow_*).
func TestDeleteDevice_EmptyDB(t *testing.T) {
	db, _ := openTestDB(t)
	jid, err := types.ParseJID("56000@s.whatsapp.net")
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	if err := DeleteDevice(context.Background(), db, DialectSQLite, jid); err != nil {
		t.Fatalf("DeleteDevice sobre BD vacía: %v", err)
	}
}
