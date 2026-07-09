package cryptostore

import (
	"bytes"
	"context"
	"testing"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// foreignContainer es un store.DeviceContainer ajeno (no producido por este paquete): sirve para
// ejercer las guardas de type-assert de NewDeviceForPairing / LoadDevice.
type foreignContainer struct{}

func (foreignContainer) PutDevice(context.Context, *store.Device) error    { return nil }
func (foreignContainer) DeleteDevice(context.Context, *store.Device) error { return nil }

// pairedDevice construye un container cifrado, fabrica un device fresco, lo "pairea"
// sintéticamente (fija JID + Account) y lo persiste (Save -> PutDevice -> wrapStores), dejando
// los stores de sesión cifrados cableados y listos para usar.
func pairedDevice(t *testing.T) (*cryptoContainer, *store.Device) {
	t.Helper()
	ctx := context.Background()
	db, _ := openTestDB(t)
	raw, err := NewEncryptedContainer(ctx, db, DialectSQLite, newDEK(t))
	if err != nil {
		t.Fatalf("NewEncryptedContainer: %v", err)
	}
	cont := raw.(*cryptoContainer)

	dev := NewDeviceForPairing(raw)
	if dev.Container == nil {
		t.Fatal("el device fresco no quedó ligado a un Container")
	}
	jid := types.NewJID("15551230000", types.DefaultUserServer)
	jid.Device = 0
	dev.ID = &jid
	dev.Account = newSyntheticAccount(t)
	if err := dev.Save(ctx); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return cont, dev
}

// TestStoreOps_DBErrors cierra el *sql.DB subyacente y comprueba que TODAS las operaciones del
// store propagan el error de BD (cubre los branches `if err != nil { return err }` que siguen a
// cada consulta SQL). El sellado/desellado con la DEK no falla; lo que falla es la BD cerrada.
func TestStoreOps_DBErrors(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	raw, err := NewEncryptedContainer(ctx, db, DialectSQLite, newDEK(t))
	if err != nil {
		t.Fatal(err)
	}
	cont := raw.(*cryptoContainer)
	dev := NewDeviceForPairing(raw)
	jid := types.NewJID("15551230000", types.DefaultUserServer)
	jid.Device = 0
	dev.ID = &jid
	dev.Account = newSyntheticAccount(t)
	if err := dev.Save(ctx); err != nil { // wrapStores: deja los stores cifrados cableados
		t.Fatalf("Save: %v", err)
	}

	_ = db.Close() // a partir de aquí, toda consulta debe fallar

	cs := dev // alias legible
	checks := []struct {
		name string
		err  error
	}{
		{"PutDevice", cont.PutDevice(ctx, dev)},
		{"DeleteDevice", cont.DeleteDevice(ctx, dev)},
		{"PutSession", cs.Sessions.PutSession(ctx, "a:0", []byte("x"))},
		{"PutManySessions", cs.Sessions.PutManySessions(ctx, map[string][]byte{"a:0": []byte("x")})},
		{"DeleteSession", cs.Sessions.DeleteSession(ctx, "a:0")},
		{"DeleteAllSessions", cs.Sessions.DeleteAllSessions(ctx, "a")},
		{"PutIdentity", cs.Identities.PutIdentity(ctx, "a:0", [32]byte{})},
		{"DeleteIdentity", cs.Identities.DeleteIdentity(ctx, "a:0")},
		{"DeleteAllIdentities", cs.Identities.DeleteAllIdentities(ctx, "a")},
		{"RemovePreKey", cs.PreKeys.RemovePreKey(ctx, 1)},
		{"MarkPreKeysAsUploaded", cs.PreKeys.MarkPreKeysAsUploaded(ctx, 1)},
		{"PutSenderKey", cs.SenderKeys.PutSenderKey(ctx, "g", "u", []byte("x"))},
	}
	for _, c := range checks {
		if c.err == nil {
			t.Errorf("%s debía fallar con la BD cerrada", c.name)
		}
	}

	// Operaciones con valor de retorno + error.
	if _, err := cont.GetDevice(ctx, jid); err == nil {
		t.Error("GetDevice debía fallar con la BD cerrada")
	}
	if _, err := cs.Sessions.GetSession(ctx, "a:0"); err == nil {
		t.Error("GetSession debía fallar")
	}
	if _, err := cs.Sessions.HasSession(ctx, "a:0"); err == nil {
		t.Error("HasSession debía fallar")
	}
	if _, err := cs.Sessions.GetManySessions(ctx, []string{"a:0"}); err == nil {
		t.Error("GetManySessions debía fallar")
	}
	if _, err := cs.Identities.IsTrustedIdentity(ctx, "a:0", [32]byte{}); err == nil {
		t.Error("IsTrustedIdentity debía fallar")
	}
	if _, err := cs.PreKeys.GenOnePreKey(ctx); err == nil {
		t.Error("GenOnePreKey debía fallar")
	}
	if _, err := cs.PreKeys.GetOrGenPreKeys(ctx, 2); err == nil {
		t.Error("GetOrGenPreKeys debía fallar")
	}
	if _, err := cs.PreKeys.GetPreKey(ctx, 1); err == nil {
		t.Error("GetPreKey debía fallar")
	}
	if _, err := cs.PreKeys.UploadedPreKeyCount(ctx); err == nil {
		t.Error("UploadedPreKeyCount debía fallar")
	}
	if _, err := cs.SenderKeys.GetSenderKey(ctx, "g", "u"); err == nil {
		t.Error("GetSenderKey debía fallar")
	}
}

func TestNewEncryptedContainer_BadDEKSize(t *testing.T) {
	db, _ := openTestDB(t)
	if _, err := NewEncryptedContainer(context.Background(), db, DialectSQLite, []byte("corta")); err == nil {
		t.Fatal("una DEK que no mide 32 bytes debía fallar")
	}
}

func TestNewDeviceForPairing_PanicsOnForeignContainer(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewDeviceForPairing debía paniquear con un container ajeno")
		}
	}()
	NewDeviceForPairing(foreignContainer{})
}

func TestLoadDevice_PanicsOnForeignContainer(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("LoadDevice debía paniquear con un container ajeno")
		}
	}()
	_, _ = LoadDevice(context.Background(), foreignContainer{}, types.EmptyJID)
}

func TestLoadDevice_RoundTripAndMissing(t *testing.T) {
	ctx := context.Background()
	cont, dev := pairedDevice(t)

	got, err := LoadDevice(ctx, cont, *dev.ID)
	if err != nil {
		t.Fatalf("LoadDevice: %v", err)
	}
	if got == nil || got.ID.String() != dev.ID.String() {
		t.Fatal("LoadDevice no recuperó el device pareado")
	}

	// JID inexistente => (nil, nil).
	other := types.NewJID("99999999999", types.DefaultUserServer)
	missing, err := LoadDevice(ctx, cont, other)
	if err != nil {
		t.Fatalf("LoadDevice (missing): %v", err)
	}
	if missing != nil {
		t.Fatal("LoadDevice de un JID inexistente debía devolver nil")
	}
}

func TestDeleteDevice(t *testing.T) {
	ctx := context.Background()
	cont, dev := pairedDevice(t)
	if err := cont.DeleteDevice(ctx, dev); err != nil {
		t.Fatalf("DeleteDevice: %v", err)
	}
	got, err := cont.GetDevice(ctx, *dev.ID)
	if err != nil {
		t.Fatalf("GetDevice tras borrar: %v", err)
	}
	if got != nil {
		t.Fatal("el device debía haber desaparecido tras DeleteDevice")
	}
	// DeleteDevice sin JID => error.
	if err := cont.DeleteDevice(ctx, &store.Device{}); err == nil {
		t.Fatal("DeleteDevice sin JID debía fallar")
	}
}

func TestPutDevice_RequiresJID(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	raw, err := NewEncryptedContainer(ctx, db, DialectSQLite, newDEK(t))
	if err != nil {
		t.Fatal(err)
	}
	cont := raw.(*cryptoContainer)
	dev := cont.newDevice() // sin ID
	if err := cont.PutDevice(ctx, dev); err == nil {
		t.Fatal("PutDevice sin JID debía fallar")
	}
}

func TestSessionStore_AllOps(t *testing.T) {
	ctx := context.Background()
	_, dev := pairedDevice(t)
	ss := dev.Sessions

	// GetSession / HasSession sobre ausente.
	if got, err := ss.GetSession(ctx, "a:0"); err != nil || got != nil {
		t.Fatalf("GetSession ausente: got=%v err=%v", got, err)
	}
	if has, err := ss.HasSession(ctx, "a:0"); err != nil || has {
		t.Fatalf("HasSession ausente: has=%v err=%v", has, err)
	}

	// PutManySessions + GetManySessions (cifrado por lote).
	want := map[string][]byte{
		"15551110000.0:0": []byte("sesion-uno-variable"),
		"15552220000.0:0": []byte("sesion-dos-mas-larga-aaaaaaaa"),
	}
	if err := dev.Sessions.PutManySessions(ctx, want); err != nil {
		t.Fatalf("PutManySessions: %v", err)
	}
	if has, err := ss.HasSession(ctx, "15551110000.0:0"); err != nil || !has {
		t.Fatalf("HasSession presente: has=%v err=%v", has, err)
	}
	addrs := []string{"15551110000.0:0", "15552220000.0:0", "ausente:0"}
	many, err := ss.GetManySessions(ctx, addrs)
	if err != nil {
		t.Fatalf("GetManySessions: %v", err)
	}
	for a, w := range want {
		if !bytes.Equal(many[a], w) {
			t.Errorf("GetManySessions[%s] = %x, quería %x", a, many[a], w)
		}
	}
	if v, ok := many["ausente:0"]; !ok || v != nil {
		t.Errorf("GetManySessions debía incluir la clave ausente con valor nil")
	}
	// Lista vacía => (nil, nil).
	if m, err := ss.GetManySessions(ctx, nil); err != nil || m != nil {
		t.Fatalf("GetManySessions(nil): m=%v err=%v", m, err)
	}

	// Delete single + DeleteAll por prefijo de teléfono.
	if err := ss.DeleteSession(ctx, "15551110000.0:0"); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if has, _ := ss.HasSession(ctx, "15551110000.0:0"); has {
		t.Error("la sesión borrada seguía presente")
	}
	// El prefijo es la parte ANTES del ":" del address (formato user.device:agent), igual que
	// upstream (LIKE phone || ':%').
	if err := ss.DeleteAllSessions(ctx, "15552220000.0"); err != nil {
		t.Fatalf("DeleteAllSessions: %v", err)
	}
	if has, _ := ss.HasSession(ctx, "15552220000.0:0"); has {
		t.Error("DeleteAllSessions no borró por prefijo")
	}

	// MigratePNToLID es no-op seguro.
	if err := dev.Sessions.MigratePNToLID(ctx, types.EmptyJID, types.EmptyJID); err != nil {
		t.Fatalf("MigratePNToLID: %v", err)
	}
}

func TestIdentityStore_AllOps(t *testing.T) {
	ctx := context.Background()
	_, dev := pairedDevice(t)
	is := dev.Identities

	var k1 [32]byte
	copy(k1[:], bytes.Repeat([]byte{0x11}, 32))

	// Identidad desconocida => confiable.
	if trusted, err := is.IsTrustedIdentity(ctx, "x:0", k1); err != nil || !trusted {
		t.Fatalf("IsTrustedIdentity desconocida: trusted=%v err=%v", trusted, err)
	}
	if err := is.PutIdentity(ctx, "x:0", k1); err != nil {
		t.Fatalf("PutIdentity: %v", err)
	}
	if trusted, err := is.IsTrustedIdentity(ctx, "x:0", k1); err != nil || !trusted {
		t.Fatalf("IsTrustedIdentity coincidente: trusted=%v err=%v", trusted, err)
	}
	var k2 [32]byte
	copy(k2[:], bytes.Repeat([]byte{0x22}, 32))
	if trusted, err := is.IsTrustedIdentity(ctx, "x:0", k2); err != nil || trusted {
		t.Fatalf("IsTrustedIdentity distinta debía ser false: trusted=%v err=%v", trusted, err)
	}

	if err := is.DeleteIdentity(ctx, "x:0"); err != nil {
		t.Fatalf("DeleteIdentity: %v", err)
	}
	// Reañadir y borrar por prefijo (parte antes del ":" del address, igual que upstream).
	if err := is.PutIdentity(ctx, "15553330000.0:0", k1); err != nil {
		t.Fatalf("PutIdentity: %v", err)
	}
	// Antes de borrar, una clave DISTINTA a la sembrada NO es de confianza.
	if trusted, _ := is.IsTrustedIdentity(ctx, "15553330000.0:0", k2); trusted {
		t.Fatal("precondición: k2 debía ser no-confiable con k1 sembrada")
	}
	if err := is.DeleteAllIdentities(ctx, "15553330000.0"); err != nil {
		t.Fatalf("DeleteAllIdentities: %v", err)
	}
	// Tras borrar, la identidad es desconocida => cualquier clave es confiable.
	if trusted, _ := is.IsTrustedIdentity(ctx, "15553330000.0:0", k2); !trusted {
		t.Error("tras DeleteAllIdentities la identidad debía quedar desconocida (=confiable)")
	}
}

func TestPreKeyStore_AllOps(t *testing.T) {
	ctx := context.Background()
	_, dev := pairedDevice(t)
	pk := dev.PreKeys

	// GenOnePreKey (uploaded=true) + GetPreKey.
	one, err := pk.GenOnePreKey(ctx)
	if err != nil {
		t.Fatalf("GenOnePreKey: %v", err)
	}
	got, err := pk.GetPreKey(ctx, one.KeyID)
	if err != nil || got == nil {
		t.Fatalf("GetPreKey: got=%v err=%v", got, err)
	}
	if !bytes.Equal(got.Priv[:], one.Priv[:]) {
		t.Error("GetPreKey no recuperó la privada sembrada")
	}
	// GetPreKey ausente => nil.
	if g, err := pk.GetPreKey(ctx, 999999); err != nil || g != nil {
		t.Fatalf("GetPreKey ausente: g=%v err=%v", g, err)
	}

	// GenOnePreKey marca uploaded=true; debe contarse.
	if n, err := pk.UploadedPreKeyCount(ctx); err != nil || n != 1 {
		t.Fatalf("UploadedPreKeyCount tras GenOnePreKey: n=%d err=%v", n, err)
	}

	// GetOrGenPreKeys genera no-subidas; reusarlas no debe duplicar.
	first, err := pk.GetOrGenPreKeys(ctx, 4)
	if err != nil || len(first) != 4 {
		t.Fatalf("GetOrGenPreKeys(4): len=%d err=%v", len(first), err)
	}
	again, err := pk.GetOrGenPreKeys(ctx, 4)
	if err != nil || len(again) != 4 {
		t.Fatalf("GetOrGenPreKeys(4) reuso: len=%d err=%v", len(again), err)
	}
	if again[0].KeyID != first[0].KeyID {
		t.Error("GetOrGenPreKeys no reutilizó las prekeys no subidas")
	}

	// MarkPreKeysAsUploaded sube las generadas; UploadedPreKeyCount sube.
	if err := pk.MarkPreKeysAsUploaded(ctx, again[3].KeyID); err != nil {
		t.Fatalf("MarkPreKeysAsUploaded: %v", err)
	}
	n, err := pk.UploadedPreKeyCount(ctx)
	if err != nil || n != 5 { // 1 (GenOnePreKey) + 4 (subidas ahora)
		t.Fatalf("UploadedPreKeyCount: n=%d err=%v", n, err)
	}

	// RemovePreKey.
	if err := pk.RemovePreKey(ctx, one.KeyID); err != nil {
		t.Fatalf("RemovePreKey: %v", err)
	}
	if g, _ := pk.GetPreKey(ctx, one.KeyID); g != nil {
		t.Error("la prekey borrada seguía presente")
	}
}

func TestSenderKeyStore_Ops(t *testing.T) {
	ctx := context.Background()
	_, dev := pairedDevice(t)
	sk := dev.SenderKeys

	// Ausente => nil.
	if g, err := sk.GetSenderKey(ctx, "g@g.us", "u:0"); err != nil || g != nil {
		t.Fatalf("GetSenderKey ausente: g=%v err=%v", g, err)
	}
	want := []byte("sender-key-de-grupo-xyz")
	if err := sk.PutSenderKey(ctx, "g@g.us", "u:0", want); err != nil {
		t.Fatalf("PutSenderKey: %v", err)
	}
	got, err := sk.GetSenderKey(ctx, "g@g.us", "u:0")
	if err != nil {
		t.Fatalf("GetSenderKey: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("GetSenderKey = %x, quería %x", got, want)
	}
	// Sobrescribir (INSERT OR REPLACE).
	want2 := []byte("sender-key-rotada")
	if err := sk.PutSenderKey(ctx, "g@g.us", "u:0", want2); err != nil {
		t.Fatalf("PutSenderKey (replace): %v", err)
	}
	got2, _ := sk.GetSenderKey(ctx, "g@g.us", "u:0")
	if !bytes.Equal(got2, want2) {
		t.Errorf("tras replace = %x, quería %x", got2, want2)
	}
}
