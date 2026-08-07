package cryptostore

// device_anchor_test.go REPRODUCE el fallo de campo `FOREIGN KEY constraint failed (787)` y demuestra
// que el ancla lo cierra. Sin ensureDeviceAnchor (ver device_anchor.go) estos tests fallan con
// "FOREIGN KEY constraint failed" en el primer PutPushName; con el ancla pasan.
//
// El test se apoya en que openTestDB abre con PRAGMA foreign_keys=ON (internal/infra/db): lo VERIFICA
// explícitamente para que no pueda pasar en vacío si alguien apagara el pragma.

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-shared/envelope"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// contactJID es un par cualquiera (el "contacto" al que se le guarda push name / message secret).
func contactJID() types.JID { return types.NewJID("15557654321", types.DefaultUserServer) }

// TestDeviceAnchor_InheritedStoresDontViolateFK es el test que MUERDE: ejercita exactamente las cuatro
// escrituras que reventaban en el e2e real (push name, business name, message secret, app state sync
// key) más el app-state version y los chat settings, todas HEREDADAS del *sqlstore.SQLStore nativo y
// todas contra tablas con `FOREIGN KEY ... REFERENCES whatsmeow_device(jid)`. Sin el ancla devuelven
// "FOREIGN KEY constraint failed"; con el ancla persisten.
func TestDeviceAnchor_InheritedStoresDontViolateFK(t *testing.T) {
	ctx := context.Background()
	cont, dev := pairedDevice(t)
	db := cont.db

	// Sin foreign_keys=ON el test no probaría nada: las FK ni se evaluarían.
	var fkOn int
	if err := db.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&fkOn); err != nil {
		t.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	if fkOn != 1 {
		t.Fatalf("la BD de test debe abrir con foreign_keys=ON (got %d): el test no mordería", fkOn)
	}
	peer := contactJID()

	// 1. "Failed to save push name" — 1.970 ocurrencias en el log de campo.
	if _, _, err := dev.Contacts.PutPushName(ctx, peer, "Doña Ana"); err != nil {
		t.Fatalf("PutPushName (whatsmeow_contacts, FK a whatsmeow_device): %v", err)
	}
	// 2. "Failed to save business name" — 102 ocurrencias.
	if _, _, err := dev.Contacts.PutBusinessName(ctx, peer, "Ana's Bakery"); err != nil {
		t.Fatalf("PutBusinessName: %v", err)
	}
	// 3. "Failed to store message secret key" — 1.782 ocurrencias.
	if err := dev.MsgSecrets.PutMessageSecret(ctx, peer, peer, types.MessageID("MSG-1"), []byte("secreto")); err != nil {
		t.Fatalf("PutMessageSecret (whatsmeow_message_secrets, FK a whatsmeow_device): %v", err)
	}
	// 4. "Failed to store app state sync key" — 12 ocurrencias. Es la que dejaba el app-state MUERTO.
	if err := dev.AppStateKeys.PutAppStateSyncKey(ctx, []byte("key-id"), store.AppStateSyncKey{
		Data: []byte("datos"), Fingerprint: []byte("fp"), Timestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutAppStateSyncKey (whatsmeow_app_state_sync_keys, FK a whatsmeow_device): %v", err)
	}
	// 5. Versión de app-state y chat settings: rompían CALLADAS (no se loguean), y son las que impedían
	//    que el device se enterara jamás de su propio push name.
	var hash [128]byte
	if err := dev.AppState.PutAppStateVersion(ctx, "critical_block", 1, hash); err != nil {
		t.Fatalf("PutAppStateVersion (whatsmeow_app_state_version, FK a whatsmeow_device): %v", err)
	}
	if err := dev.ChatSettings.PutMutedUntil(ctx, peer, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("PutMutedUntil (whatsmeow_chat_settings, FK a whatsmeow_device): %v", err)
	}

	// Y persistió de verdad: lo leemos de vuelta.
	got, err := dev.Contacts.GetContact(ctx, peer)
	if err != nil {
		t.Fatalf("GetContact: %v", err)
	}
	if got.PushName != "Doña Ana" {
		t.Errorf("PushName persistido: got %q want %q", got.PushName, "Doña Ana")
	}
	secret, _, err := dev.MsgSecrets.GetMessageSecret(ctx, peer, peer, types.MessageID("MSG-1"))
	if err != nil {
		t.Fatalf("GetMessageSecret: %v", err)
	}
	if !bytes.Equal(secret, []byte("secreto")) {
		t.Errorf("message secret persistido: got %q", secret)
	}
}

// TestDeviceAnchor_NoKeyMaterial es el guardián del ADR-0007: la fila ancla existe SOLO para que las FK
// resuelvan y NO puede contener material criptográfico. Verifica que cada columna de clave es de ceros
// (del largo que exige su CHECK) y que NINGUNA coincide con el material real del device.
func TestDeviceAnchor_NoKeyMaterial(t *testing.T) {
	ctx := context.Background()
	cont, dev := pairedDevice(t)
	db := cont.db

	var (
		regID       int64
		spkID       int64
		noise       []byte
		identity    []byte
		spk         []byte
		spkSig      []byte
		advKey      []byte
		advDetails  []byte
		advAccSig   []byte
		advAccKey   []byte
		advDevSig   []byte
		platform    string
		pushName    string
		bizName     string
		anchorCount int
	)
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM whatsmeow_device`).Scan(&anchorCount); err != nil {
		t.Fatalf("contar whatsmeow_device: %v", err)
	}
	if anchorCount != 1 {
		t.Fatalf("se esperaba exactamente 1 ancla en whatsmeow_device, hay %d", anchorCount)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT registration_id, signed_pre_key_id, noise_key, identity_key, signed_pre_key,
		       signed_pre_key_sig, adv_key, adv_details, adv_account_sig, adv_account_sig_key,
		       adv_device_sig, platform, push_name, business_name
		FROM whatsmeow_device WHERE jid=?`, dev.ID.String()).Scan(
		&regID, &spkID, &noise, &identity, &spk, &spkSig, &advKey, &advDetails,
		&advAccSig, &advAccKey, &advDevSig, &platform, &pushName, &bizName,
	); err != nil {
		t.Fatalf("leer el ancla: %v", err)
	}

	zeros := func(n int) []byte { return make([]byte, n) }
	for _, c := range []struct {
		name string
		got  []byte
		want []byte
	}{
		{"noise_key", noise, zeros(anchorKeyLen)},
		{"identity_key", identity, zeros(anchorKeyLen)},
		{"signed_pre_key", spk, zeros(anchorKeyLen)},
		{"signed_pre_key_sig", spkSig, zeros(anchorSigLen)},
		{"adv_key", advKey, zeros(anchorKeyLen)},
		{"adv_details", advDetails, zeros(anchorKeyLen)},
		{"adv_account_sig", advAccSig, zeros(anchorSigLen)},
		{"adv_account_sig_key", advAccKey, zeros(anchorKeyLen)},
		{"adv_device_sig", advDevSig, zeros(anchorSigLen)},
	} {
		if !bytes.Equal(c.got, c.want) {
			t.Errorf("ancla.%s NO es relleno inerte de ceros: %x", c.name, c.got)
		}
	}
	// Contraste explícito contra el material REAL del device: nada del device vivo se filtró al ancla.
	if bytes.Equal(noise, dev.NoiseKey.Priv[:]) || bytes.Equal(identity, dev.IdentityKey.Priv[:]) {
		t.Fatal("VIOLACIÓN ADR-0007: el ancla contiene material de clave real del device")
	}
	if bytes.Equal(spk, dev.SignedPreKey.Priv[:]) || bytes.Equal(spkSig, dev.SignedPreKey.Signature[:]) {
		t.Fatal("VIOLACIÓN ADR-0007: el ancla contiene la signed pre-key real del device")
	}
	if bytes.Equal(advDetails, dev.Account.Details) || bytes.Equal(advKey, dev.AdvSecretKey) {
		t.Fatal("VIOLACIÓN ADR-0007: el ancla contiene material ADV real del device")
	}
	if regID != 0 || spkID != 0 {
		t.Errorf("el ancla debe llevar identificadores inertes (0/0); got registration_id=%d signed_pre_key_id=%d", regID, spkID)
	}
	if platform != anchorPlatform {
		t.Errorf("el ancla debe ir marcada como %q en platform; got %q", anchorPlatform, platform)
	}
	if pushName != "" || bizName != "" {
		t.Errorf("el ancla no debe llevar metadata del device (vive cifrada en msg_enc_device); got push=%q biz=%q", pushName, bizName)
	}
}

// TestDeviceAnchor_HealsExistingStore cubre las BD QUE YA EXISTEN en máquinas reales: un store con meses
// de uso, su device en msg_enc_device y `whatsmeow_device` VACÍA. Al reabrirlo (GetDevice del arranque)
// el ancla debe crearse sola, sin re-escanear ni re-emparejar, y las escrituras heredadas empiezan a
// funcionar. Se simula borrando el ancla a mano tras el pairing.
func TestDeviceAnchor_HealsExistingStore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	env, err := envelope.NewEnvelope(newDEK(t))
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}

	db1 := openAt(t, path)
	cont1, err := newCryptoContainer(ctx, db1, DialectSQLite, env)
	if err != nil {
		t.Fatalf("newCryptoContainer: %v", err)
	}
	dev, _ := syntheticDevice(t)
	if err := cont1.PutDevice(ctx, dev); err != nil {
		t.Fatalf("PutDevice: %v", err)
	}
	jid := *dev.ID
	// Estado PRE-arreglo: la tabla nativa vacía, exactamente como los stores en producción.
	if _, err := db1.ExecContext(ctx, `DELETE FROM whatsmeow_device`); err != nil {
		t.Fatalf("vaciar whatsmeow_device: %v", err)
	}
	_ = db1.Close()

	db2 := openAt(t, path)
	defer func() { _ = db2.Close() }()
	cont2, err := newCryptoContainer(ctx, db2, DialectSQLite, env)
	if err != nil {
		t.Fatalf("newCryptoContainer (reabrir): %v", err)
	}
	restored, err := cont2.GetDevice(ctx, jid)
	if err != nil {
		t.Fatalf("GetDevice: %v", err)
	}
	if restored == nil {
		t.Fatal("GetDevice devolvió nil sobre un store ya emparejado")
	}
	var n int
	if err := db2.QueryRowContext(ctx, `SELECT COUNT(*) FROM whatsmeow_device WHERE jid=?`, jid.String()).Scan(&n); err != nil {
		t.Fatalf("contar ancla tras GetDevice: %v", err)
	}
	if n != 1 {
		t.Fatalf("el arranque sobre un store existente debía crear el ancla; hay %d filas", n)
	}
	if _, _, err := restored.Contacts.PutPushName(ctx, contactJID(), "Ana"); err != nil {
		t.Fatalf("PutPushName sobre store curado: %v", err)
	}
}

// TestDeviceAnchor_DeleteCascades: al desvincular (Device.Delete → DeleteDevice) el ancla se borra y la
// FK ON DELETE CASCADE arrastra las filas whatsmeow_* del device. Sin esto, un re-emparejamiento del
// MISMO JID heredaría contactos y app-state del emparejamiento anterior.
func TestDeviceAnchor_DeleteCascades(t *testing.T) {
	ctx := context.Background()
	cont, dev := pairedDevice(t)
	db := cont.db
	if _, _, err := dev.Contacts.PutPushName(ctx, contactJID(), "Ana"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}

	if err := cont.DeleteDevice(ctx, dev); err != nil {
		t.Fatalf("DeleteDevice: %v", err)
	}
	for _, q := range []struct {
		name  string
		query string
	}{
		{"msg_enc_device", `SELECT COUNT(*) FROM msg_enc_device WHERE jid=?`},
		{"whatsmeow_device", `SELECT COUNT(*) FROM whatsmeow_device WHERE jid=?`},
		{"whatsmeow_contacts (cascada)", `SELECT COUNT(*) FROM whatsmeow_contacts WHERE our_jid=?`},
	} {
		var n int
		if err := db.QueryRowContext(ctx, q.query, dev.ID.String()).Scan(&n); err != nil {
			t.Fatalf("contar %s: %v", q.name, err)
		}
		if n != 0 {
			t.Errorf("%s debía quedar vacía tras DeleteDevice; quedan %d filas", q.name, n)
		}
	}
}

// TestDeviceAnchor_Idempotent: asegurar el ancla dos veces no duplica ni pisa (ON CONFLICT DO NOTHING).
// Importa porque wrapStores corre en CADA arranque y en cada Save del pairing.
func TestDeviceAnchor_Idempotent(t *testing.T) {
	ctx := context.Background()
	cont, dev := pairedDevice(t)
	db := cont.db
	for i := 0; i < 3; i++ {
		if err := ensureDeviceAnchor(ctx, db, *dev.ID); err != nil {
			t.Fatalf("ensureDeviceAnchor (pasada %d): %v", i, err)
		}
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM whatsmeow_device WHERE jid=?`, dev.ID.String()).Scan(&n); err != nil {
		t.Fatalf("contar ancla: %v", err)
	}
	if n != 1 {
		t.Fatalf("el ancla debe ser única por JID; hay %d filas", n)
	}
}

// TestDeviceAnchor_PerDeviceOnSharedDB: sobre la BD ÚNICA compartida (Plan 022) conviven N devices, cada
// uno con SU DEK y SU container. Cada uno debe tener su propia ancla, y las escrituras heredadas de uno
// no deben depender del otro.
func TestDeviceAnchor_PerDeviceOnSharedDB(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)

	newContainerWithDevice := func(number string) *store.Device {
		t.Helper()
		env, err := envelope.NewEnvelope(newDEK(t))
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		cont, err := newCryptoContainer(ctx, db, DialectSQLite, env)
		if err != nil {
			t.Fatalf("newCryptoContainer: %v", err)
		}
		dev, _ := syntheticDevice(t)
		jid := types.NewJID(number, types.DefaultUserServer)
		dev.ID = &jid
		if err := cont.PutDevice(ctx, dev); err != nil {
			t.Fatalf("PutDevice(%s): %v", number, err)
		}
		return dev
	}

	devA := newContainerWithDevice("15550000001")
	devB := newContainerWithDevice("15550000002")

	for _, dev := range []*store.Device{devA, devB} {
		if _, _, err := dev.Contacts.PutPushName(ctx, contactJID(), "Ana"); err != nil {
			t.Fatalf("PutPushName(%s): %v", dev.ID.String(), err)
		}
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM whatsmeow_device`).Scan(&n); err != nil {
		t.Fatalf("contar anclas: %v", err)
	}
	if n != 2 {
		t.Fatalf("se esperaban 2 anclas (una por device) en la BD compartida; hay %d", n)
	}
}
