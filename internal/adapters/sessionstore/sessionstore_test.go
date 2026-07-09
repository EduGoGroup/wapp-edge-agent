package sessionstore

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
)

// newStore abre un store SQLite temporal YA migrado (0001+0002+0003) y devuelve el Store de sesiones.
func newStore(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.db")
	database, err := db.OpenAndMigrate(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenAndMigrate: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return New(database), database
}

// TestUpsertGetRoundTrip: insertar y leer una sesión preserva session_id/jid/estado/store_dir/timestamps.
func TestUpsertGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	paired := time.Unix(1_700_000_000, 0).UTC()
	updated := time.Unix(1_700_000_500, 0).UTC()
	in := domain.Session{
		SessionID: "11111111-1111-4111-8111-111111111111",
		JID:       "56984467443:47@s.whatsapp.net",
		State:     domain.SessionStateActive,
		StoreDir:  "sessions/11111111-1111-4111-8111-111111111111",
		PairedAt:  paired,
		UpdatedAt: updated,
	}
	if err := store.Upsert(ctx, in); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, err := store.Get(ctx, in.SessionID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.SessionID != in.SessionID || got.JID != in.JID || got.State != in.State || got.StoreDir != in.StoreDir {
		t.Fatalf("Get = %+v, esperaba %+v", got, in)
	}
	if !got.PairedAt.Equal(paired) || !got.UpdatedAt.Equal(updated) {
		t.Fatalf("timestamps = paired %v / updated %v, esperaba %v / %v",
			got.PairedAt, got.UpdatedAt, paired, updated)
	}
}

// TestUpsertPairingJIDIsNull: una sesión en 'pairing' (jid vacío, paired_at cero) persiste el JID como
// NULL y vuelve como cadena vacía / cero de Go (el número se descubre recién en PairSuccess).
func TestUpsertPairingJIDIsNull(t *testing.T) {
	ctx := context.Background()
	store, database := newStore(t)

	in := domain.Session{
		SessionID: "22222222-2222-4222-8222-222222222222",
		State:     domain.SessionStatePairing,
		StoreDir:  "sessions/22222222-2222-4222-8222-222222222222",
	}
	if err := store.Upsert(ctx, in); err != nil {
		t.Fatalf("Upsert pairing: %v", err)
	}

	// La columna jid debe ser NULL (no cadena vacía) para no chocar con el índice único parcial.
	var jidNull bool
	if err := database.QueryRowContext(ctx,
		`SELECT jid IS NULL FROM devices WHERE session_id = ?`, in.SessionID).Scan(&jidNull); err != nil {
		t.Fatalf("consulta jid IS NULL: %v", err)
	}
	if !jidNull {
		t.Fatal("se esperaba jid NULL para una sesión en pairing")
	}

	got, err := store.Get(ctx, in.SessionID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.JID != "" || !got.PairedAt.IsZero() || got.State != domain.SessionStatePairing {
		t.Fatalf("Get = %+v, esperaba jid vacío / paired cero / state pairing", got)
	}
}

// TestUpsertUpdatesState: re-upsertar el mismo session_id actualiza jid/estado/updated_at sin duplicar
// filas (promoción pairing -> active al PairSuccess).
func TestUpsertUpdatesState(t *testing.T) {
	ctx := context.Background()
	store, database := newStore(t)

	id := "33333333-3333-4333-8333-333333333333"
	t0 := time.Unix(1_700_000_000, 0).UTC()
	if err := store.Upsert(ctx, domain.Session{
		SessionID: id, State: domain.SessionStatePairing, StoreDir: "sessions/" + id, UpdatedAt: t0,
	}); err != nil {
		t.Fatalf("Upsert #1 (pairing): %v", err)
	}
	t1 := time.Unix(1_700_009_999, 0).UTC()
	if err := store.Upsert(ctx, domain.Session{
		SessionID: id, JID: "j@s.whatsapp.net", State: domain.SessionStateActive,
		StoreDir: "sessions/" + id, PairedAt: t1, UpdatedAt: t1,
	}); err != nil {
		t.Fatalf("Upsert #2 (active): %v", err)
	}

	var n int
	if err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM devices WHERE session_id=?`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("filas para session_id = %d, esperaba 1 (upsert, no insert duplicado)", n)
	}

	got, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != domain.SessionStateActive || got.JID != "j@s.whatsapp.net" {
		t.Fatalf("tras promoción: state=%q jid=%q, esperaba active / j@s.whatsapp.net", got.State, got.JID)
	}
	if !got.UpdatedAt.Equal(t1) || !got.PairedAt.Equal(t1) {
		t.Fatalf("timestamps = %v / %v, esperaba %v", got.UpdatedAt, got.PairedAt, t1)
	}
}

// TestListAndListActive: List devuelve todas; ListActive solo las 'active'. Orden por updated_at.
func TestListAndListActive(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	got, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List vacío: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("List inicial = %d, esperaba 0", len(got))
	}

	older := time.Unix(1_700_000_000, 0).UTC()
	newer := time.Unix(1_700_009_999, 0).UTC()
	// Una activa (más antigua), una en pairing (más reciente).
	if err := store.Upsert(ctx, domain.Session{
		SessionID: "a", JID: "a@x", State: domain.SessionStateActive, StoreDir: "sessions/a",
		PairedAt: older, UpdatedAt: older,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(ctx, domain.Session{
		SessionID: "b", State: domain.SessionStatePairing, StoreDir: "sessions/b", UpdatedAt: newer,
	}); err != nil {
		t.Fatal(err)
	}

	got, err = store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 || got[0].SessionID != "a" || got[1].SessionID != "b" {
		t.Fatalf("List = %+v, esperaba [a, b] por updated_at asc", got)
	}

	active, err := store.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	if len(active) != 1 || active[0].SessionID != "a" {
		t.Fatalf("ListActive = %+v, esperaba solo [a]", active)
	}
}

// TestGetNotFound: Get de un session_id inexistente devuelve app.ErrSessionNotFound.
func TestGetNotFound(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)
	if _, err := store.Get(ctx, "noexiste"); !errors.Is(err, app.ErrSessionNotFound) {
		t.Fatalf("error = %v, esperaba app.ErrSessionNotFound", err)
	}
}

// TestUpsertEmptySessionID: un session_id vacío es inválido (no se persiste material sin clave).
func TestUpsertEmptySessionID(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)
	if err := store.Upsert(ctx, domain.Session{State: domain.SessionStateActive, StoreDir: "x"}); err == nil {
		t.Fatal("se esperaba error con session_id vacío")
	}
}

// TestPartialUniqueJID: dos sesiones DISTINTAS no pueden compartir un JID no-NULL (índice único parcial),
// pero varias sesiones en 'pairing' (jid NULL) coexisten sin chocar.
func TestPartialUniqueJID(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	if err := store.Upsert(ctx, domain.Session{
		SessionID: "s1", JID: "dup@x", State: domain.SessionStateActive, StoreDir: "sessions/s1",
	}); err != nil {
		t.Fatalf("Upsert s1: %v", err)
	}
	// Mismo jid en otra sesión: debe violar ux_sessions_jid.
	if err := store.Upsert(ctx, domain.Session{
		SessionID: "s2", JID: "dup@x", State: domain.SessionStateActive, StoreDir: "sessions/s2",
	}); err == nil {
		t.Fatal("se esperaba violación de unicidad de jid entre sesiones distintas")
	}

	// Dos sesiones en pairing (jid NULL): permitidas (el índice es parcial, WHERE jid IS NOT NULL).
	if err := store.Upsert(ctx, domain.Session{
		SessionID: "p1", State: domain.SessionStatePairing, StoreDir: "sessions/p1",
	}); err != nil {
		t.Fatalf("Upsert p1 (pairing): %v", err)
	}
	if err := store.Upsert(ctx, domain.Session{
		SessionID: "p2", State: domain.SessionStatePairing, StoreDir: "sessions/p2",
	}); err != nil {
		t.Fatalf("Upsert p2 (pairing): dos jid NULL deberían coexistir: %v", err)
	}
}

// TestZeroTimestampsRoundTrip: timestamps cero persisten como NULL/0 y vuelven como cero de Go.
func TestZeroTimestampsRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)
	if err := store.Upsert(ctx, domain.Session{
		SessionID: "z", JID: "z@x", State: domain.SessionStateActive, StoreDir: "sessions/z",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, err := store.Get(ctx, "z")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.PairedAt.IsZero() || !got.UpdatedAt.IsZero() {
		t.Fatalf("timestamps = %v / %v, esperaba cero", got.PairedAt, got.UpdatedAt)
	}
}

// TestDelete: borrar elimina la fila; borrar un session_id ausente es no-op (idempotente).
func TestDelete(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)
	if err := store.Upsert(ctx, domain.Session{
		SessionID: "d", JID: "d@x", State: domain.SessionStateActive, StoreDir: "sessions/d",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "d"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, "d"); !errors.Is(err, app.ErrSessionNotFound) {
		t.Fatalf("tras Delete, Get = %v, esperaba ErrSessionNotFound", err)
	}
	// Idempotente: borrar de nuevo no es error.
	if err := store.Delete(ctx, "d"); err != nil {
		t.Fatalf("Delete idempotente: %v", err)
	}
}

// TestStoreErrorsOnClosedDB: con la conexión cerrada, Upsert/List/Get propagan el error del driver.
func TestStoreErrorsOnClosedDB(t *testing.T) {
	ctx := context.Background()
	store, database := newStore(t)
	if err := store.Upsert(ctx, domain.Session{
		SessionID: "j", JID: "j@x", State: domain.SessionStateActive, StoreDir: "sessions/j",
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	if err := store.Upsert(ctx, domain.Session{
		SessionID: "k", JID: "k@x", State: domain.SessionStateActive, StoreDir: "sessions/k",
	}); err == nil {
		t.Fatal("Upsert sobre BD cerrada debía fallar")
	}
	if _, err := store.List(ctx); err == nil {
		t.Fatal("List sobre BD cerrada debía fallar")
	}
	if _, err := store.Get(ctx, "j"); err == nil || errors.Is(err, app.ErrSessionNotFound) {
		t.Fatalf("Get sobre BD cerrada debía fallar con error de driver, got %v", err)
	}
}

// --- Locator ---

// TestLocatorNoDevice: sin device pareado en el store cifrado, PairedJID devuelve ok=false.
func TestLocatorNoDevice(t *testing.T) {
	ctx := context.Background()
	_, database := newStore(t)
	loc := NewLocator(database)
	jid, ok, err := loc.PairedJID(ctx)
	if err != nil {
		t.Fatalf("PairedJID: %v", err)
	}
	if ok || jid != "" {
		t.Fatalf("esperaba ok=false/jid vacío sin device, got ok=%v jid=%q", ok, jid)
	}
}

// TestLocatorWithDevice: con una fila en msg_enc_device, PairedJID resuelve su JID.
func TestLocatorWithDevice(t *testing.T) {
	ctx := context.Background()
	_, database := newStore(t)
	if _, err := database.ExecContext(ctx,
		`INSERT INTO msg_enc_device (jid, registration_id, signed_pre_key_id, noise_priv,
		   identity_priv, signed_pre_key_priv, signed_pre_key_sig, adv_secret_key, adv_details,
		   adv_account_sig, adv_account_sig_key, adv_device_sig)
		 VALUES ('56984467443:47@s.whatsapp.net', 1, 1, x'00', x'00', x'00', x'00', x'00', x'00', x'00', x'00', x'00')`); err != nil {
		t.Fatalf("insert device: %v", err)
	}
	loc := NewLocator(database)
	jid, ok, err := loc.PairedJID(ctx)
	if err != nil {
		t.Fatalf("PairedJID: %v", err)
	}
	if !ok {
		t.Fatal("esperaba ok=true con un device pareado")
	}
	if jid != "56984467443:47@s.whatsapp.net" {
		t.Fatalf("jid = %q", jid)
	}
}

// TestSessionStoreOnCentralMetaDB: el sessionstore opera sobre la db CENTRAL de metadatos abierta con
// el set "meta" SOLO (db.OpenAndMigrateMeta), sin el esquema del store cifrado. Es la garantía T2(d):
// separar las migraciones (store por sesión vs metadatos central) deja el sessionstore verde sobre la
// db central que usará el Manager en T3/T4.
func TestSessionStoreOnCentralMetaDB(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sessions.db")
	database, err := db.OpenAndMigrateMeta(ctx, path)
	if err != nil {
		t.Fatalf("OpenAndMigrateMeta: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	store := New(database)
	in := domain.Session{
		SessionID: "22222222-2222-4222-8222-222222222222",
		JID:       "56984467443:47@s.whatsapp.net",
		State:     domain.SessionStateActive,
		StoreDir:  "sessions/22222222-2222-4222-8222-222222222222",
		PairedAt:  time.Unix(1_700_000_000, 0).UTC(),
		UpdatedAt: time.Unix(1_700_000_500, 0).UTC(),
	}
	if err := store.Upsert(ctx, in); err != nil {
		t.Fatalf("Upsert sobre la db central: %v", err)
	}
	got, err := store.Get(ctx, in.SessionID)
	if err != nil {
		t.Fatalf("Get sobre la db central: %v", err)
	}
	if got.SessionID != in.SessionID || got.JID != in.JID || got.State != in.State {
		t.Fatalf("Get = %+v, esperaba %+v", got, in)
	}
}

// --- Modelo cuenta↔dispositivo (Plan 022 T1) ---

// TestUpsertProvisionalAccountWhenNoSelfPN: un device sin self_pn (en 'pairing', el número aún no se
// conoce) cuelga de una cuenta PROVISIONAL con account_id = session_id y self_pn NULL.
func TestUpsertProvisionalAccountWhenNoSelfPN(t *testing.T) {
	ctx := context.Background()
	store, database := newStore(t)

	id := "11111111-1111-4111-8111-1111111111aa"
	if err := store.Upsert(ctx, domain.Session{SessionID: id, State: domain.SessionStatePairing}); err != nil {
		t.Fatalf("Upsert provisional: %v", err)
	}

	got, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.AccountID != id {
		t.Fatalf("cuenta provisional: account_id = %q, esperaba el session_id %q", got.AccountID, id)
	}
	if got.SelfPN != "" {
		t.Fatalf("cuenta provisional: self_pn = %q, esperaba vacío (NULL)", got.SelfPN)
	}
	if got.Role != domain.DeviceRolePrimary {
		t.Fatalf("rol por defecto = %q, esperaba primary", got.Role)
	}
	// La columna self_pn debe ser NULL (no cadena vacía) para no chocar con UNIQUE(self_pn).
	var selfPNNull bool
	if err := database.QueryRowContext(ctx,
		`SELECT self_pn IS NULL FROM accounts WHERE account_id = ?`, id).Scan(&selfPNNull); err != nil {
		t.Fatalf("consulta self_pn IS NULL: %v", err)
	}
	if !selfPNNull {
		t.Fatal("se esperaba self_pn NULL en la cuenta provisional")
	}
}

// TestSameSelfPNSharesAccount: dos DISPOSITIVOS del MISMO número (misma self_pn, distinto session_id/jid)
// cuelgan de la MISMA cuenta (un re-escaneo NO crea un silo nuevo). Es la asociación re-escaneo↔misma cuenta.
func TestSameSelfPNSharesAccount(t *testing.T) {
	ctx := context.Background()
	store, database := newStore(t)

	const selfPN = "56984467443"
	devA := domain.Session{
		SessionID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", JID: "jidA@s.whatsapp.net",
		SelfPN: selfPN, State: domain.SessionStateActive,
	}
	devB := domain.Session{
		SessionID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", JID: "jidB@s.whatsapp.net",
		SelfPN: selfPN, State: domain.SessionStateActive,
	}
	if err := store.Upsert(ctx, devA); err != nil {
		t.Fatalf("Upsert devA: %v", err)
	}
	if err := store.Upsert(ctx, devB); err != nil {
		t.Fatalf("Upsert devB: %v", err)
	}

	gotA, err := store.Get(ctx, devA.SessionID)
	if err != nil {
		t.Fatalf("Get devA: %v", err)
	}
	gotB, err := store.Get(ctx, devB.SessionID)
	if err != nil {
		t.Fatalf("Get devB: %v", err)
	}
	if gotA.AccountID == "" || gotA.AccountID != gotB.AccountID {
		t.Fatalf("misma self_pn debía dar el MISMO account_id: A=%q B=%q", gotA.AccountID, gotB.AccountID)
	}
	if gotA.SelfPN != selfPN || gotB.SelfPN != selfPN {
		t.Fatalf("self_pn no se propagó: A=%q B=%q", gotA.SelfPN, gotB.SelfPN)
	}

	// Exactamente UNA cuenta para esa self_pn (no un silo por escaneo).
	var accounts int
	if err := database.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM accounts WHERE self_pn = ?`, selfPN).Scan(&accounts); err != nil {
		t.Fatal(err)
	}
	if accounts != 1 {
		t.Fatalf("cuentas para self_pn = %d, esperaba 1", accounts)
	}

	// GetByAccount devuelve los DOS dispositivos de la cuenta.
	devs, err := store.GetByAccount(ctx, gotA.AccountID)
	if err != nil {
		t.Fatalf("GetByAccount: %v", err)
	}
	if len(devs) != 2 {
		t.Fatalf("GetByAccount devolvió %d dispositivos, esperaba 2", len(devs))
	}
}

// TestGetByAccountEmpty: una cuenta sin dispositivos (o inexistente) devuelve una lista vacía sin error.
func TestGetByAccountEmpty(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)
	devs, err := store.GetByAccount(ctx, "cuenta-inexistente")
	if err != nil {
		t.Fatalf("GetByAccount: %v", err)
	}
	if len(devs) != 0 {
		t.Fatalf("GetByAccount de cuenta vacía = %d, esperaba 0", len(devs))
	}
}

// TestDeleteByAccount: borra la cuenta y TODOS sus dispositivos de una vez (borrado por número); es
// idempotente (borrar una cuenta ausente no es error).
func TestDeleteByAccount(t *testing.T) {
	ctx := context.Background()
	store, database := newStore(t)

	const selfPN = "56911112222"
	a := domain.Session{SessionID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", JID: "c@x", SelfPN: selfPN, State: domain.SessionStateActive}
	b := domain.Session{SessionID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", JID: "d@x", SelfPN: selfPN, State: domain.SessionStateActive}
	if err := store.Upsert(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(ctx, b); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, a.SessionID)
	if err != nil {
		t.Fatalf("Get a: %v", err)
	}

	if err := store.DeleteByAccount(ctx, got.AccountID); err != nil {
		t.Fatalf("DeleteByAccount: %v", err)
	}
	// Ni dispositivos ni cuenta quedan.
	for _, id := range []string{a.SessionID, b.SessionID} {
		if _, err := store.Get(ctx, id); !errors.Is(err, app.ErrSessionNotFound) {
			t.Fatalf("tras DeleteByAccount, Get(%s) = %v, esperaba ErrSessionNotFound", id, err)
		}
	}
	var accounts int
	if err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM accounts WHERE account_id = ?`, got.AccountID).Scan(&accounts); err != nil {
		t.Fatal(err)
	}
	if accounts != 0 {
		t.Fatalf("cuentas tras DeleteByAccount = %d, esperaba 0", accounts)
	}
	// Idempotente: borrar de nuevo no es error.
	if err := store.DeleteByAccount(ctx, got.AccountID); err != nil {
		t.Fatalf("DeleteByAccount idempotente: %v", err)
	}
}

// TestReUpsertKeepsAccountStable: re-upsertar el MISMO device (promoción pairing→active con su self_pn)
// no duplica la cuenta ni el device y conserva el account_id.
func TestReUpsertKeepsAccountStable(t *testing.T) {
	ctx := context.Background()
	store, database := newStore(t)

	id := "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	const selfPN = "56933334444"
	if err := store.Upsert(ctx, domain.Session{SessionID: id, State: domain.SessionStatePairing}); err != nil {
		t.Fatalf("Upsert #1 (pairing, provisional): %v", err)
	}
	// Promoción: ahora se conoce el número (self_pn) y el JID.
	if err := store.Upsert(ctx, domain.Session{
		SessionID: id, JID: "e@s.whatsapp.net", SelfPN: selfPN, State: domain.SessionStateActive,
	}); err != nil {
		t.Fatalf("Upsert #2 (active con self_pn): %v", err)
	}

	got, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != domain.SessionStateActive || got.SelfPN != selfPN {
		t.Fatalf("tras promoción: state=%q self_pn=%q, esperaba active / %s", got.State, got.SelfPN, selfPN)
	}
	var devices int
	if err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM devices WHERE session_id = ?`, id).Scan(&devices); err != nil {
		t.Fatal(err)
	}
	if devices != 1 {
		t.Fatalf("filas de device = %d, esperaba 1 (upsert, no duplicado)", devices)
	}
}
