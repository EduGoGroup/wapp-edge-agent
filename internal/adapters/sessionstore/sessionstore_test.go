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
		`SELECT jid IS NULL FROM sessions_v2 WHERE session_id = ?`, in.SessionID).Scan(&jidNull); err != nil {
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
	if err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions_v2 WHERE session_id=?`, id).Scan(&n); err != nil {
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
