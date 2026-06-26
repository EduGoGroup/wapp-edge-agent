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

// newStore abre un store SQLite temporal YA migrado y devuelve el Store de sesiones.
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

// TestUpsertGetRoundTrip: insertar y leer una sesión preserva jid/estado/timestamps (a segundo).
func TestUpsertGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	paired := time.Unix(1_700_000_000, 0).UTC()
	updated := time.Unix(1_700_000_500, 0).UTC()
	in := domain.Session{
		JID:       "56984467443:47@s.whatsapp.net",
		State:     domain.SessionStateActive,
		PairedAt:  paired,
		UpdatedAt: updated,
	}
	if err := store.Upsert(ctx, in); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, err := store.Get(ctx, in.JID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.JID != in.JID || got.State != in.State {
		t.Fatalf("Get = %+v, esperaba jid/state %q/%q", got, in.JID, in.State)
	}
	if !got.PairedAt.Equal(paired) || !got.UpdatedAt.Equal(updated) {
		t.Fatalf("timestamps = paired %v / updated %v, esperaba %v / %v",
			got.PairedAt, got.UpdatedAt, paired, updated)
	}
}

// TestUpsertUpdatesState: re-upsertar el mismo jid actualiza estado y updated_at sin duplicar filas,
// y preserva paired_at NO se exige (excluded.updated_at sí cambia).
func TestUpsertUpdatesState(t *testing.T) {
	ctx := context.Background()
	store, database := newStore(t)

	jid := "j@s.whatsapp.net"
	t0 := time.Unix(1_700_000_000, 0).UTC()
	if err := store.Upsert(ctx, domain.Session{JID: jid, State: domain.SessionStateActive, PairedAt: t0, UpdatedAt: t0}); err != nil {
		t.Fatalf("Upsert #1: %v", err)
	}
	t1 := time.Unix(1_700_009_999, 0).UTC()
	if err := store.Upsert(ctx, domain.Session{JID: jid, State: domain.SessionStateLoggedOut, PairedAt: t0, UpdatedAt: t1}); err != nil {
		t.Fatalf("Upsert #2: %v", err)
	}

	// Una sola fila.
	var n int
	if err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions WHERE jid=?`, jid).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("filas para jid = %d, esperaba 1 (upsert, no insert duplicado)", n)
	}

	got, err := store.Get(ctx, jid)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != domain.SessionStateLoggedOut {
		t.Fatalf("State = %q, esperaba loggedout tras update", got.State)
	}
	if !got.UpdatedAt.Equal(t1) {
		t.Fatalf("UpdatedAt = %v, esperaba %v", got.UpdatedAt, t1)
	}
}

// TestList: List devuelve todas las sesiones ordenadas por paired_at.
func TestList(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	// Vacío al inicio.
	got, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List vacío: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("List inicial = %d, esperaba 0", len(got))
	}

	older := time.Unix(1_700_000_000, 0).UTC()
	newer := time.Unix(1_700_009_999, 0).UTC()
	if err := store.Upsert(ctx, domain.Session{JID: "b@x", State: domain.SessionStateActive, PairedAt: newer, UpdatedAt: newer}); err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(ctx, domain.Session{JID: "a@x", State: domain.SessionStateActive, PairedAt: older, UpdatedAt: older}); err != nil {
		t.Fatal(err)
	}

	got, err = store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List = %d, esperaba 2", len(got))
	}
	// Orden por paired_at ascendente: a@x (older) primero.
	if got[0].JID != "a@x" || got[1].JID != "b@x" {
		t.Fatalf("orden inesperado: %q, %q", got[0].JID, got[1].JID)
	}
}

// TestGetNotFound: Get de un jid inexistente devuelve app.ErrSessionNotFound.
func TestGetNotFound(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)
	if _, err := store.Get(ctx, "noexiste@x"); !errors.Is(err, app.ErrSessionNotFound) {
		t.Fatalf("error = %v, esperaba app.ErrSessionNotFound", err)
	}
}

// TestUpsertEmptyJID: un JID vacío es inválido (no se persiste material sin clave).
func TestUpsertEmptyJID(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)
	if err := store.Upsert(ctx, domain.Session{State: domain.SessionStateActive}); err == nil {
		t.Fatal("se esperaba error con JID vacío")
	}
}

// TestZeroTimestampsRoundTrip: un Session con timestamps cero persiste 0 y vuelve como cero de Go
// (no como un instante negativo).
func TestZeroTimestampsRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)
	if err := store.Upsert(ctx, domain.Session{JID: "z@x", State: domain.SessionStateActive}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, err := store.Get(ctx, "z@x")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.PairedAt.IsZero() || !got.UpdatedAt.IsZero() {
		t.Fatalf("timestamps = %v / %v, esperaba cero", got.PairedAt, got.UpdatedAt)
	}
}

// TestStoreErrorsOnClosedDB: con la conexión cerrada, Upsert/List/Get propagan el error del driver
// (cubre las ramas de error de SQL sin red).
func TestStoreErrorsOnClosedDB(t *testing.T) {
	ctx := context.Background()
	store, database := newStore(t)
	// Siembra una fila para que el SELECT de List/Get tenga algo que (intentar) leer.
	if err := store.Upsert(ctx, domain.Session{JID: "j@x", State: domain.SessionStateActive}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	if err := store.Upsert(ctx, domain.Session{JID: "k@x", State: domain.SessionStateActive}); err == nil {
		t.Fatal("Upsert sobre BD cerrada debía fallar")
	}
	if _, err := store.List(ctx); err == nil {
		t.Fatal("List sobre BD cerrada debía fallar")
	}
	if _, err := store.Get(ctx, "j@x"); err == nil || errors.Is(err, app.ErrSessionNotFound) {
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
