package sessionmgr

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
)

// fakePairer simula app.Pair SIN WhatsApp: como app.Pair al PairSuccess, si sealDEK sella una DEK en
// la custodia de la sesión (para que dek.key exista) y devuelve el JID; o devuelve err (fallo/timeout).
type fakePairer struct {
	custody app.KeyCustody
	jid     string
	dek     []byte
	sealDEK bool
	err     error
}

func (f *fakePairer) Run(_ context.Context) (app.PairResult, error) {
	if f.err != nil {
		return app.PairResult{}, f.err
	}
	if f.sealDEK {
		if err := f.custody.Store(f.dek); err != nil {
			return app.PairResult{}, err
		}
	}
	return app.PairResult{WaJID: f.jid}, nil
}

// newPairTestManager arma un Manager con un sessionstore REAL (sessions.db en un tempdir) y el factory
// de pairing dado (un fake). Devuelve también el store y el Layout para inspeccionar el resultado.
func newPairTestManager(t *testing.T, factory pairFactory) (*Manager, *sessionstore.Store, Layout) {
	t.Helper()
	base := filepath.Join(t.TempDir(), "edge-data")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatalf("crear data_dir: %v", err)
	}
	metaDB, err := wappdb.OpenAndMigrateMeta(context.Background(), filepath.Join(base, "sessions.db"))
	if err != nil {
		t.Fatalf("abrir/migrar sessions.db: %v", err)
	}
	t.Cleanup(func() { _ = metaDB.Close() })

	layout := NewLayout(base)
	m := NewManager(layout, sessionstore.New(metaDB), 5, testLogger())
	m.newPairer = factory
	return m, sessionstore.New(metaDB), layout
}

// sealingFactory devuelve un factory que, en cada llamada, sella una DEK distinta y un JID distinto
// (jid-1, jid-2, …) — emula app.Pair feliz y permite demostrar el anti-pisado entre sesiones.
func sealingFactory() pairFactory {
	calls := 0
	return func(custody app.KeyCustody, _ *sql.DB) pairRunner {
		calls++
		return &fakePairer{
			custody: custody,
			jid:     fmt.Sprintf("jid-%d@s.whatsapp.net", calls),
			dek:     bytes.Repeat([]byte{byte(calls)}, 32),
			sealDEK: true,
		}
	}
}

// fileExists indica si path existe (helper de aserción).
func fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	t.Fatalf("stat %q: %v", path, err)
	return false
}

// TestManager_Pair_Happy: un pairing exitoso registra la sesión (pairing→active) y materializa
// dir + store.db + dek.key, con el JID en la fila (design §5 / DoD T3).
func TestManager_Pair_Happy(t *testing.T) {
	ctx := context.Background()
	m, sessions, layout := newPairTestManager(t, sealingFactory())

	res, err := m.Pair(ctx)
	if err != nil {
		t.Fatalf("Pair() error: %v", err)
	}
	if res.WaJID != "jid-1@s.whatsapp.net" {
		t.Fatalf("WaJID inesperado: %q", res.WaJID)
	}

	rows, err := sessions.List(ctx)
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("se esperaba 1 sesión registrada, hay %d", len(rows))
	}
	row := rows[0]
	if row.State != domain.SessionStateActive {
		t.Fatalf("estado esperado active, got %q", row.State)
	}
	if row.JID != res.WaJID {
		t.Fatalf("jid de la fila %q != resultado %q", row.JID, res.WaJID)
	}
	if want := filepath.Join("sessions", row.SessionID); row.StoreDir != want {
		t.Fatalf("store_dir esperado %q, got %q", want, row.StoreDir)
	}
	if row.PairedAt.IsZero() {
		t.Fatalf("paired_at no debería ser cero tras PairSuccess")
	}

	dir, _ := layout.SessionDir(row.SessionID)
	store, _ := layout.StoreDB(row.SessionID)
	dek, _ := layout.DEKPath(row.SessionID)
	if !fileExists(t, dir) || !fileExists(t, store) || !fileExists(t, dek) {
		t.Fatalf("faltan artefactos: dir=%v store=%v dek=%v",
			fileExists(t, dir), fileExists(t, store), fileExists(t, dek))
	}

	if got := m.List(); len(got) != 1 {
		t.Fatalf("el registro vivo debería tener 1 sesión, got %d", len(got))
	}
}

// TestManager_Pair_AntiClobber: dos Pair seguidos crean DOS sesiones independientes (dos session_id,
// dos dir, dos DEK, dos filas) y el segundo NO toca al primero (causa raíz de MP-01 eliminada).
func TestManager_Pair_AntiClobber(t *testing.T) {
	ctx := context.Background()
	m, sessions, layout := newPairTestManager(t, sealingFactory())

	res1, err := m.Pair(ctx)
	if err != nil {
		t.Fatalf("Pair() #1 error: %v", err)
	}
	// Estado de la 1ª sesión ANTES del 2º pairing.
	rows1, _ := sessions.List(ctx)
	if len(rows1) != 1 {
		t.Fatalf("tras el 1er pair debería haber 1 fila, hay %d", len(rows1))
	}
	id1 := rows1[0].SessionID
	cust1, _ := m.custodyFor(id1)
	dek1, err := cust1.Load()
	if err != nil {
		t.Fatalf("Load(DEK #1): %v", err)
	}

	res2, err := m.Pair(ctx)
	if err != nil {
		t.Fatalf("Pair() #2 error: %v", err)
	}

	// Dos sesiones independientes en el registro persistido.
	rows, _ := sessions.List(ctx)
	if len(rows) != 2 {
		t.Fatalf("se esperaban 2 sesiones, hay %d", len(rows))
	}
	if res1.WaJID == res2.WaJID {
		t.Fatalf("los JID no deberían coincidir: %q", res1.WaJID)
	}

	var id2 string
	for _, r := range rows {
		if r.SessionID != id1 {
			id2 = r.SessionID
		}
	}
	if id2 == "" || id1 == id2 {
		t.Fatalf("los session_id deberían diferir: id1=%q id2=%q", id1, id2)
	}

	// La 1ª sesión quedó INTACTA: su DEK no cambió y sus artefactos siguen ahí.
	dek1After, err := cust1.Load()
	if err != nil {
		t.Fatalf("Load(DEK #1) tras 2º pair: %v", err)
	}
	if !bytes.Equal(dek1, dek1After) {
		t.Fatalf("la DEK de la 1ª sesión cambió tras el 2º pair (pisado)")
	}
	dir1, _ := layout.SessionDir(id1)
	dir2, _ := layout.SessionDir(id2)
	if !fileExists(t, dir1) || !fileExists(t, dir2) {
		t.Fatalf("deberían existir ambos directorios de sesión")
	}
	dek2Path, _ := layout.DEKPath(id2)
	if !fileExists(t, dek2Path) {
		t.Fatalf("la 2ª sesión debería tener su propia dek.key")
	}

	if got := m.List(); len(got) != 2 {
		t.Fatalf("el registro vivo debería tener 2 sesiones, got %d", len(got))
	}
}

// TestManager_Pair_FailureCleansUp: si el pairing falla, no quedan restos (ni fila, ni dir, ni DEK).
func TestManager_Pair_FailureCleansUp(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("escaneo cancelado por el usuario")
	m, sessions, layout := newPairTestManager(t, func(custody app.KeyCustody, _ *sql.DB) pairRunner {
		return &fakePairer{custody: custody, err: sentinel}
	})

	_, err := m.Pair(ctx)
	if err == nil {
		t.Fatalf("Pair() debería fallar cuando el pairing falla")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("el error debería envolver la causa raíz, got %v", err)
	}

	// Sin fila persistida.
	rows, _ := sessions.List(ctx)
	if len(rows) != 0 {
		t.Fatalf("no debería quedar ninguna fila, hay %d", len(rows))
	}
	// Sin directorios de sesión bajo sessions/.
	entries, err := os.ReadDir(layout.SessionsRoot())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("leer sessions/: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("no debería quedar ningún directorio de sesión, hay %d", len(entries))
	}
	// Sin sesión viva.
	if got := m.List(); len(got) != 0 {
		t.Fatalf("el registro vivo debería estar vacío, got %d", len(got))
	}
}

// TestManager_Pair_DEKInvariant: el resultado expuesto por Pair (lo que cruzaría a /v1/pair) lleva
// SOLO el JID; la DEK NUNCA es un campo del resultado (queda sellada en la custodia del núcleo, ADR-0007).
func TestManager_Pair_DEKInvariant(t *testing.T) {
	ctx := context.Background()
	m, sessions, _ := newPairTestManager(t, sealingFactory())

	res, err := m.Pair(ctx)
	if err != nil {
		t.Fatalf("Pair() error: %v", err)
	}

	// El tipo del resultado no debe exponer material de llave: ningún campo []byte ni nombrado DEK/Key.
	rt := reflect.TypeOf(res)
	byteSlice := reflect.TypeOf([]byte(nil))
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Type == byteSlice {
			t.Fatalf("PairResult expone un campo []byte (%s): la DEK podría fugarse", f.Name)
		}
		switch f.Name {
		case "DEK", "Dek", "Key", "Secret":
			t.Fatalf("PairResult expone un campo sensible (%s)", f.Name)
		}
	}

	// La DEK SÍ vive en el núcleo: recuperable solo desde la custodia de la sesión, no desde el resultado.
	rows, _ := sessions.List(ctx)
	if len(rows) != 1 {
		t.Fatalf("se esperaba 1 sesión, hay %d", len(rows))
	}
	cust, _ := m.custodyFor(rows[0].SessionID)
	dek, err := cust.Load()
	if err != nil {
		t.Fatalf("la DEK debería estar sellada en la custodia: %v", err)
	}
	if len(dek) != 32 {
		t.Fatalf("la DEK custodiada debería medir 32 bytes, got %d", len(dek))
	}
}
