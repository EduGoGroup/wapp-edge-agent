package sessionmgr

// manager_account_mux_test.go ancla la AGREGACIÓN POR CUENTA en la frontera del multiplexor CloudLink
// (Plan 022 T6, decisión §10.J): "revocar por número = revocar sus N devices" se materializa en el Edge
// sacando CADA session_id de la cuenta del mux (Unregister). Los tests de manager_bd_unica_test.go ya
// prueban la purga en la BD (devices/cuenta/msg_enc_*/DEK) con m.cloudMux == nil; aquí INYECTAMOS un mux
// falso (recordMux) vía WithWhatsmeowListen para observar que Unlink/UnlinkAccount lo desanclan por device.
//
// El proto CloudLink (v0.6.0) lleva session_id + lease POR sesión (ADR-0008/0016 §5) y NO conoce la noción
// de cuenta: la agregación por número es responsabilidad del Edge (el mux es ciego a la cuenta). El fan-out
// de revocación por número DESDE LA NUBE es un follow-up del cloud (ver el TODO de UnlinkAccount).

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
)

// recordMux es un CloudLinkMux falso que solo REGISTRA las llamadas Register/Unregister por session_id
// (thread-safe). SinkFor/SendReceipt/SendLoggedOut son no-op: este test no arranca listeners (no llama a
// Restore/Pair), solo ejercita el borrado, que es lo que toca el mux. Satisface la interfaz completa.
type recordMux struct {
	mu           sync.Mutex
	registered   []string
	unregistered []string
}

func (r *recordMux) Register(sessionID, _ string,
	_ func(ctx context.Context, commandID, to, text string) error,
	_ func(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) error,
	_ func() bool,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registered = append(r.registered, sessionID)
}

func (r *recordMux) Unregister(sessionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unregistered = append(r.unregistered, sessionID)
}

func (r *recordMux) SinkFor(string) app.InboundSink          { return nil }
func (r *recordMux) SendReceipt(string, domain.ReceiptEvent) {}
func (r *recordMux) SendLoggedOut(string)                    {}

// unregisteredSet devuelve el conjunto ORDENADO de session_ids desanclados (para aserciones deterministas
// pese al orden de iteración de la cuenta).
func (r *recordMux) unregisteredSet() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]string(nil), r.unregistered...)
	sort.Strings(out)
	return out
}

// newBDUnicaManagerWithMux es como newBDUnicaManager pero cablea un CloudLinkMux (recordMux) vía
// WithWhatsmeowListen para observar la frontera del multiplex. No arranca listeners (el factory de escucha
// solo se invoca en Restore/Pair, que este test no llama): m.cloudMux queda cableado y m.newListener ocioso.
func newBDUnicaManagerWithMux(t *testing.T, mux CloudLinkMux) (*Manager, *sessionstore.Store, *sql.DB) {
	t.Helper()
	base := filepath.Join(t.TempDir(), "edge-data")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatalf("crear data_dir: %v", err)
	}
	database, err := wappdb.OpenAndMigrate(context.Background(), filepath.Join(base, "edge.db"))
	if err != nil {
		t.Fatalf("abrir/migrar la BD única: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store := sessionstore.New(database)
	m := NewManager(NewLayout(base), store, 5, testLogger(),
		WithSharedDB(database, wappdb.DialectSQLite),
		WithWhatsmeowListen(mux, ""))
	return m, store, database
}

// TestManager_UnlinkAccount_RevokesAllDevicesOnMux (DoD T6 · §10.J): dos devices del MISMO número cuelgan de
// la MISMA cuenta; UnlinkAccount desancla del multiplex CloudLink AMBOS session_ids (agregación por cuenta =
// revocar el número revoca sus N devices en el único stream, ADR-0008). Sin él, el kill-switch por número
// dejaría sesiones colgando del mux.
func TestManager_UnlinkAccount_RevokesAllDevicesOnMux(t *testing.T) {
	ctx := context.Background()
	mux := &recordMux{}
	m, _, database := newBDUnicaManagerWithMux(t, mux)

	const selfPN = "56911112222"
	jidA := canonJID(t, "56911112222:1@s.whatsapp.net")
	jidB := canonJID(t, "56911112222:2@s.whatsapp.net")
	accA := seedActiveDevice(t, m, database, uuidA, selfPN, jidA)
	accB := seedActiveDevice(t, m, database, uuidB, selfPN, jidB)
	if accA != accB {
		t.Fatalf("el mismo número debería dar la MISMA cuenta: A=%q B=%q", accA, accB)
	}

	if err := m.UnlinkAccount(ctx, accA); err != nil {
		t.Fatalf("UnlinkAccount: %v", err)
	}

	// Agregación por cuenta: AMBOS session_ids del número salieron del multiplex (orden de iteración libre).
	got := mux.unregisteredSet()
	want := []string{uuidA, uuidB}
	sort.Strings(want)
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Unregister por cuenta: got %v want %v (revocar el número debe desanclar sus N devices)", got, want)
	}
}

// TestManager_Unlink_RevokesSingleDeviceOnMux (DoD T6 · §10.J): borrar UN device desancla del multiplex SOLO
// su session_id (contraparte por-device de la agregación por cuenta; el otro device del número sigue anclado).
func TestManager_Unlink_RevokesSingleDeviceOnMux(t *testing.T) {
	ctx := context.Background()
	mux := &recordMux{}
	m, _, database := newBDUnicaManagerWithMux(t, mux)

	const selfPN = "56933334444"
	jidA := canonJID(t, "56933334444:1@s.whatsapp.net")
	jidB := canonJID(t, "56933334444:2@s.whatsapp.net")
	seedActiveDevice(t, m, database, uuidA, selfPN, jidA)
	seedActiveDevice(t, m, database, uuidB, selfPN, jidB)

	if err := m.Unlink(ctx, uuidA); err != nil {
		t.Fatalf("Unlink(A): %v", err)
	}

	got := mux.unregisteredSet()
	if len(got) != 1 || got[0] != uuidA {
		t.Fatalf("Unregister por device: got %v want [%s] (solo el device borrado sale del mux)", got, uuidA)
	}
}
