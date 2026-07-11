package edgeconfig

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

func testLogger() sharedlogger.Logger {
	return sharedlogger.New(sharedlogger.WithWriter(discardWriter{}), sharedlogger.WithJSON(true))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// newSQLStore abre una BD única SQLite migrada (la tabla edge_config la crea la migración 0006).
func newSQLStore(t *testing.T) *SQLStore {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "edge.db")
	database, err := db.Open(ctx, db.DialectSQLite, path)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := db.Migrate(ctx, database); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}
	return NewSQLStore(database)
}

func TestSQLStore_GetMiss_NoError(t *testing.T) {
	store := newSQLStore(t)
	_, found, err := store.Get(context.Background(), "intents")
	if err != nil {
		t.Fatalf("Get miss error: %v", err)
	}
	if found {
		t.Errorf("found=true en tabla vacía")
	}
}

func TestSQLStore_PutGet_Upsert(t *testing.T) {
	ctx := context.Background()
	store := newSQLStore(t)

	if err := store.Put(ctx, Record{Kind: "intents", Version: "v1", Payload: []byte("uno"), UpdatedUnix: 100}); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	rec, found, err := store.Get(ctx, "intents")
	if err != nil || !found {
		t.Fatalf("Get v1: found=%v err=%v", found, err)
	}
	if rec.Version != "v1" || string(rec.Payload) != "uno" || rec.UpdatedUnix != 100 {
		t.Errorf("Get v1 devolvió %+v", rec)
	}

	// Upsert: reemplaza por versión nueva (misma PK kind).
	if err := store.Put(ctx, Record{Kind: "intents", Version: "v2", Payload: []byte("dos"), UpdatedUnix: 200}); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	rec, _, _ = store.Get(ctx, "intents")
	if rec.Version != "v2" || string(rec.Payload) != "dos" {
		t.Errorf("upsert no reemplazó: %+v", rec)
	}
}

// fakeStore es un Store en memoria para los tests del Service (idempotencia/validación/notificación).
type fakeStore struct {
	recs   map[string]Record
	getErr error
	putErr error
	putCnt int
}

func newFakeStore() *fakeStore { return &fakeStore{recs: map[string]Record{}} }

func (f *fakeStore) Get(_ context.Context, kind string) (Record, bool, error) {
	if f.getErr != nil {
		return Record{}, false, f.getErr
	}
	rec, ok := f.recs[kind]
	return rec, ok, nil
}

func (f *fakeStore) Put(_ context.Context, rec Record) error {
	if f.putErr != nil {
		return f.putErr
	}
	f.putCnt++
	f.recs[rec.Kind] = rec
	return nil
}

func TestService_Apply_KindDesconocido_NoPersisteNiNotifica(t *testing.T) {
	fs := newFakeStore()
	svc := NewService(fs, testLogger())
	// sin RegisterKind: 'intents' es desconocido
	if err := svc.Apply(context.Background(), "intents", "v1", []byte("x")); err != nil {
		t.Fatalf("Apply kind desconocido devolvió error: %v", err)
	}
	if fs.putCnt != 0 {
		t.Errorf("kind desconocido no debe persistir (putCnt=%d)", fs.putCnt)
	}
}

func TestService_Apply_VersionDuplicada_Idempotente(t *testing.T) {
	fs := newFakeStore()
	fs.recs["intents"] = Record{Kind: "intents", Version: "v1", Payload: []byte("prev")}
	svc := NewService(fs, testLogger())

	notified := 0
	svc.RegisterKind("intents", nil, func(Record) { notified++ })

	if err := svc.Apply(context.Background(), "intents", "v1", []byte("nuevo")); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if fs.putCnt != 0 {
		t.Errorf("versión ya aplicada no debe re-persistir")
	}
	if notified != 0 {
		t.Errorf("versión ya aplicada no debe notificar")
	}
}

func TestService_Apply_Invalida_ConservaAnterior_Notifica0(t *testing.T) {
	fs := newFakeStore()
	svc := NewService(fs, testLogger())

	notified := 0
	svc.RegisterKind("intents",
		func([]byte) error { return errors.New("blob inválido") },
		func(Record) { notified++ },
	)

	if err := svc.Apply(context.Background(), "intents", "v2", []byte("basura")); err != nil {
		t.Fatalf("Apply inválida no debe devolver error (no reintentable): %v", err)
	}
	if fs.putCnt != 0 {
		t.Errorf("config inválida no debe persistir (last-known-good)")
	}
	if notified != 0 {
		t.Errorf("config inválida no debe notificar")
	}
}

func TestService_Apply_ValidaNueva_PersisteYNotifica(t *testing.T) {
	fs := newFakeStore()
	svc := NewService(fs, testLogger())

	var got Record
	notified := 0
	svc.RegisterKind("intents", func([]byte) error { return nil }, func(rec Record) {
		got = rec
		notified++
	})

	if err := svc.Apply(context.Background(), "intents", "v3", []byte("ok")); err != nil {
		t.Fatalf("Apply válida: %v", err)
	}
	if fs.putCnt != 1 {
		t.Errorf("config válida debe persistir una vez (putCnt=%d)", fs.putCnt)
	}
	if notified != 1 || got.Version != "v3" || string(got.Payload) != "ok" {
		t.Errorf("suscriptor mal notificado: n=%d rec=%+v", notified, got)
	}
}

func TestService_Apply_FalloPersistencia_DevuelveError(t *testing.T) {
	fs := newFakeStore()
	fs.putErr = errors.New("disco lleno")
	svc := NewService(fs, testLogger())
	svc.RegisterKind("intents", func([]byte) error { return nil })

	if err := svc.Apply(context.Background(), "intents", "v4", []byte("ok")); err == nil {
		t.Fatalf("un fallo de persistencia debe devolver error (reintentable)")
	}
}

func TestService_Bootstrap_RecargaPersistida(t *testing.T) {
	fs := newFakeStore()
	fs.recs["intents"] = Record{Kind: "intents", Version: "v-boot", Payload: []byte("persistida")}
	svc := NewService(fs, testLogger())

	var got Record
	svc.RegisterKind("intents", nil, func(rec Record) { got = rec })
	svc.Bootstrap(context.Background())

	if got.Version != "v-boot" {
		t.Errorf("Bootstrap no notificó la config persistida: %+v", got)
	}
}
