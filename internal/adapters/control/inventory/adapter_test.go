package inventory_test

import (
	"context"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/inventory"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

type fakeStore struct{}

func (f fakeStore) Upsert(context.Context, domain.Session) error { return nil }
func (f fakeStore) List(context.Context) ([]domain.Session, error) {
	return []domain.Session{{SessionID: "s1", State: domain.SessionStateActive}}, nil
}
func (f fakeStore) ListActive(context.Context) ([]domain.Session, error) { return nil, nil }
func (f fakeStore) Get(context.Context, string) (domain.Session, error)  { return domain.Session{}, nil }
func (f fakeStore) Delete(context.Context, string) error                { return nil }

func TestManagerAdapter(t *testing.T) {
	layout := sessionmgr.NewLayout(t.TempDir())
	store := fakeStore{}
	log := sharedlogger.Default()
	mgr := sessionmgr.NewManager(layout, store, 10, log)

	adapter := inventory.New(mgr)
	sessions, err := adapter.Persisted(context.Background())
	if err != nil {
		t.Fatalf("Persisted error: %v", err)
	}
	if len(sessions) != 1 || sessions[0].SessionID != "s1" {
		t.Fatalf("Persisted inesperado: %v", sessions)
	}

	_, ok := adapter.Health("s1")
	if ok {
		t.Fatal("esperaba ok=false para sesión no viva en RAM")
	}
}
