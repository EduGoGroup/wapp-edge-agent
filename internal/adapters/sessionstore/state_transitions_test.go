package sessionstore

import (
	"context"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// TestStateTransitions recorre la máquina de estados del ciclo de vida del device (Plan 022 T4):
// pairing → active → suspended → loggedout. Cada transición es un Upsert sobre el MISMO session_id
// (ON CONFLICT) y se verifica que `devices.state` persiste el valor de primera clase y se relee intacto.
// También se comprueba que el mapeo NO rompe el fleet: solo 'active' aparece en ListActive (lo que el
// arranque restaura); pairing/suspended/loggedout quedan fuera (no se restauran ⇒ el Cloud los deriva
// offline/loggedout por ausencia de latidos, ver domain.SessionState).
func TestStateTransitions(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	const sid = "22222222-2222-4222-8222-222222222222"
	base := domain.Session{
		SessionID: sid,
		JID:       "56999888777:12@s.whatsapp.net",
		SelfPN:    "56999888777",
		PairedAt:  time.Unix(1_700_000_000, 0).UTC(),
	}

	// Secuencia lineal de la máquina de estados; cada paso persiste y se relee.
	steps := []struct {
		state       domain.SessionState
		inActiveSet bool // ¿debe aparecer en ListActive (lo que restaura el arranque)?
	}{
		{domain.SessionStatePairing, false},
		{domain.SessionStateActive, true},
		{domain.SessionStateSuspended, false},
		{domain.SessionStateLoggedOut, false},
	}

	for i, step := range steps {
		s := base
		s.State = step.state
		s.UpdatedAt = time.Unix(1_700_000_100+int64(i), 0).UTC()
		if err := store.Upsert(ctx, s); err != nil {
			t.Fatalf("Upsert(%s): %v", step.state, err)
		}

		got, err := store.Get(ctx, sid)
		if err != nil {
			t.Fatalf("Get tras %s: %v", step.state, err)
		}
		if got.State != step.state {
			t.Fatalf("state persistido = %q, esperaba %q", got.State, step.state)
		}

		active, err := store.ListActive(ctx)
		if err != nil {
			t.Fatalf("ListActive tras %s: %v", step.state, err)
		}
		inSet := false
		for _, a := range active {
			if a.SessionID == sid {
				inSet = true
			}
		}
		if inSet != step.inActiveSet {
			t.Fatalf("estado %q en ListActive = %v, esperaba %v (el mapeo no debe romper el fleet)",
				step.state, inSet, step.inActiveSet)
		}
	}
}
