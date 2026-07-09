package cloudlink

import (
	"testing"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// TestHeartbeatStateFor fija el mapeo Edge→Cloud del estado (Plan 022 T4, design §4/§10.D): el estado de
// NEGOCIO del device (domain.SessionState) → estado de LÍNEA del Heartbeat (cloudlinkv1.SessionState) que
// el Cloud usa para derivar fleet_sessions.state. Verifica que solo 'loggedout' viaja como LOGGED_OUT
// (zombie) y el resto como UNSPECIFIED (liveness) — de modo que el mapeo NO rompe el fleet: 'active' se
// deriva online, y pairing/suspended (que no emiten latido) jamás se reportan como LOGGED_OUT por error.
func TestHeartbeatStateFor(t *testing.T) {
	tests := []struct {
		state domain.SessionState
		want  cloudlinkv1.SessionState
	}{
		{domain.SessionStateActive, cloudlinkv1.SessionState_SESSION_STATE_UNSPECIFIED},
		{domain.SessionStateLoggedOut, cloudlinkv1.SessionState_SESSION_STATE_LOGGED_OUT},
		{domain.SessionStatePairing, cloudlinkv1.SessionState_SESSION_STATE_UNSPECIFIED},
		{domain.SessionStateSuspended, cloudlinkv1.SessionState_SESSION_STATE_UNSPECIFIED},
	}
	for _, tc := range tests {
		if got := heartbeatStateFor(tc.state); got != tc.want {
			t.Errorf("heartbeatStateFor(%q) = %v, esperaba %v", tc.state, got, tc.want)
		}
	}
}
