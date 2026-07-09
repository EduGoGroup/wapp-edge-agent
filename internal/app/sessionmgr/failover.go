package sessionmgr

// failover.go aporta el FAILOVER MULTI-DISPOSITIVO POR NÚMERO del Manager (Plan 022 T5, design §6/§10.F/
// §10.G): el cupo de devices vivos por cuenta, la asignación de rol (primary/standby), la promoción de un
// standby cuando el primary cae/expira, y la reacción al cierre de sesión por WhatsApp (events.LoggedOut).
//
// CAVEAT INNEGOCIABLE (requisito del plan §10.F): el multi-dispositivo por número es **RESILIENCIA, NO
// SIGILO**. Tener 1 primary + N standbys da tolerancia a fallos (si a un companion lo cierran, otro sigue
// operando el número), NO evasión: a nivel de protocolo TODOS los companions reciben el fan-out y cada uno
// añade huella; más dispositivos NO reducen el riesgo de baneo, lo AUMENTAN. Por eso va OFF por defecto
// (multiDevicePerAccount = 1) y jamás se debe incentivar agotar los 4 slots de WhatsApp.
//
// ZERO-KNOWLEDGE (ADR-0007): rol y estado son metadatos de NEGOCIO EN CLARO; este archivo JAMÁS toca la
// DEK ni la loguea. La promoción/loggedout solo mueven columnas `devices.role`/`devices.state`.

import (
	"context"
	"errors"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// ErrAccountAtCapacity: un Pair excedería el cupo de dispositivos VIVOS de la cuenta (número)
// (multiDevicePerAccount, design §10.F). Con el default 1, es "ya hay un device vivo para este número".
// El plano de control lo mapea a un error claro (no crash): es un guardarraíl, no un invariante de seguridad.
var ErrAccountAtCapacity = errors.New("sessionmgr: la cuenta (número) alcanzó el cupo de dispositivos vivos")

// roleStore es el subconjunto del sessionstore concreto para PERSISTIR el rol de un device (promoción de
// standby a primary). El puerto app.SessionStore no lo expone; el runtime T5 lo usa vía type-assert y cae a
// un no-op logueado con los fakes en memoria (que no lo implementan), sin romper los tests de otras capas.
type roleStore interface {
	SetRole(ctx context.Context, sessionID string, role domain.DeviceRole) error
}

// assignRoleLocked decide el ROL del device que se está pareando para el número selfPN y VERIFICA el cupo,
// bajo m.mu (el llamante DEBE tener el lock: se ejecuta atómico con el registro en m.live para no dar dos
// primarys ni pasarse del cupo ante pairs concurrentes del mismo número). Reglas (design §6/§10.F):
//   - selfPN vacío (JID aún sin número): primary, sin agrupar (cuenta provisional por dispositivo).
//   - cuenta cuyo cupo de devices VIVOS ya está lleno ⇒ ErrAccountAtCapacity (rechaza el pairing).
//   - si el número ya tiene un primary vivo ⇒ el nuevo entra como standby; si no ⇒ primary.
//
// Cuenta SOLO los devices vivos OPERATIVOS del mismo número: los 'loggedout' (zombies aún en el registro a
// la espera del apagado/unlink) NO ocupan cupo — así un RE-ESCANEO tras un LoggedOut vuelve a caber (§10.G).
func (m *Manager) assignRoleLocked(selfPN, excludeID string) (domain.DeviceRole, error) {
	if selfPN == "" {
		return domain.DeviceRolePrimary, nil
	}
	live, primaryPresent := 0, false
	for id, s := range m.live {
		if id == excludeID || s.meta.SelfPN != selfPN {
			continue
		}
		if s.meta.State == domain.SessionStateLoggedOut {
			continue // zombie: no ocupa cupo (deja sitio al re-escaneo).
		}
		live++
		if s.meta.Role != domain.DeviceRoleStandby {
			primaryPresent = true // primary (o rol vacío = primary por defecto).
		}
	}
	if live >= m.multiDevicePerAccount {
		return "", ErrAccountAtCapacity
	}
	if primaryPresent {
		return domain.DeviceRoleStandby, nil
	}
	return domain.DeviceRolePrimary, nil
}

// onLoggedOut reacciona al cierre de sesión por WhatsApp (events.LoggedOut) del device vivo s (Plan 022 T5,
// REUSA Plan 020 T3). Es lo que T4 dejó a propósito pendiente: hoy el cierre solo se propagaba al Cloud;
// aquí, además, se PERSISTE el estado local. Secuencia:
//  1. Propaga ZOMBIE al Cloud (mux.SendLoggedOut) mientras el stream aún vive, ANTES de sacarla del mux:
//     así el Cloud DISTINGUE zombie (LoggedOut explícito) de un simple offline por caída de red.
//  2. Persiste devices.state='loggedout' LOCALMENTE conservando la cuenta por self_pn (la fila NO se borra:
//     un RE-ESCANEO del mismo número cuelga de la MISMA cuenta — sessionstore.resolveAccount por self_pn).
//  3. NO RENUEVA LEASE: saca la sesión del multiplex (Unregister) para que cesen los heartbeats (el Cloud
//     ya la vio zombie); el cliente whatsmeow muerto queda a la espera del apagado ordenado (Stop) o unlink.
//  4. FAILOVER: si el device caído era el primary del número, promueve el standby más reciente a primary.
//
// Corre DENTRO de la goroutine del listener (hook de whatsmeow) → por eso NO cancela NI espera su propia
// goroutine (sería un deadlock): el socket muerto lo recoge Stop() (apagado ordenado §10.I) o el unlink. La
// fila 'loggedout' NO se restaura al reiniciar (Restore solo levanta 'active'), fiel a "no re-emparejar
// automáticamente" (RF-6). Zero-knowledge intacto: solo toca metadatos EN CLARO (state/role).
func (m *Manager) onLoggedOut(s *liveSession) {
	ctx := context.Background()
	sid := s.meta.SessionID

	// 1. Zombie al Cloud (Plan 020 T3): con el stream aún vivo, ANTES de desregistrar.
	if m.cloudMux != nil {
		m.cloudMux.SendLoggedOut(sid)
	}

	// 2. Persiste el estado local (lo pendiente de T4). Best-effort: un fallo se loguea sin romper el hook.
	loMeta := s.meta
	loMeta.State = domain.SessionStateLoggedOut
	loMeta.UpdatedAt = time.Now().UTC()
	if err := m.sessions.Upsert(ctx, loMeta); err != nil {
		s.log.Warn("sessionmgr: no se pudo persistir el estado loggedout local", "session_id", sid, "error", err)
	} else {
		// Refleja el estado en el registro vivo bajo m.mu (coherencia con List()/assignRoleLocked: el
		// zombie deja de ocupar cupo). El propio s es el puntero de m.live[sid]; se muta bajo el lock.
		m.mu.Lock()
		s.meta.State = domain.SessionStateLoggedOut
		m.mu.Unlock()
	}

	// 3. No renovar lease: fuera del multiplex (cesan los heartbeats). Idempotente/no-op sin mux (tests).
	if m.cloudMux != nil {
		m.cloudMux.Unregister(sid)
	}

	// 4. Failover: si cae el primary, un standby toma el relevo (RESILIENCIA, no sigilo).
	if s.meta.Role != domain.DeviceRoleStandby && s.meta.SelfPN != "" {
		m.promoteStandbyForAccount(ctx, s.meta.SelfPN, sid)
	}
}

// promoteStandbyForAccount promueve el standby VIVO más reciente del número selfPN a primary (failover T5,
// design §6): política simple = el standby con PairedAt más reciente (el companion más "fresco"). No-op si el
// número no tiene ningún standby vivo (nada que promover) o si el store no soporta SetRole (fakes: se loguea).
//
// Consistencia BD↔memoria: elige el objetivo bajo m.mu (lectura), PERSISTE el rol fuera del lock (SetRole),
// y SOLO si la BD confirmó muta el rol en memoria bajo m.mu (evita divergir si la escritura falla). excludeID
// es el device que acaba de caer (no debe promoverse a sí mismo). CAVEAT §10.F: esto es resiliencia, no sigilo.
func (m *Manager) promoteStandbyForAccount(ctx context.Context, selfPN, excludeID string) {
	m.mu.Lock()
	var target *liveSession
	for id, s := range m.live {
		if id == excludeID || s.meta.SelfPN != selfPN {
			continue
		}
		if s.meta.State == domain.SessionStateLoggedOut || s.meta.Role != domain.DeviceRoleStandby {
			continue
		}
		if target == nil || s.meta.PairedAt.After(target.meta.PairedAt) {
			target = s
		}
	}
	m.mu.Unlock()
	if target == nil {
		return // sin standby vivo: el número queda sin primary hasta un re-escaneo (design §6).
	}

	rs, ok := m.sessions.(roleStore)
	if !ok {
		m.log.Warn("sessionmgr: store sin SetRole; promoción de standby omitida (solo memoria no persiste)",
			"session_id", target.meta.SessionID)
		return
	}
	if err := rs.SetRole(ctx, target.meta.SessionID, domain.DeviceRolePrimary); err != nil {
		m.log.Warn("sessionmgr: no se pudo persistir la promoción a primary", "session_id", target.meta.SessionID, "error", err)
		return
	}
	m.mu.Lock()
	target.meta.Role = domain.DeviceRolePrimary
	m.mu.Unlock()
	// RESILIENCIA, no sigilo: se logra tolerancia a fallos, no evasión de baneo (design §10.F).
	m.log.Info("sessionmgr: failover — standby promovido a primary (resiliencia, no sigilo)",
		"session_id", target.meta.SessionID, "self_pn_hint", "redactado")
}
