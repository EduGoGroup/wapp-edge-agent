package sessionmgr

// unlink.go aporta el BORRADO QUIRÚRGICO de UNA sesión (Plan 008 T5, design §7, R5): desvincular un
// teléfono concreto sin tocar a los demás. Es la evolución per-`session_id` del unlink single-sesión
// (que borraba la ranura GLOBAL `dek.key`): aquí cada paso opera SOLO sobre la entrada de la sesión.
//
// CONCURRENCIA (clave del aislamiento): el Manager tiene UN WaitGroup GLOBAL que une a TODOS los
// listeners; esperar ahí uniría a todas las sesiones. Para unir SOLO la goroutine de la sesión borrada
// se usa su canal `done` (liveSession.waitDone), que cierra esa goroutine al retornar tras su cancel.
// Así Unlink cancela y espera a SU listener, mientras los demás siguen escuchando intactos.

import (
	"context"
	"errors"
	"fmt"

	"go.mau.fi/whatsmeow/types"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cryptostore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// ErrSessionNotFound: se pidió Unlink de un session_id que no está ni VIVO (registro en RAM) ni en los
// METADATOS persistidos. Es distinguible vía errors.Is para que el plano de control lo mapee a 404
// (design §7); se diferencia de app.ErrSessionNotFound (el del store) para no acoplar el llamador a la
// capa de persistencia.
var ErrSessionNotFound = errors.New("sessionmgr: sesión no encontrada")

// Unlink desvincula y BORRA QUIRÚRGICAMENTE el dispositivo id, fiel a la secuencia de design §7 sobre la
// BD ÚNICA (decisión §10.I, cero huérfanos):
//
//  1. saca la sesión del registro vivo (bajo m.mu) y, si tenía listener, cancela SU goroutine y espera a
//     que cierre (waitDone), sin tocar a las demás sesiones (cancel FUERA del lock, como Stop);
//  2. limpia la DEK de ESE device (custodyFor(id).Clear()), NO una ranura global;
//  3. purga su material CIFRADO por JID (cryptostore.DeleteDevice: msg_enc_* + whatsmeow_*) de la BD única;
//  4. borra su fila de metadatos y, si la CUENTA queda vacía, también la cuenta, en una TX
//     (deleteDeviceMeta → DeleteDeviceCascade). YA NO hay directorio/store.db por sesión que borrar.
//
// Si la sesión NO existe (ni viva ni persistida) devuelve ErrSessionNotFound (→ 404) SIN efectos
// colaterales (el chequeo precede a cualquier borrado). Los pasos de limpieza son best-effort: cada uno
// se intenta aunque otro falle (para minimizar restos) y los errores se agregan (errors.Join) y se
// loguean; un fallo de limpieza se propaga (→ 5xx en el plano de control) sin dejar la sesión a medias.
func (m *Manager) Unlink(ctx context.Context, id string) error {
	// 1. Sacar la sesión del registro vivo y unir SOLO su goroutine (cancel/espera fuera del lock).
	jid, live := m.stopLive(id)
	if live {
		m.log.Info("sesión desvinculada: listener detenido, procediendo al borrado quirúrgico", "session_id", id)
	}

	// Unregister-on-unlink: saca la sesión del multiplex CloudLink para que sus comandos posteriores se
	// ignoren limpio (sin afectar a las demás del único stream). Idempotente: no-op si nunca se registró.
	if m.cloudMux != nil {
		m.cloudMux.Unregister(id)
	}

	// 2. Existencia: si no estaba viva, confirmar que existe en metadatos antes de borrar nada (id
	//    inexistente ⇒ ErrSessionNotFound → 404, sin efectos colaterales). Se aprovecha para conocer su JID
	//    (necesario para purgar el material cifrado). Una sesión viva ya trae su JID de s.meta.
	if !live {
		sess, err := m.sessions.Get(ctx, id)
		if err != nil {
			if errors.Is(err, app.ErrSessionNotFound) {
				return fmt.Errorf("%w: %s", ErrSessionNotFound, id)
			}
			return fmt.Errorf("sessionmgr: verificar sesión a desvincular: %w", err)
		}
		jid = sess.JID
	}

	// 3-4. Limpieza best-effort per-device (DEK + material cifrado por JID + fila/cuenta transaccional). Se
	//      intentan todos los pasos aunque uno falle (minimiza restos); los errores se agregan y se loguean.
	var errs []error
	if err := m.clearDEK(id); err != nil {
		errs = append(errs, err)
	}
	if err := m.deleteCryptoMaterial(ctx, jid); err != nil {
		errs = append(errs, err)
	}
	if err := m.deleteDeviceMeta(ctx, id); err != nil {
		errs = append(errs, fmt.Errorf("borrar fila de metadatos: %w", err))
	}

	if len(errs) > 0 {
		joined := errors.Join(errs...)
		m.log.Warn("sessionmgr: borrado quirúrgico con errores de limpieza", "session_id", id, "error", joined)
		return fmt.Errorf("sessionmgr: borrar sesión %s: %w", id, joined)
	}

	m.log.Info("sessionmgr: sesión borrada quirúrgicamente", "session_id", id)
	return nil
}

// UnlinkAccount desvincula y BORRA QUIRÚRGICAMENTE la CUENTA accountID entera y TODOS sus dispositivos
// (borrado por número, design §7/§10.I): por cada device cancela su listener vivo, lo saca del mux y purga
// su DEK y su material cifrado (msg_enc_*/whatsmeow_*); al final borra la cuenta y sus devices en una TX
// (DeleteByAccount). Cero huérfanos. Devuelve ErrSessionNotFound si la cuenta no tiene dispositivos.
//
// Requiere un sessionstore concreto con soporte por-cuenta (GetByAccount/DeleteByAccount); con los fakes
// en memoria de los tests (que no lo implementan) devuelve un error de cableado claro. Es la contraparte
// por-cuenta de Unlink, base del borrado por número (T5) y forward-compatible con el plano de control.
//
// AGREGACIÓN POR CUENTA EN CLOUDLINK (Plan 022 T6, decisión §10.J): "revocar por número = revocar sus N
// devices" se materializa AQUÍ, en el Edge, iterando los devices de la cuenta y sacando CADA session_id del
// multiplex (m.cloudMux.Unregister). Es correcto por capas: el proto CloudLink (v0.6.0) multiplexa y lleva
// lease POR session_id (ADR-0008 §"un stream", ADR-0016 §5); NO existe una noción de cuenta ni un frame de
// revocación agregable por número en el contrato — el account↔device lo conoce solo el Edge (tablas
// accounts/devices), no el mux (que es deliberadamente ciego a la cuenta).
//
// TODO(cloud, follow-up Plan 022 T6 · ver ../../docs/piezas/03-plataforma-cloud.md): para revocar una CUENTA
// entera DESDE LA NUBE (kill-switch por número, no por sesión) el Cloud debe hoy hacer FAN-OUT de N
// LeaseUpdate{revoked}, uno por cada session_id del número (mapeo cuenta→sesiones que ya vive en el fleet del
// Cloud). Si se quiere un frame agregable de primera clase (p. ej. AccountLeaseUpdate por account_id/self_pn,
// o revocación por número en un solo mensaje), el corte es del repo cloud/proto y se ANOTA como plan aparte:
// NO se amplía este tramo al cloud (regla dura T6). El Edge ya deja el otro lado listo (Unregister por device).
func (m *Manager) UnlinkAccount(ctx context.Context, accountID string) error {
	if m.account == nil {
		return fmt.Errorf("sessionmgr: el store no soporta borrado por cuenta (GetByAccount/DeleteByAccount)")
	}
	devices, err := m.account.GetByAccount(ctx, accountID)
	if err != nil {
		return fmt.Errorf("sessionmgr: listar dispositivos de la cuenta: %w", err)
	}
	if len(devices) == 0 {
		return fmt.Errorf("%w: cuenta %s", ErrSessionNotFound, accountID)
	}

	var errs []error
	for _, dev := range devices {
		jid, live := m.stopLive(dev.SessionID)
		if !live {
			jid = dev.JID
		}
		if m.cloudMux != nil {
			m.cloudMux.Unregister(dev.SessionID)
		}
		if err := m.clearDEK(dev.SessionID); err != nil {
			errs = append(errs, err)
		}
		if err := m.deleteCryptoMaterial(ctx, jid); err != nil {
			errs = append(errs, err)
		}
	}

	// Metadatos: borra TODOS los devices + la cuenta en una transacción (cero huérfanos).
	if err := m.account.DeleteByAccount(ctx, accountID); err != nil {
		errs = append(errs, fmt.Errorf("borrar cuenta y dispositivos: %w", err))
	}

	if len(errs) > 0 {
		joined := errors.Join(errs...)
		m.log.Warn("sessionmgr: borrado por cuenta con errores de limpieza", "account_id", accountID, "error", joined)
		return fmt.Errorf("sessionmgr: borrar cuenta %s: %w", accountID, joined)
	}
	m.log.Info("sessionmgr: cuenta borrada quirúrgicamente", "account_id", accountID, "dispositivos", len(devices))
	return nil
}

// stopLive saca la sesión id del registro vivo (bajo lock), cancela su listener y espera a que su
// goroutine cierre (waitDone), TODO fuera del lock (no sostener m.mu mientras se une la goroutine). No
// afecta a las demás sesiones. Devuelve el JID de la sesión que estaba viva (para el borrado cifrado) y
// si estaba viva.
func (m *Manager) stopLive(id string) (jid string, live bool) {
	m.mu.Lock()
	s, ok := m.live[id]
	if ok {
		delete(m.live, id)
	}
	m.mu.Unlock()
	// Salud (Plan 031 T6): la sesión se desvincula ⇒ deja de reportar salud (idempotente y nil-safe). Se
	// hace aunque no estuviera viva (una 'pairing' pudo dejar una entrada connecting).
	m.health.Remove(id)
	if !ok {
		return "", false
	}
	s.stop()
	s.waitDone()
	return s.meta.JID, true
}

// clearDEK limpia la DEK custodiada del device id (best-effort). Clear abstrae el backend de custodia (el
// futuro KeystoreCustody NO vive bajo el dir): es un paso PROPIO del borrado quirúrgico. Idempotente.
func (m *Manager) clearDEK(id string) error {
	custody, err := m.custodyFor(id)
	if err != nil {
		return fmt.Errorf("resolver custodia: %w", err)
	}
	if cl, ok := custody.(interface{ Clear() error }); ok {
		if err := cl.Clear(); err != nil {
			return fmt.Errorf("limpiar DEK: %w", err)
		}
	}
	return nil
}

// deleteCryptoMaterial purga el material CIFRADO del device de JID jid (msg_enc_* + whatsmeow_*) de la BD
// única (cryptostore.DeleteDevice, idempotente). No-op si el device no tiene JID (nunca emparejado) o si
// no hay BD real (tests con factories fake sin WithSharedDB). NO requiere la DEK (solo borra ciphertext).
func (m *Manager) deleteCryptoMaterial(ctx context.Context, jid string) error {
	if jid == "" || m.db == nil {
		return nil
	}
	parsed, err := types.ParseJID(jid)
	if err != nil {
		return fmt.Errorf("parsear jid para borrado cifrado: %w", err)
	}
	if err := cryptostore.DeleteDevice(ctx, m.db, m.dbDialect, parsed); err != nil {
		return fmt.Errorf("borrar material cifrado: %w", err)
	}
	return nil
}
