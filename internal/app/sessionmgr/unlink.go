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
	"os"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// ErrSessionNotFound: se pidió Unlink de un session_id que no está ni VIVO (registro en RAM) ni en los
// METADATOS persistidos. Es distinguible vía errors.Is para que el plano de control lo mapee a 404
// (design §7); se diferencia de app.ErrSessionNotFound (el del store) para no acoplar el llamador a la
// capa de persistencia.
var ErrSessionNotFound = errors.New("sessionmgr: sesión no encontrada")

// Unlink desvincula y BORRA QUIRÚRGICAMENTE la sesión id, fiel a la secuencia de design §7:
//
//  1. saca la sesión del registro vivo (bajo m.mu) y, si tenía listener, cancela SU goroutine y espera
//     a que cierre (waitDone) — eso cierra su *sql.DB vía el defer del listener (§10.I), sin tocar a las
//     demás sesiones (cancel FUERA del lock, como Stop);
//  2. limpia la DEK de ESA sesión (custodyFor(id).Clear()), NO la ranura global;
//  3. borra su fila de metadatos (sessionStore.Delete);
//  4. borra su directorio en disco (store.db + dek.key) con os.RemoveAll(SessionDir(id)).
//
// Si la sesión NO existe (ni viva ni persistida) devuelve ErrSessionNotFound (→ 404) SIN efectos
// colaterales (el chequeo precede a cualquier borrado). Los pasos de limpieza son best-effort: cada uno
// se intenta aunque otro falle (para minimizar restos) y los errores se agregan (errors.Join) y se
// loguean; un fallo de limpieza se propaga (→ 5xx en el plano de control) sin dejar la sesión a medias.
func (m *Manager) Unlink(ctx context.Context, id string) error {
	// 1. Sacar la sesión del registro vivo bajo el lock; cancelar/esperar FUERA del lock (no sostener
	//    m.mu mientras se une la goroutine). Tras el delete, List()/Health() ya no la ven.
	m.mu.Lock()
	s, live := m.live[id]
	if live {
		delete(m.live, id)
	}
	m.mu.Unlock()

	if live {
		s.stop()     // cancela el context de ESA sesión (idempotente; no afecta a las demás)
		s.waitDone() // une SOLO su goroutine; al retornar, su *sql.DB ya se cerró (defer del listener)
		s.log.Info("sesión desvinculada: listener detenido, procediendo al borrado quirúrgico")
	}

	// 2. Existencia: si no estaba viva, confirmar que existe en metadatos antes de borrar nada. Así un
	//    id inexistente devuelve ErrSessionNotFound (→ 404) sin efectos colaterales. Una sesión viva ya
	//    existía por definición: se procede directo a la limpieza.
	if !live {
		if _, err := m.sessions.Get(ctx, id); err != nil {
			if errors.Is(err, app.ErrSessionNotFound) {
				return fmt.Errorf("%w: %s", ErrSessionNotFound, id)
			}
			return fmt.Errorf("sessionmgr: verificar sesión a desvincular: %w", err)
		}
	}

	// 3-5. Limpieza per-sesión best-effort (DEK de la entrada + fila + directorio). Se intentan todos
	//      los pasos aunque uno falle (minimiza restos); los errores se agregan y se loguean.
	var errs []error

	custody, err := m.custodyFor(id)
	if err != nil {
		errs = append(errs, fmt.Errorf("resolver custodia: %w", err))
	} else if cl, ok := custody.(interface{ Clear() error }); ok {
		// Clear abstrae el backend de custodia (el futuro KeystoreCustody NO vive bajo el dir): por eso
		// es un paso PROPIO, no redundante con el RemoveAll del directorio. Idempotente.
		if err := cl.Clear(); err != nil {
			errs = append(errs, fmt.Errorf("limpiar DEK: %w", err))
		}
	}

	if err := m.sessions.Delete(ctx, id); err != nil {
		errs = append(errs, fmt.Errorf("borrar fila de metadatos: %w", err))
	}

	if dir, err := m.layout.SessionDir(id); err != nil {
		errs = append(errs, fmt.Errorf("resolver dir de sesión: %w", err))
	} else if err := os.RemoveAll(dir); err != nil {
		errs = append(errs, fmt.Errorf("borrar dir de sesión: %w", err))
	}

	if len(errs) > 0 {
		joined := errors.Join(errs...)
		m.log.Warn("sessionmgr: borrado quirúrgico con errores de limpieza", "session_id", id, "error", joined)
		return fmt.Errorf("sessionmgr: borrar sesión %s: %w", id, joined)
	}

	m.log.Info("sessionmgr: sesión borrada quirúrgicamente", "session_id", id)
	return nil
}
