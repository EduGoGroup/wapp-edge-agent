package app

// RestoreSessions es el caso de uso de ARRANQUE del Edge (T6.2, RF-7): al iniciar el daemon, resuelve
// qué sesión(es) reanudar desde los metadatos persistidos y arranca la escucha 24/7 SIN re-emparejar.
//
// DECISIÓN DE DISEÑO (design §5) — por qué este caso de uso existe sin duplicar la conexión:
// app.Listen YA realiza el "restore criptográfico" de facto (carga la DEK de custodia y, vía
// ListenGateway, descifra el store, reconstruye el *store.Device pareado, conecta el cliente
// whatsmeow y engancha el listener). Reimplementar eso aquí sería duplicar lógica de red/cripto y
// arriesgar drift. Lo que FALTABA —y lo que RestoreSessions aporta— es el REGISTRO DE NEGOCIO de las
// sesiones: la tabla `sessions` (metadatos en claro: jid/estado/timestamps). RestoreSessions:
//   1. lista las sesiones persistidas (SessionStore) — la verdad de negocio sobre qué hay vinculado;
//   2. si el registro está vacío PERO hay un device pareado en el store cifrado (caso de una BD
//      pareada ANTES de existir esta tabla, p.ej. la sesión real del spike), lo "backfillea":
//      resuelve el JID vía el PairedDeviceLocator (FirstDeviceJID) y lo da de alta como activo;
//   3. marca la sesión elegida como activa (refresca updated_at) — owns el ciclo de vida del metadato;
//   4. DELEGA el connect + escucha always-on al flujo existente (app.Listen), sin duplicarlo.
//
// Resultado (RF-7): reiniciar el binario reanuda la sesión emparejada sin QR nuevo, y el arranque
// queda gobernado por metadatos explícitos e inspeccionables (estado active/loggedout, paired_at),
// base para el multi-teléfono (ADR-0008) y el lease/kill-switch de fases posteriores.
//
// El spike maneja UNA sesión: se restaura la primera. Desacoplado por interfaces para tests con fakes.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/google/uuid"
)

// SessionStore es el puerto de PERSISTENCIA de los metadatos de negocio de las sesiones (tabla
// `sessions`). La implementación real (internal/adapters/sessionstore) lee/escribe SQLite EN CLARO;
// un fake en los tests lo simula en memoria. No custodia material cripto (eso es cryptostore/DEK).
type SessionStore interface {
	// Upsert inserta o actualiza la sesión por su session_id (clave primaria, ADR-0016 §3).
	Upsert(ctx context.Context, s domain.Session) error
	// List devuelve todas las sesiones persistidas (vacío si no hay ninguna).
	List(ctx context.Context) ([]domain.Session, error)
	// ListActive devuelve SOLO las sesiones en estado 'active': las que el arranque debe restaurar
	// (design §6). El Session Manager (sessionmgr.Manager.Restore, T4) itera ESTA lista para arrancar
	// un listener por sesión; las 'pairing'/'loggedout' se omiten por construcción.
	ListActive(ctx context.Context) ([]domain.Session, error)
	// Get devuelve la sesión con ese session_id, o ErrSessionNotFound si no existe.
	Get(ctx context.Context, sessionID string) (domain.Session, error)
	// Delete elimina la fila de la sesión con ese session_id (idempotente: borrar una ausente no es
	// error). Es la parte de metadatos del borrado quirúrgico (design §7) y de la limpieza del
	// pairing fallido (design §5): el Manager la usa para no dejar restos.
	Delete(ctx context.Context, sessionID string) error
}

// DeviceCascadeStore es una CAPACIDAD OPCIONAL sobre SessionStore (Plan 027 T4, cierra H4): borrado
// transaccional POR DISPOSITIVO (fila del device + su cuenta si queda vacía, en una sola tx — cero
// huérfanos). Puerto EXPLÍCITO de app en vez de una interfaz ad-hoc acoplada al store concreto: el
// consumidor (sessionmgr.Manager) lo resuelve por interface-upgrade UNA vez y cae a SessionStore.Delete
// si el store no la implementa (los fakes en memoria de los tests no purgan la cuenta).
type DeviceCascadeStore interface {
	DeleteDeviceCascade(ctx context.Context, sessionID string) error
}

// AccountStore es una CAPACIDAD OPCIONAL sobre SessionStore (Plan 027 T4, cierra H4): operaciones POR
// CUENTA (número) —listar los dispositivos de una cuenta y borrarlos junto a la cuenta en una tx. Puerto
// EXPLÍCITO de app; el Manager lo resuelve por interface-upgrade UNA vez y, si el store no la implementa
// (fakes en memoria), el borrado por cuenta responde con un error claro de "no soportado".
type AccountStore interface {
	GetByAccount(ctx context.Context, accountID string) ([]domain.Session, error)
	DeleteByAccount(ctx context.Context, accountID string) error
}

// PairedDeviceLocator resuelve el JID del device pareado que vive en el store CIFRADO
// (msg_enc_device) SIN descifrar material. Permite a RestoreSessions backfillear el registro de
// negocio cuando el device fue pareado antes de existir la tabla `sessions`. La implementación real
// envuelve cryptostore.FirstDeviceJID; un fake en los tests lo simula.
type PairedDeviceLocator interface {
	// PairedJID devuelve el JID de la sesión pareada y ok=true si existe; ok=false si el store no
	// tiene ningún device pareado; error ante un fallo de lectura del store.
	PairedJID(ctx context.Context) (jid string, ok bool, err error)
}

// SessionRunner abstrae el arranque de la escucha always-on de una sesión ya restaurada
// (criptográficamente). En producción lo implementa *app.Listen (Run carga la DEK de custodia y,
// vía ListenGateway, descifra el store, reconstruye el device, conecta y bloquea con el socket vivo
// hasta la cancelación del ctx). Inyectarlo por interfaz evita DUPLICAR el flujo de conexión aquí y
// mantiene RestoreSessions testeable con un fake.
type SessionRunner interface {
	Run(ctx context.Context) error
}

// Errores del caso de uso (sin material sensible).
var (
	// ErrNoSessions: no hay ninguna sesión persistida NI device pareado en el store: nada que
	// restaurar (hay que emparejar primero con `pair`).
	ErrNoSessions = errors.New("restore: no hay sesión emparejada que restaurar")
	// ErrSessionLoggedOut: la sesión a restaurar está marcada loggedout (WhatsApp la cerró); no se
	// re-empareja automáticamente (RF-6).
	ErrSessionLoggedOut = errors.New("restore: la sesión está cerrada (loggedout); re-empareja con pair")
	// ErrSessionNotFound lo devuelve SessionStore.Get cuando el JID no existe.
	ErrSessionNotFound = errors.New("sessionstore: sesión no encontrada")
)

// RestoreSessions es el caso de uso SINGLE-SESIÓN heredado (resuelve UNA sesión y delega a un runner).
//
// SUPERSEDIDO POR sessionmgr.Manager.Restore (Plan 008 T4): la restauración MULTI-SESIÓN real —iterar
// todas las activas (SessionStore.ListActive) y arrancar UN listener por sesión, cada uno en su
// goroutine con aislamiento de fallos y apagado ordenado (design §6/§10.H/§10.I)— vive en el Session
// Manager, que es quien posee el ciclo de vida de N sesiones. Este tipo se conserva mientras el
// daemon legacy (cmd/agent runRestore) no se recablee al Manager (cierre en T6, junto al plano de
// control). El `agent serve` ya usa el Manager. Sus dependencias son puertos para inyectar fakes.
type RestoreSessions struct {
	sessions SessionStore
	locator  PairedDeviceLocator
	runner   SessionRunner
	now      func() time.Time
}

// RestoreOption configura un RestoreSessions en su construcción.
type RestoreOption func(*RestoreSessions)

// withClock inyecta el reloj (tests: timestamps deterministas).
func withClock(f func() time.Time) RestoreOption {
	return func(r *RestoreSessions) {
		if f != nil {
			r.now = f
		}
	}
}

// NewRestoreSessions construye el caso de uso con los puertos dados.
func NewRestoreSessions(sessions SessionStore, locator PairedDeviceLocator, runner SessionRunner, opts ...RestoreOption) *RestoreSessions {
	r := &RestoreSessions{
		sessions: sessions,
		locator:  locator,
		runner:   runner,
		now:      time.Now,
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Run resuelve la sesión a restaurar, refresca su metadato a 'active' y DELEGA la escucha always-on
// al runner (app.Listen), bloqueando hasta la cancelación del ctx. Devuelve ErrNoSessions si no hay
// nada que restaurar, o ErrSessionLoggedOut si la sesión está cerrada.
func (r *RestoreSessions) Run(ctx context.Context) error {
	session, err := r.resolve(ctx)
	if err != nil {
		return err
	}
	if session.State == domain.SessionStateLoggedOut {
		return ErrSessionLoggedOut
	}

	// Refresca el metadato: la sesión vuelve a estar activa al restaurarse (updated_at = ahora).
	session.State = domain.SessionStateActive
	session.UpdatedAt = r.now()
	if err := r.sessions.Upsert(ctx, session); err != nil {
		return fmt.Errorf("restore: actualizar metadatos de sesión: %w", err)
	}

	// Delega el connect + escucha always-on al flujo existente (sin duplicar la conexión/cripto).
	if err := r.runner.Run(ctx); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	return nil
}

// resolve obtiene la sesión a restaurar: primero del registro de negocio (sessions); si está vacío,
// backfillea desde el store cifrado (device pareado pre-tabla). Devuelve ErrNoSessions si no hay nada.
func (r *RestoreSessions) resolve(ctx context.Context) (domain.Session, error) {
	list, err := r.sessions.List(ctx)
	if err != nil {
		return domain.Session{}, fmt.Errorf("restore: listar sesiones persistidas: %w", err)
	}
	if len(list) > 0 {
		// Spike: una sola sesión. La primera es la que se restaura.
		return list[0], nil
	}

	// Registro vacío: ¿hay un device pareado en el store cifrado (BD pareada antes de la tabla)?
	jid, ok, err := r.locator.PairedJID(ctx)
	if err != nil {
		return domain.Session{}, fmt.Errorf("restore: resolver device pareado: %w", err)
	}
	if !ok {
		return domain.Session{}, ErrNoSessions
	}
	now := r.now()
	// PUENTE T0 (Plan 008): el modelo multi-sesión llava por session_id (ADR-0016 §3); el device legacy
	// pareado pre-registro se da de alta bajo un UUID nuevo y su store_dir relativo. En T4 este backfill
	// DESAPARECE: restore itera las sesiones activas ya registradas por el Manager (design §6).
	sessionID := uuid.NewString()
	return domain.Session{
		SessionID: sessionID,
		JID:       jid,
		State:     domain.SessionStateActive,
		StoreDir:  "sessions/" + sessionID,
		PairedAt:  now, // desconocido (pareado antes del registro): se ancla al backfill.
		UpdatedAt: now,
	}, nil
}
