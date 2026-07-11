package edgeconfig

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Validator valida el blob de un kind ANTES de persistirlo. Devuelve error si el blob es inválido: el
// Service conserva entonces la config anterior (last-known-good). Para kind='intents' se cablea
// intents.ParseAndValidate.
type Validator func(payload []byte) error

// Subscriber recibe una config recién APLICADA (persistida) de un kind, para recargar en caliente (p.ej.
// el clasificador regenera prompt/schema). Se invoca en Apply (config nueva) y en Bootstrap (config
// persistida al arrancar). Debe ser rápido y no bloquear: corre en la goroutine del worker del demux.
type Subscriber func(rec Record)

// registration agrupa el validador y los suscriptores de un kind.
type registration struct {
	validate    Validator
	subscribers []Subscriber
}

// Service es la lógica de aplicación de config empujada: idempotencia por versión, validación por kind,
// persistencia y notificación en caliente. Es el ConfigApplier que el adapter CloudLink invoca al recibir
// un ConfigUpdate. Seguro para uso concurrente (los workers del demux corren por session_id en paralelo).
type Service struct {
	store Store
	log   sharedlogger.Logger
	now   func() time.Time

	mu    sync.Mutex
	kinds map[string]registration
}

// NewService construye el Service sobre un Store. Registra los kinds con RegisterKind antes de Apply/Bootstrap.
func NewService(store Store, log sharedlogger.Logger) *Service {
	if log == nil {
		log = sharedlogger.Default()
	}
	return &Service{store: store, log: log, now: time.Now, kinds: make(map[string]registration)}
}

// RegisterKind declara un kind conocido con su validador (opcional) y sus suscriptores. Un ConfigUpdate de
// un kind NO registrado se ignora con log + Ack (tolerante a kinds futuros). Se llama en el cableado ANTES
// de arrancar el stream/Bootstrap.
func (s *Service) RegisterKind(kind string, validate Validator, subs ...Subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kinds[kind] = registration{validate: validate, subscribers: subs}
}

// Apply aplica un ConfigUpdate (ADR-0021). Semántica (siempre termina en Ack; el error solo señala fallo de
// PERSISTENCIA, reintentable al reconectar):
//   - kind desconocido       ⇒ log + nil (Ack tolerante).
//   - versión ya aplicada    ⇒ nil (Ack idempotente, sin trabajo).
//   - blob inválido          ⇒ log ERROR + conserva la anterior + nil (no reintentable).
//   - válido y nuevo         ⇒ persistir + notificar suscriptores + nil.
func (s *Service) Apply(ctx context.Context, kind, version string, payload []byte) error {
	reg, known := s.registrationFor(kind)
	if !known {
		s.log.Info("edgeconfig: kind desconocido; ignorado (tolerante a kinds futuros)",
			"kind", kind, "version", version)
		return nil
	}

	cur, found, err := s.store.Get(ctx, kind)
	if err != nil {
		return err // fallo de lectura: reintentable (el Cloud reempuja al reconectar)
	}
	if found && cur.Version == version {
		s.log.Info("edgeconfig: versión ya aplicada; Ack idempotente sin trabajo",
			"kind", kind, "version", version)
		return nil
	}

	if reg.validate != nil {
		if verr := reg.validate(payload); verr != nil {
			s.log.Error("edgeconfig: config inválida; se conserva la anterior (last-known-good)",
				"kind", kind, "version", version, "error", verr)
			return nil // no reintentable: reenviar el mismo blob volvería a fallar
		}
	}

	rec := Record{Kind: kind, Version: version, Payload: payload, UpdatedUnix: s.now().Unix()}
	if perr := s.store.Put(ctx, rec); perr != nil {
		return fmt.Errorf("edgeconfig: aplicar config %q: %w", kind, perr)
	}
	s.notify(reg, rec)
	s.log.Info("edgeconfig: config aplicada y notificada en caliente", "kind", kind, "version", version)
	return nil
}

// Bootstrap recarga la config PERSISTIDA de todos los kinds registrados al arrancar (last-known-good tras un
// reinicio): por cada kind con fila, notifica a sus suscriptores para que el clasificador arranque con la
// última config buena sin esperar un nuevo push del Cloud. Un fallo de lectura NO es fatal (se loguea y se
// sigue): el Cloud reempuja al conectar.
func (s *Service) Bootstrap(ctx context.Context) {
	s.mu.Lock()
	kinds := make(map[string]registration, len(s.kinds))
	maps.Copy(kinds, s.kinds)
	s.mu.Unlock()

	for kind, reg := range kinds {
		rec, found, err := s.store.Get(ctx, kind)
		if err != nil {
			s.log.Error("edgeconfig: Bootstrap no pudo leer config persistida (se sigue)", "kind", kind, "error", err)
			continue
		}
		if !found {
			continue
		}
		s.notify(reg, rec)
		s.log.Info("edgeconfig: config persistida recargada al arrancar (last-known-good)",
			"kind", kind, "version", rec.Version)
	}
}

// registrationFor devuelve la registración de un kind bajo lock.
func (s *Service) registrationFor(kind string) (registration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reg, ok := s.kinds[kind]
	return reg, ok
}

// notify invoca a los suscriptores del kind con la config aplicada. Un pánico de un suscriptor se aísla
// (recover) para no tumbar el worker del demux ni saltarse a los demás suscriptores.
func (s *Service) notify(reg registration, rec Record) {
	for _, sub := range reg.subscribers {
		func() {
			defer func() {
				if r := recover(); r != nil {
					s.log.Error("edgeconfig: pánico en suscriptor de config (aislado)", "kind", rec.Kind, "panic", r)
				}
			}()
			sub(rec)
		}()
	}
}
