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

	// aplicaMu SERIALIZA EL CICLO ENTERO de aplicación —leer la fila, validar, persistir y notificar— de modo
	// que sea UNA SOLA sección crítica. Es un candado DISTINTO de `mu` (que solo protege el mapa de kinds) y
	// tiene que serlo: `Apply` toma éste y luego llama a `registrationFor`, que toma aquél; con un único
	// sync.Mutex —que no es reentrante— eso sería un auto-bloqueo inmediato. El orden es siempre
	// aplicaMu → mu, y NADIE lo toma al revés, así que no hay ciclo posible.
	//
	// 🔴 QUÉ AGUJERO CIERRA, Y POR QUÉ NO BASTABA CON LA GUARDA DEL KIND. Los ConfigUpdate llegan por
	// `disp.dispatch` del adapter CloudLink, que corre UN WORKER POR session_id EN PARALELO, y `PushConfig`
	// fanea el MISMO frame a todas las sesiones vivas del tenant. Sin este candado, dos workers con dos
	// versiones distintas hacen `Get → validar → Put → notificar` entrelazados: el frame VIEJO puede validar
	// (contra la foto en memoria de antes) y hacer su `Put` DESPUÉS del nuevo, porque `Store.Put` sobrescribe
	// SIN CONDICIÓN. Resultado: disco con la versión vieja y memoria con la nueva, sin un solo error.
	//
	// El síntoma no aparece hasta el REINICIO siguiente, y entonces es total: `Bootstrap` lee el disco, la
	// memoria arranca vacía y la guarda de monotonicidad —que compara contra lo vigente EN MEMORIA— no tiene
	// nada con qué disparar, así que el Edge levanta con el mapa retrasado. Una sesión que la nube reactivó
	// sigue muda para siempre y no hay ni log, ni métrica, ni error que lo diga.
	//
	// ⚠️ DESVIACIÓN DELIBERADA: esto es el mecanismo COMPARTIDO, así que también serializa `intents` y `jwks`.
	// Es estrictamente una mejora —los tres kinds ganan la atomicidad que ninguno tenía— y no toca ningún
	// camino caliente: aplicar config ocurre en el arranque y cuando la nube empuja, mientras que el camino
	// caliente del Edge es `onMessage`, que no pasa por aquí ni de lejos (INV-051.2 intacto). El precio es que
	// dos pushes de kinds DISTINTOS ya no se aplican a la vez; a la escala de este Service (un `Put` sobre una
	// fila) eso es invisible.
	//
	// ⚠️ SE MANTIENE TOMADO MIENTRAS CORREN LOS SUSCRIPTORES (`notify`), y es a propósito: publicar la foto en
	// memoria FUERA de la sección crítica devolvería la carrera por la puerta de atrás. Ningún suscriptor
	// llama de vuelta a Apply/Bootstrap —si alguno lo hiciera, se auto-bloquearía—, y todos son rápidos por
	// contrato (ver el doc de Subscriber).
	aplicaMu sync.Mutex
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
//
// 🔴 EL CUERPO ENTERO ES UNA SOLA SECCIÓN CRÍTICA (`aplicaMu`, ver su campo): leer-validar-persistir-notificar
// no puede entrelazarse con otro Apply, porque los workers del demux corren en paralelo y `Store.Put`
// sobrescribe sin condición. El kind desconocido se resuelve ANTES de tomarlo: es una consulta al mapa de
// registraciones y no toca ni el store ni a ningún suscriptor, así que un frame de un kind que nadie escucha
// no tiene por qué esperar a que termine la aplicación de otro.
func (s *Service) Apply(ctx context.Context, kind, version string, payload []byte) error {
	reg, known := s.registrationFor(kind)
	if !known {
		s.log.Info("edgeconfig: kind desconocido; ignorado (tolerante a kinds futuros)",
			"kind", kind, "version", version)
		return nil
	}

	s.aplicaMu.Lock()
	defer s.aplicaMu.Unlock()

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
//
// 🔴 NO SE AUTO-BLOQUEA CON `Apply`, y el orden de los candados es lo que lo garantiza. Bootstrap toma `mu`
// SOLO para copiar el mapa de kinds y lo SUELTA antes de tocar nada más; recién entonces toma `aplicaMu` para
// el recorrido. Nunca sostiene los dos a la vez, así que la única secuencia anidada del fichero sigue siendo
// la de Apply (aplicaMu → mu) y no hay ciclo. Tomarlo aquí sí hace falta: en el cableado del daemon Bootstrap
// corre antes de que el stream CloudLink exista, pero este método es público y notifica a los MISMOS
// suscriptores que Apply — dejarlo fuera del candado reabriría la carrera para cualquier otro orden de arranque.
func (s *Service) Bootstrap(ctx context.Context) {
	s.mu.Lock()
	kinds := make(map[string]registration, len(s.kinds))
	maps.Copy(kinds, s.kinds)
	s.mu.Unlock()

	s.aplicaMu.Lock()
	defer s.aplicaMu.Unlock()

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
