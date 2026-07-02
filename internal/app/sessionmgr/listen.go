package sessionmgr

// listen.go aporta la RESTAURACIÓN y ESCUCHA MULTI-SESIÓN del Manager (Plan 008 T4, design §6/§10.H/
// §10.I): el corazón del multi-sesión. Restore itera las sesiones ACTIVAS y arranca, por cada una, UN
// listener en SU goroutine con SU context+cancel (concurrencia Go pura, SIN broker — ADR-0003). Cada
// listener:
//   - resuelve la DEK de ESA sesión (custodia inyectada) y abre SU store cifrado;
//   - mantiene el cliente whatsmeow VIVO (lección Plan 006: nada de clientes efímeros) reusando
//     app.Listen + el ListenGateway real (NO se duplica la mecánica de whatsmeow);
//   - se AÍSLA (§10.H): un pánico o una caída se recuperan (recover) y se reintenta con backoff
//     exponencial acotado, marcando la sesión 'degraded' SIN tumbar el proceso ni a las demás;
//   - participa del APAGADO ORDENADO (§10.I): Stop cancela su context, el runner retorna, el *sql.DB
//     se cierra vía defer y el WaitGroup hace el join.
//
// REUSO (no duplicación): app.Listen YA opera por-sesión (recibe custodia+gateway+sink por inyección);
// "generalizarlo a N" es construir UNA instancia por sesión con SU custodia y SU gateway (sobre el
// *sql.DB de su store), no reescribir la conexión. El factory listenFactory es la única costura de
// producción (WithWhatsmeowListen) vs. tests (fake), igual que pairFactory en el emparejamiento.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
)

// ErrListenNotConfigured: se llamó a Restore sin haber inyectado el factory de escucha (falta
// WithWhatsmeowListen en producción). Es un error de cableado, no de runtime de una sesión.
var ErrListenNotConfigured = errors.New("sessionmgr: escucha no configurada (usa WithWhatsmeowListen)")

// listenRunner abstrae la escucha always-on de UNA sesión: Run BLOQUEA manteniendo el socket vivo
// hasta que el ctx se cancele (apagado) o la conexión caiga (devuelve error). En producción lo cumple
// *app.Listen; un fake en los tests lo controla con channels (sin red ni WhatsApp).
type listenRunner interface {
	Run(ctx context.Context) error
}

// listenFactory construye, para UNA sesión, su listenRunner y el io.Closer de los recursos que el
// listener debe cerrar en el apagado (el *sql.DB del store de la sesión, design §10.I). Recibe el ctx
// del listener (para abrir el store ligado a su vida) y la liveSession (custodia + session_id + logger).
// Devuelve error si no pudo provisionar el store/gateway: runListener lo trata como una caída (reintento).
type listenFactory func(ctx context.Context, s *liveSession) (listenRunner, io.Closer, error)

// CloudLinkMux es el multiplexor CloudLink del Edge (UN stream Connect, N sesiones por session_id,
// ADR-0008). El Manager lo usa para (a) registrar cada sesión al arrancar su listener —pasando su
// emisor por cliente vivo (sendVia) y la presencia de su DEK (hasDEK)— y quitarla al desvincularla; y
// (b) obtener el sink de SALIDA etiquetado por session_id de esa sesión. El estado de lease POR SESIÓN
// (ADR-0016 §5) lo mantiene el propio mux: el Manager no conoce lease ni proto (lo satisface el Adapter
// real de cloudlink, o el LogMux de diagnóstico). Interfaz estructural: sessionmgr no importa cloudlink.
type CloudLinkMux interface {
	// Register da de alta una sesión en el multiplex (register-on-start): send es su emisor por cliente
	// vivo (recibe el command_id del envío para la correlación del acuse, Plan 013 §10.E), hasDEK la
	// presencia de la DEK (gate 2-de-2). El mux construye el estado de lease de la sesión.
	Register(sessionID string, send func(ctx context.Context, commandID, to, text string) error, hasDEK func() bool)
	// Unregister da de baja la sesión (unregister-on-unlink): sus comandos posteriores se ignoran.
	Unregister(sessionID string)
	// SinkFor devuelve el sink de SALIDA (entrantes->cloud) etiquetado con session_id.
	SinkFor(sessionID string) app.InboundSink
	// SendReceipt sube al cloud un ACUSE (delivered/read) de un saliente por el stream, etiquetado con la
	// sesión (evt.SessionID) y correlacionado con el command_id del envío original (vacío = estado crudo).
	// Es el análogo de SinkFor para acuses; lo cablea el factory al SetReceiptHandler del gateway (T2a).
	SendReceipt(commandID string, evt domain.ReceiptEvent)
}

// WithWhatsmeowListen habilita Restore/startListener con la escucha REAL sobre whatsmeow Y la cablea al
// multiplexor CloudLink (T7): por cada sesión abre SU store.db (Layout.StoreDB(id)), construye un
// ListenGateway sobre esa *sql.DB y un app.Listen que carga la DEK de la sesión (custodia inyectada) y
// mantiene el socket vivo, reenviando los entrantes al sink ETIQUETADO de esa sesión (mux.SinkFor(id)).
// El *sql.DB se devuelve como io.Closer para el apagado ordenado.
//
// MULTIPLEX (ADR-0008: un stream por Edge): el mux es ÚNICO; SinkFor(id) etiqueta la salida con el
// session_id y el lease es por sesión (lo mantiene el mux). El live-sender se EXPONE así: cada ciclo de
// escucha rota s.setLiveSender(gateway.SendViaLiveClient) y el mux tiene registrado s.sendVia
// (indirección estable, ver startListener), de modo que un SendText para la sesión X llega al cliente
// VIVO de X (no a uno efímero, lección Plan 006). El register/unregister vive en startListener/Unlink.
//
// pushName es el nombre visible de FALLBACK para anunciar presencia (Plan 013 §10.D): se aplica a cada
// gateway y solo se usa si el store restaurado no conoce ya el nombre real de la cuenta (ver SetPushName).
func WithWhatsmeowListen(mux CloudLinkMux, pushName string) Option {
	return func(m *Manager) {
		m.cloudMux = mux
		m.newListener = func(ctx context.Context, s *liveSession) (listenRunner, io.Closer, error) {
			storePath, err := m.layout.StoreDB(s.meta.SessionID)
			if err != nil {
				return nil, nil, fmt.Errorf("resolver store de sesión: %w", err)
			}
			sdb, err := wappdb.OpenSessionStore(ctx, storePath)
			if err != nil {
				return nil, nil, fmt.Errorf("abrir store de sesión: %w", err)
			}
			gateway := whatsmeow.NewListenGateway(sdb, s.log)
			// Nombre visible de FALLBACK para anunciar presencia (Plan 013 §10.D): whatsmeow rechaza
			// SendPresence si el store restaurado no trae PushName. El gateway solo lo usa si el nombre
			// real de la cuenta (app-state) aún no está; ese prevalece.
			gateway.SetPushName(pushName)
			sid := s.meta.SessionID
			// Rota el live-sender de ESTE ciclo: el mux ya tiene registrado s.sendVia; aquí solo apunta la
			// indirección al cliente vivo recién creado (una reconexión = gateway nuevo). Usa la variante
			// TRACKED (Plan 013 §10.E): cada SendText puebla el Correlator (command_id ↔ MessageID) para
			// que el acuse posterior suba etiquetado con su command_id.
			s.setLiveSender(func(ctx context.Context, commandID, to, text string) error {
				_, err := gateway.SendViaLiveClientTracked(ctx, commandID, to, text)
				return err
			})
			// Cablea la SALIDA de acuses de ESTA sesión (Plan 013 T2a): al llegar un events.Receipt, se
			// etiqueta con el session_id, se correlaciona con el command_id del envío (Correlator) y se
			// sube al cloud por el stream. Sin correlación (vencido/desconocido) sube como estado crudo.
			gateway.SetReceiptHandler(func(evt domain.ReceiptEvent) {
				evt.SessionID = sid
				cmd, _ := gateway.Correlator().Lookup(evt.MessageIDs)
				mux.SendReceipt(cmd, evt)
			})
			runner := app.NewListen(s.custody, gateway, mux.SinkFor(sid))
			return runner, sdb, nil
		}
	}
}

// Restore reanuda TODAS las sesiones activas (design §6, RF-7): lista las 'active' (SessionStore.
// ListActive) y arranca, por cada una, un listener en su goroutine (un teléfono por sesión, ADR-0008).
// NO bloquea: deja los listeners corriendo y retorna; el llamante (daemon) bloquea hasta SIGINT y
// entonces invoca Stop() para el apagado ordenado. Un fallo al provisionar UNA sesión (custodia
// inválida) se aísla: se loguea y se omite esa sesión sin abortar las demás. ctx gobierna SOLO la
// consulta de arranque; cada listener cuelga de SU propio context (cancelado por Stop, §10.I), no de ctx.
func (m *Manager) Restore(ctx context.Context) error {
	if m.newListener == nil {
		return ErrListenNotConfigured
	}
	active, err := m.sessions.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("sessionmgr: listar sesiones activas: %w", err)
	}

	started := 0
	for _, meta := range active {
		custody, err := m.custodyFor(meta.SessionID)
		if err != nil {
			// session_id inválido (no debería ocurrir con datos del registro): se aísla y se omite.
			m.log.Error("sessionmgr: custodia inválida; se omite la sesión al restaurar",
				"session_id", meta.SessionID, "error", err)
			continue
		}
		s := &liveSession{
			meta:    meta,
			custody: custody,
			log:     m.log.With("session_id", meta.SessionID, "jid", meta.JID),
		}

		m.mu.Lock()
		if _, exists := m.live[meta.SessionID]; exists {
			// Ya viva (Restore idempotente / llamada doble): no la re-arranquemos para no orfanar su cancel.
			m.mu.Unlock()
			continue
		}
		m.live[meta.SessionID] = s
		m.mu.Unlock()

		m.startListener(s)
		started++
	}

	m.log.Info("restauración multi-sesión: listeners arrancados",
		"sesiones_activas", len(active), "listeners_arrancados", started)
	return nil
}

// startListener arranca la escucha always-on de una sesión ya registrada (la usan Pair Y Restore,
// design §6/§10.I). Crea el context+cancel del listener (parentado en Background: el listener debe
// sobrevivir al ctx de la petición que lo originó —p.ej. el request de Pair—; SOLO Stop lo cancela),
// lo guarda en la liveSession para el apagado ordenado, suma al WaitGroup y lanza la goroutine.
//
// Si la escucha NO está configurada (falta WithWhatsmeowListen, p.ej. en los tests de Pair sin
// WhatsApp), la sesión queda registrada pero SIN listener (warn), en vez de arrancar uno a medias.
func (m *Manager) startListener(s *liveSession) {
	if m.newListener == nil {
		s.log.Warn("sesión registrada pero SIN escucha: falta WithWhatsmeowListen (listener no arrancado)")
		return
	}
	// Register-on-start (Restore y Pair): multiplexa esta sesión en el único stream del Edge (ADR-0008).
	// sendVia es la indirección estable al cliente vivo (lo rota el factory en cada ciclo); custody.Exists
	// alimenta el gate 2-de-2 por sesión. nil en tests que cablean newListener sin mux.
	if m.cloudMux != nil {
		m.cloudMux.Register(s.meta.SessionID, s.sendVia, s.custody.Exists)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.arm(cancel)
	s.mark(HealthStarting, nil)
	m.wg.Add(1)
	go m.runListener(ctx, s)
}

// runListener es la goroutine de escucha de UNA sesión: corre el listener y, si cae (error o pánico),
// lo AÍSLA (§10.H) marcando la sesión 'degraded' y reintentando con backoff exponencial acotado, hasta
// que el ctx se cancele (apagado ordenado). La caída de una sesión NUNCA tumba el proceso ni a las
// demás (cada una vive en su goroutine con su context).
func (m *Manager) runListener(ctx context.Context, s *liveSession) {
	defer m.wg.Done()
	// Señala el fin de ESTA goroutine para que Unlink pueda unirla aislada (sin esperar al WaitGroup
	// global). Corre antes que wg.Done por orden LIFO de defers; ambos órdenes serían correctos.
	defer s.signalDone()

	backoff := &whatsmeow.Backoff{Base: m.backoffBase, Max: m.backoffMax}
	for {
		if ctx.Err() != nil {
			s.mark(HealthStopped, nil)
			return
		}

		err := m.runListenOnce(ctx, s)

		if ctx.Err() != nil {
			// Apagado ordenado: el runner retornó por cancelación; el *sql.DB ya se cerró (defer).
			s.mark(HealthStopped, nil)
			return
		}

		// Caída AISLADA: marca degradada, traza y reintenta tras el backoff (whatsmeow/manual).
		s.mark(HealthDegraded, err)
		delay := backoff.Next()
		s.log.Warn("listener caído; sesión degradada, reintentando con backoff",
			"error", err, "intento", backoff.Attempt(), "siguiente_delay", delay.String())

		select {
		case <-ctx.Done():
			s.mark(HealthStopped, nil)
			return
		case <-time.After(delay):
		}
	}
}

// runListenOnce ejecuta UN ciclo de escucha de la sesión: construye su runner+recursos (factory), abre
// el store, marca 'listening' y BLOQUEA en el runner hasta cancelación o caída. RECUPERA cualquier
// pánico del listener convirtiéndolo en error (aislamiento §10.H: un pánico en una sesión no debe
// derribar el proceso). Cierra SIEMPRE el *sql.DB de la sesión al salir (defer, §10.I), tanto en
// apagado como en caída (el reintento abre un handle fresco).
func (m *Manager) runListenOnce(ctx context.Context, s *liveSession) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pánico en el listener de la sesión: %v", r)
		}
	}()

	runner, closer, buildErr := m.newListener(ctx, s)
	if buildErr != nil {
		return fmt.Errorf("provisionar listener: %w", buildErr)
	}
	if closer != nil {
		defer func() { _ = closer.Close() }()
	}

	s.mark(HealthListening, nil)
	return runner.Run(ctx)
}
