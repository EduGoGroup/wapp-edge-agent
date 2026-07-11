package whatsmeow

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cryptostore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-shared/logger"
)

// ListenGateway implementa app.ListenGateway sobre whatsmeow ALWAYS-ON (RF-5/RF-6, design §5).
//
// A diferencia del Sender (cliente efímero por envío), aquí el cliente se mantiene CONECTADO de forma
// continua: carga el device pareado (store cifrado con la DEK), registra el Listener (eventos ->
// sink) y BLOQUEA hasta que el ctx se cancele, momento en que desconecta limpio. whatsmeow gestiona
// el auto-reconnect del socket (EnableAutoReconnect, por defecto activo); el Listener observa los
// eventos de conexión y traza la política de backoff (ver backoff.go).
//
// El ciclo real (Connect + bloqueo del socket vivo) NO se cubre en tests por diseño (requiere red y
// un teléfono pareado): la lógica testeable vive en el Listener (routing) y el backoff. loadDevice
// se inyecta para no acoplar a la BD en tests del cableado.
type ListenGateway struct {
	loadDevice loadDeviceFunc
	log        logger.Logger

	// mu protege SOLO el puntero `client` (no las llamadas a whatsmeow, que son seguras concurrentes).
	mu sync.RWMutex
	// client es el cliente VIVO de la escucha mientras serve() bloquea; nil fuera de una sesión Listen.
	// Permite que el envío reutilice la MISMA conexión que recibe (una sola conexión por sesión): con
	// la misma identidad multi-dispositivo, un cliente efímero aparte REEMPLAZARÍA esta conexión y
	// dejaría el socket de escucha sordo. El device ya está autenticado: enviar NO requiere la DEK.
	client *wm.Client

	// correlator ata command_id ↔ MessageID por sesión (§10.E): lo alimenta el envío por cliente vivo
	// (SendViaLiveClientTracked) al capturar el SendResponse, y lo consulta el CloudLink en T2 al llegar
	// un events.Receipt. Uno por gateway ⇒ la correlación es por sesión (ADR-0008).
	correlator *Correlator

	// onReceipt es el destino de los acuses (delivered/read) de esta sesión. El CloudLink lo cablea en
	// T2 (con SetReceiptHandler) para subir el estado a la nube; nil = se ignora. serve() lo pasa al
	// Listener al arrancar el ciclo.
	onReceipt func(domain.ReceiptEvent)

	// onLoggedOut se invoca cuando WhatsApp CIERRA la sesión (events.LoggedOut, Plan 020 T3). El CloudLink lo
	// cablea (con SetLoggedOutHandler) para propagar el estado ZOMBIE a la nube ANTES del teardown local;
	// nil = se ignora. serve() lo pasa al Listener al arrancar el ciclo. No lleva PII.
	onLoggedOut func()

	// pushName es el nombre visible de FALLBACK para anunciar presencia (§10.D). whatsmeow rechaza
	// SendPresence si Store.PushName está vacío (el store recién restaurado, MP-02, no lo trae); sin
	// presencia available WhatsApp no propaga los acuses. serve() lo aplica a Store.PushName SOLO si el
	// store no conoce ya el nombre REAL de la cuenta (app-state), que siempre prevalece. Vacío = no se
	// fuerza (si el store tampoco lo trae, SendPresence falla best-effort y la escucha sigue). Sin PII.
	pushName string
}

var _ app.ListenGateway = (*ListenGateway)(nil)
var _ app.LiveLogout = (*ListenGateway)(nil)

// newListenGateway es el constructor común: recibe el loader de device ya parametrizado (single-sesión
// vs. per-device) y arma el gateway con su Correlator por sesión (§10.E).
func newListenGateway(loadDevice loadDeviceFunc, log logger.Logger) *ListenGateway {
	return &ListenGateway{
		loadDevice: loadDevice,
		log:        log,
		correlator: NewCorrelator(0, 0), // tope/TTL por defecto (§10.E).
	}
}

// NewListenGateway construye el gateway real SINGLE-SESIÓN (legacy runRestore/listen) sobre la BD del
// Edge en SQLite: resuelve el ÚNICO device pareado (FirstDeviceJID). La BD debe estar YA migrada y con
// una sesión pareada. El daemon MULTI-device (serve) usa NewListenGatewayForDevice.
func NewListenGateway(db *sql.DB, log logger.Logger) *ListenGateway {
	return newListenGateway(realLoadDevice(db, cryptostore.DialectSQLite), log)
}

// NewListenGatewayForDevice construye el gateway real per-device sobre la BD ÚNICA COMPARTIDA (Plan 022
// T3): carga el device CONCRETO por SU jid (devices.jid) con SU DEK y el dialecto de config. Es el que
// cablea el Manager por cada sesión activa al restaurar/parear (N devices, 1 *sql.DB compartida).
func NewListenGatewayForDevice(db *sql.DB, dialect, jid string, log logger.Logger) *ListenGateway {
	return newListenGateway(realLoadDeviceByJID(db, dialect, jid), log)
}

// Correlator expone el mapa de correlación command_id ↔ MessageID de esta sesión (uno por gateway ⇒
// por sesión). El CloudLink lo consulta en T2: al recibir un events.Receipt, Lookup(MessageIDs) →
// command_id para etiquetar el acuse antes de subirlo.
func (g *ListenGateway) Correlator() *Correlator { return g.correlator }

// SetReceiptHandler cablea el destino de los acuses (delivered/read) de esta sesión. Se llama ANTES de
// Listen (al construir el gateway). El CloudLink lo usa en T2 para subir el estado; en T0 queda nil
// (los acuses se mapean y se descartan silenciosamente hasta que T2 lo conecte).
func (g *ListenGateway) SetReceiptHandler(fn func(domain.ReceiptEvent)) { g.onReceipt = fn }

// SetLoggedOutHandler cablea el destino del cierre de sesión (events.LoggedOut) de esta sesión (Plan 020
// T3). Se llama ANTES de Listen (al construir el gateway). El CloudLink lo usa para propagar el estado
// ZOMBIE a la nube; nil = solo se loguea (comportamiento previo). No lleva PII.
func (g *ListenGateway) SetLoggedOutHandler(fn func()) { g.onLoggedOut = fn }

// SetPushName cablea el nombre visible de FALLBACK usado al anunciar presencia (§10.D) cuando el store
// restaurado aún no conoce el nombre real de la cuenta. Se llama ANTES de Listen (al construir el
// gateway). Vacío = no se fuerza ningún nombre (SendPresence quedará best-effort si el store tampoco lo
// trae). No es secreto ni PII; no se loguea.
func (g *ListenGateway) SetPushName(name string) { g.pushName = name }

// Listen carga el device pareado, conecta el cliente always-on, registra el Listener (eventos ->
// sink) y BLOQUEA manteniendo el socket vivo hasta que ctx se cancele. Devuelve nil al cancelarse
// limpio, o error si la carga del device o la conexión inicial fallan. La DEK solo se usa para
// descifrar el store; no se loguea.
func (g *ListenGateway) Listen(ctx context.Context, dek []byte, sink app.InboundSink) error {
	device, err := g.loadDevice(ctx, dek)
	if err != nil {
		return err
	}
	return g.serve(ctx, device, sink)
}

// serve cablea el Listener al cliente real y mantiene el socket vivo hasta la cancelación del ctx.
// whatsmeow loguea por el puente waLog→slog (walog.go): un fallo/caída del websocket YA queda trazado
// en el log del agente (antes waLog.Noop lo silenciaba); el nivel lo gobierna el logger del agente.
func (g *ListenGateway) serve(ctx context.Context, device *store.Device, sink app.InboundSink) error {
	client := wm.NewClient(device, newWALog(g.log))
	client.EnableAutoReconnect = true // whatsmeow reintenta el socket; el Listener traza el backoff.

	// Publica el cliente VIVO para que el envío reutilice esta misma conexión, y lo limpia al salir.
	g.setLiveClient(client)
	defer g.setLiveClient(nil)

	listener := NewListener(sink, g.log)
	// Acuses (delivered/read) → destino de la sesión (nil en T0; T2 lo cablea con SetReceiptHandler).
	listener.onReceipt = g.onReceipt
	// Cierre de sesión (LoggedOut) → destino de la sesión (Plan 020 T3): propaga el estado ZOMBIE a la nube.
	listener.onLoggedOutHook = g.onLoggedOut
	// §10.D: tras cada Connected anuncia presencia (available) sobre el cliente vivo para que WhatsApp
	// PROPAGUE los acuses de entrega/lectura al companion. Best-effort: un fallo no tumba la escucha.
	//
	// PushName (hallazgo e2e Plan 013): whatsmeow rechaza SendPresence si Store.PushName está vacío
	// ("can't send presence without PushName set"), y el store restaurado (MP-02) NO lo trae poblado.
	// PREFERIMOS el nombre REAL: si whatsmeow ya lo aprendió (app-state), lo respetamos; solo si sigue
	// vacío aplicamos el fallback configurado (g.pushName) en memoria (no se persiste: si el nombre real
	// llega luego por app-state, whatsmeow lo sobreescribe). Sin fallback y sin nombre real, SendPresence
	// fallará y quedará en WARN best-effort (la escucha no se rompe).
	listener.onConnect = func() {
		if client.Store != nil {
			if name, set := resolvePushName(client.Store.PushName, g.pushName); set {
				client.Store.PushName = name
			}
		}
		if err := client.SendPresence(ctx, types.PresenceAvailable); err != nil {
			g.log.Warn("listener: no se pudo anunciar presencia (available)", "error", err)
		}
	}
	listener.Register(ctx, client)

	if err := client.Connect(); err != nil {
		return fmt.Errorf("whatsapp: conectar (listen): %w", err)
	}
	defer client.Disconnect()

	g.log.Info("escucha 24/7 iniciada: socket always-on (Ctrl-C para detener)")

	// Bloquea manteniendo el socket VIVO hasta que el caso de uso cancele el ctx (SIGINT). La DEK
	// vive en RAM durante toda esta espera (ADR-0007).
	<-ctx.Done()
	g.log.Info("escucha 24/7 detenida: cancelación recibida, cerrando socket")
	return nil
}

// resolvePushName decide qué nombre visible fijar en el store ANTES de anunciar presencia (§10.D).
// PREFIERE el nombre real ya conocido por whatsmeow (current, aprendido del app-state): si está poblado
// se respeta y NO se toca (set=false). Solo si el store aún no lo trae (store recién restaurado, MP-02)
// se usa el fallback configurado (set=true). Sin fallback tampoco se fuerza nada (SendPresence quedará
// best-effort). Puro: sin efectos, testeable sin cliente ni red.
func resolvePushName(current, fallback string) (name string, set bool) {
	if current != "" {
		return current, false
	}
	if fallback != "" {
		return fallback, true
	}
	return "", false
}

// setLiveClient publica (o limpia con nil) el cliente VIVO de la escucha bajo lock de escritura.
func (g *ListenGateway) setLiveClient(client *wm.Client) {
	g.mu.Lock()
	g.client = client
	g.mu.Unlock()
}

// LogoutLiveClient implementa app.LiveLogout: hace un LOGOUT REMOTO best-effort sobre el cliente VIVO de
// la escucha (que WhatsApp suelte el dispositivo vinculado), reutilizando la conexión ya autenticada (no
// requiere la DEK). Es el paso opcional de la desvinculación (app.UnlinkSession): si tiene éxito, el
// teléfono ve el dispositivo desvinculado; si no, la limpieza LOCAL continúa igual.
//
// Devuelve app.ErrNoLiveClient (no fatal) si no hay escucha activa (cliente nil / sin device cargado) o
// si el cliente vivo es de OTRA sesión (su JID no coincide): en single-sesión coincide, pero se comprueba
// para ser forward-compatible (MP-01). client.Logout envía remove-companion-device, desconecta y borra su
// device del store; el borrado del resto del material local lo completa cryptostore.DeleteDevice
// (idempotente con lo que Logout ya haya borrado).
func (g *ListenGateway) LogoutLiveClient(ctx context.Context, jid string) error {
	g.mu.RLock()
	client := g.client
	g.mu.RUnlock()
	if client == nil || client.Store == nil || client.Store.ID == nil {
		return app.ErrNoLiveClient
	}
	if client.Store.ID.String() != jid {
		return app.ErrNoLiveClient
	}
	if err := client.Logout(ctx); err != nil {
		return fmt.Errorf("whatsapp: logout remoto del cliente vivo: %w", err)
	}
	return nil
}

// SendViaLiveClient envía un texto REUTILIZANDO el cliente whatsmeow VIVO de la escucha (RF-4 sobre la
// conexión always-on): una sola conexión por sesión, sin abrir un socket efímero que dejaría sorda la
// escucha. Falla con error claro si no hay sesión de escucha activa (cliente nil). El cliente vivo ya
// está autenticado (device cargado al arrancar): NO necesita la DEK. client.SendMessage es seguro para
// uso concurrente; el RWMutex solo protege la lectura del puntero.
func (g *ListenGateway) SendViaLiveClient(ctx context.Context, to, text string) error {
	_, err := g.sendViaLiveClient(ctx, "", to, text)
	return err
}

// SendViaLiveClientTracked envía por el cliente vivo CORRELACIONANDO el envío con su command_id: captura
// el SendResponse (el MessageID que WhatsApp asigna) y lo registra en el Correlator, de modo que al
// llegar el events.Receipt de ese mensaje se pueda etiquetar el acuse con el command_id original (§10.E).
// Devuelve el MessageID del envío. Lo usará el CloudLink en T2 (hoy el mux envía por SendViaLiveClient
// sin command_id; T2 conmuta el mux a esta variante para alimentar la correlación).
func (g *ListenGateway) SendViaLiveClientTracked(ctx context.Context, commandID, to, text string) (string, error) {
	return g.sendViaLiveClient(ctx, commandID, to, text)
}

// sendViaLiveClient es el envío REAL por el cliente vivo (núcleo común). A diferencia del camino
// anterior, deja de DESCARTAR el SendResponse (§10.E): captura resp.ID/resp.Timestamp y, si hay
// command_id, alimenta el Correlator. Devuelve el MessageID (resp.ID) del envío. Falla con error claro
// si no hay sesión de escucha activa (cliente nil). client.SendMessage es seguro para uso concurrente;
// el RWMutex solo protege la lectura del puntero al cliente vivo.
func (g *ListenGateway) sendViaLiveClient(ctx context.Context, commandID, to, text string) (string, error) {
	g.mu.RLock()
	client := g.client
	g.mu.RUnlock()
	if client == nil {
		return "", fmt.Errorf("whatsapp: sin cliente vivo de escucha para enviar (¿está corriendo `restore`/`listen`?)")
	}

	toJID, err := parseRecipient(to)
	if err != nil {
		return "", err
	}
	resp, err := client.SendMessage(ctx, toJID, buildMessage(outgoing{to: toJID, text: text}))
	if err != nil {
		return "", fmt.Errorf("whatsapp: enviar por cliente vivo: %w", err)
	}
	// §10.E: correlación local command_id → MessageID (resp.ID, no ServerID: ver nota en correlation.go).
	// Sin command_id (camino legacy del mux hoy) no se correlaciona; el acuse subirá como estado crudo.
	if commandID != "" && g.correlator != nil {
		g.correlator.Remember(commandID, resp.ID, resp.Timestamp)
	}
	return resp.ID, nil
}

// SendMediaViaLiveClientTracked envía un ARCHIVO (documento/imagen) REUTILIZANDO el cliente whatsmeow
// VIVO de la escucha (Plan 017 §7), hermano de SendViaLiveClientTracked para media: DESCARGA el binario
// de la presigned URL con net/http normal (SIN credenciales, SIN SDK S3 — la URL prefirmada ES la
// capability, zero-knowledge ADR-0007), lo SUBE por el cliente vivo (client.Upload, que lo cifra con su
// MediaKey para el destinatario) y despacha el Document/Image con el caption embebido (§9.I). Correlaciona
// el envío con su command_id (§10.E) para que el acuse posterior (Plan 013) suba etiquetado. Devuelve el
// MessageID del envío. Falla con error claro si no hay sesión de escucha activa (cliente nil). El cliente
// vivo ya está autenticado: NO necesita la DEK (el media no se cifra con ella).
func (g *ListenGateway) SendMediaViaLiveClientTracked(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) (string, error) {
	g.mu.RLock()
	client := g.client
	g.mu.RUnlock()
	if client == nil {
		return "", fmt.Errorf("whatsapp: sin cliente vivo de escucha para enviar media (¿está corriendo `restore`/`listen`?)")
	}

	toJID, err := parseRecipient(to)
	if err != nil {
		return "", err
	}
	data, err := downloadMedia(ctx, presignedURL)
	if err != nil {
		return "", err
	}
	waMsg, err := buildMediaMessage(ctx, client, outgoing{
		mediaData: data, filename: filename, mime: mime, kind: kind, caption: caption,
	})
	if err != nil {
		return "", err
	}
	resp, err := client.SendMessage(ctx, toJID, waMsg)
	if err != nil {
		return "", fmt.Errorf("whatsapp: enviar media por cliente vivo: %w", err)
	}
	if commandID != "" && g.correlator != nil {
		g.correlator.Remember(commandID, resp.ID, resp.Timestamp)
	}
	return resp.ID, nil
}
