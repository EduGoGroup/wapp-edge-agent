// Package whatsmeow es el adaptador hacia whatsmeow (cliente de WhatsApp Web) PODADO para el
// emparejamiento por QR (RF-1): crea el store CIFRADO con la DEK del pairing, abre el canal de QR y
// conecta. NO registra handlers de mensajes entrantes ni Listen: solo lo imprescindible para el
// pairing (QR + *events.Connected). La escucha 24/7 se construye aparte (listener.go, T5).
//
// Copia-adaptación de edugo-api-messaging/internal/infra/whatsapp/connector.go (ADR-0004):
// renombrado al namespace wApp, puerto -> internal/app, cryptostore -> wApp. CAMBIO de contrato:
// el QR se emite como string CRUDO (no PNG data-URL como EduGo) porque el control del Edge lo pinta
// en TERMINAL con qrterminal (no hay <img> ni relay SSE; design §7, ADR-0003).
package whatsmeow

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cryptostore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// containerFactory construye el store.DeviceContainer CIFRADO con una DEK dada. Se inyecta para
// poder testear el Connector con un container fake sin BD real; en producción es
// cryptostore.NewEncryptedContainer sobre la BD propia del Edge.
type containerFactory func(ctx context.Context, dek []byte) (store.DeviceContainer, error)

// defaultActivationTimeout acota cuánto espera run() el *events.Connected de la RECONEXIÓN
// post-pairing tras un "success" del qrChan. whatsmeow, al completar el pairing, se desconecta (el
// server cierra el socket) y AUTO-reconecta con la sesión nueva; recién esa reconexión emite
// *events.Connected (señal que el caso de uso usa para sellar la DEK). Si desconectáramos al
// cerrarse el qrChan, cortaríamos esa reconexión y el dispositivo nunca se activaría. 25s da
// holgura para el reintento de whatsmeow sin colgar la goroutine indefinidamente.
const defaultActivationTimeout = 25 * time.Second

// Connector implementa app.Connector sobre whatsmeow PODADO.
//
// Cada llamada a StartConnection construye un container cifrado nuevo (con la DEK del pairing), un
// device fresco ligado a ese container y un cliente whatsmeow efímero. El store se persiste CIFRADO
// porque el device.Container es el decorator de cryptostore.
type Connector struct {
	newContainer containerFactory
	// signalBuffer es la capacidad del canal de señales (holgura para QR + connected sin bloquear).
	signalBuffer int
	// activationTimeout acota la espera del *events.Connected de la reconexión post-pairing.
	activationTimeout time.Duration
}

var _ app.Connector = (*Connector)(nil)

// NewConnector construye el conector real sobre la BD propia del Edge. La BD debe estar YA migrada
// (tablas whatsmeow_* y msg_enc_*; ver internal/infra/db). Cada pairing crea su propio container
// cifrado sobre esta misma conexión.
func NewConnector(db *sql.DB) *Connector {
	return &Connector{
		newContainer: func(ctx context.Context, dek []byte) (store.DeviceContainer, error) {
			// Store del Edge = SQLite embebido (ADR-0002): se pasa el dialecto explícito en vez del
			// "sqlite" que antes hardcodeaba el cryptostore (Plan 022 T0).
			return cryptostore.NewEncryptedContainer(ctx, db, cryptostore.DialectSQLite, dek)
		},
		signalBuffer:      8,
		activationTimeout: defaultActivationTimeout,
	}
}

// newConnectorWithFactory construye un Connector con una fábrica de container inyectada (tests).
func newConnectorWithFactory(f containerFactory) *Connector {
	return &Connector{newContainer: f, signalBuffer: 8, activationTimeout: defaultActivationTimeout}
}

// StartConnection arranca un pairing nuevo. Devuelve un canal de señales (QR…, luego Connected o
// Error) que se cierra al terminar. El ctx controla la cancelación: al cancelarlo (timeout o aborto
// del caso de uso) la goroutine desconecta el cliente y cierra el canal.
//
// dek se usa SOLO para construir el container cifrado; no se loguea ni se retiene aquí.
func (c *Connector) StartConnection(ctx context.Context, dek []byte) (<-chan app.PairingSignal, error) {
	container, err := c.newContainer(ctx, dek)
	if err != nil {
		return nil, fmt.Errorf("whatsmeow: construir store cifrado: %w", err)
	}

	// Device fresco cuyo Container es el decorator cifrado: al completar el pairing, whatsmeow llama
	// Device.Save -> PutDevice (cifrado). Logger silencioso (sin volcar material sensible).
	device := cryptostore.NewDeviceForPairing(container)
	client := wm.NewClient(device, waLog.Noop)

	qrChan, err := client.GetQRChannel(ctx)
	if err != nil {
		return nil, fmt.Errorf("whatsmeow: abrir canal de QR: %w", err)
	}

	signals := make(chan app.PairingSignal, c.signalBuffer)

	// connectedCh señaliza a run() que llegó el *events.Connected de la reconexión post-pairing
	// (device activado): run() lo espera tras un "success" del qrChan ANTES de desconectar, para no
	// cortar esa reconexión. Se cierra UNA sola vez (connectedOnce) aunque haya reconexiones.
	connectedCh := make(chan struct{})
	var connectedOnce sync.Once
	client.AddEventHandler(func(evt interface{}) {
		if _, ok := evt.(*events.Connected); ok && client.Store.ID != nil {
			connectedOnce.Do(func() {
				jid := client.Store.ID.String()
				c.emit(ctx, signals, app.PairingSignal{
					Type:  app.PairingSignalConnected,
					WaJID: jid,
				})
				close(connectedCh) // libera la espera de activación en run().
			})
		}
	})

	go c.run(ctx, client, qrChan, signals, connectedCh)
	return signals, nil
}

// run conduce el ciclo de vida del pairing: bombea QRs al canal de señales, conecta, y al terminar
// (canal de QR cerrado, ctx cancelado o desconexión) desconecta el cliente y cierra el canal. El
// *events.Connected lo emite el handler registrado en StartConnection.
//
// CLAVE post-pairing: cuando el qrChan se cierra TRAS un "success", whatsmeow se desconecta y
// AUTO-reconecta con la sesión nueva; esa reconexión emite *events.Connected y ACTIVA el dispositivo
// del lado de WhatsApp. Por eso, si hubo "success", run() NO desconecta al cerrarse el qrChan:
// espera el *events.Connected (vía connectedCh) o un timeout/ctx antes de soltar el deferred
// Disconnect. Sin esto, el Disconnect prematuro cortaba la reconexión y el teléfono mostraba "no se
// pudo vincular" (la DEK nunca se sellaba).
func (c *Connector) run(
	ctx context.Context,
	client *wm.Client,
	qrChan <-chan wm.QRChannelItem,
	signals chan app.PairingSignal,
	connectedCh <-chan struct{},
) {
	defer close(signals)
	defer client.Disconnect()

	if err := client.Connect(); err != nil {
		c.emit(ctx, signals, app.PairingSignal{
			Type: app.PairingSignalError,
			Err:  fmt.Errorf("conectar a WhatsApp: %w", err),
		})
		return
	}

	// paired marca que el qrChan reportó "success": al cerrarse el canal hay que esperar la
	// reconexión (Connected) en vez de desconectar de inmediato.
	paired := false

	for {
		select {
		case <-ctx.Done():
			// Cancelación (timeout/aborto): el deferred Disconnect + close liberan recursos.
			return
		case item, ok := <-qrChan:
			if !ok {
				// Canal de QR cerrado por whatsmeow: pairing terminó. Si fue ÉXITO, esperamos el
				// *events.Connected de la reconexión post-pairing antes de desconectar (lo emite el
				// handler y cierra connectedCh). Si NO fue éxito, salimos ya.
				if paired {
					c.waitActivation(ctx, connectedCh)
				}
				return
			}
			switch item.Event {
			case wm.QRChannelEventCode:
				// QR CRUDO (no PNG): el control lo pinta en terminal con qrterminal.
				c.emit(ctx, signals, app.PairingSignal{Type: app.PairingSignalQR, QR: item.Code})
			case "success":
				// Pairing OK a nivel de canal de QR; el JID/Connected lo entrega el handler tras la
				// reconexión. Marcamos paired para esperar esa reconexión cuando se cierre el qrChan.
				paired = true
			case wm.QRChannelEventError:
				c.emit(ctx, signals, app.PairingSignal{
					Type: app.PairingSignalError,
					Err:  fmt.Errorf("pairing falló: %w", item.Error),
				})
				return
			default:
				// timeout / err-unexpected-state / err-client-outdated / scanned-without-multidevice.
				c.emit(ctx, signals, app.PairingSignal{
					Type: app.PairingSignalError,
					Err:  fmt.Errorf("evento de pairing inesperado: %s", item.Event),
				})
				return
			}
		}
	}
}

// waitActivation espera el *events.Connected de la reconexión post-pairing (connectedCh cerrado por
// el handler) antes de que run() suelte el deferred Disconnect. Acota la espera con
// activationTimeout y respeta ctx.Done() (timeout/aborto del caso de uso). Si connectedCh ya está
// cerrado retorna de inmediato.
func (c *Connector) waitActivation(ctx context.Context, connectedCh <-chan struct{}) {
	select {
	case <-connectedCh:
		// Reconexión completada: el handler ya emitió PairingSignalConnected. Disconnect seguro.
	case <-time.After(c.activationTimeout):
		// La reconexión no llegó a tiempo: desconectamos igual para no colgar la goroutine.
	case <-ctx.Done():
		// Timeout/aborto del caso de uso: liberar recursos.
	}
}

// emit envía una señal sin bloquear indefinidamente: si el consumidor desapareció (ctx cancelado)
// la descarta en vez de colgar la goroutine del pairing.
func (c *Connector) emit(ctx context.Context, signals chan app.PairingSignal, sig app.PairingSignal) {
	select {
	case signals <- sig:
	case <-ctx.Done():
	}
}
