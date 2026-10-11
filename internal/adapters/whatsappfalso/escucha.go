package whatsappfalso

import (
	"context"
	"errors"
	"sync"
	"time"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/latencia"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// ErrSinEscucha: se pidió un envío a una sesión falsa que no está escuchando. Es el hermano del «sin
// cliente vivo» del envío real: el comando se rechaza y el Ack sube como fallo.
var ErrSinEscucha = errors.New("whatsapp falso: la sesión no está escuchando; no hay a quién entregar el envío")

// retardoAcuse es lo que espera el acuse `delivered` tras un envío. En el camino real el acuse llega de la
// red DESPUÉS de que el envío devolvió y su Ack subió; emitirlo dentro del propio envío invertiría ese
// orden hacia la nube. No es una garantía de orden, es no fabricar el orden contrario.
const retardoAcuse = 50 * time.Millisecond

// colaAcuses es cuántos acuses pueden esperar su turno. Si se llena, el acuse se pierde y se avisa: el
// envío ya quedó capturado, que es lo que la prueba lee.
const colaAcuses = 256

type acusePendiente struct {
	messageID string
	listo     time.Time
}

// Escucha es la escucha de UN ciclo de UNA sesión falsa. Cumple la frontera del gestor de sesiones
// (sessionmgr.Escucha) con el mismo conjunto de métodos que el gateway real.
//
// Todo lo que NO es transporte lo delega en `puerta`, que es un *whatsmeow.ListenGateway de verdad sin
// forma de conectar: los setters, el inyector de entrantes y la correlación de acuses son, literalmente,
// los métodos de producción. Lo propio de este tipo es lo que sustituye: escuchar sin socket y enviar a un
// registro en vez de a WhatsApp.
type Escucha struct {
	puerta   *whatsmeow.ListenGateway
	registro *Registro
	log      sharedlogger.Logger

	mu        sync.Mutex
	sessionID string

	acuses chan acusePendiente
}

// NuevaEscucha construye la escucha falsa de una sesión. registro es el del daemon, compartido por todas.
func NuevaEscucha(registro *Registro, log sharedlogger.Logger) *Escucha {
	return &Escucha{
		puerta:   whatsmeow.NewListenGatewaySinTransporte(log),
		registro: registro,
		log:      log,
		acuses:   make(chan acusePendiente, colaAcuses),
	}
}

// Listen publica la puerta de entrantes y BLOQUEA hasta que el ctx se cancele. La DEK se ignora: este
// transporte no tiene almacén que abrir (la cola de entrantes sí la usa, pero la pide a la custodia por su
// cuenta, no a través de aquí).
func (e *Escucha) Listen(ctx context.Context, _ []byte) error {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		e.entregarAcuses(ctx)
	}()
	err := e.puerta.EscucharSinTransporte(ctx)
	wg.Wait()
	return err
}

func (e *Escucha) SetHealthReporter(r health.SessionReporter) { e.puerta.SetHealthReporter(r) }
func (e *Escucha) SetPushName(name string)                    { e.puerta.SetPushName(name) }
func (e *Escucha) SetInboundMargin(d time.Duration)           { e.puerta.SetInboundMargin(d) }
func (e *Escucha) SetSesionPasiva(fn func(string) bool)       { e.puerta.SetSesionPasiva(fn) }
func (e *Escucha) SetAviso(aviso chan<- struct{})             { e.puerta.SetAviso(aviso) }
func (e *Escucha) SetLatencia(h *latencia.Histograma)         { e.puerta.SetLatencia(h) }
func (e *Escucha) SetLoggedOutHandler(fn func())              { e.puerta.SetLoggedOutHandler(fn) }
func (e *Escucha) SetReceiptHandler(fn func(domain.ReceiptEvent)) {
	e.puerta.SetReceiptHandler(fn)
}

// SetCola cablea la cola en la puerta y, de paso, aprende el session_id con el que se etiqueta lo capturado.
func (e *Escucha) SetCola(c app.ColaEntrantes, sessionID string) {
	e.mu.Lock()
	e.sessionID = sessionID
	e.mu.Unlock()
	e.puerta.SetCola(c, sessionID)
}

// InyectarEntrante es el inyector del gateway real (ADR-0041), sin envolver: fabrica el evento y lo entrega
// a la puerta, con sus guardas y su doble marca.
func (e *Escucha) InyectarEntrante(ctx context.Context, p app.InyeccionEntrante) (bool, error) {
	return e.puerta.InyectarEntrante(ctx, p)
}

// ComandoDe correlaciona un acuse con el command_id de su envío, en el Correlator de la puerta.
func (e *Escucha) ComandoDe(messageIDs []string) (string, bool) {
	return e.puerta.ComandoDe(messageIDs)
}

// SendViaLiveClientTracked «envía» un texto: lo anota en el registro y devuelve un identificador sintético.
// No sale nada a WhatsApp.
func (e *Escucha) SendViaLiveClientTracked(_ context.Context, commandID, to, text string) (string, error) {
	return e.capturar(Envio{CommandID: commandID, To: to, Text: text})
}

// SendMediaViaLiveClientTracked «envía» un archivo: anota lo que la nube pidió enviar. 🔴 NO descarga la
// URL (el real sí): es una diferencia declarada del transporte falso.
func (e *Escucha) SendMediaViaLiveClientTracked(_ context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) (string, error) {
	return e.capturar(Envio{CommandID: commandID, To: to, Media: &Media{
		URL: presignedURL, Filename: filename, MIME: mime, Kind: kind, Caption: caption,
	}})
}

func (e *Escucha) capturar(envio Envio) (string, error) {
	if !e.puerta.HayEscuchaSinTransporte() {
		return "", ErrSinEscucha
	}
	id, err := idMensajeFalso()
	if err != nil {
		return "", err
	}
	ahora := time.Now()
	e.mu.Lock()
	envio.SessionID = e.sessionID
	e.mu.Unlock()
	envio.MessageID = id
	envio.At = ahora.UTC()

	e.puerta.RecordarEnvioSinTransporte(envio.CommandID, id, ahora)
	e.registro.Anotar(envio)

	select {
	case e.acuses <- acusePendiente{messageID: id, listo: ahora.Add(retardoAcuse)}:
	default:
		e.log.Warn("whatsapp falso: cola de acuses llena; el envío quedó capturado pero su acuse no sube",
			"command_id", envio.CommandID)
	}
	return id, nil
}

// entregarAcuses emite, mientras dure la escucha, el acuse `delivered` de cada envío por la puerta real.
// Solo `delivered`: nadie leyó nada, y un `read` sería inventar una lectura.
func (e *Escucha) entregarAcuses(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case a := <-e.acuses:
			if espera := time.Until(a.listo); espera > 0 {
				t := time.NewTimer(espera)
				select {
				case <-ctx.Done():
					t.Stop()
					return
				case <-t.C:
				}
			}
			if err := e.puerta.AcusarEntregaSinTransporte(ctx, a.messageID, time.Now()); err != nil {
				e.log.Warn("whatsapp falso: no se pudo entregar el acuse", "error", err)
			}
		}
	}
}
