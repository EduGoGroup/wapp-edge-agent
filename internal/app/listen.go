package app

// Listen es el caso de uso de ESCUCHA 24/7 (always-on) del Edge (RF-5/RF-6, design §5).
//
// A diferencia de Send (ciclo efímero connect-send-disconnect), Listen mantiene el socket de
// WhatsApp VIVO de forma continua: arranca la conexión, registra el listener que enruta los mensajes
// entrantes hacia el sink y BLOQUEA hasta que el ctx se cancele (Ctrl-C / SIGINT en el daemon).
//
// Orquesta el flujo DESACOPLADO por interfaces para ser testeable sin teléfono ni red:
//   - recupera la DEK del puerto KeyCustody (la misma sellada en el pairing, zero-knowledge);
//   - delega la conexión+escucha al puerto ListenGateway, que en producción descifra el store con la
//     DEK, carga el device pareado, conecta el cliente whatsmeow always-on y registra el listener.
//
// Invariante de seguridad (ADR-0007): la DEK en claro vive SOLO en RAM y, en modo always-on,
// MIENTRAS el socket viva. ListenGateway.Listen bloquea hasta cancelación, así que la DEK permanece
// en scope durante toda la sesión y se borra (zero) al salir, pase lo que pase. NUNCA se loguea.

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-shared/logger"
)

// dekLoadWarnAfter es el umbral del watchdog de la carga de la DEK (observabilidad, follow-up Plan 029
// Ola 3): en macOS, si la ACL del ítem del Keychain quedó invalidada (típico tras RECOMPILAR el binario),
// SecItemCopyMatching se BLOQUEA esperando el diálogo de permiso en pantalla y, sin este aviso, nada lo
// delata en el log (caso real: 31 minutos colgado en silencio).
const dekLoadWarnAfter = 10 * time.Second

// InboundSink es el puerto de SALIDA de los mensajes entrantes ya descifrados (domain.InboundEvent).
// En la Fase 1 lo implementa el cliente CloudLink (reenvío a la nube por gRPC bidi-stream); en el
// spike lo implementa un adaptador de LOG (CloudLink stub, design §8). Abstrae el destino para que
// ni el caso de uso ni el listener conozcan a dónde va el evento.
type InboundSink interface {
	// Deliver entrega un evento entrante al destino. Devuelve error si no pudo entregarlo (el
	// listener lo registra y sigue escuchando: una entrega fallida no debe tumbar el socket).
	Deliver(ctx context.Context, evt domain.InboundEvent) error
}

// ListenGateway ABSTRAE la conexión always-on + escucha sobre whatsmeow para el caso de uso. La
// implementación real (internal/adapters/whatsmeow) construye el store CIFRADO con la DEK, carga el
// device pareado, conecta el cliente, registra el listener que enruta los *events.Message hacia el
// sink y mantiene el socket VIVO hasta que el ctx se cancele. Un fake en los tests emite eventos al
// sink y respeta la cancelación sin red.
//
// Contrato de seguridad: la DEK (32 bytes) entra SOLO para descifrar el store y NUNCA se persiste ni
// se loguea.
type ListenGateway interface {
	// Listen conecta, registra el listener (eventos -> sink) y BLOQUEA manteniendo el socket vivo
	// hasta que ctx se cancele. Devuelve nil al cancelarse limpio, o error si la conexión inicial
	// (o la carga del device) falla.
	Listen(ctx context.Context, dek []byte, sink InboundSink) error
}

// LiveSender es el puerto de ENVÍO sobre el cliente VIVO de la escucha always-on: reutiliza la MISMA
// conexión que recibe (una sola conexión por sesión) en lugar de abrir un socket efímero que, con la
// misma identidad multi-dispositivo, reemplazaría la conexión y dejaría la escucha sorda. El cliente
// vivo ya está autenticado: NO requiere la DEK. Lo satisface *whatsmeow.ListenGateway (el adaptador).
type LiveSender interface {
	// SendViaLiveClient despacha un texto a `to` (número crudo o JID) por el cliente vivo. Devuelve
	// error si no hay sesión de escucha activa (sin cliente vivo) o si el envío falla.
	SendViaLiveClient(ctx context.Context, to, text string) error
	// SendViaLiveClientTracked despacha como SendViaLiveClient pero CORRELACIONA el envío con su
	// command_id (Plan 013 §10.E): puebla el Correlator (command_id ↔ MessageID del SendResponse) para
	// que, al llegar el events.Receipt, el acuse se etiquete con el command_id original. Devuelve el
	// MessageID del envío. Es el camino que alimenta la subida de acuses correlacionados (T2a).
	SendViaLiveClientTracked(ctx context.Context, commandID, to, text string) (string, error)
	// SendMediaViaLiveClientTracked despacha un ARCHIVO (documento/imagen) por el cliente vivo (Plan 017
	// §7): DESCARGA el binario de la presigned URL (GET sin credenciales), lo sube a WhatsApp y despacha
	// el Document/Image con el caption embebido. Correlaciona con el command_id igual que el de texto.
	// Devuelve el MessageID del envío.
	SendMediaViaLiveClientTracked(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) (string, error)
}

// Listen es el caso de uso. Sus dependencias son puertos (interfaces) para inyectar fakes en tests.
type Listen struct {
	custody KeyCustody
	gateway ListenGateway
	sink    InboundSink
	log     logger.Logger
}

// NewListen construye el caso de uso con los puertos dados. log traza la carga de la DEK (el llamador
// multi-sesión pasa su logger con session_id; ver sessionmgr); nil ⇒ se descarta (tests).
func NewListen(custody KeyCustody, gateway ListenGateway, sink InboundSink, log logger.Logger) *Listen {
	if log == nil {
		log = logger.New(logger.WithWriter(io.Discard))
	}
	return &Listen{custody: custody, gateway: gateway, sink: sink, log: log}
}

// Run recupera la DEK de custodia y arranca la escucha always-on, bloqueando hasta que el ctx se
// cancele (o falle la conexión). Garantiza que la DEK en claro se borra de RAM al salir (defer zero):
// como Listen bloquea toda la sesión, la DEK permanece viva mientras el socket viva (ADR-0007).
// La DEK en sí NUNCA se loguea; solo el hecho y la duración de su carga (observabilidad).
func (l *Listen) Run(ctx context.Context) error {
	l.log.Info("custodia: cargando DEK…")
	start := time.Now()
	// Watchdog barato: si la carga se cuelga (>10s), avisa UNA vez con la causa probable. time.AfterFunc
	// no fuga goroutines: si no dispara, Stop lo cancela; si dispara, su goroutine termina al loguear.
	watchdog := time.AfterFunc(dekLoadWarnAfter, func() {
		l.log.Warn("custodia: la carga de la DEK sigue pendiente — posible diálogo de permiso del Keychain pendiente; requiere atención en pantalla (típico tras recompilar el binario)",
			"umbral", dekLoadWarnAfter.String())
	})
	dek, err := l.custody.Load()
	watchdog.Stop()
	if err != nil {
		return fmt.Errorf("listen: cargar DEK de custodia: %w", err)
	}
	defer zeroBytes(dek)
	l.log.Info("custodia: DEK cargada", "duracion", time.Since(start).String())

	if err := l.gateway.Listen(ctx, dek, l.sink); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	return nil
}
