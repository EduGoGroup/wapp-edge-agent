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
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/watchdog"
	"github.com/EduGoGroup/wapp-shared/logger"
)

// defaultDEKLoadTimeout es el plazo del watchdog de la carga de la DEK (Plan 031 T6, generaliza el WARN
// >10s del follow-up 029): en macOS, si la ACL del ítem del Keychain quedó invalidada (típico tras
// RECOMPILAR el binario), SecItemCopyMatching se BLOQUEA en una llamada cgo esperando el diálogo de permiso
// en pantalla y, sin plazo, nada lo delata (caso real 2026-07-11: 31 minutos colgado en silencio con el
// Cloud diciendo "online"). Al vencer, Run ABANDONA la espera (la cgo no es cancelable) y devuelve
// ErrDEKLoadTimeout ⇒ el runner marca la sesión DEGRADED{dek_load_timeout} y reintenta con backoff, en vez
// de colgarse para siempre. 10s da holgura al Keychain normal y hace visible el cuelgue en <15s.
const defaultDEKLoadTimeout = 10 * time.Second

// ErrDEKLoadTimeout: la carga de la DEK excedió su plazo (defaultDEKLoadTimeout) y Run abandonó la espera
// (Plan 031 T6). El runner del sessionmgr lo reconoce (errors.Is) para etiquetar la degradación con el
// motivo health.ReasonDEKLoadTimeout; NO es un fallo fatal: se reintenta con el backoff existente.
var ErrDEKLoadTimeout = errors.New("listen: la carga de la DEK excedió el plazo (posible diálogo de permiso del Keychain pendiente)")

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

	// dekLoadTimeout es el plazo del watchdog de la carga de la DEK (Plan 031 T6); 0 ⇒ defaultDEKLoadTimeout.
	dekLoadTimeout time.Duration
	// onDEKDuration reporta la duración de la carga de la DEK (éxito o retorno tardío de una carga
	// abandonada) para poblar dek_load_duration_ms en el registro de salud (T6→T7). nil ⇒ no se reporta.
	onDEKDuration func(time.Duration)
}

// ListenOption configura opciones del caso de uso Listen sin romper la firma de NewListen (variádica). En
// producción el sessionmgr inyecta el watchdog+reporte por sesión; los tests las omiten (defaults seguros).
type ListenOption func(*Listen)

// WithDEKLoadTimeout ajusta el plazo del watchdog de la carga de la DEK (Plan 031 T6). <=0 ⇒ el default.
// Los tests lo bajan para ejercitar el timeout de forma determinista y rápida.
func WithDEKLoadTimeout(d time.Duration) ListenOption {
	return func(l *Listen) {
		if d > 0 {
			l.dekLoadTimeout = d
		}
	}
}

// WithDEKDurationReporter inyecta el callback que registra la duración de la carga de la DEK (T6→T7:
// dek_load_duration_ms). nil se ignora.
func WithDEKDurationReporter(fn func(time.Duration)) ListenOption {
	return func(l *Listen) {
		if fn != nil {
			l.onDEKDuration = fn
		}
	}
}

// NewListen construye el caso de uso con los puertos dados. log traza la carga de la DEK (el llamador
// multi-sesión pasa su logger con session_id; ver sessionmgr); nil ⇒ se descarta (tests). opts inyectan el
// watchdog/reporte de salud (Plan 031 T6); sin opts, defaults seguros (plazo default, sin reporte).
func NewListen(custody KeyCustody, gateway ListenGateway, sink InboundSink, log logger.Logger, opts ...ListenOption) *Listen {
	if log == nil {
		log = logger.New(logger.WithWriter(io.Discard))
	}
	l := &Listen{custody: custody, gateway: gateway, sink: sink, log: log, dekLoadTimeout: defaultDEKLoadTimeout}
	for _, o := range opts {
		o(l)
	}
	return l
}

// reportDEKDuration publica la duración de la carga de la DEK si hay reporter cableado (nil-safe).
func (l *Listen) reportDEKDuration(d time.Duration) {
	if l.onDEKDuration != nil {
		l.onDEKDuration(d)
	}
}

// Run recupera la DEK de custodia y arranca la escucha always-on, bloqueando hasta que el ctx se
// cancele (o falle la conexión). Garantiza que la DEK en claro se borra de RAM al salir (defer zero):
// como Listen bloquea toda la sesión, la DEK permanece viva mientras el socket viva (ADR-0007).
// La DEK en sí NUNCA se loguea; solo el hecho y la duración de su carga (observabilidad).
func (l *Listen) Run(ctx context.Context) error {
	l.log.Info("custodia: cargando DEK…")
	// Watchdog "abandona y reporta" (Plan 031 T6): custody.Load() puede colgarse en una llamada cgo al
	// Keychain (macOS) que NO se puede cancelar. Al vencer el plazo abandonamos la espera (la goroutine
	// muere cuando la cgo retorne) y devolvemos ErrDEKLoadTimeout ⇒ DEGRADED + reintento con backoff, en
	// vez de quedar colgados para siempre. onLate registra la duración REAL si la carga abandonada retorna
	// tarde (p. ej. el usuario atendió el diálogo) y borra la DEK que ya no usaremos.
	res := watchdog.Guard(l.dekLoadTimeout,
		func() ([]byte, error) { return l.custody.Load() },
		func(late watchdog.Result[[]byte]) {
			if late.Err == nil {
				l.reportDEKDuration(late.Elapsed)
				zeroBytes(late.Value) // la carga abandonada retornó una DEK que ya no se usará: bórrala.
			}
			l.log.Warn("custodia: la carga de la DEK abandonada retornó tarde",
				"duracion", late.Elapsed.String(), "error", late.Err)
		})

	if res.TimedOut {
		l.log.Warn("custodia: la carga de la DEK excedió el plazo — sesión DEGRADED; posible diálogo de permiso del Keychain pendiente (típico tras recompilar el binario), requiere atención en pantalla",
			"umbral", l.dekLoadTimeout.String(), "reason", "dek_load_timeout")
		return ErrDEKLoadTimeout
	}
	if res.Err != nil {
		return fmt.Errorf("listen: cargar DEK de custodia: %w", res.Err)
	}
	dek := res.Value
	defer zeroBytes(dek)
	l.reportDEKDuration(res.Elapsed)
	l.log.Info("custodia: DEK cargada", "duracion", res.Elapsed.String())

	if err := l.gateway.Listen(ctx, dek, l.sink); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	return nil
}
