package whatsmeow

import (
	"context"
	"database/sql"
	"fmt"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
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
}

var _ app.ListenGateway = (*ListenGateway)(nil)

// NewListenGateway construye el gateway real sobre la BD propia del Edge. La BD debe estar YA migrada
// y con una sesión pareada (T3).
func NewListenGateway(db *sql.DB, log logger.Logger) *ListenGateway {
	return &ListenGateway{
		loadDevice: realLoadDevice(db),
		log:        log,
	}
}

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
// Logger silencioso de whatsmeow: no debe volcar material sensible (claves/store) a los logs.
func (g *ListenGateway) serve(ctx context.Context, device *store.Device, sink app.InboundSink) error {
	client := wm.NewClient(device, waLog.Noop)
	client.EnableAutoReconnect = true // whatsmeow reintenta el socket; el Listener traza el backoff.

	listener := NewListener(sink, g.log)
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
