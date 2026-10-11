package sessionmgr

// transporte.go — LA FRONTERA DEL TRANSPORTE DE WHATSAPP (ADR-0049).
//
// QUÉ RESUELVE. Hasta aquí el gestor de sesiones CONSTRUÍA el adaptador de whatsmeow con sus propias manos
// (`whatsmeow.NewListenGatewayForDevice` en la escucha, `whatsmeow.NewConnector` en el emparejamiento) y
// hablaba con él por métodos del tipo concreto que no estaban en ningún puerto. Enchufar otro transporte
// exigía, por tanto, editar el gestor. Ahora el gestor RECIBE el transporte ya elegido y solo conoce lo que
// declara este fichero; quién es ese transporte lo decide el arranque, una vez (internal/infra/daemon).
//
// POR QUÉ VIVE AQUÍ Y NO EN `internal/app`, que es donde están los demás puertos: la interfaz nombra
// `health.SessionReporter` y `*latencia.Histograma`, y esos dos paquetes importan `internal/app`. Declararla
// allí sería un ciclo. Es además el sitio idiomático: la interfaz la define quien la CONSUME.
//
// POR QUÉ UN STRUCT DE DOS FUNCIONES Y NO UNA INTERFAZ CON DOS MÉTODOS. Un adaptador no puede devolver un
// tipo de este paquete sin importarlo, y este paquete todavía importa el del adaptador real (por
// `whatsmeow.Backoff`, deuda anotada en documentations/deuda.md): sería otro ciclo. Con funciones, la cola
// que une cada adaptador con esta frontera se escribe FUERA de los dos, en internal/infra/wiring, que es el
// único sitio que conoce a ambos.
//
// 🔴 PENSADO PARA MÁS DE DOS. Hoy hay dos transportes (whatsmeow y el falso), pero la frontera no dice
// «real o falso»: dice «un transporte». Un tercero —otra librería, un proceso auxiliar, la API oficial— es
// otro adaptador que rellena este mismo struct y otra rama en el punto de elección del arranque; el gestor
// no se entera. Lo que SÍ habrá que hacer ese día está escrito en documentations/deuda.md: los nombres de
// los métodos de `Escucha` heredan los del adaptador de whatsmeow, y la puerta de entrantes (las guardas)
// sigue viviendo dentro de él.

import (
	"context"
	"database/sql"
	"time"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/latencia"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// Escucha es lo que el gestor usa de la escucha de UNA sesión durante UN ciclo (una reconexión = una
// Escucha nueva). Es exactamente el conjunto de métodos que el factory de escucha llamaba sobre el tipo
// concreto del adaptador de whatsmeow, ni uno más: ninguno lleva tipos de la librería.
type Escucha interface {
	// Listen conecta y BLOQUEA manteniendo la sesión viva hasta que el ctx se cancele (app.ListenGateway).
	app.ListenGateway

	// Lo que el daemon comparte y se inyecta POR SESIÓN antes de escuchar.
	SetHealthReporter(r health.SessionReporter)
	SetPushName(name string)
	SetInboundMargin(d time.Duration)
	SetCola(c app.ColaEntrantes, sessionID string)
	SetSesionPasiva(fn func(sessionID string) bool)
	SetAviso(aviso chan<- struct{})
	SetLatencia(h *latencia.Histograma)

	// InyectarEntrante mete un entrante sintético por la puerta real de la escucha (ADR-0041). Sin escucha
	// publicada devuelve un error que envuelve app.ErrSinEscuchaViva.
	InyectarEntrante(ctx context.Context, p app.InyeccionEntrante) (acusar bool, err error)

	// Los dos envíos por la sesión viva. Devuelven el identificador del mensaje enviado.
	SendViaLiveClientTracked(ctx context.Context, commandID, to, text string) (string, error)
	SendMediaViaLiveClientTracked(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) (string, error)

	// Los acuses de lo enviado: el manejador que los recibe y la correlación con el command_id del envío.
	SetReceiptHandler(fn func(domain.ReceiptEvent))
	ComandoDe(messageIDs []string) (commandID string, ok bool)

	// SetLoggedOutHandler cablea el aviso de que el otro extremo CERRÓ la sesión.
	SetLoggedOutHandler(fn func())
}

// Transporte es la fábrica del transporte de WhatsApp de este Edge. Lo construye el arranque y lo reciben
// WithEscucha y WithEmparejamiento; el gestor nunca construye un adaptador concreto.
type Transporte struct {
	// NuevoConector construye el conector de UN emparejamiento sobre la BD única compartida.
	NuevoConector func(storeDB *sql.DB, dialect string, log sharedlogger.Logger) app.Connector
	// NuevaEscucha construye la escucha de UN ciclo de UNA sesión, identificada por su JID.
	NuevaEscucha func(db *sql.DB, dialect, jid string, log sharedlogger.Logger) Escucha
}
