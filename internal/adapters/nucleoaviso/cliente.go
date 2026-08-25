// Package nucleoaviso es el LADO CLIENTE de la señal cajero→núcleo (Plan 044 · Ola 1.8 · T1.8-5,
// D-044.43): el worker-cajero le dice al daemon de una instalación que su socket de inferencia ya sirve
// («listo») o que va a dejar de servir («caído»), hablando HTTP/1.1 sobre el Unix socket del plano de
// control /v1 que el núcleo ya expone (ADR-0015).
//
// ES LA DIRECCIÓN CONTRARIA A inferenciacliente, Y SON DOS CANALES DISTINTOS. Aquel es daemon→cajero
// por `<data_dir>/cajero.sock` y lleva TRABAJO (el prompt). Este es cajero→daemon por el socket del
// plano de control y lleva UN ENUM. No comparten socket, ni sentido, ni contenido, y confundirlos sería
// meter la señal de vida en el mismo canal que se cae cuando el cajero muere — que es exactamente lo
// que hace inútil a un latido.
//
// EL DUEÑO DEL CONTRATO ES EL SERVIDOR: los tipos del cable (server.ReadinessRequest, la ruta y los dos
// valores) se IMPORTAN de internal/adapters/control/server en vez de declararse aquí. Mismo criterio,
// palabra por palabra, que el de cajerosock ↔ inferenciacliente: dos declaraciones paralelas de la
// misma forma es la clase de par que diverge en silencio (un campo renombrado en un lado se serializa a
// `null` en el otro sin un solo error).
//
// 🔴 QUÉ NO HACE, Y NO ES UNA CARENCIA: no reintenta, no lleva estado y no sondea. El aviso es UNA
// petición por transición. Si falla, el núcleo se entera igual por el camino que ya tenía —la siguiente
// inferencia le devuelve app.ErrInferenciaOllamaCaido y pasa a DOWN—, así que un reintento aquí
// compraría poco y añadiría un reloj al arranque, que es justo lo que D-044.43 vino a quitar.
package nucleoaviso

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/server"
)

// PlazoAviso es el plazo de UNA llamada cuando el llamante no pone el suyo.
//
// 🔴 ES CORTO A PROPÓSITO, Y EL NÚMERO SALE DEL PRESUPUESTO DEL APAGADO. El aviso de «caído» se emite
// dentro del apagado ordenado, y ahí el supervisor concede 20 s de StopTimeout
// (cmd/wapp-ctl/main.go) que están justificados línea a línea para DRENAR el lote en vuelo. Una señal
// que se comiera ese presupuesto convertiría un apagado limpio en un SIGKILL, o sea que el aviso de
// «me voy» provocaría justo la muerte sucia que pretendía evitar. 2 s sobre un socket local —cuyo RTT
// medido está en el orden de las decenas de microsegundos— es holgadísimo para el caso sano y
// despreciable para el presupuesto en el caso enfermo.
const PlazoAviso = 2 * time.Second

// Cliente habla con el plano de control del núcleo. Construir con Nuevo; es seguro para uso concurrente
// (lo es http.Client).
type Cliente struct {
	socket string
	url    string
	http   *http.Client
}

// Nuevo construye el cliente contra el socket /v1 en `ruta` (cfg.ControlSocketPath).
//
// EL CAJERO CONOCE ESA RUTA PORQUE COMPARTE CONFIG CON EL DAEMON: los dos son hijos del mismo
// supervisor, arrancan con el mismo EnvironmentFile y el mismo WorkingDirectory, así que
// `WAPP_AGENT_CONTROL_SOCKET_PATH` (o su default) resuelve a lo mismo en los dos procesos. No hay una
// segunda variable que mantener sincronizada, y esa es la razón de pasar la ruta y no derivarla.
func Nuevo(ruta string) *Cliente {
	return &Cliente{
		socket: ruta,
		// Host de relleno: el DialContext ignora host y puerto y marca el socket. Mismo molde que
		// inferenciacliente y que el reverse-proxy de wapp-ctl (cmd/wapp-ctl/proxy.go).
		url: "http://unix" + server.RutaReadinessInferencia,
		http: &http.Client{
			// Sin Timeout global: el plazo lo pone el context de cada llamada (ver Anunciar). Un timeout del
			// http.Client se aplicaría por igual al arranque y al apagado, que tienen presupuestos distintos.
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", ruta)
				},
				// Una conexión ociosa basta: esto manda dos peticiones en toda la vida del proceso.
				MaxIdleConns:    1,
				IdleConnTimeout: 30 * time.Second,
			},
		},
	}
}

// Socket devuelve la ruta del socket del núcleo. Sólo para el log del cableado.
func (c *Cliente) Socket() string { return c.socket }

// Anunciar manda UN aviso de readiness de la instalación `dataDir`. `listo` true ⇒ "ready"; false ⇒
// "down".
//
// EL `dataDir` NO ES OPCIONAL Y NO TIENE DEFAULT: el cajero atiende N instalaciones con N sockets de
// inferencia, y «el cajero está listo» no es una frase completa sin decir de cuál habla. El núcleo lo
// compara con el suyo y descarta el que no le toca (ver el encabezado de control/server/readiness.go).
//
// Devuelve error si el aviso no llegó o el núcleo lo rechazó. QUIEN DECIDE QUÉ HACER CON ESE ERROR ES
// EL LLAMANTE, y en los dos usos de hoy la respuesta es la misma: avisar en el log y seguir. Este canal
// es una aceleración, no un requisito de arranque ni de apagado.
func (c *Cliente) Anunciar(ctx context.Context, dataDir string, listo bool) error {
	estado := server.ReadinessCaido
	if listo {
		estado = server.ReadinessListo
	}
	cuerpo, err := json.Marshal(server.ReadinessRequest{Readiness: estado, DataDir: dataDir})
	if err != nil {
		// Imposible con dos strings; se propaga en vez de ignorarse porque un `_ =` aquí escondería un
		// cambio futuro del tipo del cable.
		return fmt.Errorf("nucleoaviso: serializar el aviso de readiness: %w", err)
	}

	// El plazo propio se pone AQUÍ y no en el llamante para que ninguno de los dos usos pueda olvidarlo:
	// el de «caído» corre dentro del apagado, donde el ctx del proceso YA está cancelado (por eso el
	// llamante pasa un context.WithoutCancel) y sin plazo propio se quedaría sin techo.
	ctx, cancel := context.WithTimeout(ctx, PlazoAviso)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(cuerpo))
	if err != nil {
		return fmt.Errorf("nucleoaviso: construir la petición: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("nucleoaviso: no se pudo avisar al núcleo por %s: %w", c.socket, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Se DRENA el cuerpo aunque no se use: sin leerlo, la conexión no vuelve al pool y el keep-alive no
	// sirve de nada. El techo evita que una respuesta anómala se lea entera en memoria.
	crudo, err := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	if err != nil {
		return fmt.Errorf("nucleoaviso: respuesta ilegible del núcleo (http %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("nucleoaviso: el núcleo respondió http %d al aviso de readiness (%s)",
			resp.StatusCode, string(crudo))
	}
	return nil
}
