// Package inferenciacliente es el LADO CLIENTE del canal daemon→cajero (Plan 044 · Ola 1.6 · T1.6-2,
// ADR-0045 §2, REQ-34): implementa app.ServidorInferencia hablando HTTP/1.1 sobre el Unix socket que
// levanta el proceso `agent cajero` (internal/adapters/cajerosock).
//
// POR QUÉ ES UN ADAPTADOR Y NO UNA LLAMADA DIRECTA: el daemon NO PUEDE hablar con Ollama (REQ-051.10 —
// «ningún otro proceso que el worker habla con Ollama», custodiado por un grep de `ollama.New`). Este
// paquete es la única forma que tiene `agent serve` de conseguir una inferencia, y desde su punto de
// vista el cajero ES el proveedor local: si el socket no está, la respuesta honesta es la misma que
// daría un Ollama parado.
//
// 🔴 INV-051.1: el prompt y la salida pasan por aquí y NO SALEN POR NINGÚN LOG. Este paquete, de hecho,
// no loguea nada: quien tiene el contexto para decir algo útil es el carril del adaptador de CloudLink,
// que es quien conoce el desenlace y el `command_id`.
package inferenciacliente

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cajerosock"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// MaxRespuesta es el techo del cuerpo de la respuesta que se lee. Igual que el de la petición
// (cajerosock.MaxCuerpo) y por lo mismo: la salida de un modelo con num_predict acotado es de
// kilobytes, así que 8 MiB no recorta nada legítimo, y lo que corta es un cuerpo corrupto que si no se
// leería entero en memoria.
const MaxRespuesta = cajerosock.MaxCuerpo

// MargenSocket es el tiempo EXTRA que el cliente le da a su propia llamada por encima del plazo que le
// manda al cajero.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 SIN ESTE MARGEN, LA FRONTERA TIMEOUT/EDGE_SIN_CAPACIDAD SE PIERDE EN EL CABLE
// ─────────────────────────────────────────────────────────────────────────────
// Quien sabe distinguir «se agotó el plazo esperando plaza» de «se agotó con el modelo trabajando» es
// el CAJERO: es el único que ve las dos fases (ver cajero.Aforo). Esa distinción viaja de vuelta en el
// cuerpo de la respuesta. Pero si el ctx del CLIENTE vence al mismo tiempo que el del cajero, el cliente
// aborta la conexión ANTES de que la respuesta llegue y se queda sin cuerpo que leer: entonces tendría
// que adivinar, y adivinaría TIMEOUT siempre — que es precisamente el diagnóstico invertido que
// EDGE_SIN_CAPACIDAD existe para evitar (mandaría al dueño a mirar su red en vez de su hardware).
//
// Con el margen, el que vence primero es SIEMPRE el plazo de dentro, y el veredicto lo emite quien lo
// sabe. 2 s es holgado: lo que tiene que cubrir es el viaje de vuelta por un socket local, medido en el
// orden del milisegundo (ver el benchmark de RTT en cajerosock).
//
// El ctx del cliente sigue existiendo —no se quita el plazo, se corre— porque es la última red: si el
// cajero se cuelga sin responder (un bug, un SIGSTOP), el daemon no puede quedarse esperando para
// siempre con el carril ocupado.
const MargenSocket = 2 * time.Second

// Cliente habla con el socket del cajero. Construir con Nuevo; es seguro para uso concurrente (lo es
// http.Client).
type Cliente struct {
	socket string
	http   *http.Client
	url    string
}

// Nuevo construye el cliente contra el socket en `ruta` (típicamente layout.CajeroSock()).
func Nuevo(ruta string) *Cliente {
	return &Cliente{
		socket: ruta,
		// La URL lleva un host de relleno: el DialContext ignora host y puerto y marca el socket. Es el
		// mismo molde que el reverse-proxy de wapp-ctl hacia el plano de control (cmd/wapp-ctl/proxy.go).
		url: "http://unix" + cajerosock.Ruta,
		http: &http.Client{
			// 🔴 SIN Timeout GLOBAL, Y ES DELIBERADO: el plazo lo pone el context de cada llamada. Un
			// timeout del http.Client se aplicaría por igual a una inferencia de 200 ms y a una de 45 s, así
			// que o corta las largas (convirtiendo trabajo legítimo en fallo) o no acota nada. Es el mismo
			// criterio con el que se construye el cliente de Ollama (ollama.New).
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", ruta)
				},
				// Una sola conexión ociosa por host basta y sobra: el aforo del cajero es de una plaza, así
				// que más conexiones vivas sólo servirían para hacer cola dentro del cajero.
				MaxIdleConns:    2,
				IdleConnTimeout: 90 * time.Second,
			},
		},
	}
}

// Socket devuelve la ruta del socket. Sólo para el log del cableado.
func (c *Cliente) Socket() string { return c.socket }

// Inferir implementa app.ServidorInferencia. Traduce la petición al cable, la manda por el socket y
// traduce la respuesta de vuelta.
func (c *Cliente) Inferir(ctx context.Context, p app.PeticionInferencia) (app.RespuestaInferencia, error) {
	cuerpo, err := json.Marshal(cajerosock.PeticionWire{
		CommandID:   p.CommandID,
		SessionID:   p.SessionID,
		Prompt:      p.Prompt,
		Format:      p.Format,
		Temperature: p.Temperature,
		TimeoutMS:   p.Timeout.Milliseconds(),
	})
	if err != nil {
		// Serializar cinco campos escalares no falla salvo por algo imposible. Se devuelve el error
		// genérico del proveedor porque el llamante necesita UNO de los cinco para responder al Cloud.
		return app.RespuestaInferencia{}, fmt.Errorf("%w: no se pudo serializar la petición: %w",
			app.ErrInferenciaOllamaCaido, err)
	}

	// EL PLAZO DEL CLIENTE VA POR DELANTE DEL DE DENTRO (ver MargenSocket). Sólo se aplica si la petición
	// trae plazo: con 0 («el Cloud no lo fijó») manda el ctx que venga de arriba, igual que el cajero
	// aplicará su default.
	if p.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.Timeout+MargenSocket)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(cuerpo))
	if err != nil {
		return app.RespuestaInferencia{}, fmt.Errorf("%w: %w", app.ErrInferenciaOllamaCaido, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return app.RespuestaInferencia{}, c.errorDeTransporte(ctx, err)
	}
	defer func() { _ = resp.Body.Close() }()

	crudo, err := io.ReadAll(io.LimitReader(resp.Body, MaxRespuesta))
	if err != nil {
		return app.RespuestaInferencia{}, c.errorDeTransporte(ctx, err)
	}

	var wire cajerosock.RespuestaWire
	if err := json.Unmarshal(crudo, &wire); err != nil {
		// El cajero respondió algo que no es su contrato. Es fallo de transporte desde el punto de vista
		// del daemon: el proveedor local no dio una respuesta utilizable.
		return app.RespuestaInferencia{}, fmt.Errorf(
			"%w: respuesta ilegible del socket del cajero (http %d, %d bytes): %w",
			app.ErrInferenciaOllamaCaido, resp.StatusCode, len(crudo), err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return app.RespuestaInferencia{RawJSON: wire.RawJSON}, nil
	case http.StatusServiceUnavailable:
		canonico, ok := app.ErrorInferenciaDe(wire.Error)
		if !ok {
			// 🔴 UN CÓDIGO DESCONOCIDO NO SE PARECE A NINGUNO. Significa que los dos binarios están
			// desalineados (el cajero de una versión, el daemon de otra), y elegir «el más parecido»
			// convertiría un problema de despliegue en un diagnóstico falso sobre la máquina del cliente. Se
			// reporta como fallo del proveedor CITANDO el código, que es lo que hace el problema visible.
			return app.RespuestaInferencia{}, fmt.Errorf(
				"%w: el cajero devolvió un código de error desconocido %q (¿binarios desalineados?)",
				app.ErrInferenciaOllamaCaido, wire.Error)
		}
		return app.RespuestaInferencia{}, canonico
	default:
		return app.RespuestaInferencia{}, fmt.Errorf(
			"%w: el socket del cajero respondió http %d", app.ErrInferenciaOllamaCaido, resp.StatusCode)
	}
}

// errorDeTransporte traduce un fallo de red del socket al error canónico que le toca.
//
// 🔴 SE PREGUNTA AL CONTEXTO, NO AL ERROR. El transporte de Go envuelve la cancelación dentro de su
// propio `*url.Error`, y encima la envoltura cambia entre versiones; mirar el texto —o incluso
// `errors.Is` sobre un error de red que ya se tragó la causa— clasificaría mal justo el caso que
// importa. El contexto sí sabe la verdad y la sabe sin ambigüedad.
//
// Si el ctx del cliente venció, el veredicto es TIMEOUT y NO EDGE_SIN_CAPACIDAD, y eso es correcto pese
// a la frontera: llegar aquí significa que venció el plazo del cliente, que va MargenSocket POR ENCIMA
// del que se le dio al cajero (ver MargenSocket). O sea que el cajero tuvo su plazo entero y ni siquiera
// respondió — no es que la máquina fuera corta de plazas, es que el proveedor no contestó a tiempo.
func (c *Cliente) errorDeTransporte(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: el cajero no respondió dentro del plazo del socket: %w",
			app.ErrInferenciaTimeout, err)
	}
	// Todo lo demás —socket inexistente, conexión rechazada, cajero parado, ctx del proceso cancelado— es
	// «el proveedor local no responde». Desde el daemon, un cajero que no está y un Ollama que no está
	// son el mismo hecho y piden la misma acción.
	return fmt.Errorf("%w: no se pudo hablar con el cajero por %s: %w",
		app.ErrInferenciaOllamaCaido, c.socket, err)
}
