// Package cajerosock es el LADO SERVIDOR del canal daemon→cajero para servir inferencia (Plan 044 ·
// Ola 1.6 · T1.6-2, ADR-0045 §2, REQ-34): un `http.Server` sobre un Unix domain socket con permisos
// 0600, uno por `data_dir`.
//
// ─────────────────────────────────────────────────────────────────────────────
// POR QUÉ UN SOCKET Y NO UNA TABLA CON SONDEO
// ─────────────────────────────────────────────────────────────────────────────
// El frame `inference_request` llega al proceso `agent serve`, pero quien puede hablar con Ollama es
// `agent cajero` (REQ-051.10). Hacen falta, pues, un canal entre los dos procesos, y la alternativa
// obvia —una tabla más en `cola_entrantes.db` que el cajero sondee— se descartó por tres razones, en
// orden de peso:
//
//  1. EL ADR-0045 EXIGE QUE UN BREAKER ABIERTO RESPONDA INMEDIATAMENTE. Con un sondeo de 500 ms (el
//     poll que tenía el cajero) ese «inmediatamente» cuesta hasta ~1 s ida y vuelta. Por el socket son
//     **53 µs** — MEDIDOS, no estimados: `BenchmarkRTTSocket` de este paquete, mediana de 6 corridas de
//     200 iteraciones sobre un socket real, con el rechazo inmediato del breaker abierto; el caso de
//     éxito con 2 KB de salida sube a ~105 µs, así que el canal lo domina el viaje y no los datos. Son
//     CUATRO órdenes de magnitud de diferencia. Un rechazo que tarda un segundo no es un rechazo, es una
//     degradación más.
//  2. SERÍA UN TERCER ESCRITOR SOBRE `cola_entrantes.db`. La contención de los dos que ya hay costó el
//     arreglo del Plan 051 T3.15 (los pragmas por-conexión de db.OpenCola). Añadir un tercero para un
//     tráfico que no necesita durabilidad es pagar aquel precio otra vez.
//  3. 🔴 AQUÍ LA DURABILIDAD ES UN ANTI-FEATURE. Una inferencia contestada tarde se entrega al vacío: el
//     Cloud ya degradó y siguió. Persistirla garantizaría que, tras un reinicio del cajero, se queme CPU
//     y se ocupe la única plaza de Ollama para producir una respuesta que nadie va a leer. Que la
//     petición muera con la conexión es la propiedad correcta, no una carencia.
//
// ─────────────────────────────────────────────────────────────────────────────
// EL PROTOCOLO — Y POR QUÉ SUS TIPOS VIVEN AQUÍ
// ─────────────────────────────────────────────────────────────────────────────
// HTTP/1.1 + JSON sobre el socket, un solo endpoint: `POST /inferencia`. El cliente
// (internal/adapters/inferenciacliente) IMPORTA los tipos de este paquete en vez de declarar los suyos
// propios: son dos mitades del mismo contrato, y dos declaraciones paralelas de la misma forma es la
// clase de par que diverge en silencio (un campo renombrado en un lado se serializa a `null` en el otro
// sin un solo error). El dueño del contrato es el SERVIDOR.
//
// 🔴 INV-051.1: el prompt y la salida cruzan este socket porque no hay otra forma de servir la
// inferencia, pero NO SALEN POR NINGÚN LOG de este paquete, ni siquiera truncados. Lo que se loguea es
// `command_id`, tamaños y desenlace.
package cajerosock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Ruta es el único endpoint del socket. Exportada porque el cliente la usa para construir su URL: es
// parte del contrato, no un detalle de este fichero.
const Ruta = "/inferencia"

// MaxCuerpo es el techo del cuerpo de una petición. 8 MiB es un orden de magnitud por encima del prompt
// más grande que tiene sentido mandarle a un modelo con la ventana de contexto del Edge (4.096 tokens),
// así que no puede recortar tráfico legítimo; lo que corta es un cuerpo corrupto o malicioso que, sin
// techo, se leería entero en memoria antes de descubrir que no servía.
const MaxCuerpo = 8 << 20

// PeticionWire es la forma en el cable de una petición de inferencia. Espeja
// app.PeticionInferencia; el mapeo entre las dos vive en este paquete (Desde/Hacia) para que no haya
// dos traducciones.
type PeticionWire struct {
	CommandID string `json:"command_id"`
	SessionID string `json:"session_id,omitempty"`
	// 🔴 Contenido de negocio. Cruza el socket, no cruza el log.
	Prompt string `json:"prompt"`
	Format string `json:"format,omitempty"`
	// Temperature es un PUNTERO también en el cable: `omitempty` sobre un float haría indistinguibles
	// «quiero 0» y «no dije nada», que es exactamente la distinción que el `optional` del contrato de
	// CloudLink existe para preservar. Con puntero, ausente en el JSON ⇒ nil ⇒ default del Edge.
	Temperature *float32 `json:"temperature,omitempty"`
	// TimeoutMS es el plazo YA RESUELTO por el daemon (el del Cloud, o 0 si no lo fijó). El cajero le
	// aplica su default y su techo.
	TimeoutMS int64 `json:"timeout_ms,omitempty"`
}

// RespuestaWire es la forma en el cable de la respuesta. Exactamente uno de los dos campos viene
// poblado, y el que decide cuál es el CÓDIGO HTTP (200 ⇒ RawJSON; 503 ⇒ Error), no la ausencia del
// otro: un cuerpo que llegara con los dos vacíos por un fallo de serialización se leería como «el
// modelo no dijo nada» si el discriminador fuera el campo.
type RespuestaWire struct {
	// 🔴 Contenido de negocio. Cruza el socket, no cruza el log.
	RawJSON string `json:"raw_json,omitempty"`
	// Error es el CÓDIGO canónico (app.ErrorInferencia.Codigo()), no un texto libre. El cliente lo
	// resuelve con app.ErrorInferenciaDe y trata un código desconocido como fallo — que es lo correcto:
	// significa que los dos binarios están desalineados.
	Error string `json:"error,omitempty"`
}

// Servidor sirve `POST /inferencia` sobre un Unix socket. Construir con Nuevo.
type Servidor struct {
	ruta string
	srv  *http.Server
	log  sharedlogger.Logger
}

// Nuevo construye el servidor del socket en `ruta`, delegando cada petición en `puerto`.
//
// `ctxProceso` es el contexto de vida del proceso y se instala como BaseContext del servidor HTTP. Esa
// línea hace dos cosas a la vez, y las dos hacen falta: el ctx de cada petición hereda la cancelación
// del apagado (así una inferencia en vuelo se corta con el SIGTERM en vez de retener la plaza del aforo
// hasta su plazo completo) y sigue cancelándose por su cuenta si el cliente cierra la conexión.
func Nuevo(ctxProceso context.Context, ruta string, puerto app.ServidorInferencia, log sharedlogger.Logger) *Servidor {
	if log == nil {
		log = sharedlogger.Default()
	}
	s := &Servidor{ruta: ruta, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("POST "+Ruta, s.handler(puerto))

	s.srv = &http.Server{
		Handler: mux,
		// ReadHeaderTimeout acota el «slowloris» (una conexión que manda cabeceras a gotas). Es el único
		// plazo del servidor, y los otros dos brillan por su ausencia A PROPÓSITO:
		//
		//   - SIN ReadTimeout ni WriteTimeout. Una inferencia legítima puede durar hasta el techo del Edge
		//     (DefaultMaxTimeoutMS, 120 s) y esos plazos son de CONEXIÓN, no de handler: cortarían la
		//     respuesta a mitad y el daemon leería un cuerpo truncado —que traduciría a OLLAMA_DOWN— en vez
		//     del error nombrado que el cajero sí sabía dar. El plazo de una inferencia se acota donde se
		//     sabe cuál es: dentro del handler, con el `timeout_ms` de la petición.
		//   - El socket es unix y 0600: el único que puede conectarse es un proceso del mismo usuario. La
		//     superficie de abuso es el propio daemon, no la red.
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctxProceso },
	}
	return s
}

// Escuchar crea el socket con permisos 0600 y devuelve el listener listo para Servir.
//
// LIMPIA UN SOCKET RANCIO de un arranque previo —el que deja un SIGKILL, que no ejecuta ningún
// `defer`— pero SE NIEGA a borrar un fichero regular en esa ruta: sin esa distinción, una ruta mal
// configurada destruiría datos del usuario en vez de fallar. Es el mismo criterio (y casi el mismo
// código) del plano de control del daemon, control/server.Listen, y estar duplicado es deliberado: son
// dos adaptadores distintos y compartirlo obligaría a que uno importase al otro por diez líneas.
func (s *Servidor) Escuchar() (net.Listener, error) {
	if s.ruta == "" {
		return nil, errors.New("cajerosock: ruta de socket vacía")
	}
	if info, err := os.Stat(s.ruta); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("cajerosock: %q ya existe y no es un socket; abortando para no borrar datos", s.ruta)
		}
		if err := os.Remove(s.ruta); err != nil {
			return nil, fmt.Errorf("cajerosock: no se pudo eliminar el socket previo %q: %w", s.ruta, err)
		}
	}
	ln, err := net.Listen("unix", s.ruta)
	if err != nil {
		return nil, fmt.Errorf("cajerosock: no se pudo escuchar en %q: %w", s.ruta, err)
	}
	if err := os.Chmod(s.ruta, 0o600); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("cajerosock: no se pudo aplicar permisos 0600 a %q: %w", s.ruta, err)
	}
	return ln, nil
}

// Servir atiende peticiones en ln y BLOQUEA hasta que Apagar cierre el servidor. Devuelve nil en cierre
// limpio.
func (s *Servidor) Servir(ln net.Listener) error {
	s.log.Info("cajero: sirviendo inferencia por el socket (ADR-0045: el Cloud pide, el Edge sirve)",
		"socket", s.ruta, "ruta", Ruta)
	if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Apagar cierra el servidor drenando las peticiones en curso hasta que ctx expire, y borra el socket.
//
// ES AQUÍ —y no en Cajero.Run— DONDE SE ESPERA A LAS INFERENCIAS EN VUELO: este servidor es quien tiene
// las conexiones, así que es el único que sabe cuántas hay. Ver la nota de Run.
func (s *Servidor) Apagar(ctx context.Context) error {
	err := s.srv.Shutdown(ctx)
	_ = os.Remove(s.ruta)
	return err
}

// handler traduce HTTP↔puerto. No decide NADA sobre la inferencia: eso es del servidor de dominio
// (internal/app/cajero.servidorInferencia).
func (s *Servidor) handler(puerto app.ServidorInferencia) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var pet PeticionWire
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxCuerpo)).Decode(&pet); err != nil {
			// 🔴 Un cuerpo ilegible NO se traduce a un error del vocabulario de inferencia. Los cinco
			// códigos describen por qué el EDGE no pudo servir, y esto es que el DAEMON mandó algo que no
			// se entiende: devolverle OLLAMA_DOWN le haría degradar culpando a la máquina del cliente de un
			// bug nuestro. Un 400 pelado dice lo que pasa y el cliente lo trata como fallo de transporte.
			s.log.Error("cajero: petición de inferencia ilegible por el socket", "error", err)
			http.Error(w, "cuerpo ilegible", http.StatusBadRequest)
			return
		}

		if puerto == nil {
			// Sin proveedor local (Deps.Ollama nil, feature apagada). El socket existe —el proceso está
			// vivo— pero no hay nada detrás, y eso es exactamente OLLAMA_DOWN: «el proveedor local no
			// responde». Es la misma respuesta que daría un Ollama parado, que es la verdad operativa.
			s.responderError(w, pet.CommandID, app.ErrInferenciaOllamaCaido)
			return
		}

		resp, err := puerto.Inferir(r.Context(), app.PeticionInferencia{
			CommandID:   pet.CommandID,
			SessionID:   pet.SessionID,
			Prompt:      pet.Prompt,
			Format:      pet.Format,
			Temperature: pet.Temperature,
			Timeout:     time.Duration(pet.TimeoutMS) * time.Millisecond,
		})
		if err != nil {
			canonico, ok := app.EsErrorInferencia(err)
			if !ok {
				// El puerto devolvió un error que NO es del vocabulario. Es un bug de este repo (el puerto
				// promete los cinco), y se trata como el fallo genérico del proveedor porque el Cloud
				// necesita UN código para degradar. Se grita en Error para que el bug se vea.
				s.log.Error("cajero: el servidor de inferencia devolvió un error FUERA del vocabulario canónico "+
					"(bug: app.ServidorInferencia promete los cinco); se responde como fallo del proveedor",
					"command_id", pet.CommandID, "error", err)
				canonico = app.ErrInferenciaOllamaCaido
			}
			s.responderError(w, pet.CommandID, canonico)
			return
		}

		s.escribir(w, http.StatusOK, RespuestaWire{RawJSON: resp.RawJSON}, pet.CommandID)
	}
}

// responderError escribe un error nombrado con 503. El código HTTP es el DISCRIMINADOR del oneof del
// cable (ver RespuestaWire) y 503 es el que describe el hecho: el servicio de inferencia no está
// disponible ahora mismo, por una de cinco razones que van en el cuerpo.
func (s *Servidor) responderError(w http.ResponseWriter, commandID string, e *app.ErrorInferencia) {
	s.escribir(w, http.StatusServiceUnavailable, RespuestaWire{Error: e.Codigo()}, commandID)
}

// escribir serializa la respuesta. Un fallo al escribir sólo se loguea: la conexión ya se está yendo y
// no hay nada mejor que hacer con ese error que dejarlo dicho.
func (s *Servidor) escribir(w http.ResponseWriter, codigo int, cuerpo RespuestaWire, commandID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(codigo)
	if err := json.NewEncoder(w).Encode(cuerpo); err != nil {
		// 🔴 INV-051.1: el cuerpo NO se loguea, sólo su tamaño y el código.
		s.log.Warn("cajero: no se pudo escribir la respuesta de inferencia en el socket",
			"error", err, "command_id", commandID, "http", codigo, "salida_bytes", len(cuerpo.RawJSON))
	}
}
