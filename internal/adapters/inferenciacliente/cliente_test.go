package inferenciacliente_test

// cliente_test.go — EL LADO DAEMON DEL CANAL (Plan 044 · Ola 1.6 · T1.6-2, ADR-0045 §2).
//
// Lo que se prueba aquí no es «el cliente hace una petición HTTP»: es lo que el cliente TIENE QUE NO
// ROMPER. El daemon es el que traduce el desenlace de una inferencia al frame que el Cloud usa para
// decidir si degrada y cómo, así que cualquier error que este paquete clasifique mal se convierte
// directamente en un diagnóstico falso sobre la máquina del cliente. Los tres invariantes:
//
//	 1. La FRONTERA TIMEOUT / EDGE_SIN_CAPACIDAD sobrevive al cable (el test grande de abajo).
//	 2. Un cajero ausente es «el proveedor local no responde», no un error de transporte anónimo.
//	 3. Una respuesta que no es del contrato se reporta CITANDO lo que llegó.
//
// El fichero es `package inferenciacliente_test` (externo) para poder montar el SERVIDOR REAL de
// cajerosock enfrente: la frontera del punto 1 la emite el servidor y la interpreta el cliente, así que
// probarla con un doble del servidor sería probar el doble.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cajerosock"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/inferenciacliente"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// ─────────────────────────────────────────────────────────────────────────────
// ARNÉS
// ─────────────────────────────────────────────────────────────────────────────

// fallos recoge errores ocurridos DENTRO de las goroutines del servidor. No se reportan con t.Errorf
// desde allí porque el framework entra en pánico si se le llama después de que el test haya terminado, y
// una goroutine de servidor puede sobrevivirle; se vuelcan en un Cleanup. Se vuelcan con t.Errorf y no
// con t.Logf: un error escrito con t.Logf en un test que pasa lo descarta Go, o sea que es un error
// tragado con buena letra.
type fallos struct {
	mu    sync.Mutex
	lista []string
}

func (f *fallos) anota(formato string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lista = append(f.lista, fmt.Sprintf(formato, args...))
}

func (f *fallos) volcar(t testing.TB) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range f.lista {
		t.Errorf("fallo en el lado servidor: %s", m)
	}
}

// puertoDoble es el app.ServidorInferencia que el servidor real tiene detrás. Cada test dicta el
// desenlace y —esto es lo que importa en este fichero— CUÁNTO TARDA en darlo.
type puertoDoble struct {
	fn func(context.Context, app.PeticionInferencia) (app.RespuestaInferencia, error)
}

func (d *puertoDoble) Inferir(ctx context.Context, p app.PeticionInferencia) (app.RespuestaInferencia, error) {
	return d.fn(ctx, p)
}

// rutaSocket devuelve la ruta de un socket unix efímero, y NO usa t.TempDir() a propósito: t.TempDir()
// mete el NOMBRE DEL TEST en la ruta y `sun_path` son 104 bytes en macOS, así que un nombre descriptivo
// —que aquí son largos— revienta net.Listen con «invalid argument», un error que no se parece en nada a
// su causa. El directorio se devuelve creado; el fichero del socket NO existe todavía.
func rutaSocket(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ic")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("no se pudo limpiar el directorio temporal %q: %v", dir, err)
		}
	})
	ruta := filepath.Join(dir, "c.sock")
	if len(ruta) > 100 {
		t.Fatalf("la ruta del socket mide %d bytes y no cabe en sun_path (~104): %q", len(ruta), ruta)
	}
	return ruta
}

// montarServidorReal levanta el servidor de cajerosock con el puerto que el test dicte y devuelve un
// cliente apuntándole.
func montarServidorReal(ctx context.Context, t testing.TB, fn func(context.Context, app.PeticionInferencia) (app.RespuestaInferencia, error)) *inferenciacliente.Cliente {
	t.Helper()

	ruta := rutaSocket(t)
	srv := cajerosock.Nuevo(ctx, ruta, &puertoDoble{fn: fn},
		sharedlogger.New(sharedlogger.WithWriter(io.Discard)))
	ln, err := srv.Escuchar()
	if err != nil {
		t.Fatalf("Escuchar en %q: %v", ruta, err)
	}
	servido := make(chan error, 1)
	go func() { servido <- srv.Servir(ln) }()

	t.Cleanup(func() {
		ctxApagado, cancelar := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelar()
		if err := srv.Apagar(ctxApagado); err != nil {
			t.Errorf("Apagar devolvió error: %v", err)
		}
		select {
		case err := <-servido:
			if err != nil {
				t.Errorf("Servir devolvió error tras el apagado (se esperaba nil): %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("Servir no retornó tras Apagar: el servidor se quedó colgado")
		}
	})

	return inferenciacliente.Nuevo(ruta)
}

// montarCrudo levanta un servidor HTTP A MANO sobre el socket. Hace falta para los cuerpos que el
// servidor real NO PUEDE producir —basura que no es JSON, códigos HTTP fuera de su repertorio—, que es
// justo lo que este fichero necesita provocar: el cliente tiene que comportarse bien ante un cajero
// roto, no sólo ante uno sano.
func montarCrudo(t testing.TB, h http.HandlerFunc) *inferenciacliente.Cliente {
	t.Helper()

	ruta := rutaSocket(t)
	ln, err := net.Listen("unix", ruta)
	if err != nil {
		t.Fatalf("net.Listen unix %q: %v", ruta, err)
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	servido := make(chan error, 1)
	go func() { servido <- srv.Serve(ln) }()
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("cerrando el servidor crudo: %v", err)
		}
		select {
		case err := <-servido:
			// Serve SIEMPRE termina con error; el único aceptable es el del cierre que acabamos de pedir.
			// Cualquier otro significa que el servidor del test se murió por su cuenta y que lo que el
			// cliente observó no era lo que el test creía estar montando.
			if !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("el servidor crudo no murió por el cierre pedido: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("el servidor crudo no retornó tras Close")
		}
	})
	return inferenciacliente.Nuevo(ruta)
}

// ─────────────────────────────────────────────────────────────────────────────
// 6 · 🔴 LA FRONTERA TIMEOUT / EDGE_SIN_CAPACIDAD SOBREVIVE AL CABLE
// ─────────────────────────────────────────────────────────────────────────────

// TestLaFronteraSinCapacidadVsTimeoutSobreviveAlCable es el test central de este paquete.
//
// EL HECHO: quien sabe distinguir «el plazo venció ESPERANDO PLAZA» de «venció CON EL MODELO
// TRABAJANDO» es el cajero, porque es el único que ve las dos fases. Las dos se observan igual desde
// dentro (un ctx que vence) y significan lo contrario: la primera dice que el equipo va corto de
// hardware, la segunda que el modelo tarda más de lo que el Cloud espera. Confundirlas no da un error:
// da un DIAGNÓSTICO INVERTIDO, y manda al dueño del equipo a mirar su red en vez de su máquina.
//
// EL RIESGO QUE ESTE TEST CUBRE: esa distinción viaja en el CUERPO de la respuesta, así que sólo llega
// si el cliente sigue conectado cuando el cajero la emite. Y el cajero la emite justamente al agotarse
// el plazo. Si el ctx del cliente venciera con el mismo plazo, abortaría la conexión en ese mismo
// instante, se quedaría sin cuerpo que leer y tendría que adivinar — y adivina TIMEOUT siempre (ver
// errorDeTransporte). O sea: la frontera se perdería EN EL CABLE, después de haberse calculado bien.
//
// EL MECANISMO QUE LO EVITA es MargenSocket: el ctx del cliente vence MargenSocket DESPUÉS del que le
// mandó al cajero, así que el que vence primero es siempre el de dentro y el veredicto lo emite quien lo
// sabe. La forma del test refleja eso: el servidor responde DESPUÉS del plazo que se le mandó (que es lo
// que hace un cajero real: detecta que se agotó, y entonces serializa y escribe) pero dentro del margen.
//
// MUTACIÓN QUE LO PONE ROJO (ejecutada, no supuesta): cambiar en cliente.go
// `context.WithTimeout(ctx, p.Timeout+MargenSocket)` por `context.WithTimeout(ctx, p.Timeout)`. El
// veredicto pasa a ErrInferenciaTimeout y el test falla.
func TestLaFronteraSinCapacidadVsTimeoutSobreviveAlCable(t *testing.T) {
	// Sin t.Parallel: este test mide tiempos y compartir la máquina con otros le añade ruido gratis.
	ctx, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelar()

	// El plazo que el daemon manda al cajero, y lo que el cajero tarda en dictaminar. `tardanza > plazo`
	// a propósito: es la situación exacta para la que MargenSocket existe.
	const plazo = 200 * time.Millisecond
	const tardanza = 500 * time.Millisecond

	// Premisa del test contra la constante de producción: si alguien recortara MargenSocket por debajo de
	// la tardanza, este test empezaría a fallar por una razón que no es la que persigue. Mejor decirlo
	// aquí que depurar un rojo confuso.
	if tardanza-plazo >= inferenciacliente.MargenSocket {
		t.Fatalf("MargenSocket (%v) ya no cubre la tardanza de este test (%v-%v): "+
			"ajusta las constantes del test o revisa por qué se recortó el margen",
			inferenciacliente.MargenSocket, tardanza, plazo)
	}

	cli := montarServidorReal(ctx, t, func(_ context.Context, p app.PeticionInferencia) (app.RespuestaInferencia, error) {
		if p.Timeout != plazo {
			return app.RespuestaInferencia{}, fmt.Errorf("%w: el plazo no llegó al cajero (quería %v, llegó %v)",
				app.ErrInferenciaOllamaCaido, plazo, p.Timeout)
		}
		// El cajero espera plaza hasta agotar SU plazo y sólo entonces dictamina. NO se atiende al ctx de
		// la petición a propósito: si el cliente abortase, el cajero seguiría intentando responder, y lo
		// que este test tiene que ver es el desenlace de esa carrera, no evitarla.
		time.Sleep(tardanza)
		return app.RespuestaInferencia{}, app.ErrInferenciaSinCapacidad
	})

	inicio := time.Now()
	_, err := cli.Inferir(ctx, app.PeticionInferencia{
		CommandID: "cmd-frontera", Prompt: "da igual", Timeout: plazo,
	})
	transcurrido := time.Since(inicio)

	if errors.Is(err, app.ErrInferenciaTimeout) {
		t.Fatalf("EL DIAGNÓSTICO SE INVIRTIÓ EN EL CABLE: el cajero dijo %q y el daemon entendió %q "+
			"(tras %v). El dueño del equipo miraría su red en vez de su hardware. Error: %v",
			app.ErrInferenciaSinCapacidad.Codigo(), app.ErrInferenciaTimeout.Codigo(), transcurrido, err)
	}
	if !errors.Is(err, app.ErrInferenciaSinCapacidad) {
		t.Fatalf("se esperaba %q y obtuve %v (tras %v)", app.ErrInferenciaSinCapacidad.Codigo(), err, transcurrido)
	}

	// 🔴 LA COMPROBACIÓN QUE HACE QUE ESTE TEST MIRE: la respuesta llegó DESPUÉS de que el plazo mandado
	// al cajero hubiera vencido. Sin esta afirmación, el test seguiría verde con un servidor que contesta
	// rápido — y con un servidor que contesta rápido el margen no pinta nada, así que sería un test que
	// pasa aunque MargenSocket no exista.
	if transcurrido < plazo {
		t.Fatalf("la respuesta llegó en %v, ANTES del plazo de %v: este test no está probando el margen",
			transcurrido, plazo)
	}
}

// TestElVeredictoDeTimeoutSeReservaParaElCajeroQueNoContesta es la otra mitad de la frontera, y hace
// falta: un cliente que devolviera EDGE_SIN_CAPACIDAD siempre pasaría el test de arriba sin entender
// nada.
//
// Aquí el cajero se cuelga y NO responde nunca. El plazo del cliente —que va MargenSocket por encima—
// vence de verdad, y entonces TIMEOUT es el veredicto correcto: el cajero tuvo su plazo entero y ni
// siquiera contestó, así que no es que la máquina fuera corta de plazas, es que el proveedor no
// respondió. Esta es la razón por la que el ctx del cliente no se quita: es la última red contra un
// cajero colgado (un bug, un SIGSTOP) que si no dejaría el carril del daemon ocupado para siempre.
func TestElVeredictoDeTimeoutSeReservaParaElCajeroQueNoContesta(t *testing.T) {
	ctx, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelar()

	// Plazo diminuto: lo que se espera de verdad es plazo + MargenSocket (~2 s).
	const plazo = 10 * time.Millisecond
	colgado := make(chan struct{})
	t.Cleanup(func() { close(colgado) })

	cli := montarCrudo(t, func(_ http.ResponseWriter, r *http.Request) {
		// Se cuelga hasta que el cliente se rinda (o hasta que el test acabe).
		select {
		case <-r.Context().Done():
		case <-colgado:
		}
	})

	inicio := time.Now()
	_, err := cli.Inferir(ctx, app.PeticionInferencia{
		CommandID: "cmd-colgado", Prompt: "da igual", Timeout: plazo,
	})
	transcurrido := time.Since(inicio)

	if !errors.Is(err, app.ErrInferenciaTimeout) {
		t.Fatalf("un cajero que no contesta tiene que ser %q; obtuve %v (tras %v)",
			app.ErrInferenciaTimeout.Codigo(), err, transcurrido)
	}
	if errors.Is(err, app.ErrInferenciaSinCapacidad) {
		t.Errorf("un cajero mudo no es falta de plazas: %v", err)
	}
	// Y esperó el margen entero antes de rendirse: si se rindiera con el plazo pelado, el test de la
	// frontera de arriba no tendría ventana.
	if transcurrido < plazo+inferenciacliente.MargenSocket {
		t.Errorf("el cliente se rindió en %v, antes de plazo+MargenSocket (%v): "+
			"el margen no se está aplicando", transcurrido, plazo+inferenciacliente.MargenSocket)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 7 · UN CAJERO QUE NO ESTÁ Y UN OLLAMA QUE NO ESTÁ SON EL MISMO HECHO
// ─────────────────────────────────────────────────────────────────────────────

// TestUnSocketQueNoExisteEsElProveedorLocalCaido custodia una traducción que parece arbitraria y no lo
// es: desde el daemon, el cajero ES el proveedor local (es la única forma que tiene `agent serve` de
// conseguir una inferencia, REQ-051.10). Un cajero parado y un Ollama parado piden exactamente la misma
// acción del Cloud —degradar— y del operador —arrancar lo que se cayó—, así que darles dos códigos
// distintos sólo añadiría un estado que nadie sabe usar.
//
// Se comprueban tres cosas, y las tres importan por separado: que el código es OLLAMA_DOWN, que NO es
// TIMEOUT (el plazo no se agotó: no llegó a haber espera), y que es INMEDIATO — si un socket ausente
// costara el plazo entero, un Edge con el cajero caído devolvería el carril del daemon ocupado varios
// segundos por cada petición.
func TestUnSocketQueNoExisteEsElProveedorLocalCaido(t *testing.T) {
	ctx, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelar()

	ruta := rutaSocket(t) // el directorio existe; el socket no.
	if _, err := os.Lstat(ruta); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("premisa rota: %q existe (Lstat: %v)", ruta, err)
	}
	cli := inferenciacliente.Nuevo(ruta)

	const plazo = 3 * time.Second
	inicio := time.Now()
	_, err := cli.Inferir(ctx, app.PeticionInferencia{
		CommandID: "cmd-sin-cajero", Prompt: "da igual", Timeout: plazo,
	})
	transcurrido := time.Since(inicio)

	if !errors.Is(err, app.ErrInferenciaOllamaCaido) {
		t.Fatalf("un socket ausente tiene que ser %q; obtuve %v", app.ErrInferenciaOllamaCaido.Codigo(), err)
	}
	if errors.Is(err, app.ErrInferenciaTimeout) {
		t.Errorf("un socket ausente no es un plazo agotado: %v", err)
	}
	// El error cita la ruta del socket: es el dato con el que un operador arregla esto.
	if !strings.Contains(err.Error(), ruta) {
		t.Errorf("el error no dice qué socket falló (%q): %v", ruta, err)
	}
	if transcurrido > time.Second {
		t.Errorf("marcar a un socket inexistente tardó %v: tiene que fallar al instante, no consumir el plazo (%v)",
			transcurrido, plazo)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 8 · UNA RESPUESTA QUE NO ES DEL CONTRATO SE REPORTA CITANDO LO QUE LLEGÓ
// ─────────────────────────────────────────────────────────────────────────────

// TestUnaRespuestaFueraDelContratoSeDegradaCitandoElStatusYElTamano cubre los dos modos en que el cajero
// puede contestar algo que el cliente no sabe leer. Los dos se degradan como fallo del proveedor —el
// Cloud necesita UNO de los cinco códigos para decidir— pero el error tiene que llevar dentro con qué se
// diagnostica, que es lo único que distingue «tu Ollama está parado» de «vuestros binarios no casan».
//
// 🔴 Y lo que el error NO puede llevar es el cuerpo (INV-051.1): un cuerpo ilegible puede ser una salida
// del modelo a medio escribir, o sea contenido de negocio. Por eso el contrato del mensaje es «http N,
// M bytes» y no el volcado — y por eso el test afirma el tamaño, no el texto.
func TestUnaRespuestaFueraDelContratoSeDegradaCitandoElStatusYElTamano(t *testing.T) {
	ctx, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelar()

	t.Run("cuerpo que no es JSON", func(t *testing.T) {
		// Lo que devuelve, por ejemplo, un proxy metido por medio: 200 con HTML dentro.
		const basura = "<html><body>502 Bad Gateway</body></html>"

		anotador := &fallos{}
		t.Cleanup(func() { anotador.volcar(t) })
		cli := montarCrudo(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			if _, err := io.WriteString(w, basura); err != nil {
				anotador.anota("escribiendo el cuerpo: %v", err)
			}
		})

		_, err := cli.Inferir(ctx, app.PeticionInferencia{
			CommandID: "cmd-basura", Prompt: "da igual", Timeout: 3 * time.Second,
		})
		if err == nil {
			t.Fatalf("un 200 con HTML dentro se leyó como una inferencia válida")
		}
		if !errors.Is(err, app.ErrInferenciaOllamaCaido) {
			t.Errorf("se esperaba degradar como %q; obtuve %v", app.ErrInferenciaOllamaCaido.Codigo(), err)
		}
		if !strings.Contains(err.Error(), "http 200") {
			t.Errorf("el error no cita el status recibido: %v", err)
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("%d bytes", len(basura))) {
			t.Errorf("el error no cita el tamaño del cuerpo (%d bytes), que es el dato con el que se sabe "+
				"si llegó truncado o llegó otra cosa: %v", len(basura), err)
		}
		// Y no filtra el cuerpo entero: lo que se cita es su tamaño (INV-051.1).
		if strings.Contains(err.Error(), basura) {
			t.Errorf("INV-051.1 roto: el error lleva dentro el cuerpo recibido: %v", err)
		}
	})

	t.Run("codigo http fuera del repertorio", func(t *testing.T) {
		// El servidor real sólo emite 200, 503 y —para un cuerpo ilegible— 400. Cualquier otra cosa
		// significa que del otro lado no hay lo que creemos que hay; el cliente no tiene que inventar un
		// veredicto, tiene que decir qué le contestaron.
		anotador := &fallos{}
		t.Cleanup(func() { anotador.volcar(t) })
		cli := montarCrudo(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTeapot)
			if _, err := io.WriteString(w, `{}`); err != nil {
				anotador.anota("escribiendo el cuerpo: %v", err)
			}
		})

		_, err := cli.Inferir(ctx, app.PeticionInferencia{
			CommandID: "cmd-tetera", Prompt: "da igual", Timeout: 3 * time.Second,
		})
		if !errors.Is(err, app.ErrInferenciaOllamaCaido) {
			t.Fatalf("se esperaba degradar como %q; obtuve %v", app.ErrInferenciaOllamaCaido.Codigo(), err)
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("http %d", http.StatusTeapot)) {
			t.Errorf("el error no cita el status %d recibido: %v", http.StatusTeapot, err)
		}
	})
}
