package cajerosock_test

// servidor_test.go — EL CANAL daemon→cajero PROBADO COMO PAR (Plan 044 · Ola 1.6 · T1.6-2, ADR-0045 §2).
//
// Estos tests no ejercitan el servidor contra un cliente de mentira: montan el SERVIDOR REAL de este
// paquete y el CLIENTE REAL de internal/adapters/inferenciacliente sobre un Unix socket DE VERDAD, y
// comprueban qué sobrevive al viaje. La razón es la misma que da el docstring del paquete para que los
// tipos del cable vivan aquí: servidor y cliente son DOS MITADES DEL MISMO CONTRATO, y los defectos que
// este canal puede tener —un campo que se serializa a `null`, un código que se traduce «al más
// parecido», un plazo que mata la conexión antes de que llegue el veredicto— sólo son visibles con las
// dos mitades cableadas. Un test de cada mitad por separado sale verde con el par roto.
//
// 🔴 POR QUÉ ESTE FICHERO ES `package cajerosock_test` Y NO `package cajerosock`: el cliente IMPORTA a
// este paquete (los tipos del cable son suyos), así que un test interno que importase al cliente sería
// un ciclo de importación. El test externo es la única forma de sentar a los dos en la misma mesa.

import (
	"bytes"
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

// bufferSeguro es un io.Writer para el logger del servidor. Existe con mutex porque el servidor escribe
// desde SUS goroutines y el test lee desde la suya: un bytes.Buffer pelado sería una carrera que -race
// caza (y con razón), no una comodidad.
type bufferSeguro struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *bufferSeguro) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *bufferSeguro) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// fallos recoge los errores que ocurren DENTRO de las goroutines del servidor. No se reportan con
// t.Errorf desde allí a propósito: el framework de tests entra en pánico si se le llama después de que
// el test haya terminado, y una goroutine de servidor puede sobrevivirle. Se anotan y se vuelcan en un
// Cleanup, que sí corre en el momento correcto — y se vuelcan con t.Errorf, NUNCA con t.Logf: un error
// escrito con t.Logf es un error tragado (Go descarta la salida de los tests que pasan).
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

// puertoDoble es el app.ServidorInferencia que hay detrás del socket. Sustituye al servidor de dominio
// real (app/cajero) porque lo que se prueba aquí es EL CANAL, no la decisión: el doble deja que cada
// test dicte el desenlace exacto —una salida, uno de los cinco errores, una tardanza— y guarda lo que
// recibió para poder afirmar que la petición cruzó ÍNTEGRA.
type puertoDoble struct {
	fn func(context.Context, app.PeticionInferencia) (app.RespuestaInferencia, error)

	mu     sync.Mutex
	vistas []app.PeticionInferencia
}

func (d *puertoDoble) Inferir(ctx context.Context, p app.PeticionInferencia) (app.RespuestaInferencia, error) {
	d.mu.Lock()
	d.vistas = append(d.vistas, p)
	d.mu.Unlock()
	return d.fn(ctx, p)
}

// vista devuelve la i-ésima petición que llegó al otro lado del socket.
func (d *puertoDoble) vista(t testing.TB, i int) app.PeticionInferencia {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	if i >= len(d.vistas) {
		t.Fatalf("se esperaba al menos %d petición(es) en el puerto, llegaron %d", i+1, len(d.vistas))
	}
	return d.vistas[i]
}

// arnes es el par cableado: servidor real de cajerosock ↔ cliente real de inferenciacliente.
type arnes struct {
	ruta   string
	cli    *inferenciacliente.Cliente
	puerto *puertoDoble
	log    *bufferSeguro
}

// montar levanta el servidor sobre un socket efímero y devuelve el par listo. El apagado (drenaje +
// borrado del socket) y su verificación quedan registrados como Cleanup.
func montar(ctx context.Context, t testing.TB, fn func(context.Context, app.PeticionInferencia) (app.RespuestaInferencia, error)) *arnes {
	t.Helper()

	ruta := rutaSocket(t)
	registro := &bufferSeguro{}
	puerto := &puertoDoble{fn: fn}
	srv := cajerosock.Nuevo(ctx, ruta, puerto,
		sharedlogger.New(sharedlogger.WithWriter(registro), sharedlogger.WithJSON(true)))

	ln, err := srv.Escuchar()
	if err != nil {
		t.Fatalf("Escuchar en %q: %v", ruta, err)
	}
	servido := make(chan error, 1)
	go func() { servido <- srv.Servir(ln) }()

	t.Cleanup(func() {
		ctxApagado, cancelar := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelar()
		if err := srv.Apagar(ctxApagado); err != nil {
			t.Errorf("Apagar devolvió error: %v", err)
		}
		select {
		case err := <-servido:
			// Servir promete nil en cierre limpio: http.ErrServerClosed lo absorbe él.
			if err != nil {
				t.Errorf("Servir devolvió error tras el apagado (se esperaba nil): %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("Servir no retornó tras Apagar: el servidor se quedó colgado")
		}
		// Apagar borra el socket. Que la ruta quede limpia es parte de su contrato: si no lo hiciera, el
		// siguiente arranque dependería de la limpieza del socket rancio para funcionar.
		if _, err := os.Lstat(ruta); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("tras Apagar el socket %q sigue en disco (Lstat: %v)", ruta, err)
		}
	})

	return &arnes{ruta: ruta, cli: inferenciacliente.Nuevo(ruta), puerto: puerto, log: registro}
}

// rutaSocket devuelve la ruta de un socket unix efímero, y NO usa t.TempDir() a propósito.
//
// 🔴 t.TempDir() mete el NOMBRE DEL TEST en la ruta, y `sun_path` —el hueco del kernel donde cabe la
// ruta de un socket unix— son 104 bytes en macOS. Los nombres de test de este repo son descriptivos y
// largos, así que la combinación revienta `net.Listen` con «invalid argument»: un error que no se parece
// en nada a su causa y que se depura durante media hora. Con un prefijo corto la ruta cabe siempre, y el
// guardia de abajo convierte el fallo en un mensaje que dice lo que pasa.
func rutaSocket(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "cs")
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

// montarCrudo levanta un servidor HTTP A MANO sobre el socket, con el handler que el test dicte.
//
// POR QUÉ HACE FALTA además del arnés real: el vocabulario de errores es CERRADO —app.ErrorInferencia
// tiene los campos sin exportar, así que desde fuera de `app` no se puede fabricar un código que no esté
// en la lista—. Esa es una buena propiedad del diseño y precisamente por ella el caso «el cajero mandó
// un código que no conozco» no se puede provocar a través del servidor real: hay que escribir el cuerpo
// a mano. Lo mismo vale para un cuerpo que no sea JSON.
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
// 1 · IDA Y VUELTA SOBRE UN SOCKET DE VERDAD
// ─────────────────────────────────────────────────────────────────────────────

// TestIdaYVueltaPorElSocketEntregaLaSalidaIntegraYLaPeticionEntera afirma lo básico del canal: lo que el
// daemon pide llega ENTERO al cajero y lo que el cajero devuelve llega ENTERO al daemon.
//
// Los tres detalles que no son obvios y que este test custodia:
//
//   - La salida se compara BYTE A BYTE con acentos, comillas escapadas y un salto de línea dentro. El
//     contrato dice que `raw_json` sube «crudo tal cual, sin validar, sin reformatear y sin truncar»: si
//     alguien decidiera «arreglar» la forma por el camino, se vería aquí.
//   - `Temperature` es un PUNTERO en los dos lados y el test pasa el valor 0, que es el que más se va a
//     pedir (determinismo para clasificar) y a la vez el cero del tipo. Con `omitempty` sobre un float
//     este caso se serializaría como ausencia y llegaría como nil: «quiero 0» y «no dije nada» serían el
//     mismo byte, que es justo lo que el `optional` del contrato existe para distinguir.
//   - 🔴 INV-051.1: el prompt y la salida CRUZAN el socket y NO CRUZAN el log. Se comprueba sobre el
//     registro real del servidor — y se comprueba antes que el registro tenga algo dentro, porque un
//     buffer vacío haría pasar esta afirmación sin mirar nada.
func TestIdaYVueltaPorElSocketEntregaLaSalidaIntegraYLaPeticionEntera(t *testing.T) {
	ctx, cancelar := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelar()

	const prompt = "clasifica esto: quiero dos pizzas y una coca-cola"
	const salida = `{"intent":"pedido","detalle":"dos \"pizzas\"\nañadidas"}`

	a := montar(ctx, t, func(_ context.Context, _ app.PeticionInferencia) (app.RespuestaInferencia, error) {
		return app.RespuestaInferencia{RawJSON: salida}, nil
	})

	cero := float32(0)
	resp, err := a.cli.Inferir(ctx, app.PeticionInferencia{
		CommandID:   "cmd-ida-vuelta",
		SessionID:   "ses-1",
		Prompt:      prompt,
		Format:      `{"type":"object"}`,
		Temperature: &cero,
		Timeout:     3 * time.Second,
	})
	if err != nil {
		t.Fatalf("Inferir devolvió error en el camino feliz: %v", err)
	}
	if resp.RawJSON != salida {
		t.Fatalf("la salida no sobrevivió al viaje:\n  quería %q\n  obtuve %q", salida, resp.RawJSON)
	}

	vista := a.puerto.vista(t, 0)
	if vista.CommandID != "cmd-ida-vuelta" {
		t.Errorf("command_id: quería %q, llegó %q", "cmd-ida-vuelta", vista.CommandID)
	}
	if vista.SessionID != "ses-1" {
		t.Errorf("session_id: quería %q, llegó %q", "ses-1", vista.SessionID)
	}
	if vista.Prompt != prompt {
		t.Errorf("prompt: quería %q, llegó %q", prompt, vista.Prompt)
	}
	if vista.Format != `{"type":"object"}` {
		t.Errorf("format: quería %q, llegó %q", `{"type":"object"}`, vista.Format)
	}
	if vista.Timeout != 3*time.Second {
		t.Errorf("timeout: quería %v, llegó %v (¿se perdió en la conversión a milisegundos?)", 3*time.Second, vista.Timeout)
	}
	if vista.Temperature == nil {
		t.Fatalf("temperature 0 llegó como nil: el cable confundió «quiero 0» con «no dije nada»")
	}
	if *vista.Temperature != 0 {
		t.Errorf("temperature: quería 0, llegó %v", *vista.Temperature)
	}

	// La otra mitad de la misma distinción: nil se queda nil (⇒ el Edge aplica su default).
	if _, err := a.cli.Inferir(ctx, app.PeticionInferencia{
		CommandID: "cmd-sin-temperatura", Prompt: prompt, Timeout: 3 * time.Second,
	}); err != nil {
		t.Fatalf("segunda inferencia: %v", err)
	}
	if temp := a.puerto.vista(t, 1).Temperature; temp != nil {
		t.Errorf("temperature ausente llegó como %v: el cable inventó un valor que el Cloud no pidió", *temp)
	}

	// 🔴 INV-051.1 sobre el registro real del servidor.
	registro := a.log.String()
	if !strings.Contains(registro, cajerosock.Ruta) {
		t.Fatalf("el registro del servidor no menciona %q: está vacío o no se capturó, "+
			"así que las dos afirmaciones de abajo no estarían mirando nada:\n%s", cajerosock.Ruta, registro)
	}
	if strings.Contains(registro, prompt) {
		t.Errorf("INV-051.1 roto: el PROMPT salió por el log del servidor:\n%s", registro)
	}
	if strings.Contains(registro, salida) {
		t.Errorf("INV-051.1 roto: la SALIDA del modelo salió por el log del servidor:\n%s", registro)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 2 · LOS CINCO ERRORES CANÓNICOS SOBREVIVEN AL VIAJE
// ─────────────────────────────────────────────────────────────────────────────

// TestLosCincoErroresCanonicosSobrevivenAlViaje recorre app.ErroresInferencia y exige que cada uno vuelva
// a salir por el otro extremo del socket SIENDO EL MISMO error.
//
// POR QUÉ SE RECORRE LA LISTA EN VEZ DE ESCRIBIR CINCO CASOS A MANO: el vocabulario tiene TRES sitios
// que hay que tocar a la vez (la lista de `app`, el enum del .proto y el switch del carril), y el modo
// natural de romperlo es añadir un sexto error y olvidar una mitad. Cinco casos escritos a mano seguirían
// verdes para siempre con el sexto sin cubrir; recorriendo la lista, el sexto entra en el test el mismo
// día que entra en el vocabulario.
//
// Cada error se prueba DOS VECES: devuelto pelado y devuelto ENVUELTO. El puerto promete «uno de los
// cinco, envuelto o no» (app.ServidorInferencia), así que el servidor tiene que extraerlo con
// EsErrorInferencia y no comparar por identidad — comparar por identidad funcionaría con el pelado y
// convertiría el envuelto en un OLLAMA_DOWN silencioso.
func TestLosCincoErroresCanonicosSobrevivenAlViaje(t *testing.T) {
	ctx, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelar()

	// El contrato (ADR-0045 §2) dice CINCO. Si esta cifra cambia hay tres sitios que mover; y sin este
	// guardia, una lista vaciada haría que el bucle de abajo no ejecutara ni una afirmación.
	if len(app.ErroresInferencia) != 5 {
		t.Fatalf("app.ErroresInferencia tiene %d entradas y el contrato define 5: "+
			"si el vocabulario creció, hay que moverlo también en el enum del .proto y en el switch del carril",
			len(app.ErroresInferencia))
	}

	for _, canonico := range app.ErroresInferencia {
		t.Run(canonico.Codigo(), func(t *testing.T) {
			a := montar(ctx, t, func(_ context.Context, p app.PeticionInferencia) (app.RespuestaInferencia, error) {
				if p.CommandID == "envuelto" {
					return app.RespuestaInferencia{}, fmt.Errorf("el proveedor local dijo que no: %w", canonico)
				}
				return app.RespuestaInferencia{}, canonico
			})

			for _, forma := range []string{"pelado", "envuelto"} {
				_, err := a.cli.Inferir(ctx, app.PeticionInferencia{
					CommandID: forma, Prompt: "da igual", Timeout: 3 * time.Second,
				})
				if !errors.Is(err, canonico) {
					t.Fatalf("[%s] el error %q no sobrevivió al socket: obtuve %v", forma, canonico.Codigo(), err)
				}
				// Y no encaja con ninguno de los otros cuatro: el veredicto es UNO, no «alguno de la
				// familia». INV-051.3 exige contarlos por separado, y eso sólo es posible si son
				// distinguibles al llegar.
				for _, otro := range app.ErroresInferencia {
					if otro != canonico && errors.Is(err, otro) {
						t.Errorf("[%s] %q llegó también como %q: los códigos no son distinguibles al otro lado",
							forma, canonico.Codigo(), otro.Codigo())
					}
				}
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 3 · UN CÓDIGO DESCONOCIDO NO SE PARECE A NINGUNO
// ─────────────────────────────────────────────────────────────────────────────

// TestUnCodigoDesconocidoNoSeResuelveAlMasParecidoYSeCita cubre el caso de binarios desalineados: el
// cajero de una versión y el daemon de otra.
//
// La tentación es resolver «al más parecido» (un código con otra caja, un sufijo, un sinónimo) y así
// «no perder» la respuesta. Sería el peor de los desenlaces: convertiría un problema de DESPLIEGUE en un
// diagnóstico sobre la MÁQUINA DEL CLIENTE, y el dueño del equipo se pondría a mirar un Ollama sano. Lo
// correcto es degradar como fallo del proveedor pero CITANDO el código recibido, que es el único dato
// que hace visible el desalineamiento.
//
// El segundo subtest usa `TIMEOUT` en mayúsculas justamente porque es el «parecido» más creíble.
func TestUnCodigoDesconocidoNoSeResuelveAlMasParecidoYSeCita(t *testing.T) {
	ctx, cancelar := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelar()

	for _, codigoRaro := range []string{"modelo_en_huelga", "TIMEOUT"} {
		t.Run(codigoRaro, func(t *testing.T) {
			// Premisa del test: el código NO puede estar en el vocabulario. Sin este guardia, elegir por
			// descuido un código válido haría que el test probara el camino contrario sin avisar.
			if _, ok := app.ErrorInferenciaDe(codigoRaro); ok {
				t.Fatalf("%q SÍ está en el vocabulario: este test no está probando lo que dice", codigoRaro)
			}

			anotador := &fallos{}
			t.Cleanup(func() { anotador.volcar(t) })
			cli := montarCrudo(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				if _, err := io.WriteString(w, `{"error":"`+codigoRaro+`"}`); err != nil {
					anotador.anota("escribiendo el cuerpo: %v", err)
				}
			})

			_, err := cli.Inferir(ctx, app.PeticionInferencia{
				CommandID: "cmd-desalineado", Prompt: "da igual", Timeout: 3 * time.Second,
			})
			if err == nil {
				t.Fatalf("un 503 con un código desconocido se tragó sin error")
			}
			// Degrada como fallo del proveedor: el Cloud necesita UNO de los cinco para decidir.
			if !errors.Is(err, app.ErrInferenciaOllamaCaido) {
				t.Errorf("se esperaba degradar como %q; obtuve %v", app.ErrInferenciaOllamaCaido.Codigo(), err)
			}
			// Pero CITANDO el código: sin esta cita, un despliegue desalineado se ve exactamente igual
			// que un Ollama parado y nadie lo encuentra nunca.
			if !strings.Contains(err.Error(), codigoRaro) {
				t.Errorf("el error no cita el código recibido %q: %v", codigoRaro, err)
			}
			// Y no se «parece» a ninguno de los otros cuatro.
			for _, otro := range app.ErroresInferencia {
				if otro != app.ErrInferenciaOllamaCaido && errors.Is(err, otro) {
					t.Errorf("%q se resolvió al parecido %q en vez de tratarse como desconocido",
						codigoRaro, otro.Codigo())
				}
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 4 · SOCKET RANCIO SÍ, FICHERO REGULAR NO
// ─────────────────────────────────────────────────────────────────────────────

// TestEscucharLimpiaElSocketRancioPeroSeNiegaABorrarUnFicheroRegular custodia la asimetría de Escuchar,
// que es una decisión y no un detalle: los dos casos son «ya hay algo en la ruta» y las respuestas son
// opuestas.
//
//   - Un socket huérfano es basura NUESTRA (la deja un SIGKILL, que no ejecuta ningún defer). Borrarlo
//     es lo que permite que el cajero vuelva a arrancar sin intervención; si no lo hiciera, un mátalo-y-
//     arráncalo dejaría el Edge sin servicio de inferencia hasta que alguien borrase un fichero a mano.
//   - Un fichero regular es un dato AJENO: una ruta mal configurada apuntando a algo del usuario. Ahí
//     borrar sería destruir datos, y la promesa que se prueba abajo no es «falla» sino «NO DESTRUYE» —
//     por eso se comprueba el CONTENIDO del fichero, no sólo su existencia.
func TestEscucharLimpiaElSocketRancioPeroSeNiegaABorrarUnFicheroRegular(t *testing.T) {
	ctx, cancelar := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelar()
	silencioso := sharedlogger.New(sharedlogger.WithWriter(io.Discard))

	t.Run("socket rancio de un arranque previo: se limpia y se vuelve a escuchar", func(t *testing.T) {
		ruta := rutaSocket(t)

		// Se reproduce el SIGKILL pidiéndole al listener que NO desenlace al cerrar, que es exactamente
		// lo que el kernel tampoco hace cuando el proceso muere sin ejecutar sus defers.
		previo, err := net.Listen("unix", ruta)
		if err != nil {
			t.Fatalf("net.Listen unix %q: %v", ruta, err)
		}
		unixLn, ok := previo.(*net.UnixListener)
		if !ok {
			t.Fatalf("net.Listen unix devolvió %T y no un *net.UnixListener", previo)
		}
		unixLn.SetUnlinkOnClose(false)
		if err := previo.Close(); err != nil {
			t.Fatalf("cerrando el listener previo: %v", err)
		}
		// Premisa: el fichero sigue ahí y sigue siendo un socket. Si el cierre lo hubiera borrado, el
		// caso de abajo sería «escuchar en una ruta libre» y no probaría la limpieza.
		info, err := os.Lstat(ruta)
		if err != nil {
			t.Fatalf("el socket rancio no quedó en disco: %v", err)
		}
		if info.Mode()&os.ModeSocket == 0 {
			t.Fatalf("lo que quedó en %q no es un socket (modo %v)", ruta, info.Mode())
		}

		ln, err := cajerosock.Nuevo(ctx, ruta, nil, silencioso).Escuchar()
		if err != nil {
			t.Fatalf("Escuchar sobre un socket rancio falló: %v", err)
		}
		t.Cleanup(func() {
			if err := ln.Close(); err != nil {
				t.Errorf("cerrando el listener: %v", err)
			}
		})

		// Y el socket nuevo nace 0600: el único que puede conectarse es un proceso del mismo usuario, que
		// es la razón por la que el servidor no lleva ni autenticación ni más plazos.
		nuevo, err := os.Lstat(ruta)
		if err != nil {
			t.Fatalf("Lstat del socket nuevo: %v", err)
		}
		if perm := nuevo.Mode().Perm(); perm != 0o600 {
			t.Errorf("permisos del socket: quería 0600, obtuve %#o", perm)
		}
	})

	t.Run("fichero regular: se niega y NO lo borra", func(t *testing.T) {
		ruta := rutaSocket(t)
		contenido := []byte("esto es un dato del usuario, no un socket\n")
		if err := os.WriteFile(ruta, contenido, 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}

		ln, err := cajerosock.Nuevo(ctx, ruta, nil, silencioso).Escuchar()
		if err == nil {
			if cerrar := ln.Close(); cerrar != nil {
				t.Errorf("cerrando el listener inesperado: %v", cerrar)
			}
			t.Fatalf("Escuchar aceptó una ruta ocupada por un fichero regular")
		}
		if !strings.Contains(err.Error(), ruta) {
			t.Errorf("el error no dice QUÉ ruta está ocupada, que es el dato con el que se arregla: %v", err)
		}

		leido, err := os.ReadFile(ruta)
		if err != nil {
			t.Fatalf("el fichero regular desapareció: Escuchar destruyó datos del usuario: %v", err)
		}
		if !bytes.Equal(leido, contenido) {
			t.Errorf("el fichero regular cambió de contenido:\n  quería %q\n  obtuve %q", contenido, leido)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// 5 · EL RTT — LO QUE CONVIERTE «INMEDIATO» EN UN NÚMERO
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkRTTSocket mide el ida y vuelta completo de `POST /inferencia` cuando el puerto rechaza SIN
// intentar nada — el caso del breaker abierto.
//
// 🔴 ESTE NÚMERO ES LA JUSTIFICACIÓN DEL DISEÑO, no una curiosidad. El docstring del paquete descarta la
// alternativa (una tabla más en cola_entrantes.db que el cajero sondee) con el argumento de que el
// ADR-0045 exige que un breaker abierto responda INMEDIATAMENTE, y que con un sondeo de 500 ms ese
// «inmediatamente» cuesta hasta ~1 s ida y vuelta. Ese argumento sólo se sostiene si el socket cuesta
// órdenes de magnitud menos, y «órdenes de magnitud menos» es una afirmación que hay que medir. Correr
// esto es lo que la convierte en verificable:
//
//	go test -run '^$' -bench BenchmarkRTTSocket -benchtime=200x ./internal/adapters/cajerosock/
//
// El bucle AFIRMA el desenlace además de medirlo: un benchmark que no comprueba qué devolvió puede estar
// cronometrando un camino de error y dar un número precioso que no significa nada.
//
// MEDIDO el 2026-08-23 (Apple M1 Pro, darwin/arm64, -benchtime=200x, 6 corridas, la máquina con carga
// media 14 por otros trabajos en paralelo): 43–70 µs/op, mediana ~53 µs — es decir 0,05 ms, no 1 ms. El
// docstring del paquete dice «~1 ms» y se queda CORTO en un orden de magnitud, cosa que no debilita su
// argumento sino que lo refuerza: contra el ~1 s de un sondeo de 500 ms, el socket cuesta cuatro órdenes
// de magnitud menos. Una corrida aislada con la máquina más cargada dio 218 µs; incluso ese peor caso
// observado sigue tres órdenes por debajo del sondeo.
func BenchmarkRTTSocket(b *testing.B) {
	ctx := context.Background()
	a := montar(ctx, b, func(_ context.Context, _ app.PeticionInferencia) (app.RespuestaInferencia, error) {
		return app.RespuestaInferencia{}, app.ErrInferenciaBreakerAbierto
	})
	pet := app.PeticionInferencia{CommandID: "bench-breaker", Prompt: "da igual"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := a.cli.Inferir(ctx, pet); !errors.Is(err, app.ErrInferenciaBreakerAbierto) {
			b.Fatalf("el benchmark no está midiendo el rechazo inmediato: %v", err)
		}
	}
}

// BenchmarkRTTSocketSalida2KB mide el mismo ida y vuelta en el camino feliz, con una salida del tamaño
// de una respuesta real del modelo (~2 KB). Sirve para saber si el TAMAÑO DEL CUERPO mueve la aguja: si
// este número y el del breaker están en el mismo orden, el coste del canal es el viaje y no los datos, y
// entonces el techo de 8 MiB de MaxCuerpo no tiene ninguna consecuencia práctica sobre la latencia.
//
// MEDIDO el 2026-08-23 (mismas condiciones): 75–113 µs/op, mediana ~105 µs. O sea que 2 KB de salida
// cuestan unos 50 µs más que un rechazo vacío: el tamaño SÍ se nota, pero deja el ida y vuelta en el
// mismo orden de magnitud (décimas de milisegundo). El canal lo domina el viaje, no los datos.
func BenchmarkRTTSocketSalida2KB(b *testing.B) {
	ctx := context.Background()
	salida := `{"texto":"` + strings.Repeat("a", 2048) + `"}`
	a := montar(ctx, b, func(_ context.Context, _ app.PeticionInferencia) (app.RespuestaInferencia, error) {
		return app.RespuestaInferencia{RawJSON: salida}, nil
	})
	pet := app.PeticionInferencia{CommandID: "bench-2kb", Prompt: "da igual"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := a.cli.Inferir(ctx, pet)
		if err != nil {
			b.Fatalf("el benchmark no está midiendo el camino feliz: %v", err)
		}
		if len(resp.RawJSON) != len(salida) {
			b.Fatalf("la salida llegó truncada: %d bytes de %d", len(resp.RawJSON), len(salida))
		}
	}
}
