package main

// cajero_readiness_test.go — el AVISO del cajero al núcleo (Plan 044 · Ola 1.8 · T1.8-5, criterios (a) y
// (b) primera mitad).
//
// LO QUE SE PRUEBA AQUÍ ES EL ORDEN, NO LA PRESENCIA. Que el POST «listo» llegue alguna vez es fácil y no
// vale de nada: si llega DESPUÉS de que el socket haya empezado a servir, hay una ventana en la que el
// Cloud ya podría estar mandando trabajo a un Edge que aún se declara desconocido — que es exactamente la
// carrera que la ola vino a cerrar. Por eso hay un contador de secuencia COMPARTIDO entre el núcleo falso
// y el proveedor de inferencia falso: los dos anotan su turno en la misma escala y el test compara los
// dos números.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (las DOS se escribieron, se compilaron y se EJECUTARON el
// 2026-08-24; ninguna se dedujo):
//
//  1. ANUNCIAR DESPUÉS DE SERVIR — intercambiar las fases 2 y 3 de levantarSocketsDeInferencia, de modo
//     que el `go Servir(...)` arranque antes del POST «listo».
//     ⇒ ROJO en TestCajero_AvisoListo_LlegaAntesDeAtenderLaPrimeraPeticion: «ORDEN ROTO: el aviso
//     «listo» tomó el turno 2 y la primera petición atendida el 1».
//  2. NO ANUNCIAR «CAÍDO» — recortar a cero el bucle de anuncio del cierre ordenado (`dataDirs[:0]`).
//     ⇒ ROJO en TestCajero_ApagadoOrdenado_AnunciaCaido: «avisos = 1 ([{1 ready …}]), want 2».
//
// El transporte es REAL en las dos direcciones: un `http.Server` sobre un Unix socket haciendo de plano
// de control del núcleo, el `nucleoaviso.Cliente` de producción hablando con él, y el
// `inferenciacliente.Cliente` de producción pidiendo la inferencia por el socket del cajero. Lo único
// falso es el proveedor de LLM y el estado del núcleo.

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/server"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/inferenciacliente"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/nucleoaviso"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/logger"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// avisoAnotado es un POST /v1/inference/readiness tal y como lo vio el núcleo falso, con su TURNO en la
// escala compartida con el proveedor.
type avisoAnotado struct {
	turno     int64
	readiness string
	dataDir   string
}

// nucleoFalso es el plano de control del núcleo reducido a lo que esta prueba necesita: un socket unix
// que registra los avisos de readiness que le llegan. Usa el TIPO DE CABLE REAL (server.ReadinessRequest)
// para que un cambio del contrato rompa aquí y no en campo.
type nucleoFalso struct {
	socket string
	turnos *atomic.Int64
	// retardo es lo que el núcleo tarda en ATENDER el aviso.
	//
	// 🔴 ES LO QUE HACE QUE EL TEST DE ORDEN MIDA ALGO. Con un núcleo instantáneo, mover el anuncio detrás
	// del `go Servir(...)` seguiría saliendo verde: el POST local acabaría en microsegundos, antes de que
	// el test llegara a pedir nada. Con un núcleo lento hay una VENTANA de verdad, y si el socket ya está
	// sirviendo durante esa ventana, la petición que estaba esperando entra ANTES que el aviso y los
	// turnos lo delatan. La ventana la abre el test, no la sufre el producto: en campo el núcleo contesta
	// en microsegundos.
	retardo time.Duration

	mu     sync.Mutex
	avisos []avisoAnotado
}

// nuevoNucleoFalso levanta el socket y sirve hasta que acabe el test.
func nuevoNucleoFalso(t *testing.T, dir string, turnos *atomic.Int64, retardo time.Duration) *nucleoFalso {
	t.Helper()
	n := &nucleoFalso{socket: filepath.Join(dir, "edge.sock"), turnos: turnos, retardo: retardo}

	mux := http.NewServeMux()
	mux.HandleFunc("POST "+server.RutaReadinessInferencia, func(w http.ResponseWriter, r *http.Request) {
		var req server.ReadinessRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "cuerpo ilegible", http.StatusBadRequest)
			return
		}
		// El turno se anota DESPUÉS del retardo: lo que interesa es cuándo el núcleo LO SABE, no cuándo le
		// llamaron.
		time.Sleep(n.retardo)
		n.mu.Lock()
		n.avisos = append(n.avisos, avisoAnotado{
			turno: n.turnos.Add(1), readiness: req.Readiness, dataDir: req.DataDir,
		})
		n.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(server.ReadinessResponse{
			Readiness: req.Readiness, Applied: true, Changed: true,
		})
	})

	ln, err := net.Listen("unix", n.socket)
	if err != nil {
		t.Fatalf("levantar el núcleo falso en %s: %v", n.socket, err)
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return n
}

// vistos devuelve una COPIA de los avisos registrados.
func (n *nucleoFalso) vistos() []avisoAnotado {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]avisoAnotado(nil), n.avisos...)
}

// proveedorConTurno implementa app.ServidorInferencia y anota el TURNO de su primera invocación en la
// misma escala que los avisos. Ese número es la mitad del criterio (a).
type proveedorConTurno struct {
	turnos       *atomic.Int64
	primerTurno  atomic.Int64 // 0 = nunca lo llamaron
	unaSolaMarca sync.Once
}

func (p *proveedorConTurno) Inferir(context.Context, app.PeticionInferencia) (app.RespuestaInferencia, error) {
	p.unaSolaMarca.Do(func() { p.primerTurno.Store(p.turnos.Add(1)) })
	return app.RespuestaInferencia{RawJSON: `{"ok":true}`}, nil
}

// entornoCajero arma el escenario común: directorio corto, núcleo falso, proveedor con turno y logger.
//
// RUTAS CORTAS BAJO /tmp y no t.TempDir(): el `sun_path` de un Unix socket no admite las rutas largas que
// t.TempDir() fabrica en macOS (/var/folders/… con el nombre del test dentro). Mismo criterio, y por el
// mismo motivo, que serve_test.go y que control/server/server_test.go.
func entornoCajero(t *testing.T, retardoNucleo time.Duration) (dataDir string, nucleo *nucleoFalso, prov *proveedorConTurno, log sharedlogger.Logger) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "wapp-cajero-rdy-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	turnos := &atomic.Int64{}
	nucleo = nuevoNucleoFalso(t, dir, turnos, retardoNucleo)
	prov = &proveedorConTurno{turnos: turnos}
	log = logger.New(config.Config{LogLevel: "error"}) // silencioso: este test no inspecciona logs
	return dir, nucleo, prov, log
}

// pedirInferencia hace UNA petición real por el socket del cajero de esa instalación, con el cliente de
// producción. Devuelve el error tal cual para que el llamante lo clasifique.
func pedirInferencia(ctx context.Context, dataDir string) error {
	cli := inferenciacliente.Nuevo(filepath.Join(dataDir, "cajero.sock"))
	_, err := cli.Inferir(ctx, app.PeticionInferencia{
		CommandID: "cmd-orden", Prompt: "hola", Timeout: 2 * time.Second,
	})
	return err
}

// ─────────────────────────────────────────────────────────────────────────────
// (a) EL AVISO «LISTO» LLEGA ANTES DE QUE EL SOCKET ATIENDA LA PRIMERA PETICIÓN
// ─────────────────────────────────────────────────────────────────────────────

// TestCajero_AvisoListo_LlegaAntesDeAtenderLaPrimeraPeticion: tras levantar los sockets, el núcleo tiene
// registrado un «ready» de ESTA instalación, y su turno es ANTERIOR al de la primera llamada al
// proveedor.
//
// 🔴 LA COMPARACIÓN DE TURNOS ES EL TEST; el resto es andamiaje. Un test que sólo mirase «¿llegó el
// aviso?» seguiría verde si alguien moviera el anuncio detrás del `go Servir(...)` —la ventana volvería a
// abrirse y nadie se enteraría—. Con los turnos, ese movimiento pone esto rojo.
func TestCajero_AvisoListo_LlegaAntesDeAtenderLaPrimeraPeticion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Núcleo LENTO: 300 ms de ventana en la que el aviso está en vuelo. Ver nucleoFalso.retardo.
	dataDir, nucleo, prov, log := entornoCajero(t, 300*time.Millisecond)

	// 🔴 LA SONDA ARRANCA **ANTES** QUE EL CAJERO, y ahí está el filo del test. Es un cliente real que
	// pide inferencia en bucle: mientras el socket no existe se lleva un OLLAMA_DOWN y reintenta; en
	// cuanto existe, su conexión entra en el backlog del kernel y SE QUEDA ESPERANDO a que alguien la
	// atienda. Si el anuncio ocurriera con los `Servir` ya en marcha, esa conexión pendiente se atendería
	// durante los 300 ms del aviso y el proveedor tomaría su turno ANTES — que es exactamente la ventana
	// que esta tarea cierra. Pedir la inferencia DESPUÉS de que la función retorne no probaría nada: para
	// entonces las tres fases han terminado y los dos órdenes posibles salen verdes.
	servida := make(chan error, 1)
	go func() {
		for {
			err := pedirInferencia(ctx, dataDir)
			if err == nil || ctx.Err() != nil {
				servida <- err
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	cerrar, err := levantarSocketsDeInferencia(ctx, []string{dataDir}, prov,
		nucleoaviso.Nuevo(nucleo.socket), 2*time.Second, log)
	if err != nil {
		t.Fatalf("levantarSocketsDeInferencia: %v", err)
	}
	t.Cleanup(cerrar)

	select {
	case err := <-servida:
		if err != nil {
			t.Fatalf("la inferencia por el socket nunca llegó a servirse: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("timeout esperando a que el socket del cajero sirviera una inferencia: %v", ctx.Err())
	}

	avisos := nucleo.vistos()
	if len(avisos) != 1 {
		t.Fatalf("avisos recibidos por el núcleo = %d (%v), want exactamente 1 («listo»)", len(avisos), avisos)
	}
	if avisos[0].readiness != server.ReadinessListo {
		t.Errorf("readiness del aviso = %q, want %q", avisos[0].readiness, server.ReadinessListo)
	}
	// 🔴 EL AVISO IDENTIFICA LA INSTALACIÓN. Un aviso sin data_dir (o con otro) sería una señal global, y
	// con N instalaciones en la misma máquina cada una contaminaría la readiness de las demás.
	if avisos[0].dataDir != dataDir {
		t.Errorf("data_dir del aviso = %q, want %q (el aviso es POR INSTALACIÓN)", avisos[0].dataDir, dataDir)
	}

	primera := prov.primerTurno.Load()
	if primera == 0 {
		t.Fatal("el proveedor de inferencia no llegó a ser invocado: el test no midió ningún orden")
	}
	if avisos[0].turno >= primera {
		t.Errorf("ORDEN ROTO: el aviso «listo» tomó el turno %d y la primera petición atendida el %d. "+
			"El anuncio tiene que salir ANTES de que ningún socket empiece a servir: entre `Escuchar()` y "+
			"`Servir(ln)`, no después de los dos", avisos[0].turno, primera)
	}
}

// TestCajero_AvisoListo_UnoPorInstalacion: con dos data_dir's, el núcleo recibe DOS avisos, uno por cada
// uno, y ninguno habla en nombre de los dos.
//
// El cajero atiende N instalaciones con UN solo servidor de dominio pero N sockets; su readiness es un
// hecho por instalación y así tiene que viajar. Un único aviso «el cajero está listo» no diría de quién
// habla, y el núcleo que lo recibiera no podría saber si le toca.
func TestCajero_AvisoListo_UnoPorInstalacion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dataDirA, nucleo, prov, log := entornoCajero(t, 0)
	dataDirB := filepath.Join(dataDirA, "segunda")
	if err := os.MkdirAll(dataDirB, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	cerrar, err := levantarSocketsDeInferencia(ctx, []string{dataDirA, dataDirB}, prov,
		nucleoaviso.Nuevo(nucleo.socket), 2*time.Second, log)
	if err != nil {
		t.Fatalf("levantarSocketsDeInferencia: %v", err)
	}
	t.Cleanup(cerrar)

	avisos := nucleo.vistos()
	if len(avisos) != 2 {
		t.Fatalf("avisos = %d (%v), want 2 (uno por instalación)", len(avisos), avisos)
	}
	vistos := map[string]string{}
	for _, a := range avisos {
		vistos[a.dataDir] = a.readiness
	}
	for _, d := range []string{dataDirA, dataDirB} {
		if vistos[d] != server.ReadinessListo {
			t.Errorf("no hay aviso «listo» para la instalación %q (vistos: %v)", d, vistos)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (b) primera mitad — EL APAGADO ORDENADO ANUNCIA «CAÍDO»
// ─────────────────────────────────────────────────────────────────────────────

// TestCajero_ApagadoOrdenado_AnunciaCaido: la función de cierre —la que corre como `defer` de runCajero
// cuando llega el SIGTERM— manda un POST «down» por instalación, y lo manda ANTES de drenar.
//
// EL ORDEN «AVISO → DRENAJE» ES LA MITAD DEL VALOR: anunciarlo primero le da al Cloud la ventaja de dejar
// de mandar inferencias a este Edge mientras se termina lo que ya estaba en vuelo. Al revés, el aviso
// saldría cuando ya no queda nada que proteger.
func TestCajero_ApagadoOrdenado_AnunciaCaido(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dataDir, nucleo, prov, log := entornoCajero(t, 0)

	cerrar, err := levantarSocketsDeInferencia(ctx, []string{dataDir}, prov,
		nucleoaviso.Nuevo(nucleo.socket), 2*time.Second, log)
	if err != nil {
		t.Fatalf("levantarSocketsDeInferencia: %v", err)
	}

	// 🔴 EL ctx SE CANCELA ANTES DE CERRAR, que es como llega de verdad: el SIGTERM cancela el contexto
	// del proceso y sólo entonces corren los `defer`. Si el aviso de «caído» colgara de ese ctx en vez de
	// llevar `context.WithoutCancel`, moriría sin salir y este test se pondría rojo.
	cancel()
	cerrar()

	avisos := nucleo.vistos()
	if len(avisos) != 2 {
		t.Fatalf("avisos = %d (%v), want 2 («listo» al abrir y «caído» al cerrar)", len(avisos), avisos)
	}
	if avisos[1].readiness != server.ReadinessCaido {
		t.Errorf("el segundo aviso = %q, want %q", avisos[1].readiness, server.ReadinessCaido)
	}
	if avisos[1].dataDir != dataDir {
		t.Errorf("data_dir del «caído» = %q, want %q", avisos[1].dataDir, dataDir)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (b) segunda mitad — LA MUERTE POR SEÑAL NO ANUNCIA NADA
// ─────────────────────────────────────────────────────────────────────────────

// TestCajero_MuertePorSenal_NoAnunciaCaido: si el proceso muere por señal, el `defer` NO corre y por tanto
// NO sale ningún «caído». Lo que este test vigila es que no exista NINGÚN OTRO emisor de esa señal.
//
// 🔴 POR QUÉ NO ES TAUTOLÓGICO. El error natural al implementar esto es colgar el aviso de la cancelación
// del contexto («cuando el proceso se vaya, avisa»), que suena igual de bien y es falso: un SIGKILL no
// cancela ningún contexto, y un ctx cancelado NO significa que el socket haya dejado de servir. Por eso
// aquí se CANCELA EL CONTEXTO y se comprueba que, aun así, no sale ningún «caído» mientras nadie llame al
// cierre ordenado. Con un vigilante de `ctx.Done()` en el código, esto se pone rojo.
//
// LO QUE ESTE TEST **NO** REPRODUCE, dicho para que nadie lo lea de más: no mata un proceso de verdad.
// Simula la desaparición del cajero retirando su socket del sistema de ficheros. Un SIGKILL real DEJA el
// fichero (por eso cajerosock.Escuchar limpia sockets rancios) y el cliente obtiene ECONNREFUSED en vez
// de ENOENT; los dos caen en la MISMA rama de inferenciacliente.errorDeTransporte y producen el MISMO
// error canónico, que es lo que se comprueba abajo.
func TestCajero_MuertePorSenal_NoAnunciaCaido(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dataDir, nucleo, prov, log := entornoCajero(t, 0)

	cerrar, err := levantarSocketsDeInferencia(ctx, []string{dataDir}, prov,
		nucleoaviso.Nuevo(nucleo.socket), 2*time.Second, log)
	if err != nil {
		t.Fatalf("levantarSocketsDeInferencia: %v", err)
	}
	t.Cleanup(cerrar) // el test acaba limpio; lo que se mide ocurre ANTES de esta línea

	// El proceso "muere": su socket desaparece sin que nadie ejecute el cierre ordenado. Y se cancela el
	// contexto, que es el otro camino por el que alguien podría haber colgado el aviso.
	if err := os.Remove(filepath.Join(dataDir, "cajero.sock")); err != nil {
		t.Fatalf("retirar el socket del cajero: %v", err)
	}
	cancel()

	// La siguiente petición del NÚCLEO se topa con el vacío. Ese error es el que, del otro lado, pasa la
	// readiness a DOWN (ver internal/adapters/cloudlink/readiness_test.go: es el otro de los «dos caminos»).
	errInf := pedirInferencia(context.Background(), dataDir)
	if !errors.Is(errInf, app.ErrInferenciaOllamaCaido) {
		t.Fatalf("la petición contra un cajero muerto devolvió %v; want app.ErrInferenciaOllamaCaido, que es "+
			"la única señal por la que el núcleo aprende una muerte sin aviso", errInf)
	}

	// Y el núcleo NO recibió ningún «caído»: no hay más emisor que el cierre ordenado.
	for _, a := range nucleo.vistos() {
		if a.readiness == server.ReadinessCaido {
			t.Fatalf("salió un aviso «caído» sin que corriera el cierre ordenado (%v). Alguien colgó la señal "+
				"de la cancelación del contexto: un SIGKILL no cancela ningún contexto, y un contexto "+
				"cancelado no prueba que el socket haya dejado de servir", a)
		}
	}
}
