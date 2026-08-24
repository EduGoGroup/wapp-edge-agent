package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cajerosock"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/cajero"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/wiring"
	"github.com/EduGoGroup/wapp-edge-intent/ollama"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// runCajero cablea y ejecuta el WORKER-CAJERO (Plan 051 Ola 2 · T2.2, `agent cajero`), el tercer hijo
// de `wapp-ctl`. Mismo molde que `agent serve`: misma config, mismo logger, mismo Layout — papel
// distinto.
//
// 🔧 EL CAJERO SÍ LEVANTA UN SOCKET PROPIO DESDE EL 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-2, ADR-0045),
// y esta decisión REVIERTE la que estaba escrita aquí. Conviene dejar dicho qué cambió, porque la
// justificación anterior no era mala: era CORRECTA PARA EL OFICIO QUE EL CAJERO TENÍA ENTONCES.
//
//	ANTES decía: «NO levanta plano de control propio. Su readiness es "el proceso está vivo" y de eso se
//	encarga el supervisor (wapp-ctl, con Restart automático). Un socket /v1 aquí sería una segunda
//	superficie que mantener para EXPONER SEIS CONTADORES.»
//
// Esa frase pesaba un socket contra su beneficio, y el beneficio era telemetría. Con el ADR-0045 el
// cajero deja de clasificar por iniciativa propia y pasa a SERVIR inferencia al Cloud: el socket ya no
// expone contadores, es EL ÚNICO CAMINO por el que el trabajo llega al proceso. Sin él, `agent serve`
// —que es quien recibe el frame `inference_request`— tendría que hablar con Ollama, y eso rompe
// REQ-051.10. No es que el argumento de antes fuera flojo; es que la pieza cambió de oficio y con ella
// el lado en que cae la balanza.
//
// LO QUE **NO** ES ESE SOCKET, para que nadie lo confunda: no es un plano de control `/v1`. No tiene
// autenticación de operador, no expone estado ni contadores, y su único endpoint es `POST /inferencia`
// (internal/adapters/cajerosock). La readiness del cajero SIGUE siendo «el proceso está vivo», y de eso
// sigue encargándose el supervisor.
//
// ✅ REQ-051.10 SE CUMPLE DESDE T3.0 («ningún otro proceso que el worker habla con Ollama»). Durante la
// Ola 1 no se cumplía: `agent serve` clasificaba en línea con su decorador y había DOS clientes de Ollama
// en la máquina. La Ola 3 retiró aquel decorador, y desde entonces el `ollama.New` de más abajo es el
// ÚNICO del repo fuera de un test con build tag. Si alguien vuelve a instanciar un cliente de Ollama en
// `agent serve`, el requisito se rompe otra vez aunque el decorador no vuelva: el gate es el grep, no el
// nombre de la pieza.
func runCajero(ctx context.Context, cfg config.Config, log sharedlogger.Logger) error {
	if !cfg.Intent.Enabled {
		// Sin LLM no hay nada que servir, pero SALIR sería peor: el supervisor lo interpretaría como caída
		// y lo reiniciaría en bucle. Se aparca hasta SIGTERM, que es la forma honesta de decir «estoy vivo
		// y no tengo trabajo».
		//
		// 🔴 CON LA FEATURE APAGADA NO SE LEVANTA EL SOCKET, y esa ausencia ES la respuesta: el daemon no
		// podrá conectar y traducirá el fallo a OLLAMA_DOWN, que es la verdad operativa —desde `agent
		// serve`, el proveedor local de LLM es este proceso, y este proceso no está sirviendo—. Levantar el
		// socket para responder «apagado» exigiría un sexto código en el contrato que no existe, y el que
		// más se le parece diría algo falso.
		log.Warn("cajero: el LLM local está DESHABILITADO (WAPP_AGENT_INTENT_ENABLED=false); el worker queda " +
			"inactivo y NO levanta su socket de inferencia (el daemon verá OLLAMA_DOWN)")
		<-ctx.Done()
		return nil
	}

	// ── Las colas: UNA POR INSTALACIÓN, en round-robin ────────────────────────
	// T4.1: el cajero deja de operar sobre UNA cola y pasa a turnarse entre las de la lista
	// WAPP_WORKER_DATA_DIRS (default: el `data_dir` único de siempre, así que una instalación sola no
	// nota ninguna diferencia). Cada data_dir es una instalación con su propio cola_entrantes.db.
	dataDirs := cfg.Worker.DataDirs
	if len(dataDirs) == 0 {
		// Defensa en profundidad: config.Load ya garantiza al menos una entrada (normalizarDataDirs cae a
		// []string{cfg.DataDir}). Esto sólo cubre a un llamante que fabricara la Config a mano — sin ello,
		// el cajero arrancaría con cero colas y se quedaría vivo sin reclamar nada, en silencio.
		dataDirs = []string{cfg.DataDir}
	}

	// 🔴 EL CIERRE NO PUEDE SER UN `defer` DENTRO DEL BUCLE, y esto no es estilo. El orden LIFO de los
	// defer de esta función es LOAD-BEARING (ver el bloque del paro del refresco, más abajo): el refresco
	// tiene que pararse ANTES de que se cierren las BDs. Un `defer colaDB.Close()` por vuelta apilaría N
	// cierres en medio de esa secuencia y el razonamiento dejaría de ser comprobable de un vistazo. En su
	// lugar cada apertura devuelve SU función de cierre, se acumulan, y `cerrarColas` las invoca EN ORDEN
	// INVERSO —el mismo que tendrían como defer— desde un único punto.
	var colas []cajero.ColaNombrada
	var cierres []func()
	cerrarColas := func() {
		for i := len(cierres) - 1; i >= 0; i-- {
			cierres[i]()
		}
	}
	for _, dataDir := range dataDirs {
		cola, cerrar, err := abrirCola(ctx, cfg, dataDir, log)
		if err != nil {
			cerrarColas() // el `defer` de abajo aún no está registrado: se cierra a mano lo ya abierto
			return err
		}
		colas = append(colas, cola)
		cierres = append(cierres, cerrar)
	}
	defer cerrarColas()

	// ── El proveedor local de LLM ─────────────────────────────────────────────
	//
	// 🔴 AQUÍ VIVÍA EL CLASIFICADOR, Y SE RETIRÓ ENTERO EL 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-2,
	// ADR-0045). Había un `classifier.New(...)` con su prompt y su schema, y un `contratoIntenciones` que
	// sondeaba `edge.db` cada 30 s para recargarlo en caliente. Los dos existían para que ESTE proceso
	// supiera clasificar por su cuenta; bajo pull no clasifica por iniciativa propia, así que se quedaron
	// sin llamante y se borran (un objeto sin llamante es deuda, no previsión).
	//
	// CON ELLOS SE FUE LA ÚNICA RAZÓN POR LA QUE EL CAJERO ABRÍA `edge.db`, y con ella la decisión de T4.1
	// sobre «el contrato es uno, el del primer data_dir» y su límite declarado (dos instalaciones con
	// contratos distintos clasificándose con el de la primera). Ese límite ya no existe porque el contrato
	// de intenciones ya no lo lee este proceso: lo usa el CLOUD para armar el prompt, y lo que baja por el
	// frame es el prompt ya construido.
	//
	// LO QUE SÍ SE CONSERVA es el cliente de Ollama. `ollama.New` de abajo sigue siendo el ÚNICO del repo
	// fuera de un test con build tag: ese grep es el gate de REQ-051.10.
	client := ollama.New(cfg.Intent.OllamaURL)

	// ── El bucle ──────────────────────────────────────────────────────────────
	c, err := cajero.New(cajero.Deps{
		// Colas (plural) y NO Cola: la lista es la fuente única desde T4.1, incluso con una entrada. Pasar
		// las dos cosas sería dejar dos verdades sobre lo mismo esperando a divergir.
		Colas:  colas,
		Ollama: client,
		// EL MODELO LO ELIGE EL EDGE y no viaja en el frame (ADR-0045): es propiedad de la máquina del
		// cliente. `WAPP_AGENT_INTENT_MODEL` sigue siendo la variable que lo fija.
		Modelo: cfg.Intent.Model,
		// Las tres opciones de modelo, EXPLÍCITAS. Antes se las pasaba el clasificador (WithLLMOptions) y
		// ahora se las pasa el servidor de inferencia; los valores son LOS MISMOS y salen de la misma
		// config, así que la máquina ve exactamente la petición que veía antes. La temperatura NO va aquí:
		// tiene su propio campo porque el Cloud puede sobreescribirla por petición.
		Opciones: map[string]any{
			"num_thread":  cfg.Worker.NumThread,
			"num_predict": cfg.Worker.NumPredict,
			"num_ctx":     cfg.Worker.NumCtx,
		},
		Despertador:   cajero.NewPollFijo(time.Duration(cfg.Worker.PollMS) * time.Millisecond),
		Log:           log,
		MaxConcurrent: cfg.Worker.MaxConcurrent,
		Lease:         time.Duration(cfg.ColaLeaseSeconds) * time.Second,
		// El plazo por DEFECTO de una inferencia (cuando el Cloud manda `timeout_ms=0`). El argumento del
		// número —y por qué el 15 s anterior estaba calibrado contra una muestra CENSURADA por él mismo—
		// está en cajero.DefaultInferenceTimeoutMS.
		Timeout: time.Duration(cfg.Worker.InferenceTimeoutMS) * time.Millisecond,
		// Y el TECHO de lo que el Cloud puede pedir: sin él, un `timeout_ms` mal calculado arriba ocuparía
		// la única plaza de Ollama el tiempo que dijera.
		TimeoutMax: time.Duration(cfg.Inference.MaxTimeoutMS) * time.Millisecond,
		StatsEvery: time.Duration(cfg.Worker.StatsEveryMS) * time.Millisecond,
		OllamaURL:  cfg.Intent.OllamaURL,
		// T2.8 · el mismo número que se le manda a Ollama arriba, aquí sólo para que el aviso de
		// sobresuscripción compare lo que DE VERDAD se está pidiendo y no el default.
		NumThread: cfg.Worker.NumThread,
	})
	if err != nil {
		return err
	}

	// ── EL SOCKET DE INFERENCIA: UNO POR data_dir ─────────────────────────────
	//
	// POR QUÉ N SOCKETS Y NO UNO: el cliente es el `agent serve` de CADA instalación, y cada uno conoce
	// exactamente un directorio —el suyo—. Un socket único obligaría a que los N daemons supieran cuál de
	// los data_dir's es el «principal», que es un acoplamiento nuevo entre instalaciones que hoy no se
	// conocen entre sí. Con un socket por data_dir, cada daemon busca `<su data_dir>/cajero.sock` y no hay
	// nada que configurar.
	//
	// 🔴 LOS N SOCKETS COMPARTEN UN SOLO SERVIDOR DE DOMINIO, y por tanto UN SOLO AFORO, UN SOLO BREAKER y
	// UN SOLO HISTOGRAMA. Es la propiedad que no se puede romper: N aforos de una plaza serían N
	// inferencias simultáneas contra el mismo Ollama —el solapamiento que la O0 midió como causa de que el
	// p50 se dispare—. Por eso el servidor sale de `c.ServidorInferencia()` (el Cajero es su dueño) y se
	// pasa EL MISMO a los N listeners, en vez de construir uno por socket.
	puerto := c.ServidorInferencia()
	if puerto == nil {
		// No debería ocurrir: Deps.Ollama está poblado unas líneas más arriba. Se comprueba porque un nil
		// aquí daría N sockets que responden OLLAMA_DOWN a todo sin que nadie sepa por qué.
		return fmt.Errorf("cajero: no se pudo construir el servidor de inferencia (¿proveedor local nil?)")
	}

	// MISMO PATRÓN DE CIERRE QUE LAS COLAS, y por el mismo motivo: los cierres se acumulan y se invocan en
	// orden INVERSO desde un único punto, en vez de apilar N `defer` dentro del bucle. Aquí importa además
	// que el apagado de los sockets ocurra ANTES de que se cierren las BDs (los defer de arriba), porque
	// una inferencia en vuelo puede estar publicando su parte.
	var sockets []*cajerosock.Servidor
	var servidores sync.WaitGroup
	cerrarSockets := func() {
		for i := len(sockets) - 1; i >= 0; i-- {
			// El plazo del drenaje es el techo de UNA inferencia: es exactamente lo que puede quedar en
			// vuelo. Cortar antes tiraría trabajo ya pagado; esperar más dejaría al supervisor mandando
			// SIGKILL sobre un proceso que se está tomando su tiempo.
			ctxCierre, cancelCierre := context.WithTimeout(context.WithoutCancel(ctx),
				time.Duration(cfg.Inference.MaxTimeoutMS)*time.Millisecond)
			if err := sockets[i].Apagar(ctxCierre); err != nil {
				log.Warn("cajero: el socket de inferencia no cerró limpiamente", "error", err)
			}
			cancelCierre()
		}
		servidores.Wait()
	}
	for _, dataDir := range dataDirs {
		layout := sessionmgr.NewLayout(dataDir)
		srv := cajerosock.Nuevo(ctx, layout.CajeroSock(), puerto, log)
		ln, err := srv.Escuchar()
		if err != nil {
			cerrarSockets() // el defer de abajo aún no está registrado: se cierra a mano lo ya abierto
			// ES FATAL, con el mismo criterio que la cola: sin socket, el daemon de esta instalación no
			// puede pedir NI UNA inferencia y el único síntoma sería un OLLAMA_DOWN permanente con Ollama
			// perfectamente vivo. Arrancar sería prometer un servicio que no existe.
			return fmt.Errorf("cajero: abrir el socket de inferencia de %s: %w", dataDir, err)
		}
		sockets = append(sockets, srv)
		servidores.Add(1)
		go func(srv *cajerosock.Servidor, ln net.Listener) {
			defer servidores.Done()
			if err := srv.Servir(ln); err != nil {
				log.Error("cajero: el socket de inferencia falló", "error", err)
			}
		}(srv, ln)
	}
	defer cerrarSockets()

	log.Info("agent cajero: el Edge SIRVE inferencia al Cloud por socket (ADR-0045); ya no clasifica por iniciativa propia",
		"data_dir", cfg.DataDir,
		// `colas` es cuántas instalaciones atiende y `data_dirs` cuáles; `sockets` coincide con las dos
		// porque hay exactamente un socket de inferencia por instalación.
		"colas", len(colas),
		"sockets", len(sockets),
		"data_dirs", strings.Join(dataDirs, ","),
		"ollama_url", cfg.Intent.OllamaURL,
		"model", cfg.Intent.Model,
		"max_concurrent", cfg.Worker.MaxConcurrent,
		"poll_ms", cfg.Worker.PollMS,
		"num_thread", cfg.Worker.NumThread,
		"num_predict", cfg.Worker.NumPredict,
		"num_ctx", cfg.Worker.NumCtx,
		"inferencia_timeout_ms", cfg.Worker.InferenceTimeoutMS,
		"inferencia_timeout_max_ms", cfg.Inference.MaxTimeoutMS,
		"stats_cada_ms", cfg.Worker.StatsEveryMS,
		// T4.5 · la cadencia del PARTE va al lado de la del latido de log para que se vean juntas y se
		// entienda que son DOS relojes distintos: `stats_cada_ms` es verbosidad de log (y un 0 la apaga),
		// `parte_cada_s` es la frescura de la señal de salud que llega a la nube, y no se puede apagar.
		"parte_cada_s", int(app.ParteCada.Seconds()),
	)

	// El cierre de los sockets y de las colas lo hacen los defer de arriba, en TODOS los caminos de salida,
	// y en el orden inverso al de apertura: {sockets} → {las N colas}.
	return c.Run(ctx)
}

// abrirCola abre, migra y construye la cola de UNA instalación (un data_dir) y devuelve, además de la
// cola ya nombrada, la función que la cierra.
//
// DEVUELVE EL CIERRE EN VEZ DE REGISTRAR UN `defer`, y esa es toda su razón de ser: el llamante abre N
// colas en un bucle y necesita cerrarlas en orden inverso DENTRO de la secuencia de defer que ya tiene
// (ver el comentario del bucle en runCajero). Una función que registrase su propio defer lo haría en el
// ámbito equivocado —el suyo— y cerraría la BD antes de devolverla.
//
// El `cerrar` devuelto es IDEMPOTENTE en la práctica (Close sobre un *sql.DB ya cerrado devuelve error, y
// aquí se ignora) y nunca es nil cuando err == nil.
func abrirCola(ctx context.Context, cfg config.Config, dataDir string, log sharedlogger.Logger) (cajero.ColaNombrada, func(), error) {
	layout := sessionmgr.NewLayout(dataDir)

	// Se abre y migra igual que en el daemon (internal/infra/daemon): es la MISMA BD propia de la cola
	// (<data_dir>/cola_entrantes.db) y las migraciones son idempotentes, así que da igual quién de los
	// dos procesos arranque primero.
	//
	// ES FATAL, y desde el 2026-08-17 (Plan 051 O3) TAMBIÉN LO ES EN EL DAEMON. Los dos procesos que abren
	// este fichero tratan su ausencia igual, por razones distintas y ambas suficientes: sin cola el cajero
	// no tiene ninguna otra cosa que hacer (se quedaría en un bucle que no reclama nada), y sin cola el
	// daemon no tiene camino de entrega (perdería cada entrante con el socket conectado). Fallar el
	// arranque es lo honesto en los dos casos, y que la política sea la misma evita que el operador tenga
	// que recordar cuál de los dos hijos aguanta sin cola.
	//
	// CON N INSTALACIONES SIGUE SIENDO FATAL, y a propósito: arrancar con 4 de 5 colas dejaría a una
	// empresa entera sin clasificar mientras el proceso se declara sano, y el único síntoma sería una
	// línea de log en el arranque que nadie vuelve a leer. Por eso todos los errores de aquí nombran el
	// data_dir: con cinco instalaciones, «no se pudo abrir la BD de la cola» sin decir cuál no es un
	// diagnóstico, es un acertijo.
	//
	// db.OpenCola, no db.Open, y AQUÍ importa más que en ningún sitio: este proceso es el que escribe los
	// UPDATE de lote, o sea el que provocaba los checkpoints que frenaban al handler del daemon (T3.15).
	// `synchronous` y `wal_autocheckpoint` son pragmas por-conexión: si el cajero abriera con el perfil
	// conservador, el perfil del daemon no le afectaría y el problema seguiría exactamente igual.
	colaDB, err := db.OpenCola(ctx, layout.ColaDB())
	if err != nil {
		return cajero.ColaNombrada{}, nil, fmt.Errorf("cajero: abrir la BD de la cola (%s): %w", layout.ColaDB(), err)
	}
	cerrar := func() { _ = colaDB.Close() }

	if err := db.MigrateCola(ctx, colaDB); err != nil {
		cerrar()
		return cajero.ColaNombrada{}, nil, fmt.Errorf("cajero: migrar la BD de la cola de %s: %w", dataDir, err)
	}

	// La custodia de la DEK sale del MISMO criterio que el daemon (Plan 035 · DIP): un único punto de
	// verdad sobre dónde vive la DEK. Reusar wiring.BuildCola —y no reconstruir el crypterFor a mano—
	// es lo que garantiza que el worker abre los sobres con exactamente la misma llave con la que el
	// listener los selló. Zero-knowledge intacto: la DEK se resuelve en local y no se loguea.
	//
	// EL `layout` ES EL DE ESTE data_dir, no el del daemon: es de él de donde BuildCola deriva
	// <data_dir>/sessions/<id>/dek.key. Pasar el layout equivocado daría un cajero que abre la cola de una
	// instalación con las DEKs de otra — filas que no se pueden descifrar, y ni un solo error hasta el
	// primer claim.
	custodyFor := func(p string) app.KeyCustody { return keycustody.NewFileCustody(p) }
	colaPort := wiring.BuildCola(ctx, cfg, colaDB, layout, custodyFor, log)
	if colaPort == nil {
		cerrar()
		return cajero.ColaNombrada{}, nil, fmt.Errorf(
			"cajero: la cola de entrantes de %s no se pudo construir (ver el error anterior)", dataDir)
	}

	// LA COSTURA: BuildCola devuelve app.ColaEntrantes (el lado del listener) porque su llamante
	// histórico es el daemon, pero el objeto concreto que hay detrás (*colaentrantes.Store) implementa
	// TAMBIÉN app.ColaCajero. Se resuelve con una ASERCIÓN DE INTERFAZ a interfaz en vez de (a) cambiar
	// la firma de BuildCola a devolver el tipo concreto —que rompería la dirección hexagonal para todos
	// sus llamantes— o (b) escribir aquí un crypterFor paralelo —que duplicaría el criterio de custodia
	// justo donde una divergencia se traduce en filas que no se pueden descifrar—. El precio es que el
	// fallo se descubre en tiempo de ejecución; se paga con este error explícito, en el arranque, no en
	// mitad de un claim.
	colaCajero, ok := colaPort.(app.ColaCajero)
	if !ok {
		cerrar()
		return cajero.ColaNombrada{}, nil, fmt.Errorf(
			"cajero: la cola construida para %s no implementa app.ColaCajero (%T)", dataDir, colaPort)
	}

	// T4.5 · EL BUZÓN DEL PARTE, por la MISMA costura y con el mismo objeto concreto: el *Store que
	// respalda la cola implementa también app.ParteWorkerEscritor (internal/adapters/colaentrantes/
	// parte.go), porque el parte vive en una tabla de ESTA misma BD — que es justo lo que hace que el
	// daemon de esta instalación pueda leerlo sin ningún IPC nuevo.
	//
	// 🔴 AQUÍ LA ASERCIÓN FALLIDA NO ES FATAL, al revés que la de app.ColaCajero de arriba, y la asimetría
	// es la decisión: sin ColaCajero el worker no tiene NADA que hacer (no podría reclamar un solo lote),
	// mientras que sin buzón de parte clasifica exactamente igual y lo único que se pierde es que el
	// heartbeat de esta instalación publique `intent_circuit` — telemetría. Abortar el arranque de un
	// cajero que funciona porque no puede contar cómo está sería cambiar una degradación por una caída.
	// Se avisa en Warn (no en Info: es una pérdida de visibilidad que alguien tendrá que arreglar) y el
	// nil viaja hasta cajero.ColaNombrada.Parte, que lo trata como «esta cola no recibe parte».
	parteEscritor, ok := colaPort.(app.ParteWorkerEscritor)
	if !ok {
		log.Warn("cajero: la cola construida no implementa app.ParteWorkerEscritor; esta instalación NO publicará "+
			"su parte de salud y su heartbeat llevará `intent_circuit` vacío (la clasificación NO se ve afectada)",
			"data_dir", dataDir, "tipo", fmt.Sprintf("%T", colaPort))
	}

	// El NOMBRE de la cola es el data_dir: es lo que un operador con cinco instalaciones reconoce de un
	// vistazo en el log, y es material público (una ruta de directorio), nunca contenido de negocio.
	return cajero.ColaNombrada{Nombre: dataDir, Cola: colaCajero, Parte: parteEscritor}, cerrar, nil
}
