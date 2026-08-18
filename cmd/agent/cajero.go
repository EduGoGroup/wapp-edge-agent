package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/edgeconfig"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/cajero"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/daemon"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/wiring"
	"github.com/EduGoGroup/wapp-edge-intent/classifier"
	"github.com/EduGoGroup/wapp-edge-intent/ollama"
	"github.com/EduGoGroup/wapp-shared/intents"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// refrescoContrato es cada cuánto el cajero relee el contrato de intenciones de edge.db. Los
// ConfigUpdate del Cloud los recibe y persiste `agent serve` (es quien tiene el stream CloudLink); el
// cajero es otro proceso y no se entera del push, así que sondea. 30 s es holgado: cambiar el contrato
// de intenciones de un tenant no es una operación de segundos.
const refrescoContrato = 30 * time.Second

// runCajero cablea y ejecuta el WORKER-CAJERO (Plan 051 Ola 2 · T2.2, `agent cajero`), el tercer hijo
// de `wapp-ctl`. Mismo molde que `agent serve`: misma config, mismo logger, mismo Layout — papel
// distinto.
//
// LO QUE NO TIENE, y es deliberado: NO levanta plano de control propio. Su readiness es «el proceso
// está vivo» y de eso se encarga el supervisor (`wapp-ctl`, con Restart automático). Un socket /v1
// aquí sería una segunda superficie que mantener para exponer seis contadores.
//
// ✅ REQ-051.10 SE CUMPLE DESDE T3.0 («ningún otro proceso que el worker habla con Ollama»). Durante la
// Ola 1 no se cumplía: `agent serve` clasificaba en línea con su decorador y había DOS clientes de Ollama
// en la máquina. La Ola 3 retiró aquel decorador, y desde entonces el `ollama.New` de más abajo es el
// ÚNICO del repo fuera de un test con build tag. Si alguien vuelve a instanciar un cliente de Ollama en
// `agent serve`, el requisito se rompe otra vez aunque el decorador no vuelva: el gate es el grep, no el
// nombre de la pieza.
func runCajero(ctx context.Context, cfg config.Config, log sharedlogger.Logger) error {
	if !cfg.Intent.Enabled {
		// Sin clasificador no hay nada que hacer, pero SALIR sería peor: el supervisor lo interpretaría
		// como caída y lo reiniciaría en bucle. Se aparca hasta SIGTERM, que es la forma honesta de
		// decir «estoy vivo y no tengo trabajo».
		log.Warn("cajero: el clasificador de intenciones está DESHABILITADO (WAPP_AGENT_INTENT_ENABLED=false); el worker queda inactivo")
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

	// ── El contrato de intenciones ────────────────────────────────────────────
	// Vive en edge.db (tabla edge_config), que MIGRA `agent serve`: aquí se abre SIN migrar, porque dos
	// procesos aplicando el mismo set de migraciones a la vez es pedir problemas y el daemon es el
	// dueño de esa BD. Si la tabla aún no existe (instalación limpia, `serve` nunca arrancó), el Get
	// falla, `Listo()` devuelve false y el cajero simplemente no reclama hasta que aparezca.
	//
	// El nombre del fichero sale de daemon.SingleDBFileName —el DUEÑO de esa BD— y no de un literal
	// local: un literal duplicado aquí fallaría en silencio si el original cambiara (el cajero abriría
	// un edge.db vacío, `Listo()` sería false para siempre y no reclamaría nada, sin error alguno).
	//
	// ─────────────────────────────────────────────────────────────────────────
	// 🔴 DECISIÓN DE T4.1, EXPLÍCITA Y REVISABLE: EL CONTRATO ES UNO, EL DEL PRIMER data_dir
	// ─────────────────────────────────────────────────────────────────────────
	// Cada data_dir de la lista tiene TAMBIÉN su propio edge.db, así que la pregunta «¿el contrato de
	// intenciones es por-instalación o compartido?» es real y no se deja ambigua. Aquí se resuelve como
	// COMPARTIDO, tomando el del PRIMER data_dir de la lista.
	//
	// El motivo NO es la pereza de no abrir N ficheros: es que EL CLASIFICADOR ES UNO. `classifier.New`
	// produce un objeto con UN prompt y UN schema, y `cls.Reload(parsed)` los sustituye en caliente. Con N
	// contratos distintos aplicados sobre ese único objeto, el último refresco ganaría y las otras N-1
	// instalaciones se clasificarían contra el contrato de un vecino —silenciosamente, y con un
	// `config_version` en el sobre que MENTIRÍA sobre con qué se clasificó—. Servir N contratos de verdad
	// exige N clasificadores, y eso arrastra N goroutines de `mantener`, N conexiones a edge.db y una
	// decisión sobre cómo se reparte entre ellos el semáforo único de Ollama: es una tarea, no un detalle
	// de esta.
	//
	// LO QUE ESTO IMPLICA, dicho para que nadie lo descubra en campo: el round-robin de T4.1 está pensado
	// para N instalaciones DEL MISMO tenant/contrato en una máquina. Si dos instalaciones de la lista
	// tuvieran contratos de intenciones distintos, la segunda se clasificaría con el de la primera. Ese es
	// el límite de esta ola y el disparador para revisar esta decisión.
	dirContrato := dataDirs[0]
	edgeDSN := cfg.DBDSN
	if cfg.DBDialect == db.DialectSQLite && edgeDSN == "" {
		edgeDSN = filepath.Join(dirContrato, daemon.SingleDBFileName)
	}
	edgeDB, err := db.Open(ctx, cfg.DBDialect, edgeDSN)
	if err != nil {
		return fmt.Errorf("cajero: abrir la BD única del Edge (dialecto %q): %w", cfg.DBDialect, err)
	}
	defer func() { _ = edgeDB.Close() }()

	// ── El clasificador ───────────────────────────────────────────────────────
	// Arranca con un contrato VACÍO, igual que el decorador inline: hasta que el refresco encuentre una
	// config persistida no hay prompt ni schema útiles, y `Listo()` mantiene al cajero sin reclamar.
	client := ollama.New(cfg.Intent.OllamaURL)
	cls := classifier.New(client, cfg.Intent.Model, &intents.Config{},
		// T2.5 · el techo de entrada: sin él, pegar ~65 KB en un chat basta para que la inferencia
		// tarde lo suficiente como para abrir el breaker COMPARTIDO y apagar la clasificación de todas
		// las sesiones. Es la denegación de servicio más barata que existe hoy contra esta pieza.
		classifier.WithMaxRunes(cfg.Worker.MaxRunes),
		// T2.5 · las tres opciones de modelo, EXPLÍCITAS. El clasificador ya las manda por su cuenta con
		// los mismos valores (classifier.DefaultNumThread/NumCtx/NumPredict, que es de donde salen los
		// defaults de config), así que pasarlas aquí no cambia nada mientras el operador no las toque:
		// lo que hace es dar a WAPP_WORKER_NUM_* un efecto real sin tener que releasear el módulo del
		// clasificador para ajustar una máquina concreta. WithLLMOptions FUSIONA, así que la
		// temperatura 0.1 —que no se nombra— sobrevive intacta.
		classifier.WithLLMOptions(map[string]any{
			"num_thread":  cfg.Worker.NumThread,
			"num_predict": cfg.Worker.NumPredict,
			"num_ctx":     cfg.Worker.NumCtx,
		}),
	)

	contrato := &contratoIntenciones{store: edgeconfig.NewSQLStore(edgeDB), cls: cls, log: log}
	contrato.refrescar(ctx) // primer intento en el arranque: si hay contrato, se empieza clasificando ya

	ctxRefresco, pararRefresco := context.WithCancel(ctx)
	var refresco sync.WaitGroup
	refresco.Add(1)
	go func() {
		defer refresco.Done()
		contrato.mantener(ctxRefresco, refrescoContrato)
	}()
	// 🔴 EL PARO DEL REFRESCO ES UN defer, Y ESTÁ AQUÍ POR ORDEN, NO POR ESTILO. La goroutine de
	// `mantener` hace `store.Get` contra edgeDB; los `defer` que cierran edgeDB y las N colas
	// (`cerrarColas`) ya están registrados ARRIBA, así que este defer se ejecuta ANTES que ellos (LIFO) y
	// garantiza que nadie está dentro de un Get cuando el *sql.DB se cierra. La versión anterior sólo
	// esperaba a la goroutine en el camino FELIZ (al final de la función): cualquier `return err`
	// intermedio —y hay uno justo debajo, el de cajero.New— cerraba las BDs con el refresco todavía vivo.
	// Carrera real, de las que `-race` caza.
	//
	// T4.1 NO CAMBIÓ ESTE ORDEN, y ese era el riesgo de pasar a N colas: `cerrarColas` es UN solo defer
	// registrado tras el bucle de aperturas, no N defer apilados dentro de él, así que la secuencia sigue
	// siendo exactamente {paro del refresco} → {edgeDB} → {las N colas, en orden inverso al de apertura}.
	defer func() {
		pararRefresco()
		refresco.Wait()
	}()

	// ── El bucle ──────────────────────────────────────────────────────────────
	c, err := cajero.New(cajero.Deps{
		// Colas (plural) y NO Cola: la lista es la fuente única desde T4.1, incluso con una entrada. Pasar
		// las dos cosas sería dejar dos verdades sobre lo mismo esperando a divergir.
		Colas:         colas,
		Clasificador:  cls,
		Despertador:   cajero.NewPollFijo(time.Duration(cfg.Worker.PollMS) * time.Millisecond),
		Log:           log,
		MaxFilas:      cfg.ColaClaimMaxFilas,
		MaxConcurrent: cfg.Worker.MaxConcurrent,
		// T2.19 · el freno del lote venenoso. Sin este cableado la constante existiría y nadie la miraría:
		// un lote cuya inferencia siempre falla vuelve a `nuevo` conservando su `seq`, se lo lleva otra vez
		// el claim siguiente y —con MaxConcurrent=1— congela la cola entera sin más síntoma que el contador
		// de fallos subiendo.
		MaxIntentos: cfg.Worker.MaxIntentos,
		Lease:       time.Duration(cfg.ColaLeaseSeconds) * time.Second,
		// 🔴 EL PLAZO SALE DE cfg.Worker.InferenceTimeoutMS, NO DE cfg.Intent.WaitMS. Aquel es el
		// presupuesto del DESPACHADOR (4 s: no retener la entrega más de la cuenta) y usarlo aquí
		// calibraba el worker PARA FALLAR — queda pegado a la p95 medida por la O0 (3.736 ms), así que
		// abortaría una fracción grande de las inferencias, abriría el breaker y dejaría la cola dando
		// vueltas sin progresar. El worker existe justo para poder tardar. Ver el doc comment de
		// config.DefaultWorkerInferenceTimeoutMS y el de cajero.Deps.Timeout.
		Timeout:       time.Duration(cfg.Worker.InferenceTimeoutMS) * time.Millisecond,
		StatsEvery:    time.Duration(cfg.Worker.StatsEveryMS) * time.Millisecond,
		Listo:         contrato.Listo,
		ConfigVersion: contrato.Version,
		OllamaURL:     cfg.Intent.OllamaURL,
		// T2.8 · el mismo número que se le manda al clasificador arriba (WithLLMOptions), aquí sólo para
		// que el aviso de sobresuscripción compare lo que DE VERDAD se está pidiendo y no el default.
		NumThread: cfg.Worker.NumThread,
	})
	if err != nil {
		return err
	}

	log.Info("agent cajero: worker-cajero de la cola de entrantes (Plan 051 Ola 2; round-robin de N colas desde la Ola 4)",
		"data_dir", cfg.DataDir,
		// `colas` es cuántas instalaciones se turnan y `data_dirs` cuáles: con una sola instalación
		// coincide con `data_dir` y la línea dice lo mismo que decía antes de T4.1.
		"colas", len(colas),
		"data_dirs", strings.Join(dataDirs, ","),
		"data_dir_contrato", dirContrato,
		"ollama_url", cfg.Intent.OllamaURL,
		"model", cfg.Intent.Model,
		"max_concurrent", cfg.Worker.MaxConcurrent,
		"poll_ms", cfg.Worker.PollMS,
		"max_runes", cfg.Worker.MaxRunes,
		"num_thread", cfg.Worker.NumThread,
		"num_predict", cfg.Worker.NumPredict,
		"num_ctx", cfg.Worker.NumCtx,
		"max_intentos", cfg.Worker.MaxIntentos,
		"inferencia_timeout_ms", cfg.Worker.InferenceTimeoutMS,
		"stats_cada_ms", cfg.Worker.StatsEveryMS,
		// T4.5 · la cadencia del PARTE va al lado de la del latido de log para que se vean juntas y se
		// entienda que son DOS relojes distintos: `stats_cada_ms` es verbosidad de log (y un 0 la apaga),
		// `parte_cada_s` es la frescura de la señal de salud que llega a la nube, y no se puede apagar.
		"parte_cada_s", int(app.ParteCada.Seconds()),
	)

	// El paro del refresco lo hace el defer de arriba, en TODOS los caminos de salida (este y los
	// `return err` intermedios), y siempre antes de que se cierren las BDs.
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

// contratoIntenciones mantiene vivo el contrato de intenciones del clasificador del cajero leyéndolo
// de edge.db, y responde las dos preguntas que el bucle le hace: «¿hay contrato?» y «¿qué versión?».
//
// POR QUÉ SONDEA Y NO SE SUSCRIBE: el push del Cloud (ConfigUpdate) llega por el stream CloudLink, que
// vive en `agent serve`. El cajero es OTRO PROCESO y el único canal que comparten es el disco. Un
// sondeo cada 30 s es la costura más barata que no inventa un IPC nuevo.
type contratoIntenciones struct {
	store edgeconfig.Store
	cls   *classifier.Classifier
	log   sharedlogger.Logger

	mu      sync.RWMutex
	version string
}

// Listo reporta si hay contrato cargado. Es el espejo del `ready` del decorador inline: sin contrato
// el clasificador no tiene prompt ni schema, y reclamar sería marcar filas `tomado` para clasificarlas
// contra un prompt vacío.
func (c *contratoIntenciones) Listo() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.version != ""
}

// Version devuelve la versión del contrato vigente, que viaja en el sobre que el cajero persiste.
func (c *contratoIntenciones) Version() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.version
}

// refrescar relee el contrato y, si la versión cambió, recarga el clasificador en caliente.
func (c *contratoIntenciones) refrescar(ctx context.Context) {
	// El `kind` sale de wiring.IntentsConfigKind —el paquete que lo REGISTRA— por el mismo motivo que
	// el nombre del fichero: desalinearlos no daría error, sólo un worker que no clasifica nunca.
	rec, encontrado, err := c.store.Get(ctx, wiring.IntentsConfigKind)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		c.log.Warn("cajero: no se pudo leer el contrato de intenciones; el worker no reclamará hasta que aparezca",
			"error", err)
		return
	}
	if !encontrado {
		return
	}

	c.mu.RLock()
	actual := c.version
	c.mu.RUnlock()
	if rec.Version == actual {
		return
	}

	parsed, err := intents.ParseAndValidate(rec.Payload)
	if err != nil {
		// El Service del daemon valida ANTES de persistir, así que un fallo aquí sería un blob corrupto
		// en disco. Se conserva el contrato anterior en vez de recargar con basura.
		c.log.Error("cajero: contrato de intenciones ilegible (se conserva el anterior)",
			"version", rec.Version, "error", err)
		return
	}

	c.cls.Reload(parsed)
	c.mu.Lock()
	c.version = rec.Version
	c.mu.Unlock()
	c.log.Info("cajero: contrato de intenciones cargado; la clasificación está ACTIVA", "config_version", rec.Version)
}

// mantener refresca el contrato cada `cada` hasta que ctx se cancele.
func (c *contratoIntenciones) mantener(ctx context.Context, cada time.Duration) {
	t := time.NewTicker(cada)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.refrescar(ctx)
		}
	}
}
