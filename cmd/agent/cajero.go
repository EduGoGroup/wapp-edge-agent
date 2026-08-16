package main

import (
	"context"
	"fmt"
	"path/filepath"
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
// ⚠️ LIMITACIÓN CONOCIDA — REQ-051.10 («ningún otro proceso habla con Ollama») AÚN NO SE CUMPLE, y no
// se puede forzar desde aquí: mientras dure la escritura doble de la Ola 1, `agent serve` sigue
// clasificando en línea con su decorador, así que hay DOS clientes de Ollama en la máquina. La Ola 3
// retira el decorador y entonces el `grep` de llamadas a Ollama fuera del worker da cero. Adelantarlo
// aquí (apagando el decorador desde el cajero) dejaría la cola sin clasificar durante la ventana en
// que el worker todavía no está desplegado en todas las máquinas.
func runCajero(ctx context.Context, cfg config.Config, log sharedlogger.Logger) error {
	if !cfg.Intent.Enabled {
		// Sin clasificador no hay nada que hacer, pero SALIR sería peor: el supervisor lo interpretaría
		// como caída y lo reiniciaría en bucle. Se aparca hasta SIGTERM, que es la forma honesta de
		// decir «estoy vivo y no tengo trabajo».
		log.Warn("cajero: el clasificador de intenciones está DESHABILITADO (WAPP_AGENT_INTENT_ENABLED=false); el worker queda inactivo")
		<-ctx.Done()
		return nil
	}

	layout := sessionmgr.NewLayout(cfg.DataDir)

	// ── La cola de entrantes ──────────────────────────────────────────────────
	// Se abre y migra igual que en el daemon (internal/infra/daemon): es la MISMA BD propia de la cola
	// (<data_dir>/cola_entrantes.db) y las migraciones son idempotentes, así que da igual quién de los
	// dos procesos arranque primero. Aquí SÍ es fatal, al revés que en el daemon: sin cola, el cajero
	// no tiene ninguna otra cosa que hacer, y fallar el arranque es más honesto que quedarse en un
	// bucle que no reclama nada.
	colaDB, err := db.Open(ctx, db.DialectSQLite, layout.ColaDB())
	if err != nil {
		return fmt.Errorf("cajero: abrir la BD de la cola (%s): %w", layout.ColaDB(), err)
	}
	defer func() { _ = colaDB.Close() }()
	if err := db.MigrateCola(ctx, colaDB); err != nil {
		return fmt.Errorf("cajero: migrar la BD de la cola: %w", err)
	}

	// La custodia de la DEK sale del MISMO criterio que el daemon (Plan 035 · DIP): un único punto de
	// verdad sobre dónde vive la DEK. Reusar wiring.BuildCola —y no reconstruir el crypterFor a mano—
	// es lo que garantiza que el worker abre los sobres con exactamente la misma llave con la que el
	// listener los selló. Zero-knowledge intacto: la DEK se resuelve en local y no se loguea.
	custodyFor := func(p string) app.KeyCustody { return keycustody.NewFileCustody(p) }
	colaPort := wiring.BuildCola(ctx, cfg, colaDB, layout, custodyFor, log)
	if colaPort == nil {
		return fmt.Errorf("cajero: la cola de entrantes no se pudo construir (ver el error anterior)")
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
		return fmt.Errorf("cajero: la cola construida no implementa app.ColaCajero (%T)", colaPort)
	}

	// ── El contrato de intenciones ────────────────────────────────────────────
	// Vive en edge.db (tabla edge_config), que MIGRA `agent serve`: aquí se abre SIN migrar, porque dos
	// procesos aplicando el mismo set de migraciones a la vez es pedir problemas y el daemon es el
	// dueño de esa BD. Si la tabla aún no existe (instalación limpia, `serve` nunca arrancó), el Get
	// falla, `Listo()` devuelve false y el cajero simplemente no reclama hasta que aparezca.
	//
	// El nombre del fichero sale de daemon.SingleDBFileName —el DUEÑO de esa BD— y no de un literal
	// local: un literal duplicado aquí fallaría en silencio si el original cambiara (el cajero abriría
	// un edge.db vacío, `Listo()` sería false para siempre y no reclamaría nada, sin error alguno).
	edgeDSN := cfg.DBDSN
	if cfg.DBDialect == db.DialectSQLite && edgeDSN == "" {
		edgeDSN = filepath.Join(cfg.DataDir, daemon.SingleDBFileName)
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
	// `mantener` hace `store.Get` contra edgeDB; los `defer` que cierran edgeDB/colaDB ya están
	// registrados ARRIBA, así que este defer se ejecuta ANTES que ellos (LIFO) y garantiza que nadie
	// está dentro de un Get cuando el *sql.DB se cierra. La versión anterior sólo esperaba a la
	// goroutine en el camino FELIZ (al final de la función): cualquier `return err` intermedio —y hay
	// uno justo debajo, el de cajero.New— cerraba las BDs con el refresco todavía vivo. Carrera real,
	// de las que `-race` caza.
	defer func() {
		pararRefresco()
		refresco.Wait()
	}()

	// ── El bucle ──────────────────────────────────────────────────────────────
	c, err := cajero.New(cajero.Deps{
		Cola:          colaCajero,
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
		// 🔴 EL PLAZO SALE DE cfg.Worker.InferenceTimeoutMS, NO DE cfg.Intent.TimeoutMS. Aquel es el
		// presupuesto del camino INLINE (3 s: soltar el handler de whatsmeow cuanto antes) y usarlo aquí
		// calibraba el worker PARA FALLAR — queda por debajo de la p95 medida por la O0 (3.736 ms), así
		// que abortaría del orden del 20-40 % de las inferencias, abriría el breaker y dejaría la cola
		// dando vueltas sin progresar. El worker existe justo para poder tardar. Ver el doc comment de
		// config.DefaultWorkerInferenceTimeoutMS y el de cajero.Deps.Timeout.
		Timeout:       time.Duration(cfg.Worker.InferenceTimeoutMS) * time.Millisecond,
		StatsEvery:    time.Duration(cfg.Worker.StatsEveryMS) * time.Millisecond,
		Listo:         contrato.Listo,
		ConfigVersion: contrato.Version,
		OllamaURL:     cfg.Intent.OllamaURL,
	})
	if err != nil {
		return err
	}

	log.Info("agent cajero: worker-cajero de la cola de entrantes (Plan 051 Ola 2)",
		"data_dir", cfg.DataDir,
		"cola_db", layout.ColaDB(),
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
	)

	// El paro del refresco lo hace el defer de arriba, en TODOS los caminos de salida (este y los
	// `return err` intermedios), y siempre antes de que se cierren las BDs.
	return c.Run(ctx)
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
