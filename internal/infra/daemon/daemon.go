// Package daemon orquesta el arranque unificado del Edge Agent (Plan 008 + 007 + 022 + 027 + 029 + 031 + 033):
// BD única, outbox, clasificador de intenciones, auth de operador, session manager, servidor /v1 y
// shutdown ordenado.
package daemon

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/enrolladapter"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/inventory"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/logsink"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/server"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/intent"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/diagnostics"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/latencia"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/edgemigrate"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/wiring"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// SingleDBFileName es el nombre de la BD ÚNICA del Edge (Plan 022 T3) bajo data_dir.
//
// ESTÁ EXPORTADA porque el daemon ya NO es el único proceso que abre ese fichero: el worker-cajero
// (`agent cajero`, Plan 051 Ola 2) lee de ahí el contrato de intenciones. Tenía el nombre duplicado en
// un literal de cmd/agent y ese duplicado fallaba EN SILENCIO: si el nombre cambiara aquí, el cajero
// abriría un `edge.db` vacío, `Listo()` devolvería false para siempre y el worker no reclamaría nada,
// sin un solo error. Un símbolo compartido cuesta menos que ese modo de fallo.
//
// Sigue siendo el daemon el DUEÑO del fichero: es quien lo migra. El cajero sólo lo abre para leer.
const SingleDBFileName = "edge.db"

// Daemon encapsula el ciclo de vida y la orquestación de componentes del Edge Agent multi-sesión.
type Daemon struct {
	cfg       config.Config
	log       sharedlogger.Logger
	sink      *logsink.Sink
	version   string
	startedAt time.Time
}

// New construye una instancia del Daemon.
func New(cfg config.Config, log sharedlogger.Logger, sink *logsink.Sink, version string, startedAt time.Time) *Daemon {
	return &Daemon{
		cfg:       cfg,
		log:       log,
		sink:      sink,
		version:   version,
		startedAt: startedAt,
	}
}

// Run ejecuta el daemon multi-sesión unificado y bloquea hasta la cancelación del contexto o fallo del servidor.
func (d *Daemon) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg := d.cfg
	log := d.log

	// VARIABLES RETIRADAS (Plan 051 Ola 3, T3.1): lo PRIMERO del arranque, antes de tocar la BD, porque
	// es un aviso al operador y no debe depender de que el resto del arranque llegue a algún sitio.
	//
	// Se emite AQUÍ y no en config.Load por dos razones: (1) Load se ejecuta antes de que exista el
	// logger —meterle uno sería invertir la dependencia—, y (2) éste es el proceso que CONSUMÍA la
	// variable retirada (el camino inline vivía en el daemon), así que es su operador el que necesita
	// leerlo. El logger del daemon es además el que alimenta el ring buffer del plano de control, de
	// modo que el aviso también se ve por `wapp-ctl logs` y no sólo en stderr al arrancar.
	//
	// NO es fatal: una variable de más no impide operar, sólo miente sobre lo que gobierna.
	for _, aviso := range config.VariablesRetiradas() {
		log.Warn("variable de entorno RETIRADA presente: ya no gobierna nada",
			"variable", aviso.Variable, "sustituta", aviso.Sustituta, "motivo", aviso.Motivo)
	}

	dbDSN := cfg.DBDSN
	if cfg.DBDialect == db.DialectSQLite && dbDSN == "" {
		dbDSN = filepath.Join(cfg.DataDir, SingleDBFileName)
	}
	database, err := db.Open(ctx, cfg.DBDialect, dbDSN)
	if err != nil {
		return fmt.Errorf("serve: abrir la BD única (dialecto %q): %w", cfg.DBDialect, err)
	}
	defer func() { _ = database.Close() }()
	if err := db.Migrate(ctx, database); err != nil {
		return fmt.Errorf("serve: migrar la BD única: %w", err)
	}

	if err := edgemigrate.RestoreArchivedActiveSessions(ctx, cfg.DataDir, database, cfg.DBDialect, log); err != nil {
		log.Error("serve: restauración de sesiones activas archivadas (T6.5) falló (continuo de todas formas)",
			"error", err, "data_dir", cfg.DataDir)
	}

	sessions := sessionstore.New(database)
	layout := sessionmgr.NewLayout(cfg.DataDir)

	// Custodia de la DEK por sesión: ÚNICO punto de verdad sobre cómo se custodia (Plan 035 · DIP). Lo
	// comparten el session manager (WithKeyCustodyFactory) y la cola de entrantes (CrypterFor), para que no
	// existan dos criterios distintos de dónde vive la DEK.
	custodyFor := func(p string) app.KeyCustody { return keycustody.NewFileCustody(p) }

	// Cola durable de entrantes (Plan 051 Ola 1): vive en su PROPIA BD SQLite (<data_dir>/cola_entrantes.db),
	// NUNCA dentro de edge.db — por eso se abre aparte y se migra con MigrateCola (set "cola"), no con
	// Migrate. El handle se cierra igual que el de edge.db (defer, apagado ordenado §10.I).
	//
	// 🔴🔴 ES FATAL, Y ESO CAMBIÓ EL 2026-08-17 (Plan 051 O3). Hasta entonces un fallo aquí se logueaba y el
	// daemon seguía «degradado». Era defendible mientras el listener entregaba además por su cuenta
	// (`sink.Deliver` inline): sin cola se perdía la durabilidad y la clasificación, pero el mensaje subía.
	// T3.0 retiró ese camino y el despachador de la cola pasó a ser EL ÚNICO por el que un entrante llega a
	// la nube. Con eso, «seguir sin cola» dejó de ser una degradación y pasó a ser el peor modo de fallo que
	// tiene esta pieza: el socket conectado, el teléfono del cliente acusando ✓✓, y CADA mensaje entrante
	// evaporándose sin una sola línea de error. Nadie lo diagnostica en campo.
	//
	// Arrancar así sería PROMETER UN SERVICIO QUE NO EXISTE. Se prefiere que el proceso no levante: el
	// supervisor lo reintenta, el operador ve el error en el arranque y el cliente sigue viendo sus mensajes
	// sin entregar en WhatsApp —que es información honesta— en vez de un doble check azul mentiroso.
	//
	// El worker-cajero (cmd/agent/cajero.go) ya era fatal por su lado desde la Ola 2; ahora los dos procesos
	// que abren este fichero tratan su ausencia igual.
	//
	// Se abre con db.OpenCola y NO con db.Open: la cola lleva su propio perfil de escritura SQLite
	// (synchronous=NORMAL + un WAL 4× más ancho, Plan 051 · T3.15) porque es la única BD del Edge con dos
	// procesos escribiéndola a la vez. Esos pragmas son POR-CONEXIÓN, así que el cajero tiene que abrirla
	// exactamente igual —y lo hace, por el mismo constructor—; si uno de los dos se quedara en el perfil
	// conservador, sus checkpoints seguirían bloqueando al otro.
	colaDB, err := db.OpenCola(ctx, layout.ColaDB())
	if err != nil {
		return fmt.Errorf("serve: abrir la BD de la cola de entrantes (%s): sin cola no hay camino de entrega, "+
			"arrancar sería prometer un servicio que no existe (Plan 051 O3): %w", layout.ColaDB(), err)
	}
	defer func() { _ = colaDB.Close() }()
	if err := db.MigrateCola(ctx, colaDB); err != nil {
		return fmt.Errorf("serve: migrar la BD de la cola de entrantes (%s): sin cola no hay camino de entrega, "+
			"arrancar sería prometer un servicio que no existe (Plan 051 O3): %w", layout.ColaDB(), err)
	}
	cola := wiring.BuildCola(ctx, cfg, colaDB, layout, custodyFor, log)
	// Y la construcción del Store TAMBIÉN es fatal, por el mismo motivo exacto: `BuildCola` sigue devolviendo
	// nil sin drama (su molde es el de BuildOutbox y lo comparte con el cajero, que hace su propia guarda),
	// pero un nil aquí deja el mismo agujero negro que un fichero que no abre. La política de arranque se
	// decide AQUÍ, en el dueño del proceso, no dentro del constructor compartido.
	if cola == nil {
		return fmt.Errorf("serve: la cola de entrantes no se pudo construir sobre %s (ver el error anterior); "+
			"sin cola no hay camino de entrega (Plan 051 O3)", layout.ColaDB())
	}

	// Cronómetro del handler de entrantes (Plan 051 Ola 3 · T3.13). UNO SOLO para todo el Edge: el criterio
	// de cierre de la ola —«handler < 50 ms p99», INV-051.2— se juzga sobre el Edge, no sesión a sesión, y
	// un p99 por sesión con tres mensajes cada una no responde la pregunta. Se arma aquí, junto a la cola,
	// porque el bloque que lo publica cuenta las dos cosas en la misma línea.
	//
	// Las DOS PUNTAS (la que llena y la que publica) salen de buildLatencia y no de este cuerpo: ver ahí el
	// porqué, que es el único motivo por el que este paquete tiene tests.
	lat := buildLatencia(cfg, cola, log)

	outbox := wiring.BuildOutbox(ctx, cfg, database, log)
	intentStack := wiring.BuildIntent(cfg, database, log)

	edgeCfgSvc := wiring.SharedEdgeConfigService(intentStack, log)
	authKeyStore := wiring.RegisterJWKS(edgeCfgSvc, log)
	intentStack.Applier = edgeCfgSvc
	edgeCfgSvc.Bootstrap(ctx)

	healthReg := health.NewRegistry()
	// 🔴 AQUÍ SE PAGA LA DEUDA QUE LA OLA 3 DEJÓ DECLARADA (Plan 051 Ola 4 · T4.3). Hasta esta ola este
	// argumento era un `nil` EXPLÍCITO donde antes iba el breaker del decorador inline: T3.0 retiró aquel
	// decorador, el breaker dejó de existir en este proceso y `intent_circuit` quedó viajando VACÍO en cada
	// latido y en cada bundle de diagnóstico. Vacío era honesto —«este proceso no tiene circuito»— pero era
	// una pregunta sin responder: el circuito REAL existe, lo conoce el WORKER-CAJERO, y corre en OTRO
	// PROCESO. La única frontera que los dos comparten es la BD de la cola, y por ahí llega ahora el PARTE.
	//
	// El parte trae los TRES campos que este proceso no puede conocer por sí mismo —circuito, taskset y p50
	// de inferencia— con una regla de rancidez estricta: un parte viejo se descarta ENTERO (ver
	// health.Collector.parteDelWorker). NUNCA se hereda un "closed" de un cajero muerto; eso sería mandar a
	// la nube una señal de salud INVENTADA, que es exactamente lo que la Ola 3 se negó a hacer.
	//
	// Si el adaptador de la cola no respalda el lado lector, `parteDelWorker` devuelve nil y el colector se
	// comporta como hasta hoy (los tres campos vacíos). La telemetría no decide la política de arranque.
	healthCollector := health.NewCollector(healthReg, outbox, parteDelWorker(cola, log), d.version, d.startedAt,
		health.WithLogger(log))
	diagBuilder := diagnostics.NewBuilder(d.sink, healthCollector, cfg.DiagLogLines)

	mux, authRelay := wiring.BuildMux(ctx, cfg, log, outbox, intentStack, healthCollector, diagBuilder)

	authMgr := wiring.BuildAuthManager(cfg, log, authKeyStore, authRelay)
	authMgr.StartProactiveRefresh(ctx)

	mgr := sessionmgr.NewManager(layout, sessions, cfg.MaxSessions, log,
		sessionmgr.WithSharedDB(database, cfg.DBDialect),
		sessionmgr.WithHealthRegistry(healthReg),
		sessionmgr.WithWhatsmeowListen(mux, cfg.PushName),
		sessionmgr.WithWhatsmeowPairing(app.DefaultPairTimeout),
		sessionmgr.WithMultiDevicePerAccount(cfg.MultiDevicePerAccount),
		// Ventana temporal de ingesta (ADR-0037): lo que llegó del buzón que WhatsApp reencola tras una
		// caída no se ingiere. config.Load ya garantiza un margen > 0.
		sessionmgr.WithInboundMargin(time.Duration(cfg.InboundMarginSeconds)*time.Second),
		// (aquí iba WithInboundDecorator, el decorador de intenciones inline; retirado en T3.0 junto con la
		// opción del Manager: la clasificación ya no ocurre en el camino del listener.)
		sessionmgr.WithKeyCustodyFactory(custodyFor),
		// Cola durable de entrantes (Plan 051 Ola 1): cada listener anota el entrante en disco (cifrado con
		// la DEK de SU sesión), y ahí acaba su trabajo. Aquí `cola` NO PUEDE SER nil: la apertura, la
		// migración y la construcción de arriba son fatales desde el 2026-08-17.
		sessionmgr.WithColaEntrantes(cola),
		// Despachador de la cola (Plan 051 Ola 3, T3.3): cada sesión estrena, junto a su listener, el bucle
		// que DRENA su cola en orden de `seq` y entrega al cable. Es el otro extremo del MISMO adaptador de
		// la línea de arriba (no una segunda cola).
		//
		// 🔴 DESDE T3.0 ES EL ÚNICO CAMINO DE ENTREGA (ya no hay doble entrega: el listener solo encola), y
		// por eso desde el 2026-08-17 el daemon no arranca sin cola. `BuildColaDespachador` sigue pudiendo
		// devolver nil por su OTRO camino —que el adaptador no implemente el lado despachador—, que es un
		// fallo de programación imposible con el *Store real y que él grita con un Warn.
		sessionmgr.WithColaDespachador(wiring.BuildColaDespachador(cola, log)),
		// Presupuesto de espera al cajero (WAPP_AGENT_INTENT_WAIT_MS, 4000 ms): el techo de latencia que la
		// cola añade al camino de un mensaje. config.Load ya garantiza un valor > 0.
		sessionmgr.WithPresupuestoIntent(time.Duration(cfg.Intent.WaitMS)*time.Millisecond),
		// Interruptor del clasificador (Plan 051 Ola 2, T2.12): con la feature apagada, el entrante nace en
		// la cola YA resuelto (marca `apagado`) en vez de que el cajero gaste una plaza del semáforo para
		// descartarlo igual. Tras T3.0 el stack ya no tiene decorador y este predicado lee el flag de config
		// directamente — que es lo que `MotivoApagado` siempre significó.
		sessionmgr.WithClasificadorActivo(intentStack.ClasificadorActivoFunc()),
		// Cronómetro del handler de entrantes (Plan 051 Ola 3 · T3.13): cada listener anota en él cuánto
		// tardó su onMessage. Es lo único que hace MEDIBLE el criterio de cierre de la ola; sin esta línea
		// el histograma existe y nadie lo llena.
		//
		// La opción viene ARMADA de buildLatencia —en vez de un `sessionmgr.WithLatencia(algo)` escrito
		// aquí— para que en este cuerpo no quede ninguna expresión capaz de nombrar OTRO histograma que el
		// que se publica. Es la mitad de la reparación del agujero que describe buildLatencia.
		lat.opt,
	)

	// CABLEADO TARDÍO DEL DESGLOSE POR MOTIVO (Plan 051 Ola 4 · T4.0). El Manager es quien retiene el
	// despachador de cada sesión, así que es él quien sabe contestar «cuántas omisiones por `breaker` lleva
	// esta sesión». No se puede pasar en `health.NewCollector` porque el orden del wiring lo impide: el
	// colector alimenta al multiplexor CloudLink (línea de arriba) y el multiplexor alimenta al Manager, de
	// modo que el Manager es forzosamente lo último de la cadena. Misma costura que `srv.SetHealthProvider`.
	//
	// 🔴 VA ANTES DE `Restore`: en cuanto Restore levanta listeners, el multiplexor puede empezar a mandar
	// latidos. Cablear después dejaría una ventana —corta, pero real— de heartbeats con el desglose a cero,
	// que es indistinguible de un Edge sin tráfico.
	healthCollector.SetDespachoReader(mgr)

	if err := mgr.Restore(ctx); err != nil {
		return fmt.Errorf("serve: restaurar sesiones activas: %w", err)
	}

	srv := server.New(
		server.Config{SocketPath: cfg.ControlSocketPath, Version: d.version},
		log, inventory.New(mgr),
	)
	srv.SetHealthProvider(healthCollector)
	srv.SetAuthorizer(authMgr)
	srv.RegisterAuth(authMgr)
	srv.HandleAuthorized(http.MethodGet, "/v1/logs", "edge.status.read", false, logsink.Handler(d.sink))
	// GET /v1/intent/status — ADELGAZADO en T3.0: reporta el CONTRATO (feature, modelo y versión persistida)
	// y señala al worker-cajero. Ya no reporta `circuit` ni `ollama_ok`: los dos vivían en el decorador
	// inline y sostenerlos aquí sería informar de un estado que este proceso ya no tiene. Ver el doc comment
	// de intent.statusResponse.
	srv.HandleAuthorized(http.MethodGet, "/v1/intent/status", "edge.status.read", false, intent.StatusHandler(intent.StatusDeps{
		Enabled:       intentStack.Enabled,
		Model:         intentStack.Model,
		ConfigVersion: intentStack.ConfigVersion,
	}))
	srv.RegisterPairing(mgr)
	srv.RegisterUnlink(mgr)
	srv.RegisterEnroll(enrolladapter.New(cfg, log))

	ln, err := srv.Listen()
	if err != nil {
		return fmt.Errorf("serve: abrir socket /v1: %w", err)
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	// LATIDO DE LATENCIA DEL HANDLER (Plan 051 Ola 3 · T3.13). El histograma sin emisor es un instrumento
	// con la aguja tapada: sus contadores solo se leerían al morir el proceso, que es cuando ya no sirven.
	//
	// Contexto PROPIO y espera EXPLÍCITA al final. Con el `ctx` del daemon a secas, el bloque FINAL —el que
	// resume la sesión de medida entera— competiría con el `return` de esta función y se perdería la mitad
	// de las veces. Así el cierre es determinista: se cancela, se espera y solo entonces se retorna.
	latidoCtx, pararLatido := context.WithCancel(context.Background())
	latidoFin := make(chan struct{})
	go func() {
		defer close(latidoFin)
		latencia.Latido(latidoCtx, lat.deps)
	}()

	log.Info("agent serve: daemon multi-sesión + plano de control /v1 en un solo proceso",
		"socket", cfg.ControlSocketPath, "version", d.version, "data_dir", cfg.DataDir, "max_sesiones", cfg.MaxSessions,
		"latencia_stats_cada_ms", cfg.InboundStatsEveryMS)

	select {
	case <-ctx.Done():
		log.Info("agent serve: señal de cierre recibida, apagando")
	case err := <-serveErr:
		if err != nil {
			log.Error("agent serve: el servidor /v1 falló, apagando", "error", err)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("agent serve: cierre del servidor /v1 con error", "error", err)
	}
	mgr.Stop()

	// El bloque FINAL de latencia se emite DESPUÉS de parar los listeners: así cubre hasta el último
	// entrante atendido, y su COUNT de la cola es la foto real de lo que queda sin despachar al apagar.
	// Se espera a que retorne (el latido hace su COUNT sobre un contexto desligado con plazo propio).
	pararLatido()
	<-latidoFin

	log.Info("agent serve: detenido limpiamente (socket /v1 cerrado, listeners apagados)")
	return nil
}

// cableadoLatencia son las DOS PUNTAS del cronómetro del handler (Plan 051 Ola 3 · T3.13) atadas en un
// solo valor: la que lo LLENA (la opción que el Manager reparte a cada listener) y la que lo PUBLICA (lo
// que consume el latido). Van juntas porque un cronómetro con las puntas en instrumentos distintos no
// falla: mide y publica, cada uno lo suyo, y el resultado es un bloque «sin dato» para siempre.
type cableadoLatencia struct {
	// hist es el instrumento. `Run` no lo toca —le bastan las dos puntas—: está aquí para que el test
	// pueda AFIRMAR la identidad, que es exactamente lo que nadie podía comprobar.
	hist *latencia.Histograma
	// opt es la punta que lo LLENA: cada listener anota en él cuánto tardó su onMessage.
	opt sessionmgr.Option
	// deps es la punta que lo PUBLICA: la cadencia, el logger y el contador de la cola del bloque.
	deps latencia.Deps
}

// buildLatencia arma el cronómetro del handler de entrantes y sus dos puntas.
//
// 🔴 POR QUÉ ES UNA FUNCIÓN Y NO TRES LÍNEAS DENTRO DE `Run`. Este paquete no tenía un solo `_test.go` y
// sus únicos importadores son `cmd/agent`, cuyos tests no ejercitan `Run`: TODO el cableado de T3.13 era
// invisible para los tres gates. Se comprobó ejecutándolo, no suponiéndolo — poner un `latencia.Nuevo()`
// recién construido en el `Hist:` del latido dejaba `go build`, `go vet` y `go test ./... -p 1` en verde.
//
// Y el modo de fallo que eso compra es de los caros: en campo el bloque saldría con `n=0` y sin
// percentiles PARA SIEMPRE, con el Edge funcionando de maravilla. El síntoma —«el latido sale pero el p99
// no aparece»— no apunta a su causa: quien lo vea buscará en el histograma, en los listeners o en la
// cadencia, y el fallo está en una línea de wiring que ningún test miraba.
//
// Extraerlo es lo que lo hace comprobable: aquí dentro el histograma se nombra UNA sola vez y las dos
// puntas cuelgan de esa misma variable, y el test afirma esa identidad (ver latencia_cableado_test.go).
// La regla que lo sostiene es que `Run` USE esto: una función paralela que `Run` no llama probaría un
// cableado que no corre en producción, que es justo el error que esto viene a cerrar.
func buildLatencia(cfg config.Config, cola app.ColaEntrantes, log sharedlogger.Logger) cableadoLatencia {
	h := latencia.Nuevo()
	return cableadoLatencia{
		hist: h,
		opt:  sessionmgr.WithLatencia(h),
		deps: latencia.Deps{
			Hist: h,
			Cada: time.Duration(cfg.InboundStatsEveryMS) * time.Millisecond,
			Log:  log,
			// El MISMO adaptador de la cola, en su papel de solo-contar (app.ColaContador). Se comprueba la
			// aserción de tipo en vez de darla por hecha: `BuildCola` devuelve el puerto de encolado, y un
			// pánico aquí por una implementación que no respalde el lado contador sería tumbar el daemon por
			// una línea de observabilidad. nil ⇒ el bloque sale sin los campos de cola, que es correcto.
			Cola: colaContador(cola, log),
		},
	}
}

// colaContador devuelve el lado SOLO-CONTAR de la cola, o nil si el adaptador no lo respalda.
//
// La aserción de tipo se comprueba en vez de darse por hecha, por el mismo criterio que
// `wiring.BuildColaDespachador`: con el *Store real siempre se cumple, así que un fallo aquí es un error
// de programación, no un estado degradado — pero tumbar el daemon con un pánico por una línea de
// observabilidad sería peor que la ausencia del dato. Se avisa y se sigue: el bloque de latencia saldrá
// sin los campos de cola, que es lo honesto.
func colaContador(cola app.ColaEntrantes, log sharedlogger.Logger) app.ColaContador {
	c, ok := cola.(app.ColaContador)
	if !ok {
		log.Warn("agent serve: la cola de entrantes no respalda el lado CONTADOR (app.ColaContador); el bloque " +
			"de latencia del handler saldrá sin `cola_pendientes`. Es un fallo de cableado, no un modo de operación")
		return nil
	}
	return c
}

// parteDelWorker devuelve el lado LECTOR DEL PARTE del worker-cajero, o nil si el adaptador no lo respalda
// (Plan 051 Ola 4 · T4.3).
//
// ES EL MISMO ADAPTADOR DE LA COLA VISTO POR OTRO PUERTO, no una segunda BD ni una conexión nueva: el parte
// lo ESCRIBE el proceso del cajero y lo LEE éste, y el fichero `cola_entrantes.db` que los dos ya tienen
// abierto es justo la frontera que comparten. Abrir aquí un segundo handle sobre el mismo SQLite sería
// añadir un tercer escritor a la única BD del Edge con contención real (T3.15).
//
// La aserción de tipo se comprueba en vez de darse por hecha, por el mismo criterio que `colaContador`: con
// el *Store real siempre se cumple, así que un fallo aquí es un error de cableado, no un modo de operación
// — pero tumbar el daemon por una línea de telemetría sería peor que la ausencia del dato. Se avisa y se
// sigue: `intent_circuit`, `worker_taskset` e `intent_p50_ms` viajarán vacíos, exactamente como en la Ola 3.
func parteDelWorker(cola app.ColaEntrantes, log sharedlogger.Logger) app.ParteWorkerLector {
	l, ok := cola.(app.ParteWorkerLector)
	if !ok {
		log.Warn("agent serve: la cola de entrantes no respalda el lado LECTOR DEL PARTE (app.ParteWorkerLector); " +
			"el heartbeat viajará sin `intent_circuit`/`worker_taskset`/`intent_p50_ms`. Es un fallo de cableado, " +
			"no un modo de operación")
		return nil
	}
	return l
}
