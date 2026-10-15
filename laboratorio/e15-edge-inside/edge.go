//go:build laboratorio

package main

// edge.go — EL EDGE POR DENTRO, MONTADO A MANO.
//
// Es `internal/infra/daemon.Run` RECORTADO: las mismas piezas, construidas con los mismos constructores y en
// el mismo orden, menos lo que E15 no estudia (CloudLink, el lease, el auth de operador, el JWKS, el
// clasificador, el outbox, el latido). Cada recorte está dicho en su línea.
//
// Por qué un programa y no `agent serve` con las palancas: el binario NO deja apagar el authorizer, ni
// cambiar el Keychain por un fichero, ni cambiar CloudLink por un sumidero en memoria, y así debe seguir
// (ADR-0049, pieza 9 y 10). Eso solo se puede hacer FUERA del binario, y Go solo deja importar
// `internal/` desde dentro del módulo: por eso este programa vive en `laboratorio/` del Edge.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"time"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/diag"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/inventory"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/server"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsappfalso"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/latencia"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/daemon"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/wiring"
)

// socketName es el socket del plano de control dentro del directorio del laboratorio. La ruta entera
// tiene que caber en 104 caracteres (sun_path de macOS): por eso el directorio por defecto es corto.
const socketName = "edge.sock"

// edgeInside es un Edge sin nube. El almacenamiento (las dos BD, la cola y el registro de capturados)
// vive lo que vive el programa; el gestor de sesiones y el servidor se arrancan y se paran por fases,
// como un reinicio del daemon.
type edgeInside struct {
	dir    string
	layout sessionmgr.Layout
	log    sharedlogger.Logger

	database *sql.DB
	colaDB   *sql.DB
	cola     app.ColaEntrantes
	registro *whatsappfalso.Registro
	mux      *memoryMux

	// Lo de la fase en curso.
	frozen   bool
	hist     *latencia.Histograma
	mgr      *sessionmgr.Manager
	srv      *server.Server
	serveErr chan error
}

// custodyFor es el ÚNICO criterio de custodia del programa, compartido —como en el daemon— por el gestor
// de sesiones (que sella la DEK al emparejar) y por la cola (que la carga para sellar cada fila).
func (e *edgeInside) custodyFor(path string) app.KeyCustody { return fileCustody{path: path} }

// openEdge abre las dos BD del Edge y construye la cola con el wiring de producción.
func openEdge(ctx context.Context, dir string, logOut io.Writer, rejectFirst int) (*edgeInside, error) {
	e := &edgeInside{
		dir:    dir,
		layout: sessionmgr.NewLayout(dir),
		log:    sharedlogger.New(sharedlogger.WithWriter(logOut)),
		mux:    newMemoryMux(rejectFirst),
	}

	// La BD única (edge.db): sesiones, dispositivos, y el almacén de whatsmeow que el falso NO usa.
	database, err := db.Open(ctx, db.DialectSQLite, filepath.Join(dir, daemon.SingleDBFileName))
	if err != nil {
		return nil, fmt.Errorf("abrir la BD única: %w", err)
	}
	if err := db.Migrate(ctx, database); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("migrar la BD única: %w", err)
	}
	e.database = database

	// La cola, en su BD PROPIA, por el ÚNICO constructor que le aplica su perfil de escritura (Plan 051 ·
	// T3.15). Es lo mismo que hace daemon.Run, línea por línea.
	colaDB, err := db.OpenCola(ctx, e.layout.ColaDB())
	if err != nil {
		e.close()
		return nil, fmt.Errorf("abrir la BD de la cola: %w", err)
	}
	e.colaDB = colaDB
	if err := db.MigrateCola(ctx, colaDB); err != nil {
		e.close()
		return nil, fmt.Errorf("migrar la BD de la cola: %w", err)
	}
	cfg := config.Config{ColaMaxRows: config.DefaultColaMaxRows, ColaTTLHours: config.DefaultColaTTLHours}
	e.cola = wiring.BuildCola(ctx, cfg, colaDB, e.layout, e.custodyFor, e.log)
	if e.cola == nil {
		e.close()
		return nil, errors.New("wiring.BuildCola devolvió nil (ver el log del Edge)")
	}

	// El registro de lo que el WhatsApp falso «envía». En el daemon lo crea elegirTransporte con la
	// palanca WAPP_AGENT_WHATSAPP_FALSO=true; aquí se crea a mano, una vez para todo el programa.
	e.registro = whatsappfalso.NuevoRegistro(0)
	return e, nil
}

// start arranca una fase: el gestor de sesiones (que restaura las sesiones activas) y el plano de control
// en el socket unix. frozen=true es WAPP_AGENT_DESPACHADOR_APAGADO=true: las sesiones escuchan y encolan,
// pero nadie drena la cola.
func (e *edgeInside) start(ctx context.Context, frozen bool) error {
	e.frozen = frozen
	e.hist = latencia.Nuevo()
	transporte := wiring.TransporteFalso(e.registro)

	e.mgr = sessionmgr.NewManager(e.layout, sessionstore.New(e.database), 5, e.log,
		sessionmgr.WithSharedDB(e.database, db.DialectSQLite),
		// El transporte FALSO en los dos papeles (escucha y emparejamiento), y el multiplexor DOBLE.
		sessionmgr.WithEscucha(transporte, e.mux, "wApp-lab"),
		sessionmgr.WithEmparejamiento(transporte, app.DefaultPairTimeout),
		sessionmgr.WithKeyCustodyFactory(e.custodyFor),
		// La cola REAL en sus dos extremos: el listener encola, el despachador drena.
		sessionmgr.WithColaEntrantes(e.cola),
		sessionmgr.WithColaDespachador(wiring.BuildColaDespachador(e.cola, e.log)),
		// La palanca de T3.17. En el daemon la arma opcionPalancaDespachador(cfg).
		sessionmgr.WithDespachadorApagado(frozen),
		// El cronómetro del handler de entrantes (INV-051.2: < 50 ms p99).
		sessionmgr.WithLatencia(e.hist),
		// NO VA: WithSesionPasiva. Sin el mapa de perfiles que empuja la nube el predicado es nil y el
		// Manager hace FAIL-OPEN (nadie es pasiva). En campo la sesión nace PASIVA (INV-E5) y hay que
		// activarla en la nube; aquí no hay nube que la active, así que tampoco se la calla.
		// NO VA: WithHealthRegistry ni WithInboundMargin (este último cae en su default).
	)
	if err := e.mgr.Restore(ctx); err != nil {
		e.mgr.Stop()
		return fmt.Errorf("restaurar sesiones: %w", err)
	}

	e.srv = server.New(server.Config{SocketPath: e.socketPath(), Version: "laboratorio-e15"}, e.log, inventory.New(e.mgr))
	// 🔴 SIN SetAuthorizer. Con el authorizer nil, `guard` deja pasar todo (server.go: «tests / pre-Plan
	// 033»): es lo que hacen los tests internal/infra/daemon/palanca_*_test.go. En el daemon iría
	// srv.SetAuthorizer(authMgr), y cada llamada necesitaría el Bearer de un operador y el JWKS de la nube.
	e.srv.RegisterPairing(e.mgr)
	e.srv.RegisterUnlink(e.mgr)
	// Las dos rutas de /v1/diag, como las cuelgan registrarInyectorEntrantes y registrarCapturados con sus
	// palancas echadas (WAPP_AGENT_INYECTOR_ENTRANTES=true y WAPP_AGENT_WHATSAPP_FALSO=true).
	e.srv.HandleAuthorized(http.MethodPost, diag.RutaInyectar, diag.RecursoInyectar, true,
		diag.InyectorHandler(diag.InyectorDeps{Inyectar: e.mgr.InyectarEntrante}))
	e.srv.HandleAuthorized(http.MethodGet, diag.RutaCapturados, diag.RecursoCapturados, false,
		diag.CapturadosHandler(diag.CapturadosDeps{Leer: e.registro.De}))

	ln, err := e.srv.Listen()
	if err != nil {
		e.mgr.Stop()
		return fmt.Errorf("abrir el socket del plano de control: %w", err)
	}
	e.serveErr = make(chan error, 1)
	go func() { e.serveErr <- e.srv.Serve(ln) }()
	return nil
}

// stop para la fase: primero el servidor, luego los listeners (el orden de daemon.Run).
func (e *edgeInside) stop() {
	if e.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = e.srv.Shutdown(ctx)
		cancel()
		<-e.serveErr
		e.srv = nil
	}
	if e.mgr != nil {
		e.mgr.Stop()
		e.mgr = nil
	}
}

// close para lo que quede y cierra las dos BD.
func (e *edgeInside) close() {
	e.stop()
	if e.colaDB != nil {
		_ = e.colaDB.Close()
	}
	if e.database != nil {
		_ = e.database.Close()
	}
}

func (e *edgeInside) socketPath() string { return filepath.Join(e.dir, socketName) }

// dekPath es la ruta de la custodia de la DEK de una sesión, la que fabrica el layout.
func (e *edgeInside) dekPath(sessionID string) (string, error) { return e.layout.DEKPath(sessionID) }

// handlerP99 lee el cronómetro del handler en el camino que cuenta para INV-051.2 (encolado).
func (e *edgeInside) handlerP99() (n uint64, p99 latencia.Corte, maxMS float64, ok bool) {
	m := e.hist.Snapshot(latencia.Encolado)
	c, ok := latencia.Percentil(m, 0.99)
	return m.N, c, m.MaxMS(), ok
}

// edgeDBName es el nombre de la BD única, el que exporta el daemon para no duplicarlo.
const edgeDBName = daemon.SingleDBFileName
