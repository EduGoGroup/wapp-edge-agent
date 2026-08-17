package wiring

import (
	"context"
	"database/sql"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cloudlink"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/edgeconfig"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-shared/intents"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// IntentsConfigKind es el kind de config empujada que gobierna el clasificador (Plan 029, ADR-0021).
//
// ESTÁ EXPORTADA porque el worker-cajero (`agent cajero`, Plan 051 Ola 2) es OTRO PROCESO y lee ese
// mismo registro de edge.db por su cuenta (sondea, porque el ConfigUpdate del Cloud llega por el
// stream CloudLink que vive en `agent serve`). Tenerlo duplicado en un literal de cmd/agent fallaba en
// silencio: con el kind desalineado el `Get` no encontraría nada, `Listo()` devolvería false para
// siempre y el cajero no reclamaría ni una fila, sin un solo error en el log.
const IntentsConfigKind = "intents"

// IntentStack agrupa lo que queda del CLASIFICADOR de intenciones (Plan 029) DENTRO del daemon tras la
// Ola 3 del Plan 051: el CONTRATO y su conducto, nada más.
//
// 🔴 EL DAEMON YA NO CLASIFICA (T3.0). El decorador inline —el que envolvía el sink de cada sesión y
// llamaba a Ollama en el hilo de whatsmeow— se retiró entero, y con él el cliente de Ollama del agente y
// su circuit breaker. Quien clasifica hoy es el WORKER-CAJERO (`agent cajero`), que es OTRO PROCESO: lee
// las filas de la cola durable y su propio contrato desde `edge.db`.
//
// LO QUE ESTE STACK SIGUE HACIENDO, y es lo único: RECIBIR el `ConfigUpdate` del kind 'intents' que llega
// por el stream CloudLink, VALIDARLO y PERSISTIRLO en edge.db. Ese registro persistido es el ÚNICO canal
// entre la nube y el cajero (el worker lo sondea cada 30 s, cmd/agent/cajero.go), así que romper esta
// cadena deja al clasificador sin contrato para siempre y sin un solo error en el log.
//
// Con la feature OFF, Applier/Service son nil y solo Store queda vivo.
type IntentStack struct {
	// Applier persiste/valida/notifica los ConfigUpdate (edgeconfig.Service; nil ⇒ off, Ack tolerante).
	Applier cloudlink.ConfigApplier
	// Service es el mismo edgeconfig.Service (para Bootstrap al arrancar). nil ⇒ off.
	Service *edgeconfig.Service
	// Store lee la config persistida. SIEMPRE presente (el status lo consulta aun con la feature off).
	Store edgeconfig.Store

	// Enabled/Model reflejan la config del Edge. NO describen al decorador (que ya no existe): Enabled es
	// el interruptor de la feature `llm_intent` en esta máquina —el mismo que decide si una fila nace
	// reclamable o con la marca `apagado`— y Model es informativo para el operador.
	Enabled bool
	Model   string
}

// applier devuelve el ConfigApplier del stack de forma nil-safe (nil si la feature está off o el stack es
// nil): WithConfigApplier ignora nil ⇒ el adapter Ack-ea tolerante sin persistir.
func (s *IntentStack) applier() cloudlink.ConfigApplier {
	if s == nil {
		return nil
	}
	return s.Applier
}

// ConfigVersion devuelve la versión de la config 'intents' vigente (persistida) o "": alimenta GET
// /v1/intent/status con independencia de que la feature esté on/off.
//
// LEE DEL DISCO, no de memoria, y eso ahora importa más que antes: es EXACTAMENTE la misma fila que el
// worker-cajero sondea para cargar su contrato, así que lo que responde aquí es lo que el cajero verá.
// Es el dato honesto que el daemon todavía puede dar sobre la clasificación.
func (s *IntentStack) ConfigVersion() string {
	if s == nil || s.Store == nil {
		return ""
	}
	rec, ok, err := s.Store.Get(context.Background(), IntentsConfigKind)
	if err != nil || !ok {
		return ""
	}
	return rec.Version
}

// ClasificadorActivo reporta si el CLASIFICADOR de intenciones está encendido en este Edge. Lo consume el
// listener (Plan 051 Ola 2, T2.12) para que una fila que llega con el clasificador apagado nazca ya resuelta
// (`apagado`) en vez de gastar una plaza del semáforo del cajero.
//
// 🔄 CAMBIÓ DE FUENTE EN T3.0, y el cambio es una MEJORA, no un apaño. Antes leía `Decorator != nil`, que
// era un proxy de la feature: BuildIntent solo construía el decorador con la feature ON. Retirado el
// decorador, se lee el flag DIRECTO (`s.Enabled` = cfg.Intent.Enabled), que es lo que la semántica
// documentada de `app.MotivoApagado` siempre dijo —«la feature llm_intent está apagada»— y de paso elimina
// la salvedad que el comentario anterior arrastraba sobre `Decorator.ready`.
//
// ⚠️ LO QUE SIGUE SIN SABER: si el WORKER-CAJERO está realmente corriendo y con contrato cargado. Eso vive
// en otro proceso y el daemon no puede verlo. Con la feature ON y el cajero caído, las filas nacen `nuevo`
// y esperan en la cola hasta que el supervisor lo levante; el despachador las entrega igual al agotar su
// presupuesto. Es el fallo del lado barato y visible, que es el que se eligió a propósito.
func (s *IntentStack) ClasificadorActivo() bool {
	return s != nil && s.Enabled
}

// ClasificadorActivoFunc devuelve el predicado que el sessionmgr cablea en cada listener
// (sessionmgr.WithClasificadorActivo). Devuelve un MÉTODO y no un bool ya evaluado para no congelar la foto
// del arranque: quien lo llame lee el estado del stack en el momento del mensaje. Con el stack nil devuelve
// nil, y el Listener cae a su default SEGURO (activo).
func (s *IntentStack) ClasificadorActivoFunc() func() bool {
	if s == nil {
		return nil
	}
	return s.ClasificadorActivo
}

// BuildIntent construye el conducto del CONTRATO de intenciones (Plan 029) sobre la BD única YA migrada
// (la tabla edge_config la crea db.Migrate). El Store se crea SIEMPRE (lo consulta el status). Con
// cfg.Intent.Enabled=false devuelve el stack "vacío" (sin applier/service).
//
// 🔴 LO QUE ESTA FUNCIÓN YA NO HACE (T3.0), y por qué NO hay que devolverlo: no crea cliente de Ollama, no
// crea clasificador y no crea decorador. El daemon no habla con el LLM (REQ-051.10: «ningún otro proceso
// que el worker habla con Ollama»); si vuelves a poner un `ollama.New` aquí, ese requisito deja de
// cumplirse aunque el decorador no vuelva.
//
// 🔴 LO QUE SÍ HACE, Y ES LOAD-BEARING: registrar el kind 'intents' en el edgeconfig.Service. `Service.Apply`
// IGNORA los kinds no registrados (registrationFor → `!known` ⇒ log + Ack sin persistir), así que este
// RegisterKind es lo ÚNICO que hace que un ConfigUpdate de intenciones llegue a disco. Y ese disco es el
// único canal hacia el worker-cajero, que lo sondea cada 30 s. Si lo quitas, el cajero se queda sin
// contrato para siempre, `Listo()` devuelve false, no reclama ni una fila y NO HAY UN SOLO ERROR EN NINGÚN
// LOG. Se registra sin suscriptores: ya no hay nada en ESTE proceso que recargar en caliente.
func BuildIntent(cfg config.Config, database *sql.DB, log sharedlogger.Logger) *IntentStack {
	store := edgeconfig.NewSQLStore(database)
	st := &IntentStack{Store: store, Enabled: cfg.Intent.Enabled, Model: cfg.Intent.Model}
	if !cfg.Intent.Enabled {
		log.Info("clasificador de intenciones DESHABILITADO (WAPP_AGENT_INTENT_ENABLED=false): no se acepta config 'intents' y las filas de la cola nacen con la marca `apagado`")
		return st
	}

	svc := edgeconfig.NewService(store, log)
	// Validador SÍ, suscriptores NO. El validador es lo que impide que un blob corrupto sustituya al
	// contrato bueno en disco (last-known-good), y sigue siendo tan necesario como antes: el que se comería
	// la basura ahora no es un decorador en memoria, es el cajero en el arranque siguiente.
	svc.RegisterKind(IntentsConfigKind,
		func(payload []byte) error { _, err := intents.ParseAndValidate(payload); return err },
	)

	st.Applier = svc
	st.Service = svc
	// `wait_ms` es el presupuesto del DESPACHADOR (lo único que este proceso controla del camino de la
	// intención). El plazo de la inferencia y la URL de Ollama son del worker y se loguean allí: emitirlos
	// aquí sugeriría que este proceso los usa.
	log.Info("contrato de intenciones HABILITADO (Plan 029, ADR-0020); la clasificación la ejecuta el worker-cajero, no el daemon",
		"model", cfg.Intent.Model, "wait_ms", cfg.Intent.WaitMS)
	return st
}
