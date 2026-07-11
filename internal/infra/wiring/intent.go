package wiring

import (
	"context"
	"database/sql"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cloudlink"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/edgeconfig"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/intent"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-intent/classifier"
	"github.com/EduGoGroup/wapp-edge-intent/ollama"
	"github.com/EduGoGroup/wapp-shared/intents"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// intentsConfigKind es el kind de config empujada que gobierna el clasificador (Plan 029, ADR-0021).
const intentsConfigKind = "intents"

// IntentStack agrupa las piezas del CLASIFICADOR de intenciones (Plan 029) que el arranque cablea al conducto
// CloudLink y al plano de control. Con la feature OFF, Decorator/Applier/Service/Prober son nil (cableado
// idéntico al previo) y solo Store queda vivo (GET /v1/intent/status reporta enabled=false + config_version
// persistida si la hubiera).
type IntentStack struct {
	// Decorator envuelve el sink de entrada para anotar la intención LLM (nil ⇒ off, sink sin decorar).
	Decorator *intent.Decorator
	// Applier persiste/valida/notifica los ConfigUpdate (edgeconfig.Service; nil ⇒ off, Ack tolerante).
	Applier cloudlink.ConfigApplier
	// Service es el mismo edgeconfig.Service (para Bootstrap al arrancar). nil ⇒ off.
	Service *edgeconfig.Service
	// Store lee la config persistida. SIEMPRE presente (el status lo consulta aun con la feature off).
	Store edgeconfig.Store

	// Enabled/Model reflejan la config; Prober sondea Ollama para el status (nil ⇒ off, ollama_ok=false).
	Enabled bool
	Model   string
	Prober  intent.HealthProber
}

// WrapSink envuelve un sink con el decorador de intenciones si la feature está ON; si no, lo devuelve tal
// cual. Costura de cableado para el camino single-sesión (BuildSink); el multi-sesión usa Decorator.Wrap vía
// sessionmgr.WithInboundDecorator.
func (s *IntentStack) WrapSink(next app.InboundSink) app.InboundSink {
	if s == nil || s.Decorator == nil {
		return next
	}
	return s.Decorator.Wrap(next)
}

// applier devuelve el ConfigApplier del stack de forma nil-safe (nil si la feature está off o el stack es
// nil): WithConfigApplier ignora nil ⇒ el adapter Ack-ea tolerante sin persistir.
func (s *IntentStack) applier() cloudlink.ConfigApplier {
	if s == nil {
		return nil
	}
	return s.Applier
}

// DecoratorWrap devuelve la función de envoltura del sink para el camino multi-sesión
// (sessionmgr.WithInboundDecorator), o nil si la feature está off. Devolver nil (no una identidad) mantiene
// el cableado del Manager idéntico byte a byte al previo cuando el clasificador está deshabilitado.
func (s *IntentStack) DecoratorWrap() func(app.InboundSink) app.InboundSink {
	if s == nil || s.Decorator == nil {
		return nil
	}
	return s.Decorator.Wrap
}

// ConfigVersion devuelve la versión de la config 'intents' vigente (persistida) o "": alimenta GET
// /v1/intent/status con independencia de que la feature esté on/off.
func (s *IntentStack) ConfigVersion() string {
	if s == nil || s.Store == nil {
		return ""
	}
	rec, ok, err := s.Store.Get(context.Background(), intentsConfigKind)
	if err != nil || !ok {
		return ""
	}
	return rec.Version
}

// CircuitFunc devuelve el lector del estado del circuito para el status (nil si la feature está off).
func (s *IntentStack) CircuitFunc() func() string {
	if s == nil || s.Decorator == nil {
		return nil
	}
	return s.Decorator.Circuit
}

// BuildIntent construye el stack del clasificador de intenciones (Plan 029) sobre la BD única YA migrada
// (la tabla edge_config la crea db.Migrate). El Store se crea SIEMPRE (lo consulta el status). Con
// cfg.Intent.Enabled=false devuelve el stack "vacío" (sin decorador/applier): el cableado del sink queda
// idéntico byte a byte al previo.
//
// Con la feature ON: un cliente Ollama local + un clasificador (arranca sin config útil hasta el primer push
// o el Bootstrap), el decorador compartido y el edgeconfig.Service con el kind 'intents' registrado
// (validador = intents.ParseAndValidate; suscriptor = recarga en caliente del clasificador).
func BuildIntent(cfg config.Config, database *sql.DB, log sharedlogger.Logger) *IntentStack {
	store := edgeconfig.NewSQLStore(database)
	st := &IntentStack{Store: store, Enabled: cfg.Intent.Enabled, Model: cfg.Intent.Model}
	if !cfg.Intent.Enabled {
		log.Info("clasificador de intenciones DESHABILITADO (WAPP_AGENT_INTENT_ENABLED=false): sink sin decorar")
		return st
	}

	client := ollama.New(cfg.Intent.OllamaURL)
	// Arranca con una config vacía: el decorador no clasifica (ready=false) hasta que SetConfig llegue por un
	// push del Cloud o por el Bootstrap de la config persistida.
	cls := classifier.New(client, cfg.Intent.Model, &intents.Config{})
	dec := intent.New(cls, time.Duration(cfg.Intent.TimeoutMS)*time.Millisecond, log)

	svc := edgeconfig.NewService(store, log)
	svc.RegisterKind(intentsConfigKind,
		func(payload []byte) error { _, err := intents.ParseAndValidate(payload); return err },
		func(rec edgeconfig.Record) {
			parsed, err := intents.ParseAndValidate(rec.Payload)
			if err != nil {
				// El Service ya validó antes de persistir/notificar; un fallo aquí sería un blob corrupto en
				// disco. Se loguea y se deja el clasificador con la config previa (no se recarga con basura).
				log.Error("intent: config 'intents' ilegible al recargar (se conserva la anterior)",
					"version", rec.Version, "error", err)
				return
			}
			dec.SetConfig(parsed, rec.Version)
		},
	)

	st.Decorator = dec
	st.Applier = svc
	st.Service = svc
	st.Prober = client
	log.Info("clasificador de intenciones HABILITADO (Plan 029, ADR-0020)",
		"ollama_url", cfg.Intent.OllamaURL, "model", cfg.Intent.Model, "timeout_ms", cfg.Intent.TimeoutMS)
	return st
}
