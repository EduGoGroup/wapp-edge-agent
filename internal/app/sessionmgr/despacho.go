package sessionmgr

import (
	"context"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/despachador"
)

// despacho.go — CABLEADO DEL DESPACHADOR DE LA COLA POR SESIÓN (Plan 051 Ola 3 · T3.3).
//
// El Manager ya poseía el ciclo de vida del LISTENER de cada sesión (nace en startListener, muere en
// stopLive/Stop). Esta ola le añade un segundo hilo por sesión: el DESPACHADOR, que drena la cola durable
// de esa misma sesión y entrega al cable en orden de `seq`. Los dos cuelgan del MISMO context cancelable
// y del MISMO WaitGroup, porque son la misma vida: una sesión que deja de escuchar tampoco tiene nada que
// drenar, y una sesión que se desvincula no puede seguir entregando sus mensajes viejos.
//
// 🔴 SIN COLA NO HAY ENTREGA, Y LA POLÍTICA DE ARRANQUE YA ESTÁ DECIDIDA (2026-08-17). T3.0 retiró el
// `sink.Deliver` inline del listener: este bucle es hoy el ÚNICO camino por el que un entrante llega a la
// nube, así que una sesión sin despachador no entrega nada. Mientras la apertura de `cola_entrantes.db` en
// daemon.go NO era fatal, ese nil era alcanzable de verdad y el agujero se tapaba con avisos (el listener
// gritando al construirse). Ya no: el daemon no arranca si la cola no se puede abrir, migrar o construir.
//
// EL CRITERIO DE ESTE FICHERO NO CAMBIA —sin cola no se arranca despachador y no se toca nada más—, y los
// caminos nil-safe SE CONSERVAN: aquí no se decide la política de arranque, y los tests cablean Managers a
// mano sin cola ni mux. Lo que cambió es que producción ya no puede llegar a ellos.
//
// Un despachador construido sobre una cola nil sería un pánico — por eso `despachador.New` rechaza el nil
// en frío, en vez de tolerarlo hasta la primera vuelta del bucle de una sesión viva.

// WithColaDespachador inyecta el lado DESPACHADOR de la cola durable (Plan 051 Ola 3): con él, cada sesión
// arranca —junto a su listener— el bucle que drena su cola y entrega al cable en orden de `seq`, con el
// presupuesto de espera al cajero acotado por WithPresupuestoIntent.
//
// En producción lo cablea el daemon desde wiring.BuildColaDespachador (el MISMO *Store que respalda
// WithColaEntrantes; son dos caras del mismo adaptador, no dos colas). nil se IGNORA.
//
// 🔴 DESDE T3.0 ESTA OPCIÓN ES EL ÚNICO CAMINO DE ENTREGA DE ENTRANTES. Durante la Ola 1 convivió con el
// `sink.Deliver` del listener (doble entrega declarada, que el cable toleraba deduplicando por
// `wa_message_id`); ese camino se retiró. Omitir esta opción ya no «deja el Manager como antes de la Ola 3»:
// deja al Edge recibiendo mensajes que nadie sube.
func WithColaDespachador(c app.ColaDespachador) Option {
	return func(m *Manager) {
		if c != nil {
			m.colaDespachador = c
		}
	}
}

// WithPresupuestoIntent fija cuánto RETIENE el despachador la cabeza de una sesión esperando a que el
// cajero la clasifique, antes de entregarla igual sin intención (WAPP_AGENT_INTENT_WAIT_MS, 4000 ms por
// defecto). Es el techo de latencia que la cola añade al camino de un mensaje.
//
// Un valor <= 0 se IGNORA (no desactiva nada): manda el default del propio despachador. Un presupuesto de
// 0 no significaría «no esperes», significaría «despacha sin intención SIEMPRE», que apaga el clasificador
// entero por la puerta de atrás y sin decirlo.
func WithPresupuestoIntent(d time.Duration) Option {
	return func(m *Manager) {
		if d > 0 {
			m.presupuestoIntent = d
		}
	}
}

// startDespachador arranca, si procede, el bucle de drenado de UNA sesión. Lo llama startListener con el
// ctx del listener ya creado, de modo que el mismo cancel apaga a los dos.
//
// TRES PUERTAS ANTES DE ARRANCAR, y ninguna es fatal para la sesión — el Manager no es el dueño de la
// política de arranque del proceso (ese es el daemon, y desde el 2026-08-17 él sí falla sin cola):
//
//  1. Sin cola despachadora (opción no inyectada) ⇒ silencio absoluto: en producción no ocurre, y avisar
//     por cada sesión sería ruido por un cableado de test.
//  2. Sin multiplexor CloudLink (tests que cablean newListener a mano) ⇒ no hay sink al que entregar.
//  3. Con sink nil (el mux existe pero devuelve nil, como los dobles de los tests) ⇒ igual.
//
// EL SINK ES EL CRUDO DE LA SESIÓN, `mux.SinkFor(sid)`. Cuando este bucle se escribió, había que decir
// además «deliberadamente SIN el inboundDecorator»: existía un decorador que habría mandado el mensaje a
// Ollama por segunda vez para reescribir la intención que la cola ya traía resuelta en su `intent_json`. Ese
// decorador se retiró en T3.0 y hoy no hay más sink que el crudo — la advertencia se conserva por si alguien
// se plantea reintroducir una envoltura aquí: la intención llega YA resuelta, no hay nada que decorar.
func (m *Manager) startDespachador(ctx context.Context, s *liveSession) {
	if m.colaDespachador == nil {
		return
	}
	if m.cloudMux == nil {
		s.log.Warn("sessionmgr: cola despachadora inyectada pero sin multiplexor CloudLink; la sesión NO drena su cola")
		return
	}
	sid := s.meta.SessionID
	sink := m.cloudMux.SinkFor(sid)
	if sink == nil {
		s.log.Warn("sessionmgr: el multiplexor no dio sink para la sesión; la sesión NO drena su cola")
		return
	}

	d, err := despachador.New(despachador.Deps{
		Cola:      m.colaDespachador,
		Sink:      sink,
		SessionID: sid,
		// s.log ya arrastra session_id/jid: la traza del drenado sale etiquetada por sesión, igual que la
		// de la carga de DEK.
		Log:         s.log,
		Presupuesto: m.presupuestoIntent,
	})
	if err != nil {
		// Error y no fatal: la sesión queda escuchando y ENCOLANDO, pero sin drenar. Nada se pierde —las
		// filas siguen en disco y un reinicio las despacha—, pero nada sube: el síntoma es su cola creciendo.
		s.log.Error("sessionmgr: no se pudo construir el despachador de la cola; la sesión escucha pero NO drena", "error", err)
		return
	}

	// DOS contadores para la MISMA goroutine, y cada uno responde una pregunta distinta:
	//   - m.wg es el join GLOBAL del apagado ordenado (Stop espera a que no quede ninguna goroutine viva);
	//   - s.despachadores es el join POR SESIÓN, el que necesita stopLive para el borrado quirúrgico: al
	//     desvincular UNA sesión hay que esperar a que SU despachador haya parado antes de borrarle la DEK,
	//     porque si no seguiría intentando descifrar filas de una sesión que ya no existe.
	// Es exactamente el mismo par (WaitGroup global + señal por sesión) que ya tienen listener y `s.done`.
	s.despachadores.Add(1)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer s.despachadores.Done()
		// Red de seguridad de último recurso. El despachador YA recupera por vuelta de bucle (aislamiento
		// del molde de commandDispatcher), así que llegar aquí significa un pánico FUERA del bucle, que es
		// un bug. Se recupera igual porque un pánico sin recuperar en una goroutine se lleva el PROCESO
		// entero —y con él las demás sesiones—, que es justo lo que §10.H prohíbe.
		//
		// 🔴 Y NO SE REARRANCA, a diferencia del listener: el listener reintenta con backoff porque su fallo
		// esperado es la RED (una caída de socket es normal y transitoria), mientras que un pánico aquí es
		// un defecto de código que se repetiría idéntico en el reintento. La sesión queda escuchando y sin
		// drenar, con su contador de pánicos y su línea de Error; el rearranque de verdad es reiniciar.
		//
		// INV-051.1: el valor recuperado NO se loguea (puede arrastrar el texto del mensaje).
		defer func() {
			if r := recover(); r != nil {
				s.log.Error("sessionmgr: pánico en el despachador de la sesión (aislado); la escucha sigue, el drenado NO")
			}
		}()
		if err := d.Run(ctx); err != nil {
			s.log.Error("sessionmgr: el despachador de la sesión terminó con error", "error", err)
		}
	}()
}

// waitDespachadores bloquea hasta que el despachador de ESTA sesión (si lo hubo) haya retornado. Es el
// gemelo de waitDone para el segundo hilo de la sesión, y lo usa stopLive para no borrar la DEK de una
// sesión cuyo despachador todavía está intentando abrir sus filas.
//
// Sin despachador arrancado es un no-op inmediato (un WaitGroup en cero).
func (s *liveSession) waitDespachadores() { s.despachadores.Wait() }
