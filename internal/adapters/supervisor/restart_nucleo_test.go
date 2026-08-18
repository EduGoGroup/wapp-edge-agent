package supervisor

// restart_nucleo_test.go — EL RELANZADO DEL NÚCLEO, QUE TIENE PLANO HTTP (Plan 051 Ola 5 · T5.4).
//
// 🔴 QUÉ SE CUSTODIA AQUÍ Y POR QUÉ NO LO CUBRÍA restart_test.go. Todos los tests del relanzado se
// escribieron para el CAJERO: hijo sin plano HTTP, ready por ProbeProcesoVivo (una gracia y ya). El núcleo
// es la otra forma — readiness por GET /v1/health de verdad, con socket— y desde T5.4 lleva la misma
// política. La diferencia no es cosmética: en el camino del núcleo el hijo relanzado tiene que volver a
// ABRIR el socket que el anterior dejó, y su readiness puede fallar por sí sola, cosa que con
// ProbeProcesoVivo es casi imposible.
//
// EL HALLAZGO DE CAMPO QUE LO ORIGINA (PC-13): la unidad systemd vigila a `wapp-ctl`, y el núcleo es hijo
// suyo. Con el núcleo muerto y el portero vivo, `systemctl is-active wapp-edge` dice `active`. Tras un
// `kill -9`, el cajero volvió solo y el núcleo no: ~5 minutos sin poder recibir WhatsApp con el indicador
// en verde.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo, y todas se ejecutaron.

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"
)

// cfgNucleo arma la Config de un hijo CON plano HTTP (el fake en modo normal: sirve /v1/health por el
// socket y cierra limpio con SIGTERM), que es la forma del núcleo real, con el relanzado activado y un
// backoff minúsculo para no gastar segundos de reloj.
func cfgNucleo(t *testing.T) Config {
	t.Helper()
	cfg := fakeCfg(t, "")
	cfg.Restart = RestartPolicy{Enabled: true, MinBackoff: 10 * time.Millisecond, MaxBackoff: 40 * time.Millisecond}
	return cfg
}

// TestNucleo_MuerteVIOLENTAVuelveSolo es el escenario de PC-13 reproducido: al núcleo ya ready se le hace
// un SIGKILL desde fuera (nadie pidió pararlo) y tiene que volver ARRIBA Y READY por sí mismo.
//
// Se exige un PID DISTINTO, no solo "running": con el lock file de por medio, comprobar únicamente el
// estado dejaría pasar un supervisor que se limita a creerse su propio fichero.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): quitar `Restart` de cfgNucleo —que es EXACTAMENTE el estado
// del que sale T5.4, porque la Config del núcleo en cmd/wapp-ctl no lo llevaba— ⇒ el núcleo se queda
// muerto para siempre y el test agota su espera.
func TestNucleo_MuerteVIOLENTAVuelveSolo(t *testing.T) {
	sup := New(cfgNucleo(t), nil)

	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start del núcleo: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })

	primero := sup.Status(context.Background())
	if primero.State != StateRunning || primero.PID == 0 {
		t.Fatalf("el núcleo no quedó arriba tras Start: %+v", primero)
	}

	// SIGKILL: la muerte que NO da ninguna oportunidad de cerrar limpio y que deja el socket file huérfano.
	// Es la del `kill -9` de PC-13, y la que distingue este camino del de un SIGTERM ordenado.
	if err := syscall.Kill(primero.PID, syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL al núcleo (pid %d): %v", primero.PID, err)
	}

	vuelto := esperarHasta(10*time.Second, func() bool {
		st := sup.Status(context.Background())
		return st.State == StateRunning && st.PID != 0 && st.PID != primero.PID && st.Healthy
	})
	if !vuelto {
		st := sup.Status(context.Background())
		t.Fatalf("el núcleo NO volvió solo tras un kill -9 (estado %+v, pid anterior %d): en campo eso es el "+
			"Edge sin poder recibir WhatsApp mientras `systemctl is-active` sigue diciendo `active`, que es "+
			"el defecto entero que T5.4 cierra", st, primero.PID)
	}
}

// TestNucleo_LaParadaPEDIDAGanaAlRelanzado es EL RIESGO NÚMERO UNO de activar el relanzado del núcleo: si
// se rompiera, POST /v1/daemon/stop dejaría de poder parar el daemon y no habría forma de bajarlo salvo
// matando a wapp-ctl.
//
// Se comprueba con el hijo VIVO Y ADOPTADO (no en mitad de un backoff, que ya cubre
// TestStopDuranteBackoffNoRelanza): ese es el caso que ocurre de verdad al pulsar «parar» en la consola.
// Y se espera DESPUÉS un tiempo largo comparado con el backoff configurado, porque un relanzado indebido
// no es inmediato: llega tarde, que es como se colaría en un test que solo mirase el estado justo después
// de Stop.
//
// ⚠️ MUTACIÓN VERDE — HALLAZGO, NO DEFECTO DEL TEST. Este comportamiento está defendido POR TRIPLICADO en
// Stop y trasSalida, y ninguna mutación SUELTA lo pone en rojo (las tres se ejecutaron):
//
//  1. quitar `s.p.restartable = false` de Stop      ⇒ VERDE (cortan las otras dos)
//  2. quitar `s.stopping = true` de Stop            ⇒ VERDE (cortan las otras dos)
//  3. quitar `s.cancelPendingRestartLocked()`       ⇒ VERDE (cortan las otras dos)
//
// El rojo solo llega quitando las TRES a la vez, y entonces el núcleo resucita tras el Stop. Se deja
// escrito porque es la conclusión útil: la garantía no depende de una línea que alguien pueda borrar por
// descuido, y un test por conducta no puede custodiar aquí una guardia concreta — solo la propiedad.
func TestNucleo_LaParadaPEDIDAGanaAlRelanzado(t *testing.T) {
	cfg := cfgNucleo(t)
	sup := New(cfg, nil)

	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start del núcleo: %v", err)
	}
	if st := sup.Status(context.Background()); st.State != StateRunning {
		t.Fatalf("el núcleo no quedó arriba tras Start: %+v", st)
	}

	if err := sup.Stop(context.Background()); err != nil {
		t.Fatalf("Stop del núcleo: %v", err)
	}

	// Un relanzado indebido tardaría un backoff en aparecer; se le da MUCHO más que eso.
	resucito := esperarHasta(1*time.Second, func() bool {
		return sup.Status(context.Background()).State == StateRunning
	})
	if resucito {
		t.Fatal("el núcleo RESUCITÓ tras una parada pedida: POST /v1/daemon/stop dejaría de poder bajar el " +
			"daemon, y la única forma de pararlo sería matar a wapp-ctl")
	}
	if _, err := os.Stat(cfg.PIDFile); !os.IsNotExist(err) {
		t.Errorf("el lock file sobrevivió a la parada pedida (%v): el siguiente arranque lo leería como un "+
			"núcleo vivo y se retiraría por idempotencia", err)
	}
}

// TestNucleo_TrasUnStopUnStartExplicitoVuelveAArmarElRelanzado cierra el ciclo del operador: parar y
// volver a arrancar no puede dejar el núcleo sin red. `stopping` es pegajoso a propósito (nada relanza tras
// un Stop) y lo único que lo levanta es un Start explícito; si eso se rompiera, el primer Stop de la vida
// del proceso desactivaría el relanzado hasta el siguiente reinicio de wapp-ctl, en silencio.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): quitar `s.stopping = false` de Start ⇒ el núcleo arranca,
// pero al morir ya no vuelve: el relanzado queda muerto para el resto de la vida del supervisor.
func TestNucleo_TrasUnStopUnStartExplicitoVuelveAArmarElRelanzado(t *testing.T) {
	sup := New(cfgNucleo(t), nil)

	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("primer Start: %v", err)
	}
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("segundo Start (tras la parada): %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })

	segundo := sup.Status(context.Background())
	if segundo.State != StateRunning || segundo.PID == 0 {
		t.Fatalf("el núcleo no volvió a arrancar tras el Stop: %+v", segundo)
	}
	if err := syscall.Kill(segundo.PID, syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL al núcleo (pid %d): %v", segundo.PID, err)
	}

	vuelto := esperarHasta(10*time.Second, func() bool {
		st := sup.Status(context.Background())
		return st.State == StateRunning && st.PID != 0 && st.PID != segundo.PID && st.Healthy
	})
	if !vuelto {
		t.Fatal("tras un ciclo parar/arrancar el núcleo ya no se relanza solo: un operador que use el botón " +
			"de parar dejaría el Edge sin red de seguridad hasta el siguiente reinicio de wapp-ctl, y sin " +
			"ninguna señal de que eso ha pasado")
	}
}

// TestNucleo_ConRelanzadoActivoNoSeArrancaSolo fija la PROPIEDAD NEGATIVA que sostiene el contrato de
// arranque del Edge: el relanzado gobierna al hijo que YA arrancó y se murió, nunca al que nadie ha pedido.
//
// Importa porque en el VPS el núcleo se pide por HTTP —el `ExecStartPost` de la unidad hace `curl` a
// POST /v1/daemon/start—, y `wapp-ctl` corre sin -autostart. Si activar `Restart` hubiera convertido eso en
// un autoarranque, el núcleo levantaría ANTES de que el entorno de la unidad esté puesto, que es justo la
// clase de cambio silencioso que ya costó una sesión de WhatsApp en este proyecto (el EnvironmentFile que
// faltaba). El derecho a relanzarse se concede en adoptLocked, y a adoptLocked solo se llega tras un Start.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): hacer que New arranque el hijo cuando Restart.Enabled (un
// `go s.Start(context.Background())` al final de New) ⇒ el núcleo se levanta sin que nadie lo pida.
func TestNucleo_ConRelanzadoActivoNoSeArrancaSolo(t *testing.T) {
	cfg := cfgNucleo(t)
	sup := New(cfg, nil)

	// Muy por encima del MinBackoff de este test (10 ms): si algo fuera a lanzarse solo, ya habría pasado.
	if arranco := esperarHasta(300*time.Millisecond, func() bool {
		return sup.Status(context.Background()).State == StateRunning
	}); arranco {
		t.Fatal("el supervisor arrancó el núcleo SIN que nadie lo pidiera: el arranque del Edge dejaría de " +
			"estar gobernado por POST /v1/daemon/start (el ExecStartPost de la unidad) y el núcleo levantaría " +
			"antes de que su entorno esté puesto")
	}
	if _, err := os.Stat(cfg.PIDFile); !os.IsNotExist(err) {
		t.Errorf("apareció un lock file sin que nadie arrancara nada (%v): el siguiente Start lo leería y se "+
			"retiraría por idempotencia, dejando el núcleo abajo para siempre", err)
	}
}
