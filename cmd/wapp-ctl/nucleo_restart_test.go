package main

// nucleo_restart_test.go — LA POLÍTICA DE RELANZADO DEL NÚCLEO (Plan 051 Ola 5 · T5.4).
//
// 🔴 QUÉ CLASE DE TEST ES ESTE, dicho sin adornos: es un CERROJO DE REGRESIÓN sobre una decisión, no una
// prueba de conducta. La conducta —que un hijo con esta política vuelva solo, y que una parada pedida gane
// igual— la custodian los tests del paquete supervisor (restart_nucleo_test.go). Lo que se fija aquí es que
// la Config DEL NÚCLEO la lleve puesta.
//
// Y hace falta precisamente porque el defecto de campo era de esa forma: un CAMPO QUE NO ESTABA en un
// struct literal dentro de main(). No hay conducta que probar en un campo ausente —el valor cero de
// RestartPolicy es «no relanzar», que es una política válida y silenciosa—, no hay error que devolver y no
// hay rama que recorrer. El resultado en el VPS fue ~5 minutos sin poder recibir WhatsApp tras un `kill -9`,
// con `systemctl is-active wapp-edge` diciendo `active` todo el rato, porque systemd vigila a `wapp-ctl` y
// el núcleo es hijo suyo. El cajero sí llevaba la política y volvió solo; el núcleo no.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo, y todas se ejecutaron.

import (
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/supervisor"
)

// TestConfigNucleo_LlevaElRelanzadoAutomatico.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (ejecutada): borrar la línea `Restart: supervisor.RestartPolicy{Enabled:
// true}` de configNucleo ⇒ vuelve exactamente el estado del que sale T5.4, y el resto del árbol sigue en
// verde (ningún otro test mira esta Config: main() no lo ejercita nadie).
func TestConfigNucleo_LlevaElRelanzadoAutomatico(t *testing.T) {
	cfg := configNucleo("/ruta/agent", "/ruta/edge.sock", "")

	if !cfg.Restart.Enabled {
		t.Fatal("el supervisor del NÚCLEO se construye sin relanzado automático: un núcleo que muera solo se " +
			"queda muerto hasta que alguien llame a POST /v1/daemon/start, mientras systemd —que vigila a " +
			"wapp-ctl, no al núcleo— sigue reportando la unidad como `active`. Es el defecto de PC-13")
	}
}

// TestConfigNucleo_NoCambiaNadaMasDeLoQueYaHabia: la reparación de T5.4 es UNA línea, y esto lo fija. El
// resto de la Config del núcleo tiene que seguir siendo la de antes, y muy en particular el PIDFile vacío,
// que es lo que hace que el supervisor derive <socket>.pid — el lock del núcleo. Ponerle aquí un default
// propio lo separaría del que ya hay escrito en disco en cada Edge en producción, y el arranque siguiente
// no vería vivo al núcleo que sí lo está: lanzaría un SEGUNDO `agent serve` sobre la misma BD.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - dar a PIDFile un valor por defecto en configNucleo (p. ej. socketPath+".nucleo.pid") ⇒ dos locks
//     distintos para el mismo hijo entre dos versiones del binario.
//   - añadir un ReadyProbe propio ⇒ el núcleo dejaría de sondear GET /v1/health y su `Healthy` pasaría a la
//     familia DÉBIL (ver Status.Probe), que es justo la señal que aquí sí se puede tener de verdad.
func TestConfigNucleo_NoCambiaNadaMasDeLoQueYaHabia(t *testing.T) {
	cfg := configNucleo("/ruta/agent", "/ruta/edge.sock", "")

	if cfg.AgentBin != "/ruta/agent" || cfg.SocketPath != "/ruta/edge.sock" {
		t.Errorf("configNucleo no pasa el binario y el socket tal cual: got %q y %q", cfg.AgentBin, cfg.SocketPath)
	}
	if cfg.PIDFile != "" {
		t.Errorf("configNucleo inventó un PIDFile (%q) en vez de dejar que el supervisor derive <socket>.pid: "+
			"el lock cambiaría de sitio entre versiones y el arranque siguiente lanzaría un segundo núcleo "+
			"sobre la misma BD", cfg.PIDFile)
	}
	if cfg.ReadyProbe != nil {
		t.Error("el núcleo tiene plano HTTP y su readiness es GET /v1/health: un ReadyProbe propio lo " +
			"degradaría a la familia `proceso-vivo`, que no distingue un núcleo sano de uno colgado")
	}
	// El -pid-file explícito del operador sigue mandando (es un flag documentado).
	if propio := configNucleo("/ruta/agent", "/ruta/edge.sock", "/tmp/mio.pid"); propio.PIDFile != "/tmp/mio.pid" {
		t.Errorf("configNucleo ignora el -pid-file explícito: got %q", propio.PIDFile)
	}
}

// Compila-o-rompe: si alguien cambia el tipo de la política, este fichero lo dice aquí y no en main().
var _ supervisor.RestartPolicy = configNucleo("", "", "").Restart
