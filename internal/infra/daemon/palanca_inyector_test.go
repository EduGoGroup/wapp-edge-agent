package daemon

// palanca_inyector_test.go — EL GUARDARRAÍL DEL REGISTRO CONDICIONAL (MP-10 Parte A).
//
// 🔴 QUÉ SE CUSTODIA AQUÍ. `registrarInyectorEntrantes` es cableado puro: no tiene ramas interesantes, no
// devuelve errores y «se ve bien». Y decide algo que no se puede corregir después: si la ruta que fabrica
// entrantes FALSOS —que acaban en la cola durable y, si el despachador drena, EN LA NUBE— existe o no existe
// en un Edge de producción. Un `if !cfg.InyectorEntrantes` invertido la registra en TODOS los Edge del
// ecosistema y no la registra en el único que la pidió, con los cuatro gates en verde y sin un solo error.
//
// El molde es el de palanca_despachador_test.go, que cerró el mismo agujero para la otra palanca: `Run` no
// lo ejercita ningún test (sus únicos importadores son `cmd/agent`, cuyos tests no lo llaman), así que la
// única forma de mirar esto es extraer la decisión a una función y preguntarle a un `*server.Server` REAL
// qué contesta en los dos estados.
//
// Se pregunta por el ESTADO DEL MUX y no por una bandera interna a propósito: lo que importa no es que la
// función tomara el camino corto, es que la superficie HTTP no exista. Un test que mirase un booleano
// seguiría en verde si alguien registrara la ruta dos líneas más abajo.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo. NO se han ejecutado (este
// entorno no tiene toolchain de Go): están RAZONADAS contra el código, no verificadas en verde.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/diag"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/server"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// inyectorFalso cuenta las inyecciones que le llegan. Sustituye al *sessionmgr.Manager: lo que se mide aquí
// es el CABLEADO (¿llega la petición HTTP hasta el puerto?), no el enrutado por sesión, que ya tiene sus
// propios tests en internal/app/sessionmgr.
type inyectorFalso struct {
	veces int
	lotes []string
}

func (f *inyectorFalso) InyectarEntrante(_ context.Context, _ string, p app.InyeccionEntrante) (bool, error) {
	f.veces++
	f.lotes = append(f.lotes, p.Lote)
	return true, nil
}

// autorizadorEspia deja pasar todo y ANOTA con qué recurso se le preguntó. Existe porque el `resource` y el
// `write` que se pasan a HandleAuthorized son invisibles desde fuera —con el Authorizer sin cablear el guard
// es un no-op— y son justo lo que decide QUIÉN puede inyectar tráfico falso en un Edge de producción.
type autorizadorEspia struct {
	recursos []string
	writes   []bool
}

func (a *autorizadorEspia) Authorize(_ context.Context, _, resource string, write bool) (bool, int, string, string) {
	a.recursos = append(a.recursos, resource)
	a.writes = append(a.writes, write)
	return true, 0, "", ""
}

// servidorDePrueba arma un *server.Server real (sin socket: se le habla por ServeHTTP) con el espía de auth
// cableado, y devuelve además el buffer donde cae el log del daemon, que es donde tiene que salir el grito.
func servidorDePrueba(t *testing.T) (*server.Server, *autorizadorEspia, sharedlogger.Logger, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	log := sharedlogger.New(sharedlogger.WithWriter(buf))
	srv := server.New(server.Config{SocketPath: "", Version: "test"}, log, nil)
	espia := &autorizadorEspia{}
	srv.SetAuthorizer(espia)
	return srv, espia, log, buf
}

// pedirInyeccion dispara la petición contra el servidor sin levantar socket (Server implementa http.Handler,
// y es su ServeHTTP el que produce el 404/405 — no el handler).
func pedirInyeccion(srv *server.Server, metodo, cuerpo string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(metodo, diag.RutaInyectar, strings.NewReader(cuerpo))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	return w
}

// TestPalancaInyector_ConLaPalancaBAJADALaRutaNiSiquieraEXISTE es EL test de esta tarea, y el único cuyo
// fallo es un incidente de seguridad y no una molestia.
//
// Se exige un 404 y NO un 403 a propósito. Un 403 significaría que la ruta está registrada, el handler
// construido y el cuerpo decodificándose en todos los Edge del ecosistema, con un `if` dentro del handler
// como único guardián de un camino que fabrica entrantes falsos y los sube a la nube. El 404 significa que
// ese código no está en el mux: la superficie es CERO, no «cero mientras el `if` de dentro se porte bien».
//
// Se afirma además que el puerto no se tocó y que el log NO grita: un aviso que sale en los Edge sanos es
// ruido, y el ruido es lo que hace que el aviso de verdad no se lea.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas: sin toolchain de Go en este entorno):
//   - en registrarInyectorEntrantes, `if !cfg.InyectorEntrantes { return }` → `if cfg.InyectorEntrantes` ⇒ la
//     ruta aparece en TODOS los Edge (aquí sale 405 o 200 en vez de 404) y desaparece en el que la pidió (lo
//     caza el test de abajo).
//   - borrar el `return` y registrar siempre, dejando la palanca dentro del handler ⇒ 405/400 en vez de 404:
//     la superficie queda viva, que es justo el diseño que D2 prohíbe.
//   - registrar la ruta antes del `if` ⇒ idéntico, por otro camino.
func TestPalancaInyector_ConLaPalancaBAJADALaRutaNiSiquieraEXISTE(t *testing.T) {
	srv, _, log, buf := servidorDePrueba(t)
	fake := &inyectorFalso{}

	registrarInyectorEntrantes(srv, config.Config{InyectorEntrantes: false}, fake, log)

	w := pedirInyeccion(srv, http.MethodPost, `{"session_id":"s-1","texto":"hola"}`)
	if w.Code != http.StatusNotFound {
		t.Errorf("con la palanca BAJADA la ruta %s tiene que NO EXISTIR (404) y contestó %d: un 403 o un 400 "+
			"significan que el handler está registrado y que el único guardián de la fabricación de entrantes "+
			"falsos es un `if` dentro de él. Cuerpo: %s", diag.RutaInyectar, w.Code, w.Body.String())
	}
	if fake.veces != 0 {
		t.Errorf("con la palanca BAJADA llegaron %d inyecciones al puerto: se están fabricando entrantes falsos "+
			"en un Edge que no lo pidió", fake.veces)
	}
	if strings.Contains(buf.String(), "WAPP_AGENT_INYECTOR_ENTRANTES") {
		t.Errorf("el daemon GRITA la palanca del inyector en un Edge que no la tiene puesta: un aviso que sale "+
			"siempre deja de leerse, y este tiene que leerse la vez que sí importa. Log: %q", buf.String())
	}
}

// TestPalancaInyector_ConLaPalancaECHADALaRutaEXISTEYElDaemonLoGRITA es el CASO DE CONTROL, y sin él el test
// de arriba no vale nada: un `registrarInyectorEntrantes` que no registrara NUNCA lo dejaría en verde.
//
// Afirma cuatro cosas que se rompen por separado:
//
//   - la ruta responde (no 404) y la petición LLEGA al puerto — el cableado completo;
//   - el verbo registrado es POST: un GET da 405 y no 404. Eso prueba de paso que el handler NO valida el
//     método por su cuenta (D2: el 405 con su cabecera `Allow` lo produce server.ServeHTTP);
//   - el gate de auth se evaluó con el recurso y el `write` correctos — con `edge.status.read` cualquier
//     operador de solo-lectura podría inyectar tráfico falso en producción;
//   - el arranque GRITA a nivel WARN y nombra la variable: el estado que deja —números de latencia que no
//     son de tráfico real— es indistinguible de un Edge sano mirando solo la línea del latido.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas: sin toolchain de Go en este entorno):
//   - `if !cfg.InyectorEntrantes { return }` → `if cfg.InyectorEntrantes { return }` ⇒ 404 en el primer
//     bloque: la palanca deja de encender nada y MP-10 no se puede medir.
//   - cambiar `http.MethodPost` por `http.MethodGet` en HandleAuthorized ⇒ el POST da 405.
//   - cambiar `diag.RecursoInyectar` por `"edge.status.read"`, o `write: true` por `false` ⇒ rojo en el
//     bloque del espía; los dos abren la ruta a quien no debe (el segundo, en modo degradado ≤2h).
//   - quitar el `log.Warn` ⇒ el encendido se vuelve mudo y en campo nadie sabe que los números que lee
//     pueden ser sintéticos.
//   - bajar el aviso a `log.Info` ⇒ se pierde entre el tráfico normal del daemon (falla la exigencia de WARN).
func TestPalancaInyector_ConLaPalancaECHADALaRutaEXISTEYElDaemonLoGRITA(t *testing.T) {
	srv, espia, log, buf := servidorDePrueba(t)
	fake := &inyectorFalso{}

	registrarInyectorEntrantes(srv, config.Config{InyectorEntrantes: true}, fake, log)

	w := pedirInyeccion(srv, http.MethodPost, `{"session_id":"s-1","texto":"hola"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("con la palanca ECHADA la ruta %s debe responder 200 y contestó %d: la medición de MP-10 no "+
			"se puede hacer. Cuerpo: %s", diag.RutaInyectar, w.Code, w.Body.String())
	}
	if fake.veces != 1 {
		t.Errorf("la petición no llegó al puerto de inyección (%d llamadas): la ruta está colgada pero no "+
			"cableada al Manager, que es un 200 que no mide nada", fake.veces)
	}

	// El verbo: un GET a la misma ruta tiene que dar 405 (la plantilla casa, el método no) y NO 404. Si
	// diera 404, la ruta se habría registrado con otro verbo del que el runbook documenta.
	if g := pedirInyeccion(srv, http.MethodGet, ""); g.Code != http.StatusMethodNotAllowed {
		t.Errorf("un GET a %s debería dar 405 (la ruta se registra con POST y el 405 lo produce ServeHTTP) y "+
			"dio %d", diag.RutaInyectar, g.Code)
	}

	// El gate de auth: se evaluó una vez, con el recurso propio del inyector y marcado como ESCRITURA.
	if len(espia.recursos) == 0 {
		t.Fatal("la ruta se registró SIN gate de auth: cualquiera con acceso al socket podría inyectar")
	}
	if espia.recursos[0] != diag.RecursoInyectar {
		t.Errorf("la ruta se guarda con el recurso %q en vez de %q: un recurso de lectura dejaría inyectar "+
			"tráfico falso a un operador de solo-lectura", espia.recursos[0], diag.RecursoInyectar)
	}
	if !espia.writes[0] {
		t.Error("la ruta se registró como LECTURA (write=false): fabricar entrantes que acaban en la cola y en " +
			"la nube es una escritura, y marcarla como lectura la deja pasar en el modo degradado ≤2h")
	}

	// El grito.
	linea := buf.String()
	if !strings.Contains(linea, "WAPP_AGENT_INYECTOR_ENTRANTES") {
		t.Errorf("el arranque no nombra la variable que encendió el inyector; quien vea números de latencia "+
			"sintéticos no tiene por dónde empezar. Log: %q", linea)
	}
	if !strings.Contains(linea, "WARN") {
		t.Errorf("el aviso del inyector no sale a nivel WARN: un Info se pierde entre el tráfico normal del "+
			"daemon, que es justo lo que esta línea no puede permitirse. Log: %q", linea)
	}
}

// TestPalancaInyector_ElLatidoPublicaLaPalancaQueSeCableo cubre la SEGUNDA punta (la del latido). Si esta se
// rompe no se inyecta nada de más —el Edge hace lo correcto—, se pierde la única forma de saber, al leer la
// línea de latencia, que sus percentiles pueden estar hechos de mensajes fabricados. Y esa línea es la que
// se cita como evidencia de INV-051.2.
//
// Molde exacto: TestPalancaDespachador_ElLatidoPublicaLaPalancaQueSeCableo.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas: sin toolchain de Go en este entorno):
//   - en buildLatencia, `InyectorEntrantes: cfg.InyectorEntrantes` → un literal `false` ⇒ el latido publica
//     `inyector=no` SIEMPRE, incluso durante la medición sintética.
//   - la misma línea negada ⇒ el bloque advierte de tráfico sintético en todos los Edge sanos y calla en el
//     que sí lo tiene.
//   - cablearla desde `cfg.DespachadorApagado` (un copy-paste plausible del vecino) ⇒ los dos casos fallan.
func TestPalancaInyector_ElLatidoPublicaLaPalancaQueSeCableo(t *testing.T) {
	echada := buildLatencia(config.Config{InboundStatsEveryMS: 1234, InyectorEntrantes: true}, colaCompleta{}, logMudo())
	if !echada.deps.InyectorEntrantes {
		t.Error("el latido no recibió la palanca del inyector ECHADA: la línea de latencia diría `inyector=no` " +
			"sobre una población de entrantes sintéticos, y ese p99 acabaría en el journal como si fuera real")
	}
	if echada.deps.DespachadorApagado {
		t.Error("cablear el inyector encendió también la palanca del despachador: son dos instrumentos " +
			"distintos y confundirlos deja al Edge sin entregar durante una medición")
	}

	bajada := buildLatencia(config.Config{InboundStatsEveryMS: 1234, InyectorEntrantes: false}, colaCompleta{}, logMudo())
	if bajada.deps.InyectorEntrantes {
		t.Error("el latido recibió una palanca que nadie echó: publicaría la advertencia de tráfico sintético " +
			"en todos los Edge sanos, y una advertencia que sale siempre deja de leerse")
	}
}

// TestRegistrarInyector_ElLoteVacioSeGeneraYVuelveEnLaRespuesta cierra el frente de extremo a extremo por el
// único camino que importa en campo: el operador que NO pasa lote.
//
// 🔴 POR QUÉ ESTE TEST ESTÁ AQUÍ Y NO SOLO EN EL PAQUETE `diag`. Allí se comprueba que el handler genera el
// lote; aquí se comprueba que ese lote llega DE VERDAD hasta el puerto de inyección atravesando el servidor
// real, el gate de auth y el mux. Un lote vacío que se colara hasta el fabricante haría que la segunda
// corrida repitiera los `wa_message_id`, y el índice único (session_id, wa_message_id) se la tragaría EN
// SILENCIO: el histograma saldría corto y nadie sabría por qué, que es exactamente el modo de fallo que
// MP-10 existe para eliminar.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas: sin toolchain de Go en este entorno):
//   - en InyectorHandler, quitar la generación del lote y propagar `pet.Lote` tal cual ⇒ el puerto recibe ""
//     (rojo en el primer bloque) y la respuesta trae `"lote":""` (rojo en el segundo).
//   - devolver en la respuesta un lote distinto del que se inyectó (p. ej. `pet.Lote`) ⇒ rojo en la
//     comparación final: el operador barrería después las filas equivocadas creyendo que barre las suyas.
func TestRegistrarInyector_ElLoteVacioSeGeneraYVuelveEnLaRespuesta(t *testing.T) {
	srv, _, log, _ := servidorDePrueba(t)
	fake := &inyectorFalso{}
	registrarInyectorEntrantes(srv, config.Config{InyectorEntrantes: true}, fake, log)

	w := pedirInyeccion(srv, http.MethodPost, `{"session_id":"s-1","texto":"hola","n":3}`)
	if w.Code != http.StatusOK {
		t.Fatalf("se esperaba 200 y llegó %d: %s", w.Code, w.Body.String())
	}

	if len(fake.lotes) != 3 {
		t.Fatalf("se pidieron 3 inyecciones y llegaron %d al puerto", len(fake.lotes))
	}
	for i, l := range fake.lotes {
		if l == "" {
			t.Fatalf("la inyección %d llegó al puerto con el LOTE VACÍO: el fabricante la rechaza, y si algún "+
				"día no lo hiciera dos corridas repetirían los wa_message_id y la segunda se evaporaría sin error", i)
		}
		if l != fake.lotes[0] {
			t.Errorf("las inyecciones de una misma tanda no comparten lote (%q != %q): el lote es lo que permite "+
				"saber qué filas son de esta medición y cuáles quedaron de una anterior", l, fake.lotes[0])
		}
	}

	var resp struct {
		Lote string `json:"lote"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("la respuesta no es JSON: %v (%s)", err, w.Body.String())
	}
	if resp.Lote != fake.lotes[0] {
		t.Errorf("la respuesta devuelve el lote %q y se inyectó con %q: sin el lote REAL el operador no puede "+
			"filtrar sus filas ni barrerlas después", resp.Lote, fake.lotes[0])
	}
}
