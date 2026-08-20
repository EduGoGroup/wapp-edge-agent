package diag

// inyector_test.go — EL BORDE DEL INYECTOR (MP-10 Parte A).
//
// 🔴 QUÉ CLASE DE TESTS SON ESTOS. El handler no calcula nada: valida, enruta N veces y cuenta. Todo su
// valor está en las DECISIONES DE BORDE, y cada una existe porque su ausencia produce un fallo que no se ve:
//
//   - un `texto` vacío que se cuela ⇒ la fila nace `clasificado`/`sin_texto` y NO recorre el camino que se
//     mide: la medición sale con menos población y nadie sabe por qué;
//   - un `lote` vacío que se cuela ⇒ dos corridas repiten el `wa_message_id` y la segunda la absorbe el
//     índice único SIN error (el store trata el duplicado como caso normal);
//   - un `n` sin tope ⇒ el daemon 24/7 moliendo horas por la única conexión SQLite de la cola;
//   - un `ErrSesionNoViva` traducido a 500 (o a un 200 con `errores: 100`) ⇒ el operador cree que midió.
//
// Ninguno de los cuatro tiene síntoma en el momento en que ocurre. Por eso el borde se prueba entero.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo. NO se han ejecutado (este
// entorno no tiene toolchain de Go): están RAZONADAS contra el código, no verificadas en verde.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
)

// --- dobles y helpers ---

// espiaInyector es el puerto falso: anota lo que le llega y contesta lo que se le diga. `responder` recibe el
// índice de la inyección para poder fallar solo en una de ellas (que es como se distingue «aborta la tanda»
// de «cuenta y sigue»).
type espiaInyector struct {
	recibidas []app.InyeccionEntrante
	sesiones  []string
	responder func(i int) (bool, error)
}

func (e *espiaInyector) inyectar(_ context.Context, sessionID string, p app.InyeccionEntrante) (bool, error) {
	i := len(e.recibidas)
	e.recibidas = append(e.recibidas, p)
	e.sesiones = append(e.sesiones, sessionID)
	if e.responder == nil {
		return true, nil
	}
	return e.responder(i)
}

// pedir dispara el handler con el cuerpo dado. El método siempre es POST: el 405 lo produce el servidor
// (server.ServeHTTP) a partir de las plantillas registradas, no este handler, así que aquí no hay nada que
// probar sobre el verbo (ver el test del verbo en internal/infra/daemon/palanca_inyector_test.go).
func pedir(h http.HandlerFunc, cuerpo string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, RutaInyectar, strings.NewReader(cuerpo))
	w := httptest.NewRecorder()
	h(w, req)
	return w
}

// parte decodifica el cuerpo de una respuesta 200.
func parte(t *testing.T, w *httptest.ResponseRecorder) respuestaInyeccion {
	t.Helper()
	var r respuestaInyeccion
	if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil {
		t.Fatalf("la respuesta 200 no es el parte esperado: %v (cuerpo: %s)", err, w.Body.String())
	}
	return r
}

// codigoDeError extrae el `code` del envelope de error del contrato /v1.
func codigoDeError(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var e errorBody
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
		t.Fatalf("la respuesta de error no lleva el envelope /v1 {\"error\":{\"code\",\"message\"}}: %v "+
			"(cuerpo: %s). Un cliente del plano de control discrimina por `code` sin parsear el mensaje",
			err, w.Body.String())
	}
	return e.Error.Code
}

// --- validación de entrada ---

// TestInyector_LasPeticionesQueNoSePuedenMedir agrupa los 400. Van juntos porque comparten
// exactamente un razonamiento: cada uno de estos cuerpos produciría, si se dejara pasar, una MEDICIÓN QUE
// MIENTE en vez de un error — y una medición que miente es peor que no medir, porque se publica.
//
// Se exige además que el puerto NO se haya tocado en ninguno: un 400 que ya escribió filas en la cola deja
// basura sintética que el operador no sabe que tiene que barrer.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas: sin toolchain de Go en este entorno):
//   - quitar el `dec.DisallowUnknownFields()` ⇒ el caso del campo desconocido pasa a 200: un `"lotte"` mal
//     tecleado generaría un lote aleatorio y el operador barrería después las filas equivocadas.
//   - quitar la guarda de `session_id` ⇒ ese caso llega al Manager, que devuelve ErrSesionNoViva ⇒ 404 en vez
//     de 400 (rojo aquí), y el mensaje deja de decir qué falta.
//   - quitar la guarda de `texto` ⇒ 200 con la fila naciendo `clasificado`/`sin_texto`: se mide un camino que
//     no es el camino.
//   - subir `MaxInyecciones` o quitar la comparación ⇒ el caso del tope pasa a 200 y el daemon 24/7 se queda
//     moliendo por la única conexión SQLite de la cola.
//   - tratar `n` negativo como el default 1 ⇒ ese caso pasa a 200 escondiendo un error de quien llama.
func TestInyector_LasPeticionesQueNoSePuedenMedir(t *testing.T) {
	casos := []struct {
		nombre string
		cuerpo string
	}{
		{"cuerpo que no es JSON", `no soy json`},
		{"JSON truncado", `{"session_id":"s-1","texto":`},
		{"tipo equivocado en n", `{"session_id":"s-1","texto":"hola","n":"muchas"}`},
		{"campo desconocido (typo del operador)", `{"session_id":"s-1","texto":"hola","lotte":"abcd"}`},
		{"sin session_id", `{"texto":"hola"}`},
		{"session_id en blanco", `{"session_id":"   ","texto":"hola"}`},
		{"sin texto", `{"session_id":"s-1"}`},
		{"texto en blanco", `{"session_id":"s-1","texto":"  \t "}`},
		{"n por encima del tope", fmt.Sprintf(`{"session_id":"s-1","texto":"hola","n":%d}`, MaxInyecciones+1)},
		{"n negativo", `{"session_id":"s-1","texto":"hola","n":-3}`},
		{"espera_ms negativa", `{"session_id":"s-1","texto":"hola","espera_ms":-1}`},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			espia := &espiaInyector{}
			w := pedir(InyectorHandler(InyectorDeps{Inyectar: espia.inyectar}), c.cuerpo)

			if w.Code != http.StatusBadRequest {
				t.Errorf("se esperaba 400 y llegó %d (cuerpo: %s)", w.Code, w.Body.String())
			}
			if code := codigoDeError(t, w); code != codeInvalidRequest {
				t.Errorf("el envelope debe traer code=%q y trajo %q", codeInvalidRequest, code)
			}
			if len(espia.recibidas) != 0 {
				t.Errorf("una petición rechazada llegó a inyectar %d veces: un 400 que ya escribió filas deja "+
					"basura sintética en la cola que nadie sabe que tiene que barrer", len(espia.recibidas))
			}
		})
	}
}

// TestInyector_ElTopeEstaJustoDondeSeDice fija el LÍMITE y no solo su existencia: `MaxInyecciones` pasa y
// `MaxInyecciones+1` no. Un test que solo probara un número enorme seguiría en verde con el tope puesto en 5,
// que rompería la medición de otra manera (harían falta 20 llamadas para juntar 100 muestras).
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas):
//   - `pet.N > MaxInyecciones` → `>=` ⇒ el valor del tope deja de ser alcanzable (rojo en el primer bloque).
//   - bajar MaxInyecciones por debajo de 100 ⇒ sigue en verde aquí (el test usa la constante), pero el
//     comentario de la constante fija el porqué del 100: es el suelo de la regla del journal.
func TestInyector_ElTopeEstaJustoDondeSeDice(t *testing.T) {
	if MaxInyecciones < 100 {
		t.Fatalf("el tope es %d: por debajo de 100 una sola llamada no puede producir un p99 legible (la regla "+
			"del journal es que un p99 con n < 100 no es un dato)", MaxInyecciones)
	}

	espia := &espiaInyector{}
	w := pedir(InyectorHandler(InyectorDeps{Inyectar: espia.inyectar}),
		fmt.Sprintf(`{"session_id":"s-1","texto":"hola","n":%d}`, MaxInyecciones))
	if w.Code != http.StatusOK {
		t.Errorf("n = MaxInyecciones (%d) tiene que ser ACEPTADO y dio %d: el tope se documenta como el máximo "+
			"alcanzable, no como el primer valor prohibido", MaxInyecciones, w.Code)
	}
	if len(espia.recibidas) != MaxInyecciones {
		t.Errorf("llegaron %d inyecciones de %d pedidas", len(espia.recibidas), MaxInyecciones)
	}
}

// --- camino feliz ---

// TestInyector_CaminoFelizElParteCuadraConLoQueDevolvioElPuerto es el test de CONDUCTA del handler: con un
// puerto que dice que sí a unas y que no a otras, el parte tiene que contar cada cosa en su casilla.
//
// Las tres casillas son HECHOS DISTINTOS y confundirlas es lo que hace inútil el parte:
//
//   - `inyectados` = recorrieron el camino (sin error de enrutado) ⇒ es la población que entró en el
//     histograma, y por tanto la `n` que el latido publicará;
//   - `acusados` = además devolvieron el permiso de acuse ⇒ la fila llegó a disco. Un `inyectados > acusados`
//     dice que el camino se recorrió pero la fila NO se escribió, que es el mismo síntoma que haría a
//     WhatsApp reofrecer un mensaje real;
//   - `errores` = ni siquiera se recorrió.
//
// Se comprueba también el `Indice`: es lo que desempata dos inyecciones del mismo lote caídas en el mismo
// milisegundo, y sin él dos filas comparten `wa_message_id` y el índice único se traga la segunda EN
// SILENCIO.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas):
//   - contar `inyectados++` también cuando hay error ⇒ el parte afirma haber medido lo que no midió.
//   - contar `acusados++` sin mirar el bool ⇒ `acusados` deja de distinguir «recorrió» de «llegó a disco», y
//     con ello se pierde la única señal de que la cola no está escribiendo.
//   - `Indice: 0` fijo (o `Indice: i+1`) ⇒ rojo en la comprobación de los índices; con el fijo, todas las
//     filas de la tanda comparten wa_message_id y solo sobrevive UNA, sin error.
//   - `Solicitados: len(recibidas)` en vez de la `n` pedida ⇒ el parte dejaría de poder decir «pedí 100 y
//     recorrieron 98».
func TestInyector_CaminoFelizElParteCuadraConLoQueDevolvioElPuerto(t *testing.T) {
	espia := &espiaInyector{responder: func(i int) (bool, error) {
		switch i {
		case 2:
			return false, nil // recorrió el camino y NO acusó: la fila no llegó a disco.
		case 3:
			return false, errors.New("fallo suelto de escritura")
		default:
			return true, nil
		}
	}}

	w := pedir(InyectorHandler(InyectorDeps{Inyectar: espia.inyectar}),
		`{"session_id":"s-42","texto":"hola","n":5,"chat_jid":"123@s.whatsapp.net","lote":"deadbeef"}`)

	if w.Code != http.StatusOK {
		t.Fatalf("se esperaba 200 y llegó %d: %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("el parte debe salir como JSON y salió como %q", ct)
	}

	p := parte(t, w)
	if p.Solicitados != 5 {
		t.Errorf("`solicitados` debe ser la n pedida (5) y fue %d", p.Solicitados)
	}
	if p.Inyectados != 4 {
		t.Errorf("`inyectados` (las que recorrieron el camino) debe ser 4 y fue %d: es la población que entra "+
			"en el histograma, así que este número es el que explica la `n` del latido", p.Inyectados)
	}
	if p.Acusados != 3 {
		t.Errorf("`acusados` debe ser 3 y fue %d: un inyectado que no acusa recorrió el camino pero NO dejó "+
			"fila, y esa diferencia es la única señal de que la cola no está escribiendo", p.Acusados)
	}
	if p.Errores != 1 {
		t.Errorf("`errores` debe ser 1 y fue %d", p.Errores)
	}
	if !strings.Contains(p.PrimerError, "fallo suelto de escritura") {
		t.Errorf("`primer_error` no trae el fallo que ocurrió: got %q", p.PrimerError)
	}
	if p.Lote != "deadbeef" {
		t.Errorf("el lote del operador tiene que volver TAL CUAL en el parte (es con lo que se filtran y se "+
			"barren las filas después): got %q", p.Lote)
	}
	if p.DuracionMS < 0 {
		t.Errorf("`duracion_ms` negativa: %d", p.DuracionMS)
	}

	// Lo que llegó al puerto.
	if len(espia.recibidas) != 5 {
		t.Fatalf("se pidieron 5 inyecciones y llegaron %d", len(espia.recibidas))
	}
	for i, inj := range espia.recibidas {
		if espia.sesiones[i] != "s-42" {
			t.Errorf("la inyección %d fue a la sesión %q en vez de a s-42", i, espia.sesiones[i])
		}
		if inj.Indice != i {
			t.Errorf("la inyección %d llegó con Indice=%d: el índice es lo que desempata dos inyecciones del "+
				"mismo lote en el mismo milisegundo, y sin él dos filas comparten wa_message_id", i, inj.Indice)
		}
		if inj.Lote != "deadbeef" || inj.Texto != "hola" || inj.ChatJID != "123@s.whatsapp.net" {
			t.Errorf("la inyección %d llegó alterada: %+v", i, inj)
		}
	}
}

// TestInyector_ElLoteVacioSeGENERAYVuelveEnLaRespuesta es el test que evita el fallo MÁS CARO de este frente,
// porque es el único de los cuatro que no deja rastro: sin lote, el fabricante rechaza la inyección (hoy) o —
// si algún día no lo hiciera— dos corridas repetirían los `wa_message_id` y la segunda la absorbería el
// índice único (session_id, wa_message_id) devolviendo nil, es decir SIN error. El histograma saldría corto y
// nadie sabría por qué.
//
// Se exige además que el lote GENERADO vuelva en la respuesta: un lote que no se devuelve es basura
// sintética en una cola de producción que el operador no puede ni filtrar ni barrer.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas):
//   - propagar `pet.Lote` tal cual sin generar ⇒ el puerto recibe "" (rojo en el bucle) y el parte trae
//     `"lote":""` (rojo abajo).
//   - devolver `pet.Lote` en la respuesta en vez del lote usado ⇒ el parte miente sobre qué barrer.
//   - generar un lote DISTINTO por inyección (mover la generación dentro del bucle) ⇒ las filas de una misma
//     tanda dejan de compartir lote y el filtro que las agrupa deja de existir.
func TestInyector_ElLoteVacioSeGENERAYVuelveEnLaRespuesta(t *testing.T) {
	espia := &espiaInyector{}
	w := pedir(InyectorHandler(InyectorDeps{Inyectar: espia.inyectar}),
		`{"session_id":"s-1","texto":"hola","n":3}`)

	if w.Code != http.StatusOK {
		t.Fatalf("se esperaba 200 y llegó %d: %s", w.Code, w.Body.String())
	}
	p := parte(t, w)
	if strings.TrimSpace(p.Lote) == "" {
		t.Fatal("el handler no generó lote: el fabricante rechaza el lote vacío a propósito, y una tanda sin " +
			"lote no se puede distinguir de otra ni barrer después")
	}
	if len(espia.recibidas) != 3 {
		t.Fatalf("llegaron %d inyecciones de 3", len(espia.recibidas))
	}
	for i, inj := range espia.recibidas {
		if inj.Lote != p.Lote {
			t.Errorf("la inyección %d se hizo con el lote %q y el parte devuelve %q: el operador barrería las "+
				"filas equivocadas", i, inj.Lote, p.Lote)
		}
	}

	// Dos tandas seguidas NO pueden compartir lote generado: es justo el caso (un script que llama dos veces)
	// para el que el lote es aleatorio y no un sello de tiempo.
	espia2 := &espiaInyector{}
	w2 := pedir(InyectorHandler(InyectorDeps{Inyectar: espia2.inyectar}), `{"session_id":"s-1","texto":"hola"}`)
	if p2 := parte(t, w2); p2.Lote == p.Lote {
		t.Errorf("dos tandas consecutivas compartieron el lote generado (%q): sus wa_message_id coincidirían y "+
			"la segunda tanda se evaporaría entera en el índice único, sin un solo error", p2.Lote)
	}
}

// --- mapeo de errores del Manager ---

// TestInyector_LosDosErroresDeSESIONTienenSuPropioStatus fija la traducción de los centinelas. Los dos son
// 4xx y no 500 porque los dos los arregla QUIEN LLAMA, y son códigos DISTINTOS porque se arreglan de forma
// opuesta: el 404 pide emparejar (o corregir el id) y el 409 pide esperar a que el listener suba. Un 500
// mandaría a mirar los logs del daemon, que es el sitio equivocado en los dos casos.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas):
//   - comparar con `==` en vez de `errors.Is` ⇒ los dos casos caen a 200/errores, porque el Manager envuelve
//     el centinela con %w (`fmt.Errorf("%w: %s", ErrSesionNoViva, sessionID)`).
//   - mapear los dos al mismo status ⇒ el operador no puede distinguir «no existe» de «espera un momento».
//   - devolver 500 en cualquiera de los dos ⇒ manda a diagnosticar el daemon por un error de la petición.
func TestInyector_LosDosErroresDeSESIONTienenSuPropioStatus(t *testing.T) {
	casos := []struct {
		nombre string
		err    error
		status int
		code   string
	}{
		{"sesión no viva", fmt.Errorf("%w: %s", sessionmgr.ErrSesionNoViva, "s-1"), http.StatusNotFound, codeNotFound},
		{"listener sin cablear", fmt.Errorf("envuelto: %w", sessionmgr.ErrInyectorNoCableado), http.StatusConflict, codeConflict},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			espia := &espiaInyector{responder: func(int) (bool, error) { return false, c.err }}
			w := pedir(InyectorHandler(InyectorDeps{Inyectar: espia.inyectar}),
				`{"session_id":"s-1","texto":"hola","n":50}`)

			if w.Code != c.status {
				t.Errorf("se esperaba %d y llegó %d (cuerpo: %s)", c.status, w.Code, w.Body.String())
			}
			if code := codigoDeError(t, w); code != c.code {
				t.Errorf("el envelope debe traer code=%q y trajo %q", c.code, code)
			}
			if len(espia.recibidas) != 1 {
				t.Errorf("la tanda no abortó en la PRIMERA inyección (%d llegaron): sin camino que recorrer, las "+
					"49 restantes fallarían igual y llenarían el parte de ruido", len(espia.recibidas))
			}
		})
	}
}

// TestInyector_ElBackoffDeReconexionAbortaLaTandaCon409 es el caso de CAMPO del 409, y va aparte del test de
// la tabla de arriba porque prueba una forma de error DISTINTA: la que produce la cadena real.
//
// EL ESTADO. La sesión está viva, el daemon corre, pero su gateway cayó y `runListener` espera el backoff de
// reconexión —hasta 60 s—. El cable del inyector sigue publicado apuntando a ese gateway muerto, así que el
// adaptador contesta `whatsmeow.ErrSinEscuchaViva` y el Manager lo traduce envolviendo su propio centinela:
// `fmt.Errorf("%w: %v", ErrInyectorNoCableado, err)`. Esa forma —centinela en el `%w` y el texto del
// adaptador detrás en `%v`— es la que llega aquí, y es la que este test replica.
//
// POR QUÉ IMPORTA QUE ABORTE. Si no aborta, las 500 inyecciones fallan idénticamente, se cuentan una a una y
// el operador recibe un 200 con `inyectados: 0, errores: 500`: un parte que dice «he medido» sin haber
// medido nada. El 409 dice lo contrario y dice qué hacer — esperar unos segundos y repetir.
//
// NO SE IMPORTA `internal/adapters/whatsmeow` PARA CONSTRUIR EL ERROR, a propósito: este paquete es el borde
// HTTP y solo debe conocer los centinelas del puerto (ver InyectorDeps). Importar el adaptador aquí ataría
// el borde a whatsmeow justo por donde el diseño lo separa. Se replica la FORMA del envoltorio, que es lo
// único de lo que este test responde.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas):
//   - quitar el bloque `errors.Is(err, sessionmgr.ErrInyectorNoCableado)` del mapeo ⇒ 200 con `errores: 50`
//     y `inyectados: 0`, que es el 200 mentiroso que este test existe para impedir.
//   - en el Manager, traducir con `%v` en vez de `%w` ⇒ el centinela no viaja y este test cae al 200.
//   - mapear este centinela al 404 ⇒ el operador va a emparejar una sesión que solo necesita esperar.
func TestInyector_ElBackoffDeReconexionAbortaLaTandaCon409(t *testing.T) {
	// La forma EXACTA que produce Manager.InyectarEntrante al traducir el centinela del adaptador.
	errDeCampo := fmt.Errorf("%w: %v", sessionmgr.ErrInyectorNoCableado,
		errors.New("whatsapp: la sesión no tiene Listener publicado ahora mismo"))

	espia := &espiaInyector{responder: func(int) (bool, error) { return false, errDeCampo }}
	w := pedir(InyectorHandler(InyectorDeps{Inyectar: espia.inyectar}),
		`{"session_id":"s-1","texto":"hola","n":50}`)

	if w.Code != http.StatusConflict {
		t.Fatalf("el gateway en backoff de reconexión tiene que abortar la tanda con 409 y llegó %d (cuerpo: %s): "+
			"un 200 con inyectados=0 y errores=50 le contesta «he medido» a quien no midió nada, que es el modo "+
			"de fallo que MP-10 existe para eliminar", w.Code, w.Body.String())
	}
	if code := codigoDeError(t, w); code != codeConflict {
		t.Errorf("el envelope debe traer code=%q y trajo %q", codeConflict, code)
	}
	if len(espia.recibidas) != 1 {
		t.Errorf("la tanda siguió tras la PRIMERA inyección (%d llegaron): el listener no vuelve en lo que dura "+
			"la tanda, así que las 49 restantes fallarían igual y solo sirven para llenar el parte de ruido",
			len(espia.recibidas))
	}
}

// TestInyector_LosMismosErroresAMitadDeTandaNOAbortan es la otra mitad de la regla, y la que evita tirar a la
// basura una medición buena. Un `ErrSesionNoViva` en la inyección 0 significa «no hay nada que medir»; el
// MISMO error en la inyección 7 significa «la sesión se cayó durante la medición», y las 7 inyecciones ya
// hechas siguen siendo datos. Un parte parcial con su `primer_error` es más útil que un 404 que borra lo
// medido.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas):
//   - quitar la guarda `if i == 0` del mapeo ⇒ la tanda aborta con 404 y se pierden las inyecciones hechas.
//   - hacer que cualquier error corte el bucle (`break` en vez de `continue`) ⇒ `inyectados` se queda en 1 y
//     el parte no refleja lo que sí se midió.
func TestInyector_LosMismosErroresAMitadDeTandaNOAbortan(t *testing.T) {
	espia := &espiaInyector{responder: func(i int) (bool, error) {
		if i == 2 {
			return false, fmt.Errorf("%w: %s", sessionmgr.ErrSesionNoViva, "s-1")
		}
		return true, nil
	}}

	w := pedir(InyectorHandler(InyectorDeps{Inyectar: espia.inyectar}),
		`{"session_id":"s-1","texto":"hola","n":5}`)

	if w.Code != http.StatusOK {
		t.Fatalf("un centinela a mitad de tanda NO puede abortarla: se esperaba 200 y llegó %d (%s)",
			w.Code, w.Body.String())
	}
	p := parte(t, w)
	if p.Inyectados != 4 || p.Errores != 1 || p.Solicitados != 5 {
		t.Errorf("el parte parcial no cuadra: solicitados=%d inyectados=%d errores=%d (se esperaba 5/4/1)",
			p.Solicitados, p.Inyectados, p.Errores)
	}
	if len(espia.recibidas) != 5 {
		t.Errorf("la tanda se cortó en la %d: las inyecciones posteriores a un fallo suelto siguen siendo datos",
			len(espia.recibidas))
	}
}

// --- espera, cancelación y cableado ---

// TestInyector_ElClienteQueSeVaCORTALaTanda: el operador que hace Ctrl-C en su `curl` a mitad de una tanda de
// 500 con espera. Seguir inyectando llenaría la cola de un Edge de producción con filas de una medición que
// ya nadie va a leer — y esas filas las drena el despachador HACIA LA NUBE.
//
// Precedente del patrón: logsink/handler.go, que también cuelga del `r.Context()`.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas):
//   - sustituir el `select` de la espera por un `time.Sleep(espera)` ⇒ el handler ignora la cancelación y
//     completa las 20 inyecciones (rojo en el conteo) tras 2 s de reloj.
//   - quitar el `if err := ctx.Err(); err != nil { break }` del principio del bucle ⇒ con `espera_ms: 0` la
//     tanda entera correría con el cliente ya ido.
func TestInyector_ElClienteQueSeVaCORTALaTanda(t *testing.T) {
	espia := &espiaInyector{}
	h := InyectorHandler(InyectorDeps{Inyectar: espia.inyectar})

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, RutaInyectar,
		strings.NewReader(`{"session_id":"s-1","texto":"hola","n":20,"espera_ms":100}`)).WithContext(ctx)

	// El cliente se va tras un par de esperas. 250 ms contra esperas de 100 ms deja MARGEN: con 120 ms el
	// corte caía a 20 ms del vencimiento del primer timer y una máquina de CI cargada (o un `-race`) podía
	// meter esos 20 ms de retraso en cualquier parte, cancelando antes de la inyección 1 o después de la 3.
	// La aserción de abajo no fija un número exacto por la misma razón: lo que se custodia es «cortó y algo
	// hizo», no cuántas cabían en la ventana.
	go func() {
		time.Sleep(250 * time.Millisecond)
		cancel()
	}()

	w := httptest.NewRecorder()
	h(w, req)

	if n := len(espia.recibidas); n >= 20 {
		t.Errorf("la tanda completó las 20 inyecciones pese a que el cliente se fue: con espera_ms=100 eso son "+
			"2 s de filas sintéticas escribiéndose en la cola de un Edge de producción sin nadie escuchando "+
			"(llegaron %d)", n)
	}
	if len(espia.recibidas) < 1 {
		t.Error("no se inyectó NADA antes de la cancelación: el corte se está evaluando antes de la primera " +
			"inyección y el `espera_ms` no se estaría respetando entre inyecciones, sino antes de todas")
	}
	if w.Code != http.StatusOK {
		t.Errorf("el corte limpio devuelve el parte PARCIAL con 200 (aunque el cliente ya no lo lea): se "+
			"esperaba 200 y llegó %d", w.Code)
	}
	if p := parte(t, w); !strings.Contains(p.PrimerError, "cortada") {
		t.Errorf("el parte parcial no dice que la tanda se CORTÓ: un parte con menos inyecciones y sin "+
			"explicación se lee como un fallo del Edge. got %q", p.PrimerError)
	}
}

// TestInyector_SinPuertoCableadoContesta500YNoEntraEnPANICO fija la tolerancia a nil de las deps. La
// alternativa —un nil pointer dereference— se lleva por delante el DAEMON 24/7 con el socket de WhatsApp
// detrás, por una ruta de diagnóstico. Ese intercambio no se acepta ni una vez.
//
// El 500 dice además de QUÉ lado está el fallo: es de cableado del daemon, no del `curl` de quien llama.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO (razonada, no ejecutada): quitar el `if deps.Inyectar == nil` ⇒ pánico al
// invocar la función nil, y con él la caída del proceso entero.
func TestInyector_SinPuertoCableadoContesta500YNoEntraEnPANICO(t *testing.T) {
	w := pedir(InyectorHandler(InyectorDeps{}), `{"session_id":"s-1","texto":"hola"}`)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("con las deps a medio cablear se esperaba 500 y llegó %d (%s)", w.Code, w.Body.String())
	}
	if code := codigoDeError(t, w); code != codeInternal {
		t.Errorf("el envelope debe traer code=%q y trajo %q", codeInternal, code)
	}
}

// TestInyector_NIngunaRespuestaLLEVAElTextoDelMensaje custodia INV-051.1 en el borde. El `texto` es
// contenido de mensaje y se trata con las mismas reglas que el de un entrante real, aunque lo haya escrito un
// operador: el camino que atraviesa no distingue, y una excepción «solo para lo sintético» sería una
// excepción en el código que también procesa lo real.
//
// Este handler no tiene logger a propósito (ver InyectorDeps), así que lo único que puede filtrar el texto es
// la respuesta. Se comprueba en los tres caminos que producen cuerpo: el parte, el 4xx del centinela y el 400
// de validación.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (razonadas, no ejecutadas):
//   - añadir el texto al parte (p. ej. un campo `"texto"` de eco) ⇒ el contenido del mensaje acaba en la
//     respuesta y, por el camino de `wapp-ctl`, en el navegador y en cualquier captura que se pegue en un
//     ticket.
//   - incrustar `pet.Texto` en el mensaje de un 4xx ⇒ lo mismo por la puerta de atrás.
//   - añadir un `Log` a las deps y trazar la petición completa ⇒ este test no lo cazaría, y por eso la struct
//     no tiene logger: la garantía es estructural, no de disciplina.
func TestInyector_NIngunaRespuestaLLEVAElTextoDelMensaje(t *testing.T) {
	const secreto = "SECRETO-DEL-CLIENTE-42"
	cuerpo := fmt.Sprintf(`{"session_id":"s-1","texto":%q,"n":2}`, secreto)

	casos := map[string]func(int) (bool, error){
		"camino feliz":  nil,
		"centinela 404": func(int) (bool, error) { return false, fmt.Errorf("%w: s-1", sessionmgr.ErrSesionNoViva) },
		"fallo suelto":  func(int) (bool, error) { return false, errors.New("fallo de escritura") },
	}
	for nombre, responder := range casos {
		t.Run(nombre, func(t *testing.T) {
			espia := &espiaInyector{responder: responder}
			w := pedir(InyectorHandler(InyectorDeps{Inyectar: espia.inyectar}), cuerpo)
			if strings.Contains(w.Body.String(), secreto) {
				t.Errorf("la respuesta lleva el TEXTO del mensaje (INV-051.1): %s", w.Body.String())
			}
		})
	}
}
