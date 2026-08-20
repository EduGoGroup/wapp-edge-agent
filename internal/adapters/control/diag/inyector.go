// Package diag sirve las rutas de DIAGNÓSTICO del plano de control local: instrumentos de medida que
// existen para responder una pregunta concreta y que se BORRAN cuando esa pregunta está respondida.
//
// 🔴 POR QUÉ UN PAQUETE PROPIO Y NO UNA RUTA MÁS EN `control/server`. Tres razones, en orden de peso:
//
//  1. LO QUE SE BORRA JUNTO SE ESCRIBE JUNTO. Todo lo que MP-10 Parte A añade a la superficie del plano de
//     control vive en este directorio: el día que la medición del p99 esté cerrada, retirar el frente es
//     borrar este paquete y las cinco líneas del daemon que lo registran, sin peinar `server/` buscando qué
//     era del inyector y qué no. Es el mismo criterio con el que el adaptador metió el fabricante y la
//     puerta en un solo `adapters/whatsmeow/inyector.go` («se lee, se audita y, si un día sobra, se BORRA
//     como una sola unidad»).
//  2. EL REGISTRO TIENE QUE PODER NO OCURRIR. Las rutas de `server` se cuelgan desde el constructor o desde
//     métodos `Register*` que se llaman siempre; la de aquí NO se registra cuando la palanca está bajada
//     (ver registrarInyectorEntrantes en internal/infra/daemon/daemon.go). Un handler que vive fuera del
//     servidor y se cuelga con `HandleAuthorized` desde el daemon es exactamente la forma que permite eso.
//  3. ES EL PRECEDENTE VIVO. `internal/adapters/intent.StatusHandler` ya es un handler externo al paquete
//     `server` que el daemon cuelga con `HandleAuthorized`. Este copia su molde: constructor que cierra
//     sobre sus dependencias y devuelve un `http.HandlerFunc`, con las deps tolerantes a nil.
//
// EL COSTE ACEPTADO de estar fuera de `server` es que `writeJSON`/`writeError` (server/respond.go) no son
// exportados y aquí no se pueden usar: el envelope de error se re-escribe abajo (responderError) copiando
// su forma `{"error":{"code","message"}}` para que la ruta de diagnóstico hable el MISMO dialecto que el
// resto de /v1. Exportarlos habría sido ensanchar el contrato de un paquete estable por un instrumento
// temporal, que es el intercambio equivocado.
package diag

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
)

// MaxInyecciones es el TOPE DURO de inyecciones por petición. Un `n` sin techo en un daemon 24/7 es una
// bomba y no una comodidad: la inyección es SÍNCRONA y cada una escribe una fila cifrada en SQLite por la
// ÚNICA conexión que el Edge abre sobre la cola (SetMaxOpenConns(1), T3.15), así que un `"n": 1000000`
// tecleado de más no devuelve un error — deja el daemon moliendo durante horas, compitiendo con los
// entrantes REALES por esa conexión y llenando la cola hasta su TTL.
//
// 500 y no otro número, por los dos extremos:
//
//   - por abajo: la regla del journal es que un p99 con n < 100 no es un dato, así que el tope tiene que
//     dejar holgadamente por encima de 100. 500 da cinco veces el mínimo en UNA sola llamada.
//   - por arriba: al objetivo que se mide (INV-051.2, < 50 ms p99) 500 inyecciones son ~25 s de trabajo
//     síncrono en el peor caso, que es lo que un operador aguanta esperando delante de un `curl` y lo que
//     un cliente HTTP no corta por timeout por defecto. Con el handler degradado a 200 ms serían 100 s y
//     seguiría siendo una espera acotada y diagnosticable, no un cuelgue.
//
// Una medición más larga se hace repitiendo la llamada —el `lote` es justo lo que permite distinguir las
// tandas—, y repetir es una decisión consciente del operador cada vez.
const MaxInyecciones = 500

// RutaInyectar es la ruta de la inyección. Está EXPORTADA a propósito: la registra el daemon y la afirma su
// test de guardarraíl, y una cadena literal escrita en dos sitios es una divergencia esperando a ocurrir —
// justo la clase de divergencia que dejaría el test comprobando el 404 de una ruta que ya no es la que se
// registra, es decir, en verde y mirando a otro lado.
//
// Cuelga de `/v1/diag/` y no de `/v1/intent/` ni de `/v1/sessions/` por una razón operativa concreta:
// `wapp-ctl` (cmd/wapp-ctl) proxya `/v1/*` al socket con sombras propias (`/v1/daemon/`, `/v1/cajero/`) y
// una denylist (`/v1/auth/`), y el prefijo `/v1/diag/` no está en ninguna de las dos ⇒ la ruta es
// alcanzable por el puerto 8105 SIN tocar el supervisor. Además nombra lo que es: diagnóstico.
const RutaInyectar = "/v1/diag/inbound/inject"

// RecursoInyectar es el grant RBAC que el gate `edge.*` evalúa para esta ruta (ADR-0025). El recurso es
// CADENA LIBRE evaluada contra los grants del token (fail-closed, internal/auth/manager.go): no hay catálogo
// que registrar en ningún otro repo, y hoy solo el rol `tenant_admin` —que lleva el grant `'*'`— cubre un
// recurso nuevo. Es decir: por defecto esta ruta solo la alcanza un administrador, que es exactamente quien
// debe poder inyectar tráfico falso en un Edge de producción.
const RecursoInyectar = "edge.diag.inbound.inject"

// InyectorDeps son las dependencias del handler de POST /v1/diag/inbound/inject. Tolerantes a nil: un nil
// aquí es un fallo de CABLEADO, y el handler contesta 500 con ese diagnóstico en vez de entrar en pánico y
// llevarse por delante el daemon 24/7 por una ruta de diagnóstico.
//
// 🔴 NO HAY LOGGER EN ESTA STRUCT, Y ES DELIBERADO (INV-051.1). El cuerpo de la petición trae el `texto` del
// mensaje sintético, y ese texto se trata con las MISMAS reglas que el de un entrante real: se persiste
// cifrado con la DEK de la sesión y NUNCA se imprime en un log. La forma más barata de garantizar que este
// handler no lo loguea es que no tenga a dónde loguearlo. Si algún día hiciera falta trazar aquí, la regla
// no cambia: se traza el session_id, el lote y las cuentas — nunca nada derivado del contenido.
type InyectorDeps struct {
	// Inyectar mete UNA inyección por el camino real del handler de la sesión indicada. Lo cumple
	// `(*sessionmgr.Manager).InyectarEntrante`.
	//
	// Es una FUNCIÓN y no el *Manager por dos motivos, ninguno de los cuales es el ciclo de importación —
	// no lo hay: `internal/adapters/control/server/unlink.go` ya importa `internal/app/sessionmgr` desde el
	// plano de control, y `sessionmgr` no importa nada de `adapters/control`—. Los motivos son (1) que el
	// puerto quede ESTRECHO, del ancho exacto de lo que este borde usa (mismo criterio que
	// `server.sessionUnlinker`), y (2) que el test del handler no tenga que construir un Manager, un Layout
	// y un directorio temporal para comprobar que un cuerpo sin `texto` devuelve 400.
	//
	// Los CENTINELAS del mapeo de errores sí se importan directamente de `sessionmgr` (abajo): pasarlos
	// también por deps habría hecho posible cablear un centinela distinto del que el Manager devuelve, y
	// entonces el 404 y el 409 se convertirían en 500 sin que nada se pusiera rojo.
	Inyectar func(ctx context.Context, sessionID string, p app.InyeccionEntrante) (acusar bool, err error)
}

// peticionInyeccion es el cuerpo JSON de POST /v1/diag/inbound/inject.
//
// Los campos con cero útil (`n`, `chat_jid`, `lote`, `espera_ms`) son OPCIONALES y su cero significa el
// valor por defecto documentado en cada uno. `session_id` y `texto` no tienen default posible: sin sesión no
// hay camino que recorrer y sin texto la fila nace `clasificado`/`sin_texto` y NO recorre el camino que se
// quiere medir (ver whatsmeow.FabricarEntranteSintetico).
type peticionInyeccion struct {
	// SessionID es la sesión VIVA en cuyo handler se inyecta. Obligatorio.
	SessionID string `json:"session_id"`
	// Texto es el cuerpo del mensaje sintético. Obligatorio y no en blanco.
	Texto string `json:"texto"`
	// N es cuántas inyecciones hacer. 0 (o ausente) ⇒ 1. Tope MaxInyecciones.
	N int `json:"n"`
	// ChatJID etiqueta la fila; "" ⇒ el chat sintético por defecto del adaptador.
	ChatJID string `json:"chat_jid"`
	// Lote agrupa las N inyecciones de esta medición. "" ⇒ se GENERA uno aleatorio (ver loteAleatorio: el
	// lote vacío no se puede propagar tal cual, el fabricante lo rechaza a propósito).
	Lote string `json:"lote"`
	// EsperaMS separa una inyección de la siguiente. 0 ⇒ a ráfaga. Sirve para medir el handler SIN el
	// backlog que una ráfaga genera en la cola, que es una pregunta distinta y también hace falta.
	EsperaMS int `json:"espera_ms"`
}

// respuestaInyeccion es el parte de la tanda. Es lo que el operador pega en el journal, así que se basta
// sola: cuántas se pidieron, cuántas recorrieron el camino, cuántas habrían acusado a WhatsApp y cuánto
// costó todo.
//
// `primer_error` va SIN `omitempty` a propósito, por el mismo criterio que los campos de la puerta en el
// bloque de latencia: una cadena vacía explícita es un DATO («ninguna falló») y un campo ausente es una
// DUDA («¿no falló nada, o esta versión ya no lo reporta?»). El parte se lee a ciegas, días después.
type respuestaInyeccion struct {
	// Lote es el que se USÓ, venga del operador o generado aquí. Es el dato con el que luego se filtran las
	// filas de la cola y se barren (`DELETE … WHERE wa_message_id LIKE 'SINTETICO-<lote>-%'`), así que
	// devolverlo NO es cortesía: sin él, un lote generado es basura sintética que nadie sabe distinguir.
	Lote string `json:"lote"`
	// Solicitados es la `n` YA RESUELTA (con el default aplicado), no la que venía en el cuerpo.
	Solicitados int `json:"solicitados"`
	// Inyectados son las que RECORRIERON el camino del handler (sin error de enrutado). Es la población que
	// entró en el histograma, y por tanto la `n` que el latido publicará.
	Inyectados int `json:"inyectados"`
	// Acusados son las que devolvieron el permiso de acuse (bool true): la fila llegó a disco. Un
	// `inyectados > acusados` dice que el camino se recorrió pero la fila NO se escribió — el mismo síntoma
	// que haría a WhatsApp reofrecer un mensaje real.
	Acusados int `json:"acusados"`
	// Errores son las inyecciones que fallaron sin abortar la tanda.
	Errores int `json:"errores"`
	// DuracionMS es la pared de la tanda ENTERA, esperas incluidas. No es una medida del handler —esa la da
	// el latido— sino el contexto que explica la tanda.
	DuracionMS int64 `json:"duracion_ms"`
	// PrimerError es el texto del primer fallo. Solo el primero: N fallos iguales son un fallo, y una lista
	// de 500 mensajes idénticos en el parte lo haría ilegible.
	PrimerError string `json:"primer_error"`
}

// InyectorHandler construye el handler de POST /v1/diag/inbound/inject: mete N entrantes SINTÉTICOS por el
// camino REAL del handler de una sesión viva (MP-10 Parte A).
//
// 🔴 NO VALIDA EL MÉTODO. El 405 (y el 404) los genera `server.ServeHTTP` a partir de las plantillas
// registradas: comprobar aquí `r.Method != http.MethodPost` produciría un segundo criterio que puede
// divergir del primero, y un 405 sin la cabecera `Allow` que el servidor sí calcula.
//
// La inyección es SÍNCRONA y SECUENCIAL, con `Indice` incremental desde 0. Secuencial porque lo que se mide
// es el camino de UN entrante, y N goroutines compitiendo por la única conexión SQLite medirían la
// contención en vez del handler. Síncrona porque el parte tiene que poder afirmar cuántas llegaron: una
// inyección en segundo plano devolvería un 202 y dejaría al operador sin saber si midió algo.
func InyectorHandler(deps InyectorDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if deps.Inyectar == nil {
			// Fallo de cableado, no de la petición: la ruta se registró sin puerto al que enrutar. Se dice
			// tal cual para que quien lo vea busque en el daemon y no en su `curl`.
			responderError(w, http.StatusInternalServerError, codeInternal,
				"el inyector no tiene puerto cableado (InyectorDeps.Inyectar == nil): es un fallo de cableado del daemon")
			return
		}

		pet, err := leerPeticion(r)
		if err != nil {
			responderError(w, http.StatusBadRequest, codeInvalidRequest, err.Error())
			return
		}

		lote := strings.TrimSpace(pet.Lote)
		if lote == "" {
			generado, err := loteAleatorio()
			if err != nil {
				responderError(w, http.StatusInternalServerError, codeInternal,
					"no se pudo generar un lote aleatorio: "+err.Error())
				return
			}
			lote = generado
		}

		res, status, errRes := inyectarTanda(r.Context(), deps.Inyectar, pet, lote)
		if errRes != nil {
			responderError(w, status, errRes.code, errRes.message)
			return
		}
		responderJSON(w, http.StatusOK, res)
	}
}

// errBorde es un error YA TRADUCIDO a la forma que sale por el cable (status + code + message). Existe para
// que `inyectarTanda` pueda abortar con un 404/409 sin conocer el `http.ResponseWriter`, que es lo que la
// hace comprobable sin levantar un servidor.
type errBorde struct {
	code    string
	message string
}

// inyectarTanda ejecuta las N inyecciones y arma el parte. Devuelve (parte, 200, nil) en el caso normal, o
// (zero, status, errBorde) cuando la tanda aborta antes de empezar.
//
// 🔴 SOLO LA PRIMERA INYECCIÓN PUEDE ABORTAR LA TANDA, y solo por los dos errores que hablan de la SESIÓN y
// no del mensaje. El razonamiento es de forma, no de gusto:
//
//   - `ErrSesionNoViva` y `ErrInyectorNoCableado` en la PRIMERA dicen que no hay camino que recorrer, así
//     que las otras N−1 fallarían idénticamente. Devolver un 200 con `errores: 500` sería contestar «he
//     medido» a quien no ha medido nada; el 404 y el 409 dicen qué hacer (emparejar / esperar al listener).
//   - los MISMOS errores a mitad de tanda significan otra cosa: la sesión se cayó DURANTE la medición. Eso
//     no invalida las inyecciones ya hechas, y el parte parcial —con sus cuentas y su `primer_error`— es
//     más útil que un 404 que tira a la basura lo medido hasta ahí.
//   - cualquier OTRO error (un JID mal formado, un fallo de escritura suelto) cuenta y sigue: abortar la
//     tanda por un fallo esporádico dejaría el histograma corto justo cuando lo interesante es ver cuántos
//     fueron.
func inyectarTanda(ctx context.Context, inyectar func(context.Context, string, app.InyeccionEntrante) (bool, error),
	pet peticionInyeccion, lote string) (respuestaInyeccion, int, *errBorde) {

	espera := time.Duration(pet.EsperaMS) * time.Millisecond
	inicio := time.Now()

	res := respuestaInyeccion{Lote: lote, Solicitados: pet.N}
	for i := 0; i < pet.N; i++ {
		// El cliente se fue (Ctrl-C en el curl) o el servidor está cerrando: se corta LIMPIAMENTE y se
		// contesta con lo hecho hasta aquí. Seguir inyectando serviría para llenar la cola de una medición
		// que ya nadie va a leer. Precedente: logsink/handler.go, que también cuelga del r.Context().
		if err := ctx.Err(); err != nil {
			res.PrimerError = primerNoVacio(res.PrimerError, "tanda cortada: "+err.Error())
			break
		}
		if i > 0 && espera > 0 {
			t := time.NewTimer(espera)
			select {
			case <-ctx.Done():
				t.Stop()
				res.PrimerError = primerNoVacio(res.PrimerError, "tanda cortada durante la espera: "+ctx.Err().Error())
				res.DuracionMS = time.Since(inicio).Milliseconds()
				return res, http.StatusOK, nil
			case <-t.C:
			}
		}

		acusar, err := inyectar(ctx, pet.SessionID, app.InyeccionEntrante{
			ChatJID: pet.ChatJID,
			Texto:   pet.Texto,
			Lote:    lote,
			Indice:  i,
		})
		if err != nil {
			if i == 0 {
				// El session_id NO es dato sensible (aparece en cada línea de log del daemon) y sin él el
				// mensaje no diría CUÁL de las sesiones falta. El `texto` no aparece jamás (INV-051.1), y
				// los errores del Manager tampoco lo incrustan.
				if errors.Is(err, sessionmgr.ErrSesionNoViva) {
					return respuestaInyeccion{}, http.StatusNotFound, &errBorde{
						code: codeNotFound,
						message: "no hay sesión viva con ese session_id: el inyector exige una sesión emparejada y " +
							"escuchando (session_id=" + pet.SessionID + ")",
					}
				}
				if errors.Is(err, sessionmgr.ErrInyectorNoCableado) {
					// El mensaje nombra los DOS estados que este centinela cubre (ver su doc en
					// sessionmgr/session.go) y no solo el del arranque: el que se pisa en campo es el
					// segundo —la sesión cayó y espera el backoff de reconexión, hasta 60 s—, y un texto
					// que solo hablara de «aún no cableó» haría pensar en un daemon recién arrancado.
					// Los dos piden lo mismo: esperar y repetir. Por eso comparten 409.
					return respuestaInyeccion{}, http.StatusConflict, &errBorde{
						code: codeConflict,
						message: "la sesión está viva pero no hay escucha a la que inyectar (su listener aún no " +
							"cableó el inyector, o se descableó al caer y espera el backoff de reconexión, hasta " +
							"60 s): reintenta en unos segundos (session_id=" + pet.SessionID + ")",
					}
				}
			}
			res.Errores++
			res.PrimerError = primerNoVacio(res.PrimerError, err.Error())
			continue
		}
		res.Inyectados++
		if acusar {
			res.Acusados++
		}
	}

	res.DuracionMS = time.Since(inicio).Milliseconds()
	return res, http.StatusOK, nil
}

// leerPeticion decodifica y VALIDA el cuerpo. Devuelve un error ya redactado para el 400: el mensaje lo lee
// un humano con un `curl` delante, así que dice qué falta y por qué importa, no solo que falta.
func leerPeticion(r *http.Request) (peticionInyeccion, error) {
	var pet peticionInyeccion
	dec := json.NewDecoder(r.Body)
	// Un campo desconocido es un 400 y no un silencio. Con un `"lotte"` mal tecleado el lote se generaría
	// aleatorio y el operador barrería después las filas equivocadas creyendo que barre las suyas; con
	// `"session"` en vez de `"session_id"` el 400 diría «falta session_id», que es un diagnóstico peor que
	// «no conozco el campo "session"».
	dec.DisallowUnknownFields()
	if err := dec.Decode(&pet); err != nil {
		// ⚠️ El texto del parser va en la RESPUESTA, nunca en un log (esta struct no tiene logger, ver
		// InyectorDeps). Un error de sintaxis puede citar un carácter del cuerpo, y el cuerpo lleva el
		// `texto` del mensaje: en la respuesta al propio operador que acaba de enviarlo eso no revela nada
		// que él no tenga ya, pero en el fichero de log del VPS sería una fuga (INV-051.1).
		return peticionInyeccion{}, fmt.Errorf("cuerpo JSON inválido: %w", err)
	}

	pet.SessionID = strings.TrimSpace(pet.SessionID)
	if pet.SessionID == "" {
		return peticionInyeccion{}, errors.New("falta `session_id`: la inyección recorre el handler de UNA sesión viva " +
			"y no hay default posible (mira `wapp-ctl sessions` para el id)")
	}
	if strings.TrimSpace(pet.Texto) == "" {
		return peticionInyeccion{}, errors.New("falta `texto`: un entrante sin texto nace en la cola ya `clasificado` " +
			"con la marca `sin_texto` y NO recorre el camino que se quiere medir")
	}

	switch {
	case pet.N == 0:
		// Ausente o cero explícito ⇒ una. Es el caso «prueba que esto funciona» y no merece parámetro.
		pet.N = 1
	case pet.N < 0:
		return peticionInyeccion{}, fmt.Errorf("`n` no puede ser negativo (se recibió %d)", pet.N)
	case pet.N > MaxInyecciones:
		return peticionInyeccion{}, fmt.Errorf("`n` no puede pasar de %d (se recibió %d): la inyección es síncrona y "+
			"escribe por la ÚNICA conexión SQLite de la cola, así que un n sin techo deja el daemon 24/7 moliendo "+
			"y compitiendo con los entrantes REALES. Para medir más, repite la llamada con otro lote",
			MaxInyecciones, pet.N)
	}

	if pet.EsperaMS < 0 {
		return peticionInyeccion{}, fmt.Errorf("`espera_ms` no puede ser negativo (se recibió %d); usa 0 para ir a ráfaga",
			pet.EsperaMS)
	}
	return pet, nil
}

// loteAleatorio devuelve 8 hex de CSPRNG. Calcado de `cmd/colaseed.loteAleatorio`, y aleatorio en vez de un
// timestamp por su mismo motivo: dos tandas lanzadas en el mismo segundo (un script) compartirían lote y con
// él los `wa_message_id`, y el índice único (session_id, wa_message_id) se tragaría la segunda EN SILENCIO —
// el store trata el duplicado como caso normal y devuelve nil. El histograma saldría corto sin un solo error,
// que es exactamente el modo de fallo que MP-10 existe para eliminar.
func loteAleatorio() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// primerNoVacio conserva el PRIMER texto no vacío. Existe para que la regla «solo el primer error» se
// escriba una vez y no en los cuatro sitios que la aplican.
func primerNoVacio(actual, nuevo string) string {
	if actual != "" {
		return actual
	}
	return nuevo
}

// Códigos de error del envelope /v1. Se REPITEN aquí porque los del paquete `server` no son exportados (ver
// el doc del paquete). Los valores son los mismos a propósito: un cliente del plano de control discrimina
// por `code` sin parsear el mensaje, y dos dialectos de códigos en el mismo /v1 romperían esa promesa.
const (
	codeInvalidRequest = "invalid_request"
	codeNotFound       = "not_found"
	codeConflict       = "conflict"
	codeInternal       = "internal"
)

// errorBody replica el envelope de error del contrato /v1: {"error":{"code","message"}}.
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// responderJSON serializa v con el status dado. Molde: `intent.StatusHandler` (status.go:85-87), que es el
// precedente de handler externo al paquete `server`.
func responderJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	// El error de Encode solo ocurriría con un tipo no serializable (bug) o con un cliente que cortó; en
	// ninguno de los dos casos hay nada útil que devolver.
	_ = json.NewEncoder(w).Encode(v)
}

// responderError responde con el envelope de error del contrato /v1.
func responderError(w http.ResponseWriter, status int, code, message string) {
	responderJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message}})
}
