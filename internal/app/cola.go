package app

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/EduGoGroup/wapp-shared/intents"
)

// cola.go — Puerto de la COLA DE ENTRANTES del Edge (Plan 051 Ola 1 · T1.1 / ADR-0038 Enmienda 1).
//
// El listener («mesonero») deja de clasificar en línea: anota el mensaje en esta cola y suelta el
// handler de whatsmeow en milisegundos, de modo que el acuse sale con el mensaje YA durable. El
// worker-cajero (proceso aparte) reclama por conversación, clasifica contra Ollama y escribe el
// intent de vuelta. El puerto vive en app (hexagonal); la implementación real lo respalda con un
// fichero SQLite APARTE del edge.db (<data_dir>/cola_entrantes.db) para que la poda agresiva y el
// SetMaxOpenConns(1) del edge.db no se estorben entre dos procesos. Sin broker (ADR-0003).

// ColaItem es una fila de la cola de entrantes, ANTES de cifrar: el puerto habla en CLARO y es el
// adaptador quien sella los campos sensibles con la DEK de la sesión antes de tocar el disco.
//
// INV-051.1 — Texto y Meta JAMÁS se persisten en claro (ADR-0002/0034): el fichero .db no se cifra a
// nivel de página, así que el contenido de negocio se guarda cifrado campo a campo con la DEK de la
// sesión a la que pertenece el mensaje (nunca con una llave global: cada sesión tiene la suya).
//
// SessionID y ChatJID sí van EN CLARO en disco, y es deliberado: SessionID es a la vez la clave de
// enrutado (el despachador drena por sesión, en orden de seq) y la que ELIGE la DEK con la que se
// descifra el resto de la fila —si estuviera cifrado no habría forma de saber con qué llave abrirlo—;
// ChatJID es, junto con SessionID, la clave de conversación por la que el cajero hace su claim
// atómico (mensaje + hijos en un solo lote). Ambos son metadato de enrutado, no contenido.
type ColaItem struct {
	// SessionID es el discriminador de sesión. EN CLARO: es la clave de enrutado y la que elige la DEK
	// con que se sellan/abren Texto y Meta.
	SessionID string
	// ChatJID es el JID del chat. EN CLARO: con SessionID forma la clave de conversación del claim.
	ChatJID string
	// WAMessageID es el id de mensaje de WhatsApp: base de la idempotencia local del encolado (anotar
	// dos veces el mismo mensaje no duplica la fila).
	WAMessageID string
	// TSWhatsApp es events.Message.Info.Timestamp en epoch-segundos. La ventana ADR-0037 ya se evaluó
	// ANTES de encolar: si descartó, no hay fila.
	TSWhatsApp int64
	// Texto es el cuerpo del mensaje. Se sella con la DEK de SessionID antes de persistir (INV-051.1).
	Texto string
	// Meta son los metadatos de negocio del mensaje en JSON (push_name, …). Se sella igual que Texto
	// (INV-051.1); puede ser nil (columna NULL).
	Meta []byte
	// IntentJSON es la columna `intent_json` de la fila.
	//
	// 🔴 EL LISTENER YA NO LA RELLENA NUNCA (Plan 044 · Ola 1.6 · T1.6-5 · ADR-0045). Bajo pull el Edge no
	// clasifica, así que no hay ni intención que anotar ni omisión que justificar al nacer la fila: se
	// encola siempre "" ⇒ NULL. El campo sobrevive porque el Enqueue lo sigue escribiendo (a NULL) y
	// porque la columna sigue teniendo que LEERSE para drenar las colas escritas por binarios anteriores.
	IntentJSON string
	// Estado es el estado inicial de la fila. Hoy es SIEMPRE EstadoNuevo: el ciclo quedó en
	// `nuevo → tomado → despachado` (ADR-0045 §Decisión.4) y el atajo por el que una fila podía NACER ya
	// resuelta murió con el push.
	Estado string
}

// Estados de una fila de la cola (etiquetas estables; se persisten literales en la columna `estado`).
// El ciclo es **nuevo → tomado → despachado** (ADR-0045 §Decisión.4). Un barrido devuelve a "nuevo" las
// filas cuyo lease ("tomado") venció, y la poda por TTL borra las "despachado".
//
// 🔴 ERAN CUATRO HASTA EL PLAN 044 · Ola 1.6 · T1.6-5 (2026-08-24). El cuarto era `clasificado` —«con
// intent resuelto, lista para entregarse»— y lo disolvió el paso de PUSH a PULL: si el Edge no clasifica,
// no hay un momento en la vida de una fila en el que esté «ya clasificada». Hoy el despachador entrega
// CUALQUIER cabeza que no esté `despachado`, sin mirar su estado, así que una fila vieja marcada
// `clasificado` en el disco de un cliente se drena sola por el camino normal — por eso el retiro no
// necesitó migración de la cola.
const (
	// EstadoNuevo: recién anotada por el listener. Es el estado en el que NACEN todas las filas.
	EstadoNuevo = "nuevo"
	// EstadoTomado: reclamada con el claim por fencing (ADR-0038, conservado íntegro por el ADR-0045 §4).
	// Si el lease vence, vuelve a EstadoNuevo.
	//
	// ⚠️ `tomado` NO ES UN DERECHO DE RETENCIÓN SOBRE LA ENTREGA, y nunca lo fue: el despachador entrega
	// una cabeza `tomado` igual que cualquier otra. El claim protege que dos procesos no cierren el mismo
	// lote, no que nadie más mire la fila.
	EstadoTomado = "tomado"
	// EstadoDespachado: ya entregada al despachador (cloudlink/outbox); solo espera la poda por TTL. Es el
	// ÚNICO estado terminal, y el único que el despachador excluye al buscar cabeza.
	EstadoDespachado = "despachado"

	// EstadoClasificado es HISTÓRICO desde el 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-5 · ADR-0045
	// §Decisión.4): significaba «con intent resuelto, lista para entregarse» y era el único estado desde
	// el que el despachador entregaba. Bajo pull el Edge no clasifica, así que ninguna fila puede llegar
	// a estarlo por un camino nuevo.
	//
	// 🔴 EL VALOR NO SE BORRA, POR EL PRECEDENTE EXACTO DE `MotivoNoElegible`: hay filas ya persistidas
	// con esta etiqueta en las colas (`<data_dir>/cola_entrantes.db`) de los equipos de clientes, y el
	// Edge tiene que seguir sabiendo LEERLAS mientras se vacían. Lo que se conserva es la capacidad de
	// decodificar, no el estadio.
	//
	// LOS TRES SITIOS QUE TODAVÍA LO NOMBRAN, y por qué cada uno:
	//
	//  1. `colaentrantes/pendientes.go` — el desglose por estado. Lo cuenta para poder VER vaciarse las
	//     colas antiguas (ver ColaPendientes.Clasificado).
	//  2. `colaentrantes/colaentrantes.go` — la capa 2 del sacrificio por tope. Una fila vieja
	//     `clasificado` es la primera candidata a descartarse bajo presión, exactamente como antes.
	//  3. ⚠️ `colaentrantes/claim.go` (`MarcarClasificado`) — el ÚNICO ESCRITOR VIVO que queda, y es
	//     TRANSITORIO: es el cierre de lote del worker-cajero, que sigue clasificando hasta que T1.6-2 le
	//     cambie el oficio a servir inferencia. Mientras tanto no rompe nada —el despachador entrega la
	//     cabeza al instante, así que ese cierre llega tarde, su fence no casa y es no-op—, pero es la
	//     última raíz del push en este repo. Cuando T1.6-2 cierre, ESTE ESCRITOR DEBE DESAPARECER y aquí
	//     sólo quedan los dos lectores de arriba.
	EstadoClasificado = "clasificado"
)

// ─────────────────────────────────────────────────────────────────────────────
// El sobre `omitido` y su enum de MOTIVOS (Plan 051 Ola 2 · ADR-0038 §(e))
// ─────────────────────────────────────────────────────────────────────────────

// MotivoOmitido es la razón por la que una fila de la cola se despachó SIN intent. Se serializa en la
// columna `intent_json` como el sobre `{"omitido":"<motivo>"}` — la OTRA forma del sobre, frente a
// `{"intent":…,"params":…,"confidence":…,"config_version":…}` que escribía el cajero cuando sí clasificaba.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 LOS OCHO MOTIVOS ESTÁN HUÉRFANOS DESDE EL 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-5 · ADR-0045).
// ─────────────────────────────────────────────────────────────────────────────
//
// LA DECISIÓN, ESCRITA AQUÍ PARA QUE NO SEA IMPLÍCITA. El ADR-0045 invirtió la clasificación de PUSH a
// PULL: el Edge ya no clasifica por iniciativa propia y entrega cada entrante al instante; cuando el
// Cloud necesita saber si un texto es una solicitud, la PIDE por el frame `inference_request`. Un motivo
// de omisión responde a la pregunta «¿por qué esta fila sale SIN intención?», y esa pregunta sólo tiene
// sentido si alguien iba a ponerle una. Bajo pull NADIE iba a ponérsela nunca ⇒ **ninguno de los ocho
// tiene ya productor en el Edge**. Motivo a motivo, en la lista de abajo, está anotado quién lo escribía.
//
// ⚠️ SE CONSIDERÓ —Y SE DESCARTÓ— la hipótesis de que los motivos «de puerta» (`sin_texto`, `apagado`)
// sobrevivieran «porque deciden si se encola». NO deciden eso: **todos los entrantes se encolan y se
// entregan igual**, sin texto y con la feature apagada incluidos. Lo que decidían era el ESTADO en que
// NACÍA la fila (`clasificado` en vez de `nuevo`), o sea si el cajero la reclamaba — y eso es una
// pregunta del push, no de la puerta. Confundir «no se encola» con «el cajero no la toca» habría dejado
// dos ramas vivas escribiendo sobres que nadie volvería a mirar.
//
// 🔴 Y AUN ASÍ NO SE BORRA NI UN VALOR DEL ENUM. Hay filas en discos de clientes y en la nube con estas
// marcas ya persistidas, y el despachador tiene que poder DECODIFICARLAS mientras esas colas se vacían.
// Es el precedente exacto de `no_elegible` (huérfano desde T1.5-3 y conservado por lo mismo), sólo que
// ahora aplica a los ocho. Un valor retirado del enum convierte una fila vieja en un sobre ilegible: se
// perdería el desglose justo en el tramo en que sirve para vigilar la migración.
//
// 🔴 EL SOBRE `omitido` NUNCA VIAJÓ AL CABLE, y sigue sin viajar (ADR-0038 §(e), REQ-051.6, INV-051.3).
// Lo que sí sale del Edge es el CONTADOR por motivo, al heartbeat — nunca agregado, porque «se omitió» y
// «se omitió porque el breaker está abierto» son dos hechos operativos distintos.
//
// POR QUÉ VIVE AQUÍ, LOCAL AL EDGE, Y NO EN `wapp-shared` (decisión del 2026-08-16, tasks.md:701-710):
// un vocabulario que por diseño nunca cruza el cable no gana nada viviendo en el módulo compartido, y
// a cambio impondría un release por cada motivo nuevo. Precedente en casa: `DegradedReason`
// (health/registry.go:38) es un enum de motivos que SÍ cruza a la nube y aun así vive en el Edge.
//
// LA REGLA SIGUE EN PIE: si algún día vuelve a haber un motivo, se añade a la lista `motivosOmitido` de
// abajo. Ese slice es la lista canónica —la que cuenta la telemetría y la que documenta el ADR— y
// `SobreOmitido` deriva de él, así que un motivo que no esté en la lista no tiene sobre y el test lo caza.
type MotivoOmitido string

// Los OCHO motivos. Fueron siete hasta el 2026-08-16 y cada añadido tuvo su causa de campo: `sin_texto`
// entró en T1.8 porque `classifier.FastLane("")` devuelve true y hacía pasar por `fastlane` todo mensaje
// NO textual (imagen, audio, sticker, ubicación) —una mentira en la telemetría, no un detalle—, y
// `fallo_repetido` entró en T2.19 porque un lote que siempre falla congelaba la cola entera.
//
// Cada uno lleva anotado, en su propio comentario, DESDE CUÁNDO nadie lo escribe y POR QUÉ.
const (
	// MotivoFastlane: el regex del fastlane resolvió el intent en µs; el cajero nunca reclamaba la fila.
	// Lo escribía el LISTENER, al nacer la fila (Ola 1, T1.8).
	//
	// 🔴 HUÉRFANO DESDE EL 2026-08-24 (ADR-0045). El carril rápido no desaparece: CAMBIA DE SEDE. Bajo
	// pull la pregunta «¿hace falta el LLM para esto?» se la hace quien va a llamar al LLM, y eso es el
	// motor de flujos del Cloud (ADR-0044 §1.B). Un fastlane en el Edge sólo podía ahorrar una inferencia
	// que el Edge ya no decide lanzar.
	MotivoFastlane MotivoOmitido = "fastlane"
	// MotivoSinTexto: el mensaje no tiene cuerpo textual que clasificar (imagen, audio, sticker,
	// ubicación). Lo escribían el LISTENER (T1.8) y, como defensa en profundidad, el CAJERO (Ola 2).
	//
	// 🔴 HUÉRFANO DESDE EL 2026-08-24 (ADR-0045) EN EL LADO DEL LISTENER. ⚠️ Y NO ES UNA PUERTA: un
	// mensaje sin texto se encola y se entrega EXACTAMENTE IGUAL que uno con texto —una imagen es un
	// entrante de pleno derecho y la nube la quiere—; lo único que este motivo decidía era que el cajero
	// no gastara una plaza del semáforo en una fila que no tenía nada que clasificar. Sin cajero que
	// clasifique, no queda nada que decidir.
	MotivoSinTexto MotivoOmitido = "sin_texto"
	// MotivoNoElegible: el mensaje no cumple las condiciones de elegibilidad del clasificador — venir de un
	// GRUPO.
	//
	// 🔴 HISTÓRICO DESDE EL PLAN 044 · Ola 1.5 · T1.5-3 (REQ-36/D-044.30) — el PRIMERO en quedarse
	// huérfano, y el precedente que gobierna a los otros siete. Era el motivo del tráfico de grupos, y
	// desde T1.5-3 un entrante de grupo se DESCARTA en la puerta del listener (paso 5 de `onMessage`) sin
	// dejar fila, así que no hay fila nueva que marcar. El valor se conserva —y sigue en la lista
	// canónica— porque hay filas ANTIGUAS con esta marca en la cola local y en la nube, y borrarlo del
	// enum las volvería indecodificables.
	MotivoNoElegible MotivoOmitido = "no_elegible"
	// MotivoPresupuesto: el despachador agotó su presupuesto de espera (`WAPP_AGENT_INTENT_WAIT_MS`,
	// 4000 ms) sin que el cajero llegara a clasificar. Lo escribía el DESPACHADOR (Ola 3).
	//
	// 🔴 EL ÚNICO DE LOS OCHO QUE MURIÓ POR BORRADO DEL MECANISMO, no por falta de trabajo: el 2026-08-24
	// se retiró la espera entera (`correrPresupuesto`, la variable de entorno y su constante). No hay
	// reloj que pueda vencer porque no hay reloj. Los otros siete siguen siendo escribibles en teoría; a
	// éste no le queda ni el código.
	MotivoPresupuesto MotivoOmitido = "presupuesto"
	// MotivoBreaker: el circuit breaker del clasificador está abierto; ni siquiera se llamó a Ollama.
	// Lo escribía el CAJERO (Ola 2, T2.4).
	//
	// 🔴 HUÉRFANO DESDE EL 2026-08-24 (ADR-0045), pero el BREAKER NO SE VA: pasa a guardar la inferencia
	// que el Edge SIRVE al Cloud (ADR-0042 + ADR-0045 §Decisión.5). Lo que cambia es cómo se cuenta un
	// breaker abierto: ya no es un sobre en una fila de la cola, sino el ERROR NOMBRADO `breaker_open` en
	// la respuesta `inference_result`. La señal se conserva; cambia de canal.
	MotivoBreaker MotivoOmitido = "breaker"
	// MotivoDesconocido: se clasificó y el modelo devolvió la intención reservada «desconocido», o la
	// confianza quedó por debajo del umbral. NO castigaba al breaker: es una respuesta válida, no un fallo.
	// Lo escribía el CAJERO (vía LLM).
	//
	// 🔴 HUÉRFANO DESDE EL 2026-08-24 (ADR-0045): el umbral y el saneo de params los aplica ahora el
	// CALLER del puerto, en el Cloud (ADR-0045 §Decisión.5), que es quien tiene el contrato de intenciones
	// del tenant. El Edge devuelve JSON crudo y no juzga.
	//
	// 🔴 SE REFERENCIA, NO SE REDECLARA: el valor tiene dueño fuera de este repo
	// (shared/wapp-shared/intents/intents.go:21, `ReservedUnknown`), y es el único del enum que lo tiene.
	// Escribir "desconocido" a mano aquí sería exactamente la divergencia silenciosa que este enum
	// existe para impedir — el día que `wapp-shared` renombre la reservada, esto deja de compilar, que
	// es justo lo que queremos que pase.
	MotivoDesconocido MotivoOmitido = intents.ReservedUnknown
	// MotivoApagado: la feature `llm_intent` está apagada para esta sesión/empresa (entitlement). Lo
	// escribía el LISTENER, leyendo el interruptor local del clasificador.
	//
	// 🔴 HUÉRFANO DESDE EL 2026-08-24 (ADR-0045). ⚠️ El ENTITLEMENT sigue vivo y sigue mandando; lo que
	// muere es que se exprese como un sobre en una fila. Con la feature apagada, hoy, el entrante se
	// encola y se entrega igual —siempre se entregó igual— y quien no pregunta por la intención es el
	// Cloud. Cuando el Edge sirva inferencia (T1.6-2), el mismo interruptor decidirá si atiende o
	// responde con un error nombrado.
	MotivoApagado MotivoOmitido = "apagado"
	// MotivoFalloRepetido: el lote AGOTÓ SUS INTENTOS de clasificación (ColaLote.Intentos llegó al tope
	// WAPP_WORKER_MAX_INTENTOS) y se cerraba SIN intent para que la cola pudiera avanzar. Lo escribía el
	// CAJERO (Ola 2, T2.19).
	//
	// 🔴 HUÉRFANO DESDE EL 2026-08-24 (ADR-0045): sin clasificación por iniciativa propia no hay lote que
	// reintentar, así que no hay intentos que agotar. ⚠️ LA LECCIÓN QUE LO TRAJO SÍ SIGUE VIVA y hay que
	// llevársela a la inferencia servida: un trabajo que siempre falla y que se reintenta sin tope
	// congela la cola entera. Todo el párrafo de abajo es esa lección, y por eso no se resume.
	//
	// 🔴 EXISTIÓ POR UN LOTE VENENOSO QUE CONGELABA LA COLA ENTERA, y el mecanismo era peor de lo que
	// parece. Un fallo de inferencia NO cerraba el lote a propósito (era un reintento gratis), así que las
	// filas se quedaban en `tomado` y el barrido de leases las devolvía a `nuevo` a los 60 s — SIN TOCAR
	// SU `seq`. Y el claim elige siempre la conversación con el `seq` MÁS BAJO. Un lote cuyo texto siempre
	// hacía fallar la inferencia conservaba su seq, volvía a `nuevo` y se lo llevaba OTRA VEZ el siguiente
	// claim, para siempre. Con WAPP_WORKER_MAX_CONCURRENT=1 —el default— eso era la cola entera parada:
	// ningún otro mensaje se clasificaba jamás, y el único síntoma era el contador de fallos subiendo.
	//
	// El reintento gratis seguía siendo lo correcto para un fallo TRANSITORIO (Ollama reiniciándose, un
	// pico de carga); lo que faltaba era distinguirlo de un fallo PERMANENTE. El contador de intentos es
	// esa distinción, y este motivo es lo que se escribía cuando ya no había duda: el mensaje salía igual,
	// sin intent, que es el fallo seguro — se perdía una clasificación, nunca un mensaje.
	MotivoFalloRepetido MotivoOmitido = "fallo_repetido"
)

// motivosOmitido es la LISTA CANÓNICA de los ocho motivos, en el orden en que se documentan. Es la
// fuente de la que salen los sobres precalculados y la que debe recorrer la telemetría de la Ola 4
// para publicar el desglose (INV-051.3: nunca agregado).
var motivosOmitido = []MotivoOmitido{
	MotivoFastlane,
	MotivoSinTexto,
	MotivoNoElegible,
	MotivoPresupuesto,
	MotivoBreaker,
	MotivoDesconocido,
	MotivoApagado,
	MotivoFalloRepetido,
}

// MotivosOmitido devuelve una COPIA de la lista canónica de motivos. Copia y no el slice interno: un
// llamante que lo reordenara (para pintar un informe, por ejemplo) corrompería la lista de todos.
func MotivosOmitido() []MotivoOmitido {
	out := make([]MotivoOmitido, len(motivosOmitido))
	copy(out, motivosOmitido)
	return out
}

// sobreOmitido es el JSON YA SERIALIZADO de cada motivo, calculado una sola vez al cargar el paquete.
// Se precalcula porque `SobreOmitido` se llama en el camino caliente (una vez por mensaje entrante que
// se omite) y serializar ahí un objeto de un solo campo es trabajo puro de descarte.
var sobreOmitido = func() map[MotivoOmitido]string {
	m := make(map[MotivoOmitido]string, len(motivosOmitido))
	for _, motivo := range motivosOmitido {
		// json.Marshal y no una concatenación a mano: si mañana un motivo llevara un carácter que hay
		// que escapar, la concatenación produciría JSON inválido en silencio y esto no.
		b, err := json.Marshal(struct {
			Omitido MotivoOmitido `json:"omitido"`
		}{Omitido: motivo})
		if err != nil {
			// Imposible con un struct de un string: si ocurriera, el paquete no debe cargar con sobres
			// a medias, porque el sobre malformado se persistiría en disco y se descubriría meses después.
			panic("app: no se pudo serializar el sobre del motivo " + string(motivo) + ": " + err.Error())
		}
		m[motivo] = string(b)
	}
	return m
}()

// SobreOmitido devuelve el JSON `{"omitido":"<motivo>"}` que se persiste en la columna `intent_json`.
// Un motivo que no esté en la lista canónica devuelve "" — y "" significa NULL en la columna, es decir,
// «esta fila la reclama el cajero», que es el fallo SEGURO: se clasifica de más, nunca se pierde.
func SobreOmitido(motivo MotivoOmitido) string { return sobreOmitido[motivo] }

// sobreLeido es la forma mínima con la que se INSPECCIONA un `intent_json` ya persistido. Solo mira la
// clave `omitido`: la otra forma del sobre (la del cajero) la deserializa quien la necesita.
type sobreLeido struct {
	Omitido MotivoOmitido `json:"omitido"`
}

// EsOmitido responde si un `intent_json` ya persistido es un sobre de OMISIÓN y, si lo es, con qué
// motivo. Es la única puerta que debe usar el despachador (Ola 3) para aplicar la regla del sobre:
// si hay `omitido`, el mensaje se entrega SIN `Intent` y el sobre NO viaja al cable.
//
// Devuelve false para "" (fila sin clasificar), para el sobre del cajero (`{"intent":…}`) y para
// cualquier JSON ilegible: ante la duda, NO se trata como omisión.
func EsOmitido(intentJSON string) (MotivoOmitido, bool) {
	if intentJSON == "" {
		return "", false
	}
	var s sobreLeido
	if err := json.Unmarshal([]byte(intentJSON), &s); err != nil {
		return "", false
	}
	if s.Omitido == "" {
		return "", false
	}
	return s.Omitido, true
}

// ColaEntrantes es la cola durable de mensajes entrantes pendientes de clasificar. Es el puerto que
// usa el listener; el claim, la clasificación y la poda son del worker-cajero (otras olas). La
// implementación debe ser segura para uso concurrente (N listeners, uno por sesión, encolan a la vez).
type ColaEntrantes interface {
	// Enqueue persiste una fila de la cola, sellando Texto y Meta con la DEK de item.SessionID
	// (INV-051.1). Es IDEMPOTENTE por (SessionID, WAMessageID): anotar dos veces el mismo mensaje no
	// duplica la fila y NO es un error — el segundo intento devuelve nil, porque whatsmeow re-emite
	// eventos al reconectar y un reintento del handler es el caso normal, no una anomalía. La
	// implementación puede consumir (y perder) un número de orden en ese descarte: la secuencia solo
	// promete ser creciente, no contigua.
	Enqueue(ctx context.Context, item ColaItem) error
}

// ─────────────────────────────────────────────────────────────────────────────
// El lado CAJERO del puerto (Plan 051 Ola 2 · T2.1 / T2.7 · design.md §4)
// ─────────────────────────────────────────────────────────────────────────────

// ColaMensaje es una fila YA RECLAMADA y YA DESCIFRADA. Es lo que el cajero concatena y manda al
// clasificador, así que aquí el texto viaja en claro EN MEMORIA — nunca a disco ni a un log (INV-051.1).
type ColaMensaje struct {
	// ID es la clave primaria de la fila. Es lo que identifica la fila para el UPDATE de vuelta.
	//
	// ⚠️ NO es un sustituto de Seq para ordenar. Hoy `id` (rowid) y `seq` coinciden por construcción
	// —un solo proceso inserta, con un contador monotónico bajo el mismo candado—, pero NADA lo declara
	// ni lo prueba: es coincidencia, no contrato (T2.0). Un `AUTOINCREMENT` ausente, una fila reinsertada
	// o un `VACUUM` futuro rompen la coincidencia en silencio, y el síntoma sería un párrafo desordenado
	// que el LLM clasifica mal sin que nada falle.
	ID int64
	// Seq es el número de orden global de la cola. ES EL ÚNICO criterio de orden válido.
	Seq int64
	// WAMessageID es el id del mensaje en WhatsApp (idempotencia y trazas).
	WAMessageID string
	// TSWhatsApp es el timestamp del mensaje en epoch-segundos.
	TSWhatsApp int64
	// Texto es el cuerpo del mensaje YA DESCIFRADO con la DEK de su sesión.
	Texto string
	// Meta son los metadatos de negocio YA DESCIFRADOS; nil si la columna era NULL.
	Meta []byte
}

// ColaLote es todo lo pendiente de UNA conversación, reclamado en un solo claim atómico.
//
// Es la unidad de trabajo del cajero, y la razón de que exista: el §4 del design concatena los
// mensajes del lote y hace UNA sola inferencia, porque «mensaje + hijos» (los fragmentos que un
// humano manda en tres mensajes seguidos) son un solo turno semántico. Cinco filas ⇒ una inferencia,
// no cinco.
type ColaLote struct {
	// SessionID y ChatJID son la clave de conversación por la que se reclamó el lote.
	SessionID string
	ChatJID   string
	// TomadoEn es el sello epoch-segundos con el que Reclamar marcó estas filas como `tomado`. Es CUÁNDO
	// se tomó el lote, y sirve para UNA sola cosa: que BarrerLeasesVencidos sepa si el lease ya caducó.
	//
	// ⚠️ NO ES EL FENCE. Lo fue, y era un error: ver ClaimToken.
	TomadoEn int64
	// ClaimToken es EL TOKEN DE FENCING DEL CLAIM: 16 bytes de CSPRNG en hex, generados por Reclamar y
	// escritos en las filas del lote en el mismo UPDATE que las marca `tomado`.
	//
	// 🔴 NO ES INFORMATIVO, ES DE CORRECCIÓN. Es lo único que demuestra que el lote SIGUE SIENDO DE ESTE
	// CAJERO y no de otro que lo relevó. La carrera que cierra es real y silenciosa: el cajero A reclama y
	// se pasa del lease clasificando → BarrerLeasesVencidos devuelve las filas a `nuevo` → el cajero B las
	// reclama y las toma → A termina y cierra IGUAL, pisando el trabajo de B. Las filas existen y el número
	// cuadra, así que contar filas afectadas NO caza nada: hay que comprobar la IDENTIDAD del claim.
	//
	// POR QUÉ UN TOKEN Y NO EL SELLO `TomadoEn`, que es lo que había antes: el sello es el reloj de PARED
	// con granularidad de SEGUNDO, y el argumento de que dos claims de la misma fila no pueden compartir
	// segundo (entre uno y otro han de pasar ≥60 s de lease) asume que EL RELOJ AVANZA. La plataforma
	// objetivo es un portátil: un salto de NTP hacia atrás al despertar de la suspensión hace que el sello
	// del relevo repita el del claim original, y entonces el fence deja de morder exactamente en el caso
	// para el que existe. Un token aleatorio no depende del reloj en absoluto, y de paso separa las dos
	// preguntas que la columna única confundía: «¿cuándo se tomó?» y «¿quién lo tiene?».
	ClaimToken string
	// Intentos es CUÁNTAS VECES SE HA RECLAMADO este lote, contando el claim que lo acaba de devolver
	// (así que en un lote recién nacido vale 1, nunca 0). Es lo que permite al cajero distinguir un fallo
	// TRANSITORIO —que merece su reintento gratis— de un lote VENENOSO que hay que abandonar con
	// MotivoFalloRepetido para que la cola avance. El porqué entero está en ese motivo.
	//
	// SE CUENTAN RECLAMOS, NO FALLOS, y la diferencia importa: el contador lo incrementa el propio UPDATE
	// del claim (adapters/colaentrantes/claim.go), de forma atómica con él, así que también cuenta los
	// intentos que NADIE llegó a reportar —el cajero muerto a media inferencia, el SIGKILL, el lease
	// vencido—, que son justo los casos que un contador escrito en el camino de fallo perdería. «Cuántas
	// veces se ha intentado esto» es exactamente la pregunta que hay que responder.
	//
	// ⚠️ ES EL MÁXIMO DE LAS FILAS DEL LOTE, no una suma ni un promedio. Un claim no siempre se lleva las
	// mismas filas (llegan mensajes nuevos a la conversación entre un intento y otro, y el tope de
	// maxFilas puede partir el turno), así que un lote puede mezclar filas veteranas con filas recién
	// encoladas. Basta con que UNA fila lleve N intentos para que este lote sea el que no progresa.
	Intentos int64
	// Mensajes son las filas reclamadas, GARANTIZADAS EN ORDEN ASCENDENTE DE Seq.
	//
	// 🔴 Esa garantía es del ADAPTADOR y hay que sostenerla a mano: `UPDATE … RETURNING` NO respeta el
	// `ORDER BY` y entrega en orden de `rowid` (medido en T2.0: se pidió [seq 10, seq 20] y llegó
	// [seq 20, seq 10]). Como aquí el orden es SEMÁNTICO —es el orden en que la persona escribió—,
	// confiar en el orden del cursor sería un bug silencioso y dependiente de los datos.
	Mensajes []ColaMensaje
}

// Ultimo devuelve la última fila del lote (la de mayor Seq), que es DONDE SE ESCRIBE EL INTENT: el §4
// del design deja las anteriores en `clasificado` sin intent propio, porque son fragmentos del mismo
// párrafo y no tienen una intención independiente que anotar. Devuelve nil si el lote está vacío.
func (l *ColaLote) Ultimo() *ColaMensaje {
	if l == nil || len(l.Mensajes) == 0 {
		return nil
	}
	return &l.Mensajes[len(l.Mensajes)-1]
}

// ─────────────────────────────────────────────────────────────────────────────
// Los DOS defaults del lado cajero — son CONTRATO, no detalle del adaptador
// ─────────────────────────────────────────────────────────────────────────────
//
// Viven aquí, en el puerto, y NO en el adaptador ni en internal/infra/config, porque son lo que el
// contrato de ColaCajero promete cuando el llamante no fija nada (maxFilas <= 0, lease <= 0). Tenerlos
// declarados tres veces —una por sitio— es la duplicación silenciosa de siempre: alguien sube el 20 en la
// config y el adaptador sigue toperando en 20 sin que nada lo delate. El adaptador los REFERENCIA y la
// config los REEXPORTA como alias; este es el único sitio donde se teclean los números.

// DefaultColaClaimMaxFilas es el tope de filas que UN solo claim se lleva cuando el llamante no fija otro.
//
// Existe por el punto abierto del ADR-0038 que el design §4 deja anotado («una conversación gigante no
// monopoliza»): sin tope, un chat con miles de fragmentos pendientes se llevaría la cola entera en un
// lote, concatenaría un prompt imposible y dejaría al resto de conversaciones esperando esa inferencia.
// 20 fragmentos son ya mucho más de lo que un humano manda en un turno, y lo que exceda se reclama en el
// claim siguiente: se retrasa, no se pierde.
const DefaultColaClaimMaxFilas = 20

// DefaultColaLeaseSegundos es el margen del lease del claim, en SEGUNDOS (la unidad de `tomado_en`).
//
// El margen es AMPLIO A PROPÓSITO y acortarlo empeora las cosas: la p95 MEDIDA de una inferencia es de
// 3.736 ms — el lease es 16× ese número. Un lease corto NO protege de nada (el caso del que protege es el
// proceso muerto, y un proceso muerto lo sigue estando a los 5 s y a los 60); lo que hace es rescatar
// lotes VIVOS que aún se estaban clasificando, para que otro cajero pague una SEGUNDA inferencia por el
// mismo texto. El margen se dimensiona contra la cola larga del modelo, no contra su p95.
const DefaultColaLeaseSegundos = 60

// ColaCajero es el lado LECTOR/ESCRITOR de la cola: el puerto que consume el worker-cajero. Va aparte
// de ColaEntrantes a propósito, y no es cosmética la separación — son dos PROCESOS distintos (el
// listener vive en el agente, el cajero es el tercer hijo de `wapp-ctl`), y ninguno de los dos debe
// poder llamar por accidente a los métodos del otro.
type ColaCajero interface {
	// Reclamar toma atómicamente hasta maxFilas filas `nuevo` de la conversación MÁS ANTIGUA (la que
	// tiene la fila `nuevo` de menor Seq), las marca `tomado` con su sello de lease y su token de claim, y
	// las devuelve ya descifradas y ORDENADAS POR Seq. El token que escribió viaja de vuelta en
	// ColaLote.ClaimToken, y es lo que MarcarClasificado exige después para poder cerrar el lote.
	//
	// INCREMENTA EL CONTADOR DE INTENTOS de las filas que se lleva, en el mismo UPDATE, y devuelve el
	// máximo resultante en ColaLote.Intentos (≥1 siempre). Es lo que hace que un lote venenoso sea
	// distinguible de un fallo transitorio: ver ColaLote.Intentos y MotivoFalloRepetido.
	//
	// maxFilas <= 0 CAE AL DEFAULT (DefaultColaClaimMaxFilas): es el modo normal de llamar desde el
	// worker, que no tiene por qué conocer el número. Un 0 nunca significa «no te lleves nada» —eso
	// dejaría la cola parada en silencio, que es el peor fallo posible aquí.
	//
	// Devuelve (nil, nil) cuando no hay nada que reclamar: la cola vacía es el estado NORMAL de un
	// worker que va al día, no un error.
	//
	// Es seguro entre procesos: dos cajeros reclamando a la vez no pueden llevarse la misma fila
	// (verificado en T2.0: dos claims concurrentes dan 0 ids comunes).
	Reclamar(ctx context.Context, maxFilas int) (*ColaLote, error)

	// MarcarClasificado cierra un lote ya clasificado: escribe intentJSON en la ÚLTIMA fila y deja
	// todas las del lote en `clasificado`, listas para el despachador.
	//
	// intentJSON es el sobre del cajero (`{"intent":…}`) o un sobre de omisión (SobreOmitido). Todo o
	// nada: o se cierra el lote entero o no se cierra ninguna fila, porque un lote medio cerrado
	// dejaría filas en `tomado` que solo el barrido de leases rescataría, 60 s después.
	//
	// 🔴 EL CIERRE VA CON FENCING: solo escribe sobre las filas que SIGUEN `tomado` con el MISMO
	// lote.ClaimToken con el que Reclamar las marcó. Si el lease venció y otro cajero relevó el lote, este
	// cierre NO toca nada y devuelve ErrLoteRelevado. Un lote nil o vacío es no-op sin error.
	MarcarClasificado(ctx context.Context, lote *ColaLote, intentJSON string) error

	// BarrerLeasesVencidos devuelve a `nuevo` las filas `tomado` cuyo sello de lease es más viejo que
	// `lease`, y responde cuántas rescató. Es la red que recoge lo que un cajero muerto a mitad de
	// inferencia dejó bloqueado.
	//
	// lease <= 0 CAE AL DEFAULT (DefaultColaLeaseSegundos): un lease de 0 declararía vencidos TODOS los
	// leases al instante y re-clasificaría cada lote una y otra vez. Ver en DefaultColaLeaseSegundos por
	// qué el margen es amplio a propósito y por qué acortarlo empeora las cosas.
	BarrerLeasesVencidos(ctx context.Context, lease time.Duration) (int64, error)
}

// ─────────────────────────────────────────────────────────────────────────────
// El lado DESPACHADOR del puerto (Plan 051 Ola 3 · T3.2 · REQ-051.18/19/20)
// ─────────────────────────────────────────────────────────────────────────────

// ColaCabeza es la fila CABEZA de una sesión tal como la ve el despachador: la fila NO despachada de
// `seq` más bajo, con el texto y la meta YA DESCIFRADOS en memoria.
//
// INV-051.1 SIGUE INTACTO: en DISCO la fila sigue cifrada campo a campo con la DEK de su sesión; lo
// que este struct materializa es la copia en memoria que el despachador necesita para construir el
// evento. Igual que app.ColaMensaje, NUNCA se imprime con `%+v` ni se loguea: lleva contenido.
//
// POR QUÉ ES UN TIPO PROPIO Y NO SE REUSA ColaMensaje: son dos preguntas distintas. ColaMensaje es una
// fila de un LOTE ya reclamado (la unidad del cajero, que no necesita saber en qué estado está: acaba
// de ponerla `tomado` él mismo). La cabeza es una fila SUELTA cuyo dato más importante es justo el que
// a ColaMensaje le sobra: `Estado` e `IntentJSON`. Meterle esos campos a ColaMensaje ensuciaría el
// contrato del cajero con dos campos que él nunca mira.
//
// ⚠️ DESDE T1.6-5 EL `Estado` YA NO DECIDE NADA en el despachador: entrega cualquier cabeza que la
// consulta le devuelva (y la consulta ya excluye `despachado`). Se conserva porque es lo que se loguea
// para diagnosticar una fila concreta, y porque el desglose de pendientes lo cuenta.
type ColaCabeza struct {
	// ID es la clave primaria: lo que se pasa a MarcarDespachada / DespacharSinIntent.
	ID int64
	// Seq es el número de orden global. Es el criterio FIFO del despachador (REQ-051.18): la fila N+1
	// no sale antes que la N aunque la N+1 esté lista y la N esté esperando al cajero.
	Seq int64
	// SessionID y ChatJID son el metadato de enrutado, EN CLARO en disco (ver ColaItem).
	SessionID string
	ChatJID   string
	// WAMessageID y TSWhatsApp son los identificadores de trazabilidad del mensaje de WhatsApp.
	WAMessageID string
	TSWhatsApp  int64
	// Estado es el estado literal de la fila: EstadoNuevo o EstadoTomado en las filas nuevas, y el
	// histórico `clasificado` en las que escribió un binario anterior a T1.6-5. Nunca EstadoDespachado
	// (esas filas no son cabeza de nada: la consulta las excluye).
	Estado string
	// IntentJSON es el sobre persistido, o "" si la columna era NULL. 🔴 En una fila nueva es SIEMPRE ""
	// (bajo pull nadie escribe la columna); un valor aquí es una fila vieja drenándose.
	IntentJSON string
	// TieneIntent distingue "" de NULL, que NO son lo mismo (INV-051.3). Hoy sólo sirve para decidir si
	// merece la pena intentar leer el sobre de una fila antigua.
	TieneIntent bool
	// Texto es el cuerpo del mensaje YA DESCIFRADO con la DEK de SessionID.
	Texto string
	// Meta son los metadatos de negocio YA DESCIFRADOS; nil si la columna era NULL (y nil se distingue
	// de una meta vacía: la columna NULL no se abre siquiera).
	Meta []byte
}

// ColaDespachador es el lado LECTOR/SELLADOR de la cola que consume el despachador (Ola 3). Va aparte
// de ColaEntrantes y de ColaCajero por la misma razón que aquellas dos van separadas entre sí: son
// papeles distintos —el listener anota, el cajero clasifica, el despachador entrega y sella— y ninguno
// debe poder llamar por accidente a los métodos de otro. Aquí, además, los papeles viven en PROCESOS
// distintos: el listener y el despachador en `agent serve`, el cajero es el segundo hijo de `wapp-ctl`.
type ColaDespachador interface {
	// CabezaDeSesion devuelve la fila NO despachada de `seq` más bajo de la sesión, ya descifrada, o
	// (nil, nil) si la sesión no tiene nada pendiente.
	//
	// (nil, nil) y NO un error: una sesión al día es el estado NORMAL de un despachador que va sobrado,
	// y es lo que va a devolver la inmensa mayoría de los polls. Devolver error obligaría al bucle a
	// distinguir «no hay nada» de «algo se rompió» leyendo el texto del error, que es exactamente el
	// mismo criterio que ya sigue ColaCajero.Reclamar.
	CabezaDeSesion(ctx context.Context, sessionID string) (*ColaCabeza, error)

	// MarcarDespachada sella `estado='despachado'` + `despachado_en` sobre una fila que NO estuviera ya
	// sellada (REQ-051.20). Es lo que des-inertiza el TTL de REQ-051.7: la poda solo borra filas
	// `despachado` CON sello.
	//
	// 🔴 EL FENCE ES `estado <> 'despachado'`, NO UN ESTADO CONCRETO (T1.6-5). Hasta el 2026-08-24 exigía
	// `estado = 'clasificado'`, que era el único estado desde el que se entregaba. Al disolverse ese
	// estadio, un fence por igualdad habría dejado sin sellar TODO lo que ahora se entrega —es decir,
	// todo—: la fila se re-entregaría en cada poll, para siempre, y la poda por TTL no podría tocarla
	// nunca. El fence sigue existiendo y sigue siendo idempotente: sellar dos veces no reescribe el
	// `despachado_en` de la primera.
	//
	// Si la fila ya estaba `despachado`, la operación es no-op y NO es error.
	MarcarDespachada(ctx context.Context, id int64) error
}

// ColaPendientes es el DESGLOSE POR ESTADO de lo que la cola tiene sin despachar. Son cardinalidades
// puras: ni un identificador, ni un texto, ni un JID (INV-051.1).
//
// `Total` NO es la suma de los tres nombrados: cuenta TODAS las filas no despachadas, incluidas las de un
// estado que este struct no contemple. La diferencia entre `Total` y la suma es, por tanto, la señal de
// que hay filas en un estado que nadie está mirando — que es exactamente el fallo que un desglose cerrado
// escondería.
type ColaPendientes struct {
	// Nuevo son las filas reclamables por el cajero.
	Nuevo int64
	// Tomado son las filas con un claim vivo (una inferencia en vuelo, o un lease por vencer).
	Tomado int64
	// Clasificado son filas HISTÓRICAS: el estadio `clasificado` se disolvió en T1.6-5 (ADR-0045) y
	// ningún binario actual lo escribe. Se sigue contando aparte —en vez de dejarlo caer en el hueco
	// entre `Total` y la suma— para poder VER vaciarse las colas escritas por binarios anteriores. Un
	// número que baja hasta 0 y se queda ahí es la migración terminando; uno que sube es un escritor
	// que no debería existir.
	Clasificado int64
	// Total son TODAS las filas con estado <> 'despachado'.
	Total int64
}

// ColaContador es el CUARTO papel de la cola, y el único que no toca una sola fila: solo CUENTA. Va
// aparte de ColaEntrantes, ColaCajero y ColaDespachador por la misma razón que aquellos tres van
// separados entre sí —quien solo quiere una foto no debe poder encolar, reclamar ni sellar por accidente—
// y aquí la separación compra algo más: este puerto lo consume el LATIDO DE OBSERVABILIDAD (T3.13), que
// corre fuera de todo camino caliente y no debe poder convertirse en un escritor sin que se note.
type ColaContador interface {
	// Pendientes cuenta, agrupando por estado, todo lo que no está despachado.
	//
	// Es una lectura de solo-cuenta y NO toma el candado de escritura del adaptador: ese candado serializa
	// las ESCRITURAS (el bloque podar→tope→insertar de Enqueue) y un COUNT no escribe. Sí compite por la
	// única conexión SQLite del Edge, así que quien lo llame debe hacerlo con cadencia amplia y publicar
	// cuánto tardó.
	Pendientes(ctx context.Context) (ColaPendientes, error)
}

// ErrMotivoOmitidoDesconocido marca un DespacharSinIntent con un motivo que NO está en la lista
// canónica (motivosOmitido), y por tanto no tiene sobre precalculado.
//
// 🔴 POR QUÉ ES UN ERROR Y NO UN «SE ESCRIBE NULL Y YA»: `intent_json` NULL tiene un significado
// asignado y es el CONTRARIO del hecho que se está registrando —«esta fila aún no pasó por el cajero»
// (INV-051.3)—. Persistirlo dejaría en disco una fila `clasificado` SIN sobre, que es exactamente la
// forma de un FRAGMENTO INTERMEDIO DE LOTE: el despachador la entregaría contándola en
// `fragmentos_de_lote` (tráfico sano) en vez de en su motivo de omisión, y el porqué real se perdería
// sin rastro en la única serie que el operador mira para decidir. Sólo es alcanzable añadiendo
// una constante MotivoOmitido y olvidando meterla en `motivosOmitido`, que es un bug de programación
// que debe ser ruidoso en el test, no silencioso en el campo.
var ErrMotivoOmitidoDesconocido = errors.New("cola de entrantes: motivo de omisión fuera de la lista canónica (sin sobre que persistir)")

// ErrColaFalloRepetido marca un error de Enqueue que la implementación YA REPORTÓ en su log por esta
// misma causa y esta misma sesión, dentro de una ventana de enfriamiento. El error SIGUE siendo un
// error (la fila no se anotó y el llamante debe contarlo); lo único que cambia es que el llamante NO
// debe volver a gritarlo a nivel Error.
//
// POR QUÉ VIVE AQUÍ Y NO EN EL ADAPTADOR: el THROTTLE del log es del adaptador —es quien conoce la
// sesión, la causa y la ventana—, pero el NIVEL con que se escribe la línea es del llamante. Este
// centinela es el único puente entre ambos, y va en el puerto para que el listener no tenga que
// importar el paquete del adaptador (colaentrantes) solo para leer un error.
//
// Motivación de campo: una sesión sin DEK legible hacía fallar UN Enqueue POR MENSAJE ENTRANTE, y el
// listener escribía un log.Error por cada uno — a ritmo de socket.
var ErrColaFalloRepetido = errors.New("cola de entrantes: fallo ya reportado para esta sesión (en enfriamiento)")

// ErrLoteRelevado marca un MarcarClasificado que llegó TARDE: el lease del lote venció, el barrido
// devolvió las filas a `nuevo` y otro cajero ya las reclamó (y quizá ya las cerró). El fencing por
// lote.ClaimToken lo detecta, la transacción se revierte entera y NO se escribe una sola fila.
//
// 🔴 NO ES UN FALLO DEL CAJERO NI UNA CORRUPCIÓN: es la carrera funcionando como debe. Lo que se pierde
// es UNA INFERENCIA (trabajo de CPU ya gastado), nunca un mensaje: las filas están sanas y con el intent
// del cajero que sí llegó a tiempo. La alternativa —dejar cerrar al tardío— sí perdería datos: pisaría el
// intent_json del relevo con uno calculado sobre un lote que ya no le pertenece.
//
// POR QUÉ ES UN CENTINELA Y NO SOLO UN LOG: INV-051.3 exige que las degradaciones se CUENTEN, no solo se
// loguéen. El worker distingue con errors.Is este caso de un fallo real de BD y lo lleva a su propio
// contador (Ola 4), sin escalarlo a Error ni reintentar el cierre —reintentarlo sería volver a pisar.
//
// SI APARECE A MENUDO, el número que hay que mirar es el lease, no este error: significa que las
// inferencias están tardando más que DefaultColaLeaseSegundos.
var ErrLoteRelevado = errors.New("cola de entrantes: el lote fue relevado (lease vencido); el cierre tardío se descarta")
