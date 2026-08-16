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
	// IntentJSON es la clasificación ya resuelta. El listener la trae rellena solo cuando el fastlane
	// (regex, µs) atrapó el mensaje al nacer; "" ⇒ NULL en la columna, y el cajero la rellenará.
	IntentJSON string
	// Estado es el estado inicial de la fila: EstadoNuevo, o EstadoClasificado si el fastlane ya resolvió
	// el intent (el cajero nunca la reclama). Los otros dos estados son transiciones del worker/despachador.
	Estado string
}

// Estados de una fila de la cola (etiquetas estables; se persisten literales en la columna `estado`).
// El ciclo normal es nuevo → tomado → clasificado → despachado; el fastlane atajaba naciendo en
// clasificado. Un barrido devuelve a "nuevo" las filas cuyo lease ("tomado") venció, y la poda por TTL
// borra las "despachado".
const (
	// EstadoNuevo: recién anotada por el listener, pendiente de que el cajero la reclame.
	EstadoNuevo = "nuevo"
	// EstadoTomado: reclamada por el cajero (lease vivo). Si el lease vence, vuelve a EstadoNuevo.
	EstadoTomado = "tomado"
	// EstadoClasificado: con intent resuelto (por el cajero o por el fastlane), lista para el despachador.
	EstadoClasificado = "clasificado"
	// EstadoDespachado: ya entregada al despachador (cloudlink/outbox); solo espera la poda por TTL.
	EstadoDespachado = "despachado"
)

// ─────────────────────────────────────────────────────────────────────────────
// El sobre `omitido` y su enum de MOTIVOS (Plan 051 Ola 2 · ADR-0038 §(e))
// ─────────────────────────────────────────────────────────────────────────────

// MotivoOmitido es la razón por la que una fila de la cola se despacha SIN intent. Se serializa en la
// columna `intent_json` como el sobre `{"omitido":"<motivo>"}` — la OTRA forma del sobre, frente a
// `{"intent":…,"params":…,"confidence":…,"config_version":…}` que escribe el cajero cuando sí clasifica.
//
// 🔴 EL SOBRE `omitido` NUNCA VIAJA AL CABLE. Muere en el Edge (ADR-0038 §(e), REQ-051.6, INV-051.3):
// el proto no tiene dónde ponerlo (`ClassifiedIntent` solo lleva intent/params/confidence/
// config_version) y el despachador, al ver un `omitido`, entrega el mensaje SIN `Intent` y punto. Lo
// que sí sale del Edge es el CONTADOR por motivo, al heartbeat (Ola 4) — nunca agregado, porque
// «se omitió» y «se omitió porque el breaker está abierto» son dos hechos operativos distintos.
//
// POR QUÉ VIVE AQUÍ, LOCAL AL EDGE, Y NO EN `wapp-shared` (decisión del 2026-08-16, tasks.md:701-710):
// un vocabulario que por diseño nunca cruza el cable no gana nada viviendo en el módulo compartido, y
// a cambio impondría un release por cada motivo nuevo. Precedente en casa: `DegradedReason`
// (health/registry.go:38) es un enum de motivos que SÍ cruza a la nube y aun así vive en el Edge.
//
// LA REGLA: si añades un motivo, añádelo a la lista `motivosOmitido` de abajo. Ese slice es la lista
// canónica —la que cuenta la Ola 4 y la que documenta el ADR— y `SobreOmitido` deriva de él, así que
// un motivo que no esté en la lista no tiene sobre y el test lo caza.
type MotivoOmitido string

// Los SIETE motivos. Son siete, no seis: `sin_texto` se añadió el 2026-08-16 (T1.8) porque
// `classifier.FastLane("")` devuelve true y hacía pasar por `fastlane` todo mensaje NO textual
// (imagen, audio, sticker, ubicación) — una mentira en la telemetría, no un detalle.
const (
	// MotivoFastlane: el regex del fastlane resolvió el intent en µs; el cajero nunca reclama la fila.
	// Lo escribe el LISTENER, al nacer la fila (Ola 1, T1.8).
	MotivoFastlane MotivoOmitido = "fastlane"
	// MotivoSinTexto: el mensaje no tiene cuerpo textual que clasificar (imagen, audio, sticker,
	// ubicación). Lo escribe el LISTENER, y se comprueba ANTES que el fastlane (ver arriba).
	MotivoSinTexto MotivoOmitido = "sin_texto"
	// MotivoNoElegible: el mensaje no cumple las condiciones de elegibilidad del clasificador
	// (ver intent.Decorator.eligible). No se intentó clasificar.
	MotivoNoElegible MotivoOmitido = "no_elegible"
	// MotivoPresupuesto: el despachador agotó su presupuesto de espera (WAPP_AGENT_INTENT_WAIT_MS,
	// 4000 ms) sin que el cajero llegara a clasificar. Lo escribe el DESPACHADOR (Ola 3).
	MotivoPresupuesto MotivoOmitido = "presupuesto"
	// MotivoBreaker: el circuit breaker del clasificador está abierto; ni siquiera se llamó a Ollama.
	// Lo escribe el CAJERO (Ola 2, T2.4).
	MotivoBreaker MotivoOmitido = "breaker"
	// MotivoDesconocido: se clasificó y el modelo devolvió la intención reservada «desconocido», o la
	// confianza quedó por debajo del umbral. NO castiga al breaker: es una respuesta válida, no un fallo.
	//
	// 🔴 SE REFERENCIA, NO SE REDECLARA: el valor tiene dueño fuera de este repo
	// (shared/wapp-shared/intents/intents.go:21, `ReservedUnknown`), y es el único del enum que lo tiene.
	// Escribir "desconocido" a mano aquí sería exactamente la divergencia silenciosa que este enum
	// existe para impedir — el día que `wapp-shared` renombre la reservada, esto deja de compilar, que
	// es justo lo que queremos que pase.
	MotivoDesconocido MotivoOmitido = intents.ReservedUnknown
	// MotivoApagado: la feature `llm_intent` está apagada para esta sesión/empresa (entitlement).
	MotivoApagado MotivoOmitido = "apagado"
)

// motivosOmitido es la LISTA CANÓNICA de los siete motivos, en el orden en que se documentan. Es la
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
