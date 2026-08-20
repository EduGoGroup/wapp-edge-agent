package app

import "encoding/json"

// colasobre.go — EL CONTRATO DE LO QUE SE PERSISTE EN LAS DOS COLUMNAS OPACAS de la cola de entrantes
// (Plan 051 Ola 3 · T3.3): `meta_enc` (los metadatos de negocio del mensaje) e `intent_json` (el sobre
// de la clasificación). Vive en el PUERTO y no en un adaptador porque lo escribe un proceso —el listener
// del agente, el cajero de `wapp-ctl`— y lo lee OTRO —el despachador—, así que su forma no es un detalle
// de implementación de ninguno de los dos: es la frontera entre ellos.
//
// 🔴 LO QUE ATA A LOS DOS EXTREMOS SON LAS CLAVES JSON, NO EL TIPO GO. En disco no hay un `struct`, hay
// bytes; un renombrado de campo que "compila igual" en un lado y no en el otro es una pérdida silenciosa
// de metadatos —el mensaje sale al cable sin remitente y nadie se entera—. Por eso las etiquetas de abajo
// se copian LITERALES de quien escribe, y por eso están citadas aquí con su origen.

// ColaMeta son los metadatos de negocio que acompañan a una fila de la cola: lo que el DESPACHADOR
// necesita para reconstruir el domain.InboundEvent sin volver a ver el *events.Message, menos lo que ya
// son columnas propias (chat_jid, wa_message_id, ts_whatsapp).
//
// Se persiste CIFRADO con la DEK de la sesión (INV-051.1) — por eso puede llevar identidad del remitente,
// que en un log estaría prohibida. En MEMORIA viaja en claro (ColaCabeza.Meta) y aquí valen las mismas
// reglas que para ColaCabeza.Texto: NUNCA se imprime con `%+v` ni se loguea.
//
// ⚠️ DUPLICACIÓN TRANSITORIA DECLARADA — el productor sigue teniendo SU copia. Quien ESCRIBE estos bytes
// es `colaMetaPayload` (internal/adapters/whatsmeow/listener.go), un tipo PRIVADO de ese paquete y con
// estas mismas seis etiquetas. No se unificó en esta tarea porque `listener.go` tiene otro dueño en esta
// ola (T3.0, el que retira el camino inline), y tocarlo desde aquí sería pisarle el fichero. La deuda es
// de UNA línea: que T3.0 sustituya `colaMetaPayload` por este tipo y borre aquel. Mientras tanto, lo que
// impide que diverjan es que las etiquetas de abajo son la copia literal de las suyas.
//
// ⚠️ EL SÉPTIMO CAMPO (`Sintetico`, MP-10) ROMPE ESA SIMETRÍA A PROPÓSITO, Y HAY QUE SABERLO AL LEERLO:
// aquí se DECLARA para poder leerlo, pero el camino de los entrantes REALES no lo escribe nunca (su valor
// es siempre el cero y `omitempty` lo deja fuera del JSON). Quien lo pone a true es el camino del inyector,
// en el adaptador. Si `colaMetaPayload` no lo lleva, la marca local sencillamente no aparece en la fila —
// lo que NO se pierde en ese caso es la marca portante, el prefijo `SINTETICO-` del `WAMessageID`, que es
// columna propia y no depende de este sobre.
//
// 🔴 `IsFromMe` NO ESTÁ, Y NO ES UN OLVIDO. El listener descarta el ECO PROPIO en la PUERTA
// (`onMessage`, filtro 1: `if e.Info.IsFromMe { … return }`), ANTES de encolar, así que NO EXISTE una
// fila de la cola cuyo mensaje sea propio: persistir el campo sería persistir una constante `false`. El
// despachador, en consecuencia, construye el evento con `IsFromMe = false` — y eso no es una suposición
// suya, es una propiedad que sostiene aquel filtro. Si algún día el eco propio volviera a admitirse,
// AÑADIR EL CAMPO AQUÍ ES PARTE DE ESE CAMBIO: sin él, el eco saldría al cable disfrazado de entrante.
type ColaMeta struct {
	// Sender es el JID principal del remitente (`…@s.whatsapp.net` o `…@lid`, según AddressingMode).
	Sender string `json:"sender,omitempty"`
	// SenderAlt es la dirección ALTERNATIVA del mismo remitente (número↔LID). Vacía si whatsmeow aún no
	// aprendió el mapeo (primer contacto): es tolerancia esperada, no un fallo.
	SenderAlt string `json:"sender_alt,omitempty"`
	// AddressingMode es "pn" o "lid": cuál de los dos formatos es Sender.
	AddressingMode string `json:"addressing_mode,omitempty"`
	// PushName es el nombre visible que publica el remitente. SENSIBLE: nunca a un log.
	PushName string `json:"push_name,omitempty"`
	// Type es el tipo de mensaje que reporta whatsmeow (p.ej. "text"); informativo.
	Type string `json:"type,omitempty"`
	// IsGroup indica que el chat es un grupo/lista de difusión.
	IsGroup bool `json:"is_group,omitempty"`
	// Sintetico marca que esta fila NO nació de WhatsApp: la fabricó el INYECTOR DE ENTRANTES (MP-10 Parte
	// A, ver app.InyeccionEntrante) para medir el p99 del handler sin mandar cien mensajes reales contra el
	// número de producción.
	//
	// 🔴 ES LA MARCA **LOCAL**, Y NO ES LA QUE VE LA NUBE. Vive donde vive el resto del sobre: en la fila
	// CIFRADA de la cola, que no sale del Edge. La marca **PORTANTE** —la que sí viaja— es el prefijo
	// `SINTETICO-` del `WAMessageID`, porque el ID del mensaje es lo que el adaptador de CloudLink pone en
	// el proto; este campo no tiene hueco allí. Quien audite «qué mensajes sintéticos llegaron al cloud»
	// mirando SOLO este bool está mirando una columna que la nube nunca recibió: la pregunta se responde
	// sobre el ID.
	//
	// ⚠️ `omitempty` NO ES COSMÉTICA. Sin él, TODA fila real —las que ya están escritas en disco y las que
	// escriba el listener a partir de ahora— pasaría a llevar `"sintetico":false` en su sobre: se cambiaría
	// el JSON persistido de filas que no tienen nada que ver con el inyector, y con él lo que un dump de la
	// cola enseña y lo que cualquier comparación byte a byte de esos bytes devuelve. Con `omitempty`, el
	// camino real produce EXACTAMENTE los mismos bytes que antes de MP-10 y la clave solo existe donde
	// significa algo.
	Sintetico bool `json:"sintetico,omitempty"`
}

// DecodeColaMeta abre el JSON de `meta_enc` YA DESCIFRADO. Un `raw` nil o vacío devuelve el cero sin
// error: la columna es NULLABLE y una fila sin meta es un caso NORMAL (el listener anota la fila igual si
// la serialización del metadato falló — el texto durable vale más que su metadato).
//
// El error de un JSON ILEGIBLE sí se propaga, para que el llamante pueda contarlo y decidir: el criterio
// del despachador es entregar el mensaje IGUAL, con los metadatos vacíos, porque perder el remitente es
// malo pero perder el mensaje es peor.
func DecodeColaMeta(raw []byte) (ColaMeta, error) {
	var m ColaMeta
	if len(raw) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		// El error NO incrusta `raw`: son metadatos de negocio en claro (INV-051.1), y un mensaje de error
		// acaba en un log tarde o temprano.
		return ColaMeta{}, err
	}
	return m, nil
}

// SobreClasificado es la forma del `intent_json` cuando SÍ hubo intención accionable: la OTRA cara del
// sobre, frente al `{"omitido":"<motivo>"}` que produce SobreOmitido.
//
// 🔴 LAS CUATRO CLAVES SON COPIA LITERAL DE `sobreCajero` (internal/app/cajero/cajero.go), que es quien
// las ESCRIBE. Este es el fallo caro de la Ola 3: deserializar con una clave distinta —"name" en vez de
// "intent", "version" en vez de "config_version"— no rompe nada, no falla ningún Unmarshal y no aparece
// en ningún log; simplemente el campo llega VACÍO y el Cloud resuelve el flujo contra una intención sin
// nombre. Se lee lo mismo que se escribe, y por eso la lista está aquí, junta y citada.
//
// La deuda es la misma que la de ColaMeta y se salda igual: que el cajero pase a usar ESTE tipo y borre
// el suyo. No se hizo en esta tarea para no tocar dos ficheros con dueño ajeno en la misma ola.
type SobreClasificado struct {
	// Intent es el nombre de la intención (`domain.ClassifiedIntent.Name`).
	Intent string `json:"intent"`
	// Params son los parámetros extraídos. SENSIBLES: llevan texto literal del cliente.
	Params map[string]string `json:"params,omitempty"`
	// Confidence es la confianza [0,1]. float64 aquí y en el dominio; el paso a float32 lo hace el
	// adaptador de CloudLink al traducir al proto (classifiedIntentToProto), no este lado.
	Confidence float64 `json:"confidence"`
	// ConfigVersion es la versión del contrato de intenciones con el que se clasificó.
	ConfigVersion string `json:"config_version,omitempty"`
}

// LeerSobreClasificado abre un `intent_json` que se espera que sea el sobre del CAJERO y responde si de
// verdad lo era.
//
// 🔴 EL ORDEN DE LAS DOS PUERTAS NO ES INTERCAMBIABLE: hay que llamar ANTES a EsOmitido. Un sobre de
// omisión (`{"omitido":"fastlane"}`) deserializa aquí SIN error —los campos que no casan se ignoran— y
// produciría un SobreClasificado en blanco. Lo que impide que eso se cuele como intención es el
// `Intent == ""` de abajo, pero apoyarse en él sería apoyarse en un accidente: el sobre de omisión tiene
// su propia puerta y es la que decide qué motivo se cuenta.
//
// Devuelve ok=false para "" (fila sin clasificar), para un JSON ilegible y para un sobre SIN nombre de
// intención. Los tres casos significan lo mismo aguas arriba —el mensaje sale SIN `Intent`— y ninguno es
// motivo para retener el mensaje: el fallo seguro de esta cola es perder una clasificación, jamás un
// mensaje.
func LeerSobreClasificado(intentJSON string) (SobreClasificado, bool) {
	if intentJSON == "" {
		return SobreClasificado{}, false
	}
	var s SobreClasificado
	if err := json.Unmarshal([]byte(intentJSON), &s); err != nil {
		return SobreClasificado{}, false
	}
	if s.Intent == "" {
		return SobreClasificado{}, false
	}
	return s, true
}
