package colaentrantes

// claim.go — EL LADO CAJERO del adapter (Plan 051 Ola 2 · T2.1 / T2.7 · design.md §4).
//
// Aquí vive la implementación de app.ColaCajero sobre el MISMO *Store que usa el listener para
// encolar. Que compartan tipo NO es descuido: comparten la BD, el reloj inyectado (s.now), el logger
// y —sobre todo— el CACHÉ DE SOBRES por sesión (s.crypter), que es el que evita preguntarle a la
// custodia una vez por fila. Lo que NO comparten es el candado: ver la nota de concurrencia en
// Reclamar.
//
// 🔴 INV-051.1 se aplica igual que en el encolado, y aquí es MÁS fácil de violar porque este fichero
// sí manipula texto en claro (es su trabajo: descifrar para clasificar). Ni un solo error ni log de
// este fichero puede llevar texto ni meta: solo session_id, wa_message_id, seq, id y cardinalidades.
//
// 🔴 Y TAMPOCO EL `chat_jid` EN CLARO, que es la regla que faltaba. El chat_jid es
// `593999XXXXXX@s.whatsapp.net`: el TELÉFONO del cliente. No es el texto del mensaje, así que la letra de
// INV-051.1 no lo nombraba, pero es el dato personal más directo que pasa por este fichero y estaba
// entrando en el log local por la puerta de atrás: los `fmt.Errorf` de MarcarClasificado lo embebían y el
// cajero loguea ese error tal cual. Desde el code review de la Ola 2:
//
//   - en los ERRORES no va: el `session_id` (UUID, no dato personal) ya identifica la fila para
//     diagnosticar, y el `claim_token_pref` correlaciona el claim con su cierre.
//   - en los LOGS donde saber DE QUÉ CONVERSACIÓN se habla sí aporta (una sesión tiene muchos chats) va
//     `chat_jid_hash`, nunca el valor: ver chatJIDHash.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// Defaults del lado cajero. NO se teclean aquí: son parte del CONTRATO del puerto (lo que ColaCajero
// promete cuando el llamante pasa 0), así que el número vive UNA sola vez, en internal/app/cola.go, y
// este adaptador solo lo referencia. Declararlos otra vez aquí es lo que había antes y es exactamente la
// duplicación que este plan viene eliminando: alguien sube el tope en la config y el adaptador sigue
// toperando en 20 sin que nada lo delate.
const (
	// defaultClaimMaxFilas: el tope de filas de un claim. El porqué del 20, en app.DefaultColaClaimMaxFilas.
	defaultClaimMaxFilas = app.DefaultColaClaimMaxFilas

	// defaultLease: el margen del lease (T2.7), aquí ya como Duration porque es lo que recibe el barrido.
	// El porqué del 60 —y de por qué acortarlo empeora las cosas— en app.DefaultColaLeaseSegundos.
	defaultLease = app.DefaultColaLeaseSegundos * time.Second

	// claimTokenBytes es el tamaño en BYTES del token de fencing (se persiste en hex, así que la columna
	// guarda el doble de caracteres). 16 bytes = 128 bits: la única propiedad que se le pide al token es
	// no repetirse nunca, y con 128 bits de CSPRNG la colisión no es un caso que haya que razonar. No es
	// material criptográfico —no firma ni cifra nada—, así que no crece con el tamaño de una llave.
	claimTokenBytes = 16

	// claimTokenPrefLen es cuántos caracteres HEX del token se escriben en el log. 8, y el número está
	// elegido contra el scrubber de diagnóstico, no a ojo: ver claimTokenPref.
	claimTokenPrefLen = 8

	// chatJIDHashLen es cuántos caracteres HEX del hash del chat_jid se escriben en el log. 8, por la
	// MISMA razón que claimTokenPrefLen (el scrubber redacta a partir de 32 hex seguidos): ver chatJIDHash.
	chatJIDHashLen = 8
)

// claimTokenPref devuelve el PREFIJO del token de fencing que sí se puede escribir en un log.
//
// 🔴 EXISTE POR EL SCRUBBER DE DIAGNÓSTICO, y el problema que resuelve es de los que solo aparecen en
// campo. El token entero son 32 caracteres hex (claimTokenBytes = 16 ⇒ 32 nibbles), y el bundle de
// diagnóstico redacta con `secretPattern` (internal/app/diagnostics/builder.go:73):
// `[0-9a-fA-F]{32,}|[A-Za-z0-9+/_-]{40,}={0,2}`. El token cae EXACTAMENTE en el umbral de la primera rama,
// así que en un bundle —el sitio donde alguien mira estos logs cuando algo ha ido mal— el `claim_token`
// salía como `[REDACTED]`: se redactaba justo el campo que existe para emparejar un descarte con su claim.
// Nada falla; simplemente el dato no está cuando hace falta.
//
// No se arregla bajando la entropía del token ni tocando el scrubber (que hace bien su trabajo: ante la
// duda, redacta). Se arregla escribiendo MENOS: 8 hex no llegan a los 32 de la primera rama, y para caer en
// la segunda harían falta 40 caracteres SEGUIDOS de [A-Za-z0-9+/_-] — inalcanzable con un valor de 8,
// porque tanto en logfmt (`clave=valor`) como en JSON (`"clave":"valor"`) el separador (`=`, `"`, `:`) NO
// pertenece a esa clase y corta la racha antes de que clave y valor se peguen. Verificado LEYENDO
// builder.go:73, no supuesto.
//
// 32 bits bastan para lo único que se le pide: emparejar dos líneas del MISMO log. No es un identificador
// global y NUNCA se compara contra la BD —el fence sigue usando el token ENTERO—, así que una colisión de
// prefijos costaría una lectura confusa de un log, no una decisión incorrecta.
//
// El campo del log se llama `claim_token_pref` y no `claim_token` a propósito: son valores distintos, y un
// nombre igual invitaría a compararlo con la columna o a copiarlo a una consulta.
func claimTokenPref(token string) string {
	if len(token) <= claimTokenPrefLen {
		return token
	}
	return token[:claimTokenPrefLen]
}

// chatJIDHash devuelve un identificador ESTABLE y no legible de una conversación, apto para el log.
//
// EL PROBLEMA: el `chat_jid` es `593999XXXXXX@s.whatsapp.net` — el número de teléfono del cliente. Escribirlo
// en un log local (que además acaba en el bundle de diagnóstico, el fichero que se comparte con soporte)
// es escribir PII en claro, y sobra: quien diagnostica no necesita saber QUIÉN es, necesita poder decir
// «estas dos líneas hablan de la MISMA conversación» y «esta otra es de OTRA».
//
// POR QUÉ UN HASH Y NO UN PREFIJO, que es lo que se hace con el claim_token: los primeros caracteres de un
// JID son el prefijo de país + operadora + los primeros dígitos del número. Un prefijo aquí filtraría
// justo lo que hay que no filtrar. El hash no conserva ninguna estructura del original.
//
// ⚠️ HONESTIDAD SOBRE LO QUE ESTO ES Y LO QUE NO ES: NO es anonimización criptográfica. Un teléfono tiene
// poquísima entropía (~10^10 candidatos) y el hash no lleva sal ni llave, así que quien tenga el hash y
// ganas puede barrer el espacio de números y encontrar preimágenes. Lo que compra es que el número deje de
// estar EN CLARO —ni a la vista de quien abre el log, ni indexable por un grep, ni copiable de un bundle— y
// que el diagnóstico no pierda nada. Si algún día hace falta la propiedad fuerte, la forma es un HMAC con
// una llave local del Edge, no alargar este hash.
//
// 8 caracteres hex por el MISMO motivo que claimTokenPref: el scrubber del bundle
// (internal/app/diagnostics/builder.go) redacta cualquier racha de 32+ hex, así que un hash entero saldría
// `[REDACTED]` justo donde hace falta. 32 bits bastan para emparejar líneas del mismo log; una colisión
// costaría una lectura confusa, nunca una decisión.
func chatJIDHash(jid string) string {
	if jid == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(jid))
	return hex.EncodeToString(sum[:])[:chatJIDHashLen]
}

// nuevoClaimToken genera el token de fencing de UN claim: 16 bytes de CSPRNG en hex.
//
// SI FALLA, SE PROPAGA EL ERROR Y NO SE RECLAMA NADA. La tentación es caer a algo derivado del reloj o de
// un contador «solo por esta vez», y es exactamente el fallo que este token viene a corregir: un fence
// predecible o repetible no es un fence, y el cierre tardío volvería a pisar el trabajo del relevo sin que
// nada lo delate. Un `crypto/rand` que falla es un sistema roto de verdad (en Go moderno solo ocurre si el
// SO no puede dar entropía), y lo correcto entonces es que el cajero no trabaje: la fila se queda `nuevo`
// y el claim siguiente la recoge.
func nuevoClaimToken() (string, error) {
	b := make([]byte, claimTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("colaentrantes: generar token de fencing del claim: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// var _ app.ColaCajero: el Store respalda TAMBIÉN el lado cajero del puerto. La aserción va aquí, en el
// fichero que aporta los métodos, para que borrar este fichero rompa la compilación en el sitio obvio.
var _ app.ColaCajero = (*Store)(nil)

// sqlReclamar es EL claim atómico (design §4, con el tope de filas metido dentro).
//
// Se escribe en UNA sola sentencia y no en un SELECT + UPDATE, que es lo natural de escribir y lo
// equivocado: entre el SELECT y el UPDATE otro cajero (OTRO PROCESO — el worker no vive en el agente)
// se llevaría las mismas filas y se pagarían dos inferencias por el mismo texto. Medido en T2.0: dos
// claims concurrentes con esta forma dan 0 ids comunes.
//
// ⚠️ `LIMIT` directo en el `UPDATE` es ERROR DE SINTAXIS en este driver (medido en T2.0:
// `near "ORDER": syntax error`) — de ahí la forma `WHERE id IN (SELECT … ORDER BY seq LIMIT ?)`, que no
// es una preferencia estética sino la única que compila.
//
// ⚠️ Las dos subconsultas de cabecera (session_id y chat_jid) se evalúan por separado, y SE ESPERA que
// elijan LA MISMA fila: misma tabla, mismo filtro por estado, mismo `ORDER BY seq LIMIT 1`, todo dentro
// de una única sentencia (que en SQLite es su propia transacción implícita, así que nadie escribe entre
// medias). El único modo en que podrían divergir es un EMPATE en `seq`, imposible por construcción
// (contador atómico, un solo escritor). Aun así Reclamar lleva un cinturón de seguridad en Go, porque
// el precio de equivocarse aquí es concatenar dos conversaciones distintas en un mismo prompt.
//
// ⚠️ `intentos = intentos + 1` VA AQUÍ, DENTRO DEL CLAIM, y no en el camino de fallo del cajero (T2.19).
// Tres razones, y ninguna es de estilo: (1) es ATÓMICO con el claim —una sola sentencia, sin ventana en la
// que dos cajeros incrementen el mismo lote—, (2) no cuesta una sentencia extra en el camino caliente, y
// (3) —la que importa— NO SE PIERDE SI EL CAJERO MUERE A MEDIA INFERENCIA, que es exactamente el caso que
// hay que contar: un lote que tumba al worker no llegaría nunca a un `catch` que sumara el intento, y sería
// el más venenoso de todos. Lo que se cuenta son RECLAMOS, que es literalmente «cuántas veces se ha
// intentado esto», incluidos los intentos que nadie llegó a reportar.
//
// El `RETURNING` devuelve el valor YA INCREMENTADO (SQLite entrega los valores de DESPUÉS del UPDATE), así
// que un lote recién nacido llega al cajero con Intentos = 1, nunca 0.
const sqlReclamar = `
UPDATE cola_entrantes
SET estado = ?, tomado_en = ?, claim_token = ?, intentos = intentos + 1
WHERE id IN (
  SELECT id FROM cola_entrantes
  WHERE session_id = (SELECT session_id FROM cola_entrantes WHERE estado = ? ORDER BY seq LIMIT 1)
    AND chat_jid   = (SELECT chat_jid   FROM cola_entrantes WHERE estado = ? ORDER BY seq LIMIT 1)
    AND estado = ?
  ORDER BY seq LIMIT ?)
RETURNING id, seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc, meta_enc, intentos`

// filaCruda es una fila TAL CUAL sale del cursor: aún cifrada. Existe para poder AGOTAR el cursor antes
// de descifrar.
//
// EL MOTIVO ES EL CURSOR, NO LA CONEXIÓN: descifrar dentro del bucle de rows.Next() y abortar a media
// iteración (un blob ilegible) dejaría el cursor a medio consumir, y es el `defer rows.Close()` quien
// tendría que arreglarlo. Separar «leer» de «abrir» cuesta un slice y elimina ese camino. Lo que este
// patrón NO compra es liberar la conexión antes: `database/sql` la devuelve al pool cuando `rows.Next()`
// agota el cursor, así que la conexión se retiene exactamente igual de poco en ambas formas.
type filaCruda struct {
	id          int64
	seq         int64
	sessionID   string
	chatJID     string
	waMessageID string
	tsWhatsApp  int64
	textoEnc    []byte
	metaEnc     []byte
	// intentos es el contador de reclamos de ESTA fila, ya incrementado por el UPDATE (ver sqlReclamar).
	intentos int64
}

// Reclamar toma atómicamente hasta maxFilas filas `nuevo` de la conversación MÁS ANTIGUA, las marca
// `tomado` con su sello de lease y las devuelve ya descifradas y ORDENADAS POR Seq (contrato de
// app.ColaCajero).
//
// 🔴 EL ORDEN SE SOSTIENE EN GO, NO EN EL SQL. `UPDATE … RETURNING` NO respeta el `ORDER BY`: entrega en
// orden de `rowid`. Caso MEDIDO en T2.0 con modernc.org/sqlite v1.53.0 (SQLite 3.53.2): se pidió
// `ORDER BY seq` esperando [seq 10, seq 20] y llegó [seq 20, seq 10]. Como el orden de este lote es
// SEMÁNTICO —es el orden en que la persona escribió los fragmentos que luego se concatenan en UNA sola
// inferencia—, confiar en el cursor sería un bug silencioso, dependiente de los datos y que solo se
// manifiesta como «el LLM clasificó raro»: nada falla, nada se loguea. De ahí el sort.Slice de abajo y
// la aserción explícita de orden en el test.
//
// Y se ordena por `Seq`, NUNCA por `ID`: hoy coinciden por construcción, pero eso es coincidencia y no
// contrato (ver el comentario de app.ColaMensaje.ID).
//
// CONCURRENCIA — este método NO toma s.mu, y es deliberado. `s.mu` existe para serializar el bloque
// podar→contar→insertar de Enqueue, que son varias sentencias que deben verse como una. El claim es UNA
// sola sentencia: SQLite ya la hace atómica, y una transacción IMMEDIATE a mano tampoco haría falta
// (T2.0: dos claims concurrentes no se solapan y el COMMIT tras consumir el cursor no da «database is
// locked»). Tomar s.mu aquí serializaría al CAJERO contra el LISTENER —el mesonero dejaría de anotar
// mensajes mientras el cajero reclama— a cambio de exactamente nada.
//
// MODO DE FALLO SI EL DESCIFRADO FALLA: se devuelve el error y el lote entero se abandona. Las filas ya
// quedaron `tomado` por el UPDATE, así que las rescata el barrido de leases (60 s) y el cajero volverá a
// intentarlo. Se elige esto frente a las alternativas porque las alternativas son peores: SALTARSE la
// fila ilegible clasificaría un turno INCOMPLETO como si estuviera completo (una mentira silenciosa
// sobre el contenido), y DESCARTAR la fila perdería exactamente el mensaje que este plan promete no
// perder. El coste del error es 1 claim fallido por minuto y conversación, acotado y visible en el log;
// el caso normal (la DEK de la sesión no se puede resolver) ni siquiera llega aquí, porque s.crypter ya
// lo corta con su caché negativo. Si el campo mostrara blobs corruptos permanentes (Open falla con el
// sobre correcto), la solución NO es callar la fila: es un contador de reintentos por fila que la
// aparte a un estado de veneno explícito — decisión que no se toma sin datos.
func (s *Store) Reclamar(ctx context.Context, maxFilas int) (*app.ColaLote, error) {
	if maxFilas <= 0 {
		maxFilas = defaultClaimMaxFilas
	}
	// s.now() y no time.Now(): el reloj es inyectable (WithClock) y el barrido de leases mide contra
	// ESTE mismo sello. Dos relojes distintos entre el claim y el barrido harían que los tests de lease
	// pasaran mintiendo. Epoch-SEGUNDOS, como el resto de la tabla (ts_whatsapp, despachado_en).
	//
	// El sello dice CUÁNDO se tomó el lote —es lo único que mide el barrido—; QUIÉN lo tiene lo dice el
	// token de abajo. Fueron la misma columna y era un bug: ver app.ColaLote.ClaimToken.
	tomadoEn := s.now().Unix()

	// El token se genera ANTES del UPDATE y se propaga si falla: sin token no hay fence, y sin fence este
	// claim no debe existir (ver nuevoClaimToken). No se toca la BD, así que la cola queda como estaba.
	claimToken, err := nuevoClaimToken()
	if err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(ctx, sqlReclamar,
		app.EstadoTomado, tomadoEn, claimToken,
		app.EstadoNuevo, app.EstadoNuevo, app.EstadoNuevo,
		maxFilas)
	if err != nil {
		return nil, fmt.Errorf("colaentrantes: reclamar lote (max_filas=%d): %w", maxFilas, err)
	}
	defer func() { _ = rows.Close() }()

	var crudas []filaCruda
	for rows.Next() {
		var f filaCruda
		// meta_enc es NULLABLE: el driver asigna nil a un *[]byte cuando la columna es NULL, que es
		// justo lo que queremos (nil ⇒ «no había meta», sin llamar a Open).
		if err := rows.Scan(&f.id, &f.seq, &f.sessionID, &f.chatJID, &f.waMessageID, &f.tsWhatsApp,
			&f.textoEnc, &f.metaEnc, &f.intentos); err != nil {
			return nil, fmt.Errorf("colaentrantes: escanear fila reclamada: %w", err)
		}
		crudas = append(crudas, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("colaentrantes: recorrer el lote reclamado: %w", err)
	}
	// Cursor vacío ⇒ no hay nada que reclamar. La cola vacía es el estado NORMAL de un worker que va al
	// día, no un error: devolver aquí un error obligaría al bucle del worker a distinguir «no hay nada»
	// de «algo se rompió» leyendo el texto del error.
	if len(crudas) == 0 {
		return nil, nil
	}

	// La conversación del lote la fija la PRIMERA fila del cursor; el SQL garantiza que todas comparten
	// (session_id, chat_jid).
	sessionID, chatJID := crudas[0].sessionID, crudas[0].chatJID

	crypter, err := s.crypter(sessionID)
	if err != nil {
		// INV-051.1: solo identificadores. El error de la custodia habla de llaves, nunca de contenido.
		// SIN `chat_jid`: el sobre lo elige el session_id, así que el JID no aporta nada al diagnóstico y
		// este error lo loguea el cajero tal cual (ver la cabecera del fichero).
		return nil, fmt.Errorf("colaentrantes: resolver sobre del lote (session_id=%s, filas=%d): %w",
			sessionID, len(crudas), err)
	}

	mensajes := make([]app.ColaMensaje, 0, len(crudas))
	// intentos del LOTE = el MÁXIMO de sus filas, no la suma ni el promedio (contrato de
	// app.ColaLote.Intentos). Un claim no siempre se lleva las mismas filas —entre un intento y otro
	// llegan mensajes nuevos a la conversación, y maxFilas puede partir el turno—, así que un lote mezcla
	// filas veteranas con filas recién encoladas y el promedio diluiría justo la señal: basta con que UNA
	// fila lleve N intentos para que este lote sea el que no progresa.
	//
	// Se calcula sobre las filas que ENTRAN al lote, no sobre `crudas`: una fila que el cinturón de
	// seguridad excluyó no forma parte de este lote y su historial no debe decidir su abandono.
	var intentos int64
	for _, f := range crudas {
		// CINTURÓN DE SEGURIDAD (ver la nota de sqlReclamar): una fila de OTRA conversación aquí no
		// debería poder existir. Si existiera, meterla en el lote concatenaría dos conversaciones en un
		// mismo prompt, que es un fallo de contenido, no de mecánica. Se excluye y se grita; la fila
		// queda `tomado` y la devuelve a `nuevo` el barrido de leases.
		if f.sessionID != sessionID || f.chatJID != chatJID {
			// Los dos JIDs van HASHEADOS y no en claro: lo que hay que poder ver aquí es que son DISTINTOS
			// (y cuál se repite si vuelve a pasar), no de qué números se trata. Ver chatJIDHash.
			s.log.Warn("colaentrantes: fila de otra conversación en el mismo claim; se excluye del lote",
				"session_id", f.sessionID, "chat_jid_hash", chatJIDHash(f.chatJID), "seq", f.seq, "id", f.id,
				"lote_session_id", sessionID, "lote_chat_jid_hash", chatJIDHash(chatJID))
			continue
		}
		texto, err := crypter.Open(f.textoEnc)
		if err != nil {
			return nil, fmt.Errorf("colaentrantes: abrir texto de fila reclamada (session_id=%s, wa_message_id=%s, seq=%d): %w",
				f.sessionID, f.waMessageID, f.seq, err)
		}
		var meta []byte
		if f.metaEnc != nil {
			meta, err = crypter.Open(f.metaEnc)
			if err != nil {
				return nil, fmt.Errorf("colaentrantes: abrir meta de fila reclamada (session_id=%s, wa_message_id=%s, seq=%d): %w",
					f.sessionID, f.waMessageID, f.seq, err)
			}
		}
		if f.intentos > intentos {
			intentos = f.intentos
		}
		mensajes = append(mensajes, app.ColaMensaje{
			ID:          f.id,
			Seq:         f.seq,
			WAMessageID: f.waMessageID,
			TSWhatsApp:  f.tsWhatsApp,
			Texto:       string(texto),
			Meta:        meta,
		})
	}
	if len(mensajes) == 0 {
		// Solo alcanzable si el cinturón de arriba descartó TODO. No es un error del cajero: que lo
		// recoja el barrido.
		return nil, nil
	}

	// EL REORDENADO. Va después de descifrar y no antes por comodidad, no por necesidad: el criterio es
	// `Seq`, que ya venía del cursor. Lo importante es que ESTÉ.
	sort.Slice(mensajes, func(i, j int) bool { return mensajes[i].Seq < mensajes[j].Seq })

	// `claim_token_pref` VA AQUÍ Y NO SOLO EN EL DESCARTE, que es lo que faltaba: un prefijo en el Warn del
	// cierre descartado no correlaciona con NADA si el claim que lo originó nunca dijo su token. Con las dos
	// líneas, un log de campo permite reconstruir la carrera entera (qué claim tomó el lote y qué cierre se
	// tiró). INV-051.1 intacto: el prefijo es un identificador de claim, no contenido.
	//
	// `chat_jid_hash` y NO `chat_jid`: una sesión tiene MUCHAS conversaciones, así que poder distinguirlas
	// en el log sí aporta —pero saber el teléfono no—. Ver chatJIDHash.
	//
	// `intentos` también va al log, y es el número que delata al lote venenoso ANTES de que el cajero lo
	// abandone: un valor que sube claim tras claim sobre el mismo chat_jid_hash es el patrón entero.
	s.log.Debug("colaentrantes: lote reclamado",
		"session_id", sessionID, "chat_jid_hash", chatJIDHash(chatJID), "filas", len(mensajes),
		"seq_min", mensajes[0].Seq, "seq_max", mensajes[len(mensajes)-1].Seq,
		"claim_token_pref", claimTokenPref(claimToken), "intentos", intentos)

	// ClaimToken viaja en el lote y es EL MISMO valor que se acaba de escribir en la columna `claim_token`:
	// es el token de fencing que MarcarClasificado exigirá para poder cerrar (ver app.ColaLote.ClaimToken).
	// Regenerarlo allí sería tener dos tokens distintos y ningún fencing. TomadoEn viaja también, pero solo
	// como dato del lease: NO es lo que se compara al cerrar.
	return &app.ColaLote{
		SessionID:  sessionID,
		ChatJID:    chatJID,
		TomadoEn:   tomadoEn,
		ClaimToken: claimToken,
		Intentos:   intentos,
		Mensajes:   mensajes,
	}, nil
}

// MarcarClasificado cierra un lote ya clasificado: escribe intentJSON en la ÚLTIMA fila (la de mayor
// Seq) y deja TODAS las del lote en `clasificado`.
//
// ═════════════════════════════════════════════════════════════════════════════════════════════════════
// ⏳ ESTADO TRANSITORIO DE LA RAMA `feat/044-o1.6` — ESTE CIERRE YA NO GANA NUNCA (T1.6-5, ADR-0045)
// ═════════════════════════════════════════════════════════════════════════════════════════════════════
//
// LÉELO ANTES DE ABRIR UN BUG. Desde el 2026-08-24 este método SIEMPRE llega tarde, y no es un defecto:
// es el hueco que queda entre T1.6-5 (que retiró el push del despachador) y T1.6-2 (que le cambiará el
// oficio al worker-cajero). La secuencia, paso a paso:
//
//  1. el cajero reclama el lote y se pone a inferir (p50 medido: ~8 s);
//  2. el despachador, que desde T1.6-5 NO retiene nada, ve esa misma fila de cabeza —`tomado` se entrega
//     igual que cualquier otro estado— y la entrega EN EL ACTO;
//  3. `MarcarDespachada` la sella `despachado`;
//  4. el cajero termina, llama aquí, y el fence `estado = 'tomado' AND claim_token = ?` NO CASA porque la
//     fila ya no está `tomado`. Rollback, `app.ErrLoteRelevado`, y `CierresDescartadosPorFence` +1.
//
// LO QUE SE VE EN CAMPO: `CierresDescartadosPorFence` subiendo en CADA lote, y CPU de Ollama gastada en
// inferencias que no se guardan en ninguna parte. 🔴 NO HAY PÉRDIDA DE MENSAJES —el mensaje ya salió, y
// salió ANTES que con el modelo viejo—; lo que se tira es la clasificación, que bajo pull no tiene ya por
// dónde viajar (el proto retiró `ClassifiedIntent`).
//
// POR QUÉ NO SE APAGA AQUÍ Y AHORA: apagar el bucle de clasificación del cajero es reescribir lo que
// T1.6-2 va a reordenar de todos modos, con dos manos sobre el mismo fichero. Con el reparto elegido,
// T1.6-2 se limita a BORRAR una puerta que ya existe. Y nada de esto alcanza producción: esta rama no se
// despliega hasta que la ola entera cierre.
//
// ⚠️ CUANDO T1.6-2 RETIRE EL BUCLE DE CLASIFICACIÓN, ESTE MÉTODO SE VA CON ÉL. Es el ÚLTIMO ESCRITOR de
// `app.EstadoClasificado` que queda en el repo (ver el doc de esa constante): detrás de él sólo quedan
// dos LECTORES, que se conservan para siempre porque hay filas ya persistidas en los discos de clientes.
//
// ⚠️ Y EL FENCE NO SE TOCA MIENTRAS TANTO. Es tentador «arreglar» el ruido ampliando el predicado para que
// el cierre tardío muerda igual: sería exactamente el bug que el fencing existe para impedir (ADR-0038),
// y encima escribiría un intent en una fila YA ENTREGADA, que es un dato que nadie volvería a leer.
//
// TODO O NADA, y por eso aquí SÍ hay transacción (a diferencia del claim, que es una sola sentencia):
// son DOS sentencias, y un lote medio cerrado deja filas en `tomado` que nadie mira hasta que el barrido
// de leases las rescate 60 s después — y entonces se re-clasificaría un texto YA clasificado, pagando la
// inferencia dos veces y pudiendo escribir dos intents distintos para el mismo turno.
//
// 🔴 LA TRANSACCIÓN NO BASTA, Y CREER QUE SÍ ERA EL BUG. La atomicidad resuelve «las dos sentencias se
// ven como una»; NO resuelve «este lote sigue siendo mío». La carrera que faltaba cerrar es esta: el
// cajero A reclama, la inferencia se pasa de los 60 s del lease, BarrerLeasesVencidos devuelve las filas
// a `nuevo`, el cajero B las reclama — y A termina y cierra IGUAL, pisando el trabajo de B. Un
// `WHERE id IN (…)` pelado escribe encantado: las filas existen, RowsAffected cuadra y no se loguea nada.
// De ahí el FENCING: las dos sentencias llevan `AND estado = 'tomado' AND claim_token = ?` con el token
// que Reclamar devolvió en lote.ClaimToken, que es lo único que identifica al CLAIM y no solo a la fila.
//
// ⚠️ EL FENCE ES EL TOKEN, NO `tomado_en`. Fue el sello, y era frágil por una razón que ningún test veía:
// es el reloj de pared con granularidad de segundo, y el argumento que lo sostenía («entre dos claims de
// la misma fila median ≥60 s de lease») asume que el reloj avanza. En un portátil —la plataforma
// objetivo— un salto de NTP hacia atrás al despertar de la suspensión hace que el sello del relevo repita
// el del claim original, y el fence deja de morder justo en el caso para el que existe. El token de
// CSPRNG no depende del reloj. `tomado_en` conserva su otro papel, el del lease (ver BarrerLeasesVencidos).
//
// Y SI EL FENCE FALLA, SE HACE ROLLBACK, NO COMMIT: cerrar «las que quedan» dejaría el lote a medias —el
// intent en una fila que no es la última del turno del relevo, o filas `clasificado` sueltas—, que es
// peor que no cerrar nada. Revertido, las filas quedan como estén (las del relevo, cerradas por él; y si
// la causa fue la poda, las supervivientes vuelven a `nuevo` en el siguiente barrido y se reclasifican).
// Se devuelve app.ErrLoteRelevado para que el worker lo distinga de un fallo real y no lo escale a Error.
// La CUENTA (INV-051.3), en cambio, la lleva el Store: CierresDescartadosPorFence aquí y RescatadasPorLease
// en el barrido. Fiarlo al worker era el agujero — el relevo lo provoca BarrerLeasesVencidos por su cuenta,
// sobre filas que pudo tomar OTRO proceso, así que hay relevos que ningún worker llega a ver.
//
// El intent va SOLO en la última fila porque las anteriores son fragmentos del mismo párrafo y no tienen
// intención propia que anotar (design §4); quien decide cuál es «la última» es lote.Ultimo(), y ese
// método se apoya en que Reclamar entregó el slice ORDENADO por Seq.
func (s *Store) MarcarClasificado(ctx context.Context, lote *app.ColaLote, intentJSON string) error {
	ultimo := lote.Ultimo() // nil-safe: cubre lote nil y lote vacío de una vez.
	if ultimo == nil {
		// No-op sin error: un lote vacío es lo que devuelve Reclamar cuando la cola está al día, y el
		// worker no debería tener que comprobarlo antes de cerrar.
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		// Sin `chat_jid` (ver la cabecera del fichero): el session_id ya localiza la fila y este error lo
		// escribe el cajero en el log local tal cual llega.
		return fmt.Errorf("colaentrantes: abrir transacción de cierre de lote (session_id=%s, filas=%d): %w",
			lote.SessionID, len(lote.Mensajes), err)
	}
	defer func() { _ = tx.Rollback() }() // no-op tras Commit; revierte el lote entero si algo falla.

	// intentJSON vacío ⇒ NULL, mismo criterio que Enqueue: la columna a NULL significa «esta fila no
	// tiene intent», y una cadena vacía significaría «tiene un intent que es la cadena vacía».
	intent := sql.NullString{String: intentJSON, Valid: intentJSON != ""}
	// FENCE también aquí, y no solo en la sentencia de estado: es ESTA la que pisa contenido (el
	// intent_json del relevo). Va primero a propósito, mientras la fila todavía está `tomado` con el token
	// original — la sentencia siguiente es la que lo borra.
	if _, err := tx.ExecContext(ctx,
		`UPDATE cola_entrantes SET intent_json = ? WHERE id = ? AND estado = ? AND claim_token = ?`,
		intent, ultimo.ID, app.EstadoTomado, lote.ClaimToken); err != nil {
		return fmt.Errorf("colaentrantes: escribir intent en la última fila del lote (session_id=%s, wa_message_id=%s, seq=%d): %w",
			lote.SessionID, ultimo.WAMessageID, ultimo.Seq, err)
	}

	// IN (?,?,…) con placeholders GENERADOS a partir de los ids, nunca concatenando los valores: aunque
	// aquí los ids sean int64 nuestros (no entrada de usuario), el hábito de construir SQL por
	// concatenación es lo que hay que no tener.
	//
	// `tomado_en = NULL, claim_token = NULL` cierra el ciclo del claim: el token ya se ha usado como fence
	// y el sello ya no mide ningún lease; dejarlos puestos haría que una fila `clasificado` siguiera
	// pareciendo reclamada, y por alguien concreto. Hoy es inocuo (el barrido filtra por estado='tomado'),
	// pero el despachador de la Ola 3 leerá esta tabla y no debe encontrarse dos campos que mienten sobre
	// quién tiene la fila.
	args := make([]any, 0, len(lote.Mensajes)+3)
	args = append(args, app.EstadoClasificado)
	for i := range lote.Mensajes {
		args = append(args, lote.Mensajes[i].ID)
	}
	args = append(args, app.EstadoTomado, lote.ClaimToken)
	marcas := strings.TrimSuffix(strings.Repeat("?,", len(lote.Mensajes)), ",")
	res, err := tx.ExecContext(ctx,
		`UPDATE cola_entrantes SET estado = ?, tomado_en = NULL, claim_token = NULL
		 WHERE id IN (`+marcas+`) AND estado = ? AND claim_token = ?`, args...)
	if err != nil {
		return fmt.Errorf("colaentrantes: marcar lote clasificado (session_id=%s, filas=%d): %w",
			lote.SessionID, len(lote.Mensajes), err)
	}

	// EL FENCE, COMPROBADO ANTES DEL COMMIT (y por eso este bloque está aquí y no después, como estaba).
	// Con `AND estado = 'tomado' AND claim_token = ?` en el WHERE, afectar MENOS filas de las esperadas ya
	// no significa «la poda se llevó una»: significa que el lote —entero o en parte— DEJÓ DE SER NUESTRO
	// mientras clasificábamos. La causa dominante es el relevo por lease vencido; la poda es la otra, y se
	// tratan igual porque en ambos casos este cierre ya no puede ser íntegro, y un cierre parcial deja el
	// lote a medias (ver la cabecera). Así que se sale SIN Commit: el defer de arriba revierte.
	if n, _ := res.RowsAffected(); n < int64(len(lote.Mensajes)) {
		// INV-051.3: la degradación se CUENTA, no solo se loguea. El worker no puede llevar esta cuenta por
		// su lado —le llega un app.ErrLoteRelevado, pero el relevo lo provoca el barrido del Store, sobre
		// filas que quizá tomó OTRO proceso—, así que el acumulado vive donde ocurre el hecho. Se cuentan
		// CIERRES (lotes), no filas: cada uno es una inferencia pagada y tirada.
		descartados := s.cierresDescartadosPorFence.Add(1)
		// Warn y no Error: no es un fallo del cajero, es la carrera funcionando. Lo que se pierde es una
		// inferencia, no un mensaje. INV-051.1: solo identificadores y cardinalidades, nunca texto.
		//
		// `claim_token_pref` y NO el token entero: los 32 hex del token completo los REDACTA el scrubber del
		// bundle de diagnóstico, que es donde este log acaba leyéndose (ver claimTokenPref). El prefijo
		// sobrevive al Scrub y empareja con el `claim_token_pref` que dejó el Debug de Reclamar.
		//
		// `chat_jid_hash` y no `chat_jid`: aquí sí interesa saber si el relevo se ceba SIEMPRE con la misma
		// conversación (una que produce lotes demasiado largos), y para eso basta un identificador estable.
		// El teléfono no añade nada al diagnóstico y sí PII al log (ver chatJIDHash).
		s.log.Warn("colaentrantes: el lote fue relevado (lease vencido) mientras se clasificaba; este cierre se DESCARTA entero",
			"session_id", lote.SessionID, "chat_jid_hash", chatJIDHash(lote.ChatJID),
			"esperadas", len(lote.Mensajes), "afectadas", n,
			"claim_token_pref", claimTokenPref(lote.ClaimToken), "tomado_en", lote.TomadoEn,
			"cierres_descartados_acumulados", descartados)
		// El ERROR va aún más desnudo que el Warn: lo propaga el worker y puede acabar envuelto en sitios
		// que no controlamos, así que ni siquiera el hash. Para correlacionar está el Warn de arriba.
		return fmt.Errorf("colaentrantes: cierre de lote descartado (session_id=%s, esperadas=%d, afectadas=%d): %w",
			lote.SessionID, len(lote.Mensajes), n, app.ErrLoteRelevado)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("colaentrantes: confirmar cierre de lote (session_id=%s, filas=%d): %w",
			lote.SessionID, len(lote.Mensajes), err)
	}
	return nil
}

// BarrerLeasesVencidos devuelve a `nuevo` las filas `tomado` cuyo sello de lease es más viejo que
// `lease`, y responde cuántas rescató (T2.7, lado store). Es la red que recoge lo que un cajero muerto a
// mitad de inferencia dejó bloqueado: sin ella, un worker que se cae con un lote tomado deja esas filas
// invisibles PARA SIEMPRE — ni el claim las ve (no están en `nuevo`) ni la poda las borra (no están en
// `despachado`).
//
// ⚠️ POR QUÉ 60 s ES UN MARGEN AMPLIO A PROPÓSITO, y por qué acortarlo empeora las cosas: la p95 MEDIDA
// de una inferencia es de 3.736 ms — el lease es 16× ese número. Un lease corto NO protege de nada (el
// caso que protege es el proceso muerto, y un proceso muerto lo sigue estando a los 5 s y a los 60);
// lo que hace es rescatar lotes que estaban VIVOS y todavía clasificándose, para que otro cajero los
// reclame y pague una SEGUNDA inferencia por el mismo texto. El margen se dimensiona contra la cola
// larga del modelo, no contra su p95.
//
// LO QUE ESTE MÉTODO NO DECIDE: cada cuánto se le llama, ni cómo despierta el worker (`PRAGMA
// data_version` frente a un poll de intervalo fijo). Esa es la otra mitad de T2.7, exige MEDICIÓN sobre
// el worker ya en marcha, y va con el proceso worker en otra tanda. Aquí solo está el barrido en sí.
func (s *Store) BarrerLeasesVencidos(ctx context.Context, lease time.Duration) (int64, error) {
	if lease <= 0 {
		lease = defaultLease
	}
	// Mismo reloj y misma unidad (epoch-segundos) que el sello que pone Reclamar en `tomado_en`.
	corte := s.now().Unix() - int64(lease.Seconds())

	// `tomado_en IS NOT NULL` además del estado: una fila `tomado` SIN sello es un bug del claim, y ante
	// la duda no se toca (mismo criterio que la poda por TTL con `despachado_en`). Rescatarla a ciegas
	// podría devolver a `nuevo` un lote que se está clasificando ahora mismo.
	//
	// Se rescata POR EL SELLO y no por el token, y no es un descuido: la pregunta del barrido es «¿cuándo
	// se tomó esto?», que es justo lo que mide `tomado_en`; el token responde a otra («¿de quién es?») y no
	// se ordena por él. El `claim_token = NULL` va por la MISMA razón que el `tomado_en = NULL` que ya
	// estaba, y con la misma honestidad sobre su alcance: no es lo que impide el cierre tardío —de eso se
	// encargan el `estado = 'tomado'` del fence y, si el relevo llega, su propio token, que sobrescribe
	// este—, sino que una fila libre no se quede diciendo que pertenece a un cajero que ya la soltó. El
	// despachador de la Ola 3 lee estas columnas.
	res, err := s.db.ExecContext(ctx,
		`UPDATE cola_entrantes SET estado = ?, tomado_en = NULL, claim_token = NULL
		 WHERE estado = ? AND tomado_en IS NOT NULL AND tomado_en < ?`,
		app.EstadoNuevo, app.EstadoTomado, corte)
	if err != nil {
		return 0, fmt.Errorf("colaentrantes: barrer leases vencidos (lease=%s): %w", lease.String(), err)
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		// INV-051.3: el rescate se CUENTA, y aquí es la única cuenta posible. El worker no ve este suceso —
		// el barrido lo hace el Store, sobre filas de un cajero que probablemente ya no existe—, así que sin
		// este acumulado el único testigo de un relevo era una línea de Warn, que se rota y se pierde. Se
		// suman las filas REALMENTE rescatadas (las afectadas por el UPDATE), no el tope ni una estimación.
		total := s.rescatadasPorLease.Add(n)
		// Warn y no Info: rescatar filas significa que un cajero murió a mitad de un lote. Es una
		// anomalía REAL, no rutina — el caso sano es que este barrido no encuentre nunca nada.
		//
		// ⚠️ AQUÍ NO SE PUEDE NOMBRAR EL `claim_token` DEL CAJERO RELEVADO, y no es un olvido: el barrido es
		// UNA sola sentencia que pone la columna a NULL, y el `RETURNING` de SQLite entrega los valores de
		// DESPUÉS del UPDATE (devolvería NULL para todas). Leer los tokens antes exigiría un SELECT previo,
		// que reabre exactamente la ventana de carrera que esta sentencia única cierra. La correlación de
		// este suceso se hace por la otra punta: el cierre descartado del cajero SÍ lleva su
		// `claim_token_pref`, y ahí es donde empareja con el Debug de Reclamar.
		s.log.Warn("colaentrantes: leases vencidos rescatados; un cajero murió a mitad de un lote",
			"rescatadas", n, "lease", lease.String(), "rescatadas_acumuladas", total)
	}
	return n, nil
}
