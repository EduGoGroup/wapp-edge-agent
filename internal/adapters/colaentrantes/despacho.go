package colaentrantes

// despacho.go — EL LADO DESPACHADOR del adapter (Plan 051 Ola 3 · T3.2 · ✅ CIERRA REQ-051.20).
//
// Aquí vive la implementación de app.ColaDespachador sobre el MISMO *Store que usa el listener para
// encolar y el cajero para clasificar. Comparten la BD, el reloj inyectado (s.now), el logger y el
// CACHÉ DE SOBRES por sesión (s.crypter) — que aquí importa especialmente: el despachador lee la cabeza
// de CADA sesión CADA 500 ms, y sin ese caché sería una consulta a la custodia por poll.
//
// 🔴 ESTE FICHERO ES EL QUE DES-INERTIZA EL TTL. `pruneTTLLocked` (colaentrantes.go) borra únicamente
// filas con `estado='despachado'` Y `despachado_en IS NOT NULL`, y hasta esta ola NADIE escribía
// ninguna de las dos cosas: la poda de T1.6 llevaba desde la Ola 1 corriendo en cada Enqueue sin poder
// borrar jamás una sola fila.
//
// 🔴 Y EL ÚNICO SITIO DE TODO EL SISTEMA QUE ESCRIBE ESAS DOS COSAS ES `sqlMarcarDespachada`. Fueron dos
// sentencias hasta el 2026-08-17: `sqlDespacharSinIntent` también sellaba `despachado`, y ESO PERDÍA EL
// MENSAJE del presupuesto vencido (ver su doc comment). Hoy aquella deja la fila `clasificado` y la
// entrega —con su sello— la hace el camino normal. Un solo escritor del estado terminal: si mañana
// aparece un segundo, el orden «entrega antes de sello» hay que volver a demostrarlo entero.
//
// 🔴 LA UNIDAD DE `despachado_en` ES EPOCH-SEGUNDOS, y equivocarla es el fallo caro de esta tarea: la
// poda compara `despachado_en < s.now().Unix() - ttl.Seconds()`. Un sello en MILIS sería ~1000× mayor
// que cualquier corte y la fila no se borraría NUNCA (el TTL volvería a ser decorativo, en silencio);
// un sello en milis con un corte en segundos al revés borraría a destiempo. Se usa `s.now().Unix()`,
// exactamente igual que `tomado_en` (Reclamar) y que el corte de `pruneTTLLocked` / `BarrerLeasesVencidos`.
//
// 🔴 INV-051.1 se aplica igual que en el claim, y aquí vuelve a ser fácil de violar porque este fichero
// también maneja texto en claro (es su trabajo: descifrar para poder entregar). Ni un solo log ni error
// de este fichero lleva texto, meta ni el `chat_jid` en claro — para el JID está `chatJIDHash` (ver
// claim.go); en la práctica, ninguna de las tres operaciones de abajo necesita nombrarlo siquiera.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// var _ app.ColaDespachador: el Store respalda TAMBIÉN el lado despachador del puerto. La aserción va
// aquí, en el fichero que aporta los métodos, para que borrar este fichero rompa la compilación en el
// sitio obvio (mismo criterio que la de app.ColaCajero en claim.go).
var _ app.ColaDespachador = (*Store)(nil)

// sqlCabezaDeSesion lee la fila NO despachada de `seq` más bajo de una sesión: la CABEZA de su cola.
//
// `estado <> 'despachado'` Y NO `estado IN ('nuevo','tomado','clasificado')`, que es la forma que
// parece más precisa y es la peligrosa: con el IN, una fila en un estado que la lista no contemple
// —un estado nuevo que alguien añada mañana, o el 'zombi' que los tests del tope siembran a mano— se
// volvería INVISIBLE para el despachador, que pasaría de largo y entregaría la fila SIGUIENTE. Eso
// rompe el FIFO por sesión (REQ-051.18) en silencio y sin dejar rastro. Con `<>`, una fila así BLOQUEA
// la cabeza: el despachador se planta, el presupuesto vence y la fila sale por el camino de omisión.
// Se prefiere una parada visible y acotada a un salto de orden invisible.
//
// ÍNDICE QUE SE ESPERA: `ix_cola_conv(session_id, chat_jid, estado)`, por su columna guía `session_id`
// —es la única de los tres índices que empieza por ella—. SQLite acota por ahí las filas de la sesión y
// ordena el resto en memoria: `ix_cola_estado_seq(estado, seq)` no sirve para el `ORDER BY seq` porque
// su columna guía es `estado` y aquí se compara con `<>`, que no es un seek.
//
// ⚠️ EL ÍNDICE IDEAL SERÍA `(session_id, seq)`, que resolvería filtro y orden de un tirón y convertiría
// esto en una lectura de una sola entrada del índice. NO SE AÑADE AQUÍ: un índice nuevo es una
// migración, y una migración de esta tabla es una decisión que no toma esta tarea (regla T2.18: el
// `.sql` no se toca y una columna/índice nuevo va por un `ensure…` guardado en Go). Queda anotado como
// la palanca de rendimiento si el poll de 500 ms × N sesiones llegara a pesar; con las cardinalidades
// del Edge (una cola de paso, tope 50.000 y podada) hoy no pesa.
//
// `session_id` NO se proyecta: el WHERE lo fija por igualdad, así que la columna solo puede devolver el
// valor que ya trae el parámetro. Traerla obligaría a escanearla sobre el propio parámetro (o sobre una
// variable espejo) sin poder diferir jamás del original.
const sqlCabezaDeSesion = `
SELECT id, seq, chat_jid, wa_message_id, ts_whatsapp, texto_enc, meta_enc, intent_json, estado
FROM cola_entrantes
WHERE session_id = ? AND estado <> ?
ORDER BY seq
LIMIT 1`

// sqlMarcarDespachada sella la fila entregada. El predicado `estado = 'clasificado'` es el fence de
// esta operación: solo se sella lo que estaba listo, nunca una fila que volvió a `nuevo` (barrido) ni
// una que ya estaba `despachado` (doble sellado que movería el `despachado_en` hacia adelante y
// retrasaría su poda).
const sqlMarcarDespachada = `
UPDATE cola_entrantes
SET estado = ?, despachado_en = ?
WHERE id = ? AND estado = ?`

// sqlDespacharSinIntent RESUELVE la fila del PRESUPUESTO VENCIDO, y va en UNA sola sentencia a propósito
// (misma disciplina que sqlReclamar y que dropConversacionPendienteMasViejaLocked).
//
// EL PORQUÉ DE LA SENTENCIA ÚNICA: escribir el sobre y sellar el estado por separado abriría una
// ventana en la que OTRO PROCESO —el worker-cajero, que no vive en este binario— puede cerrar el lote
// entre ambas. Con dos sentencias se llegaría a persistir el sobre `{"omitido":"presupuesto"}` PISANDO
// el `intent_json` real que el cajero acababa de escribir, y el mensaje saldría sin intención teniendo
// una perfectamente calculada. Colapsadas en un UPDATE, la comprobación y la escritura ocurren dentro
// del mismo lock de escritura del motor y no queda ventana.
//
// 🔴🔴 EL ESTADO DESTINO ES `clasificado`, **NO** `despachado`, Y NO ES UN DETALLE: es el arreglo del
// 2026-08-17 (Plan 051 O3). Esta sentencia NO escribe `despachado_en` NI el estado terminal — de eso se
// encarga `sqlMarcarDespachada`, DESPUÉS de que la entrega al sink haya salido bien. Ver el doc comment
// de DespacharSinIntent para las cuatro razones; la corta es que sellar aquí PERDÍA el mensaje
// (`sqlCabezaDeSesion` filtra `estado <> 'despachado'`, así que la relectura del despachador ya no
// encontraba la fila y nadie la entregaba nunca).
//
// `tomado_en = NULL, claim_token = NULL` por la MISMA razón que en el cierre del cajero: el sello y el
// token dejan de significar algo en cuanto la fila sale de `tomado`, y una fila que ya no pertenece a
// ningún claim y sigue diciendo «me tiene el cajero X» es un campo que miente a quien lea esta tabla
// después. Además son la MITAD DEL FENCE (ver DespacharSinIntent): un cierre tardío del cajero exige
// `estado='tomado' AND claim_token = ?`, y tras esta sentencia falla los dos predicados.
const sqlDespacharSinIntent = `
UPDATE cola_entrantes
SET intent_json = ?, estado = ?, tomado_en = NULL, claim_token = NULL
WHERE id = ? AND estado IN (?, ?)`

// CabezaDeSesion devuelve la fila NO despachada de `seq` más bajo de la sesión, con el texto y la meta
// YA DESCIFRADOS, o (nil, nil) si la sesión no tiene nada pendiente (contrato de app.ColaDespachador).
//
// Es LA consulta del bucle del despachador: se ejecuta una vez por sesión y por poll (500 ms), así que
// no escribe NADA —ni una línea de Debug— en su camino feliz. Un log por poll y por sesión es la misma
// tormenta que crypterFailureCooldown combate en el encolado, con el agravante de que aquí ocurriría
// también con la cola VACÍA, que es el estado normal.
//
// CONCURRENCIA: no toma s.mu, igual que Reclamar. `s.mu` existe para serializar el bloque
// podar→contar→insertar de Enqueue (varias sentencias que deben verse como una); esto es un SELECT
// suelto, y tomar el candado serializaría al despachador contra el listener a cambio de nada.
//
// EL DESCIFRADO SIGUE EL MISMO CAMINO QUE Reclamar, sin variantes: `s.crypter(sessionID)` —con su caché
// positivo, su caché NEGATIVO y su enfriamiento de 60 s—, y si falla se devuelve el error tal cual. No
// se inventa aquí un manejo propio: un segundo criterio para el mismo fallo haría que la sesión se
// recuperase distinto según por qué puerta se entrara. Que el error suba (en vez de devolver la fila a
// medio descifrar, o saltársela) es lo correcto: una cabeza que no se puede abrir no se puede entregar,
// y saltársela rompería el FIFO exactamente igual que un hueco.
func (s *Store) CabezaDeSesion(ctx context.Context, sessionID string) (*app.ColaCabeza, error) {
	var (
		id          int64
		seq         int64
		chatJID     string
		waMessageID string
		tsWhatsApp  int64
		textoEnc    []byte
		// meta_enc es NULLABLE: el driver asigna nil a un *[]byte cuando la columna es NULL, que es
		// justo lo que queremos (nil ⇒ «no había meta», sin llamar a Open).
		metaEnc    []byte
		intentJSON sql.NullString
		estado     string
	)
	err := s.db.QueryRowContext(ctx, sqlCabezaDeSesion, sessionID, app.EstadoDespachado).Scan(
		&id, &seq, &chatJID, &waMessageID, &tsWhatsApp,
		&textoEnc, &metaEnc, &intentJSON, &estado)
	if errors.Is(err, sql.ErrNoRows) {
		// Sesión al día: NO es un error, es el estado normal de casi todos los polls (ver el contrato
		// del puerto). Es el gemelo del `(nil, nil)` de Reclamar con la cola vacía.
		return nil, nil
	}
	if err != nil {
		// INV-051.1: solo el identificador de enrutado. Ni el chat_jid (aún no se ha escaneado nada
		// utilizable si el Scan falló) ni, por supuesto, nada del contenido.
		return nil, fmt.Errorf("colaentrantes: leer la cabeza de la sesión (session_id=%s): %w", sessionID, err)
	}

	crypter, err := s.crypter(sessionID)
	if err != nil {
		// SIN `chat_jid`: el sobre lo elige el session_id, así que el JID no aporta al diagnóstico, y
		// este error lo loguea el despachador tal cual llega (mismo criterio que en Reclamar).
		return nil, fmt.Errorf("colaentrantes: resolver sobre de la cabeza (session_id=%s, wa_message_id=%s, seq=%d): %w",
			sessionID, waMessageID, seq, err)
	}

	texto, err := crypter.Open(textoEnc)
	if err != nil {
		return nil, fmt.Errorf("colaentrantes: abrir texto de la cabeza (session_id=%s, wa_message_id=%s, seq=%d): %w",
			sessionID, waMessageID, seq, err)
	}
	var meta []byte
	if metaEnc != nil {
		meta, err = crypter.Open(metaEnc)
		if err != nil {
			return nil, fmt.Errorf("colaentrantes: abrir meta de la cabeza (session_id=%s, wa_message_id=%s, seq=%d): %w",
				sessionID, waMessageID, seq, err)
		}
	}

	return &app.ColaCabeza{
		ID:          id,
		Seq:         seq,
		SessionID:   sessionID,
		ChatJID:     chatJID,
		WAMessageID: waMessageID,
		TSWhatsApp:  tsWhatsApp,
		Estado:      estado,
		IntentJSON:  intentJSON.String, // "" cuando la columna es NULL (NullString deja el cero)
		// NULL Y CADENA VACÍA SE COLAPSAN AL MISMO LADO, y es deliberado. Hoy la cadena vacía no puede
		// llegar a la columna (Enqueue y MarcarClasificado mapean "" ⇒ NULL los dos), pero si llegara,
		// `TieneIntent = intentJSON.Valid` a secas diría «esta fila tiene sobre» con un sobre que
		// `app.EsOmitido` no sabría leer y que la traducción del contrato tampoco. Tratarla como «aún no
		// hay intent» es el fallo SEGURO: el despachador espera o despacha sin intención, nunca entrega
		// una intención inventada.
		TieneIntent: intentJSON.Valid && intentJSON.String != "",
		Texto:       string(texto),
		Meta:        meta,
	}, nil
}

// MarcarDespachada sella `estado='despachado'` + `despachado_en` sobre una fila que sigue `clasificado`
// (REQ-051.20). Es el sello que hace que la poda por TTL tenga por fin algo que borrar.
//
// 0 FILAS AFECTADAS NO ES ERROR, y esa es la mitad del contrato que hay que respetar: significa que la
// fila ya no estaba `clasificado` cuando llegó el sello. El despachador la releerá en el poll siguiente
// y volverá a decidir con lo que haya en disco, que es lo correcto — la BD manda sobre la foto que el
// despachador tenía en la mano.
//
// 🔴 Y NO LLEVA CONTADOR PROPIO, decidido a conciencia y no por omisión. Las únicas causas alcanzables
// de un 0 aquí son:
//
//   - la fila la BORRÓ el tope mientras se entregaba (dropOldestLocked, capa 2), que YA se cuenta en
//     `descartadasPorTope` y se grita con un Warn en el sitio donde ocurre;
//   - la fila la selló otro sellado de este mismo despachador, imposible por construcción: el
//     despachador es SERIAL POR SESIÓN (T3.3) y es el único escritor de `despachado` en todo el sistema.
//
// Un contador aquí, por tanto, o repetiría un número que ya existe o contaría un suceso imposible; en
// ambos casos sería un valor que nadie puede accionar, y el ruido de una serie más en el heartbeat de
// la Ola 4 tiene coste. Lo que sí se deja es la traza en Debug: diagnosticable subiendo el nivel en
// campo, sin estado nuevo ni un byte más en el camino caliente.
func (s *Store) MarcarDespachada(ctx context.Context, id int64) error {
	// s.now().Unix(): EPOCH-SEGUNDOS, la misma unidad que mide pruneTTLLocked. Y el reloj INYECTADO
	// (WithClock), nunca time.Now() directo: los tests del TTL adelantan este mismo reloj 25 h, y dos
	// relojes distintos entre el sello y la poda harían que pasaran mintiendo.
	res, err := s.db.ExecContext(ctx, sqlMarcarDespachada,
		app.EstadoDespachado, s.now().Unix(), id, app.EstadoClasificado)
	if err != nil {
		// INV-051.1: solo el id de la fila. Aquí no hay ni session_id que citar —la firma es por id, que
		// es lo que el despachador tiene en la mano— y con él basta para localizarla en la tabla.
		return fmt.Errorf("colaentrantes: sellar fila despachada (id=%d): %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		s.log.Debug("colaentrantes: el sello de despacho no encontró la fila en 'clasificado' (¿la borró el tope?); se releerá",
			"id", id)
	}
	return nil
}

// DespacharSinIntent escribe el sobre de omisión y RESUELVE la fila —la deja `clasificado`, soltando su
// claim— sobre una fila que sigue `nuevo` o `tomado`, en UNA sola sentencia (ver sqlDespacharSinIntent).
// Es el camino del presupuesto vencido (T3.4): lo que garantiza que un cajero atascado retrase un
// mensaje, nunca lo pierda ni lo detenga indefinidamente.
//
// 🔴🔴 NO SELLA `despachado` NI `despachado_en`, Y EL NOMBRE DEL MÉTODO ENGAÑA SI SE LEE DEPRISA. Lo que
// hace es dejar la fila LISTA PARA ENTREGARSE SIN INTENCIÓN; quien la entrega y quien la sella es el
// despachador, por su camino normal (`entregar` → `app.EsOmitido` → `MarcarDespachada`). Escribió
// `despachado` hasta el 2026-08-17 y ESO PERDÍA EL MENSAJE. Las cuatro razones del cambio, por orden de
// peso:
//
//  1. CRASH-SAFE. Si el proceso muere entre este UPDATE y la entrega, la fila sigue `clasificado`: el
//     despachador la relee al arrancar y la entrega. Con `despachado` quedaba sellada en disco sin haber
//     salido jamás al cable, y el TTL se la llevaba a las 24 h sin una sola línea de log.
//  2. RESPETA «ENTREGA ANTES DE SELLO», la regla que ya rige en Despachador.entregar y que es una
//     decisión de PÉRDIDA DE DATOS: un duplicado es un incidente de idempotencia (el cable lo tolera
//     deduplicando por `wa_message_id`), una pérdida es un incidente de negocio. Sellar aquí era
//     exactamente el orden contrario —sello primero, entrega nunca—.
//  3. EL WORKER SIGUE SIN TRABAJARLA EN VANO, que es lo que exige el design §5. `Reclamar` solo toma
//     filas `nuevo`; `clasificado` la saca de su alcance igual de bien que `despachado`. No se pierde
//     nada de lo que el sello terminal aportaba en ese frente.
//  4. EL FENCING SE MANTIENE INTACTO (ver el bloque de abajo): el cierre tardío del cajero exige
//     `estado='tomado'` Y `claim_token`, y esta sentencia rompe los dos predicados a la vez.
//
// ⚠️ ESTO ENMIENDA LA LETRA DE REQ-051.19, que decía `estado='despachado'`, conservando su INTENCIÓN
// («un cajero atascado retrasa el mensaje, nunca lo pierde»). La letra describía un mecanismo que hacía
// justo lo contrario de lo que el requisito promete. La enmienda queda documentada en el plan.
//
// 0 FILAS AFECTADAS ⇒ NO-OP SILENCIOSO, NO ERROR: el cajero cerró el lote justo antes y la fila ya está
// `clasificado` CON SU INTENT REAL. El despachador la relee en el poll siguiente y la entrega con él,
// que es el desenlace BUENO de esa carrera. Se cuenta (ver Store.despachosSinIntentNoAplicados) porque
// es la única forma de saber cuántas veces se pagó la espera entera del presupuesto para nada.
//
// ⚠️ Y NO ES IDEMPOTENTE EN EL CONTADOR: una segunda llamada sobre una fila que ESTE MISMO método ya
// resolvió también da 0 filas y también suma a `despachosSinIntentNoAplicados`, contaminando la serie
// con un suceso que no es «el cajero ganó la carrera». No es alcanzable hoy —`correrPresupuesto` marca
// `disparado` y la vuelta siguiente encuentra la fila `clasificado` y la ENTREGA, sin volver a sellar—,
// pero es la trampa a vigilar si alguien añade un segundo llamante a este método.
//
// ─── ¿ROMPE ESTO EL FENCING DE MarcarClasificado? NO: LO REFUERZA ───
//
// El fence del cajero exige, en SUS DOS sentencias, `estado = 'tomado' AND claim_token = ?`. Tras este
// UPDATE la fila queda `estado='clasificado'` y `claim_token = NULL`, así que un cierre tardío del
// cajero falla los DOS predicados a la vez —le bastaba fallar uno—, sus dos sentencias afectan 0 filas,
// el `RowsAffected < len(lote.Mensajes)` lo detecta ANTES del Commit, la transacción se revierte entera
// y devuelve app.ErrLoteRelevado. Ni una fila escrita. Es exactamente el camino que el fence ya tenía
// para el relevo por lease, reutilizado tal cual.
//
// ⚠️ LA CONSECUENCIA QUE HAY QUE CONOCER, porque no es obvia y no es un bug: el cajero clasifica por
// LOTES (una conversación entera) y el despachador resuelve de UNA EN UNA (la cabeza). Si el lote tenía
// 3 filas y el presupuesto solo venció para la cabeza, el cierre del cajero se descarta ENTERO —el fence
// es todo-o-nada, ver MarcarClasificado—, así que las 2 hermanas se quedan `tomado` con el token de un
// claim ya muerto. NINGUNA SE PIERDE; lo que se pierde es la CLASIFICACIÓN, y de una forma que conviene
// no idealizar. Hay DOS redes, y en el sistema cableado gana la primera:
//
//   - EL DESPACHADOR, a los ~4000 ms de que cada hermana pase a ser cabeza. `sqlDespacharSinIntent`
//     muerde sobre `tomado`, así que la resuelve con su propio sobre `{"omitido":"presupuesto"}` y la
//     entrega SIN intención. ESTE ES EL CAMINO NORMAL: el lote entero sale sin intent, escalonado un
//     presupuesto por fila, y el barrido de leases no llega a ver nada.
//   - EL BARRIDO DE LEASES, a los 60 s, que las devolvería a `nuevo` para que el claim siguiente las
//     reclasificara. Solo actúa si el despachador de esa sesión NO está corriendo (arranque a medias,
//     `startDespachador` que no arrancó, proceso caído): es la red de la red, no el desenlace esperado.
//
// Lo que se paga, por tanto, es UNA INFERENCIA tirada (`cierresDescartadosPorFence`) MÁS el intent de las
// hermanas. Se prefiere igual a la alternativa —dejar cerrar al cajero sobre la cabeza ya resuelta—
// porque esa sí PISARÍA el sobre `{"omitido":"presupuesto"}` con un intent calculado, y el mensaje podría
// salir al cable con una intención que el despachador ya había decidido no esperar.
//
// LA CARRERA CONTRARIA también es segura, y no hace falta ordenarla: si el cajero commitea primero, la
// fila pasa a `clasificado`, el `estado IN ('nuevo','tomado')` de aquí no casa y esto es no-op. SQLite
// serializa los dos escritores (un solo lock de escritura), así que uno de los dos llega antes y el
// otro se encuentra el predicado incumplido. No hay orden en el que ambos escriban.
func (s *Store) DespacharSinIntent(ctx context.Context, id int64, motivo app.MotivoOmitido) error {
	// EL SOBRE SE RESUELVE ANTES DE TOCAR LA BD: un motivo fuera de la lista canónica no tiene sobre
	// precalculado y `SobreOmitido` devuelve "", que en esta columna significa NULL, es decir «esta fila
	// no pasó por el cajero» — lo CONTRARIO del hecho que se está registrando. Persistir esa mentira es
	// peor que fallar ruidosamente: la fila quedaría `clasificado` SIN sobre, que es exactamente la forma
	// de un fragmento intermedio de lote, y el despachador la contaría como tráfico sano en vez de por su
	// motivo — el porqué real se perdería sin rastro. Ver app.ErrMotivoOmitidoDesconocido.
	sobre := app.SobreOmitido(motivo)
	if sobre == "" {
		return fmt.Errorf("colaentrantes: despachar sin intent (id=%d, motivo=%q): %w",
			id, motivo, app.ErrMotivoOmitidoDesconocido)
	}

	// SIN RELOJ: esta sentencia ya no escribe `despachado_en`. El único sello temporal de la fila lo pone
	// MarcarDespachada, tras la entrega, y con él la unidad (epoch-segundos) queda en un solo sitio.
	res, err := s.db.ExecContext(ctx, sqlDespacharSinIntent,
		sobre, app.EstadoClasificado, id, app.EstadoNuevo, app.EstadoTomado)
	if err != nil {
		return fmt.Errorf("colaentrantes: despachar sin intent (id=%d, motivo=%q): %w", id, motivo, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// INV-051.3: se CUENTA, no solo se loguea. El motivo va en el log porque es lo que permite leer
		// la línea sin cruzarla con nada; el acumulado es agregado y sin PII (una cardinalidad).
		total := s.despachosSinIntentNoAplicados.Add(1)
		// Debug y no Warn: nada ha ido mal. La fila sale igual y sale MEJOR (con su intent real); lo
		// único que cuenta esta línea es que el presupuesto se agotó por muy poco.
		s.log.Debug("colaentrantes: el cajero cerró la fila antes de que venciera el presupuesto; el sobre de omisión es no-op",
			"id", id, "motivo", string(motivo), "no_aplicados_acumulados", total)
	}
	return nil
}

// DespachosSinIntentNoAplicados devuelve cuántas veces DespacharSinIntent llegó tarde: el presupuesto
// venció, pero el cajero ya había cerrado la fila y el sobre de omisión no tocó nada. Acumulado
// monotónico, sin PII: solo una cardinalidad.
//
// SIGUE CONTANDO LO MISMO TRAS EL ARREGLO DEL 2026-08-17 (que la sentencia deje la fila en `clasificado`
// en vez de en `despachado`): lo que cuenta es el `RowsAffected == 0`, y su causa DOMINANTE sigue siendo
// exactamente la misma —la fila salió de `('nuevo','tomado')` porque el cajero commiteó primero—. Las
// otras dos formas de dar cero, para que nadie lea este número como más limpio de lo que es:
//
//   - LA BORRÓ EL TOPE mientras el presupuesto corría (dropOldestLocked capa 3: bajo presión real
//     sacrifica CONVERSACIONES PENDIENTES enteras, y la cabeza que se está esperando es una fila
//     pendiente). Es rarísimo y ya se grita y se cuenta donde ocurre (`descartadasPorTope`), pero es
//     alcanzable — no es «imposible por construcción».
//   - UNA SEGUNDA LLAMADA de este mismo método sobre la fila que él ya resolvió: hoy no hay llamante que
//     lo haga (ver el aviso de no-idempotencia en DespacharSinIntent).
//   - 🔴 LA FILA ZOMBI (cuarta causa, añadida el 2026-08-17; la doc listaba sólo tres). Una cabeza en un
//     estado que esta versión no conoce no casa el predicado `estado IN ('nuevo','tomado')`, así que su
//     PRIMER disparo ya da cero y suma aquí. No es «la carrera del lado bueno» como las otras tres: es
//     una fila que NO va a salir nunca —su sesión deja de drenar— contaminando justo la serie con la que
//     se calibra `WAPP_AGENT_INTENT_WAIT_MS`. Si este número sube sin que suba el tráfico, hay que
//     cruzarlo con `CabezasAtascadas()` del despachador antes de tocar el presupuesto.
//
// CERO NO ES EL VALOR SANO AQUÍ (a diferencia de RescatadasPorLease): es el borde de la calibración del
// presupuesto contra la latencia del modelo (4000 ms frente a una p95 medida de 3.736 ms). Un número
// que crece dice que se está pagando la espera completa para recibir el intent igualmente — la palanca
// entonces es WAPP_AGENT_INTENT_WAIT_MS, no este código.
func (s *Store) DespachosSinIntentNoAplicados() int64 { return s.despachosSinIntentNoAplicados.Load() }
