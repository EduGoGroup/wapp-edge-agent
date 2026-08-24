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
// 🔴 Y EL ÚNICO SITIO DE TODO EL SISTEMA QUE ESCRIBE ESAS DOS COSAS ES `sqlMarcarDespachada`. Lo fue
// siempre salvo entre la Ola 3 y el 2026-08-17, cuando `sqlDespacharSinIntent` también sellaba
// `despachado` y ESO PERDÍA EL MENSAJE. Aquella sentencia se retiró entera el 2026-08-24 con el
// presupuesto (T1.6-5, ADR-0045), así que hoy vuelve a haber un solo escritor del estado terminal: si
// mañana aparece un segundo, el orden «entrega antes de sello» hay que volver a demostrarlo entero.
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
// rompe el FIFO por sesión (REQ-051.18) en silencio y sin dejar rastro. Con `<>`, una fila así es cabeza
// como cualquier otra y SALE: desde T1.6-5 el despachador entrega sin mirar el estado, así que el
// desenlace de un estado imprevisto ya no es «bloquea de forma visible y acotada» sino directamente «se
// entrega». Y ES ESTA CLÁUSULA la que lo hace posible — con el `IN` la fila seguiría siendo invisible.
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

// sqlMarcarDespachada sella la fila entregada. El predicado `estado <> 'despachado'` es el fence de esta
// operación: impide el DOBLE SELLADO, que movería el `despachado_en` hacia adelante y retrasaría su poda.
//
// 🔴 ERA `estado = 'clasificado'` HASTA EL 2026-08-24 (T1.6-5, ADR-0045), y cambiarlo era obligatorio,
// no cosmético. Aquel fence expresaba «sólo se sella lo que estaba listo», y «listo» significaba
// `clasificado`. Al disolverse ese estadio, el despachador entrega filas `nuevo` y `tomado`: con el fence
// viejo el UPDATE habría afectado 0 filas SIEMPRE, la fila jamás habría llegado a `despachado`, el poll
// siguiente la habría vuelto a entregar —duplicando cada mensaje, indefinidamente— y la poda por TTL
// habría quedado otra vez inerte. Todo ello sin un solo error: `MarcarDespachada` trata el 0 como no-op.
const sqlMarcarDespachada = `
UPDATE cola_entrantes
SET estado = ?, despachado_en = ?
WHERE id = ? AND estado <> ?`

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

// MarcarDespachada sella `estado='despachado'` + `despachado_en` sobre una fila que no lo estuviera ya
// (REQ-051.20). Es el sello que hace que la poda por TTL tenga por fin algo que borrar.
//
// 0 FILAS AFECTADAS NO ES ERROR, y esa es la mitad del contrato que hay que respetar: significa que la
// fila ya estaba sellada, o que ya no está. El despachador la releerá en el poll siguiente y volverá a
// decidir con lo que haya en disco, que es lo correcto — la BD manda sobre la foto que él tenía en la mano.
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
// ambos casos sería un valor que nadie puede accionar, y el ruido de una serie más en el heartbeat tiene
// coste. Lo que sí se deja es la traza en Debug: diagnosticable subiendo el nivel en campo, sin estado
// nuevo ni un byte más en el camino caliente.
func (s *Store) MarcarDespachada(ctx context.Context, id int64) error {
	// s.now().Unix(): EPOCH-SEGUNDOS, la misma unidad que mide pruneTTLLocked. Y el reloj INYECTADO
	// (WithClock), nunca time.Now() directo: los tests del TTL adelantan este mismo reloj 25 h, y dos
	// relojes distintos entre el sello y la poda harían que pasaran mintiendo.
	res, err := s.db.ExecContext(ctx, sqlMarcarDespachada,
		app.EstadoDespachado, s.now().Unix(), id, app.EstadoDespachado)
	if err != nil {
		// INV-051.1: solo el id de la fila. Aquí no hay ni session_id que citar —la firma es por id, que
		// es lo que el despachador tiene en la mano— y con él basta para localizarla en la tabla.
		return fmt.Errorf("colaentrantes: sellar fila despachada (id=%d): %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		s.log.Debug("colaentrantes: el sello de despacho no encontró la fila sin sellar (¿la borró el tope, o ya estaba sellada?); se releerá",
			"id", id)
	}
	return nil
}
