// Package colaentrantes implementa la COLA DE ENTRANTES del Edge (Plan 051 Ola 1 · ADR-0038): el buzón
// durable donde el listener (el "mesonero") anota el mensaje recién llegado en milisegundos y se suelta,
// para que el worker-cajero lo clasifique después sin bloquear a whatsmeow.
//
// Es el adapter del puerto app.ColaEntrantes sobre la BD `cola_entrantes.db` (fichero APARTE del edge.db,
// design §2 D-2). Sigue el molde del outbox (internal/adapters/outbox): recibe un *sql.DB YA abierto y YA
// migrado, no lleva DDL inline, y ordena las filas con una secuencia `seq` monotónica generada en Go y
// sembrada de MAX(seq) al arrancar (portable, sin AUTOINCREMENT; el orden sobrevive a reinicios).
//
// CIFRADO POR SESIÓN (ADR-0002/0034, decisión cerrada del Plan 051): cada fila se sella con la DEK de SU
// sesión — `session_id` y `chat_jid` viajan EN CLARO porque son la clave de enrutado y la que elige la
// DEK; `texto_enc` y `meta_enc` van cifrados con AES-256-GCM (envelope). No se usa el paquete cryptostore
// (su decorator es privado y está acoplado a whatsmeow): aquí se usa envelope directo.
//
// 🔴 INV-051.1: el texto del mensaje y su meta NO aparecen NUNCA en un log ni en un mensaje de error, ni
// truncados. Todo error de este paquete se anota solo con session_id, wa_message_id y seq.
package colaentrantes

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-shared/envelope"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Defaults del Store (decisiones cerradas del Plan 051).
const (
	defaultMaxRows  = 50000
	defaultTTLHours = 24
)

// crypterFailureCooldown es cuánto se RECUERDA un fallo de CrypterFor antes de volver a intentarlo.
//
// El porqué: si la DEK de una sesión falta o no se puede leer, CrypterFor falla para TODOS los mensajes
// de esa sesión. Sin memoria del fallo se reinvocaría la custodia una vez POR MENSAJE ENTRANTE —a ritmo
// de socket— y cada intento devolvería error, que el listener escribía como log.Error: una tormenta de
// logs y de trabajo inútil por un fallo que es de la SESIÓN, no del mensaje.
//
// 60 s es el compromiso: suficiente para que una ráfaga entera (que es cuando duele) se resuelva con UNA
// sola consulta a la custodia, y lo bastante corto para que una custodia que vuelve —el Guardián
// reconecta, la DEK se re-inyecta— se recupere sola en menos de un minuto sin reiniciar nada.
const crypterFailureCooldown = 60 * time.Second

// CrypterFor resuelve el sobre (Crypter) de una sesión a partir de su DEK. El llamante NO cachea: el
// caché vive dentro del Store (ver Store.crypters), así CrypterFor se invoca UNA sola vez por sesión viva
// y no en cada INSERT.
type CrypterFor func(sessionID string) (envelope.Crypter, error)

// Store respalda app.ColaEntrantes sobre la BD de la cola.
type Store struct {
	db  *sql.DB
	log sharedlogger.Logger

	// crypterFor resuelve la DEK/sobre de una sesión; se consulta solo en el primer mensaje de la sesión.
	crypterFor CrypterFor

	// crypters cachea el sobre YA construido por session_id. Coste en memoria: la DEK son 32 bytes
	// (envelope.DEKSize) retenidos dentro del AEAD por cada sesión viva — despreciable frente a evitar
	// una resolución de custodia por mensaje. Mutex PROPIO (no s.mu): el caché se toca fuera del candado
	// de escritura para no serializar el sellado con la BD.
	cryptersMu sync.RWMutex
	crypters   map[string]envelope.Crypter

	// crypterFails es el CACHÉ NEGATIVO: el último fallo de CrypterFor por sesión y CUÁNDO ocurrió.
	// Mientras no pase crypterFailureCooldown se devuelve ese error memorizado SIN reinvocar la custodia;
	// pasado el enfriamiento se reintenta UNA vez (y si va bien, la entrada se borra y la sesión se
	// recupera sola). Vive bajo el mismo candado que el caché positivo: son el mismo estado, "qué sé yo
	// hoy del sobre de esta sesión". Cardinalidad acotada por el número de sesiones vivas.
	crypterFails map[string]crypterFailure

	// maxRows es el tope de filas retenidas: al alcanzarlo, Enqueue descarta las de menor seq
	// (drop-oldest) antes de insertar — es el ÚNICO límite que aplica a lo aún no despachado. ttl poda,
	// al encolar, solo las filas YA DESPACHADAS más viejas que ese tiempo (ver pruneTTLLocked).
	maxRows int
	ttl     time.Duration

	// now inyecta el reloj (tests de TTL). Producción usa time.Now.
	now func() time.Time

	// descartadasPorTope acumula cuántas filas se han tirado por drop-oldest desde que arrancó el proceso
	// (INV-051.3: ninguna degradación se cierra con solo un log). Atómico porque se toca bajo s.mu pero se
	// LEE desde fuera. La publicación al heartbeat es de la Ola 4; aquí solo se garantiza que el número
	// exista y sea legible (DescartadasPorTope).
	descartadasPorTope atomic.Int64

	// seq es la secuencia monotónica del orden de la cola, sembrada de MAX(seq) en New. Atómica: el Edge
	// encola desde varios listeners (uno por sesión) en paralelo.
	seq atomic.Int64

	// mu serializa el bloque podar→tope→insertar de Enqueue (correcto aunque el pool no aísle).
	mu sync.Mutex
}

var _ app.ColaEntrantes = (*Store)(nil)

// crypterFailure es un fallo MEMORIZADO de CrypterFor: el error tal cual y el instante en que ocurrió,
// que es lo que mide el enfriamiento.
type crypterFailure struct {
	err error
	at  time.Time
}

// Option configura el Store (reloj de test, etc.).
type Option func(*Store)

// WithClock inyecta el reloj (tests de TTL deterministas).
func WithClock(now func() time.Time) Option {
	return func(s *Store) {
		if now != nil {
			s.now = now
		}
	}
}

// New construye el Store sobre la BD de la cola YA migrada (la migración creó `cola_entrantes`). Siembra
// la secuencia de MAX(seq) para que el orden continúe tras un reinicio.
//
// maxRows<=0 cae al default (50000). ttlHours<=0 cae al default de 24 h — OJO, aquí 0 NO desactiva el TTL
// como en el outbox: se desvía del molde a propósito porque esta cola es un buzón de paso (design §2) y
// sin TTL las filas YA DESPACHADAS se acumularían para siempre; el TTL de 24 h es decisión cerrada del
// Plan 051, no un parámetro que se pueda apagar por descuido. Lo que el TTL NO contiene es lo pendiente
// (nuevo/tomado/clasificado): de eso se encarga maxRows (ver pruneTTLLocked).
func New(ctx context.Context, db *sql.DB, crypterFor CrypterFor, maxRows, ttlHours int, log sharedlogger.Logger, opts ...Option) (*Store, error) {
	if log == nil {
		log = sharedlogger.Default()
	}
	if crypterFor == nil {
		return nil, fmt.Errorf("colaentrantes: CrypterFor es obligatorio (sin sobre no se puede cifrar la fila)")
	}
	if maxRows <= 0 {
		maxRows = defaultMaxRows
	}
	if ttlHours <= 0 {
		ttlHours = defaultTTLHours
	}
	s := &Store{
		db:           db,
		log:          log,
		crypterFor:   crypterFor,
		crypters:     make(map[string]envelope.Crypter),
		crypterFails: make(map[string]crypterFailure),
		maxRows:      maxRows,
		ttl:          time.Duration(ttlHours) * time.Hour,
		now:          time.Now,
	}
	for _, o := range opts {
		o(s)
	}
	var maxSeq sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MAX(seq) FROM cola_entrantes`).Scan(&maxSeq); err != nil {
		return nil, fmt.Errorf("colaentrantes: sembrar la secuencia desde MAX(seq): %w", err)
	}
	s.seq.Store(maxSeq.Int64) // 0 si la tabla está vacía (NullInt64 nulo => 0)
	return s, nil
}

// Enqueue persiste un mensaje entrante con el texto (y la meta) ya sellados con la DEK de su sesión.
// Antes poda por TTL las filas YA DESPACHADAS y, si la cola llegó al tope, descarta las más viejas —
// nunca crece sin límite. Es IDEMPOTENTE por (SessionID, WAMessageID): el duplicado devuelve nil.
//
// Orden deliberado: el sellado (AES sobre unos KB, sin tocar la BD) se hace FUERA de s.mu; dentro del
// candado solo queda podar→contar→insertar. Con SetMaxOpenConns(1) en la BD de la cola la contención del
// candado es el cuello real, y meter cripto ahí dentro la agravaría sin ganar nada.
func (s *Store) Enqueue(ctx context.Context, item app.ColaItem) error {
	crypter, err := s.crypter(item.SessionID)
	if err != nil {
		// INV-051.1: nada del contenido; solo identificadores.
		return fmt.Errorf("colaentrantes: resolver sobre de la sesión (session_id=%s, wa_message_id=%s): %w",
			item.SessionID, item.WAMessageID, err)
	}

	textoEnc, err := crypter.Seal([]byte(item.Texto))
	if err != nil {
		return fmt.Errorf("colaentrantes: sellar texto (session_id=%s, wa_message_id=%s): %w",
			item.SessionID, item.WAMessageID, err)
	}
	// Meta nil => columna NULL. Se elige NO sellar el nil (Seal(nil) cifraría igual y dejaría 28 bytes de
	// relleno indistinguibles de una meta vacía real): NULL es más barato y expresa "no había meta"
	// explícitamente, y el lector distingue el caso sin descifrar.
	var metaEnc []byte
	if item.Meta != nil {
		metaEnc, err = crypter.Seal(item.Meta)
		if err != nil {
			return fmt.Errorf("colaentrantes: sellar meta (session_id=%s, wa_message_id=%s): %w",
				item.SessionID, item.WAMessageID, err)
		}
	}

	estado := item.Estado
	if estado == "" {
		estado = app.EstadoNuevo
	}
	// intent_json vacío => NULL en la columna (el worker aún no clasificó; solo el fastlane nace con él).
	intent := sql.NullString{String: item.IntentJSON, Valid: item.IntentJSON != ""}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().Unix()
	if err := s.pruneTTLLocked(ctx, now); err != nil {
		return err
	}
	if err := s.dropOldestLocked(ctx); err != nil {
		return err
	}

	seq := s.seq.Add(1)
	// IDEMPOTENCIA (contrato de app.ColaEntrantes.Enqueue): INSERT OR IGNORE contra el índice único
	// ux_cola_session_wamid (session_id, wa_message_id). Un INSERT pelado devolvería un error de
	// constraint en un caso PERFECTAMENTE NORMAL —whatsmeow re-emite eventos al reconectar y el handler
	// se puede reintentar—, y el listener lo escupiría como Error en cada reconexión.
	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO cola_entrantes (seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc, meta_enc, intent_json, estado)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		seq, item.SessionID, item.ChatJID, item.WAMessageID, item.TSWhatsApp, textoEnc, metaEnc, intent, estado)
	if err != nil {
		return fmt.Errorf("colaentrantes: encolar entrante (session_id=%s, wa_message_id=%s, seq=%d): %w",
			item.SessionID, item.WAMessageID, seq, err)
	}
	// 0 filas afectadas ⇒ la fila YA estaba (duplicado esperado). NO es un fallo: se devuelve nil, y como
	// mucho se deja constancia en Debug. Jamás un Error: llenaría el log en campo en cada reconexión.
	//
	// El `seq` de esta llamada YA SE CONSUMIÓ y se pierde: la secuencia deja un HUECO. Es aceptable y
	// deliberado —el claim y el despachador ordenan con ORDER BY seq, no exigen contigüidad— y se prefiere
	// a reservar el número dentro del candado tras comprobar la existencia (un SELECT extra por mensaje).
	if n, _ := res.RowsAffected(); n == 0 {
		s.log.Debug("colaentrantes: entrante duplicado, ya estaba en la cola (idempotencia)",
			"session_id", item.SessionID, "wa_message_id", item.WAMessageID)
	}
	return nil
}

// crypter devuelve el sobre de la sesión, resolviéndolo con CrypterFor solo la PRIMERA vez y cacheándolo
// después (doble comprobación bajo el candado de escritura para no resolver dos veces en carrera).
//
// CACHÉ NEGATIVO (ver crypterFailureCooldown): si CrypterFor FALLA, el fallo también se cachea, con su
// instante. Durante el enfriamiento las llamadas siguientes de esa sesión devuelven el error memorizado
// SIN tocar la custodia; pasado el enfriamiento se reintenta una vez.
//
// EL THROTTLE DEL LOG VIVE AQUÍ, y es deliberado: el store es quien conoce la sesión, la causa y la
// ventana de enfriamiento, así que puede gritar el fallo UNA vez por sesión y por ventana. El listener no
// tiene esa información — solo ve un error por mensaje. Para que no lo repita, los errores devueltos
// DESDE el caché negativo se marcan con app.ErrColaFalloRepetido: el listener los cuenta igual pero los
// escribe en Debug en vez de Error (ver whatsmeow/listener.go, enqueueCola).
func (s *Store) crypter(sessionID string) (envelope.Crypter, error) {
	s.cryptersMu.RLock()
	c, ok := s.crypters[sessionID]
	fail, failed := s.crypterFails[sessionID]
	s.cryptersMu.RUnlock()
	if ok {
		return c, nil
	}
	if failed && s.now().Sub(fail.at) < crypterFailureCooldown {
		return nil, fmt.Errorf("%w: %w", app.ErrColaFalloRepetido, fail.err)
	}

	s.cryptersMu.Lock()
	defer s.cryptersMu.Unlock()
	if c, ok := s.crypters[sessionID]; ok {
		return c, nil
	}
	// Segunda comprobación del caché negativo: otra goroutine pudo anotar el fallo (y gritarlo) mientras
	// esperábamos el candado; sin esto, N mensajes en carrera darían N invocaciones y N líneas de log.
	now := s.now()
	if fail, failed := s.crypterFails[sessionID]; failed && now.Sub(fail.at) < crypterFailureCooldown {
		return nil, fmt.Errorf("%w: %w", app.ErrColaFalloRepetido, fail.err)
	}

	c, err := s.crypterFor(sessionID)
	if err == nil && c == nil {
		err = fmt.Errorf("sobre nulo para la sesión (session_id=%s)", sessionID)
	}
	if err != nil {
		s.crypterFails[sessionID] = crypterFailure{err: err, at: now}
		// EL GRITO: una vez por sesión y por ventana de enfriamiento. INV-051.1 — el error de la custodia
		// habla de llaves, nunca del contenido del mensaje.
		s.log.Error("colaentrantes: no se pudo resolver el sobre de la sesión; sus entrantes NO se encolan hasta que la custodia vuelva",
			"session_id", sessionID, "error", err, "enfriamiento", crypterFailureCooldown.String())
		return nil, err
	}
	// La custodia volvió: se olvida el fallo para que el próximo tropiezo vuelva a gritarse.
	delete(s.crypterFails, sessionID)
	s.crypters[sessionID] = c
	return c, nil
}

// DescartadasPorTope devuelve cuántas filas ha tirado el drop-oldest desde que arrancó el proceso
// (INV-051.3). Es un acumulado monotónico, sin PII: solo una cardinalidad. Quien lo publique al
// heartbeat es la Ola 4; aquí solo se garantiza que el número exista y sea legible.
func (s *Store) DescartadasPorTope() int64 { return s.descartadasPorTope.Load() }

// pruneTTLLocked borra las filas YA DESPACHADAS más viejas que el TTL. Debe llamarse bajo s.mu.
//
// 🔴 REQ-051.7 / ADR-0038 §Enmienda 1 — LA PODA JAMÁS TOCA `nuevo`, `tomado` NI `clasificado`, y el
// filtro por estado NO es una optimización que se pueda "simplificar" mañana:
//
//   - El corte por `ts_whatsapp` a secas (como estaba) borraba mensajes JAMÁS DESPACHADOS: basta con el
//     worker-cajero caído 24 h para que la cola se vacíe sola y se pierda exactamente lo que este plan
//     promete no perder. La cola es un buzón durable, no un caché.
//   - `despachado_en` es el ÚNICO sello que demuestra que la fila ya salió (cloudlink/outbox la tienen);
//     por eso se exige NOT NULL además del estado: una fila marcada 'despachado' sin sello es un bug del
//     despachador, y ante la duda no se borra.
//   - El crecimiento de lo NO despachado lo contiene el TOPE DE FILAS (dropOldestLocked), que es el
//     mecanismo correcto: descarta lo más viejo bajo presión real y lo ANOTA en el log, en vez de
//     borrar en silencio por el mero paso del tiempo.
func (s *Store) pruneTTLLocked(ctx context.Context, nowUnix int64) error {
	cutoff := nowUnix - int64(s.ttl.Seconds())
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM cola_entrantes WHERE estado = ? AND despachado_en IS NOT NULL AND despachado_en < ?`,
		app.EstadoDespachado, cutoff)
	if err != nil {
		return fmt.Errorf("colaentrantes: podar TTL: %w", err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		s.log.Info("colaentrantes: filas despachadas podadas por TTL", "expiradas", n, "ttl", s.ttl.String())
	}
	return nil
}

// dropOldestLocked descarta las filas más viejas (menor seq) si la cola alcanzó el tope, dejando sitio
// para una nueva (política drop-oldest, igual que el outbox). Debe llamarse bajo s.mu.
func (s *Store) dropOldestLocked(ctx context.Context) error {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cola_entrantes`).Scan(&count); err != nil {
		return fmt.Errorf("colaentrantes: contar filas: %w", err)
	}
	if count < s.maxRows {
		return nil
	}
	// Descarta las que sobran para dejar hueco a 1 nueva (normalmente 1, pero cubre un tope reducido).
	toDrop := count - s.maxRows + 1
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM cola_entrantes WHERE id IN (SELECT id FROM cola_entrantes ORDER BY seq ASC LIMIT ?)`, toDrop)
	if err != nil {
		return fmt.Errorf("colaentrantes: drop-oldest: %w", err)
	}
	n, _ := res.RowsAffected()
	// INV-051.3: la degradación se CUENTA, no solo se loguea (un log se pierde; el acumulado no).
	total := s.descartadasPorTope.Add(n)
	s.log.Warn("colaentrantes: LLENA, descartando las más viejas (drop-oldest)",
		"descartadas", n, "tope", s.maxRows, "descartadas_acumuladas", total)
	return nil
}
