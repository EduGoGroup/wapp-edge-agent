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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-shared/envelope"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	sqlitedriver "modernc.org/sqlite" // NO es el blank import del driver: se usa su tipo *sqlite.Error (ver sqliteConstraintUnique)
)

// Defaults del Store (decisiones cerradas del Plan 051).
const (
	defaultMaxRows  = 50000
	defaultTTLHours = 24
)

// sqliteConstraintUnique es el código de resultado EXTENDIDO SQLITE_CONSTRAINT_UNIQUE (19 | (8<<8) =
// 2067): «este INSERT chocó contra un índice UNIQUE». Es la firma EXACTA del duplicado que Enqueue
// tiene que tragarse en silencio (el choque contra ux_cola_session_wamid), y solo esa.
//
// El porqué del número desnudo: modernc.org/sqlite expone las constantes en su subpaquete `lib`
// (modernc.org/sqlite/lib), que es la traducción a Go de la biblioteca C entera —importarlo aquí
// arrastraría ese peso al Edge por UN entero—. El driver activa los códigos extendidos al abrir la
// conexión (sqlite3_extended_result_codes(db, 1) en newConn) y `(*sqlite.Error).Code()` devuelve el
// `rc` crudo, sin enmascarar, así que aquí llega el 2067 y no el 19 pelado.
//
// 🔴 NO lo generalices a `Code()&0xff == 19` (SQLITE_CONSTRAINT a secas): eso englobaría también
// NOT NULL (1299), FK (787) y CHECK (275) — justo las violaciones que T2.11 existe para NO tragarse.
// Tampoco se acepta 1555 (SQLITE_CONSTRAINT_PRIMARYKEY): un choque de rowid en esta tabla sería un
// bug real del `id INTEGER PRIMARY KEY` autoasignado, no un mensaje repetido.
const sqliteConstraintUnique = 2067

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

	// descartadasPorTope acumula la PÉRDIDA REAL del drop-oldest desde que arrancó el proceso
	// (INV-051.3: ninguna degradación se cierra con solo un log). Atómico porque se toca bajo s.mu pero se
	// LEE desde fuera. La publicación al heartbeat es de la Ola 4; aquí solo se garantiza que el número
	// exista y sea legible (DescartadasPorTope).
	//
	// 🔴 "PÉRDIDA REAL" EXCLUYE LA CAPA 1: las filas `despachado` YA salieron al cable (cloudlink/outbox
	// las tienen), así que borrarlas es LIMPIEZA, no pérdida. Contarlas aquí —como se hacía— convertía
	// este número en una alarma que se dispara sola: una cola que rota sano por su capa 1 inflaría el
	// acumulado sin que se haya perdido un solo mensaje, y la señal de degradación dejaría de significar
	// nada. El desglose de las tres capas sigue entero en el log de Warn, que es donde sirve para
	// diagnosticar; el acumulado es para ALARMAR, y solo alarma lo que se perdió.
	descartadasPorTope atomic.Int64

	// pendientesDescartadasPorTope acumula, del total anterior, SOLO las filas `nuevo`/`tomado` — la capa
	// 3, el sacrificio de conversaciones enteras. Es el subconjunto que más duele: una `clasificado` al
	// menos llegó a pasar por el cajero, mientras que una pendiente es un mensaje que el cajero NUNCA
	// llegará a leer. Se expone aparte (mismo patrón atómico, ver PendientesDescartadasPorTope) porque
	// "hemos perdido cosas" y "hemos perdido cosas que nadie había mirado siquiera" son dos niveles de
	// gravedad distintos, y desde un acumulado agregado no hay forma de separarlos.
	pendientesDescartadasPorTope atomic.Int64

	// rescatadasPorLease acumula las filas que BarrerLeasesVencidos devolvió a `nuevo` porque su lease
	// venció: el número de mensajes que un cajero se llevó y no cerró. Mismo molde atómico que los dos de
	// arriba, y por la misma razón (INV-051.3: una degradación no se cierra solo con un log).
	//
	// EL RELEVO NO ERA CONTABLE, y ese era el agujero: el único testigo de un lease vencido eran dos líneas
	// de Warn. Un log se rota y se pierde; el acumulado no. Y el worker NO puede llevar esta cuenta por su
	// lado —el barrido lo hace el Store por su cuenta, sobre filas de OTRO proceso que quizá ya murió—, así
	// que si no se cuenta aquí no se cuenta en ninguna parte.
	rescatadasPorLease atomic.Int64

	// cierresDescartadosPorFence acumula los CIERRES (lotes, no filas) que MarcarClasificado tuvo que tirar
	// enteros porque el fence no mordió: el lote dejó de ser de este cajero mientras se clasificaba. Es la
	// otra cara de rescatadasPorLease —una cuenta las filas que se rescataron, la otra el trabajo que se
	// perdió por ello— y se cuentan APARTE porque no son la misma magnitud: un solo cierre descartado puede
	// corresponder a 20 filas rescatadas, y cada inferencia tirada es un coste de LLM que sí se pagó.
	cierresDescartadosPorFence atomic.Int64

	// 🔴 AQUÍ VIVÍA `despachosSinIntentNoAplicados`, RETIRADO EL 2026-08-24 CON SU MÉTODO (Plan 044 · Ola
	// 1.6 · T1.6-5 · ADR-0045). Contaba las veces que `DespacharSinIntent` NO tocaba nada: el despachador
	// agotaba su presupuesto de 4 s y iba a escribir el sobre de omisión, pero el cajero había cerrado la
	// fila un instante antes. Era el borde de la calibración —presupuesto de 4000 ms contra una p95 medida
	// de 3.736 ms—, y por eso no era un número raro sino una carrera que se corría en CADA mensaje lento.
	//
	// Se va entero porque la carrera dejó de existir: no hay presupuesto que agotar. Y la medida que mató
	// aquel diseño explica el número: de 430 inferencias, UNA cupo en la ventana.

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
	// IDEMPOTENCIA (contrato de app.ColaEntrantes.Enqueue) por la vía del ERROR CLASIFICADO, no del
	// `INSERT OR IGNORE` (T2.11). El duplicado sigue siendo un caso PERFECTAMENTE NORMAL —whatsmeow
	// re-emite eventos al reconectar y el handler se puede reintentar— y se sigue devolviendo nil; lo
	// que cambia es que ahora se RECONOCE cuál es.
	//
	// El porqué del cambio: `INSERT OR IGNORE` no ignora solo el choque de unicidad, ignora CUALQUIER
	// violación de restricción de la fila. Hoy no hay otra alcanzable desde onMessage (ninguna columna
	// NOT NULL puede llegar a NULL y Seal devuelve ≥28 B aun con texto vacío), pero el día que alguien
	// añada un CHECK al DDL su violación se degradaría a «duplicado» y el mensaje se PERDERÍA con un
	// log.Debug —silencio absoluto sobre una pérdida de datos—. Con el INSERT pelado, solo el
	// SQLITE_CONSTRAINT_UNIQUE se traga; todo lo demás sube por el error de abajo.
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO cola_entrantes (seq, session_id, chat_jid, wa_message_id, ts_whatsapp, texto_enc, meta_enc, intent_json, estado)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		seq, item.SessionID, item.ChatJID, item.WAMessageID, item.TSWhatsApp, textoEnc, metaEnc, intent, estado)
	if err != nil {
		// Choque contra ux_cola_session_wamid ⇒ la fila YA estaba (duplicado esperado). NO es un fallo:
		// se devuelve nil, y como mucho se deja constancia en Debug. Jamás un Error: llenaría el log en
		// campo en cada reconexión.
		//
		// El `seq` de esta llamada YA SE CONSUMIÓ y se pierde: la secuencia deja un HUECO. Sigue
		// existiendo igual que con el INSERT OR IGNORE, solo que ahora el hueco lo abre el camino del
		// error en vez del de las 0 filas afectadas. Es aceptable y deliberado —el claim y el
		// despachador ordenan con ORDER BY seq, no exigen contigüidad— y se prefiere a reservar el
		// número dentro del candado tras comprobar la existencia (un SELECT extra por mensaje).
		var sqliteErr *sqlitedriver.Error
		if errors.As(err, &sqliteErr) && sqliteErr.Code() == sqliteConstraintUnique {
			s.log.Debug("colaentrantes: entrante duplicado, ya estaba en la cola (idempotencia)",
				"session_id", item.SessionID, "wa_message_id", item.WAMessageID)
			return nil
		}
		// INV-051.1: ni texto ni meta en el error; solo identificadores y el seq consumido.
		return fmt.Errorf("colaentrantes: encolar entrante (session_id=%s, wa_message_id=%s, seq=%d): %w",
			item.SessionID, item.WAMessageID, seq, err)
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

// Forget OLVIDA lo que el Store sabe del sobre de una sesión: borra su entrada del caché positivo Y la del
// caché negativo, bajo el MISMO candado que las escribe (s.cryptersMu). Tras un Forget, el siguiente
// Enqueue/Reclamar de esa sesión vuelve a invocar CrypterFor UNA vez y re-cachea el resultado.
//
// Se llama en el TEARDOWN de la sesión (sessionmgr.stopLive, el mismo punto donde se suelta el listener).
// Es idempotente y no-op si la sesión nunca estuvo cacheada; nunca entra en pánico ni devuelve error,
// porque un teardown no puede fallar por no poder olvidar algo.
//
// 🟢 HOY ESTO ES UNA FUGA DE ~60 B, NO UN FALLO DE CIFRADO, y conviene decirlo para que nadie lo lea como
// más grave —ni menos— de lo que es: cada emparejamiento acuña un `session_id` NUEVO (UUIDv4,
// sessionmgr/pairing.go), así que una sesión re-pareada nunca vuelve a consultar la entrada vieja del
// caché. Lo que quedaba sin Forget era la DEK de una sesión muerta retenida dentro de su AEAD hasta que el
// daemon reiniciara.
//
// 🔴 RECUPERA SU GRAVEDAD el día que se ROTE LA DEK CONSERVANDO EL `session_id` (rotación de llaves sin
// re-emparejar, que es justo lo que traería un KMS/custodia con rotación). Ahí el caché seguiría sellando
// con la DEK VIEJA sin que nada falle en el momento: el síntoma llega después, y es un tag GCM que no
// valida al abrir la fila —el worker no puede clasificar un mensaje que sí se guardó—. Es decir: pérdida
// silenciosa, no un error ruidoso. Si se implementa esa rotación, Forget deja de ser higiene y pasa a ser
// parte del contrato: hay que llamarlo en el mismo sitio donde se rota.
// 🔴 RECEPTOR NIL ⇒ NO-OP, y es el otro extremo de la guarda de sessionmgr.forgetColaCrypter. El llamante
// alcanza este método por una ASERCIÓN DE TIPO sobre `app.ColaEntrantes` (`m.cola.(interface{ Forget(string) })`),
// y una interfaz que envuelve un `(*Store)(nil)` —un typed nil, lo que sale de `var s *Store; return s`—
// pasa esa aserción sin problema. Sin este `if`, el `s.cryptersMu.Lock()` de abajo desreferenciaría nil y
// el teardown moriría en pánico. Es una línea, es idiomático en Go para métodos de limpieza, y encaja con
// lo que este método ya promete tres párrafos más arriba: «nunca entra en pánico».
func (s *Store) Forget(sessionID string) {
	if s == nil {
		return
	}
	s.cryptersMu.Lock()
	defer s.cryptersMu.Unlock()
	// Los dos mapas son el mismo estado ("qué sé yo hoy del sobre de esta sesión", ver crypterFails), así
	// que olvidar a medias dejaría a la sesión arrastrando un fallo memorizado de una encarnación anterior.
	delete(s.crypters, sessionID)
	delete(s.crypterFails, sessionID)
}

// DescartadasPorTope devuelve cuántas filas ha PERDIDO el drop-oldest desde que arrancó el proceso
// (INV-051.3): las `clasificado` y las `nuevo`/`tomado`, NO las `despachado` (esas ya salieron al cable;
// borrarlas es limpieza, ver descartadasPorTope). Es un acumulado monotónico, sin PII: solo una
// cardinalidad. Quien lo publique al heartbeat es la Ola 4; aquí solo se garantiza que exista y sea
// legible.
func (s *Store) DescartadasPorTope() int64 { return s.descartadasPorTope.Load() }

// PendientesDescartadasPorTope devuelve, del total de DescartadasPorTope, cuántas eran `nuevo`/`tomado`:
// mensajes que el worker-cajero NUNCA llegó a leer (capa 3, conversaciones sacrificadas enteras). Mismo
// contrato que el anterior: acumulado monotónico, sin PII.
func (s *Store) PendientesDescartadasPorTope() int64 { return s.pendientesDescartadasPorTope.Load() }

// RescatadasPorLease devuelve cuántas FILAS ha devuelto a `nuevo` el barrido de leases desde que arrancó
// el proceso: la cuenta de mensajes que un cajero reclamó y no cerró a tiempo (proceso muerto, inferencia
// eternizada, relevo). Acumulado monotónico, sin PII: solo una cardinalidad.
//
// Cero es el valor SANO: el barrido no debería encontrar nunca nada. Un número que crece sostenidamente es
// la señal de que el lease se queda corto para el modelo o de que el worker se está cayendo — y es la
// única forma de verlo sin tener el log delante en el momento exacto.
func (s *Store) RescatadasPorLease() int64 { return s.rescatadasPorLease.Load() }

// CierresDescartadosPorFence devuelve cuántos CIERRES DE LOTE se han descartado enteros porque el fence
// (estado `tomado` + mismo `claim_token`) ya no mordía: el lote había dejado de ser de ese cajero. Cuenta
// LOTES, no filas — es el número de inferencias pagadas y tiradas—. Acumulado monotónico, sin PII.
func (s *Store) CierresDescartadosPorFence() int64 { return s.cierresDescartadosPorFence.Load() }

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

// dropOldestLocked descarta filas si la cola alcanzó el tope, dejando sitio para UNA nueva (política
// drop-oldest, igual que el outbox). Debe llamarse bajo s.mu.
//
// 🔴 SACRIFICIO POR CAPAS, Y NO ES UN ADORNO (T2.10): el drop-oldest ORIGINAL borraba por `seq` más
// bajo SIN MIRAR EL ESTADO, y eso podía PARTIR UNA CONVERSACIÓN POR LA MITAD. El claim del cajero
// (Ola 2) reclama TODAS las filas `nuevo` de una misma (session_id, chat_jid) y las CONCATENA en una
// sola inferencia; si el tope se llevó un fragmento intermedio, el LLM lee como continuo un texto al
// que le falta un trozo. No hay error, no hay log, no hay nada que revisar después: es una MENTIRA
// SEMÁNTICA, la peor clase de fallo que puede tener esta cola.
//
// De ahí las tres capas, de menos a más doloroso, aplicadas EN ORDEN hasta que quepa la fila nueva:
//
//  1. `despachado` — ya salieron al cable (cloudlink/outbox las tienen). Son basura pura: se borran
//     de una en una, las de menor seq.
//  2. `clasificado` — filas HEREDADAS de un binario anterior a T1.6-5 (el estadio se disolvió con el
//     push, ADR-0045). Traen su intent y el claim NUNCA las vuelve a concatenar, así que borrarlas es
//     PÉRDIDA (ese mensaje no se despachará) pero NO abre hueco. Se borran las de menor seq.
//     ⚠️ ESTA CAPA SE VACÍA SOLA a medida que las colas de campo se drenan; el día que no queden filas
//     `clasificado` en ningún disco, se salta sin borrar nada y la presión pasa directa a la capa 3.
//     No se retira porque mientras exista UNA sola de esas filas, sigue siendo el sacrificio barato.
//  3. `nuevo`/`tomado` — aquí sí hay riesgo de hueco, así que NO se borran filas sueltas: se borra la
//     CONVERSACIÓN COMPLETA más antigua (la (session_id, chat_jid) con el MIN(seq) más bajo entre sus
//     filas pendientes) de golpe, y se repite con la siguiente mientras siga faltando sitio. Invariante
//     que compra: una conversación o está ENTERA o no está.
//
// ⚠️ LA CAPA 3 SE PASA DE LARGO A PROPÓSITO: si sobraban 2 huecos y la conversación más vieja tiene 8
// fragmentos, se van los 8. Es deliberado —se prefiere sacrificar de más antes que dejar un hueco— y
// por eso el bucle PARA en cuanto hay sitio, sin intentar afinar el corte.
//
// GUARDARRAÍL: si una vuelta de la capa 3 no borra NADA (no queda conversación pendiente que borrar,
// p. ej. porque el resto de la tabla está en un estado que estas capas no cubren), se sale con lo
// hecho en vez de girar para siempre.
func (s *Store) dropOldestLocked(ctx context.Context) error {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cola_entrantes`).Scan(&count); err != nil {
		return fmt.Errorf("colaentrantes: contar filas: %w", err)
	}
	if count < s.maxRows {
		return nil
	}
	// Cuántos huecos hay que abrir para que quepa 1 fila nueva (normalmente 1, pero cubre un tope
	// reducido o una cola que ya venía pasada). Cada capa lo va bajando.
	faltan := count - s.maxRows + 1

	// Capa 1: las ya despachadas.
	despachadas, err := s.dropPorEstadoLocked(ctx, app.EstadoDespachado, faltan)
	if err != nil {
		return err
	}
	faltan -= int(despachadas)

	// Capa 2: las HEREDADAS en `clasificado` (pérdida, pero nunca hueco). Ver el doc de arriba.
	var clasificadas int64
	if faltan > 0 {
		clasificadas, err = s.dropPorEstadoLocked(ctx, app.EstadoClasificado, faltan)
		if err != nil {
			return err
		}
		faltan -= int(clasificadas)
	}

	// Capa 3: conversaciones pendientes ENTERAS, de la más vieja hacia adelante.
	var pendientes, conversaciones int64
	for faltan > 0 {
		n, err := s.dropConversacionPendienteMasViejaLocked(ctx)
		if err != nil {
			return err
		}
		if n == 0 {
			break // guardarraíl: no queda conversación pendiente; nos vamos con lo hecho.
		}
		conversaciones++
		pendientes += n
		faltan -= int(n) // puede quedar NEGATIVO: es el "pasarse de largo" documentado arriba.
	}

	borradas := despachadas + clasificadas + pendientes
	// PÉRDIDA ≠ BORRADO: lo que se acumula es solo lo que NO había salido al cable (ver
	// descartadasPorTope). Las `despachado` de la capa 1 se borran y se loguean, pero no se cuentan como
	// pérdida: contarlas hacía que una cola rotando sano alarmara igual que una que está tirando mensajes.
	perdidas := clasificadas + pendientes
	// 🔴 EL WARN Y EL CONTADOR SOLO CUANDO DE VERDAD SE BORRÓ ALGO. Antes se ejecutaban también con
	// `borradas == 0` —el camino del guardarraíl de arriba—, y eso era lo peor de dos mundos: una línea
	// de Warn POR MENSAJE ENTRANTE (a ritmo de socket, la misma tormenta que crypterFailureCooldown
	// combate 380 líneas más arriba) diciendo «descartando» con `descartadas=0`, que no informa de nada.
	if borradas > 0 {
		// INV-051.3: la degradación se CUENTA, no solo se loguea (un log se pierde; el acumulado no).
		// Los Add pueden sumar 0 (una poda que solo tocó la capa 1): eso NO es el ruido que se quitó
		// arriba —aquí sí hubo poda real, y el acumulado sin cambios es justo lo que hay que reportar—,
		// y Add(0) devuelve el valor vigente, que es lo que el log necesita.
		total := s.descartadasPorTope.Add(perdidas)
		totalPendientes := s.pendientesDescartadasPorTope.Add(pendientes)
		// INV-051.1: ni texto ni meta; solo cardinalidades y el tope. El desglose de las TRES capas se
		// mantiene entero —`despachadas` incluida, aunque no cuente como pérdida— porque para diagnosticar
		// en campo importa saber por qué capa se fue la poda. `conversaciones` es la señal de que se llegó
		// a la capa 3, la única que duele de verdad (mensajes que el cajero nunca leerá).
		s.log.Warn("colaentrantes: LLENA, descartando por capas (drop-oldest sin partir conversaciones)",
			"borradas", borradas, "perdidas", perdidas,
			"despachadas", despachadas, "clasificadas", clasificadas,
			"pendientes", pendientes, "conversaciones", conversaciones,
			"tope", s.maxRows, "descartadas_acumuladas", total, "pendientes_acumuladas", totalPendientes)
		return nil
	}
	// TOPE ALCANZADO Y NADA QUE SE PUEDA SACRIFICAR: sí es una anomalía —la cola está llena de filas que
	// las tres capas no cubren, o de pendientes que no se pueden borrar sin partir una conversación— y por
	// eso NO se calla del todo; pero se deja en Debug, que es el nivel que este mismo fichero ya usa para
	// el otro suceso normal-pero-frecuente del camino caliente (el duplicado idempotente de Enqueue).
	//
	// Debug y no un throttle al estilo del caché negativo: el throttle exigiría memoria + reloj DENTRO de
	// s.mu (el candado que serializa todo Enqueue) para un suceso que ya tiene testigo durable —la tabla
	// por encima de `tope`, visible en cualquier inspección—; Debug lo hace diagnosticable en campo
	// (subiendo el nivel un rato) sin estado nuevo ni un byte más en el camino caliente de producción.
	s.log.Debug("colaentrantes: en el tope pero NINGUNA capa encontró qué descartar; la cola se pasa del tope (mal menor frente a colgar el listener)",
		"filas", count, "tope", s.maxRows, "faltaban", faltan)
	return nil
}

// dropPorEstadoLocked borra hasta `limite` filas del estado dado, las de menor seq (capas 1 y 2 del
// sacrificio). Debe llamarse bajo s.mu. Devuelve cuántas borró.
//
// La forma `DELETE … WHERE id IN (SELECT id … ORDER BY seq ASC LIMIT ?)` es OBLIGATORIA, no un rodeo:
// un LIMIT directo en el DELETE es sintaxis que este driver (modernc.org/sqlite, compilado sin
// SQLITE_ENABLE_UPDATE_DELETE_LIMIT) rechaza con error de sintaxis.
func (s *Store) dropPorEstadoLocked(ctx context.Context, estado string, limite int) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM cola_entrantes
		   WHERE id IN (SELECT id FROM cola_entrantes WHERE estado = ? ORDER BY seq ASC LIMIT ?)`,
		estado, limite)
	if err != nil {
		return 0, fmt.Errorf("colaentrantes: drop-oldest de filas en estado %q: %w", estado, err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// dropConversacionPendienteMasViejaLocked borra DE GOLPE todas las filas `nuevo`/`tomado` de la
// conversación (session_id, chat_jid) más antigua —la del MIN(seq) más bajo— y devuelve cuántas borró.
// Debe llamarse bajo s.mu. Devuelve 0, nil cuando ya no queda ninguna conversación pendiente: ese 0 es
// la señal de parada del guardarraíl del llamante, NO un error.
//
// Se elige por MIN(seq) y no por el seq de cada fila porque la unidad de sacrificio aquí es la
// CONVERSACIÓN ENTERA: lo que se ordena es "qué conversación empezó antes", no "qué fila es más vieja".
// Y se borran `nuevo` Y `tomado` juntos porque un `tomado` es un `nuevo` con lease vivo: si el lease
// vence, el barrido lo devuelve a `nuevo` y volvería a ser candidato del claim —dejarlo atrás
// reintroduciría exactamente el hueco que esta capa existe para evitar—.
//
// 🔴 UNA SOLA SENTENCIA, Y NO ES ESTILO: antes eran DOS (un SELECT que elegía la conversación y un
// DELETE que la borraba). Entre ambas cabe OTRO PROCESO —el worker-cajero corre fuera de este binario,
// y la cola es un .db aparte precisamente para eso (design §2 D-2)— pasando una fila de `tomado` a
// `clasificado`: esa fila ya no encajaría en el `estado IN (nuevo, tomado)` del DELETE, sobreviviría al
// borrado de sus hermanas y dejaría EXACTAMENTE el hueco que esta capa entera existe para impedir. Ni
// s.mu ni SetMaxOpenConns(1) cubren eso: son intra-proceso. Colapsadas en un solo DELETE, la elección y
// el borrado ocurren dentro de la misma sentencia y bajo el mismo lock de escritura del motor, así que
// no queda ventana en medio.
//
// Los ROW VALUES `(session_id, chat_jid) IN (SELECT …)` están VERIFICADOS EJECUTANDO contra este driver
// (modernc.org/sqlite v1.53.0 ⇒ SQLite 3.53.2; los row values existen desde 3.15), no supuestos. El
// LIMIT va DENTRO de la subconsulta, que es lo que el driver acepta: en el DELETE directo lo rechaza por
// sintaxis, igual que en dropPorEstadoLocked (compilado sin SQLITE_ENABLE_UPDATE_DELETE_LIMIT).
func (s *Store) dropConversacionPendienteMasViejaLocked(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM cola_entrantes
		   WHERE estado IN (?, ?)
		     AND (session_id, chat_jid) IN (
		           SELECT session_id, chat_jid FROM cola_entrantes
		             WHERE estado IN (?, ?)
		             GROUP BY session_id, chat_jid
		             ORDER BY MIN(seq) ASC
		             LIMIT 1)`,
		app.EstadoNuevo, app.EstadoTomado, app.EstadoNuevo, app.EstadoTomado)
	if err != nil {
		// INV-051.1: ni texto ni meta en el error. Aquí ya no hay ni identificadores que citar —la
		// conversación la elige el propio SQL y nunca sube a Go—, así que el error va desnudo.
		return 0, fmt.Errorf("colaentrantes: descartar la conversación pendiente más vieja: %w", err)
	}
	// 0 filas ⇒ no quedaba conversación pendiente (antes lo decía el sql.ErrNoRows del SELECT). Es la
	// señal de parada del guardarraíl del llamante, no un error.
	n, _ := res.RowsAffected()
	return n, nil
}
