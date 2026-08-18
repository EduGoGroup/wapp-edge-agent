// Command colaseed inyecta mensajes SINTÉTICOS directamente en la COLA DE ENTRANTES del Edge
// (<data_dir>/cola_entrantes.db, Plan 051), sin pasar por WhatsApp.
//
// ─────────────────────────────────────────────────────────────────────────────
// POR QUÉ EXISTE (y por qué no es un lujo)
// ─────────────────────────────────────────────────────────────────────────────
// wApp habla con WhatsApp por un cliente NO oficial (whatsmeow). Generar ráfagas de cientos de mensajes
// reales para probar la cola es, desde fuera, indistinguible de spam: la sanción es el BLOQUEO DEL NÚMERO
// DE TRABAJO. Las pruebas de campo anteriores ya metieron 4.301 mensajes en un día con picos de 145/min;
// no se repite. Esta herramienta produce exactamente la misma carga AGUAS ABAJO —las filas que el cajero
// reclama y el despachador drena— sin tocar el socket.
//
// Y desbloquea algo que en campo es directamente imposible: fabricar N CONVERSACIONES DISTINTAS
// (`chat_jid` distintos), que es lo que hace falta para ejercitar el semáforo del cajero y el orden con
// conversaciones cruzadas. Conseguir eso de verdad exigiría N interlocutores humanos.
//
// ─────────────────────────────────────────────────────────────────────────────
// POR QUÉ ES UN BINARIO APARTE Y NO UN SUBCOMANDO DE `agent`
// ─────────────────────────────────────────────────────────────────────────────
// Porque ESCRIBE EN LA COLA. Un `agent colaseed` viajaría dentro del binario de producción instalado en
// el equipo del cliente y estaría a un typo de distancia de inyectar tráfico falso en una instalación
// viva. Aquí no: `CMDS := agent wapp-ctl` en el Makefile, así que este `main` no entra en ningún dist/ ni
// en ningún paquete. Se compila a mano (`go build ./cmd/colaseed`) en la máquina donde se va a medir.
//
// ─────────────────────────────────────────────────────────────────────────────
// CÓMO ESCRIBE (y por qué reusa el wiring de producción en vez de hablar SQL)
// ─────────────────────────────────────────────────────────────────────────────
// Reusa las MISMAS piezas que los dos procesos que ya escriben en ese fichero:
//
//   - `db.OpenCola(ctx, layout.ColaDB())` — la ÚNICA apertura que aplica el perfil de escritura de la cola
//     (synchronous=NORMAL, wal_autocheckpoint de 16 MiB). Los pragmas son POR-CONEXIÓN y este proceso es un
//     TERCER ESCRITOR sobre el mismo fichero que `agent serve` y `agent cajero`: abrirlo con el perfil
//     conservador metería fsync por commit y checkpoints en mitad del tráfico ajeno, que es el mecanismo
//     medido detrás de los picos del p99 del handler (PC-11). O sea: la herramienta de medir alteraría
//     justo lo que se mide.
//   - `wiring.BuildCola(...)` — el mismo constructor del daemon y del cajero, con la misma resolución de
//     DEK por sesión. Es lo ÚNICO que garantiza que estas filas se sellan con exactamente la llave con la
//     que el cajero las va a abrir. Un `INSERT` a mano con cifrado propio sería un segundo criterio de
//     custodia esperando a divergir, y el síntoma sería un fallo de descifrado que parecería un bug del
//     cajero (INV-051.1: ni el texto ni la meta salen jamás a un log, así que diagnosticarlo cuesta caro).
//
// DE DÓNDE SALE LA DEK, y por qué esto funciona en el VPS: de la MISMA custodia local que usa el cajero,
// `keycustody.NewFileCustody(<data_dir>/keys/<session_id>.key)`. En Linux eso es Secret Service con
// degradación a fichero 0600 cuando no hay D-Bus (o sea, siempre en un servidor headless: es el camino que
// el cajero ya recorre hoy en el VPS). En macOS es el Keychain. La única condición es la que ya tiene el
// cajero: correr como EL MISMO USUARIO dueño del data_dir. La DEK no sale del equipo ni se loguea
// (ADR-0007).
//
// ─────────────────────────────────────────────────────────────────────────────
// LAS FILAS SON INDISTINGUIBLES DE LAS REALES AGUAS ABAJO
// ─────────────────────────────────────────────────────────────────────────────
// Nacen en `estado = nuevo` con `intent_json` NULL, que es como nace un entrante que el cajero SÍ tiene que
// clasificar. No se escribe ninguna marca de omisión: el fastlane, el filtro de grupo y el interruptor de
// la feature son decisiones del listener sobre un mensaje real, y falsificarlas aquí produciría filas que
// el cajero nunca reclama — justo lo contrario de lo que la herramienta existe para provocar.
//
// Lo que SÍ las distingue —a propósito— es el `chat_jid`: `colaseed-<lote>-NNNN@s.whatsapp.net`. No puede
// colisionar con un JID real (ningún número de teléfono empieza por letras), grita «esto es de prueba» a
// cualquiera que mire la tabla, y se borra entero con un LIKE. La herramienta imprime el DELETE al acabar.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/wiring"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// sufijoJID es el dominio de un JID de usuario de WhatsApp (chat 1:1). Se conserva para que la fila tenga
// exactamente la FORMA de una real: hoy nada aguas abajo parsea el `chat_jid` (el despachador lo copia tal
// cual a domain.InboundEvent.Chat), pero el día que alguien lo parsee, una fila sintética no debe ser la
// que lo descubra.
const sufijoJID = "@s.whatsapp.net"

// frasesDePedido es el corpus del texto sintético. NO es lorem ipsum, y esa es la diferencia entre medir
// la cola y medir otra cosa: el texto acaba en el prompt del clasificador local (qwen3), y la duración de
// una inferencia depende de lo que se le da a inferir. Con basura, la p50/p95 que se mida no será la que
// el Edge ve en campo.
//
// Son frases de pedido del negocio real (comida para llevar), cortas como las de WhatsApp, con acentos y
// sin puntuación cuidada — igual que las escriben los clientes. La mezcla es deliberada: pedidos con
// producto y cantidad, preguntas de catálogo, logística y cierre de comanda, para que el reparto de
// intenciones se parezca al de una jornada y no todo caiga en la misma rama del contrato.
var frasesDePedido = []string{
	"hola buenas quiero 2 hamburguesas con queso y una coca cola",
	"me manda por favor una pizza margarita grande",
	"cuanto cuesta el combo familiar?",
	"buenas tardes, estan abiertos ahora?",
	"quiero hacer un pedido para llevar",
	"me puede pasar el menu del dia porfa",
	"necesito 3 empanadas de queso y un jugo de naranja",
	"agregame una arepa reina pepiada al pedido",
	"por favor sin cebolla en la hamburguesa",
	"cuanto tardan en llegar a la zona?",
	"aceptan pago movil o solo efectivo?",
	"quiero dos cafes con leche para llevar",
	"me confirma si tienen delivery hasta las 10",
	"listo eso seria todo, cuanto es el total?",
	"disculpe quiero cancelar el pedido que hice",
	"tienen mesa para 4 personas esta noche?",
}

// pasoDeFrase es el salto con el que cada conversación recorre `frasesDePedido`. Es coprimo con las 16
// frases (7 y 16 no comparten divisores), así que dos conversaciones distintas no se dicen lo mismo a la
// vez y cada una recorre el corpus ENTERO antes de repetir. Con un salto de 1, las N conversaciones
// arrancarían todas con la misma frase y el clasificador vería N copias del mismo prompt — que es
// exactamente el caso que MEJOR se comporta y el que menos se parece a la realidad.
const pasoDeFrase = 7

// opciones son los parámetros de una siembra. Se agrupan en un struct (en vez de pasarlos sueltos) porque
// `sembrar` es el punto de entrada que prueba el test, y así el test declara el caso de una pieza.
type opciones struct {
	// dataDir es el data_dir del Edge: de él salen la ruta de la cola (<data_dir>/cola_entrantes.db) y la
	// de la DEK (<data_dir>/keys/<session_id>.key). NO tiene default a propósito: escribir en la cola de
	// una instalación viva por heredar un default es justo el accidente que esta herramienta no debe tener.
	dataDir string
	// sessionID es la sesión a la que se atribuyen los mensajes. Tiene que ser una sesión REAL y con DEK
	// custodiada: es su llave la que sella las filas, y sin ella no hay nada que insertar.
	sessionID string
	// conversaciones es cuántos `chat_jid` distintos se fabrican.
	conversaciones int
	// mensajes es cuántas filas se encolan POR conversación.
	mensajes int
	// pausa es el hueco entre inserciones. 0 = ráfaga máxima. Sirve para reproducir un ritmo de campo
	// (~45 msg/min por caja) en vez de un pico que la cola nunca ve.
	pausa time.Duration
	// prefijoTexto se antepone al texto de cada mensaje. Vacío por defecto: lo que se le da al
	// clasificador debe parecerse a lo que recibe el negocio, y un marcador delante lo desvía.
	prefijoTexto string
	// prefijoJID es la primera parte del `chat_jid` sintético. Es lo que hace las filas reconocibles y
	// borrables de un LIKE.
	prefijoJID string
	// lote identifica ESTA corrida dentro del prefijo, para que dos siembras seguidas no compartan
	// conversaciones ni choquen en el índice único (session_id, wa_message_id).
	lote string
	// intercalar decide el ORDEN de inserción: true (default) alterna entre conversaciones en cada vuelta,
	// de modo que los `seq` quedan cruzados —que es el escenario que el semáforo y el orden por
	// conversación tienen que sobrevivir—; false encola cada conversación entera antes de pasar a la
	// siguiente, que es el caso fácil.
	intercalar bool
	// maxFilas es el tope de la cola que se le pasa al Store (drop-oldest). Se expone porque el descarte
	// por tope es SILENCIOSO para el llamante: si aquí se usara un tope distinto del que tiene el daemon
	// en campo, la siembra podría tirar filas que el operador cree haber insertado.
	maxFilas int
}

// resumen es lo que la siembra reporta, y NO es decorativo: `encoladas` cuenta las llamadas a Enqueue que
// devolvieron nil, y `filas` cuenta lo que DE VERDAD hay en la tabla. No tienen por qué coincidir —Enqueue
// se traga los duplicados en silencio (idempotencia por (session_id, wa_message_id)) y el drop-oldest puede
// descartar por tope—, así que el segundo número es una VERIFICACIÓN, no un adorno. Si difieren, la
// herramienta lo dice en vez de mentir con el primero.
type resumen struct {
	encoladas      int
	filas          int
	conversaciones int
	seqMin         int64
	seqMax         int64
	patron         string
}

func main() {
	o := opciones{}
	flag.StringVar(&o.dataDir, "data-dir", "", "OBLIGATORIO: data_dir del Edge (contiene cola_entrantes.db y keys/)")
	flag.StringVar(&o.sessionID, "session-id", "", "OBLIGATORIO: session_id (UUID) al que se atribuyen los mensajes; su DEK sella las filas")
	flag.IntVar(&o.conversaciones, "conversaciones", 1, "cuántos chat_jid distintos fabricar")
	flag.IntVar(&o.mensajes, "mensajes", 1, "cuántos mensajes por conversación")
	flag.DurationVar(&o.pausa, "pausa", 0, "pausa entre inserciones (p.ej. 1300ms); 0 = ráfaga")
	flag.StringVar(&o.prefijoTexto, "prefijo-texto", "", "prefijo que se antepone al texto (vacío = frases de pedido puras)")
	flag.StringVar(&o.prefijoJID, "prefijo-jid", "colaseed", "prefijo del chat_jid sintético: <prefijo>-<lote>-NNNN@s.whatsapp.net")
	flag.StringVar(&o.lote, "lote", "", "identificador de esta corrida (default: 8 hex aleatorios)")
	flag.BoolVar(&o.intercalar, "intercalar", true, "alternar entre conversaciones al insertar (seq cruzados); false = una conversación entera cada vez")
	flag.IntVar(&o.maxFilas, "max-filas", config.DefaultColaMaxRows, "tope de filas de la cola (drop-oldest); debe coincidir con el del daemon en campo")
	flag.Parse()

	if o.lote == "" {
		l, err := loteAleatorio()
		if err != nil {
			fmt.Fprintf(os.Stderr, "colaseed: no se pudo generar el identificador de lote: %v\n", err)
			os.Exit(1)
		}
		o.lote = l
	}

	// SIGINT/SIGTERM cancelan la siembra: con `-pausa` una corrida puede durar minutos y hay que poder
	// pararla sin dejar el proceso escribiendo. Lo ya insertado se queda (es durable, como debe ser) y el
	// resumen del error dice hasta dónde llegó.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// El log va a STDERR y no a stdout: stdout es del resumen, para que se pueda canalizar a un fichero o a
	// otro proceso sin que lo ensucien las líneas de arranque de BuildCola. Tampoco se usa
	// internal/infra/logger: ese respeta WAPP_LOG_FILE y escribiría las trazas de una herramienta de prueba
	// dentro del edge.log de producción.
	log := sharedlogger.New(sharedlogger.WithWriter(os.Stderr))

	// La custodia es la MISMA factory que usa `agent cajero` (cmd/agent/cajero.go): un único punto de
	// verdad sobre dónde vive la DEK. En Linux headless (el VPS) esto degrada al fichero 0600, que es de
	// donde el cajero la lee hoy.
	custodyFor := func(p string) app.KeyCustody { return keycustody.NewFileCustody(p) }

	res, err := sembrar(ctx, o, custodyFor, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "colaseed: %v\n", err)
		os.Exit(1)
	}
	imprimirResumen(os.Stdout, o, res)
}

// sembrar es TODO el trabajo, y está separado de main para que el test lo pueda ejecutar con una custodia
// de mentira (así la suite no toca el Keychain del macOS de nadie ni exige un D-Bus en el CI).
func sembrar(ctx context.Context, o opciones, custodyFor func(string) app.KeyCustody, log sharedlogger.Logger) (resumen, error) {
	if err := o.validar(); err != nil {
		return resumen{}, err
	}

	layout := sessionmgr.NewLayout(o.dataDir)

	// La DEK se comprueba ANTES de abrir nada. Sin este chequeo, la primera fila fallaría al sellar y —por
	// el caché negativo del Store, que recuerda el fallo 60 s— TODAS las siguientes devolverían el mismo
	// error sin volver a consultar la custodia: N mensajes, N errores, y el operador leyendo el último en
	// vez de la causa. Aquí falla una vez, temprano, y nombrando la ruta que falta.
	dekPath, err := layout.DEKPath(o.sessionID)
	if err != nil {
		return resumen{}, fmt.Errorf("session_id inválido: %w", err)
	}
	if !custodyFor(dekPath).Exists() {
		return resumen{}, fmt.Errorf("no hay DEK custodiada para la sesión %s (se esperaba en %s): "+
			"¿es el data_dir correcto y se está corriendo como el usuario dueño del Edge?", o.sessionID, dekPath)
	}

	// db.OpenCola y no db.Open: ver la cabecera del fichero (tercer escritor, pragmas por-conexión).
	colaDB, err := db.OpenCola(ctx, layout.ColaDB())
	if err != nil {
		return resumen{}, fmt.Errorf("abrir la BD de la cola (%s): %w", layout.ColaDB(), err)
	}
	defer func() { _ = colaDB.Close() }()

	// Migrar es idempotente y es lo que hacen los otros dos procesos (daemon y cajero) al abrir: da igual
	// quién arranque primero. Sirve además para que la herramienta funcione contra un data_dir recién
	// creado (el caso del test), donde el daemon todavía no ha corrido.
	if err := db.MigrateCola(ctx, colaDB); err != nil {
		return resumen{}, fmt.Errorf("migrar la BD de la cola: %w", err)
	}

	cfg := config.Config{ColaMaxRows: o.maxFilas, ColaTTLHours: config.DefaultColaTTLHours}
	cola := wiring.BuildCola(ctx, cfg, colaDB, layout, custodyFor, log)
	if cola == nil {
		return resumen{}, errors.New("la cola de entrantes no se pudo construir (ver el error anterior en stderr)")
	}

	encoladas := 0
	insertar := func(conv, msg int) error {
		item, err := o.item(conv, msg)
		if err != nil {
			return err
		}
		if err := cola.Enqueue(ctx, item); err != nil {
			// El error de Enqueue ya viene sin contenido de negocio (INV-051.1) — solo identificadores.
			return fmt.Errorf("encolar (conversación %d, mensaje %d, ya encoladas %d): %w",
				conv+1, msg+1, encoladas, err)
		}
		encoladas++
		if o.pausa <= 0 {
			return nil
		}
		t := time.NewTimer(o.pausa)
		defer t.Stop()
		select {
		case <-ctx.Done():
			// Ctrl-C a mitad de una siembra larga. Lo ya insertado SE QUEDA —es durable, y borrarlo sería
			// peor— así que el error dice cuánto hay para que el operador sepa qué está limpiando.
			return fmt.Errorf("siembra interrumpida tras %d filas (se quedan en la cola; ver el DELETE de "+
				"limpieza con chat_jid LIKE '%s'): %w", encoladas, o.patronLike(), ctx.Err())
		case <-t.C:
			return nil
		}
	}

	// EL ORDEN DE LOS DOS BUCLES ES EL PARÁMETRO, no un detalle: con `intercalar` el índice de mensaje va
	// FUERA, así que el `seq` global alterna entre conversaciones y el cajero se encuentra con lo que se
	// quiere probar (varias conversaciones vivas compitiendo por un semáforo de un hueco).
	if o.intercalar {
		for m := 0; m < o.mensajes; m++ {
			for c := 0; c < o.conversaciones; c++ {
				if err := insertar(c, m); err != nil {
					return resumen{}, err
				}
			}
		}
	} else {
		for c := 0; c < o.conversaciones; c++ {
			for m := 0; m < o.mensajes; m++ {
				if err := insertar(c, m); err != nil {
					return resumen{}, err
				}
			}
		}
	}

	res, err := verificar(ctx, colaDB, o)
	if err != nil {
		return resumen{}, err
	}
	res.encoladas = encoladas
	return res, nil
}

// verificar RELEE de la tabla lo que la siembra dejó. Existe porque `Enqueue` devuelve nil en dos casos que
// no son «se insertó una fila»: el duplicado (idempotencia) y —río arriba— el drop-oldest, que puede haber
// descartado filas viejas para hacer sitio. Contar llamadas exitosas y llamarlo «filas insertadas» sería
// exactamente el tipo de resumen que miente sin fallar.
func verificar(ctx context.Context, colaDB *sql.DB, o opciones) (resumen, error) {
	res := resumen{patron: o.patronLike()}
	var minSeq, maxSeq sql.NullInt64
	err := colaDB.QueryRowContext(ctx,
		`SELECT COUNT(*), COUNT(DISTINCT chat_jid), MIN(seq), MAX(seq)
		   FROM cola_entrantes
		  WHERE session_id = ? AND chat_jid LIKE ?`,
		o.sessionID, res.patron).Scan(&res.filas, &res.conversaciones, &minSeq, &maxSeq)
	if err != nil {
		return resumen{}, fmt.Errorf("verificar lo sembrado: %w", err)
	}
	res.seqMin, res.seqMax = minSeq.Int64, maxSeq.Int64
	return res, nil
}

// item construye la fila en CLARO. El sellado con la DEK lo hace el adaptador (colaentrantes.Store), que es
// donde tiene que estar: aquí no se cifra nada a mano.
func (o opciones) item(conv, msg int) (app.ColaItem, error) {
	chatJID := o.chatJID(conv)
	meta, err := json.Marshal(app.ColaMeta{
		// En un chat 1:1 el remitente ES el chat. Se reusa app.ColaMeta —el tipo del PUERTO, el mismo que
		// abre el despachador— en vez de escribir el JSON a mano: lo que ata a los dos extremos son las
		// claves, y una copia local acabaría divergiendo en silencio (ver internal/app/colasobre.go).
		Sender:         chatJID,
		AddressingMode: "pn",
		PushName:       fmt.Sprintf("Cliente de prueba %02d", conv+1),
		Type:           "text",
		IsGroup:        false,
	})
	if err != nil {
		return app.ColaItem{}, fmt.Errorf("serializar el metadato sintético: %w", err)
	}
	return app.ColaItem{
		SessionID:   o.sessionID,
		ChatJID:     chatJID,
		WAMessageID: fmt.Sprintf("COLASEED-%s-C%04d-M%04d", o.lote, conv+1, msg+1),
		// Epoch-SEGUNDOS y hora de AHORA, como el listener: es el sello con el que la cola ordena y poda.
		// Una hora vieja no descartaría la fila (la ventana ADR-0037 la evalúa el listener, río arriba de
		// aquí), pero sí la dejaría a tiro de la poda por TTL en cuanto se despachara.
		TSWhatsApp: time.Now().Unix(),
		Texto:      o.texto(conv, msg),
		Meta:       meta,
		// 🔴 NACE `nuevo` Y CON intent_json NULL. Es lo que hace que el cajero TENGA que clasificarla, que
		// es el trabajo que esta herramienta existe para provocar. Marcarla `clasificado` (con una marca de
		// omisión, como hace el listener para el fastlane o los grupos) daría filas que el cajero no
		// reclama nunca: la carga desaparecería sin que nada fallara.
		Estado: app.EstadoNuevo,
	}, nil
}

// chatJID fabrica el JID sintético de una conversación.
func (o opciones) chatJID(conv int) string {
	return fmt.Sprintf("%s-%s-%04d%s", o.prefijoJID, o.lote, conv+1, sufijoJID)
}

// patronLike es el patrón SQL que selecciona TODO lo de esta corrida — el que se imprime en el DELETE del
// resumen. Que exista un patrón así es el punto: una siembra abandonada se limpia con una línea.
func (o opciones) patronLike() string {
	return fmt.Sprintf("%s-%s-%%", o.prefijoJID, o.lote)
}

// texto elige la frase del mensaje (ver `pasoDeFrase`) y le antepone el prefijo si lo hay.
func (o opciones) texto(conv, msg int) string {
	frase := frasesDePedido[(conv*pasoDeFrase+msg)%len(frasesDePedido)]
	if o.prefijoTexto == "" {
		return frase
	}
	return o.prefijoTexto + " " + frase
}

// validar rechaza lo que produciría una siembra silenciosamente inútil o una limpieza imposible.
func (o opciones) validar() error {
	if strings.TrimSpace(o.dataDir) == "" {
		return errors.New("falta -data-dir (no tiene default: escribir en la cola de una instalación viva por descuido no es un accidente aceptable)")
	}
	if strings.TrimSpace(o.sessionID) == "" {
		return errors.New("falta -session-id (es la sesión cuya DEK sella las filas)")
	}
	if o.conversaciones <= 0 {
		return fmt.Errorf("-conversaciones debe ser >= 1 (se recibió %d)", o.conversaciones)
	}
	if o.mensajes <= 0 {
		return fmt.Errorf("-mensajes debe ser >= 1 (se recibió %d)", o.mensajes)
	}
	// El prefijo y el lote acaban DENTRO de un patrón LIKE. Un `%` o un `_` ahí no rompen la siembra, pero
	// convierten el DELETE que se imprime en el resumen en una escoba que puede barrer filas REALES — que
	// es el peor fallo que esta herramienta podría causar. Se corta en la entrada.
	for nombre, v := range map[string]string{"-prefijo-jid": o.prefijoJID, "-lote": o.lote} {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("%s no puede estar vacío: sin él las filas sintéticas no se distinguen de las reales", nombre)
		}
		if strings.ContainsAny(v, "%_'\"@ ") {
			return fmt.Errorf("%s no puede contener %%, _, comillas, @ ni espacios (se recibió %q): "+
				"acaba dentro de un patrón LIKE y de un JID", nombre, v)
		}
	}
	return nil
}

// loteAleatorio devuelve 8 hex de CSPRNG. Aleatorio y no un timestamp: dos siembras lanzadas en el mismo
// segundo (un script) compartirían lote, y con él las conversaciones y los wa_message_id — el índice único
// se tragaría la mitad de las filas en silencio, porque el duplicado es un caso NORMAL para Enqueue.
func loteAleatorio() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// imprimirResumen escribe el parte de la siembra. Va a stdout, es legible por un humano y lleva el DELETE
// ya montado: quien deje filas de prueba en una instalación tiene la escoba en la misma pantalla.
func imprimirResumen(w io.Writer, o opciones, r resumen) {
	orden := "una conversación entera cada vez"
	if o.intercalar {
		orden = "intercalado entre conversaciones"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "colaseed: %d filas en %d conversaciones (seq %d..%d)\n",
		r.filas, r.conversaciones, r.seqMin, r.seqMax)
	fmt.Fprintf(&b, "  data_dir      : %s\n", o.dataDir)
	fmt.Fprintf(&b, "  session_id    : %s\n", o.sessionID)
	fmt.Fprintf(&b, "  lote          : %s\n", o.lote)
	fmt.Fprintf(&b, "  orden         : %s\n", orden)
	fmt.Fprintf(&b, "  primer chat   : %s\n", o.chatJID(0))
	fmt.Fprintf(&b, "  último chat   : %s\n", o.chatJID(o.conversaciones-1))
	fmt.Fprintf(&b, "  limpiar con   : DELETE FROM cola_entrantes WHERE session_id='%s' AND chat_jid LIKE '%s';\n",
		o.sessionID, r.patron)
	// La discrepancia se grita, no se esconde: si Enqueue dijo que sí más veces de las filas que hay, algo
	// se descartó (duplicado o tope) y el número de arriba es el bueno.
	if r.encoladas != r.filas {
		fmt.Fprintf(&b, "  ⚠️  se encolaron %d y en la tabla hay %d: la diferencia son duplicados tragados por "+
			"la idempotencia (session_id, wa_message_id) o filas descartadas por el tope (-max-filas=%d).\n",
			r.encoladas, r.filas, o.maxFilas)
	}

	// UNA sola escritura, y el descarte del error es explícito y único. Los `Fprintf` de arriba van contra
	// un strings.Builder, que por contrato NO falla nunca (su `Write` devuelve siempre nil), así que no hay
	// nada que comprobar ahí. Aquí sí podría fallar —stdout cerrado, disco lleno—, y aun así se ignora a
	// conciencia: esto es el parte final de una herramienta de laboratorio, y si el propio stdout está roto
	// no queda a dónde reportarlo. Lo que NO se hace es repartir ocho descartes por la función.
	_, _ = io.WriteString(w, b.String())
}
