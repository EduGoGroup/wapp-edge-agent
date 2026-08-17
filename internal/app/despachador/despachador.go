package despachador

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// despachador.go — EL DESPACHADOR POR SESIÓN (Plan 051 Ola 3 · T3.3 + T3.4 · REQ-051.18/19/20).
//
// QUÉ ES: el bucle que drena la cola durable de UNA sesión y entrega sus mensajes al cable, en el orden
// en que llegaron. Es la pieza que cierra el circuito que la Ola 1 abrió (el listener anota) y la Ola 2
// llenó (el cajero clasifica): sin él, la cola se llena y nadie la vacía.
//
// LA INVARIANTE QUE COMPRA LA OLA, y por la que existe todo esto (REQ-051.18): **la fila N+1 no sale
// antes que la N**, aunque la N+1 esté lista y la N siga esperando al cajero. Un mensaje entregado fuera
// de orden es una conversación reordenada delante del cliente final, y eso no se arregla aguas arriba.
// El precio de esa invariante es que una fila atascada RETIENE a su sesión, y por eso existe el
// PRESUPUESTO (T3.4): la retención está acotada por reloj (WAPP_AGENT_INTENT_WAIT_MS, 4000 ms por
// defecto) y al vencer el mensaje sale igual, sin intención. Se retrasa, nunca se pierde.
//
// ─── EL MOLDE, Y LAS DOS DIFERENCIAS DELIBERADAS ───
//
// Sigue el patrón de `commandDispatcher` (internal/adapters/cloudlink/dispatcher.go): concurrente ENTRE
// sesiones, SERIAL dentro de cada sesión, con `recover()` por unidad de trabajo para que un pánico aísle
// una sesión en vez de tumbar el proceso. Dos cosas cambian a propósito:
//
//  1. NO HAY CANAL NI BACKPRESSURE. Allí el productor es un stream gRPC y la cola es de memoria, así que
//     el buffer y el bloqueo del productor son el mecanismo. Aquí el productor es SQLITE: la cola ya está
//     en disco, ya es durable y ya tiene su propio tope (REQ-051.7). Meter un canal delante sería una
//     segunda cola, en RAM, delante de una cola en disco — dos verdades sobre lo mismo. El bucle hace
//     POLL contra la BD y no hay nada que presionar.
//  2. VIVE CON LA SESIÓN, NO CON EL STREAM. Aquel dispatcher nace y muere con cada invocación de
//     `runOnce` (una reconexión = un dispatcher nuevo). Este cuelga del ctx del LISTENER de su sesión: una
//     caída del stream a la nube no debe detener el drenado —el sink cae al outbox durable y sigue— y una
//     reconexión no debe reiniciar el reloj del presupuesto.
//
// ─── EL CAMINO INLINE YA NO EXISTE (T3.0, 2026-08-17) ───
//
// Cuando este bucle se escribió, el listener SEGUÍA entregando el mensaje al sink por su cuenta además de
// anotarlo en la cola (la escritura doble de la Ola 1), y con el despachador cableado eso significaba doble
// entrega transitoria —tolerada por la deduplicación del cable por `wa_message_id`—. T3.0 retiró aquella
// entrega: ESTE BUCLE ES HOY EL ÚNICO CAMINO por el que un entrante llega a la nube. La consecuencia
// práctica: una sesión cuyo despachador no arranca no entrega nada (sus filas se acumulan en disco hasta
// que alguien lo levante), y ya no hay una segunda vía que lo disimule.
//
// ─── INV-051.1 EN ESTE FICHERO ───
//
// Este bucle maneja el texto y los metadatos EN CLARO: es su trabajo, es lo que entrega. Por eso la regla
// es absoluta y no admite un «solo para depurar»: ni `Texto`, ni `Meta`, ni `PushName`, ni el `chat_jid`
// salen jamás por un log. Lo que se loguea son identificadores de enrutado (`session_id`), de fila (`id`,
// `seq`, `estado`) y el `wa_message_id`, que es un identificador opaco de WhatsApp, no contenido.
//
// 🔴 EL `chat_jid` NO SE LOGUEA EN NINGUNA LÍNEA DE ESTE PAQUETE, y por eso aquí NO hay una tercera copia
// del helper `chatJIDHash` (existe en internal/app/cajero y en internal/adapters/colaentrantes). Es una
// decisión, no un olvido: el `chat_jid` es el TELÉFONO del cliente, y con `id` + `wa_message_id` ya se
// localiza la fila sin nombrar la conversación. Si algún día hace falta citarla, se copia el helper con
// su hash de 8 hex — NUNCA el JID.

// DefaultPresupuestoMS es el presupuesto de espera por defecto, en milisegundos: cuánto retiene el
// despachador la cabeza de su sesión esperando a que el cajero la clasifique, antes de entregarla igual
// sin intención.
//
// 🔴 ES EL MISMO NÚMERO QUE `config.DefaultIntentWaitMS`, y que esté escrito dos veces es deliberado y
// acotado. La alternativa sería importar `internal/infra/config` desde `internal/app/…`, que invierte la
// dirección de la hexagonal: el núcleo pasaría a depender de la infraestructura para leer una constante.
// El valor REAL siempre llega inyectado (`Deps.Presupuesto`, cableado desde `cfg.Intent.WaitMS`); este
// default sólo cubre un `Deps` que no lo fije — es decir, los tests. Si divergieran, el que manda en
// producción es el de config, y este no se usaría jamás.
const DefaultPresupuestoMS = 4000

// plazoSello acota la escritura del SELLO (`MarcarDespachada`) cuando el contexto de la sesión ya se
// canceló. El sello corre con `context.WithoutCancel` a propósito —ver `entregar`: el mensaje ya salió
// al cable y no anotarlo obligaría a re-entregarlo en el arranque siguiente—, y un contexto sin
// cancelación necesita un plazo propio para que una parada no pueda colgarse esperando a SQLite.
//
// Dos segundos es holgadísimo para un UPDATE por rowid sobre una BD local; sólo se consumiría con la
// conexión única de la cola tomada por el cajero, y aun así es preferible esperar a re-entregar.
const plazoSello = 2 * time.Second

// Deps son las dependencias del despachador de UNA sesión. Todo lo que no es esencial tiene un default
// seguro (mismo criterio que `cajero.New`): un despachador que se niega a arrancar es una sesión cuya
// cola no se vacía nunca.
type Deps struct {
	// Cola es el lado despachador del puerto: leer la cabeza y sellarla. OBLIGATORIA.
	Cola app.ColaDespachador
	// Sink es el destino de la entrega: el `sessionSink` de ESTA sesión (mux.SinkFor(session_id)).
	// OBLIGATORIO. Aguas abajo NO cambia nada respecto del camino inline que existió hasta T3.0: el sink
	// decide vivo u outbox y sella los sensibles con la pública de la nube exactamente igual.
	//
	// 🔴 ES EL SINK CRUDO, SIN NINGUNA ENVOLTURA QUE CLASIFIQUE. Hasta T3.0 había un decorador
	// (adapters/intent) que clasificaba todo lo que pasaba por él, y esta nota existía para que nadie lo
	// aplicara también aquí; el decorador ya está retirado, pero la regla se conserva porque el motivo no ha
	// cambiado: la intención YA viene resuelta desde `intent_json`, así que envolver este sink significaría
	// clasificar por segunda vez el mismo texto — pagando la inferencia dos veces y arriesgando dos
	// veredictos distintos para un solo mensaje.
	Sink app.InboundSink
	// SessionID es la sesión que este bucle drena. OBLIGATORIO: es la clave de enrutado con la que la cola
	// elige tanto las filas como la DEK con que las abre.
	SessionID string
	// Log es el logger; nil ⇒ el default del proceso. Conviene pasarlo ya etiquetado con session_id.
	Log sharedlogger.Logger
	// Ahora es el reloj INYECTABLE con el que se mide el presupuesto. nil ⇒ time.Now.
	//
	// Nunca `time.Now()` directo en el cuerpo del bucle: el presupuesto es la única lógica temporal de esta
	// pieza, y un test que no pueda adelantar el reloj sólo puede comprobarla durmiendo — que es como se
	// escriben los tests que fallan en CI un martes de cada tres.
	Ahora func() time.Time
	// Presupuesto es cuánto se retiene la cabeza esperando al cajero (WAPP_AGENT_INTENT_WAIT_MS).
	// <= 0 ⇒ DefaultPresupuestoMS.
	Presupuesto time.Duration
	// Despertador es cómo se espera entre dos miradas a la cola. nil ⇒ PollFijo(DefaultPollMS).
	Despertador Despertador
}

// cabezaEnCurso es el estado del PRESUPUESTO: qué fila es hoy la cabeza y desde cuándo se la está
// esperando. Lo toca SÓLO la goroutine de Run, así que no lleva candado.
//
// 🔴 EL RELOJ ARRANCA CUANDO LA FILA SE CONVIERTE EN CABEZA DESPACHABLE, NO CUANDO SE ENCOLÓ, y la
// diferencia es el sentido entero del presupuesto. Si se midiera desde el `Enqueue`, una fila que pasó
// diez minutos detrás de otras nacería con el presupuesto ya agotado y saldría sin intención sin que
// nadie le hubiera dado al cajero una sola oportunidad de clasificarla. Lo que se acota es LA ESPERA DE
// ESTE DESPACHADOR, que es lo único que él controla.
// reintentoEntrega recuerda cuántas veces seguidas ha fallado la entrega de UNA fila concreta y a
// partir de cuándo tiene sentido volver a intentarlo. Se identifica por el par (id, seq) por el mismo
// motivo que `cabezaEnCurso`: el rowid solo no identifica una fila a lo largo del tiempo.
//
// Se limpia en cuanto una entrega sale bien, así que un fallo aislado —un corte de red que se cura—
// no deja rastro ni penaliza a la fila siguiente.
type reintentoEntrega struct {
	id     int64
	seq    int64
	fallos int
	// desde marca a partir de cuándo se puede reintentar. Mientras no llegue, la vuelta no llama al
	// sink: devuelve «sin progreso» y el bucle paga su poll normal.
	desde time.Time
}

// esperaTrasFallo devuelve cuánto hay que esperar tras `fallos` entregas fallidas consecutivas de la
// misma fila. Duplica desde el propio poll (500 ms) hasta un techo de 30 s.
//
// El PRIMER fallo no espera nada extra —devuelve 0—, para no penalizar el caso dominante y sano: un
// tropiezo puntual que se cura en el poll siguiente. El espaciado empieza a partir del segundo, que es
// cuando el fallo ya parece determinista.
//
// El techo es 30 s y no infinito porque esto NO es un abandono: la fila se sigue intentando para
// siempre, y con un techo bajo la recuperación es inmediata en cuanto el sink vuelve a aceptar.
func esperaTrasFallo(fallos int) time.Duration {
	if fallos <= 1 {
		return 0
	}
	espera := pollBase << (fallos - 2)
	if espera > topeBackoffEntrega || espera <= 0 { // el <= 0 atrapa el desbordamiento del shift
		return topeBackoffEntrega
	}
	return espera
}

const (
	// pollBase es el escalón inicial del backoff: el mismo medio segundo del poll, para que el primer
	// espaciado no se note frente al ritmo normal del bucle.
	pollBase = 500 * time.Millisecond
	// topeBackoffEntrega acota la espera entre re-entregas. Ver esperaTrasFallo.
	topeBackoffEntrega = 30 * time.Second
)

type cabezaEnCurso struct {
	// id es la fila que se está esperando; 0 = ninguna.
	id int64
	// seq acompaña al id porque EL ROWID SOLO NO IDENTIFICA UNA FILA A LO LARGO DEL TIEMPO (T3.10):
	// la tabla no declara AUTOINCREMENT, y SQLite reutiliza el rowid de una fila borrada. Entre la
	// poda TTL y el tope de filas, borrar es rutina aquí. Si una fila nueva heredara el rowid de la
	// que este bucle estaba esperando, la comparación por id sola diría «es la misma cabeza» y la
	// nueva HEREDARÍA el `desde` de la vieja: su presupuesto nacería ya consumido y se despacharía
	// sin intención de inmediato. Una degradación silenciosa y difícil de creer leyendo los logs.
	//
	// `seq` es monotónico por sesión y nunca se reusa, así que el par (id, seq) sí identifica.
	seq int64
	// desde es el instante en que se la vio por primera vez como cabeza sin clasificar.
	desde time.Time
	// disparado marca que YA se llamó a DespacharSinIntent para esta fila. Impide re-disparar en bucle si
	// la sentencia no la sacó de `nuevo`/`tomado` (ver correrPresupuesto). En el camino normal esta marca
	// vive un suspiro: la vuelta siguiente encuentra la fila `clasificado`, la entrega, y `olvidarCabeza`
	// borra el struct entero.
	disparado bool
	// avisada marca que ya se gritó el Warn de «esta cabeza no avanza». Un aviso por fila, no por poll.
	avisada bool
}

// Despachador drena la cola de UNA sesión, en orden de `seq`, y entrega al sink. Uno por sesión; entre
// sesiones son independientes y concurrentes.
type Despachador struct {
	cola        app.ColaDespachador
	sink        app.InboundSink
	sessionID   string
	log         sharedlogger.Logger
	ahora       func() time.Time
	presupuesto time.Duration
	despertador Despertador

	// enCurso es estado LOCAL DEL BUCLE (no compartido): sólo lo lee y escribe Run/vuelta.
	enCurso cabezaEnCurso

	// reintento es el freno de la RE-ENTREGA, y también estado local del bucle.
	//
	// 🔴 POR QUÉ EXISTE: una fila cuyo `Deliver` falla de forma determinista —un evento que el sink
	// rechaza siempre, no un corte de red— se reintentaba **cada 500 ms para siempre**, sin espaciado y
	// sin límite. Es EXACTAMENTE el patrón del «lote venenoso» que ya congeló la cola en la Ola 2 y que
	// obligó a inventar `MotivoFalloRepetido` para el cajero (T2.19). El cajero tenía la vacuna; el
	// despachador no.
	//
	// 🔴 Y POR QUÉ AQUÍ NO SE ABANDONA NUNCA, a diferencia del cajero (decisión de Jhoan, 2026-08-17):
	// abandonar en el cajero significa renunciar a la CLASIFICACIÓN —el mensaje sale igual, sin
	// intención—, mientras que abandonar aquí significaría **no entregar el mensaje**, es decir
	// perderlo. La invariante del plan es «se retrasa, nunca se pierde», así que el freno sólo espacia:
	// deja de quemar CPU y de inundar el log, y sigue intentándolo indefinidamente.
	reintento reintentoEntrega

	// ─── Contadores (INV-051.3: las degradaciones se CUENTAN, no sólo se loguean) ───
	//
	// Son las series LOCALES que la Ola 4 (T4.0) publicará al heartbeat. Todos son acumulados monotónicos
	// sin PII: cardinalidades, nunca contenido.
	despachados          atomic.Int64
	conIntent            atomic.Int64
	fragmentosDeLote     atomic.Int64
	sobresIlegibles      atomic.Int64
	metasIlegibles       atomic.Int64
	presupuestosVencidos atomic.Int64
	fallosLectura        atomic.Int64
	fallosEntrega        atomic.Int64
	panicos              atomic.Int64
	// EL SELLO FALLA EN DOS SITIOS QUE NO SIGNIFICAN LO MISMO, y por eso son dos series y no una
	// (misma razón por la que `omitidos` desglosa por motivo en vez de agregar):
	//
	//   - fallosSelloPresupuesto: falló `DespacharSinIntent`. La fila sigue en `nuevo`/`tomado`, se
	//     reintenta en el poll siguiente y AGUAS ARRIBA NO PASA NADA. Es ruido operativo.
	//   - fallosSelloDespacho: falló `MarcarDespachada` DESPUÉS de una entrega con éxito. Cada uno de
	//     estos es **un duplicado garantizado en la nube**: el mensaje ya salió y la fila se releerá.
	//
	// Agregarlos haría ilegible justo el número que importa. Y desde que el sello corre con
	// `context.WithoutCancel` (ver `entregar`), el segundo dejó de dispararse en cada apagado —antes ni
	// siquiera se contaba ahí—, así que ahora sí es una señal limpia de «hubo duplicados».
	fallosSelloPresupuesto atomic.Int64
	fallosSelloDespacho    atomic.Int64
	// LA CABEZA ATASCADA: una fila en un estado que esta versión no conoce se queda de cabeza para
	// siempre y su sesión NO DRENA MÁS. Es la peor degradación posible del despachador y hasta el
	// 2026-08-17 su única señal era UN `Warn` en toda la vida del proceso (`avisada`), sin un solo
	// atómico subiendo: la telemetría de la Ola 4 habría publicado una sesión MUERTA como una sesión
	// ociosa, que es exactamente lo que INV-051.3 prohíbe (ninguna degradación silenciosa).
	//
	// Son dos porque responden preguntas distintas: `cabezasAtascadas` cuántas filas han llegado a este
	// estado (sube una vez por fila, como el aviso), y `pollsCabezaAtascada` cuánto lleva la sesión sin
	// drenar AHORA (sube en cada vuelta que sigue bloqueada). La primera sola no distingue «pasó una vez
	// hace horas» de «lleva parada desde entonces».
	cabezasAtascadas    atomic.Int64
	pollsCabezaAtascada atomic.Int64
	// omitidos desglosa los despachos SIN intención POR MOTIVO. Se construye en New recorriendo
	// app.MotivosOmitido() y NO se muta después, así que leerlo concurrentemente es seguro.
	//
	// 🔴 SE RECORRE LA LISTA CANÓNICA, JAMÁS UNA LISTA ESCRITA A MANO. Los motivos son OCHO y han sido
	// siete dos veces: `sin_texto` entró en T1.8 y `fallo_repetido` en T2.19, cada uno por un incidente de
	// campo. Una lista copiada aquí se quedaría corta a la tercera, y el síntoma sería un motivo que
	// simplemente no aparece en la telemetría — invisible, no ruidoso.
	omitidos map[app.MotivoOmitido]*atomic.Int64
	// omitidosFueraDeLista cuenta los sobres cuyo motivo NO está en la lista canónica: un `intent_json`
	// escrito por una versión MÁS NUEVA del Edge (un binario viejo leyendo la cola de uno nuevo tras un
	// rollback). Se entrega igual, sin intención; lo único que se pierde es el desglose.
	omitidosFueraDeLista atomic.Int64
}

// New valida las dependencias y aplica los defaults. Sólo falla por lo que hace IMPOSIBLE el bucle: sin
// cola no hay qué leer, sin sink no hay dónde entregar y sin session_id no se sabe qué drenar.
//
// 🔴 UN DESPACHADOR CON COLA nil SERÍA UN PÁNICO, no un no-op. Por eso se rechaza AQUÍ, en la
// construcción, y no se tolera dentro del bucle: una dependencia obligatoria se valida donde se cablea, en
// frío, no en la vuelta 1 del bucle de una sesión viva.
//
// ⚠️ EL nil YA NO ES ALCANZABLE DESDE `agent serve` (2026-08-17): la apertura, migración y construcción de
// la cola en `daemon.go` son FATALES, así que un daemon vivo tiene cola. Esta validación no sobra por eso —
// `New` es público, lo llaman los tests y cualquier cableado futuro—, pero conviene saber que su papel
// pasó de «última barrera contra un agujero real» a «contrato de construcción».
func New(deps Deps) (*Despachador, error) {
	if deps.Cola == nil {
		return nil, errors.New("despachador: falta la cola (app.ColaDespachador)")
	}
	if deps.Sink == nil {
		return nil, errors.New("despachador: falta el sink de salida de la sesión")
	}
	if deps.SessionID == "" {
		return nil, errors.New("despachador: falta el session_id")
	}

	log := deps.Log
	if log == nil {
		log = sharedlogger.Default()
	}
	ahora := deps.Ahora
	if ahora == nil {
		ahora = time.Now
	}
	presupuesto := deps.Presupuesto
	if presupuesto <= 0 {
		presupuesto = DefaultPresupuestoMS * time.Millisecond
	}
	desp := deps.Despertador
	if desp == nil {
		desp = NewPollFijo(DefaultPollMS * time.Millisecond)
	}

	// El desglose por motivo se materializa AQUÍ, una sola vez, desde la lista canónica: así el mapa tiene
	// las ocho entradas desde el arranque (un motivo con 0 es un dato, no un hueco) y el camino caliente
	// nunca escribe en el mapa — sólo incrementa el atómico que encuentra.
	omitidos := make(map[app.MotivoOmitido]*atomic.Int64, len(app.MotivosOmitido()))
	for _, m := range app.MotivosOmitido() {
		omitidos[m] = new(atomic.Int64)
	}

	return &Despachador{
		cola:        deps.Cola,
		sink:        deps.Sink,
		sessionID:   deps.SessionID,
		log:         log,
		ahora:       ahora,
		presupuesto: presupuesto,
		despertador: desp,
		omitidos:    omitidos,
	}, nil
}

// Run drena la sesión hasta que ctx se cancele. Devuelve nil en la parada ordenada: que la sesión se pare
// porque la cancelaron no es un fallo, y devolver error ahí haría que el supervisor lo tratara como caída.
//
// 🔴 CIERRE LIMPIO — NINGUNA ENTREGA OCURRE DESPUÉS DE CANCELAR EL CTX. La propiedad se sostiene sobre
// tres cosas, no sobre una: (a) el `ctx.Err()` al principio de cada vuelta, (b) el `Esperar` que sale por
// `ctx.Done()` sin agotar el tick, y (c) que el propio `Deliver` recibe el ctx, de modo que una entrega
// ya empezada se corta con él. Es lo que el gate `-race` de la ola tiene que comprobar.
func (d *Despachador) Run(ctx context.Context) error {
	d.log.Info("despachador: arrancando (drenado de la cola de entrantes)",
		"session_id", d.sessionID,
		"presupuesto_ms", d.presupuesto.Milliseconds(),
	)

	for {
		if ctx.Err() != nil {
			return d.detener()
		}

		// progreso = «se movió la cabeza»: se entregó una fila o se selló una por presupuesto. En ese caso
		// se vuelve a mirar INMEDIATAMENTE, sin pagar un poll: una ráfaga de mensajes ya clasificados sale
		// de una tirada en vez de a razón de uno cada 500 ms, que sería latencia inventada.
		progreso := d.vuelta(ctx)

		if ctx.Err() != nil {
			return d.detener()
		}
		if progreso {
			continue
		}
		if err := d.despertador.Esperar(ctx); err != nil {
			return d.detener()
		}
	}
}

// detener escribe el bloque final de contadores y devuelve nil. Aparte para que las tres salidas de Run
// no puedan divergir.
func (d *Despachador) detener() error {
	d.log.Info("despachador: detenido limpiamente", d.contadores()...)
	return nil
}

// vuelta ejecuta UNA iteración del bucle: mira la cabeza y decide. Devuelve si hubo progreso (la cabeza
// se movió) para que Run sepa si puede volver a mirar sin esperar.
//
// EL `recover()` VA AQUÍ, por vuelta, siguiendo el molde de `commandDispatcher.handle`: un pánico —de un
// driver de BD, de un descifrado, de un sink— aísla a ESTA sesión y el bucle sigue en la vuelta
// siguiente, en vez de llevarse por delante el proceso entero y con él las demás sesiones.
//
// 🔴 EL VALOR RECUPERADO NO SE LOGUEA (INV-051.1), igual que en `enqueueCola`: un pánico arrastra en su
// mensaje el argumento que lo provocó, y aquí ese argumento puede ser el TEXTO del mensaje o su
// metadato. Se anota el session_id y la fila, que es lo que permite correlacionar sin transcribir nada.
func (d *Despachador) vuelta(ctx context.Context) (progreso bool) {
	defer func() {
		if r := recover(); r != nil {
			d.panicos.Add(1)
			d.log.Error("despachador: pánico en una vuelta del bucle (aislado); la sesión sigue drenando",
				"session_id", d.sessionID, "id_cabeza", d.enCurso.id)
			progreso = false
		}
	}()

	cabeza, err := d.cola.CabezaDeSesion(ctx, d.sessionID)
	if err != nil {
		// Una cancelación en vuelo NO es un fallo de la cola: se sale callando y Run detecta el ctx.
		if ctx.Err() != nil {
			return false
		}
		// La causa típica es la DEK: una sesión cuya custodia no se puede leer no puede abrir su propia
		// cabeza. El adaptador ya trae su enfriamiento de log para no gritar por poll; aquí se cuenta
		// siempre (INV-051.3) y se reintenta en el poll siguiente. NO se salta la fila: saltársela rompería
		// el FIFO exactamente igual que un hueco.
		d.fallosLectura.Add(1)
		d.log.Error("despachador: no se pudo leer la cabeza de la sesión; se reintenta en el siguiente poll",
			"session_id", d.sessionID, "error", err)
		return false
	}
	if cabeza == nil {
		// Sesión al día: el estado NORMAL de casi todos los polls. Se olvida el presupuesto en curso —no
		// hay a quién esperar— para que la próxima cabeza estrene su reloj entero.
		d.olvidarCabeza()
		return false
	}

	if cabeza.Estado == app.EstadoClasificado {
		// 🔴 EL `olvidarCabeza()` AQUÍ ES LO QUE HACE QUE EL PRESUPUESTO NO SE REINICIE POR ID, SINO POR
		// «esta fila ya no es una espera». Desde el arreglo del 2026-08-17, `nuevo/tomado → clasificado`
		// SIN cambiar de id es un camino NORMAL y frecuente (es el desenlace del propio presupuesto: el
		// sobre de omisión resuelve la fila en su sitio). Si el reinicio dependiera sólo del `id != id`
		// de `correrPresupuesto`, el estado `{disparado, avisada}` de la fila anterior sobreviviría a su
		// entrega y contaminaría a la SIGUIENTE cabeza que casualmente reusara el mismo id — y con
		// `disparado=true` heredado, esa fila nunca se sellaría por presupuesto: se quedaría esperando
		// eternamente tras un único Warn. Se olvida ANTES de entregar, no después, para que el olvido
		// ocurra también cuando la entrega falla.
		d.olvidarCabeza()
		// EL FRENO DE LA RE-ENTREGA (ver `reintentoEntrega`). Si esta misma fila viene fallando, se deja
		// pasar la vuelta sin llamar al sink: no es progreso, así que el bucle paga su poll y vuelve.
		// Ojo al orden — va DESPUÉS de `olvidarCabeza`, porque el presupuesto ya se resolvió: lo que se
		// frena es la entrega, no la espera.
		if d.reintento.id == cabeza.ID && d.reintento.seq == cabeza.Seq &&
			d.ahora().Before(d.reintento.desde) {
			return false
		}
		return d.entregar(ctx, cabeza)
	}
	// `nuevo`, `tomado` — o cualquier estado que esta versión no conozca. Se les corre el presupuesto a
	// todos por el mismo criterio con el que sqlCabezaDeSesion usa `estado <> 'despachado'` en vez de un
	// `IN`: una fila en un estado imprevisto BLOQUEA la cabeza de forma visible y acotada, en vez de
	// volverse invisible y provocar un salto de orden silencioso.
	return d.correrPresupuesto(ctx, cabeza)
}

// olvidarCabeza reinicia el reloj del presupuesto. Se llama cuando la cabeza deja de ser una fila que se
// esté esperando (no hay nada pendiente, o la que hay ya está clasificada).
func (d *Despachador) olvidarCabeza() { d.enCurso = cabezaEnCurso{} }

// correrPresupuesto es T3.4a: gestiona la espera acotada de una cabeza que aún no está clasificada.
//
// TRES ESTADOS, en este orden:
//
//  1. Es una cabeza NUEVA para nosotros ⇒ se arranca su reloj y se espera un poll. Cambiar de cabeza
//     REINICIA el reloj, y tiene que ser así: cada fila tiene derecho a su presupuesto completo.
//  2. El reloj aún no ha vencido ⇒ se espera otro poll. La cabeza sigue siendo la misma y la fila N+1
//     NO se adelanta, aunque esté lista (REQ-051.18: eso es justo lo que se está comprando).
//  3. El reloj venció ⇒ se resuelve con `DespacharSinIntent(presupuesto)` y se RELEE INMEDIATAMENTE.
//
// 🔴 AQUÍ NO SE ENTREGA NADA, Y ESA AUSENCIA ES EL ARREGLO DEL 2026-08-17. `DespacharSinIntent` deja la
// fila en `clasificado` con su sobre `{"omitido":"presupuesto"}` y suelta el claim; NO la sella. Así la
// relectura de la vuelta siguiente SÍ la encuentra (`sqlCabezaDeSesion` sólo excluye `despachado`), la
// rama `EstadoClasificado` de `vuelta` la lleva a `entregar`, y ahí el camino NORMAL hace lo correcto sin
// una sola línea de código especial: `app.EsOmitido` devuelve `presupuesto`, `evt.Intent` queda nil (el
// sobre NO viaja al cable), se cuenta el omitido POR MOTIVO y `MarcarDespachada` pone el sello terminal.
//
// La versión anterior sellaba `despachado` aquí y releía: la relectura ya no encontraba la fila, `entregar`
// —única puerta al sink— no se ejecutaba jamás y el mensaje MORÍA en disco. Si alguien vuelve a meter una
// rama de entrega en esta función, es la señal de que el sello volvió a ser terminal: mirar el SQL primero.
//
// 🔴 POR QUÉ SE RELEE EN VEZ DE ENTREGAR CON LO QUE TENÍAMOS EN LA MANO (esto sigue siendo cierto y es la
// segunda razón para no entregar aquí): el sello puede ser NO-OP porque el cajero cerró el lote un instante
// antes (`estado IN ('nuevo','tomado')` deja de casar). Ese es el desenlace BUENO de la carrera —el mensaje
// sale CON su intención real— y sólo se descubre releyendo. Entregar con la foto vieja tiraría una
// clasificación ya pagada y escrita en disco.
func (d *Despachador) correrPresupuesto(ctx context.Context, cabeza *app.ColaCabeza) bool {
	ahora := d.ahora()

	// Se compara el PAR (id, seq), no el id solo: ver `cabezaEnCurso.seq`. Un rowid reciclado por
	// SQLite tras una poda haría que una fila nueva heredase el presupuesto ya consumido de la vieja.
	if d.enCurso.id != cabeza.ID || d.enCurso.seq != cabeza.Seq {
		d.enCurso = cabezaEnCurso{id: cabeza.ID, seq: cabeza.Seq, desde: ahora}
		return false
	}
	if ahora.Sub(d.enCurso.desde) < d.presupuesto {
		return false
	}

	if d.enCurso.disparado {
		// SÓLO ALCANZABLE SI LA FILA SIGUE SIN ESTAR `clasificado` DESPUÉS DEL SELLO, es decir: en un
		// estado que esta versión no conoce (uno nuevo que alguien añada mañana, o un 'zombi' escrito a
		// mano). Los dos desenlaces normales del sello salen por la rama `EstadoClasificado` de `vuelta`,
		// no por aquí: si el UPDATE aterrizó, la fila quedó `clasificado` con su sobre de omisión; y si
		// fue no-op, quedó `clasificado` con el intent real del cajero. En ambos casos se entrega.
		//
		// Un aviso POR FILA (no por poll) y a esperar: gritarlo cada 500 ms convertiría una anomalía en una
		// tormenta de log, que es la forma más rápida de que nadie la lea.
		// El poll SIEMPRE se cuenta, aunque el aviso sea uno solo: es lo que permite distinguir «esto
		// pasó una vez» de «esta sesión lleva cuatro horas sin drenar».
		d.pollsCabezaAtascada.Add(1)
		if !d.enCurso.avisada {
			d.enCurso.avisada = true
			d.cabezasAtascadas.Add(1)
			d.log.Warn("despachador: la cabeza no avanza tras el sello por presupuesto (estado inesperado); la sesión no drena",
				"session_id", d.sessionID, "id", cabeza.ID, "seq", cabeza.Seq,
				"estado", cabeza.Estado, "wa_message_id", cabeza.WAMessageID)
		}
		return false
	}

	if err := d.cola.DespacharSinIntent(ctx, cabeza.ID, app.MotivoPresupuesto); err != nil {
		if ctx.Err() != nil {
			return false
		}
		d.fallosSelloPresupuesto.Add(1)
		d.log.Error("despachador: no se pudo sellar la cabeza por presupuesto vencido; se reintenta en el siguiente poll",
			"session_id", d.sessionID, "id", cabeza.ID, "seq", cabeza.Seq, "error", err)
		return false
	}
	d.enCurso.disparado = true
	// Se cuenta el DISPARO, que no es lo mismo que la entrega sin intención: si el cajero ganó la carrera,
	// el sobre fue no-op y la fila saldrá con su intención real. Cuántas de esas esperas se pagaron para
	// nada lo cuenta el adaptador (Store.DespachosSinIntentNoAplicados); aquí se cuenta cuántas veces se
	// agotó el reloj. Ver PresupuestosVencidos() para cuándo pueden diferir legítimamente de
	// OmitidosPorMotivo()[presupuesto].
	d.presupuestosVencidos.Add(1)
	d.log.Debug("despachador: presupuesto agotado esperando al cajero; la fila se resuelve sin intención y se entrega en la vuelta siguiente",
		"session_id", d.sessionID, "id", cabeza.ID, "seq", cabeza.Seq,
		"presupuesto_ms", d.presupuesto.Milliseconds())
	// PROGRESO: la cabeza se movió (de `nuevo`/`tomado` a `clasificado`), así que Run vuelve a mirar SIN
	// pagar un poll y la entrega ocurre en la misma ráfaga. No es una optimización: sin este `true`, un
	// mensaje ya resuelto se quedaría 500 ms más en disco por nada.
	return true
}

// entregar publica la cabeza en el sink y, SÓLO SI la entrega salió bien, la sella como despachada.
//
// 🔴 EL ORDEN ES ENTREGA → SELLO, Y ES UNA DECISIÓN DE PÉRDIDA DE DATOS, no de estilo. Los dos órdenes
// fallan de forma distinta y hay que elegir cuál fallo se prefiere:
//
//   - SELLO PRIMERO: si la entrega falla después, la fila ya está `despachado`, nadie la volverá a mirar
//     y el mensaje SE PIERDE, en silencio y para siempre. Un mensaje del cliente final que nunca llegó a
//     la nube y del que no queda ni el rastro de una fila pendiente.
//   - ENTREGA PRIMERO (lo que se hace): si el sello falla después, la fila sigue `clasificado`, el poll
//     siguiente la relee y la vuelve a entregar. El resultado es un DUPLICADO aguas arriba — que el cable
//     ya tolera, porque la deduplicación por `wa_message_id` existe desde el principio.
//
// Un duplicado es un incidente de idempotencia; una pérdida es un incidente de negocio. Se elige el
// primero, y eso es lo que exige el espíritu de INV-051.2: la cola no puede ser peor que no tenerla.
//
// ⚠️ CONSECUENCIA CONOCIDA de esa elección: en la re-entrega, los contadores de abajo suman DOS VECES por
// un solo mensaje. Es correcto y no se corrige — cuentan ENTREGAS, no mensajes, y una entrega repetida es
// exactamente el hecho operativo que se quiere ver si el sello empieza a fallar.
func (d *Despachador) entregar(ctx context.Context, cabeza *app.ColaCabeza) bool {
	evt, v := d.evento(cabeza)

	if err := d.sink.Deliver(ctx, evt); err != nil {
		if ctx.Err() != nil {
			return false
		}
		d.fallosEntrega.Add(1)
		// La fila NO se sella: sigue `clasificado` y es la cabeza del siguiente poll. Ese es el reintento.
		// Lo que se apunta aquí es el ESPACIADO, para que un fallo determinista no se convierta en un
		// bucle caliente de re-entregas cada 500 ms (ver `reintentoEntrega`). No hay abandono: la fila
		// se seguirá intentando siempre, sólo que cada vez más de tarde en tarde.
		if d.reintento.id != cabeza.ID || d.reintento.seq != cabeza.Seq {
			d.reintento = reintentoEntrega{id: cabeza.ID, seq: cabeza.Seq}
		}
		d.reintento.fallos++
		espera := esperaTrasFallo(d.reintento.fallos)
		d.reintento.desde = d.ahora().Add(espera)
		d.log.Error("despachador: no se pudo entregar el entrante al sink; la fila NO se sella y se reintenta",
			"session_id", d.sessionID, "id", cabeza.ID, "seq", cabeza.Seq,
			"wa_message_id", cabeza.WAMessageID, "error", err,
			"fallos_seguidos", d.reintento.fallos, "proximo_intento_en_ms", espera.Milliseconds())
		return false
	}
	// Entrega buena: se olvida el espaciado. Un tropiezo aislado no penaliza a nadie.
	d.reintento = reintentoEntrega{}

	d.despachados.Add(1)
	switch {
	case v.ilegible:
		d.sobresIlegibles.Add(1)
	case v.fragmento:
		d.fragmentosDeLote.Add(1)
	case v.omitido:
		d.contarOmitido(v.motivo)
	default:
		d.conIntent.Add(1)
	}

	// EL SELLO NO HEREDA LA CANCELACIÓN, y esa es la diferencia entre un duplicado inevitable y uno
	// autoinfligido. Cuando se llega aquí el mensaje YA SALIÓ al cable; lo único que falta es anotarlo.
	// Si el sello se saltara por un ctx recién cancelado, la fila quedaría entregada y sin sellar, y el
	// arranque siguiente la volvería a entregar: es decir, CADA PARADA ORDENADA del Edge (un
	// `systemctl restart`, un `stopLive`) produciría duplicados. Ante un `kill -9` no hay nada que
	// hacer y el diseño los tolera (idempotencia por `wa_message_id`); en una parada ordenada son
	// evitables, porque sellar es una escritura local de microsegundos sobre una BD que sigue abierta
	// —el daemon no cierra `cola_entrantes.db` hasta que este `Run` retorna—.
	//
	// Se acota con plazo propio: un cierre no puede quedarse colgado si SQLite está contendido (la
	// cola se abre con SetMaxOpenConns(1) y el cajero puede tener la conexión).
	ctxSello, cancelSello := context.WithTimeout(context.WithoutCancel(ctx), plazoSello)
	defer cancelSello()
	if err := d.cola.MarcarDespachada(ctxSello, cabeza.ID); err != nil {
		// SIEMPRE se cuenta y se grita, también con el ctx cancelado. Antes se volvía en silencio en ese
		// caso: la re-entrega quedaba sin contador y sin log, o sea invisible justo en el escenario en
		// que ocurría de verdad.
		d.fallosSelloDespacho.Add(1)
		d.log.Error("despachador: el mensaje SE ENTREGÓ pero la fila no se pudo sellar; se re-entregará (duplicado tolerado)",
			"session_id", d.sessionID, "id", cabeza.ID, "seq", cabeza.Seq,
			"wa_message_id", cabeza.WAMessageID, "error", err)
		// false: NO se considera progreso. La cabeza sigue siendo la misma y volver a mirar de inmediato
		// sería un bucle caliente de re-entregas; se paga un poll antes de reintentar.
		return false
	}
	return true
}

// contarOmitido incrementa el desglose por motivo. Un motivo que no esté en la lista canónica NO se
// inventa una entrada en el mapa (eso sería una escritura concurrente sobre un mapa que se declaró
// inmutable): se cuenta aparte.
func (d *Despachador) contarOmitido(motivo app.MotivoOmitido) {
	if c, ok := d.omitidos[motivo]; ok {
		c.Add(1)
		return
	}
	d.omitidosFueraDeLista.Add(1)
	d.log.Warn("despachador: sobre de omisión con un motivo fuera de la lista canónica; el mensaje sale sin intención",
		"session_id", d.sessionID, "motivo", string(motivo))
}

// veredicto es lo que se aprendió del `intent_json` de una fila: qué se cuenta cuando su entrega salga
// bien. Es un tipo aparte —y no tres booleanos sueltos— para que el `switch` de `entregar` sea exhaustivo
// de un vistazo.
type veredicto struct {
	// motivo es el del sobre de omisión; vacío en los demás casos.
	motivo app.MotivoOmitido
	// omitido: el sobre era `{"omitido":…}` y el mensaje sale SIN intención.
	omitido bool
	// fragmento: fila `clasificado` con `intent_json` NULL, es decir un fragmento intermedio de un lote
	// (ver evento).
	fragmento bool
	// ilegible: había sobre, pero no se pudo leer como ninguna de las dos formas conocidas.
	ilegible bool
}

// evento reconstruye el domain.InboundEvent de una fila `clasificado` y aplica LA REGLA DEL SOBRE
// (T3.4b). Es la traducción inversa exacta de `toInboundEvent` + `colaMeta` del listener.
//
// ─── LA REGLA DEL SOBRE, en las cuatro formas que puede tener `intent_json` ───
//
//  1. `{"omitido":"<motivo>"}` ⇒ `evt.Intent = nil` y EL SOBRE MUERE AQUÍ. No viaja al cable: el proto
//     `ClassifiedIntent` no tiene dónde ponerlo (sólo intent/params/confidence/config_version) y, sobre
//     todo, no debe — lo que sale del Edge es el CONTADOR por motivo, en el heartbeat de la Ola 4. Byte a
//     byte, es el comportamiento que tenía el decorador inline retirado en T3.0: tampoco anotaba nada en
//     esos casos. Esa equivalencia es la garantía de que la Ola 3 no cambió lo que ve la nube.
//  2. Un sobre del cajero legible ⇒ se rellena `evt.Intent`. Y SE RELLENA Y YA: la traducción al proto la
//     hace `classifiedIntentToProto` en el adaptador de CloudLink. Traducir aquí también sería traducir
//     dos veces y abrir la puerta a que las dos traducciones diverjan.
//  3. NULL (`TieneIntent == false`) ⇒ ver el bloque de abajo. NO es el caso del presupuesto.
//  4. Cualquier otra cosa ⇒ se entrega SIN intención y se cuenta como sobre ilegible. Nunca se retiene el
//     mensaje por no entender su sobre.
func (d *Despachador) evento(c *app.ColaCabeza) (domain.InboundEvent, veredicto) {
	meta, err := app.DecodeColaMeta(c.Meta)
	if err != nil {
		d.metasIlegibles.Add(1)
		// El error NO se incrusta en el log: puede arrastrar el fragmento de JSON que no parseó, y ese JSON
		// son metadatos de negocio en claro (INV-051.1). El mensaje sale IGUAL, sin remitente conocido:
		// perder el metadato es malo, retener el mensaje es peor.
		d.log.Warn("despachador: metadato de la fila ilegible; el mensaje se entrega sin remitente ni push_name",
			"session_id", d.sessionID, "id", c.ID, "seq", c.Seq, "wa_message_id", c.WAMessageID)
		// `meta` es el cero de app.ColaMeta (DecodeColaMeta no devuelve nada a medias), así que el evento se
		// construye igual, con los campos de identidad vacíos.
	}

	evt := domain.InboundEvent{
		MessageID:      c.WAMessageID,
		Chat:           c.ChatJID,
		Sender:         meta.Sender,
		SenderAlt:      meta.SenderAlt,
		AddressingMode: meta.AddressingMode,
		PushName:       meta.PushName,
		// `ts_whatsapp` se persistió como `Info.Timestamp.Unix()` y el sink lo vuelve a mandar como
		// `.Unix()`, así que el viaje por la cola es exacto en lo que llega al cable. Lo único que se pierde
		// es la fracción de segundo, que nunca salió del Edge.
		Timestamp: time.Unix(c.TSWhatsApp, 0),
		Type:      meta.Type,
		Text:      c.Texto,
		// IsFromMe SIEMPRE false, y no es una suposición: el listener descarta el eco propio en la puerta,
		// antes de encolar, así que no existe fila de la cola con un mensaje propio. Ver app.ColaMeta.
		IsFromMe: false,
		IsGroup:  meta.IsGroup,
	}

	if motivo, ok := app.EsOmitido(c.IntentJSON); ok {
		return evt, veredicto{motivo: motivo, omitido: true}
	}

	if !c.TieneIntent {
		// ⚠️ ESTE CASO ES NORMAL Y NO ES EL DEL PRESUPUESTO. El cajero cierra un LOTE (una conversación
		// entera) escribiendo el `intent_json` SÓLO en la ÚLTIMA fila y dejando las anteriores en
		// `clasificado` sin sobre propio: son fragmentos del mismo párrafo —los tres mensajes seguidos que
		// una persona manda en un turno— y no tienen intención independiente que anotar. Un turno de tres
		// mensajes produce, por diseño, dos filas que llegan aquí con NULL.
		//
		// 🔴 POR ESO NO SE CUENTAN COMO `presupuesto`, y es una desviación consciente del encargo. El
		// presupuesto vencido ESCRIBE su sobre `{"omitido":"presupuesto"}` en la MISMA sentencia que pasa la
		// fila a `clasificado` (sqlDespacharSinIntent), así que una fila resuelta por presupuesto llega aquí
		// con `TieneIntent = true` y se va por la rama de arriba: este NULL no puede venir de ahí — un
		// `clasificado` sin sobre sólo lo produce el cierre por lotes del cajero. Cargarlo al contador de
		// `presupuesto` inflaría con tráfico perfectamente sano justo la serie que el operador usa para
		// decidir si sube WAPP_AGENT_INTENT_WAIT_MS — le haría girar una palanca contra un problema que no
		// existe. Es el mismo error que MotivoSinTexto vino a corregir en T1.8, y se evita igual: contador
		// propio, hecho distinto.
		return evt, veredicto{fragmento: true}
	}

	sobre, ok := app.LeerSobreClasificado(c.IntentJSON)
	if !ok {
		d.log.Warn("despachador: sobre de clasificación ilegible; el mensaje se entrega SIN intención",
			"session_id", d.sessionID, "id", c.ID, "seq", c.Seq, "wa_message_id", c.WAMessageID)
		return evt, veredicto{ilegible: true}
	}
	evt.Intent = &domain.ClassifiedIntent{
		Name:          sobre.Intent,
		Params:        sobre.Params,
		Confidence:    sobre.Confidence,
		ConfigVersion: sobre.ConfigVersion,
	}
	return evt, veredicto{}
}

// ─────────────────────────────────────────────────────────────────────────────
// Lectores de los contadores (INV-051.3) — las series que publicará la Ola 4
// ─────────────────────────────────────────────────────────────────────────────

// Despachados es el TOTAL de entregas que salieron bien (con o sin intención). Cuenta ENTREGAS: una
// re-entrega por sello fallido suma dos (ver `entregar`).
func (d *Despachador) Despachados() int64 { return d.despachados.Load() }

// ConIntent es cuántas entregas llevaron una intención real en el sobre.
func (d *Despachador) ConIntent() int64 { return d.conIntent.Load() }

// FragmentosDeLote es cuántas entregas fueron filas intermedias de un lote (sin sobre propio porque el
// intent vive en la última fila del turno). Es tráfico SANO: un número alto sólo dice que los clientes
// escriben en varios mensajes seguidos.
func (d *Despachador) FragmentosDeLote() int64 { return d.fragmentosDeLote.Load() }

// SobresIlegibles es cuántas veces el `intent_json` no se pudo leer como ninguna de las dos formas
// conocidas. CERO ES EL VALOR SANO: cualquier otra cosa es un sobre corrupto o un cambio de formato sin
// migrar, y el coste es una clasificación perdida por mensaje.
func (d *Despachador) SobresIlegibles() int64 { return d.sobresIlegibles.Load() }

// MetasIlegibles es cuántas veces el `meta_enc` descifrado no era JSON válido. CERO ES EL VALOR SANO: si
// crece, los mensajes están saliendo sin remitente ni push_name.
func (d *Despachador) MetasIlegibles() int64 { return d.metasIlegibles.Load() }

// PresupuestosVencidos es cuántas veces se agotó la espera al cajero y se pidió resolver la cabeza sin
// intención. CUENTA DISPAROS (vencimientos del reloj), no entregas.
//
// 🔴 NO ES LA MISMA SERIE QUE `OmitidosPorMotivo()[presupuesto]`, QUE CUENTA ENTREGAS, y las dos DEBEN
// poder diferir en los dos sentidos. Que alguien las «cuadre» es exactamente el error a evitar:
//
//   - vencimiento SIN entrega por presupuesto: el cajero ganó la carrera y `DespacharSinIntent` fue no-op
//     (lo confirma Store.DespachosSinIntentNoAplicados). La fila sale CON su intención real y se cuenta en
//     `con_intent`. Es el desenlace BUENO, y con un presupuesto de 4000 ms frente a una p95 medida de
//     3.736 ms no es un caso raro.
//   - entrega por presupuesto SIN vencimiento EN ESTE PROCESO: la fila la resolvió el despachador de una
//     EJECUCIÓN ANTERIOR (se selló `clasificado` con su sobre y el proceso murió antes de entregarla), y
//     este bucle se la encuentra ya resuelta al arrancar y la entrega. Es precisamente la propiedad
//     crash-safe que compró el arreglo del 2026-08-17; los contadores son por proceso, la cola es durable.
//   - y una re-entrega por sello fallido suma en `omitido_presupuesto` sin sumar aquí (ver `entregar`).
//
// SI CRECE, la palanca es WAPP_AGENT_INTENT_WAIT_MS o la latencia de Ollama, no este código.
func (d *Despachador) PresupuestosVencidos() int64 { return d.presupuestosVencidos.Load() }

// FallosLectura es cuántos polls no pudieron leer la cabeza (típicamente: la DEK de la sesión no se puede
// abrir). Mientras crece, ESA SESIÓN NO DRENA: es el contador que hay que mirar si una sesión se queda
// callada sin que nada más falle.
func (d *Despachador) FallosLectura() int64 { return d.fallosLectura.Load() }

// FallosEntrega es cuántas veces el sink rechazó la entrega. Con outbox durable cableado debería ser 0
// incluso con el stream caído (el sink encola en vez de fallar).
func (d *Despachador) FallosEntrega() int64 { return d.fallosEntrega.Load() }

// FallosSello es el TOTAL de sellos fallidos, y se conserva por compatibilidad de la serie. Para
// decidir hay que mirar las dos de abajo: sumadas no se pueden interpretar, porque sólo una de las dos
// tiene consecuencia aguas arriba.
func (d *Despachador) FallosSello() int64 {
	return d.fallosSelloPresupuesto.Load() + d.fallosSelloDespacho.Load()
}

// FallosSelloPresupuesto: falló `DespacharSinIntent`. La fila se reintenta en el poll siguiente y NADA
// sale mal aguas arriba. Es ruido operativo, no un incidente.
func (d *Despachador) FallosSelloPresupuesto() int64 { return d.fallosSelloPresupuesto.Load() }

// FallosSelloDespacho: falló `MarcarDespachada` tras una entrega CON ÉXITO. 🔴 Cada uno es **un
// duplicado en la nube**: el mensaje ya salió y la fila se releerá. Es el número que se mira.
func (d *Despachador) FallosSelloDespacho() int64 { return d.fallosSelloDespacho.Load() }

// CabezasAtascadas es cuántas filas quedaron de cabeza en un estado que esta versión no conoce. 🔴 CERO
// ES EL ÚNICO VALOR SANO: cada una es una sesión que dejó de drenar y no vuelve sola.
func (d *Despachador) CabezasAtascadas() int64 { return d.cabezasAtascadas.Load() }

// PollsCabezaAtascada dice si el atasco es HISTORIA o es AHORA: sigue creciendo mientras la sesión siga
// bloqueada. Un `CabezasAtascadas` > 0 con este número estable es un susto pasado; creciendo, es una
// sesión muerta en este instante.
func (d *Despachador) PollsCabezaAtascada() int64 { return d.pollsCabezaAtascada.Load() }

// Panicos es cuántas vueltas del bucle murieron en un pánico recuperado. CERO ES EL VALOR SANO y
// cualquier otra cosa es un bug: el aislamiento existe para que no tumbe el proceso, no para tolerarlo.
func (d *Despachador) Panicos() int64 { return d.panicos.Load() }

// OmitidosPorMotivo devuelve el desglose de despachos SIN intención, motivo a motivo, con las OCHO
// entradas de la lista canónica presentes SIEMPRE (un motivo a 0 es información: dice que ese camino no se
// está tomando).
//
// 🔴 NUNCA AGREGADO (INV-051.3): «se omitió» y «se omitió porque el breaker está abierto» son dos hechos
// operativos distintos y uno de ellos exige mirar Ollama. Devuelve una COPIA: el mapa interno es
// inmutable desde New y quien lo recibiera podría escribirlo.
//
// ⚠️ LA ENTRADA `presupuesto` CUENTA ENTREGAS, y NO tiene por qué cuadrar con `PresupuestosVencidos()`,
// que cuenta VENCIMIENTOS del reloj. Ver allí las tres formas legítimas en que difieren; ninguna es un
// bug y ninguna se debe «corregir» haciendo que una serie alimente a la otra.
func (d *Despachador) OmitidosPorMotivo() map[app.MotivoOmitido]int64 {
	out := make(map[app.MotivoOmitido]int64, len(d.omitidos))
	for _, m := range app.MotivosOmitido() {
		if c, ok := d.omitidos[m]; ok {
			out[m] = c.Load()
		}
	}
	return out
}

// Omitidos es el total de despachos sin intención POR UN SOBRE DE OMISIÓN (la suma del desglose). NO
// incluye fragmentos de lote ni sobres ilegibles: aquellos no llevaban sobre de omisión.
func (d *Despachador) Omitidos() int64 {
	var total int64
	for _, c := range d.omitidos {
		total += c.Load()
	}
	return total + d.omitidosFueraDeLista.Load()
}

// OmitidosFueraDeLista es cuántos sobres traían un motivo que esta versión no conoce (una cola escrita por
// un binario más nuevo). CERO ES EL VALOR SANO; si crece, hay un rollback a medias.
func (d *Despachador) OmitidosFueraDeLista() int64 { return d.omitidosFueraDeLista.Load() }

// contadores devuelve el bloque COMPLETO como pares clave/valor para el logger, con el desglose por
// motivo desplegado. Existe por la misma razón que su gemelo del cajero: que un contador nuevo se añada
// UNA vez y aparezca en todos los sitios donde se imprime el bloque.
//
// 🔴 INV-051.1: aquí no entra nada derivado del contenido de un mensaje. Todo son cuentas.
func (d *Despachador) contadores() []any {
	kv := []any{
		"session_id", d.sessionID,
		"despachados", d.Despachados(),
		"con_intent", d.ConIntent(),
		"omitidos", d.Omitidos(),
		"fragmentos_de_lote", d.FragmentosDeLote(),
		"sobres_ilegibles", d.SobresIlegibles(),
		"metas_ilegibles", d.MetasIlegibles(),
		"presupuestos_vencidos", d.PresupuestosVencidos(),
		"fallos_lectura", d.FallosLectura(),
		"fallos_entrega", d.FallosEntrega(),
		"fallos_sello", d.FallosSello(),
		// Desglosado: sólo el de despacho implica duplicados aguas arriba, y el bloque final de un
		// apagado es justo donde alguien va a leer este número.
		"fallos_sello_despacho", d.FallosSelloDespacho(),
		"fallos_sello_presupuesto", d.FallosSelloPresupuesto(),
		"cabezas_atascadas", d.CabezasAtascadas(),
		"polls_cabeza_atascada", d.PollsCabezaAtascada(),
		"panicos", d.Panicos(),
		"omitidos_fuera_de_lista", d.OmitidosFueraDeLista(),
	}
	// El desglose SIEMPRE desde la lista canónica y en su orden: ocho pares más, con o sin tráfico.
	for _, m := range app.MotivosOmitido() {
		if c, ok := d.omitidos[m]; ok {
			kv = append(kv, "omitido_"+string(m), c.Load())
		}
	}
	return kv
}
