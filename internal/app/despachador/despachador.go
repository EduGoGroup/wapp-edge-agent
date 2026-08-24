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
// antes que la N**. Un mensaje entregado fuera de orden es una conversación reordenada delante del
// cliente final, y eso no se arregla aguas arriba.
//
// ─── EL PRESUPUESTO DE ESPERA MURIÓ (Plan 044 · Ola 1.6 · T1.6-5 · ADR-0045 · D-044.31 · REQ-35) ───
//
// Hasta el 2026-08-23 este bucle RETENÍA la cabeza hasta 4 s (`WAPP_AGENT_INTENT_WAIT_MS`) esperando a que
// el worker-cajero le dejara un intent en la columna `intent_json`. El ADR-0045 invirtió la clasificación
// de PUSH a PULL —el Cloud PIDE la inferencia por el frame `inference_request` cuando la necesita— y con
// ella disolvió la espera entera: **ya no hay nada que esperar, así que ya no se espera**.
//
// 🔴 NO SE «BAJÓ EL PRESUPUESTO A CERO», SE BORRÓ EL MECANISMO, y la diferencia importa para quien lea
// esto buscando la palanca: no la hay. El número no estaba mal calibrado —la medida de campo fue 1 de 430
// inferencias dentro de la ventana, con descartes a 8 ms de llegar la etiqueta—; lo que estaba mal era
// acoplar la ENTREGA a la INFERENCIA. Desacoplarlas hace que la única pregunta de este bucle sea «¿hay
// cabeza?», y la respuesta se entrega en el acto.
//
// CONSECUENCIA EN LA COLA: sin espera, `clasificado` dejó de ser un estadio del ciclo (queda
// `nuevo → tomado → despachado`, ADR-0045 §Decisión.4) y este bucle entrega CUALQUIER cabeza que no esté
// ya `despachado`, sea cual sea su estado. Ver `vuelta`.
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
	// Ahora es el reloj INYECTABLE con el que se mide el ESPACIADO ENTRE RE-ENTREGAS (`reintentoEntrega`).
	// nil ⇒ time.Now.
	//
	// 🔴 YA NO MIDE NINGÚN PRESUPUESTO: ésa era su razón de ser hasta T1.6-5 y esa lógica murió con el
	// push. Sigue inyectable porque el backoff de la re-entrega es hoy la única lógica temporal de la
	// pieza, y un test que no pueda adelantar el reloj sólo puede comprobarla durmiendo — que es como se
	// escriben los tests que fallan en CI un martes de cada tres.
	Ahora func() time.Time
	// Despertador es cómo se espera entre dos miradas a la cola. nil ⇒ PollFijo(DefaultPollMS).
	Despertador Despertador
}

// reintentoEntrega recuerda cuántas veces seguidas ha fallado la entrega de UNA fila concreta y a
// partir de cuándo tiene sentido volver a intentarlo.
//
// Se limpia en cuanto una entrega sale bien, así que un fallo aislado —un corte de red que se cura—
// no deja rastro ni penaliza a la fila siguiente.
type reintentoEntrega struct {
	id int64
	// seq acompaña al id porque EL ROWID SOLO NO IDENTIFICA UNA FILA A LO LARGO DEL TIEMPO (T3.10): la
	// tabla no declara AUTOINCREMENT, y SQLite reutiliza el rowid de una fila borrada. Entre la poda TTL
	// y el tope de filas, borrar es rutina aquí. Si una fila nueva heredara el rowid de la que este bucle
	// venía reintentando, la comparación por id sola diría «es la misma fila» y la nueva NACERÍA FRENADA:
	// su primera entrega se saltaría por un backoff que nunca fue suyo.
	//
	// `seq` es monotónico por sesión y nunca se reusa, así que el par (id, seq) sí identifica. (La otra
	// mitad de este argumento vivía en `cabezaEnCurso.seq`, que murió con el presupuesto en T1.6-5; se
	// conserva aquí porque el peligro es el mismo y ahora éste es su único sitio.)
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

// Despachador drena la cola de UNA sesión, en orden de `seq`, y entrega al sink. Uno por sesión; entre
// sesiones son independientes y concurrentes.
type Despachador struct {
	cola        app.ColaDespachador
	sink        app.InboundSink
	sessionID   string
	log         sharedlogger.Logger
	ahora       func() time.Time
	despertador Despertador

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
	despachados atomic.Int64
	// intentsDescartados cuenta las filas ANTIGUAS que se drenan trayendo un sobre de clasificación REAL
	// escrito bajo el modelo push, y cuya intención YA NO PUEDE VIAJAR: el ADR-0045 retiró
	// `ClassifiedIntent` del proto, así que no existe campo donde ponerla. El mensaje sale igual y
	// completo; lo único que se pierde es una clasificación que ya estaba pagada.
	//
	// 🔴 CERO ES EL VALOR SANO Y ESPERADO, y crecer NO es un bug: es el rastro de las colas que venían
	// escritas por un binario anterior a T1.6-5 vaciándose. Si crece SOSTENIDAMENTE en un Edge que lleva
	// días con este binario, entonces sí es una señal — significa que algo sigue escribiendo sobres de
	// clasificación en `intent_json`, es decir que el push no está del todo muerto en esa máquina.
	intentsDescartados atomic.Int64
	sobresIlegibles    atomic.Int64
	metasIlegibles     atomic.Int64
	fallosLectura      atomic.Int64
	fallosEntrega      atomic.Int64
	panicos            atomic.Int64
	// fallosSelloDespacho: falló `MarcarDespachada` DESPUÉS de una entrega con éxito. Cada uno de éstos es
	// **un duplicado garantizado en la nube**: el mensaje ya salió y la fila se releerá.
	//
	// 🔴 ERAN DOS SERIES HASTA T1.6-5, y la otra —`fallosSelloPresupuesto`, el fallo de
	// `DespacharSinIntent`— murió con el presupuesto: ya no existe esa segunda escritura. Se separaron en
	// su día (T3.12) porque sólo UNA de las dos tiene consecuencia aguas arriba, y ésta es esa. El campo
	// `failed_seal_budget` del heartbeat sobrevive por contrato de proto y queda clavado a 0; ver
	// `health.DespachoStats.FallosSelloPresupuesto`.
	fallosSelloDespacho atomic.Int64
	// 🔴 LA CABEZA ATASCADA SE DISOLVIÓ CON EL PRESUPUESTO (T1.6-5), y aquí vivían sus dos contadores
	// (`cabezasAtascadas`, `pollsCabezaAtascada`). El atasco era ESTE caso y sólo éste: una fila que el
	// sello por presupuesto dejaba en un estado que esta versión no conocía se quedaba de cabeza para
	// siempre, porque `vuelta` sólo entregaba desde `clasificado`. Hoy `vuelta` entrega CUALQUIER cabeza
	// que no esté `despachado` —el estado ya no es una condición de entrega—, así que ningún estado
	// imprevisto puede retener a una sesión y no hay nada que contar.
	//
	// Los campos `stuck_heads` / `stuck_head_polls` del heartbeat sobreviven por contrato de proto y
	// quedan clavados a 0. Retirarlos es un cambio de proto y no es de esta tarea (T1.6-1).

	// omitidos desglosa por MOTIVO las filas que se drenan trayendo un sobre `{"omitido":…}`. Se construye
	// en New recorriendo app.MotivosOmitido() y NO se muta después, así que leerlo concurrentemente es
	// seguro.
	//
	// 🔴 BAJO PULL ESTE DESGLOSE SÓLO PUEDE MOVERSE CON FILAS ANTIGUAS. Ningún productor del Edge escribe
	// ya sobres de omisión (ADR-0045: el Edge no clasifica, así que no hay omisión que anotar); lo que
	// queda es DECODIFICAR las colas escritas por binarios anteriores a T1.6-5 mientras se vacían. Ver la
	// lista canónica en `internal/app/cola.go`, donde cada motivo dice desde cuándo está huérfano.
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
	d.log.Info("despachador: arrancando (drenado de la cola de entrantes, sin retención)",
		"session_id", d.sessionID,
	)

	for {
		if ctx.Err() != nil {
			return d.detener()
		}

		// progreso = «se movió la cabeza»: se entregó una fila. En ese caso se vuelve a mirar
		// INMEDIATAMENTE, sin pagar un poll: una ráfaga de mensajes sale de una tirada en vez de a razón de
		// uno cada 500 ms, que sería latencia inventada.
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
				"session_id", d.sessionID)
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
		// Sesión al día: el estado NORMAL de casi todos los polls.
		return false
	}

	// 🔴 SE ENTREGA SIN MIRAR EL `estado`, Y ESA AUSENCIA DE `if` ES LA TAREA ENTERA (T1.6-5, ADR-0045).
	// Hasta aquí había una bifurcación: `clasificado` ⇒ entregar, cualquier otra cosa ⇒ correr el
	// presupuesto y esperar. Bajo pull no hay a quién esperar, así que la cabeza sale en el acto y el
	// estado deja de ser una condición de entrega. Tres consecuencias que conviene tener escritas:
	//
	//  1. LAS FILAS ANTIGUAS SE DRENAN SOLAS, SIN MIGRACIÓN. Las colas escritas por un binario anterior
	//     traen filas en `clasificado` (y con su sobre en `intent_json`); aquí entran por la misma puerta
	//     que las nuevas y salen. Por eso T1.6-5 no necesitó tocar `migrations/cola/`.
	//  2. UNA FILA EN UN ESTADO IMPREVISTO YA NO PUEDE ATASCAR LA SESIÓN. Antes bloqueaba la cabeza para
	//     siempre —era la peor degradación del bucle, y tenía dos contadores propios—; ahora se entrega
	//     como cualquier otra. El fallo seguro cambió de «retener» a «entregar», que es el que esta cola
	//     quiere: se retrasa, nunca se pierde.
	//  3. UNA FILA `tomado` SE ENTREGA IGUAL. El claim con fencing sigue vivo (ADR-0038, ADR-0045 §4) y
	//     protege lo que siempre protegió —que dos procesos no cierren el mismo lote—, pero NO es un
	//     derecho de retención sobre la entrega y nunca lo fue.
	//
	// EL FRENO DE LA RE-ENTREGA (ver `reintentoEntrega`). Si esta misma fila viene fallando, se deja pasar
	// la vuelta sin llamar al sink: no es progreso, así que el bucle paga su poll y vuelve.
	if d.reintento.id == cabeza.ID && d.reintento.seq == cabeza.Seq &&
		d.ahora().Before(d.reintento.desde) {
		return false
	}
	return d.entregar(ctx, cabeza)
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
	// EL SWITCH SÓLO CLASIFICA LO QUE TRAÍA LA FILA, y bajo pull su rama dominante es la ÚLTIMA: una fila
	// nueva no lleva nada en `intent_json`, así que no incrementa ninguna de las series de sobre. Las tres
	// primeras ramas sólo las alcanzan las filas ANTIGUAS mientras las colas de campo se vacían.
	switch {
	case v.ilegible:
		d.sobresIlegibles.Add(1)
	case v.omitido:
		d.contarOmitido(v.motivo)
	case v.intentDescartado:
		d.intentsDescartados.Add(1)
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
//
// 🔴 SU CERO ES HOY EL CASO NORMAL: una fila escrita bajo pull no lleva sobre ninguno, así que las tres
// marcas de abajo sólo las levantan filas ANTIGUAS. Ver `evento`.
type veredicto struct {
	// motivo es el del sobre de omisión; vacío en los demás casos.
	motivo app.MotivoOmitido
	// omitido: el sobre era `{"omitido":…}` y el mensaje sale SIN intención.
	omitido bool
	// intentDescartado: el sobre traía una clasificación REAL, escrita bajo el modelo push, que ya no tiene
	// por dónde viajar (el ADR-0045 retiró `ClassifiedIntent` del proto). El mensaje sale entero; la
	// clasificación se tira. Ver `Despachador.intentsDescartados`.
	intentDescartado bool
	// ilegible: había sobre, pero no se pudo leer como ninguna de las dos formas conocidas.
	ilegible bool
}

// evento reconstruye el domain.InboundEvent de una fila de la cola. Es la traducción inversa exacta de
// `toInboundEvent` + `colaMeta` del listener.
//
// ─── LA REGLA DEL SOBRE, y por qué HOY sólo se aplica a filas ANTIGUAS (T1.6-5, ADR-0045) ───
//
// Bajo pull NADIE escribe ya en `intent_json`: el Edge no clasifica, así que no hay ni intención que
// anotar ni omisión que justificar. Una fila nacida con este binario llega aquí con la columna a NULL,
// SIEMPRE, y ése es el camino normal. Lo que sigue existiendo es la lectura, para drenar las colas que ya
// estaban escritas en el disco de un cliente cuando se actualizó el binario:
//
//  1. NULL (`TieneIntent == false`) ⇒ EL CASO NORMAL. Nada que contar, nada que decidir.
//  2. `{"omitido":"<motivo>"}` ⇒ se cuenta el MOTIVO y ya. El sobre nunca viajó al cable y sigue sin
//     hacerlo; lo que sale del Edge es el contador por motivo, en el heartbeat.
//  3. Un sobre del cajero legible (una clasificación REAL, ya pagada) ⇒ SE TIRA, y se cuenta en
//     `intentsDescartados`. 🔴 No hay dónde ponerla: el ADR-0045 retiró `ClassifiedIntent` del proto —de
//     `IncomingMessage` (11) y de `SensitivePayload` (5), ambos con `reserved`— porque bajo pull la
//     señal la pide el Cloud, no la empuja el Edge. Ésta es la ÚNICA pérdida de la migración, es acotada
//     (las filas ya escritas) y es de una etiqueta, jamás de un mensaje.
//  4. Cualquier otra cosa ⇒ se entrega igual y se cuenta como sobre ilegible. Nunca se retiene el mensaje
//     por no entender su sobre.
//
// 🔴 EL `evt.Intent` YA NO EXISTE. `domain.InboundEvent` perdió el campo en T1.6-5 y no es un descuido de
// limpieza: mientras existiera, rellenarlo aquí compilaba, no fallaba ningún test y no llegaba a ninguna
// parte — un dato que se produce y nadie consume es el fallo más caro de este repo, porque es invisible.
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
		// `meta.IsGroup` es CONSTANTEMENTE false en las filas NUEVAS desde el Plan 044 · Ola 1.5 · T1.5-3:
		// el listener corta el grupo en la puerta y no deja fila. Se sigue leyendo —no se retira— porque
		// las filas ANTIGUAS, las anotadas antes de esa tarea, lo llevan a `true` y tienen que decodificarse
		// igual. Un `true` que salga por aquí es una fila vieja drenándose, no una señal viva de que el Edge
		// esté atendiendo grupos.
		IsGroup: meta.IsGroup,
		// `meta.Sintetico` NO SE PROPAGA, y es una decisión (MP-10): domain.InboundEvent no tiene campo de
		// metadatos, así que traerlo obligaría a tocar el contrato de dominio Y el adaptador de CloudLink por un
		// dato que la nube YA recibe —el prefijo `SINTETICO-` del `WAMessageID`, que sí está en el proto—. Ver
		// app.ColaMeta.Sintetico: ese bool es la marca LOCAL, el ID es la portante.
	}

	if !c.TieneIntent {
		// EL CAMINO NORMAL BAJO PULL: la columna está a NULL porque nadie la escribe.
		//
		// ⚠️ HASTA T1.6-5 ESTE MISMO NULL SIGNIFICABA OTRA COSA y se contaba como `fragmentosDeLote`: era
		// una fila intermedia de un lote, de las que el cajero dejaba sin sobre propio porque el intent del
		// turno vivía en la última fila. Aquel contador se retiró aquí mismo. Mantenerlo habría sido peor
		// que quitarlo: subiría en CADA mensaje entrante y seguiría rotulado «fragmentos de lote», que es
		// exactamente la clase de contador que hace tomar decisiones al revés.
		return evt, veredicto{}
	}

	if motivo, ok := app.EsOmitido(c.IntentJSON); ok {
		return evt, veredicto{motivo: motivo, omitido: true}
	}

	if _, ok := app.LeerSobreClasificado(c.IntentJSON); !ok {
		d.log.Warn("despachador: sobre de clasificación ilegible; el mensaje se entrega igual",
			"session_id", d.sessionID, "id", c.ID, "seq", c.Seq, "wa_message_id", c.WAMessageID)
		return evt, veredicto{ilegible: true}
	}
	// Sobre bueno de una fila vieja: la clasificación se TIRA (ya no hay campo en el proto donde ponerla) y
	// se cuenta. El contenido NO se loguea —son params con texto literal del cliente (INV-051.1)—; lo único
	// que se publica es que ocurrió.
	return evt, veredicto{intentDescartado: true}
}

// ─────────────────────────────────────────────────────────────────────────────
// Lectores de los contadores (INV-051.3) — las series que publicará la Ola 4
// ─────────────────────────────────────────────────────────────────────────────

// Despachados es el TOTAL de entregas que salieron bien (con o sin intención). Cuenta ENTREGAS: una
// re-entrega por sello fallido suma dos (ver `entregar`).
func (d *Despachador) Despachados() int64 { return d.despachados.Load() }

// IntentsDescartados es cuántas filas ANTIGUAS se drenaron trayendo una clasificación real que ya no
// tiene por dónde viajar (T1.6-5, ADR-0045: `ClassifiedIntent` salió del proto). El mensaje salió entero;
// lo que se tiró fue la etiqueta.
//
// CERO ES EL VALOR ESPERADO EN RÉGIMEN, y un número que sube tras actualizar el binario y luego se
// estabiliza es exactamente lo que debe pasar: la cola vieja vaciándose. 🔴 Si sigue subiendo días
// después, algo está escribiendo sobres de clasificación en `intent_json` — es decir, el push sigue vivo
// en esa máquina.
func (d *Despachador) IntentsDescartados() int64 { return d.intentsDescartados.Load() }

// SobresIlegibles es cuántas veces el `intent_json` no se pudo leer como ninguna de las dos formas
// conocidas. CERO ES EL VALOR SANO: cualquier otra cosa es un sobre corrupto o un cambio de formato sin
// migrar, y el coste es una clasificación perdida por mensaje.
func (d *Despachador) SobresIlegibles() int64 { return d.sobresIlegibles.Load() }

// MetasIlegibles es cuántas veces el `meta_enc` descifrado no era JSON válido. CERO ES EL VALOR SANO: si
// crece, los mensajes están saliendo sin remitente ni push_name.
func (d *Despachador) MetasIlegibles() int64 { return d.metasIlegibles.Load() }

// FallosLectura es cuántos polls no pudieron leer la cabeza (típicamente: la DEK de la sesión no se puede
// abrir). Mientras crece, ESA SESIÓN NO DRENA: es el contador que hay que mirar si una sesión se queda
// callada sin que nada más falle.
func (d *Despachador) FallosLectura() int64 { return d.fallosLectura.Load() }

// FallosEntrega es cuántas veces el sink rechazó la entrega. Con outbox durable cableado debería ser 0
// incluso con el stream caído (el sink encola en vez de fallar).
func (d *Despachador) FallosEntrega() int64 { return d.fallosEntrega.Load() }

// FallosSelloDespacho: falló `MarcarDespachada` tras una entrega CON ÉXITO. 🔴 Cada uno es **un
// duplicado en la nube**: el mensaje ya salió y la fila se releerá. Es el número que se mira.
func (d *Despachador) FallosSelloDespacho() int64 { return d.fallosSelloDespacho.Load() }

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
// ⚠️ BAJO PULL NINGUNA DE LAS OCHO ENTRADAS TIENE PRODUCTOR VIVO en este Edge: sólo se mueven al drenar
// filas antiguas (ver `evento`). Las ocho claves siguen publicándose porque un motivo a 0 sigue siendo un
// dato, y porque el contrato del heartbeat las espera.
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
		"omitidos", d.Omitidos(),
		"intents_descartados", d.IntentsDescartados(),
		"sobres_ilegibles", d.SobresIlegibles(),
		"metas_ilegibles", d.MetasIlegibles(),
		"fallos_lectura", d.FallosLectura(),
		"fallos_entrega", d.FallosEntrega(),
		"fallos_sello_despacho", d.FallosSelloDespacho(),
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
