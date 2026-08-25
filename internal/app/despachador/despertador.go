package despachador

import (
	"context"
	"time"
)

// despertador.go — CÓMO ESPERA el despachador entre dos miradas a la cola (Plan 051 Ola 3 · T3.3).
//
// 🔴 ES UN GEMELO DECLARADO de internal/app/cajero/despertador.go, NO un descuido. La pieza es la misma
// de veinte líneas y la tentación evidente es importar la del cajero; se decidió replicarla, y el porqué
// es de DEPENDENCIAS, no de estilo:
//
//   - El paquete `cajero` arrastra consigo el clasificador, el circuit breaker, el cliente de Ollama y la
//     lectura de afinidades de CPU en /proc. El despachador vive DENTRO del daemon `agent serve`; el
//     cajero es un PROCESO APARTE (el hijo de `wapp-ctl`). Importarlo desde aquí metería todo ese árbol
//     —incluida una pieza que sólo funciona en Linux— en el binario que corre 24/7 en la máquina del
//     cliente, a cambio de no repetir un `time.Timer`. Es un mal negocio.
//   - La alternativa limpia sería un paquete tercero (`internal/app/poll`) del que dependieran los dos,
//     pero eso obliga a reescribir el cableado del cajero, que tiene otro dueño en esta ola. Queda
//     anotado como la refactorización correcta si aparece un TERCER usuario; con dos, el coste de la
//     indirección supera al de la copia.
//
// ⚠️ T1.8-7 TAMPOCO TOCA EL DESPERTADOR DEL CAJERO, y conviene dejar escritas las razones VERIFICADAS
// porque la que se da de memoria —«ese poll barre los leases vencidos»— es FALSA: los leases los barre
// `barrerLeases` con su PROPIO `time.Ticker(c.lease)` (cajero/cajero.go:1110-1112), que no le pregunta
// nada a este despertador. Las razones de verdad son dos:
//
//  1. BAJO PULL EL CAJERO YA NO SONDEA NINGUNA COLA (ADR-0045). Su bucle (cajero/cajero.go:836) conserva
//     el despertador como RELOJ de dos latidos de telemetría y como costura de test —lo dice su propio
//     comentario, cajero/cajero.go:832-835—, no como disparador de trabajo: el trabajo le llega empujado
//     por `inference_request` (cajero/servidor.go). No hay ahí ningún `Enqueue` cuya latencia acortar.
//  2. NO COMPARTE PROCESO CON QUIEN ESCRIBE SU ENTRADA (`agent serve` y `agent cajero` son dos procesos,
//     cmd/agent/main.go:103,134). Un canal de Go NO le serviría — que es exactamente la simetría inversa
//     de la que hace que aquí sí sirva.
//
// ─── EL VEREDICTO DE T2.7: SIGUE EN PIE, Y SU CONCLUSIÓN SE REABRE CON OTRO MECANISMO ───
//
// 🔴 HASTA EL 2026-08-24 AQUÍ PONÍA «EL VEREDICTO MEDIDO DE T2.7 APLICA IDÉNTICO Y NO SE REABRE AQUÍ:
// poll de intervalo FIJO, no `PRAGMA data_version`», y el argumento contra esa sonda era que «cambia
// cuando OTRA conexión escribió el fichero, y el despachador comparte proceso con el listener que encola
// — es decir, no vería precisamente las escrituras que más le importan, las de su propia sesión».
//
// ESE ARGUMENTO SIGUE SIENDO CIERTO Y ES, LITERALMENTE, LA RAZÓN DEL CAMBIO (Plan 044 · Ola 1.8 ·
// T1.8-7). Lo que se introduce NO es `data_version` ni ninguna otra sonda sobre el fichero: es un CANAL
// DE GO (`AvisoConRespaldo`, abajo), que ve EXACTAMENTE esas escrituras PRECISAMENTE PORQUE comparten
// proceso. Es la objeción del 051 vuelta del revés, no su contradicción: la MEDICIÓN de T2.7 no se
// discute — se aplica a un mecanismo distinto, que la medición nunca midió.
//
// Y EL POLL NO CAMBIA DE VALOR, CAMBIA DE PAPEL. `DefaultPollMS` sigue siendo 500 y no se toca (el 051 lo
// prohíbe explícitamente: HANDOFF-CLI-O3-2026-08-17.md:396). Lo que deja de ser es el DISPARADOR: con el
// aviso cableado quien despierta al bucle es el `Enqueue` del listener, y el intervalo pasa a ser el
// RESPALDO largo de `DefaultRespaldoMS`. Sin aviso cableado —tests, cableados que no vienen del daemon—
// manda `PollFijo(DefaultPollMS)` exactamente igual que antes de esta tarea.

// DefaultPollMS es el intervalo por defecto entre dos miradas a la cabeza de la sesión, en milisegundos.
// Es el MISMO número que el poll del cajero y por la misma razón: medio segundo de espera es ruido frente
// al coste de una inferencia (p95 medida de 3.736 ms) y deja el proceso a ~2 consultas por segundo y por
// sesión contra un SQLite local cuando la cola está vacía, que es el estado normal.
//
// ⚠️ AQUÍ SE MULTIPLICA POR EL NÚMERO DE SESIONES VIVAS, cosa que en el cajero no pasa (allí hay un solo
// bucle para toda la máquina). Con las cardinalidades del Edge —unidades de sesiones por caja— eso son
// unas pocas consultas por segundo sobre un índice; si un día una caja llevara decenas de sesiones, la
// palanca no es subir este número sino el índice `(session_id, seq)` que anota sqlCabezaDeSesion.
const DefaultPollMS = 500

// Despertador es CÓMO el despachador espera a que haya trabajo. Es interfaz por la misma costura que en
// el cajero: el bucle no sabe nada del mecanismo, y los tests inyectan un despertador MANUAL gobernado
// por un canal para que la sincronía sea determinista y no dependa de dormir.
type Despertador interface {
	// Esperar bloquea hasta que toque volver a mirar la cola, o hasta que ctx se cancele. Devuelve nil
	// cuando hay que volver a mirar y el error del contexto cuando la sesión se está parando: el bucle usa
	// ESE error como su señal de parada, no un bool.
	Esperar(ctx context.Context) error
}

// PollFijo espera un intervalo fijo. Implementación de partida (y única hoy) del Despertador.
type PollFijo struct {
	intervalo time.Duration
}

var _ Despertador = (*PollFijo)(nil)

// NewPollFijo construye el poll de intervalo fijo. Un intervalo <= 0 cae a DefaultPollMS: un poll de 0
// convertiría el bucle en una espera activa que quemaría un core POR SESIÓN con la cola vacía.
func NewPollFijo(intervalo time.Duration) *PollFijo {
	if intervalo <= 0 {
		intervalo = DefaultPollMS * time.Millisecond
	}
	return &PollFijo{intervalo: intervalo}
}

// Esperar duerme el intervalo, o vuelve antes con el error del contexto si la sesión se para. Usa un
// Timer con Stop en defer y no time.Sleep para que el apagado no tenga que esperar al tick pendiente —
// que es, literalmente, la mitad del criterio «ninguna entrega ocurre después de cancelar el ctx».
func (p *PollFijo) Esperar(ctx context.Context) error {
	t := time.NewTimer(p.intervalo)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Intervalo devuelve el intervalo configurado (para el log de arranque y los tests).
func (p *PollFijo) Intervalo() time.Duration { return p.intervalo }

// ─────────────────────────────────────────────────────────────────────────────────────────────────────
// EL AVISO (Plan 044 · Ola 1.8 · T1.8-7)
// ─────────────────────────────────────────────────────────────────────────────────────────────────────

// DefaultRespaldoMS es el intervalo del RESPALDO de AvisoConRespaldo, en milisegundos: cada cuánto vuelve
// a mirar la cola un despachador que YA tiene el aviso cableado y no ha recibido ninguno.
//
// 🔴 NO ES «EL POLL SUBIDO A 5 s», Y LA DIFERENCIA ES TODO EL SENTIDO DE LA TAREA. `DefaultPollMS` no se
// mueve: sigue en 500 ms y sigue gobernando a un despachador SIN aviso. Este número gobierna otra cosa —
// una RED DE SEGURIDAD— y por eso puede ser diez veces mayor sin empeorar ninguna latencia: el camino
// normal (el entrante que llega por el listener) ya no espera a ningún tick, lo despierta el `Enqueue`.
//
// POR QUÉ 5 s, con los dos lados del argumento:
//
//   - EL SUELO lo pone lo que el respaldo tiene que cubrir de verdad: las escrituras que NO pasan por el
//     listener de ESTE proceso — `cmd/colaseed`, y cualquier otro escritor de la cola desde fuera. Para
//     ésas el canal no existe (el aviso es intra-proceso por construcción, ver la cabecera) y el respaldo
//     es el ÚNICO camino, así que su latencia PEOR es exactamente este número. Cinco segundos para la fila
//     sintética de una medición son ruido; el criterio (d) de la tarea se fija midiendo justo eso.
//   - EL TECHO lo pone la VENTANA DE AGREGACIÓN del Cloud: un job cierra a los 45 s de silencio desde el
//     último mensaje (D-044.37). Un respaldo por encima de ese número podría hacer que una fila llegara
//     DESPUÉS de que su propia ráfaga se hubiera cerrado — o sea, cambiaría el resultado en vez de sólo
//     retrasarlo. Con 5 s queda casi un orden de magnitud de margen.
//
// NO TIENE KNOB DE CONFIG, igual que DefaultPollMS: es una constante. Si algún día hay que moverlo en
// campo, la conversación es la misma que la del poll y se tiene entera, no por una variable de entorno
// que nadie recuerda haber puesto (ver el hallazgo del `.env` del VPS pisando un default recalibrado).
const DefaultRespaldoMS = 5000

// AvisoConRespaldo es el Despertador de T1.8-7: el bucle despierta por AVISO —un canal que el listener
// toca justo después de que el INSERT del entrante haya vuelto sin error— y, si no llega ninguno, por un
// RESPALDO de intervalo fijo.
//
// 🔴 EL AVISO ES UNA PISTA; LA VERDAD DURABLE SIGUE SIENDO EL `INSERT`. Nada de lo que decide este tipo
// puede perder una fila, y esa propiedad no depende de que el canal sea fiable, sino de que el bucle
// DRENE TODO lo pendiente cada vez que despierta (Despachador.Run vuelve a mirar sin pagar espera mientras
// haya progreso) y de que la primera mirada ocurra ANTES del primer Esperar. Un aviso de más hace una
// vuelta en vacío; un aviso de menos lo cubre el respaldo.
//
// ─── POR QUÉ NO HAY DESPERTAR PERDIDO, que es la pregunta que este patrón siempre invita a hacer ───
//
// El escritor manda con buffer 1 y envío NO BLOQUEANTE (`select { case ch <- struct{}{}: default: }`, ver
// `Listener.avisar`), así que un aviso se puede DESCARTAR. No importa, y el argumento es cerrado:
// descartar sólo ocurre si el buffer estaba lleno en ese instante, es decir, si hay un aviso PENDIENTE que
// nadie ha recibido todavía. Ese aviso pendiente se recibirá en algún Esperar POSTERIOR al descarte, que a
// su vez es posterior al INSERT que lo provocó — y al volver de ese Esperar el bucle hace una mirada
// COMPLETA a la cola, que por tanto ve la fila. La fila nunca se queda esperando a un aviso que ya se usó.
//
// ─── LA CARRERA DE ARRANQUE TAMBIÉN ESTÁ CUBIERTA, Y POR DOS VÍAS ───
//
// En `sessionmgr` el listener arranca ANTES que el despachador (listen.go, `go m.runListener` precede a
// `m.startDespachador`), así que un entrante puede encolarse cuando todavía no hay nadie escuchando el
// canal. No se pierde: (1) el buffer 1 GUARDA ese aviso hasta que el despachador nace y llega a su primer
// Esperar, y (2) `Run` hace una `vuelta` completa ANTES del primer Esperar, o sea que el drenado de
// arranque existía ya y sigue existiendo. Con una de las dos bastaría; están las dos.
//
// ⚠️ UN `aviso` NIL NO ES UN ERROR PERO SÍ ES UNA DEGRADACIÓN SILENCIOSA: un canal nil nunca está listo en
// un select, así que este despertador se convierte en un poll puro del intervalo del respaldo — es decir,
// en `PollFijo(5 s)` en vez del `PollFijo(500 ms)` de siempre. Por eso el cableado de producción
// (sessionmgr/despacho.go) NO construye este tipo cuando la sesión no tiene canal: deja el `Despertador`
// a nil y manda el default de `New`, que es el poll de 500 ms. Se acepta el nil aquí para no convertir un
// constructor en algo que puede fallar, pero el sitio donde eso se decide es aquél, no éste.
type AvisoConRespaldo struct {
	// aviso es el lado RECEPTOR del canal. Sólo se lee: quien decide si un aviso se manda o se descarta es
	// el escritor, y esa asimetría es el patrón entero (el camino caliente de whatsmeow no puede bloquearse
	// esperando a que este bucle esté escuchando).
	aviso <-chan struct{}
	// respaldo es cada cuánto se mira la cola aunque no llegue ningún aviso. Ver DefaultRespaldoMS.
	respaldo time.Duration
}

var _ Despertador = (*AvisoConRespaldo)(nil)

// NewAvisoConRespaldo construye el despertador por aviso. Un respaldo <= 0 cae a DefaultRespaldoMS, por la
// misma razón que en NewPollFijo: un intervalo de 0 convertiría el bucle en una espera activa que quemaría
// un core POR SESIÓN con la cola vacía. Un `aviso` nil se acepta y degrada a poll puro (ver el tipo).
func NewAvisoConRespaldo(aviso <-chan struct{}, respaldo time.Duration) *AvisoConRespaldo {
	if respaldo <= 0 {
		respaldo = DefaultRespaldoMS * time.Millisecond
	}
	return &AvisoConRespaldo{aviso: aviso, respaldo: respaldo}
}

// Esperar bloquea hasta que llegue un aviso, venza el respaldo o se cancele el ctx.
//
// Es el mismo molde que `PollFijo.Esperar` con un `case` más, y conserva sus dos propiedades: el Timer con
// Stop en defer (para que el apagado no tenga que esperar al tick pendiente, que es la mitad del criterio
// «ninguna entrega ocurre después de cancelar el ctx») y el `ctx.Err()` como señal de parada.
//
// 🔴 EL `case <-ctx.Done()` NO GARANTIZA PRIORIDAD y no hace falta que la garantice: `select` elige al azar
// entre los casos listos, así que una cancelación simultánea a un aviso puede resolverse por el aviso. El
// bucle lo absorbe — `Run` comprueba `ctx.Err()` al principio de cada vuelta y otra vez después de ella—,
// que es exactamente el mismo trato que ya recibe el `case <-t.C` de PollFijo.
func (a *AvisoConRespaldo) Esperar(ctx context.Context) error {
	t := time.NewTimer(a.respaldo)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.aviso:
		return nil
	case <-t.C:
		return nil
	}
}

// Respaldo devuelve el intervalo del respaldo configurado (para el log de arranque y los tests).
func (a *AvisoConRespaldo) Respaldo() time.Duration { return a.respaldo }
