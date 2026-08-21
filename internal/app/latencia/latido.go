package latencia

// latido.go — CÓMO SE LEE EL CRONÓMETRO EN CAMPO (Plan 051 Ola 3 · T3.13).
//
// El histograma sin emisor es un instrumento con la aguja tapada: los contadores solo se leerían cuando
// el proceso muere, que es justo cuando ya no sirven (misma lección que T2.6 en el cajero). Este fichero
// es la goroutine que los publica cada N y al parar.
//
// 🔴 LOS LOGS DEL VPS VAN A UN FICHERO (/root/source/wApp/logs/edge.log), NO A journald. Eso manda sobre
// la forma del bloque y no es negociable: la lectura de campo es
//
//	grep 'latencia del handler' /root/source/wApp/logs/edge.log | tail -3
//
// y lo que salga de ahí es lo que se pega crudo en el journal. Por eso el bloque es UNA SOLA LÍNEA por
// emisión, con un prefijo estable y único (msgLatido) y TODOS los campos en esa misma línea. Un bloque
// repartido en varias líneas obligaría a un `grep -A` que el operador no va a acertar a la primera.
//
// 🔴 INV-051.1: aquí no entra nada derivado del contenido de un mensaje. Duraciones y cuentas.

import (
	"context"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-shared/logger"
)

// msgLatido es el PREFIJO DE GREP, y es contrato con el runbook: cambiarlo deja mudo al operador que
// sigue el procedimiento escrito. Es idéntico en la emisión periódica y en la final a propósito (el campo
// `emision` las distingue): un solo grep tiene que traer las dos.
const msgLatido = "listener: latencia del handler de entrantes"

// Etiquetas del campo `emision`. Se declaran como constantes porque los tests afirman sobre ellas y una
// cadena suelta en dos sitios es una divergencia esperando a ocurrir.
const (
	emisionPeriodica = "periodica"
	emisionFinal     = "final"
)

// conteoTimeoutCierre acota el COUNT del bloque final. El bloque final se emite con el contexto del
// daemon YA CANCELADO (es lo que despertó al latido), así que la consulta va sobre un contexto DESLIGADO
// —si no, fallaría siempre y el bloque más interesante de todos saldría sin el estado de la cola—. Ese
// desligue necesita un plazo propio: sin él, un SQLite atascado en el apagado colgaría el cierre del
// proceso por una línea de observabilidad, que es exactamente al revés de lo que debe pesar.
const conteoTimeoutCierre = 2 * time.Second

// Deps es todo lo que el latido necesita. Se pasa por valor: no guarda estado entre emisiones salvo las
// dos fotos anteriores, que viven en la pila de Latido.
type Deps struct {
	// Hist es el histograma que llenan los listeners. nil ⇒ Latido no arranca (no hay nada que publicar).
	Hist *Histograma
	// Cada es la cadencia de la emisión periódica. <= 0 la DESACTIVA (el bloque final se emite igual).
	// Es el mismo criterio que el latido del cajero: el cero es un valor legítimo («cállate»), no un dedazo.
	Cada time.Duration
	// Log es a dónde sale el bloque. nil ⇒ Latido no arranca.
	Log logger.Logger
	// Cola cuenta lo pendiente. nil ⇒ el bloque sale SIN los campos de cola (y sin `conteo_ms`), que es lo
	// correcto: mejor un bloque más corto que unos ceros que se leen como «la cola está vacía».
	Cola app.ColaContador
	// DespachadorApagado dice si la PALANCA DE DIAGNÓSTICO de T3.17 está echada
	// (WAPP_AGENT_DESPACHADOR_APAGADO). No cambia NADA de lo que el latido mide: cambia lo que la línea
	// SIGNIFICA, y por eso se publica aquí y en el primer tramo del bloque.
	//
	// 🔴 POR QUÉ ESTE DATO VIVE EN EL LATIDO Y NO SOLO EN UN AVISO DE ARRANQUE. Con la palanca echada la
	// cola no drena y ningún entrante sube; el síntoma que se ve es `cola_pendientes` subiendo sin freno,
	// que es EXACTAMENTE el síntoma de un despachador roto, de una nube caída o de un Edge saturado. El
	// aviso del arranque explica eso una vez, hace tres días, en un fichero de log de cientos de miles de
	// líneas. Esta línea lo explica cada vez que alguien la mira, que es cuando hace falta.
	//
	// El cero (false) significa «activo», que es el estado sano: una Deps a medio cablear publica el
	// estado bueno, no una alarma falsa. La contrapartida —una palanca echada que nadie cableó aquí
	// saldría como `activo`— la custodia el test de cableado del daemon, que es donde nace el dato.
	DespachadorApagado bool
	// InyectorEntrantes dice si la PALANCA DE DIAGNÓSTICO de MP-10 Parte A está echada
	// (WAPP_AGENT_INYECTOR_ENTRANTES). Igual que su vecina de arriba, no cambia NADA de lo que el latido
	// mide: cambia lo que la línea SIGNIFICA, y por eso se publica aquí y en el primer tramo del bloque.
	//
	// 🔴 POR QUÉ ESTE DATO ES IMPRESCINDIBLE EN LA LÍNEA. Con el inyector encendido, la población del
	// histograma —`n`, `p50_ms`, `p99_ms`, todo— puede estar hecha de entrantes SINTÉTICOS fabricados dentro
	// del proceso, no de mensajes de clientes. Los dos casos producen exactamente la misma línea: mismos
	// campos, mismos buckets, mismo aspecto. Quien lea un `p99_ms` sin este campo no tiene forma de saber si
	// está mirando el rendimiento de producción o el de una tanda de prueba que alguien lanzó ayer — y el
	// criterio que se juzga con esta línea (INV-051.2) es precisamente sobre el camino de producción.
	//
	// El cero (false) significa «no hay inyector», que es el estado sano y el de todos los Edge en campo:
	// una Deps a medio cablear publica el estado bueno, no una alarma falsa. La contrapartida —una palanca
	// echada que nadie cableó aquí saldría como `no` en la línea— la custodia el test de cableado del
	// daemon, que es donde nace el dato.
	InyectorEntrantes bool
	// Ahora es el reloj, inyectable para los tests. nil ⇒ time.Now.
	Ahora func() time.Time
}

// Latido publica el bloque cada Deps.Cada y UNA VEZ MÁS al cancelarse el contexto. Bloquea hasta que el
// contexto muere: el llamante lo arranca en su propia goroutine y espera a que retorne antes de dar el
// proceso por cerrado (si no, el bloque final se pierde en la carrera con el `return`).
//
// El bloque FINAL se emite pase lo que pase, incluso con Cada <= 0. Es el que cierra la sesión de medida:
// con la emisión periódica apagada sigue siendo la única forma de saber qué pasó, y apagar el latido no
// puede significar apagar también el resumen.
func Latido(ctx context.Context, d Deps) {
	if d.Hist == nil || d.Log == nil {
		return
	}
	ahora := d.Ahora
	if ahora == nil {
		ahora = time.Now
	}
	inicio := ahora()
	desde := inicio
	// 🔴 LAS FOTOS ANTERIORES ARRANCAN EN CERO, NO EN UN Snapshot. Es deliberado: con un snapshot de
	// arranque, todo lo que se hubiera medido ANTES de que esta goroutine llegara a ejecutarse quedaría
	// fuera del primer intervalo Y fuera de todos los demás — muestras reales que no aparecerían en ninguna
	// ventana. Arrancando en cero, el primer intervalo cubre desde el inicio del proceso y NINGUNA muestra
	// se queda sin ventana. El coste es que el primer bloque arrastra el arranque en frío, que es
	// exactamente lo que el riesgo 3 del diseño manda tener presente (un p99 con n < 100 no es un dato).
	var prevEnc, prevDesc Muestra

	// Cada <= 0 ⇒ canal NIL, que en un select nunca está listo. Así el caso «desactivado» no necesita un
	// segundo camino de código (mismo truco que cajero.bucle), y de paso evita el pánico de
	// time.NewTicker con una duración no positiva.
	var tick <-chan time.Time
	if d.Cada > 0 {
		t := time.NewTicker(d.Cada)
		defer t.Stop()
		tick = t.C
	}

	for {
		select {
		case <-ctx.Done():
			// El contexto que trajo hasta aquí está muerto: el COUNT del bloque final necesita uno vivo.
			cierre, cancel := context.WithTimeout(context.WithoutCancel(ctx), conteoTimeoutCierre)
			d.Log.Info(msgLatido, d.bloque(cierre, emisionFinal, inicio, desde, ahora, &prevEnc, &prevDesc)...)
			cancel()
			return
		case <-tick:
			d.Log.Info(msgLatido, d.bloque(ctx, emisionPeriodica, inicio, desde, ahora, &prevEnc, &prevDesc)...)
			desde = ahora()
		}
	}
}

// bloque arma el bloque COMPLETO como pares clave/valor para el logger, y AVANZA las fotos anteriores.
//
// Existe —igual que Cajero.contadores— para que la emisión periódica y la final no puedan divergir: un
// campo nuevo se añade UNA vez y aparece en las dos. Que además mute `prevEnc`/`prevDesc` es lo que hace
// que el delta sea el del intervalo real entre emisiones y no una ventana inventada.
//
// 🔴 INV-051.1: cada campo de aquí es una cuenta, una duración o una etiqueta de bucket.
func (d Deps) bloque(ctx context.Context, emision string, inicio, desde time.Time, ahora func() time.Time,
	prevEnc, prevDesc *Muestra) []any {

	t := ahora()
	encAcum := d.Hist.Snapshot(Encolado)
	descAcum := d.Hist.Snapshot(Descartado)
	enc := Delta(*prevEnc, encAcum)
	desc := Delta(*prevDesc, descAcum)
	*prevEnc, *prevDesc = encAcum, descAcum

	args := []any{
		"emision", emision,
		// EL ESTADO DEL DESPACHADOR VA AL PRINCIPIO, y no es una preferencia estética: si está APAGADO,
		// todos los números que vienen detrás significan otra cosa (`cola_pendientes` sube porque nadie
		// drena, no porque haya carga). Leerlo al final obligaría a reinterpretar la línea entera hacia
		// atrás. Va SIEMPRE, como `n` y los contadores de la puerta: un `activo` explícito es un dato, y un
		// campo ausente sería la duda de si alguien dejó de mirarlo.
		"despachador", estadoDespachador(d.DespachadorApagado),
		// EL ESTADO DEL INYECTOR VA JUSTO DETRÁS, y por el mismo criterio exacto: si está ACTIVO, los
		// números que vienen después pueden no ser de tráfico real sino de entrantes fabricados dentro del
		// proceso. Es otro estado que cambia el SIGNIFICADO del resto de la línea, así que va en el primer
		// tramo y no al final. Va SIEMPRE: la pregunta que se hace en el VPS es «¿estos números son de
		// verdad?», y esa pregunta no se puede contestar con una ausencia.
		"inyector", estadoInyector(d.InyectorEntrantes),
		// La ventana REAL medida, no la pedida: en el bloque final (y tras un tick que se retrasó) las dos
		// no coinciden, y es la real la que da sentido a `n`.
		"ventana_ms", t.Sub(desde).Milliseconds(),
		"uptime_s", int64(t.Sub(inicio).Seconds()),
		// 🔴 `n` ES OBLIGATORIO Y VA ANTES QUE LOS PERCENTILES. Un p99 sin población no significa nada, y
		// la regla del journal es explícita: un p99 con n < 100 no es un dato. Sin este campo la línea no
		// es interpretable y el criterio de la ola no se puede juzgar con ella.
		"n", enc.N,
	}
	args = append(args, percentil(enc, 0.50, "p50_ms", "")...)
	args = append(args, percentil(enc, 0.95, "p95_ms", "")...)
	args = append(args, percentil(enc, 0.99, "p99_ms", "p99_bucket")...)

	// La serie de DESCARTES va aparte y con su propio `n`: es la población que explica de dónde salió el
	// número de arriba. Sin ella, un p99 estupendo medido sobre 4 mensajes en una ráfaga de 40.000
	// descartes se leería como un aprobado.
	args = append(args, "n_descartes", desc.N)
	args = append(args, percentil(desc, 0.99, "p99_ms_descartes", "")...)

	// El ACUMULADO arrastra el arranque en frío (la primera resolución de DEK de cada sesión, la
	// compilación de sentencias) y por eso no sustituye al intervalo; pero es lo único que sobrevive a que
	// nadie estuviera mirando la ventana en la que pasó lo interesante.
	args = append(args, "n_acum", encAcum.N)
	args = append(args, percentil(encAcum, 0.99, "p99_ms_acum", "")...)
	args = append(args, "max_ms_acum", encAcum.MaxMS())

	// LA PUERTA DE ENTRADA (T1.13, ampliada por el Plan 046 · T2.3). Van SIEMPRE, incluso en cero, y por dos
	// motivos distintos:
	//
	//   - un cero explícito es un DATO («no se ha degradado nada en toda la vida del proceso») y un campo
	//     ausente es una DUDA («¿no pasó nada, o dejó de mirarse?»). Es el mismo criterio que `n` y
	//     `cola_pendientes`, y el contrario que el de los percentiles —que sí se omiten con n=0, porque
	//     allí el cero se leería como «tardó 0 ms», que es una afirmación falsa y no una ausencia—;
	//   - van FUERA del bloque de `d.Cola`: estos contadores los lleva el instrumento en memoria y no
	//     dependen del COUNT ni de que el adaptador respalde el lado contador. Un Edge cuya cola no sepa
	//     contar sigue teniendo derecho a ver si está reofreciendo mensajes.
	//
	// `d.Hist` no puede ser nil aquí (Latido retorna antes si lo es), así que estos TRES campos no tienen
	// ninguna condición que los pueda dejar fuera de la línea.
	//
	// ⚠️ `cola_enqueue_errores` NO CUENTA MENSAJES PERDIDOS desde T1.13: cuenta mensajes que WhatsApp
	// tendrá que REOFRECER, porque lo que no deja fila tampoco se acusa. Sube = el Edge está reintentando.
	// `cola_enqueue_panics` es otra cosa: cualquier valor > 0 ahí es un defecto, no una condición de campo.
	//
	// `descartes_perfil_pasivo` (Plan 046 · T2.3) es el TERCERO y el único que NO es una degradación: son
	// los entrantes cortados en la puerta por venir a una sesión con perfil PASIVO (REQ-07), o sea la
	// configuración funcionando. Va aquí y no en otro tramo porque se llena desde el mismo sitio y con el
	// mismo instrumento; y va SIEMPRE, en cero incluido, por el mismo criterio que sus dos vecinos.
	//
	// 🔴 EL AVISO QUE NO SE PUEDE OMITIR: este contador LE QUITA CUENTA A LOS DESCARTES POR VENTANA. El
	// corte pasivo está en el paso 1.5 del handler, ANTES de la ventana del ADR-0037, así que un entrante de
	// una pasiva que además venía fuera de ventana se cuenta aquí y ya no allí. Quien vea bajar los
	// descartes por ventana sin leer este campo concluirá «el Edge ingiere mejor» cuando lo que pasa es que
	// hay más sesiones pasivas. Los dos números se leen a la vez o no se leen.
	//
	// ⚠️ Y ES DEL EDGE, NO DE UNA SESIÓN: para saber CUÁL está callada, `GET /v1/health` publica el mismo
	// contador por sesión como `dropped_passive`. Los tres mueren con el proceso (que se relanza solo desde
	// T5.4): un cero recién arrancado no dice «no pasó nada», dice «acabo de nacer» — `uptime_s` lo aclara.
	puerta := d.Hist.Puerta().Snapshot()
	args = append(args,
		"cola_enqueue_errores", puerta.EnqueueErrors,
		"cola_enqueue_panics", puerta.EnqueuePanics,
		"descartes_perfil_pasivo", puerta.DescartesPasivos,
	)

	if d.Cola != nil {
		t0 := ahora()
		p, err := d.Cola.Pendientes(ctx)
		// `conteo_ms` es EL EFECTO OBSERVADOR ESCRITO EN LA MISMA LÍNEA. El COUNT compite con el Enqueue
		// por la única conexión SQLite del Edge (SetMaxOpenConns(1)), así que quien lea el p99 tiene
		// derecho a ver cuánto costó mirarlo. Se emite también cuando el COUNT falla: un conteo lento que
		// acaba en error es justo el dato que hay que ver.
		conteoMS := ahora().Sub(t0).Milliseconds()
		if err != nil {
			args = append(args, "cola_error", err.Error())
		} else {
			args = append(args,
				"cola_pendientes", p.Total,
				"cola_nuevo", p.Nuevo,
				"cola_tomado", p.Tomado,
				"cola_clasificado", p.Clasificado,
			)
		}
		args = append(args, "conteo_ms", conteoMS)
	}
	return args
}

// Valores del campo `despachador` del bloque. Son constantes porque el runbook y los tests afirman sobre
// ellas, y una cadena suelta en dos sitios es una divergencia esperando a ocurrir (mismo criterio que las
// etiquetas de `emision`).
const (
	despachadorActivo = "activo"
	// El valor del estado APAGADO lleva dentro su propia explicación, y es deliberado: la lectura de campo
	// es un `grep … | tail -3` cuyo resultado se pega crudo en el journal, así que la línea tiene que
	// bastarse sola. Nadie va a ir a buscar qué significa una etiqueta de tres letras a las 3 de la mañana.
	// En el caso sano el campo mide seis caracteres; el texto largo solo aparece cuando hace falta.
	despachadorApagado = "APAGADO (palanca de diagnostico WAPP_AGENT_DESPACHADOR_APAGADO: se ENCOLA pero NO se drena; nada sube a la nube)"
)

// estadoDespachador traduce la palanca al valor del campo. Existe para que las dos constantes se nombren
// en un solo sitio y el `bloque` no cargue con un condicional.
func estadoDespachador(apagado bool) string {
	if apagado {
		return despachadorApagado
	}
	return despachadorActivo
}

// Valores del campo `inyector` del bloque (MP-10 Parte A). Constantes por el mismo motivo que las de
// `despachador`: el runbook y los tests afirman sobre ellas.
const (
	// El estado sano es corto y aburrido a propósito: la línea se emite cada minuto durante meses en todos
	// los Edge del ecosistema, y `no` son dos caracteres que no compiten con los números por la atención.
	inyectorInactivo = "no"
	// El valor del estado ACTIVO lleva dentro su propia explicación, igual que `despachadorApagado`, y aquí
	// la explicación importa MÁS que allí: la lectura de campo es un `grep … | tail -3` que se pega crudo en
	// el journal como evidencia de un criterio, así que la línea tiene que decir por sí sola que los números
	// que la acompañan pueden no ser de tráfico real. Un `si` a secas se leería y se olvidaría.
	inyectorActivo = "ACTIVO (palanca de diagnostico WAPP_AGENT_INYECTOR_ENTRANTES: hay entrantes SINTETICOS en la cola; " +
		"los numeros de esta linea pueden no ser de trafico real)"
)

// estadoInyector traduce la palanca al valor del campo. Existe para que las dos constantes se nombren en un
// solo sitio y el `bloque` no cargue con un condicional (mismo molde que estadoDespachador).
func estadoInyector(activo bool) string {
	if activo {
		return inyectorActivo
	}
	return inyectorInactivo
}

// percentil devuelve los pares clave/valor de un percentil, o NADA si la población está vacía.
//
// Omitir el campo con n=0 es deliberado: publicar un 0 ahí se leería como «tardó 0 ms», que es lo
// contrario de «no hubo muestras». El `n` que ya va en la línea es lo que explica la ausencia.
//
// claveBucket vacía ⇒ no se emite la etiqueta del tramo. Solo el p99 la lleva: es el único percentil sobre
// el que se juzga el criterio, y repetirla en los otros dos ensancharía la línea sin añadir decisión.
func percentil(m Muestra, q float64, clave, claveBucket string) []any {
	c, ok := Percentil(m, q)
	if !ok {
		return nil
	}
	if claveBucket == "" {
		return []any{clave, c.CotaMS}
	}
	return []any{clave, c.CotaMS, claveBucket, c.Bucket}
}
