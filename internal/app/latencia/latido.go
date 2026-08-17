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
