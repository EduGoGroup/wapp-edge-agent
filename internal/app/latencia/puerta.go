package latencia

// puerta.go — LOS CONTADORES DE LA PUERTA DE ENTRADA (Plan 051 · T1.13).
//
// 🔴 QUÉ AGUJERO CIERRA ESTE FICHERO, y no es de código: es de OBSERVABILIDAD. T1.13 cambió el
// comportamiento del Edge —un entrante que no deja fila ya NO se acusa, así que WhatsApp lo reenvía— y
// dejó ese cambio SIN forma de verlo. Los contadores que lo miden existían (`InboundStats` del listener,
// ADR-0037 §4) pero no los publicaba nadie: al barrer el repo, `Listener.InboundStats()` tenía ONCE
// llamantes y los once eran `_test.go`. Se incrementaban en memoria, por sesión, y morían con el proceso.
//
// El resultado en campo era un Edge con la cola rota reofreciendo entrantes en bucle y con la única
// huella de una línea de log POR MENSAJE —y las repetidas en Debug, o sea invisibles—. Cambiar una
// garantía sin dejar cómo observarla es medio arreglo.
//
// ─────────────────────────────────────────────────────────────────────────────
// POR QUÉ ESTOS CONTADORES SON DEL EDGE Y NO DE LA SESIÓN
// ─────────────────────────────────────────────────────────────────────────────
// Los `InboundStats` del listener son POR SESIÓN; el bloque del latido es POR EDGE. Había que elegir, y se
// eligió SUMAR entre sesiones, con dos razones y un precio reconocido:
//
//   - LA LÍNEA TIENE QUE SER DE FORMA FIJA. El bloque es una sola línea `grep`-able cuyo contrato con el
//     runbook es que salga siempre igual (ver la cabecera de latido.go). Publicar por sesión la haría de
//     ancho variable —dos campos por sesión viva, con nombres que cambian— y eso rompe justo lo que hace
//     útil al bloque: que un `grep | tail -3` se pegue crudo en el journal y se entienda.
//   - LA IDENTIDAD DE LA SESIÓN ROTA YA ESTÁ EN EL LOG, y con más contexto del que cabría aquí: el
//     adaptador de la cola grita UNA vez por sesión y por ventana de enfriamiento con su `session_id` y su
//     causa (ver el throttle de colaentrantes). Este bloque responde «¿está pasando, y cuánto?»; esa otra
//     línea responde «¿a quién?». Duplicar la segunda pregunta aquí costaría el formato y no añadiría un
//     dato nuevo.
//
// EL PRECIO, dicho sin adornos: un contador que sube NO dice qué sesión está rota. Con varias sesiones
// vivas hay que ir a la línea del adaptador para saberlo. Se acepta a cambio del formato fijo.
//
// ⚠️ TRAMPA DE NOMBRES, y por esta ya casi se pasa alguien: `InboundStatsEveryMS` (config) gobierna la
// CADENCIA DE ESTE BLOQUE y nada más. NO tiene relación con `whatsmeow.InboundStats`, que es el acumulado
// por sesión del listener —el que no publicaba nadie— pese a llamarse casi igual. Comparten prefijo y no
// comparten nada: quien busque «InboundStats» en la config y deduzca que aquello se publica, deducirá mal.
//
// 🔴 INV-051.1: aquí no entra nada derivado del contenido. Cuatro cardinalidades (el Plan 046 · T2.3 sumó
// la tercera —los descartes por perfil pasivo— y el Plan 044 · T1.5-3 la cuarta: los descartes de GRUPO).

import "sync/atomic"

// PuertaStats es la foto de los contadores. Cardinalidades, sin PII.
type PuertaStats struct {
	// EnqueueErrors son los entrantes que NO dejaron fila porque Enqueue devolvió error (disco lleno, DEK
	// ausente, BD bloqueada).
	//
	// ⚠️ NO SON BAJAS. Desde T1.13 ese entrante tampoco se acusa, así que sigue vivo en WhatsApp y vuelve
	// en el siguiente intento: esto mide MENSAJES QUE WHATSAPP TENDRÁ QUE REOFRECER. Un valor que sube es
	// un Edge que está reintentando, no un Edge que está perdiendo — y aun así hay que ir a mirarlo,
	// porque un fallo permanente reofrece en bucle.
	EnqueueErrors uint64
	// EnqueuePanics son los pánicos RECUPERADOS del camino de encolado. Separado a propósito: cualquier
	// valor > 0 aquí es un DEFECTO, no una condición de campo, y confundirlo con lo de arriba borraría esa
	// diferencia justo cuando importa.
	EnqueuePanics uint64
	// DescartesPasivos son los entrantes que la puerta descartó por venir a una sesión con PERFIL PASIVO
	// (Plan 046 · Ola 2 · T2.3, REQ-07/REQ-11). Es el TERCER contador de la puerta y el primero que NO es
	// una degradación: los dos de arriba son cosas que van mal; este es la configuración funcionando.
	//
	// 🔴 SE PUBLICA IGUAL, Y POR LA RAZÓN CONTRARIA A LAS OTRAS DOS. El descarte por perfil pasivo no deja
	// fila, no sube al cable y ACUSA a WhatsApp exactamente igual que si hubiera entregado: para cualquier
	// observador externo es indistinguible de «a esa sesión no le escribió nadie». Este número es la única
	// prueba de que el filtro existe y de cuánto corta — y, por tanto, la única forma de ver un filtro roto
	// (0 con tráfico) o un filtro que corta de más (sube en una sesión que debería estar activa).
	//
	// 🔴 LEER JUNTO A `n_descartes` Y AL ACUMULADO POR VENTANA. El corte pasivo vive en el paso 1.5 del
	// handler, ANTES de la ventana temporal del ADR-0037, así que un entrante de una pasiva que además venía
	// fuera de ventana se cuenta AQUÍ y deja de contarse como descarte por ventana. Consecuencia práctica:
	// una CAÍDA de los descartes por ventana puede no significar «el Edge ingiere mejor» sino «hay más
	// sesiones pasivas». Los dos números solo se interpretan a la vez.
	//
	// ⚠️ ES DEL EDGE ENTERO, no de una sesión (misma decisión que los dos de arriba: la línea tiene que ser
	// de forma fija). Para saber QUÉ sesión está callada hay que ir a `GET /v1/health`, donde el mismo
	// contador sale por sesión como `dropped_passive`.
	//
	// ⚠️ VIVE EN MEMORIA Y MUERE CON EL PROCESO, que desde T5.4 del Plan 051 se relanza solo. Un 0 recién
	// arrancado no dice «no descartó nada»: dice «este proceso acaba de nacer». `uptime_s` va en la misma
	// línea justamente para poder distinguirlo.
	DescartesPasivos uint64
	// DescartesGrupo son los entrantes que la puerta descartó por venir de un GRUPO (Plan 044 · Ola 1.5 ·
	// T1.5-3, REQ-36/D-044.30). Es el CUARTO contador de la puerta y el segundo que NO es una degradación:
	// es, igual que el de arriba, el filtro funcionando.
	//
	// 🔴 QUÉ CAMBIÓ EL DÍA QUE NACIÓ ESTE CONTADOR, porque leer la serie sin saberlo lleva a la conclusión
	// contraria. Hasta T1.5-3 un entrante de grupo SÍ dejaba fila: nacía `clasificado` con la marca
	// `no_elegible` y viajaba a la nube como cualquier otro, solo que sin intención. Desde T1.5-3 no deja
	// nada: ni fila, ni entrega, ni un byte en disco. Este número es la ÚNICA prueba de que ese tráfico
	// existe — y su contrapartida es que la marca `no_elegible` deja de aparecer en filas nuevas.
	//
	// 🔴 SE ACUSA A WHATSAPP IGUAL (D-044.30, detalle 1), así que desde fuera el descarte es indistinguible
	// de «a ese grupo no le escribió nadie». Misma lección que el descarte pasivo: sin este campo, un filtro
	// roto (0 con tráfico de grupos) o un filtro que corta de más son invisibles.
	//
	// ⚠️ LE QUITA CUENTA A `DescartesPasivos` NO; le quita cuenta a las FILAS. El corte de grupo va DESPUÉS
	// del pasivo y DESPUÉS de la ventana (paso 5 de la puerta), así que no roba cuenta a ninguna de las dos
	// series de descarte: lo que baja cuando este sube es el volumen de la cola durable.
	//
	// ⚠️ ES DEL EDGE ENTERO y VIVE EN MEMORIA, como sus tres vecinos: un 0 recién arrancado dice «acabo de
	// nacer», no «no descarté nada». `uptime_s` va en la misma línea para poder distinguirlo.
	DescartesGrupo uint64
}

// Puerta lleva los contadores de degradación de la puerta de entrada, COMPARTIDOS por todas las sesiones
// del Edge. Vive dentro del Histograma (ver Histograma.Puerta) por una razón práctica: ese es el único
// objeto que YA viaja hasta cada Listener y que el latido YA lee, así que publicar esto no ha necesitado
// ni un puerto nuevo, ni una opción nueva, ni tocar el cableado del daemon — y, sobre todo, hereda su
// custodia: el test que afirma que el cronómetro llega al Listener afirma también que llegan estos.
//
// Todos sus métodos son nil-safe, por el mismo criterio que el histograma: un instrumento de medida no
// puede ser jamás la causa de que se caiga la escucha.
type Puerta struct {
	enqueueErrors    atomic.Uint64
	enqueuePanics    atomic.Uint64
	descartesPasivos atomic.Uint64
	descartesGrupo   atomic.Uint64
}

// AnotaEnqueueError suma un entrante que no dejó fila por error del Enqueue. Corre en el hilo de
// whatsmeow: un solo atomic.Add, sin locks ni asignaciones.
func (p *Puerta) AnotaEnqueueError() {
	if p == nil {
		return
	}
	p.enqueueErrors.Add(1)
}

// AnotaEnqueuePanic suma un pánico recuperado del camino de encolado.
func (p *Puerta) AnotaEnqueuePanic() {
	if p == nil {
		return
	}
	p.enqueuePanics.Add(1)
}

// AnotaDescartePasivo suma un entrante descartado en la puerta por PERFIL PASIVO de su sesión (Plan 046 ·
// T2.3). Corre en el hilo de whatsmeow, igual que sus dos hermanas: un solo atomic.Add, sin locks ni
// asignaciones. Nil-safe por el mismo criterio que el resto (un instrumento de medida jamás puede ser la
// causa de que se caiga la escucha).
func (p *Puerta) AnotaDescartePasivo() {
	if p == nil {
		return
	}
	p.descartesPasivos.Add(1)
}

// AnotaDescarteGrupo suma un entrante descartado en la puerta por venir de un GRUPO (Plan 044 · Ola 1.5 ·
// T1.5-3, REQ-36). Corre en el hilo de whatsmeow, igual que sus tres hermanas: un solo atomic.Add, sin
// locks ni asignaciones. Nil-safe por el mismo criterio que el resto (un instrumento de medida jamás puede
// ser la causa de que se caiga la escucha).
func (p *Puerta) AnotaDescarteGrupo() {
	if p == nil {
		return
	}
	p.descartesGrupo.Add(1)
}

// Snapshot devuelve la foto acumulada. Las cuatro lecturas NO son atómicas ENTRE SÍ: se puede leer un error
// contado y su pánico hermano aún no, si el snapshot cae justo en medio. Es aceptable y deliberado —son
// series independientes que se leen cada minuto para decidir si hay que ir a mirar, no un balance que
// tenga que cuadrar— y montar un candado para esto metería contención en el camino caliente a cambio de
// nada.
func (p *Puerta) Snapshot() PuertaStats {
	if p == nil {
		return PuertaStats{}
	}
	return PuertaStats{
		EnqueueErrors:    p.enqueueErrors.Load(),
		EnqueuePanics:    p.enqueuePanics.Load(),
		DescartesPasivos: p.descartesPasivos.Load(),
		DescartesGrupo:   p.descartesGrupo.Load(),
	}
}
