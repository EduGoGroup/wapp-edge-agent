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
// 🔴 INV-051.1: aquí no entra nada derivado del contenido. Dos cardinalidades.

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
	enqueueErrors atomic.Uint64
	enqueuePanics atomic.Uint64
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

// Snapshot devuelve la foto acumulada. Las dos lecturas NO son atómicas ENTRE SÍ: se puede leer un error
// contado y su pánico hermano aún no, si el snapshot cae justo en medio. Es aceptable y deliberado —son
// series independientes que se leen cada minuto para decidir si hay que ir a mirar, no un balance que
// tenga que cuadrar— y montar un candado para esto metería contención en el camino caliente a cambio de
// nada.
func (p *Puerta) Snapshot() PuertaStats {
	if p == nil {
		return PuertaStats{}
	}
	return PuertaStats{
		EnqueueErrors: p.enqueueErrors.Load(),
		EnqueuePanics: p.enqueuePanics.Load(),
	}
}
