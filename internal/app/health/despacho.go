// despacho.go — EL PUERTO DE LOS CONTADORES DEL DESPACHADOR (Plan 051 Ola 4 · T4.0, dueña de INV-051.3).
//
// EL PROBLEMA QUE CIERRA, y que no era el que parecía. El despachador de cada sesión lleva desde la Ola 3
// el desglose completo de sus omisiones (`OmitidosPorMotivo`, ocho motivos) y los cuatro contadores de
// atasco/sello. Nada de eso llegaba al heartbeat ni al plano de control — pero NO porque faltara un tubo:
// `despachador.New` devolvía una `d` que era una VARIABLE LOCAL dentro de `sessionmgr.startDespachador`, y
// nadie la guardaba. Faltaba una REFERENCIA. Este fichero declara el puerto por el que el colector la
// alcanza, una vez que la sesión viva la retiene.
//
// POR QUÉ EL PUERTO SE DECLARA AQUÍ Y NO SE IMPORTA `sessionmgr`: `sessionmgr` ya importa `health` (usa el
// Registry T6), así que la dependencia sólo puede ir en ese sentido. Es el mismo molde que `OutboxDepther`:
// el puerto MÍNIMO, declarado por quien lo consume, con tipos propios.
//
// (La línea en blanco que sigue es deliberada: pegado al `package`, este bloque sería un SEGUNDO comentario
// de paquete —el de verdad vive en collector.go— y `go doc` concatenaría los dos en una descripción
// incoherente. Separado, es lo que quiere ser: la nota de cabecera del fichero.)

package health

import "github.com/EduGoGroup/wapp-edge-agent/internal/app"

// DespachoStats son los contadores del despachador que salen del Edge (heartbeat y GET /v1/health). Es un
// SUBCONJUNTO deliberado de lo que el despachador cuenta: aquí sólo entra lo que un operador o el Cloud
// pueden ACCIONAR, no toda la telemetría interna del bucle (esa sigue en el bloque de log del despachador).
type DespachoStats struct {
	// OmitidosPorMotivo es el desglose de despachos SIN intención, motivo a motivo, con las OCHO claves de
	// `app.MotivosOmitido()` presentes SIEMPRE (un motivo a 0 es un dato, no un hueco).
	//
	// 🔴 LA LISTA SE RECORRE, JAMÁS SE TRANSCRIBE (INV-051.3). Se ha quedado corta dos veces; por eso
	// `app.MotivosOmitido()` devuelve una copia y por eso existe el guardarraíl AST de `internal/app`.
	// Ninguna línea de este repo —ni de producción ni de test— debe volver a enumerar los ocho a mano.
	//
	// 🔴 Y NUNCA SE AGREGA ENTRE MOTIVOS: `fastlane` era el camino SANO (el regex resolvía el intent en µs);
	// `presupuesto` y `breaker`, FALLOS, y uno de ellos mandaba a mirar Ollama. Sumarlos deja un número que
	// no responde ninguna pregunta. Sumar el MISMO motivo entre sesiones sí es legítimo.
	//
	// ⚠️ Desde T1.6-5 (ADR-0045) ninguno tiene productor vivo: sólo se mueven al drenar filas antiguas.
	OmitidosPorMotivo map[string]int64

	// FallosSelloDespacho: falló `MarcarDespachada` tras entregar. 🔴 Cada uno es un DUPLICADO en la nube.
	// Es el único de los cuatro que sigue teniendo productor.
	FallosSelloDespacho int64

	// 🔴 LOS TRES DE ABAJO ESTÁN CLAVADOS A 0 DESDE EL 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-5 · ADR-0045),
	// y su 0 NO significa «va todo bien»: significa que lo que medían dejó de existir.
	//
	//   - CabezasAtascadas / PollsCabezaAtascada medían una fila que se quedaba de cabeza en un estado que
	//     esta versión no conocía, dejando su sesión sin drenar. El despachador ya no mira el estado para
	//     decidir si entrega, así que ningún estado imprevisto puede retener nada.
	//   - FallosSelloPresupuesto medía los fallos de `DespacharSinIntent`, sentencia retirada con el
	//     presupuesto. Estaba separado de FallosSelloDespacho a propósito (T3.12) porque sólo uno de los
	//     dos implicaba duplicados; hoy sólo queda ese uno.
	//
	// SE CONSERVAN porque son campos del proto del heartbeat (`stuck_heads`, `stuck_head_polls`,
	// `failed_seal_budget`) y retirarlos es un cambio de contrato — decisión de T1.6-1, no de esta tarea.
	// `statsDe` (sessionmgr) ya no los asigna: se quedan en el 0 de DespachoStatsCero().
	CabezasAtascadas       int64
	PollsCabezaAtascada    int64
	FallosSelloPresupuesto int64
}

// DespachoStatsCero es el valor neutro: todo a 0 y las OCHO claves del desglose presentes. Lo devuelven los
// caminos «no hay lector» / «no hay sesión», para que el consumidor nunca tenga que distinguir entre un
// mapa nil y un motivo sin tráfico.
func DespachoStatsCero() DespachoStats {
	return DespachoStats{OmitidosPorMotivo: omitidosEnCero()}
}

// omitidosEnCero construye el desglose con las ocho claves canónicas a 0, RECORRIENDO `app.MotivosOmitido()`.
// Es el único constructor del mapa en este paquete: si mañana hay un noveno motivo, aparece aquí solo.
func omitidosEnCero() map[string]int64 {
	motivos := app.MotivosOmitido()
	out := make(map[string]int64, len(motivos))
	for _, m := range motivos {
		out[string(m)] = 0
	}
	return out
}

// DespachoReader es el puerto MÍNIMO que el colector necesita del session manager: los contadores del
// despachador de UNA sesión viva, y su agregado sobre TODAS las vivas. Lo satisface *sessionmgr.Manager.
// nil ⇒ el Report sale con el desglose a 0 (ocho claves) y los cuatro contadores a 0.
type DespachoReader interface {
	// DespachoStats devuelve los contadores del despachador de la sesión. ok=false si la sesión no está
	// viva o no llegó a arrancar despachador (la ausencia NO es un cero: son hechos distintos, y por eso
	// el segundo valor existe en vez de devolver un struct vacío).
	//
	// 🔴 CON ok=false EL PRIMER VALOR ES `DespachoStatsCero()`, NO UN `DespachoStats{}`: el mapa del
	// desglose nunca sale nil de aquí. Quien ignore el `ok` leerá ocho ceros —que es lo que el tipo
	// promete— en vez de un mapa nil que revienta o publica un hueco. La distinción «no sé» vs «no ha
	// pasado nada» sigue estando ENTERA en el bool, que es donde debe estar.
	DespachoStats(sessionID string) (DespachoStats, bool)
	// DespachoStatsVivas es el agregado sobre las sesiones VIVAS, motivo a motivo. Nunca falla: sin sesiones
	// devuelve el cero con las ocho claves.
	DespachoStatsVivas() DespachoStats
}
