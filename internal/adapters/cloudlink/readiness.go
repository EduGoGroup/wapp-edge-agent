package cloudlink

// readiness.go — LO QUE ESTE EDGE DICE SOBRE SU CAPACIDAD DE SERVIR INFERENCIA, y el latido FUERA DE
// CADENCIA que lo retransmite en el acto (Plan 044 · Ola 1.8 · T1.8-5, D-044.43; campo
// `Heartbeat.inference_readiness` de wapp-cloudlink v0.17.0).
//
// ─────────────────────────────────────────────────────────────────────────────
// LAS DOS FUENTES DE LA READINESS, Y POR QUÉ SON DOS
// ─────────────────────────────────────────────────────────────────────────────
//  1. EL AVISO DEL CAJERO (rápida, exacta, y puede faltar). Entra por POST /v1/inference/readiness
//     (internal/adapters/control/server/readiness.go) y llega en cuanto el socket abre o se cierra
//     ordenadamente. Es la fuente principal: nadie sabe antes que el propio cajero que su socket sirve.
//  2. EL DESENLACE DE UNA INFERENCIA REAL (lenta, pero no se puede perder). Si una petición vuelve con
//     app.ErrInferenciaOllamaCaido, el proveedor local no está: DOWN. Si vuelve servida, está: READY.
//     Es lo que cubre la muerte por SIGKILL —que no manda `defer`, así que no hay aviso— sin añadir un
//     probe: la observación ya estaba ocurriendo, sólo faltaba anotarla.
//
// La segunda existe para que la primera pueda fallar sin consecuencias permanentes. Un aviso perdido
// (socket del núcleo aún no abierto, daemon reiniciándose) se corrige solo con la siguiente inferencia.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 EL CERO ES «NO LO DIGO», JAMÁS «NO PUEDO»
// ─────────────────────────────────────────────────────────────────────────────
// Mientras nadie haya afirmado nada, este Edge manda INFERENCE_READINESS_UNSPECIFIED, que el contrato
// define como «este Edge no lo dice». NO se manda DOWN por defecto: DOWN es un veredicto, y emitir un
// veredicto que nadie ha comprobado haría que el Cloud dejara de calentar un Edge perfectamente sano
// —sin un solo error— hasta la primera inferencia. Es el mismo patrón que `worker_taskset` e
// `intent_circuit`, y está escrito en el proto.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 LA READINESS ES DEL EDGE; EL HEARTBEAT ES POR SESIÓN
// ─────────────────────────────────────────────────────────────────────────────
// El campo vive dentro de `Heartbeat`, que se emite UNO POR SESIÓN registrada. O sea que una transición
// produce N latidos con el mismo valor, y un Edge sin ninguna sesión registrada no produce ninguno. No
// es una duplicación evitable: el contrato no tiene un frame «del Edge» donde ponerlo, y un Edge sin
// sesiones no tiene nada que el Cloud pudiera calentar. Se documenta para que nadie lea el N como un
// bug ni el 0 como un latido perdido.

import (
	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
)

// Los tres estados de la readiness de inferencia, en el mismo orden que el enum del contrato para que
// la traducción sea directa y no haya un mapa que mantener.
const (
	// readinessDesconocida es el estado INICIAL y significa «nadie lo ha dicho todavía». Viaja como
	// INFERENCE_READINESS_UNSPECIFIED. Es el cero de un int32 a propósito: un Adapter recién construido
	// no afirma nada sin que haga falta inicializarlo.
	readinessDesconocida int32 = iota
	readinessListo
	readinessCaida
)

// MarcarInferenciaReadiness fija lo que este Edge AFIRMA sobre su capacidad de servir inferencia y, si
// eso fue una TRANSICIÓN, emite el latido fuera de cadencia que la retransmite. Devuelve true si hubo
// transición.
//
// Implementa server.InferenceReadinessSink (el puerto del plano de control). También la llama el carril
// de inferencia con el desenlace de cada petición.
//
// 🔴 EL LATIDO SOLO SALE EN LA TRANSICIÓN, y por eso el Swap va primero: sin esa guarda, un Edge con
// tráfico emitiría un latido extra POR CADA INFERENCIA SERVIDA —la fuente (2) marca READY en cada
// éxito—, que con N sesiones son N frames por petición. La cadencia normal ya publica el estado en
// todos sus latidos; lo que este camino aporta es la INMEDIATEZ del cambio, no la repetición del hecho.
//
// Es IDEMPOTENTE: repetir el mismo valor devuelve false y no emite nada. Eso es lo que permite que el
// cajero reintente su aviso sin llevar cuenta.
func (a *Adapter) MarcarInferenciaReadiness(listo bool) bool {
	nuevo := readinessCaida
	if listo {
		nuevo = readinessListo
	}
	anterior := a.infReadiness.Swap(nuevo)
	if anterior == nuevo {
		return false
	}

	a.log.Info("CloudLink: la readiness de inferencia de este Edge CAMBIA; se emite un latido fuera de cadencia",
		"anterior", nombreReadiness(anterior), "nueva", nombreReadiness(nuevo))

	// currentClient() y NO el `cl` de un stream capturado: quien llama a esto es o el plano de control o
	// un worker del carril, y ninguno de los dos tiene un stream a mano. Sin stream vivo no se emite nada
	// y no es una pérdida: `runOnce` ancla TODAS las sesiones con `heartbeatAll` en cuanto el stream
	// revive, y ese latido ya lleva el valor nuevo (viene del mismo campo). El estado sobrevive a la
	// caída del stream; el latido no hace falta que lo haga.
	if cl := a.currentClient(); cl != nil {
		a.heartbeatAll(cl)
	}
	return true
}

// readinessProto traduce el estado interno al enum del contrato. La usa sendHeartbeat, o sea TODOS los
// latidos: el campo es de ESTADO, no de transición, y el Cloud tiene que poder leerlo en cualquier
// latido sin haber visto el anterior.
func (a *Adapter) readinessProto() cloudlinkv1.InferenceReadiness {
	switch a.infReadiness.Load() {
	case readinessListo:
		return cloudlinkv1.InferenceReadiness_INFERENCE_READINESS_READY
	case readinessCaida:
		return cloudlinkv1.InferenceReadiness_INFERENCE_READINESS_DOWN
	default:
		// readinessDesconocida y cualquier valor imposible: «este Edge no lo dice». Nunca DOWN — ver el
		// bloque del cero en el encabezado.
		return cloudlinkv1.InferenceReadiness_INFERENCE_READINESS_UNSPECIFIED
	}
}

// nombreReadiness es sólo para el log: un int desnudo en la línea de la transición obligaría a abrir
// este fichero para leerla.
func nombreReadiness(v int32) string {
	switch v {
	case readinessListo:
		return "ready"
	case readinessCaida:
		return "down"
	default:
		return "sin_afirmar"
	}
}
