// Package intent sirve GET /v1/intent/status, el estado del CONTRATO de intenciones que este daemon
// conoce (Plan 029, ADR-0020 · reformado por el Plan 051 Ola 3 · T3.0).
//
// 🔴 QUÉ FUE ESTE PAQUETE Y QUÉ ES AHORA. Hasta el 2026-08-17 aquí vivía TAMBIÉN el decorador que envolvía
// el sink de entrada y llamaba a Ollama en el hilo de whatsmeow (sink.go): clasificador, circuit breaker,
// timeout y puerta de elegibilidad propios. Ese camino INLINE se retiró entero (INV-051.2). Quien clasifica
// hoy es el WORKER-CAJERO (`agent cajero`), que corre en OTRO PROCESO. De aquel paquete solo sobrevive este
// endpoint, y sobrevive ADELGAZADO a propósito: ver el porqué en statusResponse.
package intent

import (
	"encoding/json"
	"net/http"
)

// cajeroStatusPath es a dónde se manda al operador que pregunta por el estado del clasificador REAL. Lo
// sirve `wapp-ctl` (cmd/wapp-ctl/main.go), no este daemon, pero desde el navegador del operador ambos
// cuelgan del MISMO origen loopback: wapp-ctl atiende /v1/cajero/* por su cuenta y proxya el resto de /v1/*
// a este socket. Va como DATO en la respuesta, no como redirect 3xx: un redirect desde aquí rompería a
// quien consulta el socket Unix a pelo (`curl --unix-socket`), donde esa ruta no existe.
const cajeroStatusPath = "/v1/cajero/status"

// StatusDeps son las dependencias del endpoint GET /v1/intent/status. Todas tolerantes a nil para el caso
// feature OFF (el endpoint responde igual, con enabled=false).
type StatusDeps struct {
	// Enabled refleja cfg.Intent.Enabled: el interruptor de la feature en ESTA máquina. Es el mismo que
	// decide si una fila de la cola nace reclamable por el cajero o ya resuelta con la marca `apagado`.
	Enabled bool
	// Model es el modelo configurado (cfg.Intent.Model); informativo.
	Model string
	// ConfigVersion devuelve la versión de la config 'intents' PERSISTIDA en edge.db, o "". nil ⇒ "".
	// Es la fila exacta que el worker-cajero sondea para cargar su contrato: "" aquí significa «el cajero
	// no tiene con qué clasificar», que es el diagnóstico más útil que este proceso puede dar.
	ConfigVersion func() string
}

// statusResponse es el cuerpo JSON de GET /v1/intent/status.
//
// 🔴 POR QUÉ FALTAN `ollama_ok` Y `circuit`, Y POR QUÉ NO DEBEN VOLVER AQUÍ. Los servía este endpoint
// cuando el daemon clasificaba. Al retirarse el camino inline (T3.0):
//
//   - `circuit` leía el breaker del decorador. Muerto el decorador, ese breaker no lo alimenta nadie y
//     respondería "closed" para siempre. Un endpoint que contesta «el circuito está cerrado» leyendo un
//     contador que nadie incrementa es PEOR QUE UN 404: el 404 manda a leer la documentación, y el
//     "closed" manda a buscar el problema en otra parte. El circuito REAL vive en el proceso del cajero
//     (internal/app/cajero · Circuito()) y este daemon no tiene forma de leerlo: son dos procesos y el
//     único canal que comparten es el disco.
//   - `ollama_ok` exigía un cliente de Ollama DENTRO del agente para sondear /api/tags. Sostenerlo por un
//     booleano de estado contradice REQ-051.10 («ningún proceso que no sea el worker habla con Ollama») y
//     resucita justo la dependencia que esta ola vino a arrancar.
//
// Lo que queda son los tres datos que este proceso SÍ conoce de primera mano —el flag de la feature, el
// modelo configurado y la versión del contrato que hay en disco— más un puntero explícito a quién sabe el
// resto. Si algún día hace falta el estado real del clasificador en esta respuesta, la forma correcta NO es
// volver a sondear desde aquí: es que el cajero lo publique (su propio endpoint, o una fila de estado en la
// BD de la cola) y que esto lo lea. Eso es trabajo de la Ola 4 (telemetría), no de un parche aquí.
type statusResponse struct {
	Enabled       bool   `json:"enabled"`
	Model         string `json:"model"`
	ConfigVersion string `json:"config_version"`
	// ClasificaEn nombra al PROCESO que ejecuta la clasificación. Es un literal fijo y está aquí para que
	// quien lea esta respuesta a ciegas no concluya que el daemon clasifica.
	ClasificaEn string `json:"clasifica_en"`
	// WorkerStatusURL es dónde preguntar por el estado del worker (vivo/muerto/reintentando).
	WorkerStatusURL string `json:"worker_status_url"`
}

// StatusHandler construye el handler de GET /v1/intent/status: reporta el estado del CONTRATO de
// intenciones de este Edge. Se registra SIEMPRE (aun con la feature off, donde responde enabled=false).
//
// No hace E/S de red y ya no tiene timeout de sondeo: la única lectura es la de la config persistida, que
// es una consulta local a edge.db.
func StatusHandler(deps StatusDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := statusResponse{
			Enabled:         deps.Enabled,
			Model:           deps.Model,
			ClasificaEn:     "worker-cajero",
			WorkerStatusURL: cajeroStatusPath,
		}
		if deps.ConfigVersion != nil {
			resp.ConfigVersion = deps.ConfigVersion()
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}
