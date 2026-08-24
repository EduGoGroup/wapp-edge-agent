package wiring

// inferencia.go — El cableado del puerto de INFERENCIA en el daemon (Plan 044 · Ola 1.6 · T1.6-2,
// ADR-0045 §2, REQ-34).
//
// QUÉ CONECTA: el frame `inference_request` llega al stream CloudLink, que vive en `agent serve`. Quien
// puede hablar con Ollama es `agent cajero` (REQ-051.10). Esta función construye el cliente del socket
// unix que une los dos, y BuildMux se lo pasa al Adapter.

import (
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/inferenciacliente"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// BuildInferenciaCliente construye el puerto hacia el proveedor local de LLM: un cliente HTTP sobre
// <data_dir>/cajero.sock.
//
// 🔴 NO COMPRUEBA QUE EL SOCKET EXISTA, y es deliberado. El cajero es OTRO PROCESO con su propio ciclo
// de vida: puede arrancar después que el daemon, morir y reaparecer con un `Restart` del supervisor, o
// estar apagado por config. Un chequeo aquí fotografiaría el estado del arranque y lo congelaría para
// toda la vida del daemon —un daemon que arrancó primero se quedaría sin inferencia PARA SIEMPRE aunque
// el cajero levantara dos segundos después—. El cliente marca el socket EN CADA petición, así que se
// recupera solo en cuanto el cajero vuelve, y mientras tanto responde ErrInferenciaOllamaCaido, que es
// la verdad operativa: desde este proceso, el cajero ES el proveedor local.
//
// SE CONSTRUYE TAMBIÉN CON LA FEATURE APAGADA (cfg.Intent.Enabled=false), por lo mismo: con ella apagada
// el cajero no levanta socket y esto responderá OLLAMA_DOWN a cada `inference_request` — que es
// exactamente lo que hay que responderle al Cloud, y lo mismo que respondería si el LLM estuviera caído.
// Devolver nil aquí daría el mismo resultado por un camino distinto (el carril tiene su propia guarda),
// pero perdería la propiedad de arriba: un operador que enciende la feature y reinicia sólo el cajero
// tendría el daemon cableado a nil hasta que también lo reiniciara a él.
func BuildInferenciaCliente(layout sessionmgr.Layout, log sharedlogger.Logger) app.ServidorInferencia {
	cli := inferenciacliente.Nuevo(layout.CajeroSock())
	log.Info("servicio de inferencia CABLEADO (ADR-0045: el Cloud pide, el Edge sirve); el proveedor es el "+
		"proceso `agent cajero` por su socket local, no este proceso (REQ-051.10)",
		"socket", cli.Socket())
	return cli
}
