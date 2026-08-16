package app

import (
	"context"
	"errors"
)

// cola.go — Puerto de la COLA DE ENTRANTES del Edge (Plan 051 Ola 1 · T1.1 / ADR-0038 Enmienda 1).
//
// El listener («mesonero») deja de clasificar en línea: anota el mensaje en esta cola y suelta el
// handler de whatsmeow en milisegundos, de modo que el acuse sale con el mensaje YA durable. El
// worker-cajero (proceso aparte) reclama por conversación, clasifica contra Ollama y escribe el
// intent de vuelta. El puerto vive en app (hexagonal); la implementación real lo respalda con un
// fichero SQLite APARTE del edge.db (<data_dir>/cola_entrantes.db) para que la poda agresiva y el
// SetMaxOpenConns(1) del edge.db no se estorben entre dos procesos. Sin broker (ADR-0003).

// ColaItem es una fila de la cola de entrantes, ANTES de cifrar: el puerto habla en CLARO y es el
// adaptador quien sella los campos sensibles con la DEK de la sesión antes de tocar el disco.
//
// INV-051.1 — Texto y Meta JAMÁS se persisten en claro (ADR-0002/0034): el fichero .db no se cifra a
// nivel de página, así que el contenido de negocio se guarda cifrado campo a campo con la DEK de la
// sesión a la que pertenece el mensaje (nunca con una llave global: cada sesión tiene la suya).
//
// SessionID y ChatJID sí van EN CLARO en disco, y es deliberado: SessionID es a la vez la clave de
// enrutado (el despachador drena por sesión, en orden de seq) y la que ELIGE la DEK con la que se
// descifra el resto de la fila —si estuviera cifrado no habría forma de saber con qué llave abrirlo—;
// ChatJID es, junto con SessionID, la clave de conversación por la que el cajero hace su claim
// atómico (mensaje + hijos en un solo lote). Ambos son metadato de enrutado, no contenido.
type ColaItem struct {
	// SessionID es el discriminador de sesión. EN CLARO: es la clave de enrutado y la que elige la DEK
	// con que se sellan/abren Texto y Meta.
	SessionID string
	// ChatJID es el JID del chat. EN CLARO: con SessionID forma la clave de conversación del claim.
	ChatJID string
	// WAMessageID es el id de mensaje de WhatsApp: base de la idempotencia local del encolado (anotar
	// dos veces el mismo mensaje no duplica la fila).
	WAMessageID string
	// TSWhatsApp es events.Message.Info.Timestamp en epoch-segundos. La ventana ADR-0037 ya se evaluó
	// ANTES de encolar: si descartó, no hay fila.
	TSWhatsApp int64
	// Texto es el cuerpo del mensaje. Se sella con la DEK de SessionID antes de persistir (INV-051.1).
	Texto string
	// Meta son los metadatos de negocio del mensaje en JSON (push_name, …). Se sella igual que Texto
	// (INV-051.1); puede ser nil (columna NULL).
	Meta []byte
	// IntentJSON es la clasificación ya resuelta. El listener la trae rellena solo cuando el fastlane
	// (regex, µs) atrapó el mensaje al nacer; "" ⇒ NULL en la columna, y el cajero la rellenará.
	IntentJSON string
	// Estado es el estado inicial de la fila: EstadoNuevo, o EstadoClasificado si el fastlane ya resolvió
	// el intent (el cajero nunca la reclama). Los otros dos estados son transiciones del worker/despachador.
	Estado string
}

// Estados de una fila de la cola (etiquetas estables; se persisten literales en la columna `estado`).
// El ciclo normal es nuevo → tomado → clasificado → despachado; el fastlane atajaba naciendo en
// clasificado. Un barrido devuelve a "nuevo" las filas cuyo lease ("tomado") venció, y la poda por TTL
// borra las "despachado".
const (
	// EstadoNuevo: recién anotada por el listener, pendiente de que el cajero la reclame.
	EstadoNuevo = "nuevo"
	// EstadoTomado: reclamada por el cajero (lease vivo). Si el lease vence, vuelve a EstadoNuevo.
	EstadoTomado = "tomado"
	// EstadoClasificado: con intent resuelto (por el cajero o por el fastlane), lista para el despachador.
	EstadoClasificado = "clasificado"
	// EstadoDespachado: ya entregada al despachador (cloudlink/outbox); solo espera la poda por TTL.
	EstadoDespachado = "despachado"
)

// ColaEntrantes es la cola durable de mensajes entrantes pendientes de clasificar. Es el puerto que
// usa el listener; el claim, la clasificación y la poda son del worker-cajero (otras olas). La
// implementación debe ser segura para uso concurrente (N listeners, uno por sesión, encolan a la vez).
type ColaEntrantes interface {
	// Enqueue persiste una fila de la cola, sellando Texto y Meta con la DEK de item.SessionID
	// (INV-051.1). Es IDEMPOTENTE por (SessionID, WAMessageID): anotar dos veces el mismo mensaje no
	// duplica la fila y NO es un error — el segundo intento devuelve nil, porque whatsmeow re-emite
	// eventos al reconectar y un reintento del handler es el caso normal, no una anomalía. La
	// implementación puede consumir (y perder) un número de orden en ese descarte: la secuencia solo
	// promete ser creciente, no contigua.
	Enqueue(ctx context.Context, item ColaItem) error
}

// ErrColaFalloRepetido marca un error de Enqueue que la implementación YA REPORTÓ en su log por esta
// misma causa y esta misma sesión, dentro de una ventana de enfriamiento. El error SIGUE siendo un
// error (la fila no se anotó y el llamante debe contarlo); lo único que cambia es que el llamante NO
// debe volver a gritarlo a nivel Error.
//
// POR QUÉ VIVE AQUÍ Y NO EN EL ADAPTADOR: el THROTTLE del log es del adaptador —es quien conoce la
// sesión, la causa y la ventana—, pero el NIVEL con que se escribe la línea es del llamante. Este
// centinela es el único puente entre ambos, y va en el puerto para que el listener no tenga que
// importar el paquete del adaptador (colaentrantes) solo para leer un error.
//
// Motivación de campo: una sesión sin DEK legible hacía fallar UN Enqueue POR MENSAJE ENTRANTE, y el
// listener escribía un log.Error por cada uno — a ritmo de socket.
var ErrColaFalloRepetido = errors.New("cola de entrantes: fallo ya reportado para esta sesión (en enfriamiento)")
