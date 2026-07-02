package domain

import "time"

// ReceiptStatus es el estado de dominio de un mensaje SALIENTE derivado de un acuse de WhatsApp
// (events.Receipt). Enum CERRADO {delivered, read} (Plan 013 §10.A): whatsmeow expone más tipos de
// acuse, pero para el ciclo de vida de un saliente solo importan "entregado" (✓✓) y "leído" (✓✓ azul).
// Si en el futuro hiciera falta modelar "played" aparte, es aditivo (§10.A).
type ReceiptStatus string

const (
	// ReceiptDelivered: el mensaje llegó al dispositivo del destinatario (✓✓). Deriva de
	// types.ReceiptTypeDelivered.
	ReceiptDelivered ReceiptStatus = "delivered"
	// ReceiptRead: el destinatario abrió el chat y vio el mensaje (✓✓ azul). Deriva de
	// types.ReceiptTypeRead / ReceiptTypeReadSelf / ReceiptTypePlayed (§10.A).
	ReceiptRead ReceiptStatus = "read"
)

// ReceiptEvent es la entidad de dominio de un ACUSE de entrega/lectura de un mensaje SALIENTE (Plan
// 013 §3). Lo produce el listener whatsmeow a partir de un *events.Receipt y lo consumirá el CloudLink
// para subir el estado a la nube (T2), correlacionado con el command_id del envío original (§10.E). NO
// lleva contenido/PII (§10.G): solo IDs de mensaje, estado, timestamp y la sesión. Es el análogo, para
// salientes, de InboundEvent para entrantes.
type ReceiptEvent struct {
	// MessageIDs son los IDs de los mensajes acusados por este receipt. Un receipt puede acusar VARIOS
	// a la vez (whatsmeow entrega un slice; Android los manda del más nuevo al más viejo, iOS al revés).
	// Coinciden con el SendResponse.ID del envío original: son la clave de correlación (§8/§10.E).
	MessageIDs []string
	// Status es el estado de dominio derivado del tipo de acuse (delivered | read, §10.A).
	Status ReceiptStatus
	// Timestamp es el instante en que WhatsApp fechó el acuse (events.Receipt.Timestamp).
	Timestamp time.Time
	// SessionID es la sesión del Edge (cliente vivo) que recibió el acuse; la correlación es POR sesión
	// (ADR-0008 / §10.E). El listener lo deja VACÍO: lo etiqueta el sink por-sesión aguas abajo (T2),
	// igual que el InboundSink de entrantes se etiqueta con mux.SinkFor(session_id).
	SessionID string
}
