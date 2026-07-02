package domain

import "time"

// InboundEvent es la entidad de dominio de un mensaje ENTRANTE de WhatsApp ya DESCIFRADO por
// whatsmeow en RAM (RF-5, design §5). Es el "evento de negocio" que el Edge reenviará a la nube por
// CloudLink (en el spike: a un sink de log). NO contiene material cifrado ni llaves: el descifrado
// E2E lo hace whatsmeow con el store (cifrado en reposo con la DEK); aquí ya viaja el contenido en
// claro porque, según el zero-knowledge de wApp, el CONTENIDO de negocio sí sube a la nube — lo que
// nunca sale del Edge son las credenciales/llaves (DEK), no los mensajes (ADR-0005/0007).
//
// Mapea SOLO los campos útiles que expone *events.Message de whatsmeow (Info + cuerpo de texto). El
// spike cubre texto (Conversation/ExtendedTextMessage); media y otros tipos se incorporan luego.
type InboundEvent struct {
	// MessageID es el ID único del mensaje en WhatsApp (types.MessageInfo.ID).
	MessageID string
	// Chat es el JID del chat donde se recibió (DM o grupo).
	Chat string
	// Sender es el JID del usuario que envió el mensaje, en el formato PRINCIPAL que reporta
	// whatsmeow (número `…@s.whatsapp.net` o LID `…@lid`, según AddressingMode).
	Sender string
	// SenderAlt es la dirección ALTERNATIVA del mismo remitente que resuelve whatsmeow: si Sender es
	// el número, SenderAlt trae el LID, y viceversa (Plan 010 §5, identidad de contacto). Viene VACÍO
	// cuando whatsmeow aún no aprendió el mapeo (primer contacto: "No LID found"); en ese caso solo se
	// conoce Sender y NO se falla (tolerancia §10.H). Formato JID (`…@s.whatsapp.net` o `…@lid`).
	SenderAlt string
	// AddressingMode es el modo de direccionamiento del mensaje según whatsmeow: "pn" (Sender es el
	// número) o "lid" (Sender es el LID). Diagnóstico/derivación; puede venir vacío.
	AddressingMode string
	// PushName es el nombre visible que el remitente publica en WhatsApp (puede venir vacío).
	PushName string
	// Timestamp es el instante en que WhatsApp fechó el mensaje.
	Timestamp time.Time
	// Type es el tipo de mensaje reportado por whatsmeow (p.ej. "text"); informativo.
	Type string
	// Text es el cuerpo de texto ya descifrado. Vacío si el mensaje no es de texto.
	Text string
	// IsFromMe indica que el mensaje lo envió la propia sesión (eco de otro dispositivo del usuario).
	IsFromMe bool
	// IsGroup indica que el chat es un grupo/lista de difusión.
	IsGroup bool
}
