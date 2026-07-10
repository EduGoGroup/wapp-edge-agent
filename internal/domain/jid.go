package domain

import "strings"

// PhoneUserServer es el server de un JID de WhatsApp que corresponde a un NÚMERO real (E.164), frente al
// server de un LID oculto. Se replica como constante de DOMINIO porque el core NO importa
// go.mau.fi/whatsmeow (regla de dependencias hacia adentro); su valor es el mismo que
// types.DefaultUserServer del SDK.
const PhoneUserServer = "s.whatsapp.net"

// SelfPNFromJID deriva el número PROPIO (E.164 sin '+') del JID del device propio. Implementación ÚNICA
// (Plan 027 T5, cierra H6): antes estaba triplicada —cloudlink, sessionmgr y edgemigrate— con tres firmas
// distintas y riesgo de divergencia LID vs número. El JID de un teléfono es
// "<numero>[.<agent>][:<device>]@s.whatsapp.net" y su user-part ES el número. Devuelve "" si el JID viene
// vacío (sesión aún sin emparejar), si su server NO es el de un número (un LID u otro server: NUNCA se
// reporta un LID como número) o si no tiene user-part. No es secreto (metadato de negocio).
func SelfPNFromJID(jid string) string {
	at := strings.LastIndexByte(jid, '@')
	if at < 0 || jid[at+1:] != PhoneUserServer {
		return ""
	}
	user := jid[:at]
	if i := strings.IndexAny(user, ".:"); i >= 0 {
		user = user[:i]
	}
	return user
}
