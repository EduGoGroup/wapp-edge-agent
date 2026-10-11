// Package whatsappfalso es el TRANSPORTE FALSO de WhatsApp del Edge (ADR-0049): sustituye el socket, el
// device y el almacén cifrado de whatsmeow, y deja intacto todo lo demás.
//
// PARA QUÉ. Probar el Edge real —su cola cifrada, su despachador, su lease, su envío— exigía una sesión de
// WhatsApp viva, que depende de un teléfono y caduca sola. Con este transporte el Edge se empareja, escucha,
// recibe y envía sin hablar con WhatsApp: lo que entra se inyecta (ADR-0041) y lo que sale se captura.
//
// 🔴 QUÉ ES FALSO Y QUÉ NO.
//   - FALSO: el transporte. No hay socket, ni QR, ni device, ni almacén de whatsmeow; un «envío» no sale a
//     ninguna parte y la media NO se descarga.
//   - REAL: todo lo demás. La DEK la genera y la custodia el cliente como en una sesión de verdad (app.Pair);
//     el lease lo emite y lo revoca la nube, y con el lease revocado este transporte no llega a capturar nada
//     porque el gate 2-de-2 está ANTES de él (adaptador de CloudLink); la puerta de entrantes es la del
//     adaptador de whatsmeow, con sus guardas, sin una línea reescrita (whatsmeow/sin_transporte.go).
//
// 🔴 APAGADO POR DEFECTO Y PERMANENTE. Solo lo elige el arranque con WAPP_AGENT_WHATSAPP_FALSO=true, y lo
// grita al arrancar y en cada latido. Un Edge con la palanca echada NO habla con WhatsApp.
//
// LA DEK. Este paquete la recibe en dos sitios (StartConnection y Listen) porque así son los puertos, y en
// los dos la IGNORA: no la guarda, no la copia y no la registra. No tiene almacén que abrir con ella.
package whatsappfalso

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// PrefijoSintetico marca los identificadores de los mensajes «enviados» por este transporte. Es EL MISMO
// prefijo que llevan los entrantes sintéticos del ADR-0041, a propósito: en la nube, todo lo que no pasó
// por WhatsApp se reconoce con una sola regla.
const PrefijoSintetico = whatsmeow.PrefijoSintetico

// prefijoNumeroFalso encabeza el número de toda sesión falsa. Ningún prefijo internacional empieza por 0,
// así que un número con estos cuatro ceros delante no puede ser el de un teléfono: se reconoce a la vista
// en un log, en el latido y en la nube.
const prefijoNumeroFalso = "0000"

// Conector es el conector de emparejamiento del transporte falso: contesta «conectado» sin QR ni red.
type Conector struct{}

var _ app.Connector = Conector{}

// NuevoConector construye el conector falso. No recibe la BD: no tiene almacén que crear.
func NuevoConector() Conector { return Conector{} }

// StartConnection contesta de inmediato con UNA señal «conectado» y el JID de una sesión falsa nueva. La
// DEK se ignora (ver la cabecera del paquete): quien la sella en la custodia del cliente es app.Pair, al
// recibir esta señal, exactamente igual que con el conector real.
func (Conector) StartConnection(_ context.Context, _ []byte) (<-chan app.PairingSignal, error) {
	jid, err := jidFalso()
	if err != nil {
		return nil, fmt.Errorf("whatsapp falso: fabricar el JID de la sesión: %w", err)
	}
	signals := make(chan app.PairingSignal, 1)
	signals <- app.PairingSignal{Type: app.PairingSignalConnected, WaJID: jid}
	close(signals)
	return signals, nil
}

// jidFalso fabrica un JID bien formado y distinto en cada emparejamiento: el gestor de sesiones lo parsea
// y deriva de él el número propio, y con un número repetido rechazaría el segundo emparejamiento por el
// cupo de dispositivos por cuenta.
func jidFalso() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(100_000_000))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%08d@%s", prefijoNumeroFalso, n.Int64(), domain.PhoneUserServer), nil
}

// idMensajeFalso fabrica el identificador de un mensaje «enviado»: el prefijo sintético y 16 hex al azar.
func idMensajeFalso() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return PrefijoSintetico + hex.EncodeToString(b), nil
}
