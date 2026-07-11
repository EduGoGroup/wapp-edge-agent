package whatsmeow

// walog.go — puente waLog.Logger → logger.Logger (slog) para darle VOZ a whatsmeow (follow-up
// observabilidad Plan 029 Ola 3). Hasta ahora los tres NewClient usaban waLog.Noop: un fallo o caída
// del websocket JAMÁS se logueaba. Este puente enruta los logs internos de whatsmeow al logger
// estructurado del agente con el mapeo natural de niveles (Error→Error, Warn→Warn, Info→Info,
// Debug→Debug) y el sub-módulo de whatsmeow (Sub) como atributo module=.
//
// Volumen: whatsmeow a Debug es RUIDOSO. El puente delega el filtrado en el nivel del logger del
// agente (wapp-shared/logger sobre slog): si el agente corre a INFO, los Debug de whatsmeow se
// descartan en el handler y no salen.
//
// Sensibilidad: whatsmeow no vuelca material de llaves en sus logs (los mensajes son de ciclo de
// conexión/protocolo); la DEK y el store cifrado nunca pasan por aquí (ADR-0007).
//
// No existe puente previo en EduGo para copia-adaptación (ADR-0004): edugo-api-messaging también usa
// waLog.Noop en sus NewClient. Este puente es propio de wApp.

import (
	"fmt"

	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/EduGoGroup/wapp-shared/logger"
)

// newWALog construye el waLog.Logger que consume whatsmeow sobre el logger estructurado del agente.
// nil-safe: sin logger (tests/cableados sin log) devuelve waLog.Noop (comportamiento previo).
func newWALog(log logger.Logger) waLog.Logger {
	if log == nil {
		return waLog.Noop
	}
	return &waLogBridge{log: log}
}

// waLogBridge implementa waLog.Logger delegando en logger.Logger. module acumula la cadena de
// sub-módulos de whatsmeow (Sub anidados, p. ej. "Client/Socket") y viaja como atributo.
type waLogBridge struct {
	log    logger.Logger
	module string
}

var _ waLog.Logger = (*waLogBridge)(nil)

func (b *waLogBridge) Errorf(msg string, args ...any) {
	b.log.Error(b.format(msg, args), "module", b.module)
}
func (b *waLogBridge) Warnf(msg string, args ...any) {
	b.log.Warn(b.format(msg, args), "module", b.module)
}
func (b *waLogBridge) Infof(msg string, args ...any) {
	b.log.Info(b.format(msg, args), "module", b.module)
}
func (b *waLogBridge) Debugf(msg string, args ...any) {
	b.log.Debug(b.format(msg, args), "module", b.module)
}

// Sub devuelve un sub-logger con el módulo encadenado (mismo formato path que waLog.Stdout).
func (b *waLogBridge) Sub(module string) waLog.Logger {
	if b.module != "" {
		module = b.module + "/" + module
	}
	return &waLogBridge{log: b.log, module: module}
}

// format aplica el printf-style de waLog y antepone el subsistema (estilo del repo: "quién: qué pasó").
func (b *waLogBridge) format(msg string, args []any) string {
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	return "whatsmeow: " + msg
}
