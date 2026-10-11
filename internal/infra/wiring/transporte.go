package wiring

// transporte.go — LA COLA ENTRE CADA ADAPTADOR DE WHATSAPP Y LA FRONTERA DEL GESTOR (ADR-0049).
//
// El gestor de sesiones declara lo que necesita de un transporte (sessionmgr.Transporte) y no construye
// ninguno. Cada adaptador ofrece sus constructores con SUS tipos. Este fichero es el único sitio que conoce
// a los dos lados, y por eso vive en `wiring`: ni el gestor importa un adaptador para construirlo, ni un
// adaptador importa al gestor. Añadir un transporte es añadir aquí su función y una rama en el punto de
// elección del arranque (internal/infra/daemon).

import (
	"database/sql"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsappfalso"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
)

// TransporteWhatsmeow es el transporte REAL: el cliente de whatsmeow, con su socket, su device y su almacén
// cifrado. Es el de siempre y el único que habla con WhatsApp.
func TransporteWhatsmeow() sessionmgr.Transporte {
	return sessionmgr.Transporte{
		NuevoConector: func(storeDB *sql.DB, dialect string, log sharedlogger.Logger) app.Connector {
			return whatsmeow.NewConnector(storeDB, dialect, log)
		},
		NuevaEscucha: func(db *sql.DB, dialect, jid string, log sharedlogger.Logger) sessionmgr.Escucha {
			return whatsmeow.NewListenGatewayForDevice(db, dialect, jid, log)
		},
	}
}

// TransporteFalso es el transporte de WhatsApp FALSO (ADR-0049): se empareja sin QR, escucha sin socket y
// anota en `registro` lo que el Edge «envía». No usa la BD ni el dialecto: no tiene almacén. Solo lo elige
// el arranque con WAPP_AGENT_WHATSAPP_FALSO=true.
func TransporteFalso(registro *whatsappfalso.Registro) sessionmgr.Transporte {
	return sessionmgr.Transporte{
		NuevoConector: func(*sql.DB, string, sharedlogger.Logger) app.Connector {
			return whatsappfalso.NuevoConector()
		},
		NuevaEscucha: func(_ *sql.DB, _, _ string, log sharedlogger.Logger) sessionmgr.Escucha {
			return whatsappfalso.NuevaEscucha(registro, log)
		},
	}
}
