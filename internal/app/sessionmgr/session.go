package sessionmgr

import (
	"context"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// liveSession es el estado VIVO de una sesión que el Manager posee (design §1). Reúne el metadato de
// negocio, la custodia DEK resuelta para ESA sesión y (a partir de T3/T4) su store cifrado, su cliente
// whatsmeow vivo y el cancel de su goroutine listener.
//
// Esqueleto T1: solo se materializan meta/custody/log. Los campos de conexión (container/client) se
// añaden en T3/T4 cuando se implemente Pair/Restore/Listen; aquí quedan documentados como puntos de
// extensión para no reescribir la estructura entonces. cancel queda nil hasta que haya un listener.
type liveSession struct {
	// meta es el metadato de negocio persistido (session_id, jid, estado, store_dir, timestamps).
	meta domain.Session
	// custody es la custodia DEK de ESTA sesión (NewFileCustody(layout.DEKPath(id))); inyectada, no global.
	custody app.KeyCustody
	// log arrastra session_id/jid en cada línea (design §10.J); hijo del logger del Manager.
	log sharedlogger.Logger

	// cancel detiene la goroutine listener de la sesión (apagado ordenado, design §10.I). nil hasta T4.
	cancel context.CancelFunc

	// Puntos de extensión T3/T4 (no se materializan en el esqueleto para no acoplar a cryptostore/whatsmeow):
	//   container *cryptostore.Container  // store cifrado de la sesión (Layout.StoreDB(id))
	//   client    *whatsmeow.Client       // cliente vivo 24/7 (lección Plan 006: nada de efímeros)
}
