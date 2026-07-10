package sessionmgr

// pairing.go aporta el emparejamiento MULTI-SESIÓN del Manager (Plan 008 T3, design §5): cada llamada
// a Pair crea SU session_id/dir/DEK/store y registra la sesión, sin tocar ninguna sesión previa
// (anti-pisado por construcción, MP-01). El problema "el JID no se conoce hasta PairSuccess" se
// resuelve nombrando todo por el session_id (UUIDv4) generado AL INICIO (ADR-0016 §3): se da de alta
// en estado 'pairing' (jid=NULL) y se promueve a 'active' con el jid recién al completar el escaneo.
//
// REUSO (no duplicación): la lógica de QR/timeout/sellado de la DEK vive en app.Pair; el Manager solo
// le inyecta la custodia y el store DE ESTA sesión (vía newPairer) y orquesta el registro de negocio
// alrededor. app.Pair genera y SELLA la DEK al PairSuccess; el Manager no toca material cripto (solo
// lo limpia si el pairing falla). Por eso el resultado expuesto lleva SOLO el JID: la DEK nunca cruza.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/types"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cryptostore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// ErrPairingNotConfigured: se llamó a Pair sin haber inyectado el factory de pairing (falta
// WithWhatsmeowPairing en producción). Es un error de cableado, no de runtime de una sesión.
var ErrPairingNotConfigured = errors.New("sessionmgr: pairing no configurado (usa WithWhatsmeowPairing)")

// pairRunner abstrae el caso de uso de emparejamiento de UNA sesión (en producción *app.Pair). Run
// bloquea hasta éxito (devuelve el JID), error o timeout; el Manager lo usa sin conocer whatsmeow, lo
// que hace Manager.Pair testeable con un fake.
type pairRunner interface {
	Run(ctx context.Context) (app.PairResult, error)
}

// pairFactory construye el pairRunner para una sesión concreta: recibe SU custodia DEK, la *sql.DB del
// store (Plan 022 T3: la BD ÚNICA COMPARTIDA del Manager, no un .db por sesión) y el QRSink de ESTE
// emparejamiento (inyectado POR llamada para que el plano de control haga polling async del QR sin
// compartir un sink global). El factory decide qué connector (real o fake) cablear.
type pairFactory func(custody app.KeyCustody, storeDB *sql.DB, qr app.QRSink) pairRunner

// WithWhatsmeowPairing habilita Manager.Pair con el pairing REAL sobre whatsmeow: construye un Connector
// sobre la BD ÚNICA COMPARTIDA (whatsmeow.NewConnector con el dialecto del Manager) que monta el Container
// per-device con la DEK del pairing, y un app.Pair que pinta el QR en el QRSink que Manager.Pair recibe
// POR llamada y acota el flujo con timeout. La DEK la genera/sella app.Pair en keys/<id>.key. El QRSink ya
// NO es global: lo provee cada POST /v1/sessions/pair (un MemoryQRSink propio) para el polling por-emparejamiento.
func WithWhatsmeowPairing(timeout time.Duration) Option {
	return func(m *Manager) {
		m.newPairer = func(custody app.KeyCustody, storeDB *sql.DB, qr app.QRSink) pairRunner {
			connector := whatsmeow.NewConnector(storeDB, m.dbDialect)
			return app.NewPair(connector, qr, custody, app.WithTimeout(timeout))
		}
	}
}

// Pair empareja un teléfono NUEVO y lo deja registrado, fiel a la secuencia de design §7 (BD ÚNICA).
// Devuelve el JID emparejado (app.PairResult) y NUNCA la DEK (invariante zero-knowledge, ADR-0007). Es
// atómico de cara al registro: si algo falla tras registrar la fila/sellar la DEK, limpia TODO (Clear
// DEK + purga del material cifrado si ya se persistió + borrado transaccional de la fila) sin dejar restos
// ni cuentas huérfanas. YA NO provisiona directorio ni store.db por sesión: el store vive en la BD ÚNICA
// COMPARTIDA (m.db), sobre la que el Connector monta el Container per-device (Plan 022 §10.A).
//
// Anti-pisado (objetivo del plan): el session_id es un UUIDv4 nuevo en cada llamada, así que la DEK y la
// fila son propias; un segundo Pair no puede sobrescribir al primero (MP-01 por construcción).
//
// El QRSink lo provee el llamante (el plano de control inyecta un MemoryQRSink propio del emparejamiento
// para hacer polling async del QR; ver control/server/pair.go). app.Pair publica cada QR rotado ahí; la
// DEK NUNCA cruza por ese puerto (invariante zero-knowledge, ADR-0007/0015).
func (m *Manager) Pair(ctx context.Context, qr app.QRSink) (app.PairResult, error) {
	if m.newPairer == nil {
		return app.PairResult{}, ErrPairingNotConfigured
	}

	// 1. Identidad canónica de la sesión ANTES de conocer el JID (ADR-0016 §3, design §10.B).
	id := uuid.NewString()

	// 2. Custodia DEK de ESTA sesión (FileCustody sobre keys/<id>.key, DESACOPLADA del store — §3/§10.C).
	//    app.Pair sellará la DEK aquí al PairSuccess.
	custody, err := m.custodyFor(id)
	if err != nil {
		return app.PairResult{}, fmt.Errorf("sessionmgr: resolver custodia de sesión: %w", err)
	}

	// 3. Registrar la sesión en 'pairing' (jid=NULL, cuenta PROVISIONAL account_id=session_id, self_pn
	//    NULL): el número se descubre recién en PairSuccess.
	now := time.Now().UTC()
	pairingMeta := domain.Session{
		SessionID: id,
		State:     domain.SessionStatePairing,
		UpdatedAt: now,
	}
	if err := m.sessions.Upsert(ctx, pairingMeta); err != nil {
		m.cleanupPairing(ctx, id, "", custody)
		return app.PairResult{}, fmt.Errorf("sessionmgr: registrar sesión en pairing: %w", err)
	}

	// 4. Correr el pairing (QR → escaneo → Connected) sobre el Container COMPARTIDO (m.db) y la custodia de
	//    la sesión. app.Pair genera y sella la DEK; al volver sin error, el JID ya es conocido.
	res, err := m.newPairer(custody, m.db, qr).Run(ctx)
	if err != nil {
		// Fallo/cancelación/timeout ANTES de conocer el JID: sin restos (DEK + fila + cuenta provisional
		// vacía). No hay material cifrado que purgar: el device aún no se persistió (Device.Save es en éxito).
		m.cleanupPairing(ctx, id, "", custody)
		return app.PairResult{}, fmt.Errorf("sessionmgr: emparejar sesión: %w", err)
	}

	// 5. PairSuccess: promover a 'active' con el JID y RE-VINCULAR por self_pn (decisión §10.A/I): el número
	//    propio se DERIVA del JID recién conocido, de modo que un re-escaneo del MISMO número cuelgue de la
	//    MISMA cuenta (sessionstore.resolveAccount por self_pn) en vez de quedarse en el silo provisional;
	//    la cuenta provisional que queda vacía la purga el propio Upsert (cero huérfanos).
	paired := time.Now().UTC()
	selfPN := domain.SelfPNFromJID(res.WaJID)

	// Rol + cupo + registro vivo ATÓMICOS bajo m.mu (failover T5, design §6/§10.F): se decide el rol
	// (primary/standby) del device y se verifica el cupo de devices VIVOS del número, y se registra la
	// sesión en el MISMO tramo con lock, para que dos pairs del mismo número no den dos primarys ni excedan
	// el cupo. Con el default 1 (off), el 2.º device vivo del mismo número se RECHAZA (ErrAccountAtCapacity).
	m.mu.Lock()
	role, capErr := m.assignRoleLocked(selfPN, id)
	if capErr != nil {
		m.mu.Unlock()
		// El device ya se persistió cifrado al PairSuccess: purga su material por JID + DEK + fila (§10.I).
		m.cleanupPairing(ctx, id, res.WaJID, custody)
		return app.PairResult{}, fmt.Errorf("sessionmgr: emparejar sesión: %w", capErr)
	}
	activeMeta := domain.Session{
		SessionID: id,
		JID:       res.WaJID,
		SelfPN:    selfPN,
		State:     domain.SessionStateActive,
		Role:      role,
		PairedAt:  paired,
		UpdatedAt: paired,
	}
	s := &liveSession{
		meta:    activeMeta,
		custody: custody,
		log:     m.log.With("session_id", id, "jid", res.WaJID),
	}
	m.live[id] = s
	m.mu.Unlock()

	// 6. Persistir la fila 'active' (con su rol). Si falla, revertir el registro vivo y limpiar TODO: el
	//    device SÍ se persistió cifrado (whatsmeow lo guardó al PairSuccess), así que se purga su material
	//    por JID (msg_enc_*/whatsmeow_*) para no dejar huérfanos (§10.I).
	if err := m.sessions.Upsert(ctx, activeMeta); err != nil {
		m.mu.Lock()
		delete(m.live, id)
		m.mu.Unlock()
		m.cleanupPairing(ctx, id, res.WaJID, custody)
		return app.PairResult{}, fmt.Errorf("sessionmgr: promover sesión a active: %w", err)
	}

	// 7. Arrancar la escucha always-on (real vía WithWhatsmeowListen).
	m.startListener(s)

	// Invariante: solo el JID + el session_id salen del núcleo; la DEK queda sellada en la custodia de la
	// sesión y NUNCA cruza (ADR-0007). El session_id permite al plano de control correlacionar el pairing.
	return app.PairResult{WaJID: res.WaJID, SessionID: id}, nil
}

// cleanupPairing revierte TODO lo provisionado por un pairing que no llegó a 'active' (design §7, BD única):
// la DEK custodiada (Clear), el material cifrado del device SI ya se había persistido (jid != "" ⇒
// cryptostore.DeleteDevice sobre msg_enc_*/whatsmeow_*) y la fila de metadatos (borrado transaccional que
// purga también la cuenta provisional vacía). YA NO borra directorio de sesión (no existe con la BD única).
// Best-effort: cada fallo se loguea pero no aborta los demás pasos. No devuelve error: el llamante ya
// propaga la causa raíz del fallo de pairing.
func (m *Manager) cleanupPairing(ctx context.Context, id, jid string, custody app.KeyCustody) {
	if cl, ok := custody.(interface{ Clear() error }); ok {
		if err := cl.Clear(); err != nil {
			m.log.Warn("sessionmgr: limpiar DEK del pairing fallido", "session_id", id, "error", err)
		}
	}
	// Material cifrado del device: SOLO si el pairing llegó a persistirlo (jid conocido) y hay BD real.
	if jid != "" && m.db != nil {
		if parsed, perr := types.ParseJID(jid); perr != nil {
			m.log.Warn("sessionmgr: JID inválido al limpiar el pairing fallido", "session_id", id, "error", perr)
		} else if err := cryptostore.DeleteDevice(ctx, m.db, m.dbDialect, parsed); err != nil {
			m.log.Warn("sessionmgr: limpiar material cifrado del pairing fallido", "session_id", id, "error", err)
		}
	}
	if err := m.deleteDeviceMeta(ctx, id); err != nil {
		m.log.Warn("sessionmgr: borrar fila del pairing fallido", "session_id", id, "error", err)
	}
}

