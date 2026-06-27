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
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
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

// pairFactory construye el pairRunner para una sesión concreta: recibe SU custodia DEK, la *sql.DB de SU
// store ya abierto/migrado y el QRSink de ESTE emparejamiento (inyectado POR llamada para que el plano
// de control haga polling async del QR sin compartir un sink global). El factory decide qué connector
// (real o fake) cablear.
type pairFactory func(custody app.KeyCustody, storeDB *sql.DB, qr app.QRSink) pairRunner

// WithWhatsmeowPairing habilita Manager.Pair con el pairing REAL sobre whatsmeow: por cada sesión
// construye un Connector sobre su store (whatsmeow.NewConnector) y un app.Pair que pinta el QR en el
// QRSink que Manager.Pair recibe POR llamada y acota el flujo con timeout. La DEK la genera/sella
// app.Pair en la dek.key de la sesión. El QRSink ya NO es global: lo provee cada POST /v1/sessions/pair
// (un MemoryQRSink propio) para que el polling del QR sea por-emparejamiento.
func WithWhatsmeowPairing(timeout time.Duration) Option {
	return func(m *Manager) {
		m.newPairer = func(custody app.KeyCustody, storeDB *sql.DB, qr app.QRSink) pairRunner {
			connector := whatsmeow.NewConnector(storeDB)
			return app.NewPair(connector, qr, custody, app.WithTimeout(timeout))
		}
	}
}

// Pair empareja un teléfono NUEVO y lo deja registrado, fiel a la secuencia de design §5. Devuelve el
// JID emparejado (app.PairResult) y NUNCA la DEK (invariante zero-knowledge, ADR-0007). Es atómico de
// cara al registro: si algo falla tras provisionar el directorio/DEK/fila, limpia TODO (Clear DEK + rm
// dir + Delete fila) y no deja restos.
//
// Anti-pisado (objetivo del plan): el session_id es un UUIDv4 nuevo en cada llamada, así que el dir,
// la DEK y la fila son propios; un segundo Pair no puede sobrescribir al primero (MP-01 por construcción).
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

	sessionDir, err := m.layout.SessionDir(id)
	if err != nil {
		return app.PairResult{}, fmt.Errorf("sessionmgr: resolver dir de sesión: %w", err)
	}
	relDir, err := m.layout.RelSessionDir(id)
	if err != nil {
		return app.PairResult{}, fmt.Errorf("sessionmgr: resolver dir relativo de sesión: %w", err)
	}

	// 2. Provisionar el directorio de la sesión y abrir/migrar su store cifrado (set "store").
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		return app.PairResult{}, fmt.Errorf("sessionmgr: crear dir de sesión: %w", err)
	}
	storeDBPath, err := m.layout.StoreDB(id)
	if err != nil {
		_ = os.RemoveAll(sessionDir)
		return app.PairResult{}, fmt.Errorf("sessionmgr: resolver store de sesión: %w", err)
	}
	storeDB, err := wappdb.OpenSessionStore(ctx, storeDBPath)
	if err != nil {
		_ = os.RemoveAll(sessionDir)
		return app.PairResult{}, fmt.Errorf("sessionmgr: abrir store de sesión: %w", err)
	}
	// El store solo se usa durante el pairing (T3 no mantiene cliente vivo; eso es T4/Restore): se
	// cierra SIEMPRE al salir para no filtrar la conexión. En éxito, T4 reabre el store para el listener.
	defer func() { _ = storeDB.Close() }()

	// 3. Custodia DEK de ESTA sesión (FileCustody sobre Layout.DEKPath(id)). app.Pair sellará la DEK aquí.
	custody, err := m.custodyFor(id)
	if err != nil {
		_ = os.RemoveAll(sessionDir)
		return app.PairResult{}, fmt.Errorf("sessionmgr: resolver custodia de sesión: %w", err)
	}

	// 4. Registrar la sesión en 'pairing' (jid=NULL): el número se descubre recién en PairSuccess.
	now := time.Now().UTC()
	pairingMeta := domain.Session{
		SessionID: id,
		State:     domain.SessionStatePairing,
		StoreDir:  relDir,
		UpdatedAt: now,
	}
	if err := m.sessions.Upsert(ctx, pairingMeta); err != nil {
		m.cleanupPairing(ctx, id, sessionDir, custody)
		return app.PairResult{}, fmt.Errorf("sessionmgr: registrar sesión en pairing: %w", err)
	}

	// 5. Correr el pairing (QR → escaneo → Connected) sobre el store/custodia de la sesión. app.Pair
	//    genera y sella la DEK; al volver sin error, el JID ya es conocido.
	res, err := m.newPairer(custody, storeDB, qr).Run(ctx)
	if err != nil {
		// Fallo/cancelación/timeout: sin restos (DEK + dir + fila).
		m.cleanupPairing(ctx, id, sessionDir, custody)
		return app.PairResult{}, fmt.Errorf("sessionmgr: emparejar sesión: %w", err)
	}

	// 5.a. PairSuccess: promover a 'active' con el JID y el instante de emparejamiento.
	paired := time.Now().UTC()
	activeMeta := domain.Session{
		SessionID: id,
		JID:       res.WaJID,
		State:     domain.SessionStateActive,
		StoreDir:  relDir,
		PairedAt:  paired,
		UpdatedAt: paired,
	}
	if err := m.sessions.Upsert(ctx, activeMeta); err != nil {
		m.cleanupPairing(ctx, id, sessionDir, custody)
		return app.PairResult{}, fmt.Errorf("sessionmgr: promover sesión a active: %w", err)
	}

	// 5.b. Registrar la sesión como VIVA y arrancar su escucha (placeholder en T3; T4 la implementa).
	s := &liveSession{
		meta:    activeMeta,
		custody: custody,
		log:     m.log.With("session_id", id, "jid", res.WaJID),
	}
	m.mu.Lock()
	m.live[id] = s
	m.mu.Unlock()
	m.startListener(s) // arranca la escucha always-on de la sesión (real en T4; ver listen.go).

	// Invariante: solo el JID + el session_id salen del núcleo; la DEK queda sellada en la custodia de la
	// sesión y NUNCA cruza (ADR-0007). El session_id permite al plano de control correlacionar el pairing.
	return app.PairResult{WaJID: res.WaJID, SessionID: id}, nil
}

// cleanupPairing revierte TODO lo provisionado por un pairing que no llegó a 'active' (design §5): la
// DEK custodiada (Clear, abstrae el backend para el futuro KeystoreCustody que NO vive bajo el dir),
// el directorio de la sesión (store.db + dek.key) y la fila de metadatos. Best-effort: cada fallo se
// loguea pero no aborta los demás pasos, para no dejar restos a medias. No devuelve error: el llamante
// ya propaga la causa raíz del fallo de pairing.
func (m *Manager) cleanupPairing(ctx context.Context, id, sessionDir string, custody app.KeyCustody) {
	if cl, ok := custody.(interface{ Clear() error }); ok {
		if err := cl.Clear(); err != nil {
			m.log.Warn("sessionmgr: limpiar DEK del pairing fallido", "session_id", id, "error", err)
		}
	}
	if err := os.RemoveAll(sessionDir); err != nil {
		m.log.Warn("sessionmgr: limpiar dir del pairing fallido", "session_id", id, "error", err)
	}
	if err := m.sessions.Delete(ctx, id); err != nil {
		m.log.Warn("sessionmgr: borrar fila del pairing fallido", "session_id", id, "error", err)
	}
}
