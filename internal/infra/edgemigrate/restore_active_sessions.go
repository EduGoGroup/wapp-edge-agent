package edgemigrate

// restore_active_sessions.go es la FASE 2 de la migración a BD única (Plan 022 T6.5, ADR-0018 §8,
// decisión §10.K): RESTAURA las sesiones ACTIVAS archivadas por la FASE 1
// (ArchiveLegacyPerSessionLayout) hacia la BD ÚNICA, SIN re-escanear los teléfonos ya vinculados.
//
// De dónde parte (verdad de campo verificada):
//   - La fase 1 dejó el árbol viejo en `<data_dir>/_archived-pre-022/sessions/<session_id>/` con, por
//     sesión, `store.db` (SQLite POR SESIÓN, tablas msg_enc_* cifradas + whatsmeow_* no sensibles) y
//     `dek.key` (la DEK de 32B de ESA sesión, layout Plan 008 pre-022 — DEKPath = sessions/<id>/dek.key).
//   - El cifrado es AES-256-GCM con nonce EN LÍNEA por blob y SIN AAD (wapp-shared/envelope): el nonce
//     viaja dentro del ciphertext y NADA ata el sellado a la BD física ni a la fila. Por eso el ciphertext
//     de msg_enc_* se puede COPIAR TAL CUAL a la BD única: misma DEK ⇒ Open() del mismo blob devuelve el
//     mismo plaintext, viva en el .db por-sesión o en la BD única. NO se re-cifra (decisión §10.B/K).
//
// Qué se copia y qué NO (verdad de campo — capa cripto/migración):
//   - SÍ: las tablas `msg_enc_*` (device/identities/sessions/prekeys/sender_keys). Son el esquema PROPIO
//     del cryptostore, SIN foreign keys, llaveadas por jid/our_jid: copiarlas verbatim a la BD única (que
//     ya tiene ese esquema migrado) es seguro y BASTA para que la sesión de WhatsApp siga viva (el device
//     reconecta con su material Noise/Identity/Adv de msg_enc_device; Signal re-negocia lo demás).
//   - NO: las tablas `whatsmeow_*`. En el modelo híbrido (T2) el device propio vive en `msg_enc_device`, NO
//     en `whatsmeow_device`, que queda VACÍA (cryptoContainer.PutDevice solo escribe msg_enc_device). Casi
//     todas las whatsmeow_* declaran `FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid)` (upstream
//     00-latest-schema.sql), así que copiarlas con foreign_keys=ON referenciaría una fila inexistente
//     (huérfanos) y es innecesario: app-state/contactos/lid-map los RE-SINCRONIZA WhatsApp tras reconectar.
//     Ver el resumen en el retorno del ejecutor (contradicción anotada frente al texto del plan).
//
// Con red (nunca romper una sesión buena, decisión §10.K):
//   - Fallback por device: si falta/está corrupta la DEK, el store no abre, no hay device pareado, el JID
//     no parsea o la DEK NO descifra (sesión CADUCADA ~14 días / DEK cruzada) ⇒ NO se copia material; si el
//     JID es conocido se registra el device 'loggedout' (cuelga de su cuenta por self_pn, para que un
//     re-escaneo recaiga en la MISMA cuenta) y se CONSERVA el archivo para inspección. Un fallo aislado
//     NUNCA aborta la migración de los demás.
//   - Limpieza SOLO tras verificar: el subdirectorio archivado de un device se PURGA únicamente después de
//     confirmar que su fila en `devices` y su `msg_enc_device` existen ya en la BD única. Idempotente: una
//     2.ª corrida no encuentra los dirs purgados (no-op) y los que quedaron (fallback) se reintentan sin
//     duplicar (INSERT OR IGNORE + Upsert idempotente).
//
// Zero-knowledge intacto (ADR-0007): una DEK POR dispositivo; se RE-UBICA de sessions/<id>/dek.key a
// keys/<session_id>.key (0600, desacoplada del store, §10.C) sin cambiar de custodio ni cruzar la nube;
// jamás se loguea. Ninguna DEK global; no se reintroduce session_id-por-fila (no regresa T2).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	_ "modernc.org/sqlite" // driver "sqlite" (CGO-free): abrir el store.db archivado en solo-lectura.

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-shared/envelope"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"go.mau.fi/whatsmeow/types"
)

// keysDirName es el subdirectorio (bajo data_dir) donde vive la DEK por sesión DESACOPLADA del store
// (Plan 022 §3/§10.C): <data_dir>/keys/<session_id>.key (0600). Coincide con sessionmgr.Layout; se
// replica aquí para no invertir la dependencia (infra→app) por una sola ruta.
const keysDirName = "keys"

// dekFileExt es la extensión del fichero de DEK por sesión (<session_id>.key).
const dekFileExt = ".key"

// legacyStoreDBName / legacyDEKName son los ficheros que la fase 1 archivó por sesión (layout pre-022,
// Plan 008: sessions/<id>/{store.db,dek.key}). El store.db es el SQLite por-sesión; dek.key la DEK 32B.
const (
	legacyStoreDBName = "store.db"
	legacyDEKName     = "dek.key"
)

// encTables son las tablas CIFRADAS del cryptostore (esquema 0001_init.sql) que se copian VERBATIM del
// store.db por-sesión a la BD única. SIN foreign keys, llaveadas por jid/our_jid; el ciphertext se
// traslada tal cual (misma DEK ⇒ mismo Open). Lista FIJA en código (nunca entrada externa): interpolarla
// en el SQL es seguro (no hay inyección posible).
var encTables = []string{
	"msg_enc_device",
	"msg_enc_identities",
	"msg_enc_sessions",
	"msg_enc_prekeys",
	"msg_enc_sender_keys",
}

// uuidPattern valida el formato UUID canónico: el session_id es SIEMPRE un UUIDv4 (ADR-0016 §3). Validarlo
// es además la barrera anti-escape de rutas (un UUID no contiene separadores ni ".."), igual que en
// sessionmgr.Layout. Un nombre de subdir que no calce NO es un directorio de sesión: se ignora.
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// RestoreArchivedActiveSessions restaura, sobre la BD ÚNICA `database` (ya abierta y migrada), las sesiones
// ACTIVAS que la fase 1 archivó en `<dataDir>/_archived-pre-022/sessions/<id>/` — SIN re-escanear (Plan 022
// T6.5, §10.K). Por cada device recupera JID+DEK+filas msg_enc_*, las re-inserta bajo la misma cuenta/device
// con la MISMA DEK per-device (keys/<id>.key) y mismo JID, y purga su archivo SOLO tras verificar. Un device
// que falle o esté caducado cae al fallback (loggedout/re-escaneo) sin tumbar a los demás.
//
// NO es fatal para el arranque (como la fase 1): el llamante loguea el error y continúa. Idempotente: una
// 2.ª corrida es no-op sobre los dirs ya purgados. dialect es el motor de `database` (hoy SQLite embebido;
// el árbol archivado es SIEMPRE SQLite por fichero).
//
// La custodia de la DEK re-ubicada se resuelve por un factory inyectable (Option): en producción es el
// backend real (Keychain en darwin / archivo en el resto, Plan 023 T2); los tests inyectan un doble en
// memoria para NO tocar el Keychain REAL de la máquina (headless, sin contaminar).
func RestoreArchivedActiveSessions(ctx context.Context, dataDir string, database *sql.DB, dialect string, log sharedlogger.Logger, opts ...Option) error {
	o := defaultRestoreOpts()
	for _, opt := range opts {
		opt(&o)
	}

	// El árbol archivado es SIEMPRE SQLite por fichero y la copia usa SQL SQLite (INSERT OR IGNORE): si la BD
	// única corre en Postgres, este tramo NO soporta el traslado SQLite→Postgres (fuera de alcance de T6.5).
	// Se anota y se sale limpio (no-op): las sesiones se re-escanean sobre Postgres (clean-slate de la fase 1).
	if dialect == wappdb.DialectPostgres {
		log.Warn("edgemigrate: T6.5 asume SQLite; con BD única Postgres el traslado no está soportado (re-escaneo)",
			"dialect", dialect)
		return nil
	}

	archivedSessions := filepath.Join(dataDir, archivePre022DirName, sessionsDirName)

	entries, err := os.ReadDir(archivedSessions)
	if errors.Is(err, fs.ErrNotExist) {
		return nil // nada archivado (fase 1 no archivó, o ya se purgó todo): no-op.
	}
	if err != nil {
		return fmt.Errorf("edgemigrate: listar %s: %w", archivedSessions, err)
	}

	store := sessionstore.New(database)
	var restored, fellBack, skipped int
	for _, e := range entries {
		if !e.IsDir() {
			continue // ficheros sueltos no son un directorio de sesión.
		}
		id := e.Name()
		if !uuidPattern.MatchString(id) {
			log.Warn("edgemigrate: subdir archivado con nombre no-UUID; se ignora", "name", id)
			skipped++
			continue
		}
		sessionDir := filepath.Join(archivedSessions, id)

		outcome := restoreOneSession(ctx, dataDir, database, store, id, sessionDir, log, o.newCustody)
		switch outcome {
		case outcomeRestored:
			restored++
		case outcomeFallback:
			fellBack++
		default:
			skipped++
		}
	}

	log.Info("edgemigrate: restauración de sesiones ACTIVAS archivadas (T6.5) completada",
		"restauradas", restored, "fallback_reescaneo", fellBack, "omitidas", skipped)
	return nil
}

// custodyFactory construye la custodia de una DEK dada la ruta de su archivo legacy (keys/<id>.key). Por
// defecto keycustody.NewFileCustody (Keychain en darwin / archivo en el resto, Plan 023 T2).
type custodyFactory func(path string) app.KeyCustody

// restoreOpts agrupa las dependencias inyectables de la restauración (hoy solo la custodia de la DEK).
type restoreOpts struct {
	newCustody custodyFactory
}

// Option configura RestoreArchivedActiveSessions. Variádico ⇒ los call-sites de producción (cmd/agent) no
// cambian; solo los tests pasan opciones.
type Option func(*restoreOpts)

// WithCustodyFactory sustituye el backend de custodia de la DEK re-ubicada. Los tests inyectan un doble en
// memoria para no tocar el Keychain REAL de la máquina; producción usa el default (Keychain/archivo).
func WithCustodyFactory(f func(path string) app.KeyCustody) Option {
	return func(o *restoreOpts) { o.newCustody = f }
}

// defaultRestoreOpts fija el backend real de custodia (Keychain en darwin / archivo en el resto).
func defaultRestoreOpts() restoreOpts {
	return restoreOpts{
		newCustody: func(path string) app.KeyCustody { return keycustody.NewFileCustody(path) },
	}
}

// sessionOutcome clasifica el resultado de migrar UN device archivado (para el resumen y los tests).
type sessionOutcome int

const (
	outcomeSkipped  sessionOutcome = iota // nada que migrar / no es sesión válida (dir conservado).
	outcomeRestored                       // migrada y verificada en la BD única (dir purgado).
	outcomeFallback                       // caducada/fallida: loggedout/re-escaneo (dir conservado).
)

// restoreOneSession migra un único device archivado (sessions/<id>/). Devuelve su desenlace. NUNCA rompe
// una sesión buena ni aborta a las demás: cualquier fallo cae al fallback (conservando el archivo).
func restoreOneSession(ctx context.Context, dataDir string, database *sql.DB, store *sessionstore.Store,
	id, sessionDir string, log sharedlogger.Logger, newCustody custodyFactory) sessionOutcome {

	storePath := filepath.Join(sessionDir, legacyStoreDBName)
	dekPath := filepath.Join(sessionDir, legacyDEKName)

	// 1. DEK de la sesión (32B). Ausente/corrupta ⇒ no se puede descifrar nada: fallback sin JID conocido.
	dek, err := os.ReadFile(dekPath)
	if err != nil || len(dek) != envelope.DEKSize {
		log.Warn("edgemigrate: DEK archivada ausente o inválida; se conserva el archivo para inspección",
			"session_id", id, "dek_bytes", len(dek))
		return outcomeSkipped
	}

	// 2. Abrir el store.db archivado en SOLO-LECTURA (query_only): no se muta la fuente (es la red de T6.5).
	src, err := openArchivedStoreReadOnly(ctx, storePath)
	if err != nil {
		log.Warn("edgemigrate: no se pudo abrir el store.db archivado; se conserva el archivo", "session_id", id, "error", err)
		return outcomeSkipped
	}
	defer func() { _ = src.Close() }()

	// 3. Recuperar el JID pareado y el ciphertext de una clave (noise_priv) para VERIFICAR la DEK.
	jidStr, noiseCT, ok, err := readPairedDevice(ctx, src)
	if err != nil {
		log.Warn("edgemigrate: error leyendo el device archivado; se conserva el archivo", "session_id", id, "error", err)
		return outcomeSkipped
	}
	if !ok {
		// store.db sin device pareado (nunca se completó un pairing): nada que restaurar.
		log.Warn("edgemigrate: store archivado sin device pareado; se conserva el archivo", "session_id", id)
		return outcomeSkipped
	}
	jid, perr := types.ParseJID(jidStr)
	if perr != nil {
		log.Warn("edgemigrate: JID archivado no parsea; se conserva el archivo", "session_id", id, "error", perr)
		return outcomeSkipped
	}

	// 4. VERIFICAR que la DEK descifra el material (sesión buena vs. CADUCADA/DEK cruzada). Si GCM falla el
	//    tag, la DEK no corresponde a este store (o el blob está manipulado): NO copiamos; fallback loggedout.
	env, err := envelope.NewEnvelope(dek)
	if err != nil {
		log.Warn("edgemigrate: DEK no construye envelope; se conserva el archivo", "session_id", id)
		return outcomeSkipped
	}
	if _, err := env.Open(noiseCT); err != nil {
		// Caducada (~14 días) o DEK/GCM que no corresponde: registrar 'loggedout' (misma cuenta por self_pn)
		// para que un re-escaneo recaiga en ella; NO se copia material ni se escribe la DEK. Archivo conservado.
		registerLoggedOut(ctx, store, id, jidStr, log)
		log.Warn("edgemigrate: la DEK no descifra el store (sesión caducada o llave cruzada); fallback re-escaneo",
			"session_id", id) // NUNCA se loguea la DEK ni el JID en claro salvo como metadato de negocio no secreto.
		return outcomeFallback
	}

	// 5. Copiar VERBATIM las tablas msg_enc_* del store por-sesión a la BD única (INSERT OR IGNORE:
	//    idempotente y sin clobber de otros devices). El ciphertext no se re-cifra (misma DEK, mismo Open).
	if err := copyEncryptedRows(ctx, src, database); err != nil {
		log.Warn("edgemigrate: fallo copiando material cifrado; se conserva el archivo", "session_id", id, "error", err)
		return outcomeSkipped
	}

	// 6. Re-ubicar la DEK a keys/<session_id>.key (0600), desacoplada del store (§10.C). Nunca se loguea.
	dekDst, derr := dekPathFor(dataDir, id)
	if derr != nil {
		log.Warn("edgemigrate: no se pudo resolver la ruta de la DEK; se conserva el archivo", "session_id", id, "error", derr)
		return outcomeSkipped
	}
	if err := newCustody(dekDst).Store(dek); err != nil {
		log.Warn("edgemigrate: no se pudo custodiar la DEK; se conserva el archivo", "session_id", id, "error", err)
		return outcomeSkipped
	}

	// 7. Upsert de la cuenta/device 'active' con el MISMO JID y self_pn derivado (misma cuenta por número).
	now := time.Now().UTC()
	if err := store.Upsert(ctx, domain.Session{
		SessionID: id,
		JID:       jidStr,
		SelfPN:    selfPNFromJID(jid),
		State:     domain.SessionStateActive,
		Role:      domain.DeviceRolePrimary,
		PairedAt:  now,
		UpdatedAt: now,
	}); err != nil {
		log.Warn("edgemigrate: no se pudo registrar el device activo; se conserva el archivo", "session_id", id, "error", err)
		return outcomeSkipped
	}

	// 8. VERIFICAR la migración en la BD única (device + msg_enc_device presentes) ANTES de purgar el archivo.
	if !verifyMigrated(ctx, database, store, id, jidStr) {
		log.Warn("edgemigrate: verificación post-migración falló; se conserva el archivo", "session_id", id)
		return outcomeSkipped
	}

	// 9. Purga SOLO-tras-verificar: elimina el subdirectorio archivado de ESTE device (los fallidos quedan).
	if err := os.RemoveAll(sessionDir); err != nil {
		// La migración YA quedó firme en la BD única; el archivo residual no rompe nada (2.ª corrida: INSERT
		// OR IGNORE + Upsert idempotente). Se loguea sin degradar el desenlace a fallo.
		log.Warn("edgemigrate: migración OK pero no se pudo purgar el archivo (inofensivo, idempotente)",
			"session_id", id, "error", err)
	}
	log.Info("edgemigrate: sesión ACTIVA restaurada a la BD única sin re-escanear", "session_id", id)
	return outcomeRestored
}

// openArchivedStoreReadOnly abre el store.db archivado en SOLO-LECTURA: PRAGMA query_only=ON impide toda
// escritura, de modo que leer la fuente (con su -wal/-shm archivados) no la muta (la fuente es la red de
// T6.5 y debe poder re-leerse). Devuelve error si el fichero no existe o no abre.
func openArchivedStoreReadOnly(ctx context.Context, path string) (*sql.DB, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	src, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	src.SetMaxOpenConns(1)
	if _, err := src.ExecContext(ctx, "PRAGMA query_only=ON"); err != nil {
		_ = src.Close()
		return nil, fmt.Errorf("edgemigrate: fijar query_only en %s: %w", path, err)
	}
	return src, nil
}

// readPairedDevice devuelve el JID pareado y el ciphertext de noise_priv del único device del store
// por-sesión (msg_enc_device tiene una fila). ok=false si no hay device pareado. Solo lee ciphertext:
// no descifra (la verificación de la DEK la hace el llamante con env.Open).
func readPairedDevice(ctx context.Context, src *sql.DB) (jid string, noiseCT []byte, ok bool, err error) {
	row := src.QueryRowContext(ctx, `SELECT jid, noise_priv FROM msg_enc_device LIMIT 1`)
	err = row.Scan(&jid, &noiseCT)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, false, nil
	}
	if err != nil {
		return "", nil, false, err
	}
	return jid, noiseCT, true, nil
}

// copyEncryptedRows copia VERBATIM las filas de las tablas encTables (msg_enc_*) del store por-sesión (src)
// a la BD única (dst), en una transacción, con INSERT OR IGNORE (idempotente; no pisa filas de otros
// devices ya migrados). Columnas dinámicas (SELECT *): tolera el drift de msg_enc_device (push_name/
// business_name/lid nullable, ALTER-added). El ciphertext se traslada tal cual (no se re-cifra).
func copyEncryptedRows(ctx context.Context, src, dst *sql.DB) error {
	tx, err := dst.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("edgemigrate: abrir tx de copia: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op tras Commit.

	for _, table := range encTables {
		if err := copyOneTable(ctx, src, tx, table); err != nil {
			return fmt.Errorf("edgemigrate: copiar %s: %w", table, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("edgemigrate: commit de copia: %w", err)
	}
	return nil
}

// copyOneTable copia todas las filas de `table` (nombre de la lista FIJA encTables — sin inyección) de src
// a la tx dst con INSERT OR IGNORE. Lee las columnas presentes en la FUENTE (rows.Columns) y las reinserta
// por nombre: las columnas ausentes en la fuente quedan por defecto (NULL) en el destino.
func copyOneTable(ctx context.Context, src *sql.DB, dst *sql.Tx, table string) error {
	rows, err := src.QueryContext(ctx, "SELECT * FROM "+table) //nolint:gosec // table ∈ encTables (constante en código).
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",")
	insertSQL := fmt.Sprintf("INSERT OR IGNORE INTO %s (%s) VALUES (%s)",
		table, strings.Join(cols, ","), placeholders)

	for rows.Next() {
		// Punteros a interface{}: el driver modernc devuelve []byte para BLOB/TEXT e int64 para INTEGER;
		// re-vincularlos por ? preserva el tipo nativo (round-trip exacto del ciphertext y los enteros).
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		if _, err := dst.ExecContext(ctx, insertSQL, vals...); err != nil {
			return err
		}
	}
	return rows.Err()
}

// verifyMigrated confirma que la migración de ESTE device quedó firme en la BD única ANTES de purgar el
// archivo: la fila de negocio (devices/session_id) existe y su material cifrado (msg_enc_device/jid) está
// presente. Cualquier fallo ⇒ NO purgar (conservar la copia).
func verifyMigrated(ctx context.Context, database *sql.DB, store *sessionstore.Store, id, jid string) bool {
	if _, err := store.Get(ctx, id); err != nil {
		return false
	}
	var n int
	if err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM msg_enc_device WHERE jid=?`, jid).Scan(&n); err != nil {
		return false
	}
	return n > 0
}

// registerLoggedOut da de alta (idempotente) el device en estado 'loggedout' colgando de su cuenta por
// self_pn, para que un RE-ESCANEO del mismo número recaiga en la MISMA cuenta (decisión §10.G/K). NO copia
// material cifrado ni escribe la DEK: la sesión caducó/no descifra y se re-empareja desde cero (clean-slate
// de la fase 1). Best-effort: un fallo aquí no cambia el desenlace (el archivo se conserva igual).
func registerLoggedOut(ctx context.Context, store *sessionstore.Store, id, jidStr string, log sharedlogger.Logger) {
	jid, err := types.ParseJID(jidStr)
	if err != nil {
		return // sin JID parseable no hay cuenta que anclar; el archivo se conserva para inspección.
	}
	now := time.Now().UTC()
	if err := store.Upsert(ctx, domain.Session{
		SessionID: id,
		JID:       jidStr,
		SelfPN:    selfPNFromJID(jid),
		State:     domain.SessionStateLoggedOut,
		Role:      domain.DeviceRolePrimary,
		UpdatedAt: now,
	}); err != nil {
		log.Warn("edgemigrate: no se pudo registrar el device caducado como loggedout", "session_id", id, "error", err)
	}
}

// dekPathFor devuelve <data_dir>/keys/<session_id>.key validando el UUID (barrera anti-escape: un UUID no
// contiene separadores ni ".."). Réplica local de sessionmgr.Layout.DEKPath para no invertir la dependencia
// infra→app por una sola ruta.
func dekPathFor(dataDir, id string) (string, error) {
	if !uuidPattern.MatchString(id) {
		return "", fmt.Errorf("edgemigrate: session_id inválido (se esperaba UUID): %q", id)
	}
	return filepath.Join(dataDir, keysDirName, id+dekFileExt), nil
}

// selfPNFromJID deriva el número propio (self_pn, E.164 sin '+') del JID pareado: la parte User del JID de
// un teléfono ES el número (misma lógica que sessionmgr.selfPNFromJID). No es secreto (metadato de negocio).
func selfPNFromJID(jid types.JID) string {
	return jid.User
}
