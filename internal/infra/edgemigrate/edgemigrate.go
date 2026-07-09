// Package edgemigrate contiene la migración de ARRANQUE del layout en disco del Edge hacia el modelo
// multi-sesión (ADR-0016 / Plan 008, design §10.C). No toca esquemas SQL (eso lo hace internal/infra/db
// con sus migraciones embebidas); aquí se reorganiza el LAYOUT DE FICHEROS del data_dir.
//
// CLEAN-SLATE (design §10.C): el estado single-sesión vigente (un `store.db` plano + un `dek.key` plano)
// quedó INCOHERENTE con el modelo por-sesión y, además, la DEK vieja se perdió en los re-emparejamientos
// (MP-01): no es recuperable. La migración NO borra de forma destructiva: ARCHIVA el store/DEK planos en
// `<data_dir>/_archived-pre-008/` (por si el usuario quiere inspeccionarlos), avisa por WARN de que hay
// que re-emparejar, y deja el layout `<data_dir>/sessions/` creado y vacío para que el Manager (T1+) aloje
// las nuevas sesiones. Es IDEMPOTENTE: una vez que existe `sessions/`, vuelve a ejecutarse como no-op.
//
// NO toca datos de la nube ni nada fuera de los dos ficheros planos nombrados y los directorios que crea.
package edgemigrate

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// archiveDirName es el subdirectorio (bajo data_dir) donde se archiva el estado single-sesión retirado.
const archiveDirName = "_archived-pre-008"

// archivePre022DirName es el subdirectorio (bajo data_dir) donde se archiva el layout multi-sesión
// POR-DIRECTORIO (sessions/<id>/) al pasar a la BD única (Plan 022 T1, ADR-0018 §8, clean-slate fase 1).
// La FASE 2 (T6.5) LEE este árbol para restaurar las sesiones ACTIVAS sin re-escanear, así que NO se borra.
const archivePre022DirName = "_archived-pre-022"

// sessionsDirName es el subdirectorio (bajo data_dir) que aloja un directorio por sesión (ADR-0016 §4).
const sessionsDirName = "sessions"

// ArchiveLegacySingleSession migra el data_dir del layout single-sesión al multi-sesión (design §10.C).
//
// flatStorePath/flatDEKPath son las rutas PLANAS heredadas (p.ej. <data_dir>/wapp-edge.db y
// <data_dir>/dek.key). Comportamiento:
//   - Si ya existe <data_dir>/sessions/  -> NO-OP (migración previa): retorna nil sin tocar nada.
//   - Si no: archiva en <data_dir>/_archived-pre-008/ cada fichero plano que exista (el store.db con sus
//     sidecars -wal/-shm de WAL, y el dek.key), loguea WARN de re-emparejar, y crea <data_dir>/sessions/.
//
// Idempotente y no destructiva (mueve, no borra). Devuelve error solo ante fallos de E/S reales.
func ArchiveLegacySingleSession(dataDir, flatStorePath, flatDEKPath string, log sharedlogger.Logger) error {
	sessionsDir := filepath.Join(dataDir, sessionsDirName)

	// Marcador de idempotencia: si el layout sessions/ ya existe, la migración ya corrió.
	if exists, err := dirExists(sessionsDir); err != nil {
		return fmt.Errorf("edgemigrate: comprobar %s: %w", sessionsDir, err)
	} else if exists {
		return nil
	}

	archiveDir := filepath.Join(dataDir, archiveDirName)

	// El store.db en WAL arrastra sidecars -wal/-shm; se archivan junto al .db para no dejar huérfanos.
	legacy := []string{
		flatStorePath,
		flatStorePath + "-wal",
		flatStorePath + "-shm",
		flatDEKPath,
	}

	archivedAny := false
	for _, src := range legacy {
		moved, err := archiveFile(src, archiveDir)
		if err != nil {
			return err
		}
		archivedAny = archivedAny || moved
	}

	if archivedAny {
		log.Warn("estado single-sesión retirado: re-empareja tus teléfonos",
			"archived_dir", archiveDir,
			"reason", "modelo multi-sesión por session_id (ADR-0016); la DEK previa no es recuperable")
	}

	// Layout multi-sesión: directorio vacío que el Manager (T1+) poblará con un subdir por session_id.
	if err := os.MkdirAll(sessionsDir, 0o700); err != nil {
		return fmt.Errorf("edgemigrate: crear %s: %w", sessionsDir, err)
	}
	return nil
}

// ArchiveLegacyPerSessionLayout es la MIGRACIÓN CLEAN-SLATE hacia la BD ÚNICA (Plan 022 T1, ADR-0018 §8,
// fase 1). El modelo BD única (ADR-0018 §Decisión.1) retira el `.db`/directorio POR SESIÓN; el layout viejo
// `<data_dir>/sessions/<id>/{store.db,dek.key}` queda incoherente. Esta migración:
//   - ARCHIVA cada subdirectorio de sesión `sessions/<id>/` (con su store.db + dek.key) a
//     `<data_dir>/_archived-pre-022/sessions/<id>/` (mueve, NO borra), y
//   - avisa por WARN de re-emparejar, dejando `sessions/` vacío.
//
// ⚠️ NO ELIMINA el árbol viejo: la FASE 2 (T6.5) LEERÁ `_archived-pre-022/` para restaurar las sesiones
// ACTIVAS a la BD única (misma DEK/JID) SIN re-escanear. El esquema nuevo (accounts/devices + msg_enc_*/
// whatsmeow_*) lo crea aparte el runner de migraciones SQL (internal/infra/db, 0004) al abrir la BD única.
//
// IDEMPOTENTE y no destructiva: el marcador es la existencia de `_archived-pre-022/`. Si ya existe, la
// migración ya corrió → NO-OP (aunque el daemon haya creado nuevas sesiones tras migrar, no se re-archivan).
// Sin subdirectorios que archivar (instalación limpia), tampoco crea el archivo ni avisa. Devuelve error
// solo ante fallos de E/S reales.
func ArchiveLegacyPerSessionLayout(dataDir string, log sharedlogger.Logger) error {
	archiveRoot := filepath.Join(dataDir, archivePre022DirName)

	// Marcador de idempotencia: si el archivo pre-022 ya existe, la migración ya corrió (no re-archivar).
	if exists, err := dirExists(archiveRoot); err != nil {
		return fmt.Errorf("edgemigrate: comprobar %s: %w", archiveRoot, err)
	} else if exists {
		return nil
	}

	// markMigrated deja el marcador de idempotencia (_archived-pre-022/) AUNQUE no haya nada que archivar.
	// Sin él, una instalación limpia (sessions/ vacío) retornaría sin marcador y, tras emparejar un teléfono
	// (el runtime aún crea sessions/<id>/ hasta T3), el SIGUIENTE arranque archivaría esa sesión recién
	// creada por error, dejando Restore sin su store. El marcador registra "el paso pre-022 ya corrió".
	markMigrated := func() error {
		if err := os.MkdirAll(archiveRoot, 0o700); err != nil {
			return fmt.Errorf("edgemigrate: crear marcador %s: %w", archiveRoot, err)
		}
		return nil
	}

	sessionsDir := filepath.Join(dataDir, sessionsDirName)
	entries, err := os.ReadDir(sessionsDir)
	if errors.Is(err, fs.ErrNotExist) {
		return markMigrated() // sin layout por-sesión previo: nada que archivar, pero marca el paso como corrido.
	}
	if err != nil {
		return fmt.Errorf("edgemigrate: listar %s: %w", sessionsDir, err)
	}

	// Subdirectorios de sesión a archivar (sessions/<id>/). Ficheros sueltos bajo sessions/ (si los hubiera)
	// no son del layout por-sesión: se ignoran.
	var subdirs []string
	for _, e := range entries {
		if e.IsDir() {
			subdirs = append(subdirs, e.Name())
		}
	}
	if len(subdirs) == 0 {
		return markMigrated() // sessions/ existe pero vacío (instalación limpia): marca el paso; no re-archivar luego.
	}

	// Archiva cada subdir: sessions/<id>/ -> _archived-pre-022/sessions/<id>/.
	archivedSessions := filepath.Join(archiveRoot, sessionsDirName)
	if err := os.MkdirAll(archivedSessions, 0o700); err != nil {
		return fmt.Errorf("edgemigrate: crear %s: %w", archivedSessions, err)
	}
	for _, name := range subdirs {
		src := filepath.Join(sessionsDir, name)
		dst := filepath.Join(archivedSessions, name)
		if err := moveDir(src, dst); err != nil {
			return fmt.Errorf("edgemigrate: archivar %s -> %s: %w", src, dst, err)
		}
	}

	log.Warn("estado multi-sesión por-directorio retirado (BD única): re-empareja tus teléfonos",
		"archived_dir", archiveRoot,
		"reason", "modelo BD única DEK-por-dispositivo (ADR-0018); las sesiones ACTIVAS se restauran en T6.5")

	// Deja sessions/ vacío (queda como estaba, ya sin subdirs). Asegura su existencia por si acaso.
	if err := os.MkdirAll(sessionsDir, 0o700); err != nil {
		return fmt.Errorf("edgemigrate: crear %s: %w", sessionsDir, err)
	}
	return nil
}

// archiveFile mueve src a archiveDir/<base(src)> si src existe (creando archiveDir bajo demanda). Devuelve
// moved=true si movió algo; (false, nil) si src no existe (nada que archivar).
func archiveFile(src, archiveDir string) (bool, error) {
	info, err := os.Lstat(src)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("edgemigrate: inspeccionar %s: %w", src, err)
	}
	if info.IsDir() {
		// Defensa: las rutas planas heredadas son ficheros; un directorio aquí no es del layout single-sesión.
		return false, nil
	}
	if err := os.MkdirAll(archiveDir, 0o700); err != nil {
		return false, fmt.Errorf("edgemigrate: crear %s: %w", archiveDir, err)
	}
	dst := filepath.Join(archiveDir, filepath.Base(src))
	if err := moveFile(src, dst); err != nil {
		return false, fmt.Errorf("edgemigrate: archivar %s -> %s: %w", src, dst, err)
	}
	return true, nil
}

// moveFile mueve src a dst. Intenta os.Rename (atómico, barato) y SOLO si el destino está en otro
// volumen —error EXDEV "cross-device link", p.ej. data_dir y CWD en discos distintos (D3, MP-02)—
// cae a copy+remove. Cualquier otro error se propaga tal cual. Preserva permisos restrictivos (0600)
// del store/DEK, coherente con la sensibilidad del material (ADR-0007).
func moveFile(src, dst string) error {
	err := os.Rename(src, dst)
	if err == nil {
		return nil
	}
	if !errors.Is(err, syscall.EXDEV) {
		return err
	}
	// Cross-device: no se puede renombrar entre volúmenes; copiamos los bytes y borramos el origen.
	if err := copyFile(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

// copyFile copia src a dst creando dst con permisos 0600 (material sensible; el dir ya es 0700) y
// sincronizando a disco antes de cerrar, para que el remove posterior de moveFile no pierda datos.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// moveDir mueve el DIRECTORIO src a dst. Intenta os.Rename (atómico) y SOLO ante EXDEV (otro volumen,
// D3/MP-02) cae a copia recursiva + RemoveAll del origen. El RemoveAll SOLO ocurre si la copia completó
// bien (nunca se pierde el árbol viejo: es la fuente de T6.5). Cualquier otro error se propaga.
func moveDir(src, dst string) error {
	err := os.Rename(src, dst)
	if err == nil {
		return nil
	}
	if !errors.Is(err, syscall.EXDEV) {
		return err
	}
	// Cross-device: copia recursiva y, solo si toda la copia fue OK, borra el origen.
	if err := copyTree(src, dst); err != nil {
		return err
	}
	return os.RemoveAll(src)
}

// copyTree copia recursivamente el árbol src -> dst (directorios 0700, ficheros 0600 vía copyFile). Es el
// fallback EXDEV de moveDir; preserva permisos restrictivos coherentes con la sensibilidad del store/DEK.
func copyTree(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return copyFile(src, dst)
	}
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := copyTree(filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// dirExists indica si path existe y es un directorio.
func dirExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return info.IsDir(), nil
}
