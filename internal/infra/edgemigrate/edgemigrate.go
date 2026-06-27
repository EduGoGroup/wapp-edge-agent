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
	"io/fs"
	"os"
	"path/filepath"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// archiveDirName es el subdirectorio (bajo data_dir) donde se archiva el estado single-sesión retirado.
const archiveDirName = "_archived-pre-008"

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
	if err := os.Rename(src, dst); err != nil {
		return false, fmt.Errorf("edgemigrate: archivar %s -> %s: %w", src, dst, err)
	}
	return true, nil
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
