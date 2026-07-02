package edgemigrate

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// newLogger devuelve un Logger que escribe en buf (para inspeccionar el WARN de re-emparejar).
func newLogger(buf *bytes.Buffer) sharedlogger.Logger {
	return sharedlogger.New(sharedlogger.WithWriter(buf))
}

// writeFile crea un fichero con contenido conocido (helper de fixtures).
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile %s: %v", path, err)
	}
}

// fileExists indica si path existe (fichero o dir).
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// TestArchivesLegacyFlatLayout: con store.db (+ sidecars WAL) y dek.key planos, la migración los mueve a
// _archived-pre-008/, crea sessions/ vacío y avisa por WARN. Nada se borra.
func TestArchivesLegacyFlatLayout(t *testing.T) {
	dataDir := t.TempDir()
	store := filepath.Join(dataDir, "wapp-edge.db")
	dek := filepath.Join(dataDir, "dek.key")
	writeFile(t, store, "STORE")
	writeFile(t, store+"-wal", "WAL")
	writeFile(t, store+"-shm", "SHM")
	writeFile(t, dek, "DEK")

	var buf bytes.Buffer
	if err := ArchiveLegacySingleSession(dataDir, store, dek, newLogger(&buf)); err != nil {
		t.Fatalf("ArchiveLegacySingleSession: %v", err)
	}

	// Los planos ya no están en su sitio.
	for _, p := range []string{store, store + "-wal", store + "-shm", dek} {
		if fileExists(p) {
			t.Fatalf("el fichero plano %s no se archivó (sigue en su sitio)", p)
		}
	}
	// Están en el archivo, con su contenido intacto (movidos, no borrados).
	archive := filepath.Join(dataDir, archiveDirName)
	for name, want := range map[string]string{
		"wapp-edge.db":     "STORE",
		"wapp-edge.db-wal": "WAL",
		"wapp-edge.db-shm": "SHM",
		"dek.key":          "DEK",
	} {
		got, err := os.ReadFile(filepath.Join(archive, name))
		if err != nil {
			t.Fatalf("leer archivado %s: %v", name, err)
		}
		if string(got) != want {
			t.Fatalf("contenido archivado %s = %q, esperaba %q", name, got, want)
		}
	}
	// sessions/ creado y vacío.
	if !fileExists(filepath.Join(dataDir, sessionsDirName)) {
		t.Fatal("se esperaba el directorio sessions/ creado")
	}
	// WARN de re-emparejar emitido.
	if !strings.Contains(buf.String(), "re-empareja") {
		t.Fatalf("se esperaba un WARN de re-emparejar; logs:\n%s", buf.String())
	}
}

// TestIdempotentSecondRunIsNoop: una segunda ejecución (con sessions/ ya creado) es no-op: no re-archiva
// ni vuelve a avisar, aunque reaparezca un fichero plano.
func TestIdempotentSecondRunIsNoop(t *testing.T) {
	dataDir := t.TempDir()
	store := filepath.Join(dataDir, "wapp-edge.db")
	dek := filepath.Join(dataDir, "dek.key")
	writeFile(t, store, "STORE")
	writeFile(t, dek, "DEK")

	var buf1 bytes.Buffer
	if err := ArchiveLegacySingleSession(dataDir, store, dek, newLogger(&buf1)); err != nil {
		t.Fatalf("primera ejecución: %v", err)
	}

	// Simula que el daemon ya creó un store NUEVO plano tras migrar (no debe re-archivarse).
	writeFile(t, store, "NUEVO")

	var buf2 bytes.Buffer
	if err := ArchiveLegacySingleSession(dataDir, store, dek, newLogger(&buf2)); err != nil {
		t.Fatalf("segunda ejecución: %v", err)
	}
	if strings.Contains(buf2.String(), "re-empareja") {
		t.Fatalf("la segunda ejecución NO debía volver a avisar; logs:\n%s", buf2.String())
	}
	// El store nuevo plano sigue en su sitio (no se archivó de nuevo).
	got, err := os.ReadFile(store)
	if err != nil || string(got) != "NUEVO" {
		t.Fatalf("el store nuevo se tocó indebidamente: got=%q err=%v", got, err)
	}
}

// TestFreshInstallJustCreatesSessions: sin ficheros planos previos, la migración solo crea sessions/
// (sin _archived-pre-008/ ni WARN).
func TestFreshInstallJustCreatesSessions(t *testing.T) {
	dataDir := t.TempDir()
	store := filepath.Join(dataDir, "wapp-edge.db")
	dek := filepath.Join(dataDir, "dek.key")

	var buf bytes.Buffer
	if err := ArchiveLegacySingleSession(dataDir, store, dek, newLogger(&buf)); err != nil {
		t.Fatalf("ArchiveLegacySingleSession: %v", err)
	}
	if !fileExists(filepath.Join(dataDir, sessionsDirName)) {
		t.Fatal("se esperaba sessions/ creado en instalación limpia")
	}
	if fileExists(filepath.Join(dataDir, archiveDirName)) {
		t.Fatal("no debía crearse _archived-pre-008/ sin nada que archivar")
	}
	if strings.Contains(buf.String(), "re-empareja") {
		t.Fatal("no debía avisar de re-emparejar en instalación limpia")
	}
}

// TestMoveFile_SameDirUsesRename: en el mismo volumen moveFile mueve el fichero (Rename) preservando
// bytes; el origen desaparece.
func TestMoveFile_SameDirUsesRename(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	dst := filepath.Join(dir, "dst.bin")
	writeFile(t, src, "PAYLOAD")

	if err := moveFile(src, dst); err != nil {
		t.Fatalf("moveFile: %v", err)
	}
	if fileExists(src) {
		t.Fatal("el origen debía desaparecer tras moveFile")
	}
	got, err := os.ReadFile(dst)
	if err != nil || string(got) != "PAYLOAD" {
		t.Fatalf("destino: got=%q err=%v", got, err)
	}
}

// TestCopyFile_BytesAndPerms: copyFile (el camino de fallback EXDEV) preserva los bytes y crea el
// destino con permisos restrictivos 0600 (material sensible del store/DEK). No se puede forzar EXDEV
// de forma portable en test, así que se ejercita el helper directamente entre dos dirs temporales.
func TestCopyFile_BytesAndPerms(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	src := filepath.Join(srcDir, "dek.key")
	dst := filepath.Join(dstDir, "dek.key")
	writeFile(t, src, "SECRET-DEK-BYTES")

	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil || string(got) != "SECRET-DEK-BYTES" {
		t.Fatalf("bytes copiados: got=%q err=%v", got, err)
	}
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("stat destino: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("permisos del destino: got %o, want 0600", perm)
	}
	// copyFile NO borra el origen (eso lo hace moveFile tras copiar): sigue presente.
	if !fileExists(src) {
		t.Fatal("copyFile no debe borrar el origen")
	}
}

// TestDoesNotTouchOtherFiles: un fichero ajeno (p.ej. de la nube) en data_dir queda intacto.
func TestDoesNotTouchOtherFiles(t *testing.T) {
	dataDir := t.TempDir()
	store := filepath.Join(dataDir, "wapp-edge.db")
	dek := filepath.Join(dataDir, "dek.key")
	other := filepath.Join(dataDir, "cloud-cache.bin")
	writeFile(t, store, "STORE")
	writeFile(t, other, "AJENO")

	var buf bytes.Buffer
	if err := ArchiveLegacySingleSession(dataDir, store, dek, newLogger(&buf)); err != nil {
		t.Fatalf("ArchiveLegacySingleSession: %v", err)
	}
	got, err := os.ReadFile(other)
	if err != nil || string(got) != "AJENO" {
		t.Fatalf("el fichero ajeno se tocó: got=%q err=%v", got, err)
	}
}
