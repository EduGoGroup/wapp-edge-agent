package cryptostore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// TestOpenSessionContainer_IsolatedByDEK: dos store.db de sesión INDEPENDIENTES (A y B), cada uno con
// SU DEK. Cada Container persiste y descifra SOLO con su DEK; cruzar las DEKs FALLA (GCM no autentica).
// Es el criterio T2(a)/(b): N Container aislados, uno por .db, sin compartir la DEK.
func TestOpenSessionContainer_IsolatedByDEK(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	pathA := mkStorePath(t, dir, "a")
	pathB := mkStorePath(t, dir, "b")

	dekA := newDEK(t)
	dekB := newDEK(t)

	devA, _ := syntheticDevice(t)
	srcNoiseA := *devA.NoiseKey.Priv
	devB, _ := syntheticDevice(t)
	srcNoiseB := *devB.NoiseKey.Priv

	// --- Persistir cada device en SU store con SU DEK (luego cerrar el handle) ---
	putDevice(t, ctx, pathA, dekA, devA)
	putDevice(t, ctx, pathB, dekB, devB)

	// --- Reabrir cada uno con SU DEK: descifra y el NoiseKey sobrevive el round-trip ---
	if *loadOK(t, ctx, pathA, dekA, *devA.ID).NoiseKey.Priv != srcNoiseA {
		t.Error("A: NoiseKey no sobrevivió el round-trip con su DEK")
	}
	if *loadOK(t, ctx, pathB, dekB, *devB.ID).NoiseKey.Priv != srcNoiseB {
		t.Error("B: NoiseKey no sobrevivió el round-trip con su DEK")
	}

	// --- Cruzar DEKs: abrir el store de A con la DEK de B descifra MAL → LoadDevice DEBE fallar ---
	contCross, dbCross, err := OpenSessionContainer(ctx, pathA, dekB)
	if err != nil {
		t.Fatalf("OpenSessionContainer (cross) no debía fallar al abrir, solo al descifrar: %v", err)
	}
	defer func() { _ = dbCross.Close() }()
	if _, err := LoadDevice(ctx, contCross, *devA.ID); err == nil {
		t.Fatal("LoadDevice del store de A con la DEK de B DEBÍA fallar (GCM auth tag) y no falló")
	}
}

// putDevice abre el store de la sesión con su DEK, persiste el device cifrado y cierra el handle.
func putDevice(t *testing.T, ctx context.Context, path string, dek []byte, dev *store.Device) {
	t.Helper()
	cont, db, err := OpenSessionContainer(ctx, path, dek)
	if err != nil {
		t.Fatalf("OpenSessionContainer (%s): %v", path, err)
	}
	defer func() { _ = db.Close() }()
	if err := cont.PutDevice(ctx, dev); err != nil {
		t.Fatalf("PutDevice (%s): %v", path, err)
	}
}

// loadOK reabre el store en path con dek, carga el device por jid y exige que exista (no nil, sin error).
func loadOK(t *testing.T, ctx context.Context, path string, dek []byte, jid types.JID) *store.Device {
	t.Helper()
	cont, db, err := OpenSessionContainer(ctx, path, dek)
	if err != nil {
		t.Fatalf("OpenSessionContainer (reabrir %s): %v", path, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dev, err := LoadDevice(ctx, cont, jid)
	if err != nil {
		t.Fatalf("LoadDevice (%s): %v", path, err)
	}
	if dev == nil {
		t.Fatalf("LoadDevice (%s) devolvió nil tras persistir", path)
	}
	return dev
}

// mkStorePath devuelve <base>/<sub>/store.db creando el directorio padre (emula sessions/<id>/).
func mkStorePath(t *testing.T, base, sub string) string {
	t.Helper()
	d := filepath.Join(base, sub)
	if err := os.MkdirAll(d, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", d, err)
	}
	return filepath.Join(d, "store.db")
}
