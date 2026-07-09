package keycustody

import (
	"bytes"
	"fmt"
	"os"
)

// dekSink es el subconjunto de app.KeyCustody que necesita la migración archivo→custodia: persistir y
// releer para VERIFICAR antes de borrar el archivo plano. No se importa internal/app para no acoplar el
// adaptador al dominio; la migración depende solo de estas dos operaciones. En darwin lo cumple el
// Keychain (keychain_darwin.go); en los tests, un doble en memoria del puerto.
type dekSink interface {
	Store(dek []byte) error
	Load() ([]byte, error)
}

// migrateFileToCustody importa una DEK guardada en un archivo plano legacy (legacyPath) hacia sink (p. ej.
// el Keychain) y BORRA el archivo SOLO tras verificar que la relectura desde sink devuelve exactamente los
// mismos bytes. Es la pieza PURE-GO (testeable en CI headless) de la migración dek-en-archivo → Keychain
// (Plan 023 · T2); el acceso real al Keychain queda en keychain_darwin.go.
//
// Idempotente y con fallback seguro (riesgo design §10: no perder la DEK):
//   - archivo ausente → (nil, false, nil): nada que migrar (re-ejecutar no rompe).
//   - tamaño != KeySize, o Store/verificación fallan → error y el archivo SE CONSERVA.
//   - éxito → (dek, true, nil) con el archivo ya borrado.
//
// El borrado final es best-effort: si Remove fallara tras verificar, la DEK ya está a salvo en la custodia
// y el llamador reintenta la limpieza en el siguiente Load (rama "la custodia ya tiene la DEK").
func migrateFileToCustody(legacyPath string, sink dekSink) (dek []byte, migrated bool, err error) {
	if legacyPath == "" {
		return nil, false, nil
	}

	raw, readErr := os.ReadFile(legacyPath)
	if readErr != nil {
		if os.IsNotExist(readErr) {
			return nil, false, nil // nada que migrar
		}
		return nil, false, fmt.Errorf("migración DEK: no se pudo leer %s: %w", legacyPath, readErr)
	}
	if len(raw) != KeySize {
		return nil, false, fmt.Errorf("migración DEK: %s mide %d bytes, se esperaban %d (archivo conservado)", legacyPath, len(raw), KeySize)
	}

	if storeErr := sink.Store(raw); storeErr != nil {
		return nil, false, fmt.Errorf("migración DEK: no se pudo importar a la custodia (archivo conservado): %w", storeErr)
	}

	// VERIFICAR la relectura desde la custodia ANTES de borrar el archivo: si no coincide, se conserva.
	got, loadErr := sink.Load()
	if loadErr != nil {
		return nil, false, fmt.Errorf("migración DEK: no se pudo releer desde la custodia (archivo conservado): %w", loadErr)
	}
	if !bytes.Equal(got, raw) {
		return nil, false, fmt.Errorf("migración DEK: la relectura desde la custodia no coincide (archivo conservado)")
	}

	// Solo ahora, con la DEK verificada en la custodia, se borra el archivo plano (best-effort).
	_ = os.Remove(legacyPath)
	return raw, true, nil
}
