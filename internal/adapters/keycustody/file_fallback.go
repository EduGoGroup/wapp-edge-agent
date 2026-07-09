//go:build !darwin && !windows && !linux

package keycustody

// file_fallback.go define el NewFileCustody exportado para las plataformas que NO tienen keystore de SO
// integrado en este plan (ni darwin/Keychain, ni windows/DPAPI, ni linux/Secret Service): p. ej. *BSD.
// Ahí la custodia v1 es el archivo plano 0600 (FileCustody). Windows y Linux definen su propio
// NewFileCustody (DPAPI / Secret Service) y usan newFileCustody solo como fallback; darwin usa el Keychain.

// NewFileCustody construye la custodia de la DEK en el archivo plano 0600. MISMA firma que en el resto de
// plataformas para que el wiring (cmd/agent, sessionmgr, edgemigrate) no cambie.
func NewFileCustody(path string) *FileCustody {
	return newFileCustody(path)
}
