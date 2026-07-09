// Package keycustody contiene los adaptadores del puerto app.KeyCustody: la custodia de la DEK (Data
// Encryption Key, 32 bytes, AES-256) del Edge Agent.
//
// Selección por PLATAFORMA en tiempo de compilación (patrón db_postgres; Plan 023 · T2 + Plan 024 · T2).
// El constructor exportado NewFileCustody(path) tiene UNA definición por SO, todas con la MISMA firma:
//   - darwin  → keychain_darwin.go: la DEK vive en el Keychain de macOS (Security.framework, CGO).
//   - windows → dpapi_windows.go: la DEK se cifra con DPAPI (ámbito usuario) y el blob va a <path>.dpapi.
//   - linux   → secretservice_linux.go: la DEK vive en el Secret Service (D-Bus, go-keyring); degrada al
//     archivo 0600 si el entorno es headless/sin D-Bus.
//   - resto   → file_fallback.go: archivo plano 0600 (FileCustody) como suelo.
//
// El tipo FileCustody + newFileCustody (file.go, //go:build !darwin) son el FALLBACK pure-Go que reusan
// Windows/Linux cuando el keystore del SO no está disponible.
//
// El único CGO es el Keychain, ACOTADO a keychain_darwin.go (//go:build darwin): DPAPI (golang.org/x/sys/
// windows) y Secret Service (go-keyring/godbus) son pure-Go, así los cross-compile windows/linux siguen
// pure-Go (ADR-0002, binario estático único). Este archivo, SIN build-tag, aloja lo COMÚN a todas las
// impls para no duplicarlo (tamaño de la DEK y errores del puerto).
//
// El cambio de custodio es solo un cambio de adaptador: ninguna otra capa conoce el backend. La DEK
// nunca sale hacia la nube (zero-knowledge, ADR-0007); este puerto opera solo en el equipo del cliente.
package keycustody

import (
	"errors"
	"fmt"
)

// KeySize es el tamaño exacto en bytes de la DEK (AES-256). Coincide con envelope.DEKSize; se replica
// aquí como constante local para no acoplar el adaptador al paquete envelope.
const KeySize = 32

// ErrKeySize indica que la DEK pasada a Store no mide exactamente KeySize bytes.
var ErrKeySize = fmt.Errorf("la DEK debe medir exactamente %d bytes (AES-256)", KeySize)

// ErrNoKey indica que se intentó Load una DEK que aún no ha sido custodiada.
var ErrNoKey = errors.New("no hay DEK custodiada")
