// Package keycustody contiene los adaptadores del puerto app.KeyCustody: la custodia de la DEK (Data
// Encryption Key, 32 bytes, AES-256) del Edge Agent.
//
// Selección por PLATAFORMA en tiempo de compilación (patrón db_postgres, Plan 023 · T2):
//   - darwin  → keychain_darwin.go: la DEK vive en el Keychain de macOS (Security.framework, CGO).
//   - !darwin → file.go: FileCustody, la custodia PROVISIONAL en archivo plano 0600 (Windows/Linux
//     migrarán a DPAPI/Secret Service en el Plan 024; hoy siguen pure-Go).
//
// El CGO del Keychain queda ACOTADO a keychain_darwin.go (//go:build darwin): el resto del árbol —y los
// cross-compile windows/linux— siguen pure-Go (ADR-0002, binario estático único). Este archivo, SIN
// build-tag, aloja lo COMÚN a ambas impls para no duplicarlo (tamaño de la DEK y errores del puerto).
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
