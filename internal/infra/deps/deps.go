// Package deps ancla dependencias del spike que aún no tienen consumidor real.
//
// Los blank-imports de abajo existen solo para que `go mod tidy` no elimine de
// go.mod las librerías que la Fase 1 usará (socket WhatsApp, store SQLite
// pure-Go y render del QR en terminal). Se retiran en cuanto el código real
// (adaptadores whatsmeow / cryptostore / plano de control) las importe.
package deps

import (
	// _ "github.com/mdp/qrterminal/v3" — render del QR de emparejamiento en terminal.
	_ "github.com/mdp/qrterminal/v3"
	// _ "go.mau.fi/whatsmeow" — cliente WhatsApp (socket 24/7, envío y recepción).
	_ "go.mau.fi/whatsmeow"
	// _ "modernc.org/sqlite" — driver SQLite pure-Go (sin CGO) para el store cifrado.
	_ "modernc.org/sqlite"
)
