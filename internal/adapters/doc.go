// Package adapters contiene las implementaciones concretas de los puertos.
// Adaptadores previstos:
//   - whatsmeow/    → WhatsAppGateway: socket 24/7, QR, Send, Listen (reutiliza edugo-api-messaging + wApp prototipo).
//   - cryptostore/  → Store: cryptostore sobre SQLite pure-Go (modernc.org/sqlite, sin CGO); porta de PostgreSQL a SQLite.
//   - cloudlink/    → CloudLink: cliente gRPC bidi-stream saliente con mTLS (ver pieza 02).
//   - keycustody/   → KeyCustody: keystore del SO (Windows Credential Manager / macOS Keychain / Linux Secret Service).
//   - systray/      → mini-UI local: bandeja del sistema + menú (Inicio, Suspender, Cancelar, Emparejar).
package adapters
