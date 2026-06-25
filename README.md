# wapp-edge-agent (Pieza 01)

Daemon Go 24/7 que se instala en el equipo del cliente (nano/micro empresario).
Mantiene el socket de WhatsApp siempre abierto, cifra todo en reposo y vive en
la bandeja del sistema (Windows, macOS, Linux).

## Rol en wApp

Es el único punto que habla con WhatsApp (vía `whatsmeow`). La lógica de
negocio vive en la nube; el Edge es un **despachador**: recibe el payload
completo de la nube por CloudLink y lo despacha. También reenvía los mensajes
entrantes a la nube para que el Motor de Flujos los procese.

## Tecnología

| Decisión | Detalle |
|---|---|
| Lenguaje | Go 1.23, binario estático (sin CGO) |
| WhatsApp | `whatsmeow` (adaptador copiado de `edugo-api-messaging`, escucha reactiva del prototipo wApp) |
| Store | SQLite pure-Go (`modernc.org/sqlite`) + `cryptostore` (campos cifrados AES-256-GCM, portado de PostgreSQL) |
| Canal cloud | gRPC bidi-stream saliente + mTLS (ver `cloud/wapp-cloudlink`) |
| DEK | Custodiada en el keystore del SO del cliente; la nube nunca la ve (zero-knowledge) |
| Lease | Licencia operativa emitida por la nube; kill-switch anti-clon |
| UI | Systray + mini-UI local (QR sincrónico local, sin relay SSE) |

## Cómo correrá (placeholder)

```bash
# Compilar (placeholder)
go build -o bin/agent ./cmd/agent

# Ejecutar (placeholder)
./bin/agent --config /etc/wapp-edge-agent/config.yaml
```

## Estado

**Greenfield.** Este repositorio es solo el scaffold inicial (estructura sin
lógica). Ver `CLAUDE.md` para contexto arquitectónico y `../../docs/piezas/01-edge-agent.md`
para la especificación completa.

> El module path `github.com/wApp/wapp-edge-agent` es un placeholder ajustable
> al repositorio Git real cuando se publique.
