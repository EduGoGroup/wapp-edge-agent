# Changelog — wapp-edge-agent

El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.0.0/)
y [Versionado Semántico](https://semver.org/lang/es/).

## [Unreleased]

## [0.1.0] - 2026-08-13

Primer tag de este repositorio. `wapp-edge-agent` viene operando desde su
origen **sin versionar** —cero tags, cero CHANGELOG—, así que esta entrada no
resume cambios frente a una versión previa: resume el **estado en que se
corta** el daemon al estrenar versionado semántico.

### Added

- **Núcleo daemon 24/7** (`cmd/agent`): socket `whatsmeow` siempre abierto,
  envío y recepción en tiempo real. Rol de **despachador** (ADR-0005): la
  nube arma el payload completo y el Edge lo entrega contra WhatsApp; nunca
  decide flujos ni llama endpoints de negocio. Multiplexa **N
  sesiones/teléfonos** sobre un único stream a la nube (ADR-0008).
- **Tiempo real, no lotes** (ADR-0037): al reconectar, la ráfaga acumulada
  mientras el socket estuvo caído no se ingiere — se compara el
  `Info.Timestamp` de cada entrante (reloj del servidor) contra el inicio de
  la conexión menos un margen configurable
  (`WAPP_AGENT_INBOUND_MARGIN_SECONDS`, 300 s por defecto), y los ecos
  propios (`IsFromMe`) ya no suben a la nube.
- **Store cifrado** (`internal/adapters/cryptostore`, ADR-0018): BD única
  SQLite dialecto-conmutable (`sqlite` pure-Go por defecto, `postgres`
  opcional bajo build-tag), campos `whatsmeow` cifrados con AES-256-GCM y DEK
  por dispositivo; failover multi-dispositivo por número sin re-escanear QR.
- **Doble llave zero-knowledge** (ADR-0007): la DEK vive en el keystore del
  SO —Keychain (macOS) / DPAPI (Windows) / Secret Service (Linux)—, en RAM
  desde el arranque en modo always-on, y nunca llega a la nube; el **lease**
  lo emite y revoca el servidor (kill-switch anti-clon).
  `CanOperate = hasDEK ∧ leaseVigente`.
- **Ruta sagrada del store** (`data_dir`, ADR-0016): default absoluto en el
  home del usuario por sistema operativo, nunca rutas de sistema con root;
  sobrevive a un reinicio sin re-emparejar.
- **Plano de control sin systray** (ADR-0013/0014/0015): el núcleo expone un
  contrato HTTP `/v1` sobre un Unix domain socket co-ubicado; el segundo
  binario `wapp-ctl` (supervisor loopback `127.0.0.1:8105`) arranca/para el
  núcleo, hace reverse-proxy al socket y sirve la web de onboarding con QR
  local sincrónico (sin relay SSE a la nube).
- **CloudLink** (`internal/adapters/cloudlink`): cliente gRPC bidi-stream
  saliente con mTLS, enrolamiento por código de un solo uso, reconexión con
  backoff exponencial + jitter y **outbox durable en SQLite** (ADR-0003) que
  drena al reconectar.
- **Telemetría y diagnóstico** (ADR-0023): heartbeat con `SessionHealth`
  (estado del socket, motivo de degradación, edad del último entrante,
  latencia de carga de DEK, profundidad del outbox, `binary_version`,
  uptime — solo metadatos, frontera zero-knowledge dura), watchdogs por
  sesión y bundle de diagnóstico bajo demanda (log tail saneado + dump de
  goroutines + JSON de subsistemas) truncado bajo el límite de 4 MiB del
  transporte.
- **Clasificador de intenciones — opcional** (`internal/adapters/intent`):
  integración con el repo hermano `edge/wapp-edge-intent` (Ollama,
  `qwen3:1.7b` por defecto), OFF salvo `WAPP_AGENT_INTENT_ENABLED=true`, que
  aplica el catálogo de intenciones empujado por la nube (`ConfigUpdate
  kind:intents`).
- **Empaque multiplataforma**: instalador `.pkg`/`.dmg` sin firmar +
  LaunchAgent por-usuario en macOS, unit systemd de usuario en Linux,
  autostart por PowerShell en Windows; binario pure-Go (sin CGO salvo el
  Keychain de macOS).

### Plan 055 — kill-switch anti-clon (lo último que entra en este corte)

- **Persiste la clave pública del lease** recibida en el enrolamiento
  (`EnrollEdgeResponse.lease_pubkey`, Ed25519 en hex), lista para validar
  offline la firma del lease sin copiarla a mano.
- **Modo sombra del gate**: el Edge queda preparado para validar la firma del
  lease sin todavía cortar la operación —solo observa—, con el default
  anclado fail-closed en la capa de config y declarado en el log de arranque
  para que el modo sombra sea auditable.
