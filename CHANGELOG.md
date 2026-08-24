# Changelog — wapp-edge-agent

El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.0.0/)
y [Versionado Semántico](https://semver.org/lang/es/).

## [Unreleased]

### Added

Plan 044 · Ola 1.7, lado Edge. El Cloud gana dos perillas sobre cada inferencia,
al runner de Ollama deja de morírsele la caché de prefijos, y la latencia deja
de publicarse como un número que mezclaba dos regímenes.

- **`max_output_tokens` → `num_predict`** (T1.7-3). El presupuesto de salida lo
  fija el Cloud por petición; el Edge aplica su default
  (`WAPP_WORKER_NUM_PREDICT`, 256) solo cuando el Cloud calla. Se aplica
  **verbatim, el cero incluido**: el contrato lo declara `optional` justo para
  que ese cero se pueda pedir.
- **`class` como telemetría y nada más** (T1.7-3). Llega al log y al heartbeat;
  **no entra en ninguna decisión** — ni breaker, ni aforo, ni plazo. Lo custodia
  un test estructural sobre el AST del paquete, no N tests de conducta.
- **`warmup` + `keep_alive`** (T1.7-4). La marca de calentamiento llega por fin
  al cajero, y el Edge manda `keep_alive` en cada petición
  (`WAPP_WORKER_KEEP_ALIVE_SECONDS`, default `-1` = para siempre). Va en el
  **primer nivel** de `/api/chat`: dentro de `options`, Ollama lo ignora en
  silencio.
- **Prefill y generación, medidos por separado** (T1.7-5). El heartbeat estrena
  `inference_prefill`, `inference_generation`, `inference_by_regime` e
  `inference_by_class`. El cuantil viaja **atado a su muestra**, y la
  **presencia** distingue «no medible» de «medido y sale cero».
- **Umbrales de régimen configurables** (`WAPP_WORKER_PREFILL_FRIO_MS` /
  `_CALIENTE_MS`, 5000 / 2000). Entre los dos queda **`templado`**, la franja
  que los umbrales del plan no cubrían; su conteo es la señal de que hay que
  recalibrarlos. Una pareja invertida cae **entera** al default, con aviso.
- **El latido `cajero: arrancando` publica la config EFECTIVA** de esta ola:
  `keep_alive_s`, `prefill_caliente_ms`, `prefill_frio_ms` y
  `num_predict_default`. El `.env` de la máquina pisa al default del código y
  eso no lo ve ningún test — la verificación se hace leyendo esta línea.

### Fixed

- **El cable daemon→cajero tiraba campos en silencio.** `cajerosock.PeticionWire`
  no nombraba `warmup`, así que la marca moría al cruzar el socket unix: sin
  error, sin log, y el cajero se comportaba como si el Cloud no hubiera dicho
  nada. Los tres campos nuevos cruzan ahora el cable, y un **test de simetría
  por reflexión** entre el puerto y el wire impide que el próximo se pierda igual.
- **El README decía que `WAPP_WORKER_INFERENCE_TIMEOUT_MS` valía `15000`.** Son
  **45000** desde la Ola 1.6. No era una errata inocua: el umbral de lentitud del
  breaker **deriva** de ese número.

### Changed

- `wapp-cloudlink` **v0.15.0 → v0.16.0** y `wapp-edge-intent` **v0.2.0 → v0.3.0**.

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
