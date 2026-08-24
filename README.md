# wapp-edge-agent (Pieza 01)

Núcleo del ecosistema wApp que corre **en el equipo del cliente**: un daemon Go que
mantiene el socket de WhatsApp (`whatsmeow`) **siempre abierto**, envía y recibe 24/7 y
cifra todo en reposo. Es un **despachador zero-knowledge**: la nube arma el payload
completo y el Edge lo entrega contra WhatsApp; la nube **nunca** ve la DEK ni las llaves
privadas. Un mismo Edge multiplexa **N sesiones/teléfonos** sobre un único stream a la nube.

- **Module**: `github.com/EduGoGroup/wapp-edge-agent` · **Go 1.26.0** · binario pure-Go
  (SQLite `modernc.org/sqlite`, sin CGO salvo el Keychain de macOS).
- **Rol arquitectónico**: DESPACHADOR (ADR-0005). No decide flujos ni llama endpoints de
  negocio: recibe la orden completa por CloudLink, la despacha y reenvía lo entrante.

## Estado

**Implementado y en piloto.** Planes 002–031 cerrados y publicados; el auto-update
(Plan 032) está diseñado pero aún no ejecutado. Consumido en vivo contra la nube pública
por WhatsApp real. Ver el índice de planes en `../../docs/` (carpetas `completed/`,
`parcial/`) para el detalle de qué está construido.

## Arquitectura interna

Hexagonal: `internal/domain` (entidades), `internal/app` (casos de uso + puertos),
`internal/adapters` (implementaciones) e `internal/infra` (config, db, wiring, logger,
enroll, watchdog, migración).

- **Núcleo (daemon) + plano de control** (ADR-0014/0015). El núcleo (`agent serve`) expone
  un contrato **HTTP `/v1` sobre un Unix domain socket** co-ubicado (sin puerto de red):
  `/v1/sessions/pair`, `/v1/sessions/{id}/pair`, `/v1/enroll`, `/v1/logs`,
  `/v1/intent/status`. **No hay systray.**
  > **`/v1/intent/status` informa del CONTRATO de intenciones, no del clasificador**
  > (Plan 051 O3 · T3.0): `{enabled, model, config_version, clasifica_en, worker_status_url}`.
  > `circuit` y `ollama_ok` **se retiraron** —vivían en el decorador inline, que ya no existe—;
  > el estado del clasificador se pregunta en **`GET /v1/cajero/status`**.
- **Worker-cajero (`agent cajero`)** — segundo modo del mismo binario y **2.º hijo supervisado**
  por `wapp-ctl` (Plan 051, ADR-0038). Es **el único proceso que habla con Ollama**: reclama
  filas de `cola_entrantes.db`, clasifica y deja el sobre ahí. El núcleo solo encola y despacha.
- **`wapp-ctl`** (`cmd/wapp-ctl`) es el segundo binario: un **supervisor loopback**
  (`127.0.0.1:8105` por defecto) que arranca/para el núcleo **y el cajero**, hace
  **reverse-proxy** al socket `/v1` y sirve la **web de onboarding** (`internal/webui`, QR local
  sincrónico, sin relay SSE). Sirve por su cuenta (sin proxyar) `/v1/daemon/*` y `/v1/cajero/*`.
  El QR se genera y se escanea en el propio equipo.

> 🔴 **`<data_dir>/cola_entrantes.db` es condición de arranque** desde el 2026-08-17 (Plan 051
> O3): si no abre o no migra, **ni el núcleo ni el cajero levantan**. Retirado el camino inline,
> la cola es el único camino de entrega; arrancar sin ella sería acusar ✓✓ al cliente y perder
> cada entrante en silencio. En campo eso se traduce en permisos y espacio del `data_dir`.
- **Adapters** (`internal/adapters`): `whatsmeow` (socket, QR, envío, handlers de
  eventos), `cloudlink` (cliente gRPC bidi-stream saliente con mTLS), `cryptostore`
  (SQLite cifrado campo a campo AES-256-GCM con la DEK), `keycustody` (keystore del SO),
  `outbox` (cola durable SQLite, ADR-0003), `colaentrantes` (cola durable de **entrada**,
  BD propia), `sessionstore`, `supervisor`, `control` (plano de control), `edgeconfig`
  (config empujada por la nube) e `intent` (hoy **solo** el endpoint de estado del contrato:
  el decorador que clasificaba inline se retiró en el Plan 051 O3).
- **Salud y diagnóstico**: watchdogs de salud por sesión y un ring buffer de logs que
  alimenta el bundle de diagnóstico bajo demanda.

## Compilar y empaquetar

El `Makefile` compila **ambos** binarios (`agent` + `wapp-ctl`) con la versión inyectada
desde git (`-ldflags -X main.Version`) a un directorio hermano `dist/<os>-<arch>/`.

```bash
make build                # default: darwin/arm64 (CGO nativo, base del Keychain)
make build-darwin-arm64
make build-windows-amd64  # matriz cross-compile pure-Go (CGO=0)
make build-windows-arm64
make build-linux-amd64
make build-linux-arm64
make build-all            # matriz completa de 5 targets

make pkg                  # instalador .pkg macOS por-usuario, SIN firmar (consume dist/darwin-arm64)
make dmg                  # envuelve el .pkg en un .dmg
make dist-windows-amd64   # artefacto portable .zip (2 binarios + bootstrap público)
make dist-linux-amd64     # artefacto portable .tar.gz
make dist-all             # ambos portables

make fmt-check vet test   # gate estático local (espejo del CI); test = go test -race ./...
make version              # imprime la Version que se inyectaría
```

Empaque por SO en `packaging/`:

- **macOS** (`packaging/macos`): `.pkg`/`.dmg` sin firmar + **LaunchAgent** por-usuario
  (`com.wapp.edge.plist`). El target `pkg-verify-zk` aborta si se cuela material secreto.
- **Linux** (`packaging/linux`): unit **systemd** de usuario (`wapp-edge.service`) +
  `install-autostart.sh`.
- **Windows** (`packaging/windows`): autostart por **PowerShell** (`install-autostart.ps1`)
  + `run-edge.cmd`.

## Ejecutar

```bash
agent enroll <código>   # enrola el Edge (código de un solo uso) → obtiene cert de Edge/tenant
agent serve             # arranca el daemon multi-sesión (socket /v1 + stream CloudLink)
```

La configuración se carga de YAML + overlay de entorno. La ruta del YAML se resuelve del
data_dir sagrado por SO, o se fija con **`WAPP_AGENT_CONFIG`** (lo hacen el instalador y el
LaunchAgent). En operación normal el núcleo lo arranca `wapp-ctl` bajo demanda vía
`POST /v1/daemon/start`.

## Variables de entorno

Prefijo **`WAPP_AGENT_`**. Precedencia: defaults < YAML < entorno.

| Variable | Default | Propósito |
|---|---|---|
| `WAPP_AGENT_CONFIG` | (data_dir)/config.yaml | Ruta del YAML de configuración. |
| `WAPP_AGENT_DATA_DIR` | home por SO¹ | Ruta sagrada del store (layout multi-sesión, ADR-0016). Se absolutiza al cargar. |
| `WAPP_AGENT_LOG_LEVEL` | `info` | Nivel de log: debug/info/warn/error. |
| `WAPP_AGENT_MAX_SESSIONS` | `5` | Límite suave de sesiones simultáneas (guardarraíl, no invariante). |
| `WAPP_AGENT_MULTIDEVICE_PER_ACCOUNT` | `1` | Dispositivos vivos por cuenta/número (failover, ADR-0018). Clamp [1,4]. Resiliencia, no sigilo. |
| `WAPP_AGENT_PUSH_NAME` | `wApp` | Push name de fallback para la presencia si el store no conoce el real. |
| `WAPP_AGENT_DB_DIALECT` | `sqlite` | Motor SQL: `sqlite` (default) o `postgres` (solo con build-tag `postgres`). |
| `WAPP_AGENT_DB_DSN` | (vacío) | Cadena de conexión cuando el dialecto la exige (Postgres). |
| `WAPP_AGENT_OUTBOX_MAX_EVENTS` | `10000` | Tope del outbox durable; al llenarse aplica drop-oldest. |
| `WAPP_AGENT_OUTBOX_TTL_HOURS` | `0` | TTL de evento del outbox; 0 = desactivado (durabilidad primero). |
| `WAPP_AGENT_INBOUND_MARGIN_SECONDS` | `300` | Margen de la **ventana de ingesta** (ADR-0037, ver ³). Se descarta todo entrante anterior a `inicio de conexión − margen`, y lo descartado **no sube a la nube**. `<=0` cae al default. Clave YAML: `inbound_margin_seconds`. |
| `WAPP_AGENT_INBOUND_STATS_EVERY_MS` | `60000` | Cadencia del bloque de **latencia del handler de entrantes** (Plan 051 · T3.13): p50/p95/p99 del tiempo que `onMessage` pasa en el hilo de whatsmeow, más el desglose de la cola. Es lo que hace medible «handler < 50 ms p99» (INV-051.2). **`0` lo desactiva** (guardarraíl asimétrico: sólo lo negativo cae al default); el bloque final del apagado se emite igual. Se lee con `grep 'latencia del handler' <fichero de log>`. Clave YAML: `inbound_stats_every_ms`. |
| `WAPP_AGENT_DIAG_LOG_LINES` | `500` | Líneas del ring buffer en el bundle de diagnóstico. |
| `WAPP_AGENT_CONTROL_SOCKET_PATH` | `wapp-edge.sock` | Ruta del Unix socket del plano de control `/v1`. |
| `WAPP_AGENT_CLOUDLINK_ENDPOINT` | (vacío²) | Endpoint gRPC del stream Connect (mTLS). Vacío = sin nube (LogSink). |
| `WAPP_AGENT_CLOUDLINK_ENROLLMENT_ENDPOINT` | (vacío) | Endpoint gRPC de enrolamiento (TLS de servidor); vacío desactiva `enroll`. |
| `WAPP_AGENT_CLOUDLINK_RUNTIME_PORT` | `8101` | Puerto con el que el enroll deriva el endpoint de runtime. |
| `WAPP_AGENT_CLOUDLINK_ACTIVATION_CODE` | (vacío) | Código de activación (también como arg de `agent enroll`). |
| `WAPP_AGENT_CLOUDLINK_COMMAND_TIMEOUT_SECONDS` | `30` | Deadline por operación del demux CloudLink. |
| `WAPP_AGENT_INTENT_ENABLED` | `false` | Activa el clasificador LLM local de intenciones. |
| `WAPP_AGENT_INTENT_OLLAMA_URL` | `http://127.0.0.1:11434` | URL del Ollama local (loopback). |
| `WAPP_AGENT_INTENT_MODEL` | `qwen3:1.7b` | Modelo del clasificador. |
| ~~`WAPP_AGENT_INTENT_WAIT_MS`~~ | — | **RETIRADA** (Plan 044 Ola 1.6, T1.6-5 · [ADR-0045](../../docs/adr/0045-la-inferencia-la-orquesta-el-cloud-el-edge-la-sirve.md)). Era el presupuesto de espera del despachador: cuánto retenía la entrega de un mensaje aguardando a que el worker-cajero dejara su intent. **La clasificación pasó de PUSH a PULL**: el Edge entrega cada entrante al instante y es el Cloud quien PIDE la inferencia cuando la necesita, así que no hay espera que calibrar. **NO tiene sustituta**, y ése es el aviso: si sigue puesta en el entorno, el arranque emite un `Warn` y la ignora. La medida que la mató: de **430** inferencias, **1** cupo en la ventana de 4 s. |
| ~~`WAPP_AGENT_INTENT_TIMEOUT_MS`~~ | — | **RETIRADA** (Plan 051 Ola 3, T3.1). Era el plazo del camino inline de clasificación. Si sigue puesta en el entorno, el arranque del daemon emite un `Warn` y la ignora. Lo que sí sigue vivo es `WAPP_WORKER_INFERENCE_TIMEOUT_MS` (timeout de **una** inferencia), que bajo pull acota la inferencia que el Edge SIRVE al Cloud. |
| `WAPP_AGENT_PLATFORM_API_BASE_URL` | `http://localhost:8103` | URL base de la API pública HTTP de la plataforma cloud (`/api/v1/...`) que usa `wapp-ctl` para el signup del Edge (C-03/T3.5) — llamada DIRECTA por red, no relayada por el socket local. **A diferencia de `CLOUDLINK_ENDPOINT`, NO se deriva del enrolamiento**: en cualquier instalación fuera de la máquina de desarrollo hay que fijarla a mano, o el botón "solicitar acceso" llamará al propio `localhost` del Edge y nunca funcionará (queda constancia en el log de arranque mientras siga en el default, ver ⁴). `http://` solo se admite contra loopback (`localhost`/`127.0.0.1`/`::1`); con cualquier otro host el signup se rehúsa (nunca manda la contraseña en claro por la red) — usa `https://`. |
| `WAPP_LOG_FILE` | (vacío) | Redirige el log a archivo (útil bajo LaunchAgent/systemd). |

¹ macOS `~/Library/Application Support/wApp/edge` · Linux `~/.config/wApp/edge`
(o `$XDG_CONFIG_HOME`) · Windows `%AppData%\wApp\edge`.
² Si el Endpoint no viene por YAML/env, se lee del archivo `<data_dir>/cloudlink-endpoint`
que el `enroll` persiste (material público host:puerto).

³ **Ventana de ingesta — el Edge atiende tiempo real (ADR-0037).** Al reconectar, WhatsApp
entrega de golpe la ráfaga de lo ocurrido mientras el socket estuvo caído. Esa ráfaga **no se
ingiere**: el Edge compara el `Info.Timestamp` de cada entrante (reloj del **servidor**) contra
el **inicio de su conexión**, y descarta lo anterior a ese instante menos el margen. Quien
decide es el sello temporal, **no** los corchetes de sincronización offline.

El margen existe por el **desfase de reloj**: `whatsmeow` lo mide pero lo guarda privado, así
que al no poder corregirlo hay que absorberlo, y eso lo pone en minutos. De paso es la **ventana
de rescate de la microcaída**: lo enviado en los 5 min previos a reconectar se trata como vivo.
Un margen `0` no se acepta (guardarraíl): descartaría tráfico vivo en cuanto el reloj local
fuera un segundo por delante del servidor.

⚠️ Junto con esto, los **ecos propios** (`IsFromMe`) dejaron de subir a la nube. Si echas en
falta mensajes en el Cloud tras una caída larga, **es el comportamiento esperado**, no una
pérdida: revisa este margen antes de buscar el fallo en otro sitio.

⁴ **`PLATFORM_API_BASE_URL` fuera de desarrollo (Trabajo 1, code review 056 · T11).** Si al
arrancar `wapp-ctl` la URL sigue en el default de fábrica (`http://localhost:8103`), el log deja
constancia en `warn` — un log mudo es justo lo que hizo que este defecto pasara desapercibido.
Además, si la URL configurada es `http://` contra un host que **no** es loopback (mandaría la
contraseña del operador en claro por la red), el signup se **rehúsa** con el código
`platform_url_insecure` en vez de romper el arranque completo de `wapp-ctl`: esta URL solo la usa
el signup (C-03/T3.5), y `wapp-ctl` también controla el arranque/parada del núcleo 24/7 y sirve la
UI de operador — abortar el proceso entero por un problema acotado al alta de usuarios dejaría sin
control (y, en instalaciones con autoarranque por systemd, sin el propio núcleo `agent serve`,
que `wapp-ctl --autostart` lanza) a un Edge cuya única falla real está en un botón. `https://` y
loopback siempre se permiten.

### Variables del worker-cajero

Prefijo **`WAPP_WORKER_`**, **no** `WAPP_AGENT_`, y la distinción es real: el cajero
(`agent cajero`) es un **proceso aparte** con su propio bloque de entorno en la unidad que lo
lanza. Una variable del worker escrita con el prefijo del daemon **no la lee nadie** y el número
que el operador cree haber fijado no gobierna nada. Precedencia: defaults < bloque `worker:` del
mismo YAML < entorno.

| Variable | Default | Propósito |
|---|---|---|
| `WAPP_WORKER_DATA_DIRS` | (el `data_dir` del daemon) | **Lista separada por comas** de los `data_dir` que el cajero atiende en **round-robin estricto** (Plan 051 O4 · T4.1): una entrada por **instalación** en la máquina, y una cola (`<data_dir>/cola_entrantes.db`) por entrada. Se **trimea** cada entrada, se descartan las vacías y se **absolutiza** cada una; **los duplicados son error de arranque** (dos colas sobre el mismo fichero SQLite serían un round-robin que se turna consigo mismo). Vacía o sin poner ⇒ la lista de un elemento de siempre, sin ninguna diferencia observable. **No confundir con `WAPP_AGENT_DATA_DIR`**, que es el data_dir del daemon y **no** es una lista. ⚠️ El **contrato de intenciones** se toma del **primer** `data_dir` de la lista y se comparte: ver ⁵. Clave YAML: `worker.data_dirs` (lista de verdad, no comas). |
| `WAPP_WORKER_MAX_CONCURRENT` | `1` | Inferencias **simultáneas**. **Es por MÁQUINA, no por cola**: con N colas sigue habiendo un solo semáforo, porque protege al Ollama local, que es uno. El `1` está cerrado por la medición de la O0 (ADR-0038 Enmienda 1). |
| `WAPP_WORKER_POLL_MS` | `500` | Espera cuando **todas** las colas están vacías. Con N colas se duerme una vez por **vuelta completa** en blanco, no una vez por cola. |
| `WAPP_WORKER_INFERENCE_TIMEOUT_MS` | `15000` | Plazo de **una** inferencia. Es el único plazo del camino de la intención que queda en el Edge: su antiguo gemelo `WAPP_AGENT_INTENT_WAIT_MS` (el presupuesto del despachador) se retiró con el push en T1.6-5. |
| `WAPP_WORKER_MAX_INTENTOS` | `3` | Reclamos que aguanta un lote antes de abandonarlo con `fallo_repetido`. Es el freno del **lote venenoso**: sin él, un texto que siempre falla conserva el `seq` más bajo y congela la cola entera. |
| `WAPP_WORKER_MAX_RUNES` | `1000` | Techo de runas de la entrada a clasificar (guardarraíl de DoS del clasificador). |
| `WAPP_WORKER_NUM_THREAD` · `NUM_PREDICT` · `NUM_CTX` | `5` · `256` · `4096` | Opciones que se le pasan a Ollama por inferencia. Calibradas en la O0; no se tocan sin medición. |
| `WAPP_WORKER_STATS_EVERY_MS` | `300000` | Cadencia del bloque de contadores en el log del cajero. **`0` lo desactiva** (guardarraíl asimétrico: solo lo negativo cae al default). |

Todos caen al default con un valor `<=0`, salvo `STATS_EVERY_MS` (ver arriba).

⁵ **El contrato de intenciones con varios `data_dir` (límite conocido de T4.1).** El clasificador
del proceso es **uno**: un solo prompt y un solo schema, sustituibles en caliente con `Reload`.
Servir N contratos distintos exigiría N clasificadores, así que el cajero toma el contrato del
**primer** `data_dir` de `WAPP_WORKER_DATA_DIRS` y clasifica con él todas las colas. En
consecuencia, la lista está pensada para **N instalaciones del mismo tenant/contrato** en una
máquina; si dos tuvieran contratos distintos, la segunda se clasificaría con el de la primera.
Es una decisión **revisable**, escrita también en el código (`cmd/agent/cajero.go`).

## Store y custodia (zero-knowledge)

- **BD única SQLite cifrada, dialecto-conmutable** (ADR-0018): `accounts`↔`devices`, DEK
  por dispositivo, migración de sesiones activas sin re-escanear QR, failover
  multi-dispositivo por número.
- **DEK por dispositivo** custodiada en el keystore del SO: **Keychain** (macOS) /
  **DPAPI** (Windows) / **Secret Service** (Linux). La nube nunca la ve.
- **Doble llave** (ADR-0007): `CanOperate = hasDEK ∧ leaseVigente`. El **lease** lo emite y
  revoca el servidor (kill-switch anti-clon); un clon del `.db` sin lease es inútil.
- **Ruta sagrada** del store en el home del usuario (nunca rutas de sistema con root);
  sobrevive reinicio sin re-emparejar.

## Telemetría y diagnóstico (ADR-0023)

- **Heartbeat con `SessionHealth`**: snapshot operativo por sesión (estado del socket,
  motivo de degradación, edad del último entrante, latencia de carga de DEK, profundidad
  del outbox, `binary_version`, uptime). Solo metadatos; frontera zero-knowledge dura.
- **Watchdogs** que marcan sesiones `DEGRADED`/`DEAD` con prueba de vida.
- **Bundle de diagnóstico bajo demanda** (`DiagnosticsRequest`→`DiagnosticsBundle`): log
  tail saneado + dump de goroutines + JSON de subsistemas, truncado en origen bajo el
  límite de 4 MiB del transporte, con un gate que aborta si detecta material secreto.

## Clasificador de intenciones (opcional)

Integración **opcional** con [`edge/wapp-edge-intent`](../wapp-edge-intent) (clasificador
LLM local sobre **Ollama**, modelo `qwen3:1.7b` por defecto). OFF salvo
`WAPP_AGENT_INTENT_ENABLED=true`; con él, el Edge envuelve el sink de entrada, clasifica
localmente y aplica el catálogo de intenciones que la nube empuja por `ConfigUpdate`. Corre
en el mismo equipo (zero-knowledge, sin dependencia de red externa para clasificar).

## Referencias

- Especificación: `../../docs/piezas/01-edge-agent.md` · CloudLink: `../../docs/piezas/02-cloudlink.md`
- ADRs relevantes (en `../../docs/adr/`): **0001** (núcleo en el Edge), **0002** (SQLite
  pure-Go), **0003** (sin broker; outbox), **0005** (despachador), **0007** (doble llave),
  **0008** (multi-teléfono), **0013/0014/0015** (plataformas, núcleo+plano de control,
  contrato `/v1`), **0016** (custodia DEK y store por sesión), **0018** (BD única) y
  **0023** (telemetría de flota + diagnóstico remoto).
- CLAUDE.md raíz del ecosistema: `../../CLAUDE.md`
