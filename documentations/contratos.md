# Contratos de `wapp-edge-agent`

Todo lo que otros consumen de esta pieza, o le pasan. Cada lista dice **de dónde salió** y **con qué
regla se contó**.

---

## 1 · Las rutas HTTP, y por qué «el número de rutas» no existe

Hay **dos superficies HTTP distintas**, y una de ellas tiene rutas condicionadas por un flag. Por eso
cualquier cifra suelta es falsa. Tres reglas y tres números, todos correctos:

| Regla de conteo | Número |
|---|---|
| **A** · Pares *método + patrón* registrados en el mux del **núcleo** (socket unix), contando la del inyector, que solo existe con su palanca echada. | **14** (13 sin la palanca) |
| **B** · Llamadas a `mux.HandleFunc`/`mux.Handle` en `newRouterConCajero` de **`wapp-ctl`**, con `-cajero-enabled=true`, que es el default. De esas, **13 son rutas propias** y 2 son comodines (`/v1/` del proxy y `/` de la web). | **15** (12 con `-cajero-enabled=false`) |
| **C** · Lo que un navegador en `:8105` puede alcanzar: las 13 propias de `wapp-ctl` más las del núcleo que el proxy deja pasar (13 por defecto **menos** las 3 de `/v1/auth/*`, que el proxy bloquea). | **23** |

Ninguno de los tres es «el» número. Di siempre la regla.

### 1.1 · Núcleo — HTTP `/v1` sobre socket unix, **sin puerto de red**

Fuente: los `s.Handle(...)` de `internal/adapters/control/server/{server,auth,pair,unlink,enroll,readiness}.go`
más los `srv.HandleAuthorized(...)` de `internal/infra/daemon/daemon.go:294-320` y
`internal/adapters/control/diag/inyector.go:70`.
Socket: `wapp-edge.sock` por defecto (`internal/infra/config/config.go`, campo `ControlSocketPath`).

| Método | Ruta | Gate RBAC | Registro |
|---|---|---|---|
| GET | `/v1/health` | **exento** (sonda de liveness del supervisor) | `server.go:99` |
| GET | `/v1/sessions` | `edge.status.read` | `server.go:101` |
| POST | `/v1/sessions/pair` | `edge.sessions.pair` (escritura) | `pair.go:80` |
| GET | `/v1/sessions/{id}/pair` | `edge.status.read` | `pair.go:81` |
| DELETE | `/v1/sessions/{id}` | `edge.sessions.logout` (escritura) | `unlink.go:38` |
| POST | `/v1/auth/login` | **exento** | `auth.go:53` |
| POST | `/v1/auth/refresh` | **exento** | `auth.go:54` |
| POST | `/v1/auth/logout` | **exento** | `auth.go:55` |
| GET | `/v1/enroll/status` | **sin gate** (bootstrap) | `enroll.go:47` |
| POST | `/v1/enroll` | **sin gate** (bootstrap de primera ejecución; la puerta la cierra el código de activación de un solo uso) | `enroll.go:48` |
| GET | `/v1/logs` | `edge.status.read` — **SSE `text/event-stream`** | `daemon.go:294` |
| GET | `/v1/intent/status` | `edge.status.read` | `daemon.go:299` |
| POST | `/v1/inference/readiness` | **sin gate, a propósito**: el emisor es `agent cajero`, otro proceso sin Bearer de operador. La justificación completa y **qué NO se acepta por esa puerta** están en la cabecera de `internal/adapters/control/server/readiness.go` | `readiness.go:162` |
| POST | `/v1/diag/inbound/inject` | `edge.diag.inbound.inject` (escritura) — 🔴 **la ruta NO EXISTE si la palanca está bajada** | `diag/inyector.go:70`, colgada en `daemon.go:508` |

**Los cuatro recursos RBAC**, con su nombre literal: `edge.status.read`, `edge.sessions.pair`,
`edge.sessions.logout` (`internal/adapters/control/server/server.go:63-69`) y
`edge.diag.inbound.inject` (`internal/adapters/control/diag/inyector.go:77`).

**Cuerpo de `GET /v1/intent/status`**: `{enabled, model, config_version, clasifica_en, worker_status_url}`.
Ya **no** reporta `circuit` ni `ollama_ok`: los dos vivían en el decorador que se retiró, y sostenerlos
aquí sería informar de un estado que este proceso ya no tiene.

**Cuerpo de `POST /v1/inference/readiness`**: lleva el **estado al que se pasa**, no un incremento, así
que es idempotente por construcción y el cajero lo puede reintentar sin llevar cuenta.

### 1.2 · `wapp-ctl` — HTTP en `127.0.0.1:8105`

Fuente única: `cmd/wapp-ctl/main.go:235-353` (`newRouterConCajero`).

| Patrón | Protección | Línea |
|---|---|---|
| `/v1/daemon/start` | CSRF **solo si hay sesión** | `248` |
| `/v1/daemon/stop` | CSRF **solo si hay sesión** | `260` |
| `/v1/daemon/status` | ninguna | `272` |
| `/v1/cajero/start` | ídem que daemon — **solo si `-cajero-enabled`** | `292` |
| `/v1/cajero/stop` | ídem | `304` |
| `/v1/cajero/status` | ninguna — **solo si `-cajero-enabled`** | `316` |
| `GET /v1/ui/aviso-sesion-pasiva` | pública: no muta nada y el texto no lleva PII | `330` |
| `/v1/` (todo lo demás) | **reverse-proxy al socket del núcleo**: inyecta el Bearer de la sesión, exige CSRF en mutadoras con sesión, reintenta una vez tras un 401 con refresh *single-flight*. `/v1/health` y `/v1/enroll*` **exentos**; `/v1/auth/*` **devuelve 404 al navegador** | `339`, `proxy.go:73-75,96-99` |
| `POST /login` · `GET /login` · `POST /signup` · `GET /signup` · `POST /logout` · `GET /session` | borde de sesión propio; `GET /session` **nunca** devuelve el access token | `342-347` |
| `/` | `rootGate`: sin sesión **con tenant**, redirige a `/login`; los assets estáticos se sirven libres | `352` |

**Cookies**: `wapp_edge_session` (HttpOnly, id opaco; el access vive server-side, en memoria) y
`wapp_edge_csrf` (legible, *double-submit*). Las dos con `SameSite=Strict` y **`Secure=false` a
propósito**, porque el plano de control es HTTP en loopback (`cmd/wapp-ctl/session.go:162-179`).
El store de sesiones es **en memoria**: reiniciar `wapp-ctl` cierra la sesión de todos.

Si el núcleo no responde por el socket, el proxy traduce el fallo a **503 con envelope
`daemon_down`**, nunca a un 502 crudo.

### 1.3 · Socket de inferencia del cajero

`POST /inferencia` sobre `<data_dir>/cajero.sock`. **Endpoint único**, sin auth y **sin sonda de
salud**. Tope de cuerpo **8 MiB** (`internal/adapters/cajerosock/servidor.go:64`, `:135`). Hay **uno
por instalación**, y vive a nivel de `<data_dir>` porque es del Edge, no de una sesión.

Que este socket no tenga `/health` es la razón de que el `healthy` de `/v1/cajero/status` signifique
solo «el proceso no se murió en los primeros 2 s». Quien quiera saber si el Edge **puede servir**
inferencia no debe preguntar ahí: eso viaja en el `inference_readiness` del heartbeat.

---

## 2 · gRPC — el contrato con la nube

Del módulo `github.com/EduGoGroup/wapp-cloudlink v0.17.0`. Dos servicios, en **dos listeners
distintos** del lado nube.

- **Enrolamiento (saliente, unario)**: `Enrollment.EnrollEdge(EnrollEdgeRequest{activation_code, csr_pem})`
  (`internal/infra/enroll/enroll.go:120`). Devuelve `edge_cert_pem`, `ca_chain_pem`,
  `cloud_enc_pubkey`, `lease_pubkey` y `tenant_id`. **No devuelve el endpoint de runtime**: se
  **deriva** del host más `runtime_port` (`enroll.go:167-173`). Ese listener es TLS solo-servidor a
  propósito: el Edge enrola ahí **antes de tener certificado**.
- **Runtime (bidi, mTLS)**: un único stream `Connect(stream EdgeToCloud) returns (stream CloudToEdge)`
  que **abre el Edge**. Heartbeat cada **30 s** (`internal/adapters/cloudlink/adapter.go:387`).

| El Edge **recibe** | El Edge **envía** |
|---|---|
| `InferenceRequest` (carril propio, esquiva el despachador), `ConfigUpdate`, `DiagnosticsRequest`, `UserAuthResponse`, `SendText`, `SendMedia`, `LeaseUpdate`, `Ping` | `Ack`, `Heartbeat`, `Incoming`, `Receipt`, `Pong`, `DiagnosticsBundle`, `InferenceResult`, `UserLogin`, `UserRefresh`, `UserLogout` |

Fuente: `grep -n 'c2e.Get' internal/adapters/cloudlink/adapter.go:759-816` y
`grep -o 'EdgeToCloud_[A-Za-z]*' internal/adapters/cloudlink/`.

🔴 **Lo que NO viaja por aquí**: la DEK, ni su material, ni el store. El único campo del contrato con
«dek» en el nombre es una **métrica de tiempo** (`dek_load_duration_ms`).

**El único HTTP directo a la nube** (fuera del stream): `POST {platform_api_base_url}/api/v1/signup`
desde `wapp-ctl` (`cmd/wapp-ctl/auth.go:244-248`). El resto de la autenticación del operador viaja
como **frames del bidi**, no como RPCs propios — quien busque un `rpc UserLogin` no lo encontrará.

---

## 3 · CLI

### `agent` — tres subcomandos y no hay más (`cmd/agent/main.go:1-16`)

| Subcomando | Qué hace |
|---|---|
| `agent enroll` | Enrola el Edge: exige `enrollment_endpoint`, una CA pre-provista y el código de activación. Genera el par mTLS y persiste el material. |
| `agent serve` | El daemon 24/7: abre las dos bases, restaura sesiones, levanta un listener por sesión, sirve `/v1` en el socket y mantiene el stream. |
| `agent cajero` | El worker de inferencia. **Es el único proceso del repo que instancia un cliente de Ollama.** |
| *(sin subcomando)* | Loguea el arranque y termina. |

### `wapp-ctl` — flags (`cmd/wapp-ctl/main.go:46-55`)

| Flag | Default | Variable equivalente |
|---|---|---|
| `-addr` | `127.0.0.1:8105` | `WAPP_CTL_ADDR` |
| `-agent-bin` | el `agent` hermano del ejecutable; si no, el del `PATH` | `WAPP_CTL_AGENT_BIN` |
| `-socket` | el `ControlSocketPath` del config | — |
| `-platform-api-base-url` | el `PlatformAPIBaseURL` del config | — |
| `-pid-file` | `<socket>.pid` (solo del **núcleo**; el lock del cajero cuelga siempre del socket) | — |
| `-cajero-enabled` | **`true`** | `WAPP_CTL_CAJERO_ENABLED` |
| `-no-open` | `false` (abre el navegador al arrancar) | — |
| `-autostart` | `false`; arranca los hijos al iniciar en vez de esperar a `POST /v1/daemon/start` | — |

### `colaseed` — flags (`cmd/colaseed/main.go:184-193`)

`-data-dir` y `-session-id` son **obligatorios**. Los demás: `-conversaciones` (1), `-mensajes` (1),
`-pausa` (0 = ráfaga), `-prefijo-texto`, `-prefijo-jid` (`colaseed`), `-lote` (8 hex aleatorios),
`-intercalar` (`true`), `-max-filas` (el `DefaultColaMaxRows` del daemon).

---

## 4 · Variables de entorno

🔴 **Nombre efectivo = prefijo + literal.** El loader compone el prefijo (`config.go:729` y `:799`),
así que el `MAX_SESSIONS` del código se pone en la máquina como `WAPP_AGENT_MAX_SESSIONS`. Hay
**dos prefijos**, y son de **dos procesos distintos**: `WAPP_AGENT_` (daemon, `config.go:22`) y
`WAPP_WORKER_` (cajero, `config.go:121`).

### Daemon — `WAPP_AGENT_*`

| Variable | Default |
|---|---|
| `WAPP_AGENT_CONFIG` | — (ruta del YAML) |
| `WAPP_AGENT_DATA_DIR` | `~/Library/Application Support/wApp/edge` · `~/.config/wApp/edge` · `%AppData%\wApp\edge` |
| `WAPP_AGENT_LOG_LEVEL` | `info` |
| `WAPP_AGENT_MAX_SESSIONS` | `5` |
| `WAPP_AGENT_MULTIDEVICE_PER_ACCOUNT` | `1` |
| `WAPP_AGENT_PUSH_NAME` | `wApp` |
| `WAPP_AGENT_DB_DIALECT` · `WAPP_AGENT_DB_DSN` | `sqlite` · — |
| `WAPP_AGENT_CONTROL_SOCKET_PATH` | `wapp-edge.sock` |
| `WAPP_AGENT_OUTBOX_MAX_EVENTS` · `_OUTBOX_TTL_HOURS` | `10000` · `0` (sin TTL) |
| `WAPP_AGENT_COLA_TTL_HOURS` · `_COLA_MAX_ROWS` · `_COLA_CLAIM_MAX_FILAS` · `_COLA_LEASE_SECONDS` | `24` · `50000` · `20` · `60` |
| `WAPP_AGENT_INBOUND_MARGIN_SECONDS` · `_INBOUND_STATS_EVERY_MS` | `300` · `60000` |
| `WAPP_AGENT_DIAG_LOG_LINES` | `500` |
| `WAPP_AGENT_PLATFORM_API_BASE_URL` | `http://localhost:8103` |
| `WAPP_AGENT_CLOUDLINK_ENROLLMENT_ENDPOINT` · `_ENDPOINT` · `_ACTIVATION_CODE` · `_SERVER_NAME` | — |
| `WAPP_AGENT_CLOUDLINK_RUNTIME_PORT` · `_COMMAND_TIMEOUT_SECONDS` | `8101` · `30` |
| `WAPP_AGENT_INFERENCE_MAX_INFLIGHT` · `_MAX_TIMEOUT_MS` · `_LEASE_GRACIA_MS` | `4` · `120000` · `2000` |
| `WAPP_AGENT_INTENT_ENABLED` · `_INTENT_OLLAMA_URL` · `_INTENT_MODEL` | `false` · `http://127.0.0.1:11434` · `qwen3:1.7b` |
| `WAPP_AGENT_AUTH_ISSUER` | el emisor compilado (`internal/infra/wiring/auth.go:131`) |

**Las tres palancas de riesgo**, todas `false` por defecto y ninguna documentada en el `README.md`:

| Palanca | Qué hace si la enciendes |
|---|---|
| `WAPP_AGENT_CLOUDLINK_LEASE_SHADOW_MODE` | El kill-switch queda **inerte**: un lease no vigente solo produce un `Warn` y el envío sale. El propio código lo grita al arrancar por si se hereda de un `.env` viejo. |
| `WAPP_AGENT_DESPACHADOR_APAGADO` | **Apaga la entrega de entrantes**. Palanca de diagnóstico. |
| `WAPP_AGENT_INYECTOR_ENTRANTES` | Cuelga `POST /v1/diag/inbound/inject`. Con ella bajada la ruta no existe. |

### Worker-cajero — `WAPP_WORKER_*`

`MAX_CONCURRENT` (`1`), `POLL_MS` (`500`), `INFERENCE_TIMEOUT_MS` (**`45000`**), `MAX_INTENTOS` (`3`),
`STATS_EVERY_MS` (`300000`), `MAX_RUNES` · `NUM_THREAD` · `NUM_PREDICT` · `NUM_CTX` (heredados de
`wapp-edge-intent`), `KEEP_ALIVE_SECONDS`, `PREFILL_FRIO_MS` · `PREFILL_CALIENTE_MS`, y
`WAPP_WORKER_DATA_DIRS` (**lista separada por comas**; default: el `data_dir` único).

⚠️ `WAPP_WORKER_KEEP_ALIVE_SECONDS` **no tiene guardarraíl `<=0 ⇒ default`**, y es deliberado: `-1`,
`0` y positivo significan tres cosas distintas.

⚠️ **Ese límite YA NO EXISTE** (ADR-0045, Plan 044 · Ola 1.6 · T1.6-2, 2026-08-24). Hasta
entonces el contrato de intenciones con N `data_dir` se tomaba del primero y se compartía; hoy el
cajero **no lee el contrato**: se lo dice `cmd/agent/cajero.go:112-119` («el contrato de intenciones
ya no lo lee este proceso: lo usa el CLOUD para armar el prompt»), y bajo *pull* el cajero no
clasifica por iniciativa propia. En producción `wapp-shared/intents` solo se importa en
`internal/infra/wiring/intent.go:10` y `internal/app/cola.go:9`, ninguno en el camino del cajero.
No hay fallo silencioso que vigilar aquí.

### Supervisor y sueltas

`WAPP_CTL_ADDR`, `WAPP_CTL_AGENT_BIN`, `WAPP_CTL_CAJERO_ENABLED`; `WAPP_LOG_FILE` (redirige el log a
fichero, lo usan el LaunchAgent y systemd); y `WAPP_ENABLE_ALPHA_LOGIN` + `WAPP_ALPHA_TEST_ACCOUNTS`
+ `WAPP_ALPHA_TEST_PASSWORD`, que activan el selector «Usuario de prueba (Alpha)» de la pantalla de
login y autocompletan su contraseña (`cmd/wapp-ctl/auth.go:230,240`). **No las enciendas en campo.**

### Retiradas, con aviso al arrancar

`config.VariablesRetiradas()` (emitida en `internal/infra/daemon/daemon.go:82-85`) avisa de
`WAPP_AGENT_INTENT_TIMEOUT_MS` (sustituta: `WAPP_WORKER_INFERENCE_TIMEOUT_MS`) y de
`WAPP_AGENT_INTENT_WAIT_MS` (**sin sustituta**). ⚠️ Planes y ADR antiguos llaman a la sustituta
`WAPP_WORKER_TIMEOUT_MS`; lo que `Load` lee de verdad es `WAPP_WORKER_INFERENCE_TIMEOUT_MS`, y está
anotado en `config.go:1121`.

---

## 5 · Ficheros que lee y escribe

Todo bajo `<data_dir>`, creado con `MkdirAll 0700`.

| Ruta | Qué es |
|---|---|
| `<data_dir>/edge.db` | Store cifrado + metadatos + `outbox` + `edge_config` |
| `<data_dir>/cola_entrantes.db` | La cola de entrantes, **base aparte** |
| `<data_dir>/keys/<session_id>.key` | La **DEK** de esa sesión, `0600`, solo en plataformas sin keystore (en macOS va al Keychain; en Windows a `<path>.dpapi`) |
| `<data_dir>/cajero.sock` | Socket de inferencia (uno por instalación) |
| `<data_dir>/cloudlink-endpoint` | Endpoint de runtime derivado por el `enroll` (material público) |
| `<data_dir>/auth/operator.refresh` | 🔴 El **refresh token del operador**, en archivo `0600` y no en el keystore. Deuda explícita. |
| `<data_dir>/sessions/<id>/store.db` | Layout multi-sesión heredado; hoy se archiva al arrancar |
| `<data_dir>/_archived-pre-008/`, `_archived-pre-022/` | Destino de esas migraciones *clean-slate* |
| `wapp-edge.sock` + `.pid` + `.cajero.pid` | El socket `/v1` y los dos PID-locks |
| rutas de `tls_cert` / `tls_key` / `cloud_enc_pubkey_path` / `lease_pubkey_path` | Las escribe `agent enroll`: clave `0600`, certificado `0644` |
| `$WAPP_LOG_FILE` | Redirección del log |

---

## 6 · Esquema SQLite — 13 tablas en dos bases

Fuente: `internal/infra/db/migrations/{store,meta,cola}/*.sql` más las cuatro funciones `ensure…` de
`internal/infra/db/db.go`. **No hay tabla de versión de esquema** (ver INV-E1 de la constitución).

| Base | Set | Fichero | Tablas |
|---|---|---|---|
| `edge.db` | `store` | `0001_init.sql` | `msg_enc_device`, `msg_enc_identities`, `msg_enc_sessions`, `msg_enc_prekeys`, `msg_enc_sender_keys` — **todos los BLOB son ciphertext AES-256-GCM bajo la DEK** |
| `edge.db` | `meta` | `0002_sessions.sql` … `0006_edge_config.sql` | `sessions` (legado), `sessions_v2`, `accounts`, `devices`, `outbox`, `edge_config` |
| `cola_entrantes.db` | `cola` | `0001_cola_entrantes.sql`, `0002_parte_worker.sql` | `cola_entrantes`, `parte_worker` (fila única, `CHECK (id = 1)`) |

**Dónde está la frontera del cifrado en la cola**: `session_id` y `chat_jid` van **en claro** —hacen
falta para enrutar y para elegir la DEK—; `texto_enc` y `meta_enc` van sellados con la DEK **de esa
sesión**. `parte_worker` no lleva nada de negocio, a propósito.

**Postgres** existe solo tras el build-tag `postgres` (`internal/infra/db/db_postgres.go`). Sin el
tag, `openPostgres` devuelve error. Y las cuatro `ensure…` son SQLite-only por su `PRAGMA table_info`.

---

## 7 · Contratos de texto

[`literal-aviso-sesion-pasiva.md`](literal-aviso-sesion-pasiva.md) es el **original** del literal
`AVISO_SESION_PASIVA_V1`. Lo sirven dos consumidores: `GET /v1/ui/aviso-sesion-pasiva` de `wapp-ctl`
y —en el otro lado del ecosistema— el mensaje de WhatsApp que la nube manda al propio número. Las
dos copias tienen que ser **idénticas carácter a carácter**, y en este repo lo comprueba
`internal/webui/aviso_test.go` en cada `make test`. Sus reglas de uso están en el propio fichero y
son parte del contrato.
