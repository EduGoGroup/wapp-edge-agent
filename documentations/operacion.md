# Operación de `wapp-edge-agent`

Cómo se arranca, cómo se prueba de verdad, cómo se publica y cómo se depura.

---

## 1 · 🔴 El gate es LOCAL: un PR aquí no valida nada

`.github/workflows/ci.yml` se dispara **solo con `workflow_dispatch`**. Está escrito en la cabecera
del propio fichero: «la validación continua vive en la Mac de desarrollo». El único workflow que se
dispara solo es `sync-main-to-dev.yml`, que **no valida nada**: realinea `dev` con `main` tras un
push.

**Consecuencia práctica:** abrir un PR, verlo verde y mergear es abrir un PR y mergear. El gate real
es `make ci-local` en tu máquina, antes de empujar.

### 1.1 · Y un `rc=0` NO significa que todo pasó: hay que contar los SKIP

`go test` devuelve `0` con un `--- SKIP` exactamente igual que con un `--- PASS`. En este repo se
saltan solos:

| Qué se salta | Cuándo |
|---|---|
| `internal/infra/db/db_postgres_test.go` | Siempre, salvo que compiles con `-tags postgres` **y** definas `WAPP_AGENT_TEST_PG_DSN` |
| `internal/adapters/keycustody/keychain_darwin_test.go` | Si el Keychain real de macOS no da acceso (CI headless, sesión sin GUI): `t.Skipf` en `:31` y `:70` |
| `internal/adapters/keycustody/secretservice_linux_test.go` | No compila fuera de linux (`//go:build linux`); necesita D-Bus real |
| `internal/auth/manager_test.go:119` | Solo ante una colisión improbable de tokens |

**Cómo contarlos de verdad**, sin fiarte del `rc`:

```bash
GOWORK=off go test -race ./... 2>&1 | tee /tmp/edge-test.log
grep -c -- '--- SKIP' /tmp/edge-test.log
grep -c -- '--- FAIL' /tmp/edge-test.log
grep -c '^ok  ' /tmp/edge-test.log
```

Y **lee el `rc` sin pipe**: `cmd; echo $?`. Un `cmd | tee` te da el `rc` de `tee`.

### 1.2 · `make test` no lleva `GOWORK=off` — y eso importa

`lint` (`Makefile:108`) y `build-check` (`Makefile:116`) llevan `GOWORK=off`; **`test`
(`Makefile:102-103`) no**. Dentro del árbol del ecosistema hay un `go.work` que une este módulo con
`wapp-shared`, `wapp-cloudlink` y `wapp-edge-intent`, así que `make test` compila contra **el código
de al lado**, no contra los tags que fija `go.mod`. Un verde ahí no demuestra que el repo esté verde
solo. Para saberlo: `GOWORK=off go test -race ./...`.

---

## 2 · Los gates reales (`Makefile`)

| Target | Qué hace exactamente |
|---|---|
| `make fmt-check` | `gofmt -l .` debe salir **vacío** |
| `make vet` | `go vet ./...` |
| `make test` | `go test -race ./...` — 949 tests en 44 paquetes; el `-race` no es opcional (hay 18 goroutines de trabajo en producción) |
| `make lint` | `GOWORK=off golangci-lint run --timeout=5m`, versión fijada **v2.12.2**. ⚠️ **Este repo no tiene `.golangci.yml`**: corre con los linters por defecto |
| `make build-check` | `GOWORK=off go build ./...` (portable, sin cross-compile) |
| **`make ci-local`** | Los cinco anteriores. **Este es el gate.** |
| `make ci-docker` | `ci-local` dentro de `golang:1.26.5-bookworm`, instalando el mismo golangci-lint. Requiere Docker |

⚠️ `ci-local` y `ci-docker` **no dicen siempre lo mismo** sobre el mismo commit (toolchain, caché de
módulos, plataforma). Si uno está en verde y el otro en rojo, gana el rojo.

### Compilación y empaque

| Target | Salida |
|---|---|
| `make build` (default) | `dist/darwin-arm64/{agent,wapp-ctl}` con `CGO_ENABLED=1` (lo exige el Keychain) |
| `make build-windows-amd64` · `-arm64` · `build-linux-amd64` · `-arm64` | Los mismos dos binarios, pure-Go (`CGO_ENABLED=0`) |
| `make build-all` | Los cinco targets |
| `make pkg` / `make dmg` | Instalador `.pkg` / `.dmg` **sin firmar**, por usuario, solo en macOS |
| `make dist-windows-amd64` / `dist-linux-amd64` / `dist-all` | Bundles por plataforma |
| `make pkg-verify-zk` | 🔒 Corre `packaging/macos/verify-zero-knowledge.sh` sobre el *staging* y **aborta si se cuela material secreto**. `make pkg` ya lo invoca |
| `make version` | Imprime la versión que se inyectaría (`git describe --tags --always --dirty`) |

**`colaseed` no se empaqueta nunca**: `CMDS := agent wapp-ctl` (`Makefile:35`). Si necesitas la
herramienta de carga, se compila a mano con `go build ./cmd/colaseed`.

---

## 3 · Arrancar en local

### 3.1 · Primera vez

```bash
export WAPP_AGENT_CONFIG=./edge-local.yaml      # o deja que use el data_dir por defecto del SO
go build -o ./agent ./cmd/agent
go build -o ./wapp-ctl ./cmd/wapp-ctl

./agent enroll <código-de-activación>           # canjea el código y persiste el material mTLS
./wapp-ctl                                      # sirve 127.0.0.1:8105 y abre el navegador
```

`wapp-ctl` **no arranca los hijos por su cuenta** salvo con `-autostart`: en operación normal el
núcleo se arranca desde la web con `POST /v1/daemon/start`. El emparejamiento por QR sale por la web
local o por terminal ASCII.

Un `config.yaml` mínimo (sin secretos) tiene esta forma:

```yaml
log_level: info
data_dir: ./edgedata-local
max_sessions: 5
control_socket_path: /tmp/wapp-local.sock
cloudlink:
  endpoint: 127.0.0.1:8101
  enrollment_endpoint: 127.0.0.1:8102
  server_name: localhost
  tls_ca: ./edgedata-local/ca.crt
  tls_cert: ./edgedata-local/edge.crt
  tls_key: ./edgedata-local/edge.key
  lease_pubkey_path: ./edgedata-local/lease.pub
```

🔴 **Las cuatro rutas de `cloudlink` no tienen valor por defecto, y su ausencia no falla: degrada.**
Sin `tls_cert`/`tls_key`/`tls_ca` el Edge marca la nube **en claro** con un solo `Warn`
(`internal/infra/wiring/cloudlink.go:280-281`). Sin `lease_pubkey_path` el gate del kill-switch **no
se construye** (`:291-293`). Si al arrancar ves esos dos `Warn`, no estás probando el sistema real.

### 3.2 · Sin `-cajero-enabled=false`, hay tres procesos

El default de `-cajero-enabled` es **`true`**, así que `wapp-ctl` supervisa dos hijos. Comprueba que
están los tres antes de concluir nada de una prueba de inferencia:

```bash
ps -o pid,ppid,args -ax | grep -E 'wapp-ctl|agent (serve|cajero)' | grep -v grep
curl --unix-socket /tmp/wapp-local.sock http://unix/v1/health
curl -s http://127.0.0.1:8105/v1/daemon/status
curl -s http://127.0.0.1:8105/v1/cajero/status
```

### 3.3 · Arrancar sin WhatsApp: el transporte falso (ADR-0049)

Para probar el Edge entero —cola cifrada, despachador, lease, CloudLink— sin una sesión de WhatsApp
viva. Es un **modo de prueba**: con él echado este Edge **no entrega nada a nadie**.

**Echarla.** Dos variables del daemon (prefijo `WAPP_AGENT_`), las dos, y reiniciar:

```bash
WAPP_AGENT_WHATSAPP_FALSO=true          # el transporte: empareja sin QR, captura lo que «envía»
WAPP_AGENT_INYECTOR_ENTRANTES=true      # la única puerta de entrada del falso (ADR-0041)
```

Con solo la primera, el Edge arranca y «envía», pero no hay por dónde hacer entrar un mensaje
(`/v1/diag/inbound/inject` no existe). El hijo `agent serve` hereda el entorno de `wapp-ctl`.

**Comprobar que es lo que corre**, antes de concluir nada:

```bash
grep -c 'PALANCA ECHADA (WAPP_AGENT_WHATSAPP_FALSO=true)' <log del daemon>     # ≥ 1 tras este arranque
grep 'CloudLink habilitado' <log del daemon> | tail -1    # lease_gate=true sealed_transit=true lease_shadow_mode=false
curl -s --unix-socket <socket> http://unix/v1/health      # status ok, y la versión que esperas
```

Sin `lease_gate=true` el Edge envía con el lease revocado: la prueba no mide el kill-switch.

**Conducirlo** (todo por el socket unix del plano de control, con el Bearer de un operador:
`POST /v1/auth/login`; hasta que llega el JWKS de la nube las escrituras dan `403 degraded_read_only`,
y se reintenta):

| Paso | Llamada | Qué vuelve |
|---|---|---|
| Emparejar | `POST /v1/sessions/pair` y luego `GET /v1/sessions/{id}/pair` | `status: success` sin QR, un `session_id` y un JID que empieza por `0000` (ningún teléfono empieza así) |
| Activar | En la **nube**: `POST /api/v1/sessions/{session_id}/profile` con `{"profile":"active"}` | La sesión nace **pasiva** (INV-E5): hasta activarla, lo inyectado se descarta en la puerta |
| Entra un mensaje | `POST /v1/diag/inbound/inject` con `session_id`, `texto`, `chat_jid` y un `lote` **distinto cada vez** | `inyectados: 1, acusados: 1`. Un `lote` repetido se descarta en silencio (mismo `wa_message_id`). `409` = la escucha aún no está publicada: reintentar |
| Leer lo «enviado» | `GET /v1/diag/outbound/captured?session_id=…` | `capturados` en orden, cada uno con `command_id`, `to`, `text` y un `message_id` `SINTETICO-…`; `descartados` > 0 = el anillo dio la vuelta |

🔴 **Lo capturado vive en memoria**: un reinicio lo borra. Guárdalo antes de reiniciar.

🔴 **Con la palanca echada, TODA sesión `active` del `data_dir` escucha por el transporte falso**,
también una emparejada con WhatsApp real: la escucha falsa no mira el JID
(`internal/infra/wiring/transporte.go`, `whatsappfalso.NuevaEscucha`). Esa sesión sale `listening` en
`/v1/sessions` y late hacia la nube, pero **no está conectada a WhatsApp**: lo que le escriban no
entra y lo que la nube le mande se captura en vez de salir. Su almacén cifrado no se toca —el falso no
lo abre—, así que al bajar la palanca vuelve a su transporte. (Leído en el código; no ejecutado con una
sesión real.) Mira `GET /v1/sessions` **antes** de echar la palanca.

**Bajarla.** En este orden:

1. `DELETE /v1/sessions/{session_id}` de **cada** sesión falsa (las del JID `0000…`).
2. Quitar las dos variables y reiniciar.
3. Comprobar: cero líneas `PALANCA ECHADA` tras ese arranque, las dos rutas de `/v1/diag` dan `404`, y
   la línea del latido (`listener: latencia del handler de entrantes`) dice `inyector=no whatsapp=real`.

Si se baja **sin** desvincular (medido el 2026-10-11): la sesión falsa queda `active` / `degraded` y el
transporte real la reintenta con *backoff* para siempre —`listen: whatsapp: no hay device pareado para
el JID de la sesión`—. No rompe nada más, y el `DELETE` funciona igual con la palanca ya baja.

---

## 4 · Publicar una versión

El flujo del ecosistema, aplicado a este repo: **el trabajo aterriza en `dev`; a `main` se pasa al
final del plan.** Este repo **no tiene `release.yml`**: el tag se corta a mano. Hoy tiene **un solo
tag publicado, `v0.1.0`**.

1. `make ci-local` en verde, **contando los SKIP**.
2. `GOWORK=off go build ./...` — si el repo depende de un módulo de `wapp-shared` recién tocado,
   compila contra el **tag publicado**, no contra el árbol de al lado.
3. Merge de `dev` a `main`. El workflow `sync-main-to-dev.yml` realinea `dev` después.
4. Tag `vX.Y.Z` sobre `main`. La versión **no es un literal**: `make build` la inyecta con
   `-ldflags -X main.Version=$(git describe --tags --always --dirty)`. Un binario compilado con el
   árbol sucio lleva el sufijo `-dirty`, y eso es una señal, no un ruido.
5. Actualiza `CHANGELOG.md`. ⚠️ Hoy va **una versión por detrás** de `go.mod` en la línea de
   `wapp-cloudlink`.

**Antes de dar por buena una versión desplegada**, comprueba el **proceso vivo**, no el fichero
instalado: `readlink /proc/$(systemctl show -p MainPID --value <unidad>)/exe` y compara su `md5sum`
con el del binario en disco. Instalar y reiniciar son dos pasos, y el segundo se olvida.

---

## 5 · Depurar cuando falla

### 5.1 · Qué queda registrado en ejecución, y con qué regla

- **El log del daemon** va a `stdout` y, si `WAPP_LOG_FILE` está puesta, también a ese archivo
  (`internal/infra/logger/logger.go:65`). En un despliegue con systemd que redirige con
  `StandardOutput=append:`, **el log no pasa por `journald`**: `journalctl` no lo tiene.
- **Un ring buffer en memoria** alimenta `GET /v1/logs`, que es **SSE**. Es el **log global del
  daemon**, no filtrado por sesión — la propia pantalla lo avisa.
- **El prompt y la salida de una inferencia NO se registran nunca** (INV-E7): solo `command_id`,
  tamaños y desenlace.
- **Los descartes de la puerta de entrada se cuentan, no se loguean uno a uno**: `DroppedByWindow`,
  `AdmittedNoTimestamp`, `Brackets`, `DroppedByPassiveProfile`, `DroppedByGroup`. Salen agregados
  cada `WAPP_AGENT_INBOUND_STATS_EVERY_MS` (60 s por defecto) y viajan en el heartbeat.
- **Las variables de entorno retiradas se avisan al arrancar**, no se ignoran en silencio.

### 5.2 · Síntomas y dónde mirar primero

| Síntoma | Primera hipótesis |
|---|---|
| «Arrancó bien y a los minutos dejó de publicar salud» | Una columna que falta: alguien editó un `CREATE TABLE` en vez de escribir un `ensure…`. Ver INV-E1 de la constitución. El error es `no such column: …` en la primera consulta, no en el arranque. |
| «El segundo arranque muere y el primero fue bien» | Un `ALTER TABLE` pelado en un `.sql`: el runner re-ejecuta todo el DDL en cada arranque. |
| «Configuré la variable del cajero y no hace nada» | Prefijo equivocado: el cajero lee `WAPP_WORKER_*`, no `WAPP_AGENT_*`. No hay aviso. |
| «Envía aunque revoqué el lease» | O el modo sombra está encendido, o `lease_pubkey_path` está vacía y el gate no se construyó. Los dos casos dejan un `Warn` en el arranque. |
| «La consola dice 200 pero el WhatsApp no salió» | El envío real lo hace la nube en su propio paso; un 200 del plano de control no acredita la entrega. |
| «No entra nada» | Mira primero si el despachador está apagado (`WAPP_AGENT_DESPACHADOR_APAGADO`), luego el perfil de la sesión (una **pasiva** descarta los entrantes en el Edge, y es el default de una sesión recién emparejada). |
| «El circuito abrió sin un solo fallo» | Es correcto: cinco aciertos **lentos** (≥80 % del plazo) abren el circuito. Ver INV-E8. |
| «La inferencia da timeout y `ollama ps` dice que el modelo está cargado» | Modelo cargado ≠ prefijo precalentado: son dos cachés distintas. |
| Un `curl` a `/v1/...` da 404 desde el navegador | `/v1/auth/*` está **bloqueado en el proxy** a propósito; y las dos rutas de `/v1/diag` **no existen** sin su palanca (§3.3). |
| «`listener caído` … `no hay device pareado para el JID`» con un JID `0000…`, en bucle | Quedó una sesión del WhatsApp falso y la palanca ya está baja: `DELETE /v1/sessions/{id}` (§3.3). |
| «La sesión dice `listening` y no llega ni sale nada de verdad» | Mira si el arranque gritó `PALANCA ECHADA (WAPP_AGENT_WHATSAPP_FALSO=true)`: con ella este Edge no habla con WhatsApp (§3.3). |

### 5.3 · El bundle de diagnóstico

La nube puede pedir un `DiagnosticsRequest` y el Edge responde con un `DiagnosticsBundle`
(`internal/app/diagnostics/builder.go`), que incluye las últimas `WAPP_AGENT_DIAG_LOG_LINES` líneas
(500 por defecto) y los contadores de salud. Es el camino soportado para llevarse estado de una
máquina de cliente: **no** copies `edge.db` ni `cola_entrantes.db`, que llevan material cifrado bajo
una DEK que no sale de ahí.

### 5.4 · Higiene del árbol de trabajo

En un checkout usado para pruebas conviven binarios compilados, `agent.log`, sockets vivos y
directorios `edgedata-*/` con **sesiones reales de WhatsApp** (store cifrado y claves mTLS). Todo eso
está cubierto por `.gitignore` y `git status` sale limpio — pero está **a un `git add -f` de
distancia**. Nunca fuerces un add en este repo sin mirar qué añades.
