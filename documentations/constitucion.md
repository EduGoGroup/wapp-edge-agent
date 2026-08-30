# Constitución de `wapp-edge-agent`

Las reglas de esta pieza. Lo que está aquí manda sobre lo que un comentario suelto o un README
antiguo digan. Cada invariante lleva **cómo se comprueba** y si hay un test que lo vigila.

> Este repo se clona solo. Por eso los invariantes del ecosistema que le aplican están **repetidos
> aquí**, no enlazados: los ADR viven en el repo de documentación del ecosistema (`docs/adr/`), que
> **no viaja con este git**.

---

## 1 · Los invariantes del ecosistema que aplican a esta pieza

### INV-1 · Zero-knowledge: la nube nunca ve credenciales ni llaves privadas

La nube **sí** recibe el contenido de negocio a propósito (los mensajes suben para que la plataforma
decida). Lo que **nunca** sube es el material que permite suplantar al cliente: el store de
`whatsmeow`, la DEK que lo descifra, la clave privada mTLS.

**Cómo se comprueba.** El store cifrado son las cinco tablas `msg_enc_*` de este repo
(`internal/infra/db/migrations/store/0001_init.sql`) y no tienen esquema en la nube. La palabra
«DEK» aparece en el contrato gRPC solo como métrica de tiempo (`dek_load_duration_ms`), nunca como
material.

**Candado.** Del lado del empaquetado sí: `make pkg` invoca `packaging/macos/verify-zero-knowledge.sh`
sobre el *staging* y **aborta** si se cuela material secreto. Del lado del código no hay `depguard`
(este repo **no tiene `.golangci.yml`**, aunque el `Makefile` invoque `golangci-lint`).

### INV-2 · Doble llave: DEK del cliente + lease del servidor, 2-de-2

- **DEK**: descifra el store de `whatsmeow`. La custodia el cliente, hay **una por sesión**, y vive
  en el keystore del SO (Keychain / DPAPI / Secret Service) o, como suelo, en un archivo `0600`.
- **Lease**: autoriza a operar. Lo **emite y lo revoca el servidor** (kill-switch anti-clon), viaja
  firmado con Ed25519 y el Edge solo lo valida con la pública.

Para despachar hacen falta **las dos**: `internal/adapters/cloudlink/adapter.go:824` (`handleSendText`)
y `:850` (`handleSendMedia`) llaman `validator.CanOperate(hasDEK())`. La revocación es **pegajosa**:
un `LeaseUpdate` posterior no la levanta (`adapter.go:987-996`).

🔴 **Dos escapes que hay que conocer antes de tocar el wiring** (detalle en `deuda.md`):
1. **Modo sombra** (`WAPP_AGENT_CLOUDLINK_LEASE_SHADOW_MODE`): con él encendido, un lease no vigente
   produce un `Warn` y **el envío sale igual** (`adapter.go:825-828`, `:851-854`). Default `false`.
2. **Gate ausente por configuración**: si `lease_pubkey_path` está vacía o el archivo no existe,
   `internal/infra/wiring/cloudlink.go:291-293` avisa y **devuelve `nil`**; sin validator, las dos
   líneas de arriba no gatean nada. Y esa ruta **no tiene valor por defecto**.

**Candado.** Los tests del lease viven en el módulo `wapp-cloudlink`, no aquí. En este repo **no hay
ningún test que afirme «sin material de lease el Edge no debe despachar»**.

### INV-3 · Sin Redis ni broker en el Edge

La concurrencia se resuelve con **goroutines y canales**, y la durabilidad con una tabla `outbox` en
SQLite que se drena al reconectar (`internal/adapters/outbox/`).

**Cómo se comprueba.** `go list -m all | grep -iE 'redis|amqp|rabbit|nats|kafka'` no devuelve nada, y
`go.mod` no declara ninguna. **Sin candado**: bastaría un `depguard` en un `.golangci.yml` que no existe.

### INV-4 · Copia-adaptación de EduGo, nunca dependencia

Parte de este código nació copiando y adaptando `edugo-api-messaging` (el `cryptostore`, el sobre
X25519/NaCl, el adaptador de `whatsmeow`). **Está prohibido importar un repo `edugo-*`.**

**Cómo se comprueba.** `grep -rn 'EduGoGroup/edugo-' --include='*.go' .` debe salir vacío. Hoy sale
vacío. ⚠️ **Excepción legítima que confunde el grep**: `github.com/EduGoGroup/identity-shared/auth`
(`go.mod:6`) **sí** se importa: es el emisor de identidad del ecosistema, no un repo `edugo-*`.

### INV-5 · El código compartido interno vive en `wapp-shared`

`wapp-shared` es un **monorepo multi-módulo** con releases por módulo (tags `<modulo>/vX.Y.Z`). Este
repo consume cinco de sus módulos (`auth`, `config`, `envelope`, `intents`, `logger`, ver `go.mod:9-13`)
y **no consume `ui`** — su CSS es un fork local, ver `deuda.md`.

**Regla al tocar un puerto de `shared`:** se verifica contra el **tag publicado**, no contra el árbol
de al lado. Un `go build` que pasa por el `go.work` de la raíz del ecosistema no demuestra nada:
`GOWORK=off go build ./...` sí.

### INV-6 · El Edge es DESPACHADOR: no interpreta

La nube arma el payload completo (destinatario + contenido + media) y el Edge lo despacha. El Edge
**no llama endpoints de negocio** de la plataforma. Desde 2026-08-24 esto se endureció: la
inferencia también la **orquesta el Cloud** — manda el prompt ya construido y el Edge solo lo sirve
(«prompt entra, JSON sale»).

**Cómo se comprueba.** El único HTTP saliente a la nube que no va por el stream es
`POST {platform_api_base_url}/api/v1/signup` desde `wapp-ctl` (`cmd/wapp-ctl/auth.go:244-248`).

**Candado.** `internal/infra/config/config_test.go:952-969` —
`TestLoad_IntentWaitMS_YaNoSeLee` vigila que el mecanismo antiguo (el Edge esperando a un
clasificador propio) **no vuelva** por la puerta de atrás de una variable de entorno.

---

## 2 · Los invariantes propios de esta pieza

### INV-E1 · 🔴 No hay versión de esquema: una columna nueva se añade en Go, no en el `.sql`

**Es la trampa número uno de este repo.** El runner de migraciones **no lleva tabla de versión** y
**re-ejecuta todos los `.sql` embebidos en cada arranque** (`internal/infra/db/db.go:662`,
`applyMigrations`). Consecuencias:

- Todo el DDL tiene que ser **idempotente** (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`).
- Un `ALTER TABLE … ADD COLUMN` pelado en un `.sql` **arranca bien la primera vez y mata el segundo
  arranque** con `duplicate column`.
- Y editar el `CREATE TABLE IF NOT EXISTS` **no repone la columna**: sobre una base que ya existe,
  esa sentencia es un no-op. El fallo aparece luego, en la primera consulta, como
  `no such column: …` — **fallo mudo al arrancar, pérdida de señal a los minutos**.

**La única vía correcta:** una función `ensure…` en Go que lea `PRAGMA table_info(<tabla>)` y emita
el `ALTER` solo para lo ausente. Hoy hay **cuatro**, y son el patrón a copiar:

| Función | `db.go:` | Qué repone |
|---|---|---|
| `ensureDeviceMetadataColumns` | `301` | `push_name`, `business_name`, `lid` en `msg_enc_device` |
| `ensureParteInferencia` | `502` | las seis columnas de prefill en `parte_worker`, **una a una en un bucle** |
| `ensureColaClaimToken` | `560` | `claim_token` en `cola_entrantes` (nullable) |
| `ensureColaIntentos` | `621` | `intentos` en `cola_entrantes` (`NOT NULL DEFAULT 0`, y el default es lo que la hace segura en caliente) |

⚠️ Los cuatro son **SQLite-only** por el `PRAGMA table_info`: ahí se rompe la promesa de
portabilidad a Postgres, y está anotado en el propio código.

**Candado.** El SQL lo llama «REGLA, no anécdota» en el encabezado de
`internal/infra/db/migrations/cola/0001_cola_entrantes.sql`, pero **nada lo vigila**: un `ALTER` en
un `.sql` compila, pasa los tests y muere en el segundo arranque de una máquina de campo.

### INV-E2 · Son DOS bases SQLite, con perfiles de PRAGMA distintos

- `<data_dir>/edge.db` (`internal/infra/daemon/daemon.go:42`, `SingleDBFileName`): store cifrado +
  metadatos + `outbox` + `edge_config`. Se abre con `db.Open` (`db.go:95`): WAL, `foreign_keys=ON`,
  `busy_timeout=5000`, `synchronous=FULL` y **`SetMaxOpenConns(1)`** (`db.go:247`) — escritor único.
- `<data_dir>/cola_entrantes.db` (`db.OpenCola`, `db.go:148`): la cola de entrantes.
  `synchronous=NORMAL` y un WAL cuatro veces más ancho, **porque es la única base con dos procesos
  escribiendo a la vez** (el daemon anota, el cajero lee).

**Nunca metas la cola dentro de `edge.db`**: el `SetMaxOpenConns(1)` de la principal serializaría a
los dos procesos, y la poda agresiva de la cola tocaría la base del store.

**Candado.** `internal/infra/db/cola_cableado_ast_test.go:90` —
`TestColaSeAbreSiemprePorOpenCola` (sobre el AST).

### INV-E3 · El cajero es OTRO PROCESO, con OTRO prefijo de entorno

`agent cajero` es el único proceso que habla con Ollama. Es un hijo del supervisor, con su **propio
bloque de variables de entorno**: prefijo **`WAPP_WORKER_`** (`config.go:121`), distinto del
**`WAPP_AGENT_`** del daemon (`config.go:22`).

🔴 **Una variable del worker escrita con el prefijo del daemon no la lee nadie**, y el operador cree
haber configurado algo que no gobierna nada. No hay aviso: es silencio.

⚠️ Y el nombre **efectivo** es el del prefijo: el literal `MAX_CONCURRENT` del código se pone en la
máquina como `WAPP_WORKER_MAX_CONCURRENT`. El loader compone el prefijo (`config.go:729`, `:799`).

### INV-E4 · La puerta de entrantes descarta con criterios DETERMINISTAS, y el orden importa

Lo que llega mientras el Edge estuvo caído **se tira en la puerta**, y lo decide la hora del propio
mensaje contra el sello de conexión menos un margen (300 s por defecto). Las cuatro guardas de la
puerta son deterministas: eco propio, **perfil pasivo**, ventana temporal y grupo
(`internal/adapters/whatsmeow/inbound_window.go`). Ninguna es un LLM.

Detalles que se pierden si alguien «simplifica»:
- El sello se lee **fresco** en cada evaluación, es una closure, no un campo (`listener.go:426`).
- Un `Timestamp` en cero **se ingiere** y se cuenta aparte (`listener.go:679`).
- Un sello **futuro** también cae al `now` (`inbound_window.go:176-177`).
- El acuse a WhatsApp se **conserva** a propósito al desactivar el history sync.

**Candado.** Fuerte: 14 tests en `internal/adapters/whatsmeow/inbound_window_test.go`, más
`listen_gateway_margen_test.go`, que vigila que el margen configurado **llegue** al listener y que no
configurarlo **no aplaste el default**.

### INV-E5 · Sesión pasiva: nace pasiva, y el default del filtro es FAIL-OPEN a propósito

Toda sesión nace en perfil `passive`: por ella **solo se envía**, y lo que entra **se descarta en el
Edge**, ni siquiera se guarda. El perfil llega empujado desde la nube (`ConfigUpdate kind:"filters"`,
`internal/infra/wiring/filters.go:58`) y el contador sale como `dropped_passive`.

🔴 `internal/adapters/whatsmeow/listener.go:175`: **un consultor `nil` significa NINGUNA sesión es
pasiva** (fail-open, decisión escrita). Un cableado incompleto **sube** el tráfico de una pasiva en
vez de descartarlo. No lo «arregles» sin leer esa decisión.

### INV-E6 · `documentations/literal-aviso-sesion-pasiva.md` es CONTRATO EJECUTABLE

El texto que recibe el dueño de un número recién emparejado vive **por duplicado** —una copia en este
repo y otra en la nube— y en este repo tiene **dos formas**: la constante Go
`webui.AvisoSesionPasiva` (`internal/webui/aviso.go:23`) y el bloque ```` ```text ```` de
[`literal-aviso-sesion-pasiva.md`](literal-aviso-sesion-pasiva.md).

**`internal/webui/aviso_test.go` lee ese `.md` y compara carácter a carácter.** Si el fichero no
está, **falla** (no se salta): su ausencia ya es el defecto. Antes la fuente vivía en el repo de
documentación del ecosistema, y en un clon suelto el test se saltaba en silencio.

**Reglas del literal, que son parte del contrato:** texto plano sin marcado (un `*` sería asterisco
en la pantalla y negrita en WhatsApp), cero PII, las mayúsculas hacen de negrita, y si el texto
cambia **cambia su versión**: `AVISO_SESION_PASIVA_V1` → `_V2`, en el mismo commit que los dos canales.

### INV-E7 · El prompt y la salida de la inferencia NO se loguean

El carril de inferencia (`internal/adapters/cloudlink/inferencia.go`) registra **solo** `command_id`,
tamaños y desenlace. Nunca el prompt ni el JSON de vuelta. Los desenlaces se cuentan **por separado**,
no agregados en un contador único.

### INV-E8 · El circuito aprende de la LENTITUD, y su umbral se deriva del plazo de CADA petición

Una inferencia que acierta pero se come el **80 %** de su plazo cuenta como fallo del circuito.
`FraccionLentitud = 0.8` (`internal/app/cajero/cajero.go:208`) es **constante derivada, no perilla**,
y el umbral sale del plazo que viaja **con la petición** (`cajero.go:1030-1034`), no del default del
proceso. Las lentas no suman en `fallos`; el log distingue `causa=fallo` de `causa=lentitud`.

**Candado.** `internal/app/cajero/lentitud_test.go` — incluido
`TestLentitud_CincoAciertosLentosAbrenElCircuito_SinUnSoloFallo`, que es el aserto fuerte: cinco
**éxitos** abren el circuito.

### INV-E9 · El inyector de entrantes sintéticos: con la palanca bajada, la ruta NO EXISTE

`POST /v1/diag/inbound/inject` solo se **registra** si `WAPP_AGENT_INYECTOR_ENTRANTES` está en
positivo (`internal/infra/daemon/daemon.go:476-477`). No es que responda 403: es que no hay ruta.
El default es `false` y está **escrito explícitamente** en `defaults()` aunque `false` sea el cero de
Go, porque «esa decisión hay que verla, no deducirla de una línea ausente».

Todo lo inyectado lleva **doble marca** (una portante en el propio mensaje, otra local en la fila) y
entra por el **camino real** del handler. Sin escucha viva devuelve **409**, no un `200` que no midió
nada.

**Candado.** 19 tests, entre ellos `TestColaMeta_UnEntranteREAL_NoSeMarcaComoSintetico`, que es un
aserto de ausencia **con su testigo real** (sin testigo, un aserto de ausencia mide cero).

---

## 3 · Tecnología y versiones reales

Del `go.mod` (módulo `github.com/EduGoGroup/wapp-edge-agent`):

| Qué | Versión | Nota |
|---|---|---|
| **Go** | **`1.26.5`** (`go.mod:3`) | El `Makefile` y el workflow fijan la misma. ⚠️ El `README.md` del repo dice «Go 1.26.0»: está desactualizado. |
| `go.mau.fi/whatsmeow` | `v0.0.0-20260616120636-eaa388b4e537` | **pseudo-versión, sin tag**. El socket, el QR, el envío y los eventos. |
| `github.com/EduGoGroup/wapp-cloudlink` | `v0.17.0` | El contrato proto del stream bidi + los helpers mTLS y de lease. |
| `github.com/EduGoGroup/wapp-shared/logger` | `v0.2.0` | La dependencia más transversal del repo. |
| `github.com/EduGoGroup/wapp-shared/envelope` | `v0.2.1` | Sellado AES-256-GCM / X25519. |
| `github.com/EduGoGroup/wapp-shared/{auth,config,intents}` | `v0.4.1` / `v0.3.0` / `v0.1.0` | JWKS+RBAC, loader de config, catálogo de intenciones. |
| `github.com/EduGoGroup/identity-shared/auth` | `v0.3.1` | Validación ES256 del token de operador. |
| `github.com/EduGoGroup/wapp-edge-intent` | `v0.3.0` | Hoy se usa por su paquete `ollama/`; del `classifier/` solo se importan **cuatro constantes numéricas**. |
| `modernc.org/sqlite` | `v1.53.0` | Driver **pure-Go**: SQLite sin CGO. |
| `google.golang.org/grpc` | `v1.82.1` | Transporte del stream y del enrolamiento. |
| `github.com/lib/pq` | `v1.12.3` | **Solo** tras el build-tag `postgres`. Sin el tag, abrir Postgres da error. |

**CGO.** Todo compila pure-Go **salvo el Keychain de macOS** (`//go:build darwin` en
`internal/adapters/keycustody/keychain_darwin.go`). El `Makefile` fuerza `CGO_ENABLED=0` en los
cuatro targets no-darwin y `=1` en darwin/arm64.

**Sin ORM.** `database/sql` con SQL a mano.

**Frontend sin build step.** `internal/webui/` son cuatro assets (`index.html`, `login.html`,
`app.js`, `styles.css`) servidos con `embed.FS`. JS vanilla con `fetch` + `EventSource`. No hay
bundler, no hay framework, y **no consume el módulo `ui` de `wapp-shared`**.

**Tamaño.** 327 ficheros `.go` · 80.465 líneas · 186 ficheros `_test.go` con **949** funciones
`Test*` · **44 paquetes** (`go list ./...`).

---

## 4 · Convenciones de código de este repo

1. **Los comentarios son normativos y están en español.** ~31 % de las líneas del repo son
   comentarios, y muchos no describen el código: **prohíben** una alternativa y explican qué se
   rompió cuando se intentó. Léelos antes de «simplificar» una función; suelen ser el único candado.
2. **Los tests se llaman en español y describen la conducta**, no el método:
   `TestVentana_NuestroAtascoNoCuentaComoAntiguedad`, `TestOnMessage_ElINSERTQueFalla_NoSeAcusa`.
3. **Un invariante estructural se vigila sobre el AST, no con N tests de conducta.** Hay **10**
   ficheros con ese nombre (`find . \( -name '*_ast_test.go' -o -name '*_cableado_test.go' \) | wc -l`
   → 10), pero **solo 6 parsean el AST de verdad** (`… | xargs grep -l 'go/parser\|go/ast'` → 6:
   `cmd/wapp-ctl/arranque_encadenado_ast_test.go`,
   `internal/adapters/whatsmeow/listen_gateway_cableado_test.go`,
   `internal/app/cajero/clase_ast_test.go`, `internal/app/cola_enum_ast_test.go`,
   `internal/infra/daemon/perfiles_cableado_test.go`, `internal/infra/db/cola_cableado_ast_test.go`).
   Los otros cuatro —`sessionmgr/{aviso,latencia,sesion_pasiva}_cableado_test.go` y
   `daemon/latencia_cableado_test.go`— comprueban el cableado por otra vía: el sufijo del nombre
   **no** garantiza que se parsee el fichero. Si la regla es «esto se abre siempre por aquí» o «este
   orden», el test correcto parsea el fichero.
4. **Un enum tiene su test de huérfanos**: `TestEnumMotivoOmitidoNoTieneHuerfanas`,
   `TestErroresInferencia_NoTieneHuerfanos`.
5. **Toda goroutine de trabajo lleva `recover()` por unidad de trabajo** en los puntos donde un
   pánico ajeno tumbaría un worker (**hay ocho**:
   `grep -rn 'if r := recover()' --include='*.go' . | grep -v _test | wc -l` → 8 —
   `internal/app/sessionmgr/despacho.go:192` y `listen.go:357`,
   `internal/app/despachador/despachador.go:364`,
   `internal/adapters/cloudlink/dispatcher.go:109` e `inferencia.go:209`,
   `internal/adapters/edgeconfig/service.go:188`,
   `internal/adapters/whatsmeow/listener.go:467` y `:853`).
6. **Los defaults que sostienen una conducta se escriben explícitos** en `defaults()`, aunque
   coincidan con el cero de Go.
7. **Un `<= 0` cae al default** en casi toda la config… con **una excepción deliberada**:
   `WAPP_WORKER_KEEP_ALIVE_SECONDS`, donde `-1`, `0` y positivo significan tres cosas legítimas
   (`config.go:269-280`). No le pongas guardarraíl.
8. **Las variables retiradas avisan al arrancar** en vez de ignorarse en silencio
   (`config.VariablesRetiradas()`, emitida en `daemon.go:82-85`).

---

## 5 · Trampas conocidas (lo que se hace mal aquí si nadie lo dice)

| # | Trampa | Qué pasa de verdad |
|---|---|---|
| 1 | **Añadir una columna editando el `.sql`** | Ver INV-E1. Arranca la primera vez, muere la segunda, o es un no-op silencioso sobre una base que ya existe. Se hace con un `ensure…` en Go. |
| 2 | **Poner una variable del cajero con prefijo `WAPP_AGENT_`** | No la lee nadie y no hay aviso. Ver INV-E3. |
| 3 | **Creer el `CLAUDE.md` histórico del repo** | El que había antes de 2026-08-30 decía «dos binarios» (son tres), «un solo `.db`» (son dos), listaba entidades de dominio que **no existen** (`SendJob`, `Lease`, `DEK` en `domain`), no mencionaba el cajero ni una vez, y atribuía `/v1/daemon/start` al núcleo (lo sirve `wapp-ctl`). **No heredes nada sin comprobarlo.** |
| 4 | **`NewFileCustody` no devuelve un archivo** | Devuelve el **Keychain** en darwin, **DPAPI** en windows y **Secret Service** en linux; solo el fallback devuelve un archivo (`internal/adapters/keycustody/file_fallback.go:12`). El nombre sugiere lo contrario de lo que hace. |
| 5 | **`make test` no lleva `GOWORK=off`** | `lint` y `build-check` sí lo llevan; `test` no (`Makefile:102-103`). Dentro del árbol del ecosistema hay un `go.work` dos niveles arriba, así que **los tests compilan contra los módulos de al lado**, no contra los tags que fija `go.mod`. Un verde ahí no demuestra que el repo esté verde solo. |
| 6 | **Los ~30 tests de `Reclamar` vigilan código sin llamante** | `internal/app/cajero/cajero.go:288-291` lo dice: desde el cambio de doctrina el cajero **no reclama** de la cola; de los tres métodos de `ColaCajero` solo se usa `BarrerLeasesVencidos`. Verde denso sobre un camino que producción no ejecuta. |
| 7 | **Contar las rutas sin decir la regla** | Hay dos superficies (socket unix y loopback) y tres de las de `wapp-ctl` dependen de un flag. Cualquier número sin su regla es falso. Ver `contratos.md`. |
| 8 | **`POST /v1/daemon/stop` no pide nada si no hay sesión** | `requireCSRFIfSession` solo exige CSRF **si** hay cookie. Sin sesión de operador, cualquier página puede pararte el daemon 24/7. Está en `deuda.md` como hallazgo de seguridad. |
| 9 | **El bind de `wapp-ctl` se asume loopback y no lo vigila nadie** | La defensa anti-CSRF está escrita apoyándose en que el bind sea `127.0.0.1` (`cmd/wapp-ctl/session.go:163`), pero `-addr` acepta cualquier cosa y **en UAT se arranca con `-addr :8105`**. Ningún `IsLoopback` mira la dirección del listener. |
| 10 | **Tocar el envío por `internal/app/send.go` o `adapters/whatsmeow/sender.go`** | Es la vertical **muerta**: cero llamantes de producción. El envío real va por `ListenGateway.SendViaLiveClient` (`internal/app/sessionmgr/listen.go`). Ver `deuda.md`. |
| 11 | **Fiarse del `CHANGELOG.md`** | Su `[Unreleased]` (`CHANGELOG.md:53`) dice `wapp-cloudlink v0.16.0`; `go.mod:7` fija **`v0.17.0`** (la línea 8 es `wapp-edge-intent v0.3.0`). |
| 12 | **Contar un `rc=0` como «todo pasó»** | Hay `t.Skip` que se disparan solos (sin Keychain accesible, sin `WAPP_AGENT_TEST_PG_DSN`) y dos ficheros que **ni se compilan** sin su build-tag. Un `rc=0` cuenta igual un `--- SKIP` que un `--- PASS`. Ver `operacion.md`. |
