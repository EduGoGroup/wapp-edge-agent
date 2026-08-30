# Deuda viva de `wapp-edge-agent`

Lo que está mal, incompleto o pendiente **hoy**, con `fichero:línea`, qué consecuencia tiene y cómo
se cerraría. Levantado el **2026-08-30** contra `main` en `490f8212`.

Lo que **no** está aquí: las trampas que un agente pisa al programar (eso es
[`constitucion.md`](constitucion.md) §5) y las decisiones deliberadas que parecen defectos y no lo
son (marcadas ahí como tales).

---

## 1 · 🔴 Seguridad

### S-1 · Sin material mTLS, el Edge marca la nube EN CLARO

`internal/infra/wiring/cloudlink.go:280-281`. Si faltan `tls_cert`, `tls_key` o `tls_ca`, se emite un
`log.Warn("… dial INSECURE — solo desarrollo")` y se devuelve `insecure.NewCredentials()`. **Esas tres
rutas no tienen valor por defecto**: la postura de partida es abierta, no cerrada. Un Edge recién
instalado y sin enrolar habla en claro con la nube, y la única señal es un `Warn` entre muchos.

**Consecuencia.** El tráfico Edge↔nube (contenido de negocio, no llaves) circula sin cifrar hasta que
alguien lee el log.

**Cómo se cierra.** Un aserto de arranque: si el proceso no está en un modo `dev` **declarado
explícitamente**, faltar material mTLS es error fatal, no `Warn`. Y un test unitario sobre
`clientCreds` que afirme «sin las tres rutas, error» — hoy **no existe ninguno**.

### S-2 · Sin `lease_pubkey_path`, el kill-switch NO SE CONSTRUYE

`internal/infra/wiring/cloudlink.go:291-293` (y el tercer camino, `:304`, cuando el fichero aún no
existe). Devuelve `nil, nil`, y sin validator las dos comprobaciones de despacho
(`internal/adapters/cloudlink/adapter.go:824` y `:850`) **no gatean nada**.

**Consecuencia.** El kill-switch anti-clon es **opt-in por configuración**. Una instalación a la que
el enrolamiento no le entregó la clave pública despacha sin lease y nadie lo nota.

**Cómo se cierra.** Igual que S-1: aserto de arranque en modo no-dev. Nótese que **no hay ningún test
en este repo** que afirme «sin material de lease, el Edge no debe despachar»; los tests del lease
viven en `wapp-cloudlink` y prueban el validador, no su cableado.

### S-3 · `POST /v1/daemon/stop` y `/start` no exigen NADA sin sesión

`cmd/wapp-ctl/main.go:248` y `:260`, con `requireCSRFIfSession` en `:400-408`: solo valida el CSRF
**si** hay cookie de sesión. Sin operador logueado, pasa directo. Como es un POST sin cabeceras
personalizadas, **cualquier página web que el usuario visite puede lanzarlo cross-origin sin
preflight** y parar el daemon 24/7. `GET /v1/daemon/status` y `/v1/cajero/status` no piden nada en
absoluto.

**Consecuencia.** Denegación de servicio trivial contra el núcleo, disparada desde una pestaña
cualquiera, mientras el operador esté deslogueado.

**Cómo se cierra.** Acotar la exención al **bootstrap real** (sin enrolamiento completado, o dentro
de una ventana de primera ejecución) en vez de a «no hay sesión», que vale para siempre. Alternativa
mínima: exigir una cabecera personalizada, que ya obliga a preflight.

### S-4 · La defensa anti-CSRF se apoya en un bind loopback que nadie vigila

El código lo dice en cuatro sitios, entre ellos `cmd/wapp-ctl/session.go:163` («el plano de control es
loopback http (127.0.0.1); SameSite=Strict acota el CSRF de origen cruzado») y `auth.go:168`. El
default del flag es `127.0.0.1:8105` (`cmd/wapp-ctl/main.go:46`), **pero `-addr` acepta cualquier
cosa**, y en el despliegue de UAT la unidad systemd arranca `wapp-ctl -addr :8105`, es decir en
**todas** las interfaces: la pantalla de login del plano de control del Edge queda expuesta a
Internet, y con ella `POST /v1/daemon/stop` de S-3.

`grep -rn 'IsLoopback'` devuelve dos apariciones en el repo
(`internal/infra/config/platform_url.go:31` y `internal/app/cajero/taskset_linux.go:137`) y **ninguna
mira la dirección del listener**.

**Cómo se cierra.** Que el arranque **rechace o degrade** un `-addr` no-loopback salvo con una
palanca explícita (`WAPP_CTL_ALLOW_NON_LOOPBACK` o equivalente) que además obligue a `Secure` en las
cookies y a un CSRF incondicional. Y arreglar la unidad en el despliegue.

### S-5 · El refresh token del operador vive en un archivo `0600`, no en el keystore

`internal/auth/custody.go:18-23` (deuda **explícita** en el código) y `internal/infra/wiring/auth.go:62`.
El archivo es `<data_dir>/auth/operator.refresh`. El motivo está escrito: los adaptadores de
`keycustody` codifican el contrato de **32 bytes** de la DEK y no admiten un opaco de longitud
variable.

**Cómo se cierra.** Un segundo contrato en `keycustody` para blobs de longitud variable, y mover ahí
el refresh. No es un cambio pequeño: toca los cuatro backends.

### S-6 · La DEK en archivo es el «suelo», no la custodia

`internal/adapters/keycustody/file.go:15`, también deuda explícita. En plataformas sin keystore la
DEK queda en `<data_dir>/keys/<session_id>.key` con permisos `0600`. Es aceptado a propósito, pero es
el eslabón más débil de la doble llave.

### S-7 · `packaging/macos/bootstrap/ca.pem` es un PLACEHOLDER, y `make pkg` lo empaqueta igual

Ocho líneas de comentario que dicen «reemplaza este archivo por el certificado CA REAL antes de
construir el `.pkg`». Nada lo comprueba: `make pkg` y `make dist-*` lo copian tal cual.

**Consecuencia.** Un instalador construido sin sustituirlo produce un Edge que **no puede validar al
Gateway**, y el síntoma aparece en casa del cliente.

**Cómo se cierra.** Que `verify-zero-knowledge.sh` (que ya corre en `make pkg`) falle también si
`ca.pem` sigue siendo el placeholder. Es una línea de `grep`.

---

## 2 · Código muerto verificado

### M-1 · `internal/infra/deps/deps.go` — 16 líneas sin un solo importador

Su propio doc dice que se retira «en cuanto el código real las importe», y eso ya pasó: `qrterminal`
lo usa `internal/adapters/control/qr.go`, `modernc.org/sqlite` lo usa `internal/infra/db/db.go` y
`whatsmeow` lo usan 19 ficheros.

**Evidencia.** `grep -rn 'infra/deps' --include='*.go' .` → **cero resultados** fuera del propio
fichero. **Cómo se cierra.** Borrar el paquete y correr `go mod tidy`.

### M-2 · La vertical de envío efímero — 894 líneas

| Fichero | Líneas | Estado |
|---|---|---|
| `internal/app/send.go` | 107 | El caso de uso `Send` y el puerto `app.Sender`. `NewSend` solo se llama desde `send_test.go` (5 usos) |
| `internal/adapters/whatsmeow/sender.go` | 375 | La única implementación. `NewSender` tiene **cero llamantes**: solo su definición y dos comentarios |
| `internal/app/send_test.go` + `sender_test.go` | 123 + 289 | Los tests que la mantienen verde |

El propio fichero se autodeclara legado en `internal/adapters/whatsmeow/sender.go:342-346` («LEGADO /
DEPRECADO para envío real … se conserva como costura de tests/legacy»). El envío de producción va por
`ListenGateway.SendViaLiveClient(Tracked)` (`internal/app/sessionmgr/listen.go:94,164`).

De regalo, `sender.go:358` contiene un `time.Sleep` dentro de un `defer`: **el único `time.Sleep` de
todo el código de producción del repo**.

**Cómo se cierra.** Borrar los cuatro ficheros. Contradice la regla que el propio repo se fijó en su
plan de limpieza: «código que no se usa se elimina — no debe existir legacy».

### M-3 · `Reclamar` y `MarcarClasificado`: vivos en tests, muertos en producción

`internal/app/cajero/cajero.go:288-291` lo dice literalmente: desde el cambio de doctrina el cajero
**no reclama** de la cola, y de los tres métodos de `app.ColaCajero` solo se usa `BarrerLeasesVencidos`.
`grep -rn '\.Reclamar(' --include='*.go' .` da ~32 líneas y **todas** están en `_test.go`.

**Consecuencia.** ~30 tests muy detallados (fencing, relevo, lote veneno) vigilan un camino que
producción no ejecuta. Es verde que no protege nada, y peor: da la impresión de que sí.

**Cómo se cierra.** Decisión de producto pendiente. O se retira el mecanismo con sus tests, o se
documenta explícitamente que se conserva para drenar colas de discos de clientes viejos (que es la
razón por la que `EstadoClasificado` sigue declarado, `internal/app/cola.go:121`).

---

## 3 · Arquitectura y mantenibilidad

### A-1 · `app` importa adaptadores concretos en 4 ficheros (5 líneas de import)

```
internal/app/sessionmgr/pairing.go:24   → internal/adapters/cryptostore
internal/app/sessionmgr/pairing.go:25   → internal/adapters/whatsmeow
internal/app/sessionmgr/inyector.go:24  → internal/adapters/whatsmeow
internal/app/sessionmgr/unlink.go:19    → internal/adapters/cryptostore
internal/app/sessionmgr/listen.go:31    → internal/adapters/whatsmeow
```

`internal/domain` sí está limpio (cero imports internos). El plan que «desacopló DIP en `sessionmgr`»
cerró **solo la de `keycustody`**, que es lo que su tarea pedía; quien lea «DIP desacoplado»
concluirá algo falso.

**Cómo se cierra.** Extraer puertos para lo que hoy se usa de esos dos adaptadores. No es urgente,
pero **sí hay que dejar de venderlo como hexagonal limpio**.

### A-2 · `Daemon.Run` son 307 líneas y **ningún test la ejercita**

`internal/infra/daemon/daemon.go:65-371`. El propio fichero lo admite (`:388-400`) y por eso extrajo
tres opciones a funciones (`buildLatencia`, `opcionPalancaDespachador`, `opcionPerfilesSesion`) para
poder afirmarlas sobre el AST. Las **otras ~15 líneas de cableado siguen sin candado**: cambiar una
dependencia de sitio ahí no rompe ningún test.

Otras funciones grandes, por si hace falta priorizar: `config.Load` (297 líneas,
`internal/infra/config/config.go:724`), `servidorInferencia.Inferir` (229,
`internal/app/cajero/servidor.go:99`), `Listener.onMessage` (225,
`internal/adapters/whatsmeow/listener.go:572`), `runCajero` (188, `cmd/agent/cajero.go:54`),
`wapp-ctl main()` (173), `carrilInferencia.atender` (160), `Store.Reclamar` (148).

**Cómo se cierra.** El patrón ya existe en el repo: extraer cada decisión a una función nombrada y
vigilarla con un test sobre el AST. Es incremental.

### A-3 · No hay `.golangci.yml`, aunque el `Makefile` invoque `golangci-lint`

`make lint` corre con los linters **por defecto**. Consecuencia concreta: **no hay `depguard`**, que
es exactamente lo que vigilaría tres de los invariantes más nucleares del producto sin coste — que
`whatsmeow` no salga del Edge, que no entre un broker, y que no entre un import `edugo-*`.

**Cómo se cierra.** Un `.golangci.yml` con `depguard` y tres reglas. Es un bloque de YAML.

### A-4 · El único `TODO` real del repo

`internal/app/sessionmgr/unlink.go:109` — `TODO(cloud, follow-up …)`: revocar una **cuenta entera**
desde la nube exige hoy un fan-out de N `LeaseUpdate{revoked}`, uno por `session_id`, porque el
contrato no tiene un frame agregable por número. **El corte está en el repo del proto**, no aquí.

(El resto de los ~117 aciertos de «TODO» en el repo son la palabra española en mayúsculas dentro de
comentarios.)

---

## 4 · Interfaz local (la web embebida)

### U-1 · Pares mixtos: color que sigue al tema sobre fondo que no

Medido sobre los literales del CSS con la fórmula WCAG. **NO VERIFICADO en navegador**, que es como
manda medirlos la doctrina del proyecto; lo que sí está verificado es que el literal y el `var()`
conviven en la misma declaración.

| Regla | Evidencia | Claro | **Oscuro** |
|---|---|---|---|
| `.enroll-form input { background:#fff; color: var(--ink) }` | `internal/webui/styles.css:303` | 17,13:1 | **1,29:1** |
| `.badge-unknown { background:#e5e7eb; color: var(--muted) }` | `internal/webui/styles.css:273` | 7,53:1 | **1,37:1** |

🔴 El primero es **el campo del código de activación**: en modo oscuro el operador escribiría el
código casi invisible. **Cómo se cierra.** El par viaja entero: si el color sigue al tema, el fondo
también. Otra consola del ecosistema ya cerró exactamente este caso.

Además, `.badge-ok` (3,00:1) y `.badge-warn` (2,86:1) están por debajo de AA para texto normal en
**los dos** temas — no son mixtos, son bajos.

### U-2 · Cuatro clases usadas y no definidas

`wapp-link`, `wapp-snackbar--success`, `wapp-btn--outlined` y `wapp-caption` se usan en
`internal/webui/login.html` y **no están en `styles.css`**. Y `styles.css` **no tiene ninguna regla
para `a`** (`grep -nE '^\s*a[ ,{:]' styles.css` → vacío), así que los enlaces «Solicita acceso» /
«Inicia sesión» se pintan con el azul por defecto del navegador, que en oscuro da 1,82:1.

**Causa raíz.** `styles.css` es un **fork** de los tokens del módulo `ui` de `wapp-shared`, con
alias de compatibilidad propios; el Edge **no consume ese módulo** (`grep -rn 'wapp-shared/ui' .` →
cero; `go.mod:9-13` declara `auth`, `config`, `envelope`, `intents` y `logger`, no `ui`).

**Cómo se cierra.** O el Edge consume el módulo `ui` como las otras tres consolas, o el fork se
completa. Lo que no puede quedarse es a medias.

### U-3 · El plano de control no emite ninguna cabecera de seguridad

`grep -rn 'Content-Security-Policy' --glob '!*_test.go' .` → **cero**. Las tres consolas del
ecosistema sí las emiten. Es coherente con que `login.html` sea un documento autocontenido con
`<script>` inline (una CSP estricta lo rompería), pero es una diferencia deliberada que conviene
conocer antes de comparar esta UI con las otras.

---

## 5 · Documentación del repo que miente

| Dónde | Qué dice | Qué es verdad |
|---|---|---|
| `README.md:9` | «Go 1.26.0» | `go.mod:3` dice **1.26.5**, igual que el `Makefile` y el workflow |
| `README.md:16` | «planes 002–031 cerrados» | El código lleva encima seis planes posteriores; el último commit es literalmente un merge de uno de ellos |
| `README.md` §Variables | Se presenta como el mapa de variables | Faltan **18** que el código sí lee, **incluidas las tres palancas de riesgo** (modo sombra del lease, despachador apagado, cuentas alfa). Ver `contratos.md` §4 |
| `CHANGELOG.md:53` `[Unreleased]` | «`wapp-cloudlink` … v0.16.0» | `go.mod:**7**` fija **v0.17.0** (la `:8` es `wapp-edge-intent v0.3.0`) |
| `cmd/agent/main.go:11`, `cmd/agent/cajero.go:24`, `internal/infra/config/config.go:114` y `:542`, `internal/app/cola.go:478` | El cajero es «el **tercer** hijo de `wapp-ctl`» | Son **dos** supervisores (`cmd/wapp-ctl/main.go:91` y `:118`), y la propia documentación de `wapp-ctl` lo llama «SEGUNDO hijo». Cinco comentarios cuentan mal |

**Cómo se cierra.** Los cuatro primeros, editando. El quinto es un `sed` de cinco líneas — pero
**arregla el comentario, no el conteo**: no añadas un tercer supervisor para que el comentario tenga
razón.

---

## 6 · Observado en campo, sin diagnóstico

**NO VERIFICADO.** En la máquina de UAT, los WAL de las dos bases son **mayores que sus bases**:
`cola_entrantes.db-wal` = 16,5 MB frente a 287 KB de base, y `edge.db-wal` = 4,2 MB frente a 1,8 MB.
No sé si es checkpoint pendiente o consecuencia del perfil `journal_mode=WAL` con checkpoint
diferido que la cola usa a propósito. Lo dejo anotado porque un WAL 57 veces su base es un dato
llamativo, **no porque sepa que es un fallo**.

**Cómo se investigaría.** `PRAGMA wal_checkpoint(TRUNCATE)` sobre una copia, y medir si el WAL vuelve
a crecer con el mismo perfil de carga.
