# Deuda viva de `wapp-edge-agent`

Lo que está mal, incompleto o pendiente **hoy**, con `fichero:línea`, qué consecuencia tiene y cómo
se cerraría. Levantado el **2026-08-30** contra `main` en `490f8212`. **Ampliado el 2026-10-14** con
S-8, S-9, A-6 y A-7, los hallazgos del laboratorio de flujos (Plan 058 de la raíz de wApp), localizados
por símbolo ese día en la rama `laboratorio` (`e48c038`). Lo que no verifiqué va marcado **NO
VERIFICADO**, **sin medir** o **leído, sin ejecutar**.

**Recuento** (contando los títulos `### X-n` de este fichero, con X en S, M, A y U): **22**
identificadores. **4 cerrados** (M-1, U-1, U-2 y U-3), **1 cerrado en parte** (M-2), **1 nota
informativa** (A-7) y **16 abiertos**. Las tablas de §5 y la observación de §6 no llevan identificador
y no cuentan. Un identificador cerrado no se reutiliza ni se borra.

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

### S-8 · 🔴 El `chat_jid` va EN CLARO en `cola_entrantes.db`

> Añadida el 2026-10-14: hallazgo del laboratorio de flujos (Plan 058 de la raíz, escalón E15). Sin
> corregir. Localizada por símbolo ese día en la rama `laboratorio` (`e48c038`).

`internal/infra/db/migrations/cola/0001_cola_entrantes.sql:104` (la columna) y `:123` (el índice
`ix_cola_conv`). El texto y los metadatos sí van sellados con la DEK de la sesión (`texto_enc`,
`meta_enc`), pero `chat_jid` se escribe tal cual al encolar
(`internal/adapters/colaentrantes/colaentrantes.go:285`), y el `chat_jid` es el **teléfono del
cliente**. Medido por E15 sobre los bytes crudos del fichero y de su WAL: «tequeños» aparece **0**
veces y el número del cliente, **20**.

**Es una decisión escrita, no un descuido**, y por eso hay que revisarla en lugar de parchearla. El DDL
la justifica en `0001_cola_entrantes.sql:21` a `:25`: `chat_jid` es «la clave de conversación del claim
atómico del cajero y del índice `ix_cola_conv`», «metadato de enrutado, no contenido de negocio». Hay
tres cosas que no cuadran con esa justificación:

- **El mismo repo lo trata como dato personal.** `chat_jid` no se loguea en ninguna línea: los logs
  llevan una huella (`chatJIDHash`, `internal/adapters/colaentrantes/claim.go:15` a `:24`), y el
  despachador lo prohíbe (`internal/app/despachador/despachador.go:73`). Y el remitente de la misma fila
  sí va sellado dentro de `meta_enc`.
- **El claim del cajero que lo justifica no corre en producción** (M-3). Lo que sí lo usa en producción
  es el sacrificio por tope, que agrupa por conversación
  (`dropConversacionPendienteMasViejaLocked`,
  `internal/adapters/colaentrantes/colaentrantes.go:625`), y la entrega, que lo pasa al evento
  (`internal/app/despachador/despachador.go:574`).
- **Ningún ADR lo decide.** `grep -rn chat_jid` sobre los ADR de la raíz de wApp no devuelve nada: la
  decisión vive solo en el comentario del DDL.

**Consecuencia.** Quien copie `cola_entrantes.db` (una copia de seguridad, un disco que se cambia, un
fichero que se manda a soporte) se lleva la lista de teléfonos que escribieron al negocio, con su
`ts_whatsapp`, sin necesitar la DEK. Las filas `despachado` duran el TTL (24 h); las de una sesión
desvinculada, más (S-9). No rompe el zero-knowledge (es el disco del cliente y la nube no lo ve), pero
deja sin cifrar el dato personal más directo de la fila.

**Cómo se cierra.** Separar las dos funciones del campo: para agrupar y para el índice, una huella con
llave (por ejemplo, un HMAC con una llave derivada de la DEK de la sesión, que `session_id` ya permite
elegir); el valor, dentro de `meta_enc`, que ya está sellado. Ojo con la trampa del esquema: aquí no hay
tabla de versión, así que una columna nueva va con un `ensure…` en Go y no editando el `CREATE TABLE`. Y
la decisión del DDL se reescribe con ella.

**Sin medir**: si `edge.db` guarda también el número en claro en otras tablas. E15 solo miró
`cola_entrantes.db`.

**Lo reproduce**: laboratorio E15, paso 5 (Plan 058), PR #15, abierto, en revisión.

### S-9 · 🟡 Desvincular no borra las filas de la cola

> Añadida el 2026-10-14: hallazgo del laboratorio de flujos (Plan 058 de la raíz, escalón E15). Sin
> corregir. **Leído en el código, sin ejecutar** para las filas `nuevo`.

`Unlink` (`internal/app/sessionmgr/unlink.go:43`) borra la DEK (`clearDEK`, `:73`), el material
cifrado y la fila de metadatos, y olvida el sobre cacheado de la cola (`forgetColaCrypter`, `:231`, que
llama a `Store.Forget`, `internal/adapters/colaentrantes/colaentrantes.go:391`). Pero `Forget` solo
vacía dos mapas en memoria: **ninguna fila de `cola_entrantes` se borra**. La poda por TTL solo toca
filas `despachado` con sello (`pruneTTLLocked`, `colaentrantes.go:452`), a propósito (REQ-051.7).

**Consecuencia.** Las filas `despachado` de la sesión desvinculada duran hasta el TTL (24 h); E15 lo
comprobó (las 5 filas siguen tras el `DELETE`). Una fila `nuevo` de esa sesión no la drena nadie: su
despachador ya paró, el `session_id` no vuelve (cada emparejamiento acuña uno nuevo) y su texto ya no se
puede abrir, porque la DEK se borró. Solo la saca el sacrificio por tope (`dropOldestLocked`, `:498`),
bajo presión. Mientras tanto ocupa sitio y conserva el `chat_jid` en claro (S-8).

**Cómo se cierra.** Que `Unlink` borre las filas de la sesión en la cola, en el mismo punto donde hoy
llama a `forgetColaCrypter` (después del join de las goroutines, para que nadie las vuelva a escribir).
Antes hay que decidir qué pasa con una fila `nuevo` que no llegó a la nube: con la DEK borrada ya no se
puede entregar, así que borrarla es reconocer una pérdida que hoy ya existe en silencio.

**Lo reproduce**: laboratorio E15, paso 11 (Plan 058), PR #15, abierto, en revisión (solo las filas
`despachado`).

---

## 2 · Código muerto verificado

### M-1 · `internal/infra/deps/deps.go` — ✅ BORRADO 2026-09-01

Su propio doc decía que se retiraba «en cuanto el código real las importe», y eso ya había pasado:
`qrterminal` lo usa `internal/adapters/control/qr.go`, `modernc.org/sqlite` lo usa
`internal/infra/db/db.go` y `whatsmeow` lo usan 19 ficheros. Confirmado de nuevo antes de borrar
(`grep -rn 'infra/deps' --include='*.go' .` → cero fuera del propio fichero) y cerrado: se borró el
paquete entero y se corrió `go mod tidy`. `go build`/`vet`/`test ./...` en verde tras el borrado.

### M-2 · La vertical de envío efímero — ⚠️ PARCIALMENTE CERRADO 2026-09-01: el diagnóstico original ERA IMPRECISO

**La instrucción de esta ficha («Borrar los cuatro ficheros») habría roto producción.** Antes de
borrar se re-verificó cada símbolo de `internal/adapters/whatsmeow/sender.go` con
`grep -rln` **fuera** de ese fichero, y varios —`parseRecipient`, `buildMessage`, `downloadMedia`,
`buildMediaMessage`, `outgoing`, `loadDeviceFunc`, `realLoadDevice`, `realLoadDeviceByJID`— resultaron
tener llamante real en `listen_gateway.go` (`ListenGateway.SendViaLiveClient(Tracked)` y
`SendMediaViaLiveClientTracked`), que es justo el camino de producción que esta misma ficha nombraba
como reemplazo. `sender.go` era un fichero MIXTO: la mitad (el adaptador `Sender` efímero) estaba
muerta; la otra mitad eran helpers compartidos con el camino vivo. No se supo por qué esto no se vio
el 2026-08-30 — probablemente `listen_gateway.go` empezó a reusar esos helpers después de esa fecha,
o el barrido de entonces no comprobó los símbolos uno a uno, solo contó llamantes de `NewSender`.

**Lo que sí se borró (genuinamente muerto, cero llamantes reales):**

| Fichero | Qué se quitó |
|---|---|
| `internal/app/send.go` (borrado entero) | El caso de uso `Send`, el puerto `app.Sender`, `NewSend`. Cero llamantes fuera de su propio test. |
| `internal/app/send_test.go` (borrado entero) | Sus tests. `custodyWith` (helper compartido con `listen_test.go`/`listen_watchdog_test.go`) se movió a `pair_test.go`, junto a `fakeCustody`. |
| `internal/adapters/whatsmeow/sender.go` (recortado, NO borrado) | El adaptador `Sender` + `NewSender` + `newSenderWithDeps` + `SendText` + `SendMedia` + `realDispatch` + `messageFor` + `dispatchFunc` + `defaultConnectTimeout`/`defaultFlushDelay` + los imports que solo ellos usaban (`internal/app`, `wapp-shared/logger`, `waLog`). El fichero pasó de 375 a ~225 líneas. |
| `internal/adapters/whatsmeow/sender_test.go` (recortado, NO borrado) | Los tests del adaptador muerto. Los que cubrían `downloadMedia` solo indirectamente (a través de `SendMedia`) se reescribieron para llamarlo DIRECTO (`TestDownloadMedia_NoCredentials`, `TestDownloadMedia_NonOKStatus_Error`), para no perder esa cobertura al quitar el wrapper. |

De regalo, el único `time.Sleep` de todo el código de producción del repo (`realDispatch`, antiguo
`sender.go:358`) se fue con el adaptador muerto — ya no existe.

**Verificado tras el recorte**: `GOWORK=off go build/vet/test ./...` en verde, y un `grep` de cada
símbolo eliminado confirma cero referencias sueltas.

**Lección para el próximo barrido de código muerto**: contar llamantes del CONSTRUCTOR (`NewX`) no
basta cuando el fichero mezcla un tipo muerto con funciones libres que otro tipo vivo, EN EL MISMO
PAQUETE, puede llamar sin pasar por él. Hay que grepear cada símbolo exportado o no por separado.

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

### A-1 · `app` importa adaptadores concretos en 3 ficheros (3 líneas de import)

```
internal/app/sessionmgr/listen.go:31    → internal/adapters/whatsmeow     (solo `whatsmeow.Backoff`)
internal/app/sessionmgr/pairing.go:24   → internal/adapters/cryptostore
internal/app/sessionmgr/unlink.go:19    → internal/adapters/cryptostore
```

Eran 5 líneas en 4 ficheros hasta el 2026-10-11: el ADR-0049 sacó de `sessionmgr` la **construcción**
del adaptador de whatsmeow (ver A-5). Lo que queda de ese adaptador es un tipo puro, y un candado
sobre el AST no deja añadir nada más
(`TestFronteraDelTransporte_SessionmgrNoConstruyeElAdaptadorReal`, `internal/app/sessionmgr/transporte_test.go`).

`internal/domain` sí está limpio (cero imports internos). El plan que «desacopló DIP en `sessionmgr`»
cerró **solo la de `keycustody`**; quien lea «DIP desacoplado» concluirá algo falso.

**Cómo se cierra.** Mudar `Backoff` a un paquete neutral y extraer un puerto para lo que se usa de
`cryptostore`. No es urgente, pero **sí hay que dejar de venderlo como hexagonal limpio**.

### A-5 · La frontera del transporte de WhatsApp existe, pero `whatsmeow` sigue asomando fuera de su adaptador

**Qué hay (ADR-0049, 2026-10-11).** `sessionmgr` ya no construye ningún adaptador: recibe un
`sessionmgr.Transporte` (`internal/app/sessionmgr/transporte.go`) que el arranque elige una vez
(`elegirTransporte`, `internal/infra/daemon/daemon.go`). Hay dos: `wiring.TransporteWhatsmeow` y
`wiring.TransporteFalso`. Añadir un tercero —otra librería, un proceso auxiliar, la API oficial de
WhatsApp— es otro adaptador que rellena ese struct y otra rama en `elegirTransporte`.

**Qué se dejó a propósito** (decisión D-FW-4 de la reconstrucción de la plataforma: «solo lo que
estorba»; el resto es deuda **de este repo**):

| # | Qué queda | Dónde | Qué pide el siguiente transporte |
|---|---|---|---|
| a | **La puerta de entrantes vive dentro del adaptador de whatsmeow** y recibe `*events.Message`. El transporte falso la reutiliza a través de `whatsmeow/sin_transporte.go` | `internal/adapters/whatsmeow/listener.go` (`handleEvent`, las cinco guardas) | Sacar las guardas, el meta y el encolado a un sitio neutral que reciba un entrante **sin tipos de la librería**; cada adaptador traduce su evento a ese tipo. Hasta entonces, un transporte nuevo tendría que fabricar `*events.Message` o reescribir las guardas, y lo segundo está descartado en el ADR-0041 |
| b | Los métodos de `sessionmgr.Escucha` heredan los nombres del gateway de whatsmeow (`SendViaLiveClientTracked`, …) | `internal/app/sessionmgr/transporte.go` | Renombrar a nombres neutros el día que haya un segundo transporte real |
| c | `types.ParseJID` antes de `cryptostore.DeleteDevice` | `internal/app/sessionmgr/pairing.go:181`, `unlink.go:262` | Un transporte sin device de whatsmeow no tiene nada que borrar ahí: hoy funciona porque `DeleteDevice` es idempotente (medido con el transporte falso), pero la limpieza debería pedírsela al transporte |
| d | `whatsmeow.Backoff` | `internal/app/sessionmgr/listen.go` | Ver A-1 |
| e | 9 ficheros de producción fuera de `internal/adapters/whatsmeow/` importan `go.mau.fi/*` (`cryptostore`, `edgemigrate`, `sessionmgr`) | `grep -rn 'go.mau.fi/' --include='*.go' . \| grep -v _test.go` | El almacén cifrado y la migración de arranque son de whatsmeow por naturaleza; solo estorban si el transporte por defecto deja de ser whatsmeow |

**Lo que un transporte distinto cambiaría de verdad** (para quien lo aborde; no está decidido):

- **Una librería en otro lenguaje** (p. ej. Baileys, que es Node) no se enlaza en el binario: sería un
  proceso auxiliar hablando por un socket local. Sin broker (ADR-0003), y sus credenciales tendrían
  que quedar bajo la DEK del cliente como hoy el almacén de whatsmeow.
- **La API oficial de WhatsApp** no tiene device ni QR, entrega por *webhook* (el daemon hoy no abre
  ningún puerto de red) y se autentica con un token de Meta, que es una **credencial**: zero-knowledge
  exige que viva en el cliente y nunca suba a la nube. El emparejamiento, la custodia y el modelo de
  «sesión» cambian, no solo el adaptador.

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

### A-6 · 🟡 El reintento del despachador espera 5 s, aunque el log anuncie 0 y después 500 ms

> Añadida el 2026-10-14: hallazgo del laboratorio de flujos (Plan 058 de la raíz, escalón E15). Sin
> corregir.

`esperaTrasFallo` (`internal/app/despachador/despachador.go:154`) se escribió para el poll de 500 ms
(`pollBase`, `:168`): 0 tras el primer fallo, 500 ms tras el segundo, y duplica hasta 30 s. Esa espera
solo pone la marca `desde` (`:411`), y el log la anuncia como `proximo_intento_en_ms` (`:456`). Pero
tras una vuelta sin progreso, `Run` (`:318`) se duerme en el despertador (`:339`), y en producción el
despertador es `AvisoConRespaldo` (cableado en `internal/app/sessionmgr/despacho.go:149`): despierta con
un `Enqueue` nuevo o al vencer el **respaldo de 5 s** (`DefaultRespaldoMS`,
`internal/app/despachador/despertador.go:137`). Sin mensajes nuevos, el reintento llega a los 5 s.

**Consecuencia.** Un tropiezo puntual del sumidero (CloudLink reconectando) retrasa el mensaje del
cliente 5 s en vez de reintentarlo al instante, y el log da una cifra que no es la real. Medido en E15
con `-sink-rejects 2`: la primera entrega llega a los **10,0 s** (dos rechazos por 5 s), con el log
diciendo `proximo_intento_en_ms=0` y después `=500`. El backoff solo manda cuando pasa de 5 s.

**Cómo se cierra.** Que tras un fallo el bucle espere `espera` con su propio temporizador (o que el
despertador acepte un «despiértame en X»), y no el respaldo. Si se prefiere no tocar el bucle, al menos
que el log anuncie la espera real. `freno_entrega_test.go` fija los valores de `esperaTrasFallo`, no lo
que tarda la re-entrega con el despertador de producción.

**Lo reproduce**: laboratorio E15 con `-sink-rejects 2` (Plan 058), PR #15, abierto, en revisión.

### A-7 · El candado de cableado de la cola lee todos los `.go`, sin mirar etiquetas de compilación

> Añadida el 2026-10-14 como **nota informativa**, no como defecto. Salió del laboratorio de flujos
> (Plan 058 de la raíz, escalón E15).

`fuentesDeProduccion` (`internal/infra/db/cola_cableado_ast_test.go:213`), que alimenta a
`TestColaSeAbreSiemprePorOpenCola`, recorre el repo entero, descarta solo los `_test.go` (`:233`) y
parsea cada fichero con `parser.ParseFile` (`:237`) sin evaluar `//go:build`. Para los ficheros por
sistema operativo (`_linux.go`, `_darwin.go`) eso es lo que se quiere: el candado los vigila aunque el
binario de la máquina no los compile.

**Consecuencia.** El directorio `laboratorio/` (código `//go:build laboratorio` que no entra en el
binario, PR #15) también tiene que cumplir la regla de producción: si un fichero del laboratorio escribe
la ruta de la cola como literal, `make test` se pone en rojo. Hoy no pasa: el laboratorio abre la cola
con los constructores de producción.

**Cómo se cierra.** No hace falta cerrarlo. Si algún día estorba, se excluye `laboratorio/` en el
recorrido, con un comentario que diga por qué. Mientras tanto, quien escriba en el laboratorio debe
saber que este candado lo lee.

---

## 4 · Interfaz local (la web embebida)

### U-1 · Pares mixtos: color que sigue al tema sobre fondo que no — ✅ CERRADO 2026-09-01

Eran dos reglas del Dashboard (`index.html`) con un fondo **literal** emparejado con un color de
texto que **sí** sigue al tema — el defecto recurrente del ecosistema (ver
`wapp-el-par-mixto-color-y-fondo` en la memoria del proyecto: «el par viaja entero, nunca uno de
cada»). **NO VERIFICADO en navegador** (Chrome no estaba disponible en la sesión); verificado en su
lugar calculando el ratio WCAG exacto con la misma fórmula del proyecto contra los valores reales de
`:root`/`@media (prefers-color-scheme: dark)` de este mismo fichero.

| Regla | Antes (claro/oscuro) | Fix | Después (claro/oscuro) |
|---|---|---|---|
| `.enroll-form input` (`internal/webui/styles.css`, campo del código de activación) | 17,13:1 / **1,29:1** | `background:#fff` → `background: var(--card)` | 17,13:1 / **13,27:1** |
| `.badge-unknown` (ídem) | 7,53:1 / **1,37:1** | `background:#e5e7eb` → `background: var(--wapp-color-surface-variant)` | 7,22:1 / **5,48:1** |

`--card`/`--ink` y `--wapp-color-surface-variant`/`--muted` son pares donde AMBOS tokens se
redefinen juntos en el bloque `@media` de este fichero, así que viajan entero. El segundo par es
literalmente el mismo que ya usa `.wapp-chip--neutral` en `wapp-shared/ui/css/wapp-components.css`
(«el único par que SÍ sigue al tema, y por eso es seguro» — mismos números, 7,22:1 / 5,48:1).

Sin verificación en navegador real, no se cierra el hallazgo como «medido en campo», solo como
«corregido por cálculo» — si alguien tiene Chrome a mano, vale la pena confirmar visualmente.

Aparte, sin tocar (no eran mixtos, así que no entraban en este arreglo): `.badge-ok` (3,00:1) y
`.badge-warn` (2,86:1) están por debajo de AA para texto normal en **los dos** temas por igual —
son bajos, no mixtos, y su fix es distinto (subir el contraste del token, no emparejarlo).

### U-2 · Cuatro clases usadas y no definidas — ✅ CERRADO 2026-09-01

`wapp-link`, `wapp-snackbar--success`, `wapp-btn--outlined` y `wapp-caption` se usaban en
`internal/webui/login.html` sin estar definidas en `styles.css` (el fork local de los tokens de
`wapp-shared/ui`). **Cierre:** el Edge pasó a **consumir el módulo `ui` como las otras tres
consolas** (`go.mod` declara `github.com/EduGoGroup/wapp-shared/ui v0.5.0`; `cmd/wapp-ctl/main.go`
sirve `/static/css/{wapp-tokens,wapp-components,theme-edge}.css` con `sharedui.GetCSS`, mismo
patrón que `wapp-guardian-bff/internal/web/server.go`), y `login.html` enlaza esas tres hojas antes
de `styles.css`. `.wapp-btn--outlined` y `.wapp-caption` no existían en `wapp-components.css`: se
añadieron ahí (release `ui/v0.5.0`), así que el cierre beneficia también a las otras consolas si
alguna vez las necesitan. `wapp-link` no necesitó clase propia: la regla base `a { color:
var(--wapp-color-link) }` de `wapp-components.css` ya cubre cualquier `<a>` sin marcar.

`styles.css` retiró el bloque de layout de autenticación que duplicaba a mano (`.wapp-card`,
`.wapp-btn`, `.wapp-field*`, `.wapp-snackbar*`, …): nadie más lo usaba (`index.html` tiene 0 clases
`wapp-*`). Los TOKENS (`:root` y su bloque `@media (prefers-color-scheme: dark)`) se quedaron: el
Dashboard los sigue necesitando vía sus alias de compatibilidad (`--bg`, `--card`, `--accent`, …).

### U-3 · El plano de control no emite ninguna cabecera de seguridad — ✅ CERRADO 2026-09-01

`grep -rn 'Content-Security-Policy' --glob '!*_test.go' .` daba **cero**; las otras tres consolas
del ecosistema sí la emiten. **Cierre:** `handleLoginGet` (`cmd/wapp-ctl/auth.go`) genera un nonce
de 16 bytes de `crypto/rand` por petición (`newNonce`, base64 `RawURLEncoding` — el estándar mete
`+`/`=` que `html/template` escapa como entidad en el atributo) y emite `Content-Security-Policy:
default-src 'self'; script-src 'self' 'nonce-…'; style-src 'self'; connect-src 'self';
frame-ancestors 'none'; base-uri 'none'`. El único `<script>` inline de `login.html` lleva
`nonce="{{ .Nonce }}"`. No se reutilizó `wapp-shared/web/gin` (nonce+CSP de las otras consolas):
está pensado para `gin`, y `wapp-ctl` es `net/http` puro — replicarlo aquí habría significado
adoptar su middleware entero para una sola cabecera. Vigilado por
`TestLoginGet_CSPConNonce` (`cmd/wapp-ctl/auth_test.go`): confirma que la CSP lleva un nonce no
vacío y que es EL MISMO que el del `<script>` del cuerpo — un nonce que no coincide no protege nada.

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
