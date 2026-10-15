# `laboratorio/` — el laboratorio de flujos del Edge

Programas para **estudiar una pieza** de `wapp-edge-agent` por dentro: la pieza bajo estudio es
**real**, lo que la rodea son **dobles**, y cada programa cuenta por pantalla qué pasa y por qué. Es el
Plan 058 del ecosistema (su gemelo del cloud vive en `wapp-cloud-platform/laboratorio/`).

## Por qué CI no lo ve

🔒 **No es un test de integración.** No corre solo y dobla lo que en el producto no se puede doblar:
la custodia de la DEK (un fichero en lugar del Keychain), el authorizer del plano de control (no hay)
y CloudLink con su lease (un sumidero en memoria). Por eso **todo `.go` de aquí empieza por
`//go:build laboratorio`**, y sin esa etiqueta no lo compila nadie: ni `go build ./...`, ni
`go vet ./...`, ni `go test ./...`, ni `make ci-local`, ni `make ci-docker`, ni golangci-lint.
`go list ./...` sigue dando **44 paquetes**, sin ninguno de `laboratorio/`.

🔒 **Tampoco entra en ningún binario.** `agent`, `wapp-ctl` y `colaseed` no lo importan, y el
`Makefile` solo empaqueta `CMDS := agent wapp-ctl`. Es lo que exige el ADR-0049: los dobles viven
**fuera** del binario; dentro no hay lease «de pruebas» ni un segundo camino de custodia.

⚠️ **Un guardarraíl sí lo lee**: `internal/infra/db/cola_cableado_ast_test.go` parsea **todos** los
`.go` del repo sin mirar etiquetas. El laboratorio abre la cola como el daemon (`db.OpenCola` con
`layout.ColaDB()`), no escribe el nombre del fichero de la cola y no convierte su ruta a `string`
(para leer los bytes crudos le pregunta la ruta a la conexión con `PRAGMA database_list`).

⚠️ El precio: un cambio del código puede romper un escalón sin que nadie se entere. **Lo primero de
cada sesión** es compilarlo.

## Cómo se corre

Siempre desde la raíz del repo, con la toolchain del `Makefile` y contra los tags publicados:

```bash
GOTOOLCHAIN=go1.26.5 GOWORK=off go build -tags laboratorio ./laboratorio/...
GOTOOLCHAIN=go1.26.5 GOWORK=off go run -tags laboratorio ./laboratorio/e15-edge-inside
```

## Qué hay

🔤 El **código** va en inglés, como el resto del repo; los comentarios y lo que imprime el narrador,
en español. Solo el directorio `laboratorio/` y la etiqueta `laboratorio` se quedan en español.

| Ruta | Qué es |
|---|---|
| `common/` | El `Narrator` (copia-adaptación del andamio del cloud, que es otro módulo y no se importa), con un contador de comprobaciones: el escalón sale con `rc=1` si alguna falla |
| `e15-edge-inside/` | **E15 · El Edge por dentro, sin nube.** WhatsApp falso → puerta de entrantes → cola cifrada → despachador → sumidero en memoria, y la vuelta: lo enviado se captura y se acusa a los ~50 ms |

### `e15-edge-inside`

Monta el `daemon.Run` **recortado**, con los constructores de producción
(`wiring.TransporteFalso`, `wiring.BuildCola`, `wiring.BuildColaDespachador`, `sessionmgr.NewManager`,
`server.New`), y hace de `wapp-ctl` por el socket unix del plano de control.

| Bandera | Defecto | Qué hace |
|---|---|---|
| `-dir` | `/tmp/wlab-e15` | Datos del Edge. **Se borra al empezar.** Corto a propósito: la ruta de un socket unix no pasa de 104 caracteres en macOS |
| `-n` | `4` | Cuántos mensajes manda el cliente inventado |
| `-sink-rejects` | `0` | Cuántas entregas rechaza el sumidero antes de aceptar: el despachador reintenta la **misma** cabeza |
| `-v` | apagado | El log del Edge también por stderr (siempre va a `<dir>/edge.log`) |

## Dónde está la guía

Las fichas del alumno viven en la documentación de la raíz del ecosistema wApp, en
`documentations/operacion/laboratorio/` (la de este escalón es `e15-el-edge-por-dentro.md`), y el plan
en `documentations/planes/abiertos/058-laboratorio-de-flujos/`. Este repo solo guarda el código.
