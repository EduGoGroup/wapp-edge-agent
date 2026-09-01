# CLAUDE.md — `wapp-edge-agent`

> **Este fichero solo apunta. La verdad vive en [`documentations/`](documentations/README.md).**
> Si algo de aquí contradice a `documentations/`, gana `documentations/`.
> (El `CLAUDE.md` anterior a 2026-08-30 contenía afirmaciones falsas: decía «dos binarios», «un solo
> `.db`», listaba entidades de dominio inexistentes y no mencionaba el cajero. Fue reescrito entero.)

## Qué es esta pieza

El **núcleo** del ecosistema wApp: un daemon Go que corre **en el equipo del cliente** y mantiene
abierto **24/7** el socket de WhatsApp (`whatsmeow`), enviando y recibiendo. Cifra en reposo con una
clave que nunca sale de ese equipo y obedece a la plataforma cloud por un stream gRPC bidi con mTLS.

**Tres binarios**: `agent` (subcomandos `enroll` / `serve` / `cajero`), `wapp-ctl` (supervisor con web
embebida en `127.0.0.1:8105`) y `colaseed` (carga sintética, **no se empaqueta**).
En campo corren **tres procesos**: `wapp-ctl` y sus dos hijos, `agent serve` y `agent cajero`.

## Las cinco reglas innegociables

1. **Zero-knowledge.** La nube nunca ve credenciales ni llaves privadas. Protege **llaves**, no el
   contenido de negocio, que sí sube a la nube a propósito.
2. **Doble llave.** La **DEK** (descifra el store de `whatsmeow`) la custodia el cliente y **jamás
   cruza el contrato**. El **Lease** (autoriza a operar) lo emite y lo revoca el servidor: es el
   kill-switch anti-clon. Para despachar hacen falta **las dos**.
3. **Sin Redis ni broker en el Edge.** La concurrencia se resuelve con goroutines; la durabilidad,
   con la tabla `outbox` en SQLite.
4. **Copia-adaptación, nunca dependencia.** Parte del código se copió de otro producto (EduGo) y se
   adaptó al espacio de nombres de wApp. **Prohibido importar un repo `edugo-*`.**
5. **El código compartido interno vive en `wapp-shared`**, monorepo multi-módulo con releases por
   módulo (tags `<modulo>/vX.Y.Z`). Se verifica contra el **tag publicado**, no contra el árbol de
   al lado: `GOWORK=off go build ./...`.

## Las tres trampas que más cuestan aquí

- **No hay tabla de versión de esquema.** El runner re-ejecuta todo el DDL en cada arranque: una
  columna nueva **no** se añade editando un `CREATE TABLE`, se añade con un `ensure…` en Go (hay
  cuatro). Un `ALTER` en un `.sql` arranca bien la primera vez y mata la segunda.
- **Dos prefijos de entorno, dos procesos.** El daemon lee `WAPP_AGENT_*`; el cajero, `WAPP_WORKER_*`.
  Una variable del cajero con el prefijo del daemon **no la lee nadie**, y no hay aviso.
- **Un PR no valida nada** (`ci.yml` es `workflow_dispatch`). El gate es `make ci-local` en local, y
  hay que **contar los SKIP**: un `rc=0` cuenta igual un `--- SKIP` que un `--- PASS`.

## El índice

| Documento | Para qué |
|---|---|
| [`documentations/README.md`](documentations/README.md) | Portal de la pieza |
| [`documentations/constitucion.md`](documentations/constitucion.md) | **Empieza aquí**: invariantes, tecnología, convenciones y las 12 trampas conocidas |
| [`documentations/arquitectura.md`](documentations/arquitectura.md) | Capas, los 44 paquetes, los tres binarios y los diagramas |
| [`documentations/contratos.md`](documentations/contratos.md) | Rutas HTTP (con la regla de conteo), frames gRPC, CLI, variables de entorno, ficheros y tablas |
| [`documentations/operacion.md`](documentations/operacion.md) | Arrancar, probar, publicar y depurar |
| [`documentations/instalador.md`](documentations/instalador.md) | El instalador para el usuario final: SO soportados, permisos y pasos post-instalación |
| [`documentations/deuda.md`](documentations/deuda.md) | Deuda viva con `fichero:línea`, incluida la de seguridad |
| [`documentations/literal-aviso-sesion-pasiva.md`](documentations/literal-aviso-sesion-pasiva.md) | 🔒 Contrato ejecutable: un test lo lee en cada `make test`. **No lo edites sin propagar a la constante Go.** |

## Antes de escribir código

Trabaja **dentro de este repo**. Los comentarios de este código son **normativos**: muchos no
describen, prohíben — y explican qué se rompió cuando alguien lo intentó. Léelos antes de simplificar.
