# `wapp-edge-agent` — el núcleo del ecosistema wApp

**Qué es.** Un daemon Go que corre **en el equipo del cliente** y mantiene abierto **24/7** el socket
de WhatsApp (`whatsmeow`), enviando y recibiendo. Cifra todo lo que guarda en reposo con una clave
(la **DEK**) que **nunca sale de ese equipo**, y obedece las órdenes que la plataforma cloud le manda
por un único stream gRPC bidireccional con mTLS. Junto al daemon viven un **supervisor local con web
embebida** (`wapp-ctl`) y un **worker de inferencia** (`agent cajero`).

**Para qué existe.** Es la única forma de tener recepción y envío permanentes **sin que la nube toque
credenciales de WhatsApp**: el número se empareja aquí, el store de `whatsmeow` se cifra aquí y la
llave se custodia aquí. El papel de esta pieza es el de **despachador**: la nube arma el mensaje
completo y el Edge lo pone en el cable; el Edge **no decide flujos ni llama endpoints de negocio**.

**Qué NO es.** No es un servidor de negocio, no interpreta lo que el cliente final escribe, no guarda
el catálogo ni los pedidos, y no tiene broker: la concurrencia se resuelve con goroutines y una
`outbox` en SQLite.

---

## Los documentos

| Documento | Qué contesta |
|---|---|
| [`constitucion.md`](constitucion.md) | **Empieza aquí.** Los invariantes que no se pueden violar, la tecnología real con sus versiones, las convenciones y —sobre todo— **las trampas** que un agente pisa aquí si nadie se lo dice. |
| [`arquitectura.md`](arquitectura.md) | Cómo está hecha por dentro: capas, los 44 paquetes, los tres binarios y qué produce cada punto de entrada, con diagramas del proceso y del camino de un mensaje entrante. |
| [`contratos.md`](contratos.md) | Todo lo que otros consumen: las rutas HTTP del núcleo y las de `wapp-ctl` (con la regla de conteo), los frames gRPC, el socket del cajero, los subcomandos y flags, las variables de entorno con su valor por defecto, los ficheros que escribe y las 13 tablas SQLite. |
| [`operacion.md`](operacion.md) | Cómo se arranca en local, cómo se prueba (los `make` reales y qué valida cada uno), cómo se publica una versión y cómo se depura cuando falla. |
| [`deuda.md`](deuda.md) | La deuda viva con `fichero:línea`: el código muerto verificado, los fail-open de configuración, las violaciones de capa y los hallazgos de seguridad. |
| [`literal-aviso-sesion-pasiva.md`](literal-aviso-sesion-pasiva.md) | 🔒 **Contrato ejecutable, no documentación.** Es la fuente única del literal `AVISO_SESION_PASIVA_V1` y un test la lee en cada `make test`. Editarla sin propagar a la constante Go pone el gate en rojo. |

---

## Lo mínimo que hay que saber antes de tocar nada

1. **La DEK no cruza el contrato.** Ni al proto, ni a un log, ni a un campo «por si acaso».
2. **El lease lo emite y lo revoca el servidor**: es el kill-switch anti-clon, y el Edge solo lo
   valida. Sin la clave pública configurada, ese gate **no se construye** — no es que falle abierto
   con un aviso: es que no existe.
3. **No hay tabla de versión de esquema.** El runner re-ejecuta todo el DDL en cada arranque, así
   que **una columna nueva no se añade editando un `CREATE TABLE`**: se añade con un `ensure…` en Go.
   Es la trampa número uno de este repo y está explicada en [`constitucion.md`](constitucion.md).
4. **Un PR no valida nada**: `.github/workflows/ci.yml` es `workflow_dispatch`. El gate real es
   `make ci-local` en tu máquina, y hay que **contar los SKIP** ([`operacion.md`](operacion.md)).
5. Este repo se clona **solo**. Nada de lo que escribas aquí debe enlazar al repo de documentación
   del ecosistema: si necesitas citarlo, cítalo en texto.
