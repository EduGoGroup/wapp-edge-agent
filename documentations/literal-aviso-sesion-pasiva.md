# Literal canónico · `AVISO_SESION_PASIVA_V1`

🔒 **FUENTE ÚNICA DE ESTE REPO.** Este fichero es el original contra el que compara el test del
paquete que emite el aviso. No es un ejemplo ni una copia de cortesía: si diverge de la constante
Go, el test se pone rojo.

## Qué es

El texto que recibe el dueño de un número recién emparejado cuando su sesión nace en perfil
**pasiva**. Lo emiten dos canales —la pantalla de éxito del plano de control del Edge y el mensaje
de WhatsApp que la nube envía al propio número— y **usan el mismo literal, carácter a carácter**.

**ID del literal:** `AVISO_SESION_PASIVA_V1`

```text
Tu WhatsApp quedó vinculado a wApp, y esta sesión nació en perfil PASIVA.

Qué significa: por esta sesión SOLO SE ENVÍAN mensajes. Lo que te escriban NO SALE
DE ESTE EQUIPO: se queda aquí y no sube a la nube, así que wApp todavía no responde
solo.

Para que responda, cambia el perfil de la sesión a ACTIVA desde el panel de wApp, o
llama a POST /api/v1/sessions/{id}/profile con {"profile":"active"}.
```

## Las tres cosas que el texto tiene que decir, y las únicas

1. la sesión **nació pasiva**;
2. qué significa: **solo envía**, y sus entrantes **no salen de este equipo**;
3. **cómo cambiarla**: el panel de wApp o `POST /api/v1/sessions/{id}/profile`.

## Reglas de uso, que son parte del contrato

- **Texto plano, sin marcado.** Ni Markdown ni HTML: WhatsApp y la pantalla web tienen sintaxis
  distintas, así que el estilo lo pone cada canal y el literal viaja desnudo. Un `*` aquí sería un
  asterisco literal en la pantalla y una negrita en WhatsApp: dos textos distintos.
- **Cero PII.** No nombra al cliente final, ni un número de tercero, ni el propio `self_pn`. El
  destinatario ya sabe qué teléfono es: lo tiene en la mano.
- **Las mayúsculas hacen el trabajo de la negrita.** Son el único énfasis que sobrevive igual en
  los dos canales.
- **Si este literal cambia, cambia con su versión**: `AVISO_SESION_PASIVA_V1` → `_V2`, y se
  actualizan **los dos canales** y su test de render en el mismo commit.

## Por qué hay una copia en cada repo

Este texto vive por duplicado, una vez en el Edge y otra en la nube, porque cada repo se clona y
se compila solo y su test tiene que poder correr sin nada al lado. Antes vivía únicamente en el
repo de documentación, y el resultado era que en un checkout suelto el test **se saltaba en
silencio** (`t.Skip`), dejando el invariante sin vigilar justo donde más falta hacía.

Lo que esa copia cuesta —que las dos versiones diverjan entre sí— lo cubre
`scripts/check-literales-canonicos.py` del repo de documentación, que compara todas las copias
del ecosistema. Ese script solo corre en el árbol completo; el test de este repo corre siempre.
