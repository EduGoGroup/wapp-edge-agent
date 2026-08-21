// aviso.go — literal canónico del aviso de sesión pasiva (Plan 046 · T3.2, mitad (a)).
//
// 🔒 FUENTE ÚNICA EN ESTE REPO. El texto es CONTRATO: vive en
// docs/runbooks/perfiles-de-sesion.md §4 (repo raíz wApp, que NO viaja con este repo git) y aquí
// se replica carácter a carácter. La pantalla de éxito del emparejamiento (index.html/app.js) NO
// teclea este texto: lo pide a GET /v1/ui/aviso-sesion-pasiva (cmd/wapp-ctl), que sirve esta
// constante. Así el literal existe UNA vez en el repo y la UI no puede divergir de él.
//
// Reglas del contrato (§4 del runbook):
//   - Texto plano, sin marcado: ni Markdown ni HTML. El estilo lo pone la pantalla por CSS,
//     sin tocar el texto (white-space: pre-wrap en .passive-notice).
//   - Cero PII: no nombra jid, self_pn ni números de terceros.
//   - Las MAYÚSCULAS son el énfasis; no se convierten en negritas.
//   - Si el texto cambia, cambia con su versión (V1 → V2) y se actualizan los dos canales y sus
//     tests en el mismo commit. aviso_test.go compara contra el runbook, no contra una copia.
package webui

// AvisoSesionPasivaID identifica la versión vigente del literal (§4 del runbook).
const AvisoSesionPasivaID = "AVISO_SESION_PASIVA_V1"

// AvisoSesionPasiva es el literal canónico, byte a byte igual al bloque ```text del §4.
// NO editar aquí: se edita el runbook (con subida de versión) y se copia.
const AvisoSesionPasiva = `Tu WhatsApp quedó vinculado a wApp, y esta sesión nació en perfil PASIVA.

Qué significa: por esta sesión SOLO SE ENVÍAN mensajes. Lo que te escriban NO SALE
DE ESTE EQUIPO: se queda aquí y no sube a la nube, así que wApp todavía no responde
solo.

Para que responda, cambia el perfil de la sesión a ACTIVA desde el panel de wApp, o
llama a POST /api/v1/sessions/{id}/profile con {"profile":"active"}.`
