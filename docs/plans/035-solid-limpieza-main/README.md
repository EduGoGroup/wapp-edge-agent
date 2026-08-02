---
plan: 035 — Limpieza SOLID del Edge Agent (main.go + código muerto)
estado: CERRADO/PUBLICADO — 5/5 olas implementadas y verificadas con go test -race (100% verde)
fase: transversal (calidad / deuda técnica / higiene alpha)
implementado: 100% (5/5 olas)
actualizado: 2026-07-24
---

# Plan 035 — Limpieza SOLID del Edge Agent

**Estado:** CERRADO / PUBLICADO (2026-07-24; 5/5 olas ejecutadas, tests -race en verde).

**Contexto:** una auditoría SOLID del Edge Agent (`edge/wapp-edge-agent`) reveló que
`cmd/agent/main.go` (606 líneas) concentra responsabilidades que violan SRP y OCP, y mantiene
**código muerto** de los subcomandos legacy del spike (`pair`, `send`, `listen`/`restore`) que ya NO
se usan en producción (el daemon real es `serve`). Con el proyecto en **alpha**, la regla es clara:
**código que no se usa se elimina** — no debe existir legacy.

**Alcance:** SOLO la pieza 01 (Edge Agent). Sin dependencias externas nuevas. Sin cambio de conducta.

## Hallazgos de la auditoría

| ID | Principio violado | Severidad | Descripción |
|---|---|---|---|
| H1 | SRP | ALTA | `main.go` mezcla 7 responsabilidades: routing CLI, cableado de 5 subcomandos, adaptadores inline |
| H2 | OCP | ALTA | `runServe` (170 líneas) crece con cada Plan (007→033); cada feature nuevo lo modifica |
| H3 | SRP | ALTA | Subcomandos `pair`/`send`/`listen`/`restore` son **código muerto** (no se usan en prod, `serve` los subsume) |
| H4 | SRP | MEDIA | Adaptadores inline (`managerInventory`, `enrollPort`) viven en `main.go` |
| H5 | DIP | MEDIA | `app/sessionmgr` importa `adapters/keycustody` (adaptador concreto, rompe la inversión de dependencias) |
| H6 | SRP | MEDIA | `app/restore.go` / `RestoreSessions` es el caso de uso SINGLE-SESIÓN supersedido por `sessionmgr.Manager.Restore` |

## Decisiones

| # | Decisión | Motivo |
|---|---|---|
| D1 | Eliminar subcomandos legacy (`pair`, `send`, `listen`/`restore`) | Código muerto; `serve` es el daemon real; el pairing va por POST /v1/sessions/pair, el envío va por CloudLink |
| D2 | Eliminar `newEscucha` y `wiring.BuildSink` | Solo los consume el camino legacy eliminado en D1 |
| D3 | Eliminar `app.RestoreSessions` + `app.PairedDeviceLocator` + `app.SessionRunner` + `app/restore_test.go` | Caso de uso supersedido por `sessionmgr.Manager.Restore` (declarado en el propio docstring) |
| D4 | Despacho CLI: se mantiene manual (sin cobra), **deuda técnica** para evaluar más adelante | No agregar dependencias externas; con solo `serve` + `enroll` la cadena de if es trivial |
| D5 | Extraer `runServe` a un paquete `daemon` (SRP+OCP) | La función más problemática: 170 líneas, crece con cada feature |
| D6 | Mover adaptadores inline a paquetes propios en `adapters/` | SRP: cada adaptador con sus tests, `main.go` solo cablea |
| D7 | Desacoplar `sessionmgr` de `keycustody` concreto (DIP) | La capa `app` no debe importar adaptadores concretos |

## Verificación

- `go build ./...` + `go vet ./...` verdes.
- `make test` (todos los tests existentes pasan; los tests legacy eliminados en D3 se retiran).
- `agent serve` arranca y opera igual.
- `agent enroll` sigue funcionando (único subcomando CLI que se conserva).
