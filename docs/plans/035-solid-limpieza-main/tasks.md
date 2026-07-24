# Plan 035 — Tasks (olas secuenciales)

> Cada ola cierra con `go build ./... && go vet ./... && make test` verdes.
> Publicación a `dev`/`main` solo con aprobación del usuario.

## Ola 0 — Poda de código muerto (D1 + D2 + D3)

> Riesgo BAJO: se elimina código que no tiene callers en producción. Los tests verdes confirman que nada vivo
> dependía de lo eliminado.

- [x] **T0.1** Extraer `ErrSessionNotFound` de `internal/app/restore.go` a `internal/app/errors.go`
      (nuevo archivo). Verificar que todos los consumidores (`sessionstore`, `sessionmgr`) importan desde
      `app.ErrSessionNotFound` sin cambio de path. Build verde.
- [x] **T0.2** Eliminar `internal/app/restore.go` + `internal/app/restore_test.go` (caso de uso
      `RestoreSessions`, interfaces `PairedDeviceLocator`/`SessionRunner`, tipos `RestoreOption`/
      `withClock`, errores `ErrNoSessions`/`ErrSessionLoggedOut`). Build verde.
- [x] **T0.3** Eliminar de `cmd/agent/main.go`: funciones `runPair`, `runSend`, `runRestore`,
      `newEscucha` + los bloques `if` de despacho en `main()` para los subcomandos `pair`, `send`,
      `listen`/`restore`. Eliminar imports que queden sin usar. Build verde.
- [x] **T0.4** Eliminar `wiring.BuildSink` de `internal/infra/wiring/cloudlink.go` (L50-131): es el
      camino single-sesión consumido solo por `newEscucha` (eliminada en T0.3). Conservar `BuildMux`,
      `BuildOutbox`, `dialCloudLink` y helpers compartidos. Build verde.
- [x] **T0.5** Actualizar `internal/app/doc.go`: quitar `RestoreSessions` de la lista de casos de uso
      previstos (ya no existe). Quitar `PairedDeviceLocator` de los puertos mencionados si aparece.
- [x] **T0.6** `make test` verde (todos los tests existentes pasan; tests eliminados en T0.2 ya no corren).

## Ola 1 — Extraer adaptadores inline (D6)

> Riesgo BAJO: se mueve código de `main.go` a paquetes propios sin cambiar lógica.

- [x] **T1.1** Crear `internal/adapters/control/inventory/adapter.go`: mover `managerInventory` →
      `inventory.ManagerAdapter` con constructor `New(mgr)`. Implementa `server.SessionLister`.
      Tests unitarios: `Persisted` delega, `Health` traduce.
- [x] **T1.2** Crear `internal/adapters/control/enrolladapter/adapter.go`: mover `enrollPort` →
      `enrolladapter.Adapter` + `mtlsFileExists` como helper interno. Constructor `New(cfg, log)`.
      Implementa el contrato de `server.RegisterEnroll`. Tests: `Enrolled` detecta cert, `Enroll`
      valida precondiciones.
- [x] **T1.3** Actualizar `cmd/agent/main.go` (o `daemon.go` si Ola 2 ya ejecutó): reemplazar
      `managerInventory{mgr}` por `inventory.New(mgr)` y `enrollPort{cfg, log}` por
      `enrolladapter.New(cfg, log)`. Eliminar los tipos inline. Build + tests verdes.

## Ola 2 — Extraer daemon (D5)

> Riesgo MEDIO: refactor grande de movimiento de código. Cero cambio de conducta.

- [x] **T2.1** Crear `internal/infra/daemon/daemon.go`: tipo `Daemon` + constructor `New` +
      método `Run(ctx)`. Migrar el cuerpo de `runServe` de `main.go` al método `Run`. `Run` encapsula:
      apertura BD única, migración, restauración archivada, construcción de subsistemas (sessions,
      layout, outbox, intentStack, edgeCfgSvc, authKeyStore, healthReg, healthCollector, diagBuilder,
      mux, authMgr, manager, server), restauración de sesiones, loop de cierre y shutdown ordenado.
- [x] **T2.2** Actualizar `cmd/agent/main.go`: el case `serve` instancia `daemon.New(...)` y llama
      `d.Run(ctx)`. Eliminar `runServe` de `main.go`. Build verde.
- [x] **T2.3** Confirmar que `main.go` quedó ≤120 líneas con solo: bootstrap (config, logger,
      MkdirAll, migraciones), despacho switch {serve, enroll, default} y `runEnroll`. Si no, ajustar.
- [x] **T2.4** `make test` verde. Verificar manualmente `agent serve` con config existente.

## Ola 3 — Desacoplar DIP en sessionmgr (D7)

> Riesgo BAJO: cambio interno del Manager, API pública estable por el patrón Option.

- [x] **T3.1** Añadir campo `newCustody func(dekPath string) app.KeyCustody` al `Manager` en
      `internal/app/sessionmgr/manager.go`. Crear opción `WithKeyCustodyFactory(fn)`.
- [x] **T3.2** Reemplazar los usos de `keycustody.NewFileCustody(path)` dentro de `sessionmgr` por
      `m.newCustody(path)`. Eliminar el import de `internal/adapters/keycustody` de `manager.go`.
      Build verde.
- [x] **T3.3** En `daemon.go` (Ola 2) inyectar la factory:
      `sessionmgr.WithKeyCustodyFactory(func(p string) app.KeyCustody { return keycustody.NewFileCustody(p) })`.
      Build + tests verdes.
- [x] **T3.4** Verificar que el paquete `internal/app/sessionmgr` ya NO importa nada de `internal/adapters/`.
      `go list -m -json ./internal/app/sessionmgr/...` no debe listar adaptadores.

## Ola 4 — Documentación y cierre

- [x] **T4.1** Actualizar `CLAUDE.md` del Edge: reflejar la nueva estructura (`daemon/`, adaptadores
      extraídos, eliminación de subcomandos legacy).
- [x] **T4.2** Actualizar `README.md` del Edge: quitar las secciones que documenten `agent pair`,
      `agent send`, `agent listen`/`restore` como subcomandos disponibles.
- [x] **T4.3** Nota en `docs/journal/` con el resumen del refactor.
- [x] **T4.4** Marcar este plan como **cerrado** en el README.md del plan.
