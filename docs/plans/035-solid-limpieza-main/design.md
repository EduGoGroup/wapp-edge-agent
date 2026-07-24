# Plan 035 — Design

> Anclajes de campo verificados contra `cmd/agent/main.go` y `internal/` del HEAD actual.

## Arquitectura actual vs. propuesta

### Antes (main.go — 606 líneas, 7 responsabilidades)

```
cmd/agent/main.go
├── main()                    ← bootstrap + config + migración + routing CLI (5 subcomandos)
├── runPair()                 ← MUERTO: el pairing va por POST /v1/sessions/pair
├── runSend()                 ← MUERTO: el envío va por CloudLink (comando cloud→edge)
├── runRestore() + newEscucha() ← MUERTO: supersedido por sessionmgr.Manager.Restore
├── runServe()                ← 170 líneas: construye TODO el daemon
├── runEnroll()               ← VIVO: subcomando CLI de enrolamiento
├── managerInventory{}        ← adaptador inline: *Manager → SessionLister
├── enrollPort{}              ← adaptador inline: config → server.Enroller
└── mtlsFileExists()          ← helper del enrollPort
```

### Después (main.go — ~100 líneas, 2 responsabilidades: bootstrap + routing)

```
cmd/agent/main.go
├── main()                    ← bootstrap + config + migración + despacho {serve, enroll}
└── runEnroll()               ← subcomando CLI vivo

internal/infra/daemon/
└── daemon.go                 ← Daemon.Run(): ciclo de vida del daemon multi-sesión

internal/adapters/control/
├── inventory/adapter.go      ← ManagerAdapter (antes managerInventory)
└── enrolladapter/adapter.go  ← Adapter (antes enrollPort)

--- ELIMINADOS ---
cmd/agent/main.go: runPair, runSend, runRestore, newEscucha, managerInventory, enrollPort
internal/app/restore.go + restore_test.go
internal/infra/wiring/cloudlink.go: BuildSink (solo usada por camino legacy)
```

## Detalle por cambio

### D1 · Eliminar subcomandos muertos de main.go

**Archivos:** `cmd/agent/main.go`

**Qué se elimina:**
- `runPair` (L183-236): abre BD, construye adaptadores, ejecuta `app.Pair`, registra sesión.
  Ya reemplazado por `POST /v1/sessions/pair` → `sessionmgr.Manager.Pair` (Plan 008).
- `runSend` (L242-261): abre BD, construye adaptadores, ejecuta `app.Send`.
  Ya reemplazado por el despacho CloudLink `SendText`/`SendMedia` (Plan 006+013).
- `runRestore` (L270-297) + `newEscucha` (L307-328): restauración single-sesión.
  Ya reemplazado por `sessionmgr.Manager.Restore` (Plan 008 T4).
- Los bloques `if` correspondientes en `main()` (L112-150).
- Imports que queden sin usar (`waconn`, `control`, `sessionstore`, `uuid`, etc.).

**Qué se conserva:**
- `runEnroll` (L585-605): es el único subcomando CLI funcional aún necesario (bootstrap sin web).
- `runServe` (se mueve a `daemon`, ver D5).

**Impacto en `main()` post-limpieza:** pasa de ~55 líneas de despacho a ~15:

```go
func main() {
    // ... config, logger, MkdirAll, migraciones (sin cambio)

    sub := ""
    if len(os.Args) > 1 { sub = os.Args[1] }
    switch sub {
    case "serve":
        // ... sink + ctx + daemon.New(cfg, log, sink).Run(ctx)
    case "enroll":
        // ... runEnroll(ctx, cfg, log)
    default:
        log.Info("wapp-edge-agent arrancando", ...)
    }
}
```

### D2 · Eliminar `wiring.BuildSink` (camino single-sesión)

**Archivo:** `internal/infra/wiring/cloudlink.go`

`BuildSink` (L50-131) es la construcción del sink CloudLink para el camino single-sesión (legacy).
El daemon multi-sesión usa `BuildMux` (L146). Se elimina `BuildSink` y su docstring.

`dialCloudLink`, `BuildMux`, `BuildOutbox`, `clientCreds`, `loadValidatorFactory`,
`loadCloudEncPubKey`, `cloudLinkDialOpts` se **conservan** (los usa `BuildMux`).

### D3 · Eliminar `app.RestoreSessions` y puertos huérfanos

**Archivos eliminados:**
- `internal/app/restore.go` — caso de uso supersedido (declarado en su propio docstring L104-111)
- `internal/app/restore_test.go` — tests del caso eliminado

**Puertos que se eliminan de `restore.go`** (solo los consume `RestoreSessions`):
- `PairedDeviceLocator` (interfaz, L73-81)
- `SessionRunner` (interfaz, L83-90)
- `RestoreOption` + `withClock`
- `ErrNoSessions`, `ErrSessionLoggedOut`, `ErrSessionNotFound` — verificar si `ErrSessionNotFound`
  se usa en otro lugar (lo define aquí pero lo referencia `sessionstore`; **moverlo a `app/` como
  constante standalone** si tiene consumidores fuera de `restore.go`).

> [!IMPORTANT]
> `ErrSessionNotFound` se referencia en `sessionstore` y potencialmente en `sessionmgr`. Antes de
> eliminar `restore.go`, extraer `ErrSessionNotFound` a un archivo propio en `app/` (p.ej.
> `app/errors.go`) para no romper consumidores.

### D4 · Despacho CLI manual — deuda técnica aceptada

El despacho se mantiene como `switch os.Args[1]` manual. Con solo 2 subcomandos (`serve`, `enroll`)
es trivial y no justifica una dependencia como `cobra`. Se registra como deuda técnica para evaluar
si la CLI crece.

### D5 · Extraer `runServe` → paquete `daemon`

**Archivo nuevo:** `internal/infra/daemon/daemon.go`

```go
// Package daemon orquesta el arranque unificado del Edge Agent: BD única, outbox,
// clasificador de intenciones, auth de operador, session manager, servidor /v1 y
// shutdown ordenado. Encapsula la construcción de runServe (Plan 008+007+022+027+029+031+033)
// para que main.go solo instancie y ejecute.
package daemon

type Daemon struct {
    cfg        config.Config
    log        sharedlogger.Logger
    sink       *logsink.Sink
    version    string
    startedAt  time.Time
}

func New(cfg config.Config, log sharedlogger.Logger, sink *logsink.Sink, version string, startedAt time.Time) *Daemon

// Run ejecuta el daemon completo y bloquea hasta ctx.Done() o fallo del servidor /v1.
// Migra la BD, construye subsistemas, restaura sesiones, sirve /v1 y ejecuta shutdown ordenado.
func (d *Daemon) Run(ctx context.Context) error
```

**Qué migra de `runServe`:**
1. Apertura y migración de la BD única (L354-375)
2. Restauración de sesiones archivadas (L372-375)
3. Construcción de: sessions, layout, outbox, intentStack, edgeCfgSvc, authKeyStore, healthReg,
   healthCollector, diagBuilder, mux/authRelay, authMgr (L377-427)
4. Construcción del Manager con todas las opciones (L433-443)
5. Restore del Manager (L445-447)
6. Construcción y arranque del Server /v1 con todos los endpoints (L451-487)
7. Loop select de cierre + shutdown ordenado (L493-512)

**main.go `serve` case queda reducido a:**
```go
case "serve":
    sink := logsink.New(0)
    serveLog := logger.NewWithSink(cfg, sink)
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()
    if err := daemon.New(cfg, serveLog, sink, Version, processStartedAt).Run(ctx); err != nil {
        serveLog.Error("daemon multi-sesión fallido", "error", err)
        os.Exit(1)
    }
```

### D6 · Mover adaptadores inline a paquetes propios

**`managerInventory`** → `internal/adapters/control/inventory/adapter.go`

```go
// Package inventory adapta *sessionmgr.Manager al puerto de lectura del plano de control
// (server.SessionLister): combina el inventario persistido con la salud de runtime por sesión.
package inventory

type ManagerAdapter struct { mgr *sessionmgr.Manager }
func New(mgr *sessionmgr.Manager) ManagerAdapter
func (m ManagerAdapter) Persisted(ctx context.Context) ([]domain.Session, error)
func (m ManagerAdapter) Health(id string) (string, bool)
```

**`enrollPort`** → `internal/adapters/control/enrolladapter/adapter.go`

```go
// Package enrolladapter adapta el enroll real al puerto del plano de control (server.RegisterEnroll).
package enrolladapter

type Adapter struct { cfg config.Config; log sharedlogger.Logger }
func New(cfg config.Config, log sharedlogger.Logger) Adapter
func (a Adapter) Enrolled() bool
func (a Adapter) Enroll(ctx context.Context, activationCode string) error
```

`mtlsFileExists` se mueve con `enrolladapter` (es un helper interno del adaptador).

### D7 · Desacoplar `sessionmgr` de `keycustody` concreto

**Archivo:** `internal/app/sessionmgr/manager.go`

**Problema:** L9 importa `internal/adapters/keycustody` — la capa `app` depende de un adaptador.

**Solución:** inyectar una factory de custodia como función (patrón ya usado en `newPairer`/`newListener`):

```go
// En manager.go: eliminar import "adapters/keycustody"
// Añadir campo:
newCustody func(dekPath string) app.KeyCustody

// En la opción existente WithSharedDB o en una nueva:
func WithKeyCustodyFactory(fn func(string) app.KeyCustody) Option {
    return func(m *Manager) { m.newCustody = fn }
}
```

**En `cmd/agent/main.go` (o `daemon.go`):**
```go
sessionmgr.WithKeyCustodyFactory(func(path string) app.KeyCustody {
    return keycustody.NewFileCustody(path)
}),
```

## Restricciones

- **CERO cambio de conducta**: el daemon `serve` + `enroll` se comportan exactamente igual.
- **CERO dependencias externas nuevas** (D4).
- **CERO archivos fuera de `edge/wapp-edge-agent`** tocados.
