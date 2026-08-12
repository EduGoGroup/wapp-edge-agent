# CLAUDE.md — wapp-edge-agent (Pieza 01)

> Orientado a LLM. Lee esto antes de tocar cualquier archivo.
> Especificación completa: `../../docs/piezas/01-edge-agent.md`
> CLAUDE.md raíz del ecosistema: `../../CLAUDE.md` (si existe)

---

## Qué es esta pieza

**Daemon Go 24/7** que se instala en el escritorio o servidor del cliente
(nano/micro empresario, no técnico). Es el único proceso que mantiene el socket
de WhatsApp **siempre abierto**:
- Envía y recibe mensajes 24/7.
- Cifra todo en reposo en una BD SQLite única cifrada (dialecto-conmutable, ADR-0018).
- **NO tiene systray.** El plano de control es un contrato HTTP `/v1` sobre un Unix
  domain socket co-ubicado (sin puerto de red), y un segundo binario `wapp-ctl`
  (supervisor loopback `127.0.0.1:8105`) que hace reverse-proxy al socket y sirve la
  web de onboarding. El QR sale por terminal ASCII o por esa web local (ver más abajo).

**Papel arquitectónico: DESPACHADOR** (ADR-0005). La nube arma el payload
completo (teléfono + contenido + media); el Edge solo lo despacha contra
WhatsApp y reenvía lo que entra. No arma piezas ni llama endpoints de negocio.

**Estado: implementado y en piloto** (planes 002–031 cerrados; auto-update Plan 032
diseñado, no ejecutado). Consumido en vivo por WhatsApp real. Ver `README.md` del
repo para el detalle operativo (build, env, ejecución).

---

## Responsabilidad en wApp

| Qué hace el Edge | Qué NO hace el Edge |
|---|---|
| Mantiene el socket `whatsmeow` 24/7 | Decidir la lógica de flujos/campañas |
| Despacha órdenes completas de la nube | Armar payloads llamando endpoints |
| Reenvía mensajes entrantes a la nube | Gestionar usuarios/plantillas/contactos |
| Cifra el store local con DEK en RAM | Custodiar la DEK en la nube |
| Encola en `outbox` si la nube cae | Usar Redis, RabbitMQ o broker |
| Muestra QR local sincrónico | Hacer relay del QR a la nube (como EduGo) |
| Gestiona N sesiones/teléfonos | Tomar decisiones de negocio |

---

## Arquitectura (hexagonal · núcleo daemon + plano de control)

Dos binarios (`cmd/`): **`agent`** (el núcleo daemon) y **`wapp-ctl`** (supervisor
loopback + web de onboarding). ADR-0014/0015: el núcleo expone un contrato HTTP `/v1`
sobre un Unix socket; `wapp-ctl` lo arranca bajo demanda, hace reverse-proxy y sirve la
web. **No hay systray.**

```
cmd/
  agent/           → binario núcleo (daemon 24/7): socket /v1 + stream CloudLink
  wapp-ctl/        → supervisor loopback (127.0.0.1:8105): arranca/para el núcleo,
                     reverse-proxy al socket /v1, sirve la web de onboarding
internal/
  domain/     → Entidades: Session, SendJob, InboundEvent, Lease, DEK (en RAM)
  app/        → Casos de uso: Pair, Listen, Send, Logout, Outbox (+ diagnostics, health, sessionmgr)
               → Puertos (interfaces): SessionStore, DeviceCascadeStore, AccountStore, etc.
  adapters/
    whatsmeow/   → WhatsAppGateway: socket persistente, QR, Send, handlers de eventos
    cryptostore/ → Store: BD única SQLite cifrada campo a campo (AES-256-GCM + DEK)
    cloudlink/   → CloudLink: cliente gRPC bidi-stream saliente con mTLS
    keycustody/  → KeyCustody: keystore del SO — Keychain(mac)/DPAPI(Win)/Secret Service(Linux)
    outbox/      → cola durable SQLite (ADR-0003), drena al reconectar
    control/     → plano de control: servidor HTTP /v1 sobre el Unix socket + QR (terminal/PNG)
      inventory/    → adaptador de inventario (server.SessionLister)
      enrolladapter/→ adaptador de enrolamiento (server.RegisterEnroll)
    supervisor/  → control del ciclo de vida del núcleo desde wapp-ctl
    edgeconfig/  → aplica la config que la nube empuja por ConfigUpdate
    intent/      → clasificador LLM local opcional (envuelve el sink de entrada)
  infra/
    daemon/      → orquestador unificado del daemon (BD, Manager, Server, Auth, Health, Shutdown)
  webui/         → web de onboarding servida por wapp-ctl (QR local sincrónico, sin relay SSE)
```

---

## Tecnología y decisiones clave (ADRs)

| ADR | Decisión | Impacto en código |
|---|---|---|
| ADR-0001 | Núcleo `whatsmeow` en el Edge del cliente, no en la nube | Todo `whatsmeow` vive aquí; en EduGo era cloud-efímero |
| ADR-0002 | SQLite pure-Go (`modernc.org/sqlite`, sin CGO) | Binario estático único multiplataforma; no SQLCipher aún |
| ADR-0003 | Sin Redis ni broker en el Edge | Concurrencia Go pura; `outbox` en SQLite para durabilidad |
| ADR-0004 | Reutilización por copia-adaptación, no dependencia | Copiar adaptadores de `edugo-api-messaging`, no importarlos |
| ADR-0005 | Edge = despachador; lógica en la nube | El Edge nunca llama endpoints de negocio |
| ADR-0007 | Modelo de doble llave: DEK (cliente) + lease (servidor) | DEK en RAM (always-on); lease viaja por CloudLink |
| ADR-0008 | Multi-teléfono: N sesiones por Edge, un stream CloudLink | Cada sesión tiene su device y su DEK en SQLite |
| ADR-0011 | Auto-actualización firmada del binario | Diseñado (Plan 032), no ejecutado; el binario debe poder reemplazarse |
| ADR-0012 | LLM local de **filtrado/agregación de ráfagas** (futuro) | **Sigue futuro.** NO confundir con el clasificador de intenciones (ADR-0020), que **ya está construido** |
| ADR-0013/0014/0015 | Plataformas · núcleo daemon + plano de control · contrato `/v1` | Escritorio + appliance Pi; núcleo `agent` + `wapp-ctl`; sin systray |
| ADR-0016 | Custodia de DEK y store por sesión | `data_dir` sagrado en el home; layout multi-sesión |
| ADR-0018 | BD única del Edge (dialecto-conmutable) | `accounts`↔`devices`, DEK por dispositivo, failover, migración sin re-escanear |
| ADR-0020 | Clasificador de intenciones LLM local (**ya construido**) | Repo hermano `edge/wapp-edge-intent` (Ollama/`qwen3:1.7b`); opcional, OFF por defecto |
| ADR-0023 | Telemetría de flota + diagnóstico remoto | `SessionHealth` en heartbeat + bundle de diagnóstico bajo demanda |
| ADR-0037 | El Edge atiende **tiempo real**: la ráfaga de la caída no se ingiere | Manda `Info.Timestamp` (reloj del servidor) contra el inicio de conexión, **no** los corchetes de sync offline; margen `WAPP_AGENT_INBOUND_MARGIN_SECONDS` (300 s) para el desfase de reloj. Los ecos propios (`IsFromMe`) ya no suben |

---

## Qué reutiliza de EduGo (por copia-adaptación)

| Origen (EduGo) | Qué se copia | Adaptación necesaria |
|---|---|---|
| `edugo-api-messaging` → adaptador `whatsmeow` | `Connect`, `Send`, pairing/QR, event types | Reactivar escucha 24/7 (`ListenUseCase`, `SubscribeToMessages`) |
| `edugo-api-messaging` → `cryptoContainer` + `cryptoStore` | Esquema de llaves X25519 + NaCl box + cifrado de campo | Portar de PostgreSQL (`BYTEA`) a SQLite (`BLOB`); mismo cifrado |
| prototipo `wApp` | `ListenUseCase`, `handleClientEvent`, `RestoreSessions()` | Reactivar la escucha que EduGo había podado |

**No** se usa `edugo-worker` ni RabbitMQ. No se importa EduGo como librería.

---

## Modelo de datos (SQLite, un solo .db)

| Conjunto | Tablas | Contenido |
|---|---|---|
| Store cifrado | `msg_enc_device`, `msg_enc_sessions`, `msg_enc_prekeys`, `msg_enc_sender_keys` | Campos `whatsmeow` cifrados con DEK (AES-256-GCM) |
| Cola durable | `outbox` | Órdenes de envío pendientes (sin Redis) |
| Sesiones | tabla de metadatos | Una fila por número/sesión; multi-teléfono |

> **Ruta sagrada del store (`data_dir`, MP-02).** El `data_dir` (raíz del layout multi-sesión de
> ADR-0016 §4) tiene **default absoluto en el HOME del usuario** por SO (macOS
> `~/Library/Application Support/wApp/edge`; Linux `~/.config/wApp/edge`; Windows `%AppData%\wApp\edge`;
> ver `internal/infra/config/config.go:defaultDataDir`), **nunca** rutas de sistema con root. `config.Load`
> lo **absolutiza** (`filepath.Abs`) y el arranque hace `MkdirAll 0700`; se loguea la ruta efectiva en
> `serve`/`pair`. **No** vuelvas al default `"."` (CWD): reintroduce el re-emparejamiento. Override:
> `WAPP_AGENT_DATA_DIR`. El metadato `store_dir` sigue siendo **relativo/portable**.

---

## Modelo de doble llave (zero-knowledge)

- **DEK** (AES-256): custodiada en el keystore del SO del Edge. La nube **nunca** la tiene.
  En modo always-on vive en RAM desde el arranque.
- **Lease**: emitido por el servidor, viaja por CloudLink, revocable. Kill-switch anti-clon.
- **Desbloqueo 2-de-2**: parte local (DEK) + lease del servidor. Clon del `.db` sin lease = inútil.

---

## Plano de control y QR local (NO hay systray)

- **Contrato HTTP `/v1` sobre Unix socket** (co-ubicado, sin puerto de red): `/v1/sessions/pair`,
  `/v1/sessions/{id}/pair`, `/v1/enroll`, `/v1/logs`, `/v1/intent/status`, `/v1/daemon/start`.
  Lo sirve `internal/adapters/control`; el núcleo (`agent`) es quien lo expone.
- **`wapp-ctl`** (`cmd/wapp-ctl`) es el supervisor loopback (`127.0.0.1:8105`): arranca/para el
  núcleo, hace reverse-proxy al socket `/v1` y sirve la **web de onboarding** (`internal/webui`).
- QR **sincrónico local**: se genera en el Edge y se muestra por **terminal ASCII** o en la
  **web local** de onboarding; el cliente escanea ahí mismo. **No** hay relay SSE desde la nube
  (diferencia con EduGo). No hay bandeja del sistema ni mini-UI nativa en v1.

---

## Puntos abiertos (no implementar sin consenso)

- Cadencia de renovación del lease y caché offline (ADR-0007).
- Mecanismo concreto de auto-actualización firmada (ADR-0011): diseñado en el Plan 032, no ejecutado.
- LLM local de **filtrado/agregación de ráfagas** (ADR-0012): sigue futuro. (El **clasificador de
  intenciones** de ADR-0020 ya está construido en `edge/wapp-edge-intent`; no es esto.)
- Custodio final de la DEK: keystore del Edge vs. dispositivo Guardián (ADR-0007).
- Capacidades reales de `whatsmeow` para botones/polls/listas.

---

## Referencias

- Especificación: `../../docs/piezas/01-edge-agent.md`
- CloudLink (conducto con la nube): `../../docs/piezas/02-cloudlink.md`
- ADRs relevantes: `../../docs/adr/` — 0001 (núcleo en el Edge), 0002 (SQLite pure-Go),
  0003 (sin broker; outbox), 0005 (despachador), 0007 (doble llave), 0008 (multi-teléfono),
  0011 (auto-update), 0012 (LLM ráfagas, futuro), 0013/0014/0015 (plano de control), 0016
  (custodia por sesión), 0018 (BD única), 0020 (clasificador de intenciones), 0023 (telemetría)
- CLAUDE.md raíz: `../../CLAUDE.md`
