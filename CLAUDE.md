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
- Vive en la bandeja del sistema (systray) en Windows, macOS y Linux.
- Cifra todo en reposo en un SQLite local.

**Papel arquitectónico: DESPACHADOR** (ADR-0005). La nube arma el payload
completo (teléfono + contenido + media); el Edge solo lo despacha contra
WhatsApp y reenvía lo que entra. No arma piezas ni llama endpoints de negocio.

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

## Arquitectura hexagonal

```
internal/
  domain/     → Entidades: Session, SendJob, InboundEvent, Lease, DEK (en RAM)
  app/        → Casos de uso: Pair, Connect, RestoreSessions, Listen, Send, RunFlowStep
               → Puertos (interfaces): WhatsAppGateway, Store, CloudLink, KeyCustody
  adapters/
    whatsmeow/   → WhatsAppGateway: socket persistente, QR, Send, handlers de eventos
    cryptostore/ → Store: SQLite cifrado (campo a campo AES-256-GCM + DEK)
    cloudlink/   → CloudLink: cliente gRPC bidi-stream saliente con mTLS
    keycustody/  → KeyCustody: keystore del SO (Win/mac/Linux)
    systray/     → mini-UI: bandeja + menú + display de estado de sesiones
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
| ADR-0011 | Auto-actualización firmada del binario | Mecanismo por detallar; el binario debe poder reemplazarse |
| ADR-0012 | LLM local de filtrado/agregación (Gemma, futuro) | Filtro enchufable antes del reenvío por CloudLink; incremental |

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

---

## Modelo de doble llave (zero-knowledge)

- **DEK** (AES-256): custodiada en el keystore del SO del Edge. La nube **nunca** la tiene.
  En modo always-on vive en RAM desde el arranque.
- **Lease**: emitido por el servidor, viaja por CloudLink, revocable. Kill-switch anti-clon.
- **Desbloqueo 2-de-2**: parte local (DEK) + lease del servidor. Clon del `.db` sin lease = inútil.

---

## Systray y QR local

- QR **sincrónico local**: se genera en el Edge y se muestra en la mini-UI del Edge.
  El cliente escanea ahí mismo. **No** hay relay SSE desde la nube (diferencia con EduGo).
- Menú: Inicio / Suspender / Cancelar / Emparejar / lista de sesiones.

---

## Puntos abiertos (no implementar sin consenso)

- Cadencia de renovación del lease y caché offline (ADR-0007).
- Mecanismo concreto de auto-actualización firmada (ADR-0011).
- Alcance del LLM local: modelo exacto, reglas de filtrado, recursos (ADR-0012).
- Custodio final de la DEK: keystore del Edge vs. dispositivo Guardián (ADR-0007).
- Capacidades reales de `whatsmeow` para botones/polls/listas.

---

## Referencias

- Especificación: `../../docs/piezas/01-edge-agent.md`
- CloudLink (conducto con la nube): `../../docs/piezas/02-cloudlink.md`
- ADRs relevantes: `../../docs/adr/0001` a `0012` (ver lista en el encabezado de la pieza)
- CLAUDE.md raíz: `../../CLAUDE.md`
