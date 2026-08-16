-- 0001_cola_entrantes.sql — COLA DE ENTRANTES del Edge (Plan 051 Ola 1 · T1.1 / ADR-0038 Enmienda 1).
--
-- El listener («mesonero») deja de clasificar en línea: anota aquí el mensaje entrante y suelta el
-- handler de whatsmeow en milisegundos, así el acuse sale con el mensaje YA durable. El worker-cajero
-- (proceso aparte, único cliente de Ollama) reclama por CONVERSACIÓN, clasifica y escribe el intent de
-- vuelta; el despachador drena por sesión en orden de `seq` hacia cloudlink/outbox. Sin broker
-- (ADR-0003): la cola es SQLite, como el outbox.
--
-- FICHERO APARTE del edge.db (design §2, D-2): esta tabla vive en <data_dir>/cola_entrantes.db, NO en
-- la BD única del Edge. Dos motivos: (a) la poda agresiva por TTL —esta cola es un buffer, no un
-- archivo— no debe tocar ni vacuumear la BD principal; (b) el edge.db se abre con SetMaxOpenConns(1)
-- (db.go), y con el worker-cajero en OTRO proceso ese escritor único se volvería el cuello de botella
-- entre ambos. Por eso el set de migración "cola" tiene su propio runner (db.MigrateCola) y NO se
-- aplica desde Migrate().
--
-- CONTENIDO CIFRADO EN REPOSO (INV-051.1 / ADR-0002 · ADR-0034): `texto_enc` y `meta_enc` guardan el
-- texto y los metadatos de negocio SELLADOS con la DEK **de su propia sesión** (la que indica
-- `session_id`), nunca con una llave global ni en claro. El fichero .db no se cifra a nivel de página
-- (driver modernc.org/sqlite, CGO-free), así que el cifrado campo a campo es la ÚNICA barrera.
--
-- `session_id` Y `chat_jid` VAN EN CLARO, y es deliberado: `session_id` es la clave de enrutado del
-- despachador y, sobre todo, la que ELIGE la DEK con la que se abre el resto de la fila —cifrarla haría
-- imposible saber con qué llave descifrar—; `chat_jid` es, junto a ella, la clave de conversación del
-- claim atómico del cajero (mensaje + hijos en un lote) y del índice ix_cola_conv. Ambos son metadato
-- de enrutado, no contenido de negocio.
--
-- IDEMPOTENCIA LOCAL: unicidad por (session_id, wa_message_id) — el mismo mensaje de WhatsApp anotado
-- dos veces (reintento del handler, reconexión que re-emite el evento) no duplica fila; el encolado es
-- INSERT OR IGNORE por esa clave. Este índice único es la ÚNICA adición sobre el DDL literal del design
-- §2 (que no declara unicidad): sin él, el contrato "Enqueue idempotente" del puerto app.ColaEntrantes
-- exigiría un SELECT previo, que en dos procesos concurrentes tiene carrera. Es aditivo y no cambia
-- ninguna columna del design. El ORDEN lo da `seq`, secuencia monotónica GLOBAL generada por el Edge
-- (mismo patrón que el outbox: MAX(seq) al arrancar; portable, sin AUTOINCREMENT/SERIAL).
--
-- PODA POR TTL (REQ-051.7 · ADR-0038 §Enmienda 1): la poda borra SOLO filas en estado 'despachado' con
-- `despachado_en` no nulo y más viejo que el TTL. NUNCA toca 'nuevo'/'tomado'/'clasificado': cortar por
-- `ts_whatsapp` a secas borraría mensajes jamás despachados en cuanto el worker-cajero pasara caído más
-- que el TTL. El crecimiento de lo pendiente lo contiene el TOPE DE FILAS del Store (drop-oldest), que
-- descarta bajo presión real y lo deja anotado en el log.
--
-- PORTABLE SQLite/Postgres (ADR-0002 §Migración): solo TEXT/INTEGER/BLOB (BYTEA->BLOB), sin PRAGMAs, sin
-- AUTOINCREMENT, índices y unicidad con sintaxis común. IDEMPOTENTE de arriba abajo (CREATE TABLE/INDEX
-- IF NOT EXISTS): el runner applyMigrations (db.go) NO lleva tabla de versión y RE-EJECUTA el fichero
-- entero en CADA arranque, así que ninguna sentencia aquí puede fallar la segunda vez.
-- ⚠️ La portabilidad es la del DDL, NO la del código que lo usa: el ENCOLADO (Store.Enqueue) resuelve la
-- idempotencia con `INSERT OR IGNORE`, que es sintaxis ESPECÍFICA DE SQLite. Un puerto a Postgres tendría
-- que reescribir esa sentencia como `INSERT ... ON CONFLICT (session_id, wa_message_id) DO NOTHING`.
--
-- EXCEPCIÓN DOCUMENTADA a la convención de nombres: la convención dura del repo es sufijo `_unix` para
-- los timestamps epoch-segundos (ver created_unix/updated_unix en 0005_outbox.sql), pero `tomado_en` y
-- `despachado_en` se dejan TAL CUAL los nombra el DDL literal de docs/plans/051-worker-cajero-edge/
-- design.md §2, para no divergir del plan ni de las consultas de claim/poda que allí se escriben. Son
-- INTEGER epoch-segundos igual que los `*_unix`; la diferencia es solo de nombre. `ts_whatsapp` sigue el
-- mismo criterio (nombre del design).

CREATE TABLE IF NOT EXISTS cola_entrantes (
    id            INTEGER PRIMARY KEY,               -- rowid: identidad local de la fila (sin AUTOINCREMENT)
    seq           INTEGER NOT NULL,                  -- secuencia monotónica GLOBAL del Edge: ORDEN de claim y de despacho
    session_id    TEXT    NOT NULL,                  -- EN CLARO: enrutado + elige la DEK con que se abren texto_enc/meta_enc
    chat_jid      TEXT    NOT NULL,                  -- EN CLARO: con session_id, la clave de CONVERSACIÓN del claim
    wa_message_id TEXT    NOT NULL,                  -- id del mensaje de WhatsApp: idempotencia local del encolado
    ts_whatsapp   INTEGER NOT NULL,                  -- epoch-segundos de Info.Timestamp (la ventana ADR-0037 ya se evaluó ANTES)
    texto_enc     BLOB    NOT NULL,                  -- texto SELLADO con la DEK de session_id; JAMÁS en claro (INV-051.1)
    meta_enc      BLOB,                              -- metadatos (push_name, …) sellados con la misma DEK; NULL si no hay
    intent_json   TEXT,                              -- lo escribe el worker-cajero (o el fastlane al nacer la fila); NULL si aún no hay
    estado        TEXT    NOT NULL DEFAULT 'nuevo',  -- 'nuevo' | 'tomado' | 'clasificado' | 'despachado'
    tomado_en     INTEGER,                           -- epoch-segundos del claim (lease): el barrido devuelve a 'nuevo' si venció
    despachado_en INTEGER                            -- epoch-segundos del despacho: ÚNICA base de la poda por TTL (ver arriba)
);

-- Idempotencia local del encolado: el MISMO mensaje de WhatsApp, en la MISMA sesión, una sola fila.
CREATE UNIQUE INDEX IF NOT EXISTS ux_cola_session_wamid ON cola_entrantes(session_id, wa_message_id);

-- Claim del cajero y drenaje del despachador: «la fila más vieja en tal estado» (estado, luego seq).
CREATE INDEX IF NOT EXISTS ix_cola_estado_seq ON cola_entrantes(estado, seq);

-- Claim por CONVERSACIÓN: todo lo pendiente de (session_id, chat_jid) en un lote (mensaje + hijos).
CREATE INDEX IF NOT EXISTS ix_cola_conv ON cola_entrantes(session_id, chat_jid, estado);
