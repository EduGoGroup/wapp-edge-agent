-- 0005_outbox.sql — OUTBOX DURABLE del Edge (Plan 027 Ola 3 · T2, cierra H2 / ADR-0003).
--
-- Cuando el stream CloudLink está caído, los eventos del Edge hacia la nube (mensajes ENTRANTES y ACUSES)
-- se PERSISTEN aquí en vez de descartarse (comportamiento previo: log + return nil). Al reconectar se
-- DRENA en orden y se reenvía; la nube deduplica por command_id/wa_message_id (el proto NO cambia). Cumple
-- ADR-0003 literal: cola durable en SQLite, sin broker.
--
-- OUTBOX ÚNICA CON DISCRIMINADOR DE SESIÓN (ADR-0003 §Puntos abiertos, decisión): una sola tabla con la
-- columna `session_id` en vez de una tabla por sesión; el drenaje por sesión filtra por ella. El orden de
-- drenaje lo da `seq` (secuencia monotónica generada por el Edge: portable, sin AUTOINCREMENT/SERIAL), que
-- preserva el orden relativo global y POR SESIÓN (FIFO).
--
-- IDEMPOTENCIA LOCAL: `dedupe_key` = EdgeToCloud.command_id (UUID ya presente en el payload). El encolado
-- es INSERT OR IGNORE por esta clave, así un reintento de encolar el MISMO evento no lo duplica; el reenvío
-- usa los MISMOS bytes (mismo command_id) para que la nube deduplique.
--
-- CONTENIDO CIFRADO EN REPOSO: `payload` es el EdgeToCloud serializado; para los ENTRANTES sus campos
-- SENSIBLES (texto/push_name/from_pn/from_lid) viajan SELLADOS a la pública de cifrado de la nube (Plan 011
-- §6.3) cuando el Edge está enrolado — el mismo sellado que protege el cable. Así el outbox guarda contenido
-- que ni el propio Edge puede descifrar (zero-knowledge, ADR-0007), sin acoplar el transporte a la DEK
-- per-sesión. Los ACUSES no llevan PII (§10.G). El fichero .db no se cifra a nivel de página (ADR-0002).
--
-- PORTABLE SQLite/Postgres (ADR-0002 §Migración): TEXT/INTEGER/BLOB (BYTEA->BLOB), sin PRAGMAs, unicidad e
-- índices con sintaxis común. Idempotente: CREATE TABLE/INDEX IF NOT EXISTS. El runner (db.go) la aplica
-- tras 0004 dentro del set "meta" de la BD única.

CREATE TABLE IF NOT EXISTS outbox (
    dedupe_key   TEXT    PRIMARY KEY,               -- EdgeToCloud.command_id (UUID): idempotencia local (INSERT OR IGNORE)
    seq          INTEGER NOT NULL,                  -- secuencia monotónica del Edge: ORDEN de drenaje (FIFO global y por sesión)
    session_id   TEXT    NOT NULL,                  -- discriminador de sesión (outbox única con discriminador)
    kind         TEXT    NOT NULL,                  -- 'incoming' | 'receipt' (diagnóstico/métrica; no altera el reenvío)
    payload      BLOB    NOT NULL,                  -- EdgeToCloud serializado (campos sensibles sellados-a-nube si hay pubkey)
    attempts     INTEGER NOT NULL DEFAULT 0,        -- nº de reintentos de reenvío fallidos (diagnóstico)
    created_unix INTEGER NOT NULL,                  -- epoch-segundos del encolado (base del TTL y del drop-oldest)
    updated_unix INTEGER NOT NULL                   -- epoch-segundos de la última actualización (intento)
);

-- Drenaje ordenado global (drop-oldest por límite mira el menor seq).
CREATE INDEX IF NOT EXISTS ix_outbox_seq ON outbox(seq);

-- Drenaje/consulta POR SESIÓN en orden.
CREATE INDEX IF NOT EXISTS ix_outbox_session_seq ON outbox(session_id, seq);
