-- 0002_sessions.sql — metadatos de NEGOCIO de las sesiones del Edge (T6.1, RF-7).
--
-- A diferencia de las tablas msg_enc_* (0001), aquí NO vive material criptográfico: esta
-- tabla guarda metadatos EN CLARO de cada sesión vinculada (jid + estado + timestamps) para
-- que el arranque (app.RestoreSessions) sepa QUÉ sesiones restaurar sin tener que descifrar el
-- store. El device cifrado (claves whatsmeow) sigue exclusivamente en msg_enc_device; aquí solo
-- se referencia su JID. No duplicar material sensible: si una columna oliera a secreto, va cifrada
-- en msg_enc_*, no aquí.
--
-- Una fila por sesión/teléfono (semilla del multi-teléfono, ADR-0008; el spike usa UNA).
-- Idempotente: CREATE TABLE IF NOT EXISTS. El runner (db.go) la aplica en orden tras la 0001, así
-- que sobre una BD que ya tiene solo 0001 (p.ej. la sesión real del spike) añade esta tabla sin
-- tocar los datos existentes.
--
-- Estado: 'active' (sesión vinculada y operable) | 'loggedout' (cerrada por WhatsApp; no
-- re-emparejar automático, RF-6). Timestamps en epoch-segundos (INTEGER), como el resto del store.
CREATE TABLE IF NOT EXISTS sessions (
    jid        TEXT    PRIMARY KEY,            -- JID de la sesión (referencia a msg_enc_device.jid)
    state      TEXT    NOT NULL,               -- 'active' | 'loggedout'
    paired_at  INTEGER NOT NULL,               -- epoch-segundos del emparejamiento
    updated_at INTEGER NOT NULL                -- epoch-segundos de la última actualización de estado
);
