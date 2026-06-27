-- 0003_sessions_multi.sql — metadatos de NEGOCIO multi-sesión del Edge (Plan 008, ADR-0016).
--
-- EVOLUCIÓN de la tabla `sessions` (0002, jid PK): el modelo multi-sesión real (ADR-0016 §3)
-- identifica cada sesión por un `session_id` opaco (UUIDv4 generado por el Edge AL INICIAR el
-- emparejamiento), no por el JID. Razón: el JID se descubre recién en PairSuccess (no se conoce el
-- número antes de escanear) y suele ser un LID opaco; el `session_id` es además el discriminador que
-- ya usan ADR-0008 (un stream CloudLink), el fleet de la nube y el Motor de Flujos.
--
-- Esta tabla guarda metadatos EN CLARO (sin material criptográfico, igual que `sessions`): el material
-- cifrado de whatsmeow vive en las tablas msg_enc_* de un store.db POR SESIÓN (ADR-0016 §2/§4). Aquí
-- solo se referencia el JID (atributo, NULL hasta PairSuccess) y se anota el ciclo de vida + dónde
-- vive el store de la sesión (store_dir, relativo a data_dir).
--
-- La tabla `sessions` (0002) se RETIRA de USO en código (sessionstore pasa a operar `sessions_v2`); NO
-- se la elimina aquí a propósito: (a) DROP rompería los tests de la 0002 cuyo propósito es verificar su
-- efecto, y (b) el retiro REAL del estado single-sesión lo hace la migración clean-slate (§10.C, código
-- Go) archivando el store.db plano completo. La tabla vieja, vacía, coexiste sin daño.
--
-- Idempotente: CREATE TABLE/INDEX IF NOT EXISTS. El runner (db.go) la aplica en orden tras 0001/0002.
CREATE TABLE IF NOT EXISTS sessions_v2 (
    session_id TEXT    PRIMARY KEY,            -- UUIDv4 generado por el Edge al iniciar el emparejamiento
    jid        TEXT,                           -- JID whatsmeow (NULL hasta PairSuccess); UNIQUE cuando no NULL
    state      TEXT    NOT NULL,               -- 'pairing' | 'active' | 'suspended' | 'loggedout'
    store_dir  TEXT    NOT NULL,               -- ruta relativa del directorio de la sesión (sessions/<session_id>)
    paired_at  INTEGER,                        -- epoch-segundos del emparejamiento (NULL mientras pairing)
    updated_at INTEGER NOT NULL                -- epoch-segundos de la última actualización de estado
);

-- Un JID no puede repartirse entre dos sesiones, PERO varias filas pueden tener jid NULL a la vez
-- (sesiones en 'pairing' aún sin número). De ahí el índice único PARCIAL (WHERE jid IS NOT NULL).
CREATE UNIQUE INDEX IF NOT EXISTS ux_sessions_jid ON sessions_v2(jid) WHERE jid IS NOT NULL;
