-- 0004_accounts_devices.sql — modelo CUENTA↔DISPOSITIVO de la BD ÚNICA del Edge (Plan 022 T1, ADR-0018).
--
-- EVOLUCIÓN de `sessions_v2` (0003): WhatsApp es jerárquico — un NÚMERO (cuenta) admite 1 principal +
-- hasta 4 dispositivos vinculados. El modelo single-`session_id` de la 0003 trataba cada escaneo como un
-- silo huérfano; aquí se introduce la entidad CUENTA (número de negocio `self_pn`) y cada DISPOSITIVO la
-- referencia. Un RE-ESCANEO del mismo número es otro dispositivo de la MISMA cuenta, no un silo nuevo
-- (ADR-0018 §Decisión.3, design §2).
--
--   accounts(account_id PK, self_pn UNIQUE, display_name, created_at, updated_at)
--   devices (session_id PK, account_id FK→accounts, jid, state, role, paired_at, updated_at)
--
-- La tabla `devices` REEMPLAZA a `sessions_v2` como FUENTE DE VERDAD del ciclo de vida del dispositivo
-- (el sessionstore pasa a operar accounts/devices). Diferencias con `sessions_v2`:
--   - SIN columna `store_dir`: la BD única (ADR-0018 §Decisión.1) retira el directorio por sesión; la
--     ruta del store dejó de persistirse (el adaptador la DERIVA de `sessions/<session_id>` mientras los
--     consumidores runtime migran en T3).
--   - `account_id` (FK, NOT NULL): todo dispositivo cuelga de una cuenta. Mientras el número no se conoce
--     (device en 'pairing', el JID llega recién en PairSuccess) el adaptador crea una cuenta PROVISIONAL
--     por dispositivo (account_id = session_id, self_pn NULL); T3 la re-vincula a la cuenta real por self_pn.
--   - `role` ('primary'|'standby', default 'primary'): base del failover multi-dispositivo por número (T5).
--
-- Estos son metadatos de NEGOCIO EN CLARO (como `sessions_v2`): NO llevan material criptográfico. El
-- material cifrado de whatsmeow vive en las tablas msg_enc_*/whatsmeow_* de la MISMA BD única, cifrado
-- con la DEK POR DISPOSITIVO (T2); aquí solo se referencia el JID y el ciclo de vida.
--
-- NO se elimina `sessions_v2` (ni `sessions`): coexisten vacías/sin uso, igual que la 0003 conservó a la
-- 0002. El retiro REAL del estado single-sesión lo hace la migración clean-slate (edgemigrate, código Go)
-- archivando el árbol viejo — nunca un DROP destructivo aquí.
--
-- PORTABLE SQLite/Postgres (ADR-0002 §Migración): solo TEXT/INTEGER, sin PRAGMAs, unicidad e índices con
-- sintaxis común. Idempotente: CREATE TABLE/INDEX IF NOT EXISTS. El runner (db.go) la aplica tras 0003.

-- accounts: el NÚMERO de negocio. `self_pn` es UNIQUE cuando no es NULL (varias cuentas PROVISIONALES
-- pre-emparejamiento coexisten con self_pn NULL: SQLite/Postgres tratan cada NULL como distinto en UNIQUE).
CREATE TABLE IF NOT EXISTS accounts (
    account_id   TEXT    PRIMARY KEY,           -- identidad de la cuenta (UUID de la cuenta real; o el
                                                -- session_id de una cuenta provisional pre-número)
    self_pn      TEXT    UNIQUE,                -- número propio E.164 (NULL hasta conocer el JID; ÚNICO si no NULL)
    display_name TEXT,                          -- nombre visible de la cuenta (opcional)
    created_at   INTEGER NOT NULL,              -- epoch-segundos de alta de la cuenta
    updated_at   INTEGER NOT NULL               -- epoch-segundos de la última actualización
);

-- devices: el DISPOSITIVO vinculado (evolución de sessions_v2, sin store_dir). `session_id` sigue siendo
-- la identidad canónica (UUIDv4 del Edge, ADR-0016 §3). FK a accounts (NOT NULL) con foreign_keys=ON.
CREATE TABLE IF NOT EXISTS devices (
    session_id TEXT    PRIMARY KEY,             -- UUIDv4 del Edge (identidad canónica del dispositivo)
    account_id TEXT    NOT NULL REFERENCES accounts(account_id), -- cuenta (número) a la que pertenece
    jid        TEXT,                            -- JID whatsmeow (NULL hasta PairSuccess; ÚNICO si no NULL)
    state      TEXT    NOT NULL,                -- 'pairing' | 'active' | 'suspended' | 'loggedout'
    role       TEXT    NOT NULL DEFAULT 'primary', -- 'primary' | 'standby' (failover por número, T5)
    paired_at  INTEGER,                         -- epoch-segundos del emparejamiento (NULL mientras pairing)
    updated_at INTEGER NOT NULL                 -- epoch-segundos de la última actualización de estado
);

-- Un JID no puede repartirse entre dos dispositivos, PERO varios devices en 'pairing' comparten jid NULL:
-- índice único PARCIAL (WHERE jid IS NOT NULL), como ux_sessions_jid de la 0003.
CREATE UNIQUE INDEX IF NOT EXISTS ux_devices_jid ON devices(jid) WHERE jid IS NOT NULL;

-- Los dispositivos se consultan por cuenta (failover/borrado por número, GetByAccount): índice de apoyo.
CREATE INDEX IF NOT EXISTS ix_devices_account ON devices(account_id);
