-- 0006_edge_config.sql — CONFIG EMPUJADA POR LA NUBE (Plan 029 · T10 / ADR-0021).
--
-- El Cloud EMPUJA config de negocio al Edge por el stream CloudLink (frame ConfigUpdate): hoy el blob de
-- INTENCIONES por tenant (kind='intents', validado por wapp-shared/intents), del que el clasificador local
-- deriva prompt/schema. Se PERSISTE aquí para sobrevivir reinicios y servir de LAST-KNOWN-GOOD: si el Cloud
-- empuja una config inválida, el Edge la rechaza y CONSERVA la fila previa (nunca se queda sin config).
--
-- CLAVE POR KIND (no por sesión): la config es del EDGE/tenant, no de una sesión concreta — una sola fila
-- por `kind`. El ConfigUpdate viaja etiquetado con un session_id (routing del stream), pero se aplica global
-- e idempotente por `version` (si la versión ya está aplicada, el reenvío al reconectar es no-op).
--
-- CONTENIDO NO SENSIBLE: el blob de intenciones son few-shots/vocabulario de negocio del tenant (sin PII de
-- terceros ni credenciales); va en claro en `payload` (el .db no se cifra a nivel de página, ADR-0002).
--
-- PORTABLE SQLite/Postgres (ADR-0002 §Migración, patrón de 0005_outbox): TEXT/INTEGER/BLOB, sin PRAGMAs,
-- CREATE TABLE IF NOT EXISTS idempotente. El runner (db.go) la aplica tras 0005 dentro del set "meta".

CREATE TABLE IF NOT EXISTS edge_config (
    kind         TEXT    PRIMARY KEY,       -- discriminador de config ('intents' es el primer kind)
    version      TEXT    NOT NULL,          -- versión de la config aplicada (idempotencia por versión)
    payload      BLOB    NOT NULL,          -- blob validado por el contrato del kind (JSON de wapp-shared/intents)
    updated_unix INTEGER NOT NULL           -- epoch-segundos de la última aplicación (equivalente portable de updated_at)
);
