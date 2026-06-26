-- 0001_init.sql — esquema inicial del store cifrado del Edge (SQLite, ADR-0002).
--
-- Portado del esquema PostgreSQL de edugo-api-messaging (copia-adaptación, ADR-0004),
-- recortado a SOLO las tablas msg_enc_* que necesita el cryptostore del spike T2.
-- Mapeo de tipos PG -> SQLite: BYTEA->BLOB, BIGINT/INTEGER->INTEGER, BOOLEAN->INTEGER.
--
-- Principio (RF-3 / RNF-3): el fichero .db queda en CIPHERTEXT a nivel de CAMPO. NO se usa
-- SQLCipher: cada BLOB sensible guarda el sellado AES-256-GCM (nonce 12B + datos + tag 16B)
-- producido por wapp-shared/envelope bajo la DEK. Por eso estas columnas son BLOB LIBRE: el
-- esquema upstream de whatsmeow tiene CHECK(length=32/64) que rechazaría el ciphertext
-- (demostrado en schema_reject_test.go). Las tablas base whatsmeow_* (no sensibles) las crea
-- sqlstore.Upgrade aparte; aquí solo viven las columnas que cifra el decorator cryptostore.
--
-- Idempotente: CREATE TABLE IF NOT EXISTS. El runner (db.go) aplica este SQL al abrir.
--
-- RECORTADO respecto al original PG: la tabla `device_link` (puente lógico con EduGo,
-- UUID/TIMESTAMPTZ/now()) NO aplica al spike del store cifrado; se omite para no arrastrar
-- deuda muerta (pertenece a la capa de enrolamiento, fuera del alcance de T2).

-- Campos sensibles DIRECTOS del store.Device (NoiseKey, IdentityKey, SignedPreKey,
-- AdvSecretKey, Account), cifrados. Una fila por JID de sesión vinculada.
CREATE TABLE IF NOT EXISTS msg_enc_device (
    jid                  TEXT    PRIMARY KEY,
    registration_id      INTEGER NOT NULL,
    signed_pre_key_id    INTEGER NOT NULL,
    noise_priv           BLOB    NOT NULL,   -- ciphertext de NoiseKey.Priv (32B)
    identity_priv        BLOB    NOT NULL,   -- ciphertext de IdentityKey.Priv (32B)
    signed_pre_key_priv  BLOB    NOT NULL,   -- ciphertext de SignedPreKey.Priv (32B)
    signed_pre_key_sig   BLOB    NOT NULL,   -- ciphertext de SignedPreKey.Signature (64B)
    adv_secret_key       BLOB    NOT NULL,   -- ciphertext de AdvSecretKey
    adv_details          BLOB    NOT NULL,   -- ciphertext de Account.Details
    adv_account_sig      BLOB    NOT NULL,   -- ciphertext de Account.AccountSignature
    adv_account_sig_key  BLOB    NOT NULL,   -- ciphertext de Account.AccountSignatureKey
    adv_device_sig       BLOB    NOT NULL    -- ciphertext de Account.DeviceSignature
);

-- Identidades Signal de los pares (IdentityStore). La columna upstream tiene CHECK length=32.
CREATE TABLE IF NOT EXISTS msg_enc_identities (
    our_jid   TEXT NOT NULL,
    their_id  TEXT NOT NULL,
    identity  BLOB NOT NULL,                 -- ciphertext de un [32]byte
    PRIMARY KEY (our_jid, their_id)
);

-- Sesiones Signal (SessionStore). bytea libre upstream; lo ciframos igual.
CREATE TABLE IF NOT EXISTS msg_enc_sessions (
    our_jid   TEXT NOT NULL,
    their_id  TEXT NOT NULL,
    session   BLOB NOT NULL,                 -- ciphertext de []byte variable
    PRIMARY KEY (our_jid, their_id)
);

-- Prekeys propias (PreKeyStore). La privada es de 32B (CHECK upstream length=32).
CREATE TABLE IF NOT EXISTS msg_enc_prekeys (
    jid       TEXT    NOT NULL,
    key_id    INTEGER NOT NULL,
    priv      BLOB    NOT NULL,              -- ciphertext de la privada de 32B
    uploaded  INTEGER NOT NULL,             -- 0/1 (BOOLEAN portado a INTEGER)
    PRIMARY KEY (jid, key_id)
);

-- Sender keys de grupo (SenderKeyStore). bytea libre upstream; ciframos igual.
CREATE TABLE IF NOT EXISTS msg_enc_sender_keys (
    our_jid    TEXT NOT NULL,
    chat_id    TEXT NOT NULL,
    sender_id  TEXT NOT NULL,
    sender_key BLOB NOT NULL,                -- ciphertext de []byte variable
    PRIMARY KEY (our_jid, chat_id, sender_id)
);
