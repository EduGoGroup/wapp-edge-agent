-- 0002_parte_worker.sql — EL PARTE DEL WORKER-CAJERO (Plan 051 Ola 4 · T4.5).
--
-- POR QUÉ ESTA TABLA EXISTE, en una línea: el daemon (`agent serve`) construye el SessionHealth del
-- heartbeat, pero el circuit breaker del clasificador, el veredicto del `taskset` y el p50 de la
-- inferencia viven en el proceso del CAJERO (`agent cajero`). Son dos procesos y el único canal que
-- comparten es el disco. Hasta esta migración, `intent_circuit` viajaba VACÍO a la nube (el `nil`
-- explícito de health.NewCollector en internal/infra/daemon/daemon.go lo declaraba como deuda). El
-- cajero escribe aquí; el daemon lee de aquí.
--
-- 🔴 FICHERO NUEVO, Y NO UNA EDICIÓN DEL 0001. El 0001 declara la regla dura «NINGUNA columna nueva se
-- añade editando este CREATE TABLE» porque `CREATE TABLE IF NOT EXISTS` es un NO-OP sobre las colas que
-- YA existen en disco (cualquier binario de `dev` creó su cola_entrantes.db al arrancar), así que una
-- columna añadida ahí no aparecería nunca y el primer SELECT que la nombrara moriría con `no such
-- column`. Esa regla habla de COLUMNAS de una tabla que ya existe, y su mecánica no aplica a una TABLA
-- NUEVA: sobre una BD vieja, `CREATE TABLE IF NOT EXISTS parte_worker` no es un no-op —la tabla no
-- está— y la crea igual que sobre una virgen. El camino del `ensure…` en Go (ensureColaClaimToken,
-- ensureColaIntentos) existe SOLO porque un ALTER no se puede reemitir; un CREATE IF NOT EXISTS sí.
--
-- IDEMPOTENTE Y SEGURO EN CARRERA, que aquí no es teoría: el runner (applyMigrations, db.go) NO lleva
-- tabla de versión y RE-EJECUTA este fichero ENTERO en CADA arranque, y además LOS DOS PROCESOS aplican
-- MigrateCola sobre el mismo fichero, en cualquier orden y potencialmente a la vez (cmd/agent/cajero.go
-- y el wiring del daemon). `CREATE TABLE IF NOT EXISTS` se resuelve dentro de la transacción de
-- escritura de SQLite, así que el segundo en llegar ve la tabla ya creada y no falla; y si los dos
-- entran a la vez, el perdedor espera el `busy_timeout` (5 s, openSQLite) y luego encuentra la tabla.
-- No hay INSERT semilla aquí a propósito: la fila la crea el UPSERT del adaptador (colaentrantes/
-- parte.go), y así «no hay parte» y «hay parte de un cajero que arrancó» son distinguibles — una fila
-- semilla con ts=0 sería un parte rancio indistinguible de uno real, justo la ambigüedad que
-- app.ParteWorkerLector resuelve devolviendo (zero, false, nil).
--
-- FILA ÚNICA POR CONSTRUCCIÓN: `id INTEGER PRIMARY KEY CHECK (id = 1)`. El CHECK no es decoración —es
-- lo que impide que un bug del adaptador (un INSERT sin el 1, un UPSERT mal escrito) deje dos partes en
-- la tabla y el lector se lleve el que no toca, en silencio. Hay UN cajero por máquina y su breaker, su
-- taskset y su p50 son POR PROCESO (ver cajero.Deps.Colas): no hay nada por lo que particionar.
--
-- 🔴 ZERO-KNOWLEDGE (ADR-0007) / INV-051.1: en esta tabla no entra NADA de negocio. Ni texto, ni
-- session_id, ni chat_jid, ni teléfono, ni llave. Cuatro señales operativas y un sello de tiempo. Es la
-- única tabla de este fichero .db que NO necesita cifrado de campo, precisamente porque no hay nada que
-- proteger: si alguien añade aquí una columna que lleve contenido, tiene que ir sellada con la DEK como
-- `texto_enc`, y entonces el daemon no podría leerla sin la DEK — señal de que la columna no pertenece
-- a esta tabla.
--
-- PORTABLE SQLite/Postgres (ADR-0002 §Migración): solo TEXT/INTEGER, sin PRAGMAs, sin AUTOINCREMENT,
-- CHECK y DEFAULT con sintaxis común.
--
-- `ts_unix` en epoch-SEGUNDOS y con el sufijo `_unix`, que es la convención dura del repo (ver
-- created_unix/updated_unix en 0005_outbox.sql). El 0001 se saltó esa convención (`tomado_en`,
-- `despachado_en`) por fidelidad literal al DDL del design; esta tabla no arrastra esa excepción.
-- ⚠️ SEGUNDOS, no milisegundos: el lector compara contra su propio reloj de PARED con un umbral de 90 s
-- (app.ParteRancio), así que la resolución de segundo sobra y la unidad queda igual que en el resto del
-- esquema. Los dos procesos leen el reloj de la MISMA máquina, así que no hay desfase de relojes que
-- razonar; lo que sí puede pasar es un salto de NTP hacia atrás (la plataforma objetivo es un portátil
-- que se suspende), y el efecto de eso es un parte que parece del futuro o rancio de más — degrada a
-- `intent_circuit` vacío, que es el fallo seguro, nunca a una señal inventada.

CREATE TABLE IF NOT EXISTS parte_worker (
    id       INTEGER PRIMARY KEY CHECK (id = 1),  -- fila ÚNICA: un cajero por máquina (ver arriba)
    ts_unix  INTEGER NOT NULL,                    -- epoch-segundos de cuándo lo escribió el cajero
    circuito TEXT    NOT NULL DEFAULT '',         -- 'closed' | 'open' | 'half-open' (etiquetas del breaker); '' = no se sabe
    taskset  TEXT    NOT NULL DEFAULT '',         -- 'disjunta' | 'solapada' | 'cajero_sin_confinar'; '' = no se sabe
    p50_ms   INTEGER NOT NULL DEFAULT 0           -- p50 de la INFERENCIA en ms; 0 = sin muestras
);
