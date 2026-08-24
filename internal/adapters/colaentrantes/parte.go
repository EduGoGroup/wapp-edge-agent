package colaentrantes

// parte.go — EL TUBO CAJERO→DAEMON sobre la BD de la cola (Plan 051 Ola 4 · T4.5).
//
// Aquí vive el QUINTO papel del *Store, y es el único que no habla de mensajes: el buzón de una sola
// fila por el que el proceso del CAJERO le cuenta al proceso del DAEMON el estado de su circuit
// breaker, el veredicto del reparto de CPUs y el p50 de la inferencia. El daemon no puede leer esos
// tres números por su cuenta —viven en otro proceso— y por eso `intent_circuit` viajaba vacío en el
// heartbeat (la deuda declarada en internal/infra/daemon/daemon.go). El contrato está en
// internal/app/parteworker.go; esto es su única implementación.
//
// LOS DOS LADOS EN EL MISMO FICHERO, y no uno por proceso: son once columnas y un UPSERT contra su
// SELECT. Partirlos obligaría a mantener dos listas de columnas sincronizadas a mano en dos ficheros,
// que es exactamente la clase de divergencia silenciosa (se escribe una columna que nadie lee) que este
// canal no puede permitirse — su modo de fallo es la ausencia de datos, no un error.
//
// 🔴 POR QUÉ NO TOMA `s.mu`, igual que Pendientes: ese candado serializa el bloque de ESCRITURA de
// Enqueue (podar→tope→insertar), que es el camino caliente que INV-051.2 protege. Este UPSERT no toca
// `cola_entrantes` ni compite por su consistencia: escribe otra tabla, y el peor efecto de no
// serializarlo es que dos partes del mismo proceso se pisen — imposible, porque sólo lo publica el
// bucle del cajero, que es una goroutine única.
//
// 🔴 ZERO-KNOWLEDGE (ADR-0007) / INV-051.1: por aquí no pasa NADA de negocio. Ni texto, ni session_id,
// ni chat_jid. Es la única parte del adaptador que no necesita descifrar nada, precisamente porque no
// hay nada cifrado que leer.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// Las dos aserciones van en el fichero que aporta los métodos, para que borrar uno rompa la
// compilación en el sitio obvio (mismo criterio que app.ColaCajero en claim.go, app.ColaDespachador en
// despacho.go y app.ColaContador en pendientes.go).
var (
	_ app.ParteWorkerEscritor = (*Store)(nil)
	_ app.ParteWorkerLector   = (*Store)(nil)
)

// parteFilaID es el `id` de LA fila del parte. Es 1 y no puede ser otra cosa: la tabla lleva un
// `CHECK (id = 1)` (migrations/cola/0002_parte_worker.sql), así que este literal y el de la migración
// tienen que emparejar o el INSERT rebota con una violación de CHECK en el primer arranque.
const parteFilaID = 1

// sqlPublicarParte reescribe EL parte. Es un UPSERT y no un UPDATE, y esa es toda su gracia: la fila no
// la crea ninguna migración (ver el porqué en 0002_parte_worker.sql), así que la primera publicación
// del primer cajero tiene que poder inaugurarla, y las 20 siguientes de cada hora tienen que limitarse
// a pisarla. Un UPDATE pelado no afectaría a ninguna fila y no devolvería error: el daemon leería «sin
// parte» para siempre, con el cajero vivo y publicando.
//
// `ON CONFLICT(id) DO UPDATE` es sintaxis común a SQLite (>=3.24) y Postgres (>=9.5), en la línea de
// portabilidad del ADR-0002 §Migración. No se usa `INSERT OR REPLACE`, que es específico de SQLite y
// además BORRA la fila y la reinserta (perdería cualquier columna que se añada mañana y que este INSERT
// no nombre).
const sqlPublicarParte = `
INSERT INTO parte_worker (
    id, ts_unix, circuito, taskset, p50_ms,
    prefill_p50_ms, prefill_muestras, generacion_p50_ms, generacion_muestras,
    regimenes_json, clases_json
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    ts_unix             = excluded.ts_unix,
    circuito            = excluded.circuito,
    taskset             = excluded.taskset,
    p50_ms              = excluded.p50_ms,
    prefill_p50_ms      = excluded.prefill_p50_ms,
    prefill_muestras    = excluded.prefill_muestras,
    generacion_p50_ms   = excluded.generacion_p50_ms,
    generacion_muestras = excluded.generacion_muestras,
    regimenes_json      = excluded.regimenes_json,
    clases_json         = excluded.clases_json`

// sqlLeerParte lee EL parte. Por `id` y no por `ORDER BY ts_unix DESC LIMIT 1`: la fila es única por
// construcción (el CHECK), y una consulta que ordenara estaría admitiendo que puede haber varias — con
// lo que un día habría dos y nadie se enteraría.
const sqlLeerParte = `
SELECT ts_unix, circuito, taskset, p50_ms,
       prefill_p50_ms, prefill_muestras, generacion_p50_ms, generacion_muestras,
       regimenes_json, clases_json
FROM parte_worker
WHERE id = ?`

// PublicarParte deja escrito el parte del cajero, pisando el anterior.
//
// EL SELLO DE TIEMPO ES EL DEL LLAMANTE (p.TS), no `s.now()`, y la distinción importa: lo que el lector
// mide es cuánto hace que EL CAJERO estaba vivo, no cuándo tocó el disco este adaptador. Son el mismo
// instante con una diferencia de microsegundos, pero el que tiene significado es el primero.
//
// Un TS a cero se sustituye por el reloj del Store como defensa en profundidad: cero es el epoch, o
// sea un parte que nace rancio (app.ParteRancio) y que dejaría `intent_circuit` vacío con el cajero
// perfectamente vivo. No se devuelve error porque el parte es TELEMETRÍA y este método no puede ser la
// razón de que el bucle del cajero avise de algo: el fallo, si lo hay, es del llamante y se ve en que
// el sello no cuadra con el resto del log.
func (s *Store) PublicarParte(ctx context.Context, p app.ParteWorker) error {
	ts := p.TS
	if ts.IsZero() {
		ts = s.now()
	}
	if _, err := s.db.ExecContext(ctx, sqlPublicarParte,
		parteFilaID, ts.Unix(), p.Circuito, p.Taskset, p.P50ms,
		p.PrefillP50ms, p.PrefillMuestras, p.GeneracionP50ms, p.GeneracionMuestras,
		mapaAJSON(p.PorRegimen), mapaAJSON(p.PorClase)); err != nil {
		return fmt.Errorf("colaentrantes: publicar el parte del worker: %w", err)
	}
	return nil
}

// mapaAJSON serializa un reparto para su columna TEXT. Un mapa nil o vacío se escribe como CADENA VACÍA y
// no como `{}`, porque el lector distingue los dos casos y esa distinción es la que hace que «no lo mido»
// no se lea como «lo mido y todo está a cero» (ver mapaDesdeJSON).
//
// 🔴 EL ERROR DE MARSHAL SE TRAGA Y SE ESCRIBE VACÍO, que aquí es lo correcto y no una dejadez. Un
// map[string]int64 no puede fallar al serializar salvo por algo imposible, y si ocurriera, la única
// alternativa —devolver error— haría que el parte ENTERO no se publicara: el daemon perdería también el
// circuito y el taskset, y a los 90 s publicaría `intent_circuit` vacío. El parte es TELEMETRÍA; que un
// reparto se pierda no puede llevarse por delante la señal de salud que sí funciona.
func mapaAJSON(m map[string]int64) string {
	if len(m) == 0 {
		return ""
	}
	b, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(b)
}

// mapaDesdeJSON deshace lo anterior. Cadena vacía ⇒ nil («no medible»), y un JSON ROTO también ⇒ nil, por
// el mismo motivo por el que el escritor se traga su error: este código corre en el camino del HEARTBEAT,
// y que una columna de telemetría corrupta impida decir «sigo vivo» sería el peor cambio posible. La
// ausencia honesta del dato es la degradación buena.
func mapaDesdeJSON(s string) map[string]int64 {
	if s == "" {
		return nil
	}
	var m map[string]int64
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil
	}
	return m
}

// LeerParte devuelve el parte que dejó el cajero. El bool es «había parte».
//
// 🔴 SIN FILA ⇒ (ParteWorker{}, false, nil), NUNCA UN ERROR. Es el estado NORMAL de media flota: una
// instalación cuyo `agent cajero` no ha arrancado todavía (o que corre con el clasificador
// deshabilitado, WAPP_AGENT_INTENT_ENABLED=false, y por tanto nunca publica) no tiene parte, y eso no
// es un fallo del que haya que informar en cada heartbeat. Es el mismo contrato que el `(nil, nil)` de
// Reclamar con la cola vacía y el de CabezaDeSesion con la sesión al día.
//
// LO QUE SÍ ES ERROR, y por eso no se traga todo: que la tabla no exista (una BD a la que nunca se le
// aplicó MigrateCola) o que el fichero esté corrupto. Ahí el llamante tiene algo que arreglar, y
// devolver «no hay parte» lo escondería tras el mismo camino silencioso del caso normal.
//
// ESTE MÉTODO NO JUZGA LA FRESCURA. Devuelve lo que hay escrito, con su TS, y es el LECTOR quien
// compara contra app.ParteRancio y decide tirarlo entero. Filtrar aquí obligaría a este adaptador a
// tener reloj propio para una decisión que es del consumidor, y dejaría al daemon sin poder distinguir
// «no hay parte» de «hay uno viejo» — que es justo lo que querrá loguear cuando el cajero se muera.
func (s *Store) LeerParte(ctx context.Context) (app.ParteWorker, bool, error) {
	var (
		tsUnix             int64
		circuito           string
		taskset            string
		p50MS              int64
		prefillP50MS       int64
		prefillMuestras    int64
		generacionP50MS    int64
		generacionMuestras int64
		regimenesJSON      string
		clasesJSON         string
	)
	err := s.db.QueryRowContext(ctx, sqlLeerParte, parteFilaID).Scan(&tsUnix, &circuito, &taskset, &p50MS,
		&prefillP50MS, &prefillMuestras, &generacionP50MS, &generacionMuestras, &regimenesJSON, &clasesJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return app.ParteWorker{}, false, nil
	}
	if err != nil {
		return app.ParteWorker{}, false, fmt.Errorf("colaentrantes: leer el parte del worker: %w", err)
	}
	return app.ParteWorker{
		TS:                 time.Unix(tsUnix, 0),
		Circuito:           circuito,
		Taskset:            taskset,
		P50ms:              p50MS,
		PrefillP50ms:       prefillP50MS,
		PrefillMuestras:    prefillMuestras,
		GeneracionP50ms:    generacionP50MS,
		GeneracionMuestras: generacionMuestras,
		PorRegimen:         mapaDesdeJSON(regimenesJSON),
		PorClase:           mapaDesdeJSON(clasesJSON),
	}, true, nil
}
