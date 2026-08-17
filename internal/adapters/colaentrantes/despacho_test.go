package colaentrantes

// despacho_test.go — el lado DESPACHADOR del store (Plan 051 Ola 3 · T3.2 · REQ-051.20).
//
// Reutiliza los helpers de colaentrantes_test.go y claim_test.go (openDB con la migración REAL,
// newStore, fakeCrypterFor, fakeClock, sembrar*, estadoDe, intentDe, claimTokenDe, waIDs, existe):
// mismo paquete a propósito, para que el esquema de los tests no se desincronice del de producción.
//
// 🔴 NINGÚN TEST DE ESTE FICHERO TRANSCRIBE DDL (regla T2.17/T2.18): la tabla la crea `db.MigrateCola`
// a través de `openDB`, incluidas las columnas `claim_token` e `intentos` que NO están en el
// `CREATE TABLE` y sólo existen si corren los pasos guardados en Go.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// ─────────────────────────── helpers propios del lado despachador ───────────────────────────

// resumenCabeza describe una cabeza para un mensaje de fallo SIN volcar su contenido.
//
// 🔴 EXISTE PARA NO ESCRIBIR `%+v` DE UN *app.ColaCabeza, por la misma razón que resumenLote existe
// para el lote: la cabeza lleva `Texto` y `Meta` YA DESCIFRADOS, y `%+v` los imprime enteros — justo lo
// que INV-051.1 prohíbe. Aquí son fixtures y no hay PII real, pero el patrón es el que acaba copiado a
// un log de producción; se corta en el test.
func resumenCabeza(c *app.ColaCabeza) string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("cabeza(id=%d seq=%d session_id=%s wa_message_id=%s estado=%s tiene_intent=%t)",
		c.ID, c.Seq, c.SessionID, c.WAMessageID, c.Estado, c.TieneIntent)
}

// despachadoEnDe lee el sello de despacho de una fila. Es la columna sobre la que corta la poda por
// TTL, así que su UNIDAD (epoch-segundos, nunca milis) es parte de lo que estos tests fijan.
func despachadoEnDe(t *testing.T, db *sql.DB, id int64) sql.NullInt64 {
	t.Helper()
	var sello sql.NullInt64
	if err := db.QueryRow(`SELECT despachado_en FROM cola_entrantes WHERE id = ?`, id).Scan(&sello); err != nil {
		t.Fatalf("leer despachado_en de id=%d: %v", id, err)
	}
	return sello
}

// tomadoEnDe lee el sello del lease de una fila (el que mide BarrerLeasesVencidos).
func tomadoEnDe(t *testing.T, db *sql.DB, id int64) sql.NullInt64 {
	t.Helper()
	var sello sql.NullInt64
	if err := db.QueryRow(`SELECT tomado_en FROM cola_entrantes WHERE id = ?`, id).Scan(&sello); err != nil {
		t.Fatalf("leer tomado_en de id=%d: %v", id, err)
	}
	return sello
}

// marcarEnBD fuerza estado + intent_json de una fila por SQL directo. Se usa sólo para montar estados
// que el camino real no puede producir en un test corto (una fila `clasificado` sin pasar por el claim,
// p.ej.); cuando el camino real sirve, se usa el camino real.
func marcarEnBD(t *testing.T, db *sql.DB, id int64, estado, intentJSON string) {
	t.Helper()
	intent := sql.NullString{String: intentJSON, Valid: intentJSON != ""}
	if _, err := db.Exec(`UPDATE cola_entrantes SET estado = ?, intent_json = ? WHERE id = ?`,
		estado, intent, id); err != nil {
		t.Fatalf("forzar estado %q en id=%d: %v", estado, id, err)
	}
}

// ─────────────────────────── (a) CabezaDeSesion ───────────────────────────

// TestCabezaDeSesionEligeLaDeSeqMasBajoYAislaPorSesion: la cabeza es la fila NO despachada de `seq` más
// bajo DE ESA SESIÓN, y de nadie más.
//
// El aislamiento se prueba con la sesión B llevando el seq MENOR de toda la tabla: si la consulta
// filtrara mal (o no filtrara), la cabeza de A sería la fila de B y el despachador de A entregaría
// mensajes de otra sesión — con la DEK equivocada, además.
func TestCabezaDeSesionEligeLaDeSeqMasBajoYAislaPorSesion(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)

	sembrarNuevo(t, db, 1, 30, "A", "chat-1", "wa-a3", "tercero de A")
	sembrarNuevo(t, db, 2, 10, "A", "chat-1", "wa-a1", "primero de A")
	sembrarNuevo(t, db, 3, 20, "A", "chat-2", "wa-a2", "otro chat de A")
	sembrarNuevo(t, db, 4, 5, "B", "chat-3", "wa-b1", "de otra sesión")

	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	cabA, err := s.CabezaDeSesion(ctx, "A")
	if err != nil {
		t.Fatalf("CabezaDeSesion(A): %v", err)
	}
	if cabA == nil {
		t.Fatal("CabezaDeSesion(A) devolvió nil con tres filas pendientes de A")
	}
	if cabA.ID != 2 || cabA.Seq != 10 || cabA.WAMessageID != "wa-a1" {
		t.Fatalf("la cabeza de A debía ser la seq=10 (id=2, wa-a1), got %s", resumenCabeza(cabA))
	}
	if cabA.SessionID != "A" || cabA.ChatJID != "chat-1" {
		t.Fatalf("enrutado de la cabeza: got (%s,%s), want (A,chat-1)", cabA.SessionID, cabA.ChatJID)
	}
	if cabA.Texto != "primero de A" {
		t.Fatalf("texto descifrado de la cabeza: got %q want %q", cabA.Texto, "primero de A")
	}
	if cabA.Estado != app.EstadoNuevo {
		t.Fatalf("estado de la cabeza: got %q want %q", cabA.Estado, app.EstadoNuevo)
	}

	// La cabeza de B es SUYA, aunque su seq sea el más bajo de toda la tabla.
	cabB, err := s.CabezaDeSesion(ctx, "B")
	if err != nil {
		t.Fatalf("CabezaDeSesion(B): %v", err)
	}
	if cabB == nil || cabB.ID != 4 || cabB.SessionID != "B" {
		t.Fatalf("la cabeza de B debía ser su propia fila (id=4), got %s", resumenCabeza(cabB))
	}
}

// TestCabezaDeSesionIgnoraLasDespachadas: una fila ya `despachado` NO es cabeza de nada — ya salió al
// cable y sólo espera la poda. Si lo fuera, el despachador la re-entregaría en cada poll, para siempre.
func TestCabezaDeSesionIgnoraLasDespachadas(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)

	sembrar(t, db, 1, 10, "A", "chat@s", "wa-ya-salio", "esto ya salió", app.EstadoDespachado, sql.NullInt64{})
	sembrarNuevo(t, db, 2, 20, "A", "chat@s", "wa-pendiente", "esto no")

	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	cab, err := s.CabezaDeSesion(ctx, "A")
	if err != nil {
		t.Fatalf("CabezaDeSesion: %v", err)
	}
	if cab == nil || cab.ID != 2 {
		t.Fatalf("la cabeza debía saltarse la fila ya despachada y ser la id=2, got %s", resumenCabeza(cab))
	}
	// Los otros tres estados del ciclo SÍ son cabeza: el despachador tiene que verlos para saber si
	// espera (`nuevo`/`tomado`) o entrega (`clasificado`).
	for _, estado := range []string{app.EstadoNuevo, app.EstadoTomado, app.EstadoClasificado} {
		marcarEnBD(t, db, 2, estado, "")
		cab, err := s.CabezaDeSesion(ctx, "A")
		if err != nil {
			t.Fatalf("CabezaDeSesion con estado %q: %v", estado, err)
		}
		if cab == nil || cab.Estado != estado {
			t.Fatalf("una fila en %q debía seguir siendo cabeza, got %s", estado, resumenCabeza(cab))
		}
	}
}

// TestCabezaDeSesionSinNadaPendienteDevuelveNilNil: (nil, nil) es el estado NORMAL de casi todos los
// polls (una sesión al día), no un error. Si devolviera error, el bucle del despachador tendría que
// distinguir «no hay nada» de «algo se rompió» leyendo el texto del error — el mismo criterio que ya
// sigue Reclamar con la cola vacía.
func TestCabezaDeSesionSinNadaPendienteDevuelveNilNil(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	// 1. Tabla vacía.
	cab, err := s.CabezaDeSesion(ctx, "A")
	if err != nil {
		t.Fatalf("cola vacía NO es un error: %v", err)
	}
	if cab != nil {
		t.Fatalf("cola vacía debía dar cabeza nil, got %s", resumenCabeza(cab))
	}

	// 2. La sesión tiene filas, pero TODAS despachadas.
	sembrar(t, db, 1, 10, "A", "chat@s", "wa-1", "ya salió", app.EstadoDespachado, sql.NullInt64{})
	cab, err = s.CabezaDeSesion(ctx, "A")
	if err != nil {
		t.Fatalf("sesión al día NO es un error: %v", err)
	}
	if cab != nil {
		t.Fatalf("con todo despachado la cabeza debía ser nil, got %s", resumenCabeza(cab))
	}

	// 3. La sesión no existe en la cola (sesión recién emparejada, sin tráfico).
	cab, err = s.CabezaDeSesion(ctx, "sesion-sin-mensajes")
	if err != nil {
		t.Fatalf("una sesión sin mensajes NO es un error: %v", err)
	}
	if cab != nil {
		t.Fatalf("una sesión sin mensajes debía dar cabeza nil, got %s", resumenCabeza(cab))
	}
}

// TestCabezaDeSesionDescifraTextoYMetaNULL: la cabeza sale EN CLARO EN MEMORIA (es lo que el
// despachador convierte en evento) y la meta NULL se distingue de una meta vacía — queda nil, sin
// llamar a Open.
//
// De paso ejercita el AVANCE de la cabeza: al sellar la primera fila, la cabeza pasa a ser la segunda.
func TestCabezaDeSesionDescifraTextoYMetaNULL(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	sinMeta := item("A", "chat@s", "wa-1", "quiero dos empanadas")
	if err := s.Enqueue(ctx, sinMeta); err != nil {
		t.Fatalf("Enqueue sin meta: %v", err)
	}
	conMeta := item("A", "chat@s", "wa-2", "y una arepa")
	conMeta.Meta = []byte(`{"push_name":"Jhoan"}`)
	if err := s.Enqueue(ctx, conMeta); err != nil {
		t.Fatalf("Enqueue con meta: %v", err)
	}

	primera, err := s.CabezaDeSesion(ctx, "A")
	if err != nil {
		t.Fatalf("CabezaDeSesion (1ª): %v", err)
	}
	if primera == nil || primera.WAMessageID != "wa-1" {
		t.Fatalf("la cabeza debía ser wa-1, got %s", resumenCabeza(primera))
	}
	if primera.Texto != "quiero dos empanadas" {
		t.Fatalf("texto descifrado: got %q", primera.Texto)
	}
	if primera.Meta != nil {
		t.Fatalf("meta NULL debía quedar nil, got %q", string(primera.Meta))
	}
	if primera.TSWhatsApp == 0 {
		t.Fatalf("la cabeza perdió el timestamp de trazabilidad: %s", resumenCabeza(primera))
	}

	// La cabeza AVANZA cuando la primera se SELLA — y sellar es MarcarDespachada, no DespacharSinIntent.
	//
	// 🔴 ESTE TRAMO CAMBIÓ EL 2026-08-17 y la diferencia es el arreglo entero. Antes, `DespacharSinIntent`
	// bastaba para que la cabeza avanzara, porque dejaba la fila en `despachado`... sin haberla entregado
	// jamás. Hoy la deja `clasificado`: SIGUE SIENDO LA CABEZA (con su sobre de omisión, esperando su
	// entrega), y sólo el sello posterior la retira. Se comprueban las dos mitades, en orden.
	if err := s.DespacharSinIntent(ctx, primera.ID, app.MotivoPresupuesto); err != nil {
		t.Fatalf("DespacharSinIntent: %v", err)
	}
	aunCabeza, err := s.CabezaDeSesion(ctx, "A")
	if err != nil {
		t.Fatalf("CabezaDeSesion tras el sobre de omisión: %v", err)
	}
	if aunCabeza == nil || aunCabeza.ID != primera.ID || aunCabeza.Estado != app.EstadoClasificado {
		t.Fatalf("tras DespacharSinIntent la fila debía SEGUIR siendo cabeza, ya `clasificado` y pendiente de "+
			"entregarse; got %s. Si aquí avanza, la sentencia volvió a sellar 'despachado' y el mensaje se "+
			"pierde sin salir al cable", resumenCabeza(aunCabeza))
	}
	// Y ahora sí: la entrega ocurrió (aquí la simula el test) y el sello retira la fila.
	if err := s.MarcarDespachada(ctx, primera.ID); err != nil {
		t.Fatalf("MarcarDespachada: %v", err)
	}
	segunda, err := s.CabezaDeSesion(ctx, "A")
	if err != nil {
		t.Fatalf("CabezaDeSesion (2ª): %v", err)
	}
	if segunda == nil || segunda.WAMessageID != "wa-2" {
		t.Fatalf("tras sellar la primera, la cabeza debía ser wa-2, got %s", resumenCabeza(segunda))
	}
	if string(segunda.Meta) != string(conMeta.Meta) {
		t.Fatalf("meta descifrada: got %q want %q", string(segunda.Meta), string(conMeta.Meta))
	}
}

// TestCabezaDeSesionDistingueIntentNULLDeSobrePresente fija INV-051.3 en el lado del despachador:
// `intent_json` NULL significa «aún no pasó por el cajero» (hay que esperarle o correrle el
// presupuesto), NUNCA «se clasificó y no había intención». Son dos decisiones opuestas.
func TestCabezaDeSesionDistingueIntentNULLDeSobrePresente(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	sembrarNuevo(t, db, 1, 10, "A", "chat@s", "wa-1", "hola")
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	cab, err := s.CabezaDeSesion(ctx, "A")
	if err != nil {
		t.Fatalf("CabezaDeSesion: %v", err)
	}
	if cab == nil {
		t.Fatal("CabezaDeSesion devolvió nil con una fila pendiente")
	}
	if cab.TieneIntent || cab.IntentJSON != "" {
		t.Fatalf("intent_json NULL debía dar TieneIntent=false e IntentJSON=\"\", got %s (%q)",
			resumenCabeza(cab), cab.IntentJSON)
	}

	// El fastlane la pare `clasificado` con su sobre de omisión (o el cajero la cierra así).
	sobre := app.SobreOmitido(app.MotivoFastlane)
	marcarEnBD(t, db, 1, app.EstadoClasificado, sobre)

	cab, err = s.CabezaDeSesion(ctx, "A")
	if err != nil {
		t.Fatalf("CabezaDeSesion (con sobre): %v", err)
	}
	if cab == nil || !cab.TieneIntent || cab.IntentJSON != sobre {
		t.Fatalf("con sobre persistido: got %s (%q), want TieneIntent=true y el sobre íntegro",
			resumenCabeza(cab), cab.IntentJSON)
	}
	if cab.Estado != app.EstadoClasificado {
		t.Fatalf("estado de la cabeza: got %q want %q", cab.Estado, app.EstadoClasificado)
	}
	// Y lo que sale del store lo lee la ÚNICA puerta del puerto: si esta aserción cayera, el
	// despachador entregaría el sobre `omitido` al cable, que es lo que ADR-0038 §(e) prohíbe.
	motivo, ok := app.EsOmitido(cab.IntentJSON)
	if !ok || motivo != app.MotivoFastlane {
		t.Fatalf("app.EsOmitido sobre lo que devuelve la cabeza: got (%q,%t), want (fastlane,true)", motivo, ok)
	}
}

// ─────────────────────────── (b) MarcarDespachada ───────────────────────────

// TestMarcarDespachadaSellaLaFilaClasificada recorre el ciclo ENTERO por el camino real (encolar →
// reclamar → clasificar → despachar) y fija LA UNIDAD DEL SELLO.
//
// 🔴 LA ASERCIÓN DE LA UNIDAD ES LA QUE IMPORTA: `despachado_en` tiene que valer EXACTAMENTE
// `clock.Unix()`, epoch-SEGUNDOS. Es la columna sobre la que corta `pruneTTLLocked`
// (`despachado_en < now.Unix() - ttl`), así que un sello en MILIS sería ~1000× mayor que cualquier
// corte y la fila no se podaría jamás: el TTL volvería a ser decorativo, en silencio y sin que ningún
// otro test lo notara.
func TestMarcarDespachadaSellaLaFilaClasificada(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0, WithClock(clock.Now))

	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-1", "quiero dos empanadas")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	lote, err := s.Reclamar(ctx, 0)
	if err != nil || lote == nil {
		t.Fatalf("Reclamar: lote=%s err=%v", resumenLote(lote), err)
	}
	if err := s.MarcarClasificado(ctx, lote, `{"intent":"crear_pedido","confidence":0.98}`); err != nil {
		t.Fatalf("MarcarClasificado: %v", err)
	}

	cab, err := s.CabezaDeSesion(ctx, "A")
	if err != nil || cab == nil {
		t.Fatalf("CabezaDeSesion: cabeza=%s err=%v", resumenCabeza(cab), err)
	}
	if cab.Estado != app.EstadoClasificado || !cab.TieneIntent {
		t.Fatalf("la cabeza debía estar lista para entregar, got %s", resumenCabeza(cab))
	}

	// El despachador entrega y sella.
	clock.t = clock.t.Add(3 * time.Second) // el sello es el instante del DESPACHO, no el del claim
	if err := s.MarcarDespachada(ctx, cab.ID); err != nil {
		t.Fatalf("MarcarDespachada: %v", err)
	}

	if got := estadoDe(t, db, cab.ID); got != app.EstadoDespachado {
		t.Fatalf("estado tras el sello: got %q want %q", got, app.EstadoDespachado)
	}
	sello := despachadoEnDe(t, db, cab.ID)
	if !sello.Valid {
		t.Fatal("despachado_en quedó NULL: la poda por TTL exige el sello además del estado, así que esta fila NUNCA se podaría")
	}
	if sello.Int64 != clock.t.Unix() {
		t.Fatalf("despachado_en = %d, want %d (EPOCH-SEGUNDOS, la unidad que compara pruneTTLLocked; "+
			"en milis serían %d y la poda no borraría nunca)", sello.Int64, clock.t.Unix(), clock.t.UnixMilli())
	}
	// Y deja de ser cabeza: el despachador no la re-entrega en el poll siguiente.
	cab2, err := s.CabezaDeSesion(ctx, "A")
	if err != nil {
		t.Fatalf("CabezaDeSesion tras el sello: %v", err)
	}
	if cab2 != nil {
		t.Fatalf("una fila sellada no puede seguir siendo cabeza, got %s", resumenCabeza(cab2))
	}
}

// TestMarcarDespachadaFueraDeClasificadoEsNoOp: el sello sólo muerde sobre `clasificado`.
//
// Dos caminos, y los dos importan: (1) sobre una fila `nuevo` no escribe nada —sellar lo que el cajero
// aún no ha mirado sería entregar un mensaje sin darle su turno—; (2) sobre una fila YA `despachado`
// tampoco, y ese es el que protege el TTL: un segundo sello movería `despachado_en` hacia adelante y
// retrasaría la poda de esa fila tanto como durase el bucle que lo repitiera.
func TestMarcarDespachadaFueraDeClasificadoEsNoOp(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	sembrarNuevo(t, db, 1, 10, "A", "chat@s", "wa-nueva", "aún sin clasificar")
	sembrar(t, db, 2, 20, "A", "chat@s", "wa-vieja", "ya salió", app.EstadoClasificado, sql.NullInt64{})
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0, WithClock(clock.Now))

	// (1) Sobre una fila `nuevo`: no-op SIN error (el despachador la relee y decide otra vez).
	if err := s.MarcarDespachada(ctx, 1); err != nil {
		t.Fatalf("sellar una fila 'nuevo' debía ser no-op sin error: %v", err)
	}
	if got := estadoDe(t, db, 1); got != app.EstadoNuevo {
		t.Fatalf("la fila 'nuevo' cambió de estado a %q: el sello no puede saltarse al cajero", got)
	}
	if sello := despachadoEnDe(t, db, 1); sello.Valid {
		t.Fatalf("la fila 'nuevo' quedó con despachado_en=%d: la poda podría borrar un mensaje jamás entregado", sello.Int64)
	}

	// (2) Sobre una fila ya sellada: el segundo sello NO mueve el reloj de la poda.
	if err := s.MarcarDespachada(ctx, 2); err != nil {
		t.Fatalf("MarcarDespachada (1ª): %v", err)
	}
	primerSello := despachadoEnDe(t, db, 2)
	if !primerSello.Valid {
		t.Fatal("el primer sello debía escribirse")
	}
	clock.t = clock.t.Add(6 * time.Hour)
	if err := s.MarcarDespachada(ctx, 2); err != nil {
		t.Fatalf("MarcarDespachada (2ª): %v", err)
	}
	if segundo := despachadoEnDe(t, db, 2); segundo.Int64 != primerSello.Int64 {
		t.Fatalf("el segundo sello movió despachado_en de %d a %d: retrasaría la poda de esa fila indefinidamente",
			primerSello.Int64, segundo.Int64)
	}

	// Una fila que ya no existe (la borró el tope entre la entrega y el sello) tampoco es un error.
	if err := s.MarcarDespachada(ctx, 999); err != nil {
		t.Fatalf("sellar una fila inexistente debía ser no-op sin error: %v", err)
	}
}

// ─────────────────────────── (c) DespacharSinIntent ───────────────────────────

// TestDespacharSinIntentDejaLaFilaClasificadaConSuSobreYSueltaElClaim: el camino del presupuesto vencido
// escribe las CUATRO cosas de una vez —sobre de omisión, `estado='clasificado'`, `tomado_en` y
// `claim_token` a NULL— y lo hace tanto sobre una fila `nuevo` (el cajero ni la miró) como sobre una
// `tomado` (la tiene reclamada y no ha terminado).
//
// 🔴 LO QUE **NO** ESCRIBE ES LA MITAD IMPORTANTE DE ESTE TEST (arreglo del 2026-08-17): ni
// `estado='despachado'` ni `despachado_en`. Hasta ese día sí lo hacía, y esa era la pérdida de mensajes:
// `sqlCabezaDeSesion` excluye `despachado`, así que la fila desaparecía del alcance del despachador ANTES
// de haber salido al cable, y el TTL se la llevaba a las 24 h sin que nadie la hubiera entregado. El sello
// terminal es de `MarcarDespachada`, DESPUÉS de la entrega — el mismo orden «entrega antes de sello» que
// rige en el resto del despachador, porque un duplicado es un incidente de idempotencia y una pérdida es
// un incidente de negocio.
//
// Este test fija la mitad del contrato que vive en el SQL; que el despachador entregue y selle después lo
// fija circuito_ola3_test.go, contra el bucle real.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - poner `estado = 'despachado'` en el SET de `sqlDespacharSinIntent` (volver al bug) ⇒ FALLA en la
//     aserción de estado Y en la de «sigue siendo cabeza»;
//   - añadir `despachado_en = ?` a ese SET ⇒ FALLA en la aserción de `despachado_en` NULL, que es la que
//     protege contra que el TTL pode una fila jamás entregada;
//   - quitar `intent_json = ?` ⇒ la fila queda `clasificado` sin sobre, indistinguible de un fragmento de
//     lote: FALLA en la aserción del sobre;
//   - quitar `tomado_en = NULL` o `claim_token = NULL` ⇒ FALLA en las aserciones del claim (y con ellas se
//     caería el fence contra el cierre tardío del cajero);
//   - relajar el `WHERE ... estado IN ('nuevo','tomado')` ⇒ no lo caza este test sino
//     TestDespacharSinIntentSobreFilaClasificadaEsNoOpYSeCuenta.
func TestDespacharSinIntentDejaLaFilaClasificadaConSuSobreYSueltaElClaim(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}

	sembrarNuevo(t, db, 1, 10, "A", "chat@s", "wa-nueva", "el cajero ni la miró")
	sembrarTomado(t, db, 2, 20, "A", "chat@s", "wa-tomada", "el cajero la tiene", clock.t.Unix())
	if _, err := db.Exec(`UPDATE cola_entrantes SET claim_token = 'deadbeefdeadbeef' WHERE id = 2`); err != nil {
		t.Fatalf("poner el token del claim: %v", err)
	}

	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0, WithClock(clock.Now))
	sobre := app.SobreOmitido(app.MotivoPresupuesto)

	for _, id := range []int64{1, 2} {
		if err := s.DespacharSinIntent(ctx, id, app.MotivoPresupuesto); err != nil {
			t.Fatalf("DespacharSinIntent(id=%d): %v", id, err)
		}
		if got := estadoDe(t, db, id); got != app.EstadoClasificado {
			t.Fatalf("id=%d: estado %q, want %q. Un 'despachado' aquí significa que la fila sale del alcance "+
				"de CabezaDeSesion sin haberse entregado: el mensaje se pierde", id, got, app.EstadoClasificado)
		}
		if got := intentDe(t, db, id); !got.Valid || got.String != sobre {
			t.Fatalf("id=%d: intent_json %+v, want el sobre %q", id, got, sobre)
		}
		// 🔴 NADA DE SELLO TODAVÍA. La poda exige `despachado` Y `despachado_en`; escribirlo aquí sería
		// declarar entregado un mensaje que aún no ha salido, y dejar que el TTL lo borre a las 24 h.
		if sello := despachadoEnDe(t, db, id); sello.Valid {
			t.Fatalf("id=%d: despachado_en quedó en %d sobre una fila que NADIE ha entregado aún; el sello es "+
				"de MarcarDespachada, tras la entrega", id, sello.Int64)
		}
		// El claim se suelta: una fila que ya no es del cajero no puede seguir diciendo que la tiene él.
		// Y esto es, además, la mitad del fence que hace rebotar su cierre tardío.
		if got := tomadoEnDe(t, db, id); got.Valid {
			t.Fatalf("id=%d: tomado_en quedó en %d; la fila ya no pertenece a ningún claim", id, got.Int64)
		}
		if got := claimTokenDe(t, db, id); got.Valid {
			t.Fatalf("id=%d: claim_token quedó en %q; la fila ya no pertenece a ningún claim", id, got.String)
		}
	}
	// Y SIGUE HABIENDO CABEZA: la fila 1, ya `clasificado` con su sobre, esperando que el despachador la
	// entregue. Es exactamente lo contrario de lo que este test afirmaba antes del arreglo.
	cab, err := s.CabezaDeSesion(ctx, "A")
	if err != nil {
		t.Fatalf("CabezaDeSesion: %v", err)
	}
	if cab == nil || cab.ID != 1 || cab.Estado != app.EstadoClasificado || cab.IntentJSON != sobre {
		t.Fatalf("la cabeza debía seguir siendo la fila 1, `clasificado` y con su sobre de omisión, lista para "+
			"entregarse; got %s", resumenCabeza(cab))
	}
	// El contador de no-aplicados NO se mueve en el camino en que la sentencia SÍ mordió.
	if got := s.DespachosSinIntentNoAplicados(); got != 0 {
		t.Fatalf("DespachosSinIntentNoAplicados = %d, want 0 (las dos sentencias SÍ aterrizaron)", got)
	}
}

// TestDespacharSinIntentSobreFilaClasificadaEsNoOpYSeCuenta: la carrera que se resuelve del lado BUENO.
// El presupuesto venció, pero el cajero acababa de cerrar la fila; el sello de omisión no toca nada, el
// intent REAL sobrevive y el despachador la relee como `clasificado` para entregarla con él.
//
// Se cuenta porque nadie más puede: `DespacharSinIntent` devuelve sólo `error`, y el no-op no es error,
// así que desde fuera «sellé» y «llegué tarde» son indistinguibles. Y no es un caso raro: el
// presupuesto son 4000 ms y la p95 medida de una inferencia, 3.736 ms.
func TestDespacharSinIntentSobreFilaClasificadaEsNoOpYSeCuenta(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	const intentReal = `{"intent":"crear_pedido","confidence":0.98}`
	sembrar(t, db, 1, 10, "A", "chat@s", "wa-1", "quiero dos empanadas", app.EstadoClasificado, sql.NullInt64{})
	marcarEnBD(t, db, 1, app.EstadoClasificado, intentReal)

	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)
	if got := s.DespachosSinIntentNoAplicados(); got != 0 {
		t.Fatalf("el acumulado debía arrancar en 0, got %d", got)
	}

	if err := s.DespacharSinIntent(ctx, 1, app.MotivoPresupuesto); err != nil {
		t.Fatalf("llegar tarde NO es un error (el despachador la relee): %v", err)
	}
	if got := estadoDe(t, db, 1); got != app.EstadoClasificado {
		t.Fatalf("estado tras el no-op: got %q want %q (el sello no puede saltarse el cierre del cajero)", got, app.EstadoClasificado)
	}
	if got := intentDe(t, db, 1); !got.Valid || got.String != intentReal {
		t.Fatalf("el sobre de omisión PISÓ el intent real del cajero: got %+v want %q", got, intentReal)
	}
	if sello := despachadoEnDe(t, db, 1); sello.Valid {
		t.Fatalf("el no-op selló despachado_en=%d sobre una fila que aún no se había entregado", sello.Int64)
	}
	if got := s.DespachosSinIntentNoAplicados(); got != 1 {
		t.Fatalf("DespachosSinIntentNoAplicados = %d, want 1 (INV-051.3: se cuenta, no sólo se loguea)", got)
	}

	// Y sigue siendo cabeza, ahora lista para entregarse CON su intent — el desenlace bueno.
	cab, err := s.CabezaDeSesion(ctx, "A")
	if err != nil || cab == nil {
		t.Fatalf("CabezaDeSesion: cabeza=%s err=%v", resumenCabeza(cab), err)
	}
	if cab.Estado != app.EstadoClasificado || cab.IntentJSON != intentReal {
		t.Fatalf("la cabeza debía releerse como clasificada con su intent real, got %s (%q)",
			resumenCabeza(cab), cab.IntentJSON)
	}
}

// TestDespacharSinIntentMotivoFueraDeLaListaFalla: un motivo sin sobre precalculado se rechaza ANTES de
// tocar la BD.
//
// 🔴 EL FALLO QUE ESTO EVITA NO ES EL ERROR, ES LA ALTERNATIVA: `SobreOmitido` de un motivo desconocido
// devuelve "", y "" en esta columna significa NULL, es decir «esta fila NO pasó por el cajero»
// (INV-051.3) — lo contrario del hecho que se estaba registrando. Se persistiría en disco, para
// siempre, una fila `despachado` que afirma que nunca se clasificó, y el motivo real se perdería sin
// rastro. Sólo se llega aquí añadiendo un MotivoOmitido y olvidando meterlo en la lista canónica.
func TestDespacharSinIntentMotivoFueraDeLaListaFalla(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	sembrarNuevo(t, db, 1, 10, "A", "chat@s", "wa-1", "hola")
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0)

	err := s.DespacharSinIntent(ctx, 1, app.MotivoOmitido("motivo-que-nadie-declaró"))
	if !errors.Is(err, app.ErrMotivoOmitidoDesconocido) {
		t.Fatalf("un motivo fuera de la lista canónica debía dar app.ErrMotivoOmitidoDesconocido; got %v", err)
	}
	// Y la fila no se tocó: mejor una cabeza atascada y ruidosa que una mentira persistida.
	if got := estadoDe(t, db, 1); got != app.EstadoNuevo {
		t.Fatalf("la fila cambió a %q pese al error", got)
	}
	if got := intentDe(t, db, 1); got.Valid {
		t.Fatalf("la fila ganó un intent_json (%q) pese al error", got.String)
	}

	// Los OCHO motivos de la lista canónica SÍ pasan: se recorre la lista, jamás se teclea a mano
	// (misma regla que la telemetría de la Ola 4).
	for i, motivo := range app.MotivosOmitido() {
		id := int64(100 + i)
		sembrarNuevo(t, db, id, int64(1000+i), "A", "chat@s", fmt.Sprintf("wa-m%d", i), "hola")
		if err := s.DespacharSinIntent(ctx, id, motivo); err != nil {
			t.Fatalf("el motivo canónico %q debía tener sobre: %v", motivo, err)
		}
		if got := intentDe(t, db, id); !got.Valid || got.String != app.SobreOmitido(motivo) {
			t.Fatalf("motivo %q: intent_json %+v, want %q", motivo, got, app.SobreOmitido(motivo))
		}
	}
}

// ────────── EL TEST DEL CRITERIO: el sello tardío del cajero NO revive la fila ──────────

// TestCierreTardioDelCajeroTrasDespacharSinIntentEsNoOp es LA ASERCIÓN QUE T3.2 EXISTE PARA SOSTENER
// (REQ-051.20, última frase): «un sello tardío del worker sobre una fila que el despachador ya resolvió
// no deberá tener efecto».
//
// La secuencia es la de campo: (1) el cajero reclama la fila y se pone a inferir; (2) el despachador
// agota su presupuesto de 4000 ms y la resuelve SIN intent; (3) el cajero termina —tarde— y cierra
// igual. Ese cierre tiene que descartarse ENTERO: si escribiera, PISARÍA el sobre `{"omitido":…}` con un
// intent sobre un mensaje cuya suerte ya estaba decidida.
//
// 🔴 EL ARREGLO DEL 2026-08-17 MOVIÓ EL MOMENTO, NO EL RESULTADO. Antes, DespacharSinIntent dejaba la
// fila en `despachado` y el cierre tardío rebotaba contra un estado TERMINAL. Ahora la deja
// `clasificado` con el claim SUELTO, y el cierre rebota igual —pero contra la otra mitad del fence—.
// Por eso este test cubre AHORA LOS DOS MOMENTOS: el cierre tardío contra la fila resuelta-pero-no-
// entregada (§3) y contra la fila ya sellada (§5). El primero no existía antes y es el que la ventana
// nueva abrió.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (medido leyendo el SQL, no supuesto):
//
//   - quitar `AND estado = ?` Y `AND claim_token = ?` de las DOS sentencias de MarcarClasificado ⇒ el
//     cierre tardío escribe su intent y devuelve la fila a `clasificado`: FALLA en las aserciones de
//     intent (§3 y §5) y de estado (§5);
//   - quitar SÓLO `claim_token = NULL` del SET de sqlDespacharSinIntent ⇒ en §3 la fila sigue con su
//     token y el estado es `clasificado`, no `tomado`: el fence aún muerde por estado y el test pasa;
//     es TestMarcarClasificadoTardioConElRelevoAUNVIVO (claim_test.go) quien cubre el predicado del
//     token por sí solo;
//   - quitar `estado = ?` del SET de sqlDespacharSinIntent (dejar la fila en `tomado`) ⇒ el fence del
//     cajero vuelve a morder ENTERO y el cierre tardío TRIUNFA: FALLA en el ErrLoteRelevado de §3;
//   - volver a sellar `despachado` en sqlDespacharSinIntent ⇒ FALLA en la aserción de §3 de que la fila
//     sigue siendo cabeza, que es la que impide que el mensaje se pierda.
func TestCierreTardioDelCajeroTrasDespacharSinIntentEsNoOp(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0, WithClock(clock.Now))

	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-1", "quiero dos empanadas")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// (1) El cajero reclama y se pone a inferir. La fila queda `tomado` con SU token.
	lote, err := s.Reclamar(ctx, 0)
	if err != nil || lote == nil || len(lote.Mensajes) != 1 {
		t.Fatalf("Reclamar: lote=%s err=%v", resumenLote(lote), err)
	}
	if lote.ClaimToken == "" {
		t.Fatal("Reclamar no devolvió token de fencing: sin él no hay nada que probar aquí")
	}
	cab, err := s.CabezaDeSesion(ctx, "A")
	if err != nil || cab == nil {
		t.Fatalf("CabezaDeSesion: cabeza=%s err=%v", resumenCabeza(cab), err)
	}
	if cab.Estado != app.EstadoTomado || cab.TieneIntent {
		t.Fatalf("la cabeza debía estar `tomado` y sin intent (el cajero aún infiere), got %s", resumenCabeza(cab))
	}

	// (2) El presupuesto vence: la fila se resuelve SIN intent. Queda `clasificado`, con su sobre, sin
	// claim y SIN sello — todavía no ha salido al cable.
	clock.t = clock.t.Add(4 * time.Second) // WAPP_AGENT_INTENT_WAIT_MS = 4000
	if err := s.DespacharSinIntent(ctx, cab.ID, app.MotivoPresupuesto); err != nil {
		t.Fatalf("DespacharSinIntent: %v", err)
	}
	if got := despachadoEnDe(t, db, cab.ID); got.Valid {
		t.Fatalf("despachado_en = %d sobre una fila que aún no se ha entregado: el sello es de "+
			"MarcarDespachada, y ponerlo antes deja que el TTL borre un mensaje que nunca salió", got.Int64)
	}

	// (3) EL CAJERO TERMINA TARDE Y CIERRA IGUAL, con la fila resuelta pero AÚN SIN ENTREGAR. Aquí es
	// donde tiene que rebotar, y contra los DOS predicados del fence a la vez (`estado='tomado'` ya no se
	// cumple, y `claim_token` es NULL).
	clock.t = clock.t.Add(2 * time.Second)
	errCierre := s.MarcarClasificado(ctx, lote, `{"intent":"crear_pedido","confidence":0.98}`)
	if !errors.Is(errCierre, app.ErrLoteRelevado) {
		t.Fatalf("el cierre tardío sobre una fila YA resuelta por presupuesto debía devolver app.ErrLoteRelevado; got %v", errCierre)
	}

	// Y no tocó NADA de la fila.
	sobre := app.SobreOmitido(app.MotivoPresupuesto)
	if got := estadoDe(t, db, cab.ID); got != app.EstadoClasificado {
		t.Fatalf("el cierre tardío movió la fila a %q", got)
	}
	if got := intentDe(t, db, cab.ID); !got.Valid || got.String != sobre {
		t.Fatalf("el cierre tardío PISÓ el sobre de omisión con un intent que nadie pidió: got %+v want %q", got, sobre)
	}
	if got := despachadoEnDe(t, db, cab.ID); got.Valid {
		t.Fatalf("el cierre tardío selló despachado_en=%d", got.Int64)
	}
	if got := claimTokenDe(t, db, cab.ID); got.Valid {
		t.Fatalf("la fila recuperó un claim_token (%q)", got.String)
	}
	if got := tomadoEnDe(t, db, cab.ID); got.Valid {
		t.Fatalf("la fila recuperó un lease (tomado_en=%d)", got.Int64)
	}

	// (4) LA FILA SIGUE SIENDO CABEZA, y ESTA es la aserción que impide que el mensaje se pierda: el
	// despachador la releerá, verá el sobre de omisión y la entregará sin intención.
	cab2, err := s.CabezaDeSesion(ctx, "A")
	if err != nil {
		t.Fatalf("CabezaDeSesion tras el cierre tardío: %v", err)
	}
	if cab2 == nil || cab2.ID != cab.ID || cab2.Estado != app.EstadoClasificado || cab2.IntentJSON != sobre {
		t.Fatalf("la fila debía seguir siendo cabeza, `clasificado` y con su sobre, esperando entrega; got %s. "+
			"Si es nil, la sentencia volvió a sellar 'despachado' y el mensaje NUNCA sale al cable", resumenCabeza(cab2))
	}

	// (5) EL DESPACHADOR LA ENTREGA (aquí lo simula el test) y la sella. A partir de ahora el cierre tardío
	// rebota contra un estado TERMINAL, que era el único momento que este test cubría antes del arreglo.
	if err := s.MarcarDespachada(ctx, cab.ID); err != nil {
		t.Fatalf("MarcarDespachada: %v", err)
	}
	selloDespacho := despachadoEnDe(t, db, cab.ID)
	if !selloDespacho.Valid {
		t.Fatal("la entrega debía sellar despachado_en: sin él, el TTL no podaría esta fila jamás")
	}
	clock.t = clock.t.Add(2 * time.Second)
	if err := s.MarcarClasificado(ctx, lote, `{"intent":"crear_pedido","confidence":0.98}`); !errors.Is(err, app.ErrLoteRelevado) {
		t.Fatalf("el cierre tardío sobre una fila YA despachada debía devolver app.ErrLoteRelevado; got %v", err)
	}
	if got := estadoDe(t, db, cab.ID); got != app.EstadoDespachado {
		t.Fatalf("el cierre tardío sacó la fila de 'despachado' y la dejó en %q: el TTL ya no la podaría "+
			"y el despachador la re-entregaría", got)
	}
	if got := intentDe(t, db, cab.ID); !got.Valid || got.String != sobre {
		t.Fatalf("el cierre tardío PISÓ el sobre con un intent que nadie recibió: got %+v want %q", got, sobre)
	}
	if got := despachadoEnDe(t, db, cab.ID); got.Int64 != selloDespacho.Int64 {
		t.Fatalf("el cierre tardío movió despachado_en de %d a %d", selloDespacho.Int64, got.Int64)
	}
	// Y ya no es cabeza de nada: el despachador no la re-entrega.
	if cab3, err := s.CabezaDeSesion(ctx, "A"); err != nil || cab3 != nil {
		t.Fatalf("tras el sello la fila no puede seguir siendo cabeza, got cabeza=%s err=%v", resumenCabeza(cab3), err)
	}
	// INV-051.3: CADA cierre tirado se CUENTA (una inferencia pagada y perdida), no sólo se loguea. Son
	// dos: el de §3 y el de §5.
	if got := s.CierresDescartadosPorFence(); got != 2 {
		t.Fatalf("CierresDescartadosPorFence = %d, want 2 (un cierre tardío antes de la entrega y otro después)", got)
	}
}

// TestDespacharSinIntentDescartaElCierreDeTodoElLoteYElBarridoRescataLasHermanas documenta —y fija— la
// CONSECUENCIA NO OBVIA del test anterior cuando el lote tiene más de una fila.
//
// El cajero clasifica por LOTES (la conversación entera) y el despachador resuelve de UNA EN UNA (la
// cabeza). Si el presupuesto sólo venció para la cabeza, el cierre del cajero se descarta ENTERO —el
// fence es todo-o-nada— y las hermanas se quedan `tomado` con el token de un claim ya muerto. NO se
// pierden. Lo que se pierde es UNA INFERENCIA.
//
// ⚠️ ESTE TEST EJERCITA LA RED DE ABAJO, NO EL CAMINO NORMAL, y hay que leerlo sabiéndolo. Aquí NO hay
// despachador corriendo: el test lo simula a mano, y entre el sobre de la cabeza y el barrido no ejecuta
// ninguna vuelta de bucle. Con el despachador CABLEADO, en cuanto la hermana pasa a ser cabeza le corre
// su propio presupuesto y a los ~4000 ms la resuelve él con `DespacharSinIntent` (la sentencia muerde
// sobre `tomado`): sale SIN intención y el barrido de los 60 s no llega a ver nada. El rescate por lease
// que se prueba abajo es lo que ocurre cuando el despachador de esa sesión NO está corriendo — la red de
// la red. El SQL es el mismo en los dos casos, y es el SQL lo que este test fija.
//
// Este test existe para que ese comportamiento sea una DECISIÓN escrita y no un descubrimiento de
// campo: si alguien lo cambia (dejando cerrar parcialmente al cajero), este test se lo dice.
func TestDespacharSinIntentDescartaElCierreDeTodoElLoteYElBarridoRescataLasHermanas(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0, WithClock(clock.Now))

	for i := 1; i <= 2; i++ {
		if err := s.Enqueue(ctx, item("A", "chat@s", fmt.Sprintf("wa-%d", i), fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	lote, err := s.Reclamar(ctx, 0)
	if err != nil || lote == nil || len(lote.Mensajes) != 2 {
		t.Fatalf("Reclamar: lote=%s err=%v", resumenLote(lote), err)
	}
	cabeza, hermana := lote.Mensajes[0], lote.Mensajes[1]

	// El presupuesto vence SÓLO para la cabeza (es la única que el despachador estaba esperando).
	if err := s.DespacharSinIntent(ctx, cabeza.ID, app.MotivoPresupuesto); err != nil {
		t.Fatalf("DespacharSinIntent: %v", err)
	}

	// El cajero cierra: TODO O NADA ⇒ ni la hermana, que seguía siendo suya, se cierra.
	if err := s.MarcarClasificado(ctx, lote, `{"intent":"crear_pedido"}`); !errors.Is(err, app.ErrLoteRelevado) {
		t.Fatalf("el cierre del lote debía descartarse entero; got %v", err)
	}
	if got := estadoDe(t, db, hermana.ID); got != app.EstadoTomado {
		t.Fatalf("la hermana quedó en %q: el cierre debía revertirse ENTERO, dejándola como estaba", got)
	}
	if got := intentDe(t, db, hermana.ID); got.Valid {
		t.Fatalf("la hermana ganó un intent de un cierre revertido: %q", got.String)
	}

	// LA RED: el barrido la devuelve a `nuevo` y el claim siguiente la reclasifica. No se pierde.
	clock.t = clock.t.Add(61 * time.Second)
	n, err := s.BarrerLeasesVencidos(ctx, 0)
	if err != nil {
		t.Fatalf("BarrerLeasesVencidos: %v", err)
	}
	if n != 1 {
		t.Fatalf("el barrido debía rescatar SOLO la hermana, rescató %d", n)
	}
	lote2, err := s.Reclamar(ctx, 0)
	if err != nil || lote2 == nil || len(lote2.Mensajes) != 1 || lote2.Mensajes[0].ID != hermana.ID {
		t.Fatalf("el claim siguiente debía recuperar la hermana, got %s", resumenLote(lote2))
	}
	if err := s.MarcarClasificado(ctx, lote2, `{"intent":"crear_pedido","confidence":0.9}`); err != nil {
		t.Fatalf("el cajero debía poder cerrar el re-claim: %v", err)
	}

	// EL ORDEN DE SALIDA, QUE ES DONDE SE VE EL ARREGLO DEL 2026-08-17. La cabeza SIGUE siendo la fila 1
	// —resuelta por presupuesto, `clasificado` con su sobre de omisión y todavía sin entregar—, no la
	// hermana. Antes del arreglo esta fila ya estaba `despachado` sin haber salido al cable jamás, y la
	// hermana la adelantaba: el FIFO se rompía por la vía de perder el mensaje anterior.
	cab, err := s.CabezaDeSesion(ctx, "A")
	if err != nil || cab == nil {
		t.Fatalf("CabezaDeSesion: cabeza=%s err=%v", resumenCabeza(cab), err)
	}
	if cab.ID != cabeza.ID || cab.Estado != app.EstadoClasificado ||
		cab.IntentJSON != app.SobreOmitido(app.MotivoPresupuesto) {
		t.Fatalf("la cabeza debía seguir siendo la fila resuelta por presupuesto, con su sobre y pendiente de "+
			"entrega; got %s. Si es la hermana, la fila 1 se selló sin entregarse y su mensaje se perdió",
			resumenCabeza(cab))
	}
	// Se entrega (el test lo simula) y se sella. Sólo entonces avanza la cabeza a la hermana.
	if err := s.MarcarDespachada(ctx, cab.ID); err != nil {
		t.Fatalf("MarcarDespachada (cabeza): %v", err)
	}
	cab, err = s.CabezaDeSesion(ctx, "A")
	if err != nil || cab == nil {
		t.Fatalf("CabezaDeSesion (2ª): cabeza=%s err=%v", resumenCabeza(cab), err)
	}
	if cab.ID != hermana.ID || cab.Estado != app.EstadoClasificado || !cab.TieneIntent {
		t.Fatalf("la cabeza debía ser la hermana ya clasificada, got %s", resumenCabeza(cab))
	}
	if err := s.MarcarDespachada(ctx, cab.ID); err != nil {
		t.Fatalf("MarcarDespachada (hermana): %v", err)
	}
	if cab, err := s.CabezaDeSesion(ctx, "A"); err != nil || cab != nil {
		t.Fatalf("la sesión debía quedar al día, got cabeza=%s err=%v", resumenCabeza(cab), err)
	}
	// Una inferencia pagada y tirada; ningún mensaje perdido — LOS DOS salieron, y en orden.
	if got := s.CierresDescartadosPorFence(); got != 1 {
		t.Fatalf("CierresDescartadosPorFence = %d, want 1", got)
	}
}

// ────────── EL OTRO CRITERIO: el TTL, des-inertizado ──────────

// TestTTLPodaPorFinLasFilasQueElDespachadorSella es la prueba de que esta tarea DESBLOQUEA T1.6.
//
// 🔴 EL PUNTO ENTERO: `pruneTTLLocked` lleva desde la Ola 1 corriendo en CADA Enqueue y no ha podido
// borrar jamás una sola fila, porque exige `estado='despachado' AND despachado_en IS NOT NULL` y NADIE
// escribía ninguna de las dos cosas (tasks.md T1.6: «el TTL es inerte hasta la O3; lo activa T3.2»).
// Con el sello de MarcarDespachada puesto, y el reloj adelantado 25 h, la poda por fin recoge.
//
// SE CUBREN LOS DOS CAMINOS QUE LLEGAN AL SELLO, porque son distintos y sólo uno estaba probado aquí:
// el del CAJERO (encolar → reclamar → clasificar → entregar → MarcarDespachada) y el del PRESUPUESTO
// VENCIDO (encolar → reclamar → DespacharSinIntent → entregar → MarcarDespachada). El segundo termina en
// la MISMA sentencia desde el arreglo del 2026-08-17; antes sellaba por su cuenta, y esa era exactamente
// la forma de que una fila acabara podada sin haberse entregado nunca.
//
// Y lo que la poda sigue SIN tocar, que es igual de importante (REQ-051.7 · ADR-0038 §Enmienda 1):
// la fila pendiente por vieja que sea, y la fila `despachado` SIN sello —un `despachado` sin
// `despachado_en` es un bug del despachador, y ante la duda no se borra—.
func TestTTLPodaPorFinLasFilasQueElDespachadorSella(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 24, WithClock(clock.Now)) // TTL de 24 h, el de producción

	// (1) Una fila que recorre el ciclo COMPLETO por el camino real: encolar → reclamar → clasificar →
	//     despachar. Es la única forma honesta de probar que el sello que escribe producción es el que
	//     la poda sabe leer.
	if err := s.Enqueue(ctx, item("A", "chat-1@s", "wa-despachada", "quiero dos empanadas")); err != nil {
		t.Fatalf("Enqueue despachada: %v", err)
	}
	lote, err := s.Reclamar(ctx, 0)
	if err != nil || lote == nil {
		t.Fatalf("Reclamar: lote=%s err=%v", resumenLote(lote), err)
	}
	if err := s.MarcarClasificado(ctx, lote, `{"intent":"crear_pedido"}`); err != nil {
		t.Fatalf("MarcarClasificado: %v", err)
	}
	if err := s.MarcarDespachada(ctx, lote.Mensajes[0].ID); err != nil {
		t.Fatalf("MarcarDespachada: %v", err)
	}

	// (2) EL OTRO CAMINO COMPLETO: el del PRESUPUESTO VENCIDO, recorrido HASTA EL SELLO FINAL y no hasta
	//     el sobre de omisión.
	//
	//     🔴 SON DOS SENTENCIAS, NO UNA, Y ESE ES EL PUNTO (arreglo del 2026-08-17). DespacharSinIntent
	//     sólo deja la fila `clasificado` con su sobre; quien escribe `estado='despachado'` y
	//     `despachado_en` —lo único que la poda sabe leer— es MarcarDespachada, DESPUÉS de la entrega. Si
	//     alguien acortara el camino y volviera a sellar en la primera sentencia, la fila se podaría igual
	//     que aquí… pero sin haber salido nunca al cable, que es justo el bug que este orden previene.
	//     Parar este tramo en el sobre habría dejado sin cubrir la mitad que de verdad des-inertiza el TTL.
	if err := s.Enqueue(ctx, item("A", "chat-5@s", "wa-por-presupuesto", "el cajero se atascó")); err != nil {
		t.Fatalf("Enqueue por presupuesto: %v", err)
	}
	lotePresu, err := s.Reclamar(ctx, 0)
	if err != nil || lotePresu == nil || len(lotePresu.Mensajes) != 1 {
		t.Fatalf("Reclamar (la que vencerá por presupuesto): lote=%s err=%v", resumenLote(lotePresu), err)
	}
	idPresu := lotePresu.Mensajes[0].ID
	if err := s.DespacharSinIntent(ctx, idPresu, app.MotivoPresupuesto); err != nil {
		t.Fatalf("DespacharSinIntent: %v", err)
	}
	if got := despachadoEnDe(t, db, idPresu); got.Valid {
		t.Fatalf("el sobre de omisión selló despachado_en=%d: la fila aún no se ha entregado y la poda "+
			"podría llevársela sin que nadie la haya subido", got.Int64)
	}
	if err := s.MarcarDespachada(ctx, idPresu); err != nil { // la entrega ocurrió; ahora sí, el sello
		t.Fatalf("MarcarDespachada (por presupuesto): %v", err)
	}

	// (3) Una fila pendiente que NUNCA se despacha: la poda no puede tocarla por vieja que sea.
	if err := s.Enqueue(ctx, item("A", "chat-2@s", "wa-pendiente", "hola")); err != nil {
		t.Fatalf("Enqueue pendiente: %v", err)
	}
	// (4) Una fila `despachado` SIN sello (bug del despachador): ante la duda, no se borra.
	sinSello := item("A", "chat-3@s", "wa-sin-sello", "hola")
	sinSello.Estado = app.EstadoDespachado
	if err := s.Enqueue(ctx, sinSello); err != nil {
		t.Fatalf("Enqueue despachada sin sello: %v", err)
	}

	// Foto previa: las cuatro están.
	if ids := waIDs(t, db); len(ids) != 4 {
		t.Fatalf("antes de la poda debían estar las 4 filas, hay %v", ids)
	}

	// (5) +25 h y un Enqueue cualquiera, que es quien dispara la poda.
	clock.t = clock.t.Add(25 * time.Hour)
	if err := s.Enqueue(ctx, item("A", "chat-4@s", "wa-disparador", "hola")); err != nil {
		t.Fatalf("Enqueue disparador: %v", err)
	}

	if existe(t, db, "wa-despachada") {
		t.Fatal("EL TTL SIGUE INERTE: la fila sellada por el despachador sobrevivió a las 25 h. " +
			"Revisa que MarcarDespachada escriba estado='despachado' Y despachado_en en EPOCH-SEGUNDOS")
	}
	if existe(t, db, "wa-por-presupuesto") {
		t.Fatal("EL TTL SIGUE INERTE PARA EL CAMINO DEL PRESUPUESTO: la fila que se resolvió con " +
			"DespacharSinIntent y se selló al entregarse sobrevivió a las 25 h")
	}
	if !existe(t, db, "wa-pendiente") {
		t.Fatal("la poda se llevó una fila JAMÁS despachada (REQ-051.7): eso es perder un mensaje")
	}
	if !existe(t, db, "wa-sin-sello") {
		t.Fatal("la poda se llevó una fila 'despachado' SIN despachado_en: ante la duda no se borra")
	}
	if ids := waIDs(t, db); len(ids) != 3 {
		t.Fatalf("tras la poda debían quedar 3 filas (pendiente, sin-sello, disparador), hay %v", ids)
	}
}

// TestTTLNoPodaLaFilaDespachadaAntesDeTiempo: el sello no es una condena inmediata. Dentro del TTL la
// fila despachada SIGUE ahí — es lo que permite diagnosticar en campo qué se entregó hace un rato.
func TestTTLNoPodaLaFilaDespachadaAntesDeTiempo(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 24, WithClock(clock.Now))

	sembrar(t, db, 1, 10, "A", "chat@s", "wa-1", "hola", app.EstadoClasificado, sql.NullInt64{})
	if err := s.MarcarDespachada(ctx, 1); err != nil {
		t.Fatalf("MarcarDespachada: %v", err)
	}

	clock.t = clock.t.Add(23 * time.Hour) // dentro del TTL
	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-2", "hola")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !existe(t, db, "wa-1") {
		t.Fatal("a las 23 h la fila despachada aún no ha cumplido el TTL de 24 h y no debía podarse")
	}

	clock.t = clock.t.Add(2 * time.Hour) // ahora sí: 25 h
	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-3", "hola")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if existe(t, db, "wa-1") {
		t.Fatal("a las 25 h la fila despachada debía haberse podado")
	}
}
