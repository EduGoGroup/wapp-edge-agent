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

	// LA CABEZA SÓLO AVANZA CUANDO LA PRIMERA SE SELLA, y el ÚNICO que sella es `MarcarDespachada`.
	//
	// 🔴 AQUÍ HABÍA UN TRAMO MÁS, retirado el 2026-08-24 con `DespacharSinIntent` (T1.6-5): comprobaba que
	// aquella sentencia NO hacía avanzar la cabeza —dejaba la fila `clasificado`, todavía pendiente de
	// entregarse— porque hasta el 2026-08-17 sellaba `despachado` por su cuenta y el mensaje se perdía sin
	// salir al cable. Con la sentencia retirada vuelve a haber UN SOLO escritor del estado terminal, que es
	// lo que esta aserción fija hoy: entrega (aquí la simula el test) y luego sello.
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

// TestCabezaDeSesionDistingueIntentNULLDeSobrePresente fija INV-051.3 en el lado del despachador: la
// columna NULL y la columna CON sobre llegan al bucle como dos hechos distinguibles (`TieneIntent`).
//
// ⚠️ LO QUE SIGNIFICABAN CAMBIÓ EN T1.6-5, LO QUE SE PRUEBA NO. Bajo push, NULL significaba «aún no pasó
// por el cajero» —había que esperarle— y un sobre significaba «resuelta, lista para salir»: dos decisiones
// OPUESTAS del bucle. Bajo pull las dos filas se entregan igual; lo que sigue dependiendo de esta
// distinción es la TELEMETRÍA (qué serie se incrementa), y una columna vacía leída como sobre —o al revés—
// seguiría falseándola. Por eso el test se conserva entero.
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

	// Un binario ANTERIOR a T1.6-5 la dejó `clasificado` con su sobre de omisión (el camino del fastlane o
	// el cierre del cajero). Es la única forma en que hoy puede existir un sobre en esta columna.
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

// TestMarcarDespachadaSellaCualquierFilaSinSellar fija el FENCE de esta operación tras T1.6-5
// (ADR-0045): `estado <> 'despachado'`.
//
// 🔴 ESTE TEST AFIRMA HOY LO CONTRARIO DE LO QUE AFIRMABA EN SU PRIMERA MITAD. Se llamaba
// `…FueraDeClasificadoEsNoOp` y su caso (1) exigía que sellar una fila `nuevo` NO escribiera nada, con
// este argumento: «sellar lo que el cajero aún no ha mirado sería entregar un mensaje sin darle su
// turno». Ese argumento murió con el push — bajo pull el despachador entrega la fila `nuevo`
// INMEDIATAMENTE y no hay ningún turno que respetar—, y mantener aquel fence habría sido catastrófico: el
// UPDATE habría afectado 0 filas SIEMPRE, ninguna entrega se habría sellado nunca, cada mensaje se
// re-entregaría en cada poll indefinidamente y la poda por TTL habría quedado otra vez inerte. Todo ello
// sin un solo error, porque el 0 se trata como no-op.
//
// LA SEGUNDA MITAD NO CAMBIÓ Y ES LA QUE JUSTIFICA QUE SIGA HABIENDO FENCE: sobre una fila YA
// `despachado` no se escribe, porque un segundo sello movería `despachado_en` hacia adelante y retrasaría
// la poda de esa fila tanto como durase el bucle que lo repitiera.
func TestMarcarDespachadaSellaCualquierFilaSinSellar(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	sembrarNuevo(t, db, 1, 10, "A", "chat@s", "wa-nueva", "recién anotada")
	sembrar(t, db, 2, 20, "A", "chat@s", "wa-vieja", "de un binario anterior", app.EstadoClasificado, sql.NullInt64{})
	sembrar(t, db, 3, 30, "A", "chat@s", "wa-tomada", "con claim vivo", app.EstadoTomado, sql.NullInt64{})
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0, WithClock(clock.Now))

	// (1) LOS TRES ESTADOS NO TERMINALES SE SELLAN. `nuevo` y `tomado` son lo que produce el Edge hoy;
	// `clasificado` es lo que hay en los discos de campo escrito por el binario anterior. Los tres tienen
	// que poder sellarse o sus filas se quedan sin poda para siempre.
	for _, c := range []struct {
		id     int64
		estado string
	}{{1, app.EstadoNuevo}, {2, app.EstadoClasificado}, {3, app.EstadoTomado}} {
		if err := s.MarcarDespachada(ctx, c.id); err != nil {
			t.Fatalf("MarcarDespachada sobre una fila %q: %v", c.estado, err)
		}
		if got := estadoDe(t, db, c.id); got != app.EstadoDespachado {
			t.Fatalf("una fila %q no se pudo sellar (quedó %q): el fence `estado <> 'despachado'` es lo que "+
				"hace que TODA entrega quede anotada; con un fence por igualdad la fila se re-entregaría en "+
				"cada poll y el TTL no podría podarla nunca", c.estado, got)
		}
		if sello := despachadoEnDe(t, db, c.id); !sello.Valid {
			t.Fatalf("una fila %q se selló sin despachado_en: la poda exige las dos cosas", c.estado)
		}
	}

	// (2) SOBRE UNA FILA YA SELLADA: el segundo sello NO mueve el reloj de la poda.
	primerSello := despachadoEnDe(t, db, 2)
	clock.t = clock.t.Add(6 * time.Hour)
	if err := s.MarcarDespachada(ctx, 2); err != nil {
		t.Fatalf("MarcarDespachada (2ª): %v", err)
	}
	if segundo := despachadoEnDe(t, db, 2); segundo.Int64 != primerSello.Int64 {
		t.Fatalf("el segundo sello movió despachado_en de %d a %d: retrasaría la poda de esa fila indefinidamente",
			primerSello.Int64, segundo.Int64)
	}

	// (3) Una fila que ya no existe (la borró el tope entre la entrega y el sello) tampoco es un error.
	if err := s.MarcarDespachada(ctx, 999); err != nil {
		t.Fatalf("sellar una fila inexistente debía ser no-op sin error: %v", err)
	}
}

// 🔴🔴 AQUÍ VIVÍA EL BLOQUE (c) ENTERO —`DespacharSinIntent`, SEIS TESTS— Y SE FUE CON EL MÉTODO EL
// 2026-08-24 (Plan 044 · Ola 1.6 · T1.6-5 · ADR-0045 · D-044.31 · REQ-35).
//
// Aquella sentencia era el camino del PRESUPUESTO VENCIDO: en UN solo UPDATE escribía el sobre de
// omisión, pasaba la fila a `clasificado` y soltaba su claim (`tomado_en`/`claim_token` a NULL). Los seis
// tests fijaban, en este orden: que escribía las cuatro cosas de una vez; que sobre una fila ya
// `clasificado` era no-op y se contaba (la carrera ganada por el cajero, el desenlace BUENO); que un
// motivo fuera de la lista canónica se rechazaba con error antes de tocar la BD; que un cierre TARDÍO del
// cajero tras ella no pisaba nada (el fence por `claim_token`); y que el barrido de leases rescataba a las
// hermanas del lote descartado.
//
// SE BORRAN, NO SE ADAPTAN: el método no existe ni en el puerto (`app.ColaDespachador`) ni en el Store, y
// no hay presupuesto que pueda vencer porque no hay reloj que lo mida.
//
// 🔴 LO QUE NO SE VA CON ELLOS, Y HAY QUE SABER DÓNDE ESTÁ:
//   - EL FENCE POR `claim_token` del cierre del cajero (ADR-0038) sigue INTACTO y sigue probado en
//     `claim_test.go`. Lo que se retiró es UNA de las formas de perder el claim, no el fencing.
//   - `CierresDescartadosPorFence` y `RescatadasPorLease` siguen vivos y contando.
//   - El rechazo de un motivo fuera de la lista canónica sigue siendo un invariante del enum, custodiado
//     por `internal/app/cola_enum_ast_test.go` sobre el AST — que es donde debía estar, porque es una
//     propiedad de la lista, no de una sentencia SQL.
//
// ⚠️ `app.ErrMotivoOmitidoDesconocido` se quedó SIN PRODUCTOR con este borrado: era el error que devolvía
// `Store.DespacharSinIntent`. Se conserva declarado a propósito — es el centinela que documenta por qué un
// `intent_json` NULL no puede usarse para registrar una omisión— y su recuperación es de quien vuelva a
// escribir sobres, si alguien lo hace.

// ────────── EL OTRO CRITERIO: el TTL, des-inertizado ──────────

// TestTTLPodaPorFinLasFilasQueElDespachadorSella es la prueba de que esta tarea DESBLOQUEA T1.6.
//
// 🔴 EL PUNTO ENTERO: `pruneTTLLocked` lleva desde la Ola 1 corriendo en CADA Enqueue y no ha podido
// borrar jamás una sola fila, porque exige `estado='despachado' AND despachado_en IS NOT NULL` y NADIE
// escribía ninguna de las dos cosas (tasks.md T1.6: «el TTL es inerte hasta la O3; lo activa T3.2»).
// Con el sello de MarcarDespachada puesto, y el reloj adelantado 25 h, la poda por fin recoge.
//
// SE CUBREN LOS DOS CAMINOS QUE LLEGAN AL SELLO, porque tienen PROCEDENCIAS distintas y la poda no debe
// distinguirlas: el del CAJERO (encolar → reclamar → clasificar → entregar → MarcarDespachada) y el de una
// fila HEREDADA en `clasificado` de un binario anterior a T1.6-5 (entregar → MarcarDespachada). El segundo
// sustituyó al del PRESUPUESTO VENCIDO, que se retiró con el push el 2026-08-24.
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

	// (2) EL OTRO CAMINO QUE LLEGA AL SELLO: una fila HEREDADA, escrita `clasificado` por un binario
	//     anterior a T1.6-5, que este Edge se encuentra en el disco del cliente y drena directamente.
	//
	//     🔴 POR QUÉ ESTE CAMINO Y NO OTRO. Aquí vivía el del PRESUPUESTO VENCIDO —`DespacharSinIntent`
	//     dejando el sobre y `MarcarDespachada` sellando después—, retirado el 2026-08-24 con el push. Lo
	//     que ocupa su sitio es el ÚNICO riesgo de migración que queda: si una fila `clasificado` heredada
	//     no se pudiera sellar, se quedaría en el disco del cliente PARA SIEMPRE y la cola sólo crecería.
	//     La procedencia importa aunque la sentencia sea la misma, que es el argumento con el que este
	//     tramo existía ya antes.
	heredada := item("A", "chat-5@s", "wa-heredada", "de un binario anterior")
	heredada.Estado = app.EstadoClasificado
	heredada.IntentJSON = `{"intent":"crear_pedido","confidence":0.9}`
	if err := s.Enqueue(ctx, heredada); err != nil {
		t.Fatalf("Enqueue heredada: %v", err)
	}
	idHeredada := idDeWaID(t, db, "wa-heredada")
	if got := despachadoEnDe(t, db, idHeredada); got.Valid {
		t.Fatalf("la fila heredada nació con despachado_en=%d: aún no se ha entregado y la poda podría "+
			"llevársela sin que nadie la haya subido", got.Int64)
	}
	if err := s.MarcarDespachada(ctx, idHeredada); err != nil { // la entrega ocurrió; ahora sí, el sello
		t.Fatalf("MarcarDespachada (heredada): %v", err)
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
	if existe(t, db, "wa-heredada") {
		t.Fatal("EL TTL NO ALCANZA A LAS FILAS HEREDADAS: la que venía `clasificado` de un binario anterior " +
			"a T1.6-5 sobrevivió a las 25 h. Si no se puede sellar, se queda en el disco del cliente para " +
			"siempre y la cola sólo crece")
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
