package despachador

import (
	"errors"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// freno_entrega_test.go — EL FRENO DE LA RE-ENTREGA (Plan 051 Ola 3, barrido CLI del 2026-08-17).
//
// El caso que cubre este fichero nació de un code review, no del desglose de la ola, y es una degradación
// que el sistema sufría EN SILENCIO: una fila cuyo `Deliver` falla siempre se re-entregaba cada 500 ms
// para siempre — el patrón exacto del «lote venenoso» que congeló la cola en la Ola 2.
//
// 🔴 ERAN DOS CASOS. El otro era LA CABEZA ATASCADA —una fila en un estado desconocido paraba la sesión
// entera— y se DISOLVIÓ el 2026-08-24 con el push (Plan 044 · Ola 1.6 · T1.6-5 · ADR-0045): al dejar el
// despachador de mirar el estado para decidir si entrega, un estado imprevisto ya no puede retener nada.
// Su test se sustituyó por el contrario, `TestCabezaEnEstadoImprevistoSaleTambien` (despachador_test.go),
// que asevera que esa fila SALE. Los dos contadores que aquel test custodiaba (`cabezas_atascadas`,
// `polls_cabeza_atascada`) se quedaron sin productor.
//
// ⚠️ EL FRENO DE ABAJO ES HOY LA ÚNICA FORMA DE QUE UNA CABEZA RETENGA A SU SESIÓN, y por eso importa
// más que antes que no abandone: la retención es ahora siempre por un fallo de entrega, nunca por una
// espera de diseño.

// fallarCon fija el error que devolverá el sink, con el mismo lock que usa Deliver.
func (s *sinkFake) fallarCon(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func TestEsperaTrasFallo(t *testing.T) {
	// EL PRIMER FALLO NO ESPERA: el caso dominante y sano es el tropiezo puntual que se cura en el poll
	// siguiente, y penalizarlo sería añadir latencia inventada al 99 % de los casos.
	casos := []struct {
		fallos int
		quiere time.Duration
	}{
		{fallos: 0, quiere: 0},
		{fallos: 1, quiere: 0},
		{fallos: 2, quiere: 500 * time.Millisecond},
		{fallos: 3, quiere: time.Second},
		{fallos: 4, quiere: 2 * time.Second},
		{fallos: 5, quiere: 4 * time.Second},
		{fallos: 8, quiere: 32 * time.Second}, // 500ms<<6 = 32s ⇒ lo capa el tope de 30 s
	}
	for _, c := range casos {
		got := esperaTrasFallo(c.fallos)
		quiere := c.quiere
		if quiere > topeBackoffEntrega {
			quiere = topeBackoffEntrega
		}
		if got != quiere {
			t.Errorf("esperaTrasFallo(%d) = %v, se esperaba %v", c.fallos, got, quiere)
		}
	}

	// EL TECHO SE COMPRUEBA HASTA UN NÚMERO ABSURDO DE FALLOS a propósito: el cálculo es un
	// desplazamiento de bits, y sin la guarda del desbordamiento un contador alto daría una espera
	// NEGATIVA — es decir, el freno se convertiría en su contrario y volvería el bucle caliente, que es
	// justo lo que este código existe para evitar.
	for _, fallos := range []int{9, 20, 63, 64, 65, 200, 1000} {
		got := esperaTrasFallo(fallos)
		if got != topeBackoffEntrega {
			t.Errorf("esperaTrasFallo(%d) = %v, se esperaba el tope %v (¿desbordó el shift?)",
				fallos, got, topeBackoffEntrega)
		}
		if got <= 0 {
			t.Fatalf("esperaTrasFallo(%d) devolvió una espera NO POSITIVA (%v): el freno dejaría de frenar", fallos, got)
		}
	}
}

// TestReintentoDeEntregaSeEspaciaYNoAbandonaNunca cubre las DOS mitades de la decisión de Jhoan del
// 2026-08-17: el reintento se espacia (deja de ser un bucle caliente) pero NO abandona jamás, porque
// abandonar aquí sería perder el mensaje —y la invariante del plan es «se retrasa, nunca se pierde»—.
func TestReintentoDeEntregaSeEspaciaYNoAbandonaNunca(t *testing.T) {
	cola := nuevaColaFake(&app.ColaCabeza{
		ID: 1, Seq: 1, Estado: app.EstadoNuevo, WAMessageID: "wa-1",
	})
	a := arrancar(t, cola)

	fallo := errors.New("el sink lo rechaza siempre")
	a.sink.fallarCon(fallo)

	// Dos vueltas con el sink roto: la primera falla y la segunda TAMBIÉN se intenta (el primer fallo no
	// espacia). A partir de ahí el freno empieza a saltarse intentos.
	a.esperarParada(t)
	a.despertar(t)
	a.esperarLectura(t)
	a.despertar(t)
	a.esperarLectura(t)
	a.sincronizar(t)

	fallosTrasDos := a.d.FallosEntrega()
	if fallosTrasDos < 2 {
		t.Fatalf("tras dos vueltas con el sink roto hubo %d fallos de entrega, se esperaban al menos 2", fallosTrasDos)
	}

	// AHORA EL FRENO DEBE MORDER: sin avanzar el reloj, más vueltas NO deben producir más intentos.
	for i := 0; i < 5; i++ {
		a.despertar(t)
		a.esperarLectura(t)
	}
	a.sincronizar(t)
	if got := a.d.FallosEntrega(); got != fallosTrasDos {
		t.Fatalf("el freno no mordió: los fallos pasaron de %d a %d SIN avanzar el reloj. "+
			"Una fila que el sink rechaza siempre estaría re-entregándose en cada poll, que es el patrón "+
			"del lote venenoso de la Ola 2", fallosTrasDos, got)
	}

	// 🔴 Y LO QUE NO PUEDE PASAR NUNCA: que la fila se abandone. Se cura el sink, se avanza el reloj más
	// allá del tope, y el mensaje TIENE que salir. Si algún día alguien añade un abandono por N intentos,
	// este test se pone rojo y esa es exactamente su razón de ser.
	a.sink.fallarCon(nil)
	a.reloj.avanzar(2 * topeBackoffEntrega)
	a.despertar(t)
	evt := a.esperarEntrega(t)
	if evt.MessageID != "wa-1" {
		t.Fatalf("se entregó %q y se esperaba wa-1", evt.MessageID)
	}
	a.sincronizar(t)
	if got := a.d.Despachados(); got != 1 {
		t.Fatalf("despachados = %d, se esperaba 1: la fila debía acabar saliendo, nunca abandonarse", got)
	}
}
