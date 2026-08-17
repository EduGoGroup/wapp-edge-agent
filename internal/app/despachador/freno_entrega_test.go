package despachador

import (
	"errors"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// freno_entrega_test.go — EL FRENO DE LA RE-ENTREGA Y LA CABEZA ATASCADA (Plan 051 Ola 3, barrido CLI
// del 2026-08-17).
//
// Los dos casos que cubre este fichero nacieron de un code review, no del desglose de la ola, y los dos
// son degradaciones que el sistema sufría EN SILENCIO:
//
//   - una fila cuyo `Deliver` falla siempre se re-entregaba cada 500 ms para siempre (el patrón exacto
//     del «lote venenoso» que congeló la cola en la Ola 2);
//   - una cabeza en estado desconocido paraba la sesión entera sin que subiera un solo contador, así que
//     la telemetría de la Ola 4 la habría publicado como una sesión ociosa (contra INV-051.3).

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
		ID: 1, Seq: 1, Estado: app.EstadoClasificado,
		WAMessageID: "wa-1", IntentJSON: app.SobreOmitido(app.MotivoFastlane), TieneIntent: true,
	})
	a := arrancar(t, cola, 4*time.Second)

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

// TestCabezaAtascadaSeCuenta vigila INV-051.3 en el peor caso del despachador: una cabeza en un estado
// que esta versión no conoce deja la sesión SIN DRENAR para siempre. Hasta el 2026-08-17 toda la señal
// era un único Warn y ningún contador, así que la Ola 4 habría publicado una sesión muerta como ociosa.
func TestCabezaAtascadaSeCuenta(t *testing.T) {
	// El estado no es ninguno de los cuatro conocidos: simula una fila escrita por un binario más nuevo
	// (o a mano). El doble replica el predicado real (`estado IN ('nuevo','tomado')`), así que el sello
	// por presupuesto NO la mueve — igual que en SQLite.
	cola := nuevaColaFake(&app.ColaCabeza{
		ID: 1, Seq: 1, Estado: "estado_de_una_version_mas_nueva", WAMessageID: "wa-zombi",
	})
	a := arrancar(t, cola, time.Second)

	a.esperarParada(t)
	// Primera vuelta: la ve, arranca su presupuesto.
	a.despertar(t)
	a.esperarLectura(t)
	// Vence el presupuesto y se dispara el sello, que no la mueve.
	a.reloj.avanzar(2 * time.Second)
	a.despertar(t)
	a.esperarLectura(t)
	// Tercera vuelta: ya con `disparado`, entra en la rama del atasco.
	a.despertar(t)
	a.esperarLectura(t)
	a.sincronizar(t)

	if got := a.d.CabezasAtascadas(); got != 1 {
		t.Fatalf("cabezas_atascadas = %d, se esperaba 1: una sesión que dejó de drenar TIENE que contarse "+
			"(INV-051.3), no sólo avisarse una vez", got)
	}
	polls := a.d.PollsCabezaAtascada()
	if polls < 1 {
		t.Fatalf("polls_cabeza_atascada = %d, se esperaba al menos 1", polls)
	}

	// LA SEGUNDA SERIE DEBE SEGUIR CRECIENDO: es la que distingue «pasó una vez hace horas» de «esta
	// sesión está muerta ahora mismo», y sin ella el operador no puede saber cuál de las dos mira.
	a.despertar(t)
	a.esperarLectura(t)
	a.sincronizar(t)
	if got := a.d.PollsCabezaAtascada(); got <= polls {
		t.Fatalf("polls_cabeza_atascada se quedó en %d tras otra vuelta bloqueada (antes %d): "+
			"no se puede distinguir un atasco pasado de uno en curso", got, polls)
	}
	// Y el aviso sigue siendo UNO SOLO: contar por poll no debe convertirse en gritar por poll.
	if got := a.d.CabezasAtascadas(); got != 1 {
		t.Fatalf("cabezas_atascadas = %d tras varias vueltas, se esperaba 1: se cuenta por FILA, no por poll", got)
	}
}
