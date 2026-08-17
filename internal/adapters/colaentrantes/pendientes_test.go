package colaentrantes

import (
	"context"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// pendientesCero es la respuesta de una cola al día. Se declara como valor para poder compararla entera
// (el struct es comparable) en vez de campo a campo, que es donde se olvida uno.
var pendientesCero = app.ColaPendientes{}

// sembrarEstado pone una fila ya encolada en el estado dado, por UPDATE directo. Se toca la BD a mano —y
// no por los métodos del Store— a propósito: el objetivo es fijar el escenario de conteo, no ejercitar de
// paso el claim ni el despacho (que tienen sus propios tests y sus propios fences).
func sembrarEstado(t *testing.T, s *Store, waID, estado string) {
	t.Helper()
	res, err := s.db.ExecContext(context.Background(),
		`UPDATE cola_entrantes SET estado = ? WHERE wa_message_id = ?`, estado, waID)
	if err != nil {
		t.Fatalf("sembrar estado %q: %v", estado, err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("sembrar estado %q sobre %s: %d filas afectadas", estado, waID, n)
	}
}

// TestPendientes_LoDespachadoNoCuenta es EL test de esta consulta: la cola es un buzón de PASO, y lo ya
// despachado sigue en la tabla hasta que la poda por TTL se lo lleva. Contarlo convertiría la métrica de
// «cuánto queda por hacer» en «cuánto ha pasado por aquí desde el último barrido», que es un número
// distinto, que solo sube y que no sirve para decidir nada.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: quitar el `WHERE estado <> ?` (o comparar contra otro estado) ⇒ el
// total pasa a 4 y `cola_pendientes` deja de significar lo que su nombre dice.
func TestPendientes_LoDespachadoNoCuenta(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, openDB(t), newFakeCrypterFor().fn, 100, 24)

	for _, waID := range []string{"W-NUEVO", "W-TOMADO", "W-CLASIF", "W-DESPACHADO"} {
		if err := s.Enqueue(ctx, item("sess-1", "chat-1", waID, "hola")); err != nil {
			t.Fatalf("Enqueue %s: %v", waID, err)
		}
	}
	sembrarEstado(t, s, "W-TOMADO", "tomado")
	sembrarEstado(t, s, "W-CLASIF", "clasificado")
	sembrarEstado(t, s, "W-DESPACHADO", "despachado")

	p, err := s.Pendientes(ctx)
	if err != nil {
		t.Fatalf("Pendientes: %v", err)
	}
	if p.Total != 3 {
		t.Fatalf("Total = %d, se esperaban 3: las cuatro filas están en la tabla, pero la DESPACHADA ya no "+
			"está pendiente de nada", p.Total)
	}
	if p.Nuevo != 1 || p.Tomado != 1 || p.Clasificado != 1 {
		t.Fatalf("desglose = nuevo %d / tomado %d / clasificado %d, se esperaba 1/1/1",
			p.Nuevo, p.Tomado, p.Clasificado)
	}
}

// TestPendientes_UnEstadoDesconocidoEntraEnElTotal: el total se suma sobre TODOS los grupos, no sobre los
// tres nombrados. Una fila en un estado que este código no conoce —uno añadido mañana, o el 'zombi' que
// los tests del tope siembran— tiene que aparecer en `cola_pendientes` aunque no salga en el desglose: la
// diferencia entre el total y la suma de los tres es lo que delata que hay algo que nadie está mirando.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: sumar el total como `p.Nuevo + p.Tomado + p.Clasificado`, o meter el
// `p.Total += n` dentro de los `case` conocidos.
func TestPendientes_UnEstadoDesconocidoEntraEnElTotal(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, openDB(t), newFakeCrypterFor().fn, 100, 24)

	for _, waID := range []string{"W-NUEVO", "W-ZOMBI"} {
		if err := s.Enqueue(ctx, item("sess-1", "chat-1", waID, "hola")); err != nil {
			t.Fatalf("Enqueue %s: %v", waID, err)
		}
	}
	sembrarEstado(t, s, "W-ZOMBI", "zombi")

	p, err := s.Pendientes(ctx)
	if err != nil {
		t.Fatalf("Pendientes: %v", err)
	}
	if p.Total != 2 {
		t.Fatalf("Total = %d, se esperaban 2: una fila en un estado desconocido SIGUE pendiente y el total "+
			"tiene que verla", p.Total)
	}
	if suma := p.Nuevo + p.Tomado + p.Clasificado; suma != 1 {
		t.Fatalf("suma del desglose = %d, se esperaba 1: la fila 'zombi' no pertenece a ninguno de los tres, "+
			"y esa diferencia con el total es justo la señal que hay que poder ver", suma)
	}
}

// TestPendientes_ColaVaciaEsCeroYNoError: el estado normal de un Edge al día es «no queda nada», y eso no
// es un error ni una fila que falta. Mismo criterio que el (nil, nil) de CabezaDeSesion.
func TestPendientes_ColaVaciaEsCeroYNoError(t *testing.T) {
	s := newStore(t, openDB(t), newFakeCrypterFor().fn, 100, 24)

	p, err := s.Pendientes(context.Background())
	if err != nil {
		t.Fatalf("Pendientes sobre una cola vacía devolvió error: %v", err)
	}
	if p != (pendientesCero) {
		t.Fatalf("cola vacía = %+v, se esperaba todo a cero", p)
	}
}
