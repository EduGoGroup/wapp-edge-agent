package app

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// --- fakes (sin red ni teléfono) ---

// fakeListenGateway imita el gateway always-on real: registra la DEK que recibió, AVISA de que entró en
// Listen (cierra `entrado`) y BLOQUEA hasta que el ctx se cancele, como hace el socket de verdad.
//
// 🔴 YA NO ENTREGA NADA, Y ESE ES EL CAMBIO DE T3.8. Hasta esta tarea emitía N eventos sintéticos a un
// `sink` que el puerto le pasaba, y sobre esa emisión se afirmaba «los entrantes llegan al sink». La
// afirmación era falsa desde T3.0: el adaptador REAL recibía ese sink como `_` y lo ignoraba, así que la
// entrega solo ocurría dentro de este fake y NINGUNA mutación del código de producción podía poner el test
// en rojo. Quien entrega hoy es el despachador (app/despachador), y sus tests son los que lo prueban.
type fakeListenGateway struct {
	gotDEK     []byte
	connectErr error
	returned   error

	// entrado se cierra al ENTRAR en Listen. Es la sincronización que antes daba el conteo de eventos del
	// sink, y es mejor señal: dice exactamente lo que los tests quieren saber (la DEK llegó al gateway) sin
	// depender de una entrega que ya no existe. Cerrar bajo sync.Once porque el caso de uso podría llamar
	// a Listen más de una vez en un futuro reintento.
	entrado    chan struct{}
	entradoUna sync.Once
}

func newFakeListenGateway() *fakeListenGateway {
	return &fakeListenGateway{entrado: make(chan struct{})}
}

func (g *fakeListenGateway) Listen(ctx context.Context, dek []byte) error {
	// Copia defensiva: el caso de uso borra (zero) la DEK al salir.
	g.gotDEK = append([]byte(nil), dek...)
	// El cierre del canal es la barrera happens-before de gotDEK: quien lo lee espera antes en `entrado`
	// (o en el retorno de Run), nunca en carrera.
	g.entradoUna.Do(func() { close(g.entrado) })
	if g.connectErr != nil {
		return g.connectErr
	}
	<-ctx.Done() // socket vivo hasta la cancelación.
	g.returned = ctx.Err()
	return nil
}

// esperaEntrada bloquea hasta que el gateway entró en Listen, o falla por timeout.
func (g *fakeListenGateway) esperaEntrada(t *testing.T) {
	t.Helper()
	select {
	case <-g.entrado:
	case <-time.After(2 * time.Second):
		t.Fatal("el caso de uso no llegó a invocar al gateway")
	}
}

// --- tests ---

// TestListen_HandsDEKToGatewayAndStopsOnCancel es lo que quedó de TestListen_DeliversAndStopsOnCancel
// (Plan 051 Ola 3 · T3.8), y el renombrado es la mitad del arreglo: el nombre viejo prometía «Delivers»
// —que los entrantes llegan al sink— y esa propiedad dejó de ser de este caso de uso en T3.0. Se REESCRIBE
// en vez de retirarse porque lo que Listen SÍ hace hoy no está cubierto en ningún otro sitio, y son tres
// cosas que importan:
//
//  1. la DEK cargada de custodia llega INTACTA al gateway (ADR-0007: es la llave del store cifrado, y una
//     DEK corrupta o borrada antes de tiempo deja la sesión sin poder abrir su device);
//  2. Run BLOQUEA mientras el gateway sostiene el socket, en vez de retornar y dar la sesión por hecha;
//  3. al cancelar el ctx, el gateway ve la cancelación y Run retorna NIL — un apagado ordenado no es un
//     fallo, y devolver error aquí haría que runListener marcara la sesión `degraded` y la reintentara
//     con backoff en pleno cierre del daemon.
//
// MUTACIÓN QUE LO PONE EN ROJO (verificada en T3.8): mover el `defer zeroBytes(dek)` de Run ANTES de la
// llamada a gateway.Listen —o pasarle un slice recién puesto a cero— hace fallar la comprobación de la DEK
// intacta. También lo pone en rojo suprimir la llamada al gateway (esperaEntrada agota su plazo) o envolver
// el retorno limpio en un error.
func TestListen_HandsDEKToGatewayAndStopsOnCancel(t *testing.T) {
	dek := bytes.Repeat([]byte{0xCD}, DEKSize)
	cust := custodyWith(dek)
	gw := newFakeListenGateway()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- NewListen(cust, gw, nil).Run(ctx) }()

	gw.esperaEntrada(t)

	// Run debe seguir BLOQUEADO mientras el gateway sostiene el socket: si retornara ya, la sesión quedaría
	// dada por terminada con el socket vivo. Una espera corta basta para distinguir «bloquea» de «retornó
	// inmediatamente»; el caso contrario (que no retorne NUNCA) lo cubre el select de abajo.
	select {
	case err := <-done:
		t.Fatalf("Run retornó (%v) sin que nadie cancelara: no está sosteniendo el socket vivo", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run debía retornar nil al cancelar: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run no retornó tras cancelar el ctx")
	}

	if !bytes.Equal(gw.gotDEK, dek) {
		t.Fatalf("la DEK no llegó intacta al gateway: %v", gw.gotDEK)
	}
	if !errors.Is(gw.returned, context.Canceled) {
		t.Fatalf("el gateway no respetó la cancelación: %v", gw.returned)
	}
}

// TestListen_NoDEK: sin DEK custodiada, Run falla y NO invoca al gateway.
func TestListen_NoDEK(t *testing.T) {
	gw := newFakeListenGateway()
	err := NewListen(&fakeCustody{}, gw, nil).Run(context.Background())
	if err == nil {
		t.Fatal("se esperaba error al no haber DEK custodiada")
	}
	if gw.gotDEK != nil {
		t.Fatal("el gateway NO debía invocarse sin DEK")
	}
}

// TestListen_GatewayError: un fallo de conexión del gateway se propaga envuelto.
func TestListen_GatewayError(t *testing.T) {
	sentinel := errors.New("socket caído")
	cust := custodyWith(bytes.Repeat([]byte{1}, DEKSize))
	gw := newFakeListenGateway()
	gw.connectErr = sentinel
	err := NewListen(cust, gw, nil).Run(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, quería envolver %v", err, sentinel)
	}
}
