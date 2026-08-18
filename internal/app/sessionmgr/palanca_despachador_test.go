package sessionmgr

// palanca_despachador_test.go — LA CUARTA PUERTA DE startDespachador (Plan 051 Ola 5 · T3.17).
//
// 🔴 QUÉ SE CUSTODIA Y POR QUÉ NO BASTABA LO QUE HABÍA. Las otras tres puertas de startDespachador (sin
// cola, sin mux, sin sink) solo se cierran en TESTS: en producción el daemon falla antes de llegar aquí.
// Esta cuarta es la contraria — SOLO puede estar echada en producción, porque solo la echa un humano —, y
// su efecto es el más caro del Edge: recibir y no entregar.
//
// El test es una PAREJA, y esa es toda su fuerza: el mismo Manager, la misma cola, el mismo mux y el mismo
// sink, cambiando ÚNICAMENTE la palanca. Sin el caso de control, un test que solo comprobase el nil pasaría
// también con el cableado roto (que es el modo de fallo que este paquete ya conoce: ver la cabecera de
// latencia_cableado_test.go).
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo, y todas se ejecutaron.

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// logConCandado es el destino del logger de la sesión. NO es un bytes.Buffer pelado, y la diferencia la
// cazó el gate de -race: con la palanca BAJADA el despachador arranca su goroutine y esa goroutine ESCRIBE
// en el mismo log de sesión que el test LEE, así que un bytes.Buffer compartido es una carrera de libro
// (bytes.Buffer no es seguro entre goroutines). El candado la cierra sin cambiar lo que se observa.
type logConCandado struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logConCandado) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logConCandado) texto() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// muxDeUnSink es el CloudLinkMux mínimo que hace falta aquí: lo único que startDespachador le pide es un
// sink por sesión. El resto del contrato son no-ops porque este camino no los toca.
type muxDeUnSink struct{}

func (muxDeUnSink) Register(string, string,
	func(ctx context.Context, commandID, to, text string) error,
	func(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) error,
	func() bool) {
}
func (muxDeUnSink) Unregister(string)                       {}
func (muxDeUnSink) SinkFor(string) app.InboundSink          { return sinkMudo{} }
func (muxDeUnSink) SendReceipt(string, domain.ReceiptEvent) {}
func (muxDeUnSink) SendLoggedOut(string)                    {}

var _ CloudLinkMux = muxDeUnSink{}

// managerCableadoDelTodo arma un Manager con las TRES dependencias que startDespachador exige (cola, mux y
// sink), de modo que la única variable del test sea la palanca. Devuelve también el buffer del log de la
// sesión, que es donde tiene que aparecer el grito.
func managerCableadoDelTodo(t *testing.T, apagada bool) (*Manager, *liveSession, *logConCandado) {
	t.Helper()
	buf := &logConCandado{}
	m := NewManager(NewLayout(filepath.Join(t.TempDir(), "edge-data")), nil, 5, testLogger(),
		WithColaDespachador(colaMuda{}),
		WithWhatsmeowListen(muxDeUnSink{}, "wApp"),
		WithDespachadorApagado(apagada),
	)
	s := &liveSession{
		meta: domain.Session{SessionID: uuidA},
		log:  sharedlogger.New(sharedlogger.WithWriter(buf)),
	}
	return m, s, buf
}

// TestPalancaDespachador_EchadaNoArrancaElBucleYLoGRITA es el caso de la palanca puesta: con TODO cableado,
// la sesión no estrena despachador y deja escrita la razón.
//
// Se afirman las dos cosas por separado a propósito. Que no arranque es el efecto pedido; que lo GRITE es
// la mitad que evita el desastre, porque el estado resultante en campo —Edge sano, cola creciendo, nada en
// la nube— es indistinguible de un despachador roto y de una nube caída. Un apagado silencioso no es una
// palanca de diagnóstico: es una avería que alguien programó.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - borrar el bloque `if m.despachadorApagado { … return }` de startDespachador ⇒ la sesión arranca su
//     despachador con la palanca echada y la medición de T3.17 mide con el despachador vivo.
//   - dejar el `return` y quitar el `s.log.Warn` ⇒ el apagado se vuelve mudo (falla la segunda mitad).
//   - en WithDespachadorApagado, `if apagado` → `if !apagado` ⇒ la opción invierte su significado y el
//     caso de control de abajo se lleva el rojo.
func TestPalancaDespachador_EchadaNoArrancaElBucleYLoGRITA(t *testing.T) {
	m, s, buf := managerCableadoDelTodo(t, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.startDespachador(ctx, s)

	if s.getDespachador() != nil {
		t.Fatal("con la palanca ECHADA la sesión arrancó su despachador igualmente: la medida de T3.17 " +
			"correría con la cola drenando y el p99 no separaría la contención intra-proceso de la inter-proceso")
	}

	linea := buf.texto()
	if !strings.Contains(linea, "WAPP_AGENT_DESPACHADOR_APAGADO") {
		t.Errorf("el apagado no nombra la variable que lo causó; quien encuentre el Edge sin entregar no "+
			"tiene por dónde empezar. Log emitido: %q", linea)
	}
	if !strings.Contains(linea, "WARN") {
		t.Errorf("el aviso del apagado no sale a nivel WARN: un Info se pierde entre el tráfico normal del "+
			"daemon, que es justo lo que esta línea no puede permitirse. Log emitido: %q", linea)
	}
}

// TestPalancaDespachador_BajadaArrancaElBucle es el CASO DE CONTROL, y sin él el test de arriba no vale
// nada: prueba que el cableado del Manager de estos tests SÍ llega a arrancar un despachador, de modo que
// el nil de arriba solo puede venir de la palanca.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - en startDespachador, quitar la condición y apagar SIEMPRE (`return` incondicional al principio) ⇒ el
//     despachador no arrancaría nunca, que es el fallo que este control existe para cazar.
//   - en WithDespachadorApagado, guardar `true` siempre ⇒ la opción se vuelve un interruptor de una sola
//     posición.
func TestPalancaDespachador_BajadaArrancaElBucle(t *testing.T) {
	m, s, buf := managerCableadoDelTodo(t, false)

	ctx, cancel := context.WithCancel(context.Background())
	m.startDespachador(ctx, s)

	if s.getDespachador() == nil {
		t.Fatal("con la palanca BAJADA y cola+mux+sink cableados la sesión NO estrenó despachador: el " +
			"Edge no drenaría su cola en el caso normal, que es el de todos los Edge en campo")
	}
	if strings.Contains(buf.texto(), "WAPP_AGENT_DESPACHADOR_APAGADO") {
		t.Errorf("el caso sano avisa de una palanca que nadie echó: un aviso que sale siempre deja de "+
			"significar algo. Log emitido: %q", buf.texto())
	}

	// El bucle quedó corriendo en su goroutine: se le cierra el grifo y se le espera, para no dejarlo
	// sondeando la cola muda mientras corren los demás tests del paquete.
	cancel()
	s.waitDespachadores()
	m.wg.Wait()
}
