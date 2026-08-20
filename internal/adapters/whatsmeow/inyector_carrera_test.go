package whatsmeow

// inyector_carrera_test.go — LA CARRERA QUE EL FRENTE MP-10 ACEPTÓ COMO LÍMITE, EJERCITADA.
//
// 🔴 POR QUÉ EXISTE ESTE FICHERO Y NO UNA FUNCIÓN MÁS EN inyector_test.go. Aquel prueba el CONTRATO del
// inyector con el gateway quieto; este prueba lo único que aquel no puede: qué pasa cuando el Listener se
// publica y se limpia MIENTRAS alguien inyecta. La Parte A dejó ese punto explícitamente sin ejercitar
// —el entorno donde se escribió no tenía toolchain de Go— y lo documentó como límite aceptado en el doc de
// setLiveListener: `InyectarEntrante` lee el puntero bajo RLock y llama a `handleEvent` FUERA del candado,
// así que una inyección en vuelo sigue corriendo contra un Listener cuyo serve() ya salió.
//
// QUÉ AFIRMA, Y POR QUÉ ES ESO Y NO OTRA COSA. No afirma que la carrera no exista —existe, es conocida y
// está aceptada—, sino que su desenlace es SIEMPRE uno de los dos legítimos:
//
//   - la inyección entra y recorre el handler (el Listener seguía publicado), o
//   - falla con ErrSinEscuchaViva, el centinela que sessionmgr traduce a 409 y que el borde convierte en
//     «espera y repite».
//
// Lo que NO puede ocurrir jamás es un TERCER desenlace: un pánico por puntero nil, o un error distinto del
// centinela. Cualquiera de los dos rompería la cadena que hace que una tanda lanzada durante un backoff de
// reconexión conteste 409 en vez de un 200 con `inyectados: 0` — el «he medido» a quien no midió nada que
// este micro-plan existe para eliminar.
//
// ⚠️ SE CORRE CON -race O NO PRUEBA LA MITAD DE LO QUE DICE. Sin el detector, el acceso concurrente al
// puntero `g.listener` pasa inadvertido y el test solo comprueba los desenlaces. El gate del repo
// (`make ci-docker`) no pasa -race, así que este fichero se ejercita a mano:
//
//	go test -race ./internal/adapters/whatsmeow/... -run Carrera -count=4
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - quitar el RWMutex de setLiveListener/liveListener (leer y escribir g.listener a pelo) ⇒ -race lo
//     caza en el primer ciclo con un DATA RACE sobre el puntero.
//   - devolver un fmt.Errorf plano en vez de envolver ErrSinEscuchaViva en InyectarEntrante ⇒ falla la
//     aserción del centinela, que es la que sostiene el 409.
//   - borrar la guarda `if l == nil` de InyectarEntrante ⇒ pánico por nil al llamar a handleEvent, y el
//     test lo reporta como el tercer desenlace prohibido.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// colaConcurrente es el doble de la cola para este fichero. NO se reusa spyCola a propósito: aquel hace
// `append` a un slice sin candado propio (solo su callLog está protegido), así que bajo N goroutines el
// detector saltaría por una carrera DEL DOBLE DE TEST y taparía justo la del código que se quiere mirar.
// Aquí solo se CUENTA, que es lo único que este test necesita de la cola.
type colaConcurrente struct {
	mu       sync.Mutex
	anotadas int
}

var _ app.ColaEntrantes = (*colaConcurrente)(nil)

func (c *colaConcurrente) Enqueue(_ context.Context, _ app.ColaItem) error {
	c.mu.Lock()
	c.anotadas++
	c.mu.Unlock()
	return nil
}

func (c *colaConcurrente) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.anotadas
}

// TestInyectarEntrante_CarreraConLaReconexion_SoloDosDesenlaces es EL test del fichero: reproduce el
// escenario de campo —inyectar mientras la sesión cae y vuelve— y comprueba que no aparece un tercer
// desenlace.
//
// El «reconector» imita lo que hace serve() en cada ciclo: publica su Listener al arrancar y lo suelta al
// salir (`defer g.setLiveListener(nil)`). Alternarlos en bucle comprime en milisegundos lo que en campo son
// los hasta 60 s del backoff exponencial.
func TestInyectarEntrante_CarreraConLaReconexion_SoloDosDesenlaces(t *testing.T) {
	const (
		inyectores       = 8
		porInyector      = 60
		ciclosReconexion = 400
	)

	cola := &colaConcurrente{}
	g := gatewayDePrueba()
	// El Listener se construye una vez y se re-publica: lo que rota en campo es QUÉ puntero está publicado,
	// no la identidad del objeto, y es el puntero lo que esta carrera toca.
	// Se construye a mano y no con listenerConCola: aquel helper está tipado al *spyCola de la suite, y
	// este fichero necesita su propio doble con candado (ver colaConcurrente). El cableado es el mismo.
	l := NewListener(quietLogger(),
		WithCola(cola),
		WithSessionID("sess-1"),
		// fastLane determinista: que la fila nazca `nuevo` no puede depender del léxico real del
		// clasificador ni de la frase que traiga el molde.
		WithFastLane(func(string) bool { return false }),
	)
	g.setLiveListener(l)

	var exitos, sinEscucha, prohibidos atomic.Int64
	var arranque, fin sync.WaitGroup

	arranque.Add(1)

	// El reconector: publica y limpia sin descanso, como el ciclo de vida de serve().
	fin.Add(1)
	go func() {
		defer fin.Done()
		arranque.Wait()
		for i := 0; i < ciclosReconexion; i++ {
			g.setLiveListener(nil) // serve() salió: el defer limpió
			g.setLiveListener(l)   // el backoff terminó y el ciclo nuevo publicó el suyo
		}
	}()

	// Los inyectores: el plano de control empujando una tanda mientras todo eso ocurre.
	for i := 0; i < inyectores; i++ {
		fin.Add(1)
		go func(worker int) {
			defer fin.Done()
			arranque.Wait()
			for j := 0; j < porInyector; j++ {
				p := inyeccionValida()
				// Lote e índice propios por goroutine: en campo el índice distingue las filas de una tanda,
				// y repetirlo aquí escondería un choque de IDs detrás de la carrera que se investiga.
				p.Lote = "mp10-carrera"
				p.Indice = worker*porInyector + j

				_, err := g.InyectarEntrante(context.Background(), p)
				switch {
				case err == nil:
					exitos.Add(1)
				case errors.Is(err, ErrSinEscuchaViva):
					sinEscucha.Add(1)
				default:
					prohibidos.Add(1)
					// El texto del error NO se imprime: puede venir del fabricante y arrastrar el mensaje
					// del inyectado (INV-051.1 no se levanta por estar en un test). El tipo basta.
					t.Errorf("worker %d: TERCER DESENLACE — error que no es ErrSinEscuchaViva; el borde no "+
						"sabría traducirlo a 409 y la tanda contestaría 200 con inyectados=0", worker)
				}
			}
		}(i)
	}

	arranque.Done() // todos salen a la vez: sin esto los inyectores terminan antes de que el reconector empiece
	fin.Wait()

	const total = inyectores * porInyector
	if got := exitos.Load() + sinEscucha.Load() + prohibidos.Load(); got != total {
		t.Fatalf("se contabilizaron %d desenlaces de %d inyecciones: alguna se perdió sin clasificar", got, total)
	}
	if prohibidos.Load() != 0 {
		t.Fatalf("%d inyecciones acabaron en un desenlace prohibido", prohibidos.Load())
	}
	// La cola es la prueba de que lo contado como éxito REALMENTE recorrió el handler hasta el final, y no
	// se quedó en una rama corta que devuelve nil sin anotar nada.
	if anotadas := cola.total(); int64(anotadas) != exitos.Load() {
		t.Errorf("éxitos = %d pero la cola anotó %d filas: un éxito que no deja fila es una medición que "+
			"cuenta lo que no ocurrió", exitos.Load(), anotadas)
	}
}
