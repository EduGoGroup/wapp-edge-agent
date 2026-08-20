package sessionmgr

// inyector_test.go — EL TRAMO INTERMEDIO DEL INYECTOR DE ENTRANTES SINTÉTICOS (MP-10 Parte A).
//
// QUÉ SE CUSTODIA AQUÍ. Este fichero NO prueba que la inyección mida bien (eso es del adaptador, que es
// quien recorre el handler): prueba lo único de lo que responde el gestor de sesiones, que es el ENRUTADO.
// Y el enrutado tiene tres desenlaces que en campo se confunden con una facilidad peligrosa:
//
//	 1. no hay sesión viva con ese id      → no hay camino que recorrer; emparejar/restaurar
//	 2. la sesión vive pero no hay cable   → el listener aún está levantando su ciclo; ESPERAR
//	 2'. hay cable, pero apunta a un gateway SIN Listener → la sesión cayó y espera el backoff; ESPERAR
//	 3. hay cable y hay escucha            → la inyección llega intacta y su acuse vuelve tal cual
//
// El 1 y el 2 dan los dos «no se pudo inyectar» y llevan a acciones OPUESTAS, así que llevan errores
// distintos y el test los separa. El 2' es el HERMANO VIVO del 2 —el 2 casi no ocurre en producción y el 2'
// dura hasta 60 s cada vez que una sesión se reconecta— y tiene que salir con el MISMO centinela que el 2,
// porque es el único que el borde HTTP conoce; ahí es donde se traduce el del adaptador. El 3 comprueba lo
// que suena a trivial y es lo que de verdad se rompe: que los cuatro campos lleguen sin tocarse y que el
// bool que devuelve el camino real no se aplaste por el camino (un inyector que siempre dice `true` produce
// una medición que parece sana y no midió nada).
//
// ⚠️ ESTOS TESTS NO SE HAN EJECUTADO: el entorno en el que se escribieron no tiene toolchain de Go. Las
// mutaciones anotadas en cada uno son las que DEBEN ponerlo en rojo, y verificarlo es parte del barrido en
// el CLI, no una formalidad.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// inyeccionDePrueba es la carga que se usa en todos los casos: los cuatro campos con valores DISTINTOS
// entre sí y reconocibles, para que una permutación (pasar `Lote` donde va `ChatJID`) se vea en el fallo en
// vez de pasar desapercibida entre dos cadenas parecidas.
var inyeccionDePrueba = app.InyeccionEntrante{
	ChatJID: "5215550001111@s.whatsapp.net",
	Texto:   "sintético mp-10",
	Lote:    "lote-de-prueba",
	Indice:  7,
}

// managerConSesionViva arma un Manager con UNA sesión en el registro vivo y SIN cable de inyección, que es
// el estado exacto en el que queda una sesión recién registrada cuyo listener todavía no completó su primer
// ciclo. Los tests que quieren cable lo publican ellos con setLiveInyector.
func managerConSesionViva(t *testing.T, sessionID string) (*Manager, *liveSession) {
	t.Helper()
	m := NewManager(NewLayout(filepath.Join(t.TempDir(), "edge-data")), nil, 5, testLogger())
	s := &liveSession{meta: domain.Session{SessionID: sessionID}, log: testLogger()}
	m.mu.Lock()
	m.live[sessionID] = s
	m.mu.Unlock()
	return m, s
}

// TestManager_InyectarEntrante_SesionInexistenteDaErrorPropio: inyectar en un id que no está vivo falla con
// ErrSesionNoViva y NO acusa.
//
// El error se comprueba con errors.Is y no por su texto porque el plano de control lo va a mapear a un
// código HTTP: si el sentinel deja de propagarse (un fmt.Errorf con %v en vez de %w), el borde perdería la
// capacidad de distinguir «pide otra sesión» de «vuelve a intentarlo» y devolvería el mismo 500 para todo.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO:
//   - en InyectarEntrante, quitar la comprobación de `ok` ⇒ pánico por nil (se llamaría inyectarVia sobre
//     una sesión que no existe), que es exactamente el fallo que este método existe para no tener.
//   - cambiar el `%w` por `%v` en el fmt.Errorf ⇒ el sentinel deja de viajar y errors.Is falla.
//   - devolver ErrSessionNotFound (el de unlink.go) en vez de ErrSesionNoViva ⇒ rojo aquí: son afirmaciones
//     distintas (aquel habla del DISCO, y este camino no lo consulta).
func TestManager_InyectarEntrante_SesionInexistenteDaErrorPropio(t *testing.T) {
	m, _ := managerConSesionViva(t, uuidA)

	acusar, err := m.InyectarEntrante(context.Background(), uuidB, inyeccionDePrueba)

	if !errors.Is(err, ErrSesionNoViva) {
		t.Fatalf("inyectar en una sesión no viva debería dar ErrSesionNoViva; got %v", err)
	}
	if acusar {
		t.Error("la inyección acusó recibo sin que existiera sesión viva: un acuse que no respalda ningún " +
			"recorrido del handler convierte la medición en un número inventado")
	}
}

// TestManager_InyectarEntrante_SesionVivaSinCableDaOtroError: la sesión existe pero su ciclo de escucha aún
// no publicó el inyector ⇒ ErrInyectorNoCableado, y NO el error de sesión ausente.
//
// ⚠️ ESTE TEST CUBRE UNA RAMA DEFENSIVA, NO EL CAMINO REAL: fuerza `liveInyectar` a nil, y producción solo
// pasa por ahí en el hueco previo al PRIMER cableado de la sesión — el factory publica el cable por intento
// y nadie lo pone a nil nunca (listen.go:169). El estado que sí se pisa en campo (cable puesto apuntando a
// un gateway sin Listener, durante el backoff) lo cubre el test de abajo. Se conserva porque la guarda
// `fn == nil` tiene que seguir existiendo: sin ella, ese hueco es un pánico.
//
// Las dos mitades importan y la segunda más que la primera: comprobar solo que «falla» dejaría pasar el
// caso en el que los dos desenlaces colapsan en un mismo error, y ese colapso manda al operador a reparar
// una sesión que está perfectamente bien cuando lo único que hacía falta era esperar al primer Connect.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO:
//   - en inyectarVia, devolver ErrNoLiveSender (el de los emisores) en vez de ErrInyectorNoCableado ⇒ el
//     camino entrante se disfraza de camino saliente.
//   - quitar la guarda `if fn == nil` de inyectarVia ⇒ pánico al invocar una función nil.
//   - hacer que InyectarEntrante devuelva ErrSesionNoViva también cuando el cable falta ⇒ rojo en la
//     segunda comprobación (los dos desenlaces colapsados).
func TestManager_InyectarEntrante_SesionVivaSinCableDaOtroError(t *testing.T) {
	m, _ := managerConSesionViva(t, uuidA)

	acusar, err := m.InyectarEntrante(context.Background(), uuidA, inyeccionDePrueba)

	if !errors.Is(err, ErrInyectorNoCableado) {
		t.Fatalf("una sesión viva SIN inyector cableado debería dar ErrInyectorNoCableado; got %v", err)
	}
	if errors.Is(err, ErrSesionNoViva) {
		t.Error("el error de «listener aún sin cablear» viaja también como ErrSesionNoViva: los dos " +
			"desenlaces se arreglan de forma opuesta (uno esperando, el otro emparejando) y quien lea este " +
			"error no puede distinguirlos")
	}
	if acusar {
		t.Error("acusó recibo sin cable: nada recorrió el handler")
	}
}

// TestManager_InyectarEntrante_CableConGatewaySinListenerTraduceElCentinela es el caso VIVO del 409: el que
// producción SÍ alcanza, y el que el test de arriba no puede cubrir.
//
// EL ESTADO QUE REPRODUCE. El cable y el Listener se publican en momentos distintos y nadie los sincroniza:
// el factory publica `liveInyectar` por intento, ANTES de `serve()` (listen.go:169), y `serve()` publica el
// Listener después de `Register` y lo limpia con un `defer` al salir. Así que cuando la sesión cae, el
// gateway muerto se queda cableado durante TODO el backoff de reconexión —hasta 60 s con el `backoffMax`
// por defecto— y `inyectarVia` invoca alegremente una función que no tiene a dónde entregar. La closure de
// este test es exactamente eso: cable presente que devuelve whatsmeow.ErrSinEscuchaViva.
//
// POR QUÉ IMPORTA QUE SALGA COMO ErrInyectorNoCableado. Es el único centinela que el borde HTTP conoce
// (diag/inyector.go). Sin la traducción, ese error viaja como uno cualquiera: no aborta la tanda, se cuenta
// N veces y el operador recibe un 200 con `inyectados: 0, errores: 500` — «he medido» a quien no midió
// nada, que es el modo de fallo que MP-10 existe para eliminar.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO:
//   - borrar el `if errors.Is(err, whatsmeow.ErrSinEscuchaViva)` de InyectarEntrante ⇒ el error del gateway
//     sale crudo, errors.Is falla y el 409 vuelve a ser inalcanzable en el estado que más dura.
//   - cambiar el `%w` del envoltorio por `%v` ⇒ el centinela del puerto deja de viajar y el borde traduce a
//     un 200 con errores acumulados.
//   - devolver `acusar` tal cual en vez de `false` al traducir ⇒ rojo en la comprobación del acuse: un true
//     con error convierte una tanda no ejercitada en una medición aparentemente válida.
//
// ⚠️ LO QUE ESTE TEST **NO** CUSTODIA, dicho para que nadie se apoye en él de más: la closure fabrica el
// error, así que si el ADAPTADOR dejara de envolver ErrSinEscuchaViva (volviendo a un fmt.Errorf plano),
// este test seguiría verde y la cadena real estaría rota. Ese extremo lo ata
// whatsmeow.TestInyectarEntrante_SinSesionEscuchando_ErrorExplicativoYNoPanic, con su errors.Is. Los dos
// hacen falta: este prueba la traducción, aquel prueba que hay algo que traducir.
func TestManager_InyectarEntrante_CableConGatewaySinListenerTraduceElCentinela(t *testing.T) {
	m, s := managerConSesionViva(t, uuidA)

	// El cable ESTÁ puesto (esto es lo que lo distingue del test de arriba) y apunta a un gateway cuyo
	// serve() ya salió: es el estado del backoff de reconexión, tal cual.
	s.setLiveInyector(func(_ context.Context, _ app.InyeccionEntrante) (bool, error) {
		return true, fmt.Errorf("%w: no hay Listener al que entregar el sintético", whatsmeow.ErrSinEscuchaViva)
	})

	acusar, err := m.InyectarEntrante(context.Background(), uuidA, inyeccionDePrueba)

	if !errors.Is(err, ErrInyectorNoCableado) {
		t.Fatalf("el gateway sin Listener (backoff de reconexión) debería salir como ErrInyectorNoCableado y "+
			"salió %v: el borde solo conoce ese centinela, así que sin la traducción la tanda NO aborta con 409 "+
			"y devuelve un 200 con inyectados=0", err)
	}
	if errors.Is(err, ErrSesionNoViva) {
		t.Error("el estado «gateway sin Listener» viaja también como ErrSesionNoViva: mandaría al operador a " +
			"emparejar una sesión que solo necesita unos segundos de backoff")
	}
	if acusar {
		t.Error("se devolvió acuse=true con error: la closure devolvió true y la traducción lo dejó pasar; un " +
			"acuse que no respalda ningún recorrido del handler convierte la medición en un número inventado")
	}
}

// TestManager_InyectarEntrante_CableadaLlegaIntactaYDevuelveSuAcuse: con el cable puesto, la closure recibe
// los CUATRO campos sin tocar y el bool que ella devuelve es el que sale del Manager.
//
// Se prueba el `false` (y no el `true`, que sería lo cómodo) a propósito: el falso positivo peligroso de
// este camino es un enrutador que responde «sí» por su cuenta. Con `true` no se distinguiría un reenvío
// honesto de un `return true, nil` escrito a mano en el Manager; con `false`, sí.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO:
//   - en InyectarEntrante, `return true, nil` en vez de reenviar lo que devuelve el cable ⇒ rojo en el
//     acuse (es el fallo que convierte la medición en un número inventado).
//   - en inyectarVia o en el cableado de listen.go, construir una app.InyeccionEntrante nueva perdiendo un
//     campo (p.ej. no propagar Indice) ⇒ rojo en la comparación de la carga: dos filas del mismo lote sin
//     índice distinto colisionan en el `WAMessageID` y la cola las trata como duplicado.
//   - no llamar a la closure (devolver el cero sin invocarla) ⇒ rojo en el contador de llamadas.
func TestManager_InyectarEntrante_CableadaLlegaIntactaYDevuelveSuAcuse(t *testing.T) {
	m, s := managerConSesionViva(t, uuidA)

	var recibida app.InyeccionEntrante
	llamadas := 0
	// La closure corre en la MISMA goroutine que el test (InyectarEntrante es síncrono), así que estas dos
	// variables no necesitan candado: no hay segundo hilo que las toque.
	s.setLiveInyector(func(_ context.Context, p app.InyeccionEntrante) (bool, error) {
		llamadas++
		recibida = p
		return false, nil
	})

	acusar, err := m.InyectarEntrante(context.Background(), uuidA, inyeccionDePrueba)

	if err != nil {
		t.Fatalf("con el cable puesto la inyección no debería fallar en el enrutado; got %v", err)
	}
	if llamadas != 1 {
		t.Fatalf("la closure del ciclo de escucha se invocó %d veces, want 1", llamadas)
	}
	if recibida != inyeccionDePrueba {
		t.Errorf("la carga llegó ALTERADA al cable:\n  got  %+v\n  want %+v", recibida, inyeccionDePrueba)
	}
	if acusar {
		t.Error("el Manager devolvió acuse=true cuando el camino real devolvió false: el enrutador está " +
			"fabricando el acuse en vez de reenviarlo, y una medición contra un inyector así sale sana " +
			"aunque no se haya recorrido nada")
	}
}

// TestManager_InyectarEntrante_NoSostieneElCandadoDelManager custodia la propiedad de concurrencia que el
// método documenta: `m.mu` se suelta ANTES de entrar al camino de inyección.
//
// CÓMO LO PRUEBA: la closure cableada vuelve a tocar el candado del Manager (m.List()) desde dentro de la
// llamada. Si InyectarEntrante siguiera sosteniendo m.mu, eso es un auto-bloqueo —sync.Mutex no es
// reentrante— y la goroutine no volvería nunca; el temporizador lo convierte en un fallo legible en vez de
// en un test colgado hasta el timeout del paquete.
//
// POR QUÉ IMPORTA DE VERDAD: la inyección real no es una closure de test, es el handler entrante completo,
// con su escritura cifrada en SQLite. Sostener m.mu durante ese I/O congela Pair, Restore, Unlink, List,
// Health y el colector de salud —todos pasan por ese candado— y lo hace justo durante una tanda de N
// inyecciones seguidas, que es cuando el daemon está más ocupado.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO:
//   - en InyectarEntrante, cambiar el `m.mu.Unlock()` por un `defer m.mu.Unlock()` ⇒ el candado se sostiene
//     durante toda la inyección y este test se cuelga hasta el temporizador (que es el rojo).
//   - lo mismo en inyectarVia con `s.mu` si la closure tocara la sesión.
func TestManager_InyectarEntrante_NoSostieneElCandadoDelManager(t *testing.T) {
	m, s := managerConSesionViva(t, uuidA)

	s.setLiveInyector(func(_ context.Context, _ app.InyeccionEntrante) (bool, error) {
		// Toca el candado del Manager DESDE DENTRO del camino de inyección, igual que lo tocaría cualquier
		// lector concurrente mientras la inyección escribe su fila.
		_ = m.List()
		return true, nil
	})

	hecho := make(chan struct{})
	go func() {
		defer close(hecho)
		_, _ = m.InyectarEntrante(context.Background(), uuidA, inyeccionDePrueba)
	}()

	select {
	case <-hecho:
	case <-time.After(2 * time.Second):
		t.Fatal("la inyección no volvió: el Manager sostiene m.mu mientras corre el camino entrante, así que " +
			"cualquier lectura del registro de sesiones (List/Health/el colector de salud) se bloquea durante " +
			"toda la escritura cifrada de la fila")
	}
}
