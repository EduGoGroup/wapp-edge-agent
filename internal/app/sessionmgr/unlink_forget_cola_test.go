package sessionmgr

// unlink_forget_cola_test.go — el cableado de colaentrantes.Store.Forget en el teardown de sesión
// (Plan 051 Ola 2 · T2.9).
//
// 🔴 LO QUE DE VERDAD HAY QUE PROTEGER AQUÍ ES LA TYPE ASSERTION. forgetColaCrypter llega al método por
// `m.cola.(interface{ Forget(string) })` —m.cola es app.ColaEntrantes, el puerto del ENCOLADO, y no tiene
// por qué crecer con un detalle de caché del adaptador—, y el precio de ese patrón es que si la firma de
// Forget cambia (un ctx, un error de retorno, un nombre distinto) la aserción deja de casar EN SILENCIO:
// no hay error de compilación, simplemente el teardown deja de olvidar. Ese es el modo de fallo que estos
// tests cierran.

import (
	"context"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/colaentrantes"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// El Store REAL de la cola tiene que satisfacer la misma forma que busca forgetColaCrypter. Es una
// aserción de COMPILACIÓN: si Forget cambia de firma, esto no compila, en vez de degradarse a no-op.
var _ interface{ Forget(string) } = (*colaentrantes.Store)(nil)

// spyForgetCola es una cola de entrantes que solo anota a quién le pidieron olvidar.
type spyForgetCola struct {
	olvidadas []string
}

var _ app.ColaEntrantes = (*spyForgetCola)(nil)

func (s *spyForgetCola) Enqueue(context.Context, app.ColaItem) error { return nil }

func (s *spyForgetCola) Forget(sessionID string) {
	s.olvidadas = append(s.olvidadas, sessionID)
}

// colaSinForget es una cola que NO implementa Forget: el caso de un adaptador antiguo o de un fake de
// test. El teardown tiene que seguir funcionando (no-op), no romperse.
type colaSinForget struct{}

var _ app.ColaEntrantes = (*colaSinForget)(nil)

func (colaSinForget) Enqueue(context.Context, app.ColaItem) error { return nil }

// TestForgetColaCrypterAvisaALaCola: con una cola que sabe olvidar, el teardown le pasa EXACTAMENTE el
// session_id que se está desmontando, y solo ese.
func TestForgetColaCrypterAvisaALaCola(t *testing.T) {
	spy := &spyForgetCola{}
	m := &Manager{cola: spy}

	m.forgetColaCrypter("sesion-1")
	m.forgetColaCrypter("sesion-2")

	if len(spy.olvidadas) != 2 || spy.olvidadas[0] != "sesion-1" || spy.olvidadas[1] != "sesion-2" {
		t.Fatalf("sesiones olvidadas: got %v, want [sesion-1 sesion-2]", spy.olvidadas)
	}
}

// TestForgetColaCrypterSinColaEsNoOp: la cola es OPCIONAL (la opción puede no inyectarse, o la BD de la
// cola puede no haberse podido abrir). Un teardown no puede caerse por eso.
func TestForgetColaCrypterSinColaEsNoOp(t *testing.T) {
	m := &Manager{} // m.cola nil
	m.forgetColaCrypter("sesion-1")

	m2 := &Manager{cola: &colaSinForget{}} // cola que no sabe olvidar
	m2.forgetColaCrypter("sesion-1")
}

// TestForgetColaCrypterConColaTypedNilEsNoOp cierra el camino que el test de arriba NO cubre, y que es el
// único que de verdad puede acabar en pánico.
//
// 🔴 LA DIFERENCIA ENTRE «nil» Y «typed nil» ES TODO EL BUG, y conviene ser exacto sobre QUIÉN lo para:
//
//   - `&Manager{}` deja m.cola como una interfaz nil. Ahí paran DOS cosas: la guarda `m.cola == nil` (que
//     es la explícita) y, por detrás, la aserción de tipo, que sobre una interfaz nil nunca casa.
//   - `&Manager{cola: (*colaentrantes.Store)(nil)}` es OTRA cosa: la interfaz NO es nil (lleva tipo), así
//     que la guarda `m.cola == nil` NO muerde y la aserción SÍ casa. Lo único que evita el pánico aquí es
//     que el propio Forget aguante el receptor nil. Ese es el caso que este test fija.
//
// Y es un caso alcanzable, no una rareza de laboratorio: basta con que un BuildCola futuro se escriba como
// `var s *colaentrantes.Store; …; return s`, que es la forma natural de esas funciones. Hoy devuelve el nil
// LITERAL y por eso no hay pánico; ese detalle no debería ser lo que sostiene un teardown.
//
// Se prueba con el Store REAL y no con un doble: lo que importa es que el tipo concreto que se inyecta en
// producción aguante el receptor nil.
func TestForgetColaCrypterConColaTypedNilEsNoOp(t *testing.T) {
	var vacia *colaentrantes.Store
	// La interfaz NO es nil: lleva tipo (*colaentrantes.Store) y valor nil.
	m := &Manager{cola: vacia}
	if m.cola == nil {
		t.Fatal("premisa del test rota: un typed nil dentro de una interfaz NO es una interfaz nil")
	}
	// Sin el `if s == nil` de Forget, esto entra en pánico.
	m.forgetColaCrypter("sesion-1")
}
