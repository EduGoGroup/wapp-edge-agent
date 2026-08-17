package colaentrantes

// forget_test.go — la evicción del caché de sobres (Plan 051 Ola 2 · T2.9).
//
// Reutiliza los helpers de colaentrantes_test.go (openDB con la migración REAL, newStore, dekFor,
// failingCrypterFor): mismo paquete a propósito.
//
// Lo que se prueba NO es «el caché funciona» (de eso va TestCrypterSeCacheaPorSesion) sino lo contrario:
// que se pueda DESHACER. El caché sin evicción no falla nunca de forma ruidosa —hoy es una fuga de ~60 B,
// porque cada emparejamiento acuña un session_id nuevo—, pero el día que se rote la DEK conservando el
// session_id sellaría con la llave vieja en silencio. El test de la rotación de abajo es el que fija esa
// promesa por escrito.

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-shared/envelope"
)

// rotatingCrypterFor es un CrypterFor cuya DEK CAMBIA de versión sin cambiar el session_id: es la única
// forma de reproducir el caso que devuelve la gravedad a T2.9 (rotación de llaves sin re-emparejar).
type rotatingCrypterFor struct {
	mu       sync.Mutex
	llamadas map[string]int
	version  byte
}

func newRotatingCrypterFor() *rotatingCrypterFor {
	return &rotatingCrypterFor{llamadas: make(map[string]int)}
}

func (r *rotatingCrypterFor) fn(sessionID string) (envelope.Crypter, error) {
	r.mu.Lock()
	r.llamadas[sessionID]++
	v := r.version
	r.mu.Unlock()
	return envelope.NewEnvelope(dekRotada(sessionID, v))
}

func (r *rotatingCrypterFor) count(sessionID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.llamadas[sessionID]
}

func (r *rotatingCrypterFor) rota() {
	r.mu.Lock()
	r.version++
	r.mu.Unlock()
}

// dekRotada deriva la DEK de una sesión EN UNA VERSIÓN dada. La versión 0 coincide con dekFor, para no
// divergir del resto de helpers del paquete.
func dekRotada(sessionID string, version byte) []byte {
	dek := dekFor(sessionID) // devuelve un slice nuevo en cada llamada: mutarlo aquí es seguro
	dek[len(dek)-1] ^= version
	return dek
}

// textoEncDe lee el blob sellado de una fila por su wa_message_id (nunca lo descifra: el test decide con
// qué DEK intentarlo).
func textoEncDe(t *testing.T, s *Store, waID string) []byte {
	t.Helper()
	var blob []byte
	if err := s.db.QueryRow(`SELECT texto_enc FROM cola_entrantes WHERE wa_message_id = ?`, waID).Scan(&blob); err != nil {
		t.Fatalf("leer texto_enc de %s: %v", waID, err)
	}
	return blob
}

// TestForgetReinvocaElResolutorUnaSolaVez: tras Forget, el siguiente Enqueue vuelve a preguntar a la
// custodia — UNA vez, no en cada insert (el caché se rehace).
func TestForgetReinvocaElResolutorUnaSolaVez(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	cf := newFakeCrypterFor()
	s := newStore(t, db, cf.fn, 100, 0)

	for i, wa := range []string{"wa1", "wa2", "wa3"} {
		if err := s.Enqueue(ctx, item("A", "chat@s", wa, "hola")); err != nil {
			t.Fatalf("Enqueue previo %d: %v", i, err)
		}
	}
	if n := cf.count("A"); n != 1 {
		t.Fatalf("antes de Forget: CrypterFor(A) llamado %d veces, esperaba 1", n)
	}

	s.Forget("A")

	for i, wa := range []string{"wa4", "wa5", "wa6"} {
		if err := s.Enqueue(ctx, item("A", "chat@s", wa, "hola")); err != nil {
			t.Fatalf("Enqueue posterior %d: %v", i, err)
		}
	}
	// 2 y no 4: Forget invalida el caché, no lo desactiva.
	if n := cf.count("A"); n != 2 {
		t.Fatalf("tras Forget: CrypterFor(A) llamado %d veces, esperaba 2 (una re-resolución y vuelta a cachear)", n)
	}
}

// TestForgetNoTocaLasDemasSesiones: el olvido es QUIRÚRGICO. Un teardown de una sesión no puede obligar a
// las otras N sesiones vivas a volver a la custodia (ese es el coste que el caché existe para evitar).
func TestForgetNoTocaLasDemasSesiones(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	cf := newFakeCrypterFor()
	s := newStore(t, db, cf.fn, 100, 0)

	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-a1", "hola")); err != nil {
		t.Fatalf("Enqueue A: %v", err)
	}
	if err := s.Enqueue(ctx, item("B", "chat@s", "wa-b1", "hola")); err != nil {
		t.Fatalf("Enqueue B: %v", err)
	}

	s.Forget("A")

	if err := s.Enqueue(ctx, item("B", "chat@s", "wa-b2", "hola")); err != nil {
		t.Fatalf("Enqueue B tras Forget(A): %v", err)
	}
	if n := cf.count("B"); n != 1 {
		t.Fatalf("Forget(A) no debía tocar a B: CrypterFor(B) llamado %d veces, esperaba 1", n)
	}
}

// TestForgetDeSesionDesconocidaEsNoOp: olvidar algo que nunca se supo no puede entrar en pánico ni dejar
// el caché peor de lo que estaba. Importa porque el teardown llama a Forget SIEMPRE, incluso para sesiones
// que jamás encolaron un mensaje (una sesión emparejada y desvinculada sin tráfico).
func TestForgetDeSesionDesconocidaEsNoOp(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	cf := newFakeCrypterFor()
	s := newStore(t, db, cf.fn, 100, 0)

	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-a1", "hola")); err != nil {
		t.Fatalf("Enqueue A: %v", err)
	}

	s.Forget("no-existe")
	s.Forget("no-existe") // dos veces: idempotente

	if err := s.Enqueue(ctx, item("A", "chat@s", "wa-a2", "hola")); err != nil {
		t.Fatalf("Enqueue A tras Forget de una sesión desconocida: %v", err)
	}
	if n := cf.count("A"); n != 1 {
		t.Fatalf("un Forget de otra sesión no debía invalidar a A: CrypterFor(A) llamado %d veces, esperaba 1", n)
	}
}

// TestForgetSobreReceptorNilEsNoOp: el otro extremo de la guarda de sessionmgr.forgetColaCrypter.
//
// 🔴 EL CAMINO QUE ESTO CIERRA NO ES HIPOTÉTICO POR CONSTRUCCIÓN. El teardown llega a Forget por una
// ASERCIÓN DE TIPO sobre la interfaz del puerto, y una interfaz que envuelve un `(*Store)(nil)` —el typed
// nil que sale de `var s *Store; return s`, una línea perfectamente natural en un BuildCola futuro— NO es
// una interfaz nil: la aserción CASA y se invoca el método sobre un receptor nil. Sin el `if s == nil`,
// el `s.cryptersMu.Lock()` desreferenciaría nil y el Unlink moriría en pánico.
//
// La segunda mitad del test es la importante: prueba por la MISMA vía que el teardown (interfaz + aserción),
// no llamando al método a pelo, porque es esa vía la que hace alcanzable el receptor nil.
func TestForgetSobreReceptorNilEsNoOp(t *testing.T) {
	var s *Store
	s.Forget("sesion-1") // llamada directa: no debe entrar en pánico
	s.Forget("")         // e idempotente, como el resto de Forget

	// Por la vía real del teardown: un typed nil dentro del puerto.
	// SA4023 a propósito: que esta comparación NUNCA sea true es EXACTAMENTE lo que el test demuestra.
	// Un `(*Store)(nil)` dentro de una interfaz lleva tipo, así que la interfaz no es nil y la aserción de
	// abajo casa, alcanzando el método con receptor nil. Si algún día el linter tuviera razón —si esto
	// llegara a ser true— la premisa del guardarraíl se habría roto y el `t.Fatal` debe dispararse.
	var cola app.ColaEntrantes = (*Store)(nil) //nolint:staticcheck // ver comentario: el typed nil es el punto del test
	if cola == nil {                           //nolint:staticcheck // idem: comparación deliberadamente siempre-falsa
		t.Fatal("premisa del test rota: un typed nil dentro de una interfaz NO es una interfaz nil")
	}
	f, ok := cola.(interface{ Forget(string) })
	if !ok {
		t.Fatal("premisa del test rota: la aserción del teardown debía casar con un *Store typed-nil")
	}
	f.Forget("sesion-1") // el pánico que este guardarraíl existe para evitar
}

// TestForgetPermiteRotarLaDEKConservandoElSessionID es EL TEST QUE JUSTIFICA LA TAREA, y prueba el caso
// que hoy no ocurre pero que devuelve la gravedad a T2.9: la DEK cambia y el session_id NO. Sin Forget, el
// caché seguiría sellando con la llave vieja y el fallo no se vería aquí sino después, al abrir la fila —
// un tag GCM que no valida, es decir, un mensaje guardado que nadie puede leer.
func TestForgetPermiteRotarLaDEKConservandoElSessionID(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	cf := newRotatingCrypterFor()
	s := newStore(t, db, cf.fn, 100, 0)

	if err := s.Enqueue(ctx, item("S", "chat@s", "wa-viejo", "antes de rotar")); err != nil {
		t.Fatalf("Enqueue con la DEK v0: %v", err)
	}

	// La custodia rota la llave de la MISMA sesión; el teardown/rotación llama a Forget.
	cf.rota()
	s.Forget("S")

	if err := s.Enqueue(ctx, item("S", "chat@s", "wa-nuevo", "después de rotar")); err != nil {
		t.Fatalf("Enqueue con la DEK v1: %v", err)
	}
	if n := cf.count("S"); n != 2 {
		t.Fatalf("CrypterFor(S) llamado %d veces, esperaba 2 (una por versión de DEK)", n)
	}

	envV0, err := envelope.NewEnvelope(dekRotada("S", 0))
	if err != nil {
		t.Fatalf("NewEnvelope v0: %v", err)
	}
	envV1, err := envelope.NewEnvelope(dekRotada("S", 1))
	if err != nil {
		t.Fatalf("NewEnvelope v1: %v", err)
	}

	// La fila NUEVA abre con la DEK NUEVA…
	if _, err := envV1.Open(textoEncDe(t, s, "wa-nuevo")); err != nil {
		t.Fatalf("la fila posterior a la rotación debía abrir con la DEK v1: %v", err)
	}
	// …y NO con la vieja (si abriera, el Forget no habría servido de nada).
	if _, err := envV0.Open(textoEncDe(t, s, "wa-nuevo")); err == nil {
		t.Fatal("la fila posterior a la rotación NO debía abrir con la DEK v0: el caché siguió sellando con la llave vieja")
	}
	// La fila anterior sigue siendo legible con la llave con la que se selló (la rotación no reescribe nada).
	if _, err := envV0.Open(textoEncDe(t, s, "wa-viejo")); err != nil {
		t.Fatalf("la fila anterior a la rotación debía seguir abriendo con la DEK v0: %v", err)
	}
}

// TestForgetTambienOlvidaElCacheNegativo: los dos mapas son el mismo estado ("qué sé yo del sobre de esta
// sesión"), así que Forget tiene que borrar los dos. Si solo borrara el positivo, una sesión que falló
// arrastraría su error memorizado hasta 60 s después de haberla olvidado, y una sesión recién montada
// heredaría el fallo de la anterior encarnación.
func TestForgetTambienOlvidaElCacheNegativo(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	cf := &failingCrypterFor{falla: true}
	s := newStore(t, db, cf.fn, 100, 0)

	if err := s.Enqueue(ctx, item("S", "chat@s", "wa1", "hola")); err == nil {
		t.Fatal("Enqueue con la custodia caída debía fallar")
	}
	// Dentro del enfriamiento, el error viene del caché negativo (marcado, sin tocar la custodia).
	err := s.Enqueue(ctx, item("S", "chat@s", "wa2", "hola"))
	if err == nil || !errors.Is(err, app.ErrColaFalloRepetido) {
		t.Fatalf("dentro del enfriamiento se esperaba el error memorizado (ErrColaFalloRepetido), got %v", err)
	}
	if n := cf.count(); n != 1 {
		t.Fatalf("CrypterFor llamado %d veces, esperaba 1 (caché negativo)", n)
	}

	// La custodia vuelve y la sesión se olvida: NO hay que esperar los 60 s del enfriamiento.
	cf.recupera()
	s.Forget("S")

	if err := s.Enqueue(ctx, item("S", "chat@s", "wa3", "hola")); err != nil {
		t.Fatalf("tras Forget, el Enqueue debía reintentar la custodia sin esperar el enfriamiento: %v", err)
	}
	if n := cf.count(); n != 2 {
		t.Fatalf("CrypterFor llamado %d veces, esperaba 2 (el fallo y el reintento forzado por Forget)", n)
	}
}
