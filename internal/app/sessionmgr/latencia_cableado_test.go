package sessionmgr

// latencia_cableado_test.go — EL TRAMO DE EN MEDIO DEL CRONÓMETRO (Plan 051 Ola 3 · T3.13).
//
// 🔴 EL AGUJERO QUE ESTE FICHERO CIERRA. El histograma del handler viaja por una cadena de cinco tramos —
// daemon.buildLatencia → sessionmgr.WithLatencia → Manager.latencia → gateway.SetLatencia →
// whatsmeow.WithLatencia → Listener.latencia— y hasta esta tarea SOLO estaban custodiados los dos
// EXTREMOS: internal/infra/daemon/latencia_cableado_test.go (el primero) y
// internal/adapters/whatsmeow/listener_latencia_test.go (el último). El de en medio —la línea
// `gateway.SetLatencia(m.latencia)` del factory de WithWhatsmeowListen— no lo miraba NADIE: `grep -rn
// SetLatencia` daba un único llamante de producción y cero tests.
//
// Consecuencia de borrarla (verificada por mutación, no supuesta): `go build`, `go vet`, `go test ./... -p 1`
// y `make ci-docker` TODOS EN VERDE, y en campo el bloque de latencia del latido sale con `n=0` para
// siempre, con el Edge atendiendo mensajes con normalidad. El síntoma —«no hay dato»— no apunta a la
// causa, y el criterio de cierre de la ola (INV-051.2, «handler < 50 ms p99») quedaría otra vez sin
// instrumento, que es exactamente el estado del que T3.13 salió.
//
// POR QUÉ NO BASTA CON LO QUE YA HABÍA. Los dos tests de los extremos son correctos y siguen haciendo
// falta, pero cada uno mira SU punta: el del daemon comprueba que la Option deja el histograma en el
// Manager, y el del listener que un Listener con WithLatencia mide. Entre los dos hay un tramo que ningún
// test recorría, y un cable roto justo ahí deja las dos puntas «bien» y el circuito abierto.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo, y todas se ejecutaron.

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/latencia"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
)

// managerConCronometro arma un Manager como el de campo —BD única compartida + escucha REAL sobre
// whatsmeow (WithWhatsmeowListen, el factory que este fichero interroga)— con el cronómetro inyectado.
//
// Se usa el factory DE PRODUCCIÓN a propósito: los demás tests de escucha del paquete inyectan un
// `fakeFabric` que no construye ningún ListenGateway, y con un doble ahí no habría nada que custodiar —el
// cable que se quiere ver es justo el que el doble sustituye.
func managerConCronometro(t *testing.T, h *latencia.Histograma) *Manager {
	t.Helper()
	base := filepath.Join(t.TempDir(), "edge-data")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatalf("crear data_dir: %v", err)
	}
	database, err := wappdb.OpenAndMigrate(context.Background(), filepath.Join(base, "edge.db"))
	if err != nil {
		t.Fatalf("abrir/migrar la BD única: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	m := NewManager(NewLayout(base), sessionstore.New(database), 5, testLogger(),
		WithSharedDB(database, wappdb.DialectSQLite),
		WithWhatsmeowListen(&recordMux{}, ""),
		WithLatencia(h))
	m.newCustody = newMemCustodyFactory() // doble en memoria: no tocar el Keychain real (Plan 023 T2)
	return m
}

// TestWithWhatsmeowListen_ElCronometroLLEGA_AlGatewayDeCadaSesion es EL test de este fichero: el
// histograma que el daemon inyectó en el Manager tiene que acabar DENTRO del ListenGateway que el factory
// construye para cada sesión — que es de donde lo recoge el Listener al arrancar (WithLatencia en serve).
//
// Se invoca el factory REAL (`m.newListener`) en vez de arrancar una sesión entera porque lo que se mide
// es el CABLE, no la escucha: `NewListenGatewayForDevice` solo compone un cargador de device diferido, así
// que el gateway se construye sin tocar WhatsApp, sin red y sin device pareado. Se afirma por IDENTIDAD DE
// PUNTERO: dos histogramas iguales pero distintos son exactamente el fallo que esto busca (el que llena no
// es el que se publica).
//
// La lectura es por REFLEXIÓN sobre campos no exportados, igual que en el test gemelo del daemon: el
// gateway vive dentro del *app.Listen que devuelve el factory y no hay —ni debe haber— un accesor público
// para el cronómetro. `Pointer()` sí está permitido sobre un campo no exportado (a diferencia de
// `Interface()`), y comparar direcciones es justo lo que hace falta.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - borrar `gateway.SetLatencia(m.latencia)` del factory (listen.go) ⇒ el gateway queda con el campo a
//     nil y el bloque del latido sale con n=0 en campo, con todos los gates en verde.
//   - `gateway.SetLatencia(latencia.Nuevo())` ⇒ cada sesión llenaría SU propio histograma y el que publica
//     el latido no lo llenaría nadie: el mismo agujero, sin nil que lo delate.
func TestWithWhatsmeowListen_ElCronometroLLEGA_AlGatewayDeCadaSesion(t *testing.T) {
	h := latencia.Nuevo()
	m := managerConCronometro(t, h)

	s := &liveSession{
		meta: domain.Session{SessionID: "sess-cronometro", JID: "56911112222:1@s.whatsapp.net"},
		log:  testLogger(),
	}
	runner, _, err := m.newListener(context.Background(), s)
	if err != nil {
		t.Fatalf("el factory de escucha falló al construir la sesión: %v", err)
	}

	enGateway := cronometroDelGateway(t, runner)
	if enGateway == 0 {
		t.Fatal("el factory de WithWhatsmeowListen NO cableó el cronómetro en el ListenGateway de la sesión.\n" +
			"    CONSECUENCIA: el Listener arranca sin histograma, así que el cronómetro DEJA DE LLENARSE y el\n" +
			"    bloque de latencia del latido sale con n=0 y sin percentiles PARA SIEMPRE — con el Edge\n" +
			"    atendiendo mensajes con normalidad y los cuatro gates en verde. INV-051.2 vuelve a ser\n" +
			"    incomprobable en campo, que es el estado del que T3.13 vino a sacarlo.\n" +
			"    SI EL CAMBIO ES DELIBERADO (se retira el cronómetro): hay que retirar también el bloque de\n" +
			"    latencia del latido y el criterio que cuelga de él, no solo esta línea.")
	}
	if quiero := reflect.ValueOf(h).Pointer(); enGateway != quiero {
		t.Errorf("el gateway de la sesión llena un histograma DISTINTO del que se le inyectó (%#x != %#x).\n"+
			"    CONSECUENCIA: los listeners miden de verdad, pero sobre un instrumento que nadie publica; el\n"+
			"    latido sigue sacando n=0 y encima no hay ningún nil que delate el fallo.",
			enGateway, quiero)
	}
}

// TestWithWhatsmeowListen_SinCronometro_LaSesionArrancaIGUAL fija la degradación honesta del cable: la
// observabilidad NUNCA puede impedir que una sesión escuche. Sin WithLatencia el factory tiene que
// construir su gateway igual (campo a nil, listener sin medir), no fallar ni entrar en pánico.
//
// Está aquí y no en el test de arriba porque es la otra mitad de la misma decisión: el cable es
// OBLIGATORIO cuando existe el instrumento y OPCIONAL cuando no lo hay. Sin este caso, «arreglar» el rojo
// del test anterior haciendo obligatorio el histograma pasaría inadvertido.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: hacer que el factory exija el cronómetro (devolver error, o llamar a
// algo sobre m.latencia sin comprobar el nil).
func TestWithWhatsmeowListen_SinCronometro_LaSesionArrancaIgual(t *testing.T) {
	m := managerConCronometro(t, nil) // WithLatencia(nil) se ignora: el Manager queda sin cronómetro

	s := &liveSession{
		meta: domain.Session{SessionID: "sess-sin-cronometro", JID: "56911112222:2@s.whatsapp.net"},
		log:  testLogger(),
	}
	runner, _, err := m.newListener(context.Background(), s)
	if err != nil {
		t.Fatalf("sin cronómetro el factory tiene que construir la sesión igual, y falló: %v", err)
	}
	if got := cronometroDelGateway(t, runner); got != 0 {
		t.Errorf("el gateway quedó con un cronómetro (%#x) que nadie inyectó: el nil de WithLatencia debe "+
			"dejar el campo vacío, no fabricar un histograma huérfano que nadie publica", got)
	}
}

// cronometroDelGateway baja por reflexión desde el runner que devuelve el factory hasta el campo donde el
// gateway guarda el cronómetro, y devuelve su DIRECCIÓN (0 si no hay ninguno).
//
// ⚠️ SI ALGUNO DE ESTOS `Fatal` SALTA, no es el test el que está roto: alguien renombró un campo de la
// cadena y hay que actualizar el nombre aquí. Se prefiere ese rojo ruidoso a un test que deje de mirar en
// silencio — que es, literalmente, el fallo que este fichero existe para cazar.
func cronometroDelGateway(t *testing.T, runner listenRunner) uintptr {
	t.Helper()

	v := reflect.ValueOf(runner)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		t.Fatalf("el factory devolvió un runner inesperado (%T): se esperaba un *app.Listen", runner)
	}
	campoGW := v.Elem().FieldByName("gateway")
	if !campoGW.IsValid() {
		t.Fatal("app.Listen ya no tiene el campo `gateway`: ¿se renombró? Este test mira ahí a propósito, " +
			"porque es donde el factory deja el ListenGateway que lleva el cronómetro")
	}
	gw := campoGW.Elem() // valor dinámico de la interfaz: el *whatsmeow.ListenGateway real
	if !gw.IsValid() || gw.Kind() != reflect.Pointer || gw.IsNil() {
		t.Fatalf("el runner no lleva un gateway utilizable (%v): el factory de producción tiene que haber "+
			"construido uno con NewListenGatewayForDevice", campoGW.Kind())
	}
	campo := gw.Elem().FieldByName("latencia")
	if !campo.IsValid() {
		t.Fatal("whatsmeow.ListenGateway ya no tiene el campo `latencia`: ¿se renombró? Es el campo que " +
			"SetLatencia rellena y que serve() pasa al Listener")
	}
	return campo.Pointer()
}
