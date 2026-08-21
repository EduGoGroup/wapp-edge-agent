package sessionmgr

// sesion_pasiva_cableado_test.go — EL TRAMO DE EN MEDIO DEL FILTRO DE PERFILES (Plan 046 · Ola 2 · T2.2).
//
// 🔴 EL AGUJERO QUE ESTE FICHERO CIERRA, y es el mismo que el del cronómetro con otro nombre. El consultor de
// perfiles viaja por una cadena de cinco tramos —
//
//	daemon.opcionPerfilesSesion → sessionmgr.WithSesionPasiva → Manager.sesionPasiva
//	    → gateway.SetSesionPasiva → whatsmeow.WithSesionPasiva → Listener.sesionPasiva
//
// — y hasta esta tarea SOLO estaba custodiado el ÚLTIMO: `whatsmeow/listener_perfil_test.go` prueba que un
// Listener con el predicado cableado corta, y `TestListenerOpts_*` que la opción sale de `listenerOpts()`.
// Nadie miraba la línea `gateway.SetSesionPasiva(m.sesionPasiva)` del factory de `WithWhatsmeowListen`
// (listen.go) ni la del daemon: `grep -rn SetSesionPasiva` daba UN llamante de producción y CERO tests.
//
// Consecuencia de borrar cualquiera de las dos: `go build`, `go vet` y los cuatro gates EN VERDE, con el mapa
// de perfiles perfectamente construido, empujado por la nube y aplicado… y ningún Listener consultándolo.
// TODAS las sesiones pasivas de la flota siguen encolando, persistiendo y entregando su tráfico entrante, y
// REQ-07 queda incumplido SIN UN SOLO SÍNTOMA — el filtro no deja fila, no sube al cable y acusa a WhatsApp
// exactamente igual que si hubiera entregado, así que desde fuera «funciona» y «no existe» son la misma foto.
//
// POR QUÉ NO BASTA CON LO QUE YA HABÍA: los tests del listener miran SU punta y le inyectan el predicado a
// mano. Entre ese punto y el daemon hay dos cables que ningún test recorría, y uno roto ahí deja las dos
// puntas «bien» y el circuito abierto.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo.

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
)

// managerConPerfiles arma un Manager como el de campo —BD única compartida + escucha REAL sobre whatsmeow
// (WithWhatsmeowListen, el factory que este fichero interroga)— con el consultor de perfiles inyectado.
//
// Se usa el factory DE PRODUCCIÓN a propósito, igual que en latencia_cableado_test.go: los demás tests de
// escucha del paquete inyectan un `fakeFabric` que no construye ningún ListenGateway, y con un doble ahí no
// habría nada que custodiar — el cable que se quiere ver es justo el que el doble sustituye.
func managerConPerfiles(t *testing.T, pred func(string) bool) *Manager {
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
		WithSesionPasiva(pred))
	m.newCustody = newMemCustodyFactory() // doble en memoria: no tocar el Keychain real (Plan 023 T2)
	return m
}

// TestWithWhatsmeowListen_ElConsultorDePerfiles_LLEGA_AlGatewayDeCadaSesion es EL test de este fichero: el
// predicado que el daemon inyectó en el Manager tiene que acabar DENTRO del ListenGateway que el factory
// construye para cada sesión — que es de donde lo recoge el Listener al arrancar (listenerOpts → serve).
//
// Se invoca el factory REAL (`m.newListener`) en vez de arrancar una sesión entera porque lo que se mide es
// el CABLE, no la escucha: `NewListenGatewayForDevice` solo compone un cargador de device diferido, así que
// el gateway se construye sin tocar WhatsApp, sin red y sin device pareado.
//
// Se afirma por IDENTIDAD DE PUNTERO (el código de la función) y no solo por «no es nil», y la diferencia
// importa: el modo de fallo que se busca no es únicamente el olvido, es el SUSTITUTO. Un
// `SetSesionPasiva(func(string) bool { return false })` —el «arreglo» plausible de quien no entiende para qué
// está la línea— dejaría el campo no-nil, todos los gates en verde y el filtro apagado en toda la flota.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - borrar `gateway.SetSesionPasiva(m.sesionPasiva)` del factory (listen.go) ⇒ el campo queda a nil y
//     ninguna sesión pasiva se corta jamás;
//   - meterlo dentro de un `if m.cola != nil` (o de cualquier otro condicional de la cola) ⇒ el filtro
//     dependería de algo con lo que no tiene relación;
//   - pasar cualquier otro predicado ⇒ cae la comparación de punteros.
func TestWithWhatsmeowListen_ElConsultorDePerfiles_LLEGA_AlGatewayDeCadaSesion(t *testing.T) {
	// Predicado con cuerpo PROPIO (no `func(string) bool { return false }` a secas): su código tiene una
	// dirección única, y eso es lo que hace falsable la comparación de punteros de abajo.
	pred := func(id string) bool { return id == "sess-perfiles" }
	m := managerConPerfiles(t, pred)

	s := &liveSession{
		meta: domain.Session{SessionID: "sess-perfiles", JID: "56911112222:1@s.whatsapp.net"},
		log:  testLogger(),
	}
	runner, _, err := m.newListener(context.Background(), s)
	if err != nil {
		t.Fatalf("el factory de escucha falló al construir la sesión: %v", err)
	}

	enGateway := consultorDelGateway(t, runner)
	if enGateway == 0 {
		t.Fatal("el factory de WithWhatsmeowListen NO cableó el consultor de perfiles en el ListenGateway.\n" +
			"    CONSECUENCIA: el Listener arranca sin consultor, así que TODAS las sesiones marcadas PASIVAS por\n" +
			"    la nube siguen encolando, persistiendo y entregando su tráfico entrante. REQ-07 incumplido con\n" +
			"    los cuatro gates en VERDE y sin un solo síntoma: el filtro no deja fila, no sube al cable y\n" +
			"    acusa igual que si hubiera entregado, así que «funciona» y «no existe» producen la misma foto.\n" +
			"    SI EL CAMBIO ES DELIBERADO (se retira el filtro): hay que retirar también el contador\n" +
			"    `dropped_passive`, el campo `filters_version` y la ficha de la funcionalidad, no solo esta línea.")
	}
	if quiero := reflect.ValueOf(pred).Pointer(); enGateway != quiero {
		t.Errorf("el gateway de la sesión consulta un predicado DISTINTO del que se le inyectó (%#x != %#x).\n"+
			"    CONSECUENCIA: el mapa de perfiles se aplica y se mantiene al día, y la puerta pregunta a otro\n"+
			"    sitio. No hay nil que delate el fallo: el filtro simplemente contesta lo que no es.",
			enGateway, quiero)
	}
}

// TestWithWhatsmeowListen_SinConsultor_LaSesionArrancaIGUAL fija la otra mitad de la decisión: sin el
// consultor cableado (Edge sin config de filtros, cableados que no vienen del daemon) el factory tiene que
// construir su gateway igual, con el campo a nil, y el Listener cae a su default FAIL-OPEN.
//
// Está aquí y no en el test de arriba porque sin este caso «arreglar» el rojo del anterior haciendo
// obligatorio el predicado pasaría inadvertido — y eso dejaría sesiones sin escuchar por una config de
// privacidad ausente, que es exactamente la asimetría que D-046.2 prohíbe.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: hacer que el factory exija el consultor (devolver error, o llamar a algo
// sobre m.sesionPasiva sin comprobar el nil).
func TestWithWhatsmeowListen_SinConsultor_LaSesionArrancaIgual(t *testing.T) {
	m := managerConPerfiles(t, nil) // WithSesionPasiva(nil) se ignora: el Manager queda sin consultor

	s := &liveSession{
		meta: domain.Session{SessionID: "sess-sin-perfiles", JID: "56911112222:2@s.whatsapp.net"},
		log:  testLogger(),
	}
	runner, _, err := m.newListener(context.Background(), s)
	if err != nil {
		t.Fatalf("sin consultor de perfiles el factory tiene que construir la sesión igual, y falló: %v", err)
	}
	if got := consultorDelGateway(t, runner); got != 0 {
		t.Errorf("el gateway quedó con un consultor (%#x) que nadie inyectó: el nil de WithSesionPasiva debe "+
			"dejar el campo vacío y que mande el default FAIL-OPEN del Listener, no fabricar un predicado "+
			"huérfano que decida sobre el tráfico de nadie", got)
	}
}

// consultorDelGateway baja por reflexión desde el runner que devuelve el factory hasta el campo donde el
// gateway guarda el consultor de perfiles, y devuelve la DIRECCIÓN DE SU CÓDIGO (0 si no hay ninguno).
//
// Mismo molde —y mismas cautelas— que `cronometroDelGateway` en latencia_cableado_test.go: el gateway vive
// dentro del *app.Listen que devuelve el factory y no hay —ni debe haber— un accesor público para esto.
// `Pointer()` sí está permitido sobre un campo no exportado (a diferencia de `Interface()` o `Call()`).
//
// ⚠️ SI ALGUNO DE ESTOS `Fatal` SALTA, no es el test el que está roto: alguien renombró un campo de la cadena
// y hay que actualizar el nombre aquí. Se prefiere ese rojo ruidoso a un test que deje de mirar en silencio —
// que es, literalmente, el fallo que este fichero existe para cazar.
func consultorDelGateway(t *testing.T, runner listenRunner) uintptr {
	t.Helper()

	v := reflect.ValueOf(runner)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		t.Fatalf("el factory devolvió un runner inesperado (%T): se esperaba un *app.Listen", runner)
	}
	campoGW := v.Elem().FieldByName("gateway")
	if !campoGW.IsValid() {
		t.Fatal("app.Listen ya no tiene el campo `gateway`: ¿se renombró? Este test mira ahí a propósito, " +
			"porque es donde el factory deja el ListenGateway que lleva el consultor de perfiles")
	}
	gw := campoGW.Elem() // valor dinámico de la interfaz: el *whatsmeow.ListenGateway real
	if !gw.IsValid() || gw.Kind() != reflect.Pointer || gw.IsNil() {
		t.Fatalf("el runner no lleva un gateway utilizable (%v): el factory de producción tiene que haber "+
			"construido uno con NewListenGatewayForDevice", campoGW.Kind())
	}
	campo := gw.Elem().FieldByName("sesionPasiva")
	if !campo.IsValid() {
		t.Fatal("whatsmeow.ListenGateway ya no tiene el campo `sesionPasiva`: ¿se renombró? Es el campo que " +
			"SetSesionPasiva rellena y que listenerOpts() pasa al Listener")
	}
	return campo.Pointer()
}
