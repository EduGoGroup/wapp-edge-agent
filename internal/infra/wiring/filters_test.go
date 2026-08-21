package wiring

// filters_test.go — EL CONSUMO DEL kind:"filters" (Plan 046 · Ola 2 · T2.2).
//
// QUÉ SE CUSTODIA AQUÍ, que es la mitad (a) de la tarea: que el mapa de perfiles que la nube empuja llegue
// a la vista en memoria que el listener consulta, y —sobre todo— QUÉ PASA CUANDO EL PUSH VIENE MAL. Un
// filtro de privacidad que se cae con la primera config corrupta es peor que no tenerlo: deja de filtrar
// sin que nadie se entere.
//
// 🔴 LA GUARDA DE MONOTONICIDAD ES EL MOTIVO DE QUE ESTE FICHERO EXISTA. `edgeconfig.Service` NO ordena
// versiones: su única guarda es una IGUALDAD DE STRINGS (service.go:76). Una versión más VIEJA que llegue
// tarde —una reconexión con un push en vuelo— pasaría esa guarda y se aplicaría, dejando al Edge con un
// mapa retrasado: sesiones que volvieron a ser activas seguirían mudas. D-046.2 y REQ-06 prometen lo
// contrario, así que la guarda vive en el validador y en el suscriptor de ESTE kind, y solo de éste.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo.

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/edgeconfig"
	// El paquete del LISTENER. Se puede importar desde aquí (wiring → sessionmgr → whatsmeow es la dirección
	// de la dependencia) y NO al revés: por eso el criterio (f) se cierra en este fichero y no en el del
	// listener. Ver el comentario del subtest de Bootstrap.
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// --- dobles ---

// almacenDeMentira respalda edgeconfig.Store en memoria. Lleva mutex porque el Service se usa desde varias
// goroutines (los workers del demux corren por session_id en paralelo) y el test de concurrencia lo ejerce
// de verdad; el SQLStore real también es seguro concurrente.
type almacenDeMentira struct {
	mu    sync.Mutex
	filas map[string]edgeconfig.Record
}

func nuevoAlmacen() *almacenDeMentira {
	return &almacenDeMentira{filas: make(map[string]edgeconfig.Record)}
}

func (s *almacenDeMentira) Get(_ context.Context, kind string) (edgeconfig.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.filas[kind]
	return rec, ok, nil
}

func (s *almacenDeMentira) Put(_ context.Context, rec edgeconfig.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.filas[rec.Kind] = rec
	return nil
}

// versionPersistida devuelve la versión de la fila del kind (o "" si no hay): sirve para distinguir el
// last-known-good EN DISCO del que hay en memoria, que no siempre coinciden y conviene poder mirar por
// separado.
func (s *almacenDeMentira) versionPersistida(kind string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.filas[kind].Version
}

// bufSeguro es un io.Writer con candado: el logger se escribe desde varias goroutines en el test de
// concurrencia y un bytes.Buffer pelado sería una carrera del propio test (que `-race` cazaría, con razón).
type bufSeguro struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *bufSeguro) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *bufSeguro) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// bancoDePruebas monta el escenario real: Store + Service + el kind "filters" registrado, tal como lo
// arma el daemon (RegisterJWKS/RegisterFilters sobre el Service compartido).
func bancoDePruebas(t *testing.T) (*Perfiles, *edgeconfig.Service, *almacenDeMentira, *bufSeguro) {
	t.Helper()
	log := &bufSeguro{}
	store := nuevoAlmacen()
	svc := edgeconfig.NewService(store, sharedlogger.New(sharedlogger.WithWriter(log)))
	return RegisterFilters(svc, sharedlogger.New(sharedlogger.WithWriter(log))), svc, store, log
}

// payloadFiltros arma el JSON del contrato D-046.2 A MANO —con sus claves literales— y no reusando el
// struct de producción. Es deliberado: si alguien renombra el campo `profile` o `sessions`, un test que
// serializara el struct de producción seguiría verde y el Edge dejaría de entender a la nube en campo.
func payloadFiltros(t *testing.T, version int64, sesiones map[string]string) []byte {
	t.Helper()
	mapa := make(map[string]any, len(sesiones))
	for id, perfil := range sesiones {
		mapa[id] = map[string]any{"profile": perfil}
	}
	b, err := json.Marshal(map[string]any{"version": version, "sessions": mapa})
	if err != nil {
		t.Fatalf("armar el payload de prueba: %v", err)
	}
	return b
}

// --- tests ---

// TestFiltersConfigKind_EsElLiteralDelContrato: el kind es contrato ENTRE PROCESOS (lo produce el
// cloud-platform, lo consume este Edge) y un desajuste no da error de compilación ni de ejecución — el
// Service ignora los kinds desconocidos con un log Info y un Ack tolerante. El síntoma en campo sería «las
// pasivas siguen recibiendo» sin una sola línea roja.
//
// Se prueba por CONDUCTA y con el literal ESCRITO A MANO —no comparando la constante consigo misma—: lo
// que importa es que una config que llega con el kind que la nube emite de verdad acabe aplicada.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: cambiar el valor de la constante (p. ej. a "filter" o "profiles") ⇒ el
// RegisterKind registra un kind que nadie empuja, este Apply cae en «kind desconocido» y no se aplica nada.
func TestFiltersConfigKind_EsElLiteralDelContrato(t *testing.T) {
	p, svc, _, _ := bancoDePruebas(t)

	payload := payloadFiltros(t, 100, map[string]string{"sess-callada": PerfilPasivo})
	if err := svc.Apply(context.Background(), "filters", "100", payload); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if !p.EsPasiva("sess-callada") {
		t.Fatalf("una config con kind \"filters\" —el literal que emite la nube (D-046.2)— NO se aplicó; la "+
			"constante del Edge vale %q. Un desajuste aquí NO rompe nada visible: el Service ignora los kinds "+
			"desconocidos con Ack tolerante y las sesiones pasivas siguen recibiendo para siempre",
			FiltersConfigKind)
	}
}

// TestRegisterFilters_AplicaElMapaYSeConsultaPorSessionID es el camino feliz completo: un ConfigUpdate del
// kind entra por Service.Apply y sale por EsPasiva, que es lo que el listener consulta en la puerta.
//
// Se afirman los TRES casos del contrato de una vez porque los tres son la misma decisión (D-046.2): la
// pasiva calla, la activa recibe y la que NO ESTÁ EN EL MAPA recibe (fail-open).
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - borrar el `svc.RegisterKind(...)` de RegisterFilters ⇒ el Apply se va por la rama de «kind
//     desconocido» (Ack tolerante) y NADA se aplica: es el fallo mudo del que habla su doc.
//   - invertir el `case` de parseFilters (guardar las activas en vez de las pasivas) ⇒ se invierte el
//     filtro entero: callaría a las sesiones que sí tienen que recibir.
func TestRegisterFilters_AplicaElMapaYSeConsultaPorSessionID(t *testing.T) {
	p, svc, _, _ := bancoDePruebas(t)

	payload := payloadFiltros(t, 100, map[string]string{
		"sess-callada": PerfilPasivo,
		"sess-viva":    PerfilActivo,
	})
	if err := svc.Apply(context.Background(), FiltersConfigKind, "100", payload); err != nil {
		t.Fatalf("Apply devolvió error: %v", err)
	}

	if !p.EsPasiva("sess-callada") {
		t.Error("la sesión marcada `passive` no se ve como pasiva: el corte del listener no cortaría nada y " +
			"REQ-07 quedaría incumplido")
	}
	if p.EsPasiva("sess-viva") {
		t.Error("la sesión marcada `active` se ve como PASIVA: el Edge dejaría SORDA a una sesión que sí tiene " +
			"que recibir, y el cliente escribiría sin que pasara nada")
	}
	if p.EsPasiva("sess-que-no-esta-en-el-mapa") {
		t.Error("una sesión AUSENTE del mapa se ve como pasiva: D-046.2 dice lo contrario (fail-open). Un Edge " +
			"jamás puede perder tráfico por una config incompleta")
	}
	if got := p.Version(); got != 100 {
		t.Errorf("Version() = %d, se esperaba 100", got)
	}
}

// TestRegisterFilters_PayloadCorrupto_ConservaElLKG es la primera mitad del criterio (e). Lo que se
// custodia no es «no se aplica basura», es que EL MAPA ANTERIOR SIGUE VIGENTE: un filtro de privacidad que
// se apaga con el primer push corrupto deja de filtrar sin que nadie lo note.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - registrar el kind SIN validador (`svc.RegisterKind(FiltersConfigKind, nil, p.aplicar)`) ⇒ la basura
//     se persiste en disco y el last-known-good se pierde para el siguiente arranque.
//   - que `aplicar` publique una foto vacía cuando el parseo falla, en vez de retornar ⇒ el mapa anterior
//     desaparece y todas las pasivas vuelven a recibir.
func TestRegisterFilters_PayloadCorrupto_ConservaElLKG(t *testing.T) {
	p, svc, store, log := bancoDePruebas(t)
	ctx := context.Background()

	bueno := payloadFiltros(t, 100, map[string]string{"sess-callada": PerfilPasivo})
	if err := svc.Apply(ctx, FiltersConfigKind, "100", bueno); err != nil {
		t.Fatalf("Apply del bueno: %v", err)
	}

	casos := []struct {
		nombre  string
		version string
		payload []byte
	}{
		{"JSON truncado", "101", []byte(`{"version": 101, "sessions":`)},
		{"no es un objeto", "102", []byte(`"passive"`)},
		// El campo `version` AUSENTE deserializa a 0, y un 0 con sesiones dentro solo puede venir de un
		// emisor roto: aplicarlo dejaría al Edge con un mapa cuya versión no ordena nada.
		{"con sesiones y sin version", "103", []byte(`{"sessions":{"sess-viva":{"profile":"active"}}}`)},
		{"version negativa", "104", []byte(`{"version": -7, "sessions":{}}`)},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			if err := svc.Apply(ctx, FiltersConfigKind, c.version, c.payload); err != nil {
				t.Fatalf("Apply de un blob inválido NO debe devolver error (no es reintentable): %v", err)
			}
			if !p.EsPasiva("sess-callada") || p.Version() != 100 {
				t.Fatalf("tras el push corrupto el mapa vigente cambió (version=%d, pasiva=%v).\n"+
					"    CONSECUENCIA: la sesión que la nube mandó callar vuelve a recibir, persistir y entregar\n"+
					"    contenido en el Edge — y el motivo es un payload mal formado que nadie va a mirar.",
					p.Version(), p.EsPasiva("sess-callada"))
			}
			if got := store.versionPersistida(FiltersConfigKind); got != "100" {
				t.Errorf("la fila persistida quedó en la versión %q: el blob inválido llegó a DISCO y el "+
					"last-known-good del próximo arranque es basura", got)
			}
		})
	}
	if !strings.Contains(log.String(), "ERROR") {
		t.Error("no se registró ningún ERROR ante un push corrupto: un descarte silencioso de config es " +
			"indistinguible de que la nube no haya empujado nunca")
	}
}

// TestRegisterFilters_VersionAnteriorOIgual_SeDescarta es la segunda mitad del criterio (e) y LA TRAMPA que
// el mecanismo compartido no cubre.
//
// LOS DOS CASOS SON DISTINTOS Y POR ESO SON DOS:
//   - ANTERIOR: un push viejo que llega tarde (reconexión con otro en vuelo). La guarda de `Service.Apply`
//     —igualdad de strings— lo deja pasar tan campante; lo para el validador de este kind.
//   - IGUAL CON OTRO FORMATO DE FRAME: el mismo entero escrito distinto ("0100" vs "100"). Ni siquiera
//     empata en la comparación de strings del Service, así que también llega al validador.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: relajar el `<=` a `<` en `validar`/`aplicar`, o quitar la guarda entera
// y fiarse de `Service.Apply` ⇒ el Edge se queda con el mapa del push más TARDÍO en llegar, no con el más
// NUEVO, y eso no se ve en ningún log: sesiones reactivadas que siguen mudas.
func TestRegisterFilters_VersionAnteriorOIgual_SeDescarta(t *testing.T) {
	casos := []struct {
		nombre     string
		frame      string
		versionPay int64
	}{
		{"anterior", "99", 99},
		{"igual, con el frame escrito de otra forma", "0100", 100},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			p, svc, store, log := bancoDePruebas(t)
			ctx := context.Background()

			vigente := payloadFiltros(t, 100, map[string]string{"sess-callada": PerfilPasivo})
			if err := svc.Apply(ctx, FiltersConfigKind, "100", vigente); err != nil {
				t.Fatalf("Apply del vigente: %v", err)
			}

			// El push retrasado dice justo lo contrario: que la sesión callada está viva.
			viejo := payloadFiltros(t, c.versionPay, map[string]string{"sess-callada": PerfilActivo})
			if err := svc.Apply(ctx, FiltersConfigKind, c.frame, viejo); err != nil {
				t.Fatalf("Apply del retrasado NO debe devolver error: %v", err)
			}

			if !p.EsPasiva("sess-callada") || p.Version() != 100 {
				t.Fatalf("se aplicó una versión ANTERIOR O IGUAL a la vigente (version=%d, pasiva=%v).\n"+
					"    CONSECUENCIA: el Edge se queda con el mapa del push que llegó más TARDE en vez de con el\n"+
					"    más NUEVO. D-046.2 y REQ-06 prometen `version` monotónica; `Service.Apply` no la\n"+
					"    implementa (su guarda es una igualdad de strings, service.go:76) y por eso vive aquí.",
					p.Version(), p.EsPasiva("sess-callada"))
			}
			if got := store.versionPersistida(FiltersConfigKind); got != "100" {
				t.Errorf("la fila persistida pasó a %q: la versión vieja llegó a disco y el próximo arranque "+
					"levantaría con ella", got)
			}
			if !strings.Contains(log.String(), "ERROR") {
				t.Error("no se registró ningún ERROR al descartar una versión vieja: el descarte tiene que ser " +
					"visible, si no es indistinguible de que el push no haya llegado")
			}
		})
	}
}

// TestRegisterFilters_ElVersionDelFrameEsMETADATO_YNoGateaNada fija la REGLA DE AUTORIDAD del contrato
// (decisión del 2026-08-21): la versión que manda es la del **payload**; la del **frame** es metadato del
// sobre y lo único que puede provocar es un WARN.
//
// 🔴 POR QUÉ SE CAMBIÓ, porque la regla anterior («si el frame no es un entero, no apliques») producía un
// FAIL-OPEN DIFERIDO Y TOTAL, que es el peor fallo que este plan puede tener. La cadena era ésta: el
// validador daba el payload por bueno ⇒ `Service.Apply` PERSISTÍA la fila ⇒ el suscriptor la rechazaba por
// el frame ⇒ la memoria conservaba el mapa bueno y en DISCO quedaba la fila mala. Nadie notaba nada… hasta el
// reinicio siguiente: `Bootstrap` leía esa fila, `aplicar` la volvía a rechazar por lo mismo, `vigente` se
// quedaba en NIL y —fail-open de D-046.2— TODAS las sesiones volvían a ser activas. Sin un solo error nuevo.
//
// Con la regla de hoy la guarda entera vive en el VALIDADOR, que corre ANTES del `Put`: lo que no convence
// no llega al disco, y el last-known-good se conserva en las dos memorias a la vez.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - volver a rechazar en `aplicar` cuando el frame no parsea (o cuando discrepa) ⇒ la config NUEVA no se
//     aplica y `Version()` se queda en 100: el escenario descrito arriba, otra vez;
//   - degradar el aviso a nada (borrar `avisarSiElFrameNoCuadra`) ⇒ un emisor roto en la nube deja de
//     delatarse y la única pista de que el contrato se rompió desaparece;
//   - subir ese aviso a ERROR ⇒ no rompe el filtro, pero convierte un bug del emisor en una alarma de campo
//     en cada push; el test lo fija en WARN a propósito.
func TestRegisterFilters_ElVersionDelFrameEsMetadato_YNoGateaNada(t *testing.T) {
	// `aviso` es un trozo LITERAL del mensaje que debe salir. Se busca por el texto y no por el nivel
	// ("WARN") porque el formato del nivel lo decide el logger compartido y no es contrato de este test; lo
	// que sí es contrato es que el emisor roto quede señalado y que se distinga QUÉ le pasa al frame.
	casos := []struct {
		nombre string
		frame  string
		aviso  string
	}{
		{"frame no numérico", "v2-final", "es METADATO"},
		{"frame que discrepa del payload", "999", "NO coinciden"},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			p, svc, store, log := bancoDePruebas(t)
			ctx := context.Background()

			vigente := payloadFiltros(t, 100, map[string]string{"sess-callada": PerfilPasivo})
			if err := svc.Apply(ctx, FiltersConfigKind, "100", vigente); err != nil {
				t.Fatalf("Apply del vigente: %v", err)
			}

			// La config NUEVA (payload 200) reactiva la sesión. Es POSTERIOR, así que tiene que aplicarse
			// pase lo que pase con el sobre.
			nuevo := payloadFiltros(t, 200, map[string]string{"sess-callada": PerfilActivo})
			if err := svc.Apply(ctx, FiltersConfigKind, c.frame, nuevo); err != nil {
				t.Fatalf("Apply con un frame raro NO debe devolver error: %v", err)
			}

			if p.EsPasiva("sess-callada") || p.Version() != 200 {
				t.Fatalf("la config NUEVA no se aplicó por culpa del `version` del FRAME (version=%d, pasiva=%v).\n"+
					"    CONSECUENCIA: la sesión que la nube acaba de REACTIVAR sigue muda. Y peor: la fila mala ya\n"+
					"    está en disco, así que el próximo arranque la rechazará igual y `vigente` quedará NIL —\n"+
					"    fail-open total, todas las sesiones activas, sin un solo error en el log.",
					p.Version(), p.EsPasiva("sess-callada"))
			}
			if got := store.versionPersistida(FiltersConfigKind); got != c.frame {
				t.Errorf("la fila persistida quedó en %q y el frame decía %q: el Service guarda el frame tal cual "+
					"(es su identidad en disco); lo que no hace es dejar que gatee la aplicación", got, c.frame)
			}
			out := log.String()
			if !strings.Contains(out, c.aviso) {
				t.Errorf("un frame que no cuadra con el payload pasó SIN el aviso %q: es la única pista de que "+
					"el emisor de la nube rompió el contrato D-046.2.\n    LOG: %s", c.aviso, out)
			}
			if strings.Contains(out, "ERROR") {
				t.Errorf("el frame raro se registró como ERROR: es metadato y no gatea nada, así que una alarma "+
					"de campo aquí es ruido en cada push.\n    LOG: %s", out)
			}
		})
	}
}

// TestRegisterFilters_Bootstrap_RepueblaSinPush es el criterio (f): tras un reinicio del Edge, el corte
// tiene que seguir funcionando SIN esperar un push nuevo. Lo que lo consigue es `Service.Bootstrap`, que
// notifica a los suscriptores de cada kind con la fila persistida.
//
// El segundo subtest fija el ORDEN del cableado del daemon, que es donde esto se puede romper de verdad:
// Bootstrap solo recorre los kinds YA REGISTRADOS.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - registrar el kind sin suscriptor (`svc.RegisterKind(FiltersConfigKind, p.validar)`) ⇒ el push en
//     caliente seguiría funcionando y el ARRANQUE no: el Edge levantaría con todas las sesiones activas
//     hasta el siguiente push, y en esa ventana una pasiva sube todo lo que le llegue.
//   - mover `wiring.RegisterFilters(...)` por DEBAJO de `edgeCfgSvc.Bootstrap(ctx)` en daemon.go ⇒ el
//     segundo subtest describe exactamente lo que pasaría.
func TestRegisterFilters_Bootstrap_RepueblaSinPush(t *testing.T) {
	ctx := context.Background()
	payload := payloadFiltros(t, 500, map[string]string{"sess-callada": PerfilPasivo})

	t.Run("registrar y DESPUES arrancar: el mapa vuelve del disco", func(t *testing.T) {
		store := nuevoAlmacen()
		// La fila que dejó el proceso anterior antes de morir.
		if err := store.Put(ctx, edgeconfig.Record{Kind: FiltersConfigKind, Version: "500", Payload: payload}); err != nil {
			t.Fatalf("sembrar la fila persistida: %v", err)
		}
		svc := edgeconfig.NewService(store, sharedlogger.New(sharedlogger.WithWriter(&bufSeguro{})))
		p := RegisterFilters(svc, sharedlogger.New(sharedlogger.WithWriter(&bufSeguro{})))

		svc.Bootstrap(ctx)

		if !p.EsPasiva("sess-callada") || p.Version() != 500 {
			t.Fatalf("tras el reinicio el mapa NO se repobló (version=%d, pasiva=%v).\n"+
				"    CONSECUENCIA: cada reinicio del Edge —un despliegue, un reboot de la máquina del cliente—\n"+
				"    abre una ventana en la que TODAS las sesiones pasivas vuelven a encolar y entregar su\n"+
				"    tráfico entrante, hasta que la nube empuje otra vez.",
				p.Version(), p.EsPasiva("sess-callada"))
		}

		// ── Y EL LISTENER, que es lo que faltaba ──
		//
		// El mapa repoblado no compra nada por sí solo: lo que REQ-07 promete es que el CORTE siga vivo tras
		// el reinicio, y el corte lo hace el Listener. Se construye el REAL —el mismo `whatsmeow.NewListener`
		// que corre en campo— con el predicado que acaba de sobrevivir al Bootstrap y con su session_id, que
		// son las DOS condiciones de `Listener.sesionEsPasiva()`: sin cualquiera de ellas el filtro responde
		// «no pasiva» y se apaga en silencio.
		//
		// ⚠️ POR QUÉ NO SE LE ENTREGA UN MENSAJE AQUÍ, dicho para que nadie lo tome por pereza: `handleEvent`
		// no está exportado y este paquete no puede vivir en `whatsmeow` —`wiring` importa `sessionmgr`, que
		// importa `whatsmeow`, así que un test del listener que importara `wiring` sería un ciclo—. Es el
		// mismo corte que obligó a partir la custodia de `dropped_passive` en tres ficheros, y se resuelve
		// igual: un test por eslabón, sin ningún doble en medio. El tramo «predicado + session_id ⇒ no hay
		// fila» lo cierra `whatsmeow/listener_perfil_test.go`; lo que se fija AQUÍ es que el predicado que el
		// Listener sostiene tras un reinicio es EXACTAMENTE el de esta vista repoblada, y no otro.
		l := whatsmeow.NewListener(
			sharedlogger.New(sharedlogger.WithWriter(&bufSeguro{})),
			whatsmeow.WithSessionID("sess-callada"),
			whatsmeow.WithSesionPasiva(p.PasivaFunc()),
		)

		enListener := reflect.ValueOf(l).Elem().FieldByName("sesionPasiva")
		if !enListener.IsValid() {
			t.Fatal("whatsmeow.Listener ya no tiene el campo `sesionPasiva`: ¿se renombró? Este test mira ahí a " +
				"propósito, porque es donde WithSesionPasiva deja el consultor que la puerta interroga")
		}
		if enListener.IsNil() {
			t.Fatal("tras el reinicio el Listener nació SIN consultor de perfiles.\n" +
				"    CONSECUENCIA: el mapa vuelve del disco perfectamente y no lo mira nadie. Todas las sesiones\n" +
				"    pasivas del Edge encolan, persisten y entregan su tráfico entrante desde el primer mensaje\n" +
				"    posterior al arranque, con los cuatro gates en VERDE.")
		}
		if quiero := reflect.ValueOf(p.PasivaFunc()).Pointer(); enListener.Pointer() != quiero {
			t.Errorf("el Listener consulta un predicado DISTINTO del de la vista repoblada (%#x != %#x): el mapa\n"+
				"    que vuelve del disco y el que se pregunta en la puerta no son el mismo objeto",
				enListener.Pointer(), quiero)
		}
		enSesion := reflect.ValueOf(l).Elem().FieldByName("sessionID")
		if !enSesion.IsValid() || enSesion.String() != "sess-callada" {
			t.Errorf("el Listener nació sin su session_id (%v): `sesionEsPasiva()` corta en seco con el id "+
				"vacío, así que el predicado estaría cableado y el filtro apagado igualmente", enSesion)
		}
	})

	t.Run("arrancar y DESPUES registrar: el mapa se queda vacío (el orden del daemon importa)", func(t *testing.T) {
		store := nuevoAlmacen()
		if err := store.Put(ctx, edgeconfig.Record{Kind: FiltersConfigKind, Version: "500", Payload: payload}); err != nil {
			t.Fatalf("sembrar la fila persistida: %v", err)
		}
		svc := edgeconfig.NewService(store, sharedlogger.New(sharedlogger.WithWriter(&bufSeguro{})))

		svc.Bootstrap(ctx) // ← el orden equivocado: aún no hay kind registrado
		p := RegisterFilters(svc, sharedlogger.New(sharedlogger.WithWriter(&bufSeguro{})))

		if p.EsPasiva("sess-callada") {
			t.Fatal("este subtest describe el orden EQUIVOCADO y debería salir vacío: si empieza a pasar, es " +
				"que Bootstrap cambió de semántica y el comentario de daemon.go hay que reescribirlo")
		}
	})
}

// TestRegisterFilters_TenantSinNingunaFila_SeAplicaIgual cubre el frame que el emisor documenta como
// legítimo y que un guardián demasiado estricto rechazaría: `version` = 0 con el mapa VACÍO significa «este
// tenant no tiene ni una fila de sesión», y T2.1 lo empuja igual (su regla 2). Es justo lo que puede llegar
// al CONECTAR un Edge cuya sesión aún no está registrada en la flota.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: volver a exigir `version > 0` en parseFilters ⇒ un ERROR en el log en
// cada arranque de ese Edge, a cambio de nada (un mapa vacío no calla a nadie).
func TestRegisterFilters_TenantSinNingunaFila_SeAplicaIgual(t *testing.T) {
	p, svc, _, log := bancoDePruebas(t)

	if err := svc.Apply(context.Background(), FiltersConfigKind, "0", []byte(`{"version":0,"sessions":{}}`)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if strings.Contains(log.String(), "ERROR") {
		t.Error("el frame legítimo de un tenant sin filas se registró como ERROR: T2.1 lo empuja a propósito")
	}
	if p.EsPasiva("sess-1") {
		t.Error("con el mapa vacío nadie puede ser pasiva")
	}
}

// TestPerfiles_PerfilDesconocido_SeTrataComoActiva: si la nube introduce un tercer perfil, un Edge viejo no
// puede enmudecer a esa sesión «por si acaso» (D-046.2, fail-open). Se cuenta y se avisa, pero se deja pasar.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: tratar el `default` de parseFilters como pasivo, o devolver error ⇒ un
// valor nuevo en la nube dejaría sordos a todos los Edge que aún no lo conocen, y el despliegue de una
// función nueva se convertiría en una caída de recepción.
func TestPerfiles_PerfilDesconocido_SeTrataComoActiva(t *testing.T) {
	p, svc, _, log := bancoDePruebas(t)

	payload := payloadFiltros(t, 100, map[string]string{
		"sess-rara":    "shadow-mode-v3",
		"sess-callada": PerfilPasivo,
	})
	if err := svc.Apply(context.Background(), FiltersConfigKind, "100", payload); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if p.EsPasiva("sess-rara") {
		t.Error("un `profile` DESCONOCIDO se trató como pasivo: un Edge que no entiende un valor nuevo no " +
			"puede dejar de recibir por eso")
	}
	if !p.EsPasiva("sess-callada") {
		t.Error("el valor desconocido de una sesión contaminó al resto del mapa")
	}
	if !strings.Contains(log.String(), "DESCONOCIDO") {
		t.Error("el perfil desconocido no dejó aviso: es la única señal de que la nube empezó a hablar un " +
			"idioma que este Edge no entiende")
	}
}

// TestPerfiles_SinConfig_NadieEsPasiva cierra el fail-open por sus dos extremos: la vista recién creada
// (aún sin push ni fila persistida) y el receptor nil, que es lo que devuelve el cableado cuando no hay
// Service. En los dos casos el Edge se comporta como antes del Plan 046.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: quitar la guarda `if v == nil` de EsPasiva ⇒ pánico en el hilo de
// whatsmeow (que el recover de handleEvent convierte en «no se acusa»: WhatsApp reenviaría en bucle), o
// invertir el default ⇒ el Edge arranca SORDO y solo se cura cuando la nube empuja.
func TestPerfiles_SinConfig_NadieEsPasiva(t *testing.T) {
	p, _, _, _ := bancoDePruebas(t)
	if p.EsPasiva("sess-1") {
		t.Error("sin config aplicada alguien salió pasiva: sin `filters` el Edge se comporta como hoy (D-046.2)")
	}
	if p.Version() != 0 {
		t.Errorf("Version() = %d sin config aplicada, se esperaba 0", p.Version())
	}
	if fn := p.PasivaFunc(); fn == nil || fn("sess-1") {
		t.Error("PasivaFunc() de una vista sin config debe existir y responder `no pasiva`")
	}

	var nula *Perfiles
	if nula.EsPasiva("sess-1") {
		t.Error("una vista NIL debe responder `no pasiva` (nil-safe), no filtrar")
	}
	if nula.PasivaFunc() != nil {
		t.Error("PasivaFunc() de una vista nil debe devolver nil, para que el Listener caiga a su default seguro")
	}
	if nula.Version() != 0 {
		t.Error("Version() de una vista nil debe ser 0")
	}
}

// TestPerfiles_ConcurrenciaDeVerdad es el criterio (g) escrito como escenario, no como bandera de `go test`.
//
// 🔴 LA CONCURRENCIA AQUÍ NO ES HIPOTÉTICA. `Server.PushConfig` fanea el MISMO frame a TODAS las sesiones
// vivas del tenant (config_push.go:65-81) y el demux del Edge procesa por session_id EN PARALELO, así que
// varios workers entran a `Apply` a la vez —a veces con versiones distintas— mientras N hilos de whatsmeow
// leen el mapa en la puerta.
//
// Se afirman TRES cosas que `-race` por sí solo no vería:
//   - que las lecturas concurrentes no ven un mapa a medio construir (la foto es inmutable y se publica
//     con un atómico);
//   - que EN MEMORIA gana la versión MÁS ALTA, no la última en escribir. Sin el candado del check-and-swap,
//     un `Load`+`Store` de la versión vieja puede pisar a la nueva: el Edge se queda retrasado y NADA falla;
//   - que EN DISCO gana también la más alta. Ésta es la que faltaba, y era la que importaba.
//
// 🔴 POR QUÉ LA ASERCIÓN SOBRE EL DISCO ES LA IMPORTANTE (y por qué su ausencia dejaba pasar el peor fallo de
// esta ola). La guarda de monotonicidad del kind protege LA MEMORIA: `p.mu` ordena los swaps del
// `atomic.Pointer`. Pero el `Put` ocurre FUERA de ella, dentro de `Service.Apply`, y `Store.Put` sobrescribe
// SIN CONDICIÓN. Con `Apply` sin serializar, un frame VIEJO podía validar contra la foto de antes y escribir
// su fila DESPUÉS del nuevo: memoria con la versión buena, disco con la vieja, y ni un error.
//
// El síntoma llega al REINICIO siguiente y es total: `Bootstrap` lee el disco, la memoria arranca a nil, la
// guarda no tiene contra qué disparar y el Edge levanta con el mapa retrasado. Una sesión reactivada sigue
// MUDA PARA SIEMPRE, sin un solo log de error. Este test, mirando solo la memoria, salía verde.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - quitar `p.mu` de `aplicar` (dejando solo el atómico) ⇒ con `-race` no salta nada —los accesos siguen
//     siendo atómicos— y la aserción de MEMORIA cae de forma intermitente. Es el motivo exacto de que el
//     candado exista teniendo ya el atomic.Pointer;
//   - quitar `s.aplicaMu` de `edgeconfig.Service.Apply` ⇒ cae la aserción del DISCO, también de forma
//     intermitente, y con `-race` limpio.
func TestPerfiles_ConcurrenciaDeVerdad(t *testing.T) {
	p, svc, store, _ := bancoDePruebas(t)
	ctx := context.Background()

	const olas = 24
	var wg sync.WaitGroup

	// Los payloads se arman ANTES de lanzar nada: `payloadFiltros` usa t.Fatalf, y eso solo puede llamarse
	// desde la goroutine del test.
	payloads := make([][]byte, olas+1)
	for v := 1; v <= olas; v++ {
		payloads[v] = payloadFiltros(t, int64(v), map[string]string{"sess-callada": PerfilPasivo})
	}

	// Escritores: versiones 1..olas empujadas a la vez y en desorden (cada goroutine arranca cuando el
	// planificador quiera). La sesión callada lo está en TODAS: lo que varía es la versión.
	for i := 1; i <= olas; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			_ = svc.Apply(ctx, FiltersConfigKind, strconv.Itoa(v), payloads[v])
		}(i)
	}
	// Lectores: el hilo de whatsmeow de cada sesión, preguntando en la puerta.
	for i := 0; i < olas; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = p.EsPasiva("sess-callada")
				_ = p.Version()
			}
		}()
	}
	wg.Wait()

	if got := p.Version(); got != olas {
		t.Errorf("Version() = %d tras %d pushes concurrentes, se esperaba la MÁS ALTA (%d).\n"+
			"    CONSECUENCIA: el Edge se queda con un mapa retrasado y no hay forma de saberlo — ni error, ni\n"+
			"    log, ni métrica. Es lo que pasa cuando el check-and-swap no está serializado.", got, olas, olas)
	}
	if !p.EsPasiva("sess-callada") {
		t.Error("tras la tormenta de pushes la sesión callada dejó de verse como pasiva")
	}
	// EL DISCO, que es lo que sobrevive al proceso. `versionPersistida` devuelve el `version` del FRAME, y
	// aquí frame y payload son el mismo entero por construcción, así que se compara contra `olas`.
	if got := store.versionPersistida(FiltersConfigKind); got != strconv.Itoa(olas) {
		t.Errorf("la fila persistida quedó en la versión %q tras %d pushes concurrentes, se esperaba la MÁS "+
			"ALTA (%d).\n"+
			"    CONSECUENCIA: hoy no se nota nada —la memoria tiene el mapa bueno—, y por eso este fallo es tan\n"+
			"    caro: aparece en el REINICIO siguiente. Bootstrap lee esta fila, la memoria arranca vacía, la\n"+
			"    guarda de monotonicidad no tiene contra qué disparar y el Edge levanta filtrando con un mapa\n"+
			"    RETRASADO. Una sesión que la nube reactivó sigue muda para siempre, sin un solo log de error.\n"+
			"    Se arregla serializando `Service.Apply` (validar→persistir→notificar en una sola sección\n"+
			"    crítica), no tocando la guarda de este kind: el `Put` ocurre fuera de ella.",
			got, olas, olas)
	}
}
