package server

// health_perfil_pasivo_test.go — `dropped_passive` EN EL JSON DE GET /v1/health (Plan 046 · Ola 2 · T2.3,
// REQ-11).
//
// 🔑 POR QUÉ LAS ASERCIONES SON SOBRE EL JSON CRUDO Y NO SOBRE `sessionHealthView`. Un struct, por
// definición, ignora todo lo que no conoce y rellena con su cero lo que falta: deserializar en
// `healthResponse` daría `dropped_passive: 0` tanto si el campo salió con un 0 como si NO SALIÓ. Y la mitad
// del contrato de esta tarea es precisamente que el cero SE IMPRIME —sin `omitempty`—, porque un cero es un
// dato («esta sesión no está descartando nada») y un hueco obliga al que lee a preguntarse si el mecanismo
// existe. Sobre el struct, ese contrato es intestable.
//
// 🔗 ESTE FICHERO CUBRE EL ÚLTIMO ESLABÓN. El camino entero del entero es
//
//	countPassiveDrop → health.SessionReporter → Registry → Snapshot → Collector ⇒ Report → ESTE JSON
//
// y no cabe en un solo test porque `control/server` depende (vía `sessionmgr`) de `adapters/whatsmeow`: el
// paquete del listener no puede importar el servidor sin un ciclo. Los otros dos eslabones están en
// `internal/app/health/perfil_pasivo_test.go` (reporter → Report) y en
// `internal/adapters/whatsmeow/listener_perfil_test.go` (el corte → reporter). Aquí se entra por el
// Registry y el Collector REALES —no por un doble— para que el tramo Report→JSON se pruebe contra el mismo
// código que corre en producción.

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
)

// saludConDescartesPasivos arma el Registry y el Collector REALES con `descartes` descartes por perfil
// pasivo anotados en la sesión `id`, tal como los anotaría el listener a través de su SessionReporter, y con
// `mapa` como versión del mapa de perfiles vigente (lo que el daemon cablea con `health.WithFiltersVersion`).
func saludConDescartesPasivos(t *testing.T, id string, descartes int, mapa int64) *health.Collector {
	t.Helper()
	reg := health.NewRegistry()
	rep := reg.For(id)
	// La prueba de vida se sella para TODO entrante, incluidos los descartados (es salud del SOCKET, no
	// señal de negocio): reproducirlo aquí mantiene el escenario fiel al de onMessage.
	rep.MarkInbound(time.Now())
	rep.SetSocketState(health.SocketConnected, "")
	for i := 0; i < descartes; i++ {
		rep.CountPassiveDrop()
	}
	return health.NewCollector(reg, nil, nil, testVersion, time.Now().Add(-time.Minute),
		health.WithFiltersVersion(func() int64 { return mapa }))
}

// sesionCruda devuelve el bloque `sessions["<id>"]` tal cual salió del cable, sin pasar por ningún struct.
func sesionCruda(t *testing.T, body []byte, id string) map[string]json.RawMessage {
	t.Helper()
	var crudo struct {
		Sessions map[string]map[string]json.RawMessage `json:"sessions"`
	}
	if err := json.Unmarshal(body, &crudo); err != nil {
		t.Fatalf("unmarshal %q: %v", string(body), err)
	}
	s, ok := crudo.Sessions[id]
	if !ok {
		t.Fatalf("GET /v1/health salió sin la sesión %q: %s", id, body)
	}
	return s
}

// TestHealth_DroppedPassive_SaleEnElJSON es el criterio (a) de T2.3: tras un entrante descartado por perfil
// pasivo, `GET /v1/health` publica `sessions["sess-1"].dropped_passive == 1`.
//
// 🔴 QUÉ AGUJERO CIERRA, y no es teórico. El corte de la sesión pasiva es SILENCIOSO por diseño: no deja
// fila en `cola_entrantes`, no toca el outbox, no sube nada al cable y ACUSA a WhatsApp exactamente igual
// que si hubiera entregado. Desde fuera, «el filtro está funcionando» y «a ese número no le escribe nadie»
// producen la misma foto. Este campo es la única diferencia entre las dos, y por tanto la única forma de
// ver un filtro roto (cero con tráfico) o un filtro que corta de más (sube en una sesión que debía estar
// activa).
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - borrar `DroppedPassive: hr.DroppedByPassiveProfile` de `handleHealth` ⇒ sale 0;
//   - cambiar el tag json a cualquier otra cosa ⇒ la clave desaparece y el test falla por «falta la clave»
//     (es un CONTRATO con `wapp-ctl` y con el runbook, no un nombre interno);
//   - publicar en su lugar otro contador del Report (un cruce de campos) ⇒ sale un número que no es 1.
func TestHealth_DroppedPassive_SaleEnElJSON(t *testing.T) {
	c := startServerWithHealth(t, fakeLister{}, saludConDescartesPasivos(t, "sess-1", 1, 500))

	resp := do(t, c, http.MethodGet, "/v1/health")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	body := leerCuerpo(t, resp)

	raw, ok := sesionCruda(t, body, "sess-1")["dropped_passive"]
	if !ok {
		t.Fatalf("GET /v1/health no publica `dropped_passive` para una sesión que descartó un entrante.\n"+
			"    CONSECUENCIA: el filtro de la sesión pasiva no deja fila, no sube al cable y acusa igual que "+
			"si hubiera entregado. Sin este campo, un filtro roto es invisible: %s", body)
	}
	var n uint64
	if err := json.Unmarshal(raw, &n); err != nil {
		t.Fatalf("dropped_passive no es un número: %s (%v)", raw, err)
	}
	if n != 1 {
		t.Errorf("dropped_passive = %d, want 1", n)
	}
}

// TestHealth_DroppedPassive_ElCeroSeImprime es el criterio (b), y es el test que FALLA SI ALGUIEN LE PONE
// `omitempty` — que es exactamente el descuido que este test existe para impedir, porque con `omitempty` la
// suite entera seguiría en verde salvo aquí.
//
// La regla, escrita donde se comprueba: un `dropped_passive: 0` DICE «esta sesión no está descartando
// nada»; un campo ausente solo deja la duda de si el mecanismo existe, no se midió o no se dio. Es el mismo
// criterio que `intent_omitted_by_reason` y sus ocho claves, y el contrario que el de los percentiles del
// latido —que sí se omiten con n=0, porque allí el cero se leería como «tardó 0 ms», una afirmación falsa—.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: añadir `,omitempty` al tag de `sessionHealthView.DroppedPassive`.
func TestHealth_DroppedPassive_ElCeroSeImprime(t *testing.T) {
	c := startServerWithHealth(t, fakeLister{}, saludConDescartesPasivos(t, "sess-1", 0, 0))

	resp := do(t, c, http.MethodGet, "/v1/health")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	body := leerCuerpo(t, resp)

	sesion := sesionCruda(t, body, "sess-1")
	raw, ok := sesion["dropped_passive"]
	if !ok {
		t.Fatalf("`dropped_passive` DESAPARECIÓ del JSON por valer cero.\n"+
			"    CONSECUENCIA: un cero es un DATO («esta sesión no descarta nada») y un hueco es una DUDA "+
			"(«¿no descarta, o el mecanismo no existe en este binario?»). Y peor: el día que empiece a "+
			"descartar, la clave aparecerá de la nada y quien tenga un `jq` por claves fijas la verá "+
			"descuadrada. Cuerpo: %s", body)
	}
	if string(raw) != "0" {
		t.Errorf("dropped_passive = %s, want 0", raw)
	}
	// Y el mapa: el cero también se imprime aquí, y significa «este Edge no tiene config de filtros ⇒ nadie
	// es pasiva». Es la lectura que explica el `dropped_passive: 0` de al lado sin tener que adivinar.
	if raw, ok := sesion["filters_version"]; !ok || string(raw) != "0" {
		t.Errorf("`filters_version` con valor cero no se imprimió (ok=%v, raw=%s): un hueco obliga a preguntarse "+
			"si el Edge tiene mapa o si el campo no existe en este binario. Cuerpo: %s", ok, raw, body)
	}
}

// TestHealth_FiltersVersion_SaleEnElJSON es el contrato del OTRO campo del Plan 046 en `/v1/health`. El
// nombre `filters_version` es CONTRATO con `wapp-ctl` y con el runbook, igual que `dropped_passive`.
//
// 🔴 QUÉ AGUJERO CIERRA, y es el peor de esta ola porque es DIFERIDO. `edgeconfig.Store.Put` sobrescribe sin
// condición y los ConfigUpdate los procesan workers en paralelo: un push viejo que gane la carrera de
// escritura deja en disco una versión anterior a la vigente. En memoria no se nota nada; al reinicio
// siguiente el Edge levanta filtrando con el mapa RETRASADO y una sesión que la nube reactivó sigue muda para
// siempre — sin error, sin log y sin métrica. La ÚNICA forma de verlo desde fuera es leer este número y
// compararlo con el que la consola dice haber empujado.
//
// Se comprueba sobre el JSON crudo por el mismo motivo que su vecino: sobre el struct, un campo ausente y un
// cero son indistinguibles, y aquí el cero significa algo muy concreto («no hay mapa: nadie es pasiva»).
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - borrar `FiltersVersion: hr.FiltersVersion` de `handleHealth` ⇒ sale 0, que se lee como «sin mapa»;
//   - borrar `health.WithFiltersVersion(perfiles.Version)` del cableado del daemon ⇒ mismo síntoma en campo
//     (este test no lo ve: lo cubre —hasta donde se puede— el comentario de daemon.go);
//   - cambiar el tag json ⇒ la clave desaparece y `wapp-ctl` deja de encontrarla.
func TestHealth_FiltersVersion_SaleEnElJSON(t *testing.T) {
	c := startServerWithHealth(t, fakeLister{}, saludConDescartesPasivos(t, "sess-1", 3, 1723456789))

	resp := do(t, c, http.MethodGet, "/v1/health")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	body := leerCuerpo(t, resp)

	raw, ok := sesionCruda(t, body, "sess-1")["filters_version"]
	if !ok {
		t.Fatalf("GET /v1/health no publica `filters_version`.\n"+
			"    CONSECUENCIA: se puede ver CUÁNTO corta el filtro (`dropped_passive`) pero no CON QUÉ MAPA, y "+
			"sin eso un Edge que quedó con una versión retrasada tras un reinicio es indistinguible de uno al "+
			"día. Cuerpo: %s", body)
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil {
		t.Fatalf("filters_version no es un número: %s (%v)", raw, err)
	}
	if n != 1723456789 {
		t.Errorf("filters_version = %d, want 1723456789", n)
	}
}
