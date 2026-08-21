package health

// perfil_pasivo_test.go — EL TRAMO CENTRAL DEL CONTADOR DE DESCARTES POR PERFIL PASIVO (Plan 046 · Ola 2 ·
// T2.3, REQ-11).
//
// 🔑 QUÉ SE CUSTODIA AQUÍ Y POR QUÉ EXISTE ESTE FICHERO. El camino completo del entero es
//
//	whatsmeow.bracketObserver.countPassiveDrop  →  health.SessionReporter.CountPassiveDrop
//	    →  Registry (aquí)  →  Snapshot (aquí)  →  Collector.Collect ⇒ Report (aquí)
//	    →  sessionHealthView ⇒ `"dropped_passive"` en GET /v1/health   (adapters/control/server)
//
// y NINGÚN paquete puede probarlo entero: `control/server` depende de `sessionmgr`, que depende de
// `adapters/whatsmeow`, así que un test en el paquete del listener no puede importar el servidor (ciclo).
// La costura por la que se parte es `SessionReporter`, y está probada por SUS DOS LADOS a propósito: aquí
// se fija que lo que entra por el reporter sale en el Report; en `whatsmeow` se fija que el corte lo llama;
// en `control/server` se fija que el Report sale al JSON. Un test por eslabón, sin ningún doble en medio.
//
// 🔴 LA LECCIÓN QUE ESTE FICHERO IMPIDE REPETIR (T1.13 del Plan 051): un contador que nadie lee es medio
// arreglo. `Listener.InboundStats()` tuvo once llamantes durante una ola entera y los once eran tests.

import (
	"context"
	"testing"
	"time"
)

// colectorDePrueba arma un Collector sobre el Registry dado, sin outbox y sin parte del cajero: esta suite
// no va de eso y los dos son tolerantes a nil por contrato.
func colectorDePrueba(reg *Registry, now time.Time) *Collector {
	return NewCollector(reg, nil, nil, "test", now.Add(-time.Minute),
		WithClock(func() time.Time { return now }))
}

// TestRegistry_CountPassiveDrop_LlegaAlSnapshotYAlReport es el eslabón central: lo que el listener anota por
// el reporter tiene que aparecer en el Report que consumen el plano de control y el bundle.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - no copiar `sh.droppedPassive` en `Registry.Snapshot` ⇒ el Snapshot sale a 0 y el Report también;
//   - no copiar `snap.DroppedByPassiveProfile` en `Collector.Collect` ⇒ el Snapshot va bien y el Report no,
//     que es el modo de fallo caro: el contador existe, se incrementa, y aun así nadie lo ve;
//   - hacer que `CountPassiveDrop` asigne en vez de sumar ⇒ sale 1 donde se esperan 3.
func TestRegistry_CountPassiveDrop_LlegaAlSnapshotYAlReport(t *testing.T) {
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	reg := NewRegistry()

	// Se entra por el reporter LIGADO —que es literalmente lo que recibe el listener— y no por el método del
	// Registry: así el test cubre también el `boundReporter`, que es donde se elige el session_id y donde un
	// error de cableado mandaría el contador de una sesión a la fila de otra.
	rep := reg.For("sess-1")
	rep.CountPassiveDrop()
	rep.CountPassiveDrop()
	rep.CountPassiveDrop()

	// Una SEGUNDA sesión que no descarta nada: si los contadores se estuvieran sumando en una sola fila —o
	// si `entry()` estuviera resolviendo mal la clave—, esta saldría con 3.
	reg.For("sess-2").MarkInbound(now)

	snap, ok := reg.Snapshot("sess-1")
	if !ok {
		t.Fatal("la sesión sess-1 no tiene entrada de salud tras contarle descartes: CountPassiveDrop debe " +
			"crear la fila igual que el resto de setters")
	}
	if snap.DroppedByPassiveProfile != 3 {
		t.Errorf("Snapshot.DroppedByPassiveProfile = %d, want 3", snap.DroppedByPassiveProfile)
	}

	c := colectorDePrueba(reg, now)
	r, ok := c.Collect(context.Background(), "sess-1")
	if !ok {
		t.Fatal("Collect devolvió ok=false para una sesión con salud")
	}
	if r.DroppedByPassiveProfile != 3 {
		t.Errorf("Report.DroppedByPassiveProfile = %d, want 3.\n"+
			"    CONSECUENCIA: el contador se incrementa en el listener y muere en el Registry. El filtro de "+
			"la sesión pasiva no deja fila, no sube al cable y ACUSA igual que si hubiera entregado, así que "+
			"sin este número «está filtrando» y «no le escribe nadie» son indistinguibles desde fuera.",
			r.DroppedByPassiveProfile)
	}

	r2, ok := c.Collect(context.Background(), "sess-2")
	if !ok {
		t.Fatal("Collect devolvió ok=false para sess-2")
	}
	if r2.DroppedByPassiveProfile != 0 {
		t.Errorf("Report de sess-2 trae %d descartes pasivos y esa sesión no descartó ninguno: los contadores "+
			"se están mezclando entre sesiones", r2.DroppedByPassiveProfile)
	}
}

// TestRegistry_CountPassiveDrop_NoRompeNadaSinRegistro fija la regla nil-safe que el resto de setters ya
// cumple: un *Registry nil (los cableados y tests que no cablean registro) hace no-op.
//
// No es una formalidad: `Listener.SetHealthReporter` liga ESTE reporter al observador de corchetes, y el
// observador lo invoca en el camino caliente de whatsmeow. Un pánico aquí no sería un contador perdido —
// sería el `recover` de handleEvent negando el acuse y WhatsApp reofreciendo el mensaje en bucle.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO: quitar la guarda `if r == nil` de CountPassiveDrop.
func TestRegistry_CountPassiveDrop_NoRompeNadaSinRegistro(t *testing.T) {
	var reg *Registry
	reg.CountPassiveDrop("sess-1") // no debe entrar en pánico
	reg.For("sess-1").CountPassiveDrop()
}

// TestRegistry_CountPassiveDrop_SeVaConLaSesion documenta —con un test, no con un comentario— que el
// contador NO sobrevive al desvinculado: `Remove` borra la entrada entera.
//
// Está escrito porque es exactamente el tipo de cosa que alguien descubre restando dos lecturas de
// `/v1/health` y concluyendo que el Edge «perdió» descartes. No es una pérdida: es que el contador es de
// una sesión VIVA y de ESTE proceso, y las dos cosas terminan.
func TestRegistry_CountPassiveDrop_SeVaConLaSesion(t *testing.T) {
	reg := NewRegistry()
	reg.For("sess-1").CountPassiveDrop()
	reg.Remove("sess-1")

	if _, ok := reg.Snapshot("sess-1"); ok {
		t.Fatal("la sesión sigue teniendo entrada de salud tras Remove")
	}
	reg.For("sess-1").CountPassiveDrop()
	snap, ok := reg.Snapshot("sess-1")
	if !ok || snap.DroppedByPassiveProfile != 1 {
		t.Errorf("tras un Remove el contador arranca de cero: got ok=%v n=%d, want ok=true n=1",
			ok, snap.DroppedByPassiveProfile)
	}
}

// TestCollector_FiltersVersion_SeLeeFRESCA_EnCadaReport custodia el OTRO campo del Plan 046 en el Report:
// `FiltersVersion`, la versión del mapa de perfiles con la que el Edge está filtrando.
//
// 🔴 QUÉ PREGUNTA CONTESTA, y por qué `dropped_passive` SOLO no basta. El peor fallo de esta ola es el MAPA
// RETRASADO: `edgeconfig.Store.Put` sobrescribe sin condición y los ConfigUpdate los procesan workers en
// paralelo, así que un push viejo puede ganar la carrera de escritura y dejar en DISCO una versión anterior a
// la vigente. En memoria no se nota; al reinicio siguiente el Edge levanta filtrando con el mapa viejo y una
// sesión reactivada sigue muda PARA SIEMPRE, sin error, sin log y sin métrica. Comparar este número con el
// que la consola dice haber empujado es el único diagnóstico posible, y por eso viaja pegado al contador.
//
// 🔴 SE LEE FRESCA Y ESO ES LA MITAD DEL TEST. El mapa cambia EN CALIENTE: si el colector copiara el entero
// al construirse, publicaría la versión del ARRANQUE durante toda la vida del proceso — un número plausible,
// estable y falso, que es la peor clase de dato de observabilidad. Por eso la opción recibe un `func() int64`
// y aquí se mueve el valor entre dos Collect.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - no copiar `FiltersVersion` en `Collector.Collect` ⇒ sale 0 y se lee como «este Edge no tiene mapa»;
//   - guardar `fn()` en vez de `fn` en WithFiltersVersion ⇒ el segundo Collect sigue publicando 7;
//   - quitar la guarda nil de `versionDeFiltros` ⇒ pánico en el camino del HEARTBEAT, que es el peor sitio
//     donde puede reventar la telemetría (lo cubre el subtest de abajo).
func TestCollector_FiltersVersion_SeLeeFrescaEnCadaReport(t *testing.T) {
	now := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	reg := NewRegistry()
	reg.For("sess-1").MarkInbound(now)

	vigente := int64(7)
	c := NewCollector(reg, nil, nil, "test", now.Add(-time.Minute),
		WithClock(func() time.Time { return now }),
		WithFiltersVersion(func() int64 { return vigente }))

	r, ok := c.Collect(context.Background(), "sess-1")
	if !ok {
		t.Fatal("Collect devolvió ok=false para una sesión con salud")
	}
	if r.FiltersVersion != 7 {
		t.Fatalf("Report.FiltersVersion = %d, want 7.\n"+
			"    CONSECUENCIA: `filters_version` sale a 0 en GET /v1/health y en el bundle, y un 0 significa "+
			"«este Edge no tiene mapa de perfiles: nadie es pasiva». Es una mentira plausible, y tapa justo el "+
			"fallo que el campo existe para destapar (un mapa retrasado tras un reinicio).", r.FiltersVersion)
	}

	// La nube empuja una versión nueva: el SIGUIENTE Report tiene que publicarla, sin reiniciar nada.
	vigente = 8
	r2, _ := c.Collect(context.Background(), "sess-1")
	if r2.FiltersVersion != 8 {
		t.Errorf("Report.FiltersVersion = %d tras un push nuevo, want 8: el colector congeló la versión del "+
			"arranque y publicará ese número para siempre", r2.FiltersVersion)
	}
}

// TestCollector_SinFiltersVersion_PublicaCeroYNoRompe fija la degradación: los cableados que no pasan la
// opción (tests, arranques que no vienen de `agent serve`) se comportan como antes de esta línea.
//
// El 0 es AMBIGUO a propósito y hay que saberlo: significa lo mismo que responde la vista de perfiles cuando
// aún no hay config —«no hay mapa, nadie es pasiva»— y es también el valor legítimo de un tenant sin ninguna
// fila de sesión. Se lee con `dropped_passive` al lado, que es lo que distingue los casos.
func TestCollector_SinFiltersVersion_PublicaCeroYNoRompe(t *testing.T) {
	now := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	reg := NewRegistry()
	reg.For("sess-1").MarkInbound(now)

	r, ok := colectorDePrueba(reg, now).Collect(context.Background(), "sess-1")
	if !ok {
		t.Fatal("Collect devolvió ok=false para una sesión con salud")
	}
	if r.FiltersVersion != 0 {
		t.Errorf("Report.FiltersVersion = %d sin lector cableado, want 0", r.FiltersVersion)
	}
}
