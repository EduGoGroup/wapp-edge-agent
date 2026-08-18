package health

// collector_parte_test.go — LA REGLA DE RANCIDEZ DEL PARTE DEL WORKER-CAJERO (Plan 051 Ola 4 · T4.3).
//
// QUÉ SE PROTEGE AQUÍ, y por qué es el test más importante de la tarea. Desde la Ola 3 el Edge son DOS
// procesos: el daemon (socket, cola, despachador) y el worker-cajero (Ollama, circuito, taskset). El
// heartbeat lo emite el daemon, pero `intent_circuit`, `worker_taskset` e `intent_p50_ms` los conoce sólo
// el cajero. El parte es el papelito que el cajero deja en la BD de la cola para que el daemon lo lea.
//
// 🔴 UN PAPELITO VIEJO ES PEOR QUE NINGÚN PAPELITO. Si el cajero muere a las 10:00 con el circuito
// `closed`, su parte sigue en disco diciendo `closed` a las 14:00. Publicar eso es mandar a la nube una
// señal de salud INVENTADA: el operador ve un clasificador sano mientras lleva cuatro horas sin clasificar
// nada. Con el campo VACÍO ve la verdad —«este Edge no sabe»— y va a mirar el proceso del cajero.
//
// Por eso la rancidez descarta el parte ENTERO y no campo a campo: los tres valores son de un mismo
// instante de un mismo proceso, y una foto mitad viva mitad muerta no la sabe interpretar nadie.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// parteFijo satisface app.ParteWorkerLector con un parte fijo (o una ausencia, o un error).
type parteFijo struct {
	p   app.ParteWorker
	ok  bool
	err error
}

func (f parteFijo) LeerParte(context.Context) (app.ParteWorker, bool, error) {
	if f.err != nil {
		return app.ParteWorker{}, false, f.err
	}
	return f.p, f.ok, nil
}

var _ app.ParteWorkerLector = parteFijo{}

// regConSesion arma un Registry con UNA sesión con prueba de vida (sin ella Collect devuelve ok=false y
// no se llegaría a mirar el parte).
func regConSesion(id string) *Registry {
	reg := NewRegistry()
	reg.SetSocketState(id, SocketConnected, "")
	return reg
}

// TestCollector_ParteFresco_LlegaLosTres: un parte dentro de la ventana puebla circuito, taskset y p50.
func TestCollector_ParteFresco_LlegaLosTres(t *testing.T) {
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	lector := parteFijo{ok: true, p: app.ParteWorker{
		TS:       now.Add(-time.Second), // recién escrito
		Circuito: "open",
		Taskset:  "disjunta",
		P50ms:    3736,
	}}

	c := NewCollector(regConSesion("S"), nil, lector, "v", now.Add(-time.Minute),
		WithClock(func() time.Time { return now }))

	r, ok := c.Collect(context.Background(), "S")
	if !ok {
		t.Fatal("Collect devolvió ok=false para una sesión con prueba de vida")
	}
	if r.IntentCircuit != "open" {
		t.Errorf("intent_circuit = %q, want open", r.IntentCircuit)
	}
	if r.WorkerTaskset != "disjunta" {
		t.Errorf("worker_taskset = %q, want disjunta", r.WorkerTaskset)
	}
	if r.IntentP50Ms != 3736 {
		t.Errorf("intent_p50_ms = %d, want 3736", r.IntentP50Ms)
	}
}

// TestCollector_ParteRancio_LosTresACero es LA prueba de la tarea: un parte más viejo que app.ParteRancio
// se descarta ENTERO.
//
// 🔴 SE COMPRUEBA EXPLÍCITAMENTE QUE `intent_circuit` NO ES "closed". El parte de abajo dice "closed" —el
// estado sano, el que un cajero deja escrito justo antes de morir— precisamente porque ese es el valor que
// haría el daño: heredarlo pinta de verde un clasificador apagado. Si este test se pone en rojo con un
// "closed", NO se ajusta el umbral: se arregla el descarte.
func TestCollector_ParteRancio_LosTresACero(t *testing.T) {
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	lector := parteFijo{ok: true, p: app.ParteWorker{
		TS:       now.Add(-app.ParteRancio - time.Second), // justo pasado el umbral
		Circuito: "closed",
		Taskset:  "solapada",
		P50ms:    12,
	}}

	c := NewCollector(regConSesion("S"), nil, lector, "v", now.Add(-time.Minute),
		WithClock(func() time.Time { return now }))

	r, _ := c.Collect(context.Background(), "S")
	if r.IntentCircuit != "" {
		t.Errorf("intent_circuit = %q con un parte RANCIO; want \"\" (jamás se hereda el estado de un "+
			"cajero muerto: una salud inventada es peor que la ausencia del dato)", r.IntentCircuit)
	}
	if r.WorkerTaskset != "" {
		t.Errorf("worker_taskset = %q con un parte rancio; want \"\"", r.WorkerTaskset)
	}
	if r.IntentP50Ms != 0 {
		t.Errorf("intent_p50_ms = %d con un parte rancio; want 0", r.IntentP50Ms)
	}
}

// TestCollector_ParteDelFuturo_LosTresACero es el GEMELO del test de rancidez, y cierra el agujero que la
// comparación en un solo sentido dejaba abierto.
//
// 🔴 EL FALLO QUE CAZA: con `now.Sub(p.TS) > ParteRancio` a secas, un TS en el FUTURO da una edad NEGATIVA
// y el parte NUNCA caduca — es fresco para siempre. El escenario real es el del portátil que se suspende:
// el cajero escribe su parte, el reloj salta hacia atrás (NTP al despertar) y el cajero muere; a partir de
// ahí el Edge publicaría en cada latido el `"closed"` de un clasificador apagado, indefinidamente. Es la
// misma salud inventada que el test de rancidez impide, con la peor cara posible: verde permanente.
//
// La migración `0002_parte_worker.sql` ya prometía que un parte «que parece del futuro» degrada a
// `intent_circuit` vacío. Este test es esa promesa, ejecutable.
//
// El circuito vuelve a decir "closed" a propósito, por el mismo motivo que en el de rancidez: es el valor
// que hace daño al heredarse.
func TestCollector_ParteDelFuturo_LosTresACero(t *testing.T) {
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	lector := parteFijo{ok: true, p: app.ParteWorker{
		TS:       now.Add(app.ParteRancio + time.Second), // justo pasado el umbral, pero POR DELANTE
		Circuito: "closed",
		Taskset:  "disjunta",
		P50ms:    7,
	}}

	c := NewCollector(regConSesion("S"), nil, lector, "v", now.Add(-time.Minute),
		WithClock(func() time.Time { return now }))

	r, ok := c.Collect(context.Background(), "S")
	if !ok {
		t.Fatal("un parte del futuro dejó a la sesión SIN Report: la telemetría no puede tumbar el heartbeat")
	}
	if r.IntentCircuit != "" {
		t.Errorf("intent_circuit = %q con un parte DEL FUTURO; want \"\" (un reloj que saltó hacia atrás no "+
			"puede convertir el último parte de un cajero muerto en salud eterna)", r.IntentCircuit)
	}
	if r.WorkerTaskset != "" {
		t.Errorf("worker_taskset = %q con un parte del futuro; want \"\"", r.WorkerTaskset)
	}
	if r.IntentP50Ms != 0 {
		t.Errorf("intent_p50_ms = %d con un parte del futuro; want 0", r.IntentP50Ms)
	}
}

// TestCollector_ParteEnElBordeDelFuturoSigueValiendo es el borde del OTRO lado, y sostiene que la ventana
// simétrica no rompe el caso benigno: los dos procesos leen el reloj de la MISMA máquina, así que el único
// adelanto legítimo es el de la fracción de segundo que se pierde al truncar `ts_unix` a segundos. Un parte
// adelantado EXACTAMENTE ParteRancio sigue valiendo (el descarte es estricto por los dos lados).
func TestCollector_ParteEnElBordeDelFuturoSigueValiendo(t *testing.T) {
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	lector := parteFijo{ok: true, p: app.ParteWorker{
		TS:       now.Add(app.ParteRancio), // adelanto EXACTAMENTE igual al umbral
		Circuito: "open",
	}}

	c := NewCollector(regConSesion("S"), nil, lector, "v", now.Add(-time.Minute),
		WithClock(func() time.Time { return now }))

	if r, _ := c.Collect(context.Background(), "S"); r.IntentCircuit != "open" {
		t.Errorf("intent_circuit = %q para un parte adelantado EXACTAMENTE app.ParteRancio; want open "+
			"(la ventana es simétrica Y estricta por los dos lados)", r.IntentCircuit)
	}
}

// TestCollector_ParteEnElBordeSigueValiendo fija el lado ABIERTO del umbral: rancio es ESTRICTAMENTE mayor
// que app.ParteRancio. Un parte de exactamente esa edad todavía vale.
//
// Existe para que el día que alguien toque el comparador (`>` ↔ `>=`) el cambio sea deliberado y no un
// efecto colateral: con la cadencia del cajero rozando el umbral, ese signo decide si el campo parpadea.
//
// ⚠️ SIGUE SIENDO COHERENTE CON LA VENTANA SIMÉTRICA (2026-08-17): la edad de este parte es exactamente
// +ParteRancio, y el descarte por el lado viejo es `edad > ParteRancio`, así que no lo toca; el lado del
// futuro descarta `edad < -ParteRancio` y tampoco. El parte vale, igual que antes del cambio.
func TestCollector_ParteEnElBordeSigueValiendo(t *testing.T) {
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	lector := parteFijo{ok: true, p: app.ParteWorker{
		TS:       now.Add(-app.ParteRancio), // edad EXACTAMENTE igual al umbral
		Circuito: "half_open",
	}}

	c := NewCollector(regConSesion("S"), nil, lector, "v", now.Add(-time.Minute),
		WithClock(func() time.Time { return now }))

	if r, _ := c.Collect(context.Background(), "S"); r.IntentCircuit != "half_open" {
		t.Errorf("intent_circuit = %q para un parte de edad EXACTAMENTE app.ParteRancio; want half_open "+
			"(el umbral es estricto: rancio es > ParteRancio)", r.IntentCircuit)
	}
}

// TestCollector_ParteConErrorNoTumbaElHeartbeat: la BD de la cola bloqueada por el cajero ⇒ los tres a
// cero, pero el Report SIGUE SALIENDO. La telemetría de un subsistema no puede impedir decir «sigo vivo».
func TestCollector_ParteConErrorNoTumbaElHeartbeat(t *testing.T) {
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	lector := parteFijo{err: errors.New("database is locked")}

	c := NewCollector(regConSesion("S"), nil, lector, "v9", now.Add(-30*time.Second),
		WithClock(func() time.Time { return now }))

	r, ok := c.Collect(context.Background(), "S")
	if !ok {
		t.Fatal("un error leyendo el parte dejó a la sesión SIN Report: el heartbeat no debe depender de él")
	}
	if r.IntentCircuit != "" || r.WorkerTaskset != "" || r.IntentP50Ms != 0 {
		t.Errorf("con error de lectura los tres campos deben quedar a cero; got %q/%q/%d",
			r.IntentCircuit, r.WorkerTaskset, r.IntentP50Ms)
	}
	// Y el resto del Report intacto: es lo que hace que el latido siga siendo útil.
	if r.BinaryVersion != "v9" || r.DaemonUptimeS != 30 || r.SocketState != string(SocketConnected) {
		t.Errorf("el resto del Report se degradó por un error de telemetría: version=%q uptime=%d socket=%q",
			r.BinaryVersion, r.DaemonUptimeS, r.SocketState)
	}
}

// TestCollector_ParteAusenteNoEsError: el cajero aún no escribió nada (ok=false). Es el estado NORMAL de un
// Edge recién arrancado, y se resuelve igual que la rancidez: los tres a cero, sin drama.
func TestCollector_ParteAusenteNoEsError(t *testing.T) {
	now := time.Now()
	c := NewCollector(regConSesion("S"), nil, parteFijo{ok: false}, "v", now)

	r, ok := c.Collect(context.Background(), "S")
	if !ok {
		t.Fatal("la ausencia de parte dejó a la sesión sin Report")
	}
	if r.IntentCircuit != "" || r.WorkerTaskset != "" || r.IntentP50Ms != 0 {
		t.Errorf("sin parte los tres campos deben quedar a cero; got %q/%q/%d",
			r.IntentCircuit, r.WorkerTaskset, r.IntentP50Ms)
	}
}

// TestCollector_SinLectorEsElComportamientoDeHoy: un lector nil sigue siendo LEGAL y no cambia nada.
//
// No es una comprobación defensiva de más: hay tests y wirings que pasan nil, y sobre todo es el camino al
// que cae producción si el adaptador de la cola no respalda el puerto (ver daemon.parteDelWorker). Ese
// camino tiene que seguir dando exactamente el Report de la Ola 3, no un pánico.
func TestCollector_SinLectorEsElComportamientoDeHoy(t *testing.T) {
	c := NewCollector(regConSesion("S"), nil, nil, "v", time.Now())

	r, ok := c.Collect(context.Background(), "S")
	if !ok {
		t.Fatal("con lector nil la sesión debe seguir reportando salud")
	}
	if r.IntentCircuit != "" || r.WorkerTaskset != "" || r.IntentP50Ms != 0 {
		t.Errorf("con lector nil los tres campos deben quedar a su cero; got %q/%q/%d",
			r.IntentCircuit, r.WorkerTaskset, r.IntentP50Ms)
	}
}

// TestCollector_ParteSeNormalizaAlContratoDelWire: "half-open" (la forma que usa el breaker y el endpoint
// /v1/intent/status) viaja como "half_open" (la del contrato SessionHealth, ADR-0023).
func TestCollector_ParteSeNormalizaAlContratoDelWire(t *testing.T) {
	now := time.Now()
	lector := parteFijo{ok: true, p: app.ParteWorker{TS: now, Circuito: "half-open"}}
	c := NewCollector(regConSesion("S"), nil, lector, "v", now, WithClock(func() time.Time { return now }))

	if r, _ := c.Collect(context.Background(), "S"); r.IntentCircuit != "half_open" {
		t.Errorf("intent_circuit = %q, want half_open (guion → guion bajo, ADR-0023)", r.IntentCircuit)
	}
}
