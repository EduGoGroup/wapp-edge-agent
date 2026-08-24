package colaentrantes

// parte_test.go — el tubo cajero→daemon (Plan 051 Ola 4 · T4.5).
//
// Los tres hechos que este canal necesita que sean ciertos, y que no se pueden comprobar leyendo el
// SQL: que lo escrito se lee igual, que la AUSENCIA de parte no es un error, y que reescribir no
// acumula filas. El tercero es el que más duele si falla: con dos filas, el `WHERE id = 1` del lector
// devolvería siempre la primera y el daemon publicaría un circuito congelado en el arranque mientras el
// cajero real cambia de estado — una señal de salud mentirosa, que es justo lo que app.ParteRancio
// existe para no dar.
//
// La BD se abre por el CAMINO DE PRODUCCIÓN (openDB ⇒ infradb.OpenCola + infradb.MigrateCola), como el
// resto del paquete: si `parte_worker` no estuviera en el set de migraciones "cola", estos tests
// fallarían con `no such table`, que es exactamente el aviso que se quiere.

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// contarPartes cuenta las filas de parte_worker. Es la única forma de ver la diferencia entre un UPSERT
// y un INSERT que se acumula: los dos dejan un LeerParte correcto en la lectura siguiente.
func contarPartes(t *testing.T, s *Store) int64 {
	t.Helper()
	var n int64
	if err := s.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM parte_worker`).Scan(&n); err != nil {
		t.Fatalf("contar parte_worker: %v", err)
	}
	return n
}

// TestParte_RoundTrip: lo que el cajero publica es lo que el daemon lee, con los TRES campos. El TS se
// compara por segundos (Unix) porque la columna es epoch-SEGUNDOS: pedirle al roundtrip que conserve
// nanosegundos sería comprobar algo que el esquema no promete.
func TestParte_RoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, openDB(t), newFakeCrypterFor().fn, 100, 0)

	quiero := app.ParteWorker{
		TS:       time.Now(),
		Circuito: "open",
		Taskset:  "disjunta",
		P50ms:    2613, // el p50 que midió la O0 en el VPS real, para que el número no parezca inventado
	}
	if err := s.PublicarParte(ctx, quiero); err != nil {
		t.Fatalf("PublicarParte: %v", err)
	}

	tengo, hay, err := s.LeerParte(ctx)
	if err != nil {
		t.Fatalf("LeerParte: %v", err)
	}
	if !hay {
		t.Fatal("acabamos de publicar un parte: LeerParte debía encontrarlo")
	}
	if tengo.Circuito != quiero.Circuito {
		t.Errorf("circuito: quiero %q, tengo %q", quiero.Circuito, tengo.Circuito)
	}
	if tengo.Taskset != quiero.Taskset {
		t.Errorf("taskset: quiero %q, tengo %q", quiero.Taskset, tengo.Taskset)
	}
	if tengo.P50ms != quiero.P50ms {
		t.Errorf("p50_ms: quiero %d, tengo %d", quiero.P50ms, tengo.P50ms)
	}
	if tengo.TS.Unix() != quiero.TS.Unix() {
		t.Errorf("ts: quiero %d, tengo %d (epoch-segundos)", quiero.TS.Unix(), tengo.TS.Unix())
	}
}

// TestParte_SinPublicar_NoEsError es el contrato que sostiene a media flota: una instalación cuyo
// `agent cajero` no ha arrancado —o que corre con el clasificador deshabilitado y por tanto no publica
// nunca— no tiene parte, y eso NO puede llegarle al daemon como un error. Si esto se rompiera, el
// colector del heartbeat loguearía un fallo por latido en cada instalación sin cajero.
func TestParte_SinPublicar_NoEsError(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, openDB(t), newFakeCrypterFor().fn, 100, 0)

	p, hay, err := s.LeerParte(ctx)
	if err != nil {
		t.Fatalf("la AUSENCIA de parte no es un error: %v", err)
	}
	if hay {
		t.Fatal("nadie ha publicado: hay debía ser false")
	}
	// reflect.DeepEqual y no `!=`: desde T1.7-5 el parte lleva dos mapas dentro, y una struct con mapas no
	// es comparable con `==` (no compila). La promesa que se comprueba es la misma: el CERO de la struct,
	// mapas nil incluidos.
	if !reflect.DeepEqual(p, app.ParteWorker{}) {
		t.Fatalf("sin parte, el valor devuelto es el cero de la struct; es: %+v", p)
	}
}

// TestParte_Upsert_NoAcumulaFilas: publicar N veces deja UNA fila, y es la ÚLTIMA. Las dos mitades
// importan: la cuenta caza el INSERT que se acumula, y la comparación de valores caza el UPSERT que
// inserta bien pero no pisa (un `ON CONFLICT DO NOTHING` mal copiado dejaría una fila con el parte del
// arranque, congelado para siempre).
func TestParte_Upsert_NoAcumulaFilas(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, openDB(t), newFakeCrypterFor().fn, 100, 0)

	base := time.Now()
	for i := 0; i < 5; i++ {
		p := app.ParteWorker{
			TS:       base.Add(time.Duration(i) * time.Minute),
			Circuito: "closed",
			Taskset:  "solapada",
			P50ms:    int64(1000 + i),
		}
		if err := s.PublicarParte(ctx, p); err != nil {
			t.Fatalf("PublicarParte (%d): %v", i, err)
		}
	}

	if n := contarPartes(t, s); n != 1 {
		t.Fatalf("el parte es una fila ÚNICA (CHECK id = 1): hay %d filas", n)
	}

	tengo, hay, err := s.LeerParte(ctx)
	if err != nil || !hay {
		t.Fatalf("LeerParte tras 5 publicaciones: hay=%v err=%v", hay, err)
	}
	if tengo.P50ms != 1004 {
		t.Errorf("el UPSERT debe PISAR: quiero el p50 de la última publicación (1004), tengo %d", tengo.P50ms)
	}
	if tengo.TS.Unix() != base.Add(4*time.Minute).Unix() {
		t.Errorf("el UPSERT debe pisar también el sello: quiero %d, tengo %d",
			base.Add(4*time.Minute).Unix(), tengo.TS.Unix())
	}
}

// TestParte_VaciosSePublicanVacios: un circuito o un taskset desconocido viajan VACÍOS y vuelven
// vacíos. No es un caso de borde: en no-Linux el veredicto del taskset SIEMPRE es "" (taskset_other.go
// no puede leer /proc), y el contrato dice que vacío significa «no se sabe». Que la columna tenga
// `NOT NULL DEFAULT` con la cadena vacía no debe traducirse nunca en un valor por defecto INVENTADO.
// (El literal SQL de dos comillas simples no se escribe aquí a propósito: gofmt lo normaliza a una
// comilla tipográfica dentro de un comentario doc y reescribe el texto.)
func TestParte_VaciosSePublicanVacios(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, openDB(t), newFakeCrypterFor().fn, 100, 0)

	if err := s.PublicarParte(ctx, app.ParteWorker{TS: time.Now(), Circuito: "closed"}); err != nil {
		t.Fatalf("PublicarParte: %v", err)
	}
	tengo, hay, err := s.LeerParte(ctx)
	if err != nil || !hay {
		t.Fatalf("LeerParte: hay=%v err=%v", hay, err)
	}
	if tengo.Taskset != "" {
		t.Errorf("un taskset desconocido viaja VACÍO, no con un default: tengo %q", tengo.Taskset)
	}
	if tengo.P50ms != 0 {
		t.Errorf("sin muestras, el p50 es 0: tengo %d", tengo.P50ms)
	}
}

// TestParte_TSCero_CaeAlRelojDelStore: la defensa en profundidad de PublicarParte. Un TS a cero es el
// epoch, o sea un parte que nace RANCIO (app.ParteRancio) y que dejaría intent_circuit vacío con el
// cajero vivo. Se sustituye por el reloj inyectado en vez de devolver error, porque el parte es
// telemetría y no puede ser la causa de un aviso en el bucle.
func TestParte_TSCero_CaeAlRelojDelStore(t *testing.T) {
	ctx := context.Background()
	reloj := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	s := newStore(t, openDB(t), newFakeCrypterFor().fn, 100, 0, WithClock(func() time.Time { return reloj }))

	if err := s.PublicarParte(ctx, app.ParteWorker{Circuito: "half-open"}); err != nil {
		t.Fatalf("PublicarParte: %v", err)
	}
	tengo, hay, err := s.LeerParte(ctx)
	if err != nil || !hay {
		t.Fatalf("LeerParte: hay=%v err=%v", hay, err)
	}
	if tengo.TS.Unix() != reloj.Unix() {
		t.Errorf("un TS a cero cae al reloj del Store: quiero %d, tengo %d", reloj.Unix(), tengo.TS.Unix())
	}
}

// TestParte_ElRepartoDeLaInferenciaSobreviveElViaje (Plan 044 · Ola 1.7 · T1.7-5): los cuatro enteros y
// los DOS MAPAS cruzan la BD y vuelven iguales.
//
// 🔴 LOS MAPAS SON EL SUJETO. Los cuatro enteros son columnas y no tienen misterio; los repartos viajan
// como JSON en una columna TEXT, y esa decisión es la que permite que una categoría nueva —el día que los
// umbrales del calor de prefijo se recalibren en la máquina de un cliente— aparezca en el heartbeat sin
// migrar esta tabla, sin cortar versión del contrato y sin bumpear dos consumidores. Si la serialización
// se rompiera, el síntoma sería un heartbeat con el reparto SIEMPRE vacío: un silencio, no un error.
func TestParte_ElRepartoDeLaInferenciaSobreviveElViaje(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, openDB(t), newFakeCrypterFor().fn, 100, 0)

	quiero := app.ParteWorker{
		TS:                 time.Unix(1_700_000_000, 0),
		Circuito:           "closed",
		Taskset:            "disjunta",
		P50ms:              8100,
		PrefillP50ms:       1200,
		PrefillMuestras:    44,
		GeneracionP50ms:    6500,
		GeneracionMuestras: 44,
		PorRegimen:         map[string]int64{"frio": 3, "templado": 0, "caliente": 41},
		PorClase:           map[string]int64{"interactivo": 44, "lote": 0},
	}
	if err := s.PublicarParte(ctx, quiero); err != nil {
		t.Fatalf("PublicarParte: %v", err)
	}

	got, hay, err := s.LeerParte(ctx)
	if err != nil || !hay {
		t.Fatalf("LeerParte: hay=%v err=%v", hay, err)
	}
	if !reflect.DeepEqual(got, quiero) {
		t.Errorf("el parte no sobrevivió el viaje:\n got  %+v\n want %+v", got, quiero)
	}
}

// TestParte_SinRepartoLosMapasVuelvenNil: un cajero que no mide el reparto (o un parte escrito por un
// binario anterior a T1.7-5, cuya columna quedó en su DEFAULT ”) deja los mapas en NIL y no en un mapa
// vacío.
//
// 🔴 LA DISTINCIÓN ES LA SEMÁNTICA ENTERA. `nil` significa «este Edge no lo mide»; un mapa PRESENTE con
// una clave a 0 significa «lo mide y no ha visto ninguno». Traduciendo el vacío a `{}` los dos casos
// llegarían al heartbeat como lo mismo, y el consumidor tendría que adivinar cuál está mirando.
func TestParte_SinRepartoLosMapasVuelvenNil(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, openDB(t), newFakeCrypterFor().fn, 100, 0)

	if err := s.PublicarParte(ctx, app.ParteWorker{
		TS:       time.Unix(1_700_000_000, 0),
		Circuito: "closed",
	}); err != nil {
		t.Fatalf("PublicarParte: %v", err)
	}

	got, _, err := s.LeerParte(ctx)
	if err != nil {
		t.Fatalf("LeerParte: %v", err)
	}
	if got.PorRegimen != nil {
		t.Errorf("PorRegimen: got %v want nil («no lo mido» no puede leerse como «lo mido y da 0»)", got.PorRegimen)
	}
	if got.PorClase != nil {
		t.Errorf("PorClase: got %v want nil", got.PorClase)
	}
}
