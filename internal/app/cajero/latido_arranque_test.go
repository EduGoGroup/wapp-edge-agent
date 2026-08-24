package cajero

// latido_arranque_test.go — LA CONFIG EFECTIVA SE LEE EN EL LATIDO (Plan 044 · Ola 1.7).
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 POR QUÉ ESTE FICHERO EXISTE, y no es documentación: ya costó una ola entera
// ─────────────────────────────────────────────────────────────────────────────
// El `.env` de la máquina PISA al default del código, y eso NO LO PUEDE VER NINGÚN TEST. En la Ola 1.6 el
// techo de inferencia se subió a 45 s en el binario, el VPS de UAT traía
// `WAPP_WORKER_INFERENCE_TIMEOUT_MS=15000`, y el arreglo nunca llegó a producción — con el agravante de
// que ese número ENVENENABA AL BREAKER, porque el umbral de lentitud se deriva de él. Los gates estaban
// verdes todo el tiempo. La única forma de cazarlo es desplegar y LEER EL LATIDO.
//
// De ahí la regla operativa de esta ola: si un valor no se puede confirmar leyendo `cajero: arrancando`,
// el cambio NO ESTÁ TERMINADO. Lo que estos tests sostienen es esa regla — que la línea existe, que lleva
// los números de esta ola, y que lo que publica es EL VALOR EFECTIVO y no lo que alguien pidió.
//
// ⚠️ ES DELIBERADAMENTE DISTINTO del log de `cmd/agent/cajero.go`, que también publica config: aquél
// imprime lo que dice `cfg`, ANTES de que New aplique sus guardarraíles. Cuando los dos difieren, el que
// dice la verdad es éste.

import (
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-intent/ollama"
)

// valorEnLog devuelve el valor asociado a una clave de una línea de log. El bool es «la clave estaba».
func valorEnLog(e entradaLog, clave string) (any, bool) {
	for i := 0; i+1 < len(e.args); i += 2 {
		if k, ok := e.args[i].(string); ok && k == clave {
			return e.args[i+1], true
		}
	}
	return nil, false
}

// arrancarYLeerElLatido corre el cajero lo justo para que emita su línea de arranque y la devuelve.
func arrancarYLeerElLatido(t *testing.T, deps Deps) entradaLog {
	t.Helper()
	log := &logCaptura{}
	deps.Log = log
	if deps.Cola == nil && len(deps.Colas) == 0 {
		deps.Cola = &colaFake{}
	}
	if deps.Ollama == nil {
		deps.Ollama = &chateadorEspia{}
	}
	if _, err := correr(t, deps, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}
	e, ok := log.buscar("cajero: arrancando")
	if !ok {
		t.Fatal("el cajero no emitió `cajero: arrancando`: sin esa línea no hay forma de confirmar la " +
			"config efectiva tras un despliegue")
	}
	return e
}

// TestLatido_PublicaLosTresMandosDeLaOla17 es el criterio de campo: los tres números que esta ola
// introduce se pueden confirmar SIN recompilar y SIN entrar en la máquina a leer el `.env`.
func TestLatido_PublicaLosTresMandosDeLaOla17(t *testing.T) {
	e := arrancarYLeerElLatido(t, Deps{Opciones: opcionesDelEdge()})

	claves := clavesDe(e)
	for _, k := range []string{"keep_alive_s", "prefill_caliente_ms", "prefill_frio_ms",
		"num_predict_default"} {
		if !claves[k] {
			t.Errorf("`cajero: arrancando` no publica %q: ese valor no se puede confirmar tras desplegar, "+
				"y entonces el cambio no está terminado. Claves presentes: %v", k, claves)
		}
	}

	// Y los defaults, para que la línea sirva de referencia de «lo normal»: quien la lea en un VPS sabe
	// contra qué comparar.
	quiero := map[string]any{
		"keep_alive_s":        ollama.DefaultKeepAliveSeconds,
		"prefill_caliente_ms": int64(DefaultPrefillCalienteMS),
		"prefill_frio_ms":     int64(DefaultPrefillFrioMS),
		"num_predict_default": DefaultNumPredict,
	}
	for k, want := range quiero {
		got, _ := valorEnLog(e, k)
		if got != want {
			t.Errorf("%s en el latido: got %v (%T) want %v (%T)", k, got, got, want, want)
		}
	}
}

// TestLatido_ConfirmaUnaRecalibracionSinRecompilar es el bucle completo que el A/B de campo necesita:
// se cambian los tres mandos por configuración, y los tres se CONFIRMAN leyendo una línea de log. Sin esta
// segunda mitad, «configurable» sería una promesa que sólo se puede verificar entrando en la máquina a
// leer el `.env` — y el `.env` dice lo que alguien pidió, no lo que el binario aplicó.
func TestLatido_ConfirmaUnaRecalibracionSinRecompilar(t *testing.T) {
	opts := opcionesDelEdge()
	opts["num_predict"] = 64 // el lado B del A/B del truncado, forzado desde el Edge

	e := arrancarYLeerElLatido(t, Deps{
		Opciones:        opts,
		KeepAlive:       ollama.KeepAliveSeconds(0), // apagar el precalentado: el lado A del A/B
		PrefillCaliente: 4 * time.Second,
		PrefillFrio:     20 * time.Second,
	})

	quiero := map[string]any{
		"keep_alive_s":        0,
		"prefill_caliente_ms": int64(4_000),
		"prefill_frio_ms":     int64(20_000),
		"num_predict_default": 64,
	}
	for k, want := range quiero {
		got, ok := valorEnLog(e, k)
		if !ok {
			t.Errorf("el latido no publica %q", k)
			continue
		}
		if got != want {
			t.Errorf("%s: got %v (%T) want %v (%T) — el mando se puede cambiar pero NO se puede confirmar, "+
				"que para un A/B en la misma tanda es lo mismo que no poder cambiarlo", k, got, got, want, want)
		}
	}
}

// TestLatido_PublicaElValorEFECTIVOYNoElPedido es la mitad que de verdad protege contra el fallo de la Ola
// 1.6.
//
// 🔴 UNA LÍNEA QUE IMPRIMIERA LO QUE SE PIDIÓ NO SERVIRÍA PARA NADA. El caso que hay que poder distinguir
// es precisamente aquel en el que lo pedido y lo aplicado DIFIEREN: aquí se pide una pareja invertida —que
// el cajero rechaza entera— y el latido tiene que publicar los defaults que va a usar de verdad, no los
// 9 s/3 s que alguien escribió en el `.env`. Si publicara lo pedido, un operador leería su propia
// configuración de vuelta y concluiría que está aplicada.
func TestLatido_PublicaElValorEFECTIVOYNoElPedido(t *testing.T) {
	e := arrancarYLeerElLatido(t, Deps{
		Opciones:        opcionesDelEdge(),
		PrefillCaliente: 9 * time.Second, // invertida: el cajero la descarta
		PrefillFrio:     3 * time.Second,
	})

	if got, _ := valorEnLog(e, "prefill_caliente_ms"); got != int64(DefaultPrefillCalienteMS) {
		t.Errorf("prefill_caliente_ms: got %v, want el default %d — el latido publicó lo PEDIDO y no lo "+
			"aplicado, que es lo único que sirve para diagnosticar un `.env` que pisa al binario",
			got, DefaultPrefillCalienteMS)
	}
	if got, _ := valorEnLog(e, "prefill_frio_ms"); got != int64(DefaultPrefillFrioMS) {
		t.Errorf("prefill_frio_ms: got %v, want el default %d", got, DefaultPrefillFrioMS)
	}
}

// TestLatido_SigueSinLlevarNadaDeNegocio (INV-051.1): la línea de arranque publica CONFIGURACIÓN, y la
// configuración no es contenido. Se comprueba en negativo porque el riesgo aparece justo cuando alguien
// añade un campo «para depurar mejor» — y esta línea sale en cada arranque, a un fichero que se rota y se
// recoge en el bundle de diagnóstico.
func TestLatido_SigueSinLlevarNadaDeNegocio(t *testing.T) {
	e := arrancarYLeerElLatido(t, Deps{Opciones: opcionesDelEdge()})

	claves := clavesDe(e)
	for _, prohibida := range []string{"prompt", "salida", "texto", "chat_jid", "self_pn", "session_id"} {
		if claves[prohibida] {
			t.Errorf("`cajero: arrancando` lleva %q: es contenido de negocio y esta línea no puede llevarlo "+
				"(INV-051.1)", prohibida)
		}
	}
}
