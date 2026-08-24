package cajero

// presupuesto_test.go — LO QUE EL CLOUD PUEDE ACOTAR Y LO QUE EL EDGE MANDA SIEMPRE
// (Plan 044 · Ola 1.7 · T1.7-3 `max_output_tokens` y T1.7-4 `keep_alive`).
//
// Los dos son perillas de la petición a Ollama, y por eso comparten fichero, pero su DUEÑO es opuesto y
// esa es la mitad interesante:
//
//   - `num_predict` lo fija el CLOUD por petición (conoce el esquema de la respuesta que espera) y el
//     Edge sólo pone el suelo cuando el Cloud calla.
//   - `keep_alive` lo fija el EDGE en todas (es una propiedad de la máquina del cliente: cuánta RAM
//     puede tener ocupada), y el Cloud no tiene voz.
//
// 🔴 LO QUE ESTOS TESTS MIRAN ES LA PETICIÓN QUE SE LE ARMA AL PROVEEDOR, no un campo del Cajero. Un test
// que comparase `c.opciones["num_predict"]` contra la constante que quiere proteger sería tautológico:
// pasaría con cualquier valor y con el cableado roto. Lo que hay que probar es que el número llega al
// otro lado.

import (
	"context"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-intent/ollama"
)

// chateadorEspia guarda la ÚLTIMA petición que se le armó y responde bien e instantáneo.
type chateadorEspia struct {
	ultima ollama.ChatRequest
	// resp, si no es nil, es lo que devuelve; nil ⇒ una respuesta mínima válida.
	resp *ollama.ChatResponse
}

var _ Chateador = (*chateadorEspia)(nil)

func (c *chateadorEspia) Chat(_ context.Context, req ollama.ChatRequest) (*ollama.ChatResponse, error) {
	c.ultima = req
	if c.resp != nil {
		return c.resp, nil
	}
	return &ollama.ChatResponse{
		Message: ollama.Message{Role: "assistant", Content: `{"intent":"crear_pedido"}`},
		Done:    true,
	}, nil
}

func (c *chateadorEspia) SupportsThinking(_ context.Context, _ string) bool { return false }

// opcionesDelEdge son las tres que el cableado real le pasa al cajero (cmd/agent/cajero.go), con el
// num_predict por defecto. Se escriben aquí como las escribe producción para que el test mida el mismo
// camino: un `Opciones: nil` probaría un cajero que nadie construye.
func opcionesDelEdge() map[string]any {
	return map[string]any{
		"num_thread":  DefaultNumThread,
		"num_predict": DefaultNumPredict,
		"num_ctx":     DefaultNumCtx,
	}
}

// espiarUnaInferencia sirve UNA petición y devuelve la que llegó al proveedor.
func espiarUnaInferencia(ctx context.Context, t *testing.T, deps Deps, p app.PeticionInferencia) ollama.ChatRequest {
	t.Helper()
	espia, ok := deps.Ollama.(*chateadorEspia)
	if !ok {
		t.Fatalf("espiarUnaInferencia exige un *chateadorEspia, got %T", deps.Ollama)
	}
	_, s := servidorDe(t, deps)
	if _, err := s.Inferir(ctx, p); err != nil {
		t.Fatalf("Inferir: %v", err)
	}
	return espia.ultima
}

// int32Ptr existe porque Go no deja tomar la dirección de un literal y el campo es un puntero A PROPÓSITO
// (presencia: distingue «quiero 0» de «no dije nada»).
func int32Ptr(v int32) *int32 { return &v }

// ─────────────────────────────────────────────────────────────────────────────
// T1.7-3 · el presupuesto de SALIDA
// ─────────────────────────────────────────────────────────────────────────────

// TestMaxOutputTokens_MandaElCloudYSiCallaElEdge es el criterio literal de T1.7-3 por el lado Edge: un
// frame con `max_output_tokens = 512` produce una petición con `num_predict = 512`; ausente ⇒ 256.
//
// EL CASO DEL CERO NO ES UN ADORNO. El contrato declara el campo `optional` precisamente para que «quiero
// 0» sea pedible, y para Ollama `num_predict: 0` significa «no generes nada» — un valor con consecuencia
// real. Un Edge que lo tratara como ausente (`if *p.MaxOutputTokens > 0`) convertiría esa petición en 256
// tokens de generación que nadie encargó, y el fallo sería invisible: la respuesta llegaría, sólo que más
// cara. Es el mismo argumento por el que la temperatura viaja como puntero.
func TestMaxOutputTokens_MandaElCloudYSiCallaElEdge(t *testing.T) {
	ctx := context.Background()
	casos := []struct {
		nombre string
		pedido *int32
		quiero any
	}{
		{"el Cloud lo fija: manda el suyo", int32Ptr(512), 512},
		{"el Cloud calla: el default del Edge", nil, DefaultNumPredict},
		{"el Cloud pide CERO explícito: se respeta", int32Ptr(0), 0},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			p := peticionDe("dame un pedido", timeoutDeLaMedicion)
			p.MaxOutputTokens = c.pedido

			req := espiarUnaInferencia(ctx, t, Deps{
				Ollama:        &chateadorEspia{},
				Opciones:      opcionesDelEdge(),
				MaxConcurrent: 1,
				Timeout:       timeoutDeLaMedicion,
			}, p)

			got, ok := req.Options["num_predict"]
			if !ok {
				t.Fatalf("la petición a Ollama no lleva `num_predict`; Options: %v", req.Options)
			}
			if got != c.quiero {
				t.Errorf("num_predict: got %v (%T) want %v", got, got, c.quiero)
			}
		})
	}
}

// TestMaxOutputTokens_NoContaminaLasSiguientes custodia la razón por la que el mapa se COPIA: el
// presupuesto es de UNA petición, y escribirlo en `c.opciones` lo pegaría a todas las demás.
//
// 🔴 ES EL FALLO QUE NO DA ERROR. Un `c.opciones["num_predict"] = …` compila, pasa el test de arriba y
// deja el cajero sirviendo con el presupuesto de la última petición del Cloud para siempre — además de
// una carrera de datos que `-race` sólo caza si dos inferencias coinciden.
func TestMaxOutputTokens_NoContaminaLasSiguientes(t *testing.T) {
	ctx := context.Background()
	espia := &chateadorEspia{}
	_, s := servidorDe(t, Deps{
		Ollama:        espia,
		Opciones:      opcionesDelEdge(),
		MaxConcurrent: 1,
		Timeout:       timeoutDeLaMedicion,
	})

	conPresupuesto := peticionDe("primera", timeoutDeLaMedicion)
	conPresupuesto.MaxOutputTokens = int32Ptr(512)
	if _, err := s.Inferir(ctx, conPresupuesto); err != nil {
		t.Fatalf("Inferir (con presupuesto): %v", err)
	}
	if _, err := s.Inferir(ctx, peticionDe("segunda", timeoutDeLaMedicion)); err != nil {
		t.Fatalf("Inferir (sin presupuesto): %v", err)
	}

	if got := espia.ultima.Options["num_predict"]; got != DefaultNumPredict {
		t.Errorf("la SEGUNDA petición, que no pidió presupuesto, salió con num_predict=%v; want %d "+
			"(el 512 de la primera se quedó pegado al mapa del Cajero)", got, DefaultNumPredict)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// T1.7-4 · el keep_alive
// ─────────────────────────────────────────────────────────────────────────────

// TestKeepAlive_ViajaEnCadaPeticionYPorDefectoEsParaSiempre fija las dos mitades del criterio: la
// petición LLEVA `keep_alive`, su default es -1 (para siempre) y es configurable.
//
// POR QUÉ EL DEFAULT ES «PARA SIEMPRE»: cuando el runner de Ollama muere por silencio no se lleva sólo el
// modelo, se lleva LA CACHÉ DE PREFIJOS con él, y el siguiente mensaje paga carga del modelo (39 s
// medidos el 2026-08-23) más el prefill en frío del prompt entero. En UAT eso lo tapa
// `OLLAMA_KEEP_ALIVE=-1` en el env de la unidad; en la máquina de un cliente no hay quien lo ponga.
//
// ⚠️ QUE `keep_alive` SALGA EN EL PRIMER NIVEL DEL JSON lo prueba el módulo del proveedor
// (ollama.TestChatKeepAliveEnElWire, wapp-edge-intent v0.3.0), que es donde vive el marshalling. Aquí no
// se puede probar sin un `ollama.New`, y ese grep es el gate de REQ-051.10 («ningún otro proceso que el
// worker habla con Ollama»): un segundo resultado en un fichero de test volvería el gate ruidoso. Lo que
// sí se custodia aquí es la mitad que ESTE repo puede romper — ver el subtest del final.
func TestKeepAlive_ViajaEnCadaPeticionYPorDefectoEsParaSiempre(t *testing.T) {
	ctx := context.Background()

	t.Run("sin configurar: el default del proveedor (para siempre)", func(t *testing.T) {
		req := espiarUnaInferencia(ctx, t, Deps{
			Ollama:        &chateadorEspia{},
			Opciones:      opcionesDelEdge(),
			MaxConcurrent: 1,
			Timeout:       timeoutDeLaMedicion,
		}, peticionDe("hola", timeoutDeLaMedicion))

		if req.KeepAlive == nil {
			t.Fatal("la petición salió SIN keep_alive: el runner de Ollama morirá a los 5 min y se llevará " +
				"la caché de prefijos con él")
		}
		if *req.KeepAlive != ollama.DefaultKeepAliveSeconds {
			t.Errorf("keep_alive por defecto: got %d want %d", *req.KeepAlive, ollama.DefaultKeepAliveSeconds)
		}
	})

	t.Run("configurado: manda lo que diga el Edge, cero incluido", func(t *testing.T) {
		// El 0 va en la tabla a propósito: para Ollama significa «descarga el modelo en cuanto respondas»,
		// que es lo CONTRARIO del default. Un guardarraíl `<=0 ⇒ default` —el patrón de casi todos los
		// vecinos de Deps— lo haría impedible, y el operador que quisiera liberar RAM entre mensajes
		// obtendría exactamente lo opuesto sin un solo aviso.
		for _, seg := range []int{0, 300, -1} {
			req := espiarUnaInferencia(ctx, t, Deps{
				Ollama:        &chateadorEspia{},
				Opciones:      opcionesDelEdge(),
				KeepAlive:     ollama.KeepAliveSeconds(seg),
				MaxConcurrent: 1,
				Timeout:       timeoutDeLaMedicion,
			}, peticionDe("hola", timeoutDeLaMedicion))

			if req.KeepAlive == nil || *req.KeepAlive != seg {
				t.Errorf("keep_alive configurado a %d: got %v", seg, req.KeepAlive)
			}
		}
	})

	// 🔴 ESTE ES EL SUBTEST QUE JUSTIFICA EL FICHERO. `keep_alive` es un campo de PRIMER NIVEL de
	// /api/chat: metido dentro de `options`, Ollama lo IGNORA EN SILENCIO —las claves desconocidas de
	// `options` no dan error— y el runner seguiría muriéndose sin que nada lo delatara. Es la clase de
	// «simplificación» que un futuro lector hará («¿por qué esto no está con las otras opciones?»), y el
	// único síntoma sería una latencia que vuelve a subir semanas después.
	t.Run("NO va dentro de options: ahí Ollama lo ignora en silencio", func(t *testing.T) {
		req := espiarUnaInferencia(ctx, t, Deps{
			Ollama:        &chateadorEspia{},
			Opciones:      opcionesDelEdge(),
			MaxConcurrent: 1,
			Timeout:       timeoutDeLaMedicion,
		}, peticionDe("hola", timeoutDeLaMedicion))

		if v, ok := req.Options["keep_alive"]; ok {
			t.Errorf("`keep_alive` apareció DENTRO de Options (valor %v): ahí Ollama lo ignora sin error y "+
				"el modelo se descarga igual", v)
		}
	})
}

// TestKeepAlive_ElDefaultLoPoneElProveedorYNoUnaCopia es el guardián de una copia que caducaría en
// silencio: el -1 no se escribe en este repo, se toma de ollama.DefaultKeepAliveSeconds. Si mañana el
// módulo del proveedor recomienda un finito, el Edge lo hereda sin que nadie tenga que acordarse.
//
// NO ES TAUTOLÓGICO pese a comparar contra la constante: lo que se comprueba no es su VALOR sino QUIÉN es
// su dueño — que New resuelve el nil contra la constante del proveedor y no contra un literal local.
func TestKeepAlive_ElDefaultLoPoneElProveedorYNoUnaCopia(t *testing.T) {
	c, err := New(Deps{Cola: &colaFake{}, Ollama: &chateadorEspia{}, Log: &logCaptura{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.keepAlive == nil {
		t.Fatal("New dejó keepAlive nil: la petición saldría sin la clave")
	}
	if *c.keepAlive != ollama.DefaultKeepAliveSeconds {
		t.Errorf("keepAlive tras New: got %d, want ollama.DefaultKeepAliveSeconds (%d)",
			*c.keepAlive, ollama.DefaultKeepAliveSeconds)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// T1.7-4 · la marca de calentamiento cruza el cable (el complemento de calentamiento_test.go)
// ─────────────────────────────────────────────────────────────────────────────

// TestCalentamiento_SigueSinTocarElCircuitoConElEmisorReal existe para cerrar el hueco que T1.7-2 dejó
// declarado: la conducta estaba fijada, pero NADIE ponía el campo. Desde T1.7-4 lo pone el frame
// (`InferenceRequest.warmup`) y lo transporta el socket. Este test es el recordatorio de que la marca
// llega, y de que llegar no cambia nada más que el breaker.
func TestCalentamiento_SigueSinTocarElCircuitoConElEmisorReal(t *testing.T) {
	ctx := context.Background()
	espia := &chateadorEspia{}
	c, s := servidorDe(t, Deps{
		Ollama:        espia,
		Opciones:      opcionesDelEdge(),
		MaxConcurrent: 1,
		Timeout:       timeoutDeLaMedicion,
	})

	p := peticionDe("calienta el prefijo", timeoutDeLaMedicion)
	p.Calentamiento = true
	if _, err := s.Inferir(ctx, p); err != nil {
		t.Fatalf("Inferir: %v", err)
	}

	// Se sirve IGUAL: mismo modelo, mismas opciones, mismo keep_alive. Lo único distinto es que no le
	// enseña nada al circuito, y eso lo miden los tests de calentamiento_test.go.
	if espia.ultima.KeepAlive == nil {
		t.Error("un calentamiento salió sin keep_alive: es justo la petición que MÁS necesita que el " +
			"runner siga vivo después")
	}
	if c.Servidas() != 1 {
		t.Errorf("Servidas: got %d want 1 — un calentamiento se cuenta como cualquier otra", c.Servidas())
	}
}
