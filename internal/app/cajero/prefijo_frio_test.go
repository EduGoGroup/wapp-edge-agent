package cajero

// prefijo_frio_test.go — EL AVISO DE PREFIJO FRÍO (DEUDA-044.10, Plan 044).
//
// 🔴 QUÉ PROBLEMA RESUELVE, y por qué el sujeto observado es el PREFIJO y no Ollama. La readiness de este
// Edge observa a SU CAJERO: se anuncia «listo» al arrancar el cajero y «caído» al pararlo. Reiniciar
// Ollama por debajo no toca ninguna de las dos, así que el Edge sigue diciendo READY con la caché de
// prefijo vacía, el Cloud no ve ninguna transición y NADIE recalienta — el siguiente cliente paga el
// prefill entero (49 s medidos en campo el 2026-08-25, contra un techo de 45 s: no llega).
//
// Una sonda de salud contra Ollama tampoco lo vería: tras el reinicio Ollama está VIVO, con `ollama ps`
// diciendo `Forever`. Lo único que delata el hecho es el `regimen` de una inferencia REAL.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-intent/ollama"
)

// avisadorEspia cuenta los avisos y puede fallar a voluntad. Es síncrono hacia fuera: `esperar` bloquea
// hasta que llega el aviso, porque el aviso sale en goroutine (no se cobra al cliente que ya está
// pagando el prefill) y sin esto el test sería una carrera.
type avisadorEspia struct {
	mu       sync.Mutex
	avisos   int
	fallar   bool
	llegaron chan struct{}
}

func nuevoAvisadorEspia() *avisadorEspia {
	return &avisadorEspia{llegaron: make(chan struct{}, 16)}
}

func (a *avisadorEspia) AvisarPrefijoFrio(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fallar {
		a.llegaron <- struct{}{}
		return errors.New("el núcleo no contesta")
	}
	a.avisos++
	a.llegaron <- struct{}{}
	return nil
}

func (a *avisadorEspia) cuenta() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.avisos
}

// esperar bloquea hasta que llegue UN aviso, o falla el test. El plazo es generoso a propósito: lo que se
// mide aquí no es velocidad.
func (a *avisadorEspia) esperar(t *testing.T) {
	t.Helper()
	select {
	case <-a.llegaron:
	case <-time.After(2 * time.Second):
		t.Fatal("no llegó el aviso de prefijo frío")
	}
}

// servirFases sirve una inferencia con el prefill dado y devuelve el Cajero, con el avisador cableado.
func servirPrefijo(ctx context.Context, t *testing.T, esp *avisadorEspia, prefillMS int64, calentamiento bool) *Cajero {
	t.Helper()
	c, s := servidorDe(t, Deps{
		Ollama:           &chateadorEspia{resp: respuestaConFases(prefillMS, 10)},
		Opciones:         opcionesDelEdge(),
		MaxConcurrent:    1,
		Timeout:          timeoutDeLaMedicion,
		AvisoPrefijoFrio: esp,
	})
	p := peticionDe("dame un pedido", timeoutDeLaMedicion)
	p.Calentamiento = calentamiento
	if _, err := s.Inferir(ctx, p); err != nil {
		t.Fatalf("Inferir: %v", err)
	}
	return c
}

// TestPrefijoFrio_UnaInferenciaFriaPideRecalentar es el caso base: el hecho que dispara es el desenlace de
// una petición real, no un reloj ni una sonda (D-044.43).
//
// MUTACIÓN QUE LO PONE ROJO (ejecutada): quitar la llamada a `c.avisarPrefijoFrio(ctx)` en servidor.go.
func TestPrefijoFrio_UnaInferenciaFriaPideRecalentar(t *testing.T) {
	esp := nuevoAvisadorEspia()
	c := servirPrefijo(t.Context(), t, esp, DefaultPrefillFrioMS+1, false)
	esp.esperar(t)

	if got := esp.cuenta(); got != 1 {
		t.Errorf("avisos = %d, want 1", got)
	}
	if got := c.recalentamientosPedidos.Load(); got != 1 {
		t.Errorf("recalentamientos_pedidos = %d, want 1: el contador es la señal que distingue «a Ollama "+
			"lo reiniciaron una vez» de «el prefijo se está desalojando sin parar»", got)
	}
}

// TestPrefijoFrio_ElCalentamientoNoSePideASiMismo es LA GUARDA QUE CORTA EL BUCLE, y sin ella esto se
// realimenta hasta el infinito: el calentamiento que se pide es, por definición, el que paga el prefill
// frío — así que saldría `regimen=frio` y pediría otro calentamiento, y ése otro.
//
// MUTACIÓN QUE LO PONE ROJO (ejecutada): quitar `&& !p.Calentamiento` de la condición en servidor.go.
func TestPrefijoFrio_ElCalentamientoNoSePideASiMismo(t *testing.T) {
	esp := nuevoAvisadorEspia()
	servirPrefijo(t.Context(), t, esp, DefaultPrefillFrioMS+1, true)

	select {
	case <-esp.llegaron:
		t.Fatal("un CALENTAMIENTO frío pidió otro calentamiento: eso es el bucle infinito que la guarda " +
			"`!p.Calentamiento` existe para cortar")
	case <-time.After(300 * time.Millisecond):
	}
}

// TestPrefijoFrio_NoSePideUnoPorCadaInferenciaFria: entre que se pide el calentamiento y que surte efecto
// pueden llegar N inferencias, y todas saldrán frías. Sin la guarda, cada una pediría otro.
func TestPrefijoFrio_NoSePideUnoPorCadaInferenciaFria(t *testing.T) {
	esp := nuevoAvisadorEspia()
	c, s := servidorDe(t, Deps{
		Ollama:           &chateadorEspia{resp: respuestaConFases(DefaultPrefillFrioMS+1, 10)},
		Opciones:         opcionesDelEdge(),
		MaxConcurrent:    1,
		Timeout:          timeoutDeLaMedicion,
		AvisoPrefijoFrio: esp,
	})
	for range 5 {
		if _, err := s.Inferir(t.Context(), peticionDe("dame un pedido", timeoutDeLaMedicion)); err != nil {
			t.Fatalf("Inferir: %v", err)
		}
	}
	esp.esperar(t)
	time.Sleep(200 * time.Millisecond) // margen para que un segundo aviso indebido llegara

	if got := esp.cuenta(); got != 1 {
		t.Errorf("avisos = %d, want 1: cinco inferencias frías seguidas son UN episodio, no cinco", got)
	}
	if got := c.recalentamientosPedidos.Load(); got != 1 {
		t.Errorf("recalentamientos_pedidos = %d, want 1", got)
	}
}

// TestPrefijoFrio_UnaCalienteRearmaElEpisodio: el ciclo lo cierra un HECHO OBSERVADO —volver a ver una
// inferencia caliente, que es la prueba de que la caché volvió— y no un plazo. Sin esto haría falta
// elegir un número de enfriamiento arbitrario, que es exactamente el reloj que D-044.43 prohíbe.
func TestPrefijoFrio_UnaCalienteRearmaElEpisodio(t *testing.T) {
	esp := nuevoAvisadorEspia()
	c, _ := servidorDe(t, Deps{
		Ollama:           &chateadorEspia{resp: respuestaConFases(DefaultPrefillFrioMS+1, 10)},
		Opciones:         opcionesDelEdge(),
		MaxConcurrent:    1,
		Timeout:          timeoutDeLaMedicion,
		AvisoPrefijoFrio: esp,
	})

	// 1ª fría ⇒ avisa y deja la guarda levantada.
	c.avisarPrefijoFrio(t.Context(), CausaFrioServida)
	esp.esperar(t)
	if !c.prefijoFrioAvisado.Load() {
		t.Fatal("la guarda debía quedar levantada tras avisar")
	}

	// Una CALIENTE la baja (es lo que hace el llamante en servidor.go).
	c.prefijoFrioAvisado.Store(false)

	// Y entonces una nueva fría vuelve a avisar: son DOS episodios, no uno.
	c.avisarPrefijoFrio(t.Context(), CausaFrioServida)
	esp.esperar(t)
	if got := esp.cuenta(); got != 2 {
		t.Errorf("avisos = %d, want 2: tras ver una caliente, un nuevo enfriamiento es un episodio NUEVO", got)
	}
}

// TestPrefijoFrio_SiElAvisoFalla_SeRearmaParaReintentarPorEVENTO: el reintento no lo dispara un
// temporizador sino la SIGUIENTE inferencia fría. Es la misma doctrina por la que `nucleoaviso` no
// reintenta solo.
func TestPrefijoFrio_SiElAvisoFalla_SeRearmaParaReintentarPorEVENTO(t *testing.T) {
	esp := nuevoAvisadorEspia()
	esp.fallar = true
	c, _ := servidorDe(t, Deps{
		Ollama:           &chateadorEspia{resp: respuestaConFases(DefaultPrefillFrioMS+1, 10)},
		Opciones:         opcionesDelEdge(),
		MaxConcurrent:    1,
		Timeout:          timeoutDeLaMedicion,
		AvisoPrefijoFrio: esp,
	})

	c.avisarPrefijoFrio(t.Context(), CausaFrioServida)
	esp.esperar(t)

	esperarGuarda(t, c, false)
	if got := c.recalentamientosPedidos.Load(); got != 0 {
		t.Errorf("recalentamientos_pedidos = %d, want 0: un aviso que NO llegó no se cuenta como pedido", got)
	}
}

// TestPrefijoFrio_SinAvisadorCableado_NoRompe: el puerto es opcional (nil ⇒ la conducta de antes de este
// arreglo). Un Edge sin él sirve igual, sólo que nadie recalienta.
func TestPrefijoFrio_SinAvisadorCableado_NoRompe(t *testing.T) {
	_, s := servidorDe(t, Deps{
		Ollama:        &chateadorEspia{resp: respuestaConFases(DefaultPrefillFrioMS+1, 10)},
		Opciones:      opcionesDelEdge(),
		MaxConcurrent: 1,
		Timeout:       timeoutDeLaMedicion,
	})
	if _, err := s.Inferir(t.Context(), peticionDe("dame un pedido", timeoutDeLaMedicion)); err != nil {
		t.Fatalf("Inferir con avisador nil: %v", err)
	}
}

// esperarGuarda espera a que la guarda tome el valor esperado; el aviso corre en goroutine.
func esperarGuarda(t *testing.T, c *Cajero, quiero bool) {
	t.Helper()
	limite := time.Now().Add(2 * time.Second)
	for time.Now().Before(limite) {
		if c.prefijoFrioAvisado.Load() == quiero {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("la guarda no llegó a %v: un aviso fallido debe REARMARLA para que reintente la siguiente "+
		"inferencia fría (evento), no un temporizador", quiero)
}

var _ AvisadorPrefijoFrio = (*avisadorEspia)(nil)

// ═══════════════════════════════════════════════════════════════════════════════════════════════════
// 2.ª PASADA (2026-08-25, noche): EL TIMEOUT TAMBIÉN AVISA
//
// 🔴 POR QUÉ HIZO FALTA UNA SEGUNDA PASADA, y no es un olvido: la primera se desplegó en UAT y dio
// **CERO avisos** en su propio escenario canónico. Medido, con el reloj del VPS:
//
//	23:01:26  reinicio de Ollama ⇒ prefijo frío (modelo cargado: `ollama ps` = Forever)
//	23:02:57  1.ª clasificación real: MUERE POR TIMEOUT a los 37.993 ms ⇒ NO emite muestra de régimen
//	          🔑 y ese timeout CALIENTA el prefijo: 37.993 ms → 1.499 ms
//	23:03:12  2.ª clasificación: SERVIDA con `regimen=caliente`         ⇒ ya no hay nada que avisar
//
// ⇒ el aviso colgaba del CAMINO FELIZ de una operación que, en el caso que importa, FRACASA. El fallo
// se auto-reparaba borrando su propia evidencia. Estos tests fijan la mitad que faltaba.
// ═══════════════════════════════════════════════════════════════════════════════════════════════════

// chateadorQueVence se queda esperando hasta que vence el plazo de la INFERENCIA y devuelve el error
// envuelto, igual que hace el cliente real de Ollama.
//
// 🔑 ENVOLVER IMPORTA: el código de producción NO mira el texto ni hace `errors.Is` sobre este error
// —el cliente real se traga la causa—, sino que le pregunta al CONTEXTO. Un fake que devolviera
// `context.DeadlineExceeded` pelado probaría un camino que producción no recorre.
type chateadorQueVence struct{}

var _ Chateador = (*chateadorQueVence)(nil)

func (chateadorQueVence) Chat(ctx context.Context, _ ollama.ChatRequest) (*ollama.ChatResponse, error) {
	<-ctx.Done()
	return nil, fmt.Errorf("ollama no responde en http://127.0.0.1:11434: %w", ctx.Err())
}

func (chateadorQueVence) SupportsThinking(context.Context, string) bool { return false }

// plazoQueVence es corto a propósito: lo que se mide es el DESENLACE, no cuánto tarda en llegar.
const plazoQueVence = 60 * time.Millisecond

// vencerUnaInferencia provoca un timeout CON EL PROVEEDOR TRABAJANDO y devuelve el cajero y su log.
func vencerUnaInferencia(ctx context.Context, t *testing.T, esp *avisadorEspia, calentamiento bool) (*Cajero, *logCaptura) {
	t.Helper()
	log := &logCaptura{}
	c, s := servidorDe(t, Deps{
		Ollama:           chateadorQueVence{},
		Opciones:         opcionesDelEdge(),
		MaxConcurrent:    1,
		Timeout:          plazoQueVence,
		AvisoPrefijoFrio: esp,
		Log:              log,
	})
	p := peticionDe("dame un pedido", plazoQueVence)
	p.Calentamiento = calentamiento
	if _, err := s.Inferir(ctx, p); !errors.Is(err, app.ErrInferenciaTimeout) {
		t.Fatalf("se esperaba ErrInferenciaTimeout y salió: %v", err)
	}
	return c, log
}

// TestPrefijoFrio_UnTimeoutTambienPideRecalentar es EL caso que la 1.ª pasada no cubría, y el que en
// campo salió a cero.
//
// MUTACIÓN QUE LO PONE ROJO (ejecutada): quitar el bloque
// `if canonico == app.ErrInferenciaTimeout && !p.Calentamiento { c.avisarPrefijoFrio(...) }`
// de registrarFalloDeInferencia ⇒ 0 avisos. Es exactamente el código desplegado el 2026-08-25, así que
// esta mutación no es hipotética: reconstruye la versión que en campo dio cero. ⚠️ Pone rojos DOS tests,
// éste y el del episodio compartido — y es correcto que sea así: aquél también necesita este camino.
func TestPrefijoFrio_UnTimeoutTambienPideRecalentar(t *testing.T) {
	esp := nuevoAvisadorEspia()
	c, log := vencerUnaInferencia(t.Context(), t, esp, false)
	esp.esperar(t)

	if got := esp.cuenta(); got != 1 {
		t.Fatalf("avisos = %d, se esperaba 1: un plazo vencido con el proveedor TRABAJANDO es la única "+
			"evidencia que queda de que el prefijo está frío — la inferencia no llegó a emitir régimen", got)
	}
	if got := c.recalentamientosPedidos.Load(); got != 1 {
		t.Fatalf("recalentamientos_pedidos = %d, se esperaba 1: el contador del latido es lo que hace "+
			"visible el episodio en campo", got)
	}
	// La causa va al log COMO CAMPO y no dentro de la frase: son dos estados distintos —una que se sirvió
	// fría y una que no se sirvió— y el aviso tiene que decir cuál afirma.
	if !strings.Contains(log.texto(), CausaFrioTimeout) {
		t.Fatalf("el log no dice la causa %q; sin ella, a las semanas no se distingue este episodio del "+
			"de una inferencia servida en frío:\n%s", CausaFrioTimeout, log.texto())
	}
}

// TestPrefijoFrio_UnCalentamientoQueVenceNoSePideASiMismo es la guarda que corta el bucle, y en el camino
// de fallo es MÁS necesaria que en el feliz: si el calentamiento vence —el caso probable, porque es él
// quien paga el prefill frío— y eso pidiera otro calentamiento, el Edge se pediría recalentar en bucle
// justo cuando la máquina va peor.
//
// MUTACIÓN QUE LO PONE ROJO (ejecutada): quitar `&& !p.Calentamiento` de la condición del timeout.
func TestPrefijoFrio_UnCalentamientoQueVenceNoSePideASiMismo(t *testing.T) {
	esp := nuevoAvisadorEspia()
	c, _ := vencerUnaInferencia(t.Context(), t, esp, true)

	select {
	case <-esp.llegaron:
		t.Fatal("un CALENTAMIENTO que vence pidió otro calentamiento: es el bucle que la guarda evita")
	case <-time.After(150 * time.Millisecond):
	}
	if got := c.recalentamientosPedidos.Load(); got != 0 {
		t.Fatalf("recalentamientos_pedidos = %d, se esperaba 0", got)
	}
}

// TestPrefijoFrio_UnAbortoDelLlamanteNoPideRecalentar protege LA TRAMPA de este arreglo.
//
// 🔴 `registrarFalloDeInferencia` NO es el único sitio que devuelve `ErrInferenciaTimeout`: la rama del
// ABORTO —el proceso se apaga, o el cliente colgó— devuelve el mismo error canónico y retorna ANTES. Si
// alguien moviera la comprobación por encima de aquélla, o decidiera el aviso mirando el error DEVUELTO
// en vez del contexto, cada reconexión de CloudLink pediría un calentamiento sin que nadie hubiera
// tocado a Ollama. Aquí no hay prefijo frío que valga: al proveedor ni se le esperó.
//
// MUTACIÓN QUE LO PONE ROJO (ejecutada): mover el bloque del aviso por encima de la guarda
// `if ctxProceso.Err() != nil { … }` ⇒ el aborto pide recalentamiento.
func TestPrefijoFrio_UnAbortoDelLlamanteNoPideRecalentar(t *testing.T) {
	esp := nuevoAvisadorEspia()
	ctx, cancel := context.WithCancel(t.Context())

	c, s := servidorDe(t, Deps{
		Ollama:           &chateadorQueMuere{cancelar: cancel},
		Opciones:         opcionesDelEdge(),
		MaxConcurrent:    1,
		Timeout:          timeoutDeLaMedicion,
		AvisoPrefijoFrio: esp,
	})
	if _, err := s.Inferir(ctx, peticionDe("dame un pedido", timeoutDeLaMedicion)); err == nil {
		t.Fatal("se esperaba un error: la petición se abortó")
	}

	select {
	case <-esp.llegaron:
		t.Fatal("un ABORTO del llamante pidió recalentamiento: no hubo prefijo frío, no se esperó al modelo")
	case <-time.After(150 * time.Millisecond):
	}
	if got := c.recalentamientosPedidos.Load(); got != 0 {
		t.Fatalf("recalentamientos_pedidos = %d, se esperaba 0", got)
	}
}

// TestPrefijoFrio_UnEpisodioPideUNO_AunqueSeManifiestePorLOS_DOS_CAMINOS fija que las dos mitades
// comparten guarda. Un episodio real se manifiesta primero como timeout y después —cuando el prefijo va
// quedando a medias— como servida-fría; si cada camino llevara su propio candado, un solo reinicio de
// Ollama pediría DOS calentamientos a una máquina que ya va justa.
//
// MUTACIÓN QUE LO PONE ROJO (ejecutada): darle al camino del timeout su propio atomic.Bool ⇒ 2 avisos.
func TestPrefijoFrio_UnEpisodioPideUNO_AunqueSeManifiestePorLOS_DOS_CAMINOS(t *testing.T) {
	esp := nuevoAvisadorEspia()
	log := &logCaptura{}
	// El proveedor vence la PRIMERA vez y sirve en frío la segunda: es la secuencia de campo.
	prov := &chateadorQueVenceUnaVez{resp: respuestaConFases(DefaultPrefillFrioMS+1, 10)}
	_, s := servidorDe(t, Deps{
		Ollama:           prov,
		Opciones:         opcionesDelEdge(),
		MaxConcurrent:    1,
		Timeout:          plazoQueVence,
		AvisoPrefijoFrio: esp,
		Log:              log,
	})

	if _, err := s.Inferir(t.Context(), peticionDe("uno", plazoQueVence)); !errors.Is(err, app.ErrInferenciaTimeout) {
		t.Fatalf("la 1.ª tenía que vencer; salió: %v", err)
	}
	esp.esperar(t)
	if _, err := s.Inferir(t.Context(), peticionDe("dos", plazoQueVence)); err != nil {
		t.Fatalf("la 2.ª tenía que servirse (fría); salió: %v", err)
	}

	select {
	case <-esp.llegaron:
		t.Fatal("el mismo episodio pidió DOS recalentamientos: los dos caminos no comparten guarda")
	case <-time.After(150 * time.Millisecond):
	}
	if got := esp.cuenta(); got != 1 {
		t.Fatalf("avisos = %d, se esperaba 1 para todo el episodio", got)
	}
}

// chateadorQueVenceUnaVez vence en la primera llamada y responde en las siguientes. Reproduce lo que hace
// el sistema real: el timeout deja el prefijo a medio calentar, así que la siguiente SÍ termina.
type chateadorQueVenceUnaVez struct {
	mu     sync.Mutex
	usadas int
	resp   *ollama.ChatResponse
}

var _ Chateador = (*chateadorQueVenceUnaVez)(nil)

func (c *chateadorQueVenceUnaVez) Chat(ctx context.Context, _ ollama.ChatRequest) (*ollama.ChatResponse, error) {
	c.mu.Lock()
	c.usadas++
	primera := c.usadas == 1
	c.mu.Unlock()
	if primera {
		<-ctx.Done()
		return nil, fmt.Errorf("ollama no responde en http://127.0.0.1:11434: %w", ctx.Err())
	}
	return c.resp, nil
}

func (c *chateadorQueVenceUnaVez) SupportsThinking(context.Context, string) bool { return false }
