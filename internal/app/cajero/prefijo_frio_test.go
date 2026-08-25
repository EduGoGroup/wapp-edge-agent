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
	"sync"
	"testing"
	"time"
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
	c.avisarPrefijoFrio(t.Context())
	esp.esperar(t)
	if !c.prefijoFrioAvisado.Load() {
		t.Fatal("la guarda debía quedar levantada tras avisar")
	}

	// Una CALIENTE la baja (es lo que hace el llamante en servidor.go).
	c.prefijoFrioAvisado.Store(false)

	// Y entonces una nueva fría vuelve a avisar: son DOS episodios, no uno.
	c.avisarPrefijoFrio(t.Context())
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

	c.avisarPrefijoFrio(t.Context())
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
