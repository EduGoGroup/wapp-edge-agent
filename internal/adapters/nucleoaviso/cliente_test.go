package nucleoaviso

// cliente_test.go — el LADO CLIENTE de la señal cajero→núcleo.
//
// 🔴 POR QUÉ NACE ESTE FICHERO, y no es celo: hasta el 2026-08-25 este paquete NO TENÍA UN SOLO TEST, y
// eso salió caro. El aviso «listo» del arranque falló en campo 10 arranques de 10 (DEUDA-044.9) y nada lo
// delató: los tests que sí existían (cmd/agent/cajero_readiness_test.go) levantaban el núcleo ANTES, que
// es justo el caso que NO ocurre en producción. Un paquete sin tests no es un paquete simple: es uno
// donde nadie ha escrito todavía qué debe pasar.

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/server"
)

// dirCorto da un directorio temporal con nombre CORTO, y no es una manía.
//
// 🔴 `t.TempDir()` METE EL NOMBRE DEL TEST EN LA RUTA, y un socket unix tiene un techo duro de ~104 bytes
// en la `sockaddr_un`. Con nombres descriptivos —los de este fichero— el `bind` falla con «invalid
// argument», que no se parece en nada a «la ruta es demasiado larga» y manda a buscar el error donde no
// está. Ocurrió al escribir estos tests.
func dirCorto(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "na")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}

// nucleoFalso levanta un núcleo de mentira sobre un socket unix y guarda lo que le llegó.
type nucleoFalso struct {
	mu       sync.Mutex
	recibido []server.ReadinessRequest
	estado   int
}

func levantarNucleoFalso(t *testing.T, estado int) (*nucleoFalso, string) {
	t.Helper()
	socket := filepath.Join(dirCorto(t), "n.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	n := &nucleoFalso{estado: estado}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req server.ReadinessRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		n.mu.Lock()
		n.recibido = append(n.recibido, req)
		n.mu.Unlock()
		w.WriteHeader(n.estado)
		_ = json.NewEncoder(w).Encode(server.ReadinessResponse{Readiness: req.Readiness, Applied: true})
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return n, socket
}

func (n *nucleoFalso) ultimo(t *testing.T) server.ReadinessRequest {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.recibido) == 0 {
		t.Fatal("el núcleo no recibió ningún aviso")
	}
	return n.recibido[len(n.recibido)-1]
}

// TestAvisarPrefijoFrio_MandaSuPropioValorYNoUnDownSeguidoDeReady es LA razón de que este valor exista
// (DEUDA-044.10). La alternativa barata —mandar «down» y luego «ready» para provocar la transición— daría
// el mismo efecto y dejaría DOS LÍNEAS FALSAS en el log del núcleo diciendo que el cajero se cayó. No se
// cayó: lo que se enfrió fue Ollama por debajo.
//
// MUTACIÓN QUE LO PONE ROJO (ejecutada): hacer que AvisarPrefijoFrio llame a `c.enviar(ctx, dataDir,
// server.ReadinessCaido)` ⇒ readiness = "down" y un aviso de más.
func TestAvisarPrefijoFrio_MandaSuPropioValorYNoUnDownSeguidoDeReady(t *testing.T) {
	n, socket := levantarNucleoFalso(t, http.StatusOK)
	c := Nuevo(socket)

	if err := c.AvisarPrefijoFrio(t.Context(), "/var/lib/wapp/edge-a"); err != nil {
		t.Fatalf("AvisarPrefijoFrio: %v", err)
	}

	req := n.ultimo(t)
	if req.Readiness != server.ReadinessPrefijoFrio {
		t.Errorf("readiness = %q, want %q: el cable tiene que NOMBRAR el hecho, no simularlo con dos "+
			"avisos de estado que el log del núcleo escribiría como una caída que no ocurrió",
			req.Readiness, server.ReadinessPrefijoFrio)
	}
	if req.DataDir != "/var/lib/wapp/edge-a" {
		t.Errorf("data_dir = %q, want la instalación que se pasó", req.DataDir)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.recibido) != 1 {
		t.Errorf("avisos = %d, want 1: es UNA petición por hecho", len(n.recibido))
	}
}

// TestAnunciar_TraduceElBoolALosDosValoresDelCable fija el vocabulario del canal, que es lo que un
// renombrado silencioso rompería sin que compilara nada mal.
func TestAnunciar_TraduceElBoolALosDosValoresDelCable(t *testing.T) {
	for _, tc := range []struct {
		listo  bool
		quiero string
	}{
		{true, server.ReadinessListo},
		{false, server.ReadinessCaido},
	} {
		n, socket := levantarNucleoFalso(t, http.StatusOK)
		if err := Nuevo(socket).Anunciar(t.Context(), "/d", tc.listo); err != nil {
			t.Fatalf("Anunciar(%v): %v", tc.listo, err)
		}
		if got := n.ultimo(t).Readiness; got != tc.quiero {
			t.Errorf("Anunciar(%v) mandó %q, want %q", tc.listo, got, tc.quiero)
		}
	}
}

// TestAvisos_UnNucleoQueRechazaEsUnERROR_ParaElLlamante: si el núcleo contesta != 200, el llamante tiene
// que enterarse. En el cajero eso es lo que rearma la guarda para reintentar en la siguiente inferencia
// fría — si el error se tragara, el reintento no llegaría nunca.
func TestAvisos_UnNucleoQueRechazaEsUnERROR_ParaElLlamante(t *testing.T) {
	_, socket := levantarNucleoFalso(t, http.StatusInternalServerError)
	if err := Nuevo(socket).AvisarPrefijoFrio(t.Context(), "/d"); err == nil {
		t.Error("un 500 del núcleo debe salir como error: es lo que hace que el cajero reintente")
	}
}

// TestAvisos_SinNucleoAlOtroLado_EsErrorYNoUnPanic: el caso REAL del arranque —el socket todavía no
// existe—. Que sea un error normal y no un panic es lo que permite que el llamante siga su curso.
func TestAvisos_SinNucleoAlOtroLado_EsErrorYNoUnPanic(t *testing.T) {
	inexistente := filepath.Join(dirCorto(t), "no.sock")
	if err := Nuevo(inexistente).Anunciar(context.Background(), "/d", true); err == nil {
		t.Error("avisar a un socket que no existe debe devolver error")
	}
}
