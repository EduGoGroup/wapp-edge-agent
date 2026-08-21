package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/supervisor"
	"github.com/EduGoGroup/wapp-edge-agent/internal/webui"
)

// TestAvisoSesionPasivaEndpoint (Plan 046 · T3.2 mitad (a)): GET /v1/ui/aviso-sesion-pasiva lo
// atiende wapp-ctl (no el proxy) y devuelve el literal canónico EXACTO desde la fuente única
// webui.AvisoSesionPasiva. El núcleo está caído a propósito (socket inexistente): si la ruta
// cayera al proxy genérico de /v1/*, la respuesta sería 503 daemon_down — así el test también
// caza el des-registro de la ruta, no solo un cuerpo equivocado.
// Mutaciones que lo ponen rojo: quitar o renombrar la ruta del mux (→ 503 del proxy); servir un
// texto distinto de la constante (p.ej. teclearlo inline en main.go); cambiar los nombres de los
// campos JSON {id,texto} que app.js consume; o responder sin el ID de versión.
func TestAvisoSesionPasivaEndpoint(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "missing.sock") // núcleo caído: la ruta NO debe llegar al proxy
	sup := supervisor.New(supervisor.Config{SocketPath: sock}, nil)
	router := newRouter(sup, sock, "", nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/ui/aviso-sesion-pasiva", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("aviso status = %d (¿cayó al proxy?); quería 200. Body: %s", rec.Code, rec.Body.String())
	}
	var body avisoSesionPasivaResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("cuerpo inválido: %v (%s)", err, rec.Body.String())
	}
	if body.ID != webui.AvisoSesionPasivaID {
		t.Fatalf("id = %q; quería %q", body.ID, webui.AvisoSesionPasivaID)
	}
	if body.Texto != webui.AvisoSesionPasiva {
		t.Fatalf("el endpoint no sirve el literal canónico byte a byte.\n--- endpoint ---\n%q\n--- constante ---\n%q", body.Texto, webui.AvisoSesionPasiva)
	}
}
