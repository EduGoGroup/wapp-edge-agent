package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// fakeLister es un doble de SessionLister (el real lo provee *sessionmgr.Manager vía un adaptador en
// cmd/agent). Permite testear los handlers sin BD ni WhatsApp: Persisted devuelve el inventario fijo y
// Health la etiqueta de salud por session_id (vacío/no-vivo si no está en el mapa).
type fakeLister struct {
	sessions []domain.Session
	health   map[string]string
	err      error
}

func (f fakeLister) Persisted(context.Context) ([]domain.Session, error) {
	return f.sessions, f.err
}

func (f fakeLister) Health(id string) (string, bool) {
	h, ok := f.health[id]
	return h, ok
}

const testVersion = "0.0.0-test"

// startServer levanta el Server real sobre un Unix socket de prueba y devuelve un http.Client que
// marca por ese socket. Usa un directorio temporal CORTO bajo /tmp (no t.TempDir()) porque las rutas
// de t.TempDir() en macOS (/var/folders/...) suelen exceder el límite de sun_path (~104 bytes) de los
// Unix sockets.
func startServer(t *testing.T, lister SessionLister) *http.Client {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "wapp-ctl-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "edge.sock")

	srv := New(Config{SocketPath: socket, Version: testVersion}, nil, lister)
	ln, err := srv.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	// Verifica los permisos restrictivos del socket (0600).
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatalf("Stat socket: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("permisos del socket: got %o, want 600", perm)
	}

	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}
}

// get hace un GET (o el método dado) por el socket. El host de la URL es irrelevante (lo ignora el
// DialContext unix); se usa "unix" por convención.
func do(t *testing.T, c *http.Client, method, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, "http://unix"+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("leyendo body: %v", err)
	}
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("unmarshal %q: %v", string(body), err)
	}
}

func TestHealth(t *testing.T) {
	c := startServer(t, fakeLister{})

	resp := do(t, c, http.MethodGet, "/v1/health")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type: got %q", ct)
	}

	var got healthResponse
	decode(t, resp, &got)
	if got.Status != "ok" {
		t.Errorf("status: got %q, want ok", got.Status)
	}
	if got.Version != testVersion {
		t.Errorf("version: got %q, want %q", got.Version, testVersion)
	}
}

// fakeHealth satisface HealthReporter para GET /v1/health enriquecido (Plan 031 T7).
type fakeHealth struct {
	uptime  int64
	reports map[string]health.Report
	// despacho es el agregado que devuelve DespachoVivas; nil ⇒ el CERO CANÓNICO. Es puntero para que
	// «este test no se ocupa del bloque» y «este test quiere el bloque a 0» no se confundan.
	despacho *health.DespachoStats
}

func (f fakeHealth) DaemonUptimeS() int64                             { return f.uptime }
func (f fakeHealth) Reports(context.Context) map[string]health.Report { return f.reports }

// DespachoVivas (Plan 051 Ola 4 · T4.0): el agregado del desglose sobre las sesiones vivas. Sin agregado
// inyectado el doble devuelve el CERO CANÓNICO —las ocho claves a 0, construidas recorriendo
// app.MotivosOmitido()— y no un struct vacío: así ejercita el mismo invariante que producción (el bloque
// nunca sale con huecos).
func (f fakeHealth) DespachoVivas() health.DespachoStats {
	if f.despacho != nil {
		return *f.despacho
	}
	return health.DespachoStatsCero()
}

// startServerWithHealth arranca el servidor con un colector de salud cableado (SetHealthProvider).
func startServerWithHealth(t *testing.T, lister SessionLister, h HealthReporter) *http.Client {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "wapp-ctl-h-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "edge.sock")

	srv := New(Config{SocketPath: socket, Version: testVersion}, nil, lister)
	srv.SetHealthProvider(h)
	ln, err := srv.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}}}
}

// TestHealth_Enriched: con colector cableado, GET /v1/health suma uptime del daemon + salud por sesión
// (Plan 031 T7), conservando status/version (retrocompatible con el supervisor).
func TestHealth_Enriched(t *testing.T) {
	h := fakeHealth{uptime: 77, reports: map[string]health.Report{
		"sess-1": {SocketState: "degraded", DegradedReason: "dek_load_timeout", OutboxDepth: 3, BinaryVersion: testVersion, DaemonUptimeS: 77},
	}}
	c := startServerWithHealth(t, fakeLister{}, h)

	resp := do(t, c, http.MethodGet, "/v1/health")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	var got healthResponse
	decode(t, resp, &got)
	if got.Status != "ok" || got.Version != testVersion {
		t.Errorf("base: status=%q version=%q", got.Status, got.Version)
	}
	if got.UptimeS != 77 {
		t.Errorf("uptime_s = %d, want 77", got.UptimeS)
	}
	sh, ok := got.Sessions["sess-1"]
	if !ok {
		t.Fatalf("falta la sesión sess-1 en /v1/health: %+v", got.Sessions)
	}
	if sh.SocketState != "degraded" || sh.DegradedReason != "dek_load_timeout" || sh.OutboxDepth != 3 {
		t.Errorf("salud sess-1 = %+v", sh)
	}
}

// leerCuerpo devuelve el cuerpo CRUDO de la respuesta. Existe aparte de `decode` porque hay aserciones que
// sólo se pueden hacer sobre el JSON tal cual salió del cable —qué claves hay y cuáles NO— y no sobre un
// struct, que por definición ignora todo lo que no conoce.
func leerCuerpo(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("leyendo body: %v", err)
	}
	return body
}

// TestHealth_BloqueDespacho es el test que le faltaba entero al bloque `despacho` de GET /v1/health
// (Plan 051 Ola 4 · T4.0): hasta la revisión adversarial del 2026-08-17, `despachoView` era código sin UNA
// sola aserción — se construía, se serializaba y nadie miraba nunca lo que salía.
//
// QUÉ SE FIJA AQUÍ, y por qué cada cosa:
//   - que el bloque EXISTE (es la mitad LOCAL de T4.0: el desglose tiene que poder leerse en el equipo del
//     cliente con `wapp-ctl`, sin depender de que la nube reciba el latido — que es justo la situación en
//     la que alguien lo está mirando);
//   - que trae las OCHO claves, RECORRIENDO `app.MotivosOmitido()` y jamás transcribiéndolas (INV-051.3);
//   - que los DOS SELLOS VIAJAN SEPARADOS y no hay ningún total que los sume (T3.12).
//
// Los contadores del doble son distintos entre sí a propósito: con todo a 1, un cruce de campos en
// `handleHealth` —publicar los polls en `stuck_heads`, o los dos sellos al revés— pasaría en verde.
func TestHealth_BloqueDespacho(t *testing.T) {
	agg := health.DespachoStatsCero()
	agg.OmitidosPorMotivo[string(app.MotivoPresupuesto)] = 5
	agg.OmitidosPorMotivo[string(app.MotivoBreaker)] = 2
	agg.CabezasAtascadas = 3
	agg.PollsCabezaAtascada = 11
	agg.FallosSelloDespacho = 7
	agg.FallosSelloPresupuesto = 13

	h := fakeHealth{
		uptime:   42,
		reports:  map[string]health.Report{"sess-1": {SocketState: "connected", BinaryVersion: testVersion}},
		despacho: &agg,
	}
	c := startServerWithHealth(t, fakeLister{}, h)

	resp := do(t, c, http.MethodGet, "/v1/health")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	body := leerCuerpo(t, resp)

	// (1) EL BLOQUE EXISTE. Se mira sobre el JSON crudo: con `omitempty` y puntero, un bloque que no se
	// construyera desaparecería del cuerpo sin que ningún struct se quejara.
	var crudo struct {
		Despacho map[string]json.RawMessage `json:"despacho"`
	}
	if err := json.Unmarshal(body, &crudo); err != nil {
		t.Fatalf("unmarshal %q: %v", string(body), err)
	}
	if crudo.Despacho == nil {
		t.Fatalf("GET /v1/health salió SIN bloque `despacho` con un colector cableado: %s", body)
	}

	var got healthResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal %q: %v", string(body), err)
	}
	d := got.Despacho
	if d == nil {
		t.Fatalf("el bloque `despacho` no se pudo deserializar: %s", body)
	}

	// (2) LOS CONTADORES, uno a uno y sin cruces.
	if d.SesionesConSalud != len(h.reports) {
		t.Errorf("sesiones_con_salud = %d, want %d (es lo que hace legible el agregado: un stuck_heads:3 "+
			"con una sesión y con veinte no dicen lo mismo)", d.SesionesConSalud, len(h.reports))
	}
	if d.StuckHeads != 3 {
		t.Errorf("stuck_heads = %d, want 3", d.StuckHeads)
	}
	if d.StuckHeadPolls != 11 {
		t.Errorf("stuck_head_polls = %d, want 11 (si sale 3, se está publicando el otro contador del par)", d.StuckHeadPolls)
	}

	// (3) 🔴 LOS DOS SELLOS, SEPARADOS. Sólo `failed_seal_dispatch` implica mensajes DUPLICADOS ya
	// publicados en la nube; `failed_seal_budget` es una fila que se reintenta sola. Confundirlos convierte
	// ruido operativo en un incidente, o al revés.
	if d.FailedSealDispatch != 7 {
		t.Errorf("failed_seal_dispatch = %d, want 7", d.FailedSealDispatch)
	}
	if d.FailedSealBudget != 13 {
		t.Errorf("failed_seal_budget = %d, want 13", d.FailedSealBudget)
	}

	// (4) LAS OCHO CLAVES, SIEMPRE — recorriendo la lista canónica, nunca transcribiéndola (INV-051.3). El
	// doble sólo movió dos motivos; los otros seis tienen que salir a 0, porque un motivo a 0 es un dato y
	// un hueco no.
	for _, motivo := range app.MotivosOmitido() {
		n, presente := d.IntentOmittedByReason[string(motivo)]
		if !presente {
			t.Errorf("falta el motivo %q en el bloque `despacho`; las ocho claves van siempre: %s", motivo, body)
			continue
		}
		switch motivo {
		case app.MotivoPresupuesto:
			if n != 5 {
				t.Errorf("despacho.intent_omitted_by_reason[presupuesto] = %d, want 5", n)
			}
		case app.MotivoBreaker:
			if n != 2 {
				t.Errorf("despacho.intent_omitted_by_reason[breaker] = %d, want 2", n)
			}
		default:
			if n != 0 {
				t.Errorf("despacho.intent_omitted_by_reason[%q] = %d y el doble no lo tocó: los motivos se "+
					"están mezclando entre sí", motivo, n)
			}
		}
	}
	if nClaves, want := len(d.IntentOmittedByReason), len(app.MotivosOmitido()); nClaves != want {
		t.Errorf("el desglose del bloque `despacho` tiene %d claves, want %d", nClaves, want)
	}

	// (5) 🔴 NINGÚN TOTAL AGREGADO. Se comprueba sobre el JUEGO DE CLAVES del JSON crudo, no sobre el
	// struct: un campo nuevo tipo `failed_seal_total` (o un `omitidos` que sume los ocho motivos) no
	// rompería ninguna aserción de arriba y desharía T3.12 y INV-051.3 en el sitio donde el operador lee.
	claves := map[string]bool{
		"sesiones_con_salud":       true,
		"intent_omitted_by_reason": true,
		"stuck_heads":              true,
		"stuck_head_polls":         true,
		"failed_seal_dispatch":     true,
		"failed_seal_budget":       true,
	}
	for k := range crudo.Despacho {
		if !claves[k] {
			t.Errorf("el bloque `despacho` trae una clave nueva %q: si es un total agregado, deshace T3.12 "+
				"(los dos sellos separados) o INV-051.3 (los ocho motivos distinguibles). Cuerpo: %s", k, body)
		}
	}
	for k := range claves {
		if _, ok := crudo.Despacho[k]; !ok {
			t.Errorf("el bloque `despacho` perdió la clave %q: %s", k, body)
		}
	}
}

func TestSessions(t *testing.T) {
	now := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	const sid0 = "11111111-1111-4111-8111-111111111111"
	const sid1 = "22222222-2222-4222-8222-222222222222"
	lister := fakeLister{
		sessions: []domain.Session{
			{SessionID: sid0, JID: "111@s.whatsapp.net", State: domain.SessionStateActive, PairedAt: now, UpdatedAt: now},
			{SessionID: sid1, JID: "222@s.whatsapp.net", State: domain.SessionStateLoggedOut},
		},
		// sid0 está VIVA y escuchando; sid1 no está viva (sin entrada → health omitido).
		health: map[string]string{sid0: "listening"},
	}
	c := startServer(t, lister)

	resp := do(t, c, http.MethodGet, "/v1/sessions")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got sessionsResponse
	decode(t, resp, &got)
	if len(got.Sessions) != 2 {
		t.Fatalf("sessions: got %d, want 2", len(got.Sessions))
	}
	if got.Sessions[0].SessionID != sid0 || got.Sessions[0].JID != "111@s.whatsapp.net" || got.Sessions[0].State != "active" {
		t.Errorf("sesión[0]: %+v", got.Sessions[0])
	}
	if got.Sessions[0].Health != "listening" {
		t.Errorf("health[0]: got %q, want listening", got.Sessions[0].Health)
	}
	if got.Sessions[0].PairedAt != now.Format(time.RFC3339) {
		t.Errorf("paired_at: got %q, want %q", got.Sessions[0].PairedAt, now.Format(time.RFC3339))
	}
	// El segundo: session_id presente, timestamps cero omitidos (omitempty) y health omitido (no vivo).
	if got.Sessions[1].SessionID != sid1 {
		t.Errorf("sesión[1] session_id: %+v", got.Sessions[1])
	}
	if got.Sessions[1].PairedAt != "" || got.Sessions[1].UpdatedAt != "" || got.Sessions[1].Health != "" {
		t.Errorf("campos cero deberían omitirse: %+v", got.Sessions[1])
	}
}

func TestSessions_Empty(t *testing.T) {
	c := startServer(t, fakeLister{})

	resp := do(t, c, http.MethodGet, "/v1/sessions")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// El JSON debe traer "sessions": [] (no null), para que el cliente itere sin comprobar null.
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "{\"sessions\":[]}\n" {
		t.Errorf("body lista vacía: got %q", string(body))
	}
}

func TestSessions_ListerError(t *testing.T) {
	c := startServer(t, fakeLister{err: context.DeadlineExceeded})

	resp := do(t, c, http.MethodGet, "/v1/sessions")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500", resp.StatusCode)
	}
	assertErrorEnvelope(t, resp, codeInternal)
}

// TestErrorEnvelope cubre 404 (ruta desconocida) y 405 (método no permitido) con el envelope JSON.
func TestErrorEnvelope(t *testing.T) {
	c := startServer(t, fakeLister{})

	cases := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantCode   string
		wantAllow  string // Allow esperado (solo en 405)
	}{
		{"ruta desconocida", http.MethodGet, "/v1/desconocido", http.StatusNotFound, codeNotFound, ""},
		{"prefijo no v1", http.MethodGet, "/otra/cosa", http.StatusNotFound, codeNotFound, ""},
		{"metodo no permitido health", http.MethodPost, "/v1/health", http.StatusMethodNotAllowed, codeMethodNotAllowed, "GET"},
		{"metodo no permitido sessions", http.MethodDelete, "/v1/sessions", http.StatusMethodNotAllowed, codeMethodNotAllowed, "GET"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := do(t, c, tc.method, tc.path)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status: got %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if tc.wantAllow != "" {
				if allow := resp.Header.Get("Allow"); allow != tc.wantAllow {
					t.Errorf("Allow: got %q, want %q", allow, tc.wantAllow)
				}
			}
			assertErrorEnvelope(t, resp, tc.wantCode)
		})
	}
}

func assertErrorEnvelope(t *testing.T, resp *http.Response, wantCode string) {
	t.Helper()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type: got %q", ct)
	}
	var env errorBody
	decode(t, resp, &env)
	if env.Error.Code != wantCode {
		t.Errorf("error.code: got %q, want %q", env.Error.Code, wantCode)
	}
	if env.Error.Message == "" {
		t.Errorf("error.message vacío")
	}
}

// TestListen_StaleSocket verifica que un socket huérfano de un arranque previo se limpia y se puede
// volver a escuchar en la misma ruta.
func TestListen_StaleSocket(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "wapp-ctl-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "edge.sock")

	// Deja un socket presente en la ruta (Go hace unlink-on-close, así que NO lo cerramos: lo dejamos
	// vivo para que el archivo de socket exista cuando srv.Listen ejecute su limpieza).
	stale, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("socket previo: %v", err)
	}
	t.Cleanup(func() { _ = stale.Close() })
	if info, statErr := os.Stat(socket); statErr != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("precondición: el socket previo no está presente (err=%v)", statErr)
	}

	srv := New(Config{SocketPath: socket, Version: testVersion}, nil, fakeLister{})
	ln, err := srv.Listen()
	if err != nil {
		t.Fatalf("Listen sobre socket huérfano debería limpiar y reusar: %v", err)
	}
	_ = ln.Close()
}

// TestListen_RefusesRegularFile comprueba que Listen NO borra un archivo regular ajeno en la ruta.
func TestListen_RefusesRegularFile(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "wapp-ctl-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	regular := filepath.Join(dir, "edge.sock")
	if err := os.WriteFile(regular, []byte("no soy un socket"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	srv := New(Config{SocketPath: regular, Version: testVersion}, nil, fakeLister{})
	if _, err := srv.Listen(); err == nil {
		t.Fatal("Listen debería negarse a borrar un archivo regular en la ruta del socket")
	}
	if _, err := os.Stat(regular); err != nil {
		t.Errorf("el archivo regular no debería haberse borrado: %v", err)
	}
}
