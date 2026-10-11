package daemon

// palanca_whatsapp_falso_test.go — EL GUARDARRAÍL DE LA PALANCA DEL WHATSAPP FALSO (ADR-0049).
//
// Mismo motivo que palanca_inyector_test.go: `Run` no lo ejercita ningún test, así que la elección del
// transporte se extrajo a `elegirTransporte` y `registrarCapturados` para poder afirmar sobre ella. Lo que
// se vigila son los DOS sentidos de la palanca: baja, el Edge es el de siempre; echada, arranca sin device
// ni socket, y lo grita.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/diag"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsappfalso"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	wappdb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
)

const variableWhatsAppFalso = "WAPP_AGENT_WHATSAPP_FALSO"

func pedirCapturados(srv http.Handler, metodo, sessionID string) *httptest.ResponseRecorder {
	url := diag.RutaCapturados
	if sessionID != "" {
		url += "?session_id=" + sessionID
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(metodo, url, nil))
	return w
}

// RW.1.b — con la palanca BAJADA nada cambia: el transporte es el real, no hay registro, no hay ruta, no
// hay grito, y el latido dice `real`.
func TestPalancaWhatsAppFalso_ConLaPalancaBAJADANadaCambia(t *testing.T) {
	// (Que el default ESCRITO es «WhatsApp de verdad» lo afirma config/palanca_whatsapp_falso_test.go.)
	srv, _, log, buf := servidorDePrueba(t)
	cfg := config.Config{WhatsAppFalso: false}

	transporte, registro := elegirTransporte(cfg, log)
	if registro != nil {
		t.Error("con la palanca BAJADA existe un registro de envíos capturados: nadie debería poder llenarlo")
	}
	escucha := transporte.NuevaEscucha(nil, "sqlite", "5491100000000@s.whatsapp.net", log)
	if _, real := escucha.(*whatsmeow.ListenGateway); !real {
		t.Fatalf("con la palanca BAJADA la escucha es %T y debe ser el gateway REAL de whatsmeow: este Edge "+
			"no hablaría con WhatsApp sin que nadie lo hubiera pedido", escucha)
	}
	if _, real := transporte.NuevoConector(nil, "sqlite", log).(*whatsmeow.Connector); !real {
		t.Errorf("con la palanca BAJADA el conector de emparejamiento no es el REAL de whatsmeow")
	}

	registrarCapturados(srv, cfg, registro)
	if w := pedirCapturados(srv, http.MethodGet, "s-1"); w.Code != http.StatusNotFound {
		t.Errorf("con la palanca BAJADA la ruta %s tiene que NO EXISTIR (404) y contestó %d: %s",
			diag.RutaCapturados, w.Code, w.Body.String())
	}
	// Ni siquiera con un registro en la mano se registra la ruta si la palanca está baja.
	registrarCapturados(srv, cfg, whatsappfalso.NuevoRegistro(0))
	if w := pedirCapturados(srv, http.MethodGet, "s-1"); w.Code != http.StatusNotFound {
		t.Errorf("la ruta %s se registró con la palanca BAJADA por tener un registro (%d)", diag.RutaCapturados, w.Code)
	}

	if strings.Contains(buf.String(), variableWhatsAppFalso) {
		t.Errorf("el daemon GRITA la palanca del WhatsApp falso en un Edge que no la tiene puesta. Log: %q", buf.String())
	}
	if lat := buildLatencia(cfg, colaCompleta{}, logMudo()); lat.deps.WhatsAppFalso {
		t.Error("el latido recibió una palanca que nadie echó: publicaría `whatsapp=FALSO` en un Edge sano")
	}
}

// RW.3.a — con la palanca ECHADA el daemon lo GRITA al arrancar (WARN, nombrando la variable) y el latido
// lo publica en cada línea.
func TestPalancaWhatsAppFalso_ElDaemonLoGRITAYElLatidoLoPublica(t *testing.T) {
	_, _, log, buf := servidorDePrueba(t)
	cfg := config.Config{WhatsAppFalso: true, InboundStatsEveryMS: 1234}

	_, registro := elegirTransporte(cfg, log)
	if registro == nil {
		t.Fatal("con la palanca ECHADA no hay registro de envíos: lo que el Edge «envía» se perdería")
	}
	linea := buf.String()
	if !strings.Contains(linea, variableWhatsAppFalso) {
		t.Errorf("el arranque no nombra la variable que cambió el transporte. Log: %q", linea)
	}
	if !strings.Contains(linea, "WARN") {
		t.Errorf("el aviso del WhatsApp falso no sale a nivel WARN. Log: %q", linea)
	}
	if !strings.Contains(linea, "NO HABLA CON WHATSAPP") {
		t.Errorf("el aviso no dice lo único que importa: que este Edge no habla con WhatsApp. Log: %q", linea)
	}

	lat := buildLatencia(cfg, colaCompleta{}, logMudo())
	if !lat.deps.WhatsAppFalso {
		t.Error("el latido no recibió la palanca ECHADA: cada línea diría `whatsapp=real` en un Edge que no " +
			"entrega ningún mensaje")
	}
	if lat.deps.InyectorEntrantes || lat.deps.DespachadorApagado {
		t.Error("echar el WhatsApp falso encendió otra palanca en el latido: son tres instrumentos distintos")
	}
}

// RW.2.c — la ruta de lectura devuelve lo enviado por la sesión pedida, es de LECTURA y va tras el mismo
// recurso que el inyector. (Que con la palanca baja no existe lo afirma el test de arriba.)
func TestCapturados_LaRutaDevuelveLoEnviadoYConLaPalancaBAJADANoExiste(t *testing.T) {
	srv, espia, _, _ := servidorDePrueba(t)
	registro := whatsappfalso.NuevoRegistro(0)
	registro.Anotar(whatsappfalso.Envio{SessionID: "s-1", CommandID: "cmd-1", MessageID: "SINTETICO-aa", To: "549", Text: "Elige una categoría"})
	registro.Anotar(whatsappfalso.Envio{SessionID: "s-2", CommandID: "cmd-otra", To: "549", Text: "de otra sesión"})
	registro.Anotar(whatsappfalso.Envio{SessionID: "s-1", CommandID: "cmd-2", To: "549",
		Media: &whatsappfalso.Media{URL: "http://x/y", Filename: "lista.pdf", MIME: "application/pdf", Kind: "document"}})

	// Palanca baja: no existe.
	registrarCapturados(srv, config.Config{WhatsAppFalso: false}, registro)
	if w := pedirCapturados(srv, http.MethodGet, "s-1"); w.Code != http.StatusNotFound {
		t.Fatalf("con la palanca BAJADA la ruta contestó %d y debe ser 404", w.Code)
	}

	registrarCapturados(srv, config.Config{WhatsAppFalso: true}, registro)
	w := pedirCapturados(srv, http.MethodGet, "s-1")
	if w.Code != http.StatusOK {
		t.Fatalf("con la palanca ECHADA la ruta contestó %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		SessionID  string                `json:"session_id"`
		Capturados []whatsappfalso.Envio `json:"capturados"`
		Total      int                   `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("la respuesta no es JSON: %v (%s)", err, w.Body.String())
	}
	if resp.Total != 2 || len(resp.Capturados) != 2 {
		t.Fatalf("la sesión s-1 envió 2 mensajes y la ruta devuelve %d (total=%d): %s", len(resp.Capturados), resp.Total, w.Body.String())
	}
	if resp.Capturados[0].CommandID != "cmd-1" || resp.Capturados[0].Text != "Elige una categoría" {
		t.Errorf("el primer capturado no es el primero enviado: %+v", resp.Capturados[0])
	}
	if resp.Capturados[1].CommandID != "cmd-2" || resp.Capturados[1].Media == nil || resp.Capturados[1].Media.Filename != "lista.pdf" {
		t.Errorf("el segundo capturado no trae su media: %+v", resp.Capturados[1])
	}

	if len(espia.recursos) == 0 || espia.recursos[len(espia.recursos)-1] != diag.RecursoInyectar {
		t.Errorf("la ruta no pasa por el gate con el recurso %q: %v", diag.RecursoInyectar, espia.recursos)
	}
	if espia.writes[len(espia.writes)-1] {
		t.Error("la ruta de lectura se registró como ESCRITURA: en modo degradado dejaría de poder leerse")
	}
	if g := pedirCapturados(srv, http.MethodGet, ""); g.Code != http.StatusBadRequest {
		t.Errorf("sin session_id la ruta debe contestar 400 y contestó %d", g.Code)
	}
	if p := pedirCapturados(srv, http.MethodPost, "s-1"); p.Code != http.StatusMethodNotAllowed {
		t.Errorf("un POST a la ruta de lectura debe dar 405 y dio %d", p.Code)
	}
}

// --- dobles del test de arranque -------------------------------------------------------------------------

type custodiaMemoria struct {
	mu   *sync.Mutex
	deks map[string][]byte
	ruta string
}

func (c custodiaMemoria) Store(dek []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deks[c.ruta] = append([]byte(nil), dek...)
	return nil
}

func (c custodiaMemoria) Load() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dek, ok := c.deks[c.ruta]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return append([]byte(nil), dek...), nil
}

func (c custodiaMemoria) Exists() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.deks[c.ruta]
	return ok
}

func (c custodiaMemoria) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.deks, c.ruta)
	return nil
}

type sinQR struct{ vistos int }

func (q *sinQR) ShowQR(string) error { q.vistos++; return nil }

type colaQueGuarda struct {
	mu    sync.Mutex
	items []app.ColaItem
}

func (c *colaQueGuarda) Enqueue(_ context.Context, it app.ColaItem) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = append(c.items, it)
	return nil
}

func (c *colaQueGuarda) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// muxEspia es el multiplexor de CloudLink visto por el gestor: guarda el emisor que cada sesión registra.
type muxEspia struct {
	mu     sync.Mutex
	envios map[string]func(ctx context.Context, commandID, to, text string) error
	jids   map[string]string
}

func (m *muxEspia) Register(sessionID, selfJID string,
	send func(ctx context.Context, commandID, to, text string) error,
	_ func(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) error,
	_ func() bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.envios == nil {
		m.envios = map[string]func(context.Context, string, string, string) error{}
		m.jids = map[string]string{}
	}
	m.envios[sessionID] = send
	m.jids[sessionID] = selfJID
}
func (m *muxEspia) Unregister(string)                       {}
func (m *muxEspia) SinkFor(string) app.InboundSink          { return nil }
func (m *muxEspia) SendReceipt(string, domain.ReceiptEvent) {}
func (m *muxEspia) SendLoggedOut(string)                    {}
func (m *muxEspia) emisor(id string) func(context.Context, string, string, string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.envios[id]
}

// RW.1.a — con la palanca ECHADA el gestor de sesiones de verdad arranca sin ningún device, empareja sin
// QR, escucha sin socket, recibe lo inyectado y captura lo enviado, y en la base no queda ni una fila del
// almacén de whatsmeow. Es el recorrido entero del transporte falso con todo lo demás REAL: el Manager, la
// base migrada, el almacén de sesiones y el caso de uso de emparejar (que es quien genera y sella la DEK).
func TestPalancaWhatsAppFalso_ConLaPalancaECHADAElDaemonArrancaSinDeviceNiRed(t *testing.T) {
	base := t.TempDir()
	database, err := wappdb.OpenAndMigrate(context.Background(), filepath.Join(base, "edge.db"))
	if err != nil {
		t.Fatalf("abrir/migrar la BD única: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	logBuf := &bytes.Buffer{}
	var logMu sync.Mutex
	log := sharedlogger.New(sharedlogger.WithWriter(escritorConCandado{mu: &logMu, w: logBuf}))

	transporte, registro := elegirTransporte(config.Config{WhatsAppFalso: true}, log)
	if _, falso := transporte.NuevaEscucha(nil, "", "", log).(*whatsappfalso.Escucha); !falso {
		t.Fatal("con la palanca ECHADA la escucha no es la del transporte falso: abriría un socket a WhatsApp")
	}

	deks := map[string][]byte{}
	var deksMu sync.Mutex
	mux := &muxEspia{}
	cola := &colaQueGuarda{}
	mgr := sessionmgr.NewManager(sessionmgr.NewLayout(base), sessionstore.New(database), 5, log,
		sessionmgr.WithSharedDB(database, wappdb.DialectSQLite),
		sessionmgr.WithEscucha(transporte, mux, "wApp"),
		sessionmgr.WithEmparejamiento(transporte, 5*time.Second),
		sessionmgr.WithColaEntrantes(cola),
		sessionmgr.WithKeyCustodyFactory(func(path string) app.KeyCustody {
			return custodiaMemoria{mu: &deksMu, deks: deks, ruta: path}
		}))
	t.Cleanup(mgr.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 1. SIN DEVICE: restaurar con cero sesiones no es un error.
	if err := mgr.Restore(ctx); err != nil {
		t.Fatalf("Restore sin ninguna sesión pareada: %v", err)
	}

	// 2. EMPAREJAR SIN QR NI RED. La DEK la genera app.Pair y la sella en la custodia del cliente.
	qr := &sinQR{}
	res, err := mgr.Pair(ctx, qr)
	if err != nil {
		t.Fatalf("emparejar con el transporte falso: %v", err)
	}
	if qr.vistos != 0 {
		t.Errorf("el emparejamiento falso pintó %d QR: no hay teléfono que lo escanee", qr.vistos)
	}
	if !strings.HasPrefix(domain.SelfPNFromJID(res.WaJID), "0000") {
		t.Errorf("el JID de la sesión falsa es %q: no se reconoce como falso", res.WaJID)
	}
	deksMu.Lock()
	nDEK := len(deks)
	var dek []byte
	for _, d := range deks {
		dek = d
	}
	deksMu.Unlock()
	if nDEK != 1 || len(dek) != app.DEKSize {
		t.Fatalf("tras emparejar hay %d DEK en la custodia del cliente (tamaño %d): la sesión falsa debe tener "+
			"la suya, generada y sellada como la de una real", nDEK, len(dek))
	}

	// 3. ESCUCHA SIN SOCKET Y RECIBE LO INYECTADO. El 409 (sin escucha viva) es transitorio: se reintenta.
	var inyErr error
	for i := 0; i < 500; i++ {
		_, inyErr = mgr.InyectarEntrante(ctx, res.SessionID, app.InyeccionEntrante{Texto: "carrito", Lote: "arranque", Indice: i})
		if inyErr == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if inyErr != nil {
		t.Fatalf("la sesión falsa nunca llegó a escuchar: %v", inyErr)
	}
	if n := cola.total(); n != 1 {
		t.Fatalf("se inyectó 1 entrante y la cola del gestor recibió %d", n)
	}

	// 4. ENVÍA Y QUEDA CAPTURADO, por el emisor que la sesión registró en el multiplexor de CloudLink.
	send := mux.emisor(res.SessionID)
	if send == nil {
		t.Fatal("la sesión falsa no se registró en el multiplexor de CloudLink: el Cloud no tendría a quién mandar")
	}
	if err := send(ctx, "cmd-arranque", "5491100000001", "Elige una categoría"); err != nil {
		t.Fatalf("enviar por la sesión falsa: %v", err)
	}
	envios, _ := registro.De(res.SessionID)
	if len(envios) != 1 || envios[0].CommandID != "cmd-arranque" || envios[0].Text != "Elige una categoría" {
		t.Fatalf("lo capturado no es lo enviado: %+v", envios)
	}

	// 5. NI UNA FILA DEL ALMACÉN DE WHATSMEOW: no hubo device.
	filas, err := database.QueryContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'whatsmeow%' OR name LIKE 'msg_enc%')`)
	if err != nil {
		t.Fatalf("listar las tablas del almacén: %v", err)
	}
	var tablas []string
	for filas.Next() {
		var n string
		if err := filas.Scan(&n); err != nil {
			t.Fatal(err)
		}
		tablas = append(tablas, n)
	}
	_ = filas.Close()
	for _, tabla := range tablas {
		var n int
		if err := database.QueryRowContext(ctx, "SELECT count(*) FROM "+tabla).Scan(&n); err != nil {
			t.Fatalf("contar %s: %v", tabla, err)
		}
		if n != 0 {
			t.Errorf("la tabla %s del almacén de whatsmeow tiene %d filas: el transporte falso creó un device", tabla, n)
		}
	}

	// 6. LA DEK NO ASOMA EN EL LOG del arranque, el emparejamiento, la escucha y el envío.
	logMu.Lock()
	salida := logBuf.String()
	logMu.Unlock()
	if strings.Contains(salida, string(dek)) {
		t.Error("la DEK de la sesión falsa aparece en crudo en el log")
	}

	// 7. DESVINCULAR una sesión que nunca tuvo almacén de whatsmeow no falla (lo que FW-01 no pudo medir).
	if err := mgr.Unlink(ctx, res.SessionID); err != nil {
		t.Errorf("desvincular la sesión falsa: %v", err)
	}
}

type escritorConCandado struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (e escritorConCandado) Write(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.w.Write(p)
}
