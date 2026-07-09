package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// fakeEnroller es un doble del puerto enroller (Plan 023 · T1): simula la persistencia del par mTLS
// creando los archivos cert/clave al enrolar, para que Enrolled() pase de false→true igual que el
// adaptador real de cmd/agent (que reusa internal/infra/enroll). Es el "mock del enrolamiento" que pide
// T1: sin red ni Gateway real.
type fakeEnroller struct {
	mu       sync.Mutex
	certPath string
	keyPath  string
	gotCode  string
	calls    int
	failWith error
}

func (f *fakeEnroller) Enrolled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fileThere(f.certPath) && fileThere(f.keyPath)
}

func (f *fakeEnroller) Enroll(_ context.Context, code string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.gotCode = code
	if f.failWith != nil {
		return f.failWith
	}
	// Simula lo que hace enroll.Run en el adaptador real: persistir el par mTLS en disco.
	if err := os.WriteFile(f.certPath, []byte("cert"), 0o644); err != nil {
		return err
	}
	return os.WriteFile(f.keyPath, []byte("key"), 0o600)
}

func fileThere(p string) bool {
	if p == "" {
		return false
	}
	_, err := os.Stat(p)
	return err == nil
}

// newEnrollTestServer construye un Server con el enroll colgado. Se prueba vía ServeHTTP (el Server es
// http.Handler), sin socket: determinista y sin límites de sun_path.
func newEnrollTestServer(t *testing.T, e enroller) *Server {
	t.Helper()
	srv := New(Config{Version: testVersion}, nil, nil)
	srv.RegisterEnroll(e)
	return srv
}

// TestEnrollStatus_PrimeraEjecucionSinCredencial: sin par mTLS en disco, GET /v1/enroll/status reporta
// enrolled:false → la web mostrará la pantalla "enrolar" en vez del dashboard.
func TestEnrollStatus_PrimeraEjecucionSinCredencial(t *testing.T) {
	dir := t.TempDir()
	fake := &fakeEnroller{certPath: filepath.Join(dir, "edge.crt"), keyPath: filepath.Join(dir, "edge.key")}
	srv := newEnrollTestServer(t, fake)

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/enroll/status", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	var body enrollStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Enrolled {
		t.Fatal("primera ejecución sin credencial: enrolled debería ser false")
	}
}

// TestEnroll_PersisteParYConmutaAEnrolado: enrolar por la web con el activation code (mock) persiste el
// par mTLS y el status conmuta a enrolled:true. Cubre el criterio central de T1.
func TestEnroll_PersisteParYConmutaAEnrolado(t *testing.T) {
	dir := t.TempDir()
	fake := &fakeEnroller{certPath: filepath.Join(dir, "edge.crt"), keyPath: filepath.Join(dir, "edge.key")}
	srv := newEnrollTestServer(t, fake)

	body, _ := json.Marshal(enrollRequest{ActivationCode: "  code-abc  "})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/enroll", bytes.NewReader(body)))

	if rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/enroll: got %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	var resp enrollResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "enrolled" || !resp.Enrolled {
		t.Fatalf("respuesta inesperada: %+v", resp)
	}
	if fake.calls != 1 {
		t.Fatalf("Enroll llamado %d veces, want 1", fake.calls)
	}
	if fake.gotCode != "code-abc" {
		t.Fatalf("activation code: got %q, want \"code-abc\" (trimmeado)", fake.gotCode)
	}

	// El par mTLS quedó persistido (mock) → el status ahora es enrolled.
	rec2 := httptest.NewRecorder()
	srv.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/v1/enroll/status", nil))
	var st enrollStatusResponse
	if err := json.Unmarshal(rec2.Body.Bytes(), &st); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !st.Enrolled {
		t.Fatal("tras enrolar, enrolled debería ser true")
	}
}

// TestEnroll_ConCredencialPresenteDevuelve409: si ya hay par mTLS, la web muestra el dashboard y un
// POST /v1/enroll no re-enrola encima (409, sin llamar al enroll).
func TestEnroll_ConCredencialPresenteDevuelve409(t *testing.T) {
	dir := t.TempDir()
	cert := filepath.Join(dir, "edge.crt")
	key := filepath.Join(dir, "edge.key")
	if err := os.WriteFile(cert, []byte("cert"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, []byte("key"), 0o600); err != nil {
		t.Fatal(err)
	}
	fake := &fakeEnroller{certPath: cert, keyPath: key}
	srv := newEnrollTestServer(t, fake)

	// El status refleja enrolled:true (rama "dashboard").
	recSt := httptest.NewRecorder()
	srv.ServeHTTP(recSt, httptest.NewRequest(http.MethodGet, "/v1/enroll/status", nil))
	var st enrollStatusResponse
	if err := json.Unmarshal(recSt.Body.Bytes(), &st); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !st.Enrolled {
		t.Fatal("con credencial presente: enrolled debería ser true")
	}

	body, _ := json.Marshal(enrollRequest{ActivationCode: "code"})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/enroll", bytes.NewReader(body)))
	if rec.Code != http.StatusConflict {
		t.Fatalf("ya enrolado: got %d, want 409", rec.Code)
	}
	if fake.calls != 0 {
		t.Fatalf("no debe re-enrolar: Enroll llamado %d veces", fake.calls)
	}
}

// TestEnroll_SinCodigoDevuelve400: cuerpo sin activation code → 400, sin tocar el enroll.
func TestEnroll_SinCodigoDevuelve400(t *testing.T) {
	dir := t.TempDir()
	fake := &fakeEnroller{certPath: filepath.Join(dir, "c"), keyPath: filepath.Join(dir, "k")}
	srv := newEnrollTestServer(t, fake)

	body, _ := json.Marshal(enrollRequest{ActivationCode: "   "})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/enroll", bytes.NewReader(body)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("sin código: got %d, want 400", rec.Code)
	}
	if fake.calls != 0 {
		t.Fatalf("no debe llamar Enroll sin código; llamado %d", fake.calls)
	}
}

// TestEnroll_FalloDelEnrollDevuelve502: si el enroll real falla (dial/Gateway/CA), el handler responde
// 502 con el envelope de error y el código estable enroll_failed.
func TestEnroll_FalloDelEnrollDevuelve502(t *testing.T) {
	dir := t.TempDir()
	fake := &fakeEnroller{
		certPath: filepath.Join(dir, "c"),
		keyPath:  filepath.Join(dir, "k"),
		failWith: errors.New("dial gateway: rechazado"),
	}
	srv := newEnrollTestServer(t, fake)

	body, _ := json.Marshal(enrollRequest{ActivationCode: "code"})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/enroll", bytes.NewReader(body)))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("fallo de enroll: got %d, want 502", rec.Code)
	}
	var eb errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &eb); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if eb.Error.Code != codeEnrollFailed {
		t.Fatalf("code: got %q, want %q", eb.Error.Code, codeEnrollFailed)
	}
}
