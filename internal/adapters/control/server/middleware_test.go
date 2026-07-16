package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	edgeauth "github.com/EduGoGroup/wapp-edge-agent/internal/auth"
)

// fakeAuthorizer captura los argumentos del gate y devuelve una decisión configurable.
type fakeAuthorizer struct {
	allow  bool
	status int
	code   string

	gotBearer   string
	gotResource string
	gotWrite    bool
	calls       int
}

func (f *fakeAuthorizer) Authorize(_ context.Context, bearer, resource string, write bool) (bool, int, string, string) {
	f.calls++
	f.gotBearer, f.gotResource, f.gotWrite = bearer, resource, write
	if f.allow {
		return true, http.StatusOK, "", ""
	}
	return false, f.status, f.code, "denegado"
}

// mwUnlinker satisface sessionUnlinker sin borrar nada.
type mwUnlinker struct{ called bool }

func (f *mwUnlinker) Unlink(context.Context, string) error { f.called = true; return nil }

// fakeAuthService satisface AuthService sin red.
type fakeAuthService struct{ loginCalled bool }

func (f *fakeAuthService) Login(context.Context, string, string) (edgeauth.LoginResult, error) {
	f.loginCalled = true
	return edgeauth.LoginResult{AccessToken: "tok", TokenType: "Bearer"}, nil
}
func (f *fakeAuthService) Refresh(context.Context) (edgeauth.LoginResult, error) {
	return edgeauth.LoginResult{}, nil
}
func (f *fakeAuthService) Logout(context.Context, bool) error { return nil }

func newTestServer(lister SessionLister) *Server {
	return New(Config{SocketPath: "/tmp/unused.sock", Version: testVersion}, nil, lister)
}

func req(method, path, bearer string) *http.Request {
	r := httptest.NewRequest(method, "http://unix"+path, nil)
	if bearer != "" {
		r.Header.Set("Authorization", "Bearer "+bearer)
	}
	return r
}

func TestMiddleware_NoAuthorizerIsOpen(t *testing.T) {
	srv := newTestServer(fakeLister{})
	// Sin SetAuthorizer: las rutas quedan abiertas (retrocompatible con pre-Plan 033).
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req(http.MethodGet, "/v1/sessions", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("sin authorizer la ruta debe pasar; got %d", rec.Code)
	}
}

func TestMiddleware_ReadDeniedWhenUnauthorized(t *testing.T) {
	srv := newTestServer(fakeLister{})
	fa := &fakeAuthorizer{allow: false, status: http.StatusUnauthorized, code: "unauthorized"}
	srv.SetAuthorizer(fa)

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req(http.MethodGet, "/v1/sessions", ""))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("ruta de lectura sin token debe denegar; got %d", rec.Code)
	}
	if fa.gotResource != resourceStatusRead || fa.gotWrite {
		t.Fatalf("lectura debe evaluar %q write=false; got resource=%q write=%v", resourceStatusRead, fa.gotResource, fa.gotWrite)
	}
}

func TestMiddleware_ReadAllowedForwardsBearer(t *testing.T) {
	srv := newTestServer(fakeLister{})
	fa := &fakeAuthorizer{allow: true}
	srv.SetAuthorizer(fa)

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req(http.MethodGet, "/v1/sessions", "abc.def.ghi"))
	if rec.Code != http.StatusOK {
		t.Fatalf("con grant debe permitir; got %d", rec.Code)
	}
	if fa.gotBearer != "abc.def.ghi" {
		t.Fatalf("el guard debe extraer el Bearer; got %q", fa.gotBearer)
	}
}

func TestMiddleware_WriteRouteMarksWrite(t *testing.T) {
	srv := newTestServer(fakeLister{})
	srv.RegisterUnlink(&mwUnlinker{})
	// Autorizador tipo "modo degradado": deniega escrituras con 403 degraded_read_only.
	fa := &fakeAuthorizer{allow: false, status: http.StatusForbidden, code: "degraded_read_only"}
	srv.SetAuthorizer(fa)

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req(http.MethodDelete, "/v1/sessions/abc", "tok"))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("DELETE en degradado debe ser 403; got %d", rec.Code)
	}
	if fa.gotResource != resourceSessionsLogout || !fa.gotWrite {
		t.Fatalf("DELETE debe evaluar %q write=true; got resource=%q write=%v", resourceSessionsLogout, fa.gotResource, fa.gotWrite)
	}
}

func TestMiddleware_WriteRouteAllowedRunsHandler(t *testing.T) {
	srv := newTestServer(fakeLister{})
	u := &mwUnlinker{}
	srv.RegisterUnlink(u)
	srv.SetAuthorizer(&fakeAuthorizer{allow: true})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req(http.MethodDelete, "/v1/sessions/abc", "tok"))
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE autorizado debe ejecutar el handler; got %d", rec.Code)
	}
	if !u.called {
		t.Fatalf("el handler de unlink debía ejecutarse tras autorizar")
	}
}

func TestMiddleware_AuthEndpointsExempt(t *testing.T) {
	srv := newTestServer(fakeLister{})
	svc := &fakeAuthService{}
	srv.RegisterAuth(svc)
	// Authorizer que deniega TODO: los endpoints /v1/auth deben seguir accesibles (no pasan por el gate).
	srv.SetAuthorizer(&fakeAuthorizer{allow: false, status: http.StatusUnauthorized, code: "unauthorized"})

	r := httptest.NewRequest(http.MethodPost, "http://unix/v1/auth/login",
		strings.NewReader(`{"email":"a@b","password":"x"}`))

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/auth/login debe estar exento del gate; got %d", rec.Code)
	}
	if !svc.loginCalled {
		t.Fatalf("el handler de login debía ejecutarse")
	}
}

func TestMiddleware_HealthExempt(t *testing.T) {
	srv := newTestServer(fakeLister{})
	srv.SetAuthorizer(&fakeAuthorizer{allow: false, status: http.StatusUnauthorized, code: "unauthorized"})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req(http.MethodGet, "/v1/health", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/health debe estar exento (liveness del supervisor); got %d", rec.Code)
	}
}
