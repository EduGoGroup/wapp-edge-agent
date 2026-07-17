package auth

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"net/http"
	"testing"
	"time"

	sharedauth "github.com/EduGoGroup/wapp-shared/auth"
)

// setup arma un Manager con una llave ES256 instalada en el store, el relay dado y custodia en memoria.
func setup(t *testing.T, relay Relay) (*Manager, *KeyStore, *ecdsa.PrivateKey) {
	t.Helper()
	key := newES256Key(t)
	ks := NewKeyStore(testIssuer)
	if err := ks.InstallJWKS(jwksJSON(t, "es256-1", &key.PublicKey)); err != nil {
		t.Fatalf("install jwks: %v", err)
	}
	m := NewManager(relay, ks, &MemorySecretCustody{}, nil,
		WithGraceWindow(2*time.Hour), WithRefreshMargin(2*time.Minute))
	return m, ks, key
}

func TestAuthorize_ValidTokenAllows(t *testing.T) {
	relay := &fakeRelay{}
	m, _, key := setup(t, relay)
	tok := mintES256(t, key, "es256-1", "t1", sharedauth.Grants{Allow: []string{"edge.*"}}, timeFuture())
	relay.loginTokens = Tokens{AccessToken: tok, RefreshToken: "rt-1", TokenType: "Bearer", ExpiresAt: timeFuture()}

	if _, err := m.Login(context.Background(), "op@x", "pw"); err != nil {
		t.Fatalf("login: %v", err)
	}
	allowed, status, _, _ := m.Authorize(context.Background(), tok, "edge.status.read", false)
	if !allowed || status != http.StatusOK {
		t.Fatalf("token válido debe autorizar; allowed=%v status=%d", allowed, status)
	}
}

func TestAuthorize_NoTokenUnauthorized(t *testing.T) {
	m, _, _ := setup(t, &fakeRelay{})
	allowed, status, code, _ := m.Authorize(context.Background(), "", "edge.status.read", false)
	if allowed || status != http.StatusUnauthorized || code != "unauthorized" {
		t.Fatalf("sin token debe ser 401 unauthorized; got allowed=%v status=%d code=%s", allowed, status, code)
	}
}

func TestAuthorize_DefaultDeny(t *testing.T) {
	relay := &fakeRelay{}
	m, _, key := setup(t, relay)
	// Grants que NO cubren edge.sessions.logout.
	tok := mintES256(t, key, "es256-1", "t1", sharedauth.Grants{Allow: []string{"edge.status.read"}}, timeFuture())
	relay.loginTokens = Tokens{AccessToken: tok, RefreshToken: "rt", ExpiresAt: timeFuture()}
	_, _ = m.Login(context.Background(), "op", "pw")

	allowed, status, code, _ := m.Authorize(context.Background(), tok, "edge.sessions.logout", true)
	if allowed || status != http.StatusForbidden || code != "forbidden" {
		t.Fatalf("grant no cubierto debe ser 403 forbidden; got allowed=%v status=%d code=%s", allowed, status, code)
	}
}

func TestAuthorize_InvalidTokenUnauthorized(t *testing.T) {
	m, _, _ := setup(t, &fakeRelay{})
	allowed, status, _, _ := m.Authorize(context.Background(), "no-es-un-jwt", "edge.status.read", false)
	if allowed || status != http.StatusUnauthorized {
		t.Fatalf("token basura debe ser 401; got allowed=%v status=%d", allowed, status)
	}
}

func TestAuthorize_DegradedReadAllowedWriteDenied(t *testing.T) {
	relay := &fakeRelay{}
	m, _, key := setup(t, relay)
	// Token EXPIRADO (exp en el pasado) pero dentro de la gracia (past + 2h > now).
	expired := mintES256(t, key, "es256-1", "t1", sharedauth.Grants{Allow: []string{"edge.*"}}, timePast())
	relay.loginTokens = Tokens{AccessToken: expired, RefreshToken: "rt", ExpiresAt: timePast()}
	if _, err := m.Login(context.Background(), "op", "pw"); err != nil {
		t.Fatalf("login: %v", err)
	}

	// Lectura dentro de la gracia: permitida.
	allowed, status, _, _ := m.Authorize(context.Background(), expired, "edge.status.read", false)
	if !allowed || status != http.StatusOK {
		t.Fatalf("lectura degradada debe permitirse; allowed=%v status=%d", allowed, status)
	}
	// Escritura dentro de la gracia: denegada (exige refresh vivo).
	allowed, status, code, _ := m.Authorize(context.Background(), expired, "edge.sessions.logout", true)
	if allowed || status != http.StatusForbidden || code != "degraded_read_only" {
		t.Fatalf("escritura degradada debe ser 403 degraded_read_only; got allowed=%v status=%d code=%s", allowed, status, code)
	}
}

func TestAuthorize_DegradedOutsideGraceDenied(t *testing.T) {
	relay := &fakeRelay{}
	m, _, key := setup(t, relay)
	expired := mintES256(t, key, "es256-1", "t1", sharedauth.Grants{Allow: []string{"edge.*"}}, timePast())
	relay.loginTokens = Tokens{AccessToken: expired, RefreshToken: "rt", ExpiresAt: timePast()}
	_, _ = m.Login(context.Background(), "op", "pw")

	// Reloj del Manager 3h en el futuro ⇒ fuera de la gracia de 2h (la comparación de gracia usa m.now).
	m.now = func() time.Time { return time.Now().Add(3 * time.Hour) }
	allowed, status, _, _ := m.Authorize(context.Background(), expired, "edge.status.read", false)
	if allowed || status != http.StatusUnauthorized {
		t.Fatalf("fuera de la gracia debe ser 401; got allowed=%v status=%d", allowed, status)
	}
}

func TestAuthorize_DegradedOnlyHeldToken(t *testing.T) {
	relay := &fakeRelay{}
	m, _, key := setup(t, relay)
	held := mintES256(t, key, "es256-1", "t1", sharedauth.Grants{Allow: []string{"edge.*"}}, timePast())
	relay.loginTokens = Tokens{AccessToken: held, RefreshToken: "rt", ExpiresAt: timePast()}
	_, _ = m.Login(context.Background(), "op", "pw")

	// Otro token expirado válidamente firmado pero que NO es el custodiado: no obtiene gracia.
	otherExpired := mintES256(t, key, "es256-1", "t1", sharedauth.Grants{Allow: []string{"edge.*"}}, timePast())
	if otherExpired == held {
		t.Skip("tokens idénticos por colisión improbable")
	}
	allowed, status, _, _ := m.Authorize(context.Background(), otherExpired, "edge.status.read", false)
	if allowed || status != http.StatusUnauthorized {
		t.Fatalf("solo el último access custodiado obtiene gracia; got allowed=%v status=%d", allowed, status)
	}
}

func TestAuthorize_TenantMismatch(t *testing.T) {
	relay := &fakeRelay{}
	key := newES256Key(t)
	ks := NewKeyStore(testIssuer)
	_ = ks.InstallJWKS(jwksJSON(t, "es256-1", &key.PublicKey))
	m := NewManager(relay, ks, &MemorySecretCustody{}, nil, WithExpectedTenant("tenant-esperado"))

	tok := mintES256(t, key, "es256-1", "otro-tenant", sharedauth.Grants{Allow: []string{"edge.*"}}, timeFuture())
	relay.loginTokens = Tokens{AccessToken: tok, RefreshToken: "rt", ExpiresAt: timeFuture()}
	_, _ = m.Login(context.Background(), "op", "pw")

	allowed, status, code, _ := m.Authorize(context.Background(), tok, "edge.status.read", false)
	if allowed || status != http.StatusForbidden || code != "tenant_mismatch" {
		t.Fatalf("tenant cruzado debe ser 403 tenant_mismatch; got allowed=%v status=%d code=%s", allowed, status, code)
	}
}

func TestProactiveRefresh_RefreshesNearExpiry(t *testing.T) {
	relay := &fakeRelay{}
	m, _, key := setup(t, relay)
	// Access que expira en 1 min; margen de refresh 2 min ⇒ vencido para el refresh proactivo.
	near := mintES256(t, key, "es256-1", "t1", sharedauth.Grants{Allow: []string{"edge.*"}}, time.Now().Add(time.Minute))
	relay.loginTokens = Tokens{AccessToken: near, RefreshToken: "rt-old", ExpiresAt: time.Now().Add(time.Minute)}
	_, _ = m.Login(context.Background(), "op", "pw")

	// Configura el token que devolverá el refresh.
	fresh := mintES256(t, key, "es256-1", "t1", sharedauth.Grants{Allow: []string{"edge.*"}}, timeFuture())
	relay.refreshTokens = Tokens{AccessToken: fresh, RefreshToken: "rt-new", ExpiresAt: timeFuture()}

	if !m.proactiveTick(context.Background()) {
		t.Fatalf("proactiveTick debía intentar refrescar (access por expirar)")
	}
	if relay.refreshCalls != 1 {
		t.Fatalf("se esperaba 1 refresh, hubo %d", relay.refreshCalls)
	}
	if relay.lastRefresh != "rt-old" {
		t.Fatalf("el refresh debe presentar el token custodiado; got %q", relay.lastRefresh)
	}
	// Tras refrescar, el access custodiado rota al nuevo.
	if got := m.currentAccessForTest(); got != fresh {
		t.Fatalf("el access debe rotar al del refresh")
	}
}

func TestProactiveRefresh_NoSessionNoOp(t *testing.T) {
	relay := &fakeRelay{}
	m, _, _ := setup(t, relay)
	if m.proactiveTick(context.Background()) {
		t.Fatalf("sin sesión no debe intentar refresh")
	}
	if relay.refreshCalls != 0 {
		t.Fatalf("no debía llamar a refresh")
	}
}

func TestRefresh_NoCustodyIsRefreshInvalid(t *testing.T) {
	relay := &fakeRelay{}
	m, _, _ := setup(t, relay)
	_, err := m.Refresh(context.Background())
	if !errors.Is(err, ErrRefreshInvalid) {
		t.Fatalf("sin refresh custodiado se espera ErrRefreshInvalid; got %v", err)
	}
}

func TestLogout_ClearsSessionAndRevokes(t *testing.T) {
	relay := &fakeRelay{}
	m, _, key := setup(t, relay)
	tok := mintES256(t, key, "es256-1", "t1", sharedauth.Grants{Allow: []string{"edge.*"}}, timeFuture())
	relay.loginTokens = Tokens{AccessToken: tok, RefreshToken: "rt", ExpiresAt: timeFuture()}
	_, _ = m.Login(context.Background(), "op", "pw")

	if err := m.Logout(context.Background(), true); err != nil {
		t.Fatalf("logout: %v", err)
	}
	if relay.logoutCalls != 1 || !relay.lastAllSess {
		t.Fatalf("logout debe relayar la revocación con all_sessions=true")
	}
	// Tras el logout, un request con el token previo ya no obtiene gracia (sin sesión custodiada).
	allowed, status, _, _ := m.Authorize(context.Background(), tok, "edge.status.read", false)
	if allowed && status == http.StatusOK {
		// El token sigue vigente por firma (no expiró), así que la vía viva lo validaría igual: esto NO
		// prueba la revocación (que es del cloud). Solo comprobamos que el estado local se limpió.
		_ = allowed
	}
	if m.currentAccessForTest() != "" {
		t.Fatalf("logout debe limpiar el access en memoria")
	}
}

func TestLoginError_Propagates(t *testing.T) {
	relay := &fakeRelay{loginErr: ErrInvalidCredentials}
	m, _, _ := setup(t, relay)
	if _, err := m.Login(context.Background(), "op", "bad"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("el error del relay debe propagarse; got %v", err)
	}
}
