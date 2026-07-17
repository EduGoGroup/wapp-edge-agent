package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	sharedauth "github.com/EduGoGroup/wapp-shared/auth"
	"github.com/golang-jwt/jwt/v5"
)

const testIssuer = "wapp-cloud"

// timeFuture/timePast son expiraciones relativas al reloj REAL (la validación de firma del JWT usa
// time.Now internamente, no el reloj inyectable del Manager).
func timeFuture() time.Time { return time.Now().Add(10 * time.Minute) }
func timePast() time.Time   { return time.Now().Add(-1 * time.Hour) }

// currentAccessForTest lee bajo lock el access token en memoria (test-only, evita carreras del -race).
func (m *Manager) currentAccessForTest() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.access
}

// newES256Key genera un par ES256 (P-256) para los tests.
func newES256Key(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generar clave ES256: %v", err)
	}
	return k
}

// jwksJSON arma un JWK Set ES256 estándar con una llave (kid → pub), tal como lo empuja la nube.
func jwksJSON(t *testing.T, kid string, pub *ecdsa.PublicKey) []byte {
	t.Helper()
	// Bytes() devuelve la codificación sin comprimir SEC1: 0x04 || X(32) || Y(32) para P-256.
	// Se evita pub.X/pub.Y (deprecados desde Go 1.26) siguiendo la guía de crypto/ecdsa.
	raw, err := pub.Bytes()
	if err != nil {
		t.Fatalf("codificar clave pública: %v", err)
	}
	set := map[string]any{"keys": []map[string]any{{
		"kty": "EC",
		"crv": "P-256",
		"use": "sig",
		"alg": "ES256",
		"kid": kid,
		"x":   base64.RawURLEncoding.EncodeToString(raw[1:33]),
		"y":   base64.RawURLEncoding.EncodeToString(raw[33:65]),
	}}}
	b, _ := json.Marshal(set)
	return b
}

// mintES256 firma un access token ES256 con el kid, grants y expiración dados. Permite exp en el PASADO
// (para probar el modo degradado), algo que GenerateToken del IAM no deja hacer.
func mintES256(t *testing.T, priv *ecdsa.PrivateKey, kid, tenant string, grants sharedauth.Grants, exp time.Time) string {
	t.Helper()
	claims := sharedauth.Claims{
		UserID:   "u1",
		TenantID: tenant,
		Roles:    []string{"operator"},
		Grants:   grants,
		TokenUse: sharedauth.TokenUseAccess,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        "jti-1",
			Issuer:    testIssuer,
			Subject:   "u1",
			IssuedAt:  jwt.NewNumericDate(time.Now().Add(-time.Hour)),
			NotBefore: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	tok.Header["kid"] = kid
	s, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("firmar token ES256: %v", err)
	}
	return s
}

// fakeRelay es un Relay mockeado: registra llamadas y devuelve lo configurado (sin red).
type fakeRelay struct {
	loginTokens   Tokens
	loginErr      error
	refreshTokens Tokens
	refreshErr    error
	logoutErr     error

	loginCalls   int
	refreshCalls int
	logoutCalls  int
	lastRefresh  string
	lastAllSess  bool
}

func (f *fakeRelay) Login(_ context.Context, _, _ string) (Tokens, error) {
	f.loginCalls++
	return f.loginTokens, f.loginErr
}

func (f *fakeRelay) Refresh(_ context.Context, refresh string) (Tokens, error) {
	f.refreshCalls++
	f.lastRefresh = refresh
	return f.refreshTokens, f.refreshErr
}

func (f *fakeRelay) Logout(_ context.Context, refresh string, all bool) error {
	f.logoutCalls++
	f.lastRefresh = refresh
	f.lastAllSess = all
	return f.logoutErr
}
