package wiring_test

// auth_test.go verifica el cableado del guard "tenant coherente" (ADR-0025 dec.1) en el arranque: el
// session manager que construye BuildAuthManager debe tomar el tenant del cert mTLS del Edge
// (Subject.Organization[0]) y rechazar tokens de otro tenant, y quedar inerte cuando el Edge no está
// enrolado (sin cert). Los helpers de token ES256 replican los de internal/auth (que son de paquete
// interno, no importables desde aquí).

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	edgeauth "github.com/EduGoGroup/wapp-edge-agent/internal/auth"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/wiring"
	sharedauth "github.com/EduGoGroup/wapp-shared/auth"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"github.com/golang-jwt/jwt/v5"
)

const wiringTestIssuer = "wapp-cloud"

// writeEdgeCertForTenant genera un cert autofirmado con Subject.Organization = tenant (como el que emite
// el Gateway al enrolar) y lo escribe en <dir>/edge.crt. Devuelve la ruta.
func writeEdgeCertForTenant(t *testing.T, dir, tenant string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generar clave del cert: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "edge-test", Organization: []string{tenant}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("crear cert: %v", err)
	}
	path := filepath.Join(dir, "edge.crt")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("escribir cert: %v", err)
	}
	return path
}

// keyStoreWith construye un KeyStore con una llave ES256 instalada (JWK Set estándar) y devuelve la
// privada para firmar tokens de prueba.
func keyStoreWith(t *testing.T, kid string) (*edgeauth.KeyStore, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generar clave ES256: %v", err)
	}
	ks := edgeauth.NewKeyStore(wiringTestIssuer)
	if err := ks.InstallJWKS(jwksJSONForTest(t, kid, &key.PublicKey)); err != nil {
		t.Fatalf("instalar JWKS: %v", err)
	}
	return ks, key
}

func jwksJSONForTest(t *testing.T, kid string, pub *ecdsa.PublicKey) []byte {
	t.Helper()
	// Bytes() devuelve la codificación sin comprimir SEC1: 0x04 || X(32) || Y(32) para P-256.
	// Se evita pub.X/pub.Y (deprecados desde Go 1.26) siguiendo la guía de crypto/ecdsa.
	raw, err := pub.Bytes()
	if err != nil {
		t.Fatalf("codificar clave pública: %v", err)
	}
	set := map[string]any{"keys": []map[string]any{{
		"kty": "EC", "crv": "P-256", "use": "sig", "alg": "ES256", "kid": kid,
		"x": base64.RawURLEncoding.EncodeToString(raw[1:33]),
		"y": base64.RawURLEncoding.EncodeToString(raw[33:65]),
	}}}
	b, _ := json.Marshal(set)
	return b
}

func mintTokenForTenant(t *testing.T, priv *ecdsa.PrivateKey, kid, tenant string) string {
	t.Helper()
	claims := sharedauth.Claims{
		UserID:   "u1",
		TenantID: tenant,
		Roles:    []string{"operator"},
		Grants:   sharedauth.Grants{Allow: []string{"edge.*"}},
		TokenUse: sharedauth.TokenUseAccess,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        "jti-1",
			Issuer:    wiringTestIssuer,
			Subject:   "u1",
			IssuedAt:  jwt.NewNumericDate(time.Now().Add(-time.Hour)),
			NotBefore: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(10 * time.Minute)),
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

// TestBuildAuthManager_RejectsCrossTenant: con un cert mTLS enrolado a "tenant-esperado", el manager
// construido por el wiring debe RECHAZAR (403 tenant_mismatch) un token válidamente firmado de otro tenant.
func TestBuildAuthManager_RejectsCrossTenant(t *testing.T) {
	dir := t.TempDir()
	certPath := writeEdgeCertForTenant(t, dir, "tenant-esperado")
	ks, key := keyStoreWith(t, "es256-1")

	cfg := config.Config{DataDir: dir}
	cfg.CloudLink.TLSCert = certPath

	mgr := wiring.BuildAuthManager(cfg, sharedlogger.Default(), ks, nil)

	tok := mintTokenForTenant(t, key, "es256-1", "otro-tenant")
	allowed, status, code, _ := mgr.Authorize(context.Background(), tok, "edge.status.read", false)
	if allowed || status != http.StatusForbidden || code != "tenant_mismatch" {
		t.Fatalf("token de otro tenant debe ser 403 tenant_mismatch; got allowed=%v status=%d code=%q", allowed, status, code)
	}

	// Control: un token del MISMO tenant enrolado sí pasa el guard.
	same := mintTokenForTenant(t, key, "es256-1", "tenant-esperado")
	allowed, status, _, _ = mgr.Authorize(context.Background(), same, "edge.status.read", false)
	if !allowed || status != http.StatusOK {
		t.Fatalf("token del tenant enrolado debe autorizarse; got allowed=%v status=%d", allowed, status)
	}
}

// TestBuildAuthManager_InertWithoutEnrollment: sin cert mTLS (Edge no enrolado), el guard de tenant queda
// INERTE — un token de cualquier tenant válidamente firmado se autoriza (no se rompe el bootstrap).
func TestBuildAuthManager_InertWithoutEnrollment(t *testing.T) {
	dir := t.TempDir()
	ks, key := keyStoreWith(t, "es256-1")

	cfg := config.Config{DataDir: dir} // sin CloudLink.TLSCert

	mgr := wiring.BuildAuthManager(cfg, sharedlogger.Default(), ks, nil)

	tok := mintTokenForTenant(t, key, "es256-1", "cualquier-tenant")
	allowed, status, _, _ := mgr.Authorize(context.Background(), tok, "edge.status.read", false)
	if !allowed || status != http.StatusOK {
		t.Fatalf("sin enrolamiento el guard de tenant debe estar inerte; got allowed=%v status=%d", allowed, status)
	}
}
