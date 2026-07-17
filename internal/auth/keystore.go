package auth

import (
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"

	sharedauth "github.com/EduGoGroup/wapp-shared/auth"
)

// jwkSet es un JWK Set estándar (RFC 7517) tal como lo empuja la nube por
// ConfigUpdate kind:"jwks" (ADR-0025 dec.2). La llave pública ES256 es GLOBAL del
// emisor (no por-tenant): el Edge la instala por kid y valida el access token
// offline con el MultiVerifier de wapp-shared/auth.
type jwkSet struct {
	Keys []jwk `json:"keys"`
}

// jwk es una entrada EC P-256 del set: coords x/y de 32 bytes big-endian en
// base64url (sin padding), kid, use="sig", alg="ES256".
type jwk struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
}

// jwkKey es una llave pública ES256 ya parseada, lista para el MultiVerifier.
type jwkKey struct {
	Kid string
	Pub *ecdsa.PublicKey
}

// ParseJWKS parsea y valida un JWK Set ES256 (el payload de ConfigUpdate
// kind:"jwks"). Rechaza sets vacíos, kty/crv/alg inesperados, kids vacíos o
// duplicados, y coordenadas que no midan 32 bytes o no caigan en la curva P-256.
// Es la ÚNICA fuente de parseo: la usa tanto el Validator (rechaza antes de
// persistir) como el Subscriber (instala) del applier edgeconfig.
func ParseJWKS(payload []byte) ([]jwkKey, error) {
	var set jwkSet
	if err := json.Unmarshal(payload, &set); err != nil {
		return nil, fmt.Errorf("jwks: JSON inválido: %w", err)
	}
	if len(set.Keys) == 0 {
		return nil, fmt.Errorf("jwks: el set no contiene llaves")
	}

	out := make([]jwkKey, 0, len(set.Keys))
	seen := make(map[string]struct{}, len(set.Keys))
	for i, k := range set.Keys {
		if k.Kty != "EC" {
			return nil, fmt.Errorf("jwks: llave %d: kty %q no soportado (se espera EC)", i, k.Kty)
		}
		if k.Crv != "P-256" {
			return nil, fmt.Errorf("jwks: llave %d: crv %q no soportado (se espera P-256)", i, k.Crv)
		}
		if k.Alg != "" && k.Alg != "ES256" {
			return nil, fmt.Errorf("jwks: llave %d: alg %q no soportado (se espera ES256)", i, k.Alg)
		}
		if k.Kid == "" {
			return nil, fmt.Errorf("jwks: llave %d: kid vacío", i)
		}
		if _, dup := seen[k.Kid]; dup {
			return nil, fmt.Errorf("jwks: kid duplicado %q", k.Kid)
		}
		pub, err := ecPublicKeyFromXY(k.X, k.Y)
		if err != nil {
			return nil, fmt.Errorf("jwks: llave %q (kid): %w", k.Kid, err)
		}
		seen[k.Kid] = struct{}{}
		out = append(out, jwkKey{Kid: k.Kid, Pub: pub})
	}
	return out, nil
}

// ecPublicKeyFromXY decodifica las coords base64url (32B c/u) a una *ecdsa.PublicKey
// P-256, validando que el punto caiga en la curva (vía crypto/ecdh, sin la API
// deprecada elliptic.IsOnCurve).
func ecPublicKeyFromXY(xB64, yB64 string) (*ecdsa.PublicKey, error) {
	xb, err := base64.RawURLEncoding.DecodeString(xB64)
	if err != nil {
		return nil, fmt.Errorf("coord x base64url inválida: %w", err)
	}
	yb, err := base64.RawURLEncoding.DecodeString(yB64)
	if err != nil {
		return nil, fmt.Errorf("coord y base64url inválida: %w", err)
	}
	if len(xb) != 32 || len(yb) != 32 {
		return nil, fmt.Errorf("coords deben medir 32 bytes (x=%d, y=%d)", len(xb), len(yb))
	}
	// Punto no comprimido 0x04||X||Y; NewPublicKey valida que caiga en la curva.
	uncompressed := make([]byte, 0, 65)
	uncompressed = append(uncompressed, 0x04)
	uncompressed = append(uncompressed, xb...)
	uncompressed = append(uncompressed, yb...)
	if _, err := ecdh.P256().NewPublicKey(uncompressed); err != nil {
		return nil, fmt.Errorf("punto fuera de la curva P-256: %w", err)
	}
	return &ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X:     new(big.Int).SetBytes(xb),
		Y:     new(big.Int).SetBytes(yb),
	}, nil
}

// KeyStore custodia las llaves públicas ES256 del emisor por kid y construye el
// MultiVerifier consultado por el Manager para validar access tokens offline. Lo
// alimenta el applier edgeconfig kind:"jwks" (InstallJWKS en el subscriber) y lo
// consulta el middleware (Verifier). Seguro para uso concurrente (los workers del
// demux instalan mientras el /v1 valida).
type KeyStore struct {
	issuer string

	mu sync.RWMutex
	mv *sharedauth.MultiVerifier // nil hasta que llega el primer JWKS válido
}

// NewKeyStore construye un KeyStore para el issuer esperado del IAM (el `iss` que
// firma la nube; ver JWT_ISSUER del cloud, default "wapp-cloud"). Arranca SIN
// llaves: Verifier() devuelve nil hasta el primer InstallJWKS (o el Bootstrap de
// la config persistida).
func NewKeyStore(issuer string) *KeyStore {
	return &KeyStore{issuer: issuer}
}

// InstallJWKS parsea el payload de ConfigUpdate kind:"jwks" e instala TODAS sus
// llaves por kid, reconstruyendo el MultiVerifier. Reemplaza el set anterior (la
// rotación entrega el set vigente completo). Un payload inválido devuelve error y
// CONSERVA el verificador previo (last-known-good).
func (k *KeyStore) InstallJWKS(payload []byte) error {
	keys, err := ParseJWKS(payload)
	if err != nil {
		return err
	}
	byKid := make(map[string]sharedauth.VerifierKey, len(keys))
	for _, kk := range keys {
		byKid[kk.Kid] = sharedauth.ES256VerifierKey(kk.Pub)
	}
	// def en cero: un access token SIN kid se rechaza (el IAM SIEMPRE estampa kid, ADR-0019).
	mv, err := sharedauth.NewMultiVerifier(k.issuer, byKid, sharedauth.VerifierKey{})
	if err != nil {
		return fmt.Errorf("jwks: no se pudo construir el MultiVerifier: %w", err)
	}
	k.mu.Lock()
	k.mv = mv
	k.mu.Unlock()
	return nil
}

// Verifier devuelve el MultiVerifier vigente, o nil si aún no se instaló ninguna
// llave (el Edge arrancó y el Cloud no ha empujado el JWKS todavía).
func (k *KeyStore) Verifier() *sharedauth.MultiVerifier {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.mv
}
