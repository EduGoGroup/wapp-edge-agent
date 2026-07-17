package auth

import (
	"errors"
	"strings"
	"testing"

	sharedauth "github.com/EduGoGroup/wapp-shared/auth"
)

func TestParseJWKS_Valid(t *testing.T) {
	key := newES256Key(t)
	keys, err := ParseJWKS(jwksJSON(t, "es256-1", &key.PublicKey))
	if err != nil {
		t.Fatalf("ParseJWKS válido: %v", err)
	}
	if len(keys) != 1 || keys[0].Kid != "es256-1" || keys[0].Pub == nil {
		t.Fatalf("resultado inesperado: %+v", keys)
	}
}

func TestParseJWKS_Invalid(t *testing.T) {
	cases := map[string]string{
		"json roto":    `{`,
		"set vacío":    `{"keys":[]}`,
		"kty no EC":    `{"keys":[{"kty":"RSA","crv":"P-256","kid":"a","x":"AA","y":"AA"}]}`,
		"crv no P-256": `{"keys":[{"kty":"EC","crv":"P-384","kid":"a","x":"AA","y":"AA"}]}`,
		"kid vacío":    `{"keys":[{"kty":"EC","crv":"P-256","kid":"","x":"AA","y":"AA"}]}`,
		"coord corta":  `{"keys":[{"kty":"EC","crv":"P-256","kid":"a","x":"AA","y":"AA"}]}`,
		"alg no ES256": `{"keys":[{"kty":"EC","crv":"P-256","alg":"ES384","kid":"a","x":"AA","y":"AA"}]}`,
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseJWKS([]byte(payload)); err == nil {
				t.Fatalf("se esperaba error para %q", name)
			}
		})
	}
}

func TestParseJWKS_DuplicateKid(t *testing.T) {
	k1 := newES256Key(t)
	k2 := newES256Key(t)
	// Dos entradas con el mismo kid.
	payload := `{"keys":[` +
		strings.TrimPrefix(strings.TrimSuffix(string(jwksJSON(t, "dup", &k1.PublicKey)), "}"), `{"keys":[`) + `,` +
		strings.TrimPrefix(strings.TrimSuffix(string(jwksJSON(t, "dup", &k2.PublicKey)), "]}"), `{"keys":[`) + `}`
	if _, err := ParseJWKS([]byte(payload)); err == nil {
		t.Fatalf("se esperaba error por kid duplicado")
	}
}

func TestKeyStore_InstallAndVerify(t *testing.T) {
	key := newES256Key(t)
	ks := NewKeyStore(testIssuer)

	if ks.Verifier() != nil {
		t.Fatalf("Verifier debe ser nil antes de instalar ningún JWKS")
	}

	if err := ks.InstallJWKS(jwksJSON(t, "es256-1", &key.PublicKey)); err != nil {
		t.Fatalf("InstallJWKS: %v", err)
	}
	mv := ks.Verifier()
	if mv == nil {
		t.Fatalf("Verifier debe existir tras instalar el JWKS")
	}

	// Un token firmado con la clave instalada valida; su kid selecciona la entrada.
	tok := mintES256(t, key, "es256-1", "t1", sharedauth.Grants{Allow: []string{"edge.*"}}, timeFuture())
	if _, err := mv.ValidateToken(tok); err != nil {
		t.Fatalf("validar token con la llave instalada: %v", err)
	}

	// Un token de OTRA clave con el mismo kid se rechaza (firma no coincide).
	other := newES256Key(t)
	bad := mintES256(t, other, "es256-1", "t1", sharedauth.Grants{Allow: []string{"edge.*"}}, timeFuture())
	if _, err := mv.ValidateToken(bad); err == nil {
		t.Fatalf("un token firmado con otra clave debe rechazarse")
	}
}

func TestKeyStore_InstallInvalidKeepsPrevious(t *testing.T) {
	key := newES256Key(t)
	ks := NewKeyStore(testIssuer)
	if err := ks.InstallJWKS(jwksJSON(t, "es256-1", &key.PublicKey)); err != nil {
		t.Fatalf("install inicial: %v", err)
	}
	before := ks.Verifier()

	if err := ks.InstallJWKS([]byte(`{"keys":[]}`)); err == nil {
		t.Fatalf("un JWKS inválido debe devolver error")
	}
	if ks.Verifier() != before {
		t.Fatalf("un JWKS inválido debe conservar el verificador previo (last-known-good)")
	}
}

func TestKeyStore_UnknownKidRejected(t *testing.T) {
	key := newES256Key(t)
	ks := NewKeyStore(testIssuer)
	_ = ks.InstallJWKS(jwksJSON(t, "es256-1", &key.PublicKey))
	// Token con kid desconocido.
	tok := mintES256(t, key, "otro-kid", "t1", sharedauth.Grants{Allow: []string{"edge.*"}}, timeFuture())
	_, err := ks.Verifier().ValidateToken(tok)
	if err == nil || !errors.Is(err, sharedauth.ErrInvalidToken) {
		t.Fatalf("un kid desconocido debe rechazarse con ErrInvalidToken, got: %v", err)
	}
}
