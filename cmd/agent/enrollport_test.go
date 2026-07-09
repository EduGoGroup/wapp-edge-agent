package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
)

// TestEnrollPortEnrolledSegunArchivos verifica la detección de "primera ejecución" (Plan 023 · T1):
// Enrolled() es false si falta cualquiera de cert/clave y true solo con AMBOS presentes.
func TestEnrollPortEnrolledSegunArchivos(t *testing.T) {
	dir := t.TempDir()
	cert := filepath.Join(dir, "edge.crt")
	key := filepath.Join(dir, "edge.key")
	p := enrollPort{cfg: config.Config{CloudLink: config.CloudLinkConfig{TLSCert: cert, TLSKey: key}}}

	if p.Enrolled() {
		t.Fatal("sin archivos: Enrolled debería ser false")
	}
	if err := os.WriteFile(cert, []byte("c"), 0o644); err != nil {
		t.Fatal(err)
	}
	if p.Enrolled() {
		t.Fatal("solo cert: Enrolled debería seguir false (falta la clave)")
	}
	if err := os.WriteFile(key, []byte("k"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !p.Enrolled() {
		t.Fatal("cert+clave presentes: Enrolled debería ser true")
	}
}

// TestEnrollPortEnrolledRutasVacias: sin rutas configuradas, Enrolled() es false (no crashea).
func TestEnrollPortEnrolledRutasVacias(t *testing.T) {
	p := enrollPort{cfg: config.Config{}}
	if p.Enrolled() {
		t.Fatal("rutas vacías: Enrolled debería ser false")
	}
}

// TestEnrollPortEnrollValidaBootstrap verifica que Enroll rechaza —SIN tocar la red— cuando falta alguna
// pieza de bootstrap pre-provista (endpoint / TLSCA / rutas destino), con un mensaje claro. El enroll
// real (enroll.Run) tiene su propia cobertura con Gateway mock; aquí solo validamos el adaptador.
func TestEnrollPortEnrollValidaBootstrap(t *testing.T) {
	base := config.CloudLinkConfig{
		EnrollmentEndpoint: "gw.example:9444",
		TLSCA:              "/bootstrap/ca.pem",
		TLSCert:            "/data/edge.crt",
		TLSKey:             "/data/edge.key",
	}
	cases := []struct {
		name    string
		mutate  func(*config.CloudLinkConfig)
		wantSub string
	}{
		{"sin endpoint", func(c *config.CloudLinkConfig) { c.EnrollmentEndpoint = "" }, "enrollment_endpoint"},
		{"sin tls_ca", func(c *config.CloudLinkConfig) { c.TLSCA = "" }, "tls_ca"},
		{"sin tls_cert", func(c *config.CloudLinkConfig) { c.TLSCert = "" }, "tls_cert/tls_key"},
		{"sin tls_key", func(c *config.CloudLinkConfig) { c.TLSKey = "" }, "tls_cert/tls_key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cl := base
			tc.mutate(&cl)
			p := enrollPort{cfg: config.Config{CloudLink: cl}}

			err := p.Enroll(context.Background(), "code-123")
			if err == nil {
				t.Fatalf("esperaba error de validación (%s), no nil", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("mensaje %q no contiene %q", err.Error(), tc.wantSub)
			}
		})
	}
}
