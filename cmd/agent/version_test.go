package main

import (
	"strings"
	"testing"
)

// Tests de CONTRATO de la variable Version (Plan 023 · T0). No prueban el valor
// concreto inyectado por ldflags (eso depende de git describe en tiempo de
// build), sino el contrato que el Makefile y los consumidores (/v1/health, logs)
// dan por sentado: que Version sea una variable de string asignable y no vacía.

// TestVersionNoVacia blinda que el fallback de dev nunca queda en blanco: una
// Version vacía dejaría /v1/health y el log de arranque sin identificar la build.
func TestVersionNoVacia(t *testing.T) {
	if strings.TrimSpace(Version) == "" {
		t.Fatal("Version no debe estar vacía: rompe /v1/health y el log de arranque")
	}
	if strings.ContainsAny(Version, " \t\n") {
		t.Fatalf("Version no debe contener espacios (viene de `git describe`): %q", Version)
	}
}

// TestVersionEsInyectable garantiza que Version sigue siendo `var` (no `const`):
// ldflags -X main.Version solo sobre-escribe VARIABLES de string. La asignación
// de abajo NO compilaría si alguien revirtiera Version a const, así que este test
// falla en tiempo de compilación ante esa regresión.
func TestVersionEsInyectable(t *testing.T) {
	orig := Version
	t.Cleanup(func() { Version = orig })

	Version = "v9.9.9-test"
	if Version != "v9.9.9-test" {
		t.Fatalf("Version debería ser asignable (var, no const); got %q", Version)
	}
}
