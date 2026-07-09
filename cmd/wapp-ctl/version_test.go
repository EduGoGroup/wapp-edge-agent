package main

import (
	"strings"
	"testing"
)

// Tests de CONTRATO de la variable Version del supervisor (Plan 023 · T0).
// Mismo contrato que el núcleo: Version debe ser una variable de string
// asignable (inyectable por ldflags) y no vacía, ya que aparece en el log de
// arranque de wapp-ctl.

// TestVersionNoVacia blinda que el fallback de dev nunca queda en blanco.
func TestVersionNoVacia(t *testing.T) {
	if strings.TrimSpace(Version) == "" {
		t.Fatal("Version no debe estar vacía: aparece en el log de arranque de wapp-ctl")
	}
	if strings.ContainsAny(Version, " \t\n") {
		t.Fatalf("Version no debe contener espacios (viene de `git describe`): %q", Version)
	}
}

// TestVersionEsInyectable garantiza que Version sigue siendo `var` (no `const`):
// ldflags -X main.Version solo sobre-escribe variables. La asignación de abajo
// NO compilaría si Version volviera a ser const.
func TestVersionEsInyectable(t *testing.T) {
	orig := Version
	t.Cleanup(func() { Version = orig })

	Version = "v9.9.9-test"
	if Version != "v9.9.9-test" {
		t.Fatalf("Version debería ser asignable (var, no const); got %q", Version)
	}
}
