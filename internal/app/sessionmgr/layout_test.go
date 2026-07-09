package sessionmgr

import (
	"path/filepath"
	"testing"
)

// uuidA/uuidB son session_id válidos (UUID canónico) para los tests; opacos a propósito.
const (
	uuidA = "11111111-1111-4111-8111-111111111111"
	uuidB = "22222222-2222-4222-8222-222222222222"
)

func TestLayout_Paths(t *testing.T) {
	base := filepath.Join(t.TempDir(), "edge-data")
	l := NewLayout(base)

	if got := l.DataDir(); got != base {
		t.Fatalf("DataDir: got %q, want %q", got, base)
	}
	if got, want := l.SessionsRoot(), filepath.Join(base, "sessions"); got != want {
		t.Fatalf("SessionsRoot: got %q, want %q", got, want)
	}

	dir, err := l.SessionDir(uuidA)
	if err != nil {
		t.Fatalf("SessionDir(%q) error: %v", uuidA, err)
	}
	if want := filepath.Join(base, "sessions", uuidA); dir != want {
		t.Fatalf("SessionDir: got %q, want %q", dir, want)
	}

	store, err := l.StoreDB(uuidA)
	if err != nil {
		t.Fatalf("StoreDB error: %v", err)
	}
	if want := filepath.Join(base, "sessions", uuidA, "store.db"); store != want {
		t.Fatalf("StoreDB: got %q, want %q", store, want)
	}

	// La DEK vive DESACOPLADA del directorio de store (Plan 022 §3/§10.C): <data_dir>/keys/<id>.key,
	// ya NO en sessions/<id>/dek.key.
	dek, err := l.DEKPath(uuidA)
	if err != nil {
		t.Fatalf("DEKPath error: %v", err)
	}
	if want := filepath.Join(base, "keys", uuidA+".key"); dek != want {
		t.Fatalf("DEKPath: got %q, want %q", dek, want)
	}
}

// TestLayout_RejectsEscape comprueba que un id malicioso (con "..", separadores o vacío) NO produce
// rutas y por tanto no puede escapar de data_dir: todos los constructores devuelven error.
func TestLayout_RejectsEscape(t *testing.T) {
	l := NewLayout(filepath.Join(t.TempDir(), "edge-data"))

	bad := []string{
		"../x",
		"../../etc/passwd",
		"a/b",
		"..",
		".",
		"",
		"not-a-uuid",
		"11111111-1111-4111-8111-11111111111", // 31 hex: longitud inválida
	}
	for _, id := range bad {
		if _, err := l.SessionDir(id); err == nil {
			t.Errorf("SessionDir(%q): se esperaba error (id inválido), no lo hubo", id)
		}
		if _, err := l.StoreDB(id); err == nil {
			t.Errorf("StoreDB(%q): se esperaba error (id inválido), no lo hubo", id)
		}
		if _, err := l.DEKPath(id); err == nil {
			t.Errorf("DEKPath(%q): se esperaba error (id inválido), no lo hubo", id)
		}
	}
}
