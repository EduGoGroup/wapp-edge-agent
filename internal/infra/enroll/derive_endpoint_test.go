package enroll

import "testing"

// TestDeriveRuntimeEndpoint cubre la derivación del endpoint de runtime a partir del endpoint de
// enrolamiento (Plan 026 T3): se conserva el HOST del enrollment_endpoint y se le pega el puerto de
// runtime, descartando el puerto de enroll. Soporta host con/sin puerto e IPv6.
func TestDeriveRuntimeEndpoint(t *testing.T) {
	cases := []struct {
		name       string
		enrollment string
		port       string
		want       string
		wantErr    bool
	}{
		{"host:puerto enroll -> host:runtime", "gateway.tudominio.com:8102", "8101", "gateway.tudominio.com:8101", false},
		{"host sin puerto -> host:runtime", "gateway.tudominio.com", "8101", "gateway.tudominio.com:8101", false},
		{"localhost dev", "localhost:8102", "8101", "localhost:8101", false},
		{"puerto runtime distinto", "gateway.tudominio.com:8102", "9443", "gateway.tudominio.com:9443", false},
		{"ipv4 con puerto", "10.0.0.5:8102", "8101", "10.0.0.5:8101", false},
		{"ipv6 con puerto", "[2001:db8::1]:8102", "8101", "[2001:db8::1]:8101", false},
		{"enrollment vacío", "", "8101", "", true},
		{"runtime port vacío", "gateway.tudominio.com:8102", "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := deriveRuntimeEndpoint(tc.enrollment, tc.port)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("deriveRuntimeEndpoint(%q,%q) = %q, se esperaba error", tc.enrollment, tc.port, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("deriveRuntimeEndpoint(%q,%q) error inesperado: %v", tc.enrollment, tc.port, err)
			}
			if got != tc.want {
				t.Fatalf("deriveRuntimeEndpoint(%q,%q) = %q, want %q", tc.enrollment, tc.port, got, tc.want)
			}
		})
	}
}
