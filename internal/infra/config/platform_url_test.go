package config

import "testing"

func TestPlatformAPIBaseURLInsecure(t *testing.T) {
	cases := []struct {
		name string
		url  string
		want bool
	}{
		{"http loopback localhost", "http://localhost:8103", false},
		{"http loopback 127.0.0.1", "http://127.0.0.1:8103", false},
		{"http loopback IPv6 ::1", "http://[::1]:8103", false},
		{"http NO loopback host", "http://cloud.wapp.example:8103", true},
		{"http NO loopback IP publica", "http://203.0.113.10:8103", true},
		{"https NO loopback (siempre permitido)", "https://cloud.wapp.example", false},
		{"https loopback (siempre permitido)", "https://localhost:8103", false},
		{"URL malformada, escape invalido (fail-closed)", "http://%zz", true},
		{"URL sin esquema (fail-closed)", "://bad-url", true},
		{"vacio (fail-closed)", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PlatformAPIBaseURLInsecure(tc.url)
			if got != tc.want {
				t.Fatalf("PlatformAPIBaseURLInsecure(%q) = %v; quería %v", tc.url, got, tc.want)
			}
		})
	}
}
