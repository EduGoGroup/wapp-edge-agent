package domain

import "testing"

// TestSelfPNFromJID cubre la derivación del NÚMERO PROPIO (E.164 sin '+') del JID del device propio
// (implementación única, Plan 027 T5; antes vivía en cloudlink/identity_test.go): solo un JID de número
// (s.whatsapp.net) puebla el número; se normaliza a la user-part (sin device/agente); un JID vacío
// (sesión sin emparejar) o un LID devuelven "" (nunca se reporta un LID como número).
func TestSelfPNFromJID(t *testing.T) {
	tests := []struct {
		name string
		jid  string
		want string
	}{
		{name: "numero propio simple", jid: "593999@s.whatsapp.net", want: "593999"},
		{name: "numero propio con device se normaliza", jid: "593999:12@s.whatsapp.net", want: "593999"},
		{name: "sesion sin emparejar (JID vacio)", jid: "", want: ""},
		{name: "LID no se reporta como numero", jid: "10001@lid", want: ""},
		{name: "server desconocido no puebla", jid: "12345-67890@g.us", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SelfPNFromJID(tt.jid); got != tt.want {
				t.Fatalf("SelfPNFromJID(%q) = %q, quería %q", tt.jid, got, tt.want)
			}
		})
	}
}
