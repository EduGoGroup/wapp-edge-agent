package cloudlink

import "testing"

// TestDeriveContactRefs cubre la derivación número/LID del remitente entrante (Plan 010 §9): la
// clasificación es por SERVER del JID (no por orden), se normaliza a la user-part, y se tolera un
// SenderAlt vacío (mapeo aún no aprendido, §10.H) subiendo solo lo conocido.
func TestDeriveContactRefs(t *testing.T) {
	tests := []struct {
		name            string
		sender          string
		senderAlt       string
		wantPN, wantLID string
	}{
		{
			name:      "sender numero, alt LID (AddressingMode pn)",
			sender:    "593999@s.whatsapp.net",
			senderAlt: "10001@lid",
			wantPN:    "593999",
			wantLID:   "10001",
		},
		{
			name:      "sender LID, alt numero (AddressingMode lid)",
			sender:    "10001@lid",
			senderAlt: "593999@s.whatsapp.net",
			wantPN:    "593999",
			wantLID:   "10001",
		},
		{
			name:      "alt vacio: solo numero conocido (tolerancia)",
			sender:    "593999@s.whatsapp.net",
			senderAlt: "",
			wantPN:    "593999",
			wantLID:   "",
		},
		{
			name:      "alt vacio: solo LID conocido (tolerancia)",
			sender:    "10001@lid",
			senderAlt: "",
			wantPN:    "",
			wantLID:   "10001",
		},
		{
			name:      "JID con device se normaliza a la user-part",
			sender:    "593999:12@s.whatsapp.net",
			senderAlt: "10001@lid",
			wantPN:    "593999",
			wantLID:   "10001",
		},
		{
			name:      "server desconocido (grupo) no puebla identidad",
			sender:    "12345-67890@g.us",
			senderAlt: "",
			wantPN:    "",
			wantLID:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPN, gotLID := deriveContactRefs(tt.sender, tt.senderAlt)
			if gotPN != tt.wantPN || gotLID != tt.wantLID {
				t.Fatalf("deriveContactRefs(%q,%q) = (%q,%q), quería (%q,%q)",
					tt.sender, tt.senderAlt, gotPN, gotLID, tt.wantPN, tt.wantLID)
			}
		})
	}
}
