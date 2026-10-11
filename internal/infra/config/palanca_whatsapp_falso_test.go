package config

import "testing"

// palanca_whatsapp_falso_test.go — LA PALANCA DEL TRANSPORTE FALSO DE WHATSAPP (ADR-0049), en su origen.
//
// 🔴 QUÉ SE CUSTODIA. WAPP_AGENT_WHATSAPP_FALSO es la variable del Edge cuyo valor equivocado lo deja MUDO:
// con ella a true no abre socket con WhatsApp y no entrega ni recibe un solo mensaje real. Por eso se
// vigilan las dos cosas que el inyector vigila de la suya (palanca_inyector_test.go): que el default
// ESCRITO sea apagado, y que solo un booleano verdadero, con la clave exacta, la encienda.

func TestPalancaWhatsAppFalso_ElDefaultEsWhatsAppDeVerdad(t *testing.T) {
	if cargaLimpia(t).WhatsAppFalso {
		t.Fatal("con la variable SIN PONER el Edge arrancaría con el transporte FALSO de WhatsApp: ningún " +
			"Edge del ecosistema entregaría ni recibiría un mensaje, sin un solo error en el log")
	}
}

func TestPalancaWhatsAppFalso_SoloUnTrueExplicitoLaEcha(t *testing.T) {
	casos := []struct {
		nombre string
		clave  string
		valor  string
		falso  bool
	}{
		{"vacia (linea a medio editar tras terminar la prueba)", EnvPrefix + "WHATSAPP_FALSO", "", false},
		{"tecleada en castellano", EnvPrefix + "WHATSAPP_FALSO", "si", false},
		{"tecleada en ingles", EnvPrefix + "WHATSAPP_FALSO", "yes", false},
		{"numero que no es booleano", EnvPrefix + "WHATSAPP_FALSO", "2", false},
		{"nombre mal escrito", EnvPrefix + "WHATSAP_FALSO", "true", false},
		{"false explicito", EnvPrefix + "WHATSAPP_FALSO", "false", false},
		{"true explicito: la palanca SI se echa", EnvPrefix + "WHATSAPP_FALSO", "true", true},
		{"1 explicito (ParseBool lo acepta)", EnvPrefix + "WHATSAPP_FALSO", "1", true},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			t.Setenv(c.clave, c.valor)
			got := cargaLimpia(t).WhatsAppFalso
			if got == c.falso {
				return
			}
			if c.falso {
				t.Fatalf("%s=%q NO echó la palanca: la prueba con Edge real correría contra WhatsApp de verdad", c.clave, c.valor)
			}
			t.Fatalf("%s=%q ECHÓ la palanca: el Edge dejaría de hablar con WhatsApp por un valor que nadie "+
				"escribió con esa intención", c.clave, c.valor)
		})
	}
}

// Las dos palancas de prueba son INDEPENDIENTES: echar una no echa la otra.
func TestPalancaWhatsAppFalso_NoArrastraLaDelInyector(t *testing.T) {
	t.Setenv(EnvPrefix+"WHATSAPP_FALSO", "true")
	cfg := cargaLimpia(t)
	if !cfg.WhatsAppFalso {
		t.Fatal("la palanca no se echó con true")
	}
	if cfg.InyectorEntrantes {
		t.Error("echar el WhatsApp falso encendió también el inyector de entrantes: son dos palancas y cada " +
			"una grita lo suyo")
	}
}
