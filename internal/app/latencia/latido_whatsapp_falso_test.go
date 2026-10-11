package latencia

import (
	"strings"
	"testing"
	"time"
)

// latido_whatsapp_falso_test.go — EL CAMPO `whatsapp` DEL BLOQUE (ADR-0049, RW.3.a). Mismo molde que
// latido_inyector_test.go: el aviso del arranque se pierde en el scrollback, la línea del latido no. Con el
// transporte falso este Edge no entrega ni recibe un mensaje real, y cada línea tiene que decirlo.

func depsConWhatsApp(log *logCaptura, falso bool) Deps {
	d := depsConInyector(log, false)
	d.WhatsAppFalso = falso
	return d
}

func TestLatido_ConWhatsAppDeVerdadElCampoDiceReal(t *testing.T) {
	log := &logCaptura{}
	correrLatido(t, depsConWhatsApp(log, false), 30*time.Millisecond)

	emisiones := log.latidos()
	if len(emisiones) == 0 {
		t.Fatal("el latido no emitió nada: sin emisiones este test pasaría en verde por vacío")
	}
	for _, e := range emisiones {
		v, ok := e.clave("whatsapp")
		if !ok {
			t.Fatal("el bloque salió SIN el campo `whatsapp`: una ausencia no contesta si este Edge habla con WhatsApp")
		}
		if v != whatsAppReal {
			t.Errorf("con el transporte real el campo debe decir %q y dijo %v", whatsAppReal, v)
		}
	}
}

func TestLatido_ConElWhatsAppFALSOCadaLineaLoDice(t *testing.T) {
	log := &logCaptura{}
	correrLatido(t, depsConWhatsApp(log, true), 30*time.Millisecond)

	emisiones := log.latidos()
	if len(emisiones) < 2 {
		t.Fatalf("hacen falta al menos una emisión periódica y la final: got %d", len(emisiones))
	}
	for _, e := range emisiones {
		v, _ := e.clave("whatsapp")
		texto, ok := v.(string)
		if !ok {
			t.Fatalf("el campo `whatsapp` no es una cadena: got %T", v)
		}
		if !strings.Contains(texto, "FALSO") {
			t.Errorf("con la palanca echada el campo no dice FALSO: got %q", texto)
		}
		if !strings.Contains(texto, "WAPP_AGENT_WHATSAPP_FALSO") {
			t.Errorf("la línea no nombra la variable que hay que bajar: got %q", texto)
		}
		if !strings.Contains(texto, "NO habla con WhatsApp") {
			t.Errorf("la línea no dice lo que significa: que este Edge no habla con WhatsApp: got %q", texto)
		}
		// Las palancas no se pisan: el inyector sigue diciendo lo suyo.
		if iny, _ := e.clave("inyector"); iny != inyectorInactivo {
			t.Errorf("echar el WhatsApp falso cambió el campo `inyector`: got %v", iny)
		}
	}
}
