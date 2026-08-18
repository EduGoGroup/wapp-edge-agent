package main

// main_test.go — LA PRUEBA DE QUE LAS FILAS SINTÉTICAS SIRVEN.
//
// 🔴 QUÉ SE CUSTODIA AQUÍ, y por qué sin esto la herramienta no vale nada. `colaseed` escribe en la cola
// filas CIFRADAS con la DEK de una sesión, y quien las abre después es OTRO PROCESO (el worker-cajero).
// Si el sellado no fuera exactamente el mismo que el del listener, la siembra "funcionaría" —Enqueue
// devuelve nil, las filas están en la tabla, el resumen dice 60 filas— y el fallo aparecería LEJOS: un
// `Reclamar` que revienta al descifrar, en mitad de una medición de carga, con la pinta de un bug del
// cajero. Y diagnosticarlo cuesta caro a propósito, porque INV-051.1 prohíbe que el texto o la meta salgan
// a un log ni truncados.
//
// Por eso el test NO comprueba «hay N filas»: lee lo sembrado POR EL MISMO CAMINO QUE EL CAJERO
// (`app.ColaCajero.Reclamar`, que descifra con la DEK de la sesión) y exige que el texto vuelva íntegro.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO. Las siete se EJECUTARON una a una, no se dedujeron:
//
//  1. `Estado: app.EstadoClasificado` en vez de `EstadoNuevo` ⇒ «Reclamar #1 devolvió (nil, nil)».
//  2. Un único `chat_jid` para todas las conversaciones ⇒ «conversaciones distintas en disco: got 1, want 3».
//  3. `pasoDeFrase = 16` (múltiplo de len(frasesDePedido)) ⇒ «turno 1: las 4 conversaciones solo dijeron 1
//     frases distintas».
//  4. Encolar siempre en bloque, ignorando `intercalar` ⇒ rojo en TestSembrar_IntercalaLasConversaciones.
//  5. Silenciar el aviso de discrepancia del resumen ⇒ rojo en TestImprimirResumen_DiceLaDiscrepancia.
//  6. `if o.pausa >= 0` (nunca duerme) ⇒ «la siembra con -pausa=40ms tardó 6.5ms».
//  7. Leer con la MISMA DEK en el caso de la llave distinta ⇒ rojo, o sea que ese caso mide de verdad la
//     sensibilidad a la llave y no otra cosa.
//
// 🔴 Y LA QUE NO SE PONÍA ROJA, que es la que justifica haberlas corrido: la (3) pasaba VERDE en la primera
// escritura de este fichero. El aserto miraba que cada conversación recorriera el corpus entero —cierto
// también con paso 16— en vez de mirar lo que de verdad importa: que en un mismo turno las N
// conversaciones digan cosas DISTINTAS. El test decía en su comentario que cazaba esa mutación y no la
// cazaba. El aserto del turno (b) se añadió por eso.
//
// LA CUSTODIA ES DE MENTIRA A PROPÓSITO: la real es el Keychain en macOS y Secret Service en Linux, y una
// suite que las tocara pediría permisos en la máquina de quien la corre (o un D-Bus en el CI). El punto de
// inyección es el mismo parámetro `custodyFor` que ya tiene wiring.BuildCola, así que lo que se prueba es
// el cableado de verdad; lo único simulado es de dónde salen los 32 bytes.

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/wiring"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"github.com/google/uuid"
)

// custodiaFija entrega SIEMPRE la misma DEK de 32 bytes. `Exists` devuelve true porque `sembrar` comprueba
// la existencia de la DEK antes de abrir nada (y esa comprobación también es parte de lo que se prueba: con
// un false, la siembra debe abortar en vez de insertar cero filas con N errores).
type custodiaFija struct{ dek []byte }

func (c custodiaFija) Store([]byte) error    { return nil }
func (c custodiaFija) Load() ([]byte, error) { return c.dek, nil }
func (c custodiaFija) Exists() bool          { return true }

// dekDe fabrica una DEK determinista de 32 bytes rellena con el byte dado. Dos `dekDe` distintas son dos
// llaves distintas, que es lo que necesita el caso de la llave equivocada.
func dekDe(b byte) []byte {
	d := make([]byte, 32)
	for i := range d {
		d[i] = b
	}
	return d
}

// custodiaCon devuelve la factory que `sembrar` y `BuildCola` esperan.
func custodiaCon(dek []byte) func(string) app.KeyCustody {
	return func(string) app.KeyCustody { return custodiaFija{dek: dek} }
}

// logDeTest manda las trazas de BuildCola al buffer del test en vez de a stdout.
func logDeTest(t *testing.T) sharedlogger.Logger {
	t.Helper()
	return sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{}))
}

// opcionesDeTest son unas opciones válidas y deterministas (lote fijo: el aleatorio de producción haría
// irreproducible el patrón LIKE que el test comprueba).
func opcionesDeTest(dataDir, sessionID string, conversaciones, mensajes int) opciones {
	return opciones{
		dataDir:        dataDir,
		sessionID:      sessionID,
		conversaciones: conversaciones,
		mensajes:       mensajes,
		prefijoJID:     "colaseed",
		lote:           "deadbeef",
		intercalar:     true,
		maxFilas:       config.DefaultColaMaxRows,
	}
}

// abrirComoElCajero reproduce EXACTAMENTE lo que hace `agent cajero` para llegar a la cola: OpenCola sobre
// la ruta del layout + BuildCola con la custodia + la aserción a app.ColaCajero. Es el camino cuyo éxito
// prueba que el cifrado de la siembra es el correcto.
func abrirComoElCajero(t *testing.T, ctx context.Context, dataDir string, dek []byte) app.ColaCajero {
	t.Helper()
	layout := sessionmgr.NewLayout(dataDir)
	colaDB, err := db.OpenCola(ctx, layout.ColaDB())
	if err != nil {
		t.Fatalf("OpenCola: %v", err)
	}
	t.Cleanup(func() { _ = colaDB.Close() })

	cola := wiring.BuildCola(ctx, config.Config{}, colaDB, layout, custodiaCon(dek), logDeTest(t))
	if cola == nil {
		t.Fatal("BuildCola devolvió nil sobre la cola recién sembrada")
	}
	cajero, ok := cola.(app.ColaCajero)
	if !ok {
		t.Fatalf("la cola construida (%T) no implementa app.ColaCajero: el cajero no podría reclamar nada", cola)
	}
	return cajero
}

// TestSembrar_LoSembradoLoLeeElCajero es la prueba principal: siembra 3 conversaciones × 2 mensajes y las
// vuelve a sacar por el camino del cajero, comprobando a la vez el cifrado, el estado inicial y que de
// verdad son conversaciones DISTINTAS.
func TestSembrar_LoSembradoLoLeeElCajero(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	sessionID := uuid.NewString()
	dek := dekDe(0x11)

	const conversaciones, mensajes = 3, 2
	o := opcionesDeTest(dataDir, sessionID, conversaciones, mensajes)

	res, err := sembrar(ctx, o, custodiaCon(dek), logDeTest(t))
	if err != nil {
		t.Fatalf("sembrar: %v", err)
	}

	// (1) El resumen no es un adorno: `filas` sale de RELEER la tabla, así que si el Enqueue se hubiera
	// tragado duplicados en silencio, este número lo delataría.
	if res.filas != conversaciones*mensajes {
		t.Errorf("filas en disco: got %d, want %d (encoladas=%d)", res.filas, conversaciones*mensajes, res.encoladas)
	}
	if res.encoladas != res.filas {
		t.Errorf("se encolaron %d y en la tabla hay %d: hubo descartes silenciosos", res.encoladas, res.filas)
	}
	if res.conversaciones != conversaciones {
		t.Errorf("conversaciones distintas en disco: got %d, want %d", res.conversaciones, conversaciones)
	}
	// La cola nace vacía en un t.TempDir(), así que la secuencia empieza en 1 y no deja huecos.
	if res.seqMin != 1 || res.seqMax != int64(conversaciones*mensajes) {
		t.Errorf("rango de seq: got %d..%d, want 1..%d", res.seqMin, res.seqMax, conversaciones*mensajes)
	}

	// (2) EL CAMINO DEL CAJERO. Reclamar solo se lleva filas `nuevo`, así que si la siembra las hubiera
	// hecho nacer `clasificado` (con una marca de omisión) esto devolvería (nil, nil) desde el primer
	// intento. Y descifra con la DEK de la sesión: un sellado con otra llave sale por `err`.
	cajero := abrirComoElCajero(t, ctx, dataDir, dek)

	chats := map[string]int{}
	for i := 0; i < conversaciones; i++ {
		lote, err := cajero.Reclamar(ctx, 0)
		if err != nil {
			t.Fatalf("Reclamar #%d: %v (un fallo aquí es, casi siempre, que la siembra selló con otra llave)", i+1, err)
		}
		if lote == nil {
			t.Fatalf("Reclamar #%d devolvió (nil, nil): faltan lotes por reclamar. "+
				"El caso típico es que las filas no nacieran en estado %q", i+1, app.EstadoNuevo)
		}
		if lote.SessionID != sessionID {
			t.Errorf("lote #%d: session_id got %q, want %q", i+1, lote.SessionID, sessionID)
		}
		if len(lote.Mensajes) != mensajes {
			t.Errorf("lote #%d (%s): got %d mensajes, want %d", i+1, lote.ChatJID, len(lote.Mensajes), mensajes)
		}
		chats[lote.ChatJID] += len(lote.Mensajes)

		for _, m := range lote.Mensajes {
			// EL TEXTO VUELVE ÍNTEGRO: no basta con que no haya error, tiene que ser una de las frases del
			// corpus. Un descifrado que devolviera bytes sueltos sin fallar el GCM (que no puede, pero el
			// test no depende de esa promesa) se vería aquí.
			if !esFraseDelCorpus(m.Texto) {
				t.Errorf("lote #%d, seq %d: el texto descifrado no es ninguna frase del corpus: %q",
					i+1, m.Seq, m.Texto)
			}
			// La META también viaja cifrada y también tiene que abrirse — y con las claves JSON del PUERTO
			// (app.ColaMeta), que es lo que lee el despachador. Un JSON con otras claves no falla el
			// Unmarshal: deja los campos vacíos y el mensaje sale al cable sin remitente.
			meta, err := app.DecodeColaMeta(m.Meta)
			if err != nil {
				t.Errorf("lote #%d, seq %d: la meta no se pudo decodificar: %v", i+1, m.Seq, err)
				continue
			}
			if meta.Sender != lote.ChatJID {
				t.Errorf("lote #%d, seq %d: meta.sender got %q, want %q (chat 1:1: el remitente es el chat)",
					i+1, m.Seq, meta.Sender, lote.ChatJID)
			}
			if meta.PushName == "" || meta.Type != "text" || meta.IsGroup {
				t.Errorf("lote #%d, seq %d: meta con las claves del puerto mal rellenada: %+v", i+1, m.Seq, meta)
			}
			if !strings.HasPrefix(m.WAMessageID, "COLASEED-"+o.lote+"-") {
				t.Errorf("lote #%d, seq %d: wa_message_id %q no lleva la marca de la corrida",
					i+1, m.Seq, m.WAMessageID)
			}
		}
	}

	// (3) N CONVERSACIONES DISTINTAS, que es la mitad de la razón de ser de la herramienta: sin esto no se
	// puede ejercitar el semáforo ni el orden con conversaciones cruzadas, y en campo exigiría N humanos.
	if len(chats) != conversaciones {
		t.Errorf("conversaciones distintas reclamadas: got %d (%v), want %d", len(chats), chats, conversaciones)
	}
	for jid, n := range chats {
		if n != mensajes {
			t.Errorf("la conversación %s trajo %d mensajes, want %d", jid, n, mensajes)
		}
		if !strings.HasSuffix(jid, sufijoJID) || !strings.HasPrefix(jid, "colaseed-"+o.lote+"-") {
			t.Errorf("chat_jid %q no sigue el patrón reconocible/borrable %q", jid, o.patronLike())
		}
	}

	// (4) No queda nada más: se sembró lo que se pidió y ni una fila de propina.
	if sobra, err := cajero.Reclamar(ctx, 0); err != nil {
		t.Fatalf("Reclamar final: %v", err)
	} else if sobra != nil {
		t.Errorf("quedaban filas por reclamar tras %d lotes: %s con %d mensajes",
			conversaciones, sobra.ChatJID, len(sobra.Mensajes))
	}
}

// TestSembrar_LlaveDistintaRompeLaLectura es LA RED CONTRA EL FALSO VERDE del test de arriba: si el
// aserto del cifrado no estuviera mirando de verdad, este caso —leer con OTRA DEK— también pasaría.
// Aquí se exige que FALLE, o sea que el camino del cajero es sensible a la llave y que el verde del test
// principal significa algo.
func TestSembrar_LlaveDistintaRompeLaLectura(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	sessionID := uuid.NewString()

	o := opcionesDeTest(dataDir, sessionID, 1, 1)
	if _, err := sembrar(ctx, o, custodiaCon(dekDe(0x11)), logDeTest(t)); err != nil {
		t.Fatalf("sembrar: %v", err)
	}

	cajero := abrirComoElCajero(t, ctx, dataDir, dekDe(0x22)) // ← otra llave
	lote, err := cajero.Reclamar(ctx, 0)
	// La red DE LA RED: si no hubiera fila que reclamar, Reclamar devolvería (nil, nil) y el `err == nil`
	// de abajo también fallaría… pero por el motivo equivocado, y este caso no habría probado nada sobre
	// el cifrado. Se separan los dos diagnósticos.
	if err == nil && lote == nil {
		t.Fatal("no había nada que reclamar: este caso no llegó a ejercitar el descifrado " +
			"(¿la siembra dejó de insertar, o las filas ya no nacen `nuevo`?)")
	}
	if err == nil {
		t.Fatalf("Reclamar con la DEK equivocada NO falló (devolvió el lote %s con %d mensajes): entonces "+
			"el verde del test principal no prueba que la siembra cifre con la llave de la sesión",
			lote.ChatJID, len(lote.Mensajes))
	}
}

// TestSembrar_IntercalaLasConversaciones comprueba el parámetro que hace útil a la herramienta para la
// Ola 5: con `intercalar`, los `seq` consecutivos pertenecen a conversaciones DISTINTAS. Ese es el
// escenario que el semáforo de un hueco y el orden por conversación tienen que sobrevivir; sembrar cada
// conversación en bloque es el caso fácil y no prueba nada de eso.
func TestSembrar_IntercalaLasConversaciones(t *testing.T) {
	ctx := context.Background()
	const conversaciones, mensajes = 3, 3

	casos := []struct {
		nombre     string
		intercalar bool
		// quiereCambios es cuántas veces debe cambiar el chat_jid al recorrer la tabla por seq: con
		// intercalado cambia en CADA paso (8 de 8); en bloque, solo al saltar de conversación (2).
		quiereCambios int
	}{
		{"intercalado", true, conversaciones*mensajes - 1},
		{"en bloque", false, conversaciones - 1},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			dataDir := t.TempDir()
			sessionID := uuid.NewString()
			o := opcionesDeTest(dataDir, sessionID, conversaciones, mensajes)
			o.intercalar = c.intercalar

			if _, err := sembrar(ctx, o, custodiaCon(dekDe(0x33)), logDeTest(t)); err != nil {
				t.Fatalf("sembrar: %v", err)
			}

			layout := sessionmgr.NewLayout(dataDir)
			colaDB, err := db.OpenCola(ctx, layout.ColaDB())
			if err != nil {
				t.Fatalf("OpenCola: %v", err)
			}
			defer func() { _ = colaDB.Close() }()

			// `chat_jid` va EN CLARO en disco (es la clave de enrutado, no contenido): se puede leer sin DEK.
			rows, err := colaDB.QueryContext(ctx, `SELECT chat_jid FROM cola_entrantes ORDER BY seq`)
			if err != nil {
				t.Fatalf("leer el orden: %v", err)
			}
			defer func() { _ = rows.Close() }()

			var anterior string
			cambios := 0
			total := 0
			for rows.Next() {
				var jid string
				if err := rows.Scan(&jid); err != nil {
					t.Fatalf("scan: %v", err)
				}
				if total > 0 && jid != anterior {
					cambios++
				}
				anterior = jid
				total++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("recorrer: %v", err)
			}
			if total != conversaciones*mensajes {
				t.Fatalf("filas leídas: got %d, want %d", total, conversaciones*mensajes)
			}
			if cambios != c.quiereCambios {
				t.Errorf("cambios de conversación al recorrer por seq: got %d, want %d", cambios, c.quiereCambios)
			}
		})
	}
}

// TestSembrar_RechazaLoQueHariaDanio cubre las validaciones que no son cosmética: un `%` en el prefijo o en
// el lote convierte el DELETE que la herramienta imprime en el resumen en una escoba capaz de barrer filas
// REALES, y una DEK ausente tiene que abortar ANTES de insertar (si no, el caché negativo del Store devuelve
// el mismo error 60 s y el operador lee el último en vez de la causa).
func TestSembrar_RechazaLoQueHariaDanio(t *testing.T) {
	ctx := context.Background()
	base := func(t *testing.T) opciones { return opcionesDeTest(t.TempDir(), uuid.NewString(), 1, 1) }

	casos := []struct {
		nombre   string
		retocar  func(*opciones)
		custodia func(string) app.KeyCustody
	}{
		{"sin data-dir", func(o *opciones) { o.dataDir = "" }, custodiaCon(dekDe(1))},
		{"sin session-id", func(o *opciones) { o.sessionID = "" }, custodiaCon(dekDe(1))},
		{"session-id que no es UUID", func(o *opciones) { o.sessionID = "../../etc" }, custodiaCon(dekDe(1))},
		{"cero conversaciones", func(o *opciones) { o.conversaciones = 0 }, custodiaCon(dekDe(1))},
		{"cero mensajes", func(o *opciones) { o.mensajes = 0 }, custodiaCon(dekDe(1))},
		{"comodín LIKE en el prefijo", func(o *opciones) { o.prefijoJID = "cola%" }, custodiaCon(dekDe(1))},
		{"comodín LIKE en el lote", func(o *opciones) { o.lote = "dead_beef" }, custodiaCon(dekDe(1))},
		{"lote vacío", func(o *opciones) { o.lote = "" }, custodiaCon(dekDe(1))},
		{"sin DEK custodiada", func(*opciones) {}, func(string) app.KeyCustody { return custodiaAusente{} }},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			o := base(t)
			c.retocar(&o)
			if _, err := sembrar(ctx, o, c.custodia, logDeTest(t)); err == nil {
				t.Fatal("sembrar aceptó una configuración que debía rechazar")
			}
		})
	}
}

// custodiaAusente simula la sesión sin DEK: existe el data_dir, no existe la llave.
type custodiaAusente struct{}

func (custodiaAusente) Store([]byte) error    { return nil }
func (custodiaAusente) Load() ([]byte, error) { return nil, errNoHayDEK }
func (custodiaAusente) Exists() bool          { return false }

var errNoHayDEK = errorSimple("no hay DEK custodiada")

type errorSimple string

func (e errorSimple) Error() string { return string(e) }

// TestOpciones_TextoYJIDSonPlausiblesYBorrables vigila las dos propiedades del contenido sintético que un
// test de mecánica no vería: que el texto sea del corpus de pedidos (si fuera lorem ipsum, la inferencia
// que se mida no es la de campo) y que el JID no pueda colisionar con uno real.
func TestOpciones_TextoYJIDSonPlausiblesYBorrables(t *testing.T) {
	o := opcionesDeTest("/tmp/x", uuid.NewString(), 4, len(frasesDePedido))

	// (a) Cada conversación recorre el corpus ENTERO sin repetir.
	for c := 0; c < o.conversaciones; c++ {
		vistas := map[string]bool{}
		for m := 0; m < o.mensajes; m++ {
			vistas[o.texto(c, m)] = true
		}
		if len(vistas) != len(frasesDePedido) {
			t.Errorf("conversación %d: %d frases distintas en %d mensajes; el corpus tiene %d "+
				"(¿pasoDeFrase dejó de ser coprimo con len(frasesDePedido)?)",
				c+1, len(vistas), o.mensajes, len(frasesDePedido))
		}
	}

	// (b) 🔴 LA QUE DE VERDAD IMPORTA, y la que a este test le FALTABA: en un mismo turno, las N
	// conversaciones dicen cosas DISTINTAS. Con `pasoDeFrase` múltiplo de len(frasesDePedido) —16, por
	// ejemplo— el (a) de arriba sigue verde y sin embargo las N conversaciones mandan la MISMA frase a la
	// vez: el clasificador recibiría N copias del mismo prompt, que es el caso que mejor se comporta y el
	// que menos se parece a una jornada real. Una carga así mide el caché de la inferencia, no la cola.
	for m := 0; m < o.mensajes; m++ {
		vistas := map[string]bool{}
		for c := 0; c < o.conversaciones; c++ {
			vistas[o.texto(c, m)] = true
		}
		if len(vistas) != o.conversaciones {
			t.Errorf("turno %d: las %d conversaciones solo dijeron %d frases distintas "+
				"(¿pasoDeFrase es múltiplo de len(frasesDePedido)=%d?)",
				m+1, o.conversaciones, len(vistas), len(frasesDePedido))
		}
	}

	// El prefijo de texto se antepone sin comerse la frase.
	o.prefijoTexto = "[carga]"
	if got := o.texto(0, 0); !strings.HasPrefix(got, "[carga] ") || !esFraseDelCorpus(strings.TrimPrefix(got, "[carga] ")) {
		t.Errorf("prefijo de texto: got %q", got)
	}

	// El JID: reconocible, borrable de un LIKE, e imposible de confundir con un número de teléfono.
	jid := o.chatJID(0)
	if !strings.HasPrefix(jid, "colaseed-") || !strings.HasSuffix(jid, sufijoJID) {
		t.Errorf("chat_jid sintético no reconocible: %q", jid)
	}
	local := strings.TrimSuffix(jid, sufijoJID)
	if strings.IndexFunc(local, func(r rune) bool { return r < '0' || r > '9' }) < 0 {
		t.Errorf("chat_jid %q es todo dígitos: podría colisionar con un número de teléfono real", jid)
	}
	if !strings.HasPrefix(jid, strings.TrimSuffix(o.patronLike(), "%")) {
		t.Errorf("el patrón de limpieza %q no cubre el chat_jid %q", o.patronLike(), jid)
	}
}

// TestImprimirResumen_DiceLaDiscrepancia: cuando lo encolado y lo que hay en disco no cuadran, el resumen
// tiene que avisar. Un parte que reporta 60 filas cuando hay 40 es peor que no tener parte.
func TestImprimirResumen_DiceLaDiscrepancia(t *testing.T) {
	o := opcionesDeTest("/tmp/x", uuid.NewString(), 2, 3)

	var sinAviso, conAviso bytes.Buffer
	imprimirResumen(&sinAviso, o, resumen{encoladas: 6, filas: 6, conversaciones: 2, seqMin: 1, seqMax: 6, patron: o.patronLike()})
	imprimirResumen(&conAviso, o, resumen{encoladas: 6, filas: 4, conversaciones: 2, seqMin: 1, seqMax: 6, patron: o.patronLike()})

	if strings.Contains(sinAviso.String(), "⚠️") {
		t.Errorf("resumen sin discrepancia y con aviso:\n%s", sinAviso.String())
	}
	if !strings.Contains(conAviso.String(), "⚠️") {
		t.Errorf("resumen con discrepancia y SIN aviso:\n%s", conAviso.String())
	}
	// El DELETE de limpieza va en los dos: es la escoba, y tiene que estar en la misma pantalla.
	for nombre, salida := range map[string]string{"sin aviso": sinAviso.String(), "con aviso": conAviso.String()} {
		if !strings.Contains(salida, "DELETE FROM cola_entrantes WHERE") || !strings.Contains(salida, o.patronLike()) {
			t.Errorf("resumen %s sin el DELETE de limpieza:\n%s", nombre, salida)
		}
	}
}

// TestSembrar_RespetaLaPausa comprueba que `-pausa` frena de verdad: sin ella, una siembra en un VPS
// reproduce un pico que la cola nunca ve en campo (~45 msg/min por caja), y la medición diría otra cosa.
func TestSembrar_RespetaLaPausa(t *testing.T) {
	ctx := context.Background()
	o := opcionesDeTest(t.TempDir(), uuid.NewString(), 1, 3)
	o.pausa = 40 * time.Millisecond

	inicio := time.Now()
	if _, err := sembrar(ctx, o, custodiaCon(dekDe(0x44)), logDeTest(t)); err != nil {
		t.Fatalf("sembrar: %v", err)
	}
	// Tres inserciones, tres pausas: el suelo son 120 ms. Se compara contra 100 ms para no volverlo
	// frágil por la resolución del timer, pero sigue siendo imposible de pasar con pausa cero.
	if transcurrido := time.Since(inicio); transcurrido < 100*time.Millisecond {
		t.Errorf("la siembra con -pausa=%s tardó %s: la pausa no se está aplicando", o.pausa, transcurrido)
	}
}

// esFraseDelCorpus indica si el texto es una de las frases sintéticas.
func esFraseDelCorpus(texto string) bool {
	for _, f := range frasesDePedido {
		if texto == f {
			return true
		}
	}
	return false
}
