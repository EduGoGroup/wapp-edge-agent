package whatsappfalso

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// colaEspia es la cola de entrantes vista desde el puerto: guarda lo que la puerta real encola.
type colaEspia struct {
	mu    sync.Mutex
	items []app.ColaItem
}

func (c *colaEspia) Enqueue(_ context.Context, item app.ColaItem) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = append(c.items, item)
	return nil
}

func (c *colaEspia) copia() []app.ColaItem {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]app.ColaItem(nil), c.items...)
}

// dekDePrueba es una DEK reconocible: si asomara en un log o en un campo, se vería.
func dekDePrueba() []byte {
	dek := make([]byte, app.DEKSize)
	for i := range dek {
		dek[i] = byte(0xA0 + i%16)
	}
	return dek
}

// escuchaViva arranca una escucha falsa con su cola y espera a que publique la puerta. Devuelve la escucha,
// la cola espía, el registro, el log y la función que la detiene y espera a que Listen retorne.
func escuchaViva(t *testing.T, sessionID string) (*Escucha, *colaEspia, *Registro, *escritorSeguro, func()) {
	t.Helper()
	return escuchaVivaCon(t, sessionID, nil)
}

// escuchaVivaCon es escuchaViva con un paso de cableado ANTES de escuchar, que es cuando el gestor de
// sesiones cablea los manejadores en producción.
func escuchaVivaCon(t *testing.T, sessionID string, antes func(*Escucha)) (*Escucha, *colaEspia, *Registro, *escritorSeguro, func()) {
	t.Helper()
	buf := &escritorSeguro{w: &bytes.Buffer{}}
	log := sharedlogger.New(sharedlogger.WithWriter(buf))
	registro := NuevoRegistro(0)
	e := NuevaEscucha(registro, log)
	cola := &colaEspia{}
	e.SetCola(cola, sessionID)
	if antes != nil {
		antes(e)
	}

	ctx, cancel := context.WithCancel(context.Background())
	fin := make(chan error, 1)
	go func() { fin <- e.Listen(ctx, dekDePrueba()) }()

	limite := time.Now().Add(5 * time.Second)
	for !e.puerta.HayEscuchaSinTransporte() {
		if time.Now().After(limite) {
			cancel()
			t.Fatal("la escucha falsa no publicó la puerta de entrantes en 5 s")
		}
		time.Sleep(2 * time.Millisecond)
	}
	parar := func() {
		cancel()
		select {
		case err := <-fin:
			if err != nil {
				t.Errorf("Listen devolvió error al cancelar: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Listen no retornó tras cancelar el ctx: la escucha falsa deja una goroutine colgada")
		}
	}
	return e, cola, registro, buf, parar
}

// escritorSeguro serializa las escrituras del logger: la escucha escribe desde su goroutine.
type escritorSeguro struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (e *escritorSeguro) Write(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.w.Write(p)
}

func (e *escritorSeguro) String() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.w.String()
}

// RW.2.a — lo inyectado entra por la puerta REAL (el Listener del adaptador de whatsmeow, sin cliente) y
// acaba encolado por el puerto de la cola, etiquetado con su sesión.
func TestWhatsAppFalso_ElEntranteInyectadoEntraPorLaPuertaRealYCaeEnLaCola(t *testing.T) {
	e, cola, _, _, parar := escuchaViva(t, "sesion-1")
	defer parar()

	acusar, err := e.InyectarEntrante(context.Background(), app.InyeccionEntrante{
		Texto: "carrito", Lote: "lote-a", Indice: 7, ChatJID: "5491100000001",
	})
	if err != nil {
		t.Fatalf("inyectar sobre la escucha falsa: %v", err)
	}
	if !acusar {
		t.Error("la puerta no acusó el entrante: no dejó fila, y en el camino real WhatsApp lo reenviaría")
	}

	items := cola.copia()
	if len(items) != 1 {
		t.Fatalf("la cola recibió %d filas y se inyectó 1 entrante: la puerta real no encoló", len(items))
	}
	it := items[0]
	if it.SessionID != "sesion-1" {
		t.Errorf("la fila nace con session_id %q y la sesión es %q: se sellaría con la DEK de otra", it.SessionID, "sesion-1")
	}
	if it.Texto != "carrito" {
		t.Errorf("el texto encolado es %q, se inyectó %q", it.Texto, "carrito")
	}
	if !strings.HasPrefix(it.ChatJID, "5491100000001@") {
		t.Errorf("el chat encolado es %q: no es el que se inyectó", it.ChatJID)
	}
}

// RW.3.b — todo entrante del transporte falso lleva las DOS marcas del ADR-0041: el prefijo en el
// identificador del mensaje (que viaja a la nube) y `sintetico` en el meta.
func TestWhatsAppFalso_TodoEntranteLlevaLasDosMarcas(t *testing.T) {
	e, cola, _, _, parar := escuchaViva(t, "sesion-1")
	defer parar()

	for i := 0; i < 3; i++ {
		if _, err := e.InyectarEntrante(context.Background(), app.InyeccionEntrante{Texto: "hola", Lote: "lote-b", Indice: i}); err != nil {
			t.Fatalf("inyectar %d: %v", i, err)
		}
	}
	items := cola.copia()
	if len(items) != 3 {
		t.Fatalf("se inyectaron 3 entrantes y hay %d filas", len(items))
	}
	for i, it := range items {
		if !strings.HasPrefix(it.WAMessageID, PrefijoSintetico) {
			t.Errorf("fila %d: wa_message_id %q sin el prefijo %q: en la nube pasaría por un mensaje de WhatsApp",
				i, it.WAMessageID, PrefijoSintetico)
		}
		var meta struct {
			Sintetico bool `json:"sintetico"`
		}
		if err := json.Unmarshal(it.Meta, &meta); err != nil {
			t.Fatalf("fila %d: el meta no es JSON: %v", i, err)
		}
		if !meta.Sintetico {
			t.Errorf("fila %d: el meta no lleva `sintetico`: falta la segunda marca", i)
		}
	}
}

// RW.2.b — un SendText y un SendMedia quedan capturados con su command_id, no salen a ninguna parte, y el
// acuse `delivered` de cada uno vuelve correlacionado con ese command_id, como el de un envío real.
func TestWhatsAppFalso_SendTextYSendMediaQuedanCapturadosConSuCommandID(t *testing.T) {
	type acuse struct {
		cmd    string
		status domain.ReceiptStatus
	}
	acuses := make(chan acuse, 4)
	e2, _, registro2, _, parar := escuchaVivaCon(t, "sesion-1", func(esc *Escucha) {
		esc.SetReceiptHandler(func(evt domain.ReceiptEvent) {
			cmd, _ := esc.ComandoDe(evt.MessageIDs)
			acuses <- acuse{cmd: cmd, status: evt.Status}
		})
	})
	defer parar()

	idTexto, err := e2.SendViaLiveClientTracked(context.Background(), "cmd-texto", "5491100000001", "Elige una categoría")
	if err != nil {
		t.Fatalf("SendText: %v", err)
	}
	idMedia, err := e2.SendMediaViaLiveClientTracked(context.Background(), "cmd-media", "5491100000001",
		"http://127.0.0.1:1/no-se-descarga", "lista.pdf", "application/pdf", "document", "Tu lista")
	if err != nil {
		t.Fatalf("SendMedia (la URL no se descarga: un puerto cerrado no puede hacerlo fallar): %v", err)
	}
	for _, id := range []string{idTexto, idMedia} {
		if !strings.HasPrefix(id, PrefijoSintetico) {
			t.Errorf("el identificador del mensaje enviado es %q, sin el prefijo %q", id, PrefijoSintetico)
		}
	}
	if idTexto == idMedia {
		t.Error("dos envíos devolvieron el mismo identificador: sus acuses no se podrían distinguir")
	}

	envios, descartados := registro2.De("sesion-1")
	if descartados != 0 {
		t.Errorf("el registro dice que descartó %d envíos con dos anotados", descartados)
	}
	if len(envios) != 2 {
		t.Fatalf("se enviaron 2 mensajes y el registro tiene %d", len(envios))
	}
	texto, media := envios[0], envios[1]
	if texto.CommandID != "cmd-texto" || texto.To != "5491100000001" || texto.Text != "Elige una categoría" || texto.Media != nil {
		t.Errorf("el texto capturado no es el enviado: %+v", texto)
	}
	if texto.MessageID != idTexto {
		t.Errorf("el registro guarda el id %q y el envío devolvió %q", texto.MessageID, idTexto)
	}
	if media.CommandID != "cmd-media" || media.Media == nil {
		t.Fatalf("la media capturada no lleva su command_id o sus datos: %+v", media)
	}
	quiere := Media{URL: "http://127.0.0.1:1/no-se-descarga", Filename: "lista.pdf", MIME: "application/pdf", Kind: "document", Caption: "Tu lista"}
	if *media.Media != quiere {
		t.Errorf("la media capturada es %+v, se pidió enviar %+v", *media.Media, quiere)
	}
	if otros, _ := registro2.De("otra-sesion"); len(otros) != 0 {
		t.Errorf("la lectura de otra sesión devuelve %d envíos de esta", len(otros))
	}

	vistos := map[string]domain.ReceiptStatus{}
	for len(vistos) < 2 {
		select {
		case a := <-acuses:
			vistos[a.cmd] = a.status
		case <-time.After(5 * time.Second):
			t.Fatalf("no llegaron los dos acuses en 5 s (llegaron %v): el Cloud no vería el envío como entregado", vistos)
		}
	}
	for _, cmd := range []string{"cmd-texto", "cmd-media"} {
		if vistos[cmd] != domain.ReceiptDelivered {
			t.Errorf("el acuse de %s es %q y debe ser `delivered` (nadie leyó nada: `read` sería inventarlo)", cmd, vistos[cmd])
		}
	}
}

// Sin escucha publicada no hay envío ni inyección: el comando falla, no se captura nada en falso.
func TestWhatsAppFalso_SinEscuchaNiSeEnviaNiSeInyecta(t *testing.T) {
	registro := NuevoRegistro(0)
	e := NuevaEscucha(registro, sharedlogger.New(sharedlogger.WithWriter(&bytes.Buffer{})))
	e.SetCola(&colaEspia{}, "sesion-1")

	if _, err := e.SendViaLiveClientTracked(context.Background(), "cmd-1", "549", "hola"); !errors.Is(err, ErrSinEscucha) {
		t.Errorf("enviar sin escucha devolvió %v y debe ser ErrSinEscucha", err)
	}
	if envios, _ := registro.De("sesion-1"); len(envios) != 0 {
		t.Errorf("se capturaron %d envíos de una sesión que no escucha", len(envios))
	}
	_, err := e.InyectarEntrante(context.Background(), app.InyeccionEntrante{Texto: "hola", Lote: "l"})
	if !errors.Is(err, app.ErrSinEscuchaViva) {
		t.Errorf("inyectar sin escucha devolvió %v y debe envolver app.ErrSinEscuchaViva: el plano de control "+
			"no contestaría 409", err)
	}
}

// RW.3.d — la DEK que el transporte falso recibe por los puertos no se guarda ni se registra.
func TestWhatsAppFalso_LaDEKNoSeGuardaNiSeRegistra(t *testing.T) {
	dek := dekDePrueba()

	// 1. El conector la recibe y contesta sin ella.
	signals, err := NuevoConector().StartConnection(context.Background(), dek)
	if err != nil {
		t.Fatalf("StartConnection: %v", err)
	}
	sig, ok := <-signals
	if !ok || sig.Type != app.PairingSignalConnected {
		t.Fatalf("el conector falso no contestó «conectado»: %+v (ok=%v)", sig, ok)
	}

	// 2. La escucha la recibe, y tras escuchar, inyectar y enviar, el log no la contiene en ninguna forma.
	e, _, _, buf, parar := escuchaViva(t, "sesion-1")
	if _, err := e.InyectarEntrante(context.Background(), app.InyeccionEntrante{Texto: "hola", Lote: "l"}); err != nil {
		t.Fatalf("inyectar: %v", err)
	}
	if _, err := e.SendViaLiveClientTracked(context.Background(), "cmd-1", "549", "hola"); err != nil {
		t.Fatalf("enviar: %v", err)
	}
	parar()
	log := buf.String()
	for nombre, forma := range map[string]string{
		"en crudo":  string(dek),
		"en hex":    hex.EncodeToString(dek),
		"en base64": base64.StdEncoding.EncodeToString(dek),
	} {
		if strings.Contains(log, forma) {
			t.Errorf("la DEK aparece %s en el log del transporte falso", nombre)
		}
	}

	// 3. Ninguno de los dos tipos tiene dónde guardarla: ni un campo de bytes.
	for _, tipo := range []reflect.Type{reflect.TypeOf(Conector{}), reflect.TypeOf(Escucha{})} {
		for i := 0; i < tipo.NumField(); i++ {
			if f := tipo.Field(i); f.Type == reflect.TypeOf([]byte(nil)) {
				t.Errorf("%s.%s es un []byte: el transporte falso no tiene por qué guardar bytes, y la DEK lo es",
					tipo.Name(), f.Name)
			}
		}
	}
}

// El JID de cada sesión falsa es bien formado, distinto y reconocible: el gestor de sesiones deriva de él
// el número propio y rechaza un número repetido.
func TestConectorFalso_CadaEmparejamientoDaUnJIDDistintoYReconocible(t *testing.T) {
	vistos := map[string]bool{}
	for i := 0; i < 20; i++ {
		signals, err := NuevoConector().StartConnection(context.Background(), nil)
		if err != nil {
			t.Fatalf("StartConnection: %v", err)
		}
		jid := (<-signals).WaJID
		pn := domain.SelfPNFromJID(jid)
		if pn == "" {
			t.Fatalf("del JID %q no se deriva número propio: el gestor no podría asignarle rol", jid)
		}
		if !strings.HasPrefix(pn, prefijoNumeroFalso) || len(pn) != len(prefijoNumeroFalso)+8 {
			t.Errorf("el número %q no es reconocible como falso (prefijo %q y 8 cifras)", pn, prefijoNumeroFalso)
		}
		if vistos[jid] {
			t.Errorf("el JID %q se repitió: el segundo emparejamiento se rechazaría por el cupo del número", jid)
		}
		vistos[jid] = true
	}
}

// El registro tiene tope: suelta los más viejos y lo CUENTA, para que una lectura recortada no pase por entera.
func TestRegistro_ConElTopeLlenoSueltaLosViejosYLoCuenta(t *testing.T) {
	r := NuevoRegistro(3)
	for _, cmd := range []string{"a", "b", "c", "d", "e"} {
		r.Anotar(Envio{SessionID: "s", CommandID: cmd})
	}
	envios, descartados := r.De("s")
	if descartados != 2 {
		t.Errorf("descartados = %d, se anotaron 5 con tope 3", descartados)
	}
	var cmds []string
	for _, e := range envios {
		cmds = append(cmds, e.CommandID)
	}
	if strings.Join(cmds, "") != "cde" {
		t.Errorf("quedaron %v y deben quedar los tres últimos, en orden", cmds)
	}
}
