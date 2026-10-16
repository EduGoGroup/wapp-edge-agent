//go:build laboratorio

// e15-edge-inside es el escalón E15 del laboratorio de flujos (Plan 058 · Ola 6): EL EDGE POR DENTRO, SIN
// NUBE. Recorre, con el código de producción, el camino de un mensaje entrante dentro del Edge:
//
//	WhatsApp falso → puerta de entrantes → cola cifrada (cola de entrantes) → despachador → «la nube»
//
// y la vuelta: lo que la nube manda decir sale por el WhatsApp falso, queda CAPTURADO y su acuse llega
// ~50 ms después. Real: el transporte falso, la puerta, la cola y su cifrado, el despachador, el gestor de
// sesiones y el plano de control. Doble: CloudLink (un sumidero en memoria), el authorizer (no hay), la
// custodia de la DEK (un fichero, no el Keychain) y el lease (no hay).
//
//	GOTOOLCHAIN=go1.26.5 GOWORK=off go run -tags laboratorio ./laboratorio/e15-edge-inside
//	… -n 6               (seis mensajes en vez de cuatro)
//	… -sink-rejects 2    (el sumidero rechaza las dos primeras entregas: mira que el orden aguanta)
//	… -v                 (el log del Edge por stderr, además de al fichero)
//
// Los datos van a -dir (por defecto /tmp/wlab-e15), que se BORRA al empezar. Sale con rc=0 si todas las
// comprobaciones pasaron; rc=1 si alguna falló.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/EduGoGroup/wapp-shared/envelope"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/diag"
	"github.com/EduGoGroup/wapp-edge-agent/laboratorio/common"
)

// El cliente inventado: un 1:1 con un número que no es de nadie y un encargo con cifras, para que se vea
// que el texto que viaja es de negocio (y que la cola lo guarda sellado).
const clientNumber = "5491100000001"

var clientMessages = []string{
	"hola, quiero encargar 2 tortas de chocolate",
	"y un paquete de 30 tequeños",
	"para el sábado 18 a las 10:30",
	"¿cuánto sería en total? pago con transferencia",
}

// secretWord es una palabra del encargo que se busca en los bytes crudos del disco.
const secretWord = "tequeños"

func main() {
	dir := flag.String("dir", "/tmp/wlab-e15", "directorio de datos del Edge (SE BORRA al empezar; ruta corta: el socket unix no admite más de 104 caracteres en macOS)")
	n := flag.Int("n", len(clientMessages), "cuántos mensajes manda el cliente inventado")
	verbose := flag.Bool("v", false, "saca el log del Edge también por stderr")
	sinkRejects := flag.Int("sink-rejects", 0, "cuántas entregas rechaza el sumidero antes de aceptar (para ver el reintento en orden)")
	flag.Parse()

	n0 := common.NewNarrator(nil)
	if err := run(n0, *dir, *n, *verbose, *sinkRejects); err != nil {
		n0.Fail(err, "el escalón se detuvo")
	}
	passed, failed := n0.Counts()
	fmt.Printf("\n%s\n  Resultado: %d comprobaciones bien, %d mal.\n", strings.Repeat("═", 72), passed, failed)
	if failed > 0 {
		os.Exit(1)
	}
}

func run(n *common.Narrator, dir string, count int, verbose bool, sinkRejects int) error {
	if count < 1 {
		return errors.New("-n tiene que ser ≥ 1")
	}
	n.Title("E15 · El Edge por dentro, sin nube",
		"WhatsApp falso → puerta de entrantes → cola cifrada → despachador → sumidero en memoria.",
		"Real: transporte falso, puerta, cola y su cifrado, despachador, gestor de sesiones, plano de control.",
		"Doble: CloudLink (sumidero), authorizer (no hay), custodia de la DEK (fichero), lease (no hay).")
	ctx := context.Background()

	// ── 1 ──────────────────────────────────────────────────────────────────────────────────────────
	n.Step("Se prepara el banco de trabajo")
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("borrar %s: %w", dir, err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("crear %s: %w", dir, err)
	}
	socket := filepath.Join(dir, socketName)
	n.Note("Directorio %s, recién borrado. El socket del plano de control será %s (%d caracteres;", dir, socket, len(socket))
	n.Note("macOS no admite más de 104 en la ruta de un socket unix, y por eso no se usa t.TempDir ni el scratchpad).")
	if !n.Check(len(socket) < 104, "la ruta del socket cabe en sun_path") {
		return errors.New("elige un -dir más corto")
	}
	logPath := filepath.Join(dir, "edge.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = logFile.Close() }()
	logBuf := &syncBuffer{}
	var logOut io.Writer = io.MultiWriter(logFile, logBuf)
	if verbose {
		logOut = io.MultiWriter(logOut, os.Stderr)
	}
	n.Note("El log del Edge va a %s (con -v, también a stderr). El narrador va por stdout.", logPath)

	edge, err := openEdge(ctx, dir, logOut, sinkRejects)
	if err != nil {
		return err
	}
	defer edge.close()
	files, err := queueFiles(ctx, edge.colaDB)
	if err != nil {
		return err
	}
	n.OK("Abiertas las dos BD del Edge: la única (%s) y la de la cola (%s),", edgeDBName, filepath.Base(files[0]))
	n.Note("esta última con db.OpenCola, el único constructor que le aplica su perfil de escritura, y la cola")
	n.Note("construida con wiring.BuildCola: el mismo cableado que daemon.Run y que cmd/colaseed.")

	// ── 2 ──────────────────────────────────────────────────────────────────────────────────────────
	n.Step("Arranca el Edge con la cola CONGELADA (WAPP_AGENT_DESPACHADOR_APAGADO=true)")
	n.Note("Se arranca congelado para poder MIRAR la cola antes de que alguien la vacíe: con el despachador")
	n.Note("encendido, una fila vive milisegundos en disco (lo verás en el paso 9).")
	if err := edge.start(ctx, true); err != nil {
		return err
	}
	cli := newControlClient(socket)
	code, raw, err := cli.call(ctx, http.MethodGet, "/v1/health", nil, nil)
	if err != nil {
		return fmt.Errorf("GET /v1/health: %w", err)
	}
	n.Show("GET /v1/health", raw)
	n.Check(code == http.StatusOK, "el plano de control contesta por el socket unix, sin Bearer (no hay authorizer)")

	// ── 3 ──────────────────────────────────────────────────────────────────────────────────────────
	n.Step("Se empareja una sesión FALSA: sin QR, sin teléfono, sin red")
	sid, jid, err := pair(ctx, n, cli)
	if err != nil {
		return err
	}
	n.Check(strings.HasPrefix(jid, "0000"), "el JID de la sesión empieza por 0000 (ningún prefijo internacional lo hace): %s", jid)
	dekPath, err := edge.dekPath(sid)
	if err != nil {
		return err
	}
	dek, err := os.ReadFile(dekPath)
	if err != nil {
		return fmt.Errorf("leer la DEK de la custodia de fichero: %w", err)
	}
	info, err := os.Stat(dekPath)
	if err != nil {
		return err
	}
	n.Check(len(dek) == 32 && info.Mode().Perm() == 0o600,
		"la DEK la GENERÓ app.Pair y la selló en la custodia: %d bytes en %s, permisos %o", len(dek), rel(dir, dekPath), info.Mode().Perm())
	n.Note("No se imprime: es la llave del cliente (ADR-0007). En campo estaría en el Keychain; aquí, el doble fileCustody.")
	n.Check(!strings.Contains(logBuf.String(), string(dek)), "la DEK no aparece en el log del Edge")

	// ── 4 ──────────────────────────────────────────────────────────────────────────────────────────
	n.Step(fmt.Sprintf("El cliente inventado (%s) manda %d mensajes, por la puerta REAL de entrantes", clientNumber, count))
	n.Note("POST %s: el inyector del ADR-0041 fabrica un events.Message y lo entrega a handleEvent, el mismo", diag.RutaInyectar)
	n.Note("punto que cablea whatsmeow. Pasa las guardas de la puerta y acaba en Enqueue, como uno de verdad.")
	texts := make([]string, count)
	for i := range texts {
		texts[i] = clientMessages[i%len(clientMessages)]
		if i >= len(clientMessages) {
			texts[i] = fmt.Sprintf("%s (otra vez, mensaje %d)", texts[i], i+1)
		}
	}
	for i, t := range texts {
		resp, err := inject(ctx, cli, sid, t, fmt.Sprintf("e15-m%d", i+1))
		if err != nil {
			return err
		}
		n.Check(resp.Inyectados == 1 && resp.Acusados == 1,
			"mensaje %d «%s» → inyectados=%d acusados=%d en %d ms", i+1, t, resp.Inyectados, resp.Acusados, resp.DuracionMS)
	}
	n.Note("«acusados» es el PERMISO de acusar a WhatsApp: el handler solo lo da cuando la fila ya está en")
	n.Note("disco. Si no, WhatsApp reofrecería el mensaje. Primero durable, después ✓✓.")
	if nObs, p99, maxMS, ok := edge.handlerP99(); ok {
		n.Check(p99.CotaMS <= 50, "cronómetro del handler (INV-051.2): n=%d · p99 en el bucket %s · máximo %.2f ms (criterio: < 50 ms p99)",
			nObs, p99.Bucket, maxMS)
	} else {
		n.Fail(nil, "el cronómetro del handler no anotó nada: ¿WithLatencia no llegó al listener?")
	}

	// ── 5 ──────────────────────────────────────────────────────────────────────────────────────────
	n.Step("La cola en disco, congelada: lo que va en claro y lo que va sellado")
	rows, err := readQueue(ctx, edge.colaDB, sid)
	if err != nil {
		return err
	}
	n.Check(len(rows) == count, "hay %d filas de la sesión y nadie las ha drenado", len(rows))
	table := []string{fmt.Sprintf("%-3s %-4s %-10s %-29s %-23s %s", "id", "seq", "estado", "chat_jid (claro)", "wa_message_id (claro)", "texto_enc (sellado)")}
	allNew, ordered := true, true
	for i, r := range rows {
		table = append(table, fmt.Sprintf("%-3d %-4d %-10s %-29s %-23s %d B · %x…", r.ID, r.Seq, r.Estado, r.ChatJID, r.WAMessageID, len(r.TextoEnc), head(r.TextoEnc, 10)))
		allNew = allNew && r.Estado == "nuevo"
		ordered = ordered && (i == 0 || r.Seq > rows[i-1].Seq) && r.WAMessageID == fmt.Sprintf("SINTETICO-e15-m%d-0000", i+1)
	}
	n.Rows("SELECT … FROM cola_entrantes ORDER BY seq", table)
	n.Check(allNew, "todas en estado «nuevo»: con la palanca echada el despachador no arrancó")
	n.Check(ordered, "el orden de llegada es el orden de seq, y seq es el orden en que se despachará (REQ-051.18)")
	n.Note("Cada texto_enc mide %d bytes más que su texto: nonce de 12 + etiqueta de 16 (AES-GCM de", envelope.Overhead)
	n.Note("wapp-shared/envelope). session_id, chat_jid y wa_message_id van EN CLARO a propósito: son la clave")
	n.Note("de enrutado y de idempotencia. ⚠️ Eso incluye el TELÉFONO del cliente (chat_jid).")

	hits, err := grepFiles(files, secretWord)
	if err != nil {
		return err
	}
	n.Check(sumCounts(hits) == 0, "«%s» aparece %d veces en los bytes crudos de %s y su WAL: el texto NO está en disco",
		secretWord, sumCounts(hits), filepath.Base(files[0]))
	phoneHits, err := grepFiles(files, clientNumber)
	if err != nil {
		return err
	}
	n.Check(sumCounts(phoneHits) > 0, "el número %s SÍ aparece %d veces en esos bytes: va en claro, como dice el DDL",
		clientNumber, sumCounts(phoneHits))

	// ── 6 ──────────────────────────────────────────────────────────────────────────────────────────
	n.Step("Con la DEK de la sesión, la misma fila se lee")
	env, err := envelope.NewEnvelope(dek)
	if err != nil {
		return err
	}
	readable := 0
	for i, r := range rows {
		plain, err := env.Open(r.TextoEnc)
		if err != nil {
			n.Fail(err, "fila %d: Open", r.ID)
			continue
		}
		meta := "(sin meta)"
		if r.MetaEnc != nil {
			if m, err := env.Open(r.MetaEnc); err == nil {
				meta = string(m)
			} else {
				meta = "(meta ilegible: " + err.Error() + ")"
			}
		}
		if string(plain) == texts[i] {
			readable++
		}
		n.Note("fila %d · texto «%s» · meta %s", r.ID, plain, meta)
	}
	n.Check(readable == count, "%d de %d filas se abren con la DEK y dicen exactamente lo que mandó el cliente", readable, count)
	other := make([]byte, 32)
	copy(other, dek)
	other[0] ^= 0xFF
	if wrong, err := envelope.NewEnvelope(other); err == nil {
		_, errOpen := wrong.Open(rows[0].TextoEnc)
		n.Check(errOpen != nil, "con OTRA llave (la DEK con un byte cambiado) la fila no se abre: %v", errOpen)
	}

	// ── 7 ──────────────────────────────────────────────────────────────────────────────────────────
	n.Step("Idempotencia: el mismo mensaje dos veces es UNA fila")
	resp, err := inject(ctx, cli, sid, texts[0], "e15-m1")
	if err != nil {
		return err
	}
	again, err := countQueue(ctx, edge.colaDB, sid)
	if err != nil {
		return err
	}
	n.Check(resp.Acusados == 1 && again == count,
		"se reinyectó el lote e15-m1: acusados=%d y la cola sigue con %d filas (índice único session_id+wa_message_id)", resp.Acusados, again)
	n.Note("whatsmeow re-emite eventos al reconectar: el duplicado es el caso NORMAL, no un error. Por eso se acusa igual.")

	// ── 8 ──────────────────────────────────────────────────────────────────────────────────────────
	n.Step("Se reinicia el Edge con el despachador ENCENDIDO: la cola se drena, en orden")
	edge.stop()
	n.Note("Se paró el gestor de sesiones y el plano de control, como un reinicio del daemon. Las filas siguen en disco.")
	t0 := time.Now()
	if err := edge.start(ctx, false); err != nil {
		return err
	}
	n.Note("Restore() levantó la sesión activa desde edge.db, cargó su DEK de la custodia y arrancó su despachador.")
	if sinkRejects > 0 {
		n.Note("El sumidero rechazará las primeras %d entregas (-sink-rejects): el despachador reintenta la MISMA cabeza.", sinkRejects)
	}
	ok := waitUntil(15*time.Second, func() bool { return len(edge.mux.deliveredCopy()) >= count })
	delivered := edge.mux.deliveredCopy()
	n.Check(ok, "el sumidero recibió %d entregas en %s desde el arranque", len(delivered), time.Since(t0).Round(100*time.Microsecond))
	inOrder := len(delivered) >= count
	for i := 0; i < count && i < len(delivered); i++ {
		d := delivered[i]
		inOrder = inOrder && d.Event.Text == texts[i]
		n.Note("entrega %d · +%s · %s · de %s · push_name %q · «%s»", i+1, d.At.Sub(t0).Round(100*time.Microsecond),
			d.Event.MessageID, d.Event.Sender, d.Event.PushName, d.Event.Text)
	}
	n.Check(inOrder, "llegaron en el orden en que el cliente las escribió: la fila N+1 no sale antes que la N")
	if sinkRejects > 0 {
		n.Check(edge.mux.rejectedCount() == sinkRejects, "el sumidero rechazó %d entregas y aun así el orden aguantó", edge.mux.rejectedCount())
	}
	n.Note("El evento viaja EN CLARO al sumidero: el despachador descifra y entrega. En campo, CloudLink lo sella")
	n.Note("para el tránsito antes de subirlo; aquí ese tramo es el doble.")
	dispatched := waitUntil(5*time.Second, func() bool {
		rs, err := readQueue(ctx, edge.colaDB, sid)
		if err != nil {
			return false
		}
		for _, r := range rs {
			if r.Estado != "despachado" || !r.DespachadoEn.Valid {
				return false
			}
		}
		return len(rs) == count
	})
	n.Check(dispatched, "las %d filas quedaron «despachado» con su despachado_en: es lo que la poda por TTL (24 h) borrará", count)

	// ── 9 ──────────────────────────────────────────────────────────────────────────────────────────
	n.Step("En caliente: un mensaje más, con el despachador vivo")
	before := len(edge.mux.deliveredCopy())
	tHot := time.Now()
	hot := "una cosa más: que una de las tortas sea sin azúcar"
	if _, err := inject(ctx, cli, sid, hot, "e15-hot"); err != nil {
		return err
	}
	ok = waitUntil(5*time.Second, func() bool { return len(edge.mux.deliveredCopy()) > before })
	if ok {
		last := edge.mux.deliveredCopy()[before]
		n.Check(last.Event.Text == hot, "de la petición HTTP al sumidero: %s. El Enqueue «toca el timbre» del despachador (T1.8-7)",
			last.At.Sub(tHot).Round(10*time.Microsecond))
		n.Note("sin esperar al respaldo de %d ms. La fila vivió en disco solo ese rato.", 5000)
	} else {
		n.Fail(nil, "el mensaje en caliente no llegó al sumidero en 5 s")
	}

	// ── 10 ─────────────────────────────────────────────────────────────────────────────────────────
	n.Step("La «nube» contesta: el Edge envía por el WhatsApp falso y lo enviado queda CAPTURADO")
	send := edge.mux.sender(sid)
	if send == nil {
		return errors.New("la sesión no registró su emisor en el multiplexor")
	}
	reply := "¡Hola! 2 tortas de chocolate y 30 tequeños para el sábado 18: son $ 48.500."
	n.Note("El programa hace de CloudLink: llama al emisor que la sesión registró (en campo lo llama el")
	n.Note("dispatcher de comandos tras el gate 2-de-2 de lease + DEK; aquí no hay lease que mirar).")
	if err := send(ctx, "cmd-e15-1", clientNumber, reply); err != nil {
		return fmt.Errorf("enviar por la sesión falsa: %w", err)
	}
	sendDone := time.Now()
	var cap capturedResponse
	code, raw, err = cli.call(ctx, http.MethodGet, diag.RutaCapturados+"?session_id="+sid, nil, &cap)
	if err != nil {
		return err
	}
	n.Show("GET "+diag.RutaCapturados, raw)
	n.Check(code == http.StatusOK && cap.Total == 1 && cap.Capturados[0].CommandID == "cmd-e15-1" &&
		strings.HasPrefix(cap.Capturados[0].MessageID, "SINTETICO-") && cap.Capturados[0].Text == reply,
		"lo capturado es lo enviado, con un message_id SINTETICO- (no salió nada a WhatsApp)")

	ok = waitUntil(2*time.Second, func() bool { return len(edge.mux.receiptsCopy()) > 0 })
	if ok {
		r := edge.mux.receiptsCopy()[0]
		delay := r.At.Sub(sendDone)
		n.Check(r.CommandID == "cmd-e15-1" && string(r.Event.Status) == "delivered",
			"el acuse «%s» llegó al multiplexor correlacionado con su command_id (%s), %s después del envío",
			r.Event.Status, r.CommandID, delay.Round(100*time.Microsecond))
		n.Check(delay >= 45*time.Millisecond && delay < 500*time.Millisecond,
			"≈ 50 ms: el falso NO acusa dentro del envío, para no fabricar el orden contrario al de la red (retardoAcuse)")
	} else {
		n.Fail(nil, "no llegó ningún acuse en 2 s")
	}

	// ── 11 ─────────────────────────────────────────────────────────────────────────────────────────
	n.Step("Se desvincula la sesión falsa y se mira qué queda")
	code, raw, err = cli.call(ctx, http.MethodDelete, "/v1/sessions/"+sid, nil, nil)
	if err != nil {
		return err
	}
	n.Show("DELETE /v1/sessions/{id}", fmt.Sprintf("HTTP %d %s", code, bytes.TrimSpace(raw)))
	after, err := readQueue(ctx, edge.colaDB, sid)
	if err != nil {
		return err
	}
	allDispatched := len(after) > 0
	for _, r := range after {
		allDispatched = allDispatched && r.Estado == "despachado"
	}
	_, statErr := os.Stat(dekPath)
	n.Check(code < 300 && errors.Is(statErr, os.ErrNotExist), "la DEK de la custodia: %s", presence(statErr))
	n.Check(len(after) == count+1 && allDispatched,
		"las %d filas de la sesión SIGUEN en la cola, todas «despachado»: desvincular NO borra filas", len(after))
	_, errLoad := edge.custodyFor(dekPath).Load()
	n.Check(errLoad != nil, "y el Edge ya no puede abrirlas: la custodia no tiene DEK que dar (%v)", errLoad)
	n.Note("Desvincular borra la DEK (clearDEK) y olvida el sobre cacheado de la cola (Forget), pero las filas")
	n.Note("se quedan: es «borrado por llave» (crypto-shredding). Las «despachado» las borra la poda por TTL (24 h)")
	n.Note("en un Enqueue posterior. ⚠️ Una fila «nuevo» de una sesión desvinculada no la drena ya nadie: solo")
	n.Note("la expulsa el tope de filas. Quien guardara una copia de la DEK (este programa la tiene en memoria) sí")
	n.Note("podría leerlas: por eso la DEK nunca sale del equipo (ADR-0007).")
	if plain, err := env.Open(after[0].TextoEnc); err == nil {
		n.Note("Prueba: con la copia en memoria, la fila %d aún dice «%s».", after[0].ID, plain)
	}
	shout := strings.Count(logBuf.String(), "WAPP_AGENT_DESPACHADOR_APAGADO=true")
	n.Check(shout >= 1, "el log del Edge GRITÓ la palanca del despachador %d vez(es) en la fase congelada", shout)

	n.Step("Fin")
	n.OK("Un mensaje entra por la puerta real, se sella con la DEK de su sesión, espera en orden en disco y")
	n.OK("sale descifrado hacia la nube; lo que la nube contesta se captura y se acusa. Todo sin nube.")
	n.Note("Siguiente: E16 pone el servidor de verdad con un Edge doble; E17, este Edge de verdad con el servidor.")
	return nil
}

// pair hace POST /v1/sessions/pair y sondea GET /v1/sessions/{id}/pair hasta «success».
func pair(ctx context.Context, n *common.Narrator, cli *controlClient) (sessionID, jid string, err error) {
	var start pairStart
	code, raw, err := cli.call(ctx, http.MethodPost, "/v1/sessions/pair", nil, &start)
	if err != nil || code != http.StatusOK {
		return "", "", fmt.Errorf("POST /v1/sessions/pair: HTTP %d %s %v", code, raw, err)
	}
	n.Note("POST /v1/sessions/pair → id de emparejamiento %s, estado %q, QR %q", start.ID, start.Status, start.QR)
	n.Note("(el POST espera hasta 2 s por un primer QR que el falso no va a pintar nunca)")
	var poll pairPoll
	ok := waitUntil(10*time.Second, func() bool {
		code, _, err := cli.call(ctx, http.MethodGet, "/v1/sessions/"+start.ID+"/pair", nil, &poll)
		return err == nil && code == http.StatusOK && poll.Status != "pending"
	})
	if !ok || poll.Status != "success" {
		return "", "", fmt.Errorf("el emparejamiento no acabó en success: %+v", poll)
	}
	b, _ := json.Marshal(poll)
	n.Show("GET /v1/sessions/{id}/pair", b)
	n.Check(poll.QR == "", "sin QR: el conector falso contesta «conectado» de inmediato")
	return poll.SessionID, poll.JID, nil
}

// inject mete UN mensaje del cliente. Reintenta el 409 (la escucha aún no está publicada).
func inject(ctx context.Context, cli *controlClient, sid, text, lote string) (injectResponse, error) {
	var resp injectResponse
	var lastCode int
	var lastRaw []byte
	ok := waitUntil(5*time.Second, func() bool {
		code, raw, err := cli.call(ctx, http.MethodPost, diag.RutaInyectar,
			injectRequest{SessionID: sid, Texto: text, ChatJID: clientNumber, Lote: lote}, &resp)
		lastCode, lastRaw = code, raw
		return err == nil && code == http.StatusOK
	})
	if !ok {
		return resp, fmt.Errorf("POST %s: HTTP %d %s", diag.RutaInyectar, lastCode, lastRaw)
	}
	return resp, nil
}

// waitUntil sondea cond cada 5 ms hasta que se cumpla o venza el plazo.
func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func head(b []byte, max int) []byte {
	if len(b) < max {
		return b
	}
	return b[:max]
}

func rel(base, p string) string {
	if r, err := filepath.Rel(base, p); err == nil {
		return r
	}
	return p
}

func presence(statErr error) string {
	if errors.Is(statErr, os.ErrNotExist) {
		return "borrada"
	}
	if statErr != nil {
		return "desconocida (" + statErr.Error() + ")"
	}
	return "SIGUE AHÍ"
}
