package whatsmeow

// circuito_aviso_test.go — EL CIRCUITO DEL TIMBRE, DE PUNTA A PUNTA (Plan 044 · Ola 1.8 · T1.8-7).
//
// QUÉ SE PRUEBA AQUÍ Y POR QUÉ NO PODÍA PROBARSE EN OTRO SITIO. El aviso atraviesa TRES paquetes —el
// listener que lo toca (`internal/adapters/whatsmeow`), la cola que persiste antes de que se toque
// (`internal/adapters/colaentrantes`) y el bucle que lo escucha (`internal/app/despachador`)— y lo que la
// tarea compra no es ninguna de las tres mitades por separado, sino su COSTURA: que el `Deliver` ocurra
// milisegundos después del `Enqueue` en vez de esperar al reloj.
//
// 🔴 EL FICHERO VIVE EN ESTE PAQUETE POR UNA RAZÓN DE VISIBILIDAD, NO DE GUSTO: el que dispara el aviso es
// `Listener.enqueueCola`, y tanto él como `handleEvent` son NO EXPORTADOS. Desde `colaentrantes` —donde
// están el arnés de la Ola 3 del 051 y la BD real— sólo se llegaría al `Enqueue` a pelo, es decir, se
// probaría un timbre que toca el TEST en vez del que toca el listener: exactamente el atajo que se salta
// el cableado que hay que custodiar. Así que la BD real se trae aquí.
//
// ─── QUÉ ES REAL Y QUÉ NO ───
//
// Real: el `Listener` (construido con sus opciones, como lo construye `serve`), el `*colaentrantes.Store`
// sobre SQLite abierto por `db.OpenCola` + `db.MigrateCola` (el mismo camino que producción, con sus
// pragmas), el `despachador.Despachador` y su `despachador.AvisoConRespaldo`. Dobles: sólo el SINK (el
// destino de la entrega, que aquí sería la nube) y un ESPÍA que envuelve al despertador real para contar
// despertares y avisar de cuándo el bucle aparca — no sustituye nada, delega.
//
// ─── EL RELOJ SÍ ES REAL, Y ES DELIBERADO ───
//
// El resto de los tests de esta casa evitan el reloj de pared como sincronía, y con razón. Aquí NO se
// puede: el criterio de la tarea es literalmente «un `Enqueue` produce un `Deliver` en < 50 ms (reloj
// real, es I/O)». Con un reloj falso no se estaría midiendo el efecto, se estaría postulando. Lo que sí se
// respeta es la disciplina: las esperas por canal son la SINCRONÍA (el test avanza cuando el bucle aparca
// o cuando el sink recibe), y los `time.After` son GUARDIAS ANTI-CUELGUE para que un fallo salga como test
// rojo en segundos y no como un CI colgado.
//
// INV-051.1: ningún mensaje de fallo imprime un `domain.InboundEvent` ni un `app.ColaItem` — saldría el
// texto y el push_name a la salida de CI, que es un log más. Se imprimen identificadores y tiempos.

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/colaentrantes"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/despachador"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	infradb "github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-shared/envelope"
)

const (
	// avGuardia es la guardia ANTI-CUELGUE del fichero: nada de lo que se espera aquí tarda tanto, y este
	// plazo sólo existe para que un bucle atascado muera como rojo en vez de colgar el CI.
	avGuardia = 5 * time.Second
	// avSesion es la sesión de todo el fichero. El session_id elige la DEK con la que la cola sella cada
	// fila, así que tiene que ser el MISMO en el listener y en el despachador o no habría nada que abrir.
	avSesion = "sess-aviso"
	// avRespaldoCorto es el respaldo que usan los tests que quieren VER actuar al respaldo (criterios (b) y
	// (d)).
	//
	// 🔴 NO ES `despachador.DefaultRespaldoMS`, Y NO ES PEREZA: un test que asertara contra la constante que
	// protege pasaría con CUALQUIER valor de esa constante —incluido un 0 que la desactivara—, que es la
	// definición de test tautológico. Aquí el respaldo es un número del TEST, elegido para que el criterio
	// «llega por el respaldo y no antes» sea observable en menos de un segundo. Que el número de PRODUCCIÓN
	// sea 5 s y su porqué es otra afirmación, y vive donde se decide (despachador/despertador.go), no aquí.
	avRespaldoCorto = 300 * time.Millisecond
	// avRespaldoInfinito desactiva el respaldo a efectos prácticos: lo usan los criterios (a) y (c), donde
	// lo que se quiere demostrar es que el AVISO dispara SOLO. Una hora es infinito para un test cuya
	// guardia son cinco segundos.
	avRespaldoInfinito = time.Hour
)

// ─────────────────────────────────────────────────────────────────────────────
// Dobles y arnés
// ─────────────────────────────────────────────────────────────────────────────

// avSink es el destino de la entrega: guarda el orden y publica por un canal para que el test pueda
// esperar sin dormir.
type avSink struct {
	mu         sync.Mutex
	entregados []string
	entregas   chan domain.InboundEvent

	// freno RETIENE al bucle DENTRO de la primera entrega hasta que el test lo suelte, y `alcanzado` avisa
	// de que ya está retenido. La pareja existe sólo para el criterio (c): sin ella, un test que encola
	// veinte mensajes seguidos no llega a producir NINGÚN colapso de avisos —el bucle va tan rápido que
	// consume cada aviso antes de que llegue el siguiente— y acabaría afirmando «≤ 20 despertares» sobre
	// veinte despertares limpios, que es cierto y no prueba nada. Con el freno, el escenario es el que la
	// tarea describe: diecinueve avisos peleando por un buffer de uno.
	//
	// nil ⇒ el sink no retiene nada (el resto de los tests del fichero).
	freno     chan struct{}
	alcanzado chan struct{}
	unaVez    sync.Once
}

func nuevoAvSink() *avSink {
	return &avSink{entregas: make(chan domain.InboundEvent, 256)}
}

// conFreno arma la retención de la PRIMERA entrega. Devuelve el propio sink para encadenar.
func (s *avSink) conFreno() *avSink {
	s.freno = make(chan struct{})
	s.alcanzado = make(chan struct{})
	return s
}

func (s *avSink) Deliver(_ context.Context, evt domain.InboundEvent) error {
	if s.freno != nil {
		s.unaVez.Do(func() { close(s.alcanzado) })
		<-s.freno
	}
	s.mu.Lock()
	s.entregados = append(s.entregados, evt.MessageID)
	s.mu.Unlock()
	select {
	case s.entregas <- evt:
	default:
	}
	return nil
}

// ids proyecta lo entregado a SOLO los identificadores (INV-051.1: nada del contenido en la salida).
func (s *avSink) ids() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.entregados...)
}

// avEspia ENVUELVE al despertador real: no lo sustituye, delega en él. Añade dos cosas que el tipo de
// producción no tiene por qué exponer y el test necesita:
//
//   - `parado`: se toca ANTES de bloquear, así que recibir de ahí significa «el bucle acaba de aparcar y la
//     vuelta anterior terminó del todo». Es lo que permite sembrar trabajo sabiendo con certeza que el
//     bucle todavía no lo ha mirado — sin eso, medir «cuánto tardó el aviso» mediría también el trozo de
//     vuelta que quedara pendiente.
//   - `despertares`: cuántas veces `Esperar` volvió con trabajo por hacer, que es la magnitud del criterio
//     (c) («≤ 20 despertares para 20 encolados»).
type avEspia struct {
	inner       despachador.Despertador
	parado      chan struct{}
	despertares atomic.Int64
}

func nuevoAvEspia(inner despachador.Despertador) *avEspia {
	return &avEspia{inner: inner, parado: make(chan struct{}, 256)}
}

func (e *avEspia) Esperar(ctx context.Context) error {
	select {
	case e.parado <- struct{}{}:
	default:
	}
	err := e.inner.Esperar(ctx)
	if err == nil {
		e.despertares.Add(1)
	}
	return err
}

var _ despachador.Despertador = (*avEspia)(nil)

// avCircuito es el montaje completo: cola real, listener real, despachador real y el canal que los une.
type avCircuito struct {
	ruta     infradb.ColaDBPath
	cola     *colaentrantes.Store
	canal    chan struct{}
	sink     *avSink
	espia    *avEspia
	listener *Listener
	cancel   context.CancelFunc
	retorno  chan error
}

// avCrypterFor resuelve el sobre de una sesión con una DEK determinista. Es el mismo juguete que usa la
// suite de `colaentrantes`; se replica aquí y no se importa porque los helpers de test no cruzan paquetes.
func avCrypterFor(sessionID string) (envelope.Crypter, error) {
	dek := make([]byte, envelope.DEKSize)
	for i := range dek {
		dek[i] = byte(i)
	}
	for i := 0; i < len(sessionID) && i < envelope.DEKSize; i++ {
		dek[i] ^= sessionID[i]
	}
	return envelope.NewEnvelope(dek)
}

// avAbrirCola abre una BD de cola POR EL CAMINO DE PRODUCCIÓN (db.OpenCola + db.MigrateCola: los mismos
// pragmas y el mismo DDL migrado que el daemon) y devuelve también su RUTA, que es lo que el criterio (d)
// necesita para volver a abrirla desde una segunda conexión.
func avAbrirCola(ctx context.Context, t *testing.T) (*sql.DB, infradb.ColaDBPath) {
	t.Helper()
	ruta := infradb.ColaDBPath(filepath.Join(t.TempDir(), "cola_entrantes.db"))
	database, err := infradb.OpenCola(ctx, ruta)
	if err != nil {
		t.Fatalf("db.OpenCola: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := infradb.MigrateCola(ctx, database); err != nil {
		t.Fatalf("db.MigrateCola: %v", err)
	}
	return database, ruta
}

// montarCircuito arma el circuito entero y deja el despachador corriendo.
//
// `cableaAviso` es la palanca del criterio (b): con `false` el Listener nace SIN `WithAviso` —la mutación
// que compila— y todo lo demás queda idéntico, de modo que la única diferencia entre los dos casos sea el
// cable que se quiere custodiar.
func montarCircuito(ctx context.Context, t *testing.T, respaldo time.Duration, cableaAviso bool, sink *avSink) *avCircuito {
	t.Helper()
	if sink == nil {
		sink = nuevoAvSink()
	}

	database, ruta := avAbrirCola(ctx, t)
	cola, err := colaentrantes.New(ctx, database, avCrypterFor, 100, 24, quietLogger())
	if err != nil {
		t.Fatalf("colaentrantes.New: %v", err)
	}

	// BUFFER 1: el mismo que crea `sessionmgr.liveSession.arm` en producción. Si aquí se pusiera más
	// buffer, el criterio (c) dejaría de probar lo que dice probar (que el colapso de avisos no pierde
	// filas), porque no habría colapso ninguno.
	canal := make(chan struct{}, 1)

	opts := []ListenerOption{WithCola(cola), WithSessionID(avSesion)}
	if cableaAviso {
		opts = append(opts, WithAviso(canal))
	}
	listener := NewListener(quietLogger(), opts...)

	espia := nuevoAvEspia(despachador.NewAvisoConRespaldo(canal, respaldo))
	d, err := despachador.New(despachador.Deps{
		Cola:        cola,
		Sink:        sink,
		SessionID:   avSesion,
		Log:         quietLogger(),
		Despertador: espia,
	})
	if err != nil {
		t.Fatalf("despachador.New: %v", err)
	}

	bucleCtx, cancel := context.WithCancel(ctx)
	c := &avCircuito{
		ruta: ruta, cola: cola, canal: canal, sink: sink, espia: espia,
		listener: listener, cancel: cancel, retorno: make(chan error, 1),
	}
	go func() { c.retorno <- d.Run(bucleCtx) }()
	t.Cleanup(c.parar)
	return c
}

// parar cierra el grifo y espera a que `Run` salga, para no dejar goroutines vivas entre tests.
func (c *avCircuito) parar() {
	c.cancel()
	select {
	case <-c.retorno:
	case <-time.After(avGuardia):
	}
}

// esperarParada bloquea hasta que el bucle aparque en `Esperar`, SIN liberarlo. Al volver se sabe que la
// vuelta anterior terminó del todo y que el bucle está detenido en un punto conocido — que es lo que hace
// honesta la medición de tiempo que viene después.
func (c *avCircuito) esperarParada(t *testing.T) {
	t.Helper()
	select {
	case <-c.espia.parado:
	case <-time.After(avGuardia):
		t.Fatal("el bucle del despachador no llegó a aparcar en Esperar")
	}
}

// esperarEntrega espera UNA entrega o falla con la guardia.
func (c *avCircuito) esperarEntrega(t *testing.T, plazo time.Duration) domain.InboundEvent {
	t.Helper()
	select {
	case evt := <-c.sink.entregas:
		return evt
	case <-time.After(plazo):
		t.Fatalf("no llegó la entrega esperada en %s (entregados hasta ahora: %v)", plazo, c.sink.ids())
		return domain.InboundEvent{}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (a) EL AVISO DISPARA SOLO
// ─────────────────────────────────────────────────────────────────────────────

// TestCircuitoAviso_ConElRespaldoDesactivadoElAvisoEntregaSolo es el criterio (a) de T1.8-7: con el
// respaldo puesto en una hora —o sea, desactivado a todos los efectos—, un mensaje entrante tiene que
// llegar al sink en menos de 50 ms. Si el aviso no existiera, este test no acabaría nunca (moriría en la
// guardia), porque no hay ningún otro camino por el que el bucle pueda enterarse.
//
// EL RELOJ ES EL DE PARED A PROPÓSITO (ver la cabecera): lo que se mide es I/O real —un INSERT cifrado en
// SQLite y una entrega—, y con un reloj falso el número no significaría nada.
//
// El `esperarParada` de antes del mensaje no es decorativo: garantiza que el bucle está DORMIDO cuando
// llega el entrante. Sin él, el test podría estar midiendo el final de una vuelta que ya estaba en marcha
// y daría verde aunque el aviso no hiciera nada.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - borrar la llamada `l.avisar()` de `enqueueCola` ⇒ nadie toca el timbre y la entrega no llega jamás
//     (muere en la guardia de 5 s), con `go build` y `go vet` en verde;
//   - vaciar el cuerpo de `Listener.avisar` (o dejar sólo el `if l.aviso == nil { return }`) ⇒ idéntico;
//   - quitar el `case <-a.aviso` de `AvisoConRespaldo.Esperar` ⇒ el despertador se vuelve un poll de una
//     hora y el bucle no despierta.
func TestCircuitoAviso_ConElRespaldoDesactivadoElAvisoEntregaSolo(t *testing.T) {
	ctx := context.Background()
	c := montarCircuito(ctx, t, avRespaldoInfinito, true, nil)

	c.esperarParada(t)

	inicio := time.Now()
	if !c.listener.handleEvent(ctx, liveMessage("WA-AVISO-1", "quiero dos empanadas")) {
		t.Fatal("el entrante no se acusó: el Enqueue falló y el test no está midiendo lo que cree")
	}
	evt := c.esperarEntrega(t, avGuardia)
	transcurrido := time.Since(inicio)

	if evt.MessageID != "WA-AVISO-1" {
		t.Fatalf("llegó otra entrega (%s): el circuito no es el que el test cree", evt.MessageID)
	}
	t.Logf("criterio (a): Enqueue → Deliver en %s (respaldo desactivado: %s)", transcurrido, avRespaldoInfinito)
	if transcurrido >= 50*time.Millisecond {
		t.Errorf("Enqueue → Deliver tardó %s, el criterio (a) exige < 50 ms.\n"+
			"    CONSECUENCIA: el aviso llega pero no compensa; el camino del entrante sigue pagando espera\n"+
			"    donde la tarea prometía no pagar ninguna. Si el número es de POCO más de 50 ms, sospechar del\n"+
			"    I/O de la máquina antes que del mecanismo: la entrega incluye un INSERT cifrado en SQLite.",
			transcurrido)
	}
	if got := c.espia.despertares.Load(); got != 1 {
		t.Errorf("despertares = %d, se esperaba 1: un solo mensaje tiene que producir un solo despertar", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (b) EL RESPALDO SIGUE VIVO — y (a) no pasaba por él
// ─────────────────────────────────────────────────────────────────────────────

// TestCircuitoAviso_SinCablearElAviso_LaEntregaLlegaPorElRespaldo es el criterio (b), y es el CASO DE
// CONTROL de (a): el mismo circuito con el mismo canal, cambiando ÚNICAMENTE que el Listener nace sin
// `WithAviso`. Es la «mutación que compila» que pide la tarea, escrita como test en vez de aplicada a mano,
// para que siga custodiada mañana.
//
// AFIRMA LAS DOS MITADES, y ninguna sobra:
//   - que la entrega NO llega enseguida ⇒ (a) no estaba entregando por el respaldo ni por casualidad;
//   - que la entrega SÍ llega ⇒ el respaldo sigue siendo una red de verdad, no un adorno. Esa mitad es la
//     que sostiene que el `INSERT` siga siendo la verdad durable y el aviso sólo una pista.
//
// El margen de la primera mitad (medio respaldo) tiene holgura de sobra: el temporizador arranca cuando el
// bucle aparca, que es inmediatamente antes de que el test siembre el mensaje.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - quitar el `case <-t.C` de `AvisoConRespaldo.Esperar` ⇒ sin aviso no despierta NADIE y la entrega no
//     llega (muere en la guardia). Es la mutación que prueba que este test mira el respaldo;
//   - hacer que `montarCircuito` cablee el aviso también con `cableaAviso=false` ⇒ la entrega llega
//     inmediatamente y cae la primera mitad, que es el control de (a).
func TestCircuitoAviso_SinCablearElAviso_LaEntregaLlegaPorElRespaldo(t *testing.T) {
	ctx := context.Background()
	c := montarCircuito(ctx, t, avRespaldoCorto, false, nil)

	c.esperarParada(t)

	inicio := time.Now()
	if !c.listener.handleEvent(ctx, liveMessage("WA-RESPALDO-1", "quiero dos empanadas")) {
		t.Fatal("el entrante no se acusó: el Enqueue falló y el test no está midiendo lo que cree")
	}

	// PRIMERA MITAD: nada puede salir todavía. Aquí el `time.After` NO es una guardia, es la aserción.
	select {
	case evt := <-c.sink.entregas:
		t.Fatalf("la entrega de %s llegó a los %s, sin aviso cableado y con el respaldo en %s.\n"+
			"    CONSECUENCIA: el criterio (a) deja de significar algo — si sin cable también se entrega\n"+
			"    inmediatamente, no había forma de saber si el aviso hacía el trabajo o lo hacía otra cosa.",
			evt.MessageID, time.Since(inicio), avRespaldoCorto)
	case <-time.After(avRespaldoCorto / 2):
	}

	evt := c.esperarEntrega(t, avGuardia)
	transcurrido := time.Since(inicio)
	if evt.MessageID != "WA-RESPALDO-1" {
		t.Fatalf("llegó otra entrega (%s): el circuito no es el que el test cree", evt.MessageID)
	}
	t.Logf("criterio (b): Enqueue → Deliver en %s por el RESPALDO de %s (aviso NO cableado)",
		transcurrido, avRespaldoCorto)
	if transcurrido > 4*avRespaldoCorto {
		t.Errorf("la entrega por respaldo tardó %s, más de cuatro veces el intervalo (%s).\n"+
			"    CONSECUENCIA: el respaldo no está midiendo lo que dice, o el bucle se saltó su vencimiento.\n"+
			"    El margen es ancho a propósito (esto es reloj de pared bajo -race); un exceso así no es ruido.",
			transcurrido, avRespaldoCorto)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (c) LA RÁFAGA — el buffer 1 colapsa avisos, no filas
// ─────────────────────────────────────────────────────────────────────────────

// TestCircuitoAviso_RafagaDeVeinte_ColapsaAvisosPeroNoPierdeFilas es el criterio (c), y es el que de
// verdad justifica que el canal tenga buffer 1 y envío no bloqueante.
//
// LA PREGUNTA QUE RESPONDE: si veinte mensajes seguidos producen veinte avisos y el canal sólo puede
// guardar uno, ¿se pierden filas? No, y el porqué NO es el canal sino el bucle: el despachador drena TODO
// lo pendiente cada vez que despierta (`Run` vuelve a mirar sin pagar espera mientras haya progreso), así
// que un aviso descartado sólo cuesta una vuelta que otro aviso ya iba a hacer.
//
// 🔴 EL FRENO DEL SINK NO ES UN ADORNO: SIN ÉL ESTE TEST NO PROBABA LO QUE DICE. La primera versión encolaba
// los veinte mensajes a pelo y medía 20 encolados → 20 entregas → 20 despertares: ni un solo aviso llegó a
// colapsar, porque el bucle consumía cada uno antes de que llegara el siguiente. Es decir, el «≤ 20» salía
// verde sobre un escenario en el que el buffer nunca se llenó — cierto y vacío. Reteniendo al bucle DENTRO
// de la primera entrega, los diecinueve avisos restantes pelean de verdad por un hueco y dieciocho de ellos
// se descartan, que es exactamente la situación cuya seguridad hay que demostrar.
//
// SE AFIRMAN CUATRO COSAS, y cada una cubre un fallo distinto:
//   - VEINTE entregas ⇒ el colapso de avisos no perdió ninguna fila (el fallo caro);
//   - EN ORDEN ⇒ despertar por evento no ha roto el FIFO por `seq`, que es la invariante de la Ola 3 del
//     051 (REQ-051.18) y no depende de esta tarea, pero sería la primera víctima si el drenado se hubiera
//     reescrito como «un ítem por aviso»;
//   - ≤ 20 despertares ⇒ el tope que fija la tarea;
//   - Y ADEMÁS un puñado: el colapso OCURRIÓ. Con el bucle retenido, lo esperable son uno o dos despertares
//     para veinte filas, no veinte. Es la mitad que distingue «no se pierde nada» de «no se colapsa nada».
//
// El respaldo está desactivado (una hora): todo lo que ocurra aquí lo produce el aviso.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - dar al canal de `montarCircuito` un buffer de 64 en vez de 1 ⇒ dejan de colapsarse avisos y los
//     despertares se disparan: cae la cuarta aserción, que es la que custodia el «basta con uno»;
//   - quitar el `if progreso { continue }` de `Despachador.Run` —o sea, UNA fila por despertar en vez de
//     drenar— ⇒ con dos avisos efectivos salen dos filas y el test muere en la guardia esperando la
//     tercera. Es la mutación que demuestra que la seguridad del colapso la da el DRENADO, no el canal.
func TestCircuitoAviso_RafagaDeVeinte_ColapsaAvisosPeroNoPierdeFilas(t *testing.T) {
	const nRafaga = 20

	ctx := context.Background()
	c := montarCircuito(ctx, t, avRespaldoInfinito, true, nuevoAvSink().conFreno())

	c.esperarParada(t)

	esperados := make([]string, 0, nRafaga)
	encolar := func(i int) {
		t.Helper()
		id := fmt.Sprintf("WA-RAFAGA-%02d", i)
		esperados = append(esperados, id)
		if !c.listener.handleEvent(ctx, liveMessage(id, "quiero dos empanadas")) {
			t.Fatalf("el entrante %s no se acusó: el Enqueue falló y la ráfaga no es la que el test cree", id)
		}
	}

	inicio := time.Now()
	// El primero despierta al bucle, que se queda RETENIDO dentro de su Deliver.
	encolar(0)
	select {
	case <-c.sink.alcanzado:
	case <-time.After(avGuardia):
		t.Fatal("el bucle no llegó a entregar el primer mensaje: el freno no llegó a morder y el resto del " +
			"test no probaría el colapso de avisos")
	}

	// Y ahora los diecinueve restantes, con la certeza de que NADIE está escuchando el canal: es aquí donde
	// el buffer de uno tiene que colapsar dieciocho avisos sin perder una sola fila.
	for i := 1; i < nRafaga; i++ {
		encolar(i)
	}
	close(c.sink.freno)

	recibidos := make([]string, 0, nRafaga)
	for i := 0; i < nRafaga; i++ {
		recibidos = append(recibidos, c.esperarEntrega(t, avGuardia).MessageID)
	}
	transcurrido := time.Since(inicio)

	for i := range esperados {
		if recibidos[i] != esperados[i] {
			t.Fatalf("ORDEN ROTO en la posición %d: se entregó %s y tocaba %s (recibidos: %v).\n"+
				"    CONSECUENCIA: el despacho dejó de respetar el FIFO por `seq` (REQ-051.18) y la nube\n"+
				"    recibe la conversación desordenada, que para un carrito es la diferencia entre «dos\n"+
				"    empanadas y un jugo» y otro pedido.",
				i, recibidos[i], esperados[i], recibidos)
		}
	}
	// Y NO SALE NADA MÁS: una entrega de propina sería una RE-ENTREGA (una fila que no se selló), que el
	// cable dedupica por `wa_message_id` y por tanto en campo no se vería como un fallo sino como tráfico.
	//
	// 🔴 ESTA VENTANA HACE ADEMÁS DE ASENTAMIENTO, Y SIN ELLA EL RECUENTO DE ABAJO NO PROBABA NADA. La
	// primera versión leía `despertares` en cuanto tenía las veinte entregas, es decir, ANTES de que el
	// bucle hubiera consumido los avisos que quedaran pendientes: con el canal saboteado a buffer 64 el
	// contador seguía diciendo 1 en ese instante —los otros diecinueve despertares en vacío llegaban
	// después— y la mutación salía VERDE. Se comprobó ejecutándola. Con la ventana por delante, el bucle
	// ya ha quemado lo que tuviera pendiente cuando se lee el contador.
	select {
	case evt := <-c.sink.entregas:
		t.Errorf("entrega de más (%s) tras las %d de la ráfaga: alguna fila se está RE-ENTREGANDO",
			evt.MessageID, nRafaga)
	case <-time.After(300 * time.Millisecond):
	}

	despertares := c.espia.despertares.Load()
	t.Logf("criterio (c): %d encolados → %d entregas en %s con %d despertares (el buffer 1 colapsó %d avisos)",
		nRafaga, len(recibidos), transcurrido, despertares, int64(nRafaga)-despertares)
	if despertares > nRafaga {
		t.Errorf("despertares = %d para %d encolados; el criterio (c) exige ≤ %d.\n"+
			"    CONSECUENCIA: el buffer 1 no está colapsando nada y el bucle despierta más veces de las que\n"+
			"    hay trabajo — el mecanismo habría degenerado en una espera activa por sesión.",
			despertares, nRafaga, nRafaga)
	}
	// EL COLAPSO, MEDIDO. Con el bucle retenido durante toda la ráfaga sólo puede haber uno o dos
	// despertares: el que lo sacó de su primera espera y, como mucho, el del aviso que quedara en el buffer
	// al vaciarse la cola. El tope de 3 es holgura para el planificador, no incertidumbre sobre el mecanismo.
	if despertares > 3 {
		t.Errorf("despertares = %d con el bucle RETENIDO durante toda la ráfaga: se esperaban 1 ó 2.\n"+
			"    CONSECUENCIA: los avisos no se están colapsando —el canal se comporta como si tuviera cola—,\n"+
			"    así que el «basta con uno» del diseño dejó de ser cierto y cada entrante paga su propio\n"+
			"    despertar aunque el bucle ya estuviera drenando.", despertares)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (d) LO QUE ESCRIBE OTRA CONEXIÓN — para eso existe el respaldo
// ─────────────────────────────────────────────────────────────────────────────

// TestCircuitoAviso_FilaDeOtraConexion_SalePorElRespaldo es el criterio (d), y es el que FIJA EL NÚMERO
// del respaldo: es el único caso en el que ese intervalo es la latencia real de una fila, porque no hay
// canal por el que avisar.
//
// EL ESCENARIO ES `cmd/colaseed` (y cualquier otro escritor externo de la cola): un proceso APARTE abre el
// mismo `cola_entrantes.db` con su propio `db.OpenCola` y su propio `*Store`, e inserta. El aviso es
// intra-proceso por construcción —es un canal de Go—, así que esa fila no puede despertar a nadie. Aquí se
// reproduce con una segunda conexión al mismo fichero, que es exactamente lo que colaseed hace.
//
// 🔴 POR QUÉ ESTO NO ES UN AGUJERO SINO EL DISEÑO: el `INSERT` sigue siendo la verdad durable. El aviso
// acelera lo que pasa por el listener; el respaldo garantiza que TODO lo demás sale igual, sólo que más
// tarde. La tarea pide medir ese «más tarde», y es lo que imprime el `t.Logf` de abajo.
//
// El test corre con `avRespaldoCorto` y no con el número de producción por la razón escrita en esa
// constante (un test contra su propia constante no prueba nada); lo que se afirma es la CONDUCTA —sale por
// el respaldo y no antes—, que es independiente del valor.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas):
//   - quitar el `case <-t.C` de `AvisoConRespaldo.Esperar` ⇒ la fila de la otra conexión NO SALE NUNCA;
//     es el modo de fallo que este criterio existe para impedir, y en campo significaría que `colaseed`
//     —el instrumento con el que se miden las pruebas de campo— deja de producir carga sin dar un error.
func TestCircuitoAviso_FilaDeOtraConexion_SalePorElRespaldo(t *testing.T) {
	ctx := context.Background()
	c := montarCircuito(ctx, t, avRespaldoCorto, true, nil)

	// SEGUNDA CONEXIÓN al MISMO fichero, por el mismo camino de producción que usa colaseed: `db.OpenCola`
	// (los pragmas son POR CONEXIÓN) y `colaentrantes.New` con el mismo resolutor de DEK, que es lo único
	// que garantiza que la fila se sella con la llave con la que el despachador la va a abrir.
	otra, err := infradb.OpenCola(ctx, c.ruta)
	if err != nil {
		t.Fatalf("segunda apertura de la cola (el escenario de colaseed): %v", err)
	}
	t.Cleanup(func() { _ = otra.Close() })
	colaExterna, err := colaentrantes.New(ctx, otra, avCrypterFor, 100, 24, quietLogger())
	if err != nil {
		t.Fatalf("colaentrantes.New sobre la segunda conexión: %v", err)
	}

	c.esperarParada(t)

	inicio := time.Now()
	if err := colaExterna.Enqueue(ctx, app.ColaItem{
		SessionID:   avSesion,
		ChatJID:     "colaseed-t187-0001@s.whatsapp.net",
		WAMessageID: "WA-EXTERNA-1",
		TSWhatsApp:  time.Now().Unix(),
		Texto:       "quiero dos empanadas",
		Estado:      app.EstadoNuevo,
	}); err != nil {
		t.Fatalf("Enqueue desde la segunda conexión: %v", err)
	}

	// Nadie ha tocado el timbre: por aquí no puede salir antes del respaldo.
	select {
	case evt := <-c.sink.entregas:
		t.Fatalf("la fila externa (%s) salió a los %s, antes del respaldo (%s): el test no está probando el\n"+
			"    respaldo sino otra cosa — ¿alguien avisó por el canal?", evt.MessageID, time.Since(inicio), avRespaldoCorto)
	case <-time.After(avRespaldoCorto / 2):
	}

	evt := c.esperarEntrega(t, avGuardia)
	transcurrido := time.Since(inicio)
	if evt.MessageID != "WA-EXTERNA-1" {
		t.Fatalf("llegó otra entrega (%s): el circuito no es el que el test cree", evt.MessageID)
	}
	t.Logf("criterio (d): fila de OTRA conexión entregada en %s con un respaldo de %s", transcurrido, avRespaldoCorto)
	if transcurrido > 4*avRespaldoCorto {
		t.Errorf("la fila externa tardó %s, más de cuatro veces el respaldo (%s).\n"+
			"    CONSECUENCIA: el número de producción se fija midiendo ESTO (criterio (e)); si la relación\n"+
			"    real no es la que el respaldo promete, el 5 s escrito en `DefaultRespaldoMS` es una ficción.",
			transcurrido, avRespaldoCorto)
	}
}
