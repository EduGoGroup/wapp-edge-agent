package colaentrantes

// contadores_relevo_test.go — EL RELEVO, CONTABLE (Plan 051 Ola 2, arreglo de observabilidad).
//
// Nace de una auditoría con dos hallazgos sobre el mismo sitio:
//
//   (a) el único testigo de un relevo por lease eran dos líneas de Warn y un error devuelto. Un log se
//       rota y se pierde, y el worker no puede contar lo que el barrido hace por su cuenta (sobre filas
//       de otro proceso, quizá ya muerto). De ahí RescatadasPorLease y CierresDescartadosPorFence.
//   (b) el `claim_token` que se logueaba para poder emparejar el descarte con su claim son 32 hex, y el
//       scrubber del bundle de diagnóstico redacta a partir de 32 hex: en el sitio donde alguien lee esos
//       logs, el campo salía [REDACTED]. De ahí `claim_token_pref`.
//
// Reutiliza los helpers de colaentrantes_test.go y claim_test.go (openDB, newStore, sembrarTomado,
// fakeClock, resumenLote): mismo paquete a propósito.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/diagnostics"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// newStoreConLog construye un Store cuyo log se puede LEER (JSON en un buffer, nivel Debug para que
// también entre el "lote reclamado" de Reclamar). newStore corriente descarta el log, que es lo correcto
// para el resto de tests; aquí el log ES el sujeto de la prueba.
func newStoreConLog(t *testing.T, db *sql.DB, cf CrypterFor, maxRows, ttlHours int, buf *bytes.Buffer, opts ...Option) *Store {
	t.Helper()
	log := sharedlogger.New(
		sharedlogger.WithWriter(buf),
		sharedlogger.WithJSON(true),
		sharedlogger.WithLevel(slog.LevelDebug),
	)
	s, err := New(context.Background(), db, cf, maxRows, ttlHours, log, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// buscarLineaLog devuelve la línea de log (cruda y ya parseada) cuyo `msg` contiene el fragmento dado.
//
// El mensaje de fallo lista SOLO los `msg`, nunca el log entero: volcar el buffer completo en un t.Fatalf
// es el patrón que acaba copiado a sitios donde sí hay PII (INV-051.1).
func buscarLineaLog(t *testing.T, buf *bytes.Buffer, fragmento string) (string, map[string]any) {
	t.Helper()
	var vistos []string
	for _, cruda := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if strings.TrimSpace(cruda) == "" {
			continue
		}
		var campos map[string]any
		if err := json.Unmarshal([]byte(cruda), &campos); err != nil {
			t.Fatalf("una línea de log no es JSON parseable: %v", err)
		}
		msg, _ := campos["msg"].(string)
		vistos = append(vistos, msg)
		if strings.Contains(msg, fragmento) {
			return cruda, campos
		}
	}
	t.Fatalf("no se encontró ninguna línea de log con %q; mensajes vistos: %q", fragmento, vistos)
	return "", nil
}

// campoString lee un campo de una línea de log como string, fallando si no está o no es string.
func campoString(t *testing.T, campos map[string]any, clave string) string {
	t.Helper()
	v, ok := campos[clave]
	if !ok {
		t.Fatalf("la línea de log no lleva el campo %q", clave)
	}
	s, ok := v.(string)
	if !ok {
		t.Fatalf("el campo %q no es string, es %T", clave, v)
	}
	return s
}

// sembrarLoteRelevado monta la carrera completa: el cajero A reclama, su lease vence, el barrido rescata
// las filas y el cajero B las re-reclama SIN cerrar. Devuelve los dos lotes. Es la misma coreografía de
// TestMarcarClasificadoTardioConElRelevoAUNVIVO, extraída para no repetirla en cada test de contador.
//
// ⚠️ EL ORDEN `(t, ctx, …)` ES EL DEL REPO, no un descuido, y no lo cambies en este fichero suelto: los 11
// helpers de test que reciben ambos (db, cryptostore, cloudlink, este paquete) lo hacen así, sin una sola
// excepción. `revive:context-as-argument` pediría `ctx` primero, pero revive NO está activo aquí —el repo
// no tiene .golangci.yml, así que el gate «Lint» del CI corre golangci-lint con sus linters por defecto, y
// revive no está entre ellos—. Si algún día se adopta revive, esto se cambia en TODOS los helpers a la vez
// (o se configura `allowTypesBefore: "*testing.T"`, que es la opción que la propia regla trae para esto).
func sembrarLoteRelevado(t *testing.T, ctx context.Context, s *Store, clock *fakeClock, filas int) (loteA, loteB *app.ColaLote) {
	t.Helper()
	for i := 1; i <= filas; i++ {
		if err := s.Enqueue(ctx, item("A", "chat@s", fmt.Sprintf("wa-%d", i), fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	loteA, err := s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar (cajero A): %v", err)
	}
	if loteA == nil || len(loteA.Mensajes) != filas {
		t.Fatalf("el cajero A debía llevarse %d filas, got %s", filas, resumenLote(loteA))
	}

	clock.t = clock.t.Add(61 * time.Second)
	n, err := s.BarrerLeasesVencidos(ctx, 0)
	if err != nil {
		t.Fatalf("BarrerLeasesVencidos: %v", err)
	}
	if n != int64(filas) {
		t.Fatalf("el barrido debía rescatar %d filas, rescató %d", filas, n)
	}

	loteB, err = s.Reclamar(ctx, 0)
	if err != nil {
		t.Fatalf("Reclamar (cajero B, el relevo): %v", err)
	}
	if loteB == nil || len(loteB.Mensajes) != filas {
		t.Fatalf("el cajero B debía re-reclamar %d filas, got %s", filas, resumenLote(loteB))
	}
	return loteA, loteB
}

// ─────────────────────────── (a) los contadores ───────────────────────────

// TestRescatadasPorLeaseCuentaLasFilasRealmenteRescatadas: el acumulado suma las filas AFECTADAS por el
// barrido, no las candidatas ni el tope. La fila con lease VIVO no cuenta, y un segundo barrido sin nada
// que rescatar no mueve el número (un contador que sube en cada pasada no distingue una flota sana de una
// que está perdiendo cajeros).
func TestRescatadasPorLeaseCuentaLasFilasRealmenteRescatadas(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	ahora := clock.t.Unix()

	sembrarTomado(t, db, 1, 10, "A", "chat-muerto", "wa-1", "m1", ahora-61)
	sembrarTomado(t, db, 2, 20, "A", "chat-muerto", "wa-2", "m2", ahora-61)
	sembrarTomado(t, db, 3, 30, "B", "chat-vivo", "wa-3", "m3", ahora-10) // lease VIVO: no se toca

	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0, WithClock(clock.Now))
	if got := s.RescatadasPorLease(); got != 0 {
		t.Fatalf("el acumulado debía arrancar en 0, got %d", got)
	}

	n, err := s.BarrerLeasesVencidos(ctx, 0)
	if err != nil {
		t.Fatalf("BarrerLeasesVencidos: %v", err)
	}
	if n != 2 {
		t.Fatalf("filas rescatadas: got %d want 2", n)
	}
	if got := s.RescatadasPorLease(); got != 2 {
		t.Fatalf("RescatadasPorLease: got %d want 2 (las filas realmente rescatadas)", got)
	}

	// Segunda pasada: ya no queda nada `tomado` vencido.
	n2, err := s.BarrerLeasesVencidos(ctx, 0)
	if err != nil {
		t.Fatalf("BarrerLeasesVencidos (2.ª): %v", err)
	}
	if n2 != 0 {
		t.Fatalf("la 2.ª pasada no debía rescatar nada, rescató %d", n2)
	}
	if got := s.RescatadasPorLease(); got != 2 {
		t.Fatalf("RescatadasPorLease tras un barrido en vacío: got %d want 2 (el acumulado no se mueve)", got)
	}
}

// TestCierresDescartadosPorFenceCuentaLosLotesTirados: el contador cuenta CIERRES (lotes), que es la
// unidad que duele —cada uno es una inferencia pagada y tirada—, y NO se incrementa en el camino feliz.
func TestCierresDescartadosPorFenceCuentaLosLotesTirados(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStore(t, db, newFakeCrypterFor().fn, 100, 0, WithClock(clock.Now))

	loteA, loteB := sembrarLoteRelevado(t, ctx, s, clock, 2)
	if got := s.CierresDescartadosPorFence(); got != 0 {
		t.Fatalf("el acumulado debía seguir en 0 antes del cierre tardío, got %d", got)
	}

	// A cierra tarde sobre un lote que ya es de B: el fence muerde.
	err := s.MarcarClasificado(ctx, loteA, `{"intent":"saludo"}`)
	if !errors.Is(err, app.ErrLoteRelevado) {
		t.Fatalf("el cierre tardío debía devolver app.ErrLoteRelevado; got %v", err)
	}
	// UNO, no dos: la unidad es el LOTE, aunque el lote llevara 2 filas.
	if got := s.CierresDescartadosPorFence(); got != 1 {
		t.Fatalf("CierresDescartadosPorFence tras un cierre relevado: got %d want 1 (se cuentan lotes, no filas)", got)
	}

	// El dueño cierra bien: el camino feliz NO toca el contador.
	if err := s.MarcarClasificado(ctx, loteB, `{"intent":"pedido"}`); err != nil {
		t.Fatalf("el dueño del lote debía poder cerrar: %v", err)
	}
	if got := s.CierresDescartadosPorFence(); got != 1 {
		t.Fatalf("un cierre correcto no debe incrementar el contador: got %d want 1", got)
	}
	// Y el otro contador es independiente: el barrido rescató las 2 filas del lote de A.
	if got := s.RescatadasPorLease(); got != 2 {
		t.Fatalf("RescatadasPorLease: got %d want 2 (las dos magnitudes se cuentan aparte)", got)
	}
}

// ─────────────────────────── (b) el token que se autocensuraba ───────────────────────────

// TestClaimTokenEnteroLoRedactaElScrubber es la prueba de que el problema era REAL y no una sospecha: un
// token de fencing completo, tal cual se logueaba antes, desaparece del bundle de diagnóstico.
//
// Si este test se pusiera en verde por su cuenta (porque alguien cambió secretPattern o el tamaño del
// token), la mitad de las decisiones de claimTokenPref habría que revisarlas — de ahí que la aserción sea
// «SÍ se redacta», y no un comentario.
func TestClaimTokenEnteroLoRedactaElScrubber(t *testing.T) {
	token, err := nuevoClaimToken()
	if err != nil {
		t.Fatalf("nuevoClaimToken: %v", err)
	}
	if len(token) != claimTokenBytes*2 {
		t.Fatalf("el token debía tener %d caracteres hex, tiene %d", claimTokenBytes*2, len(token))
	}
	if got := diagnostics.Scrub(token); strings.Contains(got, token) {
		t.Fatalf("el token entero debía quedar redactado en un bundle de diagnóstico, salió %q", got)
	}
}

// TestClaimTokenPrefSobreviveAlScrub: el prefijo, en cambio, pasa el scrubber intacto — que es toda la
// razón de su existencia. Se prueba sobre la línea de log COMPLETA, no sobre el valor suelto: lo que el
// bundle scrubbea es la línea, y es ahí donde una clave y un valor pegados podrían formar la racha de 40
// caracteres de la segunda rama del patrón.
func TestClaimTokenPrefSobreviveAlScrub(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	var buf bytes.Buffer
	s := newStoreConLog(t, db, newFakeCrypterFor().fn, 100, 0, &buf, WithClock(clock.Now))

	loteA, _ := sembrarLoteRelevado(t, ctx, s, clock, 2)
	if err := s.MarcarClasificado(ctx, loteA, `{"intent":"saludo"}`); !errors.Is(err, app.ErrLoteRelevado) {
		t.Fatalf("el cierre tardío debía devolver app.ErrLoteRelevado; got %v", err)
	}

	crudaWarn, warn := buscarLineaLog(t, &buf, "este cierre se DESCARTA entero")
	pref := campoString(t, warn, "claim_token_pref")

	// 1. LA LONGITUD, que es lo que decide si el scrubber muerde.
	if len(pref) != claimTokenPrefLen {
		t.Fatalf("claim_token_pref: %d caracteres, esperaba %d (por debajo de los 32 hex de secretPattern)",
			len(pref), claimTokenPrefLen)
	}
	// 2. Es el prefijo del token REAL: si no, no correlaciona con nada.
	if pref != loteA.ClaimToken[:claimTokenPrefLen] {
		t.Fatalf("claim_token_pref no es el prefijo del token del claim")
	}
	// 3. El token ENTERO no aparece en la línea (si apareciera, el Scrub redactaría media línea).
	if strings.Contains(crudaWarn, loteA.ClaimToken) {
		t.Fatal("la línea de Warn lleva el claim_token ENTERO: el scrubber la redactaría")
	}
	// 4. Y la línea entera, pasada por el scrubber del bundle, CONSERVA el prefijo.
	if scrubbed := diagnostics.Scrub(crudaWarn); !strings.Contains(scrubbed, pref) {
		t.Fatalf("el prefijo NO sobrevivió al Scrub de la línea de Warn: se perdió el único campo que correlaciona")
	}

	// El Debug del claim lleva el mismo campo: sin él, el prefijo del Warn no empareja con nada.
	crudaClaim, claim := buscarLineaLog(t, &buf, "lote reclamado")
	prefClaim := campoString(t, claim, "claim_token_pref")
	if len(prefClaim) != claimTokenPrefLen {
		t.Fatalf("claim_token_pref del Debug de Reclamar: %d caracteres, esperaba %d", len(prefClaim), claimTokenPrefLen)
	}
	if prefClaim != loteA.ClaimToken[:claimTokenPrefLen] {
		t.Fatalf("el primer 'lote reclamado' debía llevar el prefijo del claim de A")
	}
	if scrubbed := diagnostics.Scrub(crudaClaim); !strings.Contains(scrubbed, prefClaim) {
		t.Fatalf("el prefijo del Debug de Reclamar no sobrevivió al Scrub")
	}
}

// TestClaimTokenPrefConTokenCorto: guardarraíl del helper. Si alguien bajara claimTokenBytes por debajo del
// prefijo, claimTokenPref no debe cortar fuera de rango (ni entrar en pánico): devuelve lo que hay.
func TestClaimTokenPrefConTokenCorto(t *testing.T) {
	for _, caso := range []struct{ in, want string }{
		{"", ""},
		{"abc", "abc"},
		{"1234567", "1234567"},
		{"12345678", "12345678"},
		{"123456789abcdef", "12345678"},
	} {
		if got := claimTokenPref(caso.in); got != caso.want {
			t.Errorf("claimTokenPref(%q) = %q, want %q", caso.in, got, caso.want)
		}
	}
}

// ─────────────────── (c) el chat_jid que se colaba por dentro de los errores ───────────────────

// jidDeCampo es un chat_jid con la FORMA REAL de producción, y por eso no vale «chat@s» aquí: lo que estos
// tests persiguen es el TELÉFONO, y un fixture sin dígitos pasaría cualquier aserción sin probar nada.
const jidDeCampo = "593999123456@s.whatsapp.net"

// telefonoDeCampo es la parte que de verdad duele: el número, sin el sufijo del servidor. Se comprueba
// aparte porque un log podría partir el JID (truncarlo, quedarse con el nodo) y seguir filtrando el número.
const telefonoDeCampo = "593999123456"

// exigirSinJID falla si un texto (una línea de log, un error) lleva el JID o el teléfono en claro. El
// mensaje de fallo NO vuelca el texto entero: si el test caza una fuga real, volcarla la duplicaría en la
// salida del CI.
func exigirSinJID(t *testing.T, que, texto string) {
	t.Helper()
	if strings.Contains(texto, jidDeCampo) {
		t.Fatalf("%s lleva el chat_jid ENTERO en claro (INV-051.1)", que)
	}
	if strings.Contains(texto, telefonoDeCampo) {
		t.Fatalf("%s lleva el TELÉFONO del cliente en claro, aunque no sea el JID completo (INV-051.1)", que)
	}
}

// TestNingunLogDelCajeroLlevaElChatJIDEnClaro recorre el camino ENTERO del cajero —claim, barrido, relevo y
// cierre descartado— con un JID realista y exige que el buffer de log no contenga el número por ninguna
// parte, y que en su lugar viaje el `chat_jid_hash`.
//
// 🔴 ES UN TEST DE REGRESIÓN DE UNA FUGA REAL: los `fmt.Errorf` de MarcarClasificado embebían
// `chat_jid=%s` completo, y el cajero loguea ese error tal cual, así que el teléfono del cliente acababa en
// el log local (y de ahí, en el bundle de diagnóstico que se comparte con soporte). No era el texto del
// mensaje, así que la letra de INV-051.1 no lo nombraba; el espíritu sí.
func TestNingunLogDelCajeroLlevaElChatJIDEnClaro(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	var buf bytes.Buffer
	s := newStoreConLog(t, db, newFakeCrypterFor().fn, 100, 0, &buf, WithClock(clock.Now))

	for i := 1; i <= 2; i++ {
		if err := s.Enqueue(ctx, item("A", jidDeCampo, fmt.Sprintf("wa-%d", i), fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	loteA, err := s.Reclamar(ctx, 0) // Debug "lote reclamado"
	if err != nil || loteA == nil {
		t.Fatalf("Reclamar (cajero A): lote=%s err=%v", resumenLote(loteA), err)
	}
	clock.t = clock.t.Add(61 * time.Second)
	if _, err := s.BarrerLeasesVencidos(ctx, 0); err != nil { // Warn del barrido
		t.Fatalf("BarrerLeasesVencidos: %v", err)
	}
	if _, err := s.Reclamar(ctx, 0); err != nil { // el relevo se lleva las filas
		t.Fatalf("Reclamar (cajero B): %v", err)
	}
	// El cierre tardío: Warn "DESCARTA entero" + el error que el worker loguea tal cual.
	errCierre := s.MarcarClasificado(ctx, loteA, `{"intent":"saludo"}`)
	if !errors.Is(errCierre, app.ErrLoteRelevado) {
		t.Fatalf("el cierre tardío debía devolver app.ErrLoteRelevado; got %v", errCierre)
	}

	exigirSinJID(t, "el log del cajero", buf.String())
	exigirSinJID(t, "el error de MarcarClasificado", errCierre.Error())

	// Y lo que SÍ tiene que estar: el hash, el mismo en el Debug del claim y en el Warn del descarte (si no
	// fueran el mismo valor, no se podría correlacionar y el arreglo habría cambiado una fuga por un dato inútil).
	_, claim := buscarLineaLog(t, &buf, "lote reclamado")
	_, warn := buscarLineaLog(t, &buf, "este cierre se DESCARTA entero")
	hClaim := campoString(t, claim, "chat_jid_hash")
	hWarn := campoString(t, warn, "chat_jid_hash")
	if hClaim != chatJIDHash(jidDeCampo) || hWarn != hClaim {
		t.Fatalf("chat_jid_hash: claim=%q warn=%q, esperaba los dos = %q", hClaim, hWarn, chatJIDHash(jidDeCampo))
	}
}

// TestErrorDeReclamarNoLlevaElChatJID cubre el OTRO error del fichero que lo embebía: el de resolver el
// sobre del lote. Se llega por el camino de la custodia caída, sembrando las filas a mano (Enqueue no
// serviría: fallaría antes, en el sellado).
func TestErrorDeReclamarNoLlevaElChatJID(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	sembrarNuevo(t, db, 1, 10, "A", jidDeCampo, "wa-1", "m1")

	var buf bytes.Buffer
	s := newStoreConLog(t, db, (&failingCrypterFor{falla: true}).fn, 100, 0, &buf)

	lote, err := s.Reclamar(ctx, 0)
	if err == nil {
		t.Fatalf("Reclamar con la custodia caída debía fallar; devolvió %s", resumenLote(lote))
	}
	exigirSinJID(t, "el error de Reclamar", err.Error())
	exigirSinJID(t, "el log de Reclamar", buf.String())
	// El session_id sigue estando: es lo que hace el error diagnosticable sin PII.
	if !strings.Contains(err.Error(), "session_id=A") {
		t.Fatalf("el error debía conservar el session_id para poder diagnosticar: %v", err)
	}
}

// TestChatJIDHashNoFiltraElNumeroYSobreviveAlScrub fija las tres propiedades del helper: no conserva nada
// del original, distingue conversaciones, y pasa el scrubber del bundle (que es donde estos logs se leen).
func TestChatJIDHashNoFiltraElNumeroYSobreviveAlScrub(t *testing.T) {
	h := chatJIDHash(jidDeCampo)
	if len(h) != chatJIDHashLen {
		t.Fatalf("chat_jid_hash: %d caracteres, esperaba %d (por debajo de los 32 hex de secretPattern)", len(h), chatJIDHashLen)
	}
	// 1. No es un prefijo disfrazado: nada del JID sobrevive. Un prefijo del JID sería el prefijo de país +
	//    operadora, justo lo que no puede salir.
	if strings.Contains(jidDeCampo, h) {
		t.Fatalf("chat_jid_hash %q aparece dentro del propio JID: no es un hash, es un trozo del dato", h)
	}
	// 2. Es estable (dos líneas del mismo log tienen que emparejar)…
	if chatJIDHash(jidDeCampo) != h {
		t.Fatal("chat_jid_hash no es estable: dos líneas de log de la misma conversación no emparejarían")
	}
	// 3. …y distingue conversaciones (si no, no serviría para nada en el Warn del cinturón de seguridad).
	if chatJIDHash("593999999999@s.whatsapp.net") == h {
		t.Fatal("dos conversaciones distintas dieron el mismo chat_jid_hash")
	}
	// 4. Y sobrevive al Scrub, por la misma razón que claimTokenPref.
	linea := fmt.Sprintf(`{"msg":"lote reclamado","chat_jid_hash":"%s"}`, h)
	if scrubbed := diagnostics.Scrub(linea); !strings.Contains(scrubbed, h) {
		t.Fatalf("el chat_jid_hash NO sobrevivió al Scrub: se perdería el único campo que identifica la conversación")
	}
	// El JID vacío (fila sin conversación, imposible por DDL pero barato de cubrir) no inventa un hash.
	if got := chatJIDHash(""); got != "" {
		t.Fatalf("chatJIDHash(\"\") = %q, want \"\"", got)
	}
}
