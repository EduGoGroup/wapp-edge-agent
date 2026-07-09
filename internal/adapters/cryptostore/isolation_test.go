package cryptostore

// isolation_test.go prueba el AISLAMIENTO PER-DEVICE del cifrado sobre una BD ÚNICA COMPARTIDA (Plan 022
// §3/§10.B, decisión A): N dispositivos comparten una sola *sql.DB, cada uno con SU DEK (envelope
// enlazado en construcción vía OpenDeviceContainer → NewEncryptedContainer). CERO DEK global; las tablas
// msg_enc_* se llavean por JID/our_jid del device (sin session_id). Vectores obligatorios del corte:
//   - ≥2 devices en UNA BD (incluido un 2º device del MISMO número, JID distinto): cada uno descifra
//     SOLO con su DEK;
//   - cruzar DEKs DEBE fallar (fila device y store de sesión, ciphertext crudo);
//   - borrar/quemar un device NO afecta a otros;
//   - la DEK jamás aparece en la BD (no legible para la nube, §3 / ADR-0007).

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/EduGoGroup/wapp-shared/envelope"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// syntheticDeviceWithJID fabrica un device sintético y le fija el JID dado (número + índice de device),
// para poblar varios devices DISTINTOS —incluido un 2º device del MISMO número (JID distinto)— en la
// misma BD compartida.
func syntheticDeviceWithJID(t *testing.T, jid types.JID) *store.Device {
	t.Helper()
	dev, _ := syntheticDevice(t)
	j := jid
	dev.ID = &j
	return dev
}

// sharedContainer construye el Container cifrado de UN device (SU DEK) sobre la BD COMPARTIDA db, sin
// poseer su ciclo de vida (lo cierra el test). Es la vía N-devices-sobre-1-BD del corte (OpenDeviceContainer).
func sharedContainer(t *testing.T, ctx context.Context, db *sql.DB, dek []byte) store.DeviceContainer {
	t.Helper()
	c, err := OpenDeviceContainer(ctx, db, DialectSQLite, dek)
	if err != nil {
		t.Fatalf("OpenDeviceContainer: %v", err)
	}
	return c
}

// TestSharedDB_PerDeviceIsolation: tres devices sobre la MISMA BD, cada uno con SU DEK —incluido un 2º
// device del MISMO número (JID distinto) y uno de número distinto—. Cada device descifra su material
// (NoiseKey + sesión) SOLO con su propia DEK; los tres coexisten sin pisarse.
func TestSharedDB_PerDeviceIsolation(t *testing.T) {
	ctx := context.Background()
	db := openAt(t, filepath.Join(t.TempDir(), "shared.db"))
	defer func() { _ = db.Close() }()

	// A1 = número N1, device 0 (primer device del número)
	// A2 = número N1, device 1 (SEGUNDO device del MISMO número: JID distinto)
	// C  = número N2, device 0 (número distinto)
	dekA1, dekA2, dekC := newDEK(t), newDEK(t), newDEK(t)

	jidA1 := types.NewJID("15551110001", types.DefaultUserServer)
	jidA1.Device = 0
	jidA2 := types.NewJID("15551110001", types.DefaultUserServer)
	jidA2.Device = 1
	jidC := types.NewJID("15559990002", types.DefaultUserServer)
	jidC.Device = 0

	// JID distintos garantizan filas msg_enc_* separadas incluso para el 2º device del MISMO número.
	if jidA1.String() == jidA2.String() {
		t.Fatalf("precondición: el 2º device del mismo número debe tener JID distinto (%s)", jidA1.String())
	}

	devA1 := syntheticDeviceWithJID(t, jidA1)
	devA2 := syntheticDeviceWithJID(t, jidA2)
	devC := syntheticDeviceWithJID(t, jidC)
	noiseA1 := *devA1.NoiseKey.Priv
	noiseA2 := *devA2.NoiseKey.Priv
	noiseC := *devC.NoiseKey.Priv

	contA1 := sharedContainer(t, ctx, db, dekA1)
	contA2 := sharedContainer(t, ctx, db, dekA2)
	contC := sharedContainer(t, ctx, db, dekC)

	// Persistir cada device con SU DEK y sembrar una sesión propia (cifrada con SU DEK, llaveada por su JID).
	seed := func(cont store.DeviceContainer, dev *store.Device, sess []byte) {
		t.Helper()
		if err := cont.PutDevice(ctx, dev); err != nil {
			t.Fatalf("PutDevice(%s): %v", dev.ID, err)
		}
		if err := dev.Sessions.PutSession(ctx, "peer:0", sess); err != nil {
			t.Fatalf("PutSession(%s): %v", dev.ID, err)
		}
	}
	sessA1 := []byte("sesion-A1-secreta")
	sessA2 := []byte("sesion-A2-secreta")
	sessC := []byte("sesion-C-secreta")
	seed(contA1, devA1, sessA1)
	seed(contA2, devA2, sessA2)
	seed(contC, devC, sessC)

	// Cada device se recupera con SU DEK: NoiseKey + sesión sobreviven el round-trip.
	check := func(cont store.DeviceContainer, jid types.JID, wantNoise [32]byte, wantSess []byte) {
		t.Helper()
		got, err := LoadDevice(ctx, cont, jid)
		if err != nil {
			t.Fatalf("LoadDevice(%s): %v", jid, err)
		}
		if got == nil {
			t.Fatalf("LoadDevice(%s) devolvió nil", jid)
		}
		if *got.NoiseKey.Priv != wantNoise {
			t.Errorf("%s: NoiseKey no sobrevivió el round-trip con su DEK", jid)
		}
		gotSess, err := got.Sessions.GetSession(ctx, "peer:0")
		if err != nil {
			t.Fatalf("GetSession(%s): %v", jid, err)
		}
		if !bytes.Equal(gotSess, wantSess) {
			t.Errorf("%s: la sesión no sobrevivió el round-trip con su DEK", jid)
		}
	}
	check(contA1, jidA1, noiseA1, sessA1)
	check(contA2, jidA2, noiseA2, sessA2)
	check(contC, jidC, noiseC, sessC)
}

// TestSharedDB_CrossDEK_MustFail es el VECTOR EXPLÍCITO de cruce: sobre una BD compartida con A y B (cada
// uno con SU DEK), la DEK de A NO descifra el material de B ni viceversa —ni la fila device (msg_enc_device)
// ni el store de sesión (msg_enc_sessions), probado también contra el ciphertext CRUDO—. Demuestra que no
// hay DEK global: la DEK de un device no abre las filas de otro (GCM no autentica).
func TestSharedDB_CrossDEK_MustFail(t *testing.T) {
	ctx := context.Background()
	db := openAt(t, filepath.Join(t.TempDir(), "shared.db"))
	defer func() { _ = db.Close() }()

	dekA, dekB := newDEK(t), newDEK(t)

	jidA := types.NewJID("15551110001", types.DefaultUserServer)
	jidA.Device = 0
	jidB := types.NewJID("15559990002", types.DefaultUserServer)
	jidB.Device = 0

	devA := syntheticDeviceWithJID(t, jidA)
	devB := syntheticDeviceWithJID(t, jidB)

	contA := sharedContainer(t, ctx, db, dekA)
	contB := sharedContainer(t, ctx, db, dekB)

	if err := contA.PutDevice(ctx, devA); err != nil {
		t.Fatalf("PutDevice A: %v", err)
	}
	if err := contB.PutDevice(ctx, devB); err != nil {
		t.Fatalf("PutDevice B: %v", err)
	}
	sessB := []byte("sesion-de-B-que-A-no-debe-abrir")
	if err := devB.Sessions.PutSession(ctx, "peer:0", sessB); err != nil {
		t.Fatalf("PutSession B: %v", err)
	}

	// --- Vector 1 (fila device): la DEK de A NO abre la fila msg_enc_device de B (y viceversa). ---
	if _, err := LoadDevice(ctx, contA, jidB); err == nil {
		t.Fatal("cross-DEK: la DEK de A abrió la fila device de B (DEBÍA fallar por GCM)")
	}
	if _, err := LoadDevice(ctx, contB, jidA); err == nil {
		t.Fatal("cross-DEK: la DEK de B abrió la fila device de A (DEBÍA fallar por GCM)")
	}

	// --- Vector 2 (store de sesión, ciphertext CRUDO): la DEK de A NO abre la sesión de B (llaveada por
	//     su JID); solo la de B. Prueba directa de la ausencia de DEK global a nivel de store. ---
	envA, err := envelope.NewEnvelope(dekA)
	if err != nil {
		t.Fatal(err)
	}
	envB, err := envelope.NewEnvelope(dekB)
	if err != nil {
		t.Fatal(err)
	}
	var ct []byte
	if err := db.QueryRowContext(ctx,
		`SELECT session FROM msg_enc_sessions WHERE our_jid=? AND their_id=?`,
		jidB.String(), "peer:0").Scan(&ct); err != nil {
		t.Fatalf("leer ciphertext de la sesión de B: %v", err)
	}
	if _, err := envA.Open(ct); err == nil {
		t.Fatal("cross-DEK: la DEK de A abrió una sesión cifrada con la DEK de B (fuga)")
	}
	pt, err := envB.Open(ct)
	if err != nil {
		t.Fatalf("la DEK de B debía abrir su propia sesión: %v", err)
	}
	if !bytes.Equal(pt, sessB) {
		t.Fatal("la sesión de B no sobrevivió con su propia DEK")
	}
}

// TestSharedDB_DeleteDeviceDoesNotAffectOthers: quemar un device (DeleteDevice: whatsmeow_* + msg_enc_*)
// borra TODO su material y NADA del de otro device en la MISMA BD. B queda intacto: su device se recupera
// con SU DEK y su sesión descifra; A desaparece (fila device y sesiones cifradas eliminadas).
func TestSharedDB_DeleteDeviceDoesNotAffectOthers(t *testing.T) {
	ctx := context.Background()
	db := openAt(t, filepath.Join(t.TempDir(), "shared.db"))
	defer func() { _ = db.Close() }()

	dekA, dekB := newDEK(t), newDEK(t)
	jidA := types.NewJID("15551110001", types.DefaultUserServer)
	jidA.Device = 0
	jidB := types.NewJID("15559990002", types.DefaultUserServer)
	jidB.Device = 0

	devA := syntheticDeviceWithJID(t, jidA)
	devB := syntheticDeviceWithJID(t, jidB)
	noiseB := *devB.NoiseKey.Priv

	contA := sharedContainer(t, ctx, db, dekA)
	contB := sharedContainer(t, ctx, db, dekB)

	if err := contA.PutDevice(ctx, devA); err != nil {
		t.Fatalf("PutDevice A: %v", err)
	}
	if err := devA.Sessions.PutSession(ctx, "peer:0", []byte("sesion-A")); err != nil {
		t.Fatalf("PutSession A: %v", err)
	}
	if err := contB.PutDevice(ctx, devB); err != nil {
		t.Fatalf("PutDevice B: %v", err)
	}
	sessB := []byte("sesion-B-intacta")
	if err := devB.Sessions.PutSession(ctx, "peer:0", sessB); err != nil {
		t.Fatalf("PutSession B: %v", err)
	}

	// Quemar A por completo (device whatsmeow_* + material cifrado msg_enc_*), como el unlink quirúrgico.
	if err := DeleteDevice(ctx, db, DialectSQLite, jidA); err != nil {
		t.Fatalf("DeleteDevice(A): %v", err)
	}

	// A desapareció: su fila device ya no está (LoadDevice → nil, sin error) y sus sesiones se borraron.
	gone, err := LoadDevice(ctx, contA, jidA)
	if err != nil {
		t.Fatalf("LoadDevice(A) tras borrar: %v", err)
	}
	if gone != nil {
		t.Fatal("A debía desaparecer tras DeleteDevice")
	}
	var nA int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM msg_enc_sessions WHERE our_jid=?`, jidA.String()).Scan(&nA); err != nil {
		t.Fatalf("contar sesiones de A: %v", err)
	}
	if nA != 0 {
		t.Fatalf("las sesiones cifradas de A debían borrarse, quedan %d", nA)
	}

	// B queda INTACTO: su device se recupera con SU DEK y su sesión descifra.
	gotB, err := LoadDevice(ctx, contB, jidB)
	if err != nil {
		t.Fatalf("LoadDevice(B) tras borrar A: %v", err)
	}
	if gotB == nil {
		t.Fatal("B no debía verse afectado por el borrado de A")
	}
	if *gotB.NoiseKey.Priv != noiseB {
		t.Error("B: NoiseKey cambió tras borrar A (aislamiento roto)")
	}
	gotSessB, err := gotB.Sessions.GetSession(ctx, "peer:0")
	if err != nil {
		t.Fatalf("GetSession(B) tras borrar A: %v", err)
	}
	if !bytes.Equal(gotSessB, sessB) {
		t.Error("B: la sesión no sobrevivió al borrado de A")
	}
}

// TestSharedDB_DEKNeverInDB comprueba que la DEK NUNCA es legible desde la BD (§3 / ADR-0007): tras
// persistir material cifrado con una DEK CONOCIDA, sus 32 bytes exactos no aparecen en NINGÚN fichero del
// store (.db/-wal/-shm). Es la expresión, a nivel de capa cripto, de "la DEK no cruza al plano de control /
// la nube": aunque la nube pudiera leer la BD, no puede recuperar la DEK de ella. (El invariante análogo
// sobre app.PairResult lo cubre sessionmgr.TestManager_Pair_DEKInvariant.)
func TestSharedDB_DEKNeverInDB(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "shared.db")
	db := openAt(t, path)

	// DEK aleatoria (retenida) para buscar sus BYTES EXACTOS: 32 bytes CSPRNG no aparecen por azar. Si
	// aparecieran, la nube (que puede leer la BD) recuperaría la DEK ⇒ violación de zero-knowledge.
	dek := newDEK(t)
	cont := sharedContainer(t, ctx, db, dek)

	dev, _ := syntheticDevice(t)
	if err := cont.PutDevice(ctx, dev); err != nil {
		t.Fatalf("PutDevice: %v", err)
	}
	if err := dev.Sessions.PutSession(ctx, "peer:0", []byte("carga-cifrada")); err != nil {
		t.Fatalf("PutSession: %v", err)
	}

	// Volcar el WAL al fichero principal y cerrar para inspeccionar TODOS los bytes del store.
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("wal_checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	scanned := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		scanned++
		if bytes.Contains(raw, dek) {
			t.Errorf("el fichero %s contiene los bytes de la DEK ⇒ la DEK es legible para la nube", e.Name())
		}
	}
	if scanned == 0 {
		t.Fatal("no se inspeccionó ningún fichero del store")
	}
}
