package wiring

// cola_papeles_test.go — los PAPELES que el daemon le pide a la cola (Plan 051 · Ola 4 · T4.5).
//
// 🔴 QUÉ SE CUSTODIA AQUÍ. `BuildCola` devuelve `app.ColaEntrantes`, que solo sabe encolar; pero el
// daemon le pide DOS papeles más por *interface upgrade* (aserción de tipo con coma-ok):
//
//   - `app.ColaContador`  → el bloque de cola del diagnóstico (daemon.colaContador);
//   - `app.ParteWorkerLector` → el PARTE del worker-cajero, o sea de dónde salen `intent_circuit`,
//     `worker_taskset` e `intent_p50_ms` en el heartbeat (daemon.parteDelWorker).
//
// Las dos aserciones son legales: si fallan, la función devuelve nil y el colector trata el nil como un
// caso válido —lo es, a propósito: es el comportamiento de un Edge sin cola—. Ahí está el agujero:
// `var _ app.ParteWorkerLector = (*colaentrantes.Store)(nil)` (parte.go) prueba que el TIPO CONCRETO
// implementa el puerto, y el compilador lo caza; lo que NADIE cazaba es que el objeto que el daemon
// tiene EN LA MANO siga siendo ese tipo. `BuildCola` es una factory, y basta con que alguien envuelva su
// retorno en un decorador que no reenvíe `LeerParte` para que `intent_circuit` viaje VACÍO PARA SIEMPRE
// —con `go build`, `go vet` y `go test` en verde, y sin un solo error en campo—. Vacío es justo lo que
// la Ola 4 define como «no lo sé», así que el síntoma sería indistinguible de un cajero muerto: la
// telemetría de salud fallando en el único modo que no sabe reportar.
//
// Es el mismo agujero que el Plan 051 Ola 3 ya pagó una vez con el histograma de latencia
// (ver internal/infra/daemon/latencia_cableado_test.go), entrando por otra puerta.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO (ejecutadas): envolver el `return store` de BuildCola en cualquier
// decorador que solo reexponga `Enqueue` ⇒ rojo en los dos papeles; quitarle a `*colaentrantes.Store` el
// método `LeerParte` ⇒ rojo en el papel del parte (y además no compila `parte.go`).

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// custodiaDeMentira entrega una DEK fija de 32 bytes. BuildCola no la usa al construir (el crypterFor es
// perezoso: solo corre al encolar), pero la firma la exige.
type custodiaDeMentira struct{}

func (custodiaDeMentira) Store([]byte) error    { return nil }
func (custodiaDeMentira) Load() ([]byte, error) { return make([]byte, 32), nil }
func (custodiaDeMentira) Exists() bool          { return true }

func TestBuildCola_RespaldaLosTresPapelesQueElDaemonLePide(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()

	colaDB, err := db.OpenCola(ctx, db.ColaDBPath(filepath.Join(dataDir, "cola_entrantes.db")))
	if err != nil {
		t.Fatalf("OpenCola: %v", err)
	}
	t.Cleanup(func() { _ = colaDB.Close() })
	if err := db.MigrateCola(ctx, colaDB); err != nil {
		t.Fatalf("MigrateCola: %v", err)
	}

	cola := BuildCola(ctx, config.Config{}, colaDB, sessionmgr.NewLayout(dataDir),
		func(string) app.KeyCustody { return custodiaDeMentira{} },
		sharedlogger.New())
	if cola == nil {
		t.Fatal("BuildCola devolvió nil sobre una cola recién migrada: sin esto el resto no significa nada")
	}

	// Papel 1: contar (el bloque de cola del diagnóstico).
	if _, ok := cola.(app.ColaContador); !ok {
		t.Errorf("lo que BuildCola devuelve (%T) no respalda app.ColaContador; daemon.colaContador "+
			"degradaría a nil y el bundle saldría sin los campos de cola", cola)
	}

	// Papel 2: EL QUE DECIDE LA OLA 4. Sin él, el heartbeat viaja sin intent_circuit para siempre y el
	// síntoma es idéntico al de un cajero muerto.
	if _, ok := cola.(app.ParteWorkerLector); !ok {
		t.Errorf("lo que BuildCola devuelve (%T) no respalda app.ParteWorkerLector; daemon.parteDelWorker "+
			"degradaría a nil e intent_circuit/worker_taskset/intent_p50_ms viajarían VACÍOS en cada "+
			"latido, indistinguibles de «el cajero está muerto»", cola)
	}
}
