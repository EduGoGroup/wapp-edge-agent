// Package enrolladapter adapta el enrolamiento real (internal/infra/enroll) al puerto del plano
// de control (server.RegisterEnroll).
package enrolladapter

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/enroll"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// Adapter adapta el enroll REAL (internal/infra/enroll) al puerto del plano de control
// (server.RegisterEnroll, Plan 023 · T1): ejecuta el mismo enrolamiento que `agent enroll <codigo>`
// REUSANDO enroll.Run —no lo reimplementa— y reporta si el par mTLS ya vive en disco. Guarda la config
// del núcleo para poblar EnrollmentEndpoint/TLSCA/rutas destino; por HTTP solo llega el activation code.
// Zero-knowledge: enroll.Run persiste el par mTLS + cloud_enc_pubkey, NUNCA la DEK.
type Adapter struct {
	cfg config.Config
	log sharedlogger.Logger
}

// New construye un adaptador de enrolamiento para el plano de control.
func New(cfg config.Config, log sharedlogger.Logger) Adapter {
	return Adapter{cfg: cfg, log: log}
}

// Enrolled reporta si el par mTLS (cert + clave) ya está presente en disco: la señal de "primera
// ejecución" que la web usa para elegir entre la pantalla enrolar y el dashboard.
func (a Adapter) Enrolled() bool {
	return mtlsFileExists(a.cfg.CloudLink.TLSCert) && mtlsFileExists(a.cfg.CloudLink.TLSKey)
}

// Enroll valida las precondiciones de bootstrap (endpoint + TLSCA pre-provistos y rutas destino) y
// delega en enroll.Run con el activation code recibido. Mismas validaciones que runEnroll (subcomando
// CLI), para dar un mensaje claro en la web en vez de un fallo opaco del dial.
func (a Adapter) Enroll(ctx context.Context, activationCode string) error {
	cfg := a.cfg
	cfg.CloudLink.ActivationCode = strings.TrimSpace(activationCode)

	if cfg.CloudLink.EnrollmentEndpoint == "" {
		return fmt.Errorf("falta enrollment_endpoint (bootstrap del paquete): configura cloudlink.enrollment_endpoint")
	}
	if cfg.CloudLink.TLSCA == "" {
		return fmt.Errorf("falta tls_ca: la CA que valida al Gateway debe estar pre-provista antes de enrolar")
	}
	if cfg.CloudLink.TLSCert == "" || cfg.CloudLink.TLSKey == "" {
		return fmt.Errorf("faltan rutas destino tls_cert/tls_key donde persistir la credencial mTLS")
	}
	if cfg.CloudLink.ActivationCode == "" {
		return fmt.Errorf("falta el código de activación")
	}

	a.log.Info("enroll web: enrolando el Edge contra el Gateway",
		"endpoint", cfg.CloudLink.EnrollmentEndpoint, "tls_cert", cfg.CloudLink.TLSCert)
	return enroll.Run(ctx, cfg, a.log)
}

// mtlsFileExists indica si path apunta a un archivo REGULAR existente (no directorio, no vacío en ruta).
func mtlsFileExists(path string) bool {
	if path == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
