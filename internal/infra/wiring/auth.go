package wiring

// auth.go cablea la identidad de OPERADOR del plano de control (Plan 033 Ola 3 / ADR-0025): registra el
// applier de config kind:"jwks" (llave pública ES256 del emisor, distribuida/rotada por ConfigUpdate) y
// construye el session manager (relay CloudLink + custodia del refresh token + verificador offline + RBAC).
//
// La llave pública ES256 NO es secreta (viaja por ConfigUpdate); el REFRESH token sí se custodia (archivo
// 0600 bajo <data_dir>/auth, patrón mono-secreto — ver internal/auth.SecretCustody). Zero-knowledge: la
// DEK nunca se toca (ADR-0007).

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/edgeconfig"
	edgeauth "github.com/EduGoGroup/wapp-edge-agent/internal/auth"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// authConfigKind es el kind de ConfigUpdate que distribuye/rota la llave pública ES256 (ADR-0025 dec.2).
const authConfigKind = "jwks"

// defaultAuthIssuer es el emisor (`iss`) por defecto del IAM de la nube (coincide con JWT_ISSUER del
// cloud-platform). Se puede sobreescribir con WAPP_AGENT_AUTH_ISSUER si el despliegue cambia el emisor.
const defaultAuthIssuer = "wapp-cloud"

// RegisterJWKS crea el store de llaves ES256 del operador y registra el kind "jwks" en el edgeconfig.Service
// COMPARTIDO (el mismo que aplica "intents"). El validador parsea/valida el JWK Set ANTES de persistir; el
// suscriptor instala/rota las llaves por kid en el store (last-known-good: un blob inválido conserva el
// verificador previo). El Bootstrap del Service (al arrancar) recarga el JWKS persistido en el store sin
// esperar un nuevo push. Devuelve el store, que el session manager consulta para validar offline.
func RegisterJWKS(svc *edgeconfig.Service, log sharedlogger.Logger) *edgeauth.KeyStore {
	keyStore := edgeauth.NewKeyStore(authIssuer())
	svc.RegisterKind(authConfigKind,
		func(payload []byte) error { _, err := edgeauth.ParseJWKS(payload); return err },
		func(rec edgeconfig.Record) {
			if err := keyStore.InstallJWKS(rec.Payload); err != nil {
				// El Service ya validó antes de persistir/notificar; un fallo aquí sería un blob corrupto en
				// disco. Se loguea y se conserva el verificador previo (no se instala basura).
				log.Error("auth: JWKS ilegible al recargar (se conserva la llave anterior)",
					"version", rec.Version, "error", err)
			}
		},
	)
	log.Info("auth: applier ConfigUpdate kind:\"jwks\" registrado (ADR-0025 dec.2)", "issuer", authIssuer())
	return keyStore
}

// BuildAuthManager construye el session manager del operador sobre el store de llaves y el relay CloudLink.
// Cuando el relay es nil (CloudLink deshabilitado / LogMux), usa un relay OFFLINE que rechaza todo login
// con ErrRelayOffline (no hay login offline de primera vez, ADR-0025 dec.3). El refresh token se custodia
// en <data_dir>/auth/operator.refresh (0600). El refresh proactivo se arranca aparte (StartProactiveRefresh).
func BuildAuthManager(cfg config.Config, log sharedlogger.Logger, keyStore *edgeauth.KeyStore, relay edgeauth.Relay) *edgeauth.Manager {
	if relay == nil {
		relay = offlineRelay{}
		log.Warn("auth: sin relay CloudLink (endpoint ausente); login/refresh de operador NO disponibles (offline)")
	}
	custodyPath := filepath.Join(cfg.DataDir, "auth", "operator.refresh")
	custody := edgeauth.NewFileSecretCustody(custodyPath)

	// Tenant coherente (ADR-0025): solo se aceptan tokens del tenant al que el Edge está enrolado. La
	// fuente de verdad es el propio cert mTLS del Edge (Subject.Organization[0]), que el Gateway firmó con
	// el tenant y lee de vuelta como identidad del canal — la MISMA que gobierna el stream CloudLink. Si el
	// Edge aún NO está enrolado (sin cert), el tenant es "" y el guard queda INERTE: no se fuerza un rechazo
	// que rompería el bootstrap enroll→login (ver enrolledTenant).
	var opts []edgeauth.Option
	if tenant := enrolledTenant(cfg, log); tenant != "" {
		opts = append(opts, edgeauth.WithExpectedTenant(tenant))
		log.Info("auth: guard de tenant coherente ACTIVO (ADR-0025 dec.1)", "tenant_id", tenant)
	} else {
		log.Info("auth: sin tenant de enrolamiento conocido; guard de tenant coherente INERTE (pre-enrolamiento)")
	}
	return edgeauth.NewManager(relay, keyStore, custody, log, opts...)
}

// enrolledTenant extrae el tenant al que el Edge está enrolado del cert mTLS emitido por el Gateway
// (cfg.CloudLink.TLSCert), leyendo Subject.Organization[0]. Es la MISMA fuente de verdad que identifica el
// canal mTLS ante la nube (ADR-0025 dec.1): el tenant NO viaja en los frames, es implícito del enrolamiento.
//
// Devuelve "" (guard inerte, sin rechazo) en los casos benignos de pre-enrolamiento —ruta de cert sin
// configurar o cert aún inexistente— para NO romper el bootstrap enroll→login. Un cert presente pero
// ilegible o sin Organization es una ANOMALÍA: se AVISA y también se devuelve "" (fail-open del guard, no
// del arranque; el resto de la validación —firma ES256 + RBAC— sigue vigente).
func enrolledTenant(cfg config.Config, log sharedlogger.Logger) string {
	certPath := cfg.CloudLink.TLSCert
	if certPath == "" {
		return "" // mTLS no configurado (dev/pre-enrolamiento): guard inerte.
	}
	pemBytes, err := os.ReadFile(certPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "" // aún no enrolado (el cert se crea en `enroll`): guard inerte, esperado.
		}
		log.Warn("auth: no se pudo leer el cert mTLS para derivar el tenant; guard de tenant INERTE", "tls_cert", certPath, "error", err)
		return ""
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		log.Warn("auth: cert mTLS sin bloque PEM legible; guard de tenant INERTE", "tls_cert", certPath)
		return ""
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		log.Warn("auth: cert mTLS ilegible al parsear; guard de tenant INERTE", "tls_cert", certPath, "error", err)
		return ""
	}
	if len(cert.Subject.Organization) == 0 || cert.Subject.Organization[0] == "" {
		log.Warn("auth: cert mTLS sin Subject.Organization (tenant); guard de tenant INERTE", "tls_cert", certPath)
		return ""
	}
	return cert.Subject.Organization[0]
}

// SharedEdgeConfigService devuelve el edgeconfig.Service COMPARTIDO sobre el que se registran los kinds
// "intents" (Plan 029) y "jwks" (Plan 033): reusa el que crea BuildIntent cuando el clasificador está ON,
// o crea uno nuevo sobre el mismo Store cuando está OFF (el auth necesita aplicar "jwks" con independencia
// del clasificador). Un solo applier con ambos kinds es lo que consume el Adapter CloudLink.
func SharedEdgeConfigService(intentStack *IntentStack, log sharedlogger.Logger) *edgeconfig.Service {
	if intentStack.Service != nil {
		return intentStack.Service
	}
	return edgeconfig.NewService(intentStack.Store, log)
}

// authIssuer resuelve el emisor esperado del IAM (env override o default).
func authIssuer() string {
	if v := os.Getenv("WAPP_AGENT_AUTH_ISSUER"); v != "" {
		return v
	}
	return defaultAuthIssuer
}

// offlineRelay es el relay de auth cuando NO hay stream CloudLink (endpoint ausente): rechaza todo con
// ErrRelayOffline. Mantiene el session manager operable (validación offline de un token previo seguiría
// funcionando) sin poder emitir/renovar credenciales nuevas.
type offlineRelay struct{}

var _ edgeauth.Relay = offlineRelay{}

func (offlineRelay) Login(context.Context, string, string) (edgeauth.Tokens, error) {
	return edgeauth.Tokens{}, edgeauth.ErrRelayOffline
}
func (offlineRelay) Refresh(context.Context, string) (edgeauth.Tokens, error) {
	return edgeauth.Tokens{}, edgeauth.ErrRelayOffline
}
func (offlineRelay) Logout(context.Context, string, bool) error {
	return edgeauth.ErrRelayOffline
}
