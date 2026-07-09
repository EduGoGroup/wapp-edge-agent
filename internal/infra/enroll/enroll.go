// Package enroll implementa el cliente REAL de enrolamiento del Edge (T6, Plan 005).
//
// Es un bootstrap de PROVISIÓN de una sola vez, no un caso de uso de dominio ni un
// adaptador de runtime: por eso vive en internal/infra (provisión/operación), junto a
// config/db/logger, y no en internal/app ni en internal/adapters/cloudlink (que sólo
// maneja el stream bidi de runtime). El cliente de enrolamiento de cloudlink vive en su
// paquete internal/ (no importable), así que aquí se REIMPLEMENTA por copia-adaptación
// (ADR-0004) usando exclusivamente el `gen` público del contrato.
//
// Flujo: genera un par ECDSA P-256 (la clave privada NUNCA sale del proceso hasta
// persistirse en disco), construye un CSR x509 (CN = EdgeID), abre una conexión gRPC con
// TLS-DE-SERVIDOR (valida al Gateway con TLSCA; ni mTLS ni insecure), llama
// Enrollment.EnrollEdge con el código de activación y persiste la clave (PKCS#8, 0600) en
// TLSKey y el cert emitido en TLSCert. Tras enrolar, el subcomando `listen` ya puede usar
// mTLS con ese par.
package enroll

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	pemTypeCSR = "CERTIFICATE REQUEST"
	pemTypeKey = "PRIVATE KEY" // PKCS#8

	// dialTimeout acota la llamada de enrolamiento para no colgar el subcomando si el Gateway no responde.
	dialTimeout = 30 * time.Second

	keyFilePerm  os.FileMode = 0o600 // clave privada: sólo el dueño
	certFilePerm os.FileMode = 0o644 // cert/cadena: material público
	dirPerm      os.FileMode = 0o700
)

// options agrupa ajustes opcionales del enrolamiento (seam de pruebas).
type options struct {
	extraDialOpts []grpc.DialOption
}

// Option configura el enrolamiento. WithDialOptions permite inyectar un dialer custom
// (p.ej. bufconn) en los tests sin abrir red real.
type Option func(*options)

// WithDialOptions añade DialOptions al cliente gRPC del enrolamiento (además de las
// transport-credentials TLS-de-servidor). Pensado para tests con bufconn.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) { o.extraDialOpts = append(o.extraDialOpts, opts...) }
}

// Run ejecuta el enrolamiento real del Edge contra el Gateway y persiste el par mTLS.
//
// Precondiciones (validadas por el llamador, revalidadas aquí): EnrollmentEndpoint, TLSCA
// (CA dev pre-provista que valida al Gateway) y ActivationCode presentes. La clave privada
// se persiste en cfg.CloudLink.TLSKey (PKCS#8 PEM, 0600) y el cert emitido en TLSCert.
func Run(ctx context.Context, cfg config.Config, log sharedlogger.Logger, opts ...Option) error {
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	cl := cfg.CloudLink

	if cl.EnrollmentEndpoint == "" {
		return errors.New("enroll: falta enrollment_endpoint (servidor de enrolamiento del Gateway)")
	}
	if cl.TLSCert == "" || cl.TLSKey == "" {
		return errors.New("enroll: faltan rutas tls_cert/tls_key donde persistir el material emitido")
	}

	edgeID := resolveEdgeID(cl)

	// 1) Par ECDSA P-256. La privada nunca sale del proceso hasta el paso de persistencia.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("enroll: generar clave ECDSA P-256: %w", err)
	}

	// 2) CSR (CN = EdgeID). El Organization/OU lo fija el Gateway al firmar; no lo ponemos aquí.
	csrPEM, err := buildCSR(key, edgeID)
	if err != nil {
		return err
	}

	// 3) Dial TLS-DE-SERVIDOR: valida al Gateway con TLSCA. NI mTLS NI insecure.
	creds, serverName, err := dialCreds(cl)
	if err != nil {
		return err
	}
	dialOpts := append([]grpc.DialOption{grpc.WithTransportCredentials(creds)}, o.extraDialOpts...)
	cc, err := grpc.NewClient(cl.EnrollmentEndpoint, dialOpts...)
	if err != nil {
		return fmt.Errorf("enroll: crear cliente gRPC hacia %q: %w", cl.EnrollmentEndpoint, err)
	}
	defer func() { _ = cc.Close() }()

	// 4) EnrollEdge con el código de activación.
	callCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	resp, err := cloudlinkv1.NewEnrollmentClient(cc).EnrollEdge(callCtx, &cloudlinkv1.EnrollEdgeRequest{
		ActivationCode: cl.ActivationCode,
		CsrPem:         csrPEM,
	})
	if err != nil {
		return mapEnrollErr(err)
	}

	// 5) Serializa la clave (PKCS#8) y VALIDA en memoria que el cert emitido y la clave forman un par
	//    cargable ANTES de tocar el disco (no dejar archivos corruptos). Esto demuestra usabilidad mTLS.
	keyPEM, err := marshalKeyPEM(key)
	if err != nil {
		return err
	}
	if len(resp.GetEdgeCertPem()) == 0 {
		return errors.New("enroll: el Gateway no devolvió edge_cert_pem")
	}
	if _, err := tls.X509KeyPair(resp.GetEdgeCertPem(), keyPEM); err != nil {
		return fmt.Errorf("enroll: el cert emitido no forma par válido con la clave generada: %w", err)
	}

	// Persiste clave (0600) y cert (0644), creando directorios si faltan.
	if err := writeFile(cl.TLSKey, keyPEM, keyFilePerm); err != nil {
		return fmt.Errorf("enroll: persistir clave en %q: %w", cl.TLSKey, err)
	}
	if err := writeFile(cl.TLSCert, resp.GetEdgeCertPem(), certFilePerm); err != nil {
		return fmt.Errorf("enroll: persistir cert en %q: %w", cl.TLSCert, err)
	}

	// TLSCA ya estaba pre-provista (se usó para el dial). Si el Gateway devolvió una cadena distinta,
	// AVISAMOS y conservamos la pre-provista (no se sobrescribe en silencio).
	reconcileCA(cl.TLSCA, resp.GetCaChainPem(), log)

	// Pública de cifrado de la nube (Plan 011 §6.4): si el Gateway la incluyó, se persiste en
	// CloudEncPubKeyPath (base64, una línea) para que el daemon selle los sensibles en tránsito. Si no
	// hay pública o no hay ruta configurada, se OMITE sin fallar (fallback claro §10.H en el reenvío).
	persistCloudEncPubKey(cl.CloudEncPubKeyPath, resp.GetCloudEncPubkey(), log)

	// Endpoint de runtime CloudLink (Plan 026 T3, cierra follow-up 023): se DERIVA del host del
	// enrollment_endpoint + el puerto de runtime (cfg.CloudLink.RuntimePort, default 8101) y se PERSISTE en
	// el archivo de estado <data_dir>/cloudlink-endpoint para que `serve` levante el stream sin que un
	// no-técnico edite el config.yaml. El proto de enroll (EnrollEdgeResponse) NO devuelve un endpoint de
	// runtime, así que derivar es la única vía sin tocar el contrato. Best-effort (no aborta el enroll: el
	// par mTLS ya está persistido); un fallo se AVISA con la instrucción manual.
	persistRuntimeEndpoint(cfg, log)

	log.Info("enrolamiento completado: par mTLS persistido; `listen` ya puede usar mTLS",
		"tenant_id", resp.GetTenantId(),
		"edge_id", edgeID,
		"server_name", serverName,
		"tls_key", cl.TLSKey,
		"tls_cert", cl.TLSCert,
		"tls_ca", cl.TLSCA,
	)
	return nil
}

// resolveEdgeID determina el CommonName del CSR: EdgeID explícito; si no, SessionID; si no,
// el hostname del equipo; como último recurso un literal estable.
func resolveEdgeID(cl config.CloudLinkConfig) string {
	if cl.EdgeID != "" {
		return cl.EdgeID
	}
	if cl.SessionID != "" {
		return cl.SessionID
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return "wapp-edge"
}

// buildCSR construye un CSR x509 con CN = commonName y lo codifica a PEM.
func buildCSR(key *ecdsa.PrivateKey, commonName string) ([]byte, error) {
	tmpl := &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: commonName},
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, tmpl, key)
	if err != nil {
		return nil, fmt.Errorf("enroll: crear CSR: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: pemTypeCSR, Bytes: der}), nil
}

// dialCreds construye las transport-credentials TLS-de-servidor para enrolar: carga TLSCA como
// RootCAs (obligatoria; sin ella NO se enrola), fija ServerName y exige TLS 1.3. Devuelve también
// el ServerName resuelto para el log.
func dialCreds(cl config.CloudLinkConfig) (credentials.TransportCredentials, string, error) {
	if cl.TLSCA == "" {
		return nil, "", errors.New("enroll: falta tls_ca (CA que valida al Gateway); no se enrola de forma insegura")
	}
	caPEM, err := os.ReadFile(cl.TLSCA)
	if err != nil {
		return nil, "", fmt.Errorf("enroll: leer tls_ca %q: %w", cl.TLSCA, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, "", fmt.Errorf("enroll: tls_ca %q no contiene certificados PEM válidos", cl.TLSCA)
	}
	serverName := cl.ServerName
	if serverName == "" {
		if host, _, splitErr := net.SplitHostPort(cl.EnrollmentEndpoint); splitErr == nil {
			serverName = host
		} else {
			serverName = cl.EnrollmentEndpoint
		}
	}
	creds := credentials.NewTLS(&tls.Config{
		RootCAs:    pool,
		ServerName: serverName,
		MinVersion: tls.VersionTLS13,
	})
	return creds, serverName, nil
}

// marshalKeyPEM serializa la clave privada ECDSA a PKCS#8 PEM.
func marshalKeyPEM(key *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("enroll: serializar clave a PKCS#8: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: pemTypeKey, Bytes: der}), nil
}

// reconcileCA conserva la CA pre-provista. Si el Gateway devolvió una cadena distinta a la ya
// presente en TLSCA, lo registra como advertencia (no sobrescribe la pre-provista).
func reconcileCA(caPath string, gatewayChain []byte, log sharedlogger.Logger) {
	if caPath == "" || len(gatewayChain) == 0 {
		return
	}
	existing, err := os.ReadFile(caPath)
	if err != nil {
		log.Warn("enroll: no se pudo leer la tls_ca pre-provista para reconciliar; se conserva", "tls_ca", caPath, "error", err)
		return
	}
	if !pemEqual(existing, gatewayChain) {
		log.Warn("enroll: el Gateway devolvió una cadena de CA distinta a la pre-provista; se CONSERVA la pre-provista (no se sobrescribe)",
			"tls_ca", caPath)
	}
}

// persistCloudEncPubKey persiste la pública de cifrado de la nube (Plan 011 §6.4) en path, en base64
// (una línea, 0644 — material público). Es best-effort: si no hay pública o no hay ruta, no hace nada;
// si falla la escritura, AVISA pero NO aborta el enrolamiento (el reenvío usará el fallback claro §10.H
// hasta el próximo enrolamiento). El tamaño se valida al CARGARLA en el daemon.
func persistCloudEncPubKey(path string, pub []byte, log sharedlogger.Logger) {
	if len(pub) == 0 {
		return
	}
	if path == "" {
		log.Warn("enroll: el Gateway devolvió cloud_enc_pubkey pero no hay cloud_enc_pubkey_path configurado; se OMITE (fallback claro)")
		return
	}
	encoded := []byte(base64.StdEncoding.EncodeToString(pub))
	if err := writeFile(path, encoded, certFilePerm); err != nil {
		log.Warn("enroll: no se pudo persistir cloud_enc_pubkey; el reenvío usará fallback claro (§10.H)", "path", path, "error", err)
		return
	}
	log.Info("enroll: pública de cifrado de la nube persistida (sellado en tránsito habilitado)", "cloud_enc_pubkey_path", path)
}

// persistRuntimeEndpoint deriva el Endpoint de runtime CloudLink y lo persiste en el archivo de estado
// del data_dir (Plan 026 T3, cierra follow-up 023) para que `serve` lo relea sin edición manual del
// config.yaml (config.Load lo lee como fallback vía RuntimeEndpointStatePath).
//
// DERIVACIÓN (NO ASUMIR — verificado contra el proto: EnrollEdgeResponse solo trae edge_cert_pem/
// ca_chain_pem/tenant_id/cloud_enc_pubkey, sin endpoint de runtime): host(EnrollmentEndpoint) + ":" +
// RuntimePort (default config.DefaultCloudLinkRuntimePort). Mantiene el contrato cloudlink intacto (sin
// release de módulo). Si el operador ya fijó un Endpoint explícito (YAML/env), se RESPETA y no se
// sobrescribe. Best-effort: cualquier fallo se AVISA con la instrucción manual pero NO aborta el enroll
// (el par mTLS ya quedó en disco; re-enrolar exigiría un código nuevo). Material PÚBLICO (host:puerto).
func persistRuntimeEndpoint(cfg config.Config, log sharedlogger.Logger) {
	cl := cfg.CloudLink
	if cl.Endpoint != "" {
		log.Info("enroll: endpoint de runtime ya configurado explícitamente; no se deriva ni sobrescribe",
			"endpoint", cl.Endpoint)
		return
	}
	if cfg.DataDir == "" {
		// Sin data_dir no hay ubicación estable donde persistir el estado (config.Load siempre lo
		// absolutiza en el flujo real; esto solo pasa en llamadas directas sin data_dir).
		log.Warn("enroll: sin data_dir; no se persiste el endpoint de runtime (configúralo en cloudlink.endpoint)")
		return
	}
	runtimePort := cl.RuntimePort
	if runtimePort == "" {
		runtimePort = config.DefaultCloudLinkRuntimePort
	}
	endpoint, err := deriveRuntimeEndpoint(cl.EnrollmentEndpoint, runtimePort)
	if err != nil {
		log.Warn("enroll: no se pudo derivar el endpoint de runtime; configúralo a mano en cloudlink.endpoint del config.yaml",
			"enrollment_endpoint", cl.EnrollmentEndpoint, "runtime_port", runtimePort, "error", err)
		return
	}
	path := config.RuntimeEndpointStatePath(cfg.DataDir)
	if err := writeFile(path, []byte(endpoint+"\n"), certFilePerm); err != nil {
		log.Warn("enroll: no se pudo persistir el endpoint de runtime; configúralo a mano en cloudlink.endpoint del config.yaml",
			"endpoint", endpoint, "path", path, "error", err)
		return
	}
	log.Info("enroll: endpoint de runtime derivado y persistido; `serve` levantará el stream sin edición manual",
		"endpoint", endpoint, "path", path)
}

// deriveRuntimeEndpoint construye el endpoint de runtime "host:runtimePort" a partir del endpoint de
// enrolamiento "host[:puerto]" (Plan 026 T3): se queda con el HOST del enrollment_endpoint (descartando su
// puerto de enroll) y le pega el puerto de runtime. Soporta host con o sin puerto e IPv6 (net.JoinHostPort
// entre corchetes). Devuelve error si no hay enrollment_endpoint o si el host queda vacío.
func deriveRuntimeEndpoint(enrollmentEndpoint, runtimePort string) (string, error) {
	if enrollmentEndpoint == "" {
		return "", errors.New("enrollment_endpoint vacío")
	}
	if runtimePort == "" {
		return "", errors.New("runtime_port vacío")
	}
	host, _, err := net.SplitHostPort(enrollmentEndpoint)
	if err != nil {
		// El endpoint de enrolamiento vino sin puerto (p.ej. "gateway.tudominio.com"): úsalo como host.
		host = enrollmentEndpoint
	}
	if host == "" {
		return "", fmt.Errorf("host vacío en enrollment_endpoint %q", enrollmentEndpoint)
	}
	return net.JoinHostPort(host, runtimePort), nil
}

// pemEqual compara dos materiales PEM por el conjunto de bloques DER que contienen (ignora
// diferencias de formato como saltos de línea u orden de cabeceras).
func pemEqual(a, b []byte) bool {
	da := pemDERs(a)
	db := pemDERs(b)
	if len(da) != len(db) {
		return false
	}
	for k, v := range da {
		if db[k] != v {
			return false
		}
	}
	return true
}

func pemDERs(data []byte) map[string]int {
	out := map[string]int{}
	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		out[string(block.Bytes)]++
	}
	return out
}

// writeFile escribe data en path con los permisos dados, creando los directorios padres si faltan.
func writeFile(path string, data []byte, perm os.FileMode) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, dirPerm); err != nil {
			return fmt.Errorf("crear directorio %q: %w", dir, err)
		}
	}
	return os.WriteFile(path, data, perm)
}

// mapEnrollErr traduce los códigos gRPC de EnrollEdge a errores claros para el operador.
func mapEnrollErr(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("enroll: EnrollEdge falló: %w", err)
	}
	switch st.Code() {
	case codes.PermissionDenied:
		return fmt.Errorf("enroll: código de activación inválido o ya usado (PermissionDenied): %s", st.Message())
	case codes.InvalidArgument:
		return fmt.Errorf("enroll: CSR rechazado por el Gateway (InvalidArgument): %s", st.Message())
	default:
		return fmt.Errorf("enroll: EnrollEdge falló (%s): %s", st.Code(), st.Message())
	}
}
