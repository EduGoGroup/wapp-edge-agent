package enroll_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/enroll"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// testCA agrupa una CA de prueba autogenerada y su material para firmar.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generar clave CA: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "wApp Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("crear cert CA: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsear cert CA: %v", err)
	}
	return testCA{
		cert: cert,
		key:  key,
		pem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// serverCert emite un cert de servidor (SAN localhost) firmado por la CA, listo para TLS.
func (ca testCA) serverCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generar clave server: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("crear cert server: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// fakeEnroll implementa cloudlinkv1.EnrollmentServer: verifica el CSR, lo firma con la CA de
// prueba y devuelve cert+cadena+tenant. failPermission simula código inválido (PermissionDenied).
type fakeEnroll struct {
	cloudlinkv1.UnimplementedEnrollmentServer
	ca             testCA
	failPermission bool
	encPub         []byte // Plan 011: pública de cifrado de la nube incluida en la respuesta (si no nil)

	mu          sync.Mutex
	gotCN       string
	gotValidSig bool
}

func (f *fakeEnroll) EnrollEdge(_ context.Context, req *cloudlinkv1.EnrollEdgeRequest) (*cloudlinkv1.EnrollEdgeResponse, error) {
	if f.failPermission {
		return nil, status.Error(codes.PermissionDenied, "código de activación inválido o ya usado")
	}
	block, _ := pem.Decode(req.GetCsrPem())
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return nil, status.Error(codes.InvalidArgument, "csr_pem no es un CSR PEM")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "csr_pem no parsea")
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, status.Error(codes.InvalidArgument, "firma del CSR inválida")
	}

	f.mu.Lock()
	f.gotCN = csr.Subject.CommonName
	f.gotValidSig = true
	f.mu.Unlock()

	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: csr.Subject.CommonName, Organization: []string{"wApp-Tenant"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, f.ca.cert, csr.PublicKey, f.ca.key)
	if err != nil {
		return nil, status.Error(codes.Internal, "firmar leaf: "+err.Error())
	}
	return &cloudlinkv1.EnrollEdgeResponse{
		EdgeCertPem:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		CaChainPem:     f.ca.pem,
		TenantId:       "tenant-test",
		CloudEncPubkey: f.encPub,
	}, nil
}

// startServer levanta el Enrollment de prueba con TLS-de-servidor sobre bufconn y devuelve el dialer.
func startServer(t *testing.T, ca testCA, fake *fakeEnroll) func(context.Context, string) (net.Conn, error) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{ca.serverCert(t)},
		MinVersion:   tls.VersionTLS13,
	})
	srv := grpc.NewServer(grpc.Creds(creds))
	cloudlinkv1.RegisterEnrollmentServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
}

func baseConfig(t *testing.T, ca testCA, edgeID string) (config.Config, string, string) {
	t.Helper()
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, ca.pem, 0o600); err != nil {
		t.Fatalf("escribir CA: %v", err)
	}
	certPath := filepath.Join(dir, "certs", "edge.crt")
	keyPath := filepath.Join(dir, "certs", "edge.key")
	cfg := config.Config{
		CloudLink: config.CloudLinkConfig{
			EnrollmentEndpoint: "passthrough:///bufnet",
			ServerName:         "localhost",
			TLSCA:              caPath,
			TLSCert:            certPath,
			TLSKey:             keyPath,
			ActivationCode:     "good-code",
			EdgeID:             edgeID,
		},
	}
	return cfg, certPath, keyPath
}

func TestRun_PersistsUsableMTLSPair(t *testing.T) {
	ca := newTestCA(t)
	fake := &fakeEnroll{ca: ca}
	dialer := startServer(t, ca, fake)
	cfg, certPath, keyPath := baseConfig(t, ca, "edge-abc-123")

	err := enroll.Run(context.Background(), cfg, sharedlogger.New(),
		enroll.WithDialOptions(grpc.WithContextDialer(dialer)))
	if err != nil {
		t.Fatalf("Run devolvió error inesperado: %v", err)
	}

	// El CSR generado lleva CN == EdgeID y firma válida.
	fake.mu.Lock()
	gotCN, validSig := fake.gotCN, fake.gotValidSig
	fake.mu.Unlock()
	if !validSig {
		t.Fatalf("el servidor no validó la firma del CSR")
	}
	if gotCN != "edge-abc-123" {
		t.Fatalf("CN del CSR: got %q, want %q", gotCN, "edge-abc-123")
	}

	// Los archivos existen y forman un par mTLS cargable (clave persistida ↔ cert emitido).
	if _, err := os.Stat(keyPath); err != nil {
		t.Fatalf("clave no persistida: %v", err)
	}
	if _, err := os.Stat(certPath); err != nil {
		t.Fatalf("cert no persistido: %v", err)
	}
	if _, err := tls.LoadX509KeyPair(certPath, keyPath); err != nil {
		t.Fatalf("el par persistido no carga como keypair mTLS: %v", err)
	}

	// La clave privada debe tener permisos 0600.
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("stat clave: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("permisos de la clave: got %o, want 600", perm)
	}
}

func TestRun_DefaultEdgeIDFromSessionID(t *testing.T) {
	ca := newTestCA(t)
	fake := &fakeEnroll{ca: ca}
	dialer := startServer(t, ca, fake)
	cfg, _, _ := baseConfig(t, ca, "") // EdgeID vacío → debe caer a SessionID
	cfg.CloudLink.SessionID = "sess-42"

	if err := enroll.Run(context.Background(), cfg, sharedlogger.New(),
		enroll.WithDialOptions(grpc.WithContextDialer(dialer))); err != nil {
		t.Fatalf("Run devolvió error inesperado: %v", err)
	}

	fake.mu.Lock()
	gotCN := fake.gotCN
	fake.mu.Unlock()
	if gotCN != "sess-42" {
		t.Fatalf("CN del CSR (default SessionID): got %q, want %q", gotCN, "sess-42")
	}
}

func TestRun_PermissionDenied_NoFilesWritten(t *testing.T) {
	ca := newTestCA(t)
	fake := &fakeEnroll{ca: ca, failPermission: true}
	dialer := startServer(t, ca, fake)
	cfg, certPath, keyPath := baseConfig(t, ca, "edge-x")

	err := enroll.Run(context.Background(), cfg, sharedlogger.New(),
		enroll.WithDialOptions(grpc.WithContextDialer(dialer)))
	if err == nil {
		t.Fatalf("Run debió fallar con PermissionDenied")
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.PermissionDenied {
		t.Fatalf("el error debió mapearse a un mensaje claro, no exponer el status crudo: %v", err)
	}

	// No deben quedar archivos corruptos.
	if _, statErr := os.Stat(keyPath); !os.IsNotExist(statErr) {
		t.Errorf("no debió escribirse la clave tras PermissionDenied (stat err=%v)", statErr)
	}
	if _, statErr := os.Stat(certPath); !os.IsNotExist(statErr) {
		t.Errorf("no debió escribirse el cert tras PermissionDenied (stat err=%v)", statErr)
	}
}

func TestRun_MissingTLSCA_Errors(t *testing.T) {
	ca := newTestCA(t)
	fake := &fakeEnroll{ca: ca}
	dialer := startServer(t, ca, fake)
	cfg, _, _ := baseConfig(t, ca, "edge-x")
	cfg.CloudLink.TLSCA = "" // sin CA → no se enrola inseguro

	err := enroll.Run(context.Background(), cfg, sharedlogger.New(),
		enroll.WithDialOptions(grpc.WithContextDialer(dialer)))
	if err == nil {
		t.Fatalf("Run debió fallar sin tls_ca")
	}
}

// TestRun_PersistsCloudEncPubKey verifica (Plan 011 §6.4) que el enrolamiento persiste la pública de
// cifrado de la nube recibida en CloudEncPubKeyPath, codificada en base64 (una línea).
func TestRun_PersistsCloudEncPubKey(t *testing.T) {
	ca := newTestCA(t)
	wantPub := make([]byte, 32)
	for i := range wantPub {
		wantPub[i] = byte(i + 1)
	}
	fake := &fakeEnroll{ca: ca, encPub: wantPub}
	dialer := startServer(t, ca, fake)
	cfg, _, _ := baseConfig(t, ca, "edge-enc")
	encPath := filepath.Join(t.TempDir(), "cloud_enc_pub.b64")
	cfg.CloudLink.CloudEncPubKeyPath = encPath

	if err := enroll.Run(context.Background(), cfg, sharedlogger.New(),
		enroll.WithDialOptions(grpc.WithContextDialer(dialer))); err != nil {
		t.Fatalf("Run devolvió error inesperado: %v", err)
	}

	data, err := os.ReadFile(encPath)
	if err != nil {
		t.Fatalf("pública de cifrado no persistida: %v", err)
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("la pública persistida no es base64 válido: %v", err)
	}
	if !bytes.Equal(decoded, wantPub) {
		t.Fatalf("pública persistida distinta: got %x want %x", decoded, wantPub)
	}
}

// TestRun_NoCloudEncPubKey_NoFile verifica que si el Gateway no devuelve la pública, no se crea archivo
// (fallback claro §10.H) y el enrolamiento no falla.
func TestRun_NoCloudEncPubKey_NoFile(t *testing.T) {
	ca := newTestCA(t)
	fake := &fakeEnroll{ca: ca} // encPub nil
	dialer := startServer(t, ca, fake)
	cfg, _, _ := baseConfig(t, ca, "edge-noenc")
	encPath := filepath.Join(t.TempDir(), "cloud_enc_pub.b64")
	cfg.CloudLink.CloudEncPubKeyPath = encPath

	if err := enroll.Run(context.Background(), cfg, sharedlogger.New(),
		enroll.WithDialOptions(grpc.WithContextDialer(dialer))); err != nil {
		t.Fatalf("Run devolvió error inesperado: %v", err)
	}
	if _, err := os.Stat(encPath); !os.IsNotExist(err) {
		t.Fatalf("no debió crearse archivo de pública sin cloud_enc_pubkey: err=%v", err)
	}
}
