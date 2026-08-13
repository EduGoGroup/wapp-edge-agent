package wiring

// cloudlink_test.go verifica (T4.3, Plan 055, cierra H-5) el ROUND-TRIP completo del gate de
// kill-switch: enroll.Run persiste la lease_pubkey recibida del Gateway en LeasePubKeyPath (vía
// persistLeasePubKey, formato HEX — no base64, ver comentario en enroll.go) y loadValidatorFactory (de
// ESTE paquete) la relee y construye un Validator FUNCIONAL con exactamente esa clave. El test ejercita
// las DOS funciones reales de producción (enroll.Run y loadValidatorFactory), no una copia del
// encoding/decoding.
//
// Se pone rojo si: persistLeasePubKey cambia de formato (p.ej. vuelve a base64, como su hermana
// persistCloudEncPubKey) sin que loadValidatorFactory se actualice a la par; si loadValidatorFactory
// deja de aceptar el formato hex que enroll.Run realmente escribe; o si cualquiera de las dos funciones
// deja de mover los mismos 32 bytes de principio a fin.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cloudlinkv1 "github.com/EduGoGroup/wapp-cloudlink/gen/wapp/cloudlink/v1"
	"github.com/EduGoGroup/wapp-cloudlink/lease"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/enroll"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/test/bufconn"
)

// leaseFakeEnroll implementa cloudlinkv1.EnrollmentServer con lo mínimo para que enroll.Run complete:
// firma el CSR con una CA de prueba propia y devuelve una lease_pubkey fija en leasePub (Plan 055
// D-055.5; el campo aún no existe en el proto generado — ver informe de la tarea).
type leaseFakeEnroll struct {
	cloudlinkv1.UnimplementedEnrollmentServer
	caCert   *x509.Certificate
	caKey    *ecdsa.PrivateKey
	leasePub []byte
}

func (f *leaseFakeEnroll) EnrollEdge(_ context.Context, req *cloudlinkv1.EnrollEdgeRequest) (*cloudlinkv1.EnrollEdgeResponse, error) {
	block, _ := pem.Decode(req.GetCsrPem())
	if block == nil {
		return nil, context.DeadlineExceeded
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, err
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, err
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: csr.Subject.CommonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, f.caCert, csr.PublicKey, f.caKey)
	if err != nil {
		return nil, err
	}
	return &cloudlinkv1.EnrollEdgeResponse{
		EdgeCertPem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		CaChainPem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.caCert.Raw}),
		TenantId:    "tenant-test",
		LeasePubkey: f.leasePub,
	}, nil
}

// TestLoadValidatorFactory_RoundTripsEnrollPersistedLeasePubKey ejercita el round-trip completo
// enroll.Run -> disco -> loadValidatorFactory, verificado en dos niveles: (1) el archivo persistido,
// decodificado en HEX, contiene exactamente los mismos 32 bytes que emitió el Issuer de prueba; (2) el
// Validator que construye la factory devuelta ACEPTA un LeaseUpdate firmado por ESE mismo Issuer — solo
// posible si loadValidatorFactory reconstruyó la clave pública correcta (una clave distinta habría
// hecho fallar la verificación Ed25519 con ErrBadSignature).
func TestLoadValidatorFactory_RoundTripsEnrollPersistedLeasePubKey(t *testing.T) {
	// CA de prueba para el dial TLS-de-servidor del enrolamiento (igual que enroll_test.go).
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generar clave CA: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "wApp Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("crear cert CA: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parsear cert CA: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generar clave servidor: %v", err)
	}
	serverTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTmpl, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("crear cert servidor: %v", err)
	}

	// Issuer real del lease (lado nube): su pública es la que se "enrola" en este test y su privada
	// firma el LeaseUpdate de prueba que el Validator reconstruido deberá aceptar.
	leasePub, leasePriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generar clave del lease: %v", err)
	}
	issuer, err := lease.NewIssuer(leasePriv)
	if err != nil {
		t.Fatalf("lease.NewIssuer: %v", err)
	}

	fake := &leaseFakeEnroll{caCert: caCert, caKey: caKey, leasePub: leasePub}

	lis := bufconn.Listen(1024 * 1024)
	tlsCreds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{serverDER}, PrivateKey: serverKey}},
		MinVersion:   tls.VersionTLS13,
	})
	srv := grpc.NewServer(grpc.Creds(tlsCreds))
	cloudlinkv1.RegisterEnrollmentServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("escribir CA: %v", err)
	}
	leasePath := filepath.Join(dir, "lease_pubkey")

	cfg := config.Config{
		CloudLink: config.CloudLinkConfig{
			EnrollmentEndpoint: "passthrough:///bufnet",
			ServerName:         "localhost",
			TLSCA:              caPath,
			TLSCert:            filepath.Join(dir, "certs", "edge.crt"),
			TLSKey:             filepath.Join(dir, "certs", "edge.key"),
			ActivationCode:     "good-code",
			EdgeID:             "edge-lease-roundtrip",
			LeasePubKeyPath:    leasePath,
		},
	}

	if err := enroll.Run(context.Background(), cfg, sharedlogger.New(),
		enroll.WithDialOptions(grpc.WithContextDialer(dialer))); err != nil {
		t.Fatalf("enroll.Run devolvió error inesperado: %v", err)
	}

	// Nivel 1: el archivo en disco es HEX, no base64 (la trampa verificada del plan T4.3).
	raw, err := os.ReadFile(leasePath)
	if err != nil {
		t.Fatalf("lease_pubkey no persistida: %v", err)
	}
	decoded, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("la lease_pubkey persistida no es hex válido (¿se escribió en base64 por error?): %v", err)
	}
	if !bytes.Equal(decoded, leasePub) {
		t.Fatalf("lease_pubkey persistida distinta: got %x want %x", decoded, leasePub)
	}

	// Nivel 2: loadValidatorFactory (la función REAL del lector, no una copia del parseo) relee ese
	// mismo archivo y construye un Validator que verifica firmas del Issuer real.
	factory, err := loadValidatorFactory(cfg.CloudLink, sharedlogger.New())
	if err != nil {
		t.Fatalf("loadValidatorFactory devolvió error inesperado: %v", err)
	}
	if factory == nil {
		t.Fatalf("loadValidatorFactory devolvió factory nil con LeasePubKeyPath configurado")
	}
	validator := factory()

	update, err := issuer.Issue("edge-lease-roundtrip", "tenant-test", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("issuer.Issue: %v", err)
	}
	if err := validator.Apply(update); err != nil {
		t.Fatalf("el Validator reconstruido por loadValidatorFactory RECHAZÓ un lease firmado por el "+
			"Issuer real (la clave releída no coincide con la persistida por enroll.Run): %v", err)
	}
	if !validator.CanOperate(true) {
		t.Fatalf("CanOperate debió ser true tras aplicar un lease vigente firmado con la clave correcta")
	}
}

// TestLoadValidatorFactory_MissingFile_DegradesWithoutError verifica que un LeasePubKeyPath CONFIGURADO
// pero cuyo archivo TODAVÍA no existe (p.ej. re-enrolamiento pendiente, o una nube vieja que no manda
// lease_pubkey — ver TestRun_NoLeasePubKey_NoFile en internal/infra/enroll) degrada como el resto del
// camino best-effort: factory nil (gate apagado), SIN error. Antes de este fix, os.ReadFile devolvía el
// error de "no such file" tal cual, y ESE error propaga hasta dialCloudLink(), que tira TODO el Adapter
// a LogMux — no solo el gate, también el envío/recepción real — mucho peor que "gate apagado". Se pone
// rojo si loadValidatorFactory vuelve a propagar el error crudo de os.ReadFile para un archivo ausente.
func TestLoadValidatorFactory_MissingFile_DegradesWithoutError(t *testing.T) {
	cl := config.CloudLinkConfig{
		LeasePubKeyPath: filepath.Join(t.TempDir(), "no-existe-todavia"),
	}

	factory, err := loadValidatorFactory(cl, sharedlogger.New())
	if err != nil {
		t.Fatalf("loadValidatorFactory devolvió error para un archivo ausente (debía degradar sin error): %v", err)
	}
	if factory != nil {
		t.Fatalf("loadValidatorFactory devolvió factory no-nil para un archivo ausente (gate debía quedar apagado)")
	}
}
