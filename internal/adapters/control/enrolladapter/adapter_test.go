package enrolladapter_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control/enrolladapter"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

func TestAdapter_Enrolled(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")

	cfg := config.Config{}
	cfg.CloudLink.TLSCert = certPath
	cfg.CloudLink.TLSKey = keyPath

	log := sharedlogger.Default()
	adapter := enrolladapter.New(cfg, log)

	if adapter.Enrolled() {
		t.Fatal("esperaba Enrolled=false antes de crear los archivos")
	}

	if err := os.WriteFile(certPath, []byte("cert"), 0600); err != nil {
		t.Fatalf("WriteFile cert: %v", err)
	}
	if err := os.WriteFile(keyPath, []byte("key"), 0600); err != nil {
		t.Fatalf("WriteFile key: %v", err)
	}

	if !adapter.Enrolled() {
		t.Fatal("esperaba Enrolled=true con cert y key presentes")
	}
}

func TestAdapter_Enroll_Validation(t *testing.T) {
	log := sharedlogger.Default()
	ctx := context.Background()

	// 1. Sin enrollment endpoint
	cfg := config.Config{}
	adapter := enrolladapter.New(cfg, log)
	if err := adapter.Enroll(ctx, "code"); err == nil {
		t.Fatal("esperaba error sin enrollment_endpoint")
	}

	// 2. Sin TLSCA
	cfg.CloudLink.EnrollmentEndpoint = "localhost:8102"
	adapter = enrolladapter.New(cfg, log)
	if err := adapter.Enroll(ctx, "code"); err == nil {
		t.Fatal("esperaba error sin tls_ca")
	}

	// 3. Sin TLSCert/TLSKey
	cfg.CloudLink.TLSCA = "/path/ca.crt"
	adapter = enrolladapter.New(cfg, log)
	if err := adapter.Enroll(ctx, "code"); err == nil {
		t.Fatal("esperaba error sin tls_cert/tls_key")
	}

	// 4. Sin activation code
	cfg.CloudLink.TLSCert = "/path/tls.crt"
	cfg.CloudLink.TLSKey = "/path/tls.key"
	adapter = enrolladapter.New(cfg, log)
	if err := adapter.Enroll(ctx, ""); err == nil {
		t.Fatal("esperaba error con código de activación vacío")
	}
}
