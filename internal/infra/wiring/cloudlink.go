// Package wiring concentra el CABLEADO del conducto CloudLink del Edge (Plan 027 T3, cierra H3): la
// construcción del sink de la escucha single-sesión (BuildSink) y del multiplexor multi-sesión (BuildMux),
// más la carga de credenciales mTLS, el dial gRPC y la carga de llaves (Validator de lease + pública de
// cifrado de la nube). Antes vivía inline en cmd/agent/main.go, donde buildSink/buildMux DUPLICABAN el
// bloque creds→dial→validator→encpub; aquí se unifica en dialCloudLink y se saca de main.go para dejar el
// comando delgado. Refactor SIN cambio de conducta: mismos fallbacks (LogSink puro / LogMux), mismos logs
// y mismo cableado del cliente vivo / acuses / LoggedOut.
//
// ZERO-KNOWLEDGE (ADR-0007): por el cable solo viaja contenido de negocio; nunca la DEK ni llaves privadas.
package wiring

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/EduGoGroup/wapp-cloudlink/lease"
	"github.com/EduGoGroup/wapp-cloudlink/mtls"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cloudlink"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/outbox"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	edgeauth "github.com/EduGoGroup/wapp-edge-agent/internal/auth"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// BuildMux construye el multiplexor CloudLink del daemon MULTI-SESIÓN (un solo stream, N sesiones por
// session_id, ADR-0008). Reusa el mismo dial mTLS y la misma factory de Validator que el camino legacy:
//
//   - Sin cfg.CloudLink.Endpoint: LogMux (diagnóstico por sesión, sin red). El daemon sigue arriba con
//     los listeners y los entrantes a log, igual que el LogSink puro hacía en el single-sesión.
//   - Con endpoint: dial gRPC (mTLS si hay cert/clave/CA; insecure en dev con advertencia) y Adapter
//     real cuyo loop de stream corre en goroutine ligada a ctx. El Manager registra cada sesión.
//
// ZERO-KNOWLEDGE: por el cable solo viaja contenido de negocio; nunca la DEK (ADR-0007).
//
// Devuelve además el RELAY de auth de operador (Plan 033 Ola 3 / ADR-0025): el mismo Adapter satisface
// edgeauth.Relay (login/refresh/logout por el stream). Cuando no hay endpoint (LogMux) el relay es nil: el
// caller cae a un relay offline (login siempre falla; no hay login offline de primera vez).
func BuildMux(ctx context.Context, cfg config.Config, log sharedlogger.Logger, ob app.Outbox, intentStack *IntentStack, collector cloudlink.HealthCollector, diagBuilder cloudlink.DiagnosticsBuilder) (sessionmgr.CloudLinkMux, edgeauth.Relay) {
	if cfg.CloudLink.Endpoint == "" {
		log.Info("CloudLink deshabilitado (sin endpoint): usando LogMux por sesión para diagnóstico")
		return cloudlink.NewLogMux(log), nil
	}

	cc, newValidator, cloudEncPub, ok := dialCloudLink(cfg.CloudLink, log, "LogMux")
	if !ok {
		return cloudlink.NewLogMux(log), nil
	}

	adapter := cloudlink.NewAdapter(cc, log, newValidator,
		cloudlink.WithCloudEncPubKey(cloudEncPub),
		// Deadline por operación del demux (Plan 027 T1, H7): un envío/descarga colgado no vive lo que vive
		// el stream ni frena a otras sesiones. Configurable por WAPP_AGENT_CLOUDLINK_COMMAND_TIMEOUT_SECONDS.
		cloudlink.WithCommandTimeout(time.Duration(cfg.CloudLink.CommandTimeoutSeconds)*time.Second),
		// Outbox durable (Plan 027 T2, H2): entrantes/acuses con el stream caído se encolan y drenan en
		// orden al reconectar en vez de descartarse. nil (fallo de init) => best-effort.
		cloudlink.WithOutbox(ob),
		// Config empujada por la nube (Plan 029 · T10): persiste/valida/notifica los ConfigUpdate. nil-safe
		// (feature off ⇒ applier nil ⇒ Ack tolerante).
		cloudlink.WithConfigApplier(intentStack.applier()),
		// Salud en el heartbeat (Plan 031 T7): cada latido lleva el SessionHealth de su sesión. nil-safe.
		cloudlink.WithHealthCollector(collector),
		// Diagnóstico bajo demanda (Plan 031 T8): responde DiagnosticsRequest con el bundle saneado. nil-safe.
		cloudlink.WithDiagnosticsBuilder(diagBuilder),
		// Modo sombra del gate de lease (D-055.4, Plan 055): con validator presente, un lease no vigente se
		// REGISTRA pero no bloquea mientras se corre el gate en campo sin haberlo visto bloquear nunca.
		// Por defecto false (fail-closed real). WAPP_AGENT_CLOUDLINK_LEASE_SHADOW_MODE.
		cloudlink.WithLeaseShadowMode(cfg.CloudLink.LeaseShadowMode),
	)
	go func() {
		_ = adapter.Run(ctx)
		_ = cc.Close()
	}()

	log.Info("CloudLink habilitado (multi-sesión): un stream multiplexado por session_id",
		"endpoint", cfg.CloudLink.Endpoint, "lease_gate", newValidator != nil, "sealed_transit", cloudEncPub != nil)
	return adapter, adapter
}

// BuildOutbox construye el outbox durable (Plan 027 Ola 3 · T2, cierra H2 / ADR-0003) sobre la BD ÚNICA ya
// migrada (la tabla `outbox` la crea db.Migrate). Aplica los límites de config (tamaño + TTL). NO es fatal:
// si la construcción falla (p.ej. no se pudo sembrar la secuencia), devuelve nil y el Adapter cae al
// best-effort previo — la durabilidad es una mejora, no un requisito de arranque.
func BuildOutbox(ctx context.Context, cfg config.Config, database *sql.DB, log sharedlogger.Logger) app.Outbox {
	ob, err := outbox.New(ctx, database, cfg.OutboxMaxEvents, cfg.OutboxTTLHours, log)
	if err != nil {
		log.Error("outbox durable: no se pudo inicializar; se sigue en best-effort (sin durabilidad)", "error", err)
		return nil
	}
	log.Info("outbox durable habilitado (ADR-0003): entrantes/acuses con stream caído se encolan y drenan al reconectar",
		"max_eventos", cfg.OutboxMaxEvents, "ttl_horas", cfg.OutboxTTLHours)
	return ob
}

// dialCloudLink concentra el bloque COMÚN de BuildSink/BuildMux (H3: antes duplicado ~90 líneas): valida
// las credenciales mTLS, crea el cliente gRPC, y carga la factory del Validator de lease + la pública de
// cifrado de la nube. Devuelve ok=false (tras loguear con la etiqueta de fallback y cerrar cc si ya se
// había creado) si algún paso falla, para que el caller caiga a su sink de diagnóstico (LogSink/LogMux).
// En éxito el caller es dueño de cc (lo cierra tras adapter.Run).
func dialCloudLink(cl config.CloudLinkConfig, log sharedlogger.Logger, fallback string) (*grpc.ClientConn, cloudlink.ValidatorFactory, []byte, bool) {
	creds, err := clientCreds(cl, log)
	if err != nil {
		log.Error("CloudLink: credenciales mTLS inválidas, cayendo a "+fallback, "error", err)
		return nil, nil, nil, false
	}

	cc, err := grpc.NewClient(cl.Endpoint, cloudLinkDialOpts(creds)...)
	if err != nil {
		log.Error("CloudLink: no se pudo crear el cliente gRPC, cayendo a "+fallback, "error", err)
		return nil, nil, nil, false
	}

	newValidator, err := loadValidatorFactory(cl, log)
	if err != nil {
		log.Error("CloudLink: clave pública de lease inválida, cayendo a "+fallback, "error", err)
		_ = cc.Close()
		return nil, nil, nil, false
	}

	cloudEncPub, err := loadCloudEncPubKey(cl, log)
	if err != nil {
		log.Error("CloudLink: clave pública de cifrado de la nube inválida, cayendo a "+fallback, "error", err)
		_ = cc.Close()
		return nil, nil, nil, false
	}

	return cc, newValidator, cloudEncPub, true
}

// cloudLinkKeepalive es la política de keepalive de TRANSPORTE del cliente gRPC del stream CloudLink
// (Plan 026 T3, design §4.a). Envía un PING de HTTP/2 cada Time y espera Timeout por el ACK antes de dar
// la conexión por muerta; PermitWithoutStream mantiene el keepalive incluso sin RPC activas (el stream
// bidi puede estar quieto sin tráfico). Detecta cortes de NAT/red ANTES que el Ping app-level y el
// backoff, que se CONSERVAN (no se eliminan): el backoff sigue gobernando la reconexión. Time=30s es >
// que la MinTime=15s del server (otro tramo, cloud-platform) para NO ser expulsado con GOAWAY
// too_many_pings.
var cloudLinkKeepalive = keepalive.ClientParameters{
	Time:                30 * time.Second,
	Timeout:             10 * time.Second,
	PermitWithoutStream: true,
}

// cloudLinkDialOpts arma las DialOptions del dial de runtime CloudLink: las transport-credentials
// (mTLS/insecure) más el keepalive de transporte (cloudLinkKeepalive). Compartido por BuildSink
// (single-sesión) y BuildMux (multi-sesión) para no duplicar la política.
func cloudLinkDialOpts(creds credentials.TransportCredentials) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(cloudLinkKeepalive),
	}
}

// clientCreds construye las transport-credentials del dial CloudLink: mTLS si están las tres rutas
// (cert/clave/CA); insecure en dev (con advertencia) si faltan.
func clientCreds(cl config.CloudLinkConfig, log sharedlogger.Logger) (credentials.TransportCredentials, error) {
	if cl.TLSCert != "" && cl.TLSKey != "" && cl.TLSCA != "" {
		serverName := cl.ServerName
		if serverName == "" {
			host, _, splitErr := net.SplitHostPort(cl.Endpoint)
			if splitErr == nil {
				serverName = host
			} else {
				serverName = cl.Endpoint
			}
		}
		return mtls.LoadClientCredsFromFiles(cl.TLSCert, cl.TLSKey, cl.TLSCA, serverName)
	}
	log.Warn("CloudLink: sin material mTLS (cert/clave/CA); dial INSECURE — solo desarrollo")
	return insecure.NewCredentials(), nil
}

// loadValidatorFactory construye la FACTORY del Validator del gate de lease si hay clave pública
// configurada. Acepta la clave en hex o como 32 bytes crudos y la parsea UNA vez; la factory devuelve un
// Validator FRESCO (estado de lease propio) por sesión (lease por sesión, ADR-0016 §5) sobre esa misma
// clave del Edge. Devuelve nil (sin gate) si no hay ruta configurada o si el archivo aún no existe
// (best-effort, mismo criterio que loadCloudEncPubKey); error solo si el archivo existe pero es
// ilegible o de tamaño inválido.
func loadValidatorFactory(cl config.CloudLinkConfig, log sharedlogger.Logger) (cloudlink.ValidatorFactory, error) {
	if cl.LeasePubKeyPath == "" {
		log.Warn("CloudLink: sin clave pública de lease; gate de kill-switch DESACTIVADO (solo desarrollo)")
		return nil, nil
	}
	raw, err := os.ReadFile(cl.LeasePubKeyPath)
	if err != nil {
		// Best-effort, MISMO patrón que loadCloudEncPubKey (H-5 fix, Plan 055 T4.3): si el archivo
		// simplemente NO EXISTE TODAVÍA (lease_pubkey_path configurado por adelantado, p.ej. vía T5.3, pero
		// el enrolamiento aún no corrió o corrió contra una nube vieja que no manda lease_pubkey), NO se
		// propaga como error duro. Un error duro aquí hace que dialCloudLink() tire TODO el Adapter a
		// LogMux (cae también el envío/recepción real, no solo el gate) — mucho peor que "gate apagado".
		// Solo es error real un archivo que EXISTE pero es ilegible o corrupto.
		if os.IsNotExist(err) {
			log.Warn("CloudLink: lease_pubkey_path configurado pero el archivo aún no existe; gate de kill-switch DESACTIVADO hasta el próximo enrolamiento",
				"path", cl.LeasePubKeyPath)
			return nil, nil
		}
		return nil, err
	}
	pub := raw
	if decoded, decErr := hex.DecodeString(strings.TrimSpace(string(raw))); decErr == nil && len(decoded) == ed25519.PublicKeySize {
		pub = decoded
	}
	if len(pub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("clave pública de lease con tamaño inválido: %d (esperado %d)", len(pub), ed25519.PublicKeySize)
	}
	return func() *lease.Validator { return lease.NewValidator(ed25519.PublicKey(pub)) }, nil
}

// loadCloudEncPubKey carga la clave pública X25519 (32B) de cifrado de la nube desde CloudEncPubKeyPath
// para el sellado en tránsito (Plan 011 §6.3). Acepta la clave en base64 (formato de persistencia del
// enrolamiento) o como 32 bytes crudos. Devuelve nil (fallback claro §10.H) si no hay ruta o el archivo
// no existe; error solo si existe pero es ilegible o de tamaño inválido.
func loadCloudEncPubKey(cl config.CloudLinkConfig, log sharedlogger.Logger) ([]byte, error) {
	if cl.CloudEncPubKeyPath == "" {
		log.Warn("CloudLink: sin clave pública de cifrado de la nube; sellado en tránsito DESACTIVADO (fallback claro §10.H)")
		return nil, nil
	}
	raw, err := os.ReadFile(cl.CloudEncPubKeyPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Warn("CloudLink: cloud_enc_pubkey_path no existe aún; sellado en tránsito DESACTIVADO (fallback claro §10.H)",
				"path", cl.CloudEncPubKeyPath)
			return nil, nil
		}
		return nil, err
	}
	pub := raw
	if decoded, decErr := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw))); decErr == nil && len(decoded) == 32 {
		pub = decoded
	}
	if len(pub) != 32 {
		return nil, fmt.Errorf("clave pública de cifrado de la nube con tamaño inválido: %d (esperado 32)", len(pub))
	}
	return pub, nil
}
