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
	waconn "github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// BuildSink construye el InboundSink de la escucha 24/7 (camino single-sesión: listen/restore).
//
//   - Sin cfg.CloudLink.Endpoint: LogSink PURO (diagnóstico, sin red). Mantiene el comportamiento del
//     spike intacto (pair/send/listen siguen funcionando sin nube).
//   - Con endpoint: dial gRPC (mTLS si hay cert/clave/CA; insecure en dev con advertencia), se construye
//     el Adapter CloudLink real conectándolo a app.Send vía SendFunc, y se devuelve un TEE (Adapter
//     primario + LogSink de diagnóstico). El loop de conexión del Adapter corre en goroutine ligada a ctx.
//     ZERO-KNOWLEDGE: por el cable solo viaja contenido de negocio; nunca la DEK.
func BuildSink(ctx context.Context, cfg config.Config, log sharedlogger.Logger, custody app.KeyCustody, database *sql.DB, gateway *waconn.ListenGateway) app.InboundSink {
	logSink := cloudlink.NewLogSink(log)
	if cfg.CloudLink.Endpoint == "" {
		log.Info("CloudLink deshabilitado (sin endpoint): usando LogSink puro para diagnóstico")
		return logSink
	}

	cc, newValidator, cloudEncPub, ok := dialCloudLink(cfg.CloudLink, log, "LogSink puro")
	if !ok {
		return logSink
	}

	// SendFunc: conecta los comandos SendText de la nube al despachador del Edge. Prioriza el CLIENTE
	// VIVO de la escucha (una sola conexión por sesión): con la misma identidad multi-dispositivo, un
	// cliente efímero aparte reemplazaría la conexión y dejaría la escucha sorda. Si el gateway no
	// expone un emisor vivo (defensivo), cae al sender efímero (NewClient+Connect+Disconnect por envío).
	var sendFunc func(ctx context.Context, commandID, to, text string) error
	var sendMediaFunc func(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) error
	if liveSender, ok := any(gateway).(app.LiveSender); ok && gateway != nil {
		// Variante TRACKED (Plan 013 §10.E): el envío puebla el Correlator del gateway con el command_id
		// para que el acuse posterior suba correlacionado.
		sendFunc = func(ctx context.Context, commandID, to, text string) error {
			_, err := liveSender.SendViaLiveClientTracked(ctx, commandID, to, text)
			return err
		}
		// Emisor de ARCHIVOS por cliente vivo (Plan 017 §7): descarga la presigned URL (GET sin
		// credenciales) y sube el binario por la misma conexión, correlacionando por command_id.
		sendMediaFunc = func(ctx context.Context, commandID, to, presignedURL, filename, mime, kind, caption string) error {
			_, err := liveSender.SendMediaViaLiveClientTracked(ctx, commandID, to, presignedURL, filename, mime, kind, caption)
			return err
		}
		log.Info("CloudLink: el envío reutilizará el CLIENTE VIVO de la escucha (conexión única por sesión)")
	} else {
		// Camino efímero (defensivo, sin cliente vivo): no hay Correlator que alimentar; el command_id se
		// ignora y el acuse subiría como estado crudo.
		sendUC := app.NewSend(custody, waconn.NewSender(database, db.DialectSQLite))
		sendFunc = func(ctx context.Context, _ /*commandID*/, to, text string) error { return sendUC.Run(ctx, to, text) }
		sendMediaFunc = func(ctx context.Context, _ /*commandID*/, to, presignedURL, filename, mime, kind, caption string) error {
			return sendUC.RunMedia(ctx, to, presignedURL, filename, mime, kind, caption)
		}
	}

	// El Adapter es un multiplexor (un stream por Edge). El camino legacy single-sesión registra LA
	// única sesión (cfg.CloudLink.SessionID) y usa SU sink etiquetado; la mecánica de mux es idéntica a
	// la del daemon multi-sesión (runServe), solo que aquí hay una sola sesión.
	adapter := cloudlink.NewAdapter(cc, log, newValidator, cloudlink.WithCloudEncPubKey(cloudEncPub))
	// Camino single-sesión (listen/restore): el JID propio no está a mano aquí (la config solo trae el
	// session_id); se registra con selfJID "" (el Cloud tolera vacío, Plan 020 T2). El número propio se
	// reporta de raíz por el daemon multi-sesión (runServe/BuildMux), donde s.meta.JID sí está poblado.
	adapter.Register(cfg.CloudLink.SessionID, "", sendFunc, sendMediaFunc, custody.Exists)
	// Acuses (Plan 013 T2a): al llegar un events.Receipt, etiqueta con el session_id, correlaciona con el
	// command_id del envío (Correlator del gateway vivo) y sube el MessageReceipt por el mismo stream.
	sid := cfg.CloudLink.SessionID
	gateway.SetReceiptHandler(func(evt domain.ReceiptEvent) {
		evt.SessionID = sid
		cmd, _ := gateway.Correlator().Lookup(evt.MessageIDs)
		adapter.SendReceipt(cmd, evt)
	})
	// LoggedOut (Plan 020 T3): propaga el estado ZOMBIE al cloud cuando WhatsApp cierra la sesión.
	gateway.SetLoggedOutHandler(func() { adapter.SendLoggedOut(sid) })
	go func() {
		_ = adapter.Run(ctx)
		_ = cc.Close()
	}()

	log.Info("CloudLink habilitado: reenviando entrantes y atendiendo comandos cloud->edge",
		"endpoint", cfg.CloudLink.Endpoint, "session_id", cfg.CloudLink.SessionID,
		"lease_gate", newValidator != nil, "sealed_transit", cloudEncPub != nil)
	return cloudlink.NewTeeSink(adapter.SinkFor(cfg.CloudLink.SessionID), logSink)
}

// BuildMux construye el multiplexor CloudLink del daemon MULTI-SESIÓN (un solo stream, N sesiones por
// session_id, ADR-0008). Reusa el mismo dial mTLS y la misma factory de Validator que el camino legacy:
//
//   - Sin cfg.CloudLink.Endpoint: LogMux (diagnóstico por sesión, sin red). El daemon sigue arriba con
//     los listeners y los entrantes a log, igual que el LogSink puro hacía en el single-sesión.
//   - Con endpoint: dial gRPC (mTLS si hay cert/clave/CA; insecure en dev con advertencia) y Adapter
//     real cuyo loop de stream corre en goroutine ligada a ctx. El Manager registra cada sesión.
//
// ZERO-KNOWLEDGE: por el cable solo viaja contenido de negocio; nunca la DEK (ADR-0007).
func BuildMux(ctx context.Context, cfg config.Config, log sharedlogger.Logger) sessionmgr.CloudLinkMux {
	if cfg.CloudLink.Endpoint == "" {
		log.Info("CloudLink deshabilitado (sin endpoint): usando LogMux por sesión para diagnóstico")
		return cloudlink.NewLogMux(log)
	}

	cc, newValidator, cloudEncPub, ok := dialCloudLink(cfg.CloudLink, log, "LogMux")
	if !ok {
		return cloudlink.NewLogMux(log)
	}

	adapter := cloudlink.NewAdapter(cc, log, newValidator,
		cloudlink.WithCloudEncPubKey(cloudEncPub),
		// Deadline por operación del demux (Plan 027 T1, H7): un envío/descarga colgado no vive lo que vive
		// el stream ni frena a otras sesiones. Configurable por WAPP_AGENT_CLOUDLINK_COMMAND_TIMEOUT_SECONDS.
		cloudlink.WithCommandTimeout(time.Duration(cfg.CloudLink.CommandTimeoutSeconds)*time.Second),
	)
	go func() {
		_ = adapter.Run(ctx)
		_ = cc.Close()
	}()

	log.Info("CloudLink habilitado (multi-sesión): un stream multiplexado por session_id",
		"endpoint", cfg.CloudLink.Endpoint, "lease_gate", newValidator != nil, "sealed_transit", cloudEncPub != nil)
	return adapter
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
// clave del Edge. Devuelve nil (sin gate) si no hay ruta configurada.
func loadValidatorFactory(cl config.CloudLinkConfig, log sharedlogger.Logger) (cloudlink.ValidatorFactory, error) {
	if cl.LeasePubKeyPath == "" {
		log.Warn("CloudLink: sin clave pública de lease; gate de kill-switch DESACTIVADO (solo desarrollo)")
		return nil, nil
	}
	raw, err := os.ReadFile(cl.LeasePubKeyPath)
	if err != nil {
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
