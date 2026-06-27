// Command agent es el daemon del Edge Agent de wApp.
//
// Bootstrap minimo (T0, Plan 002): carga configuracion, construye el logger y
// registra el arranque. El subcomando `pair` (T3.4) ejecuta el emparejamiento por
// QR local con los adaptadores REALES (store SQLite cifrado + whatsmeow + control
// en terminal + custodia de la DEK en archivo). El subcomando `send` (T4.3) despacha
// un texto a un destino usando la sesion ya pareada. El subcomando `listen` (T5.5)
// mantiene el socket VIVO 24/7 (always-on), reenviando cada mensaje entrante al LogSink
// (stub CloudLink del spike) hasta Ctrl-C / SIGINT. La logica restante (CloudLink real,
// systray) se incorpora en chunks posteriores.
package main

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"time"

	"github.com/EduGoGroup/wapp-cloudlink/lease"
	"github.com/EduGoGroup/wapp-cloudlink/mtls"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/cloudlink"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/keycustody"
	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/sessionstore"
	waconn "github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/config"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/enroll"
	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/logger"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Version identifica la build del Edge Agent.
const Version = "0.1.0-bootstrap"

func main() {
	path := os.Getenv("WAPP_AGENT_CONFIG")
	if path == "" {
		path = "config.yaml"
	}

	cfg, err := config.Load(path)
	if err != nil {
		sharedlogger.Default().Error("no se pudo cargar la configuracion",
			"error", err, "path", path)
		os.Exit(1)
	}

	log := logger.New(cfg)

	// Despacho de subcomandos. Sin argumento: bootstrap (arranque informativo).
	if len(os.Args) > 1 && os.Args[1] == "pair" {
		if err := runPair(context.Background(), cfg, log); err != nil {
			log.Error("emparejamiento fallido", "error", err)
			os.Exit(1)
		}
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "send" {
		if len(os.Args) < 4 {
			log.Error("uso: agent send <destino> <texto>")
			os.Exit(1)
		}
		if err := runSend(context.Background(), cfg, log, os.Args[2], os.Args[3]); err != nil {
			log.Error("envio fallido", "error", err)
			os.Exit(1)
		}
		return
	}

	// `enroll` ejecuta el enrolamiento real contra el Gateway: genera el par mTLS del Edge a partir de
	// un código de activación y persiste cert+clave en TLSCert/TLSKey para que `listen` use mTLS.
	if len(os.Args) > 1 && os.Args[1] == "enroll" {
		if err := runEnroll(context.Background(), cfg, log); err != nil {
			log.Error("enrolamiento fallido", "error", err)
			os.Exit(1)
		}
		return
	}

	// `listen` y `restore` comparten flujo: restaurar la sesión persistida (T6.2) y mantener el
	// socket vivo 24/7. Se mantienen ambos nombres por claridad del hito T6.3.
	if len(os.Args) > 1 && (os.Args[1] == "listen" || os.Args[1] == "restore") {
		if err := runRestore(cfg, log); err != nil {
			log.Error("restauración/escucha fallida", "error", err)
			os.Exit(1)
		}
		return
	}

	log.Info("wapp-edge-agent arrancando",
		"version", Version,
		"log_level", cfg.LogLevel,
		"log_json", cfg.LogJSON,
		"db_path", cfg.DBPath,
		"config_path", path,
	)
}

// runPair ejecuta el caso de uso app.Pair con los adaptadores REALES: abre/migra el store SQLite
// cifrado, construye el conector whatsmeow real, pinta el QR en la terminal (os.Stdout) y sella la
// DEK en la custodia de archivo. Es interactivo: requiere escanear el QR con un telefono real.
func runPair(ctx context.Context, cfg config.Config, log sharedlogger.Logger) error {
	database, err := db.OpenAndMigrate(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()

	connector := waconn.NewConnector(database)
	qrSink := control.NewTerminalQRSink(os.Stdout)
	custody := keycustody.NewFileCustody(cfg.DEKPath)

	log.Info("emparejamiento: escanea el QR con WhatsApp (Dispositivos vinculados)",
		"db_path", cfg.DBPath, "dek_path", cfg.DEKPath)

	pairer := app.NewPair(connector, qrSink, custody)
	res, err := pairer.Run(ctx)
	if err != nil {
		return err
	}

	// Registra los METADATOS DE NEGOCIO de la sesión recién pareada (RF-7) para que RestoreSessions
	// la reanude al reiniciar. En claro: session_id + jid + estado + store_dir + timestamps (sin
	// material cripto).
	//
	// PUENTE T0 (Plan 008): el modelo multi-sesión llava por session_id (ADR-0016 §3), así que aquí se
	// genera el UUID de la sesión y su store_dir relativo. La GENERALIZACIÓN real (el Manager genera el
	// session_id ANTES del pairing, mkdir del dir por sesión, Upsert(pairing)->Upsert(active)) llega en
	// T3; este `agent pair` single-sesión es legacy y se reescribe entonces.
	now := time.Now()
	sessionID := uuid.NewString()
	sess := domain.Session{
		SessionID: sessionID,
		JID:       res.WaJID,
		State:     domain.SessionStateActive,
		StoreDir:  "sessions/" + sessionID,
		PairedAt:  now,
		UpdatedAt: now,
	}
	if err := sessionstore.New(database).Upsert(ctx, sess); err != nil {
		return err
	}

	log.Info("emparejamiento completado (PairSuccess): DEK sellada, store cifrado creado y sesión registrada",
		"wa_jid", res.WaJID, "db_path", cfg.DBPath, "dek_path", cfg.DEKPath)
	return nil
}

// runSend ejecuta el caso de uso app.Send con los adaptadores REALES: abre/migra el store SQLite
// cifrado, carga la DEK custodiada en archivo, construye el sender whatsmeow real (que resuelve la
// sesion pareada, conecta un cliente efimero y despacha el texto) y envia. Requiere una sesion ya
// emparejada (subcomando `pair`); envia por red de verdad (es el hito interactivo T4.3).
func runSend(ctx context.Context, cfg config.Config, log sharedlogger.Logger, to, text string) error {
	database, err := db.OpenAndMigrate(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()

	custody := keycustody.NewFileCustody(cfg.DEKPath)
	sender := waconn.NewSender(database)

	log.Info("envio: despachando texto a WhatsApp",
		"to", to, "db_path", cfg.DBPath, "dek_path", cfg.DEKPath)

	if err := app.NewSend(custody, sender).Run(ctx, to, text); err != nil {
		return err
	}

	log.Info("envio completado: texto despachado a WhatsApp", "to", to)
	return nil
}

// runRestore ejecuta el caso de uso app.RestoreSessions con los adaptadores REALES (T6.2/T6.3): abre/
// migra el store SQLite cifrado (aplica 0001 + 0002), resuelve la sesion a restaurar desde la tabla
// `sessions` (o la backfillea desde el store cifrado si la BD se pareo antes de existir esa tabla),
// marca la sesion activa y DELEGA la escucha always-on a app.Listen (que carga la DEK custodiada,
// reconstruye el device pareado, conecta el cliente y registra el Listener). Reenvia cada mensaje
// entrante al LogSink (stub CloudLink del spike) y mantiene el socket VIVO hasta Ctrl-C / SIGINT.
// Requiere una sesion ya emparejada (subcomando `pair`); reanuda SIN re-emparejar (es el hito T6.3).
func runRestore(cfg config.Config, log sharedlogger.Logger) error {
	// ctx cancelado por SIGINT (Ctrl-C) o SIGTERM: dispara el cierre limpio del socket always-on.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	database, err := db.OpenAndMigrate(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()

	custody := keycustody.NewFileCustody(cfg.DEKPath)
	gateway := waconn.NewListenGateway(database, log)
	sessions := sessionstore.New(database)
	locator := sessionstore.NewLocator(database)

	// Sink: conducto CloudLink REAL si hay endpoint configurado (con LogSink en tee para diagnóstico);
	// si no, LogSink puro (no rompe el flujo del spike). El adaptador corre su loop de conexión en
	// segundo plano, ligado al mismo ctx (se cierra con SIGINT junto al socket).
	sink := buildSink(ctx, cfg, log, custody, database, gateway)

	// app.Listen hace el restore CRIPTOGRAFICO + socket always-on; RestoreSessions le antepone el
	// registro de negocio (resolver/backfillear/marcar activa la sesion). Sin duplicar la conexion.
	listener := app.NewListen(custody, gateway, sink)
	restore := app.NewRestoreSessions(sessions, locator, listener)

	log.Info("restaurando sesion persistida y manteniendo el socket vivo (envia un WhatsApp al numero para ver el InboundEvent; Ctrl-C para detener)",
		"db_path", cfg.DBPath, "dek_path", cfg.DEKPath)

	if err := restore.Run(ctx); err != nil {
		return err
	}

	log.Info("restauracion/escucha finalizada: socket cerrado limpiamente")
	return nil
}

// runEnroll cablea el subcomando `enroll`: lee el código de activación de cfg o de os.Args
// (`agent enroll <codigo>`), valida precondiciones (endpoint de enrolamiento, TLSCA pre-provista y
// código presentes) y delega al paquete enroll, que genera el par mTLS y lo persiste en TLSCert/TLSKey.
// No toca pair/send/listen. La TLSCA DEBE estar pre-provista antes de enrolar (valida al Gateway).
func runEnroll(ctx context.Context, cfg config.Config, log sharedlogger.Logger) error {
	// Override opcional del código por argumento posicional: `agent enroll <codigo>`.
	if len(os.Args) > 2 && os.Args[2] != "" {
		cfg.CloudLink.ActivationCode = os.Args[2]
	}

	if cfg.CloudLink.EnrollmentEndpoint == "" {
		return fmt.Errorf("falta enrollment_endpoint (configura cloudlink.enrollment_endpoint o WAPP_AGENT_CLOUDLINK_ENROLLMENT_ENDPOINT)")
	}
	if cfg.CloudLink.TLSCA == "" {
		return fmt.Errorf("falta tls_ca: la CA que valida al Gateway debe estar pre-provista antes de enrolar")
	}
	if cfg.CloudLink.ActivationCode == "" {
		return fmt.Errorf("falta el código de activación (usa `agent enroll <codigo>` o WAPP_AGENT_CLOUDLINK_ACTIVATION_CODE)")
	}

	log.Info("enrolando el Edge contra el Gateway",
		"endpoint", cfg.CloudLink.EnrollmentEndpoint, "tls_cert", cfg.CloudLink.TLSCert, "tls_key", cfg.CloudLink.TLSKey)

	return enroll.Run(ctx, cfg, log)
}

// buildSink construye el InboundSink de la escucha 24/7.
//
//   - Sin cfg.CloudLink.Endpoint: LogSink PURO (diagnóstico, sin red). Mantiene el comportamiento del
//     spike intacto (pair/send/listen siguen funcionando sin nube).
//   - Con endpoint: dial gRPC (mTLS si hay cert/clave/CA; insecure en dev con advertencia), se
//     construye el Adapter CloudLink real conectándolo a app.Send vía SendFunc, y se devuelve un TEE
//     (Adapter primario + LogSink de diagnóstico). El loop de conexión del Adapter corre en goroutine
//     ligada a ctx. ZERO-KNOWLEDGE: por el cable solo viaja contenido de negocio; nunca la DEK.
func buildSink(ctx context.Context, cfg config.Config, log sharedlogger.Logger, custody app.KeyCustody, database *sql.DB, gateway *waconn.ListenGateway) app.InboundSink {
	logSink := cloudlink.NewLogSink(log)
	if cfg.CloudLink.Endpoint == "" {
		log.Info("CloudLink deshabilitado (sin endpoint): usando LogSink puro para diagnóstico")
		return logSink
	}

	creds, err := clientCreds(cfg.CloudLink, log)
	if err != nil {
		log.Error("CloudLink: credenciales mTLS inválidas, cayendo a LogSink puro", "error", err)
		return logSink
	}

	cc, err := grpc.NewClient(cfg.CloudLink.Endpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Error("CloudLink: no se pudo crear el cliente gRPC, cayendo a LogSink puro", "error", err)
		return logSink
	}

	validator, err := loadValidator(cfg.CloudLink, log)
	if err != nil {
		log.Error("CloudLink: clave pública de lease inválida, cayendo a LogSink puro", "error", err)
		_ = cc.Close()
		return logSink
	}

	// SendFunc: conecta los comandos SendText de la nube al despachador del Edge. Prioriza el CLIENTE
	// VIVO de la escucha (una sola conexión por sesión): con la misma identidad multi-dispositivo, un
	// cliente efímero aparte reemplazaría la conexión y dejaría la escucha sorda. Si el gateway no
	// expone un emisor vivo (defensivo), cae al sender efímero (NewClient+Connect+Disconnect por envío).
	var sendFunc func(ctx context.Context, to, text string) error
	if liveSender, ok := any(gateway).(app.LiveSender); ok && gateway != nil {
		sendFunc = func(ctx context.Context, to, text string) error { return liveSender.SendViaLiveClient(ctx, to, text) }
		log.Info("CloudLink: el envío reutilizará el CLIENTE VIVO de la escucha (conexión única por sesión)")
	} else {
		sendUC := app.NewSend(custody, waconn.NewSender(database))
		sendFunc = func(ctx context.Context, to, text string) error { return sendUC.Run(ctx, to, text) }
	}

	adapter := cloudlink.NewAdapter(cc, cfg.CloudLink.SessionID, sendFunc, validator, custody.Exists, log)
	go func() {
		_ = adapter.Run(ctx)
		_ = cc.Close()
	}()

	log.Info("CloudLink habilitado: reenviando entrantes y atendiendo comandos cloud->edge",
		"endpoint", cfg.CloudLink.Endpoint, "session_id", cfg.CloudLink.SessionID,
		"lease_gate", validator != nil)
	return cloudlink.NewTeeSink(adapter, logSink)
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

// loadValidator construye el Validator del gate de lease si hay clave pública configurada. Acepta la
// clave en hex o como 32 bytes crudos. Devuelve nil (sin gate) si no hay ruta configurada.
func loadValidator(cl config.CloudLinkConfig, log sharedlogger.Logger) (*lease.Validator, error) {
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
	return lease.NewValidator(ed25519.PublicKey(pub)), nil
}
