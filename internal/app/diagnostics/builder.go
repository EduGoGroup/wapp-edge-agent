// Package diagnostics arma el BUNDLE DE DIAGNÓSTICO BAJO DEMANDA del Edge (Plan 031 T8, ADR-0023 capa 3):
// la versión a distancia de lo que hoy exige `kill -QUIT`/`lsof` en la máquina del cliente. A petición del
// Cloud (frame DiagnosticsRequest) el Edge arma un Bundle con tres partes:
//
//   - LogTail: las últimas N líneas del ring buffer de logs en memoria (reusa el logsink que ya teed el
//     logger para GET /v1/logs; no toca disco).
//   - GoroutineDump: volcado de goroutines (runtime.Stack(all=true)) — el equivalente in-process del
//     kill -QUIT del runbook §1.
//   - SubsystemsJSON: snapshot de salud por sesión + daemon (estilo /v1/intent/status), en JSON.
//
// FRONTERA ZERO-KNOWLEDGE VERIFICABLE (ADR-0007): el bundle SOLO lleva metadatos operativos. JAMÁS la DEK,
// llaves, credenciales, tokens ni contenido de mensajes. La política "logs sin secretos" ya existía; aquí
// se vuelve PRUEBA: Scrub() redacta defensivamente cualquier tira larga hex/base64 que se colara en un log,
// y el gate del test (builder_test.go) escanea el bundle generado con material sensible sembrado y falla si
// aparece. Además el bundle se TRUNCA en origen para caber holgado bajo el límite de 4 MiB del transporte.
package diagnostics

import (
	"context"
	"encoding/json"
	"regexp"
	"runtime"
	"strings"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app/health"
)

// Bundle es el resultado NEUTRAL (sin proto) del diagnóstico: el adapter CloudLink lo mapea a
// DiagnosticsBundle. Los tres campos ya van saneados (Scrub) y truncados en origen.
type Bundle struct {
	// LogTail son las últimas líneas del ring buffer, saneadas y unidas por '\n'.
	LogTail string
	// GoroutineDump es el volcado de goroutines saneado y truncado.
	GoroutineDump string
	// SubsystemsJSON es el snapshot de subsistemas (salud + daemon) en JSON.
	SubsystemsJSON string
}

// LogTailer es el puerto MÍNIMO del ring buffer de logs: las últimas n líneas. Lo satisface
// *logsink.Sink (control plane). Puerto local para no importar el adapter desde la capa app.
type LogTailer interface {
	Tail(n int) []string
}

// Reporter arma la salud por sesión + daemon (lo satisface *health.Collector). Alimenta subsystems_json.
type Reporter interface {
	Reports(ctx context.Context) map[string]health.Report
	DaemonUptimeS() int64
	Version() string
}

// Límites de truncado en ORIGEN (Plan 031 T8). El transporte gRPC impone 4 MiB por frame; el bundle debe
// caber HOLGADO. Márgenes elegidos: log y dump ≤ 1 MiB cada uno, subsystems ≤ 256 KiB ⇒ techo ~2.25 MiB,
// casi la mitad del límite, con espacio para el resto del EdgeToCloud y el overhead proto. Truncar aquí (no
// en el transporte) mantiene el frame siempre por debajo del máximo sin depender del wire.
const (
	maxLogTailBytes    = 1 << 20 // 1 MiB
	maxGoroutineBytes  = 1 << 20 // 1 MiB
	maxSubsystemsBytes = 1 << 18 // 256 KiB
	// goroutineStackBuf es el tope del buffer de runtime.Stack: por encima de esto se trunca (una flota de
	// escritorio rara vez pasa de unos miles de goroutines; 1 MiB de stack basta para el diagnóstico).
	goroutineStackBuf = 1 << 20
)

// DefaultLogLines es el número de líneas de log por defecto en el bundle (configurable por
// WAPP_AGENT_DIAG_LOG_LINES). 500 da contexto reciente amplio sin acercarse al tope de tamaño.
const DefaultLogLines = 500

// secretPattern detecta tiras largas que HUELEN a material criptográfico y no deberían estar en un log:
// hex de ≥32 nibbles (≥16 bytes: llaves, hashes, IDs cripto) o base64/base64url de ≥40 chars (DEK sellada,
// tokens). Es un SCRUBBING DEFENSIVO: si un log filtró un secreto pese a la política, aquí se redacta antes
// de que salga del proceso. No pretende clasificar; ante la duda, redacta.
var secretPattern = regexp.MustCompile(`[0-9a-fA-F]{32,}|[A-Za-z0-9+/_-]{40,}={0,2}`)

// redacted es el marcador que sustituye a un match sospechoso.
const redacted = "[REDACTED]"

// Scrub redacta del texto cualquier tira que parezca material sensible (hex/base64 largo). Se aplica a
// log_tail y goroutine_dump antes de emitir el bundle. Idempotente.
func Scrub(s string) string {
	return secretPattern.ReplaceAllString(s, redacted)
}

// Builder arma bundles. logs es el ring buffer; reporter la salud; logLines cuántas líneas incluir.
type Builder struct {
	logs     LogTailer
	reporter Reporter
	logLines int
	// stack inyecta el volcado de goroutines (tests deterministas). Producción usa dumpGoroutines.
	stack func() string
}

// NewBuilder construye el Builder. logs puede ser nil (log_tail vacío); reporter nil (subsystems mínimo);
// logLines<=0 cae a DefaultLogLines.
func NewBuilder(logs LogTailer, reporter Reporter, logLines int) *Builder {
	if logLines <= 0 {
		logLines = DefaultLogLines
	}
	return &Builder{logs: logs, reporter: reporter, logLines: logLines, stack: dumpGoroutines}
}

// Build arma el bundle para el scope pedido (hoy el scope es informativo: siempre se arma el bundle
// completo; un scope no reconocido no falla, compat aditiva del ADR-0023). Sanea y trunca en origen.
func (b *Builder) Build(ctx context.Context, scope string) Bundle {
	return Bundle{
		LogTail:        truncateTail(Scrub(b.logTail()), maxLogTailBytes),
		GoroutineDump:  truncateTail(Scrub(b.stack()), maxGoroutineBytes),
		SubsystemsJSON: truncateTail(b.subsystemsJSON(ctx), maxSubsystemsBytes),
	}
}

// logTail une las últimas líneas del ring buffer con '\n'. Vacío sin ring buffer.
func (b *Builder) logTail() string {
	if b.logs == nil {
		return ""
	}
	return strings.Join(b.logs.Tail(b.logLines), "\n")
}

// subsystemJSON es la proyección JSON del snapshot de subsistemas (metadatos operativos, snake_case). Es la
// versión estructurada de lo que /v1/intent/status y GET /v1/health exponen, empaquetada para el bundle.
type subsystemsDoc struct {
	Daemon   daemonDoc                   `json:"daemon"`
	Sessions map[string]sessionHealthDoc `json:"sessions"`
}

type daemonDoc struct {
	Version  string `json:"version"`
	UptimeS  int64  `json:"uptime_s"`
	Sessions int    `json:"sessions"`
}

type sessionHealthDoc struct {
	SocketState       string `json:"socket_state"`
	DegradedReason    string `json:"degraded_reason,omitempty"`
	LastInboundAgeS   int64  `json:"last_inbound_age_s"`
	DEKLoadDurationMs int64  `json:"dek_load_duration_ms"`
	IntentCircuit     string `json:"intent_circuit,omitempty"`
	OutboxDepth       int64  `json:"outbox_depth"`
	BinaryVersion     string `json:"binary_version"`
}

// subsystemsJSON serializa la salud de todas las sesiones + el daemon a JSON. Sin reporter devuelve un doc
// mínimo bien tipado. Nunca incluye material sensible (los Reports son metadatos derivados).
func (b *Builder) subsystemsJSON(ctx context.Context) string {
	doc := subsystemsDoc{Sessions: map[string]sessionHealthDoc{}}
	if b.reporter != nil {
		reports := b.reporter.Reports(ctx)
		doc.Daemon = daemonDoc{Version: b.reporter.Version(), UptimeS: b.reporter.DaemonUptimeS(), Sessions: len(reports)}
		for id, r := range reports {
			doc.Sessions[id] = sessionHealthDoc{
				SocketState:       r.SocketState,
				DegradedReason:    r.DegradedReason,
				LastInboundAgeS:   r.LastInboundAgeS,
				DEKLoadDurationMs: r.DEKLoadDurationMs,
				IntentCircuit:     r.IntentCircuit,
				OutboxDepth:       r.OutboxDepth,
				BinaryVersion:     r.BinaryVersion,
			}
		}
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return `{"error":"no se pudo serializar el snapshot de subsistemas"}`
	}
	return string(raw)
}

// dumpGoroutines vuelca el stack de TODAS las goroutines (runtime.Stack(all=true)), truncado al buffer.
func dumpGoroutines() string {
	buf := make([]byte, goroutineStackBuf)
	n := runtime.Stack(buf, true)
	return string(buf[:n])
}

// truncateTail recorta s a max bytes conservando el FINAL (lo más reciente/relevante en logs y dumps),
// anteponiendo una marca de truncado. Respeta fronteras UTF-8 de forma conservadora (recorta por bytes; la
// marca ASCII garantiza validez del prefijo). No-op si ya cabe.
func truncateTail(s string, max int) string {
	if len(s) <= max {
		return s
	}
	const marker = "…[truncado en origen]\n"
	keep := max - len(marker)
	if keep < 0 {
		keep = 0
	}
	return marker + s[len(s)-keep:]
}
