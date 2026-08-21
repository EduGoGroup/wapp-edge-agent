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

	// ─── Plan 051 Ola 4 · lo que ya viaja en el heartbeat y en GET /v1/health, y aquí faltaba ───
	//
	// 🔴 EL BUNDLE ES EL CANAL DE CAMPO CUANDO EL EDGE ESTÁ DESCONECTADO, que es EXACTAMENTE la situación en
	// la que alguien mira estos números: un Edge que no entrega suele ser un Edge que no habla con la nube.
	// Que el heartbeat los llevara y el bundle no dejaba al soporte con la mitad del cuadro justo en el peor
	// momento. No dependen de ningún release de proto (aquí se serializa JSON propio, no `SessionHealth`),
	// así que no había nada que esperar para cablearlos.
	//
	// Los dos del PARTE son omitempty por la misma razón que en el plano de control: su ausencia ES
	// información («el parte del worker-cajero no está o está rancio»), y un `intent_p50_ms: 0` impreso se
	// leería como «inferencia instantánea», que es lo contrario de la verdad.
	WorkerTaskset string `json:"worker_taskset,omitempty"`
	IntentP50Ms   int64  `json:"intent_p50_ms,omitempty"`

	// 🔴 `intent_omitted_by_reason` NO lleva omitempty y sus OCHO claves se imprimen SIEMPRE, aunque estén
	// todas a 0: un motivo a 0 es un dato («por aquí no se está yendo nadie»); un hueco obliga al que lee a
	// adivinar si el motivo no existe, no se midió o no se dio. La lista se RECORRE, jamás se transcribe
	// (INV-051.3) — ver `desgloseCompleto`.
	IntentOmittedByReason map[string]int64 `json:"intent_omitted_by_reason"`
	StuckHeads            int64            `json:"stuck_heads"`
	StuckHeadPolls        int64            `json:"stuck_head_polls"`
	// Los dos sellos, SEPARADOS y sin ningún campo que los sume (T3.12): sólo `failed_seal_dispatch` implica
	// mensajes duplicados ya publicados en la nube. Sumarlos aquí desharía esa tarea en el único sitio donde
	// el soporte lee el dato sin nube.
	FailedSealDispatch int64 `json:"failed_seal_dispatch"`
	FailedSealBudget   int64 `json:"failed_seal_budget"`

	// ─── Plan 046 · Ola 2 · T2.3 · el contador del filtro de la sesión PASIVA ───
	//
	// Entra aquí por el MISMO argumento escrito doce líneas más arriba para los de la Ola 4: el bundle es el
	// canal de campo cuando el Edge no habla con la nube, y este contador no viaja en el heartbeat (INV-5 de
	// la ola prohíbe tocar el proto), así que sin esta línea el soporte solo podría verlo con `wapp-ctl`
	// contra el `GET /v1/health` del equipo del cliente.
	//
	// Sin omitempty y sin agregado: un 0 es un dato («esta sesión no descarta nada»). Y se lee junto al
	// número de descartes por VENTANA: el corte pasivo va antes del ADR-0037, así que le quita cuenta.
	DroppedPassive uint64 `json:"dropped_passive"`

	// FiltersVersion es la versión del MAPA de perfiles con la que el Edge filtró esos descartes (D-046.2).
	// Va aquí y no solo en `/v1/health` porque el bundle es lo que el soporte recibe cuando el equipo del
	// cliente no habla con la nube, y sin este número el `dropped_passive` de al lado no se puede contrastar
	// con lo que la consola cree haber empujado — que es la ÚNICA forma de ver un mapa retrasado.
	FiltersVersion int64 `json:"filters_version"`
}

// desgloseCompleto devuelve el desglose por motivo con las OCHO claves canónicas presentes, partiendo del
// cero de `health.DespachoStatsCero()` —que las construye RECORRIENDO `app.MotivosOmitido()`— y volcando
// encima lo que traiga el Report.
//
// 🔴 NI UNA SOLA LISTA ESCRITA A MANO (INV-051.3): esa lista se ha quedado corta dos veces. Y se copia clave
// a clave en vez de reusar el mapa del Report por el mismo motivo que en el colector: un motivo que el Report
// no traiga —un rollback a un binario con menos motivos— sigue apareciendo a 0 en vez de desaparecer de la
// serie, y un vacío/0 significa siempre «no lo sé / no ha pasado», nunca un hueco que interpretar.
func desgloseCompleto(m map[string]int64) map[string]int64 {
	out := health.DespachoStatsCero().OmitidosPorMotivo
	for motivo, n := range m {
		out[motivo] = n
	}
	return out
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
				// Plan 051 Ola 4: los siete de la ola, tal cual llegan del Report. Vacío/0 = «no lo sé», que
				// es la semántica del propio Report (parte ausente o rancio ⇒ los tres del parte a su cero).
				WorkerTaskset: r.WorkerTaskset,
				IntentP50Ms:   r.IntentP50Ms,
				// El desglose se NORMALIZA (no se copia el mapa a pelo): el Report de producción ya trae las
				// ocho claves, pero este Reporter es una interfaz y un implementador que devuelva nil dejaría
				// un `null` en el bundle — el hueco que INV-051.3 prohíbe.
				IntentOmittedByReason: desgloseCompleto(r.IntentOmittedByReason),
				StuckHeads:            r.StuckHeads,
				StuckHeadPolls:        r.StuckHeadPolls,
				FailedSealDispatch:    r.FailedSealDispatch,
				FailedSealBudget:      r.FailedSealBudget,
				// Plan 046 · T2.3, tal cual llega del Report (ver el campo).
				DroppedPassive: r.DroppedByPassiveProfile,
				// Plan 046 · Ola 2: la versión del mapa que produjo esos descartes (ver el campo).
				FiltersVersion: r.FiltersVersion,
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
