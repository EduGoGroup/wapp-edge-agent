package server

// readiness.go — POST /v1/inference/readiness: la SEÑAL con la que el worker-cajero le dice al núcleo
// que su socket de inferencia ya sirve («listo») o que va a dejar de servir («caído») (Plan 044 ·
// Ola 1.8 · T1.8-5, D-044.43).
//
// ─────────────────────────────────────────────────────────────────────────────
// POR QUÉ UNA SEÑAL Y NO UNA SONDA
// ─────────────────────────────────────────────────────────────────────────────
// El núcleo NO comprueba si el socket del cajero existe, y eso es una decisión escrita y razonada
// (internal/infra/wiring/inferencia.go): «un chequeo fotografiaría el arranque y lo congelaría». Esta
// ruta NO la toca. Lo que añade es el camino contrario: en vez de que el núcleo pregunte, el cajero
// AVISA —una vez por transición, cuando ya es un hecho—. El único que sabe con certeza que el socket
// acepta es el proceso que acaba de abrirlo.
//
// La muerte por SIGKILL no manda aviso, y no hace falta: el núcleo la aprende por el camino que ya
// tenía —la siguiente inferencia falla con app.ErrInferenciaOllamaCaido— y pasa a DOWN por su cuenta.
// La señal ACELERA lo que el sistema ya sabía descubrir; no lo sustituye.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 POR QUÉ ESTA RUTA VA EXENTA DE AUTENTICACIÓN DE OPERADOR
// ─────────────────────────────────────────────────────────────────────────────
// Se registra con Handle y NO con HandleAuthorized, igual que GET /v1/health. Tres razones, y las tres
// tienen que seguir siendo ciertas para que la exención se sostenga:
//
//  1. EL TRANSPORTE YA ES LA CREDENCIAL. El contrato /v1 vive en un Unix domain socket 0600 co-ubicado,
//     sin puerto de red (ADR-0015): quien puede abrirlo es un proceso del MISMO usuario de la misma
//     máquina. No hay una superficie remota que autenticar.
//  2. EL EMISOR NO ES UN OPERADOR, ES UN HERMANO. Quien llama es `agent cajero`, otro hijo del mismo
//     supervisor, que no tiene —ni debe tener— un Bearer de operador: pedírselo obligaría a inventarle
//     una identidad y a custodiarle un token para decir algo que no es una orden.
//  3. ES UNA SEÑAL, NO UNA ACCIÓN. Misma naturaleza que la sonda de liveness ya exenta: informa de un
//     hecho de esta máquina. El heartbeat está exento por ADR-0025 exactamente por lo mismo.
//
// 🔴 Y POR ESO IMPORTA DECIR QUÉ **NO** SE ACEPTA POR ESTA PUERTA, que es lo que mantiene la exención
// pequeña: NADA que mute estado de negocio. No empareja, no desvincula, no envía, no toca sesiones, no
// toca config, no lee ni escribe la BD. Lo único que mueve es un enum en memoria —la readiness de
// inferencia de ESTA instalación— que el núcleo retransmite en el Heartbeat. Si algún día alguien
// quiere colgar aquí otra cosa, la pregunta no es «¿cabe?» sino «¿sigue siendo una señal?»; si no lo
// es, va por HandleAuthorized.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 EL AVISO ES POR INSTALACIÓN, NUNCA GLOBAL
// ─────────────────────────────────────────────────────────────────────────────
// El cajero atiende N instalaciones (`WAPP_WORKER_DATA_DIRS`), una por `data_dir`, con UN socket de
// inferencia por cada una; y hay un daemon —y por tanto un plano de control— por instalación. Así que
// «el cajero está listo» no es una frase completa: hay que decir DE QUÉ INSTALACIÓN. Por eso `data_dir`
// es OBLIGATORIO en el cuerpo y el núcleo lo compara con el suyo antes de aplicar nada: un aviso que
// habla de la instalación del vecino se acepta con 200 y `applied:false`, no mueve esta readiness y
// deja constancia en el log. Sin esa comparación, dos instalaciones en la misma máquina se
// contaminarían la señal la una a la otra sin un solo error.

import (
	"encoding/json"
	"net/http"
	"strings"
)

// RutaReadinessInferencia es el path de la señal. Exportada porque el CLIENTE
// (internal/adapters/nucleoaviso) la usa para construir su URL: es parte del contrato, no un detalle
// de este fichero. Mismo criterio que cajerosock.Ruta — el dueño del contrato es el servidor.
const RutaReadinessInferencia = "/v1/inference/readiness"

// Los dos únicos valores del campo `readiness` del cuerpo.
//
// SON STRINGS Y NO UN BOOL, por el mismo motivo por el que el contrato de CloudLink usa un enum de tres
// valores y no un `bool` (ver InferenceReadiness en cloudlink.proto): un JSON al que se le olvida el
// campo decodifica un bool a `false`, que aquí significaría «el cajero afirma que NO puede servir» — un
// veredicto que nadie emitió. Con strings, la ausencia es la cadena vacía y se rechaza con 400.
const (
	// ReadinessListo: el socket de inferencia de esa instalación ya acepta y sirve.
	ReadinessListo = "ready"
	// ReadinessCaido: ese socket deja de servir (apagado ordenado del cajero).
	ReadinessCaido = "down"
	// ReadinessPrefijoFrio: el cajero SIGUE SIRVIENDO, pero su caché de prefijo se perdió y la próxima
	// inferencia de un cliente pagaría el prefill entero (DEUDA-044.10, Plan 044). NO es un tercer estado
	// de readiness: es un HECHO que pide una acción —que el Cloud vuelva a calentar—, y el daemon lo
	// traduce a la única transición que el contrato de CloudLink sabe expresar (ver
	// InferenceReadinessSink.ReponerCalentamientoInferencia).
	//
	// 🔴 POR QUÉ TIENE VALOR PROPIO Y NO SE MANDAN DOS AVISOS («down» y luego «ready»), que produciría la
	// misma transición con cero código nuevo: porque el log mentiría. Un «down» seguido de un «ready»
	// deja escrito que el cajero se cayó y volvió, y **el cajero no se cayó** — lo que se enfrió fue
	// Ollama por debajo. Esta casa ya tiene la regla escrita (*un mensaje automático debe filtrar por el
	// estado que AFIRMA*), y un par de líneas falsas en el log del arranque cuestan más que este enum.
	ReadinessPrefijoFrio = "prefix_cold"
)

// ReadinessRequest es el cuerpo de POST /v1/inference/readiness. Lo escribe el cajero
// (internal/adapters/nucleoaviso), que IMPORTA este tipo en vez de declarar el suyo: dos declaraciones
// paralelas de la misma forma son el par que diverge en silencio.
//
// Frontera zero-knowledge (ADR-0007): SOLO un enum y una ruta de directorio. Aquí no viaja —ni puede
// viajar— la DEK, ni credenciales, ni números, ni contenido de negocio.
type ReadinessRequest struct {
	// Readiness es "ready" o "down". Cualquier otra cosa (incluida la ausencia) ⇒ 400.
	Readiness string `json:"readiness"`
	// DataDir identifica DE QUÉ INSTALACIÓN habla el aviso. Obligatorio: ver el bloque de arriba. Llega
	// ya absolutizado (config.Load absolutiza tanto cfg.DataDir como cada entrada de Worker.DataDirs), de
	// modo que la comparación es una igualdad de cadenas y no una resolución de rutas.
	DataDir string `json:"data_dir"`
}

// ReadinessResponse es el cuerpo de la respuesta. `applied` es el dato que le importa a quien depura en
// campo: distingue «me llegó tu aviso y era para mí» de «me llegó y hablaba de otra instalación».
// `changed` distingue además la TRANSICIÓN de la repetición idempotente — que es justo lo que dispara
// (o no) el heartbeat fuera de cadencia.
type ReadinessResponse struct {
	Readiness string `json:"readiness"`
	Applied   bool   `json:"applied"`
	Changed   bool   `json:"changed"`
	// Reason solo viaja cuando `applied` es false, y dice por qué. Es diagnóstico, no un código estable.
	Reason string `json:"reason,omitempty"`
}

// InferenceReadinessSink es el puerto ESTRECHO por el que la señal entra al núcleo. Lo implementa el
// multiplexor de CloudLink (*cloudlink.Adapter), que es quien retransmite la readiness en el Heartbeat.
// Se inyecta desde el wiring del daemon, mismo patrón que RegisterPairing/RegisterEnroll: el servidor
// de control no conoce ni CloudLink ni el proto.
type InferenceReadinessSink interface {
	// MarcarInferenciaReadiness fija la readiness afirmada por el cajero y devuelve true si eso fue una
	// TRANSICIÓN (el estado anterior era distinto). Debe ser idempotente: repetir el mismo valor no es un
	// error, simplemente no transiciona.
	MarcarInferenciaReadiness(listo bool) bool

	// ReponerCalentamientoInferencia le pide al núcleo que consiga que el Cloud vuelva a calentar el
	// prefijo de este Edge, y devuelve true si se emitió algo. La llama el cajero cuando descubre —por el
	// `regimen` de una inferencia REAL, no por una sonda— que su caché de prefijo se perdió
	// (DEUDA-044.10).
	//
	// 🔴 POR QUÉ ES UN MÉTODO APARTE Y NO `MarcarInferenciaReadiness(true)`: ese es IDEMPOTENTE, y ahí está
	// el problema. Tras un reinicio de Ollama el Edge sigue marcado READY, así que repetir READY no
	// transiciona, y el Cloud calienta **sólo en la transición a READY**
	// (gateway/grpc/readiness.go). El estado correcto y la acción necesaria se contradicen: hay que
	// PROVOCAR la transición sin que nadie escriba en el log que el cajero se cayó.
	ReponerCalentamientoInferencia() bool
}

// readinessHandler cuelga la señal sobre el puerto, atada al data_dir de ESTE daemon.
type readinessHandler struct {
	// dataDir es la instalación que sirve este núcleo (cfg.DataDir, ya absolutizado). Un aviso que nombre
	// otra se acepta y no se aplica.
	dataDir string
	// sink puede ser nil: ocurre cuando el Edge corre SIN stream a la nube (LogMux, sin endpoint de
	// CloudLink). Ver handle para por qué eso responde 200 y no un error.
	sink InferenceReadinessSink
	log  logger
}

// RegisterInferenceReadiness cuelga POST /v1/inference/readiness (Plan 044 · Ola 1.8 · T1.8-5). Se llama
// ANTES de Serve, igual que el resto de Register*.
//
// 🔴 CON Handle Y NO CON HandleAuthorized: la exención de auth está argumentada en el encabezado de este
// fichero, y su precedente literal es el registro de GET /v1/health en server.New. No la cambies sin
// leer las tres condiciones que la sostienen.
func (s *Server) RegisterInferenceReadiness(dataDir string, sink InferenceReadinessSink) {
	h := &readinessHandler{dataDir: dataDir, sink: sink}
	if s.log != nil {
		h.log = s.log
	}
	s.Handle(http.MethodPost, RutaReadinessInferencia, h.handle)
}

// handle aplica el aviso del cajero. Es IDEMPOTENTE por construcción: el cuerpo lleva el ESTADO al que
// se pasa, no un incremento, así que repetirlo no acumula nada — que es lo que permite que el cajero lo
// reintente sin llevar cuenta de si el anterior llegó.
func (h *readinessHandler) handle(w http.ResponseWriter, r *http.Request) {
	var req ReadinessRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCuerpoReadiness)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "cuerpo JSON inválido")
		return
	}

	// El valor se normaliza (trim + minúsculas) porque lo escribe otro proceso y un espacio de más no es
	// un error de nadie; lo que NO se hace es adivinar: un valor que no sea uno de los dos se rechaza.
	estado := strings.ToLower(strings.TrimSpace(req.Readiness))
	var listo, prefijoFrio bool
	switch estado {
	case ReadinessListo:
		listo = true
	case ReadinessCaido:
		listo = false
	case ReadinessPrefijoFrio:
		// Ni listo ni caído: el cajero sirve, y lo que afirma es que su prefijo se enfrió. Se resuelve
		// abajo, contra el sink, por un camino distinto — ver el bloque final de este handler.
		prefijoFrio = true
	default:
		writeError(w, http.StatusBadRequest, codeInvalidRequest,
			`readiness debe ser "`+ReadinessListo+`", "`+ReadinessCaido+`" o "`+ReadinessPrefijoFrio+`"`)
		return
	}

	// 🔴 `data_dir` VACÍO SE RECHAZA, no se interpreta como «todas». Un aviso global sería precisamente el
	// error que la corrección de arriba existe para impedir: el cajero atiende N instalaciones y la
	// readiness de una no dice nada de las otras.
	dataDir := strings.TrimSpace(req.DataDir)
	if dataDir == "" {
		writeError(w, http.StatusBadRequest, codeInvalidRequest,
			"data_dir requerido: el aviso de readiness es POR INSTALACIÓN, no global")
		return
	}
	if dataDir != h.dataDir {
		// NO ES UN ERROR DEL EMISOR y por eso no es un 4xx: el cajero está diciendo la verdad sobre una
		// instalación que este daemon no sirve. Se acepta, no se aplica y se deja dicho en el log, que es
		// donde alguien con dos instalaciones en la misma máquina lo va a buscar.
		if h.log != nil {
			h.log.Info("plano de control: aviso de readiness de inferencia de OTRA instalación; no se aplica",
				"data_dir_aviso", dataDir, "data_dir_propio", h.dataDir, "readiness", estado)
		}
		writeJSON(w, http.StatusOK, ReadinessResponse{
			Readiness: estado, Applied: false,
			Reason: "el aviso habla de otra instalación (data_dir distinto del de este daemon)",
		})
		return
	}

	if h.sink == nil {
		// SIN STREAM A LA NUBE NO HAY A QUIÉN CONTÁRSELO, y eso no es un fallo del cajero. Se responde 200
		// para que su aviso no se convierta en un error recurrente en el log de un Edge que corre en
		// diagnóstico (sin endpoint de CloudLink); `applied:false` + `reason` dicen la verdad de por qué.
		if h.log != nil {
			h.log.Info("plano de control: aviso de readiness de inferencia recibido, pero este daemon no tiene "+
				"stream a la nube (LogMux); no hay heartbeat en el que retransmitirlo", "readiness", estado)
		}
		writeJSON(w, http.StatusOK, ReadinessResponse{
			Readiness: estado, Applied: false,
			Reason: "este daemon no tiene stream a la nube (CloudLink deshabilitado)",
		})
		return
	}

	if prefijoFrio {
		// EL CAJERO NO AFIRMA UN ESTADO AQUÍ: afirma que perdió su caché de prefijo y pide la única acción
		// que lo repone. Por eso el log dice eso y no «readiness CAMBIA»: el cajero no se cayó.
		cambio := h.sink.ReponerCalentamientoInferencia()
		if h.log != nil {
			h.log.Info("plano de control: el worker-cajero avisa de que su PREFIJO se enfrió (sigue sirviendo); "+
				"se provoca la transición de readiness para que el Cloud vuelva a calentar (DEUDA-044.10)",
				"data_dir", dataDir, "emitido", cambio)
		}
		writeJSON(w, http.StatusOK, ReadinessResponse{Readiness: estado, Applied: true, Changed: cambio})
		return
	}

	cambio := h.sink.MarcarInferenciaReadiness(listo)
	if h.log != nil {
		// Info y no Debug: es UNA línea por transición del cajero (arranque y apagado), no tráfico. Es
		// también la única huella local de por qué el Cloud empezó o dejó de calentar este Edge.
		h.log.Info("plano de control: el worker-cajero AFIRMA su readiness de inferencia",
			"readiness", estado, "data_dir", dataDir, "transicion", cambio)
	}
	writeJSON(w, http.StatusOK, ReadinessResponse{Readiness: estado, Applied: true, Changed: cambio})
}

// maxCuerpoReadiness es el techo del cuerpo: dos campos, uno de ellos una ruta. 8 KiB es tres órdenes de
// magnitud por encima de lo que cabe legítimamente, así que no recorta nada real; lo que corta es un
// cuerpo corrupto que si no se leería entero en memoria.
const maxCuerpoReadiness = 8 << 10
