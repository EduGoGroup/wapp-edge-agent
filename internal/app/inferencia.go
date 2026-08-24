package app

// inferencia.go — EL PUERTO DE SERVIR INFERENCIA (Plan 044 · Ola 1.6 · T1.6-2, ADR-0045 §2, REQ-34).
//
// QUÉ RESUELVE. El Cloud manda `CloudToEdge.inference_request` por CloudLink; ese frame llega al proceso
// `agent serve`, pero quien puede hablar con Ollama es el proceso `agent cajero` (REQ-051.10: «ningún
// otro proceso que el worker habla con Ollama»). Este puerto es la frontera entre los dos: el daemon
// PIDE una inferencia y no sabe —ni debe saber— que del otro lado hay un socket unix, un cajero y un
// Ollama. El adaptador que lo implementa es internal/adapters/inferenciacliente.
//
// 🔴 EL EDGE NO INTERPRETA NADA (ADR-0045 §1): «prompt entra → JSON sale». Aquí no hay parseo del
// prompt, ni del `format`, ni de la salida. El troceado, los prompts, el orden de las llamadas y la
// validación viven ENTEROS en el Cloud. Por eso este puerto no tiene ni un solo tipo de dominio: los
// campos son los del frame, tal cual.
//
// 🔴 INV-051.1: NI EL PROMPT NI LA SALIDA SALEN POR NINGÚN LOG, tampoco en debug. Son contenido de
// negocio (el prompt lleva dentro el texto que el cliente escribió por WhatsApp). Lo que se loguea en
// todo este camino es `command_id`, TAMAÑOS y DESENLACE.

import (
	"context"
	"errors"
	"time"
)

// PeticionInferencia es lo que el Cloud pide, ya despojado del transporte. Los nombres siguen al frame
// `InferenceRequest` porque el Edge no traduce vocabulario: si el contrato cambia, el sitio donde se
// nota tiene que ser uno solo.
type PeticionInferencia struct {
	// CommandID correlaciona la respuesta. NO es un identificador de negocio y sí se loguea: es el único
	// hilo con el que un operador puede seguir una inferencia por los dos procesos.
	CommandID string
	// SessionID es la sesión que originó la pregunta cuando el Cloud lo sabe. NORMALMENTE VIENE VACÍA
	// (contrato: el servicio de inferencia es del EDGE, no de una sesión) y vacío NO es un error. Viaja
	// hasta aquí solo para la traza; NADIE lo usa para decidir nada — en particular, el gate de lease NO
	// se hace contra esta sesión (ver el carril, internal/adapters/cloudlink/inferencia.go).
	SessionID string
	// Prompt es el prompt YA CONSTRUIDO por el Cloud. Se entrega al modelo VERBATIM: no se recorta, no se
	// completa y no se le añade contexto propio. 🔴 Contenido de negocio: no se loguea.
	Prompt string
	// Format es el formato esperado de la salida: "json" a secas, o un JSON Schema serializado. Viaja
	// como string OPACO. El Edge lo reenvía al proveedor sin parsearlo — pero SÍ lo NORMALIZA para que
	// sea un valor JSON válido (ver NormalizarFormato): eso es serialización, no interpretación.
	Format string
	// Temperature es la temperatura de muestreo. nil = «el Cloud no dijo nada» ⇒ el Edge aplica su
	// default. Es un PUNTERO y tiene que serlo: 0.0 es a la vez el valor que más se va a pedir
	// (determinismo para clasificar) y el cero del tipo, así que sin presencia explícita «quiero 0» y
	// «no dije nada» serían el mismo byte.
	Temperature *float32
	// Timeout es el presupuesto de ESTA inferencia. 0 = «el Cloud no lo fijó» ⇒ default del Edge. El
	// Edge además lo ACOTA por su techo (WAPP_AGENT_INFERENCE_MAX_TIMEOUT_MS): una petición que pida
	// diez minutos ocuparía la única plaza de Ollama diez minutos.
	Timeout time.Duration
	// Calentamiento marca esta petición como TRÁFICO SINTÉTICO: no viene de nadie esperando una respuesta,
	// su único fin es dejar caliente lo que se enfría solo (hoy, la caché de prefijo de Ollama). El Edge la
	// sirve exactamente igual —mismo modelo, mismo aforo, mismo plazo— y sólo la trata distinto en UN
	// sitio: NO LE ENSEÑA NADA AL CIRCUIT BREAKER (ver cajero.servidorInferencia.Inferir).
	//
	// 🔴 POR QUÉ SE EXCLUYE, que no es una excepción de conveniencia. El breaker existe para responder «¿le
	// pido al proveedor la SIGUIENTE petición de un cliente?», y su población tiene que ser la de las
	// peticiones de clientes. Un calentamiento es justo lo contrario: se emite CUANDO NO HAY TRÁFICO, y con
	// el modelo descargado es EL MÁS LENTO de todos —es su razón de ser—. Contándolo, una máquina ociosa se
	// abriría el circuito sola a base de calentamientos lentos y rechazaría con BREAKER_OPEN la primera
	// petición real que llegase: el remedio provocando la enfermedad. Y al revés, un calentamiento rápido
	// borraría la racha de fallos de un Ollama que sigue caído.
	//
	// ⏳ HOY NADIE LO PONE A `true`, Y ES DELIBERADO: esto es la COSTURA que T1.7-2 dejó abierta para
	// T1.7-4, que es quien enseñará al Cloud a emitir calentamientos y quien decidirá cómo se marcan EN EL
	// FRAME (`inference_request`). Hasta entonces el campo sólo lo pone el test que fija la conducta. El
	// mecanismo de marcado NO está inventado aquí a propósito: lo único que esta tarea fija es DÓNDE se
	// consulta la marca —antes de que el breaker evalúe nada— y QUÉ significa.
	Calentamiento bool
}

// RespuestaInferencia es lo que el modelo devolvió. Un solo campo, y es a propósito: el contrato
// (InferenceOutput.raw_json) dice que sube el JSON CRUDO tal cual, sin validar, sin reformatear y sin
// truncar. Si el modelo devolvió algo que no es JSON, eso exactamente es lo que debe llegar arriba — un
// Edge que «arregle» la forma haría invisible el fallo del prompt, que es lo único que se puede
// corregir.
//
// 🔴 Contenido de negocio: no se loguea (INV-051.1). Lo que se loguea es su TAMAÑO.
type RespuestaInferencia struct {
	RawJSON string
}

// ServidorInferencia es el puerto: quien sepa hablar con el proveedor LOCAL de LLM lo implementa.
//
// UN SOLO MÉTODO, y sin `Salud()` ni `Listo()` al lado: la única forma honesta de saber si el proveedor
// está es pedirle algo, y las respuestas a esa pregunta ya son dos de los cinco errores de abajo
// (ErrInferenciaOllamaCaido / ErrInferenciaBreakerAbierto). Un sondeo aparte sería un tercer estado que
// puede contradecir al primero.
type ServidorInferencia interface {
	// Inferir sirve UNA inferencia. Devuelve la salida cruda, o UNO de los cinco errores canónicos de
	// abajo (envuelto o no: los llamantes comparan con errors.Is).
	//
	// NUNCA SE CUELGA: si el plazo vence, devuelve ErrInferenciaTimeout o ErrInferenciaSinCapacidad
	// según la fase en que venciera (ver la nota de la frontera en ErrInferenciaSinCapacidad).
	Inferir(ctx context.Context, p PeticionInferencia) (RespuestaInferencia, error)
}

// ─────────────────────────────────────────────────────────────────────────────
// LOS CINCO ERRORES — LISTA CANÓNICA
// ─────────────────────────────────────────────────────────────────────────────
//
// Son EXACTAMENTE los cinco valores no-cero de `InferenceError` del contrato (ADR-0045 §2), y están
// aquí —en `app`, no en el adaptador de transporte— por dos razones que se refuerzan:
//
//   - El puerto tiene que poder decir POR QUÉ falló sin importar proto. Si el vocabulario viviera en
//     internal/adapters/cloudlink, el cliente del socket tendría que importar el adaptador del cable
//     para nombrar un error, y la dirección hexagonal se invertiría.
//   - 🔴 INV-051.3 EXIGE CONTARLOS POR SEPARADO, no agregarlos en uno. Un vocabulario cerrado y con un
//     solo dueño es lo que hace que «contar por separado» sea comprobable: `ErroresInferencia` es la
//     lista, y el switch que traduce a proto se puede auditar contra ella (lo hace un test).
//
// SON PUNTEROS A UN TIPO PROPIO Y NO `errors.New` PELADOS porque el error tiene que sobrevivir a un
// viaje por HTTP+JSON: el servidor escribe `Codigo()` en el cuerpo y el cliente lo reconstruye con
// ErrorInferenciaDe. Con `errors.New` haría falta una segunda tabla código→error mantenida a mano, que
// es exactamente la clase de par que diverge (ver el hallazgo «dos caminos que divergen en un dato»).
type ErrorInferencia struct {
	codigo string
	// razon es el texto del error. Describe la CLASE de fallo, nunca el contenido que lo provocó.
	razon string
}

// Error implementa error. El texto es estable (lo leen los logs, no las decisiones).
func (e *ErrorInferencia) Error() string { return e.razon }

// Codigo es la etiqueta estable del error: lo que viaja por el socket y lo que el switch de traducción
// a proto usa como llave. Es un identificador cerrado, nunca contenido de negocio.
func (e *ErrorInferencia) Codigo() string { return e.codigo }

// Los cinco. El orden es el del enum del contrato (1..5) para que la comparación con el .proto sea de
// un vistazo.
var (
	// ErrInferenciaOllamaCaido: el proveedor local no responde (proceso caído, puerto cerrado, modelo
	// ausente). También lo devuelve el CLIENTE del socket cuando el socket del cajero no acepta la
	// conexión: desde el daemon, el cajero ES el proveedor local, y un cajero que no está es
	// indistinguible —y equivalente en consecuencias— de un Ollama que no está.
	ErrInferenciaOllamaCaido = &ErrorInferencia{
		codigo: "ollama_down",
		razon:  "inferencia: el proveedor local de LLM no responde",
	}
	// ErrInferenciaBreakerAbierto: el breaker del Edge está abierto (ADR-0042). Se rechaza SIN intentar
	// y de forma INMEDIATA — es la propiedad que el ADR-0045 exige y la razón de que el canal
	// daemon→cajero sea un socket y no un sondeo sobre una tabla.
	ErrInferenciaBreakerAbierto = &ErrorInferencia{
		codigo: "breaker_open",
		razon:  "inferencia: el circuito del proveedor local está abierto",
	}
	// ErrInferenciaTimeout: se agotó el plazo CON EL PROVEEDOR TRABAJANDO. Ver la frontera en
	// ErrInferenciaSinCapacidad: los dos son «se acabó el tiempo» y significan cosas opuestas.
	ErrInferenciaTimeout = &ErrorInferencia{
		codigo: "timeout",
		razon:  "inferencia: se agotó el plazo con el proveedor local trabajando",
	}
	// ErrInferenciaLeaseInvalido: sin lease vigente (ADR-0007). Servir inferencia es OPERAR, y operar
	// exige lease como cualquier otra orden. Lo produce el DAEMON (que es quien tiene los Validator),
	// nunca el cajero.
	ErrInferenciaLeaseInvalido = &ErrorInferencia{
		codigo: "lease_invalid",
		razon:  "inferencia: lease no vigente",
	}
	// ErrInferenciaSinCapacidad: la máquina del cliente está saturada y la petición se rechazó sin
	// llegar a llamar al modelo.
	//
	// 🔴 LA FRONTERA CON ErrInferenciaTimeout ES EL PUNTO MÁS FÁCIL DE ROMPER DE TODO ESTE CAMINO, y
	// romperla no da un error, da un DIAGNÓSTICO INVERTIDO. Las dos condiciones se observan igual desde
	// dentro (un ctx que vence), pero significan lo contrario:
	//
	//   - El plazo vence ESPERANDO PLAZA (nunca se llamó al modelo)  ⇒ SIN_CAPACIDAD. El equipo va corto:
	//     hay más peticiones que las que la máquina puede atender a la vez.
	//   - El plazo vence CON EL MODELO TRABAJANDO                     ⇒ TIMEOUT. El modelo tarda más de
	//     lo que el Cloud está dispuesto a esperar.
	//
	// Un `select` que no distinga las dos fases devolverá TIMEOUT siempre y mandará al dueño del equipo a
	// mirar su red en vez de su hardware. De ahí que el aforo tenga dos puertas (cajero.Aforo) en vez de
	// un solo `ctx`.
	ErrInferenciaSinCapacidad = &ErrorInferencia{
		codigo: "edge_sin_capacidad",
		razon:  "inferencia: el Edge no tuvo plaza libre dentro del plazo",
	}
)

// ErroresInferencia es LA LISTA CANÓNICA, en el orden del enum del contrato. Existe para que «los cinco»
// sea algo que se puede recorrer y no una frase de un comentario: la usan el cliente del socket para
// reconstruir el error desde su código y los tests para exigir que la traducción a proto los cubra todos
// (INV-051.3).
//
// AÑADIR UN ERROR NUEVO ES UN CAMBIO DE CONTRATO: va al final de esta lista, al final del enum del
// .proto y al switch de traducción del carril. Si falta cualquiera de los tres, el test estructural lo
// caza.
var ErroresInferencia = []*ErrorInferencia{
	ErrInferenciaOllamaCaido,
	ErrInferenciaBreakerAbierto,
	ErrInferenciaTimeout,
	ErrInferenciaLeaseInvalido,
	ErrInferenciaSinCapacidad,
}

// ErrorInferenciaDe resuelve un código a su error canónico. Devuelve false para un código desconocido —
// y el llamante DEBE tratar ese false como un fallo, nunca elegir un error «parecido»: un código que no
// se reconoce significa que los dos extremos del socket están desalineados (binarios de distinta
// versión), y taparlo con un OLLAMA_DOWN convertiría un problema de despliegue en un diagnóstico falso
// sobre la máquina del cliente.
func ErrorInferenciaDe(codigo string) (*ErrorInferencia, bool) {
	for _, e := range ErroresInferencia {
		if e.codigo == codigo {
			return e, true
		}
	}
	return nil, false
}

// EsErrorInferencia extrae el error canónico de una cadena de errores envuelta, si lo hay. Es el
// `errors.As` de este vocabulario, con el nombre en el idioma del repo.
func EsErrorInferencia(err error) (*ErrorInferencia, bool) {
	var e *ErrorInferencia
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// NormalizarFormato convierte el `format` del contrato en un VALOR JSON válido, listo para ir tal cual
// dentro del cuerpo de la petición al proveedor.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 POR QUÉ EXISTE: EL VERBATIM PRODUCE JSON INVÁLIDO EN EL CASO MÁS COMÚN
// ─────────────────────────────────────────────────────────────────────────────
// El campo `format` de la API del proveedor es un VALOR JSON crudo (ollama.ChatRequest.Format es un
// json.RawMessage y se serializa sin comillas ni escapes). El contrato dice que el Cloud puede mandar
// «"json" a secas, o un JSON Schema serializado», y esas dos formas NO son la misma cosa en el cable:
//
//   - Un schema empieza por '{' y ya es un valor JSON: va verbatim.
//   - La palabra `json` NO es un valor JSON. Copiada tal cual produce `"format":json` en el cuerpo —
//     sintaxis inválida—, el proveedor responde 400 y ese 400 se traduciría a OLLAMA_DOWN, o sea que
//     CULPARÍAMOS A LA MÁQUINA DEL CLIENTE de un error de serialización nuestro. El dueño del equipo
//     miraría un Ollama perfectamente sano.
//
// ESTO NO VIOLA «EL EDGE NO PARSEA» (ADR-0045 §1). No se lee el contenido, no se valida el schema y no
// se cambia lo que el Cloud pidió: se le pone la envoltura que el formato de transporte exige, igual que
// se hace con cualquier string que va a un campo JSON. Es serialización, no interpretación.
//
// Vacío ⇒ vacío (sin restricción de formato; el campo se omite aguas abajo).
func NormalizarFormato(format string) string {
	if format == "" {
		return ""
	}
	// Un valor que ya empieza por '{' es un objeto JSON (el caso del schema): va verbatim, sin mirar
	// dentro. No se comprueba que sea JSON *válido* a propósito — validarlo sería interpretarlo, y un
	// schema roto es un error del Cloud que tiene que llegar arriba como lo que es (un 400 del
	// proveedor), no convertirse aquí en otra cosa.
	if format[0] == '{' {
		return format
	}
	// Cualquier otra cosa es un escalar que hay que citar. Se cita con el codificador, no concatenando
	// comillas: un `format` con una comilla o una barra invertida dentro rompería el cuerpo entero, y
	// eso es exactamente el bug que esta función existe para no tener.
	return citarJSON(format)
}

// citarJSON envuelve s como string JSON, escapando lo que haga falta. Se escribe a mano y no con
// json.Marshal para que NormalizarFormato no pueda fallar: Marshal devuelve error, y un error aquí
// obligaría a decidir qué hacer con él en un camino donde no hay decisión buena (¿mandar el format roto?
// ¿mandar ninguno?). Un string de Go siempre se puede citar.
func citarJSON(s string) string {
	// El caso normal ("json", "text") no tiene nada que escapar; se resuelve sin recorrer dos veces.
	necesita := false
	for i := 0; i < len(s); i++ {
		if c := s[i]; c == '"' || c == '\\' || c < 0x20 {
			necesita = true
			break
		}
	}
	if !necesita {
		return `"` + s + `"`
	}
	b := make([]byte, 0, len(s)+8)
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"':
			b = append(b, '\\', '"')
		case c == '\\':
			b = append(b, '\\', '\\')
		case c == '\n':
			b = append(b, '\\', 'n')
		case c == '\r':
			b = append(b, '\\', 'r')
		case c == '\t':
			b = append(b, '\\', 't')
		case c < 0x20:
			const hex = "0123456789abcdef"
			b = append(b, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
		default:
			b = append(b, c)
		}
	}
	return string(append(b, '"'))
}
