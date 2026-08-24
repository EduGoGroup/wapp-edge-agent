package wiring

// filters.go — EL CONSUMO DEL kind:"filters" EN EL EDGE (Plan 046 · Ola 2 · T2.2; ADR-0027 sobre el
// mecanismo del ADR-0021).
//
// QUÉ ES ESTO. La nube marca cada sesión de la flota como ACTIVA o PASIVA (columna `fleet_sessions.profile`,
// Plan 046 · T1.1) y empuja el mapa entero del tenant por el stream CloudLink como un ConfigUpdate de
// kind "filters" (T2.1). Aquí se recibe, se valida, se persiste (lo hace el edgeconfig.Service) y se publica
// en una estructura EN MEMORIA que el listener consulta UNA VEZ por mensaje entrante, en la puerta.
//
// QUÉ COMPRA. REQ-07: el entrante de una sesión PASIVA no se encola, no se persiste y no se entrega —«nada
// local»—. El corte vive en internal/adapters/whatsmeow/listener.go (paso 1.5 de onMessage); este fichero
// solo aporta el DATO con el que se decide.
//
// 🔴 FAIL-OPEN EN LAS TRES DIRECCIONES (D-046.2), y es deliberado en las tres:
//   - sesión AUSENTE del mapa      ⇒ activa (el Edge no pierde tráfico por un mapa incompleto);
//   - SIN config de filtros        ⇒ todas activas (un Edge recién arrancado se comporta como hoy);
//   - `profile` DESCONOCIDO        ⇒ activa (un valor nuevo de la nube no puede enmudecer una sesión vieja).
//
// El fallo caro es dejar de recibir mensajes de un cliente real; el barato es subir tráfico de más de una
// sesión que debería estar callada —y ese ya lo cubre `reactiveBlocked` en la nube (D-046.7, defensa en
// profundidad). Se falla siempre hacia el barato.
//
// ZERO-KNOWLEDGE (ADR-0007) / ADR-0034: por aquí pasan session_id y un perfil. Ni contenido, ni teléfonos,
// ni material de llave. Los logs de este fichero emiten versiones y CARDINALIDADES, nunca la lista de
// sesiones.

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/edgeconfig"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
)

// FiltersConfigKind es el kind de config empujada que transporta los PERFILES DE SESIÓN del tenant
// (Plan 046 · D-046.2). El valor es contrato entre dos procesos que no comparten binario: lo produce el
// `filtersConfigProvider` del cloud-platform (T2.1) y lo consume este Edge.
//
// ESTÁ EXPORTADA por la misma razón que IntentsConfigKind (intent.go:14-21): un literal duplicado en otro
// punto del árbol NO da error de compilación y NO da error en ejecución — `Service.Apply` trata el kind
// desconocido como «tolerante a kinds futuros» (service.go:66-70), así que un `"filter"` mal escrito en un
// segundo sitio se ignoraría con un log Info y las sesiones pasivas seguirían subiendo tráfico para
// siempre, sin un solo error. Un único símbolo, importable, es lo que hace ese fallo imposible.
const FiltersConfigKind = "filters"

// Los dos valores de `profile` que el contrato D-046.2 reconoce. Cualquier otro se trata como PerfilActivo
// (fail-open): ver la cabecera.
const (
	// PerfilActivo: la sesión opera como siempre — sus entrantes se encolan, se despachan y suben.
	PerfilActivo = "active"
	// PerfilPasivo: la sesión SOLO emite. Sus entrantes se descartan en la puerta del listener y no dejan
	// rastro local (REQ-07).
	PerfilPasivo = "passive"
)

// filtersPayload es el contrato JSON de D-046.2, tal cual lo arma la nube:
//
//	{"version": 1723..., "sessions": {"<session_id>": {"profile": "active"|"passive"}}}
//
// NO se usa DisallowUnknownFields a propósito: la nube tiene que poder añadir campos (una fecha, un motivo)
// sin dejar mudos a los Edge que aún no los conocen. Lo que sí es estricto es lo que ESTE Edge lee.
type filtersPayload struct {
	Version  int64                    `json:"version"`
	Sessions map[string]filtersSesion `json:"sessions"`
}

// filtersSesion es la entrada por sesión del mapa. Hoy solo lleva el perfil.
type filtersSesion struct {
	Profile string `json:"profile"`
}

// perfilesVigentes es la foto INMUTABLE de la config aplicada. Se construye entera antes de publicarse y
// no se modifica después: el listener la lee sin candado desde el hilo de whatsmeow (atomic.Pointer), y eso
// solo es seguro si nadie escribe en el mapa una vez publicado.
//
// Se guardan SOLO LAS PASIVAS —no el mapa entero— porque la pregunta que el camino caliente hace es «¿esta
// sesión está callada?», y la ausencia ya significa «activa» por contrato (fail-open). Un mapa con las
// activas dentro sería más grande y respondería lo mismo.
type perfilesVigentes struct {
	version int64
	pasivas map[string]struct{}
}

// Perfiles es la vista EN MEMORIA de los perfiles de sesión del tenant, consultable por session_id.
//
// 🔴 ES UNA ESTRUCTURA CONCURRENTE DE VERDAD, y no por gusto: el suscriptor escribe desde la goroutine del
// worker del demux CloudLink (y desde la del Bootstrap, al arrancar) mientras `onMessage` lee desde el hilo
// de eventos de whatsmeow, uno por sesión. Son hilos distintos sobre el mismo dato.
//
//   - LEER es wait-free: un `atomic.Pointer.Load` y un lookup sobre un mapa que ya no cambia. Es lo que
//     corre en el camino caliente (INV-051.2: el handler no puede pagar un mutex compartido por mensaje).
//   - ESCRIBIR se serializa con `mu`, y el candado NO sobra teniendo el atómico: la escritura es un
//     CHECK-AND-SWAP («aplica solo si es más nueva»), y `PushConfig` fanea el MISMO frame a cada sesión
//     viva del tenant (config_push.go:65-81), así que dos workers pueden entrar a la vez con dos versiones
//     distintas. Sin el candado, el `Load`+`Store` de la vieja podría pisar a la nueva y el Edge se quedaría
//     con un mapa retrasado sin que nada fallara.
type Perfiles struct {
	log sharedlogger.Logger

	// mu serializa el check-and-swap de la ESCRITURA (validar y aplicar). Nunca se toma para leer.
	mu sync.Mutex
	// vigente es la foto publicada. nil ⇒ aún no hay config (fail-open: nadie es pasiva).
	vigente atomic.Pointer[perfilesVigentes]
}

// RegisterFilters crea la vista de perfiles y registra el kind "filters" en el edgeconfig.Service
// COMPARTIDO (el mismo que aplica "intents" y "jwks"). Molde literal de RegisterJWKS (auth.go:36-51):
// validador + suscriptor, y el objeto consultable de vuelta.
//
// 🔴 EL RegisterKind ES LOAD-BEARING. `Service.Apply` IGNORA los kinds no registrados (registrationFor →
// `!known` ⇒ log Info + Ack sin persistir), así que sin esta llamada el Edge acusaría cada ConfigUpdate de
// perfiles y no aplicaría ninguno: todas las sesiones seguirían activas, subiendo el tráfico que REQ-07
// promete cortar, y NO HABRÍA UN SOLO ERROR EN NINGÚN LOG.
//
// 🔴 SE LLAMA ANTES DE `Service.Bootstrap`. Bootstrap solo recorre los kinds REGISTRADOS (service.go:103-122):
// registrar después de arrancar deja al Edge esperando un push nuevo para volver a saber quién estaba
// callado, y hasta entonces una sesión pasiva subiría todo lo que llegara. Es el criterio (f) de la tarea.
func RegisterFilters(svc *edgeconfig.Service, log sharedlogger.Logger) *Perfiles {
	if log == nil {
		log = sharedlogger.Default()
	}
	p := &Perfiles{log: log}
	if svc == nil {
		// Sin Service no hay canal de config (el daemon siempre lo cablea; esto es la red de los arranques
		// que no vienen de `agent serve`). Se devuelve una vista VACÍA y fail-open: nadie es pasiva.
		log.Warn("filtros: sin edgeconfig.Service; los perfiles de sesión NO se aplicarán (todas las sesiones operan como ACTIVAS)")
		return p
	}
	svc.RegisterKind(FiltersConfigKind, p.validar, p.aplicar)
	log.Info("filtros: applier ConfigUpdate kind:\"filters\" registrado (Plan 046, ADR-0027); las sesiones pasivas se cortarán en la puerta del listener",
		"kind", FiltersConfigKind)
	return p
}

// EsPasiva responde si la sesión está marcada como PASIVA en la config vigente. Es lo que el listener
// consulta UNA VEZ por entrante, en la puerta.
//
// Nil-safe y fail-open en todas sus salidas (ver la cabecera): receptor nil, session_id vacío o config
// ausente ⇒ false (= activa = comportamiento de hoy).
//
// 🔴 SE CONSULTA POR MENSAJE Y NO SE CACHEA AGUAS ARRIBA. El perfil cambia en caliente: una sesión que la
// consola pasa a `active` tiene que volver a recibir sin reiniciar el Edge, y una que pasa a `passive`
// tiene que callarse en el siguiente mensaje. El coste es un Load atómico y un lookup: nanosegundos.
func (p *Perfiles) EsPasiva(sessionID string) bool {
	if p == nil || sessionID == "" {
		return false
	}
	v := p.vigente.Load()
	if v == nil {
		return false // sin config de filtros ⇒ se comporta como hoy (D-046.2)
	}
	_, pasiva := v.pasivas[sessionID]
	return pasiva
}

// PasivaFunc devuelve el predicado que el sessionmgr cablea en cada listener (sessionmgr.WithSesionPasiva).
// Devuelve un MÉTODO y no un bool ya evaluado (mismo molde que el retirado ClasificadorActivoFunc): quien lo
// llame lee el estado en el momento del mensaje, no la foto del arranque. Con el receptor nil devuelve nil
// y el Listener cae a su default SEGURO (nadie es pasiva).
func (p *Perfiles) PasivaFunc() func(string) bool {
	if p == nil {
		return nil
	}
	return p.EsPasiva
}

// Version devuelve la versión de la config de perfiles vigente (0 si aún no hay ninguna).
//
// 🔴 TIENE UN LLAMANTE DE PRODUCCIÓN, Y ESO ES EL PUNTO: el daemon se la pasa al `health.Collector`
// (`health.WithFiltersVersion`), que la publica en cada Report como **`filters_version`** —el nombre es
// CONTRATO— en `GET /v1/health` y en el bundle de diagnóstico, junto a `dropped_passive`. Nació con un
// docstring que decía «la consume la observabilidad» siendo falso: el único llamante era su test, que es
// exactamente la trampa de T1.13 del Plan 051 («once llamantes y los once eran tests»).
//
// 🔴 QUÉ PREGUNTA CONTESTA, y por qué no la contesta ninguna otra señal: «¿con qué mapa está filtrando este
// Edge?». `Store.Put` sobrescribe sin condición y los ConfigUpdate los procesan workers en paralelo, así que
// un push viejo puede ganar la carrera de escritura y dejar en disco una versión anterior a la vigente. El
// síntoma no aparece hasta el reinicio siguiente —y entonces es una sesión reactivada que sigue muda, sin
// error ni log—. Comparar este número con el que la consola dice haber empujado es el único diagnóstico.
//
// Se lee con un `Load` atómico, sin candado: es el mismo dato inmutable que lee la puerta.
func (p *Perfiles) Version() int64 {
	if p == nil {
		return 0
	}
	if v := p.vigente.Load(); v != nil {
		return v.version
	}
	return 0
}

// validar es el Validator del kind. Corre DENTRO de Service.Apply, ANTES de persistir: si devuelve error, el
// Service conserva el last-known-good en disco y lo registra (service.go:82-88).
//
// 🔴 AQUÍ VIVE LA GUARDA ENTERA DEL KIND, Y ESO ES DELIBERADO: es el ÚNICO punto que corre ANTES del `Put`.
// Todo lo que este método rechaza NO LLEGA AL DISCO, así que el last-known-good se conserva en las dos
// memorias —la RAM y la fila— y el arranque siguiente levanta con un mapa bueno. Cualquier guarda puesta en
// el suscriptor llegaría tarde: para entonces la fila mala ya está escrita. Por eso la monotonicidad se juzga
// sobre el `version` del PAYLOAD, que es el único dato que esta firma recibe y —por decisión del 2026-08-21—
// el único autoritativo del contrato.
//
// 🔴 ES ASIMÉTRICA CON `intents` Y `jwks` A PROPÓSITO. El mecanismo compartido NO ordena versiones: su única
// guarda es `if found && cur.Version == version`, una IGUALDAD DE STRINGS. Una versión MÁS VIEJA que llegue
// tarde —una reconexión con un push en vuelo, dos pushes que se cruzan— pasaría esa guarda y se aplicaría,
// dejando al Edge con un mapa retrasado: sesiones que volvieron a ser activas seguirían mudas, o al revés.
// `filters` es el único kind cuyo contrato PROMETE monotonicidad (D-046.2 y REQ-06: «el Edge descarta
// versiones viejas»), así que es el único que la implementa. Subirla al Service cambiaría el comportamiento
// de los otros dos kinds, que no la piden y cuyas versiones no son necesariamente numéricas.
//
// ⚠️ ESTA GUARDA SOLO ES ATÓMICA PORQUE `Service.Apply` SERIALIZA validar→persistir→notificar bajo su propio
// candado (edgeconfig/service.go, `aplicaMu`). Sin él, dos workers del demux podían validar contra la misma
// foto y escribir en el orden contrario al de sus versiones: el `mu` de aquí ordena los swaps EN MEMORIA,
// pero no puede ordenar los `Put` que ocurren fuera.
//
// NO se compara contra la fila del store sino contra la versión VIGENTE EN MEMORIA — que es la misma cosa
// en cuanto Bootstrap corre al arrancar (repuebla desde el disco), y es la única a la que este validador
// tiene acceso: la firma de edgeconfig.Validator recibe solo el payload.
func (p *Perfiles) validar(payload []byte) error {
	nuevo, _, err := parseFilters(payload)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur := p.vigente.Load(); cur != nil && nuevo.version <= cur.version {
		return fmt.Errorf("edgeconfig/filters: versión %d no es POSTERIOR a la vigente %d; se conserva la vigente "+
			"(D-046.2: la versión es monotónica y el Edge descarta lo anterior o igual)", nuevo.version, cur.version)
	}
	return nil
}

// aplicar es el Subscriber del kind: publica el mapa recién persistido. Lo invoca el Service en DOS
// momentos —al aplicar un push (Apply) y al arrancar con lo persistido (Bootstrap)—, y el segundo es el que
// hace que el corte siga funcionando tras un reinicio sin esperar un push nuevo.
//
// 🔴 REPITE LA GUARDA DE MONOTONICIDAD, y no es una defensa duplicada por descuido: `Bootstrap` NO llama al
// validador (service.go, notifica directo), así que sin la comprobación aquí ese camino no tendría ninguna.
//
// 🔴 ESTE MÉTODO NO RECHAZA NADA POR EL `version` DEL FRAME (regla del 2026-08-21). La versión AUTORITATIVA es
// la del PAYLOAD y solo esa; la del frame es METADATO del sobre. El motivo es de dónde vive cada guarda: el
// validador corre DENTRO de `Service.Apply` y ANTES del `Put`, así que lo que él rechaza no toca el disco; lo
// que se rechazara AQUÍ ya estaría persistido, porque el suscriptor se invoca DESPUÉS de escribir. Con la
// regla anterior —«si el frame no es numérico, no apliques»— un solo frame con un `version` raro dejaba en
// disco una fila que este mismo método volvería a rechazar en el arranque siguiente, y entonces `vigente`
// quedaba NIL: fail-open total, todas las sesiones activas otra vez y ni un error en el log. Ahora el frame
// solo puede producir un Warn (ver avisarSiElFrameNoCuadra) y el LKG se conserva en disco Y en memoria.
//
// Un fallo de este método NO tumba nada: se loguea y se CONSERVA EL MAPA ANTERIOR (last-known-good en
// memoria). El precedente es el suscriptor de "jwks" (auth.go:40-47), que hace exactamente lo mismo con el
// verificador anterior.
func (p *Perfiles) aplicar(rec edgeconfig.Record) {
	nuevo, desconocidos, err := parseFilters(rec.Payload)
	if err != nil {
		p.log.Error("filtros: config de perfiles ilegible al aplicar; se CONSERVA el mapa anterior (last-known-good). "+
			"Mientras tanto, las sesiones que ya eran pasivas siguen calladas y el resto opera como hoy",
			"version", rec.Version, "error", err)
		return
	}
	p.avisarSiElFrameNoCuadra(rec.Version, nuevo.version)
	if desconocidos > 0 {
		// Fail-open explícito y CONTADO: un `profile` que este Edge no conoce se trata como ACTIVO. Si un día
		// la nube introduce un tercer perfil, esta línea es lo que lo delata antes de que nadie note nada raro.
		p.log.Warn("filtros: hay sesiones con un `profile` DESCONOCIDO en la config; se tratan como ACTIVAS (fail-open)",
			"sesiones", desconocidos, "version", nuevo.version)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if cur := p.vigente.Load(); cur != nil && nuevo.version <= cur.version {
		p.log.Warn("filtros: config de perfiles ANTERIOR O IGUAL a la vigente; se descarta y se conserva la vigente "+
			"(D-046.2: versión monotónica)", "version_recibida", nuevo.version, "version_vigente", cur.version)
		return
	}
	p.vigente.Store(nuevo)
	// Solo CARDINALIDADES (ADR-0034): cuántas sesiones están calladas, nunca cuáles.
	p.log.Info("filtros: perfiles de sesión aplicados; las sesiones pasivas se descartan en la puerta del listener (REQ-07)",
		"version", nuevo.version, "sesiones_pasivas", len(nuevo.pasivas))
}

// avisarSiElFrameNoCuadra compara el `version` del SOBRE con el del PAYLOAD y AVISA si no cuadran. No decide
// nada: no descarta, no conserva, no publica. Es la única cosa que el frame puede provocar.
//
// 🔴 POR QUÉ SOLO UN WARN, Y NO UN DESCARTE. El contrato D-046.2 dice que el frame lleva la representación
// decimal del mismo entero del payload, y cuando eso deja de cumplirse hay un emisor roto EN LA NUBE. Pero la
// reacción tiene que ser proporcional a lo que cada opción cuesta AQUÍ: descartar por un desajuste de metadato
// significa que TODAS las sesiones pasivas del tenant vuelven a encolar, persistir y entregar su tráfico
// entrante —el fallo caro, el que REQ-07 existe para impedir—; seguir con la versión del payload significa,
// como mucho, que la idempotencia por versión del Service (que compara strings, service.go) reaplique una
// config idéntica de vez en cuando. Se falla hacia lo barato y se GRITA para que se arregle en el emisor.
//
// Los dos casos se distinguen en el log a propósito: «no es un número» apunta a un emisor que cambió de
// formato, y «no coinciden» a uno que perdió la sincronía entre sobre y contenido. Son dos bugs distintos y
// quien lea el log tiene que poder decir cuál es. ADR-0034: solo versiones, ni sesiones ni contenido.
func (p *Perfiles) avisarSiElFrameNoCuadra(frame string, payload int64) {
	frameVer, err := strconv.ParseInt(strings.TrimSpace(frame), 10, 64)
	if err != nil {
		p.log.Warn("filtros: el `version` del frame NO es un entero decimal (contrato D-046.2); es METADATO y no "+
			"gatea nada: se aplica la versión del PAYLOAD, que es la autoritativa. Revisa el proveedor de la nube",
			"version_frame", frame, "version_payload", payload)
		return
	}
	if frameVer != payload {
		p.log.Warn("filtros: el `version` del frame y el del payload NO coinciden; manda el del payload (D-046.2)",
			"version_frame", frameVer, "version_payload", payload)
	}
}

// parseFilters deserializa y valida el payload del contrato D-046.2. Devuelve la foto inmutable lista para
// publicar y cuántas sesiones traían un `profile` DESCONOCIDO (que se cuentan como activas: fail-open).
//
// Es estricto en dos cosas y solo dos, y las dos son las que hacen falsable el contrato:
//   - el JSON tiene que ser un objeto legible;
//   - `version` no puede ser negativa, y el CERO solo vale con el mapa VACÍO.
//
// 🔴 EL CERO NECESITA ESA SALVEDAD Y NO ES UN CAPRICHO. El emisor documenta que «Version = 0 con Sessions
// vacío significa que este tenant no tiene ni una fila de sesión» y que ese frame SE EMPUJA IGUAL (regla 2
// de T2.1) — es lo que puede llegar al conectar un Edge cuya sesión aún no está registrada en la flota.
// Rechazarlo sería un ERROR en el log en cada arranque, para nada. Pero el cero es TAMBIÉN el valor que
// deja un campo AUSENTE, así que un payload CON sesiones y SIN `version` sí se rechaza: es un emisor roto,
// y aplicarlo dejaría al Edge con un mapa cuya versión no ordena nada.
func parseFilters(payload []byte) (*perfilesVigentes, int, error) {
	var doc filtersPayload
	if err := json.Unmarshal(payload, &doc); err != nil {
		return nil, 0, fmt.Errorf("edgeconfig/filters: payload ilegible: %w", err)
	}
	if doc.Version < 0 {
		return nil, 0, fmt.Errorf("edgeconfig/filters: `version` = %d; el contrato D-046.2 exige un entero no negativo y monotónico", doc.Version)
	}
	if doc.Version == 0 && len(doc.Sessions) > 0 {
		return nil, 0, fmt.Errorf("edgeconfig/filters: payload con %d sesiones y `version` = 0 (¿campo ausente?); "+
			"el cero solo es válido con el mapa vacío («el tenant no tiene ni una fila de sesión»)", len(doc.Sessions))
	}
	v := &perfilesVigentes{version: doc.Version, pasivas: make(map[string]struct{})}
	desconocidos := 0
	for id, s := range doc.Sessions {
		if id == "" {
			continue // una clave vacía no identifica a ninguna sesión: no puede callar a nadie
		}
		switch s.Profile {
		case PerfilPasivo:
			v.pasivas[id] = struct{}{}
		case PerfilActivo:
			// Se OMITE del mapa a propósito: la ausencia ya significa activa. Guardarla no añadiría nada y
			// haría el mapa proporcional al tenant entero en vez de a sus sesiones calladas.
		default:
			desconocidos++
		}
	}
	return v, desconocidos, nil
}
