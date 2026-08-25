package cajero

// servidor.go — EL CAJERO SIRVE INFERENCIA (Plan 044 · Ola 1.6 · T1.6-2, ADR-0045 §2/§8, REQ-34).
//
// EL OFICIO NUEVO. Hasta el ADR-0045 el cajero clasificaba por INICIATIVA PROPIA: reclamaba lotes de la
// cola y le preguntaba al modelo qué quería el cliente. Desde el ADR-0045 la clasificación es PULL: el
// Cloud —único orquestador LLM— baja el prompt ya construido en un `inference_request` y el Edge lo
// SIRVE. Este fichero es ese servicio, y `Cajero.bucle` dejó de reclamar (ver el bloque «EL BUCLE YA NO
// CLASIFICA» en cajero.go).
//
// POR QUÉ VIVE EN EL PROCESO DEL CAJERO Y NO EN EL DAEMON: REQ-051.10 — «ningún otro proceso que el
// worker habla con Ollama». El frame llega al daemon (`agent serve`, que es quien tiene el stream
// CloudLink) y cruza al cajero por un socket unix. El gate de ese requisito es un grep de `ollama.New`,
// y sigue dando un solo resultado: cmd/agent/cajero.go.
//
// 🔴 EL EDGE NO INTERPRETA NADA (ADR-0045 §1): el prompt va al modelo VERBATIM, la salida sube CRUDA. No
// se parsea, no se valida contra un schema, no se sanea y no se trunca. Todo eso es del Cloud.
//
// 🔴 INV-051.1: ni el prompt ni la salida salen por ningún log, tampoco en debug. Lo que se loguea es
// `command_id`, TAMAÑOS y DESENLACE.

import (
	"context"
	"errors"
	"time"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app/breaker"
	"github.com/EduGoGroup/wapp-edge-intent/ollama"
)

// DefaultTemperatura es la temperatura que el Edge aplica cuando el Cloud no manda ninguna
// (InferenceRequest.temperature ausente). Es 0,1 y NO es un número nuevo: es exactamente la que el
// clasificador venía fijando (classifier.New la mete en sus llmOpts), así que el día que se apagó el
// push la temperatura efectiva del modelo no cambió. Baja a propósito: casi todo lo que se le pide a
// este modelo es clasificar, y ahí el determinismo vale más que la variedad.
const DefaultTemperatura = 0.1

// DefaultMaxTimeoutMS es el TECHO de lo que el Cloud puede pedir en `InferenceRequest.timeout_ms`, en
// milisegundos. Es 120.000.
//
// POR QUÉ EXISTE (y por qué no basta con el default): el plazo lo fija el CLOUD porque es quien conoce
// su ventana, pero el aforo de Ollama es de UNA plaza. Una petición que pidiera diez minutos ocuparía
// esa plaza diez minutos y todas las demás se irían con `EDGE_SIN_CAPACIDAD` — un fallo del Cloud (o un
// `timeout_ms` mal calculado) se convertiría en una caída del servicio de inferencia de la máquina del
// cliente. El techo convierte eso en un recorte silencioso y anotado.
//
// POR QUÉ 120.000: es ~2,6× el peor máximo MEDIDO en el VPS real (45.629 ms, escenario G3 de la tabla de
// contención del 2026-08-16, docs/journal/2026-08-16.md §"Tabla B"). Deja sitio de sobra para el peor
// caso conocido sin dejar sitio para un error de cálculo de tres cifras.
const DefaultMaxTimeoutMS = 120_000

// Chateador es la dependencia del cajero hacia el proveedor local de LLM. Es la interfaz MÍNIMA que
// hace falta para servir una inferencia y vive aquí, no en el módulo del proveedor, para que los tests
// puedan meter un doble sin levantar Ollama. La cumple *ollama.Client.
//
// SupportsThinking está en la interfaz —y no resuelto fuera— porque la política de `think` es DEL EDGE
// y tiene que aplicarse en el mismo sitio donde se arma la petición: ver el bloque de `think` en
// Inferir.
type Chateador interface {
	Chat(ctx context.Context, req ollama.ChatRequest) (*ollama.ChatResponse, error)
	SupportsThinking(ctx context.Context, model string) bool
}

// ServidorInferencia devuelve el servicio que atiende los `inference_request` del Cloud.
//
// 🔴 LO CONSTRUYE EL CAJERO Y NO EL CABLEADO, Y ESA ES LA MITAD IMPORTANTE DE ESTE MÉTODO. El servidor
// necesita el AFORO, el BREAKER y el HISTOGRAMA, y los tres tienen que ser LOS MISMOS que los del
// proceso: dos aforos de una plaza son dos inferencias simultáneas contra el mismo Ollama (el
// solapamiento que la O0 midió como causa de que el p50 se dispare), dos breakers dejan a uno
// martilleando un proveedor que el otro ya sabe caído, y dos histogramas parten la población del p50 que
// viaja en el parte. Exponiendo el servidor DESDE el Cajero no existe la forma de equivocarse: no hay
// constructor público que acepte un aforo distinto.
//
// Devuelve nil si el cajero se construyó sin proveedor (Deps.Ollama nil): sin él no hay nada que servir,
// y un nil explícito es mejor que un servidor que devuelve OLLAMA_DOWN eternamente sin decir por qué.
func (c *Cajero) ServidorInferencia() app.ServidorInferencia {
	if c.ollama == nil {
		return nil
	}
	return &servidorInferencia{c: c}
}

// servidorInferencia implementa app.ServidorInferencia sobre el estado del Cajero.
type servidorInferencia struct {
	c *Cajero
}

// Inferir sirve UNA inferencia. Es el orden del ADR-0045 §2, y cada paso tiene su error nombrado:
//
//	breaker.BeginAttempt() ── abierto ────────────────► BREAKER_OPEN   (inmediato, sin tocar el modelo)
//	Aforo.TomarHasta(plazo) ── plazo agotado ─────────► EDGE_SIN_CAPACIDAD
//	Chat(ctx) ── transporte / != 200 ─────────────────► OLLAMA_DOWN
//	          └─ ctx vencido ─────────────────────────► TIMEOUT
//	RecordSuccess / RecordFailure
//
// (LEASE_INVALID no se produce aquí: lo produce el DAEMON, que es quien tiene los Validator de lease.
// Ver el carril, internal/adapters/cloudlink/inferencia.go.)
func (s *servidorInferencia) Inferir(ctx context.Context, p app.PeticionInferencia) (app.RespuestaInferencia, error) {
	c := s.c
	plazo := c.plazoDe(p.Timeout)

	// LA CLASE SE NORMALIZA UNA VEZ, AQUÍ, Y DE AQUÍ NO SALE MÁS QUE A ETIQUETAS (T1.7-3). Vacía o
	// desconocida ⇒ `interactivo` (app.NormalizarClase). Se resuelve al principio para que el log del
	// camino de fallo y el del de éxito hablen de la MISMA etiqueta, no para que nadie decida con ella:
	// por debajo de esta línea `clase` sólo aparece como argumento de un log o de un contador, y eso lo
	// custodia un test estructural sobre el AST de este paquete
	// (TestLaClaseNoGobiernaNingunaDecision), no una convención que alguien pueda olvidar.
	clase := app.NormalizarClase(p.Clase)

	// ─── ④bis ¿ESTA PETICIÓN LE ENSEÑA ALGO AL CIRCUITO? ─────────────────────
	//
	// 🔴 LA COSTURA DEL CALENTAMIENTO (T1.7-2, para T1.7-4). Se resuelve AQUÍ, en una sola variable y antes
	// de que el breaker haga nada, y no repartida por los seis sitios que lo tocan: el compromiso de
	// `BeginAttempt` obliga a que quien lo pidió lo salde, así que «pedir permiso pero no registrar el
	// resultado» dejaría el sondeo del medio-abierto RESERVADO PARA SIEMPRE (ver cerrarSinIntentar). O el
	// breaker participa entero en la petición, o no participa nada. Un calentamiento no participa.
	//
	// CONSECUENCIA QUE HAY QUE SABER LEER: un calentamiento tampoco CONSULTA el circuito, así que se sirve
	// con el circuito abierto. Es lo correcto para lo que es —con Ollama recién caído, el calentamiento es
	// exactamente lo que hay que seguir intentando para saber cuándo vuelve— y además evita gastar un
	// sondeo del medio-abierto en tráfico que a nadie le importa. Lo que sí paga como todos es el AFORO: no
	// hay adelantamiento, un calentamiento no le quita la plaza a una petición real.
	//
	// El resto del camino —aforo, plazo, histograma, contadores y log— es EL MISMO. Un calentamiento que
	// no se midiera igual que lo demás sería una inferencia invisible ocupando la máquina del cliente.
	cuentaParaElBreaker := !p.Calentamiento

	// ─── ⑤ EL BREAKER, PRIMERO Y SIN TOCAR NADA MÁS ──────────────────────────
	//
	// Va ANTES del aforo a propósito: el ADR-0045 exige que un breaker abierto responda INMEDIATAMENTE.
	// Si se pidiera plaza primero, una petición llegada con el circuito abierto se pasaría el plazo entero
	// haciendo cola para descubrir al final que no iba a intentarse — y respondería el error equivocado
	// (SIN_CAPACIDAD) por el camino.
	//
	// 🔴 SE MIRA `State()` ANTES DE `BeginAttempt()` Y NO ES REDUNDANTE: es lo único que permite saber si
	// el `true` que viene a continuación es «el circuito está cerrado» o «me reservaron EL SONDEO del
	// medio-abierto». Esa diferencia decide qué hay que hacer si abortamos sin llamar al modelo (ver
	// `cerrarSinIntentar`), y `BeginAttempt` no la dice. La lectura puede quedar obsoleta entre las dos
	// llamadas, y el sesgo del error es el bueno: si el circuito se cerró justo en medio, creeremos que
	// era sondeo y registraremos un fallo de más — conservador. El caso contrario (creer «cerrado» algo
	// que era sondeo, y dejar el flag reservado para siempre) exigiría que cinco fallos y sesenta
	// segundos cupieran entre dos instrucciones.
	var eraSondeo bool
	if cuentaParaElBreaker {
		eraSondeo = c.breaker.State() == breaker.StateHalfOpen
		if !c.breaker.BeginAttempt() {
			c.errBreakerAbierto.Add(1)
			c.log.Warn("cajero: inferencia RECHAZADA con el circuito abierto; no se llamó al proveedor",
				"command_id", p.CommandID, "circuito", c.breaker.State(),
				"prompt_bytes", len(p.Prompt))
			return app.RespuestaInferencia{}, app.ErrInferenciaBreakerAbierto
		}
	}

	// ─── ⑥ EL AFORO, CON ESPERA ACOTADA ──────────────────────────────────────
	//
	// La espera se acota con EL MISMO plazo de la petición, no con uno propio: el presupuesto es del
	// Cloud y partirlo en dos (tanto para la cola, tanto para el modelo) inventaría un reparto que nadie
	// pidió y dejaría plazo sin usar en el caso normal. Lo que sí cambia es el VEREDICTO, y por eso el
	// aforo lo devuelve él (ver Aforo.TomarHasta): agotarlo AQUÍ es EDGE_SIN_CAPACIDAD —nunca TIMEOUT—,
	// porque el modelo no llegó a mirar la petición.
	//
	// Es también el punto donde el plazo restante deja de ser el plazo entero: lo que se esperó en la
	// cola se le descuenta a la inferencia. Sin ese descuento, una petición que esperase 40 s de sus 45
	// tendría después otros 45 para el modelo y el Cloud vería 85 s por un presupuesto de 45.
	inicioEspera := c.ahora()
	tomada, err := c.aforo.TomarHasta(ctx, plazo)
	if !tomada {
		c.errSinCapacidad.Add(1)
		s.cerrarSinIntentar(eraSondeo)
		espera := c.ahora().Sub(inicioEspera)
		c.log.Warn("cajero: inferencia RECHAZADA por falta de plaza; el equipo no pudo atenderla dentro de su plazo "+
			"(NO es un timeout del modelo: nunca se le llamó)",
			"command_id", p.CommandID, "plazo_ms", plazo.Milliseconds(),
			"esperado_ms", espera.Milliseconds(), "plazas", c.aforo.Plazas())
		return app.RespuestaInferencia{}, err
	}
	defer c.aforo.Soltar()

	// ─── ⑦ EL MODELO ─────────────────────────────────────────────────────────
	//
	// 🔴 EL DESCUENTO SÓLO SE APLICA SI HAY PLAZO, y esa guarda no es defensiva: sin ella, `plazo == 0`
	// («sin plazo propio»: el Cloud no lo fijó Y el Edge tiene Deps.Timeout desactivado) daría
	// `restante = 0 - loQueSeEsperó`, o sea SIEMPRE negativo, y TODA inferencia se rechazaría con
	// EDGE_SIN_CAPACIDAD con el aforo libre y sin llamar jamás al modelo. Un Edge que no sirve ni una
	// inferencia, sin un solo error, por un plazo que su dueño creía haber DESACTIVADO. Hoy el guardarraíl
	// de config (InferenceTimeoutMS <= 0 ⇒ default) impide llegar aquí desde el cableado real, pero la
	// promesa de Deps.Timeout <= 0 es explícita y este es el sitio donde se cumple.
	ictx := ctx
	if plazo > 0 {
		restante := plazo - c.ahora().Sub(inicioEspera)
		if restante <= 0 {
			// La plaza llegó justo cuando el plazo se acababa. Sigue siendo capacidad, no lentitud del
			// modelo: el veredicto no cambia por haber ganado la carrera por un milisegundo.
			c.errSinCapacidad.Add(1)
			s.cerrarSinIntentar(eraSondeo)
			c.log.Warn("cajero: se consiguió plaza sin plazo restante; se rechaza sin llamar al proveedor",
				"command_id", p.CommandID, "plazo_ms", plazo.Milliseconds())
			return app.RespuestaInferencia{}, app.ErrInferenciaSinCapacidad
		}
		var cancel context.CancelFunc
		ictx, cancel = context.WithTimeout(ctx, restante)
		defer cancel()
	}

	inicio := c.ahora()
	resp, err := s.chat(ictx, p)
	transcurrido := c.ahora().Sub(inicio)

	// EL HISTOGRAMA SE ALIMENTA EN LOS DOS CAMINOS, igual que hacía el bucle de clasificación y por el
	// mismo motivo (ver histogramaInferencia.observar): un timeout es latencia que el Edge PAGÓ —ocupó la
	// única plaza todo ese tiempo—, y medir sólo los éxitos daría el número al revés de como hace falta.
	c.inferencia.observar(transcurrido)
	// EL REPARTO POR CLASE SE ALIMENTA EN LOS DOS CAMINOS, exactamente igual que el histograma de arriba y
	// por el mismo motivo: la pregunta que responde es «¿qué mezcla de tráfico está atendiendo esta
	// máquina?», y una petición de lote que se comió su plazo y murió ocupó la plaza igual que una que
	// salió bien. Contando sólo los éxitos, un Cloud que mandara una ráfaga de lotes que todos revientan
	// se vería como CERO lotes.
	//
	// ⚠️ POBLACIÓN DISTINTA A LA DEL REPARTO POR RÉGIMEN, y hay que saberlo para leerlos: el régimen sólo
	// se puede clasificar cuando hubo respuesta con prefill medible, así que suma(por_clase) >= suma(por
	// _régimen) SIEMPRE, y la diferencia son los fallos. No se dividen entre sí.
	c.porClase.contar(clase)

	if err != nil {
		return app.RespuestaInferencia{}, s.registrarFalloDeInferencia(ctx, ictx, p, err, transcurrido, eraSondeo)
	}

	// ─── ⑧ EL COMPROMISO DEL BREAKER, LADO ÉXITO ─────────────────────────────
	//
	// Pasa por `registrarAcierto`, no por `RecordSuccess()` a pelo: ahí vive el criterio del MP-09
	// (ADR-0042) —un acierto que se comió >= 80 % de su plazo castiga al breaker igual que un timeout—, y
	// saltárselo aquí devolvería el circuito a la conducta que el MP-09 encontró rota: contra un proveedor
	// LENTO no abre nunca, porque un acierto casual borra la racha de fallos.
	// 🔴 EL PLAZO VIAJA HASTA AQUÍ, Y ES EL ARREGLO DE T1.7-2. El umbral de lentitud se deriva del plazo de
	// ESTA petición y no del default del proceso: sin esto, una interactiva de 10 s que responde en 9,9 s
	// se contaba como sana (9,9 < 36, el umbral del default de 45 s) habiéndose quedado a 100 ms de morir.
	var lento bool
	if cuentaParaElBreaker {
		lento = c.registrarAcierto(transcurrido, plazo)
	}

	m := resp.Metrics()
	// ─── ⑨ PREFILL Y GENERACIÓN, POR SEPARADO (T1.7-5) ───────────────────────
	//
	// 🔴 POR QUÉ NO BASTA CON `transcurrido`, que es lo que se venía publicando: ese número mezcla DOS
	// REGÍMENES QUE SE DIFERENCIAN EN UN ORDEN DE MAGNITUD. Con el prefijo FRÍO el prefill cuesta ~21,6 ms
	// por token; con el prefijo CALIENTE baja a 0,1-1,2 s el prompt entero. Ese número mezclado es el que
	// dejó DOS p50 IRRECONCILIABLES en este repo —~20 s en el informe de diseño contra 8,1 s en campo— y
	// no era un error de medición: medían poblaciones con distinto CALOR DE PREFIJO. Con un solo número
	// esa diferencia es invisible, y con ella la única palanca que de verdad mueve la latencia.
	//
	// LA GENERACIÓN SALE DE `resp.EvalDuration` Y NO DE `Metrics`, que no la expone en ms: es el gemelo de
	// PromptMs, y ninguno de los dos incluye LoadDuration —cargar el modelo del disco—, que es una TERCERA
	// cosa y se loguea aparte como `load_ms`.
	//
	// 🔴 SON TRES FASES Y HAY QUE PUBLICAR LAS TRES, o el A/B del precalentado saca la conclusión al revés.
	// `keep_alive` protege DOS cosas que se enfrían juntas pero son distintas: el MODELO cargado (load) y la
	// CACHÉ DE PREFIJOS (prefill). Con `keep_alive=0` se pierden las dos a la vez, así que una inferencia del
	// lado A llega con load ALTO y prefill FRÍO — y sin `load_ms` no hay forma de saber cuánto de la
	// diferencia fue recargar el modelo (39 s medidos) y cuánto fue digerir el prompt. Se atribuiría todo al
	// prefijo, que es justo la magnitud que el experimento quiere medir.
	regimen := c.observarFases(m.PromptMs, resp.EvalDuration/int64(time.Millisecond))

	// ─── ⑨bis · EL PREFIJO SE ENFRIÓ SIN QUE NADIE SE ENTERARA (DEUDA-044.10) ──────────────────────────
	//
	// Una inferencia que NO es un calentamiento y sale `frio` es la prueba —la única que existe— de que la
	// caché de prefijo se perdió por debajo: típicamente porque **Ollama se reinició**, que es justo lo que
	// el fusible `MemoryMax` de `ollama.service` está puesto para provocar a propósito.
	//
	// 🔴 POR QUÉ SE MIRA EL PREFIJO Y NO A OLLAMA, que era el candidato obvio: la readiness de este Edge
	// observa a SU CAJERO, y el cajero no se entera de que Ollama se reinició —no se reinicia él—. Y una
	// sonda de salud contra Ollama tampoco serviría: mediría que está VIVO, y tras un reinicio lo está,
	// con `ollama ps` diciendo `Forever` y el prefijo helado. Eso ya pasó en campo el 2026-08-25 y costó
	// una inferencia muerta por timeout a los 37.987 ms. El prefijo es el sujeto correcto porque es lo que
	// duele; y además es evento, no reloj (D-044.43): lo dispara el desenlace de una petición real.
	//
	// EL CALENTAMIENTO SE EXCLUYE Y ES LO QUE CORTA EL BUCLE: el calentamiento que se pide aquí saldrá él
	// mismo `frio` (es el que paga el prefill), así que sin esta guarda cada uno pediría el siguiente.
	//
	// 🔴 ESTA RAMA NO BASTA, Y SE COMPROBÓ EN CAMPO: es el camino FELIZ, y tras un reinicio de Ollama la
	// inferencia fría **no se sirve, muere por timeout**. La segunda mitad del mecanismo vive en
	// `registrarFalloDeInferencia`; las dos comparten guarda, así que un episodio pide UN recalentamiento
	// aunque se manifieste por los dos caminos.
	if regimen == RegimenFrio && !p.Calentamiento {
		c.avisarPrefijoFrio(ctx, CausaFrioServida)
	} else if regimen == RegimenCaliente {
		// LA CACHÉ VOLVIÓ: se rearma el aviso. Cierra el ciclo con un HECHO OBSERVADO y no con un plazo,
		// que es lo que evita tener que elegir un número arbitrario de enfriamiento.
		c.prefijoFrioAvisado.Store(false)
	}

	salida := resp.Content()
	// 🔴 INV-051.1: ni `p.Prompt` ni `salida`. Sólo tamaños, tiempos y el desenlace.
	c.log.Info("cajero: inferencia SERVIDA",
		"command_id", p.CommandID,
		"latencia_ms", transcurrido.Milliseconds(),
		"total_ms", m.TotalMs,
		"prompt_tokens", m.PromptTokens,
		"output_tokens", m.OutputTokens,
		"tokens_per_sec", m.TokensPerSec,
		"prompt_bytes", len(p.Prompt),
		"salida_bytes", len(salida),
		"plazo_ms", plazo.Milliseconds(),
		// EL UMBRAL CON EL QUE SE JUZGÓ ESTA, al lado de su plazo y su latencia. Los tres juntos hacen que la
		// línea se pueda auditar sola: con dos vías conviviendo (ADR-0044) un `lento=false` sin el umbral
		// obliga a adivinar contra qué número se comparó. Es también el dato que necesitaría quien algún día
		// publique esto como serie — sin él habría que reconstruirlo desde una constante y un default.
		"umbral_lento_ms", umbralLentoDe(plazo).Milliseconds(),
		"lento", lento,
		"calentamiento", p.Calentamiento,
		// LAS DOS FASES AL LADO DEL TOTAL, y el régimen con el que se clasificó el prefill. Los tres juntos
		// hacen que la línea se pueda auditar sola: un `regimen=frio` con `prefill_ms=120` delataría que los
		// umbrales de esta máquina están mal puestos sin tener que ir a leer una constante.
		//
		// `regimen` VACÍO significa «no medible» —el proveedor no devolvió `prompt_eval_duration`—, nunca
		// «templado». Es la misma semántica de presencia que el sub-mensaje del heartbeat.
		"load_ms", m.LoadMs,
		"prefill_ms", m.PromptMs,
		"generacion_ms", resp.EvalDuration/int64(time.Millisecond),
		"regimen", regimen,
		"class", clase,
	)
	c.servidas.Add(1)
	return app.RespuestaInferencia{RawJSON: salida}, nil
}

// chat arma la petición al proveedor y la ejecuta. Es donde se aplican las TRES cosas que el contrato
// dice que decide el EDGE y no el Cloud: el modelo, la política de `think` y los defaults.
func (s *servidorInferencia) chat(ctx context.Context, p app.PeticionInferencia) (*ollama.ChatResponse, error) {
	c := s.c

	// EL `think` ES POLÍTICA FIJA DEL EDGE, NO UN CAMPO DEL FRAME (ADR-0045 §5). Siempre false, y
	// condicionado a que el modelo declare la capability: mandárselo a un modelo que no la tiene es un
	// error del proveedor. El porqué del false está MEDIDO: precargar sin él convirtió una inferencia de
	// 4 s en 4 MINUTOS. Por eso no se le da perilla al Cloud — su único valor no-por-defecto degrada en
	// órdenes de magnitud una máquina que no es nuestra.
	var think *bool
	if c.ollama.SupportsThinking(ctx, c.modelo) {
		no := false
		think = &no
	}

	// LAS OPCIONES DEL MODELO son las del Edge (num_thread/num_ctx/num_predict, calibradas en la O0 sobre
	// la máquina real). Se COPIA el mapa en vez de pasar el del Cajero: el cliente del proveedor lo mete
	// en el cuerpo de la petición, y compartir el mismo mapa entre inferencias concurrentes sería una
	// carrera de las que `-race` caza el día que el aforo suba de 1.
	opts := make(map[string]any, len(c.opciones)+1)
	for k, v := range c.opciones {
		opts[k] = v
	}
	// La temperatura del Cloud MANDA sobre la del Edge cuando viene; ausente ⇒ el default del Edge. El
	// puntero es lo que distingue «quiero 0» de «no dije nada» (ver app.PeticionInferencia.Temperature).
	if p.Temperature != nil {
		opts["temperature"] = float64(*p.Temperature)
	} else {
		opts["temperature"] = c.temperatura
	}

	// EL PRESUPUESTO DE SALIDA DEL CLOUD MANDA SOBRE EL DEL EDGE (T1.7-3). Mismo molde que la temperatura
	// de dos líneas arriba y por la misma razón: el Cloud es quien conoce el esquema de la respuesta que
	// espera (P1 ≈ 64, P2/P3 ≈ 512), y el Edge sólo pone el suelo cuando el Cloud calla.
	//
	// 🔴 AQUÍ NO HAY `else`, Y ESA ES LA DIFERENCIA CON LA TEMPERATURA. El default del Edge para
	// `num_predict` YA VIAJA dentro de `c.opciones` (cmd/agent/cajero.go lo pone desde cfg.Worker.NumPredict)
	// y el bucle de arriba acaba de copiarlo a `opts`. Un `else` que lo reescribiera necesitaría un segundo
	// campo del Cajero con el mismo número dentro — dos verdades sobre el mismo default esperando a
	// divergir. Con `Opciones` nil (cableado que no fija ninguna) el default es el del proveedor, que es la
	// conducta anterior a esta tarea y sigue siendo la correcta: el Edge no inventa un techo que nadie pidió.
	//
	// ⚠️ SE APLICA VERBATIM, EL CERO INCLUIDO. Para Ollama `num_predict: 0` es «no generes nada», y el
	// contrato declara el campo `optional` precisamente para que ese cero se pueda PEDIR. Recortarlo aquí
	// («0 no tiene sentido, aplico el default») sería el Edge interpretando lo que el Cloud dijo (ADR-0045
	// §1), y convertiría una petición explícita en 256 tokens de generación que nadie encargó.
	//
	// ⚠️ SE ESCRIBE EN LA COPIA, NUNCA EN `c.opciones`: el mapa se copió arriba a propósito para no
	// compartirlo entre inferencias concurrentes, y escribir en el original devolvería esa carrera Y además
	// dejaría el presupuesto de UNA petición pegado a todas las siguientes.
	if p.MaxOutputTokens != nil {
		opts["num_predict"] = int(*p.MaxOutputTokens)
	}

	// EL `format` SE NORMALIZA, NO SE INTERPRETA. Sin esto, un Cloud que mande la palabra `json` a secas
	// produce `"format":json` en el cuerpo —JSON inválido—, el proveedor responde 400 y ese 400 se
	// traduciría a OLLAMA_DOWN: culparíamos a la máquina del cliente de un error de serialización
	// nuestro. El argumento completo, en app.NormalizarFormato.
	var format []byte
	if f := app.NormalizarFormato(p.Format); f != "" {
		format = []byte(f)
	}

	return c.ollama.Chat(ctx, ollama.ChatRequest{
		// EL MODELO LO ELIGE EL EDGE y no viaja en el frame (ADR-0045): es propiedad de la máquina del
		// cliente —qué cabe en su RAM, qué tolera su CPU—. Un modelo pedido desde la nube sería una orden
		// que el Edge no siempre puede cumplir, y el fallo aparecería como lentitud, no como negativa.
		Model: c.modelo,
		// UN SOLO TURNO DE USUARIO CON EL PROMPT ENTERO. No hay mensaje de sistema: el prompt que baja del
		// Cloud YA lleva dentro sus instrucciones, sus few-shots y el texto del cliente, armados arriba
		// desde `intent_configs`. Partirlo aquí en system+user sería interpretarlo — habría que decidir
		// dónde está la costura, y esa decisión es del Cloud.
		Messages: []ollama.Message{{Role: "user", Content: p.Prompt}},
		Format:   format,
		Think:    think,
		Options:  opts,
		// 🔴 `keep_alive` VA EN EL PRIMER NIVEL DE /api/chat, NUNCA DENTRO DE `Options` (T1.7-4). Metido en
		// `options`, Ollama lo IGNORA EN SILENCIO —las claves desconocidas de `options` no dan error— y el
		// runner seguiría muriéndose sin que nada lo delatara. Por eso es un campo propio de ChatRequest
		// (wapp-edge-intent v0.3.0) y no una entrada más del mapa de arriba.
		//
		// POR QUÉ SE MANDA EN CADA PETICIÓN Y NO SE CONFIGURA LA MÁQUINA: cuando el runner de Ollama muere
		// por silencio no se lleva sólo el modelo, se lleva LA CACHÉ DE PREFIJOS con él, y el siguiente
		// mensaje paga carga del modelo (39 s MEDIDOS el 2026-08-23) más el prefill en frío del prompt
		// entero. En el VPS de UAT eso lo tapa hoy `OLLAMA_KEEP_ALIVE=-1` en el env de la unidad, pero eso
		// es una propiedad de ESA máquina: en el equipo de un cliente no hay quien la ponga, y este campo sí
		// viaja con cada petición.
		KeepAlive: c.keepAlive,
	})
}

// registrarFalloDeInferencia traduce un fallo del proveedor al error canónico que le corresponde, lo
// cuenta (INV-051.3: por separado, nunca agregados) y lo registra en el breaker.
//
// 🔴 AQUÍ VIVE LA MITAD «CON EL MODELO TRABAJANDO» DE LA FRONTERA. La otra mitad la resuelve el aforo
// (ver Aforo.TomarHasta): si el plazo venció ESPERANDO PLAZA es EDGE_SIN_CAPACIDAD y esta función ni
// siquiera se llama, porque nunca se llegó al modelo. Todo lo que llega hasta aquí ya pagó una llamada.
func (s *servidorInferencia) registrarFalloDeInferencia(
	ctxProceso, ctxInferencia context.Context, p app.PeticionInferencia, err error,
	transcurrido time.Duration, eraSondeo bool,
) error {
	c := s.c

	// ─── EL ABORTO SE COMPRUEBA ANTES DE REGISTRAR NADA ──────────────────────
	//
	// 🔴 Y AQUÍ NO SE PUEDE SABER QUIÉN ABORTÓ, QUE ES EL DATO QUE GOBIERNA TODO LO DEMÁS. El ctx de la
	// petición se cancela por DOS causas indistinguibles desde dentro:
	//
	//   - el SIGTERM del proceso (el servidor del socket instala el ctx del proceso como BaseContext), o
	//   - el CLIENTE que cerró su conexión (el daemon se rindió, CloudLink se reconectó, el plazo venció
	//     arriba). El proceso sigue perfectamente vivo.
	//
	// El bucle de clasificación tenía una excepción aquí —salir sin registrar nada en el breaker— y su
	// justificación era literal: «sólo es segura porque el proceso ENTERO se está yendo». Esa
	// justificación NO SOBREVIVE al socket, porque el segundo caso no mata a nadie: el flag de sondeo se
	// quedaría reservado en un proceso que va a seguir funcionando horas, y el circuito no volvería a
	// dejar pasar una sola inferencia.
	//
	// LA REGLA ES LA MISMA QUE EN `cerrarSinIntentar`, Y TIENE QUE SERLO: la situación es idéntica
	// (permiso concedido, resultado no obtenido), así que dos guardas distintas serían dos criterios
	// distintos para el mismo hecho. Se registra fallo SÓLO si el intento era el SONDEO del medio-abierto
	// —lo que hay que evitar es dejarlo reservado—, y con el circuito cerrado no se registra nada: un
	// aborto del cliente no es evidencia de que el proveedor esté enfermo, y contarlo haría que cinco
	// reconexiones de CloudLink abrieran un circuito que protege a un Ollama sano.
	if ctxProceso.Err() != nil {
		// 🔴 SE CUENTA, aunque no suba por el cable. INV-051.3 pide que los desenlaces no se agreguen, y
		// éste NO ES NINGUNO DE LOS CINCO del contrato: no hay error que responder porque no hay nadie
		// escuchando. Sin este contador, un daemon que abortase sistemáticamente sus peticiones —un plazo
		// mal calculado arriba, un stream que se cae en bucle— quemaría el LLM de la máquina del cliente
		// sin dejar UNA SOLA huella en la telemetría: ni `servidas`, ni ninguno de los cuatro errores.
		n := c.abortadas.Add(1)
		s.cerrarSinIntentar(eraSondeo)
		c.log.Info("cajero: inferencia ABORTADA por quien la pidió (apagado del proceso o cliente que colgó); "+
			"no hay a quién responder y el proveedor no tiene la culpa",
			"command_id", p.CommandID, "latencia_ms", transcurrido.Milliseconds(),
			"era_sondeo", eraSondeo, "abortadas", n)
		return app.ErrInferenciaTimeout
	}

	// ¿VENCIÓ EL PLAZO O SE CAYÓ EL PROVEEDOR? La pregunta se le hace AL CONTEXTO DE LA INFERENCIA, no al
	// error: el cliente del proveedor envuelve el `context.DeadlineExceeded` dentro de su propio «ollama
	// no responde en …», así que mirar el texto —o incluso `errors.Is(err, context.DeadlineExceeded)`
	// sobre un error de red que ya se tragó la causa— clasificaría un timeout como caída. El contexto sí
	// sabe la verdad, y la sabe sin ambigüedad.
	canonico := app.ErrInferenciaOllamaCaido
	motivo := "el proveedor local no respondió"
	if errors.Is(ctxInferencia.Err(), context.DeadlineExceeded) {
		canonico = app.ErrInferenciaTimeout
		motivo = "se agotó el plazo con el proveedor trabajando"
	}

	if canonico == app.ErrInferenciaTimeout {
		c.errTimeout.Add(1)
	} else {
		c.errOllamaCaido.Add(1)
	}

	// ─── EL TIMEOUT TAMBIÉN ES EVIDENCIA DE PREFIJO FRÍO (DEUDA-044.10, 2.ª PASADA) ─────────────────────
	//
	// 🔴 LA 1.ª PASADA COLGÓ EL AVISO DEL CAMINO FELIZ, Y POR ESO NO SE DISPARÓ NUNCA. Se desplegó el
	// 2026-08-25 mirando sólo el `regimen` de una inferencia SERVIDA (ver ⑨bis en Inferir), y en campo dio
	// CERO avisos en su propio escenario canónico. La secuencia medida, con el reloj del VPS:
	//
	//   23:01:26  reinicio de Ollama ⇒ prefijo frío (el modelo SÍ está cargado: `ollama ps` = Forever)
	//   23:02:57  1.ª clasificación real: MUERE POR TIMEOUT a los 37.993 ms  ⇒ no emite muestra de régimen
	//             🔑 y ese timeout CALIENTA el prefijo: 37.993 ms → 1.499 ms en la siguiente
	//   23:03:12  2.ª clasificación: SERVIDA con `regimen=caliente`          ⇒ ya no hay nada que avisar
	//
	// ⇒ **la ventana en la que aquel aviso podía dispararse es precisamente la que no ocurre**. Y la frase
	// que lo predecía estaba escrita en el propio plan (T1.7-5): «una inferencia que muere por timeout no
	// emite muestra de régimen». El fallo se auto-reparaba BORRANDO SU PROPIA EVIDENCIA antes de que nadie
	// la leyera, así que medir después siempre daba «no pasó nada».
	//
	// POR QUÉ EL TIMEOUT ES BUENA EVIDENCIA, y no un parche: aquí ya se ha distinguido —preguntándole al
	// CONTEXTO, no al texto del error— entre «el proveedor no respondió» (ollama_down) y «se agotó el plazo
	// CON EL PROVEEDOR TRABAJANDO». Lo segundo, en este sistema, es el prefill frío: el prefijo cuesta
	// ~21,6 ms/token en frío y ~1,5 s entero en caliente, así que un plazo agotado con Ollama vivo y el
	// modelo cargado no tiene otra causa habitual. El `ollama_down` NO entra aquí a propósito: ése ya lo
	// cubre la readiness de T1.8-5, que baja a DOWN y produce su propia transición al volver.
	//
	// ⚠️ EL ABORTO NO LLEGA HASTA AQUÍ, Y ES DELIBERADO. La rama `ctxProceso.Err() != nil` de arriba también
	// devuelve `ErrInferenciaTimeout` —un cliente que colgó, o el apagado del proceso— pero RETORNA ANTES.
	// Si esta comprobación se moviera por encima de aquélla, cada reconexión de CloudLink pediría un
	// calentamiento sin que nadie hubiera tocado a Ollama.
	//
	// 🔴 Y EL RIESGO QUE SE ACEPTA, dicho y no escondido: un timeout puede venir de una máquina saturada y
	// no de un prefijo frío, y entonces este aviso añade UNA inferencia a una máquina que ya va justa. Lo
	// acota el `CompareAndSwap` de `avisarPrefijoFrio`: mientras no se observe una inferencia CALIENTE no
	// se pide un segundo recalentamiento, así que el peor caso de un episodio de saturación —donde nada
	// sale caliente— es exactamente **una** inferencia de más, no una por fallo.
	//
	// El calentamiento se excluye por lo mismo que en ⑨bis: es él quien paga el prefill, y sin la guarda
	// un calentamiento que venciera pediría el siguiente.
	if canonico == app.ErrInferenciaTimeout && !p.Calentamiento {
		c.avisarPrefijoFrio(ctxProceso, CausaFrioTimeout)
	}
	// LA MISMA COSTURA QUE EN EL LADO ÉXITO, y por el mismo motivo (ver `cuentaParaElBreaker` en Inferir):
	// un calentamiento que falla no es evidencia contra el proveedor para las peticiones de nadie. El
	// CONTADOR DEL DESENLACE de arriba (err_timeout / err_ollama_caido) sí se toca —el fallo ocurrió y tiene
	// que verse en la telemetría—; lo que no se toca es el circuito. `fallos` tampoco sube, y es coherente:
	// vive DENTRO de registrarFallo porque cuenta la misma población que el breaker juzga.
	// `eraSondeo` llega en false por construcción cuando la petición no cuenta (nunca se pidió permiso), así
	// que las dos guardas de `cerrarSinIntentar` de este camino ya están cubiertas sin repetir la condición.
	if !p.Calentamiento {
		c.registrarFallo()
	}

	// 🔴 INV-051.1: del error se dice que existe (va en `error`, que es lo que ya se loguea en el resto
	// del paquete), jamás el prompt que lo provocó.
	c.log.Warn("cajero: la inferencia FALLÓ; el Cloud recibe el error nombrado y degrada",
		"command_id", p.CommandID, "codigo", canonico.Codigo(), "motivo", motivo, "error", err,
		"latencia_ms", transcurrido.Milliseconds(), "prompt_bytes", len(p.Prompt),
		"circuito", c.breaker.State())
	return canonico
}

// cerrarSinIntentar salda el COMPROMISO DEL BREAKER cuando se recibió permiso para intentar y al final
// no se llamó al proveedor.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 POR QUÉ HACE FALTA UNA REGLA NUEVA AQUÍ
// ─────────────────────────────────────────────────────────────────────────────
// `BeginAttempt()==true` OBLIGA a llamar después a RecordSuccess o RecordFailure. Si no, y el permiso
// era el sondeo del medio-abierto, el flag `probing` queda RESERVADO y el circuito no vuelve a dejar
// pasar nada nunca más — el proveedor puede haber vuelto y el Edge seguirá respondiendo BREAKER_OPEN
// para siempre.
//
// El bucle de clasificación tenía UNA excepción a esa obligación (ctx cancelado ⇒ return sin registrar),
// y su justificación era literal: «sólo es segura porque el proceso ENTERO se está yendo; el breaker
// muere con él y nace limpio en el siguiente arranque». BAJO EL SOCKET ESA JUSTIFICACIÓN SE CAE: un
// cliente que aborta su petición —el daemon que se rinde, una reconexión de CloudLink, un plazo que
// vence arriba— NO mata al cajero. El proceso sigue vivo, y con él el flag reservado.
//
// LA REGLA: si el intento era el SONDEO del medio-abierto y se abandona sin resolverlo, se registra
// FALLO. Es lo conservador —reabre la ventana 60 s en vez de dejar el circuito mudo— y es honesto: un
// sondeo que no se llegó a hacer no es evidencia de que el proveedor haya vuelto.
//
// Si el circuito estaba CERRADO no se registra nada, y eso también es deliberado: `BeginAttempt` con el
// circuito cerrado no reserva ningún estado (mira `failures` y devuelve true), así que no hay nada que
// saldar. Registrar un fallo ahí castigaría al proveedor por una saturación de NUESTRA máquina — cinco
// rechazos por capacidad seguidos abrirían un circuito que protege a un Ollama perfectamente sano.
func (s *servidorInferencia) cerrarSinIntentar(eraSondeo bool) {
	if !eraSondeo {
		return
	}
	s.c.log.Warn("cajero: se abandonó el SONDEO del medio-abierto sin llegar a intentar; se registra como fallo " +
		"para no dejar el circuito con el sondeo reservado (ver Breaker.BeginAttempt)")
	s.c.registrarFallo()
}

// plazoDe resuelve el plazo efectivo de UNA inferencia a partir del que pidió el Cloud.
//
// Las dos correcciones son de sentido opuesto y las dos son necesarias:
//
//   - `<= 0` significa «el Cloud no lo fijó» (contrato: 0 = default del Edge) ⇒ se aplica el default.
//     Un 0 tomado al pie de la letra sería un plazo nulo y TODA inferencia moriría al instante.
//   - Por encima del techo se RECORTA y se AVISA. No se rechaza la petición: el Cloud pidió más tiempo
//     del que la máquina puede regalar, y servirle en menos tiempo es mejor respuesta que no servirle.
//     El aviso existe porque un recorte silencioso haría que el Cloud creyera tener un presupuesto que
//     no tiene y leyera los TIMEOUT resultantes como lentitud del modelo.
func (c *Cajero) plazoDe(pedido time.Duration) time.Duration {
	if pedido <= 0 {
		return c.timeout
	}
	if c.timeoutMax > 0 && pedido > c.timeoutMax {
		c.log.Warn("cajero: el plazo pedido por el Cloud supera el techo del Edge; se RECORTA",
			"pedido_ms", pedido.Milliseconds(), "techo_ms", c.timeoutMax.Milliseconds())
		return c.timeoutMax
	}
	return pedido
}
