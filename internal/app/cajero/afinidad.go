package cajero

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ─────────────────────────────────────────────────────────────────────────────
// T2.8 · el REPARTO de CPUs entre Ollama y el cajero (lógica pura, sin /proc)
// ─────────────────────────────────────────────────────────────────────────────
//
// Este fichero NO tiene build tag a propósito. El dato de entrada sólo se puede LEER en Linux
// (`sched_getaffinity(2)` vía /proc, ver taskset_linux.go), pero decidir si dos repartos se pisan es
// aritmética de conjuntos: sacarla aquí la hace testeable en el macOS donde se corren los gates, y de
// paso separa «cómo se averigua» de «qué significa», que son dos cosas que fallan por motivos distintos.
//
// 🔴 POR QUÉ NO BASTA CON MIRAR A OLLAMA. La primera versión de T2.8 comprobaba SÓLO a qué CPUs estaba
// confinado Ollama, y la medición contra el Ollama real del VPS (6 vCPU, lotes reales, num_thread=5)
// demostró que eso da falsa tranquilidad. Dándole al vecino EXACTAMENTE las mismas 2 vCPU:
//
//	escenario                                          reparto   lotes por encima de los 15 s
//	── una sola instancia compartida ──────────────────   —       100 %  (p50 47.991 ms · 11,2×)
//	── vecino confinado a 2 vCPU, cajero LIBRE en las 6   parcial  17,2 %
//	── vecino en 4-5, cajero confinado a 0-3 ──────────   disjunto   0 %  (máximo 12.098 ms)
//	── vecino aislado 5/1 ─────────────────────────────   disjunto   0 %  (máximo 11.670 ms)
//
// Las dos filas del medio son el hallazgo: mismo vecino, mismas 2 vCPU para él, y 17,2 % contra 0 %. La
// conclusión medida es que AISLAR SÓLO AL VECINO NO SIRVE — hay que aislar a los dos y que los conjuntos
// sean DISJUNTOS. Dejar al cajero «libre» en las 6 vCPU no le regala potencia, le regala interferencia:
// se pisa con el vecino justo en las CPU que ese ya está ocupando.
//
// Cuánto duele el vecino, medido: 1 vCPU de 6 → 0 % · 2 → 17,2 % · 3 → 79,3 % · sin límite y
// compartiendo instancia → 100 %.

// lecturaAfinidad es lo que el arranque consigue AVERIGUAR del reparto: las dos afinidades y el censo de
// la máquina. Los errores viajan DENTRO de la struct, uno por lado, y no como un error de retorno, por
// el requisito no-fatal de T2.8: que no se pueda leer la afinidad de Ollama —el caso típico, porque
// corre con otro usuario— no debe impedir aprovechar la del cajero, que siempre es legible (es la de
// este mismo proceso). Cada campo se usa sólo si su error es nil.
type lecturaAfinidad struct {
	// Ollama es el `Cpus_allowed_list` del proceso de Ollama; "" si ErrOllama != nil.
	Ollama    string
	ErrOllama error
	// Cajero es el `Cpus_allowed_list` de ESTE proceso; "" si ErrCajero != nil.
	Cajero    string
	ErrCajero error
	// Presentes es el censo de CPUs de la máquina. Es accesorio (ver clasificarReparto) y por eso no
	// tiene error propio: si no se pudo leer, viene vacío.
	Presentes string
}

// maxCPUsPlausibles es el tope de índice de CPU que se acepta al parsear una lista del kernel. Existe
// como CORTAFUEGOS de memoria: un rango como "0-999999999" en una lectura corrupta materializaría mil
// millones de entradas en el conjunto antes de que nadie pudiera opinar. 8192 es el CONFIG_NR_CPUS más
// alto que se ve en kernels reales, así que ninguna máquina legítima roza este techo.
const maxCPUsPlausibles = 8192

// conjuntoCPU es un conjunto de índices de CPU. Se modela como conjunto y no como la cadena del kernel
// porque la pregunta que importa —¿se pisan?— es una intersección, y "0-3" contra "2,4" no se puede
// responder comparando texto.
type conjuntoCPU map[int]struct{}

// parsearListaCPUs traduce el formato `Cpus_allowed_list` del kernel a un conjunto. El formato es una
// lista de elementos separados por comas, donde cada elemento es un índice ("3") o un rango cerrado
// ("0-3"): "0-3", "2,4", "0-1,6", "3". Es el mismo texto que enseña `taskset -pc <pid>`.
//
// Se RECHAZA lo que no encaje, incluida la cadena vacía: el kernel nunca emite un Cpus_allowed_list
// vacío (todo proceso vivo corre en alguna CPU), así que una cadena vacía significa que la lectura salió
// mal —no que el proceso no pueda usar ninguna CPU— y devolver un conjunto vacío haría que la
// comparación dijera «disjuntos» tan campante. Ese es exactamente el veredicto tranquilizador que este
// fichero existe para no dar por error.
func parsearListaCPUs(lista string) (conjuntoCPU, error) {
	lista = strings.TrimSpace(lista)
	if lista == "" {
		return nil, fmt.Errorf("lista de CPUs vacía")
	}
	conjunto := make(conjuntoCPU)
	for _, elemento := range strings.Split(lista, ",") {
		elemento = strings.TrimSpace(elemento)
		if elemento == "" {
			return nil, fmt.Errorf("lista de CPUs %q: elemento vacío", lista)
		}
		desde, hasta, esRango := strings.Cut(elemento, "-")
		if !esRango {
			hasta = desde
		}
		n0, err := indiceCPU(desde)
		if err != nil {
			return nil, fmt.Errorf("lista de CPUs %q: %w", lista, err)
		}
		n1, err := indiceCPU(hasta)
		if err != nil {
			return nil, fmt.Errorf("lista de CPUs %q: %w", lista, err)
		}
		if n1 < n0 {
			return nil, fmt.Errorf("lista de CPUs %q: el rango %q va hacia atrás", lista, elemento)
		}
		for n := n0; n <= n1; n++ {
			conjunto[n] = struct{}{}
		}
	}
	return conjunto, nil
}

// indiceCPU valida un índice suelto. Rechaza el signo, el vacío y lo que pase de maxCPUsPlausibles.
func indiceCPU(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("índice de CPU vacío")
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("índice de CPU ilegible %q", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("índice de CPU negativo %q", s)
	}
	if n > maxCPUsPlausibles {
		return 0, fmt.Errorf("índice de CPU %d fuera de rango (tope %d)", n, maxCPUsPlausibles)
	}
	return n, nil
}

// interseccion devuelve las CPUs que están en los dos conjuntos: las que los dos procesos se disputan.
func (c conjuntoCPU) interseccion(otro conjuntoCPU) conjuntoCPU {
	comunes := make(conjuntoCPU)
	// Se recorre el más pequeño: el coste es el del menor de los dos, no el del mayor.
	menor, mayor := c, otro
	if len(mayor) < len(menor) {
		menor, mayor = mayor, menor
	}
	for cpu := range menor {
		if _, ok := mayor[cpu]; ok {
			comunes[cpu] = struct{}{}
		}
	}
	return comunes
}

// contieneTodas dice si este conjunto cubre entero al otro.
func (c conjuntoCPU) contieneTodas(otro conjuntoCPU) bool {
	for cpu := range otro {
		if _, ok := c[cpu]; !ok {
			return false
		}
	}
	return true
}

// String devuelve la lista ORDENADA y separada por comas ("0,1,2,3"). No se reconstruyen rangos: el
// destino es una línea de log que un operador compara con lo que él escribió en el `taskset`, y ahí una
// lista explícita se lee sin traducir nada.
func (c conjuntoCPU) String() string {
	if len(c) == 0 {
		return ""
	}
	cpus := make([]int, 0, len(c))
	for cpu := range c {
		cpus = append(cpus, cpu)
	}
	slices.Sort(cpus)
	partes := make([]string, 0, len(cpus))
	for _, cpu := range cpus {
		partes = append(partes, strconv.Itoa(cpu))
	}
	return strings.Join(partes, ",")
}

// veredictoAfinidad es el diagnóstico del reparto. Son los tres casos que la medición separa, y cada uno
// lleva una acción distinta para el operador.
type veredictoAfinidad string

const (
	// afinidadDisjunta: los dos conjuntos no comparten ni una CPU. Es la configuración buena (0 % de
	// lotes por encima del timeout en la medición).
	afinidadDisjunta veredictoAfinidad = "disjunta"
	// afinidadSolapada: comparten al menos una CPU. Aquí vive el 17,2 %.
	afinidadSolapada veredictoAfinidad = "solapada"
	// afinidadCajeroSinConfinar: el cajero puede subirse a TODAS las CPUs de la máquina, o sea que nadie
	// le puso `taskset`. Es un solapamiento, pero se distingue porque el arreglo es distinto: no hay que
	// mover al vecino, hay que confinar a este proceso. Es el escenario G2 exacto de la medición.
	afinidadCajeroSinConfinar veredictoAfinidad = "cajero_sin_confinar"
)

// repartoCPU es el resultado de comparar las dos afinidades: el veredicto y los conjuntos que lo
// sostienen, para que la línea de log lleve el dato y no sólo la conclusión.
type repartoCPU struct {
	Veredicto veredictoAfinidad
	Ollama    conjuntoCPU
	Cajero    conjuntoCPU
	// Comunes son las CPUs que los dos se disputan. Vacío si y sólo si el veredicto es disjunto.
	Comunes conjuntoCPU
}

// clasificarReparto compara las dos listas del kernel y dice cuál de los tres casos es.
//
// cpusPresentes es el censo de CPUs de la MÁQUINA (/sys/devices/system/cpu/present) y sirve para una
// sola cosa: distinguir «el cajero está confinado a un trozo que resulta ser todo» de «al cajero no le
// pusieron taskset». Puede venir vacío —no todo sistema lo expone— y entonces ese caso simplemente no se
// separa: se cae en `afinidadSolapada`, que lleva el mismo Warn y la misma receta. Se pierde precisión
// en el mensaje, nunca la detección.
//
// 🔴 EL ORDEN DE LAS COMPROBACIONES IMPORTA. «Sin confinar» se mira ANTES que la intersección porque un
// cajero suelto en toda la máquina siempre solapa con Ollama: si se preguntara primero por el
// solapamiento, este caso nunca se distinguiría y el operador recibiría el consejo equivocado (mover al
// vecino, que ya estaba en su sitio).
func clasificarReparto(cpusOllama, cpusCajero, cpusPresentes string) (repartoCPU, error) {
	ollama, err := parsearListaCPUs(cpusOllama)
	if err != nil {
		return repartoCPU{}, fmt.Errorf("afinidad de Ollama: %w", err)
	}
	cajero, err := parsearListaCPUs(cpusCajero)
	if err != nil {
		return repartoCPU{}, fmt.Errorf("afinidad del cajero: %w", err)
	}

	reparto := repartoCPU{
		Ollama:  ollama,
		Cajero:  cajero,
		Comunes: cajero.interseccion(ollama),
	}

	// El censo de la máquina es OPCIONAL: si no se pudo leer o viene ilegible, se ignora en silencio y la
	// comparación sigue. Devolver error aquí convertiría un dato accesorio en la causa de que no se
	// comprobara nada.
	if presentes, err := parsearListaCPUs(cpusPresentes); err == nil && cajero.contieneTodas(presentes) {
		reparto.Veredicto = afinidadCajeroSinConfinar
		return reparto, nil
	}
	if len(reparto.Comunes) == 0 {
		reparto.Veredicto = afinidadDisjunta
		return reparto, nil
	}
	reparto.Veredicto = afinidadSolapada
	return reparto, nil
}

// recetaSolapamiento es lo que el log le dice al operador cuando los dos procesos se pisan. Es el
// hallazgo de la medición en una frase, y está aquí —y no incrustado en el logger— para que el test lo
// pueda comprobar y para que los dos veredictos malos digan exactamente lo mismo.
const recetaSolapamiento = "aislar SÓLO a Ollama no basta: con el vecino confinado a 2 vCPU pero el cajero suelto en las 6, " +
	"el 17,2 % de las clasificaciones pasó del timeout; con los dos conjuntos DISJUNTOS, el 0 %. " +
	"Confina también a este proceso (taskset -c) a CPUs que Ollama no use"
