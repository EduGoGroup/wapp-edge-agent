//go:build linux

package cajero

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// leerAfinidades averigua LAS DOS afinidades de CPU que T2.8 (REQ-051.16) necesita comparar —la del
// proceso de Ollama y la de ESTE proceso— más el censo de CPUs de la máquina. Los valores vienen en el
// formato `Cpus_allowed_list` del kernel: "0-3", "2,4", "0-1,6"… Quién decide qué significan es
// clasificarReparto (afinidad.go); aquí sólo se leen.
//
// POR QUÉ ESTA COMPROBACIÓN EXISTE, y por qué mira a los dos lados. El aislamiento por CPU es LA palanca
// de rendimiento de esta pieza, y la medición contra el Ollama real del VPS (6 vCPU, LOTES reales,
// num_thread=5) puso número a las dos mitades del asunto:
//
//   - CUÁNTO CUESTA OLVIDAR EL `taskset`: con una sola instancia compartida, el p50 se va a 47.991 ms
//     —11,2× de degradación— y el 100 % de las clasificaciones pasa del timeout de 15 s. El 100 %, y en
//     SILENCIO: el clasificador que se rinde devuelve «desconocido» y el flujo sigue por el camino de
//     siempre (ADR-0020 §3), así que no hay error, ni alarma, ni una sola línea roja. Simplemente deja
//     de haber clasificación. La cifra vieja de este comentario (6,3× y p50 21,8 s) es de la O0 y se
//     midió con MENSAJES SUELTOS; con lotes reales duele el doble.
//   - AISLAR SÓLO A OLLAMA NO BASTA: dándole al vecino las mismas 2 vCPU, dejar al cajero suelto en las 6
//     deja el 17,2 % de los lotes por encima del timeout, mientras que confinarlo a CPUs disjuntas lo
//     deja en 0 %. La tabla completa está en el encabezado de afinidad.go.
//
// El problema operativo es que un reinicio manual o una unidad systemd editada destruyen ese
// aislamiento SIN QUE NADA AVISE. Esto es el aviso.
//
// LA AFINIDAD DEL PROPIO CAJERO es la barata de las dos: /proc/self/status siempre es legible por su
// dueño, así que este lado no falla nunca en la práctica. La cara es la de Ollama, y de ahí el rodeo que
// sigue.
//
// CÓMO SE IDENTIFICA EL PID DE OLLAMA, y qué falla si no se encuentra. No hay API de Ollama que lo
// diga, así que se deduce del sistema de ficheros, sin cgo y sin ejecutar binarios externos:
//
//  1. CAMINO PRINCIPAL — por el socket que estamos usando. De la URL se saca el puerto (11434 por
//     defecto); en /proc/net/tcp y /proc/net/tcp6 se busca el socket en estado LISTEN de ese puerto y
//     se lee su INODO; luego se recorre /proc/<pid>/fd/* resolviendo los enlaces hasta encontrar el
//     que apunta a "socket:[<inodo>]". Ese es, por construcción, el proceso que atiende exactamente la
//     URL contra la que este worker infiere — no «un proceso que se llama ollama».
//     ⚠️ Falla con EACCES si Ollama corre con OTRO usuario (el caso típico de la unidad systemd, que
//     usa el usuario `ollama`): /proc/<pid>/fd sólo lo puede listar el dueño del proceso o root.
//
//  2. FALLBACK — por nombre de proceso. Se recorren los /proc/<pid>/comm buscando "ollama".
//     /proc/<pid>/comm y /proc/<pid>/status SÍ son legibles por cualquier usuario, así que este camino
//     sí funciona con Ollama corriendo como otro usuario. Es menos exacto: si hubiera dos instancias
//     (justo el escenario de las dos instancias aisladas por `taskset`), devuelve la de menor PID y no
//     hay forma de saber cuál atiende nuestra URL. Por eso es el fallback y no el camino principal.
//
// SI NINGUNO DE LOS DOS ENCUENTRA EL PROCESO se devuelve error EN ErrOllama y el llamante lo registra
// como Warn: el cajero arranca IGUAL. Los casos en que eso pasa son (a) Ollama no está corriendo todavía
// —el worker puede arrancar antes que él—, (b) Ollama es remoto o vive en otro namespace de red/PID
// (contenedor), y (c) /proc está restringido (hidepid=2). En ninguno de los tres tiene sentido impedir
// el arranque: lo que se pierde es una comprobación de rendimiento, no la corrección.
func leerAfinidades(ctx context.Context, urlOllama string) lecturaAfinidad {
	var lec lecturaAfinidad
	if err := ctx.Err(); err != nil {
		lec.ErrOllama, lec.ErrCajero = err, err
		return lec
	}

	lec.Cajero, lec.ErrCajero = cpusPermitidasEn(rutaStatusPropio)
	lec.Presentes = cpusPresentes()
	lec.Ollama, lec.ErrOllama = afinidadOllama(urlOllama)
	return lec
}

// afinidadOllama es el rodeo del doc comment de arriba: puerto → inodo → PID → Cpus_allowed_list.
func afinidadOllama(urlOllama string) (string, error) {
	puerto, err := puertoDe(urlOllama)
	if err != nil {
		return "", err
	}

	pid, err := pidPorPuerto(puerto)
	if err != nil {
		fallbackPID, errNombre := pidPorNombre("ollama")
		if errNombre != nil {
			return "", fmt.Errorf("no se pudo identificar el proceso de Ollama (por socket: %v; por nombre: %v)", err, errNombre)
		}
		pid = fallbackPID
	}

	return cpusPermitidas(pid)
}

// rutaStatusPropio es el /proc/<pid>/status de ESTE proceso. Se usa la vista mágica `self` en vez de
// os.Getpid() a propósito: `self` la resuelve el kernel por el hilo que lee, así que no hay ventana en la
// que el PID que se compone aquí deje de ser el nuestro.
const rutaStatusPropio = "/proc/self/status"

// rutaCPUsPresentes es el censo de CPUs de la MÁQUINA, en el mismo formato de lista que
// Cpus_allowed_list. Es el único modo de distinguir «el cajero está confinado a un trozo que resulta ser
// todo» de «al cajero no le pusieron taskset»: runtime.NumCPU() NO sirve para esto, porque Go lo calcula
// respetando la afinidad del proceso y por tanto devuelve 4 en un proceso confinado a 4 de 6 vCPU —
// justo el dato que aquí haría falta que no estuviera ya filtrado.
const rutaCPUsPresentes = "/sys/devices/system/cpu/present"

// cpusPresentes lee el censo de CPUs de la máquina. Devuelve "" si no se puede: es un dato ACCESORIO
// (afina el mensaje, no la detección) y no merece un error que el llamante tenga que gestionar.
func cpusPresentes() string {
	b, err := os.ReadFile(rutaCPUsPresentes)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// puertoDe extrae el puerto de la URL de Ollama y comprueba que apunta a esta misma máquina: si el
// Ollama es remoto, su afinidad de CPU no es observable desde aquí y decirlo es más útil que devolver
// un dato inventado.
func puertoDe(urlOllama string) (uint64, error) {
	if urlOllama == "" {
		return 0, fmt.Errorf("no hay URL de Ollama configurada")
	}
	u, err := url.Parse(urlOllama)
	if err != nil {
		return 0, fmt.Errorf("URL de Ollama ilegible %q: %w", urlOllama, err)
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "11434" // el puerto por defecto de Ollama
	}
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return 0, fmt.Errorf("el Ollama de %q no es local: su afinidad de CPU no se puede observar desde este proceso", urlOllama)
		}
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("puerto de Ollama ilegible %q: %w", port, err)
	}
	return n, nil
}

// estadoListen es el valor de la columna `st` de /proc/net/tcp para TCP_LISTEN (0x0A).
const estadoListen = "0A"

// pidPorPuerto resuelve el PID que ESCUCHA en el puerto dado, vía el inodo de su socket.
func pidPorPuerto(puerto uint64) (int, error) {
	inodo, err := inodoDelListener(puerto)
	if err != nil {
		return 0, err
	}
	return pidPorInodo(inodo)
}

// inodoDelListener busca en /proc/net/tcp y /proc/net/tcp6 el socket en LISTEN del puerto y devuelve
// su inodo. El formato de esas tablas es estable desde hace décadas: columnas separadas por espacios,
// con local_address="<IP hex>:<puerto hex>" en la 1, el estado en la 3 y el inodo en la 9.
//
// SE COMPRUEBA sc.Err(), y aquí importa más que en un fichero normal: /proc/net/tcp se genera al
// vuelo por el kernel y una lectura puede cortarse (línea más larga que el buffer del Scanner con
// muchísimos sockets, o un error de E/S del pseudo-fichero). Sin comprobarlo, un recorrido TRUNCADO se
// confunde con «no hay ningún listener»: el mensaje diría que Ollama no está corriendo cuando lo que
// pasó es que no se llegó a mirar entero, y el operador iría a arreglar el servicio equivocado. El
// fallo de lectura se ACUMULA y viaja en el mensaje final; sigue siendo NO FATAL, como todo T2.8.
func inodoDelListener(puerto uint64) (string, error) {
	var truncadas []string
	for _, tabla := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		f, err := os.Open(tabla)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		primera := true
		for sc.Scan() {
			if primera { // cabecera
				primera = false
				continue
			}
			campos := strings.Fields(sc.Text())
			if len(campos) < 10 || campos[3] != estadoListen {
				continue
			}
			local := campos[1]
			i := strings.LastIndex(local, ":")
			if i < 0 {
				continue
			}
			p, err := strconv.ParseUint(local[i+1:], 16, 32)
			if err != nil || p != puerto {
				continue
			}
			inodo := campos[9]
			_ = f.Close()
			return inodo, nil
		}
		if err := sc.Err(); err != nil {
			truncadas = append(truncadas, fmt.Sprintf("%s: %v", tabla, err))
		}
		_ = f.Close()
	}
	if len(truncadas) > 0 {
		return "", fmt.Errorf("no se encontró un socket en LISTEN sobre el puerto %d, pero la lectura de /proc quedó INCOMPLETA "+
			"(así que puede haberlo y no haberse visto): %s", puerto, strings.Join(truncadas, "; "))
	}
	return "", fmt.Errorf("no hay ningún socket en LISTEN sobre el puerto %d (¿Ollama no está corriendo?)", puerto)
}

// pidPorInodo recorre /proc/<pid>/fd/* buscando el descriptor que apunta a socket:[<inodo>].
func pidPorInodo(inodo string) (int, error) {
	objetivo := "socket:[" + inodo + "]"
	entradas, err := os.ReadDir("/proc")
	if err != nil {
		return 0, fmt.Errorf("no se pudo listar /proc: %w", err)
	}
	denegados := 0
	for _, e := range entradas {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue // no es un directorio de proceso
		}
		fds, err := os.ReadDir(filepath.Join("/proc", e.Name(), "fd"))
		if err != nil {
			if os.IsPermission(err) {
				denegados++
			}
			continue
		}
		for _, fd := range fds {
			destino, err := os.Readlink(filepath.Join("/proc", e.Name(), "fd", fd.Name()))
			if err != nil {
				continue
			}
			if destino == objetivo {
				return pid, nil
			}
		}
	}
	if denegados > 0 {
		return 0, fmt.Errorf("el socket del inodo %s pertenece a un proceso de otro usuario (%d /proc/<pid>/fd sin permiso)", inodo, denegados)
	}
	return 0, fmt.Errorf("ningún proceso visible tiene abierto el socket del inodo %s", inodo)
}

// pidPorNombre devuelve el PID MÁS BAJO cuyo /proc/<pid>/comm coincide con nombre. Es el fallback: ver
// la advertencia sobre las dos instancias en el doc comment de afinidadOllama.
func pidPorNombre(nombre string) (int, error) {
	entradas, err := os.ReadDir("/proc")
	if err != nil {
		return 0, fmt.Errorf("no se pudo listar /proc: %w", err)
	}
	mejor := 0
	for _, e := range entradas {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		comm, err := os.ReadFile(filepath.Join("/proc", e.Name(), "comm"))
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(comm)) != nombre {
			continue
		}
		if mejor == 0 || pid < mejor {
			mejor = pid
		}
	}
	if mejor == 0 {
		return 0, fmt.Errorf("ningún proceso se llama %q", nombre)
	}
	return mejor, nil
}

// cpusPermitidas lee la afinidad del PID dado. Ver cpusPermitidasEn para el porqué del formato.
func cpusPermitidas(pid int) (string, error) {
	return cpusPermitidasEn(filepath.Join("/proc", strconv.Itoa(pid), "status"))
}

// cpusPermitidasEn lee la línea `Cpus_allowed_list` de un /proc/<pid>/status, que es la proyección
// legible de sched_getaffinity(2) — el mismo dato que consulta `taskset -pc <pid>`, sin cgo y sin
// ejecutar nada. /proc/<pid>/status es legible por cualquier usuario, así que este paso funciona aunque
// Ollama corra con otro uid.
//
// TOMA LA RUTA Y NO EL PID para que el propio proceso se lea por /proc/self/status con ESTE MISMO
// parser: el campo es idéntico en los dos casos y tener dos lectores del mismo formato es la forma de
// que uno de ellos se quede atrás.
func cpusPermitidasEn(ruta string) (string, error) {
	f, err := os.Open(ruta)
	if err != nil {
		return "", fmt.Errorf("no se pudo leer %s: %w", ruta, err)
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		linea := sc.Text()
		valor, ok := strings.CutPrefix(linea, "Cpus_allowed_list:")
		if !ok {
			continue
		}
		return strings.TrimSpace(valor), nil
	}
	if err := sc.Err(); err != nil {
		return "", fmt.Errorf("no se pudo recorrer %s: %w", ruta, err)
	}
	return "", fmt.Errorf("%s no trae Cpus_allowed_list", ruta)
}
