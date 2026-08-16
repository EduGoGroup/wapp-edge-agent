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

// afinidadOllama devuelve la lista de CPUs a las que el proceso de Ollama tiene permitido subirse
// (T2.8, REQ-051.16), en el formato de `Cpus_allowed_list` del kernel: "0-3", "2,4", "0-1,6"…
//
// POR QUÉ ESTA COMPROBACIÓN EXISTE: la O0 midió que el aislamiento por CPU es LA palanca de
// rendimiento de esta pieza. Con una sola instancia de Ollama y el modelo grande al lado, el p50 se va
// a 21,8 s (6,3×, picos de 80 s); con dos instancias aisladas por `taskset` la degradación desaparece.
// El problema operativo es que un reinicio manual o una unidad systemd editada destruyen ese
// aislamiento SIN QUE NADA AVISE. Esto es el aviso.
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
// SI NINGUNO DE LOS DOS ENCUENTRA EL PROCESO se devuelve error y el llamante lo registra como Warn: el
// cajero arranca IGUAL. Los casos en que eso pasa son (a) Ollama no está corriendo todavía —el worker
// puede arrancar antes que él—, (b) Ollama es remoto o vive en otro namespace de red/PID (contenedor),
// y (c) /proc está restringido (hidepid=2). En ninguno de los tres tiene sentido impedir el arranque:
// lo que se pierde es una comprobación de rendimiento, no la corrección.
func afinidadOllama(ctx context.Context, urlOllama string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
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

// cpusPermitidas lee la línea `Cpus_allowed_list` de /proc/<pid>/status, que es la proyección legible
// de sched_getaffinity(2) — el mismo dato que consulta `taskset -pc <pid>`, sin cgo y sin ejecutar
// nada. /proc/<pid>/status es legible por cualquier usuario, así que este paso funciona aunque Ollama
// corra con otro uid.
func cpusPermitidas(pid int) (string, error) {
	ruta := filepath.Join("/proc", strconv.Itoa(pid), "status")
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
