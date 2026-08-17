package cajero

import (
	"context"
	"time"
)

// DefaultPollMS es el intervalo por defecto del poll del cajero, en milisegundos
// (WAPP_WORKER_POLL_MS). 500 ms es el compromiso entre latencia percibida y ruido: la p95 de una
// inferencia medida por la O0 es de 3.736 ms, así que medio segundo de espera al despertar es <15 %
// del coste del trabajo que viene después, y a cambio deja el proceso a ~2 consultas/s contra un
// SQLite local cuando la cola está vacía.
const DefaultPollMS = 500

// Despertador es CÓMO el cajero espera a que haya trabajo. Existe como interfaz porque la decisión de
// fondo NO está tomada y no se puede tomar aquí.
//
// 🔴 T2.7, SEGUNDA MITAD — SIN DECIDIR, Y EXIGE MEDICIÓN. El plan pone dos candidatos sobre la mesa:
// el poll de intervalo fijo (lo que implementa PollFijo, más abajo) y `PRAGMA data_version`, que en
// SQLite cambia cuando OTRO proceso escribió el fichero y permitiría despertar sólo cuando el listener
// ha anotado algo. El criterio de la tarea dice literalmente «con la justificación MEDIDA, no
// supuesta», y esa medición necesita el worker vivo delante: cuánto cuesta un `PRAGMA data_version`
// contra un fichero WAL con el daemon escribiendo al lado, y cuánta latencia real ahorra frente a los
// 500 ms del poll. Hasta que exista ese número, aquí hay UNA sola implementación y el hueco queda
// nombrado en vez de rellenado a ojo.
//
// Lo que sí está decidido es la costura: el bucle del cajero no sabe nada del mecanismo, así que el
// día que la medición elija `data_version` se añade un tipo nuevo en este mismo fichero y se cambia
// una línea de cableado en cmd/agent. No hace falta tocar el bucle.
type Despertador interface {
	// Esperar bloquea hasta que toque volver a mirar la cola, o hasta que ctx se cancele. Devuelve
	// nil cuando hay que volver a mirar y el error del contexto cuando el proceso se está apagando:
	// el bucle usa ESE error como su señal de parada, no un bool.
	Esperar(ctx context.Context) error
}

// PollFijo espera un intervalo fijo. Es la implementación de partida del Despertador: la más simple
// que puede funcionar, sin estado compartido con el escritor y sin depender de un detalle del motor.
type PollFijo struct {
	intervalo time.Duration
}

var _ Despertador = (*PollFijo)(nil)

// NewPollFijo construye el poll de intervalo fijo. Un intervalo <= 0 cae a DefaultPollMS: un poll de 0
// convertiría el bucle en una espera activa que quemaría un core entero con la cola vacía, que es
// exactamente lo contrario de lo que la O0 pide de esta máquina.
func NewPollFijo(intervalo time.Duration) *PollFijo {
	if intervalo <= 0 {
		intervalo = DefaultPollMS * time.Millisecond
	}
	return &PollFijo{intervalo: intervalo}
}

// Esperar duerme el intervalo, o vuelve antes con el error del contexto si el proceso se apaga. Usa un
// Timer con Stop en defer y no time.Sleep para que SIGTERM no tenga que esperar al tick pendiente.
func (p *PollFijo) Esperar(ctx context.Context) error {
	t := time.NewTimer(p.intervalo)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Intervalo devuelve el intervalo configurado (para el log de arranque y los tests).
func (p *PollFijo) Intervalo() time.Duration { return p.intervalo }
