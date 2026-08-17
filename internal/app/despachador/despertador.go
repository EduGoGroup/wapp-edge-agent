package despachador

import (
	"context"
	"time"
)

// despertador.go — CÓMO ESPERA el despachador entre dos miradas a la cola (Plan 051 Ola 3 · T3.3).
//
// 🔴 ES UN GEMELO DECLARADO de internal/app/cajero/despertador.go, NO un descuido. La pieza es la misma
// de veinte líneas y la tentación evidente es importar la del cajero; se decidió replicarla, y el porqué
// es de DEPENDENCIAS, no de estilo:
//
//   - El paquete `cajero` arrastra consigo el clasificador, el circuit breaker, el cliente de Ollama y la
//     lectura de afinidades de CPU en /proc. El despachador vive DENTRO del daemon `agent serve`; el
//     cajero es un PROCESO APARTE (el hijo de `wapp-ctl`). Importarlo desde aquí metería todo ese árbol
//     —incluida una pieza que sólo funciona en Linux— en el binario que corre 24/7 en la máquina del
//     cliente, a cambio de no repetir un `time.Timer`. Es un mal negocio.
//   - La alternativa limpia sería un paquete tercero (`internal/app/poll`) del que dependieran los dos,
//     pero eso obliga a reescribir el cableado del cajero, que tiene otro dueño en esta ola. Queda
//     anotado como la refactorización correcta si aparece un TERCER usuario; con dos, el coste de la
//     indirección supera al de la copia.
//
// EL VEREDICTO MEDIDO DE T2.7 APLICA IDÉNTICO Y NO SE REABRE AQUÍ: poll de intervalo FIJO, no
// `PRAGMA data_version`. Esa sonda cambia cuando OTRA conexión escribió el fichero, y el despachador
// comparte proceso con el listener que encola — es decir, no vería precisamente las escrituras que más le
// importan, las de su propia sesión.

// DefaultPollMS es el intervalo por defecto entre dos miradas a la cabeza de la sesión, en milisegundos.
// Es el MISMO número que el poll del cajero y por la misma razón: medio segundo de espera es ruido frente
// al coste de una inferencia (p95 medida de 3.736 ms) y deja el proceso a ~2 consultas por segundo y por
// sesión contra un SQLite local cuando la cola está vacía, que es el estado normal.
//
// ⚠️ AQUÍ SE MULTIPLICA POR EL NÚMERO DE SESIONES VIVAS, cosa que en el cajero no pasa (allí hay un solo
// bucle para toda la máquina). Con las cardinalidades del Edge —unidades de sesiones por caja— eso son
// unas pocas consultas por segundo sobre un índice; si un día una caja llevara decenas de sesiones, la
// palanca no es subir este número sino el índice `(session_id, seq)` que anota sqlCabezaDeSesion.
const DefaultPollMS = 500

// Despertador es CÓMO el despachador espera a que haya trabajo. Es interfaz por la misma costura que en
// el cajero: el bucle no sabe nada del mecanismo, y los tests inyectan un despertador MANUAL gobernado
// por un canal para que la sincronía sea determinista y no dependa de dormir.
type Despertador interface {
	// Esperar bloquea hasta que toque volver a mirar la cola, o hasta que ctx se cancele. Devuelve nil
	// cuando hay que volver a mirar y el error del contexto cuando la sesión se está parando: el bucle usa
	// ESE error como su señal de parada, no un bool.
	Esperar(ctx context.Context) error
}

// PollFijo espera un intervalo fijo. Implementación de partida (y única hoy) del Despertador.
type PollFijo struct {
	intervalo time.Duration
}

var _ Despertador = (*PollFijo)(nil)

// NewPollFijo construye el poll de intervalo fijo. Un intervalo <= 0 cae a DefaultPollMS: un poll de 0
// convertiría el bucle en una espera activa que quemaría un core POR SESIÓN con la cola vacía.
func NewPollFijo(intervalo time.Duration) *PollFijo {
	if intervalo <= 0 {
		intervalo = DefaultPollMS * time.Millisecond
	}
	return &PollFijo{intervalo: intervalo}
}

// Esperar duerme el intervalo, o vuelve antes con el error del contexto si la sesión se para. Usa un
// Timer con Stop en defer y no time.Sleep para que el apagado no tenga que esperar al tick pendiente —
// que es, literalmente, la mitad del criterio «ninguna entrega ocurre después de cancelar el ctx».
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
