package colaentrantes

// pendientes.go — EL CONTADOR de la cola (Plan 051 Ola 3 · T3.13).
//
// Aquí vive el cuarto papel del *Store: el que solo CUENTA. Lo consume el latido de latencia del handler
// (internal/app/latencia), que publica junto al p99 cuántas filas quedaban sin despachar — porque un p99
// estupendo medido con la cola vacía y uno medido con 1.200 filas encima no son el mismo número, y sin
// este dato la línea del log no permite distinguirlos.
//
// 🔴 POR QUÉ NO TOMA `s.mu`. Ese candado serializa las ESCRITURAS (el bloque podar→tope→insertar de
// Enqueue) y un COUNT no escribe: tomarlo aquí metería una consulta de observabilidad en el camino
// crítico del encolado, que es exactamente lo que INV-051.2 no tolera. Lo peor que puede pasar sin él es
// leer una foto un Enqueue por detrás, y eso en una métrica publicada cada minuto no significa nada.
//
// 🔴 LO QUE SÍ CUESTA es la ÚNICA CONEXIÓN. El Edge abre la BD con SetMaxOpenConns(1) (infra/db), así que
// este COUNT y el INSERT del listener se turnan. Por eso: cadencia amplia, recorrido de índice, y el
// llamante publica `conteo_ms` en la misma línea para que el efecto observador sea visible en vez de
// sospechado.
//
// 🔴 INV-051.1: cardinalidades y nada más. Ni un identificador, ni un JID, ni un byte de texto.

import (
	"context"
	"fmt"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// var _ app.ColaContador: el Store respalda también el lado contador del puerto. La aserción va en el
// fichero que aporta el método, para que borrarlo rompa la compilación en el sitio obvio (mismo criterio
// que la de app.ColaCajero en claim.go y la de app.ColaDespachador en despacho.go).
var _ app.ColaContador = (*Store)(nil)

// sqlPendientes cuenta, por estado, todo lo que NO está despachado.
//
// `estado <> 'despachado'` y NO `estado IN ('nuevo','tomado','clasificado')`, por el MISMO motivo que en
// sqlCabezaDeSesion (ver despacho.go): con el IN, una fila en un estado que la lista no contemple —uno
// nuevo que alguien añada mañana— se volvería invisible para el contador y el total mentiría por debajo
// justo cuando hay algo raro que mirar. Con `<>`, esa fila aparece en el total y su estado sale nombrado
// en el desglose, que es la forma de que el problema se vea.
//
// ÍNDICE QUE SE ESPERA: `ix_cola_estado_seq(estado, seq)` (migrations/cola/0001_cola_entrantes.sql). Su
// columna guía es `estado`, que es justo por lo que se agrupa: SQLite recorre el índice —no la tabla— y
// no necesita ordenar nada. El `<>` no permite un seek, pero sí un recorrido de índice, que es lo que
// evita leer los BLOB cifrados de cada fila.
const sqlPendientes = `
SELECT estado, COUNT(*)
FROM cola_entrantes
WHERE estado <> ?
GROUP BY estado`

// Pendientes cuenta lo que la cola tiene sin despachar, agrupado por estado.
//
// El TOTAL se suma sobre TODOS los grupos devueltos, no sobre los tres nombrados: si aparece un estado
// desconocido, entra en el total y la diferencia con la suma de los tres lo delata. Contar solo los
// conocidos escondería precisamente lo que no se sabe que está pasando.
func (s *Store) Pendientes(ctx context.Context) (app.ColaPendientes, error) {
	var p app.ColaPendientes
	rows, err := s.db.QueryContext(ctx, sqlPendientes, app.EstadoDespachado)
	if err != nil {
		return app.ColaPendientes{}, fmt.Errorf("colaentrantes: contar pendientes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var estado string
		var n int64
		if err := rows.Scan(&estado, &n); err != nil {
			return app.ColaPendientes{}, fmt.Errorf("colaentrantes: leer el conteo por estado: %w", err)
		}
		switch estado {
		case app.EstadoNuevo:
			p.Nuevo = n
		case app.EstadoTomado:
			p.Tomado = n
		case app.EstadoClasificado:
			p.Clasificado = n
		}
		p.Total += n
	}
	if err := rows.Err(); err != nil {
		return app.ColaPendientes{}, fmt.Errorf("colaentrantes: recorrer el conteo por estado: %w", err)
	}
	return p, nil
}
