package sessionmgr

// inyector.go — EL ENRUTADOR DEL INYECTOR DE ENTRANTES SINTÉTICOS (MP-10 Parte A).
//
// FICHERO PROPIO, y no una función más en manager.go, por el mismo criterio con el que despacho.go se
// separó en su día: aquí vive UNA capacidad entera y pequeña —enrutar una inyección hasta el gateway vivo
// de una sesión— con su error propio y su porqué; manager.go ya carga el ciclo de vida completo del
// registro de sesiones y añadirle un frente ajeno lo hace más difícil de leer sin hacer más fácil nada.
//
// QUÉ TRAMO ES ESTE. El puente tiene tres piezas y esta es la del medio:
//
//	plano de control (HTTP local)  →  Manager.InyectarEntrante  →  liveSession.inyectarVia  →  gateway
//	  elige la sesión y valida        busca la sesión viva          lee el cable del ciclo     camino REAL
//
// El Manager NO conoce whatsmeow ni el plano de control: recibe un session_id y un app.InyeccionEntrante
// (el tipo del PUERTO, ver internal/app/inyector.go) y devuelve lo que el camino real conteste. Ese es
// justamente el motivo por el que el tipo vive en `app` y no aquí ni en el adaptador.

import (
	"context"
	"errors"
	"fmt"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/whatsmeow"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// ErrSesionNoViva: se pidió inyectar en un session_id que NO está en el registro vivo. Puede no existir en
// absoluto, o existir persistido pero sin listener arrancado (una sesión 'pairing', o una activa cuyo
// Restore no llegó a levantarla). Los dos casos son lo mismo PARA EL INYECTOR y por eso comparten error: no
// hay camino entrante que recorrer, así que no hay nada que medir.
//
// Se mantiene SEPARADO de ErrSessionNotFound (unlink.go) aunque se parezcan: aquel afirma que la sesión no
// existe NI viva NI persistida —una afirmación sobre el disco, que este camino no consulta— y quien lo lea
// aquí concluiría que hay que emparejar de nuevo cuando quizá solo falta esperar a que el listener suba.
var ErrSesionNoViva = errors.New("sessionmgr: no hay sesión viva con ese id; el inyector exige una sesión emparejada y escuchando")

// InyectarEntrante mete UN entrante sintético por el camino REAL del handler de la sesión indicada (MP-10
// Parte A). Es el método que el plano de control alcanza a través de un puerto estrecho de un solo método
// declarado en SU paquete (el molde de sessionUnlinker, adapters/control/server/unlink.go): aquí solo se
// pone el método, satisfecho estructuralmente.
//
// Devuelve el ACUSE del camino entrante (acusar) y, aparte, el error de ENRUTADO. Son dos hechos distintos
// y no se colapsan: `acusar=false, err=nil` significa que la inyección llegó al handler y el handler la
// descartó —resultado legítimo, y una señal de la medición—, mientras que un error significa que nunca
// llegó a recorrerse nada. Confundirlos convertiría un camino no ejercitado en una medición aparentemente
// válida.
//
// 🔴 EL CANDADO DEL MANAGER SE SUELTA ANTES DE LLAMAR AL GATEWAY, Y NO ES UNA PRECAUCIÓN TEÓRICA. La
// inyección recorre el handler entrante COMPLETO: filtra, serializa metadatos y ESCRIBE una fila cifrada en
// SQLite (la BD única compartida por todas las sesiones). Sostener `m.mu` durante ese I/O congelaría el
// gestor entero —Pair, Restore, Unlink, List, Health y el colector de salud pasan todos por ese mismo
// candado—, y lo haría precisamente durante una tanda de N inyecciones seguidas, que es cuando más se nota.
// El molde es el de DespachoStats/Health: se toma `m.mu` solo para sacar el puntero del mapa y se suelta;
// el resto lo protege el candado de la propia liveSession, que inyectarVia también libera antes de llamar.
// Anidar los dos (m.mu → s.mu) crearía además un orden de bloqueo que hoy no existe en ningún camino.
func (m *Manager) InyectarEntrante(ctx context.Context, sessionID string, p app.InyeccionEntrante) (acusar bool, err error) {
	m.mu.Lock()
	s, ok := m.live[sessionID]
	m.mu.Unlock()
	if !ok {
		// El session_id sí va en el error (no es dato sensible: aparece en cada línea de log del daemon), y
		// hace falta para que el operador sepa CUÁL de las sesiones pidió y no está. El contenido de `p` NO
		// se incrusta jamás: lleva el texto del mensaje, que se trata con las reglas de INV-051.1.
		return false, fmt.Errorf("%w: %s", ErrSesionNoViva, sessionID)
	}
	// Fuera del candado del Manager: la sesión existe y es suya, y a partir de aquí manda el candado de la
	// sesión (ver inyectarVia). Si su ciclo de escucha aún no publicó el cable, el error es
	// ErrInyectorNoCableado — distinto de ErrSesionNoViva a propósito: uno se arregla esperando, el otro no.
	acusar, err = s.inyectarVia(ctx, p)
	// 🔴 TRADUCCIÓN DEL CENTINELA DEL ADAPTADOR AL CENTINELA DEL PUERTO, y no es cosmética: sin ella hay un
	// estado ENTERO de campo que no tiene 409.
	//
	// El cable (`liveInyectar`) y el Listener del gateway se publican en momentos DISTINTOS y nadie los
	// sincroniza: el factory publica el cable por intento, antes de `serve()` (listen.go:169), y `serve()`
	// publica el Listener después de `Register` y lo limpia al salir (listen_gateway.go). Entre medias —y
	// durante TODO el backoff de reconexión, hasta 60 s, que es la ventana larga— el cable existe y apunta a
	// un gateway sin Listener. `inyectarVia` no ve nada raro (fn != nil, la llama), el gateway responde con
	// ErrSinEscuchaViva y, sin esta línea, ese error llega al borde como uno cualquiera: no aborta la tanda,
	// se cuenta 500 veces y sale un 200 con `inyectados: 0, errores: 500`. Un «he medido» a quien no midió
	// nada, que es el modo de fallo que MP-10 existe para eliminar.
	//
	// VA AQUÍ, EN EL MANAGER, Y NO EN inyectarVia (session.go), por dos razones:
	//   1. session.go define el `liveSession` que TODOS los caminos del gestor usan y está deliberadamente
	//      limpio de imports de adaptadores; `inyectarVia` es además el calco exacto de sendVia/sendViaMedia
	//      y romper esa simetría por un instrumento temporal es el intercambio equivocado.
	//   2. Este fichero es la unidad que se BORRA entera cuando MP-10 cierre. Lo que se borra junto se
	//      escribe junto — el mismo criterio con el que el adaptador y el paquete `diag` se agruparon.
	// No añade arista nueva al grafo: sessionmgr ya importa internal/adapters/whatsmeow (listen.go:31), y el
	// adaptador no importa sessionmgr (solo internal/app), así que no hay ciclo posible.
	//
	// El envoltorio usa `%v` para el error de abajo, no `%w`, DELIBERADAMENTE: el centinela del adaptador
	// muere aquí. Aguas arriba solo viaja el del puerto (ErrInyectorNoCableado), que es lo que el borde
	// conoce; dejar salir los dos invitaría al plano de control a discriminar por el del adaptador y ataría
	// el borde HTTP a whatsmeow. El TEXTO sí sube entero: es el que explica si fue el hueco o el backoff.
	if errors.Is(err, whatsmeow.ErrSinEscuchaViva) {
		return false, fmt.Errorf("%w: %v", ErrInyectorNoCableado, err)
	}
	return acusar, err
}
