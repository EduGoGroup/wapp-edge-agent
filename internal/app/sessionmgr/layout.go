// Package sessionmgr es el corazón multi-sesión del Edge (ADR-0016 / Plan 008 §1): un registro vivo
// de N sesiones que POSEE el ciclo de vida de cada una y resuelve, dado un session_id, su
// {custodia, store, container, cliente, listener}. Si mañana cambia el layout físico del store (p.ej.
// keystore del SO para la DEK), el cambio queda LOCALIZADO aquí sin tocar a los llamadores.
//
// Este archivo aporta el Layout: la ÚNICA fuente de rutas por sesión. Nadie fuera de aquí arma rutas
// a mano (design §2, ADR-0016 §4).
package sessionmgr

import (
	"fmt"
	"path/filepath"
	"regexp"

	"github.com/EduGoGroup/wapp-edge-agent/internal/infra/db"
)

// Nombres del layout en disco bajo <data_dir> (ADR-0016 §4).
const (
	// sessionsDirName aloja un subdirectorio por session_id.
	sessionsDirName = "sessions"
	// storeDBName es el Container whatsmeow cifrado de la sesión.
	storeDBName = "store.db"
	// keysDirName aloja las DEK por session_id, DESACOPLADAS del directorio de store (Plan 022 §3/§10.C):
	// <data_dir>/keys/<session_id>.key (0600). Al no colgar del árbol de store, la custodia deja de
	// depender del layout del store (que T3 retira al pasar a la BD única) y puede migrar al keystore del
	// SO sin tocar rutas de store; además el borrado quirúrgico de la DEK (custody.Clear) es un paso
	// propio, independiente del rm del store.
	keysDirName = "keys"
	// dekFileExt es la extensión del fichero de DEK por sesión (<session_id>.key).
	dekFileExt = ".key"
	// colaDBName es la COLA DE ENTRANTES (Plan 051): una BD SQLite propia a nivel de <data_dir>, fuera
	// de sessions/ porque es GLOBAL a todas las sesiones (ver ColaDB).
	colaDBName = "cola_entrantes.db"
	// cajeroSockName es el SOCKET DE INFERENCIA del worker-cajero (Plan 044 · Ola 1.6 · T1.6-2): el canal
	// por el que `agent serve` le pide una inferencia a `agent cajero`, que es el único proceso que puede
	// hablar con Ollama (REQ-051.10). Vive a nivel de <data_dir>, junto a la cola y por el mismo motivo:
	// es del EDGE, no de una sesión (ver CajeroSock).
	cajeroSockName = "cajero.sock"
)

// uuidPattern valida el formato UUID canónico (8-4-4-4-12 hex). El session_id es SIEMPRE un UUIDv4
// generado por el Edge (ADR-0016 §3, design §10.B), así que validar el formato es además la barrera
// que impide construir rutas que se escapen de <data_dir>: un UUID no puede contener separadores de
// ruta ni "..". Cualquier id que no calce se rechaza (no se "limpia" en silencio para no colapsar dos
// sesiones distintas en el mismo directorio).
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Layout resuelve las rutas por sesión a partir del directorio base del Edge (data_dir). Es un valor
// inmutable barato de copiar; toda la construcción de rutas pasa por sus métodos.
type Layout struct {
	dataDir string
}

// NewLayout construye un Layout anclado a dataDir (cfg.DataDir). dataDir puede ser relativo (default
// ".") o absoluto; las rutas derivadas heredan esa base.
func NewLayout(dataDir string) Layout {
	return Layout{dataDir: dataDir}
}

// DataDir devuelve el directorio base.
func (l Layout) DataDir() string { return l.dataDir }

// SessionsRoot devuelve <data_dir>/sessions (la raíz común de todas las sesiones).
func (l Layout) SessionsRoot() string { return filepath.Join(l.dataDir, sessionsDirName) }

// SessionDir devuelve <data_dir>/sessions/<id>. Devuelve error si id no es un session_id válido (UUID),
// lo que garantiza que la ruta no se escape de data_dir.
func (l Layout) SessionDir(id string) (string, error) {
	if !validSessionID(id) {
		return "", fmt.Errorf("layout: session_id inválido (se esperaba UUID): %q", id)
	}
	return filepath.Join(l.dataDir, sessionsDirName, id), nil
}

// RelSessionDir devuelve sessions/<id>: la ruta RELATIVA a data_dir del directorio de la sesión, tal
// como se persiste en el metadato domain.Session.StoreDir (ADR-0016 §4). No mezcla data_dir para que
// el registro sea portable si el daemon se mueve de directorio base. Misma validación de UUID que
// SessionDir (impide rutas que se escapen).
func (l Layout) RelSessionDir(id string) (string, error) {
	if !validSessionID(id) {
		return "", fmt.Errorf("layout: session_id inválido (se esperaba UUID): %q", id)
	}
	return filepath.Join(sessionsDirName, id), nil
}

// StoreDB devuelve <data_dir>/sessions/<id>/store.db (Container whatsmeow cifrado de la sesión).
func (l Layout) StoreDB(id string) (string, error) {
	dir, err := l.SessionDir(id)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, storeDBName), nil
}

// ColaDB devuelve <data_dir>/cola_entrantes.db (la COLA DE ENTRANTES del Edge, Plan 051).
//
// NO recibe session_id y NO cuelga de sessions/<id>/, a diferencia de StoreDB, por tres razones:
// la cola es GLOBAL a todas las sesiones (un solo fichero donde anotan los N listeners); su `seq` es
// una secuencia monotónica GLOBAL, que con una BD por sesión no podría ordenar el drenaje entre ellas;
// y el worker-cajero de la O4 hará round-robin sobre N data_dir's —uno por instalación—, no sobre N
// sesiones, así que su unidad de trabajo es exactamente este fichero. Cada fila sigue llevando su
// `session_id` en claro y su contenido sellado con la DEK DE ESA sesión (INV-051.1), de modo que la BD
// compartida no mezcla llaves.
//
// Al no haber id que validar no hay ruta que pueda escaparse de data_dir, así que no devuelve error
// (mismo criterio que DataDir/SessionsRoot).
//
// DEVUELVE db.ColaDBPath Y NO string (Plan 051 · T3.16): ese tipo es lo único que acepta db.OpenCola, y
// no encaja en el `dsn string` de db.Open. Así el compilador —y no un comentario— impide que uno de los
// dos procesos que escriben la cola la abra por el constructor equivocado y se quede sin el perfil de
// escritura que necesita (los pragmas son POR-CONEXIÓN: el porqué completo, en el doc de db.ColaDBPath).
// Este método es el ÚNICO productor del tipo, que es lo que cierra el círculo: la ruta nace aquí ya
// tipada y no hay otra forma legítima de fabricarla.
//
// Es también el motivo de que este paquete —que es `app`— importe `infra/db`: el import existe solo por
// ese tipo, no por comportamiento. La alternativa (declarar el tipo aquí y que db lo importase) crea un
// ciclo en cuanto los tests de sessionmgr importan db, cosa que ya hacen.
func (l Layout) ColaDB() db.ColaDBPath {
	return db.ColaDBPath(filepath.Join(l.dataDir, colaDBName))
}

// CajeroSock devuelve <data_dir>/cajero.sock: el Unix domain socket por el que el daemon le pide
// inferencia al worker-cajero (Plan 044 · Ola 1.6 · T1.6-2, ADR-0045).
//
// NO RECIBE session_id, igual que ColaDB y por una razón todavía más fuerte: el servicio de inferencia
// es del EDGE —un proceso, un Ollama por máquina—, no de una sesión de WhatsApp. El propio contrato lo
// dice dejando `InferenceRequest.session_id` normalmente VACÍO.
//
// SÍ CUELGA DEL data_dir, Y NO DE UN DIRECTORIO GLOBAL DE LA MÁQUINA, aunque el cajero pueda atender
// varias instalaciones a la vez: el CLIENTE es el `agent serve` de cada instalación, y cada uno conoce
// exactamente un directorio —el suyo—. Con un socket por data_dir, cada daemon busca en su propia casa y
// no hay nada que configurar; con uno global habría que enseñarle a cada daemon dónde está el de los
// demás. El cajero levanta N listeners sobre el MISMO servidor de inferencia, así que el aforo de Ollama
// sigue siendo uno (ver cmd/agent/cajero.go).
//
// Al no haber id que validar no hay ruta que pueda escaparse de data_dir, así que no devuelve error
// (mismo criterio que DataDir/SessionsRoot/ColaDB).
//
// Devuelve `string` y no un tipo propio, a diferencia de ColaDB: allí el tipo existía para impedir que
// la BD se abriera por el constructor equivocado (los pragmas son por-conexión y la elección era
// invisible), y aquí no hay dos formas de abrir un socket que difieran en algo que no se vea.
func (l Layout) CajeroSock() string {
	return filepath.Join(l.dataDir, cajeroSockName)
}

// DEKPath devuelve <data_dir>/keys/<session_id>.key (DEK de la sesión, custodiada por FileCustody).
//
// DESACOPLADA del directorio de store (Plan 022 §3/§10.C): ya NO vive en sessions/<id>/dek.key. Así la
// DEK no depende del layout del store (que T3 retira con la BD única) y su borrado quirúrgico (Clear) es
// un paso propio, no redundante con el rm del árbol de store. Valida el UUID directamente (misma barrera
// anti-escape que SessionDir: un UUID no contiene separadores de ruta ni ".."), sin colgar de SessionDir.
func (l Layout) DEKPath(id string) (string, error) {
	if !validSessionID(id) {
		return "", fmt.Errorf("layout: session_id inválido (se esperaba UUID): %q", id)
	}
	return filepath.Join(l.dataDir, keysDirName, id+dekFileExt), nil
}

// validSessionID indica si id es un UUID canónico utilizable como nombre de directorio seguro.
func validSessionID(id string) bool {
	return uuidPattern.MatchString(id)
}
