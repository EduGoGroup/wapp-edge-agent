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
)

// Nombres del layout en disco bajo <data_dir> (ADR-0016 §4).
const (
	// sessionsDirName aloja un subdirectorio por session_id.
	sessionsDirName = "sessions"
	// storeDBName es el Container whatsmeow cifrado de la sesión.
	storeDBName = "store.db"
	// dekFileName es la DEK de la sesión (0600), custodiada por FileCustody.
	dekFileName = "dek.key"
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

// StoreDB devuelve <data_dir>/sessions/<id>/store.db (Container whatsmeow cifrado de la sesión).
func (l Layout) StoreDB(id string) (string, error) {
	dir, err := l.SessionDir(id)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, storeDBName), nil
}

// DEKPath devuelve <data_dir>/sessions/<id>/dek.key (DEK de la sesión, custodiada por FileCustody).
func (l Layout) DEKPath(id string) (string, error) {
	dir, err := l.SessionDir(id)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, dekFileName), nil
}

// validSessionID indica si id es un UUID canónico utilizable como nombre de directorio seguro.
func validSessionID(id string) bool {
	return uuidPattern.MatchString(id)
}
