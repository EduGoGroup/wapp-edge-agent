// Package inventory adapta *sessionmgr.Manager al puerto de lectura del plano de control
// (server.SessionLister): combina el inventario persistido con la salud de runtime por sesión.
package inventory

import (
	"context"

	"github.com/EduGoGroup/wapp-edge-agent/internal/app/sessionmgr"
	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// ManagerAdapter adapta *sessionmgr.Manager al puerto de LECTURA del plano de control (server.
// SessionLister): GET /v1/sessions combina el inventario persistido (Persisted) con la salud de runtime
// por sesión (Health → etiqueta). Mantiene el paquete server desacoplado del tipo SessionHealth.
type ManagerAdapter struct {
	mgr *sessionmgr.Manager
}

// New construye un adaptador de inventario para el plano de control.
func New(mgr *sessionmgr.Manager) ManagerAdapter {
	return ManagerAdapter{mgr: mgr}
}

// Persisted devuelve TODAS las sesiones registradas (incluye 'pairing' aún no viva).
func (m ManagerAdapter) Persisted(ctx context.Context) ([]domain.Session, error) {
	return m.mgr.Persisted(ctx)
}

// Health devuelve la etiqueta de salud de runtime de una sesión viva (ok=false si no está viva).
func (m ManagerAdapter) Health(id string) (string, bool) {
	h, ok := m.mgr.Health(id)
	return h.String(), ok
}
