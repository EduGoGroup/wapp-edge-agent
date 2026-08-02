package app

import (
	"context"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// ports.go — Puertos (interfaces) de la capa de aplicación que no pertenecen a un caso de uso concreto.
// Los puertos ESPECÍFICOS de un caso de uso (Connector, QRSink, ListenGateway, InboundSink, etc.)
// siguen definidos junto al caso de uso que los consume.

// SessionStore es el puerto de PERSISTENCIA de los metadatos de negocio de las sesiones (tabla
// `sessions`). La implementación real (internal/adapters/sessionstore) lee/escribe SQLite EN CLARO;
// un fake en los tests lo simula en memoria. No custodia material cripto (eso es cryptostore/DEK).
type SessionStore interface {
	// Upsert inserta o actualiza la sesión por su session_id (clave primaria, ADR-0016 §3).
	Upsert(ctx context.Context, s domain.Session) error
	// List devuelve todas las sesiones persistidas (vacío si no hay ninguna).
	List(ctx context.Context) ([]domain.Session, error)
	// ListActive devuelve SOLO las sesiones en estado 'active': las que el arranque debe restaurar
	// (design §6). El Session Manager (sessionmgr.Manager.Restore, T4) itera ESTA lista para arrancar
	// un listener por sesión; las 'pairing'/'loggedout' se omiten por construcción.
	ListActive(ctx context.Context) ([]domain.Session, error)
	// Get devuelve la sesión con ese session_id, o ErrSessionNotFound si no existe.
	Get(ctx context.Context, sessionID string) (domain.Session, error)
	// Delete elimina la fila de la sesión con ese session_id (idempotente: borrar una ausente no es
	// error). Es la parte de metadatos del borrado quirúrgico (design §7) y de la limpieza del
	// pairing fallido (design §5): el Manager la usa para no dejar restos.
	Delete(ctx context.Context, sessionID string) error
}

// DeviceCascadeStore es una CAPACIDAD OPCIONAL sobre SessionStore (Plan 027 T4, cierra H4): borrado
// transaccional POR DISPOSITIVO (fila del device + su cuenta si queda vacía, en una sola tx — cero
// huérfanos). Puerto EXPLÍCITO de app en vez de una interfaz ad-hoc acoplada al store concreto: el
// consumidor (sessionmgr.Manager) lo resuelve por interface-upgrade UNA vez y cae a SessionStore.Delete
// si el store no la implementa (los fakes en memoria de los tests no purgan la cuenta).
type DeviceCascadeStore interface {
	DeleteDeviceCascade(ctx context.Context, sessionID string) error
}

// AccountStore es una CAPACIDAD OPCIONAL sobre SessionStore (Plan 027 T4, cierra H4): operaciones POR
// CUENTA (número) —listar los dispositivos de una cuenta y borrarlos junto a la cuenta en una tx. Puerto
// EXPLÍCITO de app; el Manager lo resuelve por interface-upgrade UNA vez y, si el store no la implementa
// (fakes en memoria), el borrado por cuenta responde con un error claro de "no soportado".
type AccountStore interface {
	GetByAccount(ctx context.Context, accountID string) ([]domain.Session, error)
	DeleteByAccount(ctx context.Context, accountID string) error
}
