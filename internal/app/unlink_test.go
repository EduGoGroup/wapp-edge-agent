package app

import (
	"context"
	"errors"
	"testing"

	"github.com/EduGoGroup/wapp-edge-agent/internal/domain"
)

// --- Fakes de los puertos de UnlinkSession (sin BD ni WhatsApp) ---

type fakeRegistry struct {
	sessions map[string]domain.Session
	deleted  []string
}

func (f *fakeRegistry) Get(_ context.Context, jid string) (domain.Session, error) {
	s, ok := f.sessions[jid]
	if !ok {
		return domain.Session{}, ErrSessionNotFound
	}
	return s, nil
}

func (f *fakeRegistry) Delete(_ context.Context, jid string) error {
	f.deleted = append(f.deleted, jid)
	delete(f.sessions, jid)
	return nil
}

// fakeLocator (resolver del device pareado) se reusa de restore_test.go (mismo paquete).

type fakeEraser struct {
	erased []string
	err    error
}

func (f *fakeEraser) DeleteDevice(_ context.Context, jid string) error {
	if f.err != nil {
		return f.err
	}
	f.erased = append(f.erased, jid)
	return nil
}

type fakeCleaner struct {
	cleared bool
	err     error
}

func (f *fakeCleaner) Clear() error {
	if f.err != nil {
		return f.err
	}
	f.cleared = true
	return nil
}

type fakeLogout struct {
	calledJID string
	err       error
}

func (f *fakeLogout) LogoutLiveClient(_ context.Context, jid string) error {
	f.calledJID = jid
	return f.err
}

const testJID = "56999@s.whatsapp.net"

func newRegistryWith(jid string) *fakeRegistry {
	return &fakeRegistry{sessions: map[string]domain.Session{
		jid: {JID: jid, State: domain.SessionStateActive},
	}}
}

// TestUnlink_HappyPath: con sesión registrada y cliente vivo, borra device + registro + DEK y reporta
// logout remoto OK.
func TestUnlink_HappyPath(t *testing.T) {
	reg := newRegistryWith(testJID)
	eraser := &fakeEraser{}
	custody := &fakeCleaner{}
	logout := &fakeLogout{}

	u := NewUnlinkSession(reg, &fakeLocator{}, eraser, custody, logout)
	res, err := u.Run(context.Background(), testJID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.JID != testJID || res.RemoteLogout != LogoutOK {
		t.Fatalf("resultado inesperado: %+v", res)
	}
	if len(eraser.erased) != 1 || eraser.erased[0] != testJID {
		t.Errorf("device no borrado: %+v", eraser.erased)
	}
	if len(reg.deleted) != 1 || reg.deleted[0] != testJID {
		t.Errorf("registro no borrado: %+v", reg.deleted)
	}
	if !custody.cleared {
		t.Error("DEK de custodia no limpiada")
	}
	if logout.calledJID != testJID {
		t.Errorf("logout no invocado con el JID: %q", logout.calledJID)
	}
}

// TestUnlink_NotFound: sin registro y sin device pareado → ErrSessionNotFound (→ 404), sin borrar nada.
func TestUnlink_NotFound(t *testing.T) {
	reg := &fakeRegistry{sessions: map[string]domain.Session{}}
	eraser := &fakeEraser{}
	custody := &fakeCleaner{}

	u := NewUnlinkSession(reg, &fakeLocator{ok: false}, eraser, custody, &fakeLogout{})
	_, err := u.Run(context.Background(), testJID)
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("err: got %v, want ErrSessionNotFound", err)
	}
	if len(eraser.erased) != 0 || custody.cleared {
		t.Error("no debería haber borrado nada en un 404")
	}
}

// TestUnlink_DevicePresentWithoutRegistry: sin fila de negocio pero con device pareado (caso del pairing
// por /v1 que aún no registró) → desvincula igual (existencia por el store).
func TestUnlink_DevicePresentWithoutRegistry(t *testing.T) {
	reg := &fakeRegistry{sessions: map[string]domain.Session{}}
	eraser := &fakeEraser{}
	custody := &fakeCleaner{}

	u := NewUnlinkSession(reg, &fakeLocator{jid: testJID, ok: true}, eraser, custody, &fakeLogout{err: ErrNoLiveClient})
	res, err := u.Run(context.Background(), testJID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.RemoteLogout != LogoutSkipped {
		t.Errorf("logout: got %q, want skipped", res.RemoteLogout)
	}
	if len(eraser.erased) != 1 || !custody.cleared {
		t.Error("debería haber borrado device y DEK pese a no haber registro")
	}
}

// TestUnlink_Idempotent: el caso de uso no falla aunque los borrados sean no-ops (device/DEK ya ausentes
// → los fakes devuelven nil), y un segundo Run sobre un JID ya desvinculado da 404.
func TestUnlink_Idempotent(t *testing.T) {
	reg := newRegistryWith(testJID)
	u := NewUnlinkSession(reg, &fakeLocator{}, &fakeEraser{}, &fakeCleaner{}, &fakeLogout{err: ErrNoLiveClient})

	if _, err := u.Run(context.Background(), testJID); err != nil {
		t.Fatalf("primer Run: %v", err)
	}
	// Segundo Run: la sesión ya no está → 404 (idempotencia a nivel de recurso).
	if _, err := u.Run(context.Background(), testJID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("segundo Run: got %v, want ErrSessionNotFound", err)
	}
}

// TestUnlink_LogoutFailureNonFatal: si el logout remoto falla, la limpieza local se completa igual y se
// reporta failed.
func TestUnlink_LogoutFailureNonFatal(t *testing.T) {
	reg := newRegistryWith(testJID)
	eraser := &fakeEraser{}
	custody := &fakeCleaner{}

	u := NewUnlinkSession(reg, &fakeLocator{}, eraser, custody, &fakeLogout{err: errors.New("red caída")})
	res, err := u.Run(context.Background(), testJID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.RemoteLogout != LogoutFailed {
		t.Errorf("logout: got %q, want failed", res.RemoteLogout)
	}
	if len(eraser.erased) != 1 || !custody.cleared {
		t.Error("la limpieza local debe completarse aunque falle el logout remoto")
	}
}

// TestUnlink_EraserErrorPropagates: un error REAL de borrado del device aborta y se propaga (→ 500), sin
// confundirse con idempotencia.
func TestUnlink_EraserErrorPropagates(t *testing.T) {
	reg := newRegistryWith(testJID)
	custody := &fakeCleaner{}

	u := NewUnlinkSession(reg, &fakeLocator{}, &fakeEraser{err: errors.New("io")}, custody, &fakeLogout{})
	if _, err := u.Run(context.Background(), testJID); err == nil {
		t.Fatal("se esperaba error al fallar el borrado del device")
	}
	if custody.cleared {
		t.Error("no debería limpiar la DEK si el borrado del device falló antes")
	}
}

// TestUnlink_NoLogoutPort: con logout nil (sin gateway), desvincula local y reporta skipped.
func TestUnlink_NoLogoutPort(t *testing.T) {
	reg := newRegistryWith(testJID)
	u := NewUnlinkSession(reg, &fakeLocator{}, &fakeEraser{}, &fakeCleaner{}, nil)
	res, err := u.Run(context.Background(), testJID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.RemoteLogout != LogoutSkipped {
		t.Errorf("logout: got %q, want skipped", res.RemoteLogout)
	}
}
