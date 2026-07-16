package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"sync"
	"time"

	sharedauth "github.com/EduGoGroup/wapp-shared/auth"
	sharedlogger "github.com/EduGoGroup/wapp-shared/logger"
	"github.com/golang-jwt/jwt/v5"
)

// Valores por defecto del modo degradado y el refresh proactivo (ADR-0025 dec.3).
const (
	// defaultGraceWindow: ventana máxima de operación con el último access EXPIRADO
	// cuando la nube no responde el refresh. Solo lectura (ADR-0025 dec.3: ≤2h).
	defaultGraceWindow = 2 * time.Hour
	// defaultRefreshMargin: margen antes de la expiración para el refresh proactivo.
	defaultRefreshMargin = 2 * time.Minute
	// defaultTickInterval: cadencia del bucle de refresh proactivo.
	defaultTickInterval = 30 * time.Second
)

// LoginResult es lo que el endpoint /v1/auth/{login,refresh} devuelve a wapp-ctl:
// el ACCESS token (para que wapp-ctl lo reenvíe como Bearer al núcleo) + metadatos.
// El REFRESH token NUNCA sale del núcleo (se custodia); por eso no está aquí.
type LoginResult struct {
	AccessToken string    `json:"access_token"`
	TokenType   string    `json:"token_type"`
	ExpiresAt   time.Time `json:"expires_at"`
	Roles       []string  `json:"roles,omitempty"`
}

// Manager es el session manager del operador (Plan 033 Ola 3 / ADR-0025): relaya
// login/refresh/logout al IAM por CloudLink, custodia el refresh token, mantiene
// el access token en memoria, lo valida OFFLINE (ES256 por kid) y aplica el gate
// RBAC edge.* con default DENY, incluyendo el modo degradado ≤2h (solo lectura).
type Manager struct {
	relay   Relay
	keys    *KeyStore
	custody SecretCustody
	log     sharedlogger.Logger
	now     func() time.Time

	graceWindow    time.Duration
	refreshMargin  time.Duration
	tickInterval   time.Duration
	expectedTenant string // si != "", solo se aceptan tokens de este tenant (ADR-0025: coherencia de tenant)

	refreshMu sync.Mutex // single-flight del refresh (la rotación invalida al perdedor de una carrera)

	mu             sync.Mutex
	access         string
	claims         *sharedauth.Claims
	expires        time.Time
	degradedLogged bool // evita spamear el log al entrar en modo degradado
}

// Option configura aspectos opcionales del Manager.
type Option func(*Manager)

// WithGraceWindow fija la ventana de gracia del modo degradado (default 2h).
func WithGraceWindow(d time.Duration) Option {
	return func(m *Manager) {
		if d > 0 {
			m.graceWindow = d
		}
	}
}

// WithRefreshMargin fija el margen del refresh proactivo (default 2min).
func WithRefreshMargin(d time.Duration) Option {
	return func(m *Manager) {
		if d > 0 {
			m.refreshMargin = d
		}
	}
}

// WithTickInterval fija la cadencia del bucle de refresh proactivo (default 30s).
func WithTickInterval(d time.Duration) Option {
	return func(m *Manager) {
		if d > 0 {
			m.tickInterval = d
		}
	}
}

// WithClock inyecta el reloj (tests deterministas).
func WithClock(now func() time.Time) Option {
	return func(m *Manager) {
		if now != nil {
			m.now = now
		}
	}
}

// WithExpectedTenant restringe la validación a tokens cuyo tenant coincida con el
// del enrolamiento del Edge (ADR-0025). Vacío ⇒ no se comprueba el tenant.
func WithExpectedTenant(tenant string) Option {
	return func(m *Manager) { m.expectedTenant = tenant }
}

// NewManager construye el session manager sobre el relay (CloudLink), el store de
// llaves JWKS y la custodia del refresh token.
func NewManager(relay Relay, keys *KeyStore, custody SecretCustody, log sharedlogger.Logger, opts ...Option) *Manager {
	if log == nil {
		log = sharedlogger.Default()
	}
	m := &Manager{
		relay:         relay,
		keys:          keys,
		custody:       custody,
		log:           log,
		now:           time.Now,
		graceWindow:   defaultGraceWindow,
		refreshMargin: defaultRefreshMargin,
		tickInterval:  defaultTickInterval,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Login canjea email+password contra el IAM (vía relay), custodia el refresh token
// y adopta el access en memoria. Devuelve el access token para wapp-ctl.
func (m *Manager) Login(ctx context.Context, email, password string) (LoginResult, error) {
	tk, err := m.relay.Login(ctx, email, password)
	if err != nil {
		return LoginResult{}, err
	}
	if err := m.adoptTokens(tk); err != nil {
		return LoginResult{}, err
	}
	return m.loginResult(), nil
}

// Refresh canjea el refresh token custodiado por un par nuevo (rotación) y adopta
// el access nuevo. Single-flight: dos refresh concurrentes no compiten (la
// rotación invalidaría al perdedor). Sin refresh custodiado ⇒ ErrRefreshInvalid.
func (m *Manager) Refresh(ctx context.Context) (LoginResult, error) {
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()

	rt, err := m.custody.Load()
	if err != nil {
		return LoginResult{}, ErrRefreshInvalid
	}
	tk, err := m.relay.Refresh(ctx, string(rt))
	if err != nil {
		return LoginResult{}, err
	}
	if err := m.adoptTokens(tk); err != nil {
		return LoginResult{}, err
	}
	return m.loginResult(), nil
}

// Logout revoca el refresh custodiado contra el IAM (o todas las sesiones si
// allSessions) y limpia el estado local SIEMPRE (aunque el relay falle: la
// sesión local debe terminar). Sin refresh custodiado, limpia local y no falla.
func (m *Manager) Logout(ctx context.Context, allSessions bool) error {
	rt, err := m.custody.Load()
	if err != nil {
		m.clearSession()
		return nil
	}
	lerr := m.relay.Logout(ctx, string(rt), allSessions)
	m.clearSession()
	return lerr
}

// adoptTokens custodia el refresh (si viene) y fija el access + claims + expiración
// en memoria. La expiración es la que reporta el IAM (autoritativa). Los claims se
// parsean best-effort para el RBAC del modo degradado.
func (m *Manager) adoptTokens(tk Tokens) error {
	if tk.RefreshToken != "" {
		if err := m.custody.Store([]byte(tk.RefreshToken)); err != nil {
			return err
		}
	}
	claims := m.parseClaims(tk.AccessToken)
	m.mu.Lock()
	m.access = tk.AccessToken
	m.claims = claims
	m.expires = tk.ExpiresAt
	m.degradedLogged = false
	m.mu.Unlock()
	return nil
}

// clearSession olvida el access/claims/refresh locales (logout).
func (m *Manager) clearSession() {
	_ = m.custody.Clear()
	m.mu.Lock()
	m.access = ""
	m.claims = nil
	m.expires = time.Time{}
	m.degradedLogged = false
	m.mu.Unlock()
}

// loginResult proyecta el estado en memoria al DTO del endpoint.
func (m *Manager) loginResult() LoginResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := LoginResult{AccessToken: m.access, TokenType: "Bearer", ExpiresAt: m.expires}
	if m.claims != nil {
		res.Roles = m.claims.Roles
	}
	return res
}

// parseClaims obtiene los claims del access token: verificados por el MultiVerifier
// si ya hay JWKS instalado; si no, parseo SIN verificar (el token viene recién
// emitido por la nube sobre mTLS y solo se usa para bookkeeping local — el
// middleware sí verifica en cada request). Devuelve nil si no se pueden leer.
func (m *Manager) parseClaims(token string) *sharedauth.Claims {
	if mv := m.keys.Verifier(); mv != nil {
		if c, err := mv.ValidateToken(token); err == nil {
			return c
		}
	}
	var c sharedauth.Claims
	if _, _, err := jwt.NewParser().ParseUnverified(token, &c); err != nil {
		m.log.Warn("auth: no se pudieron parsear los claims del access token (bookkeeping local)", "error", err)
		return nil
	}
	return &c
}

// Authorize valida el bearer (ES256 offline) y evalúa el grant `resource` con
// default DENY. write=true marca una operación destructiva/de escritura: en modo
// degradado (≤2h con el access expirado) SOLO se permiten lecturas; una escritura
// exige refresh vivo (ADR-0025 dec.3). Devuelve (allowed, statusHTTP, code, msg)
// con primitivas para NO acoplar el paquete server a este.
//
//	allowed=true            ⇒ status 200, code/msg vacíos.
//	falta/inválido el token ⇒ 401 unauthorized.
//	grant denegado          ⇒ 403 forbidden.
//	escritura en degradado  ⇒ 403 degraded_read_only.
//	tenant cruzado          ⇒ 403 tenant_mismatch.
func (m *Manager) Authorize(_ context.Context, bearer, resource string, write bool) (bool, int, string, string) {
	if bearer == "" {
		return false, http.StatusUnauthorized, "unauthorized", "falta el token de acceso"
	}

	mv := m.keys.Verifier()
	if mv != nil {
		claims, err := mv.ValidateToken(bearer)
		switch {
		case err == nil:
			m.exitDegraded()
			return m.evaluate(claims, resource)
		case errors.Is(err, sharedauth.ErrTokenExpired):
			// cae a la ruta degradada
		default:
			return false, http.StatusUnauthorized, "unauthorized", "token de acceso inválido"
		}
	}
	// Ruta DEGRADADA: el token está expirado, o aún no hay JWKS para verificarlo.
	// Solo el ÚLTIMO access que la nube emitió PARA NOSOTROS obtiene gracia, y solo lectura.
	return m.authorizeDegraded(bearer, resource, write)
}

// authorizeDegraded resuelve la autorización cuando el token no valida como vigente:
// exige que el bearer sea EXACTAMENTE el último access custodiado en memoria
// (provenance: lo recibimos de la nube sobre mTLS), dentro de la gracia y de lectura.
func (m *Manager) authorizeDegraded(bearer, resource string, write bool) (bool, int, string, string) {
	m.mu.Lock()
	held := m.access
	heldClaims := m.claims
	exp := m.expires
	m.mu.Unlock()

	if held == "" || subtle.ConstantTimeCompare([]byte(bearer), []byte(held)) != 1 {
		return false, http.StatusUnauthorized, "unauthorized", "token de acceso expirado"
	}
	if heldClaims == nil {
		return false, http.StatusUnauthorized, "unauthorized", "sesión sin claims verificables"
	}
	if m.now().After(exp.Add(m.graceWindow)) {
		return false, http.StatusUnauthorized, "unauthorized", "sesión expirada fuera de la ventana de gracia"
	}
	m.enterDegraded()
	if write {
		return false, http.StatusForbidden, "degraded_read_only",
			"modo degradado: la operación de escritura requiere refresh vivo con la nube"
	}
	return m.evaluate(heldClaims, resource)
}

// evaluate aplica el tenant (si se exige) y el RBAC glob (default DENY) sobre los
// claims para el recurso pedido.
func (m *Manager) evaluate(claims *sharedauth.Claims, resource string) (bool, int, string, string) {
	if m.expectedTenant != "" && claims.TenantID != m.expectedTenant {
		return false, http.StatusForbidden, "tenant_mismatch", "el token pertenece a otro tenant"
	}
	if !sharedauth.EvaluateGrants(claims.Grants, resource) {
		return false, http.StatusForbidden, "forbidden", "permiso denegado para " + resource
	}
	return true, http.StatusOK, "", ""
}

// enterDegraded registra (una sola vez por episodio) la entrada en modo degradado.
func (m *Manager) enterDegraded() {
	m.mu.Lock()
	first := !m.degradedLogged
	m.degradedLogged = true
	m.mu.Unlock()
	if first {
		m.log.Warn("auth: MODO DEGRADADO activo — access expirado, operando en gracia SOLO LECTURA (ADR-0025 dec.3)")
	}
}

// exitDegraded limpia la marca de modo degradado tras una validación viva.
func (m *Manager) exitDegraded() {
	m.mu.Lock()
	was := m.degradedLogged
	m.degradedLogged = false
	m.mu.Unlock()
	if was {
		m.log.Info("auth: modo degradado terminado — token vigente validado de nuevo")
	}
}

// StartProactiveRefresh arranca el bucle de refresh proactivo (goroutine ligada a
// ctx): cuando falta < refreshMargin para expirar, refresca antes de que el access
// caduque. Un fallo NO es fatal: el modo degradado cubre la caída del cloud.
func (m *Manager) StartProactiveRefresh(ctx context.Context) {
	go func() {
		t := time.NewTicker(m.tickInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.proactiveTick(ctx)
			}
		}
	}()
}

// proactiveTick refresca si hay sesión y el access está por expirar. Devuelve true
// si INTENTÓ un refresh (para los tests). Público-para-tests dentro del paquete.
func (m *Manager) proactiveTick(ctx context.Context) bool {
	m.mu.Lock()
	due := m.access != "" && m.now().After(m.expires.Add(-m.refreshMargin))
	m.mu.Unlock()
	if !due {
		return false
	}
	if _, err := m.Refresh(ctx); err != nil {
		m.log.Warn("auth: refresh proactivo falló (el modo degradado cubrirá si la nube no vuelve)", "error", err)
	}
	return true
}
