package main

// session.go implementa la SESIÓN DEL OPERADOR en el borde navegador (Plan 033 · Ola 3 · Paso B,
// ADR-0025 dec.5/6). El núcleo (socket /v1) SOLO devuelve el ACCESS token y custodia el refresh; wapp-ctl
// es el punto donde vive la sesión de la webui:
//
//   - Cookie HttpOnly `wapp_edge_session` = id opaco de sesión. El ACCESS token queda LIGADO a esa cookie
//     pero server-side (mapa en proceso), nunca en el navegador. Así, al refrescar, el access rota sin
//     tocar la cookie (la cookie es estable ⇒ clave natural del single-flight de refresh).
//   - Cookie legible `wapp_edge_csrf` = token CSRF (patrón double-submit): el JS la lee y la reenvía en el
//     header X-CSRF-Token de las peticiones mutadoras; wapp-ctl exige que coincida con la de la sesión.
//
// El refresh token NUNCA sale del núcleo: wapp-ctl jamás lo ve ni lo persiste.

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"sync"
	"time"
)

const (
	// cookieSession es la cookie HttpOnly que ata el navegador a una sesión de operador server-side.
	cookieSession = "wapp_edge_session"
	// cookieCSRF es la cookie LEGIBLE (no HttpOnly) del token CSRF (double-submit). El JS la reenvía en
	// el header X-CSRF-Token de las mutadoras.
	cookieCSRF = "wapp_edge_csrf"
	// headerCSRF es el header que la SPA debe enviar en peticiones mutadoras (double-submit).
	headerCSRF = "X-CSRF-Token"
)

// opSession es la sesión de un operador logueado. El access rota (refresh); la cookie/id y el csrf no.
type opSession struct {
	id   string
	csrf string

	mu      sync.Mutex
	access  string
	roles   []string
	expires time.Time
	gen     uint64 // generación del access: la incrementa cada refresh (single-flight)

	refreshMu sync.Mutex // serializa el refresh: solo un caller golpea el socket, el resto reusa el token nuevo
}

// snapshot devuelve el access vigente y su generación (para el patrón single-flight del refresh).
func (s *opSession) snapshot() (access string, gen uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.access, s.gen
}

// meta devuelve los datos NO sensibles para pintar la UI (roles + expiración).
func (s *opSession) meta() (roles []string, expires time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.roles, s.expires
}

// apply actualiza el access/roles/expires de la sesión y avanza la generación.
func (s *opSession) apply(access string, roles []string, expires time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.access = access
	s.roles = roles
	s.expires = expires
	s.gen++
}

// refreshIfStale implementa el SINGLE-FLIGHT del refresh: solo el primer caller que llega con la
// generación vigente (gen0) ejecuta `do` (la llamada al socket /v1/auth/refresh); los que perdieron la
// carrera reusan el access ya rotado sin volver a golpear el núcleo (la rotación invalidaría al perdedor).
func (s *opSession) refreshIfStale(gen0 uint64, do func() (string, []string, time.Time, error)) (string, error) {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()

	s.mu.Lock()
	curGen, curAccess := s.gen, s.access
	s.mu.Unlock()
	if curGen != gen0 {
		// Otro caller ya refrescó mientras esperábamos el lock: reusa su token.
		return curAccess, nil
	}

	access, roles, expires, err := do()
	if err != nil {
		return "", err
	}
	s.apply(access, roles, expires)
	return access, nil
}

// sessionStore es el registro en-proceso de sesiones de operador (id opaco → *opSession).
type sessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*opSession
}

func newSessionStore() *sessionStore {
	return &sessionStore{sessions: make(map[string]*opSession)}
}

// create registra una sesión nueva a partir del resultado de login del núcleo y devuelve la sesión con
// su id opaco y su token CSRF ya generados.
func (st *sessionStore) create(access string, roles []string, expires time.Time) *opSession {
	sess := &opSession{
		id:      randToken(),
		csrf:    randToken(),
		access:  access,
		roles:   roles,
		expires: expires,
		gen:     1,
	}
	st.mu.Lock()
	st.sessions[sess.id] = sess
	st.mu.Unlock()
	return sess
}

// fromRequest resuelve la sesión a partir de la cookie HttpOnly. Devuelve nil si no hay cookie o el id no
// existe (sesión desconocida/expirada ⇒ el borde la trata como "sin sesión").
func (st *sessionStore) fromRequest(r *http.Request) *opSession {
	c, err := r.Cookie(cookieSession)
	if err != nil || c.Value == "" {
		return nil
	}
	st.mu.RLock()
	sess := st.sessions[c.Value]
	st.mu.RUnlock()
	return sess
}

// delete elimina una sesión del registro (logout / refresh fallido).
func (st *sessionStore) delete(id string) {
	st.mu.Lock()
	delete(st.sessions, id)
	st.mu.Unlock()
}

// csrfValid comprueba el double-submit: el header X-CSRF-Token debe coincidir (comparación en tiempo
// constante) con el token de la sesión. Sin sesión no hay token que comparar ⇒ inválido.
func csrfValid(r *http.Request, sess *opSession) bool {
	if sess == nil {
		return false
	}
	got := r.Header.Get(headerCSRF)
	if got == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(sess.csrf)) == 1
}

// setSessionCookies emite la cookie HttpOnly de sesión + la cookie legible del CSRF. Secure=false porque
// el plano de control es loopback http (127.0.0.1); SameSite=Strict acota el CSRF de origen cruzado.
func setSessionCookies(w http.ResponseWriter, sess *opSession) {
	http.SetCookie(w, &http.Cookie{
		Name:     cookieSession,
		Value:    sess.id,
		Path:     "/",
		HttpOnly: true,
		Secure:   false,
		SameSite: http.SameSiteStrictMode,
	})
	http.SetCookie(w, &http.Cookie{
		Name:     cookieCSRF,
		Value:    sess.csrf,
		Path:     "/",
		HttpOnly: false, // legible por el JS: double-submit
		Secure:   false,
		SameSite: http.SameSiteStrictMode,
	})
}

// clearSessionCookies caduca ambas cookies (logout o refresh fallido).
func clearSessionCookies(w http.ResponseWriter) {
	for _, name := range []string{cookieSession, cookieCSRF} {
		http.SetCookie(w, &http.Cookie{
			Name:     name,
			Value:    "",
			Path:     "/",
			MaxAge:   -1,
			HttpOnly: name == cookieSession,
			SameSite: http.SameSiteStrictMode,
		})
	}
}

// randToken genera un identificador opaco de 256 bits en base64url (id de sesión / token CSRF).
func randToken() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand no falla en la práctica; si lo hiciera, un pánico es preferible a un token débil.
		panic("wapp-ctl: no hay entropía para el token de sesión: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}
