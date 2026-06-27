package server

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/EduGoGroup/wapp-edge-agent/internal/adapters/control"
	"github.com/EduGoGroup/wapp-edge-agent/internal/app"
)

// firstQRWait acota cuánto espera POST /v1/sessions/pair el PRIMER QR antes de responder. Si llega,
// la respuesta ya trae la imagen; si no, responde pending y la UI obtiene el QR por el primer poll.
// Margen corto: el QR de whatsmeow suele aparecer en cientos de ms tras conectar.
const firstQRWait = 2 * time.Second

// sessionPairer es el puerto ESTRECHO que el plano de control necesita para emparejar, RE-LLAVEADO a
// session_id (integración Plan 008): lo cumple *sessionmgr.Manager.Pair. SIN conocer whatsmeow ni la
// DEK, el control dispara el pairing y observa el QR; el Manager genera el session_id, su dir/DEK/store
// y registra la sesión. Un doble lo implementa en los tests SIN WhatsApp real.
//
// Pair BLOQUEA hasta que el pairing termina (success, error, timeout o cancelación de ctx), publicando
// cada QR rotado en qr.ShowQR. En éxito devuelve el session_id + el JID emparejado. La DEK se genera y
// se SELLA en la custodia de la sesión DENTRO de Pair; NUNCA cruza este puerto ni el contrato /v1
// (ADR-0014/0007/0015).
type sessionPairer interface {
	Pair(ctx context.Context, qr app.QRSink) (app.PairResult, error)
}

// pairManager gestiona en memoria los emparejamientos del plano de control: arranca Manager.Pair (vía
// el puerto sessionPairer) en una goroutine con un MemoryQRSink propio, indexa el estado por un id de
// EMPAREJAMIENTO (distinto del session_id, que el Manager genera dentro) y respeta la cancelación por
// contexto. MVP (decisión §10.F/H): UN pairing activo a la vez; un segundo POST mientras hay uno en
// curso → 409 conflict.
type pairManager struct {
	pairer sessionPairer
	log    logger
	// base es el contexto que acota el ciclo de vida de los pairings (independiente del ctx de la
	// petición HTTP, que muere al responder el POST). Por defecto context.Background; Manager.Pair aplica
	// su propio timeout (app.DefaultPairTimeout vía WithWhatsmeowPairing).
	base context.Context //nolint:containedctx // ciclo de vida del pairing, no de una request.

	mu     sync.Mutex
	active bool // true mientras hay un pairing en curso (single-flight MVP).
	states map[string]*pairingState
}

// pairingState reúne el sink de QR de un emparejamiento y su desenlace (session_id + jid), que la
// goroutine rellena al completarse para que el poll lo reporte. Los campos de desenlace se leen/escriben
// bajo pairManager.mu; el sink tiene su propio lock interno (Snapshot/Finish).
type pairingState struct {
	sink      *control.MemoryQRSink
	sessionID string // set en éxito (bajo pairManager.mu)
	jid       string // set en éxito (bajo pairManager.mu)
}

// logger es el subconjunto de sharedlogger.Logger que usa el manager (puede ser nil).
type logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

// RegisterPairing cuelga los endpoints de emparejamiento (POST /v1/sessions/pair y
// GET /v1/sessions/{id}/pair) sobre el sessionPairer dado. Se llama ANTES de Serve. runServe lo invoca
// con el *sessionmgr.Manager; los tests con un doble. La firma de New NO cambia.
func (s *Server) RegisterPairing(p sessionPairer) {
	m := &pairManager{
		pairer: p,
		base:   context.Background(),
		states: make(map[string]*pairingState),
	}
	if s.log != nil {
		m.log = s.log
	}
	s.Handle(http.MethodPost, "/v1/sessions/pair", m.handlePair)
	s.Handle(http.MethodGet, "/v1/sessions/{id}/pair", m.handlePoll)
}

// pairResponse es el cuerpo de POST /v1/sessions/pair: id del EMPAREJAMIENTO (para hacer poll) + QR
// vigente (data-URL PNG, "" si aún no llegó) + estado. NO transporta DEK ni store (ADR-0014/0007).
type pairResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	QR     string `json:"qr"`
}

// pollResponse es el cuerpo de GET /v1/sessions/{id}/pair: estado + QR vigente (mientras pending) +
// session_id/jid de la sesión resultante (en éxito) + mensaje de error (en error). Tampoco transporta
// DEK ni store.
type pollResponse struct {
	Status    string `json:"status"`
	QR        string `json:"qr"`
	SessionID string `json:"session_id,omitempty"`
	JID       string `json:"jid,omitempty"`
	Error     string `json:"error,omitempty"`
}

// handlePair arranca un pairing nuevo y responde con el id + el primer QR (o pending si aún no llegó
// en firstQRWait). Si ya hay uno en curso → 409 conflict.
func (m *pairManager) handlePair(w http.ResponseWriter, r *http.Request) {
	id, sink, ok := m.start()
	if !ok {
		writeError(w, http.StatusConflict, codeConflict, "ya hay un emparejamiento en curso")
		return
	}

	// Espera breve por el primer QR (acotada por firstQRWait y por el ctx de la petición).
	ctx, cancel := context.WithTimeout(r.Context(), firstQRWait)
	defer cancel()
	sink.WaitFirstQR(ctx)

	snap := sink.Snapshot()
	qrURL, err := renderQR(snap.QR)
	if err != nil {
		if m.log != nil {
			m.log.Error("plano de control: no se pudo renderizar el QR", "error", err)
		}
		writeError(w, http.StatusInternalServerError, codeInternal, "no se pudo renderizar el QR")
		return
	}
	writeJSON(w, http.StatusOK, pairResponse{ID: id, Status: string(snap.Status), QR: qrURL})
}

// handlePoll devuelve el estado del pairing identificado por {id}: pending (con QR vigente
// renderizado), success (con session_id+jid de la sesión resultante) o error. id desconocido → 404.
func (m *pairManager) handlePoll(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	st := m.lookup(id)
	if st == nil {
		writeError(w, http.StatusNotFound, codeNotFound, "emparejamiento no encontrado: "+id)
		return
	}

	snap := st.sink.Snapshot()
	resp := pollResponse{Status: string(snap.Status), Error: snap.Err}
	if snap.Status == control.PairPending {
		qrURL, err := renderQR(snap.QR)
		if err != nil {
			if m.log != nil {
				m.log.Error("plano de control: no se pudo renderizar el QR", "error", err)
			}
			writeError(w, http.StatusInternalServerError, codeInternal, "no se pudo renderizar el QR")
			return
		}
		resp.QR = qrURL
	}
	if snap.Status == control.PairSuccess {
		m.mu.Lock()
		resp.SessionID, resp.JID = st.sessionID, st.jid
		m.mu.Unlock()
	}
	writeJSON(w, http.StatusOK, resp)
}

// renderQR convierte el QR crudo a data-URL PNG; un QR vacío (aún no rotó) devuelve "" sin error.
func renderQR(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	return control.PNGDataURL(raw)
}

// start crea un pairing nuevo si no hay otro activo y lanza Manager.Pair (vía sessionPairer) en
// goroutine. ok=false indica que ya había uno en curso (→ 409). El ctx del pairing es m.base (NO el de
// la petición HTTP), para que sobreviva a la respuesta del POST; Manager.Pair aplica su propio timeout.
func (m *pairManager) start() (id string, sink *control.MemoryQRSink, ok bool) {
	m.mu.Lock()
	if m.active {
		m.mu.Unlock()
		return "", nil, false
	}
	id = uuid.NewString()
	sink = control.NewMemoryQRSink()
	st := &pairingState{sink: sink}
	m.active = true
	m.states[id] = st
	m.mu.Unlock()

	go func() {
		res, err := m.pairer.Pair(m.base, sink)
		m.mu.Lock()
		st.sessionID, st.jid = res.SessionID, res.WaJID
		m.active = false
		m.mu.Unlock()
		sink.Finish(err)
		if m.log != nil {
			if err != nil {
				m.log.Error("plano de control: pairing terminó con error", "id", id, "error", err)
			} else {
				m.log.Info("plano de control: pairing completado", "id", id, "session_id", res.SessionID, "jid", res.WaJID)
			}
		}
	}()
	return id, sink, true
}

// lookup devuelve el estado del pairing id, o nil si no existe.
func (m *pairManager) lookup(id string) *pairingState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.states[id]
}
