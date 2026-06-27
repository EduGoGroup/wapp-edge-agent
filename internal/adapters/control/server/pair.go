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

// Pairer es el puerto ESTRECHO que el plano de control necesita para emparejar, SIN conocer
// whatsmeow ni la DEK. Es el CONTRATO para T3: la implementación viva (LivePairer) envuelve
// app.Pair sobre el Connector real y el custodio del SO; un doble lo implementa en los tests SIN
// WhatsApp real.
//
// Run BLOQUEA hasta que el pairing termina (success, error, timeout o cancelación de ctx),
// publicando cada QR rotado en sink.ShowQR. En éxito devuelve el JID emparejado. La DEK se genera y
// se SELLA en el custodio DENTRO de Run (app.Pair); NUNCA cruza este puerto ni el contrato /v1
// (ADR-0014/0007).
type Pairer interface {
	Run(ctx context.Context, sink app.QRSink) (jid string, err error)
}

// LivePairer es el Pairer de producción: envuelve app.Pair sobre el Connector vivo de whatsmeow y el
// custodio de llaves del SO. T3 lo construye con las deps reales y lo pasa a (*Server).RegisterPairing.
// No reimplementa el pairing: reusa internal/app/pair.go tal cual (lección Plan 006: un solo cliente
// vivo, sin clientes efímeros).
type LivePairer struct {
	connector app.Connector
	custody   app.KeyCustody
	opts      []app.PairOption
}

var _ Pairer = (*LivePairer)(nil)

// NewLivePairer construye el Pairer vivo con el Connector y el custodio reales (wire-up en T3).
func NewLivePairer(connector app.Connector, custody app.KeyCustody, opts ...app.PairOption) *LivePairer {
	return &LivePairer{connector: connector, custody: custody, opts: opts}
}

// Run ejecuta un pairing real reusando app.Pair: el QR se publica en sink y la DEK se sella en el
// custodio al completarse, dentro de esta llamada.
func (lp *LivePairer) Run(ctx context.Context, sink app.QRSink) (string, error) {
	res, err := app.NewPair(lp.connector, sink, lp.custody, lp.opts...).Run(ctx)
	return res.WaJID, err
}

// pairManager gestiona en memoria los emparejamientos del plano de control: arranca app.Pair (vía el
// puerto Pairer) en una goroutine con un MemoryQRSink, indexa el estado por id y respeta la
// cancelación por contexto. MVP (decisión §10.F/H): UN pairing activo a la vez; un segundo POST
// mientras hay uno en curso → 409 conflict.
type pairManager struct {
	pairer Pairer
	log    logger
	// base es el contexto que acota el ciclo de vida de los pairings (independiente del ctx de la
	// petición HTTP, que muere al responder el POST). T3 puede inyectar uno ligado al shutdown del
	// daemon; por defecto es context.Background y app.Pair aplica su propio timeout (90s).
	base context.Context //nolint:containedctx // ciclo de vida del pairing, no de una request.

	mu     sync.Mutex
	active bool // true mientras hay un pairing en curso (single-flight MVP).
	states map[string]*control.MemoryQRSink
}

// logger es el subconjunto de sharedlogger.Logger que usa el manager (puede ser nil).
type logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

// RegisterPairing cuelga los endpoints de emparejamiento (POST /v1/sessions/pair y
// GET /v1/sessions/{id}/pair) sobre el Pairer dado. Se llama ANTES de Serve, igual que las rutas de
// T0/T1. T3 invoca esto con un LivePairer; los tests con un doble. La firma de New (T0) NO cambia.
func (s *Server) RegisterPairing(p Pairer) {
	m := &pairManager{
		pairer: p,
		base:   context.Background(),
		states: make(map[string]*control.MemoryQRSink),
	}
	if s.log != nil {
		m.log = s.log
	}
	s.Handle(http.MethodPost, "/v1/sessions/pair", m.handlePair)
	s.Handle(http.MethodGet, "/v1/sessions/{id}/pair", m.handlePoll)
}

// pairResponse es el cuerpo de POST /v1/sessions/pair: id del pairing + QR vigente (data-URL PNG, ""
// si aún no llegó) + estado. NO transporta DEK ni store (invariante ADR-0014/0007).
type pairResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	QR     string `json:"qr"`
}

// pollResponse es el cuerpo de GET /v1/sessions/{id}/pair: estado + QR vigente (mientras pending) +
// mensaje de error (en error). Tampoco transporta DEK ni store.
type pollResponse struct {
	Status string `json:"status"`
	QR     string `json:"qr"`
	Error  string `json:"error,omitempty"`
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
// renderizado), success o error. id desconocido → 404 envelope.
func (m *pairManager) handlePoll(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sink := m.lookup(id)
	if sink == nil {
		writeError(w, http.StatusNotFound, codeNotFound, "emparejamiento no encontrado: "+id)
		return
	}

	snap := sink.Snapshot()
	qrURL := ""
	if snap.Status == control.PairPending {
		var err error
		if qrURL, err = renderQR(snap.QR); err != nil {
			if m.log != nil {
				m.log.Error("plano de control: no se pudo renderizar el QR", "error", err)
			}
			writeError(w, http.StatusInternalServerError, codeInternal, "no se pudo renderizar el QR")
			return
		}
	}
	writeJSON(w, http.StatusOK, pollResponse{Status: string(snap.Status), QR: qrURL, Error: snap.Err})
}

// renderQR convierte el QR crudo a data-URL PNG; un QR vacío (aún no rotó) devuelve "" sin error.
func renderQR(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	return control.PNGDataURL(raw)
}

// start crea un pairing nuevo si no hay otro activo y lanza app.Pair (vía Pairer) en goroutine. ok=false
// indica que ya había uno en curso (→ 409). El ctx del pairing es m.base (NO el de la petición HTTP),
// para que sobreviva a la respuesta del POST; app.Pair aplica su propio timeout.
func (m *pairManager) start() (id string, sink *control.MemoryQRSink, ok bool) {
	m.mu.Lock()
	if m.active {
		m.mu.Unlock()
		return "", nil, false
	}
	id = uuid.NewString()
	sink = control.NewMemoryQRSink()
	m.active = true
	m.states[id] = sink
	m.mu.Unlock()

	go func() {
		jid, err := m.pairer.Run(m.base, sink)
		sink.Finish(err)
		m.mu.Lock()
		m.active = false
		m.mu.Unlock()
		if m.log != nil {
			if err != nil {
				m.log.Error("plano de control: pairing terminó con error", "id", id, "error", err)
			} else {
				m.log.Info("plano de control: pairing completado", "id", id, "jid", jid)
			}
		}
	}()
	return id, sink, true
}

// lookup devuelve el sink del pairing id, o nil si no existe.
func (m *pairManager) lookup(id string) *control.MemoryQRSink {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.states[id]
}
