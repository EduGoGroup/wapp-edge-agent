package whatsappfalso

import (
	"sync"
	"time"
)

// TopeRegistro es cuántos envíos guarda el registro antes de soltar los más viejos. Es memoria del
// proceso, compartida por todas las sesiones: una conversación de prueba son decenas de envíos.
const TopeRegistro = 1000

// Media son los datos de un envío de archivo TAL COMO LLEGARON en el comando. El transporte falso no
// descarga la URL: lo que se captura es lo que la nube pidió enviar, no el binario.
type Media struct {
	URL      string `json:"url"`
	Filename string `json:"filename"`
	MIME     string `json:"mime"`
	Kind     string `json:"kind"`
	Caption  string `json:"caption"`
}

// Envio es un mensaje que el Edge «envió» por el transporte falso: lo que la nube mandó decir y a quién.
type Envio struct {
	SessionID string    `json:"session_id"`
	CommandID string    `json:"command_id"`
	MessageID string    `json:"message_id"`
	To        string    `json:"to"`
	Text      string    `json:"text"`
	Media     *Media    `json:"media,omitempty"`
	At        time.Time `json:"at"`
}

// Registro guarda en MEMORIA los envíos capturados, con tope. Se pierde al reiniciar, a propósito: una
// tabla nueva en el Edge arrastraría su trampa de migraciones a cambio de sobrevivir a un reinicio que la
// prueba no necesita, y aquí no hay Redis ni broker (ADR-0003). Es seguro para uso concurrente.
type Registro struct {
	mu     sync.Mutex
	tope   int
	envios []Envio
	// descartados cuenta los envíos que el tope expulsó. Sale en la lectura para que quien lea sepa que lo
	// que ve no es todo: un registro recortado en silencio daría por buena una conversación incompleta.
	descartados int
}

// NuevoRegistro construye un registro con el tope dado; con tope <= 0 usa TopeRegistro.
func NuevoRegistro(tope int) *Registro {
	if tope <= 0 {
		tope = TopeRegistro
	}
	return &Registro{tope: tope}
}

// Anotar guarda un envío. Si el registro está lleno, suelta el más viejo y lo cuenta.
func (r *Registro) Anotar(e Envio) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.envios) >= r.tope {
		sobran := len(r.envios) - r.tope + 1
		r.envios = append(r.envios[:0], r.envios[sobran:]...)
		r.descartados += sobran
	}
	r.envios = append(r.envios, e)
}

// De devuelve, en orden de envío, una COPIA de los envíos de la sesión dada, y cuántos expulsó el tope en
// total (de cualquier sesión).
func (r *Registro) De(sessionID string) (envios []Envio, descartados int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	envios = make([]Envio, 0, len(r.envios))
	for _, e := range r.envios {
		if e.SessionID == sessionID {
			envios = append(envios, e)
		}
	}
	return envios, r.descartados
}
