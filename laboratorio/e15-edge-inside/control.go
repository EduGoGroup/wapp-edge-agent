//go:build laboratorio

package main

// control.go — EL CLIENTE DEL PLANO DE CONTROL. En E15 el programa hace de `wapp-ctl` (o del `curl
// --unix-socket` de operacion.md §3.3): habla HTTP por el socket unix del Edge. Sin Bearer, porque el
// servidor se montó sin authorizer.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

type controlClient struct{ hc *http.Client }

func newControlClient(socket string) *controlClient {
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}}
	return &controlClient{hc: &http.Client{Transport: tr, Timeout: 30 * time.Second}}
}

// call hace una petición y decodifica la respuesta en out (si no es nil). Devuelve el código HTTP y el
// cuerpo crudo, para poder enseñarlo.
func (c *controlClient) call(ctx context.Context, method, path string, body, out any) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://unix"+path, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if out != nil && resp.StatusCode < 300 && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, raw, fmt.Errorf("respuesta no es el JSON esperado: %w", err)
		}
	}
	return resp.StatusCode, raw, nil
}

// Las formas de las respuestas que el programa lee (copiadas de los handlers: son tipos no exportados).

type pairStart struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	QR     string `json:"qr"`
}

type pairPoll struct {
	Status    string `json:"status"`
	QR        string `json:"qr"`
	SessionID string `json:"session_id"`
	JID       string `json:"jid"`
	Error     string `json:"error"`
}

type injectRequest struct {
	SessionID string `json:"session_id"`
	Texto     string `json:"texto"`
	ChatJID   string `json:"chat_jid"`
	Lote      string `json:"lote"`
}

type injectResponse struct {
	Lote        string `json:"lote"`
	Solicitados int    `json:"solicitados"`
	Inyectados  int    `json:"inyectados"`
	Acusados    int    `json:"acusados"`
	Errores     int    `json:"errores"`
	DuracionMS  int64  `json:"duracion_ms"`
	PrimerError string `json:"primer_error"`
}

type capturedResponse struct {
	SessionID  string `json:"session_id"`
	Capturados []struct {
		CommandID string    `json:"command_id"`
		MessageID string    `json:"message_id"`
		To        string    `json:"to"`
		Text      string    `json:"text"`
		At        time.Time `json:"at"`
	} `json:"capturados"`
	Total       int `json:"total"`
	Descartados int `json:"descartados"`
}
