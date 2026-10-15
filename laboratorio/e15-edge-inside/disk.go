//go:build laboratorio

package main

// disk.go — MIRAR LA COLA EN DISCO, por debajo del adaptador: las filas tal como las guardó SQLite y los
// bytes crudos del fichero. Es lectura pura; nada aquí escribe en la cola.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
)

// queueRow es una fila de la cola tal como está en disco: lo que va en claro y lo que va sellado.
type queueRow struct {
	ID           int64
	Seq          int64
	Estado       string
	ChatJID      string // EN CLARO: con session_id es la clave de la conversación
	WAMessageID  string // EN CLARO: idempotencia del encolado
	TSWhatsApp   int64
	TextoEnc     []byte // SELLADO con la DEK de la sesión (INV-051.1)
	MetaEnc      []byte // SELLADO igual; puede ser NULL
	DespachadoEn sql.NullInt64
}

// readQueue lee las filas de UNA sesión en el orden de despacho (`seq`).
func readQueue(ctx context.Context, colaDB *sql.DB, sessionID string) ([]queueRow, error) {
	rows, err := colaDB.QueryContext(ctx, `SELECT id, seq, estado, chat_jid, wa_message_id, ts_whatsapp,
		texto_enc, meta_enc, despachado_en FROM cola_entrantes WHERE session_id = ? ORDER BY seq`, sessionID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []queueRow
	for rows.Next() {
		var r queueRow
		if err := rows.Scan(&r.ID, &r.Seq, &r.Estado, &r.ChatJID, &r.WAMessageID, &r.TSWhatsApp,
			&r.TextoEnc, &r.MetaEnc, &r.DespachadoEn); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// countQueue cuenta todas las filas de una sesión.
func countQueue(ctx context.Context, colaDB *sql.DB, sessionID string) (int, error) {
	var n int
	err := colaDB.QueryRowContext(ctx, `SELECT count(*) FROM cola_entrantes WHERE session_id = ?`, sessionID).Scan(&n)
	return n, err
}

// queueFiles devuelve el fichero de la cola y su WAL. La ruta se le PREGUNTA a la conexión (PRAGMA
// database_list) en vez de rearmarla: el layout es el único que la fabrica, y el guardarraíl
// cola_cableado_ast_test.go prohíbe escribir el nombre del fichero o convertir su ruta a string.
func queueFiles(ctx context.Context, colaDB *sql.DB) ([]string, error) {
	var seq int
	var name, file string
	if err := colaDB.QueryRowContext(ctx, `PRAGMA database_list`).Scan(&seq, &name, &file); err != nil {
		return nil, err
	}
	if file == "" {
		return nil, errors.New("la conexión de la cola no tiene fichero (¿BD en memoria?)")
	}
	return []string{file, file + "-wal"}, nil
}

// grepFiles cuenta cuántas veces aparece needle en los bytes crudos de cada fichero. Un fichero que no
// existe (el WAL tras un checkpoint) cuenta 0.
func grepFiles(files []string, needle string) (map[string]int, error) {
	out := make(map[string]int, len(files))
	for _, f := range files {
		b, err := os.ReadFile(f)
		if errors.Is(err, os.ErrNotExist) {
			out[f] = 0
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("leer %s: %w", f, err)
		}
		out[f] = bytes.Count(b, []byte(needle))
	}
	return out, nil
}

func sumCounts(m map[string]int) int {
	t := 0
	for _, v := range m {
		t += v
	}
	return t
}
