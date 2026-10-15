//go:build laboratorio

// Package common es el andamio que comparten los escalones del laboratorio de flujos (Plan 058) dentro
// del Edge.
//
// # Qué es el laboratorio, y qué NO es
//
// Un laboratorio PEDAGÓGICO: cada escalón (`laboratorio/eNN-<nombre>/main.go`) estudia UNA pieza del
// Edge dejándola real y doblando lo que la rodea, y cuenta por pantalla qué pasó y por qué. NO es un
// test de integración: no corre solo y puede doblar la custodia de la llave, el lease o CloudLink si
// estorban a lo que se estudia.
//
// Por eso TODO fichero de `laboratorio/` empieza por `//go:build laboratorio`: `go build ./...`,
// `go vet ./...`, `go test ./...`, `make ci-local`, `make ci-docker` y golangci-lint no lo compilan, y
// ningún binario del Edge (`agent`, `wapp-ctl`, `colaseed`) lo enlaza: los dobles viven FUERA del binario
// (ADR-0049 rechaza un lease «de pruebas» o un camino de custodia nuevo dentro de él). Se compila y se
// corre a mano, desde la raíz del repo:
//
//	GOTOOLCHAIN=go1.26.5 GOWORK=off go build -tags laboratorio ./laboratorio/...
//	GOTOOLCHAIN=go1.26.5 GOWORK=off go run -tags laboratorio ./laboratorio/e15-edge-inside
//
// ⚠️ Un guardarraíl SÍ lo ve: `internal/infra/db/cola_cableado_ast_test.go` parsea TODOS los `.go` del
// repo sin mirar etiquetas. El laboratorio abre la cola como la abre el daemon (`db.OpenCola` con
// `layout.ColaDB()`), sin el nombre del fichero escrito a mano y sin convertir la ruta a string.
//
// ⚠️ Precio conocido: como CI no lo compila, un cambio del código puede romper un escalón sin que nadie
// se entere. El primer paso de cada sesión es el `go build -tags laboratorio` de arriba.
//
// Es una copia-adaptación del andamio del cloud (`wapp-cloud-platform/laboratorio/common`), no una
// dependencia: son módulos distintos y el laboratorio de uno no importa el del otro.
package common
