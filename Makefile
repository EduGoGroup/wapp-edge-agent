# Makefile — empaque y build del Edge (Plan 023 · T0).
#
# Compila los DOS binarios del Edge (agent + wapp-ctl) con la Version INYECTADA
# desde git (ldflags -X main.Version), dejándolos en un directorio de salida
# HERMANO por plataforma: dist/<os>-<arch>/. Ese layout hermano es EXACTAMENTE
# lo que espera defaultAgentBin() de wapp-ctl (cmd/wapp-ctl/main.go:186-194):
# resuelve el núcleo como "agent" junto al ejecutable de wapp-ctl.
#
# Alcance del plan (DoD T0): SOLO darwin/arm64 se compila y prueba aquí. El resto
# de la matriz de 5 targets (Win/mac/Linux × amd64/arm64) queda DECLARADO como
# andamiaje para el release multiplataforma futuro; no forma parte del DoD.
#
# Nota CGO (para T2): hoy TODO es pure-Go (modernc.org/sqlite), así que todos los
# targets compilan sin C. Cuando T2 añada el Keychain de macOS (CGO tras
# //go:build darwin), darwin/arm64 necesitará CGO_ENABLED=1 (build NATIVO en Mac,
# ya contemplado abajo) y los no-darwin seguirán pure-Go usando el keycustody de
# archivo por build-tag. No romper el cross-compile pure-Go del resto.

SHELL := /bin/bash

GO ?= go

# --- Versionado desde git (NO literal) -------------------------------------
# git describe usa el tag más cercano; si aún no hay tags (estado de partida del
# plan), --always cae al SHA corto y --dirty marca árbol con cambios sin
# commitear. Override posible: `make build VERSION=v1.2.3`.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.Version=$(VERSION)

# Binarios del Edge (ambos package main → símbolo ldflags "main.Version").
CMDS    := agent wapp-ctl
DISTDIR := dist

.DEFAULT_GOAL := build

# build_target(os,arch,cgo): compila AMBOS binarios a dist/<os>-<arch>/.
# Los binarios windows reciben sufijo .exe. -trimpath para builds reproducibles.
define build_target
	@mkdir -p $(DISTDIR)/$(1)-$(2)
	@for cmd in $(CMDS); do \
		out=$(DISTDIR)/$(1)-$(2)/$$cmd$(if $(filter windows,$(1)),.exe,); \
		echo ">> build $(1)/$(2): $$cmd -> $$out (v=$(VERSION))"; \
		GOOS=$(1) GOARCH=$(2) CGO_ENABLED=$(3) \
			$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $$out ./cmd/$$cmd || exit 1; \
	done
endef

## build: (default) compila ambos binarios para darwin/arm64 — target soportado del plan.
.PHONY: build
build: build-darwin-arm64

## build-darwin-arm64: DoD del plan. CGO nativo (=1): base para el Keychain de T2.
.PHONY: build-darwin-arm64
build-darwin-arm64:
	$(call build_target,darwin,arm64,1)

# --- Andamiaje matriz de 5 targets (NO DoD en este plan) -------------------
# Declarados para el release multiplataforma futuro. Compilan pure-Go (CGO=0);
# cuando T2 introduzca el Keychain (CGO, //go:build darwin) estos targets
# seguirán usando el keycustody de archivo por build-tag.
.PHONY: build-windows-amd64 build-windows-arm64 build-linux-amd64 build-linux-arm64 build-all

build-windows-amd64:
	$(call build_target,windows,amd64,0)

build-windows-arm64:
	$(call build_target,windows,arm64,0)

build-linux-amd64:
	$(call build_target,linux,amd64,0)

build-linux-arm64:
	$(call build_target,linux,arm64,0)

## build-all: matriz completa (andamiaje; solo darwin/arm64 es DoD/probado aquí).
build-all: build-darwin-arm64 build-windows-amd64 build-windows-arm64 build-linux-amd64 build-linux-arm64

# --- Estática y pruebas (espejo del CI para el gate local) -----------------

## version: imprime la Version que se inyectaría (útil para depurar el gate).
.PHONY: version
version:
	@echo $(VERSION)

## fmt-check: gofmt -l . debe salir vacío (igual que CI).
.PHONY: fmt-check
fmt-check:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then echo "Archivos sin gofmt:"; echo "$$unformatted"; exit 1; fi

## vet: go vet ./...
.PHONY: vet
vet:
	$(GO) vet ./...

## test: go test -race ./... (incluye los tests de contrato de Version).
.PHONY: test
test:
	$(GO) test -race ./...

## clean: elimina el directorio de salida.
.PHONY: clean
clean:
	rm -rf $(DISTDIR)

## help: lista los targets documentados.
.PHONY: help
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed -e 's/## //'
