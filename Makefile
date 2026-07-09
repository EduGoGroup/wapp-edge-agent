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

# --- Instalador macOS (T4) — .pkg/.dmg SIN firmar (D1) ---------------------
# Empaqueta los 2 binarios (layout hermano) BAJO EL HOME (por-usuario, sin root) + bootstrap PÚBLICO
# (TLSCA/endpoint) + el LaunchAgent (reusa packaging/macos de T3). Zero-knowledge (R6): pkg-verify-zk
# aborta si se cuela material secreto. Solo corre en macOS (pkgbuild/productbuild/hdiutil); el CI no lo llama.
PKG_ID        := com.wapp.edge
PKG_OUT       := $(DISTDIR)/wApp-Edge-$(VERSION).pkg
DMG_OUT       := $(DISTDIR)/wApp-Edge-$(VERSION).dmg
PKG_BUILD     := build/pkg
PKG_MACOS     := packaging/macos
BOOTSTRAP_DIR ?= $(PKG_MACOS)/bootstrap

## pkg: instalador .pkg por-usuario SIN firmar (macOS; consume dist/darwin-arm64/). Decisión D1.
.PHONY: pkg
pkg: build-darwin-arm64
	@command -v pkgbuild >/dev/null 2>&1 && command -v productbuild >/dev/null 2>&1 || \
		{ echo "make pkg requiere macOS (pkgbuild/productbuild)"; exit 1; }
	rm -rf $(PKG_BUILD)
	mkdir -p $(PKG_BUILD)/root/bin $(PKG_BUILD)/scripts $(PKG_BUILD)/flat
	cp $(DISTDIR)/darwin-arm64/agent $(DISTDIR)/darwin-arm64/wapp-ctl $(PKG_BUILD)/root/bin/
	chmod 755 $(PKG_BUILD)/root/bin/agent $(PKG_BUILD)/root/bin/wapp-ctl
	cp $(PKG_MACOS)/scripts/postinstall $(PKG_BUILD)/scripts/postinstall
	cp $(PKG_MACOS)/install-launchagent.sh $(PKG_BUILD)/scripts/install-launchagent.sh
	cp $(PKG_MACOS)/com.wapp.edge.plist.template $(PKG_BUILD)/scripts/com.wapp.edge.plist.template
	cp $(BOOTSTRAP_DIR)/config.yaml.template $(PKG_BUILD)/scripts/config.yaml.template
	cp $(BOOTSTRAP_DIR)/ca.pem $(PKG_BUILD)/scripts/ca.pem
	chmod 755 $(PKG_BUILD)/scripts/postinstall $(PKG_BUILD)/scripts/install-launchagent.sh
	bash $(PKG_MACOS)/verify-zero-knowledge.sh $(PKG_BUILD)
	pkgbuild --root $(PKG_BUILD)/root \
		--install-location "Library/Application Support/wApp" \
		--scripts $(PKG_BUILD)/scripts \
		--identifier $(PKG_ID) --version $(VERSION) \
		$(PKG_BUILD)/flat/wapp-edge-component.pkg
	productbuild --distribution $(PKG_MACOS)/Distribution.xml \
		--package-path $(PKG_BUILD)/flat \
		$(PKG_OUT)
	@echo "OK: $(PKG_OUT)"
	@echo "SIN firmar (D1) — Gatekeeper: click-derecho -> Abrir. Ver $(PKG_MACOS)/README.md"

## pkg-verify-zk: guarda zero-knowledge del staging del .pkg (falla si hay material secreto).
.PHONY: pkg-verify-zk
pkg-verify-zk:
	bash $(PKG_MACOS)/verify-zero-knowledge.sh $(PKG_BUILD)

## dmg: envuelve el .pkg en un .dmg (opcional; macOS, hdiutil).
.PHONY: dmg
dmg: pkg
	@command -v hdiutil >/dev/null 2>&1 || { echo "make dmg requiere macOS (hdiutil)"; exit 1; }
	rm -f $(DMG_OUT)
	hdiutil create -volname "wApp Edge" -srcfolder $(PKG_OUT) -ov -format UDZO $(DMG_OUT)
	@echo "OK: $(DMG_OUT)"

# --- Artefactos portables Windows/Linux (Plan 024 · T0) --------------------
# .zip (Windows) / .tar.gz (Linux) con los 2 binarios (layout hermano, .exe en Windows) +
# bootstrap PÚBLICO (ca.pem + config.yaml) + README mínimo. TODO pure-Go (CGO=0): el Keychain
# es //go:build darwin, no alcanza a Win/Linux → el cross-build sale gratis desde el Mac.
#
# Insumos bootstrap: se REUSA el ca.pem público de macOS ($(BOOTSTRAP_DIR), TLSCA OS-agnóstico,
# fuente única) pero el config.yaml sale de una plantilla PROPIA (packaging/common): la de macOS
# lleva marcadores @@DATA_DIR@@ que sustituye el postinstall del .pkg con rutas absolutas; el kit
# portable se descomprime en cualquier carpeta, así que usa rutas relativas y omite data_dir
# (RUTA SAGRADA por SO). Zero-knowledge (R6): se corre verify-zero-knowledge.sh sobre el staging.
COMMON_DIR          ?= packaging/common
# Endpoint de enrolamiento embebido en el config de bootstrap. Override:
# `make dist-linux-amd64 ENROLLMENT_ENDPOINT=gateway.tu-nube:8102`.
ENROLLMENT_ENDPOINT ?= gateway.wapp.example:8102
DIST_STAGE          := build/dist
ZIP_OUT             := $(DISTDIR)/wapp-edge-$(VERSION)-windows-amd64.zip
TGZ_OUT             := $(DISTDIR)/wapp-edge-$(VERSION)-linux-amd64.tar.gz

# dist_stage(os,arch,stage): copia los 2 binarios (con .exe en windows) + ca.pem público +
# config.yaml (endpoint sustituido) + README.txt + los artefactos de autoarranque por-SO
# (packaging/<os>/, Plan 024 · T1) al staging, y corre la guarda zero-knowledge.
# Los artefactos de autostart son PÚBLICOS (scripts/plantillas sin secretos): Windows lleva
# run-edge.cmd + install/uninstall-autostart.ps1; Linux la unit template + install/uninstall-autostart.sh.
define dist_stage
	rm -rf $(3)
	mkdir -p $(3)
	cp $(DISTDIR)/$(1)-$(2)/agent$(if $(filter windows,$(1)),.exe,) $(3)/
	cp $(DISTDIR)/$(1)-$(2)/wapp-ctl$(if $(filter windows,$(1)),.exe,) $(3)/
	cp $(BOOTSTRAP_DIR)/ca.pem $(3)/ca.pem
	sed 's|@@ENROLLMENT_ENDPOINT@@|$(ENROLLMENT_ENDPOINT)|g' $(COMMON_DIR)/config.yaml.template > $(3)/config.yaml
	cp $(COMMON_DIR)/README.txt $(3)/README.txt
	cp packaging/$(1)/* $(3)/
	$(if $(filter linux,$(1)),chmod +x $(3)/install-autostart.sh $(3)/uninstall-autostart.sh,)
	bash $(PKG_MACOS)/verify-zero-knowledge.sh $(3)
endef

## dist-windows-amd64: artefacto portable .zip (Win amd64) — 2 binarios + bootstrap público.
.PHONY: dist-windows-amd64
dist-windows-amd64: build-windows-amd64
	$(call dist_stage,windows,amd64,$(DIST_STAGE)/windows-amd64)
	@mkdir -p $(DISTDIR)
	rm -f $(ZIP_OUT)
	cd $(DIST_STAGE)/windows-amd64 && zip -q -X -r "$(abspath $(ZIP_OUT))" .
	@echo "OK: $(ZIP_OUT)"

## dist-linux-amd64: artefacto portable .tar.gz (Linux amd64) — 2 binarios + bootstrap público.
.PHONY: dist-linux-amd64
dist-linux-amd64: build-linux-amd64
	$(call dist_stage,linux,amd64,$(DIST_STAGE)/linux-amd64)
	@mkdir -p $(DISTDIR)
	rm -f $(TGZ_OUT)
	tar -C $(DIST_STAGE)/linux-amd64 -czf $(TGZ_OUT) .
	@echo "OK: $(TGZ_OUT)"

## dist-all: ambos artefactos portables (Windows .zip + Linux .tar.gz).
.PHONY: dist-all
dist-all: dist-windows-amd64 dist-linux-amd64

## clean: elimina los directorios de salida (dist/ y el staging del .pkg).
.PHONY: clean
clean:
	rm -rf $(DISTDIR) build

## help: lista los targets documentados.
.PHONY: help
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed -e 's/## //'
