#!/usr/bin/env bash
# Genera el instalador .pkg/.dmg del Edge de wApp en una Mac REAL (o un runner macOS de CI).
#
# NO existe un equivalente Docker de este script: `make pkg`/`make dmg` invocan pkgbuild,
# productbuild y hdiutil — herramientas de Apple Installer que no existen fuera de macOS, ni
# siquiera dentro de un contenedor Linux corriendo en Docker Desktop para Mac (ese contenedor
# sigue siendo un kernel Linux). Además, `internal/adapters/keycustody/keychain_darwin.go` usa CGO
# real contra Security.framework/CoreFoundation: sin macOS real, el binario ni compila (no hay una
# ruta pure-Go alternativa en darwin — file.go, el fallback, está excluido con
# `//go:build !darwin`). Ver documentations/instalador.md para el detalle completo.
#
# Misma interfaz que packaging/docker/{linux,windows}/: WAPP_EDGE_REPO, WAPP_EDGE_REF,
# ENROLLMENT_ENDPOINT como variables de entorno, y un directorio de salida como argumento.
#
# Requisitos: macOS con Xcode Command Line Tools (pkgbuild/productbuild/hdiutil) instaladas y Go
# 1.26.5 (mismo toolchain que fija el Makefile/CI).
#
# Uso:
#   WAPP_EDGE_REF=v0.1.0 ENROLLMENT_ENDPOINT=gateway.tu-nube.example:8102 \
#     packaging/macos/build-native.sh [directorio-de-salida]
set -euo pipefail

if [ "$(uname -s)" != "Darwin" ]; then
	echo "ERROR: este script debe correr en macOS real — pkgbuild/productbuild/hdiutil no existen en otro SO." >&2
	exit 1
fi

command -v pkgbuild >/dev/null 2>&1 && command -v productbuild >/dev/null 2>&1 || {
	echo "ERROR: faltan pkgbuild/productbuild. Instala las Xcode Command Line Tools: xcode-select --install" >&2
	exit 1
}

WAPP_EDGE_REPO="${WAPP_EDGE_REPO:-https://github.com/EduGoGroup/wapp-edge-agent.git}"
WAPP_EDGE_REF="${WAPP_EDGE_REF:-main}"
ENROLLMENT_ENDPOINT="${ENROLLMENT_ENDPOINT:-gateway.wapp.example:8102}"
OUT_DIR="${1:-$(pwd)/dist-out}"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

echo "== clonando $WAPP_EDGE_REPO @ $WAPP_EDGE_REF =="
# Clon COMPLETO (sin --depth): `make pkg` inyecta la versión con `git describe --tags --always
# --dirty` (Makefile:31); con historia truncada degradaría a una versión "dev" genérica.
git clone --branch "$WAPP_EDGE_REF" --single-branch "$WAPP_EDGE_REPO" "$WORKDIR/src"

cd "$WORKDIR/src"
echo "== wapp-edge-agent @ $(git rev-parse --short HEAD) =="
echo "== ENROLLMENT_ENDPOINT=$ENROLLMENT_ENDPOINT ${VERSION:+VERSION=$VERSION} =="

make pkg ENROLLMENT_ENDPOINT="$ENROLLMENT_ENDPOINT" ${VERSION:+VERSION="$VERSION"}
make dmg

mkdir -p "$OUT_DIR"
cp -v dist/*.pkg dist/*.dmg "$OUT_DIR"/
echo "OK: instalador(es) copiados a $OUT_DIR"
