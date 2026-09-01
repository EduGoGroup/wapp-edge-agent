#!/usr/bin/env bash
# Entrypoint de la imagen "wapp-edge-installer-windows": compila y empaqueta el kit portable
# Windows/amd64 del Edge (`make dist-windows-amd64`) y copia el resultado al volumen montado en
# /out. El cross-compile a Windows es pure-Go (CGO_ENABLED=0, sin el Keychain de macOS), así que
# corre sin problema dentro de este contenedor Linux.
set -euo pipefail

: "${ENROLLMENT_ENDPOINT:=gateway.wapp.example:8102}"

if [ ! -d /out ]; then
	echo "ERROR: no hay nada montado en /out. Corre con -v \"\$PWD/dist-out:/out\"." >&2
	exit 1
fi

cd /src
echo "== wapp-edge-agent @ $(git rev-parse --short HEAD 2>/dev/null || echo '?') =="
echo "== ENROLLMENT_ENDPOINT=$ENROLLMENT_ENDPOINT ${VERSION:+VERSION=$VERSION} =="

make dist-windows-amd64 ENROLLMENT_ENDPOINT="$ENROLLMENT_ENDPOINT" ${VERSION:+VERSION="$VERSION"}

cp -v dist/*.zip /out/
echo "OK: instalador copiado a /out/"
