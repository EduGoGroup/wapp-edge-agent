#!/usr/bin/env bash
# Entrypoint de la imagen "wapp-edge-installer-linux": compila y empaqueta el kit portable
# Linux/amd64 del Edge (`make dist-linux-amd64`) y copia el resultado al volumen montado en /out.
# El repo ya quedó clonado en /src durante `docker build` (ver Dockerfile); aquí solo se compila,
# para poder reusar la misma imagen con distintos ENROLLMENT_ENDPOINT sin reconstruirla.
set -euo pipefail

: "${ENROLLMENT_ENDPOINT:=gateway.wapp.example:8102}"

if [ ! -d /out ]; then
	echo "ERROR: no hay nada montado en /out. Corre con -v \"\$PWD/dist-out:/out\"." >&2
	exit 1
fi

cd /src
echo "== wapp-edge-agent @ $(git rev-parse --short HEAD 2>/dev/null || echo '?') =="
echo "== ENROLLMENT_ENDPOINT=$ENROLLMENT_ENDPOINT ${VERSION:+VERSION=$VERSION} =="

make dist-linux-amd64 ENROLLMENT_ENDPOINT="$ENROLLMENT_ENDPOINT" ${VERSION:+VERSION="$VERSION"}

cp -v dist/*.tar.gz /out/
echo "OK: instalador copiado a /out/"
