# Genera el kit portable Windows/amd64 del Edge de wApp (`wapp-edge-agent`) dentro de un
# contenedor LINUX (cross-compile pure-Go, sin necesitar Windows). Ver packaging/docker/README.md
# para el uso completo y documentations/instalador.md del propio repo para la matriz de qué
# plataformas se empaquetan hoy y por qué.
#
# Uso:
#   docker build -t wapp-edge-installer-windows packaging/docker/windows
#   docker run --rm \
#     -e ENROLLMENT_ENDPOINT=gateway.tu-nube.example:8102 \
#     -v "$PWD/dist-out:/out" \
#     wapp-edge-installer-windows
#
# El resultado (wapp-edge-<version>-windows-amd64.zip) queda en ./dist-out en tu máquina.

FROM golang:1.26.5-bookworm

# `zip` no viene en la imagen base y `make dist-windows-amd64` lo invoca directo (Makefile:232).
RUN apt-get update && apt-get install -y --no-install-recommends \
		git \
		ca-certificates \
		zip \
	&& rm -rf /var/lib/apt/lists/*

ARG WAPP_EDGE_REPO=https://github.com/EduGoGroup/wapp-edge-agent.git
ARG WAPP_EDGE_REF=main

WORKDIR /src
# Clon COMPLETO (sin --depth): ver la nota equivalente en packaging/docker/linux/Dockerfile —
# `git describe --tags` necesita la historia completa para no degradar a una versión "dev".
RUN git clone --branch "${WAPP_EDGE_REF}" --single-branch "${WAPP_EDGE_REPO}" .

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV ENROLLMENT_ENDPOINT=gateway.wapp.example:8102

VOLUME ["/out"]
ENTRYPOINT ["/entrypoint.sh"]
