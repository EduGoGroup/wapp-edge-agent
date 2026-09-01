# Genera el kit portable Linux/amd64 del Edge de wApp (`wapp-edge-agent`) dentro de un contenedor,
# sin instalar el toolchain de Go en tu máquina. Ver packaging/docker/README.md para el uso
# completo y documentations/instalador.md (del propio repo wapp-edge-agent) para la matriz de qué
# plataformas se empaquetan hoy y por qué.
#
# Por qué ESTA plataforma sí se puede generar en Docker: el kit Linux es 100% pure-Go
# (CGO_ENABLED=0, sin el Keychain de macOS que exige CGO real) y `make dist-linux-amd64` solo
# necesita git + tar, ambos disponibles en cualquier Linux — a diferencia de macOS, cuyo `.pkg`
# exige pkgbuild/productbuild/hdiutil (herramientas que Apple no ofrece fuera de macOS).
#
# Uso:
#   docker build -t wapp-edge-installer-linux packaging/docker/linux
#   docker run --rm \
#     -e ENROLLMENT_ENDPOINT=gateway.tu-nube.example:8102 \
#     -v "$PWD/dist-out:/out" \
#     wapp-edge-installer-linux
#
# El resultado (wapp-edge-<version>-linux-amd64.tar.gz) queda en ./dist-out en tu máquina.

FROM golang:1.26.5-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
		git \
		ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

# Repo y ref a clonar — overridables en `docker build --build-arg`. Se resuelven en BUILD TIME
# porque fijan qué CÓDIGO se empaqueta; ENROLLMENT_ENDPOINT en cambio se resuelve en runtime
# (ver entrypoint.sh), porque solo cambia el bootstrap horneado en el artefacto final.
ARG WAPP_EDGE_REPO=https://github.com/EduGoGroup/wapp-edge-agent.git
ARG WAPP_EDGE_REF=main

WORKDIR /src
# Clon COMPLETO (sin --depth): `make dist-linux-amd64` inyecta la versión con
# `git describe --tags --always --dirty` (Makefile:31). Con historia truncada no encontraría el
# tag más cercano y el artefacto quedaría marcado como una versión "dev" genérica aunque exista un
# vX.Y.Z real más arriba en el historial.
RUN git clone --branch "${WAPP_EDGE_REF}" --single-branch "${WAPP_EDGE_REPO}" .

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV ENROLLMENT_ENDPOINT=gateway.wapp.example:8102

VOLUME ["/out"]
ENTRYPOINT ["/entrypoint.sh"]
