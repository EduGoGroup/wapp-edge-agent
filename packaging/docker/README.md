# Generar los instaladores del Edge por plataforma

Tres mecanismos, uno por SO, con la **misma interfaz de variables** (`WAPP_EDGE_REPO`,
`WAPP_EDGE_REF`, `ENROLLMENT_ENDPOINT`, opcionalmente `VERSION`). Dos son imágenes Docker; el
tercero es un script nativo porque **no hay forma de generar el `.pkg` de macOS dentro de un
contenedor** (ver la sección 4). Contexto completo de qué se empaqueta y para qué plataformas en
[`documentations/instalador.md`](../../documentations/instalador.md).

## 1 · Linux/amd64 — `packaging/docker/linux/`

```sh
docker build -t wapp-edge-installer-linux packaging/docker/linux

docker run --rm \
  -e ENROLLMENT_ENDPOINT=gateway.tu-nube.example:8102 \
  -v "$PWD/dist-out:/out" \
  wapp-edge-installer-linux
```

Resultado: `./dist-out/wapp-edge-<version>-linux-amd64.tar.gz`.

## 2 · Windows/amd64 — `packaging/docker/windows/`

```sh
docker build -t wapp-edge-installer-windows packaging/docker/windows

docker run --rm \
  -e ENROLLMENT_ENDPOINT=gateway.tu-nube.example:8102 \
  -v "$PWD/dist-out:/out" \
  wapp-edge-installer-windows
```

Resultado: `./dist-out/wapp-edge-<version>-windows-amd64.zip`. Es cross-compile pure-Go
(`CGO_ENABLED=0`) corriendo dentro de un contenedor **Linux** — no hace falta Windows en ningún
punto de la cadena.

## 3 · Una versión concreta en vez de `main`

`WAPP_EDGE_REF` se resuelve en **build time** (fija qué código se clona), no en runtime:

```sh
docker build -t wapp-edge-installer-linux \
  --build-arg WAPP_EDGE_REF=v0.1.0 \
  packaging/docker/linux
```

`ENROLLMENT_ENDPOINT` (y el opcional `VERSION=vX.Y.Z` para forzar el string de versión en vez del
que infiere `git describe`) se resuelven en **runtime** (`docker run -e ...`), a propósito: así una
misma imagen construida una vez sirve para generar kits contra distintos gateways (dev/UAT/prod)
sin reconstruirla.

## 4 · macOS/arm64 — por qué NO hay Dockerfile, y qué usar en su lugar

El `.pkg`/`.dmg` de macOS **no se puede generar con Docker**, en ninguna máquina, por dos motivos
que no son de esfuerzo sino de plataforma:

1. `internal/adapters/keycustody/keychain_darwin.go` usa **CGO real** contra
   `Security.framework`/`CoreFoundation` de Apple. Sin macOS real (o su SDK), el binario **no
   compila**: no hay una ruta pure-Go alternativa en darwin (`file.go`, el fallback, está excluido
   con `//go:build !darwin`).
2. `make pkg`/`make dmg` invocan `pkgbuild`, `productbuild` y `hdiutil` — herramientas de Apple
   Installer que **no existen fuera de macOS**, ni siquiera dentro de un contenedor Linux corriendo
   en Docker Desktop para Mac (ese contenedor sigue siendo un kernel Linux, no macOS).

Usa en su lugar [`packaging/macos/build-native.sh`](../macos/build-native.sh) en una Mac real (o un
runner macOS de CI, p. ej. `macos-14` en GitHub Actions) — misma interfaz de variables:

```sh
WAPP_EDGE_REF=v0.1.0 ENROLLMENT_ENDPOINT=gateway.tu-nube.example:8102 \
  packaging/macos/build-native.sh ./dist-out
```

Requiere Xcode Command Line Tools instaladas (`xcode-select --install`) y Go 1.26.5.

## 5 · Qué falta (por si se necesita)

Solo se empaquetan `linux/amd64`, `windows/amd64` y `darwin/arm64` — la matriz completa está en
[`documentations/instalador.md`](../../documentations/instalador.md#2--para-qué-plataforma-está-la-matriz-real).
`windows/arm64` y `linux/arm64` ya **compilan** (`make build-windows-arm64` / `build-linux-arm64`
existen), pero no hay targets `dist-windows-arm64` / `dist-linux-arm64` que arme el kit — añadirlos
al `Makefile` es una extensión trivial del macro `dist_stage` existente, pero es un cambio al
propio Makefile del repo, no algo que resuelvan estos Dockerfiles por sí solos.
