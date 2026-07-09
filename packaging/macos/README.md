# Instalador macOS del Edge de wApp (Plan 023 · T4)

Instalador **por-usuario, bajo el HOME, SIN root** y **SIN firmar** (decisión **D1**). Empaqueta los dos
binarios del Edge (`agent` + `wapp-ctl`, layout hermano), el **LaunchAgent** por-usuario (T3) y un
**bootstrap público** (TLSCA + endpoint de enrolamiento). El usuario enrola y empareja por la web
(`http://127.0.0.1:8765`); el par mTLS lo genera el enroll (T1) y la DEK vive en el Keychain (T2).

## Qué instala

| Artefacto | Destino |
|---|---|
| `agent`, `wapp-ctl` | `~/Library/Application Support/wApp/bin/` (mismo dir, layout hermano) |
| `config.yaml`, `ca.pem` (bootstrap) | `~/Library/Application Support/wApp/edge/` (`<data_dir>`, **sin sobrescribir** uno existente) |
| `com.wapp.edge.plist` (LaunchAgent) | `~/Library/LaunchAgents/` (cargado con `launchctl bootstrap gui/<uid>`) |
| logs | `~/Library/Application Support/wApp/edge/logs/edge.{out,err}.log` |

Tras instalar, el LaunchAgent arranca `wapp-ctl` (con `--autostart`, que levanta `agent serve`) al iniciar
sesión, y el `postinstall` abre la web de onboarding en la primera instalación.

## Zero-knowledge (invariante, R6 / ADR-0007)

En el paquete viaja **SOLO material PÚBLICO**: el `TLSCA` de bootstrap y el `enrollment_endpoint`. **NUNCA**
la DEK, ni claves privadas mTLS (`tls_key`/`tls_cert`), ni `activation_code`. Esos se generan y custodian en
el equipo durante el enroll y en el Keychain. El target `make pkg` corre `verify-zero-knowledge.sh` sobre el
staging y **aborta** si detecta material secreto.

## Construir

Requisitos: macOS (con `pkgbuild`/`productbuild`, y `hdiutil` para el `.dmg`) y el toolchain Go.

```sh
# 1) (recomendado) reemplaza el bootstrap por los valores reales de tu nube:
#    - packaging/macos/bootstrap/ca.pem                → el TLSCA REAL (PEM público) del Gateway
#    - WAPP_ENROLLMENT_ENDPOINT=host:puerto            → endpoint de enrolamiento real
# 2) compila los binarios darwin/arm64 y arma el .pkg:
make pkg                       # → dist/wApp-Edge-<version>.pkg
make dmg                       # opcional → dist/wApp-Edge-<version>.dmg (envoltorio)
```

`make pkg` compila `dist/darwin-arm64/` (T0), valida el zero-knowledge y construye un `.pkg` por-usuario.

## Instalar (Gatekeeper — SIN firma, D1)

Al no llevar firma Developer ID + notarización, **Gatekeeper** bloquea el doble-click con
*"no se puede abrir porque proviene de un desarrollador no identificado"*. Para la máquina del usuario:

- **Click-derecho (Control-clic) → Abrir** sobre el `.pkg`, y confirmar **Abrir** en el diálogo. **o**
- Quitar la marca de cuarentena y abrir normal:
  ```sh
  xattr -d com.apple.quarantine ~/Downloads/wApp-Edge-*.pkg
  ```

> Instala **para tu usuario** (no uses `sudo`): el instalador coloca todo bajo tu HOME y el LaunchAgent
> necesita ver **tu** Keychain (la DEK, T2). Un `LaunchDaemon` de sistema (root) no lo vería.

## Desinstalar

```sh
packaging/macos/uninstall-launchagent.sh          # descarga y borra el LaunchAgent
rm -rf "$HOME/Library/Application Support/wApp"    # binarios + data_dir (config/logs/store) — OPCIONAL
# La DEK del Keychain se borra al desvincular las sesiones (Clear) o desde Acceso a Llaveros.
```

## Follow-up (fuera del alcance de v1 / D1)

- **Firma y notarización** para distribución real: *Developer ID Application* (binarios) + *Developer ID
  Installer* (el `.pkg`), **hardened runtime**, `notarytool submit` + `stapler staple`. Elimina el bloqueo
  de Gatekeeper sin `xattr`.
- **Auto-update** (D3, diferido): en v1 actualizar = reinstalar el `.pkg`.
- Matriz multiplataforma (Windows/Linux) y sus custodios (DPAPI/Secret Service): Plan 024.
