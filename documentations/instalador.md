# El instalador del Edge

Cómo se construye, para qué plataformas existe, con qué permisos corre y qué le queda por hacer al
usuario después de instalarlo. Fuentes: `Makefile` (targets `pkg`/`dist-*`) y `packaging/` (macOS,
Windows, Linux).

---

## 1 · Qué hay, en concreto

No hay UN instalador: hay **tres mecanismos distintos**, uno por familia de sistema operativo,
todos generados desde el mismo `Makefile`:

| SO | Mecanismo | Target `make` | Salida |
|---|---|---|---|
| macOS | `.pkg` de Apple Installer (+ `.dmg` opcional) | `make pkg` / `make dmg` | `dist/wApp-Edge-<version>.pkg` |
| Windows | Kit portable `.zip` (descomprimir y ejecutar, sin instalador real) | `make dist-windows-amd64` | `dist/wapp-edge-<version>-windows-amd64.zip` |
| Linux | Kit portable `.tar.gz` (descomprimir y ejecutar) | `make dist-linux-amd64` | `dist/wapp-edge-<version>-linux-amd64.tar.gz` |

🔧 **Generarlos sin instalar el toolchain de Go**: `packaging/docker/{linux,windows}/` trae un
`Dockerfile` por plataforma que clona el repo, compila y empaqueta dentro del contenedor, dejando
el resultado en un volumen (`docker run -v $PWD/dist-out:/out ...`) — ver
[`packaging/docker/README.md`](../packaging/docker/README.md). El `.pkg` de macOS **no tiene
equivalente Docker posible** (pkgbuild/productbuild/hdiutil no existen fuera de macOS, y el
Keychain usa CGO real contra frameworks de Apple): en su lugar,
[`packaging/macos/build-native.sh`](../packaging/macos/build-native.sh) hace lo mismo en una Mac
real o un runner macOS de CI, con la misma interfaz de variables.

Los tres empaquetan lo mismo: los binarios `agent` + `wapp-ctl` (layout **hermano**, en el mismo
directorio — así los resuelve `defaultAgentBin()` en `cmd/wapp-ctl/main.go:186-194`), un
`config.yaml` de **bootstrap** con el endpoint de enrolamiento y el `ca.pem` (TLSCA) **públicos**, y
los artefactos de autoarranque de ese SO. `colaseed` (la herramienta de carga sintética) **nunca se
empaqueta** (`Makefile:35`, `CMDS := agent wapp-ctl`).

Antes de construir cualquiera de los tres, `verify-zero-knowledge.sh` corre sobre el staging y
**aborta** si detecta una clave privada PEM, un fichero `.key`/`edge.crt`/`dek*`, o un
`activation_code` con valor (R6 / ADR-0007): en el paquete solo puede viajar material **público**.

---

## 2 · Para qué plataforma está: la matriz real

🔴 **No es una matriz completa** — hay una diferencia entre "compila" (`build-*`) y "se empaqueta y
se entrega" (`pkg` / `dist-*`), y solo el segundo importa para un usuario final:

| Plataforma | ¿Compila? (`make build-*`) | ¿Se empaqueta? (`make pkg`/`dist-*`) |
|---|---|---|
| macOS arm64 (Apple Silicon) | ✅ `build-darwin-arm64` (DoD del plan, con CGO=1 para el Keychain) | ✅ `.pkg`/`.dmg` — **el único artefacto de macOS que existe** |
| macOS amd64 (Intel) | ❌ no hay target | ❌ no existe |
| Windows amd64 | ✅ `build-windows-amd64` | ✅ kit `.zip` |
| Windows arm64 | ✅ `build-windows-arm64` (declarado como "andamiaje") | ❌ no hay `dist-windows-arm64` |
| Linux amd64 | ✅ `build-linux-amd64` | ✅ kit `.tar.gz` |
| Linux arm64 | ✅ `build-linux-arm64` (declarado como "andamiaje") | ❌ no hay `dist-linux-arm64` |

En otras palabras: **hoy solo se le puede entregar un instalador a tres combinaciones** —
macOS/arm64, Windows/amd64 y Linux/amd64. Windows/arm64 y Linux/arm64 **compilan** (el binario
existe si alguien corre `make build-windows-arm64` o `build-linux-arm64` a mano) pero no hay un
target que arme el kit final, y macOS/Intel no tiene ni siquiera el `build-*`.

El propio `Makefile` (líneas 9-11) es honesto sobre esto: *"Alcance del plan (DoD T0): SOLO
darwin/arm64 se compila y prueba aquí. El resto de la matriz de 5 targets [...] queda DECLARADO
como andamiaje para el release multiplataforma futuro; no forma parte del DoD."* Todo es pure-Go
(`modernc.org/sqlite`, `CGO_ENABLED=0`) salvo el Keychain de macOS, así que el cross-compile del
resto de la matriz "sale gratis" el día que alguien decida empaquetarlo — pero hoy no está hecho.

---

## 3 · Qué permisos tiene al ejecutar: por-usuario, NUNCA root/admin

La regla es la misma en los tres sistemas operativos, y **no es una preferencia de estilo**: es
consecuencia directa del ADR-0007 (doble llave). La **DEK** vive en el keystore **del usuario que
inicia sesión** (`internal/adapters/keycustody`: Keychain en macOS, DPAPI en Windows, Secret
Service en Linux, con un archivo `0600` como fallback si el SO no ofrece nada mejor —
`NewFileCustody` en realidad devuelve uno de esos tres, y solo `file_fallback.go` es el archivo
literal). Un proceso de sistema (root en Unix, una cuenta de servicio en Windows) **no vería** ese
keystore. Por eso:

| SO | Dónde se aplica la regla | Qué pasa si se ejecuta como root/admin |
|---|---|---|
| macOS | `postinstall` (`packaging/macos/scripts/postinstall:16-19`) y `install-launchagent.sh:16-20` comprueban `id -u = 0` y **abortan** con un mensaje explícito | El `.pkg` se instala bajo `~/Library/Application Support/wApp`, nunca `/Library`; el LaunchAgent es **por-usuario** (`launchctl bootstrap gui/<uid>`), nunca un `LaunchDaemon` de sistema |
| Linux | `install-autostart.sh:17-21` comprueba `id -u = 0` y **aborta** | La unidad systemd es **`--user`** (`~/.config/systemd/user/`), no una unit de sistema; para que sobreviva sin sesión abierta hace falta `loginctl enable-linger $USER`, no un servicio root |
| Windows | `install-autostart.ps1` — comentario explícito: *"Es POR-USUARIO (sin admin)"* | El acceso directo va a la carpeta `Startup` **del usuario** (no la de "todos los usuarios"), y usa el registro/Task Scheduler por-usuario si se opta por la alternativa documentada |

Consecuencias prácticas para quien instala:

- **macOS**: el `.pkg` **no está firmado ni notarizado** (decisión D1, deliberada — ver
  `packaging/macos/README.md`). Gatekeeper bloquea el doble-click con *"no se puede abrir porque
  proviene de un desarrollador no identificado"*. Se sortea con **clic-derecho (Control-clic) →
  Abrir** y confirmar, o quitando la cuarentena a mano (`xattr -d com.apple.quarantine`). El
  `Distribution.xml` además fija `enable_localSystem="false"` y `enable_anywhere="false"`: el propio
  instalador de Apple solo ofrece "instalar para mí".
- **Windows**: no hay firma de código, así que SmartScreen avisa "Windows protegió tu PC"; se
  sortea con **Más información → Ejecutar de todas formas** (recogido en
  `packaging/common/README.txt:19`). El kit no es un instalador de verdad, es un `.zip` que se
  descomprime donde el usuario quiera.
- **Linux**: no hay firma que aplique (no es una plataforma con Gatekeeper/SmartScreen), pero el
  kit exige `chmod +x` antes de poder ejecutarse, y `install-autostart.sh` exige que exista
  `systemctl` (si no, dirige a la alternativa XDG autostart documentada en su propia cabecera).

**Ninguno de los tres pide una contraseña de administrador en ningún paso.** Si algo la pide, no es
el flujo esperado.

---

## 4 · Qué tiene que hacer el usuario después de instalar

La instalación (copiar binarios + plantillas) es solo la mitad. Lo que deja operativo el Edge es
un flujo de **cuatro pasos**, igual en las tres plataformas porque lo sirve la misma web embebida
de `wapp-ctl`:

1. **Arrancar `wapp-ctl`** (no `agent` directo — el núcleo lo arranca el supervisor):
   - macOS: lo hace solo el `postinstall` vía LaunchAgent, y abre el navegador en la primera
     instalación (`sleep 2; open http://127.0.0.1:8105`).
   - Windows: doble-click en `wapp-ctl.exe` (o `run-edge.cmd` si ya se activó el autoarranque).
   - Linux: `./wapp-ctl` desde una terminal en la carpeta del kit.
2. **Abrir `http://127.0.0.1:8105`** en el navegador (se abre solo en macOS; en Windows/Linux hay
   que escribirlo a mano si no se abre automáticamente).
3. **Enrolar**: pegar el **código de activación** de un solo uso que le pasó quien lo invitó, en la
   web (equivalente a `agent enroll <código>` por CLI). Este paso es el que genera y persiste **en
   ese equipo** el par de credenciales mTLS (`edge.crt`/`edge.key`) y deriva el endpoint de runtime
   de CloudLink — nada de esto viaja en el paquete, lo crea el enroll.
4. **Emparejar WhatsApp por QR**: aparece un código QR en la misma web (se genera y se escanea en
   el propio equipo, `internal/webui`); en el teléfono, WhatsApp → Dispositivos vinculados →
   Vincular un dispositivo → escanear. Hay un modo ASCII por terminal como alternativa a la web.

Después de esos cuatro pasos, **el Edge tiene que seguir corriendo** para enviar y recibir — no es
un instalador de "usar y cerrar". Dos caminos, y conviene decírselos al usuario explícitamente:

- **Dejar la ventana/terminal abierta** (Windows/Linux) mientras se prueba. Es lo mínimo, pero no
  sobrevive a un reinicio ni a cerrar sesión.
- **Activar el autoarranque** para que sobreviva a un reinicio:
  - macOS: ya viene activado por el propio `postinstall` (no hay paso extra).
  - Windows: `powershell -ExecutionPolicy Bypass -File install-autostart.ps1` dentro de la carpeta
    del kit (`uninstall-autostart.ps1` para revertir).
  - Linux: `chmod +x install-autostart.sh && ./install-autostart.sh`, y si además se quiere que
    siga vivo **sin sesión gráfica abierta**: `loginctl enable-linger $USER` (paso que el propio
    script recuerda al terminar).

**Verificación de que quedó bien** (útil como checklist post-instalación, no solo para depurar):

```bash
curl -s http://127.0.0.1:8105/v1/daemon/status   # el núcleo (agent serve) está arriba
curl -s http://127.0.0.1:8105/v1/cajero/status   # el worker de inferencia está arriba (si aplica)
```

Los logs quedan en rutas fijas por SO (útiles si algo no arranca):

| SO | Ruta de logs |
|---|---|
| macOS | `~/Library/Application Support/wApp/edge/logs/edge.{out,err}.log` |
| Windows | `%AppData%\wApp\edge\logs\edge.log` |
| Linux | `~/.config/wApp/edge/logs/edge.log` (o `~/.config/systemd/user/` si se usó el autostart) |

---

## 5 · Lo que este instalador todavía NO hace (para no prometer de más)

- **Sin firma ni notarización** en ninguna plataforma (D1, deliberado): el usuario siempre ve un
  aviso de SO no confiable en el primer arranque.
- **Sin auto-update**: en v1, actualizar = descargar e instalar/descomprimir la versión nueva
  encima. No hay un mecanismo que avise de una versión nueva ni que la aplique solo.
- **La ventana de consola queda visible** en Windows y Linux incluso con autoarranque activado
  (ocultarla es un follow-up documentado, no un bloqueo).
- **No hay instalador real en Windows/Linux**, son kits portables: nada que desinstale limpiamente
  salvo borrar la carpeta a mano (y correr el `uninstall-autostart.*` correspondiente primero).
