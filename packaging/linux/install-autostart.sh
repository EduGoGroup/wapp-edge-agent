#!/usr/bin/env bash
# Instala el autoarranque del Edge de wApp en Linux (Plan 024 · T1) como systemd USER unit.
#
# USER unit (systemctl --user), NO de sistema: la DEK vive en el keystore del USUARIO
# (archivo 0600 en v1); un servicio de sistema (root) no la veria. Arranca wapp-ctl
# (supervisor) --no-open --autostart, que a su vez lanza `agent serve`.
#
# Uso (en una terminal, dentro de la carpeta del kit):
#     chmod +x install-autostart.sh && ./install-autostart.sh
#
# Alternativa documentada — autostart XDG (si no hay systemd --user, p.ej. entornos
# minimalistas): crear ~/.config/autostart/wapp-edge.desktop con
#   [Desktop Entry]\nType=Application\nExec=<INSTALL_DIR>/wapp-ctl --no-open --autostart
# (no fija Restart ni el entorno estable: preferir systemd cuando exista).
set -euo pipefail

if [ "$(id -u)" = "0" ]; then
	echo "ERROR: instala el autostart como TU usuario, NO como root/sudo." >&2
	echo "       La DEK vive en el keystore del usuario (archivo 0600); un servicio de sistema no la veria." >&2
	exit 1
fi

# INSTALL_DIR = carpeta de ESTE script = carpeta del kit (wapp-ctl y agent hermanos).
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="$HERE"
# RUTA SAGRADA por-SO: data_dir estable (NO el CWD).
DATA_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/wApp/edge"
TEMPLATE="$HERE/wapp-edge.service.template"
UNIT_DIR="$HOME/.config/systemd/user"
UNIT_PATH="$UNIT_DIR/wapp-edge.service"
LOGS_DIR="$DATA_DIR/logs"

[ -f "$TEMPLATE" ]              || { echo "ERROR: no encuentro la plantilla en $TEMPLATE" >&2; exit 1; }
[ -x "$INSTALL_DIR/wapp-ctl" ] || { echo "ERROR: no encuentro wapp-ctl ejecutable en $INSTALL_DIR" >&2; exit 1; }
[ -x "$INSTALL_DIR/agent" ]    || echo "AVISO: no encuentro 'agent' ejecutable en $INSTALL_DIR (wapp-ctl lo necesita hermano)." >&2

command -v systemctl >/dev/null 2>&1 || {
	echo "ERROR: no hay systemctl. Usa la alternativa autostart XDG (ver cabecera de este script)." >&2
	exit 1
}

mkdir -p "$UNIT_DIR" "$LOGS_DIR"

# Render de la unit (rutas absolutas; systemd no expande ~ ni $HOME). '|' como delimitador porque hay '/'.
sed \
	-e "s|@@INSTALL_DIR@@|$INSTALL_DIR|g" \
	-e "s|@@DATA_DIR@@|$DATA_DIR|g" \
	"$TEMPLATE" > "$UNIT_PATH"

systemctl --user daemon-reload
systemctl --user enable --now wapp-edge.service

echo "Autostart (systemd --user) activado: $UNIT_PATH"
echo "  wapp-ctl : $INSTALL_DIR/wapp-ctl (--no-open --autostart)"
echo "  agent    : $INSTALL_DIR/agent"
echo "  data_dir : $DATA_DIR"
echo "  logs     : $LOGS_DIR/edge.log"
echo "Plano de control: http://127.0.0.1:8765"
echo
echo "Para que siga corriendo SIN sesion abierta:  loginctl enable-linger $USER"
echo "Estado:   systemctl --user status wapp-edge.service"
