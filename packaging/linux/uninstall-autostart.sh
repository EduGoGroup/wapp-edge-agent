#!/usr/bin/env bash
# Desactiva el autoarranque del Edge de wApp en Linux (Plan 024 · T1).
# Hace `systemctl --user disable --now wapp-edge.service` y borra la unit.
# NO borra el data_dir ni los logs.
#
# Uso:  chmod +x uninstall-autostart.sh && ./uninstall-autostart.sh
#
# Si activaste linger para que sobreviva sin sesion, quitalo aparte con:
#     loginctl disable-linger $USER
set -euo pipefail

UNIT_PATH="$HOME/.config/systemd/user/wapp-edge.service"

if command -v systemctl >/dev/null 2>&1; then
	systemctl --user disable --now wapp-edge.service 2>/dev/null || true
fi

if [ -f "$UNIT_PATH" ]; then
	rm -f "$UNIT_PATH"
	echo "Eliminada la unit: $UNIT_PATH"
else
	echo "No habia unit de autostart en $UNIT_PATH (nada que borrar)."
fi

if command -v systemctl >/dev/null 2>&1; then
	systemctl --user daemon-reload || true
fi

echo "Autostart desactivado. El Edge ya no arrancara solo al iniciar sesion."
