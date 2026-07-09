#!/usr/bin/env bash
# Desinstala el LaunchAgent por-usuario del Edge (Plan 023 · T3). NO toca el data_dir (config/logs/store) ni
# el Keychain: solo descarga y borra el plist. Idempotente.
set -euo pipefail

LABEL="com.wapp.edge"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"

if [ "$(id -u)" = "0" ]; then
	echo "ERROR: desinstala como TU usuario, NO como root/sudo (el agente vive en tu dominio gui)." >&2
	exit 1
fi

UID_NUM="$(id -u)"
launchctl bootout "gui/$UID_NUM/$LABEL" 2>/dev/null || true
rm -f "$PLIST"

echo "LaunchAgent desinstalado ($LABEL)."
echo "El data_dir y el Keychain del usuario NO se han tocado (borra la app y el Keychain a mano si quieres)."
