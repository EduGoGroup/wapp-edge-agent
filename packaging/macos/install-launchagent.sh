#!/usr/bin/env bash
# Instala el LaunchAgent POR-USUARIO del Edge de wApp (Plan 023 · T3).
#
# ⛔ NUNCA LaunchDaemon: la DEK vive en el Keychain del USUARIO (T2); un daemon de sistema (root) no lo
# vería. Por eso este script EXIGE correr como tu usuario (no sudo) y usa `launchctl … gui/<uid>`.
#
# Rutas ABSOLUTAS: launchd no expande ~ ni $HOME en el plist. Overridables por env (útil para el .pkg de T4):
#   WAPP_BIN_DIR        (default: ~/Library/Application Support/wApp/bin)   — donde están agent y wapp-ctl (hermanos)
#   WAPP_AGENT_DATA_DIR (default: ~/Library/Application Support/wApp/edge)  — data_dir (config.yaml + logs + store)
set -euo pipefail

LABEL="com.wapp.edge"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="$HERE/com.wapp.edge.plist.template"

if [ "$(id -u)" = "0" ]; then
	echo "ERROR: instala el LaunchAgent como TU usuario, NO como root/sudo." >&2
	echo "       La DEK vive en el Keychain del usuario (T2); un LaunchDaemon de sistema no la vería." >&2
	exit 1
fi

BIN_DIR="${WAPP_BIN_DIR:-$HOME/Library/Application Support/wApp/bin}"
DATA_DIR="${WAPP_AGENT_DATA_DIR:-$HOME/Library/Application Support/wApp/edge}"
LA_DIR="$HOME/Library/LaunchAgents"
PLIST="$LA_DIR/$LABEL.plist"
LOGS_DIR="$DATA_DIR/logs"

CTL_BIN="$BIN_DIR/wapp-ctl"
AGENT_BIN="$BIN_DIR/agent"
CONFIG_PATH="$DATA_DIR/config.yaml"
STDOUT_LOG="$LOGS_DIR/edge.out.log"
STDERR_LOG="$LOGS_DIR/edge.err.log"

[ -f "$TEMPLATE" ] || { echo "ERROR: no encuentro la plantilla del plist en $TEMPLATE" >&2; exit 1; }
[ -x "$CTL_BIN" ]  || { echo "ERROR: no encuentro wapp-ctl ejecutable en $CTL_BIN" >&2; exit 1; }
[ -x "$AGENT_BIN" ] || echo "AVISO: no encuentro 'agent' en $AGENT_BIN (wapp-ctl lo necesita como hermano)." >&2

mkdir -p "$LA_DIR" "$LOGS_DIR"

# Render del plist desde la plantilla (mismos tokens @@...@@ que valida el test Go RenderPlist). Se usa '|'
# como delimitador de sed porque las rutas llevan '/'.
sed \
	-e "s|@@WAPP_CTL_BIN@@|$CTL_BIN|g" \
	-e "s|@@AGENT_BIN@@|$AGENT_BIN|g" \
	-e "s|@@DATA_DIR@@|$DATA_DIR|g" \
	-e "s|@@CONFIG_PATH@@|$CONFIG_PATH|g" \
	-e "s|@@STDOUT_LOG@@|$STDOUT_LOG|g" \
	-e "s|@@STDERR_LOG@@|$STDERR_LOG|g" \
	"$TEMPLATE" > "$PLIST"

# Validar el plist antes de cargarlo (si plutil está disponible).
if command -v plutil >/dev/null 2>&1; then
	plutil -lint "$PLIST" >/dev/null
fi

UID_NUM="$(id -u)"
# Idempotente: si ya estaba cargado, descárgalo antes (ignora el error si no existía).
launchctl bootout "gui/$UID_NUM/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$UID_NUM" "$PLIST"
# Arranca ya (sin esperar al próximo login) y reinicia si estaba a medias.
launchctl kickstart -k "gui/$UID_NUM/$LABEL"

echo "LaunchAgent instalado: $PLIST"
echo "  wapp-ctl : $CTL_BIN (--no-open --autostart)"
echo "  agent    : $AGENT_BIN"
echo "  data_dir : $DATA_DIR"
echo "  logs     : $STDOUT_LOG  |  $STDERR_LOG"
echo "Plano de control: http://127.0.0.1:8765"
