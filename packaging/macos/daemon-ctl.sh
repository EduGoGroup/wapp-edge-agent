#!/usr/bin/env bash
# Suspender/reanudar el núcleo (agent serve) SIN desemparejar, vía el plano de control loopback (Plan 023 ·
# T3). Encaja con ADR-0014: el control dispara ciclo de vida; NUNCA toca la DEK. El supervisor (wapp-ctl)
# sigue vivo bajo el LaunchAgent (KeepAlive); esto solo pausa/reanuda la escucha del núcleo.
#
# Alternativa "dura" (parar TODO, incluido el supervisor): launchctl bootout/bootstrap (ver *-launchagent.sh).
set -euo pipefail

ADDR="${WAPP_CTL_ADDR:-127.0.0.1:8765}"
BASE="http://$ADDR/v1/daemon"

case "${1:-}" in
	stop | suspend)
		curl -fsS -X POST "$BASE/stop" && echo
		;;
	start | resume)
		curl -fsS -X POST "$BASE/start" && echo
		;;
	status)
		curl -fsS "$BASE/status" && echo
		;;
	*)
		echo "uso: $0 {suspend|resume|status}" >&2
		echo "  suspend  -> POST /v1/daemon/stop  (pausa la escucha del núcleo, sin desemparejar)" >&2
		echo "  resume   -> POST /v1/daemon/start (reanuda la recepción 24/7)" >&2
		echo "  status   -> GET  /v1/daemon/status" >&2
		exit 2
		;;
esac
