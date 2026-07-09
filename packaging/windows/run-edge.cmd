@echo off
rem ===========================================================================
rem  run-edge.cmd — lanzador de autoarranque del Edge de wApp en Windows.
rem  (Plan 024 · T1). Fija el entorno ESTABLE (data_dir/config/log) y arranca
rem  wapp-ctl.exe --no-open --autostart (que a su vez lanza agent.exe serve).
rem
rem  NO se ejecuta agent.exe directo: el nucleo lo arranca wapp-ctl (supervisor).
rem  %~dp0 = carpeta de ESTE .cmd → resuelve el layout HERMANO (agent.exe junto
rem  a wapp-ctl.exe), asi el autostart funciona con cualquier CWD.
rem
rem  El acceso directo de la carpeta Startup (install-autostart.ps1) apunta aqui.
rem  La ventana de consola queda visible en v1 (ocultarla = follow-up).
rem ===========================================================================

rem Carpeta del kit (donde viven wapp-ctl.exe y agent.exe), sin barra final.
set "WAPP_HERE=%~dp0"
if "%WAPP_HERE:~-1%"=="\" set "WAPP_HERE=%WAPP_HERE:~0,-1%"

rem RUTA SAGRADA por-SO: data_dir estable = %AppData%\wApp\edge (NO el CWD).
set "WAPP_AGENT_DATA_DIR=%AppData%\wApp\edge"
set "WAPP_AGENT_CONFIG=%WAPP_AGENT_DATA_DIR%\config.yaml"
set "WAPP_CTL_AGENT_BIN=%WAPP_HERE%\agent.exe"
set "WAPP_LOG_FILE=%WAPP_AGENT_DATA_DIR%\logs\edge.log"

rem Asegura el directorio de logs (el logger tambien hace MkdirAll, cinturon+tirantes).
if not exist "%WAPP_AGENT_DATA_DIR%\logs" mkdir "%WAPP_AGENT_DATA_DIR%\logs"

rem CWD estable = la carpeta del kit (donde esta la config de bootstrap ca.pem/config.yaml).
cd /d "%WAPP_HERE%"

"%WAPP_HERE%\wapp-ctl.exe" --no-open --autostart
