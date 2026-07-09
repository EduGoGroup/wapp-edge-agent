# ============================================================================
#  install-autostart.ps1 — activa el autoarranque del Edge de wApp en Windows.
#  (Plan 024 · T1). Coloca un acceso directo (.lnk) a run-edge.cmd en la carpeta
#  Startup del usuario ([Environment]::GetFolderPath('Startup')), de modo que al
#  INICIAR SESION Windows lance wapp-ctl.exe --no-open --autostart.
#
#  Es POR-USUARIO (sin admin): la DEK vive en el keystore del usuario (archivo
#  0600 en v1). No usa servicios de sistema (contexto distinto no veria la DEK).
#
#  Uso (en la carpeta del kit, doble-click no sirve para .ps1):
#     powershell -ExecutionPolicy Bypass -File install-autostart.ps1
#
#  Alternativa documentada — Task Scheduler (arranca aunque el .lnk se pierda):
#     schtasks /create /tn "wApp Edge" /sc onlogon /tr "\"%~dp0run-edge.cmd\""
#  o la clave de registro HKCU\Software\Microsoft\Windows\CurrentVersion\Run.
#
#  La ventana de consola queda visible en v1 (ocultarla con un .vbs/wscript o un
#  wrapper sin consola = follow-up, no bloquea el test delegado).
# ============================================================================
$ErrorActionPreference = 'Stop'

# Carpeta de ESTE script = carpeta del kit (run-edge.cmd es hermano).
$here    = Split-Path -Parent $MyInvocation.MyCommand.Definition
$target  = Join-Path $here 'run-edge.cmd'
if (-not (Test-Path $target)) {
    throw "No encuentro run-edge.cmd junto a este script ($target)."
}

# Crea el directorio de logs bajo la RUTA SAGRADA (%AppData%\wApp\edge\logs).
$dataDir = Join-Path $env:AppData 'wApp\edge'
$logsDir = Join-Path $dataDir 'logs'
New-Item -ItemType Directory -Force -Path $logsDir | Out-Null

# Acceso directo en la carpeta Startup del usuario.
$startup  = [Environment]::GetFolderPath('Startup')
$lnkPath  = Join-Path $startup 'wApp Edge.lnk'

$shell = New-Object -ComObject WScript.Shell
$lnk   = $shell.CreateShortcut($lnkPath)
$lnk.TargetPath       = $target
$lnk.WorkingDirectory = $here      # CWD estable = carpeta del kit (bootstrap ca.pem/config.yaml).
$lnk.WindowStyle      = 7          # 7 = minimizada (la consola no molesta en primer plano).
$lnk.Description      = 'wApp Edge — arranque al iniciar sesion'
$lnk.Save()

Write-Host "Autostart activado."
Write-Host "  Acceso directo : $lnkPath"
Write-Host "  Lanza          : $target (wapp-ctl.exe --no-open --autostart)"
Write-Host "  data_dir       : $dataDir"
Write-Host "  logs           : $logsDir\edge.log"
Write-Host "Plano de control : http://127.0.0.1:8765"
Write-Host "Se activara en el proximo inicio de sesion. Para probar ya, ejecuta run-edge.cmd."
