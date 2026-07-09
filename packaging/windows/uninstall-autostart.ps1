# ============================================================================
#  uninstall-autostart.ps1 — desactiva el autoarranque del Edge de wApp (Windows).
#  (Plan 024 · T1). Borra el acceso directo "wApp Edge.lnk" de la carpeta Startup
#  colocado por install-autostart.ps1. NO borra el data_dir ni los logs.
#
#  Uso:  powershell -ExecutionPolicy Bypass -File uninstall-autostart.ps1
#
#  Si activaste el arranque por Task Scheduler en vez del .lnk, quitalo con:
#     schtasks /delete /tn "wApp Edge" /f
# ============================================================================
$ErrorActionPreference = 'Stop'

$startup = [Environment]::GetFolderPath('Startup')
$lnkPath = Join-Path $startup 'wApp Edge.lnk'

if (Test-Path $lnkPath) {
    Remove-Item $lnkPath -Force
    Write-Host "Autostart desactivado: eliminado $lnkPath"
} else {
    Write-Host "No habia acceso directo de autostart en $startup (nada que hacer)."
}
Write-Host "El Edge ya no arrancara solo al iniciar sesion. Para pararlo ahora, cierra su ventana."
