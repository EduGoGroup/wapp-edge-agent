wApp Edge — kit portable (Windows / Linux amd64)
================================================

Este paquete contiene:
  - wapp-ctl(.exe)   El programa que arrancas tú.
  - agent(.exe)      El nucleo que wapp-ctl arranca por dentro (no lo ejecutes a mano).
  - config.yaml      Configuracion de arranque (endpoint de enrolamiento + CA publica).
  - ca.pem           Certificado publico de confianza (TLSCA). No es secreto.
  - README.txt       Este archivo.
  Windows:  run-edge.cmd, install-autostart.ps1, uninstall-autostart.ps1
  Linux:    wapp-edge.service.template, install-autostart.sh, uninstall-autostart.sh
            (activan que el Edge arranque solo al iniciar sesion — ver "ARRANCAR SOLO").

COMO ARRANCARLO
---------------
1. Descomprime este paquete en una carpeta tuya (el Escritorio sirve).
2. Ejecuta wapp-ctl:
     - Windows: doble-click en "wapp-ctl.exe".
                Si aparece "Windows protegio tu PC": Mas informacion -> Ejecutar de todas formas.
     - Linux:   en una terminal, dentro de la carpeta:  chmod +x wapp-ctl && ./wapp-ctl
3. Se abre tu navegador en:  http://127.0.0.1:8765
   (si no se abre solo, escribe esa direccion a mano en el navegador).
4. Pega el codigo de activacion que te pasaron y confirma.
5. Aparece un codigo QR: en el telefono, WhatsApp -> Dispositivos vinculados -> escanear.
6. Listo: envia y recibe un WhatsApp para probar.

Deja la ventana/terminal abierta: el Edge debe seguir corriendo para enviar y recibir.

ARRANCAR SOLO (sobrevivir a un reinicio)
----------------------------------------
Para que el Edge vuelva a arrancar solo cada vez que enciendes el equipo:
  - Windows: en la carpeta del kit, ejecuta en PowerShell:
       powershell -ExecutionPolicy Bypass -File install-autostart.ps1
       (para desactivarlo: uninstall-autostart.ps1)
  - Linux:   en una terminal, dentro de la carpeta:
       chmod +x install-autostart.sh && ./install-autostart.sh
       (para desactivarlo: ./uninstall-autostart.sh)
Los registros (logs) quedan en:
  - Windows: %AppData%\wApp\edge\logs\edge.log
  - Linux:   ~/.config/wApp/edge/logs/edge.log

El guion completo (paso a paso, resolucion de problemas y como sobrevivir a un reinicio)
lo entrega aparte quien te paso este paquete.
