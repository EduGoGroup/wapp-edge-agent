wApp Edge — kit portable (Windows / Linux amd64)
================================================

Este paquete contiene:
  - wapp-ctl(.exe)   El programa que arrancas tú.
  - agent(.exe)      El nucleo que wapp-ctl arranca por dentro (no lo ejecutes a mano).
  - config.yaml      Configuracion de arranque (endpoint de enrolamiento + CA publica).
  - ca.pem           Certificado publico de confianza (TLSCA). No es secreto.
  - README.txt       Este archivo.

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

El guion completo (paso a paso, resolucion de problemas y como sobrevivir a un reinicio)
lo entrega aparte quien te paso este paquete.
