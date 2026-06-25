package main

import (
	"fmt"
	"log"
)

func main() {
	// TODO: Inicializar el Edge Agent
	// - Cargar configuración (ruta del .db, modo DEK always-on/on-demand)
	// - Abrir KeyCustody: desbloquear DEK del keystore del SO
	// - Inicializar cryptostore sobre SQLite (adaptador Store)
	// - Restaurar sesiones whatsmeow (RestoreSessions)
	// - Abrir conexión gRPC saliente → CloudLink (adaptador CloudLink, mTLS)
	// - Registrar manejadores de eventos whatsmeow (Listen)
	// - Arrancar systray + mini-UI local
	// - Bloquear en loop de señales del SO (SIGTERM/SIGINT → graceful shutdown)

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	fmt.Println("wapp-edge-agent: placeholder — sin lógica aún")
	log.Println("TODO: implementar arranque real del daemon Edge")
}
