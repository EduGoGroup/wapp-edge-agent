// Package app contiene los casos de uso del Edge Agent.
// Casos de uso REALES (verificado 2026-08-12): Pair, Listen, Send, Logout, Outbox,
// más los subpaquetes diagnostics, health y sessionmgr. NO hay RunFlowStep: la
// máquina de estados vive entera en la nube (ADR-0005) y las respuestas llegan como
// SendText/SendMedia — el campo del contrato nunca tuvo productor ni consumidor.
// Depende solo del paquete domain y de los puertos (interfaces) definidos aquí:
//   - WhatsAppGateway: conectar, emparejar (QR), enviar, suscribir entrantes.
//   - Store:           persistencia local cifrada + cola outbox (SQLite).
//   - CloudLink:       canal saliente con la nube (órdenes, eventos, lease).
//   - KeyCustody:      custodia/entrega de la DEK y validación del lease.
package app
