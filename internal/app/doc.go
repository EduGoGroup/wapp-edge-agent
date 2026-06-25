// Package app contiene los casos de uso del Edge Agent.
// Casos de uso previstos: Pair, Connect, RestoreSessions, Listen, Send, RunFlowStep.
// Depende solo del paquete domain y de los puertos (interfaces) definidos aquí:
//   - WhatsAppGateway: conectar, emparejar (QR), enviar, suscribir entrantes.
//   - Store:           persistencia local cifrada + cola outbox (SQLite).
//   - CloudLink:       canal saliente con la nube (órdenes, eventos, lease).
//   - KeyCustody:      custodia/entrega de la DEK y validación del lease.
package app
