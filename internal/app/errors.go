package app

import "errors"

// Errores canónicos de la capa de aplicación (puertos/casos de uso). Definidos aquí para que los
// adaptadores y consumidores importen un solo punto estable, sin acoplarse a un caso de uso concreto.

// ErrSessionNotFound lo devuelve SessionStore.Get cuando el session_id no existe en la BD.
var ErrSessionNotFound = errors.New("sessionstore: sesión no encontrada")
