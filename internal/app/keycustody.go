package app

// KeyCustody es el puerto de custodia de la DEK (Data Encryption Key, 32 bytes,
// AES-256) del Edge Agent. Abstrae *dónde* y *cómo* se guarda el material de
// llave para que el resto de la aplicación dependa solo de este contrato y no
// del backend concreto (archivo local, keystore del SO, dispositivo Guardián).
//
// El cambio de custodio (ADR-0007 / ADR-0013) debe ser solo un cambio de
// adaptador: ninguna otra capa conoce el almacenamiento subyacente.
//
// La DEK nunca sale hacia la nube (principio zero-knowledge): este puerto opera
// exclusivamente en el equipo del cliente.
type KeyCustody interface {
	// Store persiste la DEK (debe medir exactamente 32 bytes). Sobrescribe
	// cualquier DEK previamente custodiada.
	Store(dek []byte) error

	// Load recupera la DEK custodiada. Devuelve un error claro si todavía no
	// se ha custodiado ninguna (ver Exists).
	Load() ([]byte, error)

	// Exists indica si hay una DEK ya custodiada disponible para Load.
	Exists() bool
}
