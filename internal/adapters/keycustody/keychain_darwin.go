//go:build darwin

package keycustody

// keychain_darwin.go es la custodia de la DEK en macOS: la guarda como un ítem "generic password" en el
// Keychain del usuario (Security.framework) en vez de en un archivo plano. Es la ÚNICA parte del Edge que
// usa CGO (patrón db_postgres): queda acotada tras //go:build darwin, así los cross-compile windows/linux
// siguen pure-Go (ADR-0002) y el resto del árbol no ve C.
//
// Una entrada por dispositivo: service="com.wapp.edge", account=<session_id> (custodia multi-entrada del
// Plan 022 / ADR-0018). La DEK la custodia y la usa SOLO el núcleo, en el equipo; nunca cruza el plano de
// control ni sube a la nube (zero-knowledge, ADR-0007).

/*
#cgo LDFLAGS: -framework CoreFoundation -framework Security
#include <CoreFoundation/CoreFoundation.h>
#include <Security/Security.h>
#include <stdlib.h>
#include <string.h>

// wapp_dict construye un CFDictionary con la identidad del ítem (class genérica + service + account) más
// extraN pares clave/valor opcionales. CFDictionarySetValue retiene claves y valores, así que las
// CFString locales se liberan aquí; el diccionario devuelto es propiedad del llamador (CFRelease).
static CFDictionaryRef wapp_dict(const char *service, const char *account,
                                 const void **extraKeys, const void **extraVals, int extraN) {
	CFStringRef svc = CFStringCreateWithCString(NULL, service, kCFStringEncodingUTF8);
	CFStringRef acc = CFStringCreateWithCString(NULL, account, kCFStringEncodingUTF8);
	CFMutableDictionaryRef d = CFDictionaryCreateMutable(NULL, 3 + extraN,
		&kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
	CFDictionarySetValue(d, kSecClass, kSecClassGenericPassword);
	CFDictionarySetValue(d, kSecAttrService, svc);
	CFDictionarySetValue(d, kSecAttrAccount, acc);
	for (int i = 0; i < extraN; i++) {
		CFDictionarySetValue(d, extraKeys[i], extraVals[i]);
	}
	CFRelease(svc);
	CFRelease(acc);
	return d;
}

// wapp_keychain_store guarda data (dataLen bytes) para (service, account), sobrescribiendo cualquier ítem
// previo (delete + add) para que sea idempotente. Devuelve 0 en éxito, o el OSStatus (!=0) del fallo.
static int wapp_keychain_store(const char *service, const char *account, const void *data, int dataLen) {
	CFDictionaryRef delQ = wapp_dict(service, account, NULL, NULL, 0);
	SecItemDelete(delQ); // ignora errSecItemNotFound: solo garantizamos que no quede un duplicado
	CFRelease(delQ);

	CFDataRef val = CFDataCreate(NULL, (const UInt8 *)data, dataLen);
	const void *ks[1] = { kSecValueData };
	const void *vs[1] = { val };
	CFDictionaryRef addQ = wapp_dict(service, account, ks, vs, 1);
	OSStatus st = SecItemAdd(addQ, NULL);
	CFRelease(addQ);
	CFRelease(val);
	if (st == errSecSuccess) {
		return 0;
	}
	return (int)st;
}

// wapp_keychain_load recupera la DEK de (service, account). Devuelve 0 y rellena *out (malloc, el llamador
// hace free) + *outLen en éxito; 1 si no existe (errSecItemNotFound); el OSStatus (<0) en otro error.
static int wapp_keychain_load(const char *service, const char *account, void **out, int *outLen) {
	const void *ks[2] = { kSecReturnData, kSecMatchLimit };
	const void *vs[2] = { kCFBooleanTrue, kSecMatchLimitOne };
	CFDictionaryRef q = wapp_dict(service, account, ks, vs, 2);
	CFTypeRef result = NULL;
	OSStatus st = SecItemCopyMatching(q, &result);
	CFRelease(q);
	if (st == errSecItemNotFound) {
		return 1;
	}
	if (st != errSecSuccess) {
		return (int)st;
	}
	CFDataRef data = (CFDataRef)result;
	CFIndex len = CFDataGetLength(data);
	void *buf = malloc((size_t)len);
	if (buf == NULL) {
		CFRelease(result);
		return -1;
	}
	memcpy(buf, CFDataGetBytePtr(data), (size_t)len);
	CFRelease(result);
	*out = buf;
	*outLen = (int)len;
	return 0;
}

// wapp_keychain_exists indica si existe el ítem: 0 sí, 1 no, OSStatus (<0) en error.
static int wapp_keychain_exists(const char *service, const char *account) {
	const void *ks[1] = { kSecMatchLimit };
	const void *vs[1] = { kSecMatchLimitOne };
	CFDictionaryRef q = wapp_dict(service, account, ks, vs, 1);
	OSStatus st = SecItemCopyMatching(q, NULL);
	CFRelease(q);
	if (st == errSecSuccess) {
		return 0;
	}
	if (st == errSecItemNotFound) {
		return 1;
	}
	return (int)st;
}

// wapp_keychain_delete borra el ítem. Idempotente: errSecItemNotFound cuenta como éxito (0). OSStatus en
// otro fallo.
static int wapp_keychain_delete(const char *service, const char *account) {
	CFDictionaryRef q = wapp_dict(service, account, NULL, NULL, 0);
	OSStatus st = SecItemDelete(q);
	CFRelease(q);
	if (st == errSecSuccess || st == errSecItemNotFound) {
		return 0;
	}
	return (int)st;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"
)

// keychainService es el service de todos los ítems de DEK del Edge en el Keychain (ADR-0018: identidad por
// dispositivo bajo un service común; el account discrimina el session_id).
const keychainService = "com.wapp.edge"

// errKeychainNotFound señala que no hay ítem en el Keychain para la cuenta (errSecItemNotFound). Interno:
// Load lo traduce a ErrNoKey (o dispara la migración desde archivo).
var errKeychainNotFound = errors.New("keycustody: no hay ítem en el Keychain para esta cuenta")

// KeychainCustody implementa app.KeyCustody (Store/Load/Exists) + Clear() respaldándose en el Keychain de
// macOS. Es el equivalente darwin de FileCustody, con la MISMA firma de constructor para que el núcleo no
// cambie. legacyPath es la ruta del archivo plano heredado (keys/<id>.key o dek.key): solo se usa como
// ORIGEN de la migración one-shot archivo→Keychain, nunca como almacén.
type KeychainCustody struct {
	account    string
	legacyPath string
}

// NewFileCustody construye la custodia de la DEK en darwin. MISMA firma que la impl pure-Go (file.go bajo
// !darwin) para que los llamadores (cmd/agent, sessionmgr, edgemigrate) no cambien. path es la ruta legacy
// de la DEK (layout.DEKPath(id)=keys/<id>.key, o cfg.DEKPath=dek.key); de su nombre base se deriva el
// account del Keychain (el session_id). El archivo no es el almacén: solo el origen de migración.
func NewFileCustody(path string) *KeychainCustody {
	return &KeychainCustody{account: accountFromPath(path), legacyPath: path}
}

// accountFromPath deriva el account del Keychain del nombre base de la ruta, sin extensión: para el layout
// multi-sesión keys/<session_id>.key → <session_id> (el Manager ya validó el UUID en Layout.DEKPath); para
// la ruta legacy single-session dek.key → "dek". Nunca vacío: cae a "default" si el base no aporta nombre.
func accountFromPath(path string) string {
	base := filepath.Base(path)
	base = strings.TrimSuffix(base, filepath.Ext(base))
	if base == "" || base == "." || base == string(filepath.Separator) {
		return "default"
	}
	return base
}

// Store persiste la DEK en el Keychain (sobrescribe la previa de la misma cuenta). Devuelve ErrKeySize si
// no mide KeySize bytes. NO toca el archivo legacy: la migración/limpieza del archivo la hacen Load/Clear.
func (c *KeychainCustody) Store(dek []byte) error {
	if len(dek) != KeySize {
		return ErrKeySize
	}
	cService := C.CString(keychainService)
	cAccount := C.CString(c.account)
	defer C.free(unsafe.Pointer(cService))
	defer C.free(unsafe.Pointer(cAccount))

	st := C.wapp_keychain_store(cService, cAccount, unsafe.Pointer(&dek[0]), C.int(len(dek)))
	if st != 0 {
		return fmt.Errorf("keycustody: no se pudo guardar la DEK en el Keychain (OSStatus %d, cuenta %q)", int(st), c.account)
	}
	return nil
}

// Load recupera la DEK. Si el Keychain la tiene, la devuelve (y retira cualquier archivo plano legacy que
// quedara). Si no la tiene pero existe el archivo legacy, MIGRA (importa al Keychain, verifica la relectura
// y borra el archivo). Devuelve ErrNoKey si no hay ni una ni otro.
func (c *KeychainCustody) Load() ([]byte, error) {
	dek, err := c.keychainLoad()
	if err == nil {
		c.removeLegacyFile() // el Keychain manda: no dejar la DEK también en disco plano
		return dek, nil
	}
	if !errors.Is(err, errKeychainNotFound) {
		return nil, err
	}

	// Miss en Keychain: intenta la migración one-shot desde el archivo plano legacy (idempotente; verifica
	// la relectura desde el Keychain ANTES de borrar el archivo). rawKeychain evita recursión con Load.
	migratedDEK, migErr := c.migrateFromLegacy()
	if migErr != nil {
		return nil, migErr
	}
	if migratedDEK != nil {
		return migratedDEK, nil
	}
	return nil, fmt.Errorf("%w: cuenta %q del Keychain", ErrNoKey, c.account)
}

// Exists indica si hay DEK disponible: en el Keychain, o en el archivo legacy pendiente de migrar.
func (c *KeychainCustody) Exists() bool {
	cService := C.CString(keychainService)
	cAccount := C.CString(c.account)
	defer C.free(unsafe.Pointer(cService))
	defer C.free(unsafe.Pointer(cAccount))

	if C.wapp_keychain_exists(cService, cAccount) == 0 {
		return true
	}
	return c.legacyFileExists()
}

// Clear borra la entrada del Keychain Y cualquier archivo plano legacy (borrado quirúrgico por sesión,
// ADR-0016 §3). Idempotente: borrar algo ausente no es error. La limpieza al desvincular no debe dejar la
// DEK ni en Keychain ni en disco.
func (c *KeychainCustody) Clear() error {
	cService := C.CString(keychainService)
	cAccount := C.CString(c.account)
	defer C.free(unsafe.Pointer(cService))
	defer C.free(unsafe.Pointer(cAccount))

	st := C.wapp_keychain_delete(cService, cAccount)
	rmErr := c.removeLegacyFileErr()
	if st != 0 {
		return fmt.Errorf("keycustody: no se pudo borrar la DEK del Keychain (OSStatus %d, cuenta %q)", int(st), c.account)
	}
	return rmErr
}

// keychainLoad lee la DEK DIRECTAMENTE del Keychain (sin migración). Devuelve errKeychainNotFound si no
// existe. Usado por Load y por la verificación de la migración (rawKeychain), para no recursar.
func (c *KeychainCustody) keychainLoad() ([]byte, error) {
	cService := C.CString(keychainService)
	cAccount := C.CString(c.account)
	defer C.free(unsafe.Pointer(cService))
	defer C.free(unsafe.Pointer(cAccount))

	var out unsafe.Pointer
	var outLen C.int
	st := C.wapp_keychain_load(cService, cAccount, &out, &outLen)
	if st == 1 {
		return nil, errKeychainNotFound
	}
	if st != 0 {
		return nil, fmt.Errorf("keycustody: no se pudo leer la DEK del Keychain (OSStatus %d, cuenta %q)", int(st), c.account)
	}
	defer C.free(out)

	dek := C.GoBytes(out, outLen)
	if len(dek) != KeySize {
		return nil, fmt.Errorf("keycustody: la DEK en Keychain mide %d bytes, se esperaban %d (cuenta %q)", len(dek), KeySize, c.account)
	}
	return dek, nil
}

// migrateFromLegacy ejecuta la migración archivo→Keychain reusando la pieza pure-Go migrateFileToCustody
// (verifica antes de borrar). rawKeychain expone Store/keychainLoad DIRECTOS para que la verificación no
// vuelva a entrar en Load (recursión). Devuelve la DEK migrada (nil si no había archivo).
func (c *KeychainCustody) migrateFromLegacy() ([]byte, error) {
	dek, migrated, err := migrateFileToCustody(c.legacyPath, rawKeychain{c: c})
	if err != nil {
		return nil, err
	}
	if !migrated {
		return nil, nil
	}
	return dek, nil
}

// rawKeychain adapta el Keychain al puerto dekSink de la migración con operaciones DIRECTAS (sin la lógica
// de migración de Load), evitando recursión durante la verificación.
type rawKeychain struct{ c *KeychainCustody }

func (r rawKeychain) Store(dek []byte) error { return r.c.Store(dek) }
func (r rawKeychain) Load() ([]byte, error)  { return r.c.keychainLoad() }

// legacyFileExists indica si el archivo plano legacy existe y es regular.
func (c *KeychainCustody) legacyFileExists() bool {
	if c.legacyPath == "" {
		return false
	}
	info, err := os.Stat(c.legacyPath)
	return err == nil && info.Mode().IsRegular()
}

// removeLegacyFile borra el archivo plano legacy si existe, ignorando el error (best-effort).
func (c *KeychainCustody) removeLegacyFile() {
	_ = c.removeLegacyFileErr()
}

// removeLegacyFileErr borra el archivo plano legacy de forma idempotente (ausente ⇒ no es error).
func (c *KeychainCustody) removeLegacyFileErr() error {
	if c.legacyPath == "" {
		return nil
	}
	if err := os.Remove(c.legacyPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("keycustody: no se pudo borrar el archivo plano legacy %s: %w", c.legacyPath, err)
	}
	return nil
}
