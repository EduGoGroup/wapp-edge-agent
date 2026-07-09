package cryptostore

// cryptoContainer: decorator de store.DeviceContainer que cifra los campos sensibles
// DIRECTOS del *store.Device (NoiseKey, IdentityKey, SignedPreKey, AdvSecretKey, Account)
// antes de persistir, y los descifra al leer, rehidratando un Device usable con los stores
// de sesión cifrados reinyectados vía Device.SetAllStores.
//
// Persiste en una tabla propia msg_enc_device con columnas BLOB libres (el esquema upstream
// whatsmeow_device tiene CHECK length=32/64 que NO admite el ciphertext GCM). El esquema
// (msg_enc_*) lo crea el runner de migración (internal/infra/db); este decorator asume las
// tablas ya creadas y solo crea las whatsmeow_* (no sensibles) vía sqlstore.Upgrade.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/EduGoGroup/wapp-shared/envelope"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
)

type cryptoContainer struct {
	inner *sqlstore.Container // Container whatsmeow (de él cuelgan los SQLStore por device); puede ser COMPARTIDO
	db    *sql.DB             // conexión cruda (tablas msg_enc_* propias)
	env   *envelope.Envelope  // envelope de la DEK de UN dispositivo (Plan 022 T2: per-device, NUNCA global)
}

var _ store.DeviceContainer = (*cryptoContainer)(nil)

// newCryptoContainer construye el decorator sobre una BD YA migrada (las msg_enc_* las crea el
// runner de migración, internal/infra/db). Construye el *sqlstore.Container interno con el dialecto
// dado (Plan 022 T0: ya NO hardcodea "sqlite"; el llamante pasa DialectSQLite|DialectPostgres) y
// ejecuta Upgrade (idempotente) para crear las tablas whatsmeow_* no sensibles de las que cuelgan los
// SQLStore por device. El dialecto es el mismo que abrió la BD (Open) para que whatsmeow emita el SQL
// correcto por motor.
func newCryptoContainer(ctx context.Context, db *sql.DB, dialect string, env *envelope.Envelope) (*cryptoContainer, error) {
	inner := sqlstore.NewWithDB(db, dialect, nil)
	// Serializado POR *sql.DB (ver upgrade.go): sobre la BD única compartida, N listeners arrancan a la vez
	// y construyen su Container concurrentemente; sin este candado el CREATE TABLE de whatsmeow_version
	// choca ("already exists") y degrada la sesión con un WARN espurio + backoff en el arranque en frío.
	if err := upgradeWhatsmeowSchema(ctx, inner, db); err != nil {
		return nil, fmt.Errorf("upgrade esquema whatsmeow: %w", err)
	}
	return &cryptoContainer{inner: inner, db: db, env: env}, nil
}

// wrapStores reinyecta el cryptoStore (sesiones cifradas) en un Device.
//
// SetAllStores cablea los stores ESPECÍFICOS de sesión (Identities, Sessions, PreKeys,
// SenderKeys, AppState(Keys), Contacts, ChatSettings, MsgSecrets, PrivacyTokens, NCTSalt,
// EventBuffer). El LIDStore (mapeo LID↔PN) es un store GLOBAL que SetAllStores NO toca: el
// sqlstore nativo lo cablea aparte. Sin esta línea, d.LIDs queda nil y whatsmeow paniquea con un
// nil pointer dereference durante handlePair, justo tras el pair-success del escaneo.
//
// El mapeo LID↔PN NO es material criptográfico sensible (solo asocia identificadores), así que se
// respalda con el LIDMap nativo de whatsmeow (tabla whatsmeow_lid_map, ya migrada) en vez de
// cifrarlo: mismo patrón mixto (stores sensibles en msg_enc_*, no sensibles en whatsmeow_*).
func (c *cryptoContainer) wrapStores(d *store.Device) {
	inner := sqlstore.NewSQLStore(c.inner, *d.ID)
	cs := newCryptoStore(inner, c.db, c.env, *d.ID)
	d.SetAllStores(cs)
	d.LIDs = c.inner.LIDMap // store GLOBAL (no sensible): LIDMap nativo sobre whatsmeow_lid_map.
	d.Container = c
	d.Initialized = true
}

// newDevice fabrica un Device NUEVO (claves frescas, sin JID) cuyo Container es ESTE decorator.
// Calca lo que hace sqlstore.Container.NewDevice pero apunta Container al cryptoContainer en vez
// de al *sqlstore.Container plano: así, cuando whatsmeow llame Device.Save al completar el
// pairing, la persistencia pasa por PutDevice (cifrado). Los stores de sesión quedan en nil hasta
// wrapStores (1ª escritura).
func (c *cryptoContainer) newDevice() *store.Device {
	d := c.inner.NewDevice() // claves frescas + SignedPreKey, con Container = c.inner (plano)
	d.Container = c          // reapuntar al decorator: Save -> PutDevice cifra
	return d
}

func (c *cryptoContainer) PutDevice(ctx context.Context, d *store.Device) error {
	if d.ID == nil {
		return errors.New("device JID debe estar definido antes de persistir")
	}
	sealAll := func(parts ...[]byte) ([][]byte, error) {
		out := make([][]byte, len(parts))
		for i, p := range parts {
			ct, err := c.env.Seal(p)
			if err != nil {
				return nil, err
			}
			out[i] = ct
		}
		return out, nil
	}
	// Metadata NO-clave del device propio (§10.D / follow-up Plan 013): se cifra con la MISMA DEK y el
	// MISMO helper (Seal) que el material de clave, para que el "gafete" real (PushName/BusinessName/LID)
	// sobreviva al reinicio en vez de degradar al fallback config. El LID (types.JID) se persiste como su
	// forma string; zero => cadena vacía (al leer no se puebla). Nunca se loguea (JID/LID = PII).
	lidStr := ""
	if !d.LID.IsEmpty() {
		lidStr = d.LID.String()
	}
	cts, err := sealAll(
		d.NoiseKey.Priv[:], d.IdentityKey.Priv[:],
		d.SignedPreKey.Priv[:], d.SignedPreKey.Signature[:],
		d.AdvSecretKey,
		d.Account.Details, d.Account.AccountSignature, d.Account.AccountSignatureKey, d.Account.DeviceSignature,
		[]byte(d.PushName), []byte(d.BusinessName), []byte(lidStr),
	)
	if err != nil {
		return err
	}
	// INSERT OR REPLACE (UPSERT de SQLite): la fila es un reemplazo completo por JID, así que
	// sustituye al ON CONFLICT ... DO UPDATE del original PostgreSQL.
	_, err = c.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO msg_enc_device
			(jid, registration_id, signed_pre_key_id,
			 noise_priv, identity_priv, signed_pre_key_priv, signed_pre_key_sig, adv_secret_key,
			 adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig,
			 push_name, business_name, lid)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		d.ID.String(), d.RegistrationID, d.SignedPreKey.KeyID,
		cts[0], cts[1], cts[2], cts[3], cts[4], cts[5], cts[6], cts[7], cts[8],
		cts[9], cts[10], cts[11],
	)
	if err != nil {
		return err
	}
	if !d.Initialized {
		c.wrapStores(d)
	}
	return nil
}

func (c *cryptoContainer) GetDevice(ctx context.Context, jid types.JID) (*store.Device, error) {
	var (
		regID       uint32
		spkID       uint32
		noiseCT     []byte
		idCT        []byte
		spkPrivCT   []byte
		spkSigCT    []byte
		advSecretCT []byte
		advDetCT    []byte
		advASigCT   []byte
		advASKeyCT  []byte
		advDSigCT   []byte
		// Metadata NO-clave (nullable): un store viejo migrado por ALTER las trae NULL => nil.
		pushNameCT     []byte
		businessNameCT []byte
		lidCT          []byte
	)
	err := c.db.QueryRowContext(ctx, `
		SELECT registration_id, signed_pre_key_id, noise_priv, identity_priv,
		       signed_pre_key_priv, signed_pre_key_sig, adv_secret_key,
		       adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig,
		       push_name, business_name, lid
		FROM msg_enc_device WHERE jid=?`, jid.String()).Scan(
		&regID, &spkID, &noiseCT, &idCT, &spkPrivCT, &spkSigCT, &advSecretCT,
		&advDetCT, &advASigCT, &advASKeyCT, &advDSigCT,
		&pushNameCT, &businessNameCT, &lidCT,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	open := func(ct []byte) ([]byte, error) { return c.env.Open(ct) }
	noise, err := open(noiseCT)
	if err != nil {
		return nil, err
	}
	idPriv, err := open(idCT)
	if err != nil {
		return nil, err
	}
	spkPriv, err := open(spkPrivCT)
	if err != nil {
		return nil, err
	}
	spkSig, err := open(spkSigCT)
	if err != nil {
		return nil, err
	}
	advSecret, err := open(advSecretCT)
	if err != nil {
		return nil, err
	}
	advDet, err := open(advDetCT)
	if err != nil {
		return nil, err
	}
	advASig, err := open(advASigCT)
	if err != nil {
		return nil, err
	}
	advASKey, err := open(advASKeyCT)
	if err != nil {
		return nil, err
	}
	advDSig, err := open(advDSigCT)
	if err != nil {
		return nil, err
	}
	// Metadata NO-clave: nullable, así que un ciphertext ausente (store viejo) degrada a "" en vez de
	// fallar. Mismo helper Open (misma DEK) que el resto de campos.
	openStr := func(ct []byte) (string, error) {
		if len(ct) == 0 {
			return "", nil
		}
		pt, err := c.env.Open(ct)
		if err != nil {
			return "", err
		}
		return string(pt), nil
	}
	pushName, err := openStr(pushNameCT)
	if err != nil {
		return nil, err
	}
	businessName, err := openStr(businessNameCT)
	if err != nil {
		return nil, err
	}
	lidStr, err := openStr(lidCT)
	if err != nil {
		return nil, err
	}
	if len(noise) != 32 || len(idPriv) != 32 || len(spkPriv) != 32 || len(spkSig) != 64 {
		return nil, fmt.Errorf("longitudes descifradas inválidas: noise=%d id=%d spk=%d sig=%d",
			len(noise), len(idPriv), len(spkPriv), len(spkSig))
	}

	jidCopy := jid
	d := &store.Device{
		ID:             &jidCopy,
		RegistrationID: regID,
		NoiseKey:       keys.NewKeyPairFromPrivateKey(*(*[32]byte)(noise)),
		IdentityKey:    keys.NewKeyPairFromPrivateKey(*(*[32]byte)(idPriv)),
		AdvSecretKey:   advSecret,
		Account: &waAdv.ADVSignedDeviceIdentity{
			Details:             advDet,
			AccountSignature:    advASig,
			AccountSignatureKey: advASKey,
			DeviceSignature:     advDSig,
		},
	}
	d.SignedPreKey = &keys.PreKey{
		KeyPair:   *keys.NewKeyPairFromPrivateKey(*(*[32]byte)(spkPriv)),
		KeyID:     spkID,
		Signature: (*[64]byte)(spkSig),
	}
	// Metadata NO-clave del device propio: repuebla el "gafete" real para que sobreviva al reinicio
	// (el consumo en listen_gateway prefiere Store.PushName al fallback config). Vacío => se deja el
	// cero-value (store viejo o sin dato aún): degrada al fallback, sin romper.
	d.PushName = pushName
	d.BusinessName = businessName
	if lidStr != "" {
		lid, err := types.ParseJID(lidStr)
		if err != nil {
			return nil, fmt.Errorf("LID persistido inválido: %w", err)
		}
		d.LID = lid
	}
	c.wrapStores(d)
	return d, nil
}

func (c *cryptoContainer) DeleteDevice(ctx context.Context, d *store.Device) error {
	if d.ID == nil {
		return errors.New("device JID debe estar definido")
	}
	_, err := c.db.ExecContext(ctx, `DELETE FROM msg_enc_device WHERE jid=?`, d.ID.String())
	return err
}
