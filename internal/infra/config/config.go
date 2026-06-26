// Package config define la configuracion del Edge Agent y su carga desde
// archivo YAML con overlay de variables de entorno (prefijo WAPP_AGENT_).
//
// Se apoya en github.com/EduGoGroup/wapp-shared/config para la lectura del YAML
// y el acceso tipado a variables de entorno.
package config

import (
	sharedconfig "github.com/EduGoGroup/wapp-shared/config"
)

// EnvPrefix es el prefijo aplicado a las variables de entorno del Edge Agent.
// Por ejemplo, la clave LOG_LEVEL se lee de la variable WAPP_AGENT_LOG_LEVEL.
const EnvPrefix = "WAPP_AGENT_"

// Config agrupa los parametros minimos de arranque del Edge Agent.
type Config struct {
	// LogLevel es el nivel minimo de logging: debug, info, warn o error.
	LogLevel string `yaml:"log_level"`
	// LogJSON selecciona el formato JSON del logger cuando es true.
	LogJSON bool `yaml:"log_json"`
	// DBPath es la ruta del store SQLite cifrado del cryptostore.
	DBPath string `yaml:"db_path"`
	// DEKPath es la ruta del material relacionado con la DEK custodiada localmente.
	DEKPath string `yaml:"dek_path"`
}

// defaults devuelve la configuracion con valores por defecto sensatos.
func defaults() Config {
	return Config{
		LogLevel: "info",
		LogJSON:  false,
		DBPath:   "wapp-edge.db",
		DEKPath:  "dek.key",
	}
}

// Load construye la configuracion del Edge Agent.
//
// Orden de precedencia (de menor a mayor): valores por defecto, archivo YAML en
// path (opcional; si no existe se ignora) y variables de entorno con prefijo
// WAPP_AGENT_. Devuelve error solo si el YAML existe pero no puede leerse o
// parsearse.
func Load(path string) (Config, error) {
	cfg := defaults()

	loader := sharedconfig.New(
		sharedconfig.WithFile(path),
		sharedconfig.WithEnvPrefix(EnvPrefix),
	)

	if err := loader.Unmarshal(&cfg); err != nil {
		return Config{}, err
	}

	// Overlay de entorno: usa el valor actual (default o YAML) como fallback.
	cfg.LogLevel = loader.GetString("LOG_LEVEL", cfg.LogLevel)
	cfg.LogJSON = loader.GetBool("LOG_JSON", cfg.LogJSON)
	cfg.DBPath = loader.GetString("DB_PATH", cfg.DBPath)
	cfg.DEKPath = loader.GetString("DEK_PATH", cfg.DEKPath)

	return cfg, nil
}
