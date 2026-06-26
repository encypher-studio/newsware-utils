package nwlogger

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
)

type Env string

const (
	EnvProduction Env = "production"
	EnvStaging    Env = "staging"
	EnvDev        Env = "dev"
)

type Config struct {
	Level string `yaml:"level"` // debug, info, warn, error (default: info)
	Env   Env    `yaml:"env"`
}

// WithDefaults returns a copy of c with any unset field filled from base. This
// lets a top-level logger config provide defaults that per-service sections
// override field-by-field.
func (c Config) WithDefaults(base Config) Config {
	if c.Level == "" {
		c.Level = base.Level
	}
	if c.Env == "" {
		c.Env = base.Env
	}
	return c
}

// New creates a zerolog.Logger that writes JSON to stdout. service must be
// non-empty; cfg.Env must be EnvProduction or EnvStaging.
func New(cfg Config, service string) (zerolog.Logger, error) {
	if cfg.Env == "" {
		cfg.Env = EnvDev
	}
	if cfg.Env != EnvProduction && cfg.Env != EnvStaging && cfg.Env != EnvDev {
		return zerolog.Nop(), fmt.Errorf("nwlogger: env must be %q, %q, or %q, got %q", EnvProduction, EnvStaging, EnvDev, cfg.Env)
	}
	if service == "" {
		return zerolog.Nop(), errors.New("nwlogger: service must not be empty")
	}

	level, err := zerolog.ParseLevel(string(cfg.Level))
	if err != nil || cfg.Level == "" {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)
	zerolog.TimeFieldFormat = time.RFC3339

	host, _ := os.Hostname()

	return zerolog.New(os.Stdout).
		With().
		Timestamp().
		Str("service", service).
		Str("env", string(cfg.Env)).
		Str("host", host).
		Logger(), nil
}
