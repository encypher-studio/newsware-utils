package nwlogger

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

type Config struct {
	Level  string `yaml:"level"`  // debug, info, warn, error (default: info)
	Pretty bool   `yaml:"pretty"` // human-readable console output for dev
}

// New creates a zerolog.Logger that writes JSON to stdout, or human-readable
// output when Pretty is true. Level defaults to info if unset or invalid.
func New(cfg Config) zerolog.Logger {
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil || cfg.Level == "" {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	var w io.Writer = os.Stdout
	if cfg.Pretty {
		w = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	}
	return zerolog.New(w).With().Timestamp().Logger()
}
