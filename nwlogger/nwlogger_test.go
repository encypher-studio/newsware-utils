package nwlogger

import "testing"

func TestConfigWithDefaults(t *testing.T) {
	base := Config{Level: "info", Env: EnvProduction}

	tests := []struct {
		name     string
		cfg      Config
		base     Config
		expected Config
	}{
		{
			name:     "empty inherits both fields",
			cfg:      Config{},
			base:     base,
			expected: Config{Level: "info", Env: EnvProduction},
		},
		{
			name:     "partial override keeps other field",
			cfg:      Config{Level: "error"},
			base:     base,
			expected: Config{Level: "error", Env: EnvProduction},
		},
		{
			name:     "env override keeps level",
			cfg:      Config{Env: EnvStaging},
			base:     base,
			expected: Config{Level: "info", Env: EnvStaging},
		},
		{
			name:     "full override wins",
			cfg:      Config{Level: "debug", Env: EnvDev},
			base:     base,
			expected: Config{Level: "debug", Env: EnvDev},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.WithDefaults(tt.base)
			if got != tt.expected {
				t.Errorf("WithDefaults() = %+v, want %+v", got, tt.expected)
			}
		})
	}
}
