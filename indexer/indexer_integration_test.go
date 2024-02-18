package indexer

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/encypher-studio/newsware-utils/nwelastic"
	"gopkg.in/yaml.v3"
)

type integrationConfiguration struct {
	Indexer Config
}

var _integrationCfg *integrationConfiguration

func integrationCfg() integrationConfiguration {
	if _integrationCfg == nil {
		_, filename, _, _ := runtime.Caller(0)
		configPath := path.Join(path.Dir(filename), "../config.test.yml")
		configBytes, err := os.ReadFile(configPath)
		if err != nil {
			panic(fmt.Errorf("failed to read config.test.yml for integration tests: %w", err))
		}

		err = yaml.Unmarshal(configBytes, &_integrationCfg)
		if err != nil {
			panic(fmt.Errorf("failed to unmarshal yaml: %w", err))
		}
	}

	return *_integrationCfg
}

func TestIndexer_Index_integration(t *testing.T) {
	integration := os.Getenv("INTEGRATION")
	if integration == "" {
		t.Skip("skipping: set INTEGRATION env to run this test")
	}

	tests := []struct {
		name        string
		expectedErr error
	}{
		{
			"success",
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := new(Config{
				Host:   integrationCfg().Indexer.Host,
				ApiKey: integrationCfg().Indexer.ApiKey,
			})
			err := i.Index(&nwelastic.News{Id: "1"})
			if err != nil || tt.expectedErr != nil {
				if tt.expectedErr == nil || err.Error() != tt.expectedErr.Error() {
					t.Fatalf("error is not as expected, got '%s', expected '%s'", err, tt.expectedErr)
				}
			}
		})
	}
}
