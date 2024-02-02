package indexer

import (
	"fmt"
	"math/rand"
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
		name         string
		responseCode int
		expectedErr  error
	}{
		{
			"success",
			200,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := New(Config{
				Host:   integrationCfg().Indexer.Host,
				ApiKey: integrationCfg().Indexer.ApiKey,
			})
			err := i.Index(&nwelastic.News{Id: rand.Int63()})
			if err != nil || tt.expectedErr != nil {
				if tt.expectedErr == nil || err.Error() != tt.expectedErr.Error() {
					t.Fatalf("error is not as expected, got '%s', expected '%s'", err, tt.expectedErr)
				}
			}
		})
	}
}

func TestIndexer_IndexBatch_integration(t *testing.T) {
	integration := os.Getenv("INTEGRATION")
	if integration == "" {
		t.Skip("skipping: set INTEGRATION env to run this test")
	}

	tests := []struct {
		name                 string
		news                 []*nwelastic.News
		responseCode         int
		expectedTotalIndexed int
		expectedLastIndex    int
		expectedErr          error
	}{
		{
			"success",
			[]*nwelastic.News{
				{
					Id: rand.Int63(),
				},
				{
					Id: rand.Int63(),
				},
			},
			200,
			2,
			1,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := New(Config{
				Host:   integrationCfg().Indexer.Host,
				ApiKey: integrationCfg().Indexer.ApiKey,
			})
			actualTotalIndexed, actualLastIndexed, err := i.IndexBatch(tt.news)
			if err != nil || tt.expectedErr != nil {
				if tt.expectedErr == nil || err.Error() != tt.expectedErr.Error() {
					t.Fatalf("error is not as expected, got '%s', expected '%s'", err, tt.expectedErr)
				}
			}

			if actualTotalIndexed != tt.expectedTotalIndexed {
				t.Fatalf("totalIndexed is not as expected, got %d, expected %d", actualTotalIndexed, tt.expectedTotalIndexed)
			}

			if actualLastIndexed != tt.expectedLastIndex {
				t.Fatalf("totalIndexed is not as expected, got %d, expected %d", actualLastIndexed, tt.expectedLastIndex)
			}
		})
	}
}
